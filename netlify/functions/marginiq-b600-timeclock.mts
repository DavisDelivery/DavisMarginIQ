import type { Context, Config } from "@netlify/functions";

/**
 * Scheduled weekly pull of B600 time clock data.
 *
 * Runs every Monday at 9:00 AM ET. Pulls the previous Mon–Sun window from the
 * B600 CyberPay web UI (via AtlantaFreightQuotes public tunnel), parses the CSV,
 * rolls up by Friday-ending week, and writes to Firestore (timeclock_weekly).
 *
 * Collections written:
 *   - timeclock_weekly/{YYYY-MM-DD}   (aggregates keyed by Friday week-ending)
 *   - marginiq_config/b600_last_pull  (metadata: last_run_at, last_window, rows)
 *
 * Environment variables (set in Netlify site config):
 *   FIREBASE_API_KEY   — Firestore REST API key
 *   B600_BASE_URL      — e.g. https://b600.atlantafreightquotes.com  (no trailing slash)
 *   B600_USERNAME      — CyberPay login
 *   B600_PASSWORD      — CyberPay login
 *   B600_EXPORT_PATH   — path to CSV export endpoint (default: /reports/timeclock/export)
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const B600_BASE_URL = process.env["B600_BASE_URL"] || "https://b600.atlantafreightquotes.com";
const B600_USERNAME = process.env["B600_USERNAME"];
const B600_PASSWORD = process.env["B600_PASSWORD"];
const B600_EXPORT_PATH = process.env["B600_EXPORT_PATH"] || "/reports/timeclock/export";

// ── Date helpers ──────────────────────────────────────────────────────────────
function previousMondayToSunday(now: Date): { start: Date; end: Date } {
  // Find the Monday of the PREVIOUS week (the week just completed)
  const d = new Date(now);
  const day = d.getDay(); // 0=Sun, 1=Mon, ...
  // How many days back to the most recent Sunday (end of previous week)
  const daysBackToSun = day === 0 ? 7 : day;
  const end = new Date(d);
  end.setDate(d.getDate() - daysBackToSun);
  end.setHours(23, 59, 59, 999);
  const start = new Date(end);
  start.setDate(end.getDate() - 6);
  start.setHours(0, 0, 0, 0);
  return { start, end };
}

function fmtMDY(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  return `${mm}/${dd}/${yy}`;
}

function parseDateMDY(s: string): Date | null {
  const m = String(s).trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!m) return null;
  let yr = parseInt(m[3], 10);
  if (yr < 100) yr += 2000;
  return new Date(yr, parseInt(m[1], 10) - 1, parseInt(m[2], 10));
}

function weekEndingFriday(d: Date): string {
  // Friday = 5 in JS Date.getDay() (Sun=0, Mon=1, ..., Fri=5)
  const day = d.getDay();
  const daysToFri = (5 - day + 7) % 7;
  const fri = new Date(d);
  fri.setDate(d.getDate() + daysToFri);
  const y = fri.getFullYear();
  const m = String(fri.getMonth() + 1).padStart(2, "0");
  const dd = String(fri.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

// ── B600 auth + CSV fetch ────────────────────────────────────────────────────
async function fetchB600CSV(startDate: Date, endDate: Date): Promise<string> {
  if (!B600_USERNAME || !B600_PASSWORD) {
    throw new Error("B600_USERNAME / B600_PASSWORD not configured");
  }

  // Most CyberPay web UIs expect a form-login flow. This implementation uses
  // HTTP basic auth first; if the endpoint requires a session cookie, extend
  // this function to POST to /login and persist the cookie before hitting the
  // export URL.
  const auth = "Basic " + Buffer.from(`${B600_USERNAME}:${B600_PASSWORD}`).toString("base64");
  const url = `${B600_BASE_URL}${B600_EXPORT_PATH}?from=${fmtMDY(startDate)}&to=${fmtMDY(endDate)}&format=csv`;

  const resp = await fetch(url, {
    headers: { Authorization: auth, Accept: "text/csv" },
  });

  if (!resp.ok) {
    throw new Error(`B600 fetch failed: ${resp.status} ${await resp.text()}`);
  }
  return resp.text();
}

// ── CSV parser (CyberPay format) ─────────────────────────────────────────────
function parseCSV(text: string): Record<string, string>[] {
  const lines = text.replace(/\r\n/g, "\n").split("\n").filter(Boolean);
  if (lines.length < 2) return [];
  const parseLine = (line: string): string[] => {
    const out: string[] = [];
    let cur = "", inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') {
        if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; }
        else { inQuotes = !inQuotes; }
      } else if (c === "," && !inQuotes) {
        out.push(cur); cur = "";
      } else {
        cur += c;
      }
    }
    out.push(cur);
    return out;
  };
  const headers = parseLine(lines[0]).map((h) => h.toLowerCase().trim());
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseLine(lines[i]);
    const row: Record<string, string> = {};
    headers.forEach((h, idx) => { row[h] = (vals[idx] ?? "").trim(); });
    rows.push(row);
  }
  return rows;
}

function parseHours(v: string): number {
  if (!v) return 0;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return isFinite(n) ? n : 0;
}

interface WeeklyAgg {
  week_ending: string;
  total_hours: number;
  reg_hours: number;
  ot_hours: number;
  days_worked: number;
  unique_employees: Set<string>;
  employees: Record<string, { hours: number; reg: number; ot: number; days: number }>;
}

function rollupWeekly(rows: Record<string, string>[]): any[] {
  const byWeek: Record<string, WeeklyAgg> = {};
  for (const r of rows) {
    const name = r["display name"] || r["payroll id"];
    if (!name) continue;
    const d = parseDateMDY(r["date"]);
    if (!d) continue;
    const we = weekEndingFriday(d);
    const reg = parseHours(r["reg"]);
    const ot = parseHours(r["ot1"]) + parseHours(r["ot2"]);
    const tot = parseHours(r["total"]) || reg + ot;

    if (!byWeek[we]) {
      byWeek[we] = {
        week_ending: we,
        total_hours: 0, reg_hours: 0, ot_hours: 0, days_worked: 0,
        unique_employees: new Set(),
        employees: {},
      };
    }
    const bw = byWeek[we];
    bw.total_hours += tot;
    bw.reg_hours += reg;
    bw.ot_hours += ot;
    bw.days_worked++;
    bw.unique_employees.add(name);
    if (!bw.employees[name]) bw.employees[name] = { hours: 0, reg: 0, ot: 0, days: 0 };
    bw.employees[name].hours += tot;
    bw.employees[name].reg += reg;
    bw.employees[name].ot += ot;
    bw.employees[name].days++;
  }
  return Object.values(byWeek).map((w) => ({
    week_ending: w.week_ending,
    total_hours: Number(w.total_hours.toFixed(2)),
    reg_hours: Number(w.reg_hours.toFixed(2)),
    ot_hours: Number(w.ot_hours.toFixed(2)),
    days_worked: w.days_worked,
    unique_employees: w.unique_employees.size,
    top_employees: Object.entries(w.employees)
      .sort((a, b) => b[1].hours - a[1].hours)
      .slice(0, 60)
      .map(([name, v]) => ({
        name,
        hours: Number(v.hours.toFixed(2)),
        reg: Number(v.reg.toFixed(2)),
        ot: Number(v.ot.toFixed(2)),
        days: v.days,
      })),
  }));
}

// ── Firestore writer ─────────────────────────────────────────────────────────
function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function fsWrite(collection: string, docId: string, data: any): Promise<boolean> {
  const fields: Record<string, any> = {};
  for (const [k, v] of Object.entries(data)) fields[k] = toFsValue(v);
  const resp = await fetch(
    `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    }
  );
  return resp.ok;
}

// ── Main handler ─────────────────────────────────────────────────────────────
export default async (req: Request, _context: Context) => {
  const startedAt = new Date().toISOString();
  try {
    const { start, end } = previousMondayToSunday(new Date());
    const csv = await fetchB600CSV(start, end);
    const rows = parseCSV(csv);
    const weekly = rollupWeekly(rows);

    let saved = 0;
    for (const w of weekly) {
      const ok = await fsWrite("timeclock_weekly", w.week_ending, {
        ...w,
        source: "b600_scheduled",
        updated_at: new Date().toISOString(),
      });
      if (ok) saved++;
    }

    await fsWrite("marginiq_config", "b600_last_pull", {
      last_run_at: startedAt,
      window_start: start.toISOString(),
      window_end: end.toISOString(),
      rows_fetched: rows.length,
      weeks_saved: saved,
      status: "ok",
    });

    return new Response(
      JSON.stringify({
        ok: true,
        window: { start: start.toISOString(), end: end.toISOString() },
        rows_fetched: rows.length,
        weeks_saved: saved,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err: any) {
    await fsWrite("marginiq_config", "b600_last_pull", {
      last_run_at: startedAt,
      status: "error",
      error: String(err.message || err),
    });
    return new Response(JSON.stringify({ ok: false, error: String(err.message || err) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};

// Schedule: every Monday at 9:00 AM ET (13:00 UTC standard / 14:00 UTC DST).
// Netlify scheduled functions run in UTC. We use 13:00 UTC which = 9AM EST / 8AM EDT.
// Adjust if you prefer a specific local time year-round.
export const config: Config = {
  schedule: "0 13 * * 1",
};
