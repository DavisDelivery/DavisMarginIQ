import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — B600 Time Clock weekly auto-pull.
 *
 * Pulls previous Mon–Sun from the B600 (Icon Time TotalPass B600 hardware clock)
 * at b600.atlantafreightquotes.com, parses the CSV Extended export (same format
 * as the manual backfill CSVs), and writes weekly rollups to Firestore.
 *
 * ─── Flow (fully characterized via browser introspection 2026-04-20) ───
 *
 * 1. GET  /login.html               → seed session cookie
 * 2. POST /login.html               body: username=X&password=Y&buttonClicked=Submit
 *                                   → authenticated session, 302 → /index.html
 * 3. GET  /report.html?rt=2&from=MM/DD/YY&to=MM/DD/YY&eid=ss&export=1
 *                                   → returns CSV Extended body (headered, 25 cols)
 *
 *   rt=2       → Timecards report
 *   eid=ss     → all employees
 *   export=1   → CSV Extended format (full headers, matches backfill files exactly)
 *
 * ─── Required Netlify env vars ───
 *   FIREBASE_API_KEY    — for Firestore REST writes
 *   B600_BASE_URL       — https://b600.atlantafreightquotes.com (no trailing slash)
 *   B600_USERNAME       — TotalPass login username
 *   B600_PASSWORD       — TotalPass login password
 *
 * ─── Manual test (before enabling schedule) ───
 *   curl https://davis-marginiq.netlify.app/.netlify/functions/marginiq-b600-timeclock
 * Expected: {"ok":true,"rows_fetched":~250,"weeks_saved":1,"week_ending":"2026-MM-DD"}
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const B600_BASE_URL = (process.env["B600_BASE_URL"] || "https://b600.atlantafreightquotes.com").replace(/\/$/, "");
const B600_USERNAME = process.env["B600_USERNAME"];
const B600_PASSWORD = process.env["B600_PASSWORD"];

// ─── Cookie jar ──────────────────────────────────────────────────────────────
class CookieJar {
  private cookies: Map<string, string> = new Map();
  absorb(resp: Response) {
    const setCookies: string[] = (resp.headers as any).getSetCookie
      ? (resp.headers as any).getSetCookie()
      : (resp.headers.get("set-cookie") ? [resp.headers.get("set-cookie")!] : []);
    for (const sc of setCookies) {
      const [pair] = sc.split(";");
      const eq = pair.indexOf("=");
      if (eq < 0) continue;
      const name = pair.slice(0, eq).trim();
      const value = pair.slice(eq + 1).trim();
      if (name) this.cookies.set(name, value);
    }
  }
  header(): string {
    return [...this.cookies.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
  }
}

// ─── B600 session ────────────────────────────────────────────────────────────
async function b600Login(): Promise<CookieJar> {
  if (!B600_USERNAME || !B600_PASSWORD) {
    throw new Error("B600_USERNAME / B600_PASSWORD not configured");
  }
  const jar = new CookieJar();

  const seed = await fetch(`${B600_BASE_URL}/login.html`, { redirect: "manual" });
  jar.absorb(seed);

  const body = new URLSearchParams({
    username: B600_USERNAME,
    password: B600_PASSWORD,
    buttonClicked: "Submit",
  }).toString();

  const loginResp = await fetch(`${B600_BASE_URL}/login.html`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Cookie: jar.header(),
    },
    body,
    redirect: "manual",
  });
  jar.absorb(loginResp);
  if (loginResp.status >= 400) {
    throw new Error(`B600 login POST failed: ${loginResp.status}`);
  }

  // Verify session is real (not redirected back to login)
  const verify = await fetch(`${B600_BASE_URL}/index.html`, {
    headers: { Cookie: jar.header() },
    redirect: "manual",
  });
  jar.absorb(verify);
  const loc = verify.headers.get("location") || "";
  if (verify.status === 302 && loc.toLowerCase().includes("login")) {
    throw new Error("B600 login did not persist — redirect back to /login.html");
  }
  return jar;
}

// ─── Date helpers ────────────────────────────────────────────────────────────
function formatMDYY(d: Date): string {
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const yy = String(d.getFullYear() % 100).padStart(2, "0");
  return `${m}/${dd}/${yy}`;
}

function previousWeekMondayToSunday(now: Date = new Date()): { from: Date; to: Date } {
  // Today's day of week (0=Sun, 1=Mon, ..., 6=Sat)
  const dow = now.getDay();
  // Days back to the most recent Sunday (end of previous week if today is Mon)
  // Mon → go back 1 day to Sunday; Tue → 2; ...; Sun → 7 (prior Sunday, not today)
  const daysBackToSun = dow === 0 ? 7 : dow;
  const to = new Date(now);
  to.setDate(now.getDate() - daysBackToSun);
  to.setHours(23, 59, 59, 999);
  const from = new Date(to);
  from.setDate(to.getDate() - 6);
  from.setHours(0, 0, 0, 0);
  return { from, to };
}

function parseDateMDY(s: string): Date | null {
  const m = String(s).trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!m) return null;
  let yr = parseInt(m[3], 10);
  if (yr < 100) yr += 2000;
  return new Date(yr, parseInt(m[1], 10) - 1, parseInt(m[2], 10));
}

function weekEndingFriday(d: Date): string {
  const day = d.getDay();
  const daysToFri = (5 - day + 7) % 7;
  const fri = new Date(d);
  fri.setDate(d.getDate() + daysToFri);
  const y = fri.getFullYear();
  const mo = String(fri.getMonth() + 1).padStart(2, "0");
  const dd = String(fri.getDate()).padStart(2, "0");
  return `${y}-${mo}-${dd}`;
}

// ─── Fetch CSV Extended ──────────────────────────────────────────────────────
async function b600FetchCSV(jar: CookieJar, from: Date, to: Date): Promise<string> {
  const fromStr = formatMDYY(from);
  const toStr = formatMDYY(to);
  // Note: server expects MM/DD/YY as-is; no URL-encoding beyond what URLSearchParams provides
  const url = `${B600_BASE_URL}/report.html?rt=2&from=${encodeURIComponent(fromStr)}&to=${encodeURIComponent(toStr)}&eid=ss&export=1`;

  const resp = await fetch(url, {
    headers: { Cookie: jar.header(), Accept: "text/csv, */*" },
  });

  if (!resp.ok) {
    throw new Error(`B600 CSV fetch failed: ${resp.status} ${(await resp.text()).slice(0, 200)}`);
  }

  const text = await resp.text();
  // Guard: verify we got the CSV Extended format, not a login page or error
  if (!text.toLowerCase().startsWith("display name")) {
    // Could be an HTML error page or session-expired redirect
    throw new Error(`Unexpected response body (first 200 chars): ${text.slice(0, 200)}`);
  }
  return text;
}

// ─── CSV parser (CSV Extended format: 25 cols, headered, quoted values) ─────
function parseCSV(text: string): Record<string, string>[] {
  const lines = text.replace(/\r\n/g, "\n").split("\n").filter(l => l.trim().length > 0);
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
  const headers = parseLine(lines[0]).map(h => h.toLowerCase().trim());
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

// ─── Weekly rollup ───────────────────────────────────────────────────────────
interface WeeklyAgg {
  week_ending: string;
  total_hours: number; reg_hours: number; ot_hours: number; days_worked: number;
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
  return Object.values(byWeek).map(w => ({
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

// ─── Firestore writer ────────────────────────────────────────────────────────
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

// ─── Main handler ────────────────────────────────────────────────────────────
export default async (_req: Request, _context: Context) => {
  const startedAt = new Date().toISOString();
  try {
    const { from, to } = previousWeekMondayToSunday();
    const jar = await b600Login();
    const csv = await b600FetchCSV(jar, from, to);
    const rows = parseCSV(csv);
    const weekly = rollupWeekly(rows);

    let saved = 0;
    const weekIds: string[] = [];
    for (const w of weekly) {
      const ok = await fsWrite("timeclock_weekly", w.week_ending, {
        ...w,
        source: "b600_scheduled",
        updated_at: new Date().toISOString(),
      });
      if (ok) { saved++; weekIds.push(w.week_ending); }
    }

    await fsWrite("marginiq_config", "b600_last_pull", {
      last_run_at: startedAt,
      window_from: formatMDYY(from),
      window_to: formatMDYY(to),
      rows_fetched: rows.length,
      weeks_saved: saved,
      week_endings: weekIds,
      status: "ok",
    });

    return new Response(
      JSON.stringify({
        ok: true,
        window: { from: formatMDYY(from), to: formatMDYY(to) },
        rows_fetched: rows.length,
        weeks_saved: saved,
        week_endings: weekIds,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err: any) {
    const msg = String(err.message || err);
    await fsWrite("marginiq_config", "b600_last_pull", {
      last_run_at: startedAt,
      status: "error",
      error: msg,
    });
    return new Response(JSON.stringify({ ok: false, error: msg }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};

// Schedule: Mondays at 13:00 UTC (9AM EST / 8AM EDT).
// Commented out until a successful manual test. To enable, uncomment the line:
export const config: Config = {
  // schedule: "0 13 * * 1",
};
