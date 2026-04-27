// Fleet Management proxy — exposes the davisfleetmanagement KV store
// (drivers, weekly truck assignments) to the MarginIQ Data Hub for use
// in the bootstrap roster + employee mapping screen.
//
// Endpoints:
//   GET /api/fleet-management?action=drivers
//     → { drivers: [{ name, role, category }, ...] }   // 62 drivers, definitive roles
//
//   GET /api/fleet-management?action=trucks
//     → { trucks: ["0424","0608",...], assignments: { "Allen Council": "0424", ... } }
//
//   GET /api/fleet-management?action=all
//     → both drivers and trucks in one round trip (used by Data Hub bootstrap)

import type { Context, Config } from "@netlify/functions";

const PROJECT = "davisfleetmanagement";
const API_KEY = "AIzaSyCaaHZ0GuBoxl696-PzlgBQLPEad1xyiqw";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT}/databases/(default)/documents`;

async function getKv(key: string): Promise<any | null> {
  const url = `${FS_BASE}/kv/${encodeURIComponent(key)}?key=${API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  const j: any = await r.json();
  const v = j?.fields?.v?.stringValue;
  if (!v) return null;
  try {
    return JSON.parse(v);
  } catch {
    return null;
  }
}

async function listAssignmentKeys(limit = 8): Promise<string[]> {
  // Pull the most recent N weekly assignment docs.
  // Firestore REST API supports collection listing only, not prefix filter,
  // so we fetch the full kv collection and filter by id prefix.
  const url = `${FS_BASE}/kv?key=${API_KEY}&pageSize=300`;
  const r = await fetch(url);
  if (!r.ok) return [];
  const j: any = await r.json();
  const docs = j?.documents || [];
  const keys: string[] = docs
    .map((d: any) => d.name.split("/").pop())
    .filter((k: string) => k.startsWith("fl-asgn-"))
    .sort()
    .reverse(); // most recent first
  return keys.slice(0, limit);
}

async function getRecentTrucksAndAssignments(): Promise<{ trucks: string[]; assignments: Record<string, string> }> {
  const keys = await listAssignmentKeys(8);
  const truckSet = new Set<string>();
  const assignments: Record<string, string> = {}; // driverName -> most recent truck
  // Process oldest-first so most recent overwrites older
  for (const key of [...keys].reverse()) {
    const data = await getKv(key);
    if (!data || typeof data !== "object") continue;
    for (const [driverDay, truck] of Object.entries(data)) {
      if (!truck || typeof truck !== "string") continue;
      const t = String(truck).trim();
      if (!t) continue;
      truckSet.add(t);
      // driverDay is "Allen Council-Mon" — pull driver name out
      const m = driverDay.match(/^(.+)-(Mon|Tue|Wed|Thu|Fri|Sat|Sun)$/);
      if (m) assignments[m[1]] = t;
    }
  }
  const trucks = [...truckSet].sort();
  return { trucks, assignments };
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action") || "all";

  try {
    if (action === "drivers") {
      const drivers = (await getKv("fl-drivers")) || [];
      return new Response(JSON.stringify({ drivers, count: drivers.length }), {
        status: 200,
        headers: { "Content-Type": "application/json", "Cache-Control": "private, max-age=300" },
      });
    }

    if (action === "trucks") {
      const { trucks, assignments } = await getRecentTrucksAndAssignments();
      return new Response(JSON.stringify({ trucks, assignments, truckCount: trucks.length }), {
        status: 200,
        headers: { "Content-Type": "application/json", "Cache-Control": "private, max-age=300" },
      });
    }

    if (action === "all") {
      const [drivers, trucksData] = await Promise.all([
        getKv("fl-drivers").then(d => d || []),
        getRecentTrucksAndAssignments(),
      ]);
      return new Response(
        JSON.stringify({
          drivers,
          driverCount: drivers.length,
          trucks: trucksData.trucks,
          truckCount: trucksData.trucks.length,
          assignments: trucksData.assignments,
        }),
        { status: 200, headers: { "Content-Type": "application/json", "Cache-Control": "private, max-age=300" } }
      );
    }

    return new Response(JSON.stringify({ error: `Unknown action: ${action}` }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: String(e?.message || e) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};

export const config: Config = {
  path: "/api/fleet-management",
};

