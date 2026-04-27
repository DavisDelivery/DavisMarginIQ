import type { Context, Config } from "@netlify/functions";

// Motive API proxy.
//
// Motive auth: X-Api-Key header (NOT Authorization: Bearer — that's OAuth).
// Motive pagination: page_no=1,2,... with per_page=100, loop until you get
// fewer than per_page items in a page or pagination.next_page_url is null.

const MOTIVE_BASE = "https://api.gomotive.com";
const MAX_PAGES = 20; // safety cap — 20*100 = 2000 records max

function guessType(v: any): string {
  const n = ((v.number || "") + " " + (v.make || "") + " " + (v.model || "")).toLowerCase();
  if (/freightliner|kenworth|peterbilt|volvo|mack|international/.test(n)) return "tractor";
  return "box";
}

// Fetch a paginated Motive endpoint. Returns ALL records across pages.
// `extractKey` is the field name in the response that holds the array of
// records (e.g. "users", "vehicles", "vehicle_locations").
// `unwrapKey` unwraps each item's inner object — Motive often nests like
// { user: {...} } per item.
async function fetchAllPages(
  baseUrl: string,
  extractKey: string,
  apiKey: string,
  unwrapKey?: string
): Promise<any[]> {
  const all: any[] = [];
  for (let page = 1; page <= MAX_PAGES; page++) {
    const sep = baseUrl.includes("?") ? "&" : "?";
    const url = `${baseUrl}${sep}page_no=${page}&per_page=100`;
    const r = await fetch(url, {
      headers: { "X-Api-Key": apiKey, Accept: "application/json" },
    });
    if (!r.ok) {
      if (page === 1) {
        const body = await r.text().catch(() => "");
        throw new Error(`Motive ${extractKey} HTTP ${r.status}: ${body.slice(0, 200)}`);
      }
      break;
    }
    const j: any = await r.json();
    const rawList = j[extractKey] || [];
    if (rawList.length === 0) break;
    const items = unwrapKey
      ? rawList.map((it: any) => it[unwrapKey] || it)
      : rawList;
    all.push(...items);
    if (rawList.length < 100) break;
    if (j.pagination?.total && all.length >= j.pagination.total) break;
  }
  return all;
}

export default async (req: Request, _context: Context) => {
  const MOTIVE_API_KEY = process.env["MOTIVE_API_KEY"];
  if (!MOTIVE_API_KEY) {
    return new Response(JSON.stringify({ error: "MOTIVE_API_KEY not set" }), { status: 500 });
  }

  const url = new URL(req.url);
  const action = url.searchParams.get("action") || "vehicles";
  const startedAt = Date.now();

  try {
    let data: any;

    switch (action) {
      case "vehicles": {
        const all = await fetchAllPages(`${MOTIVE_BASE}/v1/vehicles`, "vehicles", MOTIVE_API_KEY, "vehicle");

        const locR = await fetch(`${MOTIVE_BASE}/v1/vehicle_locations?per_page=100`, {
          headers: { "X-Api-Key": MOTIVE_API_KEY, Accept: "application/json" },
        });
        const locD: any = locR.ok ? await locR.json() : { vehicle_locations: [] };
        const locMap = new Map();
        (locD.vehicle_locations || []).forEach((vl: any) => {
          const loc = vl.vehicle_location || vl;
          if (loc.vehicle?.id) locMap.set(String(loc.vehicle.id), loc);
        });

        const vehicles = all
          .filter((v: any) => !/forklift|trailer|fork/i.test(v.number || v.name || ""))
          .map((v: any) => {
            const loc = locMap.get(String(v.id));
            return {
              id: v.id, number: v.number || v.name, make: v.make, model: v.model, year: v.year, vin: v.vin,
              current_driver: loc?.current_driver || v.current_driver || null,
              odometer_miles: loc?.odometer ? loc.odometer * 0.621371 : 0,
              lat: loc?.lat, lon: loc?.lon, speed: loc?.speed, fuel_primary: loc?.fuel_primary,
              type: guessType(v),
            };
          });
        data = { vehicles, count: vehicles.length, totalRaw: all.length, ms: Date.now() - startedAt };
        break;
      }

      case "drivers":
      case "users": {
        // /v1/users — returns ALL users in the org. DO NOT pre-filter with
        // `?role=driver` — Motive doesn't reliably support that param.
        // Filter client-side instead.
        const allUsers = await fetchAllPages(`${MOTIVE_BASE}/v1/users`, "users", MOTIVE_API_KEY, "user");

        // ?include_all=1 returns every user; default returns probable drivers
        const includeAll = url.searchParams.get("include_all") === "1";

        const drivers = allUsers
          .filter((u: any) => {
            if (includeAll) return true;
            const role = String(u.role || u.user_role || "").toLowerCase();
            const roles = Array.isArray(u.roles) ? u.roles.map((r: any) => String(r).toLowerCase()) : [];
            const allRoles = [role, ...roles].filter(Boolean);
            if (allRoles.length === 0) return true; // assume driver if no role specified
            const nonDriverRoles = ["admin", "fleet_admin", "dispatcher", "mechanic", "manager", "owner_only"];
            if (allRoles.every((r: string) => nonDriverRoles.includes(r))) return false;
            return true;
          })
          .map((u: any) => ({
            id: u.id,
            first_name: u.first_name,
            last_name: u.last_name,
            email: u.email,
            phone: u.phone,
            status: u.status,
            role: u.role || u.user_role || null,
          }));

        data = {
          drivers,
          count: drivers.length,
          totalUsers: allUsers.length,
          ms: Date.now() - startedAt,
        };
        break;
      }

      case "ifta_trips": {
        const start = url.searchParams.get("start") || new Date(Date.now() - 30 * 86400000).toISOString().split("T")[0];
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        const vid = url.searchParams.get("vehicle_id");
        let u = `${MOTIVE_BASE}/v1/ifta/trips?start_date=${start}&end_date=${end}&per_page=100`;
        if (vid) u += `&vehicle_ids=${vid}`;
        const r = await fetch(u, { headers: { "X-Api-Key": MOTIVE_API_KEY, Accept: "application/json" } });
        if (!r.ok) throw new Error(`Motive IFTA: ${r.status}`);
        data = await r.json();
        break;
      }

      case "driving_periods": {
        const start = url.searchParams.get("start") || new Date(Date.now() - 7 * 86400000).toISOString().split("T")[0];
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        const did = url.searchParams.get("driver_id");
        let u = `${MOTIVE_BASE}/v1/driving_periods?start_date=${start}&end_date=${end}&per_page=100`;
        if (did) u += `&driver_ids=${did}`;
        const r = await fetch(u, { headers: { "X-Api-Key": MOTIVE_API_KEY, Accept: "application/json" } });
        if (!r.ok) throw new Error(`Motive driving_periods: ${r.status}`);
        data = await r.json();
        break;
      }

      default:
        return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400 });
    }

    return new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json" } });
  } catch (e: any) {
    console.error("[motive] error:", e?.message, e?.stack);
    return new Response(JSON.stringify({ error: String(e?.message || e), action }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};
