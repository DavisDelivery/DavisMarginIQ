import type { Context, Config } from "@netlify/functions";

const MOTIVE_BASE = "https://api.gomotive.com";

function guessType(v: any): string {
  const n = ((v.number || "") + " " + (v.make || "") + " " + (v.model || "")).toLowerCase();
  if (/freightliner|kenworth|peterbilt|volvo|mack|international/.test(n)) return "tractor";
  return "box";
}

export default async (req: Request, context: Context) => {
  const MOTIVE_API_KEY = process.env["MOTIVE_API_KEY"];
  if (!MOTIVE_API_KEY) {
    return new Response(JSON.stringify({ error: "MOTIVE_API_KEY not set" }), { status: 500 });
  }

  const url = new URL(req.url);
  const action = url.searchParams.get("action") || "vehicles";
  const mh = { Authorization: `Bearer ${MOTIVE_API_KEY}`, Accept: "application/json" };

  try {
    let data;

    switch (action) {
      case "vehicles": {
        const all: any[] = [];
        let pg = `${MOTIVE_BASE}/v1/vehicles?per_page=100`;
        while (pg) {
          const r = await fetch(pg, { headers: mh });
          if (!r.ok) throw new Error(`Motive vehicles: ${r.status}`);
          const j = await r.json();
          if (j.vehicles) all.push(...j.vehicles.map((v: any) => v.vehicle || v));
          pg = j.pagination?.next_page_url || null;
          if (all.length > 200) break;
        }

        const locR = await fetch(`${MOTIVE_BASE}/v1/vehicle_locations?per_page=100`, { headers: mh });
        const locD = locR.ok ? await locR.json() : { vehicle_locations: [] };
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
        data = { vehicles, count: vehicles.length };
        break;
      }

      case "drivers": {
        const r = await fetch(`${MOTIVE_BASE}/v1/users?per_page=100&role=driver`, { headers: mh });
        if (!r.ok) throw new Error(`Motive drivers: ${r.status}`);
        const j = await r.json();
        const drivers = (j.users || []).map((u: any) => {
          const user = u.user || u;
          return { id: user.id, first_name: user.first_name, last_name: user.last_name, email: user.email, phone: user.phone, status: user.status };
        });
        data = { drivers, count: drivers.length };
        break;
      }

      case "ifta_trips": {
        const start = url.searchParams.get("start") || new Date(Date.now() - 30 * 86400000).toISOString().split("T")[0];
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        const vid = url.searchParams.get("vehicle_id");
        let u = `${MOTIVE_BASE}/v1/ifta/trips?start_date=${start}&end_date=${end}&per_page=100`;
        if (vid) u += `&vehicle_ids=${vid}`;
        const r = await fetch(u, { headers: mh });
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
        const r = await fetch(u, { headers: mh });
        if (!r.ok) throw new Error(`Motive driving_periods: ${r.status}`);
        data = await r.json();
        break;
      }

      default:
        return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400 });
    }

    return new Response(JSON.stringify(data));
  } catch (e: any) {
    console.error("Motive proxy error:", e);
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};
