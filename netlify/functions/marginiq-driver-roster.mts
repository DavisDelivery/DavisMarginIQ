// netlify/functions/marginiq-driver-roster.mts
// Public read-only endpoint that exposes the canonical employees roster from
// the davismarginiq Firebase hub in the format SENTINEL (and other Davis apps)
// can consume directly.
//
// Output shape mirrors SENTINEL's DRIVER_ROSTER:
//   {
//     "<name lowercase>": { role: "shuttle"|"loadshift"|"tractor"|"straight"|"unknown",
//                          type: "tractor"|"straight"|"unknown",
//                          co:   "davis"|"owner"|"unknown" }
//   }
//
// Mapping rules from canonical employees.role → SENTINEL profile:
//   driver + payType=hourly      → straight (box truck)
//   driver + (truck assigned tractor) → tractor (resolved from defaultVehicleId)
//   shuttle_driver               → shuttle
//   yard_jockey                  → loadshift
//   owner_op                     → straight + co:owner (or tractor depending on vehicle)
//   warehouse / office / mechanic / management → not exposed (not drivers)
//
// All known aliases (B600 typos, NuVizz alternate names) are emitted as
// additional keys pointing at the same profile. Aliases come from each
// employee's `aliases` array (set in Data Hub UI) plus auto-generated common
// variants (first-name only, first-initial+last).

import type { Context } from "@netlify/functions";

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = "AIzaSyDyRyjuiP_UD8T_2xmW2xLjvqx9RLCYCmo";
const FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

// Convert a Firestore REST document to a plain JS object
function fromFirestore(doc: any): any {
  if (!doc?.fields) return {};
  return convertFields(doc.fields);
}

function convertFields(fields: any): any {
  const out: any = {};
  for (const [k, v] of Object.entries<any>(fields)) {
    out[k] = convertValue(v);
  }
  return out;
}

function convertValue(v: any): any {
  if (v.stringValue !== undefined) return v.stringValue;
  if (v.integerValue !== undefined) return Number(v.integerValue);
  if (v.doubleValue !== undefined) return v.doubleValue;
  if (v.booleanValue !== undefined) return v.booleanValue;
  if (v.nullValue !== undefined) return null;
  if (v.timestampValue !== undefined) return v.timestampValue;
  if (v.mapValue) return convertFields(v.mapValue.fields || {});
  if (v.arrayValue) return (v.arrayValue.values || []).map(convertValue);
  return null;
}

async function loadEmployees(): Promise<any[]> {
  const url = `${FIRESTORE_BASE}/employees?key=${FIREBASE_API_KEY}&pageSize=300`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Firestore fetch failed: ${resp.status} ${await resp.text()}`);
  const data = await resp.json();
  const docs = data.documents || [];
  return docs.map((d: any) => {
    const id = d.name.split("/").pop();
    return { id, ...fromFirestore(d) };
  });
}

async function loadVehicles(): Promise<any[]> {
  const url = `${FIRESTORE_BASE}/vehicles?key=${FIREBASE_API_KEY}&pageSize=300`;
  const resp = await fetch(url);
  if (!resp.ok) return [];
  const data = await resp.json();
  const docs = data.documents || [];
  return docs.map((d: any) => {
    const id = d.name.split("/").pop();
    return { id, ...fromFirestore(d) };
  });
}

// Map canonical role → SENTINEL profile fields
function toSentinelProfile(emp: any, vehiclesById: Record<string, any>): {
  role: string;
  type: string;
  co: string;
} | null {
  const role = emp.role;
  if (!role || !["driver", "owner_op", "shuttle_driver", "yard_jockey"].includes(role)) {
    return null;  // Not a driver — don't include in roster
  }

  const co = role === "owner_op" ? "owner" : "davis";

  if (role === "shuttle_driver") return { role: "shuttle", type: "tractor", co };
  if (role === "yard_jockey") return { role: "loadshift", type: "tractor", co };

  // For driver and owner_op: classify as tractor or straight based on default vehicle
  let type: "tractor" | "straight" = "straight";
  if (emp.defaultVehicleId && vehiclesById[emp.defaultVehicleId]) {
    const v = vehiclesById[emp.defaultVehicleId];
    if (v.unitType === "tractor") type = "tractor";
  }
  return { role: type, type, co };
}

// Generate alias keys for one employee (lowercase, like SENTINEL expects)
function generateAliases(emp: any): string[] {
  const out: string[] = [];
  const full = (emp.fullName || "").toLowerCase().trim();
  if (full) out.push(full);

  const first = (emp.firstName || "").toLowerCase().trim();
  const last = (emp.lastName || "").toLowerCase().trim();

  // First-name-only (B600 sometimes truncates)
  if (first && first.length >= 3) out.push(first);
  // First initial + last (e.g. "c head" for "chris head")
  if (first && last) out.push(`${first[0]} ${last}`);

  // Custom aliases stored in the employee doc
  const customAliases = Array.isArray(emp.aliases) ? emp.aliases : [];
  for (const a of customAliases) {
    const lower = String(a).toLowerCase().trim();
    if (lower) out.push(lower);
  }

  // NuVizz external ID — already a name string in some form
  const nv = (emp.externalIds?.nuvizz || "").toString().toLowerCase().trim();
  if (nv && nv !== full) out.push(nv);

  // Deduplicate
  return [...new Set(out.filter(Boolean))];
}

export default async (req: Request, _ctx: Context) => {
  const cors: HeadersInit = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "public, max-age=300",  // 5-min CDN cache
    "Content-Type": "application/json",
  };
  if (req.method === "OPTIONS") return new Response("", { status: 200, headers: cors });

  try {
    const [employees, vehicles] = await Promise.all([loadEmployees(), loadVehicles()]);
    const vehiclesById: Record<string, any> = {};
    for (const v of vehicles) vehiclesById[v.id] = v;

    const roster: Record<string, { role: string; type: string; co: string }> = {};
    let driverCount = 0;
    let aliasCount = 0;

    for (const emp of employees) {
      if (emp.status && emp.status !== "active") continue;
      const profile = toSentinelProfile(emp, vehiclesById);
      if (!profile) continue;
      driverCount++;
      const aliases = generateAliases(emp);
      for (const a of aliases) {
        if (!roster[a]) {
          roster[a] = profile;
          aliasCount++;
        }
      }
    }

    return new Response(JSON.stringify({
      ok: true,
      generatedAt: new Date().toISOString(),
      sourceProject: PROJECT_ID,
      driverCount,
      aliasCount,
      roster,
    }), { status: 200, headers: cors });

  } catch (e: any) {
    return new Response(JSON.stringify({
      ok: false,
      error: String(e?.message || e),
    }), { status: 500, headers: cors });
  }
};

export const config = {
  path: "/api/driver-roster",
};
