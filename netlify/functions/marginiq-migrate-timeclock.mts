// One-time migration: rewrite legacy timeclock_daily docs to the unified
// v2.47.4 schema. Old format: ${date}_${safeDocId(display_id)}, no driver_key
// field. New format: ${driver_key}_${date}, with driver_key field present.
//
// Idempotent: skips docs that already have driver_key. Safe to re-run.
//
// Invoke once after deploy:
//   curl -X POST 'https://davis-marginiq.netlify.app/.netlify/functions/marginiq-migrate-timeclock?confirm=yes'

import type { Context } from "@netlify/functions";

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

function normalizeName(v: string | null | undefined): string | null {
  if (!v) return null;
  let s = String(v).trim();
  if (!s) return null;
  if (s.includes(",")) {
    const [last, first] = s.split(",").map(x => x.trim());
    if (first && last) s = `${first} ${last}`;
  }
  return s.replace(/\s+/g, " ").split(" ")
    .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(" ");
}
function driverKey(name: string | null | undefined): string | null {
  if (!name) return null;
  return String(name).trim().toLowerCase()
    .replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140) || null;
}

function fsValueToJs(v: any): any {
  if (v == null) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue.values || []).map(fsValueToJs);
  if ("mapValue" in v) {
    const out: any = {};
    for (const [k, val] of Object.entries(v.mapValue.fields || {})) out[k] = fsValueToJs(val);
    return out;
  }
  return null;
}
function jsToFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(jsToFsValue) } };
  if (typeof v === "object") {
    const fields: any = {};
    for (const [k, val] of Object.entries(v)) fields[k] = jsToFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function listAllTimeclockDaily(): Promise<any[]> {
  const all: any[] = [];
  let pageToken = "";
  for (let i = 0; i < 100; i++) { // safety cap = 100 pages
    const url = `${BASE}/timeclock_daily?key=${FIREBASE_API_KEY}&pageSize=300${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ""}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`list failed: ${resp.status} ${await resp.text()}`);
    const j: any = await resp.json();
    for (const d of j.documents || []) all.push(d);
    if (!j.nextPageToken) break;
    pageToken = j.nextPageToken;
  }
  return all;
}

async function writeDoc(docId: string, fields: any): Promise<boolean> {
  const url = `${BASE}/timeclock_daily/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const fsFields: any = {};
  for (const [k, v] of Object.entries(fields)) fsFields[k] = jsToFsValue(v);
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: fsFields }),
  });
  return resp.ok;
}

async function deleteDoc(docId: string): Promise<boolean> {
  const url = `${BASE}/timeclock_daily/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok;
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);
  if (url.searchParams.get("confirm") !== "yes") {
    return new Response(JSON.stringify({
      error: "Add ?confirm=yes to actually run the migration. Read-only preview not yet implemented.",
    }), { status: 400, headers: { "Content-Type": "application/json" } });
  }

  const result = {
    total_seen: 0,
    already_unified: 0,    // already had driver_key — skipped
    migrated: 0,           // legacy → new format
    deleted_old: 0,
    failed: 0,
    errors: [] as string[],
  };

  try {
    const docs = await listAllTimeclockDaily();
    result.total_seen = docs.length;

    for (const doc of docs) {
      const oldDocId = doc.name.split("/").pop();
      const fields: any = {};
      for (const [k, v] of Object.entries(doc.fields || {})) fields[k] = fsValueToJs(v);

      // Already unified?
      if (fields.driver_key) {
        result.already_unified++;
        continue;
      }

      const date = fields.date;
      const displayName = fields.display_name || fields.employee;
      if (!date || !displayName) {
        result.failed++;
        result.errors.push(`${oldDocId}: missing date or display_name`);
        continue;
      }
      const dKey = driverKey(normalizeName(displayName));
      if (!dKey) {
        result.failed++;
        result.errors.push(`${oldDocId}: could not derive driver_key from "${displayName}"`);
        continue;
      }
      const newDocId = `${dKey}_${date}`;

      // Already at the new id?
      if (oldDocId === newDocId) {
        // Just patch in driver_key + employee
        const ok = await writeDoc(newDocId, {
          ...fields,
          driver_key: dKey,
          employee: displayName,
          hours: fields.hours ?? fields.total_hours ?? 0,
          month: date.slice(0, 7),
        });
        if (ok) result.already_unified++; else result.failed++;
        continue;
      }

      // Write to new id (full union schema)
      const newFields = {
        ...fields,
        driver_key: dKey,
        employee: displayName,
        hours: fields.hours ?? fields.total_hours ?? 0,
        total_hours: fields.total_hours ?? fields.hours ?? 0,
        month: date.slice(0, 7),
      };
      const ok = await writeDoc(newDocId, newFields);
      if (!ok) {
        result.failed++;
        result.errors.push(`${oldDocId}: write to ${newDocId} failed`);
        continue;
      }

      // Delete old only after new write confirmed
      const delOk = await deleteDoc(oldDocId);
      if (delOk) result.deleted_old++;
      result.migrated++;
    }
  } catch (e: any) {
    result.errors.push(`fatal: ${e.message}`);
  }

  return new Response(JSON.stringify(result, null, 2), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};
