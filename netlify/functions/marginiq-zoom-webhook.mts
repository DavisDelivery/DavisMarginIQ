import type { Context } from "@netlify/functions";
import * as crypto from "crypto";

// Zoom Phone Webhook Receiver for MarginIQ
//
// Zoom POSTs signed events here. We verify the signature, normalize the
// payload, then write to Firestore via the Firebase REST API so the
// MarginIQ Phone tab updates in real-time via onSnapshot.

const CORS = { "Content-Type": "application/json" };

function verifyZoomSignature(body: string, timestamp: string, signature: string, secret: string): boolean {
  const msg = `v0:${timestamp}:${body}`;
  const expected = "v0=" + crypto.createHmac("sha256", secret).update(msg).digest("hex");
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
  } catch { return false; }
}

function normalizeEvent(event: string, obj: any): Record<string, any> | null {
  const callId = obj.call_id || obj.id || "";
  const callee = obj.callee || {};
  const caller = obj.caller || {};

  const base = {
    call_id:      callId,
    event,
    ts:           obj.date_time || new Date().toISOString(),
    updated_at:   new Date().toISOString(),
    employee:     callee.name || callee.display_name || "",
    employee_ext: callee.extension_number || "",
    employee_id:  callee.user_id || "",
    caller_num:   caller.phone_number || caller.did_number || "",
    caller_name:  caller.name || caller.display_name || "",
    callee_num:   callee.phone_number || callee.did_number || "",
    direction:    obj.direction || "inbound",
  };

  switch (event) {
    case "phone.callee_ringing":
      return { ...base, status: "ringing" };

    case "phone.callee_answered":
      return { ...base, status: "active", answered_at: obj.date_time || new Date().toISOString() };

    case "phone.callee_ended":
    case "phone.caller_ended": {
      const duration = obj.duration ?? obj.talk_time ?? 0;
      const result   = (obj.hangup_result || obj.result || "").toLowerCase();
      const status   = result.includes("voicemail") ? "voicemail"
                     : result.includes("miss") || result.includes("no answer") ? "missed"
                     : "ended";
      return { ...base, status, duration, hangup_result: obj.hangup_result || "" };
    }

    case "phone.callee_missed":
      return { ...base, status: "missed" };

    case "phone.voicemail_received":
      return { ...base, status: "voicemail", voicemail_id: obj.voicemail_id || "" };

    default:
      return null;
  }
}

// Signature: (projectId, apiKey, collection, docId, data) — IN THIS ORDER
async function writeToFirestore(
  projectId: string,
  apiKey: string,
  collection: string,
  docId: string,
  data: Record<string, any>
): Promise<void> {
  function toFirestoreValue(v: any): any {
    if (v === null || v === undefined) return { nullValue: null };
    if (typeof v === "boolean")        return { booleanValue: v };
    if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
    if (typeof v === "string")         return { stringValue: v };
    if (Array.isArray(v))              return { arrayValue: { values: v.map(toFirestoreValue) } };
    if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k, val]) => [k, toFirestoreValue(val)])) } };
    return { stringValue: String(v) };
  }

  const fields = Object.fromEntries(
    Object.entries(data).map(([k, v]) => [k, toFirestoreValue(v)])
  );

  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}/${docId}?key=${apiKey}`;

  const res = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });

  if (!res.ok) {
    const err = await res.text().catch(() => "");
    throw new Error(`Firestore write failed ${res.status}: ${err.slice(0, 200)}`);
  }
}

export default async (req: Request, _ctx: Context) => {
  const SECRET      = process.env["ZOOM_WEBHOOK_SECRET_TOKEN"];
  const API_KEY     = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID  = process.env["FIREBASE_PROJECT_ID"];

  if (req.method === "GET") {
    return new Response(JSON.stringify({
      status: "Zoom webhook endpoint active",
      hasSecret: !!SECRET,
      hasFirebaseKey: !!API_KEY,
      hasFirebaseProject: !!PROJECT_ID,
      projectId: PROJECT_ID || null,
    }), { headers: CORS });
  }

  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const body = await req.text();
  let payload: any;
  try { payload = JSON.parse(body); } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: CORS });
  }

  // ── Zoom URL validation challenge ─────────────────────────────────────────
  if (payload.event === "endpoint.url_validation") {
    if (!SECRET) return new Response(JSON.stringify({ error: "ZOOM_WEBHOOK_SECRET_TOKEN not set" }), { status: 500, headers: CORS });
    const hash = crypto.createHmac("sha256", SECRET).update(payload.payload?.plainToken || "").digest("hex");
    return new Response(JSON.stringify({ plainToken: payload.payload?.plainToken, encryptedToken: hash }), { headers: CORS });
  }

  // ── Verify signature ──────────────────────────────────────────────────────
  if (SECRET) {
    const ts  = req.headers.get("x-zm-request-timestamp") || "";
    const sig = req.headers.get("x-zm-signature") || "";
    if (ts && sig && !verifyZoomSignature(body, ts, sig, SECRET)) {
      return new Response(JSON.stringify({ error: "Invalid signature" }), { status: 401, headers: CORS });
    }
  }

  const event = payload.event || "";
  const obj   = payload.payload?.object || {};

  // ── Normalize ─────────────────────────────────────────────────────────────
  const normalized = normalizeEvent(event, obj);
  if (!normalized || !normalized.call_id) {
    return new Response(JSON.stringify({ ok: true, skipped: event }), { headers: CORS });
  }

  // ── Write to Firestore ────────────────────────────────────────────────────
  if (!API_KEY || !PROJECT_ID) {
    console.warn("[zoom-webhook] Missing FIREBASE_API_KEY or FIREBASE_PROJECT_ID — not persisting event");
    return new Response(JSON.stringify({ ok: true, warn: "Firebase not configured" }), { headers: CORS });
  }

  try {
    // FIXED: correct argument order — (projectId, apiKey, collection, docId, data)
    await writeToFirestore(PROJECT_ID, API_KEY, "zoom_calls", normalized.call_id, normalized);
    console.log(`[zoom-webhook] wrote ${event} → zoom_calls/${normalized.call_id}`);
  } catch (e: any) {
    console.error("[zoom-webhook] Firestore error:", e?.message);
    return new Response(JSON.stringify({ ok: true, warn: "Firestore write failed", detail: e?.message }), { headers: CORS });
  }

  return new Response(JSON.stringify({ ok: true, event, call_id: normalized.call_id }), { headers: CORS });
};
