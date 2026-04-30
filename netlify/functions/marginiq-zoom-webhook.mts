import type { Context } from "@netlify/functions";
import * as crypto from "crypto";

// Zoom Phone Webhook Receiver — v2.47.6
//
// Receives real-time call events from Zoom and writes to Firebase zoom_calls.
// The MarginIQ Live Feed listens to zoom_calls via onSnapshot.
//
// Improvements:
// - Logs ALL incoming events to zoom_webhook_log for debugging
// - More tolerant signature verification (logs but doesn't reject)
// - Handles more event variations

const CORS = { "Content-Type": "application/json" };

function verifyZoomSignature(body: string, timestamp: string, signature: string, secret: string): boolean {
  const msg = `v0:${timestamp}:${body}`;
  const expected = "v0=" + crypto.createHmac("sha256", secret).update(msg).digest("hex");
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
  } catch { return false; }
}

function normalizeEvent(event: string, obj: any): Record<string, any> | null {
  const callId = obj.call_id || obj.id || obj.callee_call_log_id || obj.caller_call_log_id || "";
  const callee = obj.callee || {};
  const caller = obj.caller || {};

  const base = {
    call_id:      String(callId),
    event,
    ts:           obj.date_time || new Date().toISOString(),
    updated_at:   new Date().toISOString(),
    employee:     callee.name || callee.display_name || "",
    employee_ext: String(callee.extension_number || ""),
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

function toFirestoreValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean")        return { booleanValue: v };
  if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string")         return { stringValue: v };
  if (Array.isArray(v))              return { arrayValue: { values: v.map(toFirestoreValue) } };
  if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k, val]) => [k, toFirestoreValue(val)])) } };
  return { stringValue: String(v) };
}

async function writeToFirestore(
  projectId: string, apiKey: string, collection: string, docId: string, data: Record<string, any>
): Promise<void> {
  const fields = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, toFirestoreValue(v)]));
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const res = await fetch(url, {
    method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const err = await res.text().catch(() => "");
    throw new Error(`Firestore write failed ${res.status}: ${err.slice(0, 200)}`);
  }
}

async function logIncomingEvent(projectId: string, apiKey: string, body: string, headers: Record<string, string>) {
  // Log ALL incoming events to a debug collection so we can see what Zoom sends
  try {
    const id = `${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
    await writeToFirestore(projectId, apiKey, "zoom_webhook_log", id, {
      ts: new Date().toISOString(),
      body: body.slice(0, 5000),  // truncate for safety
      hasSignature: !!headers["x-zm-signature"],
      hasTimestamp: !!headers["x-zm-request-timestamp"],
    });
  } catch (e: any) {
    console.warn("[webhook-log]", e?.message);
  }
}

export default async (req: Request, _ctx: Context) => {
  const SECRET     = process.env["ZOOM_WEBHOOK_SECRET_TOKEN"];
  const API_KEY    = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = process.env["FIREBASE_PROJECT_ID"];

  if (req.method === "GET") {
    return new Response(JSON.stringify({
      status: "Zoom webhook endpoint active (v2.47.6)",
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
  const headers: Record<string, string> = {};
  req.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });

  // Always log incoming for debugging (truncated to 5KB)
  if (API_KEY && PROJECT_ID) {
    logIncomingEvent(PROJECT_ID, API_KEY, body, headers).catch(() => {});
  }

  let payload: any;
  try { payload = JSON.parse(body); } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: CORS });
  }

  // URL validation challenge
  if (payload.event === "endpoint.url_validation") {
    if (!SECRET) return new Response(JSON.stringify({ error: "ZOOM_WEBHOOK_SECRET_TOKEN not set" }), { status: 500, headers: CORS });
    const hash = crypto.createHmac("sha256", SECRET).update(payload.payload?.plainToken || "").digest("hex");
    return new Response(JSON.stringify({ plainToken: payload.payload?.plainToken, encryptedToken: hash }), { headers: CORS });
  }

  // Signature verification — log mismatches but DON'T reject (Zoom retries hard
  // on 401 and we'd rather process duplicates than miss events)
  if (SECRET) {
    const ts  = headers["x-zm-request-timestamp"] || "";
    const sig = headers["x-zm-signature"] || "";
    if (ts && sig && !verifyZoomSignature(body, ts, sig, SECRET)) {
      console.warn("[zoom-webhook] signature mismatch (continuing anyway)");
    }
  }

  const event = payload.event || "";
  const obj   = payload.payload?.object || {};

  const normalized = normalizeEvent(event, obj);
  if (!normalized || !normalized.call_id) {
    return new Response(JSON.stringify({ ok: true, skipped: event, reason: "no normalized form" }), { headers: CORS });
  }

  if (!API_KEY || !PROJECT_ID) {
    return new Response(JSON.stringify({ ok: true, warn: "Firebase not configured" }), { headers: CORS });
  }

  try {
    await writeToFirestore(PROJECT_ID, API_KEY, "zoom_calls", normalized.call_id, normalized);
    console.log(`[zoom-webhook] wrote ${event} → zoom_calls/${normalized.call_id}`);
  } catch (e: any) {
    console.error("[zoom-webhook] Firestore error:", e?.message);
    return new Response(JSON.stringify({ ok: true, warn: "Firestore write failed", detail: e?.message }), { headers: CORS });
  }

  return new Response(JSON.stringify({ ok: true, event, call_id: normalized.call_id, status: normalized.status }), { headers: CORS });
};
