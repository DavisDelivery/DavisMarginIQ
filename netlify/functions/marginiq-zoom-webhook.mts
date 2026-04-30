import type { Context } from "@netlify/functions";
import * as crypto from "crypto";

// Zoom Phone Webhook Receiver — v2.47.7
//
// Fixes from v2.47.6:
// 1. Read `handup_result` (Zoom's actual field name with typo) not `hangup_result`
// 2. Use call_id directly as doc ID, but track which user is currently active
//    (so concurrent legs don't overwrite each other's state)
// 3. The "winner" is whichever leg has status='active' (answered) — that
//    overwrites prior 'ringing' states. Ended events update the doc but
//    preserve the answerer.

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
  const forwardedBy = obj.forwarded_by || {};

  // CRITICAL: Zoom's field is "handup_result" (note the typo). Read both forms.
  const result = obj.handup_result || obj.hangup_result || obj.result || "";
  const resultLower = String(result).toLowerCase();

  const base = {
    call_id:      String(callId),
    event,
    ts:           obj.ringing_start_time || obj.answer_start_time || obj.call_end_time || new Date().toISOString(),
    updated_at:   new Date().toISOString(),

    // Who's involved (the callee on THIS leg)
    employee:     callee.name || "",
    employee_ext: String(callee.extension_number || ""),
    employee_id:  callee.user_id || "",

    // Caller info
    caller_num:   caller.phone_number || "",
    caller_name:  caller.name || "",

    // Routing context
    queue_name:   forwardedBy.name || "",
    queue_ext:    String(forwardedBy.extension_number || ""),

    direction:    obj.direction || (caller.extension_type === "pstn" ? "inbound" : "outbound"),
    handup_result: result,
  };

  switch (event) {
    case "phone.callee_ringing":
      return { ...base, status: "ringing", _priority: 1 };

    case "phone.callee_answered":
      return { ...base, status: "active", answered_at: obj.answer_start_time || new Date().toISOString(), _priority: 3 };

    case "phone.callee_ended":
    case "phone.caller_ended": {
      // Calculate duration from timestamps
      let duration = Number(obj.duration ?? obj.talk_time ?? 0);
      if (!duration && obj.connected_start_time && obj.call_end_time) {
        duration = Math.round((new Date(obj.call_end_time).getTime() - new Date(obj.connected_start_time).getTime()) / 1000);
      }

      // Map Zoom's result strings to our status
      let status: string;
      if (resultLower.includes("voicemail"))        status = "voicemail";
      else if (resultLower === "call connected")    status = "ended";       // truly answered & finished
      else if (resultLower === "answered by other device" ||
               resultLower === "answered by other member") status = "ended_elsewhere";  // rang here, picked up elsewhere
      else if (resultLower === "no answer" ||
               resultLower === "ring timeout")      status = "missed";
      else if (resultLower === "call cancel" ||
               resultLower === "cancel")            status = "cancelled";
      else if (resultLower === "rejected")          status = "rejected";
      else                                            status = "ended";      // default

      return { ...base, status, duration, _priority: 5 };
    }

    case "phone.callee_missed":
      return { ...base, status: "missed", _priority: 4 };

    case "phone.voicemail_received":
      return { ...base, status: "voicemail", voicemail_id: obj.voicemail_id || "", _priority: 4 };

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

function fromFirestoreValue(v: any): any {
  if (v.nullValue   !== undefined) return null;
  if (v.booleanValue !== undefined) return v.booleanValue;
  if (v.integerValue !== undefined) return Number(v.integerValue);
  if (v.doubleValue  !== undefined) return v.doubleValue;
  if (v.stringValue  !== undefined) return v.stringValue;
  return null;
}

async function getDoc(projectId: string, apiKey: string, collection: string, docId: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const res = await fetch(url);
  if (!res.ok) return null;
  const d: any = await res.json();
  if (!d.fields) return null;
  return Object.fromEntries(Object.entries(d.fields).map(([k, v]) => [k, fromFirestoreValue(v)]));
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
    throw new Error(`Firestore ${res.status}: ${err.slice(0, 200)}`);
  }
}

async function logIncomingEvent(projectId: string, apiKey: string, body: string) {
  try {
    const id = `${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
    await writeToFirestore(projectId, apiKey, "zoom_webhook_log", id, {
      ts: new Date().toISOString(),
      body: body.slice(0, 5000),
    });
  } catch (_) {}
}

export default async (req: Request, _ctx: Context) => {
  const SECRET     = process.env["ZOOM_WEBHOOK_SECRET_TOKEN"];
  const API_KEY    = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = process.env["FIREBASE_PROJECT_ID"];

  if (req.method === "GET") {
    return new Response(JSON.stringify({
      status: "Zoom webhook v2.47.7",
      hasSecret: !!SECRET, hasFirebaseKey: !!API_KEY, hasFirebaseProject: !!PROJECT_ID,
    }), { headers: CORS });
  }

  if (req.method !== "POST") return new Response("Method not allowed", { status: 405 });

  const body = await req.text();
  const headers: Record<string, string> = {};
  req.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });

  if (API_KEY && PROJECT_ID) logIncomingEvent(PROJECT_ID, API_KEY, body).catch(() => {});

  let payload: any;
  try { payload = JSON.parse(body); } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: CORS });
  }

  // URL validation
  if (payload.event === "endpoint.url_validation") {
    if (!SECRET) return new Response(JSON.stringify({ error: "secret missing" }), { status: 500, headers: CORS });
    const hash = crypto.createHmac("sha256", SECRET).update(payload.payload?.plainToken || "").digest("hex");
    return new Response(JSON.stringify({ plainToken: payload.payload?.plainToken, encryptedToken: hash }), { headers: CORS });
  }

  if (SECRET) {
    const ts  = headers["x-zm-request-timestamp"] || "";
    const sig = headers["x-zm-signature"] || "";
    if (ts && sig && !verifyZoomSignature(body, ts, sig, SECRET)) {
      console.warn("[zoom-webhook] signature mismatch (continuing)");
    }
  }

  const event = payload.event || "";
  const obj   = payload.payload?.object || {};
  const normalized = normalizeEvent(event, obj);

  if (!normalized || !normalized.call_id) {
    return new Response(JSON.stringify({ ok: true, skipped: event }), { headers: CORS });
  }

  if (!API_KEY || !PROJECT_ID) {
    return new Response(JSON.stringify({ ok: true, warn: "Firebase not configured" }), { headers: CORS });
  }

  // ── DECISION LOGIC ─────────────────────────────────────────────────────────
  // Multiple legs of the same call_id arrive in random order. We need to
  // pick the "winning" version of the call to display in Live Feed:
  //
  //   - If the existing doc shows status=active or ended (with Call connected
  //     handup_result), DON'T overwrite the employee with a different leg's
  //     ringing/missed event
  //   - If new event is "active" (someone picked up), ALWAYS overwrite —
  //     they're the real answerer
  //   - If the new event is the ended event with handup="Call connected",
  //     this is the actual answerer ending the call — write it
  //   - If new is ended with "Answered by Other Device", DON'T overwrite if
  //     someone else's "active" is already there
  //   - Use _priority field as tiebreaker
  // ───────────────────────────────────────────────────────────────────────────

  try {
    const docId = String(normalized.call_id);
    const existing = await getDoc(PROJECT_ID, API_KEY, "zoom_calls", docId);

    let shouldWrite = true;

    if (existing) {
      const existingStatus = existing.status;
      const existingPri    = Number(existing._priority) || 0;
      const newPri         = Number(normalized._priority) || 0;
      const existingIsRealAnswer = existing.handup_result === "Call connected" ||
                                   existingStatus === "active";
      const newIsRealAnswer = normalized.handup_result === "Call connected" ||
                              normalized.status === "active";

      // Once someone has truly answered, don't let other legs' "ringing" or
      // "ended_elsewhere" events overwrite that record
      if (existingIsRealAnswer && !newIsRealAnswer) {
        // BUT: do update timestamps if it's a later event for the same person
        if (existing.employee_id === normalized.employee_id && newPri > existingPri) {
          shouldWrite = true;
        } else {
          shouldWrite = false;
        }
      } else if (newPri < existingPri && !newIsRealAnswer) {
        // Lower-priority event arriving later (out of order) — skip
        shouldWrite = false;
      }
    }

    if (shouldWrite) {
      await writeToFirestore(PROJECT_ID, API_KEY, "zoom_calls", docId, normalized);
    }

    return new Response(JSON.stringify({
      ok: true, event, call_id: normalized.call_id,
      status: normalized.status, written: shouldWrite,
      employee: normalized.employee,
    }), { headers: CORS });
  } catch (e: any) {
    console.error("[zoom-webhook] error:", e?.message);
    return new Response(JSON.stringify({ ok: true, warn: e?.message }), { headers: CORS });
  }
};
