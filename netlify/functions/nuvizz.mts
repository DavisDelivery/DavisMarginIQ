/**
 * NuVizz API proxy — wraps the NuVizz v7 REST API for MarginIQ.
 *
 * Env vars required (set in Netlify):
 *   NUVIZZ_USERNAME   — Basic auth username
 *   NUVIZZ_PASSWORD   — Basic auth password
 *   NUVIZZ_COMPANY    — Company code (e.g. "davis")
 *
 * Routes (all prefixed /api/nuvizz):
 *   GET /api/nuvizz/stop/:stopNbr          — single stop by stop number
 *   GET /api/nuvizz/stops?fromDTTM=&toDTTM= — stops by date range (via customer endpoint)
 *   GET /api/nuvizz/stop/events?stopNbr=   — stop event history
 *   GET /api/nuvizz/stop/eta?stopNbr=      — ETA info
 *   GET /api/nuvizz/load/:loadNbr          — load info
 */

import type { Config, Context } from "@netlify/functions";

const BASE_URL = "https://contact-support.nuvizz.com/deliverit/openapi/v7";

function getAuth(): string {
  const user = Netlify.env.get("NUVIZZ_USERNAME") ?? "";
  const pass = Netlify.env.get("NUVIZZ_PASSWORD") ?? "";
  return "Basic " + btoa(`${user}:${pass}`);
}

function getCompany(): string {
  return Netlify.env.get("NUVIZZ_COMPANY") ?? "davis";
}

async function nuvizzFetch(path: string): Promise<Response> {
  const url = `${BASE_URL}${path}`;
  const resp = await fetch(url, {
    headers: {
      Authorization: getAuth(),
      Accept: "application/json",
    },
  });
  const text = await resp.text();
  return new Response(text, {
    status: resp.status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  // Strip the /api/nuvizz prefix to get the sub-route
  const sub = url.pathname.replace(/^\/api\/nuvizz/, "");
  const company = getCompany();
  const params = url.searchParams;

  // GET /api/nuvizz/stop/events
  if (sub === "/stop/events") {
    const stopNbr = params.get("stopNbr") ?? "";
    const stopId = params.get("stopId") ?? "";
    const q = stopNbr ? `stopNbr=${stopNbr}` : `stopId=${stopId}`;
    return nuvizzFetch(`/stop/eventinfo/${company}?${q}`);
  }

  // GET /api/nuvizz/stop/eta
  if (sub === "/stop/eta") {
    const stopNbr = params.get("stopNbr") ?? "";
    const stopId = params.get("stopId") ?? "";
    const q = stopNbr ? `stopNbr=${stopNbr}` : `stopId=${stopId}`;
    return nuvizzFetch(`/stop/etainfo/${company}?${q}`);
  }

  // GET /api/nuvizz/stop/:stopNbr — single stop by number
  const stopMatch = sub.match(/^\/stop\/([^/]+)$/);
  if (stopMatch) {
    const stopNbr = stopMatch[1];
    return nuvizzFetch(`/stop/info/${stopNbr}/${company}`);
  }

  // GET /api/nuvizz/stops?fromDTTM=&toDTTM=[&stopType=][&zipCode=]
  if (sub === "/stops") {
    const fromDTTM = params.get("fromDTTM") ?? "";
    const toDTTM = params.get("toDTTM") ?? "";
    const stopType = params.get("stopType") ?? "";
    const zipCode = params.get("zipCode") ?? "";
    const shipmentNbr = params.get("shipmentNbr") ?? "";

    // If filtering by shipment number use the shipment endpoint
    if (shipmentNbr) {
      const q = new URLSearchParams({ shipmentNbr, ...(fromDTTM && { fromDTTM }), ...(toDTTM && { toDTTM }), ...(zipCode && { zipCode }) });
      return nuvizzFetch(`/stop/infobyshipment/${company}?${q}`);
    }

    // Otherwise use the customer/date range endpoint
    const q = new URLSearchParams({
      ...(fromDTTM && { fromDTTM }),
      ...(toDTTM && { toDTTM }),
      ...(stopType && { stopType }),
      ...(zipCode && { zipCode }),
    });
    return nuvizzFetch(`/stop/info/customer/${company}?${q}`);
  }

  // GET /api/nuvizz/load/:loadNbr
  const loadMatch = sub.match(/^\/load\/([^/]+)$/);
  if (loadMatch) {
    const loadNbr = loadMatch[1];
    return nuvizzFetch(`/load/info/${loadNbr}/${company}`);
  }

  return new Response(JSON.stringify({ error: "Unknown NuVizz route", sub }), {
    status: 404,
    headers: { "Content-Type": "application/json" },
  });
};

export const config: Config = {
  path: "/api/nuvizz/*",
};
