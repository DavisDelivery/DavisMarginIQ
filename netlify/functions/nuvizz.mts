/**
 * NuVizz API proxy — wraps the NuVizz v7 REST API for MarginIQ.
 *
 * Env vars required (set in Netlify):
 *   NUVIZZ_USERNAME   — Basic auth username
 *   NUVIZZ_PASSWORD   — Basic auth password
 *   NUVIZZ_COMPANY    — Company code (e.g. "davis")
 *   NUVIZZ_BASE_URL   — e.g. https://portal.nuvizz.com/deliverit/openapi/v7
 */

import type { Config, Context } from "@netlify/functions";

function getBaseUrl(): string {
  return process.env["NUVIZZ_BASE_URL"] || "https://portal.nuvizz.com/deliverit/openapi/v7";
}

function getAuth(): string {
  const user = process.env["NUVIZZ_USERNAME"] ?? "";
  const pass = process.env["NUVIZZ_PASSWORD"] ?? "";
  return "Basic " + btoa(`${user}:${pass}`);
}

function getCompany(): string {
  return process.env["NUVIZZ_COMPANY"] ?? "davis";
}

async function nuvizzFetch(path: string): Promise<Response> {
  const BASE_URL = getBaseUrl();
  const url = `${BASE_URL}${path}`;
  try {
    const resp = await fetch(url, {
      headers: {
        Authorization: getAuth(),
        Accept: "application/json",
      },
    });
    const text = await resp.text();
    return new Response(text, {
      status: resp.status,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  } catch (err: any) {
    // Return a proper error instead of crashing
    return new Response(JSON.stringify({
      error: "NuVizz fetch failed",
      detail: err.message || String(err),
      url_attempted: url,
      base_url: BASE_URL,
      has_username: !!process.env["NUVIZZ_USERNAME"],
      has_password: !!process.env["NUVIZZ_PASSWORD"],
    }), {
      status: 502,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const sub = url.pathname.replace(/^\/api\/nuvizz/, "");
  const company = getCompany();
  const params = url.searchParams;

  // Diagnostic endpoint
  if (sub === "/diag" || sub === "/status") {
    return new Response(JSON.stringify({
      base_url: getBaseUrl(),
      company,
      has_username: !!process.env["NUVIZZ_USERNAME"],
      has_password: !!process.env["NUVIZZ_PASSWORD"],
      username: process.env["NUVIZZ_USERNAME"] || "(not set)",
    }), {
      headers: { "Content-Type": "application/json" },
    });
  }

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

  // GET /api/nuvizz/stop/:stopNbr
  const stopMatch = sub.match(/^\/stop\/([^/]+)$/);
  if (stopMatch) {
    return nuvizzFetch(`/stop/info/${stopMatch[1]}/${company}`);
  }

  // GET /api/nuvizz/stops?fromDTTM=&toDTTM=
  if (sub === "/stops") {
    const fromDTTM = params.get("fromDTTM") ?? "";
    const toDTTM = params.get("toDTTM") ?? "";
    const stopType = params.get("stopType") ?? "";
    const zipCode = params.get("zipCode") ?? "";
    const shipmentNbr = params.get("shipmentNbr") ?? "";

    if (shipmentNbr) {
      const q = new URLSearchParams({ shipmentNbr, ...(fromDTTM && { fromDTTM }), ...(toDTTM && { toDTTM }), ...(zipCode && { zipCode }) });
      return nuvizzFetch(`/stop/infobyshipment/${company}?${q}`);
    }

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
    return nuvizzFetch(`/load/info/${loadMatch[1]}/${company}`);
  }

  return new Response(JSON.stringify({ error: "Unknown NuVizz route", sub }), {
    status: 404,
    headers: { "Content-Type": "application/json" },
  });
};

export const config: Config = {
  path: "/api/nuvizz/*",
};
