// MarginIQ Payroll Scanner — Netlify Serverless Function
//
// Accepts: POST with JSON { rawText: string }
//   where rawText is the output of pdf.js text extraction (layout preserved)
//   from a CyberPay / Southern Payroll Services PDF.
//
// Returns: JSON { company, summary, employees, checks, validation, invariants }
//   - Deterministic parsing (no AI/vision)
//   - Invariant cross-validation bakes in format-change detection
//   - Identity map is applied client-side (Firebase-backed)

import type { Context, Config } from "@netlify/functions";
import { parsePayrollPDF } from "./lib/parsePayroll.mjs";
import { runInvariants } from "./lib/invariants.mjs";

interface ScanRequest {
  rawText: string;
  // optional: source file info for auditing/logging
  filename?: string;
  sha256?: string;
}

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "POST required" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body: ScanRequest;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (!body.rawText || typeof body.rawText !== "string") {
    return new Response(JSON.stringify({ error: "Missing rawText field" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  // Guard against pathological inputs
  if (body.rawText.length > 2_000_000) {
    return new Response(JSON.stringify({ error: "rawText exceeds 2MB limit" }), {
      status: 413,
      headers: { "Content-Type": "application/json" },
    });
  }

  // --- Parse ---
  const parsed = parsePayrollPDF(body.rawText);

  if (parsed.error || !parsed.company) {
    return new Response(
      JSON.stringify({
        error: parsed.error || "Failed to identify company code",
        filename: body.filename || null,
      }),
      { status: 422, headers: { "Content-Type": "application/json" } }
    );
  }

  // --- Invariant cross-validation ---
  const invariants = runInvariants(parsed);

  // --- Build response ---
  const response = {
    company: parsed.company,
    filename: body.filename || null,
    sha256: body.sha256 || null,
    scannedAt: new Date().toISOString(),
    summary: parsed.summary,
    employees: parsed.employees,
    checks: parsed.checks,
    validation: parsed.validation,
    invariants: {
      ok: invariants.ok,
      passedCount: invariants.passed.length,
      passed: invariants.passed,
      failed: invariants.failed,
      warnings: invariants.warnings,
    },
  };

  // If invariants failed, return 200 with the flagged result so the client
  // can display the problem — don't 500 because the parse itself succeeded.
  return new Response(JSON.stringify(response), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};

export const config: Config = {
  path: "/api/scan-payroll",
};
