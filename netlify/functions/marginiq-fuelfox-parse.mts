import type { Context, Config } from "@netlify/functions";
import pdfParse from "pdf-parse";

// Davis MarginIQ — FuelFox invoice pair parser.
// POST body: { pdfs: [{ filename, data_base64 }, { filename, data_base64 }] }
// Returns: { summary, log, trucks, totals } — paired and computed.
//
// TRUE price per gallon bakes in fuel + taxes + delivery fee (all of it):
//   true_rate = (fuel_cost + taxes + delivery_fee) / total_gallons
//   truck_cost = truck_gallons × true_rate
// Per-truck cost sums back to grand_total (the amount Chad actually pays).

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  try {
    const body = await req.json();
    const pdfs = body.pdfs || [];
    if (pdfs.length < 1) return json({ error: "No PDFs supplied" }, 400);

    // Parse each PDF to text + identify which is summary vs service log
    let summary: any = null;
    let log: any = null;
    const notes: string[] = [];

    for (const pdf of pdfs) {
      if (!pdf.data_base64) continue;
      const bytes = Buffer.from(pdf.data_base64, "base64");
      const parsed = await pdfParse(bytes);
      const text = parsed.text;

      // Identify type by keyword heuristics
      const isSummary = /INVOICE\s+DD?\d+/i.test(text) || /Diesel Sales[\s\S]{0,100}Diesel Taxes/i.test(text);
      const isLog = /Service Log|Unit Number\s*\n\s*Gallons/i.test(text);

      if (isSummary) summary = { ...parseSummary(text), source_filename: pdf.filename };
      else if (isLog) log = { ...parseServiceLog(text), source_filename: pdf.filename };
      else notes.push(`Unknown PDF type: ${pdf.filename}`);
    }

    if (!summary) return json({ error: "No summary invoice found in upload. Need Invoice_*.pdf" }, 400);
    if (!log) return json({ error: "No service log found in upload. Need FuelFox-ServiceLog-*.pdf" }, 400);

    // Sanity check: gallons should match
    const logTotal = log.rows.reduce((s: number, r: any) => s + r.gallons, 0);
    const gallonsMismatch = Math.abs(logTotal - summary.total_gallons) > 0.5;
    if (gallonsMismatch) {
      notes.push(`⚠️ Gallons mismatch — summary: ${summary.total_gallons}, log sum: ${logTotal.toFixed(1)}. Invoices may not belong to same period.`);
    }

    // Pair and compute using the TRUE rate (fuel + tax + delivery all baked in)
    const trueRate = summary.effective_rate_with_delivery!;
    const trucks = log.rows.map((r: any) => ({
      unit: r.unit,
      gallons: r.gallons,
      posted_rate: r.posted_rate,
      posted_charge: r.posted_charge,
      true_rate: trueRate,
      true_cost: Math.round(r.gallons * trueRate * 100) / 100,
      uplift: Math.round((r.gallons * trueRate - r.posted_charge) * 100) / 100,
      invoice_number: summary.invoice_number,
      service_date: log.service_date,
      period_key: summary.invoice_date ? summary.invoice_date.replace(/\//g, "-") : null,
    }));

    return json({
      vendor: "fuelfox",
      summary,
      log: { service_date: log.service_date, ambassador: log.ambassador, service_vehicle: log.service_vehicle },
      trucks,
      totals: {
        total_gallons: summary.total_gallons,
        posted_fuel_cost: summary.diesel_cost,
        tax: summary.diesel_tax,
        delivery_fee: summary.delivery_fee,
        grand_total: summary.diesel_cost + summary.diesel_tax + summary.delivery_fee,
        true_rate: trueRate,
        fuel_only_rate: summary.effective_rate,
        posted_rate: summary.posted_rate,
        truck_count: trucks.length,
      },
      notes,
    });
  } catch (err: any) {
    return json({ error: err.message || "Parse error", stack: err.stack?.substring(0, 500) }, 500);
  }
};

function parseSummary(text: string) {
  // Invoice number
  const invMatch = text.match(/INVOICE\s+([A-Z]+\d+)\s*DATE/) || text.match(/INVOICE\s*\n+\s*([A-Z]+\d+)/);
  const invoice_number = invMatch ? invMatch[1] : null;

  // Dates
  const dateMatch = text.match(/(\d{2}\/\d{2}\/\d{4})\s*\n\s*(\d{2}\/\d{2}\/\d{4})/);
  const invoice_date = dateMatch ? dateMatch[1] : null;
  const due_date = dateMatch ? dateMatch[2] : null;

  // QTY + RATE — appear after all descriptions, before AMOUNT column
  const midBlockMatch = text.match(/Delivery Fee\s*\n[\s\S]*?AMOUNT/);
  let diesel_qty: number | null = null;
  let diesel_rate: number | null = null;
  if (midBlockMatch) {
    const nums = midBlockMatch[0].match(/[\d,]*\d+\.\d+|\d+/g) || [];
    // Gallons: has comma OR >=100 without rate-style decimals
    for (const n of nums) {
      if (n.includes(",")) { diesel_qty = parseFloat(n.replace(/,/g, "")); break; }
      if (n.includes(".")) {
        const dec = n.split(".")[1];
        const val = parseFloat(n);
        if (val >= 100 && dec.length <= 2) { diesel_qty = val; break; }
      }
    }
    // Rate: small 3-decimal number
    for (const n of nums) {
      if (!n.includes(".")) continue;
      const dec = n.split(".")[1];
      const val = parseFloat(n.replace(/,/g, ""));
      if (dec.length === 3 && val < 50) { diesel_rate = val; break; }
    }
  }

  // Amounts in order: fuel, tax, delivery, subtotal
  const amountsBlock = text.split("AMOUNT")[1] || "";
  const amts = (amountsBlock.match(/[\d,]+\.\d{2}/g) || []).map(a => parseFloat(a.replace(/,/g, "")));
  const diesel_cost = amts[0] ?? null;
  const diesel_tax = amts[1] ?? null;
  const delivery_fee = amts[2] ?? 0;
  const subtotal = amts[3] ?? null;

  // Fallback: if diesel_qty failed but we have cost + rate, derive it
  let final_qty = diesel_qty;
  if (!final_qty && diesel_cost && diesel_rate) {
    final_qty = Math.round((diesel_cost / diesel_rate) * 100) / 100;
  }

  const effective_rate = (diesel_cost != null && diesel_tax != null && final_qty)
    ? Math.round(((diesel_cost + diesel_tax) / final_qty) * 10000) / 10000
    : null;
  const effective_rate_with_delivery = (diesel_cost != null && diesel_tax != null && final_qty)
    ? Math.round(((diesel_cost + diesel_tax + (delivery_fee || 0)) / final_qty) * 10000) / 10000
    : null;

  return {
    invoice_number, invoice_date, due_date,
    total_gallons: final_qty,
    posted_rate: diesel_rate,
    diesel_cost, diesel_tax, delivery_fee, subtotal,
    effective_rate, effective_rate_with_delivery,
  };
}

function parseServiceLog(text: string) {
  const dateMatch = text.match(/Service Date:\s*\n?\s*(\d{2}\/\d{2}\/\d{4})/);
  const service_date = dateMatch ? dateMatch[1] : null;

  const ambMatch = text.match(/Ambassador:\s*\n?\s*([^\n]+)/);
  const ambassador = ambMatch ? ambMatch[1].trim() : null;

  const svMatch = text.match(/Service Vehicle:\s*\n?\s*(\d+)/);
  const service_vehicle = svMatch ? svMatch[1] : null;

  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);
  const units: string[] = [];
  const gallons: number[] = [];
  const rates: number[] = [];
  const charges: number[] = [];

  let skipNextNumberAfterSV = false;
  for (const line of lines) {
    if (line === "Unit Number" || line === "Gallons" || line === "Price Per Gallon" || line === "Total Charge" || line === "Diesel") continue;
    if (line.startsWith("Total:")) continue;
    if (/^Service\s|^Customer:|^Ambassador:|^FoxSpot:/.test(line)) continue;
    if (line.startsWith("Service Vehicle:")) { skipNextNumberAfterSV = true; continue; }

    // Unit number: bare 3-4 digit integer
    if (/^\d{3,4}$/.test(line)) {
      if (skipNextNumberAfterSV) { skipNextNumberAfterSV = false; continue; }
      if (service_vehicle && line === service_vehicle) continue;
      units.push(line);
      continue;
    }

    // $-prefixed amounts: distinguish rate (3-4 decimals) from charge (2 decimals)
    if (line.startsWith("$")) {
      const val = parseFloat(line.replace(/[$,]/g, ""));
      if (isNaN(val)) continue;
      const decPart = line.split(".")[1] || "";
      if (decPart.length >= 3) rates.push(val);
      else charges.push(val);
      continue;
    }

    // Plain decimal: gallons
    if (/^\d+\.\d+$/.test(line)) {
      gallons.push(parseFloat(line));
      continue;
    }
  }

  const n = Math.min(units.length, gallons.length, rates.length, charges.length);
  const rows = [];
  for (let i = 0; i < n; i++) {
    rows.push({
      unit: units[i],
      gallons: gallons[i],
      posted_rate: rates[i],
      posted_charge: charges[i],
    });
  }

  return {
    service_date, ambassador, service_vehicle,
    rows,
    total_units: units.length,
    total_gallons: Math.round(gallons.slice(0, n).reduce((s, g) => s + g, 0) * 100) / 100,
  };
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
