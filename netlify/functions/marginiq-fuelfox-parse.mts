import type { Context, Config } from "@netlify/functions";
import pdfParsePkg from "pdf-parse";
const { PDFParse } = pdfParsePkg as any;

// Davis MarginIQ — FuelFox invoice pair parser.
// POST body: { pdfs: [{ filename, data_base64 }, { filename, data_base64 }] }
// Returns: { summary, log, trucks, totals } — paired and computed.
//
// TRUE price per gallon bakes in fuel + taxes + delivery fee (all of it):
//   true_rate = (fuel_cost + taxes + delivery_fee) / total_gallons
//   truck_cost = truck_gallons × true_rate
// Per-truck cost sums back to grand_total (what Chad actually pays).

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  try {
    const body = await req.json();
    const pdfs = body.pdfs || [];
    if (pdfs.length < 1) return json({ error: "No PDFs supplied" }, 400);

    let summary: any = null;
    let log: any = null;
    const notes: string[] = [];

    for (const pdf of pdfs) {
      if (!pdf.data_base64) continue;
      const bytes = Buffer.from(pdf.data_base64, "base64");
      const parser = new PDFParse({ data: bytes });
      const parsed = await parser.getText();
      const text = parsed.text || "";

      const isLog = /Service Log/i.test(text) || /Unit Number\s+Gallons\s+Price/i.test(text);
      const isSummary = /Diesel Sales/i.test(text) && /BALANCE DUE/i.test(text);

      if (isLog) log = { ...parseServiceLog(text), source_filename: pdf.filename };
      else if (isSummary) summary = { ...parseSummary(text), source_filename: pdf.filename };
      else notes.push(`Unknown PDF type: ${pdf.filename}`);
    }

    if (!summary) return json({ error: "No summary invoice found. Need the Invoice PDF (has Diesel Sales + BALANCE DUE)." }, 400);
    if (!log) return json({ error: "No service log found. Need the ServiceLog PDF (has Unit Number / Gallons columns)." }, 400);

    // Sanity: gallon totals must match
    const logTotal = log.rows.reduce((s: number, r: any) => s + r.gallons, 0);
    const gallonsMismatch = summary.total_gallons && Math.abs(logTotal - summary.total_gallons) > 0.5;
    if (gallonsMismatch) {
      notes.push(`⚠️ Gallons mismatch — summary: ${summary.total_gallons}, service log sum: ${logTotal.toFixed(1)}. Invoices may not belong to the same period.`);
    }

    const trueRate = summary.true_rate!;
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
        grand_total: summary.grand_total,
        true_rate: trueRate,
        fuel_only_rate: summary.fuel_only_rate,
        posted_rate: summary.posted_rate,
        truck_count: trucks.length,
      },
      notes,
    });
  } catch (err: any) {
    return json({ error: err.message || "Parse error", stack: (err.stack || "").substring(0, 500) }, 500);
  }
};

// ─── Parsers: work on row-based tab-separated output from PDFParse ───

function parseSummary(text: string) {
  // Example rows (from real pdf-parse output):
  //   INVOICE \tDD404
  //   DATE \t04/16/2026
  //   DUE DATE \t04/23/2026
  //   04/16/2026 \tDiesel Sales \t1,381.70 \t4.317 \t5,964.80
  //   04/16/2026 \tDiesel Taxes \t1 343.07611 \t343.08
  //   04/16/2026 \tDelivery Fee \tDelivery Fee \t1 \t150.00 \t150.00
  //   BALANCE DUE \t$6,457.88

  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);

  // Invoice number — "INVOICE \tDD404" format
  let invoice_number: string | null = null;
  for (const l of lines) {
    const m = l.match(/^INVOICE\s+([A-Z]+\d+)\s*$/i);
    if (m) { invoice_number = m[1]; break; }
  }

  // Dates
  let invoice_date: string | null = null;
  let due_date: string | null = null;
  for (const l of lines) {
    const md = l.match(/^DATE\s+(\d{1,2}\/\d{1,2}\/\d{4})/i);
    if (md && !invoice_date) { invoice_date = md[1]; continue; }
    const dd = l.match(/^DUE DATE\s+(\d{1,2}\/\d{1,2}\/\d{4})/i);
    if (dd) { due_date = dd[1]; continue; }
  }

  // Line items
  let total_gallons: number | null = null;
  let posted_rate: number | null = null;
  let diesel_cost: number | null = null;
  let diesel_tax: number | null = null;
  let delivery_fee = 0;

  for (const l of lines) {
    if (/Diesel Sales/i.test(l)) {
      // "...Diesel Sales 1,381.70 4.317 5,964.80"
      const nums = (l.match(/[\d,]+\.\d+/g) || []).map(n => parseFloat(n.replace(/,/g, "")));
      if (nums.length >= 3) {
        total_gallons = nums[0];
        posted_rate = nums[1];
        diesel_cost = nums[2];
      } else if (nums.length === 2) {
        total_gallons = nums[0];
        diesel_cost = nums[1];
      }
    } else if (/Diesel Taxes/i.test(l)) {
      // "...Diesel Taxes 1 343.07611 343.08"
      // Pick the LAST 2-decimal number as the AMOUNT column
      const twoDecimals = l.match(/[\d,]+\.\d{2}(?!\d)/g);
      if (twoDecimals && twoDecimals.length > 0) {
        diesel_tax = parseFloat(twoDecimals[twoDecimals.length - 1].replace(/,/g, ""));
      }
    } else if (/Delivery Fee/i.test(l)) {
      // "...Delivery Fee Delivery Fee 1 150.00 150.00"
      const twoDecimals = l.match(/[\d,]+\.\d{2}(?!\d)/g);
      if (twoDecimals && twoDecimals.length > 0) {
        delivery_fee = parseFloat(twoDecimals[twoDecimals.length - 1].replace(/,/g, ""));
      }
    }
  }

  // Fallback: derive gallons from cost/rate if gallons didn't parse
  if (!total_gallons && diesel_cost && posted_rate) {
    total_gallons = Math.round((diesel_cost / posted_rate) * 100) / 100;
  }

  const grand_total = (diesel_cost || 0) + (diesel_tax || 0) + (delivery_fee || 0);
  const fuel_only_rate = (diesel_cost != null && diesel_tax != null && total_gallons)
    ? Math.round(((diesel_cost + diesel_tax) / total_gallons) * 10000) / 10000
    : null;
  const true_rate = (diesel_cost != null && diesel_tax != null && total_gallons)
    ? Math.round(((diesel_cost + diesel_tax + delivery_fee) / total_gallons) * 10000) / 10000
    : null;

  return {
    invoice_number, invoice_date, due_date,
    total_gallons,
    posted_rate,
    diesel_cost, diesel_tax, delivery_fee,
    grand_total,
    fuel_only_rate,
    true_rate,
  };
}

function parseServiceLog(text: string) {
  // Row shape: "0424 100.6 $4.3170 $434.29"
  // Service vehicle # is in "Service Vehicle: 417" — must be excluded from truck list
  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);

  let service_date: string | null = null;
  let ambassador: string | null = null;
  let service_vehicle: string | null = null;

  for (const l of lines) {
    const sd = l.match(/^Service Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/i);
    if (sd) { service_date = sd[1]; continue; }
    const amb = l.match(/^Ambassador:\s*(.+)$/i);
    if (amb) { ambassador = amb[1].trim(); continue; }
    const sv = l.match(/^Service Vehicle:\s*(\d+)/i);
    if (sv) { service_vehicle = sv[1]; continue; }
  }

  // Truck row: unit + gallons + $rate + $charge
  const rowRe = /^(\d{3,4})\s+([\d.]+)\s+\$?([\d.]+)\s+\$?([\d,]+\.\d{2})\s*$/;
  const rows: any[] = [];

  for (const l of lines) {
    const m = l.match(rowRe);
    if (!m) continue;
    const unit = m[1];
    // Skip FuelFox's own service vehicle (not a Davis truck)
    if (service_vehicle && unit === service_vehicle) continue;
    rows.push({
      unit,
      gallons: parseFloat(m[2]),
      posted_rate: parseFloat(m[3]),
      posted_charge: parseFloat(m[4].replace(/,/g, "")),
    });
  }

  return {
    service_date, ambassador, service_vehicle,
    rows,
    total_units: rows.length,
    total_gallons: Math.round(rows.reduce((s, r) => s + r.gallons, 0) * 100) / 100,
  };
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
