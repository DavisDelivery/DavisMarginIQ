import type { Context, Config } from "@netlify/functions";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";

// Davis MarginIQ — Dispute Package PDF Generator
// POST body: {
//   items: [{ pro, customer, billed, paid, variance, pu_date, category, code, city, zip }],
//   customer: string,
//   ap_contact: { billing_email, ap_contact_name, ap_contact_phone }
// }
// Returns PDF bytes as base64 for client to download.

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  try {
    const body = await req.json();
    const items = body.items || [];
    const customer = body.customer || "Unknown Customer";
    const apContact = body.ap_contact || {};

    if (items.length === 0) {
      return json({ error: "No items provided" }, 400);
    }

    const pdf = await PDFDocument.create();
    const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
    const font = await pdf.embedFont(StandardFonts.Helvetica);

    const BRAND = rgb(0.118, 0.357, 0.573);  // #1e5b92
    const RED = rgb(0.937, 0.267, 0.267);
    const DARK = rgb(0.06, 0.09, 0.16);
    const MUTED = rgb(0.39, 0.45, 0.55);

    let page = pdf.addPage([612, 792]); // letter size
    const { width, height } = page.getSize();
    let y = height - 50;
    const margin = 50;

    // Header
    page.drawRectangle({ x: 0, y: height - 80, width, height: 80, color: BRAND });
    page.drawText("Davis Delivery Service, Inc.", { x: margin, y: height - 35, size: 20, font: fontBold, color: rgb(1, 1, 1) });
    page.drawText("Payment Discrepancy Claim", { x: margin, y: height - 58, size: 11, font, color: rgb(0.9, 0.95, 1) });
    y = height - 110;

    // Company info block
    page.drawText("Davis Delivery Service, Inc.", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 14;
    page.drawText("943 Gainesville Hwy, Buford, GA 30518", { x: margin, y, size: 9, font, color: MUTED });
    y -= 12;
    page.drawText("customerservice@davisdelivery.com | (770) 555-0100", { x: margin, y, size: 9, font, color: MUTED });
    y -= 20;

    // Date + To
    const today = new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
    page.drawText(`Date: ${today}`, { x: margin, y, size: 10, font, color: DARK });
    y -= 18;

    page.drawText("To:", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 13;
    page.drawText(apContact.ap_contact_name || "Accounts Payable", { x: margin + 15, y, size: 10, font, color: DARK });
    y -= 12;
    page.drawText(customer, { x: margin + 15, y, size: 10, font, color: DARK });
    if (apContact.billing_email) {
      y -= 12;
      page.drawText(apContact.billing_email, { x: margin + 15, y, size: 9, font, color: MUTED });
    }
    y -= 24;

    // Summary
    const totalBilled = items.reduce((s: number, i: any) => s + (i.billed || 0), 0);
    const totalPaid = items.reduce((s: number, i: any) => s + (i.paid || 0), 0);
    const totalClaim = totalBilled - totalPaid;

    page.drawText("CLAIM SUMMARY", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 16;
    const sumLines = [
      `Total Invoiced: $${fmt(totalBilled)}`,
      `Total Paid: $${fmt(totalPaid)}`,
      `Amount Claimed: $${fmt(totalClaim)}  (${items.length} item${items.length > 1 ? "s" : ""})`,
    ];
    for (const line of sumLines) {
      page.drawText(line, { x: margin + 10, y, size: 10, font, color: DARK });
      y -= 14;
    }
    y -= 10;

    // Line items table
    page.drawText("LINE ITEM DETAIL", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 18;

    // Table header
    const colX = [margin, margin + 85, margin + 175, margin + 285, margin + 345, margin + 410, margin + 470];
    const headers = ["PRO", "Pickup Date", "Category", "Billed", "Paid", "Variance", "Age"];
    page.drawRectangle({ x: margin - 5, y: y - 4, width: width - margin * 2 + 10, height: 16, color: rgb(0.95, 0.97, 1) });
    for (let i = 0; i < headers.length; i++) {
      page.drawText(headers[i], { x: colX[i], y, size: 8, font: fontBold, color: BRAND });
    }
    y -= 16;

    // Rows (paginate if needed)
    for (const item of items) {
      if (y < 80) {
        page = pdf.addPage([612, 792]);
        y = height - 60;
      }
      const pro = truncate(String(item.pro || ""), 12);
      const pu = item.pu_date || "";
      const cat = truncate(CATEGORY_LABELS[item.category] || item.category || "", 16);
      const billed = "$" + fmt(item.billed || 0);
      const paid = "$" + fmt(item.paid || 0);
      const variance = "$" + fmt((item.billed || 0) - (item.paid || 0));
      const age = item.age_days != null ? `${item.age_days}d` : "—";

      page.drawText(pro, { x: colX[0], y, size: 8, font, color: DARK });
      page.drawText(pu, { x: colX[1], y, size: 8, font, color: DARK });
      page.drawText(cat, { x: colX[2], y, size: 8, font, color: DARK });
      page.drawText(billed, { x: colX[3], y, size: 8, font, color: DARK });
      page.drawText(paid, { x: colX[4], y, size: 8, font, color: DARK });
      page.drawText(variance, { x: colX[5], y, size: 8, font: fontBold, color: RED });
      page.drawText(age, { x: colX[6], y, size: 8, font, color: MUTED });
      y -= 12;
    }

    y -= 15;
    if (y < 180) {
      page = pdf.addPage([612, 792]);
      y = height - 60;
    }

    // Supporting documentation note
    page.drawText("SUPPORTING DOCUMENTATION", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 15;
    const supportLines = [
      "• Original Uline billing records (available on request)",
      "• Signed proof of delivery (available on request)",
      "• Accessorial authorization (where applicable)",
      "• DDIS820 remittance file showing payment discrepancy",
    ];
    for (const line of supportLines) {
      page.drawText(line, { x: margin + 10, y, size: 9, font, color: DARK });
      y -= 13;
    }
    y -= 15;

    // Requested resolution
    page.drawText("REQUESTED RESOLUTION", { x: margin, y, size: 11, font: fontBold, color: BRAND });
    y -= 15;
    const resolution = `Please issue remittance for $${fmt(totalClaim)} within 30 days, or contact our AR office to discuss.`;
    page.drawText(resolution, { x: margin + 10, y, size: 9, font, color: DARK, maxWidth: width - margin * 2 - 20 });
    y -= 20;

    // Footer
    page.drawText("Thank you,", { x: margin, y, size: 10, font, color: DARK });
    y -= 14;
    page.drawText("Davis Delivery Service AR Team", { x: margin, y, size: 10, font: fontBold, color: DARK });
    y -= 12;
    page.drawText("customerservice@davisdelivery.com", { x: margin, y, size: 9, font, color: MUTED });

    // Page numbering footer on every page
    const pages = pdf.getPages();
    for (let i = 0; i < pages.length; i++) {
      const p = pages[i];
      p.drawText(`Page ${i + 1} of ${pages.length} — Generated by Davis MarginIQ`, {
        x: margin,
        y: 30,
        size: 7,
        font,
        color: MUTED,
      });
    }

    const pdfBytes = await pdf.save();
    const b64 = Buffer.from(pdfBytes).toString("base64");

    return json({
      data: b64,
      filename: `Dispute_${customer.replace(/[^a-z0-9]/gi, "_")}_${new Date().toISOString().slice(0, 10)}.pdf`,
      item_count: items.length,
      total_claim: totalClaim,
    });
  } catch (err: any) {
    return json({ error: err.message || "PDF generation failed" }, 500);
  }
};

const CATEGORY_LABELS: Record<string, string> = {
  paid_in_full: "Paid in Full",
  short_paid: "Short-paid",
  accessorial_ignored: "Accessorial Ignored",
  zero_pay: "Zero-pay",
  overpaid: "Overpaid",
  orphan: "Orphan",
};

function fmt(n: number): string {
  return Number(n || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function truncate(s: string, max: number): string {
  if (!s) return "";
  return s.length > max ? s.substring(0, max - 1) + "…" : s;
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
