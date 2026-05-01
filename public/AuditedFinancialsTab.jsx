// AuditedFinancialsTab.jsx — Audited Financials from AMP CPAs
// v2.0.0 — Adds Dashboard view + AI Chat Q&A. Fixes silent-fail load bug.
//
// Views: 📊 Dashboard | 📋 Statements | 💬 Ask AI | 📧 Gmail Sync

(function () {
"use strict";

const { useState, useEffect, useMemo, useCallback, useRef } = React;

// ─── Theme ───────────────────────────────────────────────────────────────────
const T = {
  brand:"#1e5b92", brandLight:"#2a7bc8", brandDark:"#143f66", brandPale:"#e8f0f8",
  bg:"#f0f4f8", bgWhite:"#ffffff", bgCard:"#ffffff", bgSurface:"#f8fafc",
  text:"#0f172a", textMuted:"#64748b", textDim:"#94a3b8",
  border:"#e2e8f0", borderLight:"#f1f5f9",
  green:"#10b981", greenBg:"#ecfdf5", greenText:"#065f46",
  red:"#ef4444", redBg:"#fef2f2", redText:"#991b1b",
  yellow:"#f59e0b", yellowBg:"#fffbeb", yellowText:"#92400e",
  blue:"#3b82f6", blueBg:"#eff6ff", blueText:"#1e40af",
  purple:"#8b5cf6", purpleBg:"#f5f3ff",
  radius:"12px", radiusSm:"8px",
  shadow:"0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
};

// ─── Formatters ───────────────────────────────────────────────────────────────
const fmt   = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK  = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1000000) return "$"+(v/1000000).toFixed(2)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtPct= (n,d=1) => n==null||isNaN(n)?"0%":Number(n).toFixed(d)+"%";

// ─── Styles ───────────────────────────────────────────────────────────────────
const card  = { background:T.bgCard, borderRadius:T.radius, border:`1px solid ${T.border}`, padding:"16px", boxShadow:T.shadow };
const tblH  = { padding:"8px 12px", textAlign:"left", fontSize:10, fontWeight:700, textTransform:"uppercase", color:T.textDim, background:T.bgSurface, borderBottom:`1px solid ${T.border}`, whiteSpace:"nowrap" };
const tblD  = { padding:"8px 12px", fontSize:12, color:T.text, borderBottom:`1px solid ${T.borderLight}` };
const tblDR = { ...tblD, textAlign:"right", fontVariantNumeric:"tabular-nums" };

// ─── Month helpers ────────────────────────────────────────────────────────────
const MONTH_NAMES = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const MONTH_FULL  = ["","January","February","March","April","May","June","July","August","September","October","November","December"];

function monthLabel(key) {
  if (!key) return "—";
  const [y, m] = key.split("-");
  return `${MONTH_NAMES[parseInt(m)]} ${y}`;
}

function extractMonthFromSubject(subject) {
  if (!subject) return null;
  const s = subject.toLowerCase();
  for (let i = 1; i <= 12; i++) {
    const full = MONTH_FULL[i].toLowerCase();
    const abbr = MONTH_NAMES[i].toLowerCase();
    const yearMatch = subject.match(/\b(20\d{2})\b/);
    const year = yearMatch ? yearMatch[1] : new Date().getFullYear().toString();
    if (s.includes(full) || s.includes(abbr)) {
      return `${year}-${String(i).padStart(2, "0")}`;
    }
  }
  const m1 = subject.match(/\b(\d{1,2})[\/-](20\d{2})\b/);
  if (m1) return `${m1[2]}-${m1[1].padStart(2,"0")}`;
  const m2 = subject.match(/\b(20\d{2})[\/-](\d{1,2})\b/);
  if (m2) return `${m2[1]}-${m2[2].padStart(2,"0")}`;
  return null;
}

// ─── PDF → PNG pages via pdf.js ───────────────────────────────────────────────
async function pdfToPages(pdfBytes, maxPages = 20) {
  if (!window.pdfjsLib) throw new Error("pdf.js not loaded");
  window.pdfjsLib.GlobalWorkerOptions.workerSrc =
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  const loadingTask = window.pdfjsLib.getDocument({ data: pdfBytes });
  const pdf = await loadingTask.promise;
  const numPages = Math.min(pdf.numPages, maxPages);
  const pages = [];
  for (let p = 1; p <= numPages; p++) {
    const page = await pdf.getPage(p);
    const viewport = page.getViewport({ scale: 1.8 });
    const canvas = document.createElement("canvas");
    canvas.width  = viewport.width;
    canvas.height = viewport.height;
    const ctx = canvas.getContext("2d");
    await page.render({ canvasContext: ctx, viewport }).promise;
    const b64 = canvas.toDataURL("image/png").split(",")[1];
    pages.push(b64);
  }
  return pages;
}

// ─── Claude vision extraction ─────────────────────────────────────────────────
const EXTRACTION_PROMPT = `You are extracting financial data from audited financial statements prepared by a CPA firm for a trucking/delivery company.

Extract ALL of the following from the PDF pages provided. Return ONLY valid JSON, no markdown, no explanation.

{
  "period": "YYYY-MM",
  "report_date": "YYYY-MM-DD or null",
  "company": "company name or null",
  "pl": {
    "revenue": number,
    "cost_of_goods_sold": number,
    "gross_profit": number,
    "operating_expenses": number,
    "operating_income": number,
    "other_income": number,
    "other_expenses": number,
    "net_income": number,
    "line_items": [{"label": "...", "amount": number, "section": "revenue|cogs|operating_expense|other"}]
  },
  "balance_sheet": {
    "total_assets": number,
    "current_assets": number,
    "fixed_assets": number,
    "total_liabilities": number,
    "current_liabilities": number,
    "long_term_liabilities": number,
    "equity": number,
    "line_items": [{"label": "...", "amount": number, "section": "current_asset|fixed_asset|current_liability|long_term_liability|equity"}]
  },
  "cash_flow": {
    "operating": number,
    "investing": number,
    "financing": number,
    "net_change": number,
    "beginning_cash": number,
    "ending_cash": number
  },
  "notes": "any important notes or caveats from the document"
}

Rules:
- All amounts in dollars as plain numbers (no $ signs, no commas)
- Expenses are POSITIVE numbers (don't negate them)
- If a statement type is not present in the PDF, set its value to null
- If a line item label is not clear, use your best judgment
- period should be the month these statements cover (YYYY-MM format)`;

async function extractFinancials(pages) {
  const imageBlocks = pages.map(b64 => ({
    type: "image",
    source: { type: "base64", media_type: "image/png", data: b64 },
  }));
  const resp = await fetch("/.netlify/functions/marginiq-scan-financials", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      max_tokens: 8192,
      messages: [{ role: "user", content: [...imageBlocks, { type: "text", text: EXTRACTION_PROMPT }] }],
    }),
  });
  if (!resp.ok) throw new Error(`Vision API error: ${resp.status}`);
  const data = await resp.json();
  const text = (data.content || []).map(b => b.text || "").join("").trim();
  const clean = text.replace(/```json|```/g, "").trim();
  return JSON.parse(clean);
}

// ─── Firestore helpers ────────────────────────────────────────────────────────
const db = () => window.db;

async function saveFinancial(monthKey, data) {
  try {
    await db().collection("audited_financials").doc(monthKey)
      .set({ ...data, period: monthKey, updated_at: new Date().toISOString() }, { merge: true });
    return true;
  } catch (e) { console.error("saveFinancial", e); return false; }
}

// Upload the original PDF to Firebase Storage so the user can view it later.
// Path: audited_financials/{period}.pdf  (one canonical copy per month;
// re-imports overwrite). Returns { storage_path, download_url } or null.
async function uploadPdfToStorage(period, pdfBytes, originalFilename) {
  if (!window.fbStorage) return null;
  try {
    const path = `audited_financials/${period}.pdf`;
    const ref = window.fbStorage.ref(path);
    const blob = new Blob([pdfBytes], { type: "application/pdf" });
    const metadata = {
      contentType: "application/pdf",
      customMetadata: { original_filename: originalFilename || "" },
    };
    await ref.put(blob, metadata);
    const url = await ref.getDownloadURL();
    return { storage_path: path, download_url: url };
  } catch (e) {
    console.error("uploadPdfToStorage failed:", e);
    return null;
  }
}

// Re-fetch the download URL for a stored PDF (URLs are short-lived tokens
// in some configs, so we always refresh just before opening).
async function refreshPdfUrl(storagePath) {
  if (!window.fbStorage || !storagePath) return null;
  try {
    return await window.fbStorage.ref(storagePath).getDownloadURL();
  } catch (e) { console.error("refreshPdfUrl failed:", e); return null; }
}

// FIXED v2.0.0: Don't use orderBy — if any doc lacks `period`, Firestore
// silently returns 0 results. Just .get() everything and sort client-side.
async function getFinancials() {
  try {
    const s = await db().collection("audited_financials").limit(120).get();
    const docs = s.docs.map(d => ({ id: d.id, ...d.data() }));
    // Fall back to doc id (which is the period key) if the period field is missing
    const withPeriod = docs.map(d => ({ ...d, period: d.period || d.id }));
    return withPeriod.sort((a, b) => (b.period || "").localeCompare(a.period || ""));
  } catch (e) {
    console.error("getFinancials failed:", e);
    return [];
  }
}

async function deleteFinancial(monthKey) {
  try {
    await db().collection("audited_financials").doc(monthKey).delete();
    return true;
  } catch (e) { console.error("deleteFinancial", e); return false; }
}

async function saveEmailProcessed(emailId, meta) {
  try {
    await db().collection("audited_financials_emails").doc(emailId)
      .set({ ...meta, processed_at: new Date().toISOString() }, { merge: true });
  } catch (e) {}
}

async function getProcessedEmailIds() {
  try {
    const s = await db().collection("audited_financials_emails").get();
    return new Set(s.docs.map(d => d.id));
  } catch (e) { return new Set(); }
}

// ─── Gmail helpers ────────────────────────────────────────────────────────────
async function searchCPAEmails() {
  const resp = await fetch("/.netlify/functions/marginiq-gmail-search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ vendor: "ampcpas", maxResults: 100 }),
  });
  if (!resp.ok) throw new Error(`Gmail search error: ${resp.status}`);
  const data = await resp.json();
  return data.results || [];
}

async function fetchAttachment(messageId, attachmentId, accountEmail, accountDocId) {
  const resp = await fetch("/.netlify/functions/marginiq-gmail-attachment", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ messageId, attachmentId, account_email: accountEmail, account_doc_id: accountDocId }),
  });
  if (!resp.ok) throw new Error(`Attachment fetch error: ${resp.status}`);
  const data = await resp.json();
  if (data.error) throw new Error(data.error);
  const bin = atob(data.data);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

// ─── Q&A backend call ─────────────────────────────────────────────────────────
async function askAI(question, financials, history) {
  const resp = await fetch("/.netlify/functions/marginiq-financials-qa", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, financials, history }),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || `QA API error: ${resp.status}`);
  return data.answer || "(empty response)";
}

// ─── UI atoms ─────────────────────────────────────────────────────────────────
const Badge = ({ text, color, bg }) => (
  <span style={{ padding:"2px 8px", borderRadius:20, fontSize:10, fontWeight:700, color:color||T.blueText, background:bg||T.blueBg, display:"inline-block" }}>{text}</span>
);
const Spinner = () => (
  <span style={{ display:"inline-block", animation:"spin 1s linear infinite", fontSize:16 }}>⏳</span>
);
const KPI = ({ label, value, sub, color, bg, trend }) => (
  <div style={{ ...card, flex:1, minWidth:160, background:bg||T.bgCard }}>
    <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.05em", marginBottom:4 }}>{label}</div>
    <div style={{ fontSize:22, fontWeight:800, color:color||T.text }}>{value}</div>
    {sub && <div style={{ fontSize:10, color:T.textDim, marginTop:2 }}>{sub}</div>}
    {trend && <div style={{ fontSize:11, marginTop:4, color: trend.startsWith("+")?T.green : trend.startsWith("-")?T.red : T.textMuted, fontWeight:600 }}>{trend}</div>}
  </div>
);

// ─── PL / BS / CF detail cards (from v1) ──────────────────────────────────────
function PLCard({ pl }) {
  if (!pl) return <div style={{ color:T.textMuted, fontSize:12 }}>No P&L data</div>;
  const margin = pl.revenue > 0 ? (pl.net_income / pl.revenue) * 100 : 0;
  const rows = [
    { label:"Revenue",            val:pl.revenue,            color:T.brand, bold:true },
    { label:"Cost of Goods Sold", val:-Math.abs(pl.cost_of_goods_sold||0), color:T.red },
    { label:"Gross Profit",       val:pl.gross_profit,       color:pl.gross_profit>=0?T.green:T.red, bold:true, border:true },
    { label:"Operating Expenses", val:-Math.abs(pl.operating_expenses||0), color:T.red },
    { label:"Operating Income",   val:pl.operating_income,   color:pl.operating_income>=0?T.green:T.red, bold:true, border:true },
    { label:"Other Income",       val:pl.other_income||0,    color:T.textMuted },
    { label:"Other Expenses",     val:-Math.abs(pl.other_expenses||0), color:T.textMuted },
    { label:"Net Income",         val:pl.net_income,         color:pl.net_income>=0?T.green:T.red, bold:true, border:true },
  ];
  return (
    <div>
      <div style={{ display:"flex", gap:12, marginBottom:12, flexWrap:"wrap" }}>
        <div style={{ ...card, flex:1, minWidth:140 }}>
          <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", marginBottom:4 }}>Revenue</div>
          <div style={{ fontSize:20, fontWeight:800, color:T.brand }}>{fmtK(pl.revenue)}</div>
        </div>
        <div style={{ ...card, flex:1, minWidth:140 }}>
          <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", marginBottom:4 }}>Net Income</div>
          <div style={{ fontSize:20, fontWeight:800, color:pl.net_income>=0?T.green:T.red }}>{fmtK(pl.net_income)}</div>
        </div>
        <div style={{ ...card, flex:1, minWidth:140 }}>
          <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", marginBottom:4 }}>Net Margin</div>
          <div style={{ fontSize:20, fontWeight:800, color:margin>=0?T.green:T.red }}>{fmtPct(margin)}</div>
        </div>
      </div>
      <div style={{ ...card, padding:0, overflow:"hidden" }}>
        {rows.map((r, i) => (
          <div key={i} style={{ display:"flex", justifyContent:"space-between", padding:"10px 16px", borderBottom:r.border?`2px solid ${T.border}`:`1px solid ${T.borderLight}`, fontWeight:r.bold?700:400 }}>
            <span style={{ fontSize:12, color:T.text }}>{r.label}</span>
            <span style={{ fontSize:12, color:r.color, fontVariantNumeric:"tabular-nums" }}>{fmt(r.val)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function BSCard({ bs }) {
  if (!bs) return <div style={{ color:T.textMuted, fontSize:12 }}>No balance sheet data</div>;
  return (
    <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:16 }}>
      <div style={{ ...card, padding:0, overflow:"hidden" }}>
        <div style={{ padding:"10px 16px", fontWeight:700, fontSize:12, background:T.bgSurface, borderBottom:`1px solid ${T.border}` }}>Assets</div>
        {[
          { label:"Current Assets",  val:bs.current_assets, color:T.brand },
          { label:"Fixed Assets",    val:bs.fixed_assets,   color:T.brand },
          { label:"Total Assets",    val:bs.total_assets,   color:T.brand, bold:true, border:true },
        ].map((r,i) => (
          <div key={i} style={{ display:"flex", justifyContent:"space-between", padding:"10px 16px", borderBottom:r.border?`2px solid ${T.border}`:`1px solid ${T.borderLight}`, fontWeight:r.bold?700:400 }}>
            <span style={{ fontSize:12, color:T.text }}>{r.label}</span>
            <span style={{ fontSize:12, color:r.color||T.text, fontVariantNumeric:"tabular-nums" }}>{fmt(r.val)}</span>
          </div>
        ))}
      </div>
      <div style={{ ...card, padding:0, overflow:"hidden" }}>
        <div style={{ padding:"10px 16px", fontWeight:700, fontSize:12, background:T.bgSurface, borderBottom:`1px solid ${T.border}` }}>Liabilities & Equity</div>
        {[
          { label:"Current Liabilities",    val:bs.current_liabilities,    color:T.red },
          { label:"Long-Term Liabilities",  val:bs.long_term_liabilities,  color:T.red },
          { label:"Total Liabilities",      val:bs.total_liabilities,      color:T.red, bold:true, border:true },
          { label:"Equity",                 val:bs.equity,                 color:T.green, bold:true },
        ].map((r,i) => (
          <div key={i} style={{ display:"flex", justifyContent:"space-between", padding:"10px 16px", borderBottom:r.border?`2px solid ${T.border}`:`1px solid ${T.borderLight}`, fontWeight:r.bold?700:400 }}>
            <span style={{ fontSize:12, color:T.text }}>{r.label}</span>
            <span style={{ fontSize:12, color:r.color||T.text, fontVariantNumeric:"tabular-nums" }}>{fmt(r.val)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function CFCard({ cf }) {
  if (!cf) return <div style={{ color:T.textMuted, fontSize:12 }}>No cash flow data</div>;
  return (
    <div style={{ ...card, padding:0, overflow:"hidden" }}>
      {[
        { label:"Operating Activities",  val:cf.operating,  color:cf.operating>=0?T.green:T.red },
        { label:"Investing Activities",  val:cf.investing,  color:cf.investing>=0?T.green:T.red },
        { label:"Financing Activities",  val:cf.financing,  color:cf.financing>=0?T.green:T.red },
        { label:"Net Change in Cash",    val:cf.net_change, color:cf.net_change>=0?T.green:T.red, bold:true, border:true },
        { label:"Beginning Cash",        val:cf.beginning_cash, color:T.textMuted },
        { label:"Ending Cash",           val:cf.ending_cash,    color:T.brand, bold:true },
      ].map((r,i) => (
        <div key={i} style={{ display:"flex", justifyContent:"space-between", padding:"10px 16px", borderBottom:r.border?`2px solid ${T.border}`:`1px solid ${T.borderLight}`, fontWeight:r.bold?700:400 }}>
          <span style={{ fontSize:12, color:T.text }}>{r.label}</span>
          <span style={{ fontSize:12, color:r.color||T.text, fontVariantNumeric:"tabular-nums" }}>{fmt(r.val)}</span>
        </div>
      ))}
    </div>
  );
}

// ─── Detail Modal ─────────────────────────────────────────────────────────────
function DetailModal({ record, onClose, onDelete }) {
  if (!record) return null;
  return (
    <div style={{ position:"fixed", inset:0, background:"rgba(0,0,0,0.5)", zIndex:1000, display:"flex", alignItems:"center", justifyContent:"center", padding:20 }} onClick={onClose}>
      <div style={{ background:T.bgWhite, borderRadius:T.radius, padding:24, maxWidth:900, width:"100%", maxHeight:"90vh", overflowY:"auto" }} onClick={e => e.stopPropagation()}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:20 }}>
          <div style={{ fontSize:16, fontWeight:800 }}>📋 {monthLabel(record.period)} — Financials</div>
          <div style={{ display:"flex", gap:8 }}>
            <button onClick={() => onDelete(record)} style={{ background:T.redBg, border:`1px solid ${T.red}`, color:T.red, padding:"6px 12px", borderRadius:T.radiusSm, fontSize:11, fontWeight:600, cursor:"pointer" }}>Delete</button>
            <button onClick={onClose} style={{ background:"none", border:"none", fontSize:20, cursor:"pointer", color:T.textMuted }}>✕</button>
          </div>
        </div>
        <div style={{ marginBottom:20 }}>
          <div style={{ fontSize:13, fontWeight:700, marginBottom:12 }}>📈 Profit & Loss</div>
          <PLCard pl={record.pl} />
        </div>
        <div style={{ marginBottom:20 }}>
          <div style={{ fontSize:13, fontWeight:700, marginBottom:12 }}>🏦 Balance Sheet</div>
          <BSCard bs={record.balance_sheet} />
        </div>
        <div style={{ marginBottom:20 }}>
          <div style={{ fontSize:13, fontWeight:700, marginBottom:12 }}>💵 Cash Flow</div>
          <CFCard cf={record.cash_flow} />
        </div>
        {record.notes && (
          <div style={{ ...card, background:T.yellowBg, borderColor:T.yellow }}>
            <div style={{ fontSize:11, fontWeight:700, color:T.yellowText, marginBottom:4 }}>📝 CPA Notes</div>
            <div style={{ fontSize:12, color:T.text, lineHeight:1.6 }}>{record.notes}</div>
          </div>
        )}
        {record.pdf_storage_path && (
          <div style={{ marginTop:18, padding:"12px 14px", background:T.brandPale, borderRadius:T.radiusSm, display:"flex", alignItems:"center", justifyContent:"space-between" }}>
            <div>
              <div style={{ fontSize:12, fontWeight:700, color:T.brandDark }}>📎 Original PDF available</div>
              <div style={{ fontSize:10, color:T.textMuted, marginTop:2 }}>{record.filename || "audited financials"}</div>
            </div>
            <button onClick={async () => {
              const url = await refreshPdfUrl(record.pdf_storage_path);
              if (url) window.open(url, "_blank", "noopener,noreferrer");
              else window.alert("Could not open PDF — check console");
            }}
              style={{ padding:"8px 16px", borderRadius:T.radiusSm, border:"none", background:T.brand, color:"#fff", fontSize:12, fontWeight:700, cursor:"pointer" }}>
              View Original PDF
            </button>
          </div>
        )}
        {record.filename && !record.pdf_storage_path && (
          <div style={{ marginTop:14, fontSize:10, color:T.textDim }}>
            Source: {record.filename}{record.email_subject ? ` · "${record.email_subject}"` : ""}
            <div style={{ marginTop:4, color:T.yellowText }}>⚠️ PDF not yet stored — re-import this month from Gmail Sync to keep a viewable copy.</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Mini sparkline / trend chart ─────────────────────────────────────────────
function MiniLineChart({ data, height=180, colors={ revenue:T.brand, expenses:T.red, netIncome:T.green } }) {
  if (!data || data.length < 2) return <div style={{ padding:"20px", textAlign:"center", color:T.textDim, fontSize:11 }}>Need at least 2 months for a trend chart</div>;

  const W = 720, H = height, pad = { l:60, r:120, t:14, b:32 };
  const innerW = W - pad.l - pad.r, innerH = H - pad.t - pad.b;
  const allVals = data.flatMap(d => [d.revenue||0, d.expenses||0, d.netIncome||0].filter(v => v != null));
  const max = Math.max(...allVals, 1);
  const min = Math.min(...allVals, 0);
  const range = max - min || 1;
  const xAt = i => pad.l + (data.length<2?0:(i/(data.length-1))*innerW);
  const yAt = v => pad.t + innerH - ((v-min)/range)*innerH;
  const series = [
    { key:"revenue",   label:"Revenue",    color:colors.revenue   },
    { key:"expenses",  label:"Expenses",   color:colors.expenses  },
    { key:"netIncome", label:"Net Income", color:colors.netIncome },
  ];
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display:"block" }}>
      {/* Y gridlines */}
      {[0, 0.25, 0.5, 0.75, 1].map(p => (
        <g key={p}>
          <line x1={pad.l} y1={pad.t+innerH-p*innerH} x2={pad.l+innerW} y2={pad.t+innerH-p*innerH} stroke={T.borderLight} strokeDasharray="2,4" />
          <text x={pad.l-8} y={pad.t+innerH-p*innerH+3} fontSize="9" fill={T.textDim} textAnchor="end">{fmtK(min+range*p)}</text>
        </g>
      ))}
      {/* X labels */}
      {data.map((d, i) => {
        const skip = Math.ceil(data.length/8);
        if (i%skip!==0 && i!==data.length-1) return null;
        const lbl = monthLabel(d.period);
        const short = lbl.replace(/(\w{3})\w* (\d{4})/, "$1 '$2".slice(0,4)+"'$2".slice(-2));
        return <text key={i} x={xAt(i)} y={H-pad.b+16} fontSize="9" fill={T.textDim} textAnchor="middle">{lbl.replace(/(\d{4})/, "'$1".slice(-3))}</text>;
      })}
      {/* Lines */}
      {series.map(s => {
        const path = data.map((d,i) => `${i===0?"M":"L"} ${xAt(i)} ${yAt(d[s.key]||0)}`).join(" ");
        return (
          <g key={s.key}>
            <path d={path} stroke={s.color} strokeWidth="2.5" fill="none" />
            {data.map((d,i) => <circle key={i} cx={xAt(i)} cy={yAt(d[s.key]||0)} r="3" fill={s.color} />)}
          </g>
        );
      })}
      {/* Legend */}
      {series.map((s, i) => (
        <g key={s.key} transform={`translate(${pad.l+innerW+10}, ${pad.t+i*22})`}>
          <rect width="12" height="3" fill={s.color} y="3" rx="1" />
          <text x="18" y="9" fontSize="11" fill={T.text}>{s.label}</text>
        </g>
      ))}
    </svg>
  );
}

// ─── Dashboard view ───────────────────────────────────────────────────────────
// ─── CFO Dashboard ───────────────────────────────────────────────────────────
// Analytical dashboard intended for executive review of monthly audited
// financials. Strict mode: only successfully-extracted records (those with
// a parsed pl object) appear here. Records still pending or failed
// extraction live in the Statements list with "Re-extract" affordances.
//
// Layout sections (top to bottom):
//   1. Period selector + Compare-vs selector
//   2. Executive KPI strip with sparklines
//   3. Revenue · Net Income trend chart (24mo or all available)
//   4. Auto-generated Strengths / Flags panel
//   5. Expense category waterfall (top movers month-over-month)
//   6. Full statement index (clickable)

function Dashboard({ records, onSelect }) {
  const [windowKey, setWindowKey] = useState("ttm"); // ttm | ytd | last1 | last3 | last6 | all
  const [compareKey, setCompareKey] = useState("prior"); // prior | yoy

  // STRICT mode: only records with a parsed pl object are dashboard-eligible
  const extracted = records.filter(r => r.period && r.pl && Object.keys(r.pl).length > 0);
  if (!extracted.length) return (
    <div style={{ ...card, textAlign:"center", padding:"60px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:8 }}>📊</div>
      <div style={{ fontSize:13, fontWeight:600 }}>No extracted financials yet</div>
      <div style={{ fontSize:11, marginTop:6 }}>Open the 📧 Gmail Sync tab → AMP CPAs vendor card to import statements.</div>
      <div style={{ fontSize:10, marginTop:4, color:T.textDim }}>The dashboard shows only fully-extracted months. Pending/failed extractions appear in the Statements tab.</div>
    </div>
  );

  // Sort ascending so [last] is most recent
  const asc = [...extracted].sort((a,b) => a.period.localeCompare(b.period));
  const latest = asc[asc.length - 1];

  // ── Window selection ─────────────────────────────────────────────────────
  // Returns { current: [], prior: [], label: string, priorLabel: string }
  const selectWindows = () => {
    const n = asc.length;
    const periodToDate = (p) => {
      const [y, m] = p.split("-").map(x => parseInt(x));
      return new Date(y, m - 1, 1);
    };
    const subtractMonths = (date, months) => {
      const d = new Date(date);
      d.setMonth(d.getMonth() - months);
      return d;
    };
    const periodOf = (date) => `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,"0")}`;
    const inWindow = (p, start, end) => p >= start && p <= end;

    const latestDate = periodToDate(latest.period);

    if (windowKey === "ttm") {
      const current = asc.slice(-12);
      const priorEnd = periodOf(subtractMonths(latestDate, 12));
      const priorStart = periodOf(subtractMonths(latestDate, 23));
      const prior = asc.filter(r => inWindow(r.period, priorStart, priorEnd));
      return { current, prior, label:"Trailing 12 Months", priorLabel: compareKey === "yoy" ? "Prior 12 Months" : "Previous TTM" };
    }
    if (windowKey === "ytd") {
      const yr = latest.period.slice(0,4);
      const current = asc.filter(r => r.period.startsWith(yr));
      const priorYr = String(parseInt(yr) - 1);
      const lastMonthNum = parseInt(latest.period.slice(5,7));
      const prior = asc.filter(r => {
        if (!r.period.startsWith(priorYr)) return false;
        return parseInt(r.period.slice(5,7)) <= lastMonthNum;
      });
      return { current, prior, label:`${yr} YTD (${current.length} mo)`, priorLabel:`${priorYr} same period` };
    }
    if (windowKey === "last1") {
      const current = asc.slice(-1);
      const prior = compareKey === "yoy"
        ? asc.filter(r => r.period === periodOf(subtractMonths(latestDate, 12)))
        : asc.slice(-2, -1);
      return { current, prior, label: monthLabel(latest.period), priorLabel: compareKey === "yoy" ? "Same month last year" : "Prior month" };
    }
    if (windowKey === "last3") {
      const current = asc.slice(-3);
      const prior = asc.slice(-6, -3);
      return { current, prior, label:"Last 3 Months", priorLabel:"Prior 3 Months" };
    }
    if (windowKey === "last6") {
      const current = asc.slice(-6);
      const prior = asc.slice(-12, -6);
      return { current, prior, label:"Last 6 Months", priorLabel:"Prior 6 Months" };
    }
    // all
    return { current: asc, prior: [], label: `All (${n} months)`, priorLabel: "" };
  };

  const { current, prior, label: windowLabel, priorLabel } = selectWindows();

  // ── Aggregations ─────────────────────────────────────────────────────────
  const sumPl = (recs, key) => recs.reduce((s, r) => s + (r.pl?.[key] || 0), 0);
  const avgMargin = (recs) => {
    const valid = recs.filter(r => r.pl?.revenue);
    if (!valid.length) return 0;
    return valid.reduce((s, r) => s + ((r.pl.net_income || 0) / r.pl.revenue) * 100, 0) / valid.length;
  };
  const ebitda = (recs) => recs.reduce((s, r) => {
    const pl = r.pl || {};
    // Approximation: operating_income + depreciation/amortization from line items.
    // Most CPA P&Ls separate these as line items under operating expenses.
    const opInc = pl.operating_income || 0;
    const depItems = (pl.line_items || []).filter(li => /deprec|amort/i.test(li.label || ""));
    const depAmort = depItems.reduce((acc, li) => acc + Math.abs(li.amount || 0), 0);
    return s + opInc + depAmort;
  }, 0);
  const grossMarginPct = (recs) => {
    const rev = sumPl(recs, "revenue");
    const cogs = sumPl(recs, "cost_of_goods_sold");
    return rev > 0 ? ((rev - cogs) / rev) * 100 : 0;
  };
  const opMarginPct = (recs) => {
    const rev = sumPl(recs, "revenue");
    const opInc = sumPl(recs, "operating_income");
    return rev > 0 ? (opInc / rev) * 100 : 0;
  };

  const cur = {
    revenue: sumPl(current, "revenue"),
    cogs: sumPl(current, "cost_of_goods_sold"),
    grossProfit: sumPl(current, "gross_profit"),
    opEx: sumPl(current, "operating_expenses"),
    opInc: sumPl(current, "operating_income"),
    netIncome: sumPl(current, "net_income"),
    ebitda: ebitda(current),
    grossMargin: grossMarginPct(current),
    opMargin: opMarginPct(current),
    netMargin: avgMargin(current),
  };
  const pre = {
    revenue: sumPl(prior, "revenue"),
    netIncome: sumPl(prior, "net_income"),
    grossProfit: sumPl(prior, "gross_profit"),
    opEx: sumPl(prior, "operating_expenses"),
    opInc: sumPl(prior, "operating_income"),
    ebitda: ebitda(prior),
    grossMargin: grossMarginPct(prior),
    opMargin: opMarginPct(prior),
  };

  const pctDelta = (curV, preV) => {
    if (!prior.length || preV == null || preV === 0) return null;
    const d = ((curV - preV) / Math.abs(preV)) * 100;
    return (d >= 0 ? "+" : "") + d.toFixed(1) + "%";
  };
  const ppDelta = (curV, preV) => {
    if (!prior.length || preV == null) return null;
    const d = curV - preV;
    return (d >= 0 ? "+" : "") + d.toFixed(1) + "pp";
  };

  // ── Sparkline data: last 12 months of asc (for KPI cards) ────────────────
  const sparkSource = asc.slice(-12);
  const sparkRev = sparkSource.map(r => r.pl?.revenue || 0);
  const sparkNet = sparkSource.map(r => r.pl?.net_income || 0);
  const sparkGM  = sparkSource.map(r => r.pl?.revenue ? ((r.pl.revenue - (r.pl.cost_of_goods_sold||0)) / r.pl.revenue) * 100 : 0);
  const sparkOM  = sparkSource.map(r => r.pl?.revenue ? ((r.pl.operating_income||0) / r.pl.revenue) * 100 : 0);
  const sparkEB  = sparkSource.map(r => {
    const pl = r.pl || {};
    const dep = (pl.line_items || []).filter(li => /deprec|amort/i.test(li.label||"")).reduce((s,li) => s + Math.abs(li.amount||0), 0);
    return (pl.operating_income || 0) + dep;
  });

  // ── Trend chart data: last 24 months ─────────────────────────────────────
  const trendSource = asc.slice(-24);
  const trendData = trendSource.map(r => ({
    period: r.period,
    revenue: r.pl?.revenue || 0,
    expenses: (r.pl?.cost_of_goods_sold || 0) + (r.pl?.operating_expenses || 0),
    netIncome: r.pl?.net_income || 0,
  }));

  // ── Auto-generated insights (Strengths / Flags) ──────────────────────────
  const insights = generateInsights(asc, current, prior, cur, pre, prior.length > 0);

  // ── Expense category analysis (top movers, latest month vs. prior) ───────
  const expenseMovers = computeExpenseMovers(asc);

  return (
    <div>
      {/* ── Period + Compare selectors ──────────────────────────────────── */}
      <div style={{ display:"flex", gap:12, marginBottom:16, flexWrap:"wrap", alignItems:"center" }}>
        <div style={{ fontSize:11, fontWeight:700, color:T.textMuted, textTransform:"uppercase" }}>Period</div>
        <SegmentedControl
          options={[
            { key:"ttm",   label:"TTM" },
            { key:"ytd",   label:"YTD" },
            { key:"last1", label:"Last Month" },
            { key:"last3", label:"3 mo" },
            { key:"last6", label:"6 mo" },
            { key:"all",   label:"All" },
          ]}
          value={windowKey}
          onChange={setWindowKey}
        />
        <div style={{ width:1, height:24, background:T.border, margin:"0 4px" }} />
        <div style={{ fontSize:11, fontWeight:700, color:T.textMuted, textTransform:"uppercase" }}>Compare vs</div>
        <SegmentedControl
          options={[
            { key:"prior", label:"Prior period" },
            { key:"yoy",   label:"Prior year" },
          ]}
          value={compareKey}
          onChange={setCompareKey}
        />
      </div>

      {/* ── Executive KPI strip ─────────────────────────────────────────── */}
      <div style={{ marginBottom:24 }}>
        <div style={{ display:"flex", alignItems:"baseline", justifyContent:"space-between", marginBottom:10 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.textMuted, textTransform:"uppercase", letterSpacing:"0.06em" }}>
            Executive Summary — {windowLabel}
          </div>
          {priorLabel && <div style={{ fontSize:10, color:T.textDim }}>vs. {priorLabel}</div>}
        </div>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(200px, 1fr))", gap:12 }}>
          <KPICard label="Revenue"
                   value={fmtK(cur.revenue)}
                   delta={pctDelta(cur.revenue, pre.revenue)}
                   spark={sparkRev}
                   sparkColor={T.brand} />
          <KPICard label="Net Income"
                   value={fmtK(cur.netIncome)}
                   valueColor={cur.netIncome >= 0 ? T.green : T.red}
                   delta={pctDelta(cur.netIncome, pre.netIncome)}
                   spark={sparkNet}
                   sparkColor={cur.netIncome >= 0 ? T.green : T.red} />
          <KPICard label="Gross Margin"
                   value={fmtPct(cur.grossMargin)}
                   delta={ppDelta(cur.grossMargin, pre.grossMargin)}
                   spark={sparkGM}
                   sparkColor={T.purple} />
          <KPICard label="Operating Margin"
                   value={fmtPct(cur.opMargin)}
                   delta={ppDelta(cur.opMargin, pre.opMargin)}
                   spark={sparkOM}
                   sparkColor={T.brand} />
          <KPICard label="EBITDA"
                   value={fmtK(cur.ebitda)}
                   delta={pctDelta(cur.ebitda, pre.ebitda)}
                   spark={sparkEB}
                   sparkColor={cur.ebitda >= 0 ? T.green : T.red} />
        </div>
      </div>

      {/* ── Revenue & profit trend chart ────────────────────────────────── */}
      <div style={{ ...card, marginBottom:24 }}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"baseline", marginBottom:10 }}>
          <div style={{ fontSize:13, fontWeight:700 }}>📈 Revenue · Expenses · Net Income</div>
          <div style={{ fontSize:10, color:T.textDim }}>{trendData.length} months</div>
        </div>
        <MiniLineChart data={trendData} height={240} />
      </div>

      {/* ── Insights panel ──────────────────────────────────────────────── */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(280px, 1fr))", gap:12, marginBottom:24 }}>
        <InsightCard title="🟢 Strengths" items={insights.strengths} emptyText="No notable strengths to flag." color={T.green} bg={T.greenBg} />
        <InsightCard title="⚠️ Flags"     items={insights.flags}     emptyText="No flags — financials look clean." color={T.yellowText} bg={T.yellowBg} />
      </div>

      {/* ── Expense category movers ─────────────────────────────────────── */}
      {expenseMovers.length > 0 && (
        <div style={{ ...card, marginBottom:24 }}>
          <div style={{ fontSize:13, fontWeight:700, marginBottom:4 }}>💸 Expense Movers — {monthLabel(latest.period)} vs. Prior Month</div>
          <div style={{ fontSize:10, color:T.textDim, marginBottom:12 }}>Largest changes by category (categories appearing in both months only)</div>
          <ExpenseMoversTable rows={expenseMovers} />
        </div>
      )}

      {/* ── Full period index ───────────────────────────────────────────── */}
      <div style={{ ...card, padding:0, overflow:"auto" }}>
        <div style={{ padding:"12px 16px", fontSize:13, fontWeight:700, borderBottom:`1px solid ${T.border}` }}>
          📋 All Periods <span style={{ fontSize:10, fontWeight:500, color:T.textDim, marginLeft:6 }}>{asc.length} extracted</span>
        </div>
        <table style={{ width:"100%", borderCollapse:"collapse" }}>
          <thead>
            <tr>
              <th style={tblH}>Period</th>
              <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
              <th style={{ ...tblH, textAlign:"right" }}>Expenses</th>
              <th style={{ ...tblH, textAlign:"right" }}>Net Income</th>
              <th style={{ ...tblH, textAlign:"right" }}>Net Margin</th>
              <th style={{ ...tblH, textAlign:"right" }}>Cash</th>
              <th style={tblH}></th>
            </tr>
          </thead>
          <tbody>
            {[...asc].reverse().map(r => {
              const pl = r.pl || {};
              const exp = (pl.cost_of_goods_sold||0) + (pl.operating_expenses||0);
              const margin = pl.revenue ? (pl.net_income/pl.revenue)*100 : 0;
              return (
                <tr key={r.id || r.period} style={{ cursor:"pointer" }} onClick={() => onSelect(r)}>
                  <td style={{ ...tblD, fontWeight:600 }}>{monthLabel(r.period)}</td>
                  <td style={tblDR}>{fmt(pl.revenue)}</td>
                  <td style={{ ...tblDR, color:T.red }}>{fmt(exp)}</td>
                  <td style={{ ...tblDR, fontWeight:700, color:(pl.net_income||0)>=0?T.green:T.red }}>{fmt(pl.net_income)}</td>
                  <td style={{ ...tblDR, color:margin>=0?T.greenText:T.redText }}>{fmtPct(margin)}</td>
                  <td style={{ ...tblDR, color:T.purple }}>{fmt(r.cash_flow?.ending_cash)}</td>
                  <td style={tblD}><span style={{ fontSize:11, color:T.brand, fontWeight:600 }}>View →</span></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Dashboard support components ────────────────────────────────────────────

function SegmentedControl({ options, value, onChange }) {
  return (
    <div style={{ display:"inline-flex", background:T.bgSurface, border:`1px solid ${T.border}`, borderRadius:8, padding:2, gap:2 }}>
      {options.map(o => (
        <button key={o.key} onClick={() => onChange(o.key)}
                style={{
                  padding:"5px 10px", fontSize:11, fontWeight:600,
                  border:"none", borderRadius:6, cursor:"pointer",
                  background: value === o.key ? T.bgWhite : "transparent",
                  color: value === o.key ? T.text : T.textMuted,
                  boxShadow: value === o.key ? T.shadow : "none",
                }}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

function KPICard({ label, value, valueColor, delta, spark, sparkColor }) {
  const deltaColor = !delta ? T.textMuted
                   : delta.startsWith("+") ? T.green
                   : delta.startsWith("-") ? T.red
                   : T.textMuted;
  return (
    <div style={{ ...card, padding:"14px 16px", display:"flex", flexDirection:"column", gap:6 }}>
      <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.05em" }}>{label}</div>
      <div style={{ fontSize:24, fontWeight:800, color: valueColor || T.text, fontVariantNumeric:"tabular-nums" }}>{value}</div>
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", gap:8 }}>
        <div style={{ fontSize:11, fontWeight:600, color: deltaColor }}>
          {delta || <span style={{ color:T.textDim, fontWeight:400 }}>—</span>}
        </div>
        {spark && spark.length > 1 && <Sparkline values={spark} color={sparkColor || T.brand} width={70} height={22} />}
      </div>
    </div>
  );
}

function Sparkline({ values, color = "#1e5b92", width = 80, height = 24 }) {
  if (!values || values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const points = values.map((v, i) => {
    const x = (i / (values.length - 1)) * width;
    const y = height - ((v - min) / range) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <svg width={width} height={height} style={{ display:"block" }}>
      <polyline fill="none" stroke={color} strokeWidth={1.5} points={points} />
      <circle cx={width} cy={height - ((values[values.length-1] - min) / range) * height} r={2} fill={color} />
    </svg>
  );
}

function InsightCard({ title, items, emptyText, color, bg }) {
  return (
    <div style={{ ...card, background: bg || T.bgCard, borderColor: color, padding:"14px 16px" }}>
      <div style={{ fontSize:13, fontWeight:700, color: color, marginBottom:8 }}>{title}</div>
      {items.length === 0 ? (
        <div style={{ fontSize:11, color:T.textMuted }}>{emptyText}</div>
      ) : (
        <ul style={{ margin:0, padding:0, listStyle:"none", display:"flex", flexDirection:"column", gap:6 }}>
          {items.map((s, i) => (
            <li key={i} style={{ fontSize:12, color:T.text, lineHeight:1.4, paddingLeft:14, position:"relative" }}>
              <span style={{ position:"absolute", left:0, color:color }}>•</span>
              {s}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function ExpenseMoversTable({ rows }) {
  return (
    <table style={{ width:"100%", borderCollapse:"collapse" }}>
      <thead>
        <tr>
          <th style={tblH}>Category</th>
          <th style={{ ...tblH, textAlign:"right" }}>Latest</th>
          <th style={{ ...tblH, textAlign:"right" }}>Prior</th>
          <th style={{ ...tblH, textAlign:"right" }}>Δ $</th>
          <th style={{ ...tblH, textAlign:"right" }}>Δ %</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            <td style={{ ...tblD, fontWeight:600 }}>{r.label}</td>
            <td style={tblDR}>{fmt(r.latest)}</td>
            <td style={{ ...tblDR, color:T.textMuted }}>{fmt(r.prior)}</td>
            <td style={{ ...tblDR, color: r.deltaAbs >= 0 ? T.red : T.green, fontWeight:700 }}>
              {(r.deltaAbs >= 0 ? "+" : "") + fmt(r.deltaAbs)}
            </td>
            <td style={{ ...tblDR, color: r.deltaPct >= 0 ? T.red : T.green }}>
              {r.deltaPct === Infinity ? "(new)" : (r.deltaPct >= 0 ? "+" : "") + r.deltaPct.toFixed(1) + "%"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ─── Insight generator ───────────────────────────────────────────────────────

function generateInsights(asc, current, prior, cur, pre, hasPrior) {
  const strengths = [];
  const flags = [];

  if (hasPrior && pre.revenue > 0) {
    const revGrowth = ((cur.revenue - pre.revenue) / pre.revenue) * 100;
    if (revGrowth >= 10) strengths.push(`Revenue up ${revGrowth.toFixed(1)}% vs. prior period`);
    else if (revGrowth <= -10) flags.push(`Revenue down ${Math.abs(revGrowth).toFixed(1)}% vs. prior period`);
  }

  if (hasPrior && pre.netIncome != null) {
    if (cur.netIncome > 0 && pre.netIncome <= 0) strengths.push("Returned to profitability this period");
    else if (cur.netIncome <= 0 && pre.netIncome > 0) flags.push("Net loss this period after profit in prior period");
  }

  // OpEx growing faster than revenue
  if (hasPrior && pre.revenue > 0 && pre.opEx > 0) {
    const revG = ((cur.revenue - pre.revenue) / pre.revenue) * 100;
    const opG  = ((cur.opEx    - pre.opEx)    / pre.opEx)    * 100;
    if (opG > revG + 5 && opG > 0) flags.push(`Operating expenses growing ${opG.toFixed(1)}% — faster than revenue (${revG.toFixed(1)}%)`);
  }

  // Margin compression
  if (hasPrior && pre.grossMargin > 0) {
    const marginDrop = pre.grossMargin - cur.grossMargin;
    if (marginDrop >= 3) flags.push(`Gross margin compressed ${marginDrop.toFixed(1)}pp (${pre.grossMargin.toFixed(1)}% → ${cur.grossMargin.toFixed(1)}%)`);
    else if (marginDrop <= -3) strengths.push(`Gross margin improved ${Math.abs(marginDrop).toFixed(1)}pp (${pre.grossMargin.toFixed(1)}% → ${cur.grossMargin.toFixed(1)}%)`);
  }

  // Cash trend (if balance sheet present)
  const latestCash = current[current.length-1]?.cash_flow?.ending_cash;
  const earliestCash = current[0]?.cash_flow?.ending_cash;
  if (latestCash != null && earliestCash != null && current.length >= 3) {
    const cashDelta = latestCash - earliestCash;
    if (cashDelta < -0.05 * Math.abs(earliestCash) && Math.abs(cashDelta) > 5000) {
      flags.push(`Cash balance down ${fmtK(Math.abs(cashDelta))} since start of window`);
    } else if (cashDelta > 0.10 * Math.abs(earliestCash) && cashDelta > 5000) {
      strengths.push(`Cash position improved by ${fmtK(cashDelta)} over window`);
    }
  }

  // Consecutive losing months
  let losingStreak = 0;
  for (let i = asc.length - 1; i >= 0; i--) {
    if ((asc[i].pl?.net_income || 0) < 0) losingStreak++;
    else break;
  }
  if (losingStreak >= 3) flags.push(`Net loss for ${losingStreak} consecutive months`);

  // Best month in window
  const bestNet = current.reduce((b, r) => (r.pl?.net_income || -Infinity) > (b.pl?.net_income || -Infinity) ? r : b, current[0]);
  if (bestNet?.pl?.net_income > 0) {
    strengths.push(`Best month: ${monthLabel(bestNet.period)} — ${fmtK(bestNet.pl.net_income)} net income`);
  }

  return { strengths, flags };
}

// Top expense category movers — latest vs. prior month, line-item level
function computeExpenseMovers(asc) {
  if (asc.length < 2) return [];
  const latest = asc[asc.length - 1];
  const prior  = asc[asc.length - 2];
  const latestItems = (latest.pl?.line_items || []).filter(li =>
    li.section === "operating_expense" || li.section === "cogs" || /expense|cost/i.test(li.label || ""));
  const priorItems = (prior.pl?.line_items || []).filter(li =>
    li.section === "operating_expense" || li.section === "cogs" || /expense|cost/i.test(li.label || ""));
  const map = {};
  for (const li of latestItems) {
    const key = (li.label || "").trim().toLowerCase();
    if (!key) continue;
    map[key] = { label: li.label, latest: Math.abs(li.amount || 0), prior: 0 };
  }
  for (const li of priorItems) {
    const key = (li.label || "").trim().toLowerCase();
    if (!key) continue;
    if (map[key]) map[key].prior = Math.abs(li.amount || 0);
    else map[key] = { label: li.label, latest: 0, prior: Math.abs(li.amount || 0) };
  }
  const rows = Object.values(map).map(r => ({
    label: r.label,
    latest: r.latest,
    prior: r.prior,
    deltaAbs: r.latest - r.prior,
    deltaPct: r.prior > 0 ? ((r.latest - r.prior) / r.prior) * 100 : (r.latest > 0 ? Infinity : 0),
  }));
  // Show only categories present in both periods, sort by absolute $ change desc, top 8
  return rows
    .filter(r => r.latest > 0 && r.prior > 0)
    .sort((a, b) => Math.abs(b.deltaAbs) - Math.abs(a.deltaAbs))
    .slice(0, 8);
}

// ─── AI Chat view ─────────────────────────────────────────────────────────────
const SUGGESTED_QUESTIONS = [
  "What was my best month for net income?",
  "How did my operating expenses change month over month?",
  "What's my average net margin over the past year?",
  "Which expenses are growing fastest?",
  "Summarize my financial health right now",
  "Compare my latest month to the same month last year",
];

function AIChat({ records }) {
  const [history, setHistory] = useState([]); // [{role, content}, ...]
  const [input, setInput]     = useState("");
  const [busy, setBusy]       = useState(false);
  const [err, setErr]         = useState("");
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [history, busy]);

  const send = async (q) => {
    const question = (q || input).trim();
    if (!question || busy) return;
    setErr(""); setInput("");
    const newHist = [...history, { role:"user", content: question }];
    setHistory(newHist);
    setBusy(true);
    try {
      const answer = await askAI(question, records, history);
      setHistory(h => [...h, { role:"assistant", content: answer }]);
    } catch(e) {
      setErr(e.message);
      setHistory(h => [...h, { role:"assistant", content: `⚠️ Error: ${e.message}` }]);
    }
    setBusy(false);
  };

  return (
    <div style={{ display:"flex", flexDirection:"column", height:"calc(100vh - 240px)", minHeight:520 }}>
      {/* Header strip */}
      <div style={{ ...card, padding:"12px 16px", marginBottom:12, display:"flex", justifyContent:"space-between", alignItems:"center", flexWrap:"wrap", gap:10 }}>
        <div>
          <div style={{ fontSize:13, fontWeight:700 }}>💬 Ask AI about your financials</div>
          <div style={{ fontSize:11, color:T.textMuted }}>Powered by Claude · Sees {records.length} month(s) of audited data</div>
        </div>
        {history.length > 0 && (
          <button onClick={() => { setHistory([]); setErr(""); }}
            style={{ padding:"6px 12px", borderRadius:T.radiusSm, border:`1px solid ${T.border}`, background:T.bgWhite, color:T.textMuted, fontSize:11, fontWeight:600, cursor:"pointer" }}>
            New conversation
          </button>
        )}
      </div>

      {/* Chat scroll area */}
      <div ref={scrollRef} style={{ ...card, flex:1, overflow:"auto", padding:"16px", marginBottom:12 }}>
        {history.length === 0 ? (
          <div>
            <div style={{ textAlign:"center", padding:"30px 10px 18px", color:T.textMuted }}>
              <div style={{ fontSize:36, marginBottom:8 }}>💬</div>
              <div style={{ fontSize:13, fontWeight:600 }}>Ask anything about your financials</div>
              <div style={{ fontSize:11, marginTop:4 }}>Try one of these or type your own question below</div>
            </div>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(260px, 1fr))", gap:8, marginTop:14 }}>
              {SUGGESTED_QUESTIONS.map((q,i) => (
                <button key={i} onClick={() => send(q)}
                  style={{ textAlign:"left", padding:"10px 14px", borderRadius:T.radiusSm, border:`1px solid ${T.border}`, background:T.bgSurface, color:T.text, fontSize:12, cursor:"pointer", fontFamily:"inherit", lineHeight:1.4 }}>
                  💡 {q}
                </button>
              ))}
            </div>
          </div>
        ) : (
          history.map((m, i) => (
            <div key={i} style={{ marginBottom:14, display:"flex", flexDirection:"column", alignItems: m.role==="user" ? "flex-end" : "flex-start" }}>
              <div style={{
                maxWidth:"85%",
                padding:"10px 14px",
                borderRadius: m.role==="user" ? "16px 16px 4px 16px" : "16px 16px 16px 4px",
                background: m.role==="user" ? T.brand : T.bgSurface,
                color: m.role==="user" ? "#fff" : T.text,
                fontSize:13, lineHeight:1.55, whiteSpace:"pre-wrap",
                border: m.role==="assistant" ? `1px solid ${T.border}` : "none",
              }}>
                {m.content}
              </div>
              <div style={{ fontSize:9, color:T.textDim, marginTop:3, padding:"0 6px" }}>
                {m.role==="user" ? "You" : "Claude"}
              </div>
            </div>
          ))
        )}
        {busy && (
          <div style={{ display:"flex", alignItems:"center", gap:8, color:T.textMuted, fontSize:12 }}>
            <Spinner /> Thinking...
          </div>
        )}
      </div>

      {err && <div style={{ fontSize:11, color:T.red, padding:"8px 12px", background:T.redBg, borderRadius:T.radiusSm, marginBottom:8 }}>{err}</div>}

      {/* Input */}
      <div style={{ display:"flex", gap:8 }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } }}
          placeholder="Ask anything about your financials..."
          disabled={busy}
          style={{
            flex:1, padding:"10px 14px", borderRadius:T.radiusSm, border:`1px solid ${T.border}`,
            background:T.bgWhite, fontSize:13, fontFamily:"inherit", outline:"none",
          }}
        />
        <button onClick={() => send()} disabled={busy || !input.trim()}
          style={{ padding:"10px 20px", borderRadius:T.radiusSm, border:"none",
            background: (busy||!input.trim()) ? T.textDim : T.brand, color:"#fff",
            fontSize:12, fontWeight:700, cursor: (busy||!input.trim())?"not-allowed":"pointer" }}>
          Send
        </button>
      </div>
    </div>
  );
}

// ─── Statements list (renamed from old "Summary") ─────────────────────────────
function StatementsList({ records, onSelect }) {
  const sorted = [...records].sort((a,b) => (b.period||"").localeCompare(a.period||""));
  if (!sorted.length) return (
    <div style={{ ...card, textAlign:"center", padding:"40px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:8 }}>📋</div>
      <div style={{ fontSize:13, fontWeight:600 }}>No audited financials yet</div>
      <div style={{ fontSize:11, marginTop:6 }}>Open the 📧 Gmail Sync tab → AMP CPAs vendor card to import statements</div>
    </div>
  );
  return (
    <div style={{ ...card, padding:0, overflow:"auto" }}>
      <table style={{ width:"100%", borderCollapse:"collapse" }}>
        <thead>
          <tr>
            <th style={tblH}>Period</th>
            <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
            <th style={{ ...tblH, textAlign:"right" }}>COGS</th>
            <th style={{ ...tblH, textAlign:"right" }}>Gross Profit</th>
            <th style={{ ...tblH, textAlign:"right" }}>Op Expenses</th>
            <th style={{ ...tblH, textAlign:"right" }}>Net Income</th>
            <th style={{ ...tblH, textAlign:"right" }}>Net %</th>
            <th style={{ ...tblH, textAlign:"right" }}>Total Assets</th>
            <th style={{ ...tblH, textAlign:"right" }}>Equity</th>
            <th style={tblH}></th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(r => {
            const pl = r.pl || {};
            const bs = r.balance_sheet || {};
            const margin = pl.revenue > 0 ? (pl.net_income / pl.revenue) * 100 : 0;
            return (
              <tr key={r.id} style={{ cursor:"pointer" }} onClick={() => onSelect(r)}>
                <td style={{ ...tblD, fontWeight:600 }}>{monthLabel(r.period)}</td>
                <td style={tblDR}>{fmt(pl.revenue)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{pl.cost_of_goods_sold ? fmt(pl.cost_of_goods_sold) : "—"}</td>
                <td style={{ ...tblDR, color:(pl.gross_profit||0)>=0?T.green:T.red }}>{fmt(pl.gross_profit)}</td>
                <td style={{ ...tblDR, color:T.red }}>{fmt(pl.operating_expenses)}</td>
                <td style={{ ...tblDR, fontWeight:700, color:(pl.net_income||0)>=0?T.green:T.red }}>{fmt(pl.net_income)}</td>
                <td style={{ ...tblDR, color:margin>=0?T.greenText:T.redText }}>{fmtPct(margin)}</td>
                <td style={tblDR}>{fmt(bs.total_assets)}</td>
                <td style={{ ...tblDR, color:T.green }}>{fmt(bs.equity)}</td>
                <td style={tblD}><span style={{ fontSize:11, color:T.brand, fontWeight:600 }}>View →</span></td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ─── Gmail Sync Panel (mostly v1, with tweaks) ────────────────────────────────
function GmailSyncPanel({ onImported }) {
  const [emails, setEmails]       = useState([]);
  const [searching, setSearching] = useState(false);
  const [searchErr, setSearchErr] = useState("");
  const [processing, setProcessing] = useState({});
  const [processedIds, setProcessedIds] = useState(new Set());
  // v2.47.5: aggregate progress so Import All has a single visible counter
  // instead of relying on per-row status that's easy to miss on a long list.
  const [bulkProgress, setBulkProgress] = useState(null); // null | { current, total, label, failures }

  const searchEmails = async () => {
    setSearching(true); setSearchErr(""); setEmails([]);
    try {
      const processed = await getProcessedEmailIds();
      setProcessedIds(processed);
      const results = await searchCPAEmails();
      setEmails(results);
      if (!results.length) setSearchErr("No emails found from @ampcpas.com with PDF attachments.");
    } catch (e) {
      setSearchErr("Gmail search failed: " + e.message + ". Make sure Gmail is connected in Settings.");
    }
    setSearching(false);
  };

  // v2.47.5: auto-search when the panel mounts so the user doesn't have to
  // click Search every time. Same behavior whether reopening the tab or
  // landing here for the first time after Gmail OAuth.
  useEffect(() => {
    searchEmails();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const processEmail = async (email) => {
    const emailId = email.emailId;
    const pdfs = (email.attachments || []).filter(a => { const fn=(a.filename||"").toLowerCase(); return fn.endsWith(".pdf") && fn.includes("financial"); });
    if (!pdfs.length) {
      setProcessing(p => ({ ...p, [emailId]:"✗ No financial-statement PDF found" }));
      return;
    }

    if (!window.pdfjsLib) {
      setProcessing(p => ({ ...p, [emailId]:"Loading PDF engine..." }));
      await new Promise((res, rej) => {
        const s = document.createElement("script");
        s.src = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js";
        s.onload = res; s.onerror = rej;
        document.head.appendChild(s);
      });
    }

    const savedPeriods = [];
    try {
      for (let pi = 0; pi < pdfs.length; pi++) {
        const att = pdfs[pi];
        const label = pdfs.length > 1 ? ` (${pi+1}/${pdfs.length}: ${att.filename})` : "";
        setProcessing(p => ({ ...p, [emailId]:`Fetching PDF${label}...` }));
        const pdfBytes = await fetchAttachment(emailId, att.attachmentId, email.account_email, email.account_doc_id);
        setProcessing(p => ({ ...p, [emailId]:`Converting to images${label}...` }));
        const pages = await pdfToPages(pdfBytes, 30);
        setProcessing(p => ({ ...p, [emailId]:`Extracting financials (${pages.length} pages)${label}...` }));
        const extracted = await extractFinancials(pages);
        let period = (extracted.period && /^\d{4}-\d{2}$/.test(extracted.period))
          ? extracted.period
          : extractMonthFromSubject(att.filename) || extractMonthFromSubject(email.emailSubject);
        if (!period) {
          setProcessing(p => ({ ...p, [emailId]:`✗ Could not determine period for ${att.filename}` }));
          continue;
        }
        // Upload PDF to Firebase Storage for later viewing
        setProcessing(p => ({ ...p, [emailId]:`Saving PDF to storage (${monthLabel(period)})...` }));
        const pdfMeta = await uploadPdfToStorage(period, pdfBytes, att.filename);

        const record = {
          ...extracted,
          period,
          email_id: emailId,
          email_subject: email.emailSubject,
          email_date: email.emailDate,
          from: email.from,
          filename: att.filename,
          pdf_storage_path: pdfMeta?.storage_path || null,
          pdf_download_url: pdfMeta?.download_url || null,
        };
        setProcessing(p => ({ ...p, [emailId]:`Saving ${monthLabel(period)} financials...` }));
        const ok = await saveFinancial(period, record);
        if (!ok) throw new Error("Firestore write failed");
        await saveEmailProcessed(emailId, { period, filename: att.filename, subject: email.emailSubject });
        setProcessedIds(prev => new Set([...prev, emailId]));
        savedPeriods.push(period);
      }
      if (savedPeriods.length > 0) {
        setProcessing(p => ({ ...p, [emailId]:`✓ Saved ${savedPeriods.map(monthLabel).join(", ")}` }));
        onImported();
      } else {
        setProcessing(p => ({ ...p, [emailId]:"✗ No financial statements could be saved" }));
      }
    } catch (e) {
      setProcessing(p => ({ ...p, [emailId]:"✗ " + e.message }));
    }
  };

  const processAll = async () => {
    const unprocessed = emails.filter(e => !processedIds.has(e.emailId) && (e.attachments||[]).some(a=>{ const fn=(a.filename||"").toLowerCase(); return fn.endsWith(".pdf") && fn.includes("financial"); }));
    if (!unprocessed.length) return;
    const failures = [];
    for (let i = 0; i < unprocessed.length; i++) {
      const email = unprocessed[i];
      const label = email.emailSubject || email.attachments?.[0]?.filename || `Email ${i+1}`;
      setBulkProgress({ current: i + 1, total: unprocessed.length, label, failures: [...failures] });
      try {
        await processEmail(email);
        // Inspect the per-email status to detect silent failures.
        // processEmail sets processing[emailId] starting with "✗" on error.
        // We can't read state synchronously, so check after a microtask flush.
        await new Promise(r => setTimeout(r, 0));
      } catch (e) {
        failures.push({ subject: label, error: e.message });
      }
    }
    setBulkProgress({ current: unprocessed.length, total: unprocessed.length, label: "Done", failures });
    // Clear progress banner after 8s so it doesn't linger forever, but keep
    // failures visible — the per-row Status column has them too.
    setTimeout(() => setBulkProgress(null), 8000);
  };

  const unprocessedCount = emails.filter(e => !processedIds.has(e.emailId)).length;

  return (
    <div>
      <div style={{ display:"flex", gap:8, marginBottom:16, alignItems:"center", flexWrap:"wrap" }}>
        <button onClick={searchEmails} disabled={searching}
          style={{ padding:"9px 18px", borderRadius:T.radiusSm, border:"none", background:T.brand, color:"#fff", fontSize:12, fontWeight:700, cursor:searching?"not-allowed":"pointer", opacity:searching?0.7:1 }}>
          {searching ? "🔍 Searching..." : "🔍 Search Gmail for AMP CPAs Emails"}
        </button>
        {emails.length > 0 && unprocessedCount > 0 && (
          <button onClick={processAll}
            style={{ padding:"9px 18px", borderRadius:T.radiusSm, border:"none", background:T.green, color:"#fff", fontSize:12, fontWeight:700, cursor:"pointer" }}>
            ⚡ Import All New ({unprocessedCount})
          </button>
        )}
        {emails.length > 0 && <span style={{ fontSize:12, color:T.textMuted }}>{emails.length} email(s) found</span>}
      </div>

      {searchErr && <div style={{ fontSize:12, color:T.red, marginBottom:12, padding:"10px 14px", background:T.redBg, borderRadius:T.radiusSm }}>{searchErr}</div>}

      {bulkProgress && (
        <div style={{ marginBottom:12, padding:"12px 14px", background:bulkProgress.current >= bulkProgress.total ? T.greenBg : T.blueBg, borderRadius:T.radiusSm, border:`1px solid ${bulkProgress.current >= bulkProgress.total ? T.green : T.blueText}` }}>
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", gap:12 }}>
            <div style={{ fontSize:13, fontWeight:600, color:bulkProgress.current >= bulkProgress.total ? T.greenText : T.blueText }}>
              {bulkProgress.current >= bulkProgress.total
                ? `✓ Imported ${bulkProgress.total - (bulkProgress.failures?.length||0)} of ${bulkProgress.total} financials`
                : `Processing ${bulkProgress.current}/${bulkProgress.total}: ${bulkProgress.label}`}
            </div>
            <div style={{ fontSize:11, color:T.textMuted }}>{Math.round((bulkProgress.current / bulkProgress.total) * 100)}%</div>
          </div>
          <div style={{ marginTop:8, height:6, background:"rgba(0,0,0,0.06)", borderRadius:3, overflow:"hidden" }}>
            <div style={{ height:"100%", width:`${(bulkProgress.current / bulkProgress.total) * 100}%`, background:bulkProgress.current >= bulkProgress.total ? T.green : T.blueText, transition:"width 0.3s" }} />
          </div>
          {bulkProgress.failures && bulkProgress.failures.length > 0 && (
            <div style={{ marginTop:10, fontSize:11, color:T.redText }}>
              <strong>{bulkProgress.failures.length} failure{bulkProgress.failures.length>1?"s":""}:</strong>{" "}
              {bulkProgress.failures.slice(0,3).map(f => f.subject).join(", ")}{bulkProgress.failures.length > 3 ? ` and ${bulkProgress.failures.length - 3} more` : ""}
            </div>
          )}
        </div>
      )}

      {emails.length > 0 && (
        <div style={{ ...card, padding:0, overflow:"hidden" }}>
          <table style={{ width:"100%", borderCollapse:"collapse" }}>
            <thead>
              <tr>
                <th style={tblH}>Date</th>
                <th style={tblH}>Subject</th>
                <th style={tblH}>From</th>
                <th style={tblH}>PDF</th>
                <th style={tblH}>Status</th>
                <th style={tblH}></th>
              </tr>
            </thead>
            <tbody>
              {emails.map(email => {
                const pdfs = (email.attachments||[]).filter(a=>{ const fn=(a.filename||"").toLowerCase(); return fn.endsWith(".pdf") && fn.includes("financial"); });
                const alreadyDone = processedIds.has(email.emailId);
                const status = processing[email.emailId] || (alreadyDone ? "✓ Already imported" : "");
                const isOk  = status.startsWith("✓");
                const isErr = status.startsWith("✗");
                const isBusy = status && !isOk && !isErr;
                return (
                  <tr key={email.emailId}>
                    <td style={{ ...tblD, whiteSpace:"nowrap", color:T.textMuted }}>{email.emailDate ? new Date(email.emailDate).toLocaleDateString() : "—"}</td>
                    <td style={{ ...tblD, maxWidth:280, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{email.emailSubject || "—"}</td>
                    <td style={{ ...tblD, color:T.textMuted, fontSize:11 }}>{email.from || "—"}</td>
                    <td style={tblD}>{pdfs.length > 0 ? <Badge text={pdfs[0].filename} color={T.blueText} bg={T.blueBg} /> : <span style={{ color:T.textDim, fontSize:11 }}>No PDF</span>}</td>
                    <td style={{ ...tblD, color:isOk?T.green:isErr?T.red:T.brand, fontSize:11 }}>{isBusy && <Spinner />} {status}</td>
                    <td style={tblD}>
                      {!alreadyDone && pdfs.length > 0 && !isBusy && (
                        <button onClick={() => processEmail(email)}
                          style={{ padding:"5px 12px", borderRadius:T.radiusSm, border:`1px solid ${T.brand}`, background:"none", color:T.brand, fontSize:11, fontWeight:600, cursor:"pointer" }}>
                          Import
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {!emails.length && !searching && !searchErr && (
        <div style={{ textAlign:"center", padding:"40px 20px", color:T.textMuted }}>
          <div style={{ fontSize:36, marginBottom:8 }}>📧</div>
          <div style={{ fontSize:13, fontWeight:600 }}>Search Gmail for AMP CPAs emails</div>
          <div style={{ fontSize:11, marginTop:4 }}>Looks for "Financial Statements" PDFs from @ampcpas.com</div>
        </div>
      )}
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
function AuditedFinancialsTab() {
  const [view, setView]         = useState("dashboard"); // dashboard | statements | chat | gmail
  const [records, setRecords]   = useState([]);
  const [loading, setLoading]   = useState(true);
  const [selected, setSelected] = useState(null);
  const [loadErr, setLoadErr]   = useState("");

  const load = useCallback(async () => {
    setLoading(true); setLoadErr("");
    try {
      const data = await getFinancials();
      setRecords(data);
    } catch (e) {
      setLoadErr(e.message || "Failed to load financials");
    }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleDelete = async (rec) => {
    if (!window.confirm(`Delete ${monthLabel(rec.period)} financials? This can't be undone.`)) return;
    const ok = await deleteFinancial(rec.period);
    if (ok) { setSelected(null); load(); }
    else window.alert("Delete failed — check console");
  };

  const views = [
    { id:"dashboard",  icon:"📊", label:"Dashboard",  count:null },
    { id:"statements", icon:"📋", label:"Statements", count:records.length },
    { id:"chat",       icon:"💬", label:"Ask AI",     count:null },
  ];

  if (loading) return (
    <div style={{ textAlign:"center", padding:"60px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:12 }}>📋</div>
      <div style={{ fontSize:14 }}>Loading audited financials...</div>
    </div>
  );

  return (
    <div style={{ padding:"20px", maxWidth:1400, margin:"0 auto" }} className="fade-in">
      <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>

      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16, flexWrap:"wrap", gap:10 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:18 }}>📋</span>
          <span style={{ fontSize:15, fontWeight:700, color:T.text }}>Financials</span>
          <span style={{ fontSize:11, color:T.textDim, marginLeft:4 }}>AMP CPAs · {records.length} month(s)</span>
        </div>
        <button onClick={load} title="Reload from Firestore"
          style={{ padding:"6px 12px", borderRadius:T.radiusSm, border:`1px solid ${T.border}`, background:T.bgWhite, color:T.textMuted, fontSize:11, fontWeight:600, cursor:"pointer" }}>
          🔄 Refresh
        </button>
      </div>

      {loadErr && <div style={{ marginBottom:12, padding:"10px 14px", background:T.redBg, color:T.red, borderRadius:T.radiusSm, fontSize:12 }}>⚠️ {loadErr}</div>}

      <div style={{ display:"flex", gap:6, marginBottom:20, flexWrap:"wrap" }}>
        {views.map(v => (
          <button key={v.id} onClick={() => setView(v.id)} style={{
            padding:"8px 16px", borderRadius:T.radiusSm, border:"none",
            background:view===v.id?T.brand:"transparent",
            color:view===v.id?"#fff":T.textMuted,
            fontSize:12, fontWeight:view===v.id?700:500,
            cursor:"pointer", transition:"all 0.2s",
            display:"flex", alignItems:"center", gap:6,
          }}>
            <span>{v.icon}</span>
            <span>{v.label}</span>
            {v.count!=null && <span style={{ padding:"1px 6px", borderRadius:10, fontSize:9, fontWeight:700, color:view===v.id?"rgba(255,255,255,0.9)":T.blueText, background:view===v.id?"rgba(255,255,255,0.2)":T.blueBg }}>{v.count}</span>}
          </button>
        ))}
      </div>

      {view === "dashboard"  && <Dashboard records={records} onSelect={setSelected} />}
      {view === "statements" && <StatementsList records={records} onSelect={setSelected} />}
      {view === "chat"       && <AIChat records={records} />}

      {selected && <DetailModal record={selected} onClose={() => setSelected(null)} onDelete={handleDelete} />}
    </div>
  );
}

window.AuditedFinancialsTab = AuditedFinancialsTab;

})();
