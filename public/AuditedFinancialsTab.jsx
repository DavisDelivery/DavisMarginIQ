// AuditedFinancialsTab.jsx — Audited Financials from AMP CPAs
// Searches Gmail for emails from @ampcpas.com with PDF attachments,
// converts PDFs to images via pdf.js, extracts P&L / Balance Sheet /
// Cash Flow using Claude vision, saves to Firestore audited_financials.
// v1.0.0

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
  radius:"12px", radiusSm:"8px",
  shadow:"0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
};

// ─── Formatters ───────────────────────────────────────────────────────────────
const fmt  = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1000000) return "$"+(v/1000000).toFixed(2)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":Number(n).toFixed(d)+"%";

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

// Extract YYYY-MM from an email subject like "March 2025 Financials"
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
  // Try MM/YYYY or YYYY-MM directly
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
      messages: [{
        role: "user",
        content: [
          ...imageBlocks,
          { type: "text", text: EXTRACTION_PROMPT },
        ],
      }],
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
      .set({ ...data, updated_at: new Date().toISOString() }, { merge: true });
    return true;
  } catch (e) { console.error("saveFinancial", e); return false; }
}

async function getFinancials() {
  try {
    const s = await db().collection("audited_financials")
      .orderBy("period", "desc").limit(60).get();
    return s.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) { return []; }
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
  // Convert base64 to Uint8Array
  const bin = atob(data.data);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

// ─── Mini components ──────────────────────────────────────────────────────────
const Badge = ({ text, color, bg }) => (
  <span style={{ padding:"2px 8px", borderRadius:20, fontSize:10, fontWeight:700, color:color||T.blueText, background:bg||T.blueBg, display:"inline-block" }}>{text}</span>
);

const StatusLine = ({ text }) => {
  if (!text) return null;
  const isErr  = text.startsWith("✗") || text.toLowerCase().includes("error");
  const isOk   = text.startsWith("✓");
  const color  = isErr ? T.red : isOk ? T.green : T.brand;
  return <div style={{ fontSize:12, color, fontWeight:600, marginTop:8 }}>{text}</div>;
};

const Spinner = () => (
  <span style={{ display:"inline-block", animation:"spin 1s linear infinite", fontSize:16 }}>⏳</span>
);

// ─── P&L Detail Card ─────────────────────────────────────────────────────────
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

// ─── Balance Sheet Card ───────────────────────────────────────────────────────
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

// ─── Cash Flow Card ───────────────────────────────────────────────────────────
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
function DetailModal({ record, onClose }) {
  if (!record) return null;
  return (
    <div style={{ position:"fixed", inset:0, background:"rgba(0,0,0,0.5)", zIndex:1000, display:"flex", alignItems:"center", justifyContent:"center", padding:20 }}
         onClick={onClose}>
      <div style={{ background:T.bgWhite, borderRadius:T.radius, padding:24, maxWidth:900, width:"100%", maxHeight:"90vh", overflowY:"auto" }}
           onClick={e => e.stopPropagation()}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:20 }}>
          <div style={{ fontSize:16, fontWeight:800 }}>📋 {monthLabel(record.period)} — Audited Financials</div>
          <button onClick={onClose} style={{ background:"none", border:"none", fontSize:20, cursor:"pointer", color:T.textMuted }}>✕</button>
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
      </div>
    </div>
  );
}

// ─── Gmail Sync Panel ─────────────────────────────────────────────────────────
function GmailSyncPanel({ onImported }) {
  const [emails, setEmails]       = useState([]);
  const [searching, setSearching] = useState(false);
  const [searchErr, setSearchErr] = useState("");
  const [processing, setProcessing] = useState({}); // emailId → status string
  const [processedIds, setProcessedIds] = useState(new Set());

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

  const processEmail = async (email) => {
    const emailId = email.emailId;
    // Only Financial Statements PDFs — skip invoice PDFs (Invoice #XXXXX)
    const pdfs = (email.attachments || []).filter(a => { const fn=(a.filename||"").toLowerCase(); return fn.endsWith(".pdf") && fn.includes("financial"); });
    if (!pdfs.length) {
      setProcessing(p => ({ ...p, [emailId]:"✗ No PDF attachment found" }));
      return;
    }

    // Load pdf.js once up front if needed
    if (!window.pdfjsLib) {
      setProcessing(p => ({ ...p, [emailId]:"Loading PDF engine..." }));
      await new Promise((res, rej) => {
        const s = document.createElement("script");
        s.src = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js";
        s.onload = res; s.onerror = rej;
        document.head.appendChild(s);
      });
    }

    // Process each financial PDF in this email (sometimes 2 arrive together e.g. March + April)
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

        // Determine period — try Claude extraction first, then filename, then subject
        let period = (extracted.period && /^\d{4}-\d{2}$/.test(extracted.period))
          ? extracted.period
          : extractMonthFromSubject(att.filename) || extractMonthFromSubject(email.emailSubject);

        if (!period) {
          setProcessing(p => ({ ...p, [emailId]:`✗ Could not determine period for ${att.filename}` }));
          continue;
        }

        const record = {
          ...extracted,
          period,
          email_id: emailId,
          email_subject: email.emailSubject,
          email_date: email.emailDate,
          from: email.from,
          filename: att.filename,
        };

        setProcessing(p => ({ ...p, [emailId]:`Saving ${monthLabel(period)}...` }));
        const ok = await saveFinancial(period, record);
        if (!ok) throw new Error("Firestore write failed");

        await saveEmailProcessed(emailId, { period, filename: att.filename, subject: email.emailSubject });
        setProcessedIds(prev => new Set([...prev, emailId]));
        savedPeriods.push(period);
      } // end for each PDF

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
    for (const email of unprocessed) {
      await processEmail(email);
    }
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
                    <td style={{ ...tblD, whiteSpace:"nowrap", color:T.textMuted }}>
                      {email.emailDate ? new Date(email.emailDate).toLocaleDateString() : "—"}
                    </td>
                    <td style={{ ...tblD, maxWidth:280, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
                      {email.emailSubject || "—"}
                    </td>
                    <td style={{ ...tblD, color:T.textMuted, fontSize:11 }}>{email.from || "—"}</td>
                    <td style={tblD}>
                      {pdfs.length > 0
                        ? <Badge text={pdfs[0].filename} color={T.blueText} bg={T.blueBg} />
                        : <span style={{ color:T.textDim, fontSize:11 }}>No PDF</span>}
                    </td>
                    <td style={{ ...tblD, color:isOk?T.green:isErr?T.red:T.brand, fontSize:11 }}>
                      {isBusy && <Spinner />} {status}
                    </td>
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
          <div style={{ fontSize:11, marginTop:4 }}>Looks for PDF attachments from @ampcpas.com</div>
        </div>
      )}
    </div>
  );
}

// ─── Summary Table ────────────────────────────────────────────────────────────
function SummaryTable({ records, onSelect }) {
  const sorted = [...records].sort((a,b) => (b.period||"").localeCompare(a.period||""));
  if (!sorted.length) return (
    <div style={{ textAlign:"center", padding:"40px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:8 }}>📋</div>
      <div style={{ fontSize:13, fontWeight:600 }}>No audited financials yet</div>
      <div style={{ fontSize:11, marginTop:4 }}>Use Gmail Sync to import monthly statements from AMP CPAs</div>
    </div>
  );

  // Summary KPIs
  const totRev = sorted.reduce((s,r) => s+(r.pl?.revenue||0), 0);
  const totNet = sorted.reduce((s,r) => s+(r.pl?.net_income||0), 0);
  const avgMargin = sorted.length
    ? sorted.reduce((s,r) => { const rev=r.pl?.revenue||0; return s+(rev>0?(r.pl?.net_income||0)/rev*100:0); }, 0) / sorted.length
    : 0;

  return (
    <div>
      <div style={{ display:"flex", gap:12, flexWrap:"wrap", marginBottom:20 }}>
        {[
          { label:"Total Revenue",  val:fmtK(totRev),          color:T.brand },
          { label:"Total Net Income", val:fmtK(totNet),         color:totNet>=0?T.green:T.red },
          { label:"Avg Net Margin", val:fmtPct(avgMargin),      color:avgMargin>=0?T.green:T.red },
          { label:"Months on File", val:sorted.length+" months", color:T.text },
        ].map(k => (
          <div key={k.label} style={{ ...card, flex:1, minWidth:150 }}>
            <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", marginBottom:4 }}>{k.label}</div>
            <div style={{ fontSize:22, fontWeight:800, color:k.color }}>{k.val}</div>
          </div>
        ))}
      </div>

      <div style={{ ...card, padding:0, overflow:"hidden" }}>
        <table style={{ width:"100%", borderCollapse:"collapse" }}>
          <thead>
            <tr>
              <th style={tblH}>Period</th>
              <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
              <th style={{ ...tblH, textAlign:"right" }}>COGS</th>
              <th style={{ ...tblH, textAlign:"right" }}>Gross Profit</th>
              <th style={{ ...tblH, textAlign:"right" }}>Expenses</th>
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
                  <td style={tblD}>
                    <span style={{ fontSize:11, color:T.brand, fontWeight:600 }}>View →</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
function AuditedFinancialsTab() {
  const [view, setView]         = useState("summary"); // summary | gmail
  const [records, setRecords]   = useState([]);
  const [loading, setLoading]   = useState(true);
  const [selected, setSelected] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    const data = await getFinancials();
    setRecords(data);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const views = [
    { id:"summary", icon:"📋", label:"Financials",  count:records.length },
    { id:"gmail",   icon:"📧", label:"Gmail Sync",  count:null },
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

      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:18 }}>📋</span>
          <span style={{ fontSize:15, fontWeight:700, color:T.text }}>Audited Financials</span>
          <span style={{ fontSize:11, color:T.textDim, marginLeft:4 }}>AMP CPAs</span>
        </div>
        <span style={{ fontSize:11, color:T.textMuted }}>Monthly CPA statements — auto-imported from Gmail</span>
      </div>

      {/* View tabs */}
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
            {v.count!=null && (
              <span style={{ padding:"1px 6px", borderRadius:10, fontSize:9, fontWeight:700, color:view===v.id?"rgba(255,255,255,0.9)":T.blueText, background:view===v.id?"rgba(255,255,255,0.2)":T.blueBg }}>{v.count}</span>
            )}
          </button>
        ))}
      </div>

      {view === "summary" && <SummaryTable records={records} onSelect={setSelected} />}
      {view === "gmail"   && <GmailSyncPanel onImported={load} />}

      {selected && <DetailModal record={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}

window.AuditedFinancialsTab = AuditedFinancialsTab;

})();
