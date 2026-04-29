// QBOImportTab.jsx — Manual QuickBooks Data Import
// Handles: P&L CSV, Invoice List CSV, Expense/Bills CSV
// Saves to Firestore: qbo_pl_monthly, qbo_invoices, qbo_expenses
// v1.0.0

(function() {
"use strict";

const { useState, useEffect, useMemo, useCallback } = React;

// ─── Theme (mirrors MarginIQ.jsx T object) ──────────────────────────────────
const T = {
  brand:"#1e5b92", brandLight:"#2a7bc8", brandDark:"#143f66", brandPale:"#e8f0f8",
  accent:"#10b981", accentWarn:"#f59e0b", accentDanger:"#ef4444",
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

// ─── Formatters ──────────────────────────────────────────────────────────────
const fmt  = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1000000) return "$"+(v/1000000).toFixed(2)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":Number(n).toFixed(d)+"%";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

// ─── Styles ──────────────────────────────────────────────────────────────────
const card = { background:T.bgCard, borderRadius:T.radius, border:`1px solid ${T.border}`, padding:"16px", boxShadow:T.shadow };
const tblH = { padding:"8px 12px", textAlign:"left", fontSize:10, fontWeight:700, textTransform:"uppercase", color:T.textDim, background:T.bgSurface, borderBottom:`1px solid ${T.border}`, whiteSpace:"nowrap" };
const tblD = { padding:"8px 12px", fontSize:12, color:T.text, borderBottom:`1px solid ${T.borderLight}`, verticalAlign:"top" };
const tblDR = { ...tblD, textAlign:"right", fontVariantNumeric:"tabular-nums" };

// ─── CSV Parser ──────────────────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.replace(/\r\n/g,"\n").replace(/\r/g,"\n").split("\n");
  const rows = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    const cols = [];
    let cur = "", inQ = false;
    for (let i=0; i<line.length; i++) {
      const ch = line[i];
      if (ch==='"' && !inQ) { inQ=true; continue; }
      if (ch==='"' && inQ && line[i+1]==='"') { cur+='"'; i++; continue; }
      if (ch==='"' && inQ) { inQ=false; continue; }
      if (ch==="," && !inQ) { cols.push(cur.trim()); cur=""; continue; }
      cur += ch;
    }
    cols.push(cur.trim());
    rows.push(cols);
  }
  return rows;
}

// ─── Money parser ────────────────────────────────────────────────────────────
function parseMoney(s) {
  if (s==null || s==="") return 0;
  const n = parseFloat(String(s).replace(/[$,\s]/g,"").replace(/\((.+)\)/,"$1"));
  // QBO uses parentheses for negatives in some exports
  const isNeg = /^\(/.test(String(s).trim());
  return isNaN(n) ? 0 : isNeg ? -Math.abs(n) : n;
}

// ─── Date helpers ────────────────────────────────────────────────────────────
function parseDate(s) {
  if (!s) return null;
  // MM/DD/YYYY or M/D/YYYY
  const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[1].padStart(2,"0")}-${m[2].padStart(2,"0")}`;
  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0,10);
  return null;
}
function toMonthKey(dateStr) {
  if (!dateStr) return null;
  return dateStr.slice(0,7); // YYYY-MM
}
function monthLabel(key) {
  if (!key) return "—";
  const [y,m] = key.split("-");
  const names = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  return `${names[parseInt(m)]} ${y}`;
}

// ─── FIRESTORE helpers ───────────────────────────────────────────────────────
const db = () => window.db;

async function savePLMonthly(monthKey, data) {
  try {
    await db().collection("qbo_pl_monthly").doc(monthKey).set({...data, updated_at: new Date().toISOString()}, {merge:true});
    return true;
  } catch(e) { console.error("savePLMonthly",e); return false; }
}
async function getPLMonthly() {
  try {
    const s = await db().collection("qbo_pl_monthly").orderBy("month","desc").limit(60).get();
    return s.docs.map(d=>({id:d.id,...d.data()}));
  } catch(e) { return []; }
}

async function saveInvoice(invId, data) {
  try {
    await db().collection("qbo_invoices").doc(invId).set({...data, updated_at: new Date().toISOString()}, {merge:true});
    return true;
  } catch(e) { console.error("saveInvoice",e); return false; }
}
async function getInvoices() {
  try {
    const s = await db().collection("qbo_invoices").orderBy("date","desc").limit(2000).get();
    return s.docs.map(d=>({id:d.id,...d.data()}));
  } catch(e) { return []; }
}

async function saveExpense(expId, data) {
  try {
    await db().collection("qbo_expenses").doc(expId).set({...data, updated_at: new Date().toISOString()}, {merge:true});
    return true;
  } catch(e) { console.error("saveExpense",e); return false; }
}
async function getExpenses() {
  try {
    const s = await db().collection("qbo_expenses").orderBy("date","desc").limit(5000).get();
    return s.docs.map(d=>({id:d.id,...d.data()}));
  } catch(e) { return []; }
}

// ─── QBO P&L CSV Parser ──────────────────────────────────────────────────────
// QBO P&L export typically has:
//   Row 0: Report title
//   Row 1: Company name
//   Row 2: Date range
//   Row 3+: Section headers + account rows with amounts by column (month or total)
//
// The tricky part: QBO puts month names as column headers when you export
// a date-range P&L. We detect the header row and map columns to months.
function parsePLCSV(text) {
  const raw = parseCSV(text);
  if (raw.length < 3) return { error:"File too short", months:[] };

  // Find the header row — it's the one where col[0] is empty/whitespace and later cols look like dates or "Total"
  let headerRowIdx = -1;
  let monthCols = []; // [{colIdx, monthKey}]

  for (let i=0; i<Math.min(20, raw.length); i++) {
    const row = raw[i];
    // Look for a row with month names like "Jan 2024", "February 2024", or "MM/YYYY"
    const monthMatches = row.filter(c => /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(c) || /\d{1,2}\/\d{4}/.test(c));
    if (monthMatches.length >= 1) {
      headerRowIdx = i;
      // Map each matching col to a month key
      const monthNames = {jan:"01",feb:"02",mar:"03",apr:"04",may:"05",jun:"06",jul:"07",aug:"08",sep:"09",oct:"10",nov:"11",dec:"12"};
      for (let ci=0; ci<row.length; ci++) {
        const cell = String(row[ci]).trim();
        // "Jan 2024" or "January 2024"
        const m1 = cell.match(/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[\s,]+(\d{4})$/i);
        if (m1) { monthCols.push({colIdx:ci, monthKey:`${m1[2]}-${monthNames[m1[1].slice(0,3).toLowerCase()]}`}); continue; }
        // "MM/YYYY"
        const m2 = cell.match(/^(\d{1,2})\/(\d{4})$/);
        if (m2) { monthCols.push({colIdx:ci, monthKey:`${m2[2]}-${m2[1].padStart(2,"0")}`}); continue; }
        // "Total" column — skip
      }
      break;
    }
  }

  // If no month columns found — single-period P&L (one "Total" column)
  let singlePeriod = null;
  if (monthCols.length === 0) {
    // Try to extract period from the first few rows
    for (let i=0; i<5; i++) {
      const joined = (raw[i]||[]).join(" ");
      const m = joined.match(/(\w+ \d{4})\s+to\s+(\w+ \d{4})/i) || joined.match(/(\d{1,2}\/\d{1,2}\/\d{4})\s+-\s+(\d{1,2}\/\d{1,2}\/\d{4})/);
      if (m) { singlePeriod = joined; break; }
    }
    // Find header: first row where a col says "Total" or "Amount"
    for (let i=0; i<Math.min(20, raw.length); i++) {
      const cols = raw[i].map(c=>c.toLowerCase());
      if (cols.includes("total") || cols.includes("amount")) {
        headerRowIdx = i;
        const totalIdx = cols.indexOf("total") !== -1 ? cols.indexOf("total") : cols.indexOf("amount");
        // We'll use "TOTAL" as a synthetic month key and prompt user to enter the month
        monthCols = [{colIdx: totalIdx, monthKey: "SINGLE"}];
        break;
      }
    }
  }

  if (headerRowIdx === -1) return { error:"Could not detect header row. Is this a QBO P&L export?", months:[] };

  // Classify accounts into Income, COGS, Expense sections
  // QBO P&L structure: Income → Gross Profit → Expenses → Net Income
  const INCOME_SECTIONS = ["income","revenue","sales","total income","total revenue","gross profit"];
  const COGS_SECTIONS = ["cost of goods sold","cogs","cost of sales","direct cost"];
  const EXPENSE_SECTIONS = ["expenses","operating expenses","other expenses","total expenses"];

  // Build per-month aggregates
  const monthData = {};
  for (const mc of monthCols) monthData[mc.monthKey] = {month:mc.monthKey, income:0, cogs:0, expenses:0, lines:[]};

  let currentSection = "unknown";
  for (let i=headerRowIdx+1; i<raw.length; i++) {
    const row = raw[i];
    const label = String(row[0]||"").trim();
    if (!label) continue;

    const labelL = label.toLowerCase();

    // Detect section changes
    if (INCOME_SECTIONS.some(s=>labelL.startsWith(s))) { currentSection = "income"; }
    else if (COGS_SECTIONS.some(s=>labelL.startsWith(s))) { currentSection = "cogs"; }
    else if (EXPENSE_SECTIONS.some(s=>labelL.startsWith(s))) { currentSection = "expense"; }

    // Skip total/subtotal rows for line items but still use for section totals
    const isTotalRow = /^(total|net|gross profit)/i.test(label);

    for (const mc of monthCols) {
      const amt = parseMoney(row[mc.colIdx]);
      if (amt === 0) continue;
      if (!isTotalRow) {
        monthData[mc.monthKey].lines.push({account:label, section:currentSection, amount:amt});
      }
      // Accumulate into sections
      if (currentSection==="income" && isTotalRow && /^total income|^total revenue/i.test(label)) {
        monthData[mc.monthKey].income = amt;
      } else if (currentSection==="cogs" && isTotalRow) {
        monthData[mc.monthKey].cogs = amt;
      } else if (currentSection==="expense" && isTotalRow && /^total expense/i.test(label)) {
        monthData[mc.monthKey].expenses = amt;
      } else if (/^net income|^net profit/i.test(label)) {
        monthData[mc.monthKey].net_income = amt;
      }
    }
  }

  // Derive income/cogs/expenses from lines if totals weren't found
  for (const mk of Object.keys(monthData)) {
    const md = monthData[mk];
    if (md.income === 0) md.income = md.lines.filter(l=>l.section==="income").reduce((s,l)=>s+l.amount,0);
    if (md.cogs === 0) md.cogs = md.lines.filter(l=>l.section==="cogs").reduce((s,l)=>s+l.amount,0);
    if (md.expenses === 0) md.expenses = md.lines.filter(l=>l.section==="expense").reduce((s,l)=>s+l.amount,0);
    if (md.net_income == null) md.net_income = md.income - md.cogs - md.expenses;
    md.gross_profit = md.income - md.cogs;
    md.gross_margin_pct = md.income > 0 ? (md.gross_profit/md.income)*100 : 0;
    md.net_margin_pct  = md.income > 0 ? (md.net_income/md.income)*100 : 0;
  }

  return { months: Object.values(monthData).filter(m=>m.income!==0||m.expenses!==0), singlePeriod };
}

// ─── QBO Invoice List CSV Parser ─────────────────────────────────────────────
// QBO Invoice List export columns (typical):
//   Num, Customer, Date, Due Date, Balance, Status, Total
// or from "Sales by Customer Detail":
//   Type, Num, Date, Name, Memo/Description, Account, Split, Amount
function parseInvoiceCSV(text) {
  const raw = parseCSV(text);
  if (raw.length < 2) return { error:"File too short", invoices:[] };

  // Find header row
  let headerIdx = -1, headers = [];
  for (let i=0; i<Math.min(10, raw.length); i++) {
    const cols = raw[i].map(c=>String(c).toLowerCase().trim());
    // Invoice list has "num" or "invoice no" and "date" and "total"/"amount"
    if ((cols.includes("num") || cols.includes("invoice no") || cols.includes("transaction type") || cols.includes("type")) && cols.includes("date")) {
      headerIdx = i;
      headers = raw[i].map(c=>String(c).trim().toLowerCase().replace(/[^a-z0-9]/g,"_"));
      break;
    }
  }
  if (headerIdx === -1) return { error:"Could not find column headers. Expected 'Num', 'Date', 'Total' columns.", invoices:[] };

  const col = (row, ...names) => {
    for (const n of names) {
      const idx = headers.indexOf(n);
      if (idx !== -1 && row[idx]) return String(row[idx]).trim();
    }
    return "";
  };

  const invoices = [];
  for (let i=headerIdx+1; i<raw.length; i++) {
    const row = raw[i];
    if (!row[0] && !row[1]) continue;

    const rawDate = col(row,"date","invoice_date","txn_date");
    const date = parseDate(rawDate);
    if (!date) continue;

    const num    = col(row,"num","invoice_no","invoice__","no_");
    const customer = col(row,"customer","name","customer_customer_job");
    const total  = parseMoney(col(row,"total","amount","total_amount","balance"));
    const status = col(row,"status","open_balance") || "";
    const memo   = col(row,"memo_description","memo","description") || "";
    const type   = col(row,"type","transaction_type") || "Invoice";

    if (!total && !num) continue;

    const month = toMonthKey(date);
    const invId = `qbo_inv_${date}_${num.replace(/\W/g,"_")}_${Math.abs(total).toFixed(0)}`;
    invoices.push({ id:invId, num, customer, date, month, total, status, memo, type });
  }

  return { invoices };
}

// ─── QBO Expense/Bills CSV Parser ────────────────────────────────────────────
// QBO Expense/Bills export columns (typical):
//   Date, Num, Vendor, Category, Memo, Amount, Account
// or from "Expense by Vendor Summary" / "Transaction List by Vendor"
function parseExpenseCSV(text) {
  const raw = parseCSV(text);
  if (raw.length < 2) return { error:"File too short", expenses:[] };

  let headerIdx = -1, headers = [];
  for (let i=0; i<Math.min(10, raw.length); i++) {
    const cols = raw[i].map(c=>String(c).toLowerCase().trim());
    if (cols.includes("date") && (cols.includes("amount") || cols.includes("total")) &&
        (cols.includes("vendor") || cols.includes("name") || cols.includes("payee") || cols.includes("category"))) {
      headerIdx = i;
      headers = raw[i].map(c=>String(c).trim().toLowerCase().replace(/[^a-z0-9]/g,"_"));
      break;
    }
  }
  if (headerIdx === -1) return { error:"Could not find column headers. Expected 'Date', 'Vendor/Name', 'Amount' columns.", expenses:[] };

  const col = (row, ...names) => {
    for (const n of names) {
      const idx = headers.indexOf(n);
      if (idx !== -1 && row[idx]) return String(row[idx]).trim();
    }
    return "";
  };

  const expenses = [];
  for (let i=headerIdx+1; i<raw.length; i++) {
    const row = raw[i];
    if (!row[0] && !row[1]) continue;
    const rawDate = col(row,"date","txn_date");
    const date = parseDate(rawDate);
    if (!date) continue;

    const vendor   = col(row,"vendor","name","payee","vendor_payee");
    const category = col(row,"category","account","expense_category","split");
    const memo     = col(row,"memo","description","memo_description") || "";
    const num      = col(row,"num","ref_no","check_num") || "";
    const type     = col(row,"type","transaction_type") || "Expense";
    const amount   = Math.abs(parseMoney(col(row,"amount","total","debit")));

    if (!amount && !vendor) continue;

    const month = toMonthKey(date);
    const expId = `qbo_exp_${date}_${vendor.replace(/\W/g,"_").slice(0,20)}_${amount.toFixed(0)}_${i}`;
    expenses.push({ id:expId, vendor, category, date, month, amount, memo, num, type });
  }

  return { expenses };
}

// ─── Badge ───────────────────────────────────────────────────────────────────
const Badge = ({text, color, bg}) => (
  <span style={{padding:"2px 8px",borderRadius:20,fontSize:10,fontWeight:700,color:color||T.blueText,background:bg||T.blueBg,display:"inline-block"}}>{text}</span>
);

// ─── Upload Zone ─────────────────────────────────────────────────────────────
function UploadZone({ label, accept, onFile, status, icon }) {
  const [drag, setDrag] = useState(false);
  const handleDrop = useCallback(e => {
    e.preventDefault(); setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) onFile(f);
  }, [onFile]);
  const handleChange = useCallback(e => { const f=e.target.files[0]; if(f) onFile(f); e.target.value=""; }, [onFile]);

  return (
    <div
      onDragOver={e=>{e.preventDefault();setDrag(true);}}
      onDragLeave={()=>setDrag(false)}
      onDrop={handleDrop}
      style={{
        border:`2px dashed ${drag?T.brand:T.border}`,
        borderRadius:T.radius,
        padding:"24px 16px",
        textAlign:"center",
        background:drag?T.brandPale:T.bgSurface,
        transition:"all 0.2s",
        cursor:"pointer",
      }}
      onClick={()=>document.getElementById(`qbo-upload-${label.replace(/\W/g,"_")}`).click()}
    >
      <input id={`qbo-upload-${label.replace(/\W/g,"_")}`} type="file" accept={accept||".csv"} style={{display:"none"}} onChange={handleChange} />
      <div style={{fontSize:28,marginBottom:6}}>{icon||"📄"}</div>
      <div style={{fontSize:13,fontWeight:600,color:T.text,marginBottom:4}}>{label}</div>
      <div style={{fontSize:11,color:T.textMuted}}>Drag & drop or click to browse</div>
      {status && <div style={{marginTop:10,fontSize:11,color:status.startsWith("✓")?T.green:status.startsWith("⚠")||status.startsWith("✗")?T.red:T.brand,fontWeight:600}}>{status}</div>}
    </div>
  );
}

// ─── Section Title ────────────────────────────────────────────────────────────
const SectionTitle = ({icon, text, right}) => (
  <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
    <div style={{display:"flex",alignItems:"center",gap:8}}>
      <span style={{fontSize:18}}>{icon}</span>
      <span style={{fontSize:15,fontWeight:700,color:T.text}}>{text}</span>
    </div>
    {right}
  </div>
);

// ─── P&L Monthly View ────────────────────────────────────────────────────────
function PLView({ months }) {
  const sorted = [...months].sort((a,b)=>b.month.localeCompare(a.month));
  if (!sorted.length) return (
    <div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}>
      <div style={{fontSize:36,marginBottom:8}}>📊</div>
      <div style={{fontSize:13,fontWeight:600}}>No P&L data yet</div>
      <div style={{fontSize:11,marginTop:4}}>Upload a QBO Profit & Loss CSV above to get started</div>
    </div>
  );

  // Summary metrics
  const totIncome   = sorted.reduce((s,m)=>s+(m.income||0),0);
  const totExpenses = sorted.reduce((s,m)=>s+(m.expenses||0),0);
  const totNet      = sorted.reduce((s,m)=>s+(m.net_income||0),0);
  const avgMargin   = sorted.length ? sorted.reduce((s,m)=>s+(m.net_margin_pct||0),0)/sorted.length : 0;

  const KPI = ({label,value,sub,color}) => (
    <div style={{...card,flex:1,minWidth:160}}>
      <div style={{fontSize:11,color:T.textMuted,marginBottom:4,fontWeight:600,textTransform:"uppercase",letterSpacing:"0.05em"}}>{label}</div>
      <div style={{fontSize:22,fontWeight:800,color:color||T.text}}>{value}</div>
      {sub && <div style={{fontSize:10,color:T.textDim,marginTop:2}}>{sub}</div>}
    </div>
  );

  return (
    <div>
      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:20}}>
        <KPI label="Total Revenue" value={fmtK(totIncome)} sub={`${sorted.length} months`} color={T.brand} />
        <KPI label="Total Expenses" value={fmtK(totExpenses)} color={T.red} />
        <KPI label="Net Income" value={fmtK(totNet)} color={totNet>=0?T.green:T.red} />
        <KPI label="Avg Net Margin" value={fmtPct(avgMargin)} color={avgMargin>=0?T.green:T.red} />
      </div>

      <div style={{...card, padding:0, overflow:"hidden"}}>
        <table style={{width:"100%", borderCollapse:"collapse"}}>
          <thead>
            <tr>
              <th style={tblH}>Month</th>
              <th style={{...tblH,textAlign:"right"}}>Revenue</th>
              <th style={{...tblH,textAlign:"right"}}>COGS</th>
              <th style={{...tblH,textAlign:"right"}}>Gross Profit</th>
              <th style={{...tblH,textAlign:"right"}}>Expenses</th>
              <th style={{...tblH,textAlign:"right"}}>Net Income</th>
              <th style={{...tblH,textAlign:"right"}}>Net %</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(m => (
              <tr key={m.month} style={{background:m.net_income<0?T.redBg:undefined}}>
                <td style={{...tblD,fontWeight:600}}>{monthLabel(m.month)}</td>
                <td style={tblDR}>{fmt(m.income)}</td>
                <td style={{...tblDR,color:T.textMuted}}>{m.cogs?fmt(m.cogs):"—"}</td>
                <td style={{...tblDR,color:m.gross_profit>=0?T.green:T.red}}>{fmt(m.gross_profit)}</td>
                <td style={{...tblDR,color:T.red}}>{fmt(m.expenses)}</td>
                <td style={{...tblDR,fontWeight:700,color:m.net_income>=0?T.green:T.red}}>{fmt(m.net_income)}</td>
                <td style={{...tblDR,color:m.net_margin_pct>=0?T.greenText:T.redText}}>
                  {fmtPct(m.net_margin_pct)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Invoice View ─────────────────────────────────────────────────────────────
function InvoiceView({ invoices }) {
  const [filterMonth, setFilterMonth] = useState("all");
  const [sortKey, setSortKey] = useState("date");
  const [sortDir, setSortDir] = useState(-1);

  const months = useMemo(() => {
    const s = new Set(invoices.map(i=>i.month).filter(Boolean));
    return ["all", ...Array.from(s).sort((a,b)=>b.localeCompare(a))];
  }, [invoices]);

  const filtered = useMemo(() => {
    let list = filterMonth==="all" ? invoices : invoices.filter(i=>i.month===filterMonth);
    list = [...list].sort((a,b)=>{
      const av = a[sortKey]||"", bv = b[sortKey]||"";
      if (sortKey==="total") return sortDir*(((b.total||0)-(a.total||0)));
      return sortDir*(av<bv?-1:av>bv?1:0);
    });
    return list;
  }, [invoices, filterMonth, sortKey, sortDir]);

  const total = filtered.reduce((s,i)=>s+(i.total||0),0);

  if (!invoices.length) return (
    <div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}>
      <div style={{fontSize:36,marginBottom:8}}>🧾</div>
      <div style={{fontSize:13,fontWeight:600}}>No invoice data yet</div>
      <div style={{fontSize:11,marginTop:4}}>Upload a QBO Invoice List CSV above</div>
    </div>
  );

  const SortTh = ({k, label, right}) => (
    <th style={{...tblH, textAlign:right?"right":"left", cursor:"pointer"}}
        onClick={()=>{if(sortKey===k)setSortDir(-sortDir);else{setSortKey(k);setSortDir(-1);}}}>
      {label}{sortKey===k?(sortDir===-1?" ↓":" ↑"):""}
    </th>
  );

  return (
    <div>
      <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:12,flexWrap:"wrap"}}>
        <select value={filterMonth} onChange={e=>setFilterMonth(e.target.value)}
          style={{padding:"6px 10px",borderRadius:T.radiusSm,border:`1px solid ${T.border}`,fontSize:12,background:T.bgWhite}}>
          {months.map(m=><option key={m} value={m}>{m==="all"?"All Months":monthLabel(m)}</option>)}
        </select>
        <span style={{fontSize:12,color:T.textMuted}}>{fmtNum(filtered.length)} invoices • {fmtK(total)} total</span>
      </div>
      <div style={{...card, padding:0, overflow:"auto", maxHeight:600}}>
        <table style={{width:"100%",borderCollapse:"collapse",minWidth:600}}>
          <thead>
            <tr>
              <SortTh k="date" label="Date" />
              <SortTh k="num" label="Num" />
              <SortTh k="customer" label="Customer" />
              <SortTh k="type" label="Type" />
              <th style={tblH}>Memo</th>
              <SortTh k="total" label="Amount" right />
            </tr>
          </thead>
          <tbody>
            {filtered.map((inv,idx) => (
              <tr key={inv.id||idx}>
                <td style={tblD}>{inv.date}</td>
                <td style={{...tblD,color:T.textMuted,fontFamily:"monospace",fontSize:11}}>{inv.num||"—"}</td>
                <td style={{...tblD,fontWeight:500}}>{inv.customer||"—"}</td>
                <td style={tblD}><Badge text={inv.type||"Invoice"} color={T.blueText} bg={T.blueBg} /></td>
                <td style={{...tblD,color:T.textMuted,fontSize:11,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{inv.memo||"—"}</td>
                <td style={{...tblDR,fontWeight:600,color:inv.total<0?T.red:T.text}}>{fmt(inv.total)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Expense View ─────────────────────────────────────────────────────────────
function ExpenseView({ expenses }) {
  const [filterMonth, setFilterMonth] = useState("all");
  const [groupBy, setGroupBy] = useState("vendor");
  const [sortKey, setSortKey] = useState("amount");
  const [sortDir, setSortDir] = useState(-1);

  const months = useMemo(() => {
    const s = new Set(expenses.map(e=>e.month).filter(Boolean));
    return ["all", ...Array.from(s).sort((a,b)=>b.localeCompare(a))];
  }, [expenses]);

  const filtered = useMemo(() => {
    return filterMonth==="all" ? expenses : expenses.filter(e=>e.month===filterMonth);
  }, [expenses, filterMonth]);

  const grouped = useMemo(() => {
    const g = {};
    for (const e of filtered) {
      const key = groupBy==="vendor" ? (e.vendor||"Unknown") : (e.category||"Uncategorized");
      if (!g[key]) g[key] = {label:key, total:0, count:0};
      g[key].total += e.amount||0;
      g[key].count++;
    }
    return Object.values(g).sort((a,b)=>b.total-a.total);
  }, [filtered, groupBy]);

  const grandTotal = filtered.reduce((s,e)=>s+(e.amount||0),0);

  if (!expenses.length) return (
    <div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}>
      <div style={{fontSize:36,marginBottom:8}}>💸</div>
      <div style={{fontSize:13,fontWeight:600}}>No expense data yet</div>
      <div style={{fontSize:11,marginTop:4}}>Upload a QBO Expense/Bills CSV above</div>
    </div>
  );

  return (
    <div>
      <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:12,flexWrap:"wrap"}}>
        <select value={filterMonth} onChange={e=>setFilterMonth(e.target.value)}
          style={{padding:"6px 10px",borderRadius:T.radiusSm,border:`1px solid ${T.border}`,fontSize:12,background:T.bgWhite}}>
          {months.map(m=><option key={m} value={m}>{m==="all"?"All Months":monthLabel(m)}</option>)}
        </select>
        <select value={groupBy} onChange={e=>setGroupBy(e.target.value)}
          style={{padding:"6px 10px",borderRadius:T.radiusSm,border:`1px solid ${T.border}`,fontSize:12,background:T.bgWhite}}>
          <option value="vendor">Group by Vendor</option>
          <option value="category">Group by Category</option>
        </select>
        <span style={{fontSize:12,color:T.textMuted}}>{fmtNum(filtered.length)} transactions • {fmtK(grandTotal)} total</span>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"1fr 2fr",gap:16}}>
        {/* Summary by group */}
        <div style={{...card, padding:0, overflow:"auto", maxHeight:580}}>
          <div style={{padding:"10px 14px",fontSize:11,fontWeight:700,color:T.textMuted,textTransform:"uppercase",borderBottom:`1px solid ${T.border}`,background:T.bgSurface}}>
            {groupBy==="vendor"?"By Vendor":"By Category"}
          </div>
          {grouped.map((g,i) => (
            <div key={g.label} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"10px 14px",borderBottom:i<grouped.length-1?`1px solid ${T.borderLight}`:undefined}}>
              <div>
                <div style={{fontSize:12,fontWeight:600,color:T.text}}>{g.label}</div>
                <div style={{fontSize:10,color:T.textDim}}>{g.count} transactions</div>
              </div>
              <div style={{fontWeight:700,color:T.red,fontSize:13}}>{fmtK(g.total)}</div>
            </div>
          ))}
        </div>

        {/* Transaction detail */}
        <div style={{...card, padding:0, overflow:"auto", maxHeight:580}}>
          <table style={{width:"100%",borderCollapse:"collapse"}}>
            <thead>
              <tr>
                <th style={tblH}>Date</th>
                <th style={tblH}>Vendor</th>
                <th style={tblH}>Category</th>
                <th style={{...tblH,textAlign:"right"}}>Amount</th>
              </tr>
            </thead>
            <tbody>
              {[...filtered].sort((a,b)=>b.date.localeCompare(a.date)).slice(0,500).map((e,idx)=>(
                <tr key={e.id||idx}>
                  <td style={{...tblD,color:T.textMuted,whiteSpace:"nowrap"}}>{e.date}</td>
                  <td style={{...tblD,fontWeight:500}}>{e.vendor||"—"}</td>
                  <td style={{...tblD,color:T.textMuted,fontSize:11}}>{e.category||"—"}</td>
                  <td style={{...tblDR,color:T.red,fontWeight:600}}>{fmt(e.amount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── Main QBOImportTab Component ──────────────────────────────────────────────
function QBOImportTab() {
  const [activeView, setActiveView] = useState("pl");

  // P&L state
  const [plMonths, setPLMonths]     = useState([]);
  const [plStatus, setPLStatus]     = useState("");
  const [plUploading, setPLUploading] = useState(false);

  // Invoice state
  const [invoices, setInvoices]     = useState([]);
  const [invStatus, setInvStatus]   = useState("");
  const [invUploading, setInvUploading] = useState(false);

  // Expense state
  const [expenses, setExpenses]     = useState([]);
  const [expStatus, setExpStatus]   = useState("");
  const [expUploading, setExpUploading] = useState(false);

  const [loading, setLoading]       = useState(true);

  // Load existing data from Firestore
  useEffect(() => {
    (async () => {
      setLoading(true);
      const [pl, inv, exp] = await Promise.all([getPLMonthly(), getInvoices(), getExpenses()]);
      setPLMonths(pl);
      setInvoices(inv);
      setExpenses(exp);
      setLoading(false);
    })();
  }, []);

  // ─── Handle P&L Upload ───────────────────────────────────────────────────
  const handlePLFile = useCallback(async file => {
    setPLStatus(`Reading ${file.name}...`);
    setPLUploading(true);
    try {
      const text = await file.text();
      const result = parsePLCSV(text);
      if (result.error) { setPLStatus(`✗ ${result.error}`); setPLUploading(false); return; }
      if (!result.months.length) { setPLStatus("✗ No monthly data found. Check file format."); setPLUploading(false); return; }

      // Handle single-period file — ask user for month
      let monthsToSave = result.months;
      if (monthsToSave.length===1 && monthsToSave[0].month==="SINGLE") {
        const input = window.prompt(
          `Single-period P&L detected${result.singlePeriod?" ("+result.singlePeriod+")":""}.\n\nEnter the month for this report (YYYY-MM, e.g. 2025-03):`
        );
        if (!input || !/^\d{4}-\d{2}$/.test(input.trim())) {
          setPLStatus("✗ Cancelled — no month provided");
          setPLUploading(false);
          return;
        }
        monthsToSave = [{...monthsToSave[0], month:input.trim()}];
      }

      setPLStatus(`Saving ${monthsToSave.length} month(s) to Firestore...`);
      let saved = 0;
      for (const m of monthsToSave) {
        const ok = await savePLMonthly(m.month, m);
        if (ok) saved++;
      }
      // Reload
      const fresh = await getPLMonthly();
      setPLMonths(fresh);
      setPLStatus(`✓ Saved ${saved} month(s) — ${monthsToSave.map(m=>monthLabel(m.month)).join(", ")}`);
    } catch(e) {
      setPLStatus(`✗ Error: ${e.message}`);
    }
    setPLUploading(false);
  }, []);

  // ─── Handle Invoice Upload ───────────────────────────────────────────────
  const handleInvFile = useCallback(async file => {
    setInvStatus(`Reading ${file.name}...`);
    setInvUploading(true);
    try {
      const text = await file.text();
      const result = parseInvoiceCSV(text);
      if (result.error) { setInvStatus(`✗ ${result.error}`); setInvUploading(false); return; }
      if (!result.invoices.length) { setInvStatus("✗ No invoices found. Check file format."); setInvUploading(false); return; }

      setInvStatus(`Saving ${result.invoices.length} invoices...`);
      let saved = 0;
      // Batch in groups of 20 to avoid timeout
      for (const inv of result.invoices) {
        const ok = await saveInvoice(inv.id, inv);
        if (ok) saved++;
      }
      const fresh = await getInvoices();
      setInvoices(fresh);
      setInvStatus(`✓ Saved ${saved} invoices from ${file.name}`);
    } catch(e) {
      setInvStatus(`✗ Error: ${e.message}`);
    }
    setInvUploading(false);
  }, []);

  // ─── Handle Expense Upload ───────────────────────────────────────────────
  const handleExpFile = useCallback(async file => {
    setExpStatus(`Reading ${file.name}...`);
    setExpUploading(true);
    try {
      const text = await file.text();
      const result = parseExpenseCSV(text);
      if (result.error) { setExpStatus(`✗ ${result.error}`); setExpUploading(false); return; }
      if (!result.expenses.length) { setExpStatus("✗ No expenses found. Check file format."); setExpUploading(false); return; }

      setExpStatus(`Saving ${result.expenses.length} expenses...`);
      let saved = 0;
      for (const exp of result.expenses) {
        const ok = await saveExpense(exp.id, exp);
        if (ok) saved++;
      }
      const fresh = await getExpenses();
      setExpenses(fresh);
      setExpStatus(`✓ Saved ${saved} expenses from ${file.name}`);
    } catch(e) {
      setExpStatus(`✗ Error: ${e.message}`);
    }
    setExpUploading(false);
  }, []);

  if (loading) return (
    <div style={{textAlign:"center",padding:"60px 20px",color:T.textMuted}}>
      <div style={{fontSize:36,marginBottom:12}}>📊</div>
      <div style={{fontSize:14}}>Loading QuickBooks data...</div>
    </div>
  );

  const views = [
    {id:"pl",    icon:"📈", label:"Profit & Loss",  count:plMonths.length,    unit:"months"},
    {id:"inv",   icon:"🧾", label:"Invoices",        count:invoices.length,    unit:"records"},
    {id:"exp",   icon:"💸", label:"Expenses & Bills",count:expenses.length,    unit:"records"},
    {id:"upload",icon:"📤", label:"Upload Data",     count:null},
  ];

  return (
    <div style={{padding:"20px",maxWidth:1400,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📊" text="QuickBooks Data"
        right={<span style={{fontSize:11,color:T.textMuted}}>Manual CSV import — export from QBO Reports → Export to Excel/CSV</span>}
      />

      {/* View selector */}
      <div style={{display:"flex",gap:6,marginBottom:20,flexWrap:"wrap"}}>
        {views.map(v => (
          <button key={v.id} onClick={()=>setActiveView(v.id)} style={{
            padding:"8px 16px", borderRadius:T.radiusSm, border:"none",
            background:activeView===v.id?T.brand:"transparent",
            color:activeView===v.id?"#fff":T.textMuted,
            fontSize:12, fontWeight:activeView===v.id?700:500,
            cursor:"pointer", transition:"all 0.2s",
            display:"flex", alignItems:"center", gap:6,
          }}>
            <span>{v.icon}</span>
            <span>{v.label}</span>
            {v.count!=null && <Badge text={v.count} color={activeView===v.id?"rgba(255,255,255,0.9)":T.blueText} bg={activeView===v.id?"rgba(255,255,255,0.2)":T.blueBg} />}
          </button>
        ))}
      </div>

      {/* Upload Panel */}
      {activeView === "upload" && (
        <div>
          <div style={{...card,marginBottom:16,background:T.brandPale,borderColor:T.brand}}>
            <div style={{fontSize:13,fontWeight:700,color:T.brand,marginBottom:8}}>📋 How to export from QuickBooks Online</div>
            <div style={{fontSize:12,color:T.text,lineHeight:1.7}}>
              <strong>P&L:</strong> Reports → Profit and Loss → set date range (month or custom) → Export → Export to CSV<br/>
              <strong>Invoice List:</strong> Reports → Invoice List (or Sales by Customer Detail) → set date range → Export to CSV<br/>
              <strong>Expenses/Bills:</strong> Reports → Transaction List by Vendor (or Expenses by Vendor Summary) → Export to CSV<br/>
              <br/>
              <span style={{color:T.textMuted}}>Tip: For P&L, you can export a full year at once with monthly columns by selecting "Display columns by: Month" before exporting.</span>
            </div>
          </div>

          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:16}}>
            <div style={card}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>📈 Profit & Loss</div>
              <UploadZone
                label={plUploading ? "Processing..." : "Drop P&L CSV here"}
                icon="📈"
                onFile={handlePLFile}
                status={plStatus}
              />
              <div style={{fontSize:11,color:T.textDim,marginTop:8}}>
                {plMonths.length} month(s) in database
              </div>
            </div>

            <div style={card}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>🧾 Invoice List</div>
              <UploadZone
                label={invUploading ? "Processing..." : "Drop Invoice CSV here"}
                icon="🧾"
                onFile={handleInvFile}
                status={invStatus}
              />
              <div style={{fontSize:11,color:T.textDim,marginTop:8}}>
                {invoices.length} invoice(s) in database
              </div>
            </div>

            <div style={card}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>💸 Expenses & Bills</div>
              <UploadZone
                label={expUploading ? "Processing..." : "Drop Expense CSV here"}
                icon="💸"
                onFile={handleExpFile}
                status={expStatus}
              />
              <div style={{fontSize:11,color:T.textDim,marginTop:8}}>
                {expenses.length} expense transaction(s) in database
              </div>
            </div>
          </div>
        </div>
      )}

      {activeView === "pl"  && <PLView months={plMonths} />}
      {activeView === "inv" && <InvoiceView invoices={invoices} />}
      {activeView === "exp" && <ExpenseView expenses={expenses} />}

    </div>
  );
}

// Expose to window so MarginIQ.jsx can render it
window.QBOImportTab = QBOImportTab;

})();
