// Davis MarginIQ v2.5 — Cost Intelligence Platform
// Revenue (billed) drives margins. Reconciliation (paid) is separate monitoring.
// v2.3: Multi-source ingest (Uline + NuVizz + Time Clock + Payroll + QBO)
// v2.3.1: Drivers tab for W2/1099 classification
// v2.4: Gmail Sync — auto-import NuVizz + Uline reports from inbox via OAuth
// v2.4.1: Labor Reality Check on Command Center (actual vs estimated)
// v2.5: AuditIQ — Revenue recovery module
// v2.6: Fuel tab — FuelFox PDF pair parser with true $/gal (fuel+tax+delivery
//       baked in). Weekly per-vendor comparison (FuelFox vs Quick Fuel).
//       Per-truck fuel history. Quick Fuel parser scaffolded but pending sample.
// v2.6.1: Rewrote FuelFox parser for new pdf-parse PDFParse class API.
//         Delivery fee baked into true $/gal (all-in rate = $4.67/gal for DD404).
// v2.6.3: Use createRequire for pdf-parse (ESM/CJS interop fix).
// v2.7.0: Gmail Sync FuelFox pair + Quick Fuel stub.
// v2.8.0: ARCHITECTURAL PIVOT — fuel parsing now uses Claude vision API.
// v2.8.1: Quick Fuel — extract invoice-level fees (Regulatory Compliance Fee
//         $4.99 on CFS-4582698) and redistribute proportionally by gallons
//         across all trucks, matching the FuelFox overhead pattern. Per-truck
//         true cost now ties out exactly to invoice total.

const { useState, useEffect, useCallback, useRef, useMemo } = React;
const APP_VERSION = "2.9.0";

// ─── Design Tokens ──────────────────────────────────────────
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

const DEFAULT_COSTS = {
  warehouse: 450000, forklifts: 84000, forklift_operators: 416000,
  truck_insurance_monthly: 1000, truck_count_box: 30, truck_count_tractor: 20,
  rate_box_driver: 23, rate_tractor_driver: 27.50, rate_dispatcher: 20, rate_admin: 18, rate_mechanic: 28,
  count_box_drivers: 16, count_tractor_drivers: 19, count_dispatchers: 2, count_admin: 3, count_mechanics: 2, count_forklift_ops: 10,
  mpg_box: 8, mpg_tractor: 6, fuel_price: 3.50,
  working_days_year: 260, avg_hours_per_shift: 10, contractor_pct: 0.40,
};

// Earliest date we consider "business started" — before this, sparse data is expected
const BUSINESS_START = "2023-03-01";

// ─── Formatters ──────────────────────────────────────────────
const fmt = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1000000) return "$"+(v/1000000).toFixed(1)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtDec = (n,d=2) => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":fmtDec(n,d)+"%";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

const puToDate = pu => { if (!pu) return null; const s = String(Math.floor(pu)); if (s.length !== 8) return null; return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`; };
const puToMonth = pu => { if (!pu) return null; const s = String(Math.floor(pu)); if (s.length !== 8) return null; return `${s.slice(0,4)}-${s.slice(4,6)}`; };
const parseDateMDY = s => { if (!s) return null; const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (!m) return null; return `${m[3]}-${m[1].padStart(2,"0")}-${m[2].padStart(2,"0")}`; };
const dateToMonth = d => d ? d.slice(0,7) : null;

// NuVizz format: "3/30/26 01:18 PM" or "3/30/2026 1:18 PM" — extract date portion only
// Also handles Excel serial dates (numbers) that SheetJS produces when reading CSV date columns.
function parseDateMDYFlexible(s) {
  if (s == null || s === "") return null;
  // Excel serial date: integer or float days since 1899-12-30 (SheetJS convention)
  if (typeof s === "number" && isFinite(s)) {
    // Excel epoch is 1899-12-30 in JS land (accounts for the 1900 leap-year bug)
    const ms = Math.round((s - 25569) * 86400 * 1000); // 25569 = days from 1970-01-01 to 1899-12-30 epoch
    const d = new Date(ms);
    if (isNaN(d)) return null;
    const y = d.getUTCFullYear();
    const mo = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${mo}-${dd}`;
  }
  const str = String(s).trim();
  // Match M/D/YY or M/D/YYYY, optional time after
  const m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (!m) return null;
  let [, mo, d, y] = m;
  if (y.length === 2) y = (parseInt(y) >= 70 ? "19" : "20") + y;
  return `${y}-${mo.padStart(2,"0")}-${d.padStart(2,"0")}`;
}

// Parse "$150.00" or "150.00" or "$1,250.50" to number; returns 0 if not a money string
function parseMoney(v) {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  const s = String(v).replace(/[$,\s]/g,"").trim();
  if (!s) return 0;
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

// Normalize employee/driver name: "LAST, FIRST" or "First Last" → "First Last"
function normalizeName(v) {
  if (!v) return null;
  let s = String(v).trim();
  if (!s) return null;
  if (s.includes(",")) {
    const [last, first] = s.split(",").map(x=>x.trim());
    if (first && last) s = `${first} ${last}`;
  }
  return s.replace(/\s+/g," ").split(" ").map(w => w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(" ");
}

// Firestore-safe key for a driver name: "Chris Head" → "chris_head"
function driverKey(name) {
  if (!name) return null;
  return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g,"_").replace(/^_+|_+$/g,"").slice(0, 140) || null;
}

// Customer name → Firestore-safe key (same slug style)
function customerKey(name) {
  return driverKey(name);
}

// ─── Audit Categorization ───────────────────────────────────
// Categorize a billed-vs-paid comparison into one of the spec's buckets.
function categorize(billed, paid, hasAccessorial, accessorialPaid, ageDays) {
  if (billed <= 0) return "orphan";
  const ratio = paid / billed;
  if (ratio >= 0.99) return "paid_in_full";
  if (ratio >= 0.5 && ratio < 0.98) return "short_paid";
  if (paid <= 0 && ageDays >= 45) return "zero_pay";
  if (paid <= 0) return "short_paid"; // too new to call zero-pay yet
  if (ratio > 1.01) return "overpaid";
  // Special case: base paid but accessorial ignored
  if (hasAccessorial && accessorialPaid <= 0 && ratio >= 0.5) return "accessorial_ignored";
  return "short_paid";
}

function ageBucket(ageDays) {
  if (ageDays == null || ageDays < 0) return "unknown";
  if (ageDays <= 30) return "0-30";
  if (ageDays <= 60) return "31-60";
  if (ageDays <= 90) return "61-90";
  if (ageDays <= 180) return "91-180";
  if (ageDays <= 365) return "181-365";
  return "365+";
}

const AGE_BUCKETS = ["0-30","31-60","61-90","91-180","181-365","365+"];
const CATEGORIES = ["paid_in_full","short_paid","accessorial_ignored","zero_pay","overpaid","orphan"];
const CATEGORY_COLORS = {
  paid_in_full: "#10b981",
  short_paid: "#f59e0b",
  accessorial_ignored: "#f97316",
  zero_pay: "#ef4444",
  overpaid: "#3b82f6",
  orphan: "#94a3b8",
  written_off: "#64748b",
};
const CATEGORY_LABELS = {
  paid_in_full: "Paid in Full",
  short_paid: "Short-paid",
  accessorial_ignored: "Accessorial Ignored",
  zero_pay: "Zero-pay",
  overpaid: "Overpaid",
  orphan: "Orphan",
};

function daysBetween(fromISO, toISO) {
  if (!fromISO) return null;
  const a = new Date(fromISO + "T00:00:00");
  const b = new Date((toISO || new Date().toISOString().slice(0,10)) + "T00:00:00");
  if (isNaN(a) || isNaN(b)) return null;
  return Math.floor((b - a) / (1000*60*60*24));
}

// ─── Shared Ingest Pipeline ─────────────────────────────────
// Takes an array of File objects + a status callback, returns a summary.
// Used by both the manual upload flow and the Gmail auto-import flow.
async function ingestFiles(files, onStatus = () => {}) {
  const stopsByPro = {};
  const paymentByPro = {};
  const ddisFileRecords = [];
  const nuvizzStops = [];
  const timeClockEntries = [];
  const payrollEntries = [];
  const qboEntries = [];
  const fileLogs = [];
  const sourceFilesLog = [];
  const countsByKind = { master:0, original:0, accessorials:0, ddis:0, nuvizz:0, timeclock:0, payroll:0, qbo_pl:0, qbo_tb:0, qbo_gl:0, unknown:0 };
  const unknownFiles = [];

  for (let i=0; i<files.length; i++) {
    const file = files[i];
    onStatus({ phase:"read", current:i+1, total:files.length, name:file.name });
    const fileId = file.name.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);
    try {
      let rows;
      const isCSV = file.name.toLowerCase().endsWith(".csv");
      if (isCSV) rows = await readCSV(file);
      else rows = await readWorkbook(file);
      if (!rows || rows.length === 0) { unknownFiles.push(file.name + " (empty)"); countsByKind.unknown++; continue; }
      const kind = detectFileType(file.name, rows[0]);
      const group = sourceGroup(kind);
      countsByKind[kind] = (countsByKind[kind]||0) + 1;
      fileLogs.push({ file_id: fileId, filename: file.name, kind, group, row_count: rows.length, uploaded_at: new Date().toISOString() });
      sourceFilesLog.push({ file_id: fileId, filename: file.name, kind, group, row_count: rows.length, uploaded_at: new Date().toISOString() });
      if (kind === "master" || kind === "original" || kind === "accessorials") {
        const stops = parseOriginalOrAccessorial(rows);
        for (const s of stops) {
          if (!s.pro || !s.week_ending) continue;
          const existing = stopsByPro[s.pro];
          if (!existing || (s.new_cost > existing.new_cost)) stopsByPro[s.pro] = s;
        }
      } else if (kind === "ddis") {
        const payments = parseDDIS(rows);
        const billDates = payments.map(p => p.bill_date).filter(Boolean).sort();
        const totalPaid = payments.reduce((s,p) => s + p.paid, 0);
        ddisFileRecords.push({
          file_id: fileId, filename: file.name,
          record_count: payments.length, total_paid: totalPaid,
          earliest_bill_date: billDates[0] || null,
          latest_bill_date: billDates[billDates.length-1] || null,
          checks: [...new Set(payments.map(p => p.check).filter(Boolean))],
          uploaded_at: new Date().toISOString(),
        });
        for (const p of payments) paymentByPro[p.pro] = (paymentByPro[p.pro] || 0) + p.paid;
      } else if (kind === "nuvizz") {
        nuvizzStops.push(...parseNuVizz(rows));
      } else if (kind === "timeclock") {
        timeClockEntries.push(...parseTimeClock(rows));
      } else if (kind === "payroll") {
        payrollEntries.push(...parsePayroll(rows));
      } else if (kind === "qbo_pl" || kind === "qbo_tb" || kind === "qbo_gl") {
        qboEntries.push(...parseQBO(rows, kind).map(e => ({...e, source_file: file.name})));
      } else {
        unknownFiles.push(file.name);
      }
    } catch(e) {
      console.error("ingest err:", file.name, e);
      unknownFiles.push(file.name + " (err: " + e.message + ")");
      countsByKind.unknown++;
    }
  }

  // Uline rollups
  onStatus({ phase:"rollup", message:"Building Uline weekly rollups..." });
  const allStops = Object.values(stopsByPro);
  const rollups = buildWeeklyRollups(allStops);
  const reconByWeek = {};
  const unpaidStops = [];
  const auditItems = [];  // v2.5 AuditIQ: all stops with any variance, categorized
  const today = new Date().toISOString().slice(0,10);
  for (const s of allStops) {
    const paid = paymentByPro[s.pro] || 0;
    if (!reconByWeek[s.week_ending]) reconByWeek[s.week_ending] = { week_ending: s.week_ending, month: s.month, billed:0, paid_matched:0, unpaid_count:0, unpaid_amount:0 };
    reconByWeek[s.week_ending].billed += s.new_cost || 0;
    reconByWeek[s.week_ending].paid_matched += paid;
    const billed = s.new_cost || 0;
    const variance = billed - paid;
    const variancePct = billed > 0 ? (variance / billed * 100) : null;
    if (paid === 0 && billed > 0) {
      reconByWeek[s.week_ending].unpaid_count++;
      reconByWeek[s.week_ending].unpaid_amount += billed;
      if (unpaidStops.length < 2000) {
        unpaidStops.push({
          pro: s.pro, customer: s.customer, city: s.city, state: s.state, zip: s.zip,
          pu_date: s.pu_date, week_ending: s.week_ending, month: s.month, billed: billed,
          code: s.code, weight: s.weight, order: s.order
        });
      }
    }
    // v2.5 AuditIQ — track any stop with variance > $1 (both unpaid AND short-paid)
    if (billed > 0 && variance > 1) {
      const ageDays = daysBetween(s.pu_date, today);
      const hasAcc = !!s.extra_cost && s.extra_cost > 0;
      const category = categorize(billed, paid, hasAcc, 0 /* unknown acc-paid split */, ageDays);
      if (auditItems.length < 3000) {
        auditItems.push({
          pro: s.pro,
          customer: s.customer || "Unknown",
          customer_key: customerKey(s.customer),
          city: s.city, state: s.state, zip: s.zip,
          pu_date: s.pu_date, week_ending: s.week_ending, month: s.month,
          billed, paid, variance,
          variance_pct: variancePct,
          accessorial_amount: s.extra_cost || 0,
          base_cost: s.cost || 0,
          code: s.code, weight: s.weight, order: s.order,
          age_days: ageDays,
          age_bucket: ageBucket(ageDays),
          category,
          dispute_status: "new", // new | queued | sent | won | lost | partial | written_off
          notes: [],
          updated_at: new Date().toISOString(),
        });
      }
    }
  }
  for (const r of Object.values(reconByWeek)) r.collection_rate = r.billed > 0 ? (r.paid_matched / r.billed * 100) : null;

  onStatus({ phase:"save", message:"Saving to Firebase..." });
  let savedWeeks = 0, savedRecon = 0, savedAudit = 0;
  for (const r of rollups) { if (await FS.saveWeeklyRollup(r.week_ending, {...r, updated_at:new Date().toISOString()})) savedWeeks++; }
  for (const r of Object.values(reconByWeek)) { if (await FS.saveReconWeekly(r.week_ending, {...r, updated_at:new Date().toISOString()})) savedRecon++; }
  for (const f of ddisFileRecords) await FS.saveDDISFile(f.file_id, f);
  const topUnpaid = unpaidStops.sort((a,b) => b.billed - a.billed).slice(0, 500);
  for (const s of topUnpaid) await FS.saveUnpaidStop(s.pro, s);
  // v2.5 save audit_items, top 1500 by variance
  const topAudit = auditItems.sort((a,b) => b.variance - a.variance).slice(0, 1500);
  for (const a of topAudit) { if (await FS.saveAuditItem(a.pro, a)) savedAudit++; }
  // v2.5 auto-seed customer_ap_contacts stubs for customers in audit
  const uniqueCustomers = new Map();
  for (const a of topAudit) {
    if (a.customer_key && !uniqueCustomers.has(a.customer_key)) {
      uniqueCustomers.set(a.customer_key, { customer: a.customer, total_owed: 0, item_count: 0 });
    }
    if (a.customer_key) {
      const c = uniqueCustomers.get(a.customer_key);
      c.total_owed += a.variance;
      c.item_count++;
    }
  }
  for (const [key, stats] of uniqueCustomers) {
    await FS.saveAPContact(key, {
      customer: stats.customer,
      customer_key: key,
      total_owed_cached: stats.total_owed,
      item_count_cached: stats.item_count,
      // Only set these if doc is new (merge preserves existing values)
      billing_email: "",
      ap_contact_name: "",
      ap_contact_phone: "",
      dispute_portal_url: "",
    });
  }

  // NuVizz
  let nvWeeksSaved = 0, nvStopsSaved = 0;
  if (nuvizzStops.length > 0) {
    onStatus({ phase:"save", message:`Saving NuVizz (${nuvizzStops.length} stops)...` });
    const nvWeekly = buildNuVizzWeekly(nuvizzStops);
    for (const w of nvWeekly) { if (await FS.saveNuVizzWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()})) nvWeeksSaved++; }
    const recent = nuvizzStops.filter(s => s.pro && s.delivery_date).sort((a,b) => (b.delivery_date||"").localeCompare(a.delivery_date||"")).slice(0, 2000);
    for (const s of recent) {
      const key = s.pro || s.stop_number;
      if (!key) continue;
      if (await FS.saveNuVizzStop(key, s)) nvStopsSaved++;
    }
  }

  // Time Clock
  let tcWeeksSaved = 0;
  if (timeClockEntries.length > 0) {
    const tcWeekly = buildTimeClockWeekly(timeClockEntries);
    for (const w of tcWeekly) { if (await FS.saveTimeClockWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()})) tcWeeksSaved++; }
  }

  // Payroll
  let payWeeksSaved = 0;
  if (payrollEntries.length > 0) {
    const payWeekly = buildPayrollWeekly(payrollEntries);
    for (const w of payWeekly) { if (await FS.savePayrollWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()})) payWeeksSaved++; }
  }

  // QBO
  let qboPeriodsSaved = 0;
  if (qboEntries.length > 0) {
    const byPeriod = {};
    for (const e of qboEntries) {
      const pid = `${e.report_type}_${e.source_file || "unknown"}`.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);
      if (!byPeriod[pid]) byPeriod[pid] = { period: e.period || null, report_type: e.report_type, source_file: e.source_file, accounts: [] };
      byPeriod[pid].accounts.push({ account: e.account, amount: e.amount, debit: e.debit, credit: e.credit });
    }
    for (const [pid, data] of Object.entries(byPeriod)) { if (await FS.saveQBOHistory(pid, {...data, uploaded_at: new Date().toISOString()})) qboPeriodsSaved++; }
  }

  // File logs
  for (const l of fileLogs) await FS.saveFileLog(l.file_id, l);
  for (const sf of sourceFilesLog) await FS.saveSourceFile(sf.file_id, sf);

  const existingMeta = await FS.getReconMeta() || {};
  await FS.saveReconMeta({
    files_count: (existingMeta.files_count || 0) + ddisFileRecords.length,
    last_upload: new Date().toISOString(),
    total_stops_processed: (existingMeta.total_stops_processed || 0) + allStops.length,
  });

  return {
    files_processed: files.length,
    counts: countsByKind,
    uline: { stops: allStops.length, weeks_saved: savedWeeks, recon_saved: savedRecon, unpaid_saved: topUnpaid.length, audit_saved: savedAudit, payments: Object.keys(paymentByPro).length },
    nuvizz: { stops: nuvizzStops.length, weeks_saved: nvWeeksSaved, stops_saved: nvStopsSaved },
    timeclock: { entries: timeClockEntries.length, weeks_saved: tcWeeksSaved },
    payroll: { entries: payrollEntries.length, weeks_saved: payWeeksSaved },
    qbo: { lines: qboEntries.length, periods_saved: qboPeriodsSaved },
    unknown: unknownFiles,
  };
}

// ═══ GMAIL SYNC — Auto-import weekly reports from inbox ═════
// ═══ FUEL — Per-Vendor Spend & Rate Tracking (v2.6) ════════
// Two sources: FuelFox + Quick Fuel. Weekly rollups by vendor so you can
// compare which vendor is actually cheaper per gallon when you include
// all fees/taxes/delivery. True rate = (fuel + taxes + delivery) / gallons.
const FUEL_VENDORS = [
  { key: "fuelfox", label: "FuelFox", color: "#dc2626", supported: true },
  { key: "quickfuel", label: "Quick Fuel", color: "#2563eb", supported: false },
];

function Fuel() {
  const [view, setView] = useState("weekly"); // weekly | invoices | upload | trucks
  const [loading, setLoading] = useState(true);
  const [invoices, setInvoices] = useState([]);
  const [weekly, setWeekly] = useState([]);
  const [byTruck, setByTruck] = useState([]);
  const [refreshTick, setRefreshTick] = useState(0);

  // Upload state (FuelFox pair)
  const [summaryPdf, setSummaryPdf] = useState(null);
  const [logPdf, setLogPdf] = useState(null);
  const [parsing, setParsing] = useState(false);
  const [parseResult, setParseResult] = useState(null);
  const [uploadStatus, setUploadStatus] = useState("");

  useEffect(() => {
    (async () => {
      setLoading(true);
      const [inv, wk, tr] = await Promise.all([
        FS.getFuelInvoices(), FS.getFuelWeekly(), FS.getFuelByTruck(3000),
      ]);
      setInvoices(inv);
      setWeekly(wk);
      setByTruck(tr);
      setLoading(false);
    })();
  }, [refreshTick]);

  // ─── Weekly rollups by vendor ───
  const weeklyByVendor = useMemo(() => {
    // Group by week_ending → { vendor: {gallons, spend, true_rate} }
    const byWeek = {};
    for (const w of weekly) {
      if (!byWeek[w.week_ending]) byWeek[w.week_ending] = { week_ending: w.week_ending, vendors: {} };
      byWeek[w.week_ending].vendors[w.vendor] = w;
    }
    return Object.values(byWeek).sort((a, b) => b.week_ending.localeCompare(a.week_ending));
  }, [weekly]);

  const vendorTotals = useMemo(() => {
    const t = {};
    for (const v of FUEL_VENDORS) t[v.key] = { gallons: 0, spend: 0, invoices: 0 };
    for (const inv of invoices) {
      const v = inv.vendor || "fuelfox";
      if (!t[v]) t[v] = { gallons: 0, spend: 0, invoices: 0 };
      t[v].gallons += inv.total_gallons || 0;
      t[v].spend += inv.grand_total || 0;
      t[v].invoices++;
    }
    // Overall true rate per vendor
    for (const k of Object.keys(t)) {
      t[k].true_rate = t[k].gallons > 0 ? t[k].spend / t[k].gallons : null;
    }
    return t;
  }, [invoices]);

  // ─── Upload: FuelFox PDF pair ───
  const fileToBase64 = (file) => new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(r.result.split(",")[1]);
    r.onerror = reject;
    r.readAsDataURL(file);
  });

  const parseFuelFoxPair = async () => {
    if (!summaryPdf || !logPdf) {
      setUploadStatus("✗ Both PDFs required");
      return;
    }
    setParsing(true);
    setUploadStatus("Converting PDFs to images...");
    setParseResult(null);
    try {
      // Identify which PDF is which by filename
      const isLogFile = (f) => /service.?log|servicelog/i.test(f.name);
      let serviceLogPdf, invoicePdf;
      if (isLogFile(logPdf) && !isLogFile(summaryPdf)) {
        serviceLogPdf = logPdf; invoicePdf = summaryPdf;
      } else if (isLogFile(summaryPdf) && !isLogFile(logPdf)) {
        serviceLogPdf = summaryPdf; invoicePdf = logPdf;
      } else {
        // Fall back to the user's zones as named
        serviceLogPdf = logPdf; invoicePdf = summaryPdf;
      }

      setUploadStatus("Rendering Service Log pages...");
      const logPages = await pdfToPngPages(serviceLogPdf, 2);
      setUploadStatus("Rendering Invoice page...");
      const invoicePages = await pdfToPngPages(invoicePdf, 2);

      setUploadStatus(`Scanning Service Log with Claude vision (${logPages.length} pages)...`);
      const slData = await scanPdfWithVision(logPages, FUELFOX_SERVICE_LOG_PROMPT);

      setUploadStatus(`Scanning Invoice with Claude vision (${invoicePages.length} pages)...`);
      const invData = await scanPdfWithVision(invoicePages, FUELFOX_INVOICE_PROMPT);

      // Validate shape
      if (!slData || !Array.isArray(slData.rows)) throw new Error("Service Log scan: no rows returned");
      if (!invData || invData.overhead_total == null) throw new Error("Invoice scan: overhead_total missing");

      // Redistribute overhead
      const redist = redistributeFuelFoxOverhead(slData.rows, invData.overhead_total);

      // Build combined result shape (compatible with old save logic)
      const grandTotal = (invData.diesel_sales || 0) + (invData.diesel_taxes || 0) + (invData.delivery_fee || 0);
      const combined = {
        vendor: "fuelfox",
        summary: {
          invoice_number: invData.invoice_number,
          invoice_date: invData.invoice_date,
          total_gallons: redist.total_gallons,
          posted_rate: slData.rows[0]?.posted_rate || null,
          diesel_cost: invData.diesel_sales,
          diesel_tax: invData.diesel_taxes,
          delivery_fee: invData.delivery_fee,
          grand_total: grandTotal,
          fuel_only_rate: redist.total_gallons > 0 ? ((invData.diesel_sales + invData.diesel_taxes) / redist.total_gallons) : null,
          true_rate: redist.total_gallons > 0 ? (grandTotal / redist.total_gallons) : null,
        },
        log: {
          service_date: slData.service_date,
          ambassador: slData.ambassador,
          service_vehicle: slData.service_vehicle,
        },
        trucks: redist.rows.map(r => ({
          unit: r.truckId,
          gallons: r.gallons,
          posted_rate: r.posted_rate,
          posted_charge: r.posted_charge,
          true_rate: r.true_rate,
          true_cost: r.true_cost,
          uplift: r.overhead_share,
          invoice_number: invData.invoice_number,
          service_date: slData.service_date,
        })),
        totals: {
          total_gallons: redist.total_gallons,
          posted_fuel_cost: invData.diesel_sales,
          tax: invData.diesel_taxes,
          delivery_fee: invData.delivery_fee,
          grand_total: grandTotal,
          true_rate: redist.total_gallons > 0 ? (grandTotal / redist.total_gallons) : null,
          posted_rate: slData.rows[0]?.posted_rate || null,
          truck_count: redist.rows.length,
        },
        notes: [redist.report],
      };
      setParseResult(combined);
      setUploadStatus(`✓ Parsed — ${combined.trucks.length} trucks, ${combined.totals.total_gallons.toFixed(1)} gal, $${grandTotal.toFixed(2)} true cost @ $${combined.totals.true_rate.toFixed(4)}/gal`);
    } catch (e) {
      setUploadStatus(`✗ Parse failed: ${e.message}`);
    }
    setParsing(false);
  };

  const saveParsedInvoice = async () => {
    if (!parseResult) return;
    setUploadStatus("Saving to Firebase...");
    try {
      const pr = parseResult;
      const invId = `fuelfox_${pr.summary.invoice_number}`;
      const weekKey = weekEndingFriday(pr.summary.invoice_date ? isoDate(pr.summary.invoice_date) : null);

      // 1) Save invoice summary
      await FS.saveFuelInvoice(invId, {
        invoice_id: invId,
        vendor: "fuelfox",
        invoice_number: pr.summary.invoice_number,
        invoice_date: pr.summary.invoice_date ? isoDate(pr.summary.invoice_date) : null,
        service_date: pr.log.service_date ? isoDate(pr.log.service_date) : null,
        week_ending: weekKey,
        total_gallons: pr.summary.total_gallons,
        posted_rate: pr.summary.posted_rate,
        fuel_cost: pr.summary.diesel_cost,
        tax: pr.summary.diesel_tax,
        delivery_fee: pr.summary.delivery_fee,
        grand_total: pr.totals.grand_total,
        true_rate: pr.totals.true_rate,
        fuel_only_rate: pr.totals.fuel_only_rate,
        truck_count: pr.totals.truck_count,
        ambassador: pr.log.ambassador,
        service_vehicle: pr.log.service_vehicle,
      });

      // 2) Save per-truck line items
      for (const t of pr.trucks) {
        const lineId = `fuelfox_${pr.summary.invoice_number}_${t.unit}`;
        await FS.saveFuelByTruck(lineId, {
          line_id: lineId,
          vendor: "fuelfox",
          invoice_number: pr.summary.invoice_number,
          invoice_id: invId,
          unit: t.unit,
          gallons: t.gallons,
          posted_rate: t.posted_rate,
          posted_charge: t.posted_charge,
          true_rate: t.true_rate,
          true_cost: t.true_cost,
          uplift: t.uplift,
          service_date: pr.log.service_date ? isoDate(pr.log.service_date) : null,
          week_ending: weekKey,
        });
      }

      // 3) Update weekly rollup for this vendor/week
      if (weekKey) {
        const weekRollupId = `fuelfox_${weekKey}`;
        // Load existing, merge additively
        const existing = weekly.find(w => w.id === weekRollupId);
        const newGallons = (existing?.gallons || 0) + pr.summary.total_gallons;
        const newSpend = (existing?.spend || 0) + pr.totals.grand_total;
        await FS.saveFuelWeekly(weekRollupId, {
          week_id: weekRollupId,
          vendor: "fuelfox",
          week_ending: weekKey,
          gallons: newGallons,
          spend: newSpend,
          true_rate: newGallons > 0 ? newSpend / newGallons : null,
          invoice_count: (existing?.invoice_count || 0) + 1,
        });
      }

      setUploadStatus(`✓ Saved — ${pr.trucks.length} trucks, invoice ${pr.summary.invoice_number}`);
      setSummaryPdf(null);
      setLogPdf(null);
      setParseResult(null);
      setRefreshTick(t => t + 1);
      setTimeout(() => setView("weekly"), 1200);
    } catch (e) {
      setUploadStatus(`✗ Save failed: ${e.message}`);
    }
  };

  // Helper: MM/DD/YYYY → YYYY-MM-DD
  function isoDate(mdy) {
    if (!mdy) return null;
    const m = String(mdy).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (!m) return null;
    return `${m[3]}-${m[1].padStart(2,"0")}-${m[2].padStart(2,"0")}`;
  }

  if (loading) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading fuel data...</div>;

  const hasData = invoices.length > 0;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="⛽" text="Fuel — Per-Vendor Tracking" right={
      <span style={{fontSize:10,color:T.textDim}}>{invoices.length} invoices • {byTruck.length} truck-fills</span>
    } />

    {/* Vendor totals summary */}
    {hasData && (
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(240px,1fr))",gap:10,marginBottom:16}}>
        {FUEL_VENDORS.map(v => {
          const t = vendorTotals[v.key] || {};
          return (
            <div key={v.key} style={{...cardStyle, borderLeft:`3px solid ${v.color}`, padding:"12px 14px"}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}>
                <div style={{fontSize:13,fontWeight:700}}>{v.label}</div>
                {!v.supported && <Badge text="soon" color={T.textDim} bg={T.bgSurface} />}
              </div>
              <div style={{fontSize:18,fontWeight:700,color:v.color}}>
                {t.true_rate ? `$${t.true_rate.toFixed(4)}/gal` : "—"}
              </div>
              <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>all-in true rate</div>
              <div style={{fontSize:11,color:T.textMuted,marginTop:8,lineHeight:1.6}}>
                <div>{fmtNum(Math.round(t.gallons||0))} gallons total</div>
                <div>{fmtK(t.spend||0)} spend • {t.invoices||0} invoices</div>
              </div>
            </div>
          );
        })}
      </div>
    )}

    {/* View tabs */}
    <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
      {[
        ["weekly","📅 Weekly Comparison"],
        ["invoices","📄 Invoices"],
        ["trucks","🚛 By Truck"],
        ["upload","📤 Upload FuelFox"],
      ].map(([id,l]) =>
        <TabButton key={id} active={view===id} label={l} onClick={()=>setView(id)} />
      )}
    </div>

    {/* ─── WEEKLY VENDOR COMPARISON ─── */}
    {view === "weekly" && (
      !hasData ? (
        <div style={cardStyle}>
          <EmptyState icon="⛽" title="No Fuel Data Yet" sub="Upload a FuelFox invoice pair to begin tracking weekly spend and true price per gallon." />
        </div>
      ) : (
        <div style={{...cardStyle, padding:0, overflow:"hidden"}}>
          <div style={{padding:"12px 14px",background:T.bgSurface,borderBottom:`1px solid ${T.border}`}}>
            <div style={{fontSize:13,fontWeight:700}}>Weekly Spend & $/Gal — Side by Side</div>
            <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>Each row is one week. Lower $/gal wins that week.</div>
          </div>
          <div style={{overflowX:"auto",maxHeight:600}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead style={{position:"sticky",top:0,background:T.bgWhite,zIndex:1}}>
                <tr style={{borderBottom:`1px solid ${T.border}`}}>
                  <th rowSpan={2} style={{padding:"6px 10px",textAlign:"left",fontSize:10,color:T.textDim,fontWeight:700,textTransform:"uppercase"}}>Week Ending</th>
                  {FUEL_VENDORS.map(v => (
                    <th key={v.key} colSpan={3} style={{padding:"6px 10px",textAlign:"center",fontSize:10,color:v.color,fontWeight:700,textTransform:"uppercase",borderBottom:`2px solid ${v.color}`}}>{v.label}</th>
                  ))}
                  <th rowSpan={2} style={{padding:"6px 10px",textAlign:"center",fontSize:10,color:T.textDim,fontWeight:700,textTransform:"uppercase"}}>Winner</th>
                </tr>
                <tr style={{borderBottom:`1px solid ${T.border}`}}>
                  {FUEL_VENDORS.flatMap(v => [
                    <th key={`${v.key}-gal`} style={{padding:"4px 8px",textAlign:"right",fontSize:9,color:T.textMuted,fontWeight:600}}>Gal</th>,
                    <th key={`${v.key}-spend`} style={{padding:"4px 8px",textAlign:"right",fontSize:9,color:T.textMuted,fontWeight:600}}>Spend</th>,
                    <th key={`${v.key}-rate`} style={{padding:"4px 8px",textAlign:"right",fontSize:9,color:T.textMuted,fontWeight:600}}>$/Gal</th>,
                  ])}
                </tr>
              </thead>
              <tbody>
                {weeklyByVendor.slice(0, 60).map(row => {
                  // Determine winner by lowest true_rate among vendors with data
                  const ratesByVendor = {};
                  for (const v of FUEL_VENDORS) {
                    const vd = row.vendors[v.key];
                    if (vd && vd.true_rate) ratesByVendor[v.key] = vd.true_rate;
                  }
                  const winnerKey = Object.keys(ratesByVendor).length > 1
                    ? Object.entries(ratesByVendor).sort((a,b) => a[1]-b[1])[0][0]
                    : null;
                  const winner = winnerKey ? FUEL_VENDORS.find(v => v.key === winnerKey) : null;
                  return (
                    <tr key={row.week_ending} style={{borderBottom:`1px solid ${T.borderLight}`}}>
                      <td style={{padding:"8px 10px",fontWeight:600}}>WE {weekLabel(row.week_ending)}</td>
                      {FUEL_VENDORS.flatMap(v => {
                        const vd = row.vendors[v.key];
                        return [
                          <td key={`${v.key}-g`} style={{padding:"8px",textAlign:"right",color:T.textMuted}}>{vd ? fmtNum(Math.round(vd.gallons)) : "—"}</td>,
                          <td key={`${v.key}-s`} style={{padding:"8px",textAlign:"right",fontWeight:600}}>{vd ? fmt(vd.spend) : "—"}</td>,
                          <td key={`${v.key}-r`} style={{padding:"8px",textAlign:"right",color:v.color,fontWeight:700}}>{vd ? `$${vd.true_rate.toFixed(4)}` : "—"}</td>,
                        ];
                      })}
                      <td style={{padding:"8px 10px",textAlign:"center"}}>
                        {winner ? <Badge text={winner.label} color={winner.color} bg={winner.color+"22"} /> : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )
    )}

    {/* ─── INVOICES ─── */}
    {view === "invoices" && (
      !hasData ? (
        <div style={cardStyle}><EmptyState icon="📄" title="No Invoices Yet" sub="Upload a FuelFox invoice pair to populate this list." /></div>
      ) : (
        <div style={{...cardStyle, padding:0}}>
          <div style={{overflowX:"auto",maxHeight:500}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead>
                <tr style={{background:T.bgSurface,position:"sticky",top:0}}>
                  {["Date","Vendor","Invoice #","Gallons","Fuel","Tax","Delivery","Grand Total","True $/Gal","Trucks"].map(h =>
                    <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {invoices.map(inv => {
                  const v = FUEL_VENDORS.find(x => x.key === inv.vendor) || FUEL_VENDORS[0];
                  return (
                    <tr key={inv.id} style={{borderBottom:`1px solid ${T.borderLight}`}}>
                      <td style={{padding:"8px 10px"}}>{inv.invoice_date || "—"}</td>
                      <td style={{padding:"8px 10px"}}><Badge text={v.label} color={v.color} bg={v.color+"22"} /></td>
                      <td style={{padding:"8px 10px",fontFamily:"monospace",fontWeight:600}}>{inv.invoice_number}</td>
                      <td style={{padding:"8px 10px"}}>{fmtNum(inv.total_gallons)}</td>
                      <td style={{padding:"8px 10px"}}>{fmt(inv.fuel_cost)}</td>
                      <td style={{padding:"8px 10px",color:T.textMuted}}>{fmt(inv.tax)}</td>
                      <td style={{padding:"8px 10px",color:T.textMuted}}>{fmt(inv.delivery_fee)}</td>
                      <td style={{padding:"8px 10px",color:T.red,fontWeight:700}}>{fmt(inv.grand_total)}</td>
                      <td style={{padding:"8px 10px",color:v.color,fontWeight:700}}>${inv.true_rate ? inv.true_rate.toFixed(4) : "—"}</td>
                      <td style={{padding:"8px 10px"}}>{inv.truck_count}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )
    )}

    {/* ─── BY TRUCK ─── */}
    {view === "trucks" && (
      !hasData ? (
        <div style={cardStyle}><EmptyState icon="🚛" title="No Truck Data" sub="Upload a FuelFox invoice pair to see per-truck fuel cost." /></div>
      ) : (
        <div style={{...cardStyle, padding:0}}>
          <div style={{padding:"10px 14px",background:T.bgSurface,borderBottom:`1px solid ${T.border}`,fontSize:12,fontWeight:700}}>
            Per-Truck Fuel History — Last {Math.min(byTruck.length, 500)} fill-ups
          </div>
          <div style={{overflowX:"auto",maxHeight:500}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead>
                <tr style={{background:T.bgSurface,position:"sticky",top:0}}>
                  {["Service Date","Unit","Gallons","Posted $","True $","Uplift","Vendor","Invoice"].map(h =>
                    <th key={h} style={{textAlign:"left",padding:"6px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:9,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {byTruck.slice(0, 500).map(t => {
                  const v = FUEL_VENDORS.find(x => x.key === t.vendor) || FUEL_VENDORS[0];
                  return (
                    <tr key={t.id} style={{borderBottom:`1px solid ${T.borderLight}`}}>
                      <td style={{padding:"6px 10px"}}>{t.service_date || "—"}</td>
                      <td style={{padding:"6px 10px",fontFamily:"monospace",fontWeight:700}}>{t.unit}</td>
                      <td style={{padding:"6px 10px"}}>{t.gallons?.toFixed(1)}</td>
                      <td style={{padding:"6px 10px",color:T.textMuted}}>{fmt(t.posted_charge)}</td>
                      <td style={{padding:"6px 10px",color:T.red,fontWeight:700}}>{fmt(t.true_cost)}</td>
                      <td style={{padding:"6px 10px",color:T.yellowText,fontSize:10}}>+{fmt(t.uplift)}</td>
                      <td style={{padding:"6px 10px",fontSize:10}}>{v.label}</td>
                      <td style={{padding:"6px 10px",fontSize:10,color:T.textMuted,fontFamily:"monospace"}}>{t.invoice_number}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )
    )}

    {/* ─── UPLOAD ─── */}
    {view === "upload" && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:6}}>Upload FuelFox Invoice Pair</div>
        <div style={{fontSize:11,color:T.textMuted,lineHeight:1.5,marginBottom:16}}>
          FuelFox sends <strong>both PDFs in the same email</strong>. Drop the summary invoice and the service log below. MarginIQ will parse both, compute the true $/gallon (fuel + taxes + delivery fee baked in), and save per-truck breakdown.
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(240px,1fr))",gap:12,marginBottom:16}}>
          <FileDropZone
            label="1. Summary Invoice"
            hint="Invoice_DDxxx_from_FuelFox_*.pdf"
            file={summaryPdf}
            onFile={setSummaryPdf}
            color={T.brand}
          />
          <FileDropZone
            label="2. Service Log"
            hint="FuelFox-ServiceLog-*.pdf"
            file={logPdf}
            onFile={setLogPdf}
            color={T.brandLight}
          />
        </div>

        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
          <PrimaryBtn text={parsing ? "Parsing..." : "🔍 Parse Invoice Pair"} onClick={parseFuelFoxPair} loading={parsing} disabled={!summaryPdf || !logPdf} />
          {parseResult && !parsing && (
            <button onClick={saveParsedInvoice} style={{padding:"8px 16px",fontSize:12,fontWeight:700,borderRadius:8,border:`1px solid ${T.green}`,background:T.green,color:"#fff",cursor:"pointer"}}>
              💾 Save to Firebase
            </button>
          )}
        </div>

        {uploadStatus && (
          <div style={{marginTop:12,padding:"10px 12px",borderRadius:8,
            background: uploadStatus.startsWith("✓") ? T.greenBg : uploadStatus.startsWith("✗") ? T.redBg : T.yellowBg,
            color: uploadStatus.startsWith("✓") ? T.greenText : uploadStatus.startsWith("✗") ? T.redText : T.yellowText,
            fontSize:12,fontWeight:600}}>
            {uploadStatus}
          </div>
        )}

        {/* Preview parsed result */}
        {parseResult && (
          <div style={{marginTop:16,padding:"12px",background:T.bgSurface,borderRadius:8,border:`1px solid ${T.border}`}}>
            <div style={{fontSize:12,fontWeight:700,marginBottom:8}}>📊 Parse Preview — Review before saving</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:10,marginBottom:12}}>
              <KPI label="Invoice #" value={parseResult.summary.invoice_number} sub={parseResult.summary.invoice_date} />
              <KPI label="Total Gallons" value={fmtNum(parseResult.totals.total_gallons)} sub={`${parseResult.totals.truck_count} trucks`} />
              <KPI label="Grand Total" value={fmt(parseResult.totals.grand_total)} sub="fuel+tax+delivery" subColor={T.red} />
              <KPI label="True $/Gal" value={`$${parseResult.totals.true_rate.toFixed(4)}`} sub={`posted $${parseResult.totals.posted_rate}`} subColor={T.brand} />
            </div>
            <div style={{fontSize:10,color:T.textMuted,marginBottom:6}}>Breakdown: ${parseResult.totals.posted_fuel_cost.toFixed(2)} fuel + ${parseResult.totals.tax.toFixed(2)} tax + ${parseResult.totals.delivery_fee.toFixed(2)} delivery</div>
            <div style={{maxHeight:240,overflowY:"auto",border:`1px solid ${T.border}`,borderRadius:6}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead><tr style={{background:T.bgWhite}}>
                  {["Unit","Gal","Posted","True","Uplift"].map(h =>
                    <th key={h} style={{padding:"4px 8px",textAlign:"left",borderBottom:`1px solid ${T.border}`,fontSize:9,color:T.textMuted,fontWeight:600}}>{h}</th>)}
                </tr></thead>
                <tbody>
                  {parseResult.trucks.map(t => (
                    <tr key={t.unit} style={{borderBottom:`1px solid ${T.borderLight}`}}>
                      <td style={{padding:"3px 8px",fontFamily:"monospace",fontWeight:600}}>{t.unit}</td>
                      <td style={{padding:"3px 8px"}}>{t.gallons}</td>
                      <td style={{padding:"3px 8px",color:T.textMuted}}>{fmt(t.posted_charge)}</td>
                      <td style={{padding:"3px 8px",color:T.red,fontWeight:600}}>{fmt(t.true_cost)}</td>
                      <td style={{padding:"3px 8px",color:T.yellowText}}>+{fmt(t.uplift)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Quick Fuel placeholder */}
        <div style={{marginTop:20,padding:"12px",background:T.bgSurface,borderRadius:8,border:`1px dashed ${T.border}`}}>
          <div style={{fontSize:12,fontWeight:700,color:T.textMuted,marginBottom:4}}>🚧 Quick Fuel (coming next)</div>
          <div style={{fontSize:11,color:T.textMuted,lineHeight:1.5}}>
            Quick Fuel / 4flyers.com parser isn't built yet. When you have a sample Quick Fuel invoice, send it over and I'll add the parser with the same logic — true $/gal including all fees — so the weekly comparison is apples-to-apples.
          </div>
        </div>
      </div>
    )}
  </div>;
}

function FileDropZone({ label, hint, file, onFile, color }) {
  const ref = useRef(null);
  return (
    <div
      onClick={() => ref.current?.click()}
      onDragOver={e => { e.preventDefault(); }}
      onDrop={e => {
        e.preventDefault();
        const f = e.dataTransfer.files[0];
        if (f) onFile(f);
      }}
      style={{
        border: `2px dashed ${file ? color : T.border}`,
        borderRadius: 10,
        padding: 14,
        cursor: "pointer",
        background: file ? color+"11" : T.bgWhite,
        transition: "all 0.15s",
      }}
    >
      <input ref={ref} type="file" accept=".pdf" style={{display:"none"}} onChange={e => onFile(e.target.files[0])} />
      <div style={{fontSize:12,fontWeight:700,color:file?color:T.text,marginBottom:4}}>{label}</div>
      <div style={{fontSize:10,color:T.textMuted,marginBottom:6}}>{hint}</div>
      {file ? (
        <div style={{fontSize:11,fontWeight:600,color,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
          ✓ {file.name} <span style={{color:T.textDim,fontWeight:400}}>({Math.round(file.size/1024)} KB)</span>
        </div>
      ) : (
        <div style={{fontSize:10,color:T.textDim}}>Drop PDF or click to browse</div>
      )}
    </div>
  );
}


// ═══ CLAUDE VISION INVOICE SCANNING (v2.8) ══════════════════
// Converts PDF pages to PNG via pdf.js, sends to Claude messages API
// via /.netlify/functions/marginiq-scan-invoice. Per-vendor prompt rules
// (Quick Fuel, FuelFox) produce structured JSON.
//
// Architecture matches Davis Fleet Management v2.10.0 production:
//   1. Service Log → per-truck rows with BASE cost
//   2. Invoice     → 1 "INVENTORY" overhead row (tax + delivery fee)
//   3. redistributeFuelFoxOverhead() spreads overhead by gallons share

async function loadPdfJs() {
  if (window.pdfjsLib) return window.pdfjsLib;
  await new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js";
    s.onload = resolve;
    s.onerror = reject;
    document.head.appendChild(s);
  });
  window.pdfjsLib.GlobalWorkerOptions.workerSrc =
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  return window.pdfjsLib;
}

// Convert a PDF File or base64 string to an array of PNG dataURLs (one per page).
async function pdfToPngPages(source, scale = 2) {
  const pdfjs = await loadPdfJs();
  let bytes;
  if (source instanceof File || source instanceof Blob) {
    bytes = new Uint8Array(await source.arrayBuffer());
  } else if (typeof source === "string") {
    // Base64 string
    const binary = atob(source);
    bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  } else {
    throw new Error("pdfToPngPages: unsupported source type");
  }
  const pdf = await pdfjs.getDocument({ data: bytes }).promise;
  const pages = [];
  for (let p = 1; p <= pdf.numPages; p++) {
    const page = await pdf.getPage(p);
    const vp = page.getViewport({ scale });
    const canvas = document.createElement("canvas");
    canvas.width = vp.width;
    canvas.height = vp.height;
    await page.render({ canvasContext: canvas.getContext("2d"), viewport: vp }).promise;
    pages.push(canvas.toDataURL("image/png"));
  }
  return pages;
}

// Call Claude vision with PNG pages + prompt. Returns parsed JSON.
async function scanPdfWithVision(pngPages, prompt) {
  const content = [];
  for (const dataUrl of pngPages) {
    const mediaType = dataUrl.split(";")[0].split(":")[1];
    const base64Data = dataUrl.split(",")[1];
    content.push({
      type: "image",
      source: { type: "base64", media_type: mediaType, data: base64Data },
    });
  }
  content.push({ type: "text", text: prompt });

  const resp = await fetch("/.netlify/functions/marginiq-scan-invoice", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      messages: [{ role: "user", content }],
      max_tokens: 4096,
    }),
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`Scan API ${resp.status}: ${errText.substring(0, 300)}`);
  }
  const data = await resp.json();
  const text = (data.content || []).find(c => c.type === "text")?.text || "";
  const cleaned = text.replace(/^```json\s*/i, "").replace(/```\s*$/, "").trim();
  try {
    return JSON.parse(cleaned);
  } catch (e) {
    throw new Error(`JSON parse failed. Raw: ${cleaned.substring(0, 400)}`);
  }
}

// ─── Vendor-specific prompt rules (verbatim from production spec) ───

const QUICK_FUEL_PROMPT = `Parse this Quick Fuel (Flyers Energy) weekly fuel-card invoice and return JSON.

RULES FOR QUICK FUEL / FLYERS ENERGY:
- Vendor should be "Quick Fuel". Invoice number starts with "CFS-" (e.g. "CFS-4582698").
- Find the section titled "Recap by Additional Info 2" — this is your CRITICAL data. Do NOT confuse with "Recap by Card" which lists per-driver totals (wrong data).
- In the "Recap by Additional Info 2" table, each row has: [Truck#] [Units/gallons] [Amount pre-tax] [Taxes] [Total]. Truck numbers can be 3-5 digits (preserve as-is, keep leading zeros). A row with truck "0" means unassigned fuel — use truckId "INVENTORY".
- EVERY row in that Recap table becomes ONE object in output array. If table has 11 rows, return 11 objects. DO NOT merge, summarize, or skip.
- For each row:
  - truckId: the truck # (or "INVENTORY" for "0" bucket)
  - gallons: Units column (REQUIRED)
  - total: Total column — THIS INCLUDES TAX (REQUIRED)
  - pricePerGallon: total / gallons, rounded to 4 decimals
  - invoiceNum: "<base invoice>-<truckId>" e.g. "CFS-4582698-0294"
  - date: invoice date YYYY-MM-DD
  - notes: "Weekly fuel card - Truck X" or "Unassigned fuel" for INVENTORY
- ALSO CAPTURE INVOICE-LEVEL FEES: Look for a section titled "Invoice Fees Total" or similar. Fees like "Regulatory Compliance Fee" are NOT per-truck — they appear as an invoice-level total. Extract as invoice_fees (number, 0 if none).
- INVOICE_TOTAL: the grand total shown at the bottom of the invoice (e.g. "15,736.44"). This should equal (sum of row totals) + invoice_fees.
- Return ONLY JSON: {"vendor":"Quick Fuel","invoice_number":"CFS-xxx","invoice_date":"YYYY-MM-DD","invoice_total":N,"invoice_fees":N,"rows":[...]}
- NO markdown fences, NO explanation, JUST the JSON.`;

const FUELFOX_SERVICE_LOG_PROMPT = `Parse this FuelFox Atlanta SERVICE LOG PDF and return JSON.

RULES FOR FUELFOX SERVICE LOG:
- Header shows "Service Log", "Customer:", "Service Date: MM/DD/YYYY", "Service Vehicle: N" (FuelFox's own truck — IGNORE this number, it's NOT a Davis truck).
- Body has "Diesel" heading, then columns: [Unit Number] [Gallons] [Price Per Gallon] [Total Charge].
- Return ONE OBJECT PER TRUCK ROW (NOT the Total row). Each row has:
  - truckId: Unit Number (preserve leading zeros: "0424", "1478")
  - gallons: Gallons column
  - posted_rate: Price Per Gallon column (base price only, NO tax, NO delivery)
  - total: Total Charge column (gallons × posted_rate — base only)
- SKIP any row where Unit Number matches the "Service Vehicle" in the header.
- SANITY: sum of row totals must equal "Total: $X,XXX.XX" at bottom.
- Return ONLY JSON: {"kind":"service_log","vendor":"FuelFox Atlanta","service_date":"YYYY-MM-DD","service_vehicle":"N","ambassador":"Name","rows":[...]}
- NO markdown, NO explanation, JUST JSON.`;

const FUELFOX_INVOICE_PROMPT = `Parse this FuelFox Atlanta INVOICE PDF and return JSON.

RULES FOR FUELFOX INVOICE:
- Header shows "FuelFox Atlanta", "INVOICE", "INVOICE DDxxx", "DATE: MM/DD/YYYY".
- Body has: "Diesel Sales" (base charge — already captured per-truck in Service Log), "Diesel Taxes", "Delivery Fee", "BALANCE DUE".
- Return EXACTLY ONE object (not per-truck):
  - kind: "invoice"
  - vendor: "FuelFox Atlanta"
  - invoice_number: e.g. "DD404"
  - invoice_date: YYYY-MM-DD from invoice date
  - diesel_sales: value of Diesel Sales line
  - diesel_taxes: value of Diesel Taxes line
  - delivery_fee: value of Delivery Fee line
  - balance_due: value of BALANCE DUE
  - overhead_total: diesel_taxes + delivery_fee (the portion to redistribute across trucks)
- Return ONLY JSON, NO markdown, NO explanation.`;

// ─── FuelFox overhead redistribution ────────────────────────
// Takes freshly-scanned service_log rows + invoice overhead, spreads the
// tax+delivery proportionally by gallons across the trucks. Returns the
// truck rows with true per-gallon cost baked in.
function redistributeFuelFoxOverhead(serviceRows, overhead) {
  if (!serviceRows || serviceRows.length === 0) {
    return { rows: [], report: "No service log rows — nothing to redistribute" };
  }
  const totalGallons = serviceRows.reduce((s, r) => s + (Number(r.gallons) || 0), 0);
  if (totalGallons <= 0) {
    return { rows: serviceRows, report: "Zero gallons total — cannot redistribute" };
  }
  const overheadAmount = Number(overhead) || 0;
  const overheadPerGal = overheadAmount / totalGallons;
  const out = serviceRows.map(r => {
    const baseTotal = Number(r.total) || 0;
    const share = (Number(r.gallons) || 0) * overheadPerGal;
    const newTotal = Math.round((baseTotal + share) * 100) / 100;
    const newPpg = (Number(r.gallons) > 0) ? Math.round((newTotal / Number(r.gallons)) * 10000) / 10000 : 0;
    return {
      ...r,
      posted_charge: baseTotal,  // keep original for audit
      overhead_share: Math.round(share * 100) / 100,
      true_cost: newTotal,
      true_rate: newPpg,
    };
  });
  return {
    rows: out,
    total_gallons: totalGallons,
    overhead_amount: overheadAmount,
    overhead_per_gallon: Math.round(overheadPerGal * 10000) / 10000,
    report: `Distributed $${overheadAmount.toFixed(2)} across ${serviceRows.length} trucks (${totalGallons.toFixed(1)} gal @ $${overheadPerGal.toFixed(4)}/gal)`,
  };
}


function GmailSync({ onRefresh }) {
  const [gmailConn, setGmailConn] = useState(null);
  const [loadingConn, setLoadingConn] = useState(true);
  const [results, setResults] = useState({}); // vendor -> emails array
  const [loading, setLoading] = useState({}); // vendor -> bool
  const [importing, setImporting] = useState({}); // emailId:attachmentId -> bool
  const [imported, setImported] = useState({}); // emailId:attachmentId -> result summary
  const [importStatus, setImportStatus] = useState("");

  // Load Gmail connection state
  useEffect(() => {
    (async () => {
      if (!hasFirebase) { setLoadingConn(false); return; }
      try {
        const d = await window.db.collection("marginiq_config").doc("gmail_tokens").get();
        if (d.exists) {
          const data = d.data();
          setGmailConn({ email: data.email, connected_at: data.connected_at });
        }
      } catch(e) {}
      setLoadingConn(false);
    })();
    // Handle OAuth callback redirect (?gmail=connected)
    const params = new URLSearchParams(window.location.search);
    if (params.get("gmail") === "connected") {
      window.history.replaceState({}, "", "/");
      setTimeout(() => window.location.reload(), 200);
    }
  }, []);

  const disconnect = async () => {
    if (!confirm("Disconnect Gmail? You'll need to reconnect to pull new reports.")) return;
    try { await window.db.collection("marginiq_config").doc("gmail_tokens").delete(); } catch(e) {}
    setGmailConn(null);
    setResults({});
  };

  const searchVendor = async (vendor) => {
    setLoading(prev => ({...prev, [vendor]: true}));
    try {
      // Default to last 60 days
      const d = new Date();
      d.setDate(d.getDate() - 60);
      const afterDate = `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()}`;
      const resp = await fetch("/.netlify/functions/marginiq-gmail-search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vendor, afterDate, maxResults: 30 }),
      });
      const data = await resp.json();
      if (data.error) {
        setResults(prev => ({...prev, [vendor]: { error: data.error }}));
      } else {
        setResults(prev => ({...prev, [vendor]: { list: data.results || [], query: data.query }}));
      }
    } catch(e) {
      setResults(prev => ({...prev, [vendor]: { error: e.message }}));
    }
    setLoading(prev => ({...prev, [vendor]: false}));
  };

  const importAttachment = async (email, attachment) => {
    const refKey = `${email.emailId}:${attachment.attachmentId}`;
    setImporting(prev => ({...prev, [refKey]: true}));
    setImportStatus(`Downloading ${attachment.filename}...`);
    try {
      // 1. Download attachment bytes
      const dlResp = await fetch("/.netlify/functions/marginiq-gmail-attachment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messageId: email.emailId, attachmentId: attachment.attachmentId }),
      });
      const dlData = await dlResp.json();
      if (dlData.error) throw new Error(dlData.error);

      // 2. Base64 → File
      const binary = atob(dlData.data);
      const bytes = new Uint8Array(binary.length);
      for (let i=0; i<binary.length; i++) bytes[i] = binary.charCodeAt(i);
      const mimeType = attachment.mimeType || (attachment.filename.endsWith(".csv") ? "text/csv" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      const blob = new Blob([bytes], { type: mimeType });
      const file = new File([blob], attachment.filename, { type: mimeType });

      // 3. Route through shared ingest pipeline
      setImportStatus(`Parsing ${attachment.filename}...`);
      const result = await ingestFiles([file], (s) => {
        if (s.message) setImportStatus(s.message);
      });

      setImported(prev => ({...prev, [refKey]: result}));
      setImportStatus(`✓ Imported ${attachment.filename}`);
      if (onRefresh) onRefresh();
    } catch(e) {
      setImported(prev => ({...prev, [refKey]: { error: e.message }}));
      setImportStatus(`✗ Failed: ${e.message}`);
    }
    setImporting(prev => ({...prev, [refKey]: false}));
  };

  // FuelFox emails contain a PAIR of PDFs (Service Log + Invoice_DDxxx).
  // Uses Claude vision API (via marginiq-scan-invoice proxy) to parse each,
  // then redistributes overhead proportionally across trucks.
  const importFuelFoxPair = async (email) => {
    const pdfs = (email.attachments || []).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
    if (pdfs.length < 2) {
      setImportStatus(`✗ FuelFox email needs 2 PDFs (found ${pdfs.length})`);
      return;
    }
    const refKey = `${email.emailId}:fuelfox_pair`;
    setImporting(prev => ({...prev, [refKey]: true}));
    try {
      // Classify: Service Log vs Invoice by filename
      let serviceLogAtt = pdfs.find(a => /service.?log|servicelog/i.test(a.filename));
      let invoiceAtt = pdfs.find(a => /invoice[_\s-]*dd/i.test(a.filename)) || pdfs.find(a => a !== serviceLogAtt);
      if (!serviceLogAtt) { serviceLogAtt = pdfs[0]; invoiceAtt = pdfs[1]; }
      if (!invoiceAtt) invoiceAtt = pdfs.find(a => a !== serviceLogAtt);

      // Download both
      setImportStatus(`Downloading FuelFox PDFs...`);
      const dl = async (a) => {
        const r = await fetch("/.netlify/functions/marginiq-gmail-attachment", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ messageId: email.emailId, attachmentId: a.attachmentId }),
        });
        const d = await r.json();
        if (d.error) throw new Error(d.error);
        return d.data; // base64
      };
      const [slB64, invB64] = await Promise.all([dl(serviceLogAtt), dl(invoiceAtt)]);

      // Convert PDFs to PNG pages (client-side via pdf.js)
      setImportStatus(`Rendering PDF pages...`);
      const [slPages, invPages] = await Promise.all([
        pdfToPngPages(slB64, 2),
        pdfToPngPages(invB64, 2),
      ]);

      // Scan each with Claude vision
      setImportStatus(`Scanning Service Log (${slPages.length} pg) with Claude vision...`);
      const slData = await scanPdfWithVision(slPages, FUELFOX_SERVICE_LOG_PROMPT);
      if (!slData?.rows?.length) throw new Error("Service Log scan: no truck rows");

      setImportStatus(`Scanning Invoice (${invPages.length} pg) with Claude vision...`);
      const invData = await scanPdfWithVision(invPages, FUELFOX_INVOICE_PROMPT);
      if (invData?.overhead_total == null) throw new Error("Invoice scan: overhead_total missing");

      // Redistribute overhead across service log trucks
      const redist = redistributeFuelFoxOverhead(slData.rows, invData.overhead_total);
      const grandTotal = (invData.diesel_sales || 0) + (invData.diesel_taxes || 0) + (invData.delivery_fee || 0);
      const trueRate = redist.total_gallons > 0 ? (grandTotal / redist.total_gallons) : null;

      // Save to Firestore — same schema as Fuel tab upload
      setImportStatus(`Saving ${redist.rows.length} trucks to Firebase...`);
      const isoInv = invData.invoice_date; // Claude returns YYYY-MM-DD per prompt
      const weekKey = weekEndingFriday(isoInv);
      const invId = `fuelfox_${invData.invoice_number}`;

      await FS.saveFuelInvoice(invId, {
        invoice_id: invId, vendor: "fuelfox",
        invoice_number: invData.invoice_number,
        invoice_date: isoInv, service_date: slData.service_date, week_ending: weekKey,
        total_gallons: redist.total_gallons,
        posted_rate: slData.rows[0]?.posted_rate || null,
        fuel_cost: invData.diesel_sales,
        tax: invData.diesel_taxes,
        delivery_fee: invData.delivery_fee,
        grand_total: grandTotal, true_rate: trueRate,
        fuel_only_rate: redist.total_gallons > 0 ? ((invData.diesel_sales + invData.diesel_taxes) / redist.total_gallons) : null,
        truck_count: redist.rows.length,
        ambassador: slData.ambassador,
        service_vehicle: slData.service_vehicle,
        gmail_email_id: email.emailId,
      });

      for (const r of redist.rows) {
        const lineId = `fuelfox_${invData.invoice_number}_${r.truckId}`;
        await FS.saveFuelByTruck(lineId, {
          line_id: lineId, vendor: "fuelfox",
          invoice_number: invData.invoice_number, invoice_id: invId,
          unit: r.truckId, gallons: r.gallons,
          posted_rate: r.posted_rate, posted_charge: r.posted_charge,
          true_rate: r.true_rate, true_cost: r.true_cost, uplift: r.overhead_share,
          service_date: slData.service_date, week_ending: weekKey,
        });
      }

      if (weekKey) {
        const weekRollupId = `fuelfox_${weekKey}`;
        await FS.saveFuelWeekly(weekRollupId, {
          week_id: weekRollupId, vendor: "fuelfox", week_ending: weekKey,
          gallons: redist.total_gallons, spend: grandTotal, true_rate: trueRate, invoice_count: 1,
        });
      }

      setImported(prev => ({...prev, [refKey]: { ok: true, trucks: redist.rows.length, total: grandTotal, rate: trueRate }}));
      setImportStatus(`✓ FuelFox ${invData.invoice_number}: ${redist.rows.length} trucks, $${grandTotal.toFixed(2)} @ $${trueRate.toFixed(4)}/gal`);
      if (onRefresh) onRefresh();
    } catch(e) {
      setImported(prev => ({...prev, [refKey]: { error: e.message }}));
      setImportStatus(`✗ FuelFox import failed: ${e.message}`);
    }
    setImporting(prev => ({...prev, [refKey]: false}));
  };

  // Quick Fuel — single PDF, Recap table, pricePerGallon = total/gallons.
  // Uses Claude vision via marginiq-scan-invoice.
  const importQuickFuel = async (email, attachment) => {
    const refKey = `${email.emailId}:${attachment.attachmentId}`;
    setImporting(prev => ({...prev, [refKey]: true}));
    setImportStatus(`Downloading ${attachment.filename}...`);
    try {
      const dlResp = await fetch("/.netlify/functions/marginiq-gmail-attachment", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messageId: email.emailId, attachmentId: attachment.attachmentId }),
      });
      const dlData = await dlResp.json();
      if (dlData.error) throw new Error(dlData.error);

      setImportStatus(`Rendering PDF pages...`);
      const pages = await pdfToPngPages(dlData.data, 2);

      setImportStatus(`Scanning (${pages.length} pg) with Claude vision...`);
      const data = await scanPdfWithVision(pages, QUICK_FUEL_PROMPT);
      if (!data?.rows?.length) throw new Error("No rows found in Recap table");

      // Save: per-truck rows as fuel_by_truck, one aggregated invoice doc
      setImportStatus(`Saving ${data.rows.length} trucks to Firebase...`);
      const isoDate = data.invoice_date;
      const weekKey = weekEndingFriday(isoDate);
      const invBase = data.invoice_number;
      const totalGal = data.rows.reduce((s, r) => s + (Number(r.gallons) || 0), 0);
      const rowSum = data.rows.reduce((s, r) => s + (Number(r.total) || 0), 0);
      const invoiceFees = Number(data.invoice_fees) || 0;
      const grandTotal = rowSum + invoiceFees;  // matches invoice_total
      // Redistribute invoice-level fees (e.g. Regulatory Compliance Fee) proportionally by gallons
      const feePerGal = totalGal > 0 ? invoiceFees / totalGal : 0;
      const avgRate = totalGal > 0 ? grandTotal / totalGal : null;

      await FS.saveFuelInvoice(`quickfuel_${invBase}`, {
        invoice_id: `quickfuel_${invBase}`, vendor: "quickfuel",
        invoice_number: invBase, invoice_date: isoDate, week_ending: weekKey,
        total_gallons: totalGal,
        fuel_cost: rowSum,  // sum of recap rows (includes per-gallon tax)
        tax: null,
        delivery_fee: invoiceFees,  // invoice-level fees (regulatory compliance, etc.)
        grand_total: grandTotal,
        true_rate: avgRate,
        fuel_only_rate: totalGal > 0 ? rowSum / totalGal : null,
        truck_count: data.rows.length,
        gmail_email_id: email.emailId,
      });

      for (const r of data.rows) {
        const lineId = `quickfuel_${invBase}_${r.truckId}`;
        const gal = Number(r.gallons) || 0;
        const baseTotal = Number(r.total) || 0;
        const share = gal * feePerGal;
        const trueTotal = Math.round((baseTotal + share) * 100) / 100;
        const trueRate = gal > 0 ? Math.round((trueTotal / gal) * 10000) / 10000 : null;
        await FS.saveFuelByTruck(lineId, {
          line_id: lineId, vendor: "quickfuel",
          invoice_number: invBase, invoice_id: `quickfuel_${invBase}`,
          unit: r.truckId, gallons: gal,
          posted_rate: r.pricePerGallon, posted_charge: baseTotal,
          true_rate: trueRate, true_cost: trueTotal,
          uplift: Math.round(share * 10000) / 10000,  // per-truck share of invoice fees
          service_date: isoDate, week_ending: weekKey,
          notes: r.notes,
        });
      }

      if (weekKey) {
        const weekRollupId = `quickfuel_${weekKey}`;
        await FS.saveFuelWeekly(weekRollupId, {
          week_id: weekRollupId, vendor: "quickfuel", week_ending: weekKey,
          gallons: totalGal, spend: grandTotal, true_rate: avgRate, invoice_count: 1,
        });
      }

      setImported(prev => ({...prev, [refKey]: { ok: true, trucks: data.rows.length, total: grandTotal, rate: avgRate }}));
      setImportStatus(`✓ Quick Fuel ${invBase}: ${data.rows.length} trucks, $${grandTotal.toFixed(2)} @ $${avgRate?.toFixed(4)}/gal${invoiceFees > 0 ? ` (includes $${invoiceFees.toFixed(2)} redistributed fees)` : ""}`);
      if (onRefresh) onRefresh();
    } catch(e) {
      setImported(prev => ({...prev, [refKey]: { error: e.message }}));
      setImportStatus(`✗ Quick Fuel failed: ${e.message}`);
    }
    setImporting(prev => ({...prev, [refKey]: false}));
  };

  if (loadingConn) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading Gmail...</div>;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="📧" text="Gmail Sync" />

    {!gmailConn ? (
      <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:8,color:T.brand}}>Connect Gmail</div>
        <div style={{fontSize:12,color:T.text,lineHeight:1.6,marginBottom:12}}>
          Auto-import weekly reports directly from your inbox. MarginIQ will search your Gmail (read-only) for attachments from known vendors and route them through the same parsers as manual upload.
          <br/><br/>
          <strong>What we search for:</strong>
          <ul style={{marginTop:6,marginLeft:20}}>
            <li><strong>NuVizz:</strong> from <code>nuvizzapps@nuvizzapps.com</code>, CSV attachments</li>
            <li><strong>Uline:</strong> from any <code>@uline.com</code> sender, XLSX/CSV attachments</li>
          </ul>
        </div>
        <a href="/.netlify/functions/marginiq-gmail-auth" style={{display:"inline-block",padding:"10px 18px",background:T.brand,color:"#fff",borderRadius:8,textDecoration:"none",fontSize:13,fontWeight:700}}>
          📧 Connect Gmail
        </a>
      </div>
    ) : (
      <>
        <div style={{...cardStyle, background:T.greenBg, borderColor:T.green}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8}}>
            <div>
              <div style={{fontSize:12,fontWeight:700,color:T.greenText}}>✓ Connected</div>
              <div style={{fontSize:13,fontWeight:600,marginTop:2}}>{gmailConn.email}</div>
              {gmailConn.connected_at && <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>Connected {new Date(gmailConn.connected_at).toLocaleString()}</div>}
            </div>
            <button onClick={disconnect} style={{padding:"6px 12px",borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,fontSize:11,cursor:"pointer"}}>Disconnect</button>
          </div>
        </div>

        {importStatus && (
          <div style={{...cardStyle, background:T.yellowBg, borderColor:T.yellow, fontSize:12, color:T.yellowText, fontWeight:600}}>
            {importStatus}
          </div>
        )}

        {/* Vendor Panels */}
        {[
          { key:"nuvizz", icon:"🚚", label:"NuVizz", desc:"Weekly driver stops from nuvizzapps@nuvizzapps.com", color:T.blue, mode:"per-attachment" },
          { key:"uline", icon:"📦", label:"Uline", desc:"Weekly billing + DDIS from @uline.com senders", color:T.brand, mode:"per-attachment" },
          { key:"fuelfox", icon:"⛽", label:"FuelFox", desc:"Fuel delivery — summary + service log PDFs from accounting@fuelfox.net", color:"#dc2626", mode:"pair" },
          { key:"quickfuel", icon:"⛽", label:"Quick Fuel", desc:"Fuel card statements from ebilling@4flyers.com", color:"#2563eb", mode:"quickfuel" },
        ].map(v => {
          const r = results[v.key];
          const isLoading = loading[v.key];
          return (
            <div key={v.key} style={{...cardStyle, borderLeft:`3px solid ${v.color}`}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8,flexWrap:"wrap",gap:8}}>
                <div>
                  <div style={{fontSize:14,fontWeight:700}}>
                    {v.icon} {v.label}
                    {v.comingSoon && <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:T.bgSurface,color:T.textDim,marginLeft:8}}>parser pending</span>}
                  </div>
                  <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>{v.desc}</div>
                </div>
                <PrimaryBtn text={isLoading ? "Searching..." : "Search Last 60 Days"} onClick={() => searchVendor(v.key)} loading={isLoading} />
              </div>

              {r?.error && <div style={{fontSize:12,color:T.redText,background:T.redBg,padding:"8px 10px",borderRadius:6,marginTop:8}}>✗ {r.error}</div>}

              {r?.list && r.list.length === 0 && (
                <div style={{fontSize:12,color:T.textMuted,padding:8}}>No matching emails in the last 60 days.</div>
              )}

              {r?.list && r.list.length > 0 && (
                <div style={{marginTop:8}}>
                  <div style={{fontSize:10,color:T.textDim,marginBottom:6}}>
                    Found {r.list.length} email{r.list.length>1?"s":""}.
                    {v.mode === "pair" && " FuelFox sends a summary + log together — click Import Pair to process both at once."}
                  </div>
                  {r.list.map((em, idx) => {
                    // For FuelFox: single "Import Pair" button per email
                    if (v.mode === "pair") {
                      const pdfs = (em.attachments || []).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
                      const refKey = `${em.emailId}:fuelfox_pair`;
                      const isImp = importing[refKey];
                      const imp = imported[refKey];
                      return (
                        <div key={em.emailId} style={{padding:"8px 10px",borderTop:idx>0?`1px solid ${T.borderLight}`:"none"}}>
                          <div style={{fontSize:12,fontWeight:600}}>{em.emailSubject || "(no subject)"}</div>
                          <div style={{fontSize:10,color:T.textMuted}}>{em.from} • {em.emailDate ? new Date(em.emailDate).toLocaleString() : "—"}</div>
                          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8,marginTop:8,padding:"6px 10px",background:T.bgSurface,borderRadius:6}}>
                            <div style={{flex:1,fontSize:11}}>
                              📎 {pdfs.length} PDF{pdfs.length!==1?"s":""}{pdfs.length === 2 ? " (summary + log)" : pdfs.length < 2 ? " ⚠️ expected 2" : ""}
                              <div style={{fontSize:9,color:T.textDim,marginTop:2}}>{pdfs.map(p => p.filename).join(" · ")}</div>
                            </div>
                            {imp?.error ? (
                              <span style={{fontSize:10,color:T.redText,maxWidth:150}}>✗ {imp.error.substring(0,60)}</span>
                            ) : imp?.ok ? (
                              <div style={{textAlign:"right"}}>
                                <div style={{fontSize:10,color:T.greenText,fontWeight:700}}>✓ {imp.trucks} trucks</div>
                                <div style={{fontSize:9,color:T.textMuted}}>${imp.total.toFixed(2)} @ ${imp.rate.toFixed(4)}/gal</div>
                              </div>
                            ) : (
                              <button onClick={() => importFuelFoxPair(em)} disabled={isImp || pdfs.length < 2}
                                style={{padding:"6px 12px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${v.color}`,background:(isImp || pdfs.length < 2)?T.bgSurface:v.color,color:(isImp || pdfs.length < 2)?T.text:"#fff",cursor:isImp?"wait":(pdfs.length < 2 ? "not-allowed":"pointer"),opacity:(isImp || pdfs.length < 2)?0.6:1}}>
                                {isImp ? "Processing..." : "⛽ Import Pair"}
                              </button>
                            )}
                          </div>
                        </div>
                      );
                    }

                    // Default: per-attachment buttons (NuVizz, Uline, Quick Fuel)
                    return (
                      <div key={em.emailId} style={{padding:"8px 10px",borderTop:idx>0?`1px solid ${T.borderLight}`:"none"}}>
                        <div style={{fontSize:12,fontWeight:600}}>{em.emailSubject || "(no subject)"}</div>
                        <div style={{fontSize:10,color:T.textMuted}}>{em.from} • {em.emailDate ? new Date(em.emailDate).toLocaleString() : "—"}</div>
                        {em.attachments?.length > 0 ? (
                          <div style={{marginTop:6,display:"flex",flexDirection:"column",gap:4}}>
                            {em.attachments.map(a => {
                              const refKey = `${em.emailId}:${a.attachmentId}`;
                              const isImp = importing[refKey];
                              const imp = imported[refKey];
                              const disabled = v.comingSoon;
                              const isQuickFuelPdf = v.mode === "quickfuel" && a.filename.toLowerCase().endsWith(".pdf");
                              return (
                                <div key={a.attachmentId} style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8,padding:"4px 8px",background:T.bgSurface,borderRadius:6}}>
                                  <div style={{flex:1,fontSize:11,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>📎 {a.filename} <span style={{color:T.textDim}}>({Math.round(a.size/1024)} KB)</span></div>
                                  {imp?.error ? (
                                    <span style={{fontSize:10,color:T.redText,maxWidth:180,whiteSpace:"normal"}}>✗ {imp.error.substring(0,80)}</span>
                                  ) : imp?.ok && v.mode === "quickfuel" ? (
                                    <div style={{textAlign:"right"}}>
                                      <div style={{fontSize:10,color:T.greenText,fontWeight:700}}>✓ {imp.trucks} trucks</div>
                                      <div style={{fontSize:9,color:T.textMuted}}>${imp.total.toFixed(2)} @ ${imp.rate?.toFixed(4)}/gal</div>
                                    </div>
                                  ) : imp ? (
                                    <span style={{fontSize:10,color:T.greenText,fontWeight:600}}>✓ Imported</span>
                                  ) : disabled ? (
                                    <span style={{fontSize:9,color:T.textDim,fontStyle:"italic"}}>parser pending</span>
                                  ) : (
                                    <button
                                      onClick={() => isQuickFuelPdf ? importQuickFuel(em, a) : importAttachment(em, a)}
                                      disabled={isImp}
                                      style={{padding:"4px 10px",fontSize:10,fontWeight:700,borderRadius:6,border:`1px solid ${v.color}`,background:isImp?T.bgSurface:v.color,color:isImp?T.text:"#fff",cursor:isImp?"wait":"pointer",opacity:isImp?0.6:1}}>
                                      {isImp ? "..." : isQuickFuelPdf ? "⛽ Scan" : "→ Import"}
                                    </button>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        ) : (
                          <div style={{fontSize:10,color:T.textDim,marginTop:4}}>No data attachments</div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </>
    )}
  </div>;
}


// Parse duration strings like "8:30" (H:MM) or "8.5" to decimal hours
function parseHours(v) {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  const s = String(v).trim();
  if (!s) return 0;
  if (s.includes(":")) {
    const [h, m] = s.split(":").map(x => parseInt(x) || 0);
    return h + (m/60);
  }
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

// Compute the Friday week-ending for a YYYY-MM-DD date
function weekEndingFriday(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr + "T00:00:00");
  if (isNaN(d)) return null;
  const day = d.getDay(); // Sun=0...Sat=6
  // Distance to Friday (day 5): if already Fri, 0. Else roll forward to Fri.
  let add = (5 - day + 7) % 7;
  // If Sat or Sun, roll back to prev Fri
  if (day === 6) add = -1;
  if (day === 0) add = -2;
  const f = new Date(d);
  f.setDate(d.getDate() + add);
  return f.toISOString().slice(0,10);
}
function addDays(dateStr, n) { const d = new Date(dateStr+"T00:00:00"); d.setDate(d.getDate()+n); return d.toISOString().slice(0,10); }
function weekLabel(friday) { if (!friday) return "—"; const d = new Date(friday+"T00:00:00"); return d.toLocaleDateString("en-US",{month:"2-digit",day:"2-digit",year:"2-digit"}); }

// ─── Firebase Helpers ────────────────────────────────────────
const hasFirebase = typeof window !== "undefined" && window.db;
const FS = {
  async getCosts() { if (!hasFirebase) return null; try { const d=await window.db.collection("marginiq_config").doc("cost_structure").get(); return d.exists?d.data():null; } catch(e) { return null; } },
  async saveCosts(data) { if (!hasFirebase) return false; try { await window.db.collection("marginiq_config").doc("cost_structure").set({...data, updated_at:new Date().toISOString()}); return true; } catch(e) { return false; } },
  async getWeeklyRollups() { if (!hasFirebase) return []; try { const s=await window.db.collection("uline_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveWeeklyRollup(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("uline_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getReconWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("recon_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveReconWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("recon_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getReconMeta() { if (!hasFirebase) return null; try { const d = await window.db.collection("marginiq_config").doc("recon_meta").get(); return d.exists?d.data():null; } catch(e) { return null; } },
  async saveReconMeta(data) { if (!hasFirebase) return false; try { await window.db.collection("marginiq_config").doc("recon_meta").set(data, {merge:true}); return true; } catch(e) { return false; } },
  async saveUnpaidStop(proKey, data) { if (!hasFirebase) return false; try { await window.db.collection("unpaid_stops").doc(String(proKey)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getUnpaidStops(limit=500) { if (!hasFirebase) return []; try { const s=await window.db.collection("unpaid_stops").orderBy("billed","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveDDISFile(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("ddis_files").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getDDISFiles() { if (!hasFirebase) return []; try { const s=await window.db.collection("ddis_files").orderBy("latest_bill_date","desc").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveFileLog(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("file_log").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getFileLog(limit=500) { if (!hasFirebase) return []; try { const s=await window.db.collection("file_log").orderBy("uploaded_at","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },

  // ─── v2.3 Multi-source helpers ───────────────────────────────
  async saveNuVizzStop(proKey, data) { if (!hasFirebase) return false; try { await window.db.collection("nuvizz_stops").doc(String(proKey)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async saveNuVizzWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("nuvizz_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getNuVizzWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("nuvizz_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveTimeClockDaily(id, data) { if (!hasFirebase) return false; try { await window.db.collection("timeclock_daily").doc(id).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async saveTimeClockWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("timeclock_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getTimeClockWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("timeclock_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async savePayrollWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("payroll_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getPayrollWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("payroll_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveQBOHistory(periodId, data) { if (!hasFirebase) return false; try { await window.db.collection("qbo_history").doc(periodId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getQBOHistory() { if (!hasFirebase) return []; try { const s=await window.db.collection("qbo_history").orderBy("period","desc").limit(120).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveSourceFile(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("source_files").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },

  // ─── Driver Classifications (W2 / 1099 / Unknown) ───────────
  // Key is normalized_name (e.g. "chris head"). Lets us tag historical drivers
  // who aren't in the current Fleet Management roster.
  async getDriverClassifications() { if (!hasFirebase) return []; try { const s=await window.db.collection("driver_classifications").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveDriverClassification(key, data) { if (!hasFirebase) return false; try { await window.db.collection("driver_classifications").doc(key).set({...data, updated_at:new Date().toISOString()}, {merge:true}); return true; } catch(e) { return false; } },

  // ─── AuditIQ: audit_items, customer_ap_contacts, disputes ───
  async saveAuditItem(id, data) { if (!hasFirebase) return false; try { await window.db.collection("audit_items").doc(String(id)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getAuditItems(limit=2000) { if (!hasFirebase) return []; try { const s=await window.db.collection("audit_items").orderBy("variance","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async deleteAuditItem(id) { if (!hasFirebase) return false; try { await window.db.collection("audit_items").doc(String(id)).delete(); return true; } catch(e) { return false; } },

  async getAPContacts() { if (!hasFirebase) return []; try { const s=await window.db.collection("customer_ap_contacts").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveAPContact(key, data) { if (!hasFirebase) return false; try { await window.db.collection("customer_ap_contacts").doc(key).set({...data, updated_at:new Date().toISOString()}, {merge:true}); return true; } catch(e) { return false; } },

  async getDisputes() { if (!hasFirebase) return []; try { const s=await window.db.collection("disputes").orderBy("updated_at","desc").limit(500).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveDispute(id, data) { if (!hasFirebase) return false; try { await window.db.collection("disputes").doc(String(id)).set({...data, updated_at:new Date().toISOString()}, {merge:true}); return true; } catch(e) { return false; } },

  // ─── Fuel (v2.6) ─────────────────────────────────────────────
  async getFuelInvoices() { if (!hasFirebase) return []; try { const s=await window.db.collection("fuel_invoices").orderBy("invoice_date","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveFuelInvoice(id, data) { if (!hasFirebase) return false; try { await window.db.collection("fuel_invoices").doc(String(id)).set({...data, updated_at:new Date().toISOString()}, {merge:true}); return true; } catch(e) { return false; } },
  async getFuelByTruck(limit=3000) { if (!hasFirebase) return []; try { const s=await window.db.collection("fuel_by_truck").orderBy("service_date","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveFuelByTruck(id, data) { if (!hasFirebase) return false; try { await window.db.collection("fuel_by_truck").doc(String(id)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getFuelWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("fuel_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveFuelWeekly(id, data) { if (!hasFirebase) return false; try { await window.db.collection("fuel_weekly").doc(String(id)).set({...data, updated_at:new Date().toISOString()}, {merge:true}); return true; } catch(e) { return false; } },
};

// ─── File Type Detection ────────────────────────────────────
function detectFileType(filename, firstRow) {
  const fn = filename.toLowerCase();
  // Uline family (existing)
  if (fn.startsWith("ddis820") || fn.includes("ddis")) return "ddis";
  if (fn.startsWith("master")) return "master";
  if (fn.includes("accessorial") || fn.includes("acesorial") || fn.includes("acessorial") || fn.includes("accesorial") || fn.includes("acceessorial")) return "accessorials";
  if (fn.startsWith("das") || fn.startsWith("das ")) return "original";
  // NuVizz family
  if (fn.includes("driver_stops") || fn.includes("driver stops") || fn.includes("nuvizz")) return "nuvizz";
  // Time clock / SENTINEL
  if (fn.includes("sentinel") || fn.includes("timeclock") || fn.includes("time_clock") || fn.includes("b600") || fn.includes("punch")) return "timeclock";
  // Payroll (CyberPay)
  if (fn.includes("cyberpay") || fn.includes("payroll") || fn.includes("paydetail") || fn.includes("pay_detail") || fn.includes("pay_register") || fn.includes("payregister")) return "payroll";
  // QBO
  if (fn.includes("profit") && fn.includes("loss")) return "qbo_pl";
  if (fn.includes("p&l") || fn.includes("p_l")) return "qbo_pl";
  if (fn.includes("trial_balance") || fn.includes("trial balance") || fn.includes("tb_")) return "qbo_tb";
  if (fn.includes("general_ledger") || fn.includes("general ledger") || fn.includes("_gl_") || fn.includes(" gl ")) return "qbo_gl";
  if (fn.includes("quickbooks") || fn.includes("qbo_")) return "qbo_pl"; // default QBO export → P&L

  if (firstRow) {
    const keys = Object.keys(firstRow).map(k => k.toLowerCase().trim());
    const keySet = new Set(keys);
    // Uline DDIS
    if (keySet.has("voucher#") || keySet.has("check#")) return "ddis";
    // NuVizz: has Stop Number + Driver Name + Ship To
    if (keySet.has("stop number") && keySet.has("driver name")) return "nuvizz";
    // Time clock: employee + punch in/out or clock in/out
    if ((keySet.has("employee") || keySet.has("employee name") || keySet.has("driver") || keySet.has("driver name")) &&
        (keySet.has("punch in") || keySet.has("clock in") || keySet.has("time in") || keySet.has("in") || keySet.has("start time"))) return "timeclock";
    // Time clock (CyberPay / B600 format): Display Name + In Time + Out Time + REG
    if ((keySet.has("display name") || keySet.has("display id") || keySet.has("payroll id")) &&
        keySet.has("in time") && keySet.has("out time") &&
        (keySet.has("reg") || keySet.has("total"))) return "timeclock";
    // Payroll: employee + hours + gross
    if ((keySet.has("employee") || keySet.has("employee name") || keySet.has("name")) &&
        (keySet.has("gross") || keySet.has("gross pay") || keySet.has("total pay")) &&
        (keySet.has("hours") || keySet.has("reg hours") || keySet.has("regular hours") || keySet.has("total hours"))) return "payroll";
    // QBO P&L: "account" + "total" or "amount"
    if (keySet.has("account") && (keySet.has("total") || keySet.has("amount") || keySet.has("balance"))) {
      if (keySet.has("debit") || keySet.has("credit")) return "qbo_tb";
      return "qbo_pl";
    }
    // Uline original/accessorial
    if (keys.some(k => k === "pro" || k === "pro#") && keySet.has("new cost")) {
      return firstRow.code ? "accessorials" : "original";
    }
  }
  return "unknown";
}

// Normalize file type to source group for UI grouping
function sourceGroup(kind) {
  if (["master","original","accessorials","ddis"].includes(kind)) return "uline";
  if (kind === "nuvizz") return "nuvizz";
  if (kind === "timeclock") return "timeclock";
  if (kind === "payroll") return "payroll";
  if (["qbo_pl","qbo_tb","qbo_gl"].includes(kind)) return "qbo";
  return "unknown";
}

async function readWorkbook(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = e => {
      try {
        const wb = XLSX.read(e.target.result, { type: "array" });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
        const norm = rows.map(r => { const o = {}; Object.keys(r).forEach(k => o[String(k).toLowerCase().trim()] = r[k]); return o; });
        resolve(norm);
      } catch(err) { reject(err); }
    };
    r.onerror = reject;
    r.readAsArrayBuffer(file);
  });
}
async function readCSV(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = e => {
      try {
        const wb = XLSX.read(e.target.result, { type: "string" });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
        const norm = rows.map(r => { const o = {}; Object.keys(r).forEach(k => o[String(k).toLowerCase().trim()] = r[k]); return o; });
        resolve(norm);
      } catch(err) { reject(err); }
    };
    r.onerror = reject;
    r.readAsText(file);
  });
}

function normalizePro(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (s === "") return null;
  s = s.replace(/\.0+$/,"");
  const stripped = s.replace(/^0+/,"");
  return stripped || s;
}

function parseOriginalOrAccessorial(rows) {
  const stops = [];
  for (const r of rows) {
    const proRaw = r.pro ?? r["pro#"];
    if (!proRaw) continue;
    const proStr = String(proRaw).toLowerCase();
    if (proStr === "pro" || proStr === "pro#") continue;
    const pro = normalizePro(proRaw);
    if (!pro) continue;
    const cost = parseFloat(r.cost) || 0;
    const newCost = parseFloat(r["new cost"]) || 0;
    const extraCost = parseFloat(r["extra cost"]) || 0;
    const wgt = parseFloat(r.wgt) || 0;
    const skid = parseInt(r.skid) || 0;
    const loose = parseInt(r.loose) || 0;
    const pu = r.pu ? parseInt(r.pu) : null;
    stops.push({
      pro,
      order: r.order ? String(r.order) : null,
      customer: r.customer ? String(r.customer).trim() : null,
      city: r.city ? String(r.city).trim() : null,
      state: r.st ? String(r.st).trim() : null,
      zip: r.zip ? String(r.zip).trim() : null,
      pu, pu_date: pu ? puToDate(pu) : null,
      month: pu ? puToMonth(pu) : null,
      week_ending: pu ? weekEndingFriday(puToDate(pu)) : null,
      cost, new_cost: newCost || cost, extra_cost: extraCost,
      warehouse: r.wh ? String(r.wh).trim() : null,
      skid, loose, weight: wgt,
      via: r.via ? String(r.via).trim() : null,
      code: r.code ? String(r.code).trim() : null,
      is_accessorial: !!r.code && (extraCost > 0),
    });
  }
  return stops;
}

function parseDDIS(rows) {
  const payments = [];
  for (const r of rows) {
    const proRaw = r["pro#"];
    if (!proRaw) continue;
    const pro = normalizePro(proRaw);
    if (!pro) continue;
    const paidAmount = parseFloat(r["paid amount"]) || 0;
    const billDate = parseDateMDY(r["bill date"]);
    payments.push({
      pro,
      voucher: r["voucher#"] ? String(r["voucher#"]) : null,
      check: r["check#"] ? String(r["check#"]) : null,
      bill_date: billDate,
      paid: paidAmount,
    });
  }
  return payments;
}

// ─── NuVizz Parser ──────────────────────────────────────────
// Columns: Delivery End, Stop Number, Stop Status, Driver Name,
//          Ship To Name, Ship To, Ship To - City, Ship To - Zip Code, Stop SealNbr
// "Stop SealNbr" is the base dollar amount used to calculate contractor pay.
// 1099 contractors get 40% of SealNbr per stop. W2 drivers are paid hourly (CyberPay).
// Driver W2/1099 classification lives in Fleet Management.
const CONTRACTOR_PAY_PCT = 0.40;

function parseNuVizz(rows) {
  const stops = [];
  for (const r of rows) {
    const stopNum = r["stop number"];
    const driver = r["driver name"];
    if (!stopNum && !driver) continue; // skip blank rows
    const deliveryDate = parseDateMDYFlexible(r["delivery end"]);
    const status = r["stop status"] ? String(r["stop status"]).trim() : null;
    const shipTo = r["ship to name"] ? String(r["ship to name"]).trim() : null;
    const city = r["ship to - city"] ? String(r["ship to - city"]).trim() : null;
    const zip = r["ship to - zip code"] ? String(r["ship to - zip code"]).trim() : null;
    const payBase = parseMoney(r["stop sealnbr"]);
    const contractorPay = payBase * CONTRACTOR_PAY_PCT; // applied only if driver is 1099
    const pro = normalizePro(stopNum); // cross-reference to Uline PRO
    stops.push({
      stop_number: stopNum ? String(stopNum).trim() : null,
      pro,
      driver_name: normalizeName(driver),
      delivery_date: deliveryDate,
      week_ending: deliveryDate ? weekEndingFriday(deliveryDate) : null,
      month: deliveryDate ? dateToMonth(deliveryDate) : null,
      status,
      ship_to: shipTo,
      city, zip,
      contractor_pay_base: payBase,         // raw $ from SealNbr column
      contractor_pay_at_40: contractorPay,  // 40% — actual cost IF driver is 1099
    });
  }
  return stops;
}

// Build NuVizz weekly rollups
// Note: contractor_pay_total assumes ALL drivers are 1099. Actual cost depends on
// Fleet Management's W2/1099 classification applied downstream.
function buildNuVizzWeekly(stops) {
  const byWeek = {};
  for (const s of stops) {
    if (!s.week_ending) continue;
    const w = s.week_ending;
    if (!byWeek[w]) {
      byWeek[w] = {
        week_ending: w, month: s.month,
        stops_total: 0, stops_completed: 0,
        pay_base_total: 0,             // sum of all SealNbr values
        contractor_pay_if_all_1099: 0, // upper bound cost if every driver were 1099
        unique_drivers: new Set(),
        unique_customers: new Set(),
        drivers: {},
      };
    }
    const bw = byWeek[w];
    bw.stops_total++;
    if (s.status && s.status.toLowerCase() === "completed") bw.stops_completed++;
    bw.pay_base_total += s.contractor_pay_base || 0;
    bw.contractor_pay_if_all_1099 += s.contractor_pay_at_40 || 0;
    if (s.driver_name) {
      bw.unique_drivers.add(s.driver_name);
      if (!bw.drivers[s.driver_name]) bw.drivers[s.driver_name] = { stops:0, pay_base:0, pay_at_40:0 };
      bw.drivers[s.driver_name].stops++;
      bw.drivers[s.driver_name].pay_base += s.contractor_pay_base || 0;
      bw.drivers[s.driver_name].pay_at_40 += s.contractor_pay_at_40 || 0;
    }
    if (s.ship_to) bw.unique_customers.add(s.ship_to);
  }
  return Object.values(byWeek).map(w => ({
    ...w,
    unique_drivers: w.unique_drivers.size,
    unique_customers: w.unique_customers.size,
    top_drivers: Object.entries(w.drivers).sort((a,b) => b[1].stops - a[1].stops).slice(0,60).map(([name, v]) => ({name, ...v})),
    drivers: undefined,
  }));
}

// ─── Time Clock Parser ──────────────────────────────────────
// Flexible column mapping: employee/driver, date, clock in, clock out, hours
// Supports both generic timeclock format AND CyberPay/B600 format (Display Name, In Time, Out Time, REG, OT1, OT2, Total)
function parseTimeClock(rows) {
  const entries = [];
  for (const r of rows) {
    const name = r["employee"] || r["employee name"] || r["driver"] || r["driver name"] || r["name"] || r["display name"] || r["payroll id"];
    if (!name) continue;
    const dateRaw = r["date"] || r["punch date"] || r["work date"] || r["day"];
    const date = parseDateMDYFlexible(dateRaw) || (dateRaw ? parseDateMDY(dateRaw) : null);
    const clockIn = r["clock in"] || r["punch in"] || r["time in"] || r["in"] || r["start time"] || r["in time"];
    const clockOut = r["clock out"] || r["punch out"] || r["time out"] || r["out"] || r["end time"] || r["out time"];
    // Hours: prefer explicit "Total" (CyberPay), else sum REG+OT1+OT2, else fall back to generic hours column
    const regHrs = parseHours(r["reg"] ?? r["reg hours"] ?? r["regular hours"] ?? 0) || 0;
    const ot1Hrs = parseHours(r["ot1"] ?? r["ot hours"] ?? r["overtime"] ?? 0) || 0;
    const ot2Hrs = parseHours(r["ot2"] ?? 0) || 0;
    const totalExplicit = parseHours(r["total"] ?? r["total hours"] ?? r["hours"] ?? r["worked"] ?? r["duration"]);
    const hours = totalExplicit || (regHrs + ot1Hrs + ot2Hrs) || 0;
    entries.push({
      employee: normalizeName(name),
      date,
      week_ending: date ? weekEndingFriday(date) : null,
      month: date ? dateToMonth(date) : null,
      clock_in: clockIn ? String(clockIn).trim() : null,
      clock_out: clockOut ? String(clockOut).trim() : null,
      hours,
      reg_hours: regHrs,
      ot_hours: ot1Hrs + ot2Hrs,
      department: r["department"] ? String(r["department"]).trim() : null,
    });
  }
  return entries;
}

function buildTimeClockWeekly(entries) {
  const byWeek = {};
  for (const e of entries) {
    if (!e.week_ending || !e.employee) continue;
    const w = e.week_ending;
    if (!byWeek[w]) {
      byWeek[w] = {
        week_ending: w, month: e.month,
        total_hours: 0, reg_hours: 0, ot_hours: 0, days_worked: 0,
        unique_employees: new Set(),
        employees: {},
      };
    }
    const bw = byWeek[w];
    bw.total_hours += e.hours || 0;
    bw.reg_hours += e.reg_hours || 0;
    bw.ot_hours += e.ot_hours || 0;
    bw.days_worked++;
    bw.unique_employees.add(e.employee);
    if (!bw.employees[e.employee]) bw.employees[e.employee] = { hours:0, reg:0, ot:0, days:0 };
    bw.employees[e.employee].hours += e.hours || 0;
    bw.employees[e.employee].reg += e.reg_hours || 0;
    bw.employees[e.employee].ot += e.ot_hours || 0;
    bw.employees[e.employee].days++;
  }
  return Object.values(byWeek).map(w => ({
    ...w,
    unique_employees: w.unique_employees.size,
    top_employees: Object.entries(w.employees).sort((a,b) => b[1].hours - a[1].hours).slice(0,60).map(([name, v]) => ({name, ...v})),
    employees: undefined,
  }));
}

// ─── Payroll (CyberPay) Parser ──────────────────────────────
// Columns: employee, pay period end, reg hours, OT hours, gross pay
function parsePayroll(rows) {
  const entries = [];
  for (const r of rows) {
    const name = r["employee"] || r["employee name"] || r["name"];
    if (!name) continue;
    const periodRaw = r["pay period end"] || r["period end"] || r["pay date"] || r["check date"] || r["week ending"];
    const periodEnd = parseDateMDYFlexible(periodRaw) || (periodRaw ? parseDateMDY(periodRaw) : null);
    const regHours = parseHours(r["reg hours"] || r["regular hours"] || r["hours"] || 0);
    const otHours = parseHours(r["ot hours"] || r["overtime hours"] || r["overtime"] || 0);
    const gross = parseMoney(r["gross"] || r["gross pay"] || r["total pay"] || r["pay"] || 0);
    const net = parseMoney(r["net"] || r["net pay"] || 0);
    const type = r["type"] || r["class"] || r["driver type"] || null; // W2/1099 if present
    entries.push({
      employee: normalizeName(name),
      period_end: periodEnd,
      week_ending: periodEnd ? weekEndingFriday(periodEnd) : null,
      month: periodEnd ? dateToMonth(periodEnd) : null,
      reg_hours: regHours,
      ot_hours: otHours,
      total_hours: regHours + otHours,
      gross, net,
      classification: type ? String(type).trim() : null,
    });
  }
  return entries;
}

function buildPayrollWeekly(entries) {
  const byWeek = {};
  for (const e of entries) {
    if (!e.week_ending || !e.employee) continue;
    const w = e.week_ending;
    if (!byWeek[w]) {
      byWeek[w] = {
        week_ending: w, month: e.month,
        gross_total: 0, net_total: 0,
        hours_total: 0, ot_hours_total: 0,
        employee_count: 0,
        employees: [],
      };
    }
    const bw = byWeek[w];
    bw.gross_total += e.gross || 0;
    bw.net_total += e.net || 0;
    bw.hours_total += e.total_hours || 0;
    bw.ot_hours_total += e.ot_hours || 0;
    bw.employee_count++;
    bw.employees.push({
      name: e.employee, gross: e.gross, hours: e.total_hours, ot: e.ot_hours,
      classification: e.classification,
    });
  }
  // Keep top 60 employees per week to stay within Firebase doc limits
  return Object.values(byWeek).map(w => ({
    ...w,
    employees: w.employees.sort((a,b) => b.gross - a.gross).slice(0, 60),
  }));
}

// ─── QBO Parser ─────────────────────────────────────────────
// P&L and Trial Balance exports have similar structure: account + amounts
function parseQBO(rows, kind) {
  const entries = [];
  // Try to detect period from filename or from a header row
  for (const r of rows) {
    const account = r["account"] || r["account name"] || r["name"];
    if (!account) continue;
    const amt = parseMoney(r["total"] || r["amount"] || r["balance"]);
    const debit = parseMoney(r["debit"]);
    const credit = parseMoney(r["credit"]);
    const period = r["period"] || r["month"] || r["date"] || null;
    entries.push({
      account: String(account).trim(),
      amount: amt || (debit - credit),
      debit, credit,
      period: period ? (parseDateMDYFlexible(period) || String(period).trim()) : null,
      report_type: kind, // qbo_pl, qbo_tb, qbo_gl
    });
  }
  return entries;
}

// ─── Build Weekly Rollups ──────────────────────────────────
function buildWeeklyRollups(stops) {
  const byWeek = {};
  for (const s of stops) {
    if (!s.week_ending) continue;
    const w = s.week_ending;
    if (!byWeek[w]) {
      byWeek[w] = {
        week_ending: w,
        month: s.month,
        stops: 0, revenue: 0, base_revenue: 0, accessorial_revenue: 0,
        weight: 0, skids: 0,
        accessorial_count: 0,
        unique_pros: new Set(),
        customers: {},
        cities: {},
      };
    }
    const bw = byWeek[w];
    bw.stops++;
    bw.revenue += s.new_cost || 0;
    bw.base_revenue += s.cost || 0;
    bw.accessorial_revenue += s.extra_cost || 0;
    bw.weight += s.weight || 0;
    bw.skids += s.skid || 0;
    bw.unique_pros.add(s.pro);
    if (s.is_accessorial) bw.accessorial_count++;
    if (s.customer) {
      if (!bw.customers[s.customer]) bw.customers[s.customer] = { stops:0, revenue:0 };
      bw.customers[s.customer].stops++;
      bw.customers[s.customer].revenue += s.new_cost || 0;
    }
    if (s.city) {
      if (!bw.cities[s.city]) bw.cities[s.city] = { stops:0, revenue:0 };
      bw.cities[s.city].stops++;
      bw.cities[s.city].revenue += s.new_cost || 0;
    }
  }
  return Object.values(byWeek).map(w => ({
    ...w,
    unique_pros: w.unique_pros.size,
    // Keep top 20 customers and cities for Firebase storage
    top_customers: Object.entries(w.customers).sort((a,b) => b[1].revenue - a[1].revenue).slice(0,20).map(([name, v]) => ({name, ...v})),
    top_cities: Object.entries(w.cities).sort((a,b) => b[1].revenue - a[1].revenue).slice(0,20).map(([name, v]) => ({name, ...v})),
    customers: undefined,
    cities: undefined,
  }));
}

// ─── Data Completeness Scanner ──────────────────────────────
function scanCompleteness(weeklyRollups, ulineFiles, fromDate=BUSINESS_START) {
  if (weeklyRollups.length === 0) return { expected: [], gaps: [], sparseWeeks: [], missingAccessorials: [] };

  // Get all week-endings from business start to latest in data
  const sorted = [...weeklyRollups].sort((a,b) => a.week_ending.localeCompare(b.week_ending));
  const firstWE = sorted[0].week_ending;
  const lastWE = sorted[sorted.length-1].week_ending;
  const startWE = firstWE < fromDate ? weekEndingFriday(fromDate) : firstWE;

  // Generate expected Fridays from startWE to lastWE
  const expected = [];
  let cur = startWE;
  while (cur <= lastWE) {
    expected.push(cur);
    cur = addDays(cur, 7);
  }

  const actual = new Set(weeklyRollups.map(r => r.week_ending));
  const gaps = expected.filter(w => !actual.has(w));

  // Avg stops for sparse detection
  const validRollups = weeklyRollups.filter(r => r.week_ending >= fromDate && r.stops > 100);
  const avgStops = validRollups.length > 0 ? validRollups.reduce((s,r) => s+r.stops, 0) / validRollups.length : 0;
  const sparseThreshold = Math.max(100, avgStops * 0.3);
  const sparseWeeks = weeklyRollups
    .filter(r => r.week_ending >= fromDate && r.stops < sparseThreshold)
    .map(r => ({ week_ending: r.week_ending, stops: r.stops, revenue: r.revenue, expected_avg: Math.round(avgStops) }));

  // Weeks with stops but no accessorials at all — suspicious given that accessorial rate avg ~1-5%
  const missingAccessorials = weeklyRollups
    .filter(r => r.week_ending >= fromDate && r.stops >= 100 && r.accessorial_count === 0)
    .map(r => ({ week_ending: r.week_ending, stops: r.stops, revenue: r.revenue }));

  return { expected, gaps, sparseWeeks, missingAccessorials, avgStops: Math.round(avgStops), firstWE: startWE, lastWE };
}

// ─── Reconciliation Matching ────────────────────────────────
function buildReconWeekly(weeklyRollups, paymentByPro) {
  // For each weekly rollup, sum up how much was paid against those PROs
  // This requires going back to stop-level data, which we only have in-memory during ingest.
  // So instead we store per-pro paid, then in the recon view we show collection rate at week level
  // Actually we rebuild recon during ingest from the stops we just parsed
  return [];
}

// ─── MARGIN ENGINE ──────────────────────────────────────────
function calculateMargins(costs, weeklyWindow) {
  const c = { ...DEFAULT_COSTS, ...costs };
  const wd = c.working_days_year || 260;
  const annualBoxDrivers = c.count_box_drivers * c.rate_box_driver * c.avg_hours_per_shift * wd;
  const annualTractorDrivers = c.count_tractor_drivers * c.rate_tractor_driver * c.avg_hours_per_shift * wd;
  const annualDispatchers = c.count_dispatchers * c.rate_dispatcher * 8 * wd;
  const annualAdmin = c.count_admin * c.rate_admin * 8 * wd;
  const annualMechanics = c.count_mechanics * c.rate_mechanic * 8 * wd;
  const annualForkliftOps = c.count_forklift_ops * 20 * 8 * wd;
  const totalAnnualLabor = annualBoxDrivers + annualTractorDrivers + annualDispatchers + annualAdmin + annualMechanics + annualForkliftOps;
  const totalTrucks = (c.truck_count_box||0) + (c.truck_count_tractor||0);
  const annualInsurance = c.truck_insurance_monthly * totalTrucks * 12;
  const annualWarehouse = c.warehouse || 0;
  const annualForklifts = c.forklifts || 0;
  const totalAnnualFixed = annualWarehouse + annualForklifts + annualInsurance;
  const totalAnnualCost = totalAnnualLabor + totalAnnualFixed;
  const dailyCost = totalAnnualCost / wd;
  const monthlyCost = totalAnnualCost / 12;

  let dailyStops = 600;
  let dailyRevenue = 0;
  let annualRevenue = 0;

  if (weeklyWindow && weeklyWindow.weeksCount > 0) {
    const weeklyAvgRevenue = weeklyWindow.totalRevenue / weeklyWindow.weeksCount;
    const weeklyAvgStops = weeklyWindow.totalStops / weeklyWindow.weeksCount;
    annualRevenue = weeklyAvgRevenue * 52;
    dailyRevenue = weeklyAvgRevenue / 5;
    dailyStops = weeklyAvgStops / 5;
  }

  const costPerStop = dailyCost / (dailyStops || 1);
  const dailyMargin = dailyRevenue - dailyCost;
  const dailyMarginPct = dailyRevenue > 0 ? (dailyMargin / dailyRevenue * 100) : 0;
  const revenuePerStop = dailyStops > 0 ? dailyRevenue / dailyStops : 0;
  const marginPerStop = revenuePerStop - costPerStop;
  const marginPerStopPct = revenuePerStop > 0 ? (marginPerStop / revenuePerStop * 100) : 0;
  const totalDrivers = c.count_box_drivers + c.count_tractor_drivers;
  const stopsPerDriver = dailyStops / (totalDrivers || 1);
  const revenuePerDriver = dailyRevenue / (totalDrivers || 1);
  const costPerDriver = dailyCost / (totalDrivers || 1);
  const marginPerDriver = revenuePerDriver - costPerDriver;
  const revenuePerTruck = dailyRevenue / (totalTrucks || 1);
  const costPerTruck = dailyCost / (totalTrucks || 1);
  const breakEvenStopsDaily = revenuePerStop > 0 ? (dailyCost / revenuePerStop) : 0;

  return {
    totalAnnualCost, totalAnnualLabor, totalAnnualFixed,
    annualBoxDrivers, annualTractorDrivers, annualDispatchers, annualAdmin, annualMechanics, annualForkliftOps,
    annualInsurance, annualWarehouse, annualForklifts,
    annualRevenue, monthlyCost,
    dailyCost, dailyRevenue, dailyMargin, dailyMarginPct, dailyStops,
    costPerStop, revenuePerStop, marginPerStop, marginPerStopPct,
    totalDrivers, stopsPerDriver, revenuePerDriver, costPerDriver, marginPerDriver,
    totalTrucks, revenuePerTruck, costPerTruck,
    breakEvenStopsDaily,
    costBreakdown: [
      { name: "Box Truck Drivers", value: annualBoxDrivers, color: "#3b82f6" },
      { name: "Tractor Drivers", value: annualTractorDrivers, color: "#6366f1" },
      { name: "Warehouse", value: annualWarehouse, color: "#f59e0b" },
      { name: "Forklift Ops", value: annualForkliftOps, color: "#10b981" },
      { name: "Insurance", value: annualInsurance, color: "#ef4444" },
      { name: "Forklifts", value: annualForklifts, color: "#8b5cf6" },
      { name: "Dispatch", value: annualDispatchers, color: "#ec4899" },
      { name: "Admin", value: annualAdmin, color: "#14b8a6" },
      { name: "Mechanics", value: annualMechanics, color: "#f97316" },
    ].filter(c => c.value > 0),
  };
}

// ─── Shared UI ──────────────────────────────────────────────
const cardStyle = { background:T.bgCard, borderRadius:T.radius, padding:"16px", border:`1px solid ${T.border}`, boxShadow:T.shadow, marginBottom:"12px" };
const inputStyle = { width:"100%", padding:"8px 12px", borderRadius:"8px", border:`1px solid ${T.border}`, background:T.bgSurface, color:T.text, fontSize:"13px", outline:"none", fontFamily:"inherit" };

function KPI({ label, value, sub, subColor, icon }) {
  return <div style={{...cardStyle, padding:"14px 16px", marginBottom:0}}>
    <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
      {icon && <span style={{fontSize:14}}>{icon}</span>}
      <span style={{fontSize:"10px",color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600}}>{label}</span>
    </div>
    <div style={{fontSize:"22px",fontWeight:700,color:T.text,letterSpacing:"-0.02em"}}>{value}</div>
    {sub && <div style={{fontSize:"11px",marginTop:"3px",fontWeight:500,color:subColor||T.textMuted}}>{sub}</div>}
  </div>;
}
function Badge({ text, color, bg }) {
  return <span style={{fontSize:"10px",fontWeight:700,color:color||T.brand,background:bg||T.brandPale,padding:"2px 8px",borderRadius:"5px",whiteSpace:"nowrap"}}>{text}</span>;
}
function MiniBar({ pct, color, height=6 }) {
  return <div style={{width:"100%",height,borderRadius:3,background:T.borderLight,overflow:"hidden"}}>
    <div style={{height:"100%",borderRadius:3,background:color||T.brand,width:`${Math.min(Math.max(pct||0,0),100)}%`,transition:"width 0.6s"}} />
  </div>;
}
function SectionTitle({ icon, text, right }) {
  return <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:"12px"}}>
    <div style={{fontSize:"15px",fontWeight:700,color:T.text,display:"flex",alignItems:"center",gap:"8px"}}>
      {icon && <span>{icon}</span>}{text}
    </div>
    {right}
  </div>;
}
function DataRow({ label, value, valueColor, bold }) {
  return <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.borderLight}`}}>
    <span style={{fontSize:"12px",color:T.textMuted}}>{label}</span>
    <span style={{fontSize:"13px",fontWeight:bold?700:600,color:valueColor||T.text}}>{value}</span>
  </div>;
}
function EmptyState({ icon, title, sub }) {
  return <div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}>
    <div style={{fontSize:36,marginBottom:8}}>{icon||"📊"}</div>
    <div style={{fontSize:14,fontWeight:600,color:T.text,marginBottom:4}}>{title}</div>
    <div style={{fontSize:12}}>{sub}</div>
  </div>;
}
function TabButton({ active, label, onClick }) {
  return <button onClick={onClick} style={{padding:"8px 16px",borderRadius:"8px",border:"none",background:active?T.brand:"transparent",color:active?"#fff":T.textMuted,fontSize:"12px",fontWeight:active?700:500,cursor:"pointer",whiteSpace:"nowrap",transition:"all 0.2s"}}>{label}</button>;
}
function PrimaryBtn({ text, onClick, loading, disabled, style:sx }) {
  return <button onClick={onClick} disabled={loading||disabled} style={{padding:"10px 20px",borderRadius:"10px",border:"none",background:(loading||disabled)?"#94a3b8":`linear-gradient(135deg,${T.brand},${T.brandLight})`,color:"#fff",fontSize:"13px",fontWeight:700,cursor:(loading||disabled)?"not-allowed":"pointer",...sx}}>{loading?"Loading...":text}</button>;
}
function BarChart({ data, labelKey, valueKey, color, maxBars=15, formatValue }) {
  const items = data.slice(0, maxBars);
  const max = Math.max(...items.map(d => d[valueKey] || 0), 1);
  const fv = formatValue || fmt;
  return <div>{items.map((d, i) => {
    const pct = (d[valueKey] || 0) / max * 100;
    return <div key={i} style={{display:"flex",alignItems:"center",gap:10,marginBottom:6}}>
      <div style={{width:90,fontSize:11,color:T.textMuted,flexShrink:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={d[labelKey]}>{d[labelKey]}</div>
      <div style={{flex:1,height:22,background:T.borderLight,borderRadius:4,overflow:"hidden"}}>
        <div style={{height:"100%",width:`${pct}%`,background:color||`linear-gradient(90deg,${T.brand},${T.brandLight})`,borderRadius:4,display:"flex",alignItems:"center",paddingLeft:8,transition:"width 0.4s"}}>
          {pct>20&&<span style={{fontSize:10,color:"#fff",fontWeight:700}}>{fv(d[valueKey])}</span>}
        </div>
      </div>
      <div style={{width:70,fontSize:12,fontWeight:700,textAlign:"right",color:T.text}}>{fv(d[valueKey])}</div>
    </div>;
  })}</div>;
}
function DonutChart({ data, size=180 }) {
  const total = data.reduce((s,d) => s+d.value, 0);
  if (total === 0) return null;
  const cx=size/2, cy=size/2, r=size*0.35, strokeW=size*0.15;
  let cum = -90;
  const arcs = data.map(d => {
    const pct = d.value / total, angle = pct * 360;
    const start = cum; cum += angle; const end = cum;
    const la = angle > 180 ? 1 : 0, rad = Math.PI/180;
    const x1 = cx + r*Math.cos(start*rad), y1 = cy + r*Math.sin(start*rad);
    const x2 = cx + r*Math.cos(end*rad), y2 = cy + r*Math.sin(end*rad);
    return { ...d, pct, path: `M ${x1} ${y1} A ${r} ${r} 0 ${la} 1 ${x2} ${y2}` };
  });
  return <div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}>
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      {arcs.map((a,i) => <path key={i} d={a.path} fill="none" stroke={a.color} strokeWidth={strokeW} strokeLinecap="butt" />)}
      <text x={cx} y={cy-6} textAnchor="middle" fill={T.text} fontSize="16" fontWeight="800" fontFamily="DM Sans">{fmtK(total)}</text>
      <text x={cx} y={cy+10} textAnchor="middle" fill={T.textMuted} fontSize="9" fontFamily="DM Sans">ANNUAL</text>
    </svg>
    <div style={{flex:1,minWidth:120}}>
      {arcs.filter(a=>a.pct>0.02).map((a,i) => <div key={i} style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}>
        <div style={{width:8,height:8,borderRadius:2,background:a.color,flexShrink:0}} />
        <span style={{fontSize:11,color:T.textMuted,flex:1}}>{a.name}</span>
        <span style={{fontSize:11,fontWeight:700,color:T.text}}>{fmtPct(a.pct*100,0)}</span>
      </div>)}
    </div>
  </div>;
}
function LineTrend({ data, xKey, yKey, y2Key, label, y2Label, color, color2, height=200 }) {
  if (!data || data.length === 0) return null;
  const W = 640, H = height, pad = { t:20, r:20, b:40, l:60 };
  const chartW = W - pad.l - pad.r, chartH = H - pad.t - pad.b;
  const maxY = Math.max(...data.map(d => Math.max(d[yKey]||0, d[y2Key]||0)), 1);
  const xStep = chartW / Math.max(data.length-1, 1);
  const points1 = data.map((d,i) => [pad.l + i*xStep, pad.t + chartH - (d[yKey]/maxY)*chartH]);
  const points2 = y2Key ? data.map((d,i) => [pad.l + i*xStep, pad.t + chartH - (d[y2Key]/maxY)*chartH]) : null;
  const path1 = "M " + points1.map(p => `${p[0]} ${p[1]}`).join(" L ");
  const path2 = points2 ? "M " + points2.map(p => `${p[0]} ${p[1]}`).join(" L ") : null;
  return <svg viewBox={`0 0 ${W} ${H}`} style={{width:"100%",height,overflow:"visible"}}>
    {[0, 0.25, 0.5, 0.75, 1].map((t,i) => {
      const y = pad.t + chartH - t*chartH;
      return <g key={i}>
        <line x1={pad.l} y1={y} x2={pad.l+chartW} y2={y} stroke={T.borderLight} strokeWidth="1" />
        <text x={pad.l-8} y={y+4} textAnchor="end" fontSize="10" fill={T.textDim}>{fmtK(maxY*t)}</text>
      </g>;
    })}
    {data.map((d,i) => {
      if (data.length > 12 && i % Math.ceil(data.length/12) !== 0) return null;
      const x = pad.l + i*xStep;
      return <text key={i} x={x} y={H-pad.b+18} textAnchor="middle" fontSize="9" fill={T.textDim}>{String(d[xKey]).slice(5)}</text>;
    })}
    <path d={path1} fill="none" stroke={color||T.brand} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
    {path2 && <path d={path2} fill="none" stroke={color2||T.green} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="4 4" />}
    {points1.map((p,i) => <circle key={i} cx={p[0]} cy={p[1]} r="3" fill={color||T.brand} />)}
    {points2 && points2.map((p,i) => <circle key={i} cx={p[0]} cy={p[1]} r="3" fill={color2||T.green} />)}
    <g transform={`translate(${pad.l}, 4)`}>
      <circle cx="4" cy="8" r="3" fill={color||T.brand} />
      <text x="12" y="12" fontSize="10" fill={T.text} fontWeight="600">{label||"Series 1"}</text>
      {y2Label && <>
        <circle cx="80" cy="8" r="3" fill={color2||T.green} />
        <text x="88" y="12" fontSize="10" fill={T.text} fontWeight="600">{y2Label}</text>
      </>}
    </g>
  </svg>;
}

// ═══ COMMAND CENTER ════════════════════════════════════════════
// ═══ LABOR REALITY — actual vs estimated labor cost ══════════
// Pulls real data from nuvizz_weekly + payroll_weekly + driver_classifications
// and compares to the cost-structure estimate. This is the payoff for the
// v2.3+ data plumbing — actual labor cost based on who's 1099 vs W2.
function LaborReality({ margins }) {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState(null);

  useEffect(() => {
    (async () => {
      if (!hasFirebase) { setLoading(false); return; }
      try {
        const [nvW, payW, clsList] = await Promise.all([
          FS.getNuVizzWeekly(), FS.getPayrollWeekly(), FS.getDriverClassifications(),
        ]);
        // Build classification map
        const cls = {};
        for (const c of clsList) cls[c.id] = c.classification || "unknown";

        // Walk each NuVizz week, compute actual 1099 cost
        // (= sum of 40% SealNbr ONLY for drivers classified 1099)
        const weeks = {};
        for (const w of nvW) {
          if (!w.top_drivers) continue;
          let actual1099 = 0, unclassifiedPay = 0;
          for (const d of w.top_drivers) {
            const k = driverKey(d.name);
            const c = k ? cls[k] : "unknown";
            if (c === "1099") actual1099 += d.pay_at_40 || 0;
            else if (c !== "w2") unclassifiedPay += d.pay_at_40 || 0;
          }
          if (!weeks[w.week_ending]) weeks[w.week_ending] = { week_ending: w.week_ending, month: w.month };
          weeks[w.week_ending].nv_1099_actual = actual1099;
          weeks[w.week_ending].nv_unclassified = unclassifiedPay;
        }

        // Walk payroll weeks — sum W2 gross for drivers classified W2
        for (const w of payW) {
          if (!w.employees) continue;
          let w2Gross = 0, unclassifiedGross = 0;
          for (const e of w.employees) {
            const k = driverKey(e.name);
            const c = k ? cls[k] : "unknown";
            if (c === "w2") w2Gross += e.gross || 0;
            else if (c !== "1099") unclassifiedGross += e.gross || 0;
          }
          if (!weeks[w.week_ending]) weeks[w.week_ending] = { week_ending: w.week_ending, month: w.month };
          weeks[w.week_ending].pay_w2_actual = w2Gross;
          weeks[w.week_ending].pay_unclassified = unclassifiedGross;
          weeks[w.week_ending].pay_gross_total = w.gross_total || 0;
        }

        // Totals for last 4 weeks (most recent)
        const sorted = Object.values(weeks).sort((a,b) => b.week_ending.localeCompare(a.week_ending));
        const recent = sorted.slice(0, 4);
        const weeksCovered = recent.length;
        const avg1099 = weeksCovered ? recent.reduce((s,w) => s + (w.nv_1099_actual||0), 0) / weeksCovered : 0;
        const avgW2 = weeksCovered ? recent.reduce((s,w) => s + (w.pay_w2_actual||0), 0) / weeksCovered : 0;
        const avgUnclNv = weeksCovered ? recent.reduce((s,w) => s + (w.nv_unclassified||0), 0) / weeksCovered : 0;
        const avgUnclPay = weeksCovered ? recent.reduce((s,w) => s + (w.pay_unclassified||0), 0) / weeksCovered : 0;
        const actualTotalWeekly = avg1099 + avgW2;

        // Estimated weekly labor from cost structure (annual / 52)
        const estimatedWeekly = (margins?.totalAnnualLabor || 0) / 52;
        const varWeekly = actualTotalWeekly - estimatedWeekly;
        const varPct = estimatedWeekly > 0 ? (varWeekly / estimatedWeekly * 100) : null;

        setData({
          hasNv: nvW.length > 0,
          hasPay: payW.length > 0,
          classifiedCount: Object.values(cls).filter(v => v === "w2" || v === "1099").length,
          unclassifiedCount: Object.values(cls).filter(v => v === "unknown").length + (Object.keys(cls).length === 0 ? 0 : 0),
          weeksCovered, avg1099, avgW2, avgUnclNv, avgUnclPay,
          actualTotalWeekly, estimatedWeekly, varWeekly, varPct,
          recent,
        });
      } catch(e) { console.error("LaborReality load err:", e); }
      setLoading(false);
    })();
  }, [margins]);

  if (loading) return null; // silent while loading — Command Center has other content
  if (!data || (!data.hasNv && !data.hasPay)) {
    return (
      <div style={{...cardStyle, background:T.bgSurface, borderLeft:`3px solid ${T.textDim}`, marginBottom:12}}>
        <div style={{fontSize:12,fontWeight:700,marginBottom:4}}>💡 Labor Reality Check</div>
        <div style={{fontSize:11,color:T.textMuted}}>Upload NuVizz + Payroll files (or connect Gmail) to see actual labor cost vs your cost-structure estimate. Then classify drivers in the Drivers tab to unlock accurate margins.</div>
      </div>
    );
  }

  const varColor = data.varPct == null ? T.textMuted
                 : Math.abs(data.varPct) < 5 ? T.green
                 : Math.abs(data.varPct) < 15 ? T.yellow
                 : T.red;
  const varSign = data.varWeekly > 0 ? "+" : "";

  return (
    <div style={{...cardStyle, borderLeft:`3px solid ${T.purple}`, marginBottom:12}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,marginBottom:10}}>
        <div>
          <div style={{fontSize:13,fontWeight:700}}>💡 Labor Reality Check</div>
          <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>Last {data.weeksCovered} week{data.weeksCovered!==1?"s":""} avg — actual payroll + contractor pay vs cost-structure estimate</div>
        </div>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:10}}>
        <KPI label="Actual Weekly" value={fmtK(data.actualTotalWeekly)} sub={`${fmtK(data.avgW2)} W2 + ${fmtK(data.avg1099)} 1099`} subColor={T.green} />
        <KPI label="Estimated Weekly" value={fmtK(data.estimatedWeekly)} sub="from cost structure" subColor={T.blue} />
        <KPI label="Variance" value={data.varPct == null ? "—" : `${varSign}${fmtPct(data.varPct)}`} sub={`${varSign}${fmtK(data.varWeekly)}/wk`} subColor={varColor} />
      </div>

      {(data.avgUnclNv > 0 || data.avgUnclPay > 0) && (
        <div style={{marginTop:10,padding:"8px 10px",background:T.yellowBg,borderRadius:8,fontSize:11,color:T.yellowText,lineHeight:1.5}}>
          ⚠️ <strong>{fmtK(data.avgUnclNv + data.avgUnclPay)}/wk is unclassified</strong> —
          {data.avgUnclNv > 0 && ` ${fmtK(data.avgUnclNv)} NuVizz pay`}
          {data.avgUnclNv > 0 && data.avgUnclPay > 0 && " +"}
          {data.avgUnclPay > 0 && ` ${fmtK(data.avgUnclPay)} payroll`}
          {" "}from drivers/employees not yet tagged W2 or 1099. Classify them in the Drivers tab for accurate actual-cost math.
        </div>
      )}
    </div>
  );
}

function CommandCenter({ margins, weeklyRollups, completeness, qboConnected, reconMeta, connections, setTab }) {
  const m = margins;
  const marginColor = m.dailyMarginPct >= 30 ? T.green : m.dailyMarginPct >= 20 ? T.yellow : T.red;

  // Monthly rollup for chart
  const byMonth = {};
  for (const w of weeklyRollups) {
    const mo = w.month;
    if (!mo) continue;
    if (!byMonth[mo]) byMonth[mo] = { month: mo, revenue: 0, stops: 0 };
    byMonth[mo].revenue += w.revenue || 0;
    byMonth[mo].stops += w.stops || 0;
  }
  const monthly = Object.values(byMonth).sort((a,b) => a.month.localeCompare(b.month)).slice(-18);

  const hasGaps = completeness && completeness.gaps.length > 0;
  const hasSparse = completeness && completeness.sparseWeeks.length > 0;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="🎯" text="Command Center" right={<span style={{fontSize:10,color:T.textDim}}>{new Date().toLocaleDateString("en-US",{weekday:"long",month:"long",day:"numeric",year:"numeric"})}</span>} />

    {(hasGaps || hasSparse) && (
      <div style={{...cardStyle, borderLeft:`4px solid ${T.yellow}`, background:T.yellowBg, marginBottom:12}}>
        <div style={{display:"flex",alignItems:"center",gap:10,justifyContent:"space-between"}}>
          <div style={{display:"flex",alignItems:"center",gap:10,flex:1}}>
            <span style={{fontSize:20}}>⚠️</span>
            <div>
              <div style={{fontSize:13,fontWeight:700,color:T.yellowText}}>Incomplete Data Detected</div>
              <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>
                {hasGaps && <span>{completeness.gaps.length} missing week(s)</span>}
                {hasGaps && hasSparse && " · "}
                {hasSparse && <span>{completeness.sparseWeeks.length} suspiciously low-volume week(s)</span>}
                {" · Your revenue numbers may be understated."}
              </div>
            </div>
          </div>
          <button onClick={()=>setTab("completeness")} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.yellow}`,background:"transparent",color:T.yellowText,fontSize:12,fontWeight:600,cursor:"pointer"}}>Review →</button>
        </div>
      </div>
    )}

    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
      <KPI icon="💰" label="Daily Revenue" value={fmt(m.dailyRevenue)} sub={`${fmtK(m.annualRevenue)}/yr projected`} subColor={T.green} />
      <KPI icon="📉" label="Daily Cost" value={fmt(m.dailyCost)} sub={`${fmtK(m.totalAnnualCost)}/yr`} subColor={T.red} />
      <KPI icon="📊" label="Daily Margin" value={fmt(m.dailyMargin)} sub={fmtPct(m.dailyMarginPct)+" margin"} subColor={marginColor} />
      <KPI icon="🚚" label="Daily Stops" value={fmtNum(m.dailyStops)} sub={`${fmtNum(m.breakEvenStopsDaily)} break-even`} />
      <KPI icon="🎯" label="Rev/Stop" value={fmt(m.revenuePerStop)} sub={`${fmt(m.costPerStop)} cost`} subColor={T.blue} />
      <KPI icon="👤" label="Rev/Driver" value={fmt(m.revenuePerDriver)} sub={`${fmtNum(m.stopsPerDriver)} stops/day`} />
    </div>

    <LaborReality margins={m} />

    <div style={{...cardStyle, borderLeft:`4px solid ${marginColor}`, marginBottom:16}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}>
        <div>
          <div style={{fontSize:13,fontWeight:700,color:T.text}}>Margin Health</div>
          <div style={{fontSize:11,color:T.textMuted}}>Target: 30% gross margin on billed revenue</div>
        </div>
        <div style={{fontSize:28,fontWeight:800,color:marginColor}}>{fmtPct(m.dailyMarginPct)}</div>
      </div>
      <MiniBar pct={m.dailyMarginPct * 2} color={marginColor} height={10} />
    </div>

    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:"12px"}}>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Annual Cost Breakdown</div>
        <DonutChart data={m.costBreakdown} />
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Per-Unit Economics</div>
        <DataRow label="Revenue per stop" value={fmt(m.revenuePerStop)} valueColor={T.green} />
        <DataRow label="Cost per stop" value={fmt(m.costPerStop)} valueColor={T.red} />
        <DataRow label="Margin per stop" value={fmt(m.marginPerStop)} valueColor={m.marginPerStop>0?T.green:T.red} bold />
        <div style={{height:12}} />
        <DataRow label="Revenue per driver/day" value={fmt(m.revenuePerDriver)} valueColor={T.green} />
        <DataRow label="Cost per driver/day" value={fmt(m.costPerDriver)} valueColor={T.red} />
        <DataRow label="Margin per driver/day" value={fmt(m.marginPerDriver)} valueColor={m.marginPerDriver>0?T.green:T.red} bold />
        <div style={{height:12}} />
        <DataRow label="Trucks in fleet" value={m.totalTrucks} />
        <DataRow label="Drivers" value={m.totalDrivers} />
      </div>
    </div>

    {monthly.length > 0 && (
      <div style={{...cardStyle, marginTop:12}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Monthly Revenue Trend (Billed)</div>
        <LineTrend data={monthly} xKey="month" yKey="revenue" label="Billed Revenue" color={T.brand} height={220} />
      </div>
    )}

    <div style={{...cardStyle, marginTop:12}}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Data Sources</div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:8}}>
        {[
          { name:"Uline Weekly", on:weeklyRollups.length>0, sub:weeklyRollups.length>0?`${weeklyRollups.length} weeks loaded`:"Upload weekly files" },
          { name:"DDIS Reconciliation", on:reconMeta && reconMeta.files_count>0, sub:reconMeta?`${reconMeta.files_count} reconciliations`:"Upload DDIS files" },
          { name:"NuVizz", on:connections.nuvizz, sub:connections.nuvizz?"Connected":"Check API" },
          { name:"QuickBooks", on:qboConnected, sub:qboConnected?"Connected":"Connect in Settings" },
          { name:"Motive", on:connections.motive, sub:connections.motive?"Connected":"Check API" },
        ].map(s => <div key={s.name} style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",borderRadius:8,background:T.bgSurface}}>
          <div style={{width:8,height:8,borderRadius:"50%",background:s.on?T.green:T.textDim,boxShadow:s.on?`0 0 6px ${T.green}`:"none"}} />
          <div>
            <div style={{fontSize:12,fontWeight:600}}>{s.name}</div>
            <div style={{fontSize:10,color:s.on?T.green:T.textDim}}>{s.sub}</div>
          </div>
        </div>)}
      </div>
    </div>
  </div>;
}

// ═══ ULINE REVENUE (Billed data) ═══════════════════════════
function UlineRevenue({ weeklyRollups }) {
  const [periodFilter, setPeriodFilter] = useState("ytd");
  const [view, setView] = useState("weekly");

  const { filtered, startDate, endDate } = useMemo(() => {
    const now = new Date();
    let start = null;
    if (periodFilter === "ytd") start = `${now.getFullYear()}-01-01`;
    else if (periodFilter === "last12") { const d = new Date(now); d.setMonth(d.getMonth()-12); start = d.toISOString().slice(0,10); }
    else if (periodFilter === "last6") { const d = new Date(now); d.setMonth(d.getMonth()-6); start = d.toISOString().slice(0,10); }
    else if (periodFilter === "last3") { const d = new Date(now); d.setMonth(d.getMonth()-3); start = d.toISOString().slice(0,10); }
    const f = weeklyRollups.filter(r => !start || r.week_ending >= start);
    return { filtered: f.sort((a,b) => a.week_ending.localeCompare(b.week_ending)), startDate: start, endDate: now.toISOString().slice(0,10) };
  }, [weeklyRollups, periodFilter]);

  const totalRevenue = filtered.reduce((s,r) => s + (r.revenue||0), 0);
  const totalStops = filtered.reduce((s,r) => s + (r.stops||0), 0);
  const totalWeight = filtered.reduce((s,r) => s + (r.weight||0), 0);
  const totalAccessorials = filtered.reduce((s,r) => s + (r.accessorial_revenue||0), 0);
  const weeksInPeriod = filtered.length;
  const avgWeeklyRevenue = weeksInPeriod > 0 ? totalRevenue / weeksInPeriod : 0;
  const avgStopsPerWeek = weeksInPeriod > 0 ? totalStops / weeksInPeriod : 0;
  const avgRevPerStop = totalStops > 0 ? totalRevenue / totalStops : 0;

  // Build monthly from weekly
  const byMonth = {};
  for (const w of filtered) {
    const m = w.month;
    if (!m) continue;
    if (!byMonth[m]) byMonth[m] = { month: m, stops:0, revenue:0, weight:0, weeks:0, accessorial_revenue:0 };
    byMonth[m].stops += w.stops || 0;
    byMonth[m].revenue += w.revenue || 0;
    byMonth[m].weight += w.weight || 0;
    byMonth[m].accessorial_revenue += w.accessorial_revenue || 0;
    byMonth[m].weeks++;
  }
  const monthly = Object.values(byMonth).sort((a,b) => a.month.localeCompare(b.month));

  // Top customers across period
  const custAgg = {};
  for (const w of filtered) {
    for (const c of (w.top_customers||[])) {
      if (!custAgg[c.name]) custAgg[c.name] = { customer: c.name, stops:0, revenue:0 };
      custAgg[c.name].stops += c.stops;
      custAgg[c.name].revenue += c.revenue;
    }
  }
  const topCustomers = Object.values(custAgg).sort((a,b) => b.revenue - a.revenue).slice(0,20);

  // Top cities
  const cityAgg = {};
  for (const w of filtered) {
    for (const c of (w.top_cities||[])) {
      if (!cityAgg[c.name]) cityAgg[c.name] = { city: c.name, stops:0, revenue:0 };
      cityAgg[c.name].stops += c.stops;
      cityAgg[c.name].revenue += c.revenue;
    }
  }
  const topCities = Object.values(cityAgg).sort((a,b) => b.revenue - a.revenue).slice(0,20);

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="💰" text="Uline Revenue" right={
      <div style={{display:"flex",gap:4,flexWrap:"wrap"}}>
        {[["last3","Last 3mo"],["last6","Last 6mo"],["last12","Last 12mo"],["ytd","YTD"],["all","All Time"]].map(([id,l])=>
          <TabButton key={id} active={periodFilter===id} label={l} onClick={()=>setPeriodFilter(id)} />
        )}
      </div>
    } />

    {weeklyRollups.length === 0 ? (
      <EmptyState icon="📤" title="No Revenue Data Yet" sub="Upload your master file, weekly originals, and accessorials in the Data Ingest tab." />
    ) : (
      <>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}>
          <KPI label="Total Revenue" value={fmtK(totalRevenue)} sub={`${weeksInPeriod} weeks`} subColor={T.green} />
          <KPI label="Total Stops" value={fmtNum(totalStops)} sub={`${fmtNum(avgStopsPerWeek)}/week avg`} />
          <KPI label="Avg Rev/Stop" value={fmt(avgRevPerStop)} />
          <KPI label="Weekly Avg" value={fmtK(avgWeeklyRevenue)} subColor={T.green} />
          <KPI label="Accessorials" value={fmtK(totalAccessorials)} sub={totalRevenue>0?fmtPct(totalAccessorials/totalRevenue*100)+" of revenue":"—"} subColor={T.purple} />
          <KPI label="Total Weight" value={fmtNum(totalWeight)+" lbs"} />
        </div>

        <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
          {[["weekly","📅 Weekly"],["monthly","📊 Monthly"],["customers","🏢 Customers"],["cities","📍 Cities"]].map(([id,l])=>
            <TabButton key={id} active={view===id} label={l} onClick={()=>setView(id)} />
          )}
        </div>

        {view === "weekly" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Weekly Revenue Trend</div>
            <LineTrend data={filtered.slice(-26)} xKey="week_ending" yKey="revenue" label="Billed" color={T.brand} height={220} />
            <div style={{marginTop:16,overflowX:"auto",maxHeight:500}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["Week Ending","Stops","Revenue","Base","Accessorials","Avg/Stop","Weight"].map(h=>
                  <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite,zIndex:1}}>{h}</th>
                )}</tr></thead>
                <tbody>
                  {filtered.slice().reverse().map((w,i) => (
                    <tr key={i}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{weekLabel(w.week_ending)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(w.stops)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(w.revenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.base_revenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.purple}}>{w.accessorial_revenue?fmt(w.accessorial_revenue):"—"}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.stops>0?w.revenue/w.stops:0)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted,fontSize:11}}>{fmtNum(w.weight)} lbs</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === "monthly" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Monthly Revenue Trend</div>
            <LineTrend data={monthly} xKey="month" yKey="revenue" label="Billed" color={T.brand} height={220} />
            <div style={{marginTop:16,overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["Month","Weeks","Stops","Revenue","Accessorials","Avg/Stop","Avg/Week"].map(h=>
                  <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>
                )}</tr></thead>
                <tbody>
                  {monthly.slice().reverse().map((m,i) => (
                    <tr key={i}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{m.month}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{m.weeks}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(m.stops)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(m.revenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.purple}}>{m.accessorial_revenue?fmt(m.accessorial_revenue):"—"}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(m.stops>0?m.revenue/m.stops:0)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(m.weeks>0?m.revenue/m.weeks:0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === "customers" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Top Customers by Revenue</div>
            <BarChart data={topCustomers} labelKey="customer" valueKey="revenue" color={T.green} maxBars={20} />
            <div style={{marginTop:16,overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["Customer","Stops","Revenue","Avg/Stop","% of Total"].map(h=>
                  <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>
                )}</tr></thead>
                <tbody>
                  {topCustomers.map((c,i) => (
                    <tr key={i}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(c.stops)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(c.revenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(c.stops>0?c.revenue/c.stops:0)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(totalRevenue>0?c.revenue/totalRevenue*100:0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === "cities" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Top Cities by Revenue</div>
            <BarChart data={topCities} labelKey="city" valueKey="revenue" color={T.blue} maxBars={20} />
            <div style={{marginTop:16,overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["City","Stops","Revenue","Avg/Stop"].map(h=>
                  <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>
                )}</tr></thead>
                <tbody>
                  {topCities.map((c,i) => (
                    <tr key={i}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.city}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(c.stops)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.blue,fontWeight:700}}>{fmt(c.revenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(c.stops>0?c.revenue/c.stops:0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </>
    )}
  </div>;
}

// ═══ DATA COMPLETENESS TAB ════════════════════════════════════
function DataCompleteness({ weeklyRollups, completeness, fileLog }) {
  if (!completeness || weeklyRollups.length === 0) {
    return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="✅" text="Data Completeness" />
      <EmptyState icon="📤" title="No Data Loaded" sub="Upload files in the Data Ingest tab first." />
    </div>;
  }

  const { expected, gaps, sparseWeeks, missingAccessorials, avgStops, firstWE, lastWE } = completeness;
  const completePct = expected.length > 0 ? ((expected.length - gaps.length) / expected.length * 100) : 100;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="✅" text="Data Completeness" />

    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
      <KPI label="Coverage" value={fmtPct(completePct,0)} subColor={completePct>=95?T.green:completePct>=85?T.yellow:T.red} sub={`${expected.length-gaps.length}/${expected.length} weeks`} />
      <KPI label="Missing Weeks" value={fmtNum(gaps.length)} subColor={gaps.length>0?T.red:T.green} sub="Zero data" />
      <KPI label="Sparse Weeks" value={fmtNum(sparseWeeks.length)} subColor={sparseWeeks.length>0?T.yellow:T.green} sub="Possible partial" />
      <KPI label="No Accessorials" value={fmtNum(missingAccessorials.length)} subColor={missingAccessorials.length>0?T.yellow:T.green} sub="Weeks missing acc file" />
      <KPI label="Avg Stops/Week" value={fmtNum(avgStops)} sub={`${weekLabel(firstWE)} → ${weekLabel(lastWE)}`} />
    </div>

    {gaps.length > 0 && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12,color:T.red}}>🚨 Missing Weeks — No Data At All ({gaps.length})</div>
        <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These weeks have zero stops in the system. You worked these weeks — find the Uline files and re-upload.</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(140px,1fr))",gap:6}}>
          {gaps.map(w => (
            <div key={w} style={{padding:"8px 12px",borderRadius:6,background:T.redBg,color:T.redText,fontSize:12,fontWeight:600,textAlign:"center",border:`1px solid ${T.red}30`}}>
              WE {weekLabel(w)}
            </div>
          ))}
        </div>
      </div>
    )}

    {sparseWeeks.length > 0 && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12,color:T.yellowText}}>⚠️ Suspiciously Low-Volume Weeks ({sparseWeeks.length})</div>
        <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These weeks have far fewer stops than your average of <strong>{fmtNum(avgStops)}</strong>. Data may be partial or a holiday week.</div>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead><tr>{["Week Ending","Stops","Revenue","Expected Stops","Gap"].map(h=>
              <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>
            )}</tr></thead>
            <tbody>
              {sparseWeeks.map((w,i) => (
                <tr key={i}>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>WE {weekLabel(w.week_ending)}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmtNum(w.stops)}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.revenue)}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted}}>{fmtNum(w.expected_avg)}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmtNum(w.expected_avg - w.stops)} missing</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )}

    {missingAccessorials.length > 0 && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12,color:T.yellowText}}>📄 Weeks Missing Accessorials File ({missingAccessorials.length})</div>
        <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These weeks have original data but no accessorial charges were found. Either you had zero accessorials that week (rare) or the accessorial file wasn't uploaded.</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(140px,1fr))",gap:6}}>
          {missingAccessorials.map((w,i) => (
            <div key={i} style={{padding:"8px 12px",borderRadius:6,background:T.yellowBg,color:T.yellowText,fontSize:12,fontWeight:600,textAlign:"center",border:`1px solid ${T.yellow}30`}}>
              WE {weekLabel(w.week_ending)}
            </div>
          ))}
        </div>
      </div>
    )}

    {gaps.length === 0 && sparseWeeks.length === 0 && missingAccessorials.length === 0 && (
      <div style={{...cardStyle, background:T.greenBg, borderColor:T.green, textAlign:"center", padding:30}}>
        <div style={{fontSize:40,marginBottom:8}}>✅</div>
        <div style={{fontSize:16,fontWeight:700,color:T.greenText,marginBottom:4}}>All Weeks Accounted For</div>
        <div style={{fontSize:12,color:T.textMuted}}>Every week from {weekLabel(firstWE)} to {weekLabel(lastWE)} has data loaded.</div>
      </div>
    )}

    {fileLog.length > 0 && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Upload Log (last {Math.min(fileLog.length,50)})</div>
        <div style={{overflowX:"auto",maxHeight:400}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><tr>{["Filename","Type","Rows","Uploaded"].map(h=>
              <th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>
            )}</tr></thead>
            <tbody>
              {fileLog.slice(0,50).map((f,i) => (
                <tr key={i}>
                  <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontSize:10}}>{f.filename}</td>
                  <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={f.kind} color={T.brand} bg={T.brandPale} /></td>
                  <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(f.row_count)}</td>
                  <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted,fontSize:10}}>{f.uploaded_at?new Date(f.uploaded_at).toLocaleString():"—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )}
  </div>;
}

// ═══ DATA INGEST ═══════════════════════════════════════════
function DataIngest({ weeklyRollups, reconMeta, onRefresh }) {
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState("");
  const [progress, setProgress] = useState({ current:0, total:0 });
  const [lastResult, setLastResult] = useState(null);
  const [sourceStats, setSourceStats] = useState(null);
  const fileRef = useRef(null);

  // Load source counts for all 5 sources on mount so cards show meaningful status
  useEffect(() => {
    (async () => {
      if (!hasFirebase) return;
      try {
        const [nvW, tcW, payW, qboH] = await Promise.all([
          FS.getNuVizzWeekly(), FS.getTimeClockWeekly(),
          FS.getPayrollWeekly(), FS.getQBOHistory(),
        ]);
        setSourceStats({
          nuvizz_weeks: nvW.length,
          nuvizz_stops: nvW.reduce((s,w)=>s+(w.stops_total||0),0),
          nuvizz_pay_if_1099: nvW.reduce((s,w)=>s+(w.contractor_pay_if_all_1099||0),0),
          timeclock_weeks: tcW.length,
          timeclock_hours: tcW.reduce((s,w)=>s+(w.total_hours||0),0),
          payroll_weeks: payW.length,
          payroll_gross: payW.reduce((s,w)=>s+(w.gross_total||0),0),
          qbo_periods: qboH.length,
        });
      } catch(e) {}
    })();
  }, []);

  const handleFiles = async (files) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setStatus(`Processing ${files.length} file${files.length>1?"s":""}...`);
    setProgress({ current:0, total:files.length });

    // Per-group accumulators
    const stopsByPro = {};         // Uline: pro -> merged stop
    const paymentByPro = {};       // DDIS
    const ddisFileRecords = [];
    const nuvizzStops = [];
    const timeClockEntries = [];
    const payrollEntries = [];
    const qboEntries = [];
    const fileLogs = [];
    const sourceFiles = [];
    const countsByKind = { master:0, original:0, accessorials:0, ddis:0, nuvizz:0, timeclock:0, payroll:0, qbo_pl:0, qbo_tb:0, qbo_gl:0, unknown:0 };
    const unknownFiles = [];

    for (let i=0; i<files.length; i++) {
      const file = files[i];
      setProgress({ current: i+1, total: files.length });
      setStatus(`[${i+1}/${files.length}] ${file.name}...`);
      const fileId = file.name.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);

      try {
        let rows;
        const isCSV = file.name.toLowerCase().endsWith(".csv");
        if (isCSV) rows = await readCSV(file);
        else rows = await readWorkbook(file);
        if (!rows || rows.length === 0) { unknownFiles.push(file.name + " (empty)"); countsByKind.unknown++; continue; }

        const kind = detectFileType(file.name, rows[0]);
        const group = sourceGroup(kind);
        countsByKind[kind] = (countsByKind[kind]||0) + 1;

        fileLogs.push({ file_id: fileId, filename: file.name, kind, group, row_count: rows.length, uploaded_at: new Date().toISOString() });
        sourceFiles.push({ file_id: fileId, filename: file.name, kind, group, row_count: rows.length, uploaded_at: new Date().toISOString() });

        // ─── Uline family ───
        if (kind === "master" || kind === "original" || kind === "accessorials") {
          const stops = parseOriginalOrAccessorial(rows);
          for (const s of stops) {
            if (!s.pro || !s.week_ending) continue;
            const existing = stopsByPro[s.pro];
            if (!existing || (s.new_cost > existing.new_cost)) stopsByPro[s.pro] = s;
          }
        } else if (kind === "ddis") {
          const payments = parseDDIS(rows);
          const billDates = payments.map(p => p.bill_date).filter(Boolean).sort();
          const totalPaid = payments.reduce((s,p) => s + p.paid, 0);
          ddisFileRecords.push({
            file_id: fileId, filename: file.name,
            record_count: payments.length, total_paid: totalPaid,
            earliest_bill_date: billDates[0] || null,
            latest_bill_date: billDates[billDates.length-1] || null,
            checks: [...new Set(payments.map(p => p.check).filter(Boolean))],
            uploaded_at: new Date().toISOString(),
          });
          for (const p of payments) paymentByPro[p.pro] = (paymentByPro[p.pro] || 0) + p.paid;
        }
        // ─── NuVizz ───
        else if (kind === "nuvizz") {
          nuvizzStops.push(...parseNuVizz(rows));
        }
        // ─── Time Clock ───
        else if (kind === "timeclock") {
          timeClockEntries.push(...parseTimeClock(rows));
        }
        // ─── Payroll ───
        else if (kind === "payroll") {
          payrollEntries.push(...parsePayroll(rows));
        }
        // ─── QBO ───
        else if (kind === "qbo_pl" || kind === "qbo_tb" || kind === "qbo_gl") {
          qboEntries.push(...parseQBO(rows, kind).map(e => ({...e, source_file: file.name})));
        }
        else {
          unknownFiles.push(file.name);
        }
      } catch(e) {
        console.error("File error:", file.name, e);
        unknownFiles.push(file.name + " (err: " + e.message + ")");
        countsByKind.unknown++;
      }
    }

    // ─── Uline: Build weekly rollups + recon ───
    setStatus("Building Uline weekly rollups...");
    const allStops = Object.values(stopsByPro);
    const rollups = buildWeeklyRollups(allStops);

    const reconByWeek = {};
    const unpaidStops = [];
    for (const s of allStops) {
      const paid = paymentByPro[s.pro] || 0;
      if (!reconByWeek[s.week_ending]) reconByWeek[s.week_ending] = { week_ending: s.week_ending, month: s.month, billed:0, paid_matched:0, unpaid_count:0, unpaid_amount:0 };
      reconByWeek[s.week_ending].billed += s.new_cost || 0;
      reconByWeek[s.week_ending].paid_matched += paid;
      if (paid === 0 && (s.new_cost||0) > 0) {
        reconByWeek[s.week_ending].unpaid_count++;
        reconByWeek[s.week_ending].unpaid_amount += s.new_cost;
        if (unpaidStops.length < 2000) {
          unpaidStops.push({
            pro: s.pro, customer: s.customer, city: s.city, state: s.state, zip: s.zip,
            pu_date: s.pu_date, week_ending: s.week_ending, month: s.month, billed: s.new_cost,
            code: s.code, weight: s.weight, order: s.order
          });
        }
      }
    }
    for (const r of Object.values(reconByWeek)) {
      r.collection_rate = r.billed > 0 ? (r.paid_matched / r.billed * 100) : null;
    }

    // ─── Save Uline ───
    setStatus("Saving Uline weekly rollups...");
    let savedWeeks = 0;
    for (const r of rollups) {
      const ok = await FS.saveWeeklyRollup(r.week_ending, {...r, updated_at:new Date().toISOString()});
      if (ok) savedWeeks++;
    }
    let savedRecon = 0;
    for (const r of Object.values(reconByWeek)) {
      const ok = await FS.saveReconWeekly(r.week_ending, {...r, updated_at:new Date().toISOString()});
      if (ok) savedRecon++;
    }
    for (const f of ddisFileRecords) await FS.saveDDISFile(f.file_id, f);
    const topUnpaid = unpaidStops.sort((a,b) => b.billed - a.billed).slice(0, 500);
    for (const s of topUnpaid) await FS.saveUnpaidStop(s.pro, s);

    // ─── NuVizz: rollups + cross-ref stops ───
    let nvWeeksSaved = 0, nvStopsSaved = 0;
    if (nuvizzStops.length > 0) {
      setStatus(`Building NuVizz weekly rollups (${nuvizzStops.length} stops)...`);
      const nvWeekly = buildNuVizzWeekly(nuvizzStops);
      for (const w of nvWeekly) {
        const ok = await FS.saveNuVizzWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()});
        if (ok) nvWeeksSaved++;
      }
      // Save up to 2000 most recent stops for cross-reference (avoid bloating Firebase)
      setStatus("Saving NuVizz stop cross-references...");
      const recent = nuvizzStops
        .filter(s => s.pro && s.delivery_date)
        .sort((a,b) => (b.delivery_date||"").localeCompare(a.delivery_date||""))
        .slice(0, 2000);
      for (const s of recent) {
        const key = s.pro || s.stop_number;
        if (!key) continue;
        const ok = await FS.saveNuVizzStop(key, s);
        if (ok) nvStopsSaved++;
      }
    }

    // ─── Time Clock: weekly rollups ───
    let tcWeeksSaved = 0;
    if (timeClockEntries.length > 0) {
      setStatus(`Building time clock weekly rollups (${timeClockEntries.length} entries)...`);
      const tcWeekly = buildTimeClockWeekly(timeClockEntries);
      for (const w of tcWeekly) {
        const ok = await FS.saveTimeClockWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()});
        if (ok) tcWeeksSaved++;
      }
    }

    // ─── Payroll: weekly rollups ───
    let payWeeksSaved = 0;
    if (payrollEntries.length > 0) {
      setStatus(`Building payroll weekly rollups (${payrollEntries.length} entries)...`);
      const payWeekly = buildPayrollWeekly(payrollEntries);
      for (const w of payWeekly) {
        const ok = await FS.savePayrollWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()});
        if (ok) payWeeksSaved++;
      }
    }

    // ─── QBO: store per-period aggregates ───
    let qboPeriodsSaved = 0;
    if (qboEntries.length > 0) {
      setStatus(`Saving QBO entries (${qboEntries.length} line items)...`);
      // Group by period+report_type+source_file
      const byPeriod = {};
      for (const e of qboEntries) {
        const pid = `${e.report_type}_${e.source_file || "unknown"}`.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);
        if (!byPeriod[pid]) byPeriod[pid] = { period: e.period || null, report_type: e.report_type, source_file: e.source_file, accounts: [] };
        byPeriod[pid].accounts.push({ account: e.account, amount: e.amount, debit: e.debit, credit: e.credit });
      }
      for (const [pid, data] of Object.entries(byPeriod)) {
        const ok = await FS.saveQBOHistory(pid, {...data, uploaded_at: new Date().toISOString()});
        if (ok) qboPeriodsSaved++;
      }
    }

    // ─── Log all files ───
    setStatus("Logging files...");
    for (const l of fileLogs) await FS.saveFileLog(l.file_id, l);
    for (const sf of sourceFiles) await FS.saveSourceFile(sf.file_id, sf);

    const existingMeta = await FS.getReconMeta() || {};
    await FS.saveReconMeta({
      files_count: (existingMeta.files_count || 0) + ddisFileRecords.length,
      last_upload: new Date().toISOString(),
      total_stops_processed: (existingMeta.total_stops_processed || 0) + allStops.length,
    });

    setLastResult({
      files_processed: files.length,
      counts: countsByKind,
      uline: { stops: allStops.length, weeks_saved: savedWeeks, recon_saved: savedRecon, unpaid_saved: topUnpaid.length, payments: Object.keys(paymentByPro).length },
      nuvizz: { stops: nuvizzStops.length, weeks_saved: nvWeeksSaved, stops_saved: nvStopsSaved },
      timeclock: { entries: timeClockEntries.length, weeks_saved: tcWeeksSaved },
      payroll: { entries: payrollEntries.length, weeks_saved: payWeeksSaved },
      qbo: { lines: qboEntries.length, periods_saved: qboPeriodsSaved },
      unknown: unknownFiles,
    });
    setStatus(`✓ Processed ${files.length} files across ${Object.values(countsByKind).filter(c=>c>0).length} source types`);
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
    onRefresh();

    // Refresh source stats
    if (hasFirebase) {
      try {
        const [nvW, tcW, payW, qboH] = await Promise.all([
          FS.getNuVizzWeekly(), FS.getTimeClockWeekly(),
          FS.getPayrollWeekly(), FS.getQBOHistory(),
        ]);
        setSourceStats({
          nuvizz_weeks: nvW.length,
          nuvizz_stops: nvW.reduce((s,w)=>s+(w.stops_total||0),0),
          nuvizz_pay_if_1099: nvW.reduce((s,w)=>s+(w.contractor_pay_if_all_1099||0),0),
          timeclock_weeks: tcW.length,
          timeclock_hours: tcW.reduce((s,w)=>s+(w.total_hours||0),0),
          payroll_weeks: payW.length,
          payroll_gross: payW.reduce((s,w)=>s+(w.gross_total||0),0),
          qbo_periods: qboH.length,
        });
      } catch(e) {}
    }
  };

  // Render a source card
  const SourceCard = ({ icon, title, sub, primary, secondary, color }) => (
    <div style={{...cardStyle, borderLeft:`3px solid ${color}`, padding:"12px 14px"}}>
      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
        <span style={{fontSize:18}}>{icon}</span>
        <span style={{fontSize:12,fontWeight:700,color:T.text}}>{title}</span>
      </div>
      <div style={{fontSize:16,fontWeight:700,color}}>{primary}</div>
      <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>{secondary}</div>
      <div style={{fontSize:10,color:T.textDim,marginTop:4,fontStyle:"italic"}}>{sub}</div>
    </div>
  );

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="📤" text="Data Ingest" right={
      <div>
        <input ref={fileRef} type="file" accept=".xlsx,.xls,.csv" multiple onChange={e=>handleFiles(e.target.files)} style={{display:"none"}} />
        <PrimaryBtn text={uploading?"Processing...":"Upload Files"} onClick={()=>fileRef.current?.click()} loading={uploading} />
      </div>
    } />

    <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:8,color:T.brand}}>📤 Bulk Upload — Any Data Source</div>
      <div style={{fontSize:12,color:T.text,lineHeight:1.6}}>
        Select any combination of files. MarginIQ auto-detects each type and routes to the right parser:
        <ul style={{marginTop:8,marginLeft:20,fontSize:12}}>
          <li><strong>Uline</strong> (master / originals / accessorials / DDIS) → weekly revenue (source of truth) + reconciliation</li>
          <li><strong>NuVizz</strong> (driver stops export) → weekly driver rollups + 1099 contractor pay base (40% per stop). <em>Not revenue.</em></li>
          <li><strong>Time Clock</strong> (SENTINEL / B600 punches) → weekly hours by employee</li>
          <li><strong>Payroll</strong> (CyberPay register) → weekly gross, hours, OT by employee</li>
          <li><strong>QuickBooks</strong> (P&L, Trial Balance, GL exports) → financial history</li>
        </ul>
        <div style={{fontSize:11,color:T.textMuted,marginTop:8}}>
          Upload all at once — no need to separate. Files auto-detected by filename and column headers.
        </div>
      </div>
    </div>

    {/* 5 source status cards */}
    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:"10px",marginTop:12}}>
      <SourceCard icon="📦" title="Uline Revenue" color={T.brand}
        primary={`${weeklyRollups.length} weeks`}
        secondary={weeklyRollups.length > 0 ? fmtK(weeklyRollups.reduce((s,r)=>s+(r.revenue||0),0)) + " billed" : "—"}
        sub="Master / weekly / accessorials" />
      <SourceCard icon="🚚" title="NuVizz" color={T.blue}
        primary={sourceStats ? `${sourceStats.nuvizz_weeks} weeks` : "—"}
        secondary={sourceStats ? `${fmtNum(sourceStats.nuvizz_stops)} stops` : "—"}
        sub="Driver attribution + 1099 pay base" />
      <SourceCard icon="⏰" title="Time Clock" color={T.purple}
        primary={sourceStats ? `${sourceStats.timeclock_weeks} weeks` : "—"}
        secondary={sourceStats ? `${fmtNum(Math.round(sourceStats.timeclock_hours))} hrs` : "—"}
        sub="SENTINEL / B600 punches" />
      <SourceCard icon="💵" title="Payroll" color={T.green}
        primary={sourceStats ? `${sourceStats.payroll_weeks} weeks` : "—"}
        secondary={sourceStats ? fmtK(sourceStats.payroll_gross) + " gross" : "—"}
        sub="CyberPay register" />
      <SourceCard icon="📊" title="QuickBooks" color={T.yellow}
        primary={sourceStats ? `${sourceStats.qbo_periods} periods` : "—"}
        secondary="P&L / TB / GL"
        sub="Financial reports" />
    </div>

    {uploading && (
      <div style={{...cardStyle, background:T.yellowBg, borderColor:T.yellow}}>
        <div style={{fontSize:12,fontWeight:600,color:T.yellowText,marginBottom:8}}>⏳ {status}</div>
        <MiniBar pct={progress.total>0?(progress.current/progress.total*100):0} color={T.yellow} height={8} />
        <div style={{fontSize:10,color:T.textMuted,marginTop:4,textAlign:"right"}}>{progress.current} / {progress.total}</div>
      </div>
    )}

    {lastResult && !uploading && (
      <div style={{...cardStyle, background:T.greenBg, borderColor:T.green}}>
        <div style={{fontSize:13,fontWeight:700,color:T.greenText,marginBottom:8}}>✓ Import Complete</div>
        <div style={{fontSize:12,color:T.text,lineHeight:1.8}}>
          <div><strong>Total files:</strong> {lastResult.files_processed}</div>
          {lastResult.uline.stops > 0 && (
            <div style={{marginTop:6}}>
              <strong>📦 Uline:</strong> {fmtNum(lastResult.uline.stops)} stops → {lastResult.uline.weeks_saved} weeks, {lastResult.uline.recon_saved} recon, {fmtNum(lastResult.uline.unpaid_saved)} unpaid
              {" "}({lastResult.counts.master||0} master, {lastResult.counts.original||0} orig, {lastResult.counts.accessorials||0} acc, {lastResult.counts.ddis||0} ddis)
            </div>
          )}
          {lastResult.nuvizz.stops > 0 && (
            <div><strong>🚚 NuVizz:</strong> {fmtNum(lastResult.nuvizz.stops)} stops → {lastResult.nuvizz.weeks_saved} weeks, {fmtNum(lastResult.nuvizz.stops_saved)} cross-refs saved ({lastResult.counts.nuvizz} files)</div>
          )}
          {lastResult.timeclock.entries > 0 && (
            <div><strong>⏰ Time Clock:</strong> {fmtNum(lastResult.timeclock.entries)} entries → {lastResult.timeclock.weeks_saved} weeks ({lastResult.counts.timeclock} files)</div>
          )}
          {lastResult.payroll.entries > 0 && (
            <div><strong>💵 Payroll:</strong> {fmtNum(lastResult.payroll.entries)} entries → {lastResult.payroll.weeks_saved} weeks ({lastResult.counts.payroll} files)</div>
          )}
          {lastResult.qbo.lines > 0 && (
            <div><strong>📊 QBO:</strong> {fmtNum(lastResult.qbo.lines)} line items → {lastResult.qbo.periods_saved} periods ({(lastResult.counts.qbo_pl||0)+(lastResult.counts.qbo_tb||0)+(lastResult.counts.qbo_gl||0)} files)</div>
          )}
          {lastResult.unknown.length > 0 && <div style={{color:T.yellowText,marginTop:6}}>⚠️ Unrecognized: {lastResult.unknown.join(", ")}</div>}
        </div>
      </div>
    )}
  </div>;
}

// ═══ AUDIT — Revenue Recovery (AuditIQ) ════════════════════
// Consolidated billed-vs-paid tracking + discrepancy workflow.
// Replaces the old simple Reconciliation tab with:
//   - Hero dashboard (Outstanding, Recovered, Win Rate, Avg Days)
//   - Aging buckets
//   - Top customers by discrepancy
//   - Full audit item list with category/age/dispute-status filters
//   - Detail view with one-click Dispute PDF generation
//   - Customer AP contacts editor
//   - Dispute tracker
function Audit({ reconWeekly, weeklyRollups }) {
  const [loading, setLoading] = useState(true);
  const [auditItems, setAuditItems] = useState([]);
  const [apContacts, setApContacts] = useState({}); // key -> contact doc
  const [disputes, setDisputes] = useState([]);
  const [view, setView] = useState("dashboard"); // dashboard | items | contacts | disputes | weekly
  const [detailItem, setDetailItem] = useState(null);
  const [editingContact, setEditingContact] = useState(null);

  // Filters for items view
  const [filterCategory, setFilterCategory] = useState("all");
  const [filterAge, setFilterAge] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const [filterCustomer, setFilterCustomer] = useState("");
  const [filterMinVar, setFilterMinVar] = useState(10);
  const [sortBy, setSortBy] = useState("variance");
  const [selectedIds, setSelectedIds] = useState({}); // pro -> true

  // PDF generation state
  const [generatingPdf, setGeneratingPdf] = useState(false);
  const [pdfStatus, setPdfStatus] = useState("");

  const loadData = useCallback(async () => {
    setLoading(true);
    const [items, contacts, disp] = await Promise.all([
      FS.getAuditItems(2000), FS.getAPContacts(), FS.getDisputes(),
    ]);
    setAuditItems(items);
    const cMap = {};
    for (const c of contacts) cMap[c.id] = c;
    setApContacts(cMap);
    setDisputes(disp);
    setLoading(false);
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  // Recompute age_days on load (they were stored at ingest time and may be stale)
  const itemsWithFreshAge = useMemo(() => {
    const today = new Date().toISOString().slice(0,10);
    return auditItems.map(i => {
      const age = daysBetween(i.pu_date, today);
      return { ...i, age_days: age, age_bucket: ageBucket(age) };
    });
  }, [auditItems]);

  // Dashboard stats
  const stats = useMemo(() => {
    const active = itemsWithFreshAge.filter(i => i.dispute_status !== "written_off" && i.dispute_status !== "won");
    const outstanding = active.reduce((s,i) => s + (i.variance||0), 0);
    const outstandingCount = active.length;
    const thisMonth = new Date().toISOString().slice(0,7);
    const wonThisMonth = disputes.filter(d => d.outcome === "won" && (d.response_date||"").startsWith(thisMonth));
    const recovered = wonThisMonth.reduce((s,d) => s + (d.amount_recovered||0), 0);
    const allDisputed = disputes.filter(d => d.submitted_date);
    const won = allDisputed.filter(d => d.outcome === "won" || d.outcome === "partial");
    const winRate = allDisputed.length > 0 ? (won.length / allDisputed.length * 100) : null;
    // Avg days to recover
    const turnarounds = allDisputed
      .filter(d => d.submitted_date && d.response_date && (d.outcome === "won" || d.outcome === "partial"))
      .map(d => daysBetween(d.submitted_date.slice(0,10), d.response_date.slice(0,10)) || 0);
    const avgTurnaround = turnarounds.length > 0 ? Math.round(turnarounds.reduce((s,t)=>s+t,0)/turnarounds.length) : null;
    return { outstanding, outstandingCount, recovered, wonThisMonthCount: wonThisMonth.length, winRate, avgTurnaround };
  }, [itemsWithFreshAge, disputes]);

  // Aging bucket breakdown (active only)
  const agingBuckets = useMemo(() => {
    const buckets = {};
    for (const b of AGE_BUCKETS) buckets[b] = { bucket: b, count: 0, amount: 0 };
    for (const i of itemsWithFreshAge) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won") continue;
      const b = i.age_bucket || "unknown";
      if (!buckets[b]) buckets[b] = { bucket: b, count: 0, amount: 0 };
      buckets[b].count++;
      buckets[b].amount += i.variance || 0;
    }
    return AGE_BUCKETS.map(b => buckets[b]).filter(b => b.count > 0);
  }, [itemsWithFreshAge]);

  // Top customers by outstanding
  const topCustomers = useMemo(() => {
    const byC = {};
    for (const i of itemsWithFreshAge) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won") continue;
      const c = i.customer || "Unknown";
      if (!byC[c]) byC[c] = { customer: c, customer_key: i.customer_key, count: 0, amount: 0, oldest_age: 0 };
      byC[c].count++;
      byC[c].amount += i.variance || 0;
      if ((i.age_days||0) > byC[c].oldest_age) byC[c].oldest_age = i.age_days || 0;
    }
    return Object.values(byC).sort((a,b) => b.amount - a.amount).slice(0, 15);
  }, [itemsWithFreshAge]);

  // Category breakdown
  const byCategory = useMemo(() => {
    const out = {};
    for (const c of CATEGORIES) out[c] = { category: c, count: 0, amount: 0 };
    for (const i of itemsWithFreshAge) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won") continue;
      const c = i.category || "short_paid";
      if (!out[c]) out[c] = { category: c, count: 0, amount: 0 };
      out[c].count++;
      out[c].amount += i.variance || 0;
    }
    return CATEGORIES.map(c => out[c]).filter(x => x.count > 0);
  }, [itemsWithFreshAge]);

  // Filtered item list
  const filteredItems = useMemo(() => {
    let arr = itemsWithFreshAge;
    if (filterCategory !== "all") arr = arr.filter(i => i.category === filterCategory);
    if (filterAge !== "all") arr = arr.filter(i => i.age_bucket === filterAge);
    if (filterStatus !== "all") arr = arr.filter(i => (i.dispute_status || "new") === filterStatus);
    if (filterCustomer.trim()) {
      const q = filterCustomer.toLowerCase();
      arr = arr.filter(i => (i.customer||"").toLowerCase().includes(q) || (i.pro||"").toLowerCase().includes(q));
    }
    arr = arr.filter(i => (i.variance||0) >= filterMinVar);
    arr = arr.slice().sort((a,b) => {
      if (sortBy === "variance") return (b.variance||0) - (a.variance||0);
      if (sortBy === "age") return (b.age_days||0) - (a.age_days||0);
      if (sortBy === "date") return (b.pu_date||"").localeCompare(a.pu_date||"");
      if (sortBy === "customer") return (a.customer||"").localeCompare(b.customer||"");
      return 0;
    });
    return arr;
  }, [itemsWithFreshAge, filterCategory, filterAge, filterStatus, filterCustomer, filterMinVar, sortBy]);

  // Selected items for bulk actions
  const selectedItems = useMemo(() =>
    filteredItems.filter(i => selectedIds[i.pro]),
  [filteredItems, selectedIds]);

  const toggleSelect = (pro) => setSelectedIds(prev => ({...prev, [pro]: !prev[pro]}));
  const selectAll = () => {
    const map = {};
    for (const i of filteredItems) map[i.pro] = true;
    setSelectedIds(map);
  };
  const clearSelect = () => setSelectedIds({});

  const updateItemStatus = async (pro, newStatus, extras = {}) => {
    const item = auditItems.find(i => i.pro === pro);
    if (!item) return;
    const updated = { ...item, dispute_status: newStatus, ...extras, updated_at: new Date().toISOString() };
    await FS.saveAuditItem(pro, updated);
    setAuditItems(prev => prev.map(i => i.pro === pro ? updated : i));
  };

  // Generate dispute PDF for selected or single item
  const generateDisputePdf = async (itemsArr, customerName) => {
    if (itemsArr.length === 0) return;
    setGeneratingPdf(true);
    setPdfStatus("Generating PDF...");
    try {
      const firstKey = itemsArr[0].customer_key || customerKey(itemsArr[0].customer);
      const contact = apContacts[firstKey] || {};
      const resp = await fetch("/.netlify/functions/marginiq-dispute-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: itemsArr,
          customer: customerName || itemsArr[0].customer,
          ap_contact: contact,
        }),
      });
      const data = await resp.json();
      if (data.error) throw new Error(data.error);

      // Download the PDF
      const link = document.createElement("a");
      link.href = "data:application/pdf;base64," + data.data;
      link.download = data.filename;
      link.click();

      setPdfStatus(`✓ Generated ${data.filename} (${itemsArr.length} items, $${data.total_claim.toFixed(2)} claim)`);

      // Create a dispute record and update item statuses
      const disputeId = `D_${Date.now()}`;
      await FS.saveDispute(disputeId, {
        dispute_id: disputeId,
        customer: customerName || itemsArr[0].customer,
        customer_key: firstKey,
        item_count: itemsArr.length,
        item_pros: itemsArr.map(i => i.pro),
        amount_claimed: data.total_claim,
        amount_recovered: 0,
        submitted_date: null, // set when user confirms sent
        submitted_to: contact.billing_email || null,
        outcome: null,
        response_date: null,
        package_generated_at: new Date().toISOString(),
        notes: [],
      });
      // Mark items as queued
      for (const item of itemsArr) {
        await updateItemStatus(item.pro, "queued", { dispute_id: disputeId });
      }
      setDisputes(prev => [{
        id: disputeId, dispute_id: disputeId, customer: customerName || itemsArr[0].customer,
        item_count: itemsArr.length, amount_claimed: data.total_claim, outcome: null,
        package_generated_at: new Date().toISOString(),
      }, ...prev]);

      clearSelect();
    } catch(e) {
      setPdfStatus(`✗ Failed: ${e.message}`);
    }
    setGeneratingPdf(false);
  };

  const saveContact = async (key, data) => {
    await FS.saveAPContact(key, data);
    setApContacts(prev => ({...prev, [key]: {...prev[key], ...data, id: key}}));
    setEditingContact(null);
  };

  if (loading) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading audit data...</div>;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="🧾" text="Audit — Revenue Recovery" right={
      <div style={{fontSize:10,color:T.textDim}}>{itemsWithFreshAge.length} items tracked</div>
    } />

    {pdfStatus && (
      <div style={{...cardStyle, background: pdfStatus.startsWith("✓") ? T.greenBg : pdfStatus.startsWith("✗") ? T.redBg : T.yellowBg,
        borderColor: pdfStatus.startsWith("✓") ? T.green : pdfStatus.startsWith("✗") ? T.red : T.yellow,
        fontSize:12, fontWeight:600}}>{pdfStatus}</div>
    )}

    {/* View tabs */}
    <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
      {[
        ["dashboard","📊 Dashboard"],
        ["items","📋 Items"],
        ["contacts","📇 AP Contacts"],
        ["disputes","📨 Disputes"],
        ["weekly","📅 By Week"],
      ].map(([id,l])=>
        <TabButton key={id} active={view===id} label={l} onClick={()=>setView(id)} />
      )}
    </div>

    {/* ─── DASHBOARD ─── */}
    {view === "dashboard" && (
      <>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))",gap:"10px",marginBottom:"16px"}}>
          <KPI icon="💰" label="Outstanding Recovery" value={fmtK(stats.outstanding)} sub={`${stats.outstandingCount} items`} subColor={T.red} />
          <KPI icon="🏆" label="Recovered This Month" value={fmtK(stats.recovered)} sub={`${stats.wonThisMonthCount} disputes won`} subColor={T.green} />
          <KPI icon="📈" label="Win Rate" value={stats.winRate==null?"—":fmtPct(stats.winRate)} sub="of submitted disputes" subColor={T.blue} />
          <KPI icon="⏱" label="Avg Days to Recover" value={stats.avgTurnaround==null?"—":`${stats.avgTurnaround} days`} sub="submission → payment" />
        </div>

        {itemsWithFreshAge.length === 0 ? (
          <div style={cardStyle}>
            <EmptyState icon="🧾" title="No Audit Data Yet" sub="Upload Uline DDIS payment files in Data Ingest. Every billed-vs-paid discrepancy will appear here for you to chase." />
          </div>
        ) : (
          <>
            {/* Aging buckets */}
            <div style={{...cardStyle, marginBottom:12}}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Aging — Outstanding by Age</div>
              <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                {agingBuckets.map(b => {
                  const color = b.bucket === "0-30" ? T.green : b.bucket === "31-60" ? T.blue : b.bucket === "61-90" ? T.yellow : T.red;
                  return (
                    <button key={b.bucket} onClick={()=>{setFilterAge(b.bucket); setView("items");}}
                      style={{flex:"1 1 140px",padding:"10px 12px",borderRadius:10,border:`1px solid ${color}`,background:T.bgWhite,cursor:"pointer",textAlign:"left"}}>
                      <div style={{fontSize:10,color:T.textMuted,fontWeight:600}}>{b.bucket} days</div>
                      <div style={{fontSize:16,fontWeight:700,color,marginTop:2}}>{fmtK(b.amount)}</div>
                      <div style={{fontSize:10,color:T.textDim}}>{b.count} items</div>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Category breakdown */}
            <div style={{...cardStyle, marginBottom:12}}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>By Category</div>
              <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
                {byCategory.map(c => (
                  <button key={c.category} onClick={()=>{setFilterCategory(c.category); setView("items");}}
                    style={{flex:"1 1 140px",padding:"10px 12px",borderRadius:10,border:`1px solid ${CATEGORY_COLORS[c.category]}`,background:T.bgWhite,cursor:"pointer",textAlign:"left"}}>
                    <div style={{fontSize:10,color:T.textMuted,fontWeight:600}}>{CATEGORY_LABELS[c.category]}</div>
                    <div style={{fontSize:16,fontWeight:700,color:CATEGORY_COLORS[c.category],marginTop:2}}>{fmtK(c.amount)}</div>
                    <div style={{fontSize:10,color:T.textDim}}>{c.count} items</div>
                  </button>
                ))}
              </div>
            </div>

            {/* Top customers */}
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Top Customers by Outstanding $</div>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                  <thead>
                    <tr>{["Customer","Items","Outstanding","Oldest Age",""].map(h =>
                      <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {topCustomers.map(c => (
                      <tr key={c.customer}>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{c.count}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:700}}>{fmt(c.amount)}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:c.oldest_age>180?T.red:T.textMuted}}>{c.oldest_age} days</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>
                          <button onClick={()=>{setFilterCustomer(c.customer); setView("items");}}
                            style={{padding:"4px 10px",fontSize:10,fontWeight:600,borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer"}}>View →</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </>
    )}

    {/* ─── ITEMS ─── */}
    {view === "items" && (
      <>
        {/* Filters */}
        <div style={{...cardStyle, padding:"10px 12px", marginBottom:8}}>
          <div style={{display:"flex",gap:6,flexWrap:"wrap",alignItems:"center"}}>
            <input type="text" value={filterCustomer} onChange={e=>setFilterCustomer(e.target.value)}
              placeholder="Search PRO or customer..." style={{...inputStyle,flex:"1 1 160px",fontSize:12}} />
            <select value={filterCategory} onChange={e=>setFilterCategory(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All categories</option>
              {CATEGORIES.map(c => <option key={c} value={c}>{CATEGORY_LABELS[c]}</option>)}
            </select>
            <select value={filterAge} onChange={e=>setFilterAge(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All ages</option>
              {AGE_BUCKETS.map(a => <option key={a} value={a}>{a} days</option>)}
            </select>
            <select value={filterStatus} onChange={e=>setFilterStatus(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All statuses</option>
              {["new","queued","sent","won","lost","partial","written_off"].map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <select value={sortBy} onChange={e=>setSortBy(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="variance">Sort: $ owed</option>
              <option value="age">Sort: age</option>
              <option value="date">Sort: date</option>
              <option value="customer">Sort: customer</option>
            </select>
          </div>
          <div style={{fontSize:10,color:T.textMuted,marginTop:6}}>
            Showing {filteredItems.length} items • Min variance ${filterMinVar}
            {Object.keys(selectedIds).filter(k=>selectedIds[k]).length > 0 && ` • ${selectedItems.length} selected`}
          </div>
        </div>

        {/* Bulk actions */}
        {selectedItems.length > 0 && (
          <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand, padding:"10px 12px", marginBottom:8}}>
            <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",flexWrap:"wrap",gap:8}}>
              <div style={{fontSize:12,fontWeight:600,color:T.brand}}>
                {selectedItems.length} selected • {fmtK(selectedItems.reduce((s,i)=>s+i.variance,0))} total
              </div>
              <div style={{display:"flex",gap:6}}>
                <button onClick={() => {
                  // Group by customer, one PDF per customer
                  const byCust = {};
                  for (const i of selectedItems) {
                    const k = i.customer || "Unknown";
                    if (!byCust[k]) byCust[k] = [];
                    byCust[k].push(i);
                  }
                  const customers = Object.entries(byCust);
                  if (customers.length > 1 && !confirm(`Generate ${customers.length} separate PDFs (one per customer)?`)) return;
                  (async () => {
                    for (const [cust, arr] of customers) await generateDisputePdf(arr, cust);
                  })();
                }} disabled={generatingPdf}
                  style={{padding:"6px 12px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.brand}`,background:generatingPdf?T.bgSurface:T.brand,color:generatingPdf?T.text:"#fff",cursor:generatingPdf?"wait":"pointer"}}>
                  {generatingPdf ? "Generating..." : "📄 Generate Dispute PDF"}
                </button>
                <button onClick={clearSelect} style={{padding:"6px 10px",fontSize:11,borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer"}}>Clear</button>
              </div>
            </div>
          </div>
        )}

        {/* Items table */}
        <div style={{...cardStyle, padding:0, overflow:"hidden"}}>
          <div style={{maxHeight:600,overflowY:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead>
                <tr style={{background:T.bgSurface,position:"sticky",top:0,zIndex:1}}>
                  <th style={{padding:"6px 8px",borderBottom:`1px solid ${T.border}`,textAlign:"center",width:30}}>
                    <input type="checkbox" checked={filteredItems.length > 0 && filteredItems.every(i => selectedIds[i.pro])}
                      onChange={e => e.target.checked ? selectAll() : clearSelect()} />
                  </th>
                  {["PRO","Customer","Billed","Paid","Variance","Age","Category","Status"].map(h =>
                    <th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:9,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {filteredItems.slice(0, 500).map(i => {
                  const catColor = CATEGORY_COLORS[i.category] || T.textMuted;
                  const statusColor = i.dispute_status === "won" ? T.green :
                                      i.dispute_status === "sent" ? T.blue :
                                      i.dispute_status === "queued" ? T.purple :
                                      i.dispute_status === "written_off" ? T.textDim : T.textMuted;
                  return (
                    <tr key={i.pro} onClick={() => setDetailItem(i)} style={{cursor:"pointer"}}>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,textAlign:"center"}} onClick={e=>e.stopPropagation()}>
                        <input type="checkbox" checked={!!selectedIds[i.pro]} onChange={() => toggleSelect(i.pro)} />
                      </td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontWeight:600}}>{i.pro}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{i.customer || "—"}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(i.billed)}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted}}>{fmt(i.paid||0)}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:700}}>{fmt(i.variance)}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:(i.age_days||0)>180?T.red:T.textMuted}}>{i.age_days==null?"—":i.age_days+"d"}</td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>
                        <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:catColor+"22",color:catColor}}>{CATEGORY_LABELS[i.category]||i.category}</span>
                      </td>
                      <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontSize:10,color:statusColor,fontWeight:600,textTransform:"uppercase"}}>{i.dispute_status || "new"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {filteredItems.length === 0 && (
            <div style={{padding:"24px",textAlign:"center",color:T.textMuted,fontSize:12}}>No items match these filters.</div>
          )}
          {filteredItems.length > 500 && (
            <div style={{padding:"10px",textAlign:"center",fontSize:10,color:T.textMuted,background:T.bgSurface}}>Showing top 500 of {filteredItems.length} — narrow filters to see more.</div>
          )}
        </div>
      </>
    )}

    {/* ─── AP CONTACTS ─── */}
    {view === "contacts" && (
      <>
        <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand, marginBottom:12}}>
          <div style={{fontSize:12,color:T.text,lineHeight:1.5}}>
            Stub contacts are auto-created for every customer with outstanding items. Fill in the billing email + AP contact name so dispute PDFs have a proper "To:" address.
          </div>
        </div>
        {Object.values(apContacts).length === 0 ? (
          <div style={cardStyle}>
            <EmptyState icon="📇" title="No Customer Contacts Yet" sub="Upload Uline DDIS files to auto-seed customer contacts from the outstanding queue." />
          </div>
        ) : (
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:10}}>
            {Object.values(apContacts).sort((a,b) => (b.total_owed_cached||0) - (a.total_owed_cached||0)).slice(0, 100).map(c => (
              <div key={c.id} style={cardStyle}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:8}}>
                  <div style={{flex:1,minWidth:0}}>
                    <div style={{fontSize:13,fontWeight:700,overflow:"hidden",textOverflow:"ellipsis"}}>{c.customer}</div>
                    <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>
                      {c.item_count_cached||0} items • {fmtK(c.total_owed_cached||0)} owed
                    </div>
                  </div>
                  <button onClick={()=>setEditingContact(c.id)}
                    style={{padding:"4px 10px",fontSize:10,borderRadius:6,border:`1px solid ${T.brand}`,background:T.bgWhite,color:T.brand,cursor:"pointer",fontWeight:600}}>
                    Edit
                  </button>
                </div>
                <div style={{marginTop:10,fontSize:11,color:T.textMuted,lineHeight:1.6}}>
                  <div><strong>Email:</strong> {c.billing_email || <em style={{color:T.redText}}>not set</em>}</div>
                  <div><strong>AP Contact:</strong> {c.ap_contact_name || <em style={{color:T.textDim}}>—</em>}</div>
                  <div><strong>Phone:</strong> {c.ap_contact_phone || <em style={{color:T.textDim}}>—</em>}</div>
                </div>
              </div>
            ))}
          </div>
        )}

        {editingContact && apContacts[editingContact] && (
          <ContactEditor contact={apContacts[editingContact]} onSave={(data) => saveContact(editingContact, data)} onCancel={() => setEditingContact(null)} />
        )}
      </>
    )}

    {/* ─── DISPUTES ─── */}
    {view === "disputes" && (
      <>
        {disputes.length === 0 ? (
          <div style={cardStyle}>
            <EmptyState icon="📨" title="No Disputes Yet" sub="Select items in the Items tab and click 'Generate Dispute PDF' to create your first dispute package." />
          </div>
        ) : (
          <div style={{...cardStyle, padding:0}}>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead>
                  <tr style={{background:T.bgSurface}}>
                    {["Customer","Items","Claimed","Recovered","Submitted","Outcome","Actions"].map(h =>
                      <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {disputes.map(d => {
                    const outcomeColor = d.outcome === "won" ? T.green : d.outcome === "lost" ? T.red : d.outcome === "partial" ? T.yellow : T.textMuted;
                    return (
                      <tr key={d.id}>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.customer}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{d.item_count}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.brand,fontWeight:700}}>{fmt(d.amount_claimed||0)}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{d.amount_recovered?fmt(d.amount_recovered):"—"}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:10}}>{d.submitted_date ? new Date(d.submitted_date).toLocaleDateString() : <em style={{color:T.textDim}}>not sent</em>}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:10,fontWeight:700,color:outcomeColor,textTransform:"uppercase"}}>{d.outcome || "pending"}</td>
                        <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>
                          <DisputeActions dispute={d} onUpdate={async (updates) => {
                            const merged = {...d, ...updates};
                            await FS.saveDispute(d.id, merged);
                            setDisputes(prev => prev.map(x => x.id === d.id ? merged : x));
                            // If marked won/lost/partial, update the items too
                            if (updates.outcome && d.item_pros) {
                              for (const pro of d.item_pros) {
                                await updateItemStatus(pro, updates.outcome);
                              }
                            }
                          }} />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </>
    )}

    {/* ─── WEEKLY (legacy reconciliation by-week view) ─── */}
    {view === "weekly" && (
      <WeeklyReconView reconWeekly={reconWeekly} />
    )}

    {/* Item detail modal */}
    {detailItem && (
      <AuditItemDetail item={detailItem} contact={apContacts[detailItem.customer_key]} onClose={()=>setDetailItem(null)}
        onGeneratePdf={() => { generateDisputePdf([detailItem], detailItem.customer); setDetailItem(null); }}
        onUpdateStatus={(s) => { updateItemStatus(detailItem.pro, s); setDetailItem({...detailItem, dispute_status: s}); }}
      />
    )}
  </div>;
}

// ─── Sub-components ─────────────────────────────────────────

function WeeklyReconView({ reconWeekly }) {
  const reconWithPayments = reconWeekly.filter(r => r.paid_matched > 0);
  const reconWeeks = reconWithPayments.sort((a,b) => a.week_ending.localeCompare(b.week_ending));
  const totalBilled = reconWithPayments.reduce((s,r) => s + (r.billed||0), 0);
  const totalPaid = reconWithPayments.reduce((s,r) => s + (r.paid_matched||0), 0);
  const overallCollectionRate = totalBilled > 0 ? (totalPaid / totalBilled * 100) : 0;

  if (reconWithPayments.length === 0) {
    return <div style={cardStyle}><EmptyState icon="📅" title="No Weekly Data Yet" sub="Upload DDIS files to see billed vs paid by week." /></div>;
  }

  return <div style={cardStyle}>
    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:10,marginBottom:12}}>
      <KPI label="Collection Rate" value={fmtPct(overallCollectionRate)} subColor={overallCollectionRate>=95?T.green:overallCollectionRate>=85?T.yellow:T.red} />
      <KPI label="Total Billed" value={fmtK(totalBilled)} subColor={T.green} />
      <KPI label="Total Paid" value={fmtK(totalPaid)} subColor={T.blue} />
      <KPI label="Gap" value={fmtK(totalBilled-totalPaid)} subColor={T.red} />
    </div>
    <LineTrend data={reconWeeks} xKey="week_ending" yKey="billed" y2Key="paid_matched" label="Billed" y2Label="Paid" height={200} />
    <div style={{marginTop:16,overflowX:"auto",maxHeight:400}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
        <thead><tr>{["Week","Billed","Paid","Gap","Collect %","Unpaid $"].map(h=>
          <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>
        )}</tr></thead>
        <tbody>
          {reconWeeks.slice().reverse().map((r,i) => {
            const cr = r.collection_rate;
            const crColor = cr==null?T.textDim : cr>=95?T.green : cr>=85?T.yellow : T.red;
            const gap = (r.paid_matched||0) - (r.billed||0);
            return <tr key={i}>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>WE {weekLabel(r.week_ending)}</td>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.green}}>{fmt(r.billed)}</td>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(r.paid_matched)}</td>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:gap<0?T.red:T.green}}>{fmt(gap)}</td>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{cr!=null?<Badge text={fmtPct(cr)} color={crColor} bg={crColor===T.green?T.greenBg:crColor===T.yellow?T.yellowBg:T.redBg} />:"—"}</td>
              <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{r.unpaid_amount?fmt(r.unpaid_amount):"—"}</td>
            </tr>;
          })}
        </tbody>
      </table>
    </div>
  </div>;
}

function ContactEditor({ contact, onSave, onCancel }) {
  const [form, setForm] = useState({
    billing_email: contact.billing_email || "",
    ap_contact_name: contact.ap_contact_name || "",
    ap_contact_phone: contact.ap_contact_phone || "",
    dispute_portal_url: contact.dispute_portal_url || "",
    expected_response_sla_days: contact.expected_response_sla_days || 30,
  });
  const upd = (k, v) => setForm(p => ({...p, [k]: v}));
  return (
    <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.5)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:1000,padding:16}} onClick={onCancel}>
      <div style={{background:T.bgWhite,borderRadius:12,padding:20,maxWidth:500,width:"100%",maxHeight:"90vh",overflowY:"auto"}} onClick={e=>e.stopPropagation()}>
        <div style={{fontSize:15,fontWeight:700,marginBottom:4}}>{contact.customer}</div>
        <div style={{fontSize:11,color:T.textMuted,marginBottom:16}}>AP Contact Information</div>
        {[
          ["billing_email","Billing Email","email"],
          ["ap_contact_name","AP Contact Name","text"],
          ["ap_contact_phone","Phone","tel"],
          ["dispute_portal_url","Dispute Portal URL","url"],
          ["expected_response_sla_days","Expected Response SLA (days)","number"],
        ].map(([k,l,type]) => (
          <div key={k} style={{marginBottom:10}}>
            <label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:3}}>{l}</label>
            <input type={type} value={form[k]} onChange={e=>upd(k, type==="number"?parseInt(e.target.value)||0:e.target.value)} style={{...inputStyle,width:"100%"}} />
          </div>
        ))}
        <div style={{display:"flex",gap:8,marginTop:16,justifyContent:"flex-end"}}>
          <button onClick={onCancel} style={{padding:"8px 16px",borderRadius:8,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer",fontSize:12}}>Cancel</button>
          <PrimaryBtn text="Save" onClick={()=>onSave(form)} />
        </div>
      </div>
    </div>
  );
}

function DisputeActions({ dispute, onUpdate }) {
  const [expanded, setExpanded] = useState(false);
  if (!expanded) {
    return <button onClick={()=>setExpanded(true)} style={{padding:"4px 10px",fontSize:10,borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer"}}>Update</button>;
  }
  return (
    <div style={{display:"flex",flexDirection:"column",gap:4}}>
      {!dispute.submitted_date && (
        <button onClick={() => { onUpdate({ submitted_date: new Date().toISOString() }); setExpanded(false); }}
          style={{padding:"3px 8px",fontSize:9,borderRadius:4,border:`1px solid ${T.blue}`,background:T.bgWhite,color:T.blue,cursor:"pointer",fontWeight:600}}>Mark Sent</button>
      )}
      <button onClick={() => {
        const amt = parseFloat(prompt("Amount recovered ($):", dispute.amount_claimed) || "0");
        if (amt > 0) onUpdate({ outcome: amt >= dispute.amount_claimed*0.99 ? "won" : "partial", amount_recovered: amt, response_date: new Date().toISOString() });
        setExpanded(false);
      }}
        style={{padding:"3px 8px",fontSize:9,borderRadius:4,border:`1px solid ${T.green}`,background:T.bgWhite,color:T.green,cursor:"pointer",fontWeight:600}}>Won/Partial</button>
      <button onClick={() => { onUpdate({ outcome: "lost", response_date: new Date().toISOString() }); setExpanded(false); }}
        style={{padding:"3px 8px",fontSize:9,borderRadius:4,border:`1px solid ${T.red}`,background:T.bgWhite,color:T.red,cursor:"pointer",fontWeight:600}}>Lost</button>
      <button onClick={()=>setExpanded(false)} style={{padding:"3px 8px",fontSize:9,borderRadius:4,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer"}}>Cancel</button>
    </div>
  );
}

function AuditItemDetail({ item, contact, onClose, onGeneratePdf, onUpdateStatus }) {
  const catColor = CATEGORY_COLORS[item.category] || T.textMuted;
  return (
    <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.5)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:1000,padding:16}} onClick={onClose}>
      <div style={{background:T.bgWhite,borderRadius:12,padding:20,maxWidth:600,width:"100%",maxHeight:"90vh",overflowY:"auto"}} onClick={e=>e.stopPropagation()}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:12,gap:8}}>
          <div>
            <div style={{fontSize:10,color:T.textMuted,fontFamily:"monospace"}}>PRO {item.pro}</div>
            <div style={{fontSize:16,fontWeight:700,marginTop:2}}>{item.customer}</div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>{item.city}{item.state?`, ${item.state}`:""}{item.zip?` ${item.zip}`:""}</div>
          </div>
          <span style={{fontSize:10,fontWeight:700,padding:"4px 10px",borderRadius:6,background:catColor+"22",color:catColor}}>{CATEGORY_LABELS[item.category]}</span>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginBottom:12}}>
          <div style={{padding:"10px",background:T.bgSurface,borderRadius:8}}>
            <div style={{fontSize:10,color:T.textMuted}}>Billed</div>
            <div style={{fontSize:18,fontWeight:700,color:T.brand}}>{fmt(item.billed)}</div>
          </div>
          <div style={{padding:"10px",background:T.bgSurface,borderRadius:8}}>
            <div style={{fontSize:10,color:T.textMuted}}>Paid</div>
            <div style={{fontSize:18,fontWeight:700}}>{fmt(item.paid||0)}</div>
          </div>
        </div>
        <div style={{padding:"10px",background:T.redBg,borderRadius:8,marginBottom:12}}>
          <div style={{fontSize:10,color:T.redText}}>Variance — Outstanding</div>
          <div style={{fontSize:22,fontWeight:800,color:T.red}}>{fmt(item.variance)}</div>
          <div style={{fontSize:10,color:T.redText,marginTop:2}}>{item.age_days!=null?`${item.age_days} days old`:""} • {item.variance_pct!=null?`${fmtPct(item.variance_pct)} variance`:""}</div>
        </div>

        <div style={{fontSize:11,color:T.textMuted,lineHeight:1.7,marginBottom:12}}>
          <div><strong>Pickup:</strong> {item.pu_date || "—"}</div>
          <div><strong>Week Ending:</strong> {item.week_ending || "—"}</div>
          {item.weight > 0 && <div><strong>Weight:</strong> {fmtNum(item.weight)} lbs</div>}
          {item.code && <div><strong>Accessorial Code:</strong> {item.code}</div>}
          {item.order && <div><strong>Order #:</strong> {item.order}</div>}
          {item.accessorial_amount > 0 && <div><strong>Accessorial Amount:</strong> {fmt(item.accessorial_amount)}</div>}
          <div><strong>Current Status:</strong> <span style={{textTransform:"uppercase",fontWeight:600}}>{item.dispute_status||"new"}</span></div>
        </div>

        {contact && (
          <div style={{padding:"10px",background:T.brandPale,borderRadius:8,marginBottom:12,fontSize:11,color:T.text}}>
            <div style={{fontWeight:700,marginBottom:4}}>AP Contact on File</div>
            <div>📧 {contact.billing_email || <em style={{color:T.redText}}>not set — set in AP Contacts tab before generating PDF</em>}</div>
            {contact.ap_contact_name && <div>👤 {contact.ap_contact_name}</div>}
            {contact.ap_contact_phone && <div>📞 {contact.ap_contact_phone}</div>}
          </div>
        )}

        <div style={{display:"flex",gap:6,flexWrap:"wrap",marginTop:16}}>
          <PrimaryBtn text="📄 Generate Dispute PDF" onClick={onGeneratePdf} />
          {["new","queued","sent","written_off"].map(s => (
            <button key={s} onClick={()=>onUpdateStatus(s)}
              style={{padding:"6px 10px",fontSize:11,borderRadius:6,border:`1px solid ${item.dispute_status===s?T.brand:T.border}`,background:item.dispute_status===s?T.brand:T.bgWhite,color:item.dispute_status===s?"#fff":T.text,cursor:"pointer",fontWeight:600}}>
              Mark {s}
            </button>
          ))}
          <button onClick={onClose} style={{padding:"6px 10px",fontSize:11,borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,cursor:"pointer"}}>Close</button>
        </div>
      </div>
    </div>
  );
}

// ═══ COSTS ═══════════════════════════════════════════════════
function CostStructure({ costs, onSave, margins }) {
  const [c, setC] = useState({...DEFAULT_COSTS, ...costs});
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const upd = (k, v) => setC(prev => ({...prev, [k]: v}));
  const save = async () => { setSaving(true); await FS.saveCosts(c); onSave(c); setSaving(false); setSaved(true); setTimeout(()=>setSaved(false), 2000); };
  const Field = ({ label, field, prefix, suffix, step }) => (
    <div style={{marginBottom:8}}>
      <label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:3}}>{label}</label>
      <div style={{display:"flex",alignItems:"center",gap:4}}>
        {prefix && <span style={{fontSize:12,color:T.textDim}}>{prefix}</span>}
        <input type="number" value={c[field]||""} step={step||"any"} onChange={e=>upd(field, parseFloat(e.target.value)||0)} style={{...inputStyle,width:"100%"}} />
        {suffix && <span style={{fontSize:12,color:T.textDim,whiteSpace:"nowrap"}}>{suffix}</span>}
      </div>
    </div>
  );
  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="⚙️" text="Cost Structure" right={
      <div style={{display:"flex",gap:8,alignItems:"center"}}>
        {saved && <Badge text="✓ Saved" color={T.greenText} bg={T.greenBg} />}
        <PrimaryBtn text="Save Changes" onClick={save} loading={saving} style={{padding:"8px 16px",fontSize:12}} />
      </div>
    } />
    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>🏭 Facility</div>
        <Field label="Warehouse (annual)" field="warehouse" prefix="$" />
        <Field label="Forklifts (annual)" field="forklifts" prefix="$" />
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>👥 Headcounts</div>
        <Field label="Box Truck Drivers" field="count_box_drivers" />
        <Field label="Tractor Trailer Drivers" field="count_tractor_drivers" />
        <Field label="Dispatchers" field="count_dispatchers" />
        <Field label="Admin/Office" field="count_admin" />
        <Field label="Mechanics" field="count_mechanics" />
        <Field label="Forklift Operators" field="count_forklift_ops" />
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>💵 Hourly Rates</div>
        <Field label="Box Truck Driver" field="rate_box_driver" prefix="$" suffix="/hr" step="0.50" />
        <Field label="Tractor Driver" field="rate_tractor_driver" prefix="$" suffix="/hr" step="0.50" />
        <Field label="Dispatcher" field="rate_dispatcher" prefix="$" suffix="/hr" step="0.50" />
        <Field label="Admin" field="rate_admin" prefix="$" suffix="/hr" step="0.50" />
        <Field label="Mechanic" field="rate_mechanic" prefix="$" suffix="/hr" step="0.50" />
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>🚛 Fleet</div>
        <Field label="Box Truck Count" field="truck_count_box" />
        <Field label="Tractor Count" field="truck_count_tractor" />
        <Field label="Insurance per truck/month" field="truck_insurance_monthly" prefix="$" />
        <Field label="Box Truck MPG" field="mpg_box" suffix="MPG" />
        <Field label="Tractor MPG" field="mpg_tractor" suffix="MPG" />
        <Field label="Fuel Price" field="fuel_price" prefix="$" suffix="/gal" step="0.01" />
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>📅 Operations</div>
        <Field label="Working Days/Year" field="working_days_year" />
        <Field label="Avg Hours/Shift" field="avg_hours_per_shift" />
        <Field label="Contractor Payout %" field="contractor_pct" step="0.01" />
      </div>
      <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10,color:T.brand}}>📊 Calculated Totals</div>
        <DataRow label="Annual Labor" value={fmtK(margins.totalAnnualLabor)} valueColor={T.brand} bold />
        <DataRow label="Annual Fixed" value={fmtK(margins.totalAnnualFixed)} valueColor={T.brand} bold />
        <DataRow label="TOTAL ANNUAL COST" value={fmtK(margins.totalAnnualCost)} valueColor={T.red} bold />
        <DataRow label="Daily Cost" value={fmt(margins.dailyCost)} valueColor={T.red} />
        <DataRow label="Monthly Cost" value={fmtK(margins.monthlyCost)} valueColor={T.red} />
      </div>
    </div>
  </div>;
}

// ═══ DRIVERS — W2/1099 Classification ═══════════════════════
// Reads all unique driver names from NuVizz weekly rollups and payroll weeks,
// merges with saved classifications, and lets user tag each as W2 / 1099 / Unknown.
// Calculates true contractor cost by only applying 40% to drivers marked 1099.
function Drivers() {
  const [nvWeekly, setNvWeekly] = useState([]);
  const [payWeekly, setPayWeekly] = useState([]);
  const [classifications, setClassifications] = useState({}); // key -> {classification, notes, source}
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState({}); // key -> bool
  const [filter, setFilter] = useState("all"); // all | unknown | w2 | 1099
  const [search, setSearch] = useState("");

  useEffect(() => {
    (async () => {
      setLoading(true);
      const [nv, pw, cls] = await Promise.all([
        FS.getNuVizzWeekly(), FS.getPayrollWeekly(), FS.getDriverClassifications(),
      ]);
      setNvWeekly(nv);
      setPayWeekly(pw);
      const clsMap = {};
      for (const c of cls) clsMap[c.id] = c;
      setClassifications(clsMap);
      setLoading(false);
    })();
  }, []);

  // Aggregate all drivers seen across NuVizz + Payroll
  const drivers = useMemo(() => {
    const agg = {}; // key -> {name, nv_stops, nv_pay_base, pay_weeks_seen, last_seen, sources}
    for (const w of nvWeekly) {
      if (!w.top_drivers) continue;
      for (const d of w.top_drivers) {
        if (!d.name) continue;
        const key = driverKey(d.name);
        if (!key) continue;
        if (!agg[key]) agg[key] = { key, name: d.name, nv_stops:0, nv_pay_base:0, nv_pay_at_40:0, pay_weeks:0, pay_gross:0, pay_hours:0, last_seen:null, sources:new Set() };
        agg[key].nv_stops += d.stops || 0;
        agg[key].nv_pay_base += d.pay_base || 0;
        agg[key].nv_pay_at_40 += d.pay_at_40 || 0;
        agg[key].sources.add("nuvizz");
        if (!agg[key].last_seen || w.week_ending > agg[key].last_seen) agg[key].last_seen = w.week_ending;
      }
    }
    for (const w of payWeekly) {
      if (!w.employees) continue;
      for (const e of w.employees) {
        if (!e.name) continue;
        const key = driverKey(e.name);
        if (!key) continue;
        if (!agg[key]) agg[key] = { key, name: e.name, nv_stops:0, nv_pay_base:0, nv_pay_at_40:0, pay_weeks:0, pay_gross:0, pay_hours:0, last_seen:null, sources:new Set() };
        agg[key].pay_weeks++;
        agg[key].pay_gross += e.gross || 0;
        agg[key].pay_hours += e.hours || 0;
        agg[key].sources.add("payroll");
        if (!agg[key].last_seen || w.week_ending > agg[key].last_seen) agg[key].last_seen = w.week_ending;
      }
    }
    return Object.values(agg).map(d => ({...d, sources: Array.from(d.sources)}));
  }, [nvWeekly, payWeekly]);

  // Merge with saved classifications
  const classified = useMemo(() => drivers.map(d => {
    const cls = classifications[d.key];
    return {
      ...d,
      classification: cls?.classification || "unknown",
      notes: cls?.notes || "",
      class_source: cls?.source || null,
    };
  }).sort((a,b) => b.nv_stops - a.nv_stops), [drivers, classifications]);

  const filtered = useMemo(() => {
    let arr = classified;
    if (filter !== "all") arr = arr.filter(d => d.classification === filter);
    if (search.trim()) {
      const q = search.toLowerCase();
      arr = arr.filter(d => d.name.toLowerCase().includes(q));
    }
    return arr;
  }, [classified, filter, search]);

  const setClass = async (key, name, newClass) => {
    setSaving(prev => ({...prev, [key]:true}));
    setClassifications(prev => ({...prev, [key]: {...(prev[key]||{}), classification: newClass, name, source: "manual"}}));
    await FS.saveDriverClassification(key, { name, classification: newClass, source: "manual" });
    setSaving(prev => ({...prev, [key]:false}));
  };

  // Totals
  const totals = useMemo(() => {
    const t = { all:0, w2:0, contractor_1099:0, unknown:0,
                nv_pay_base_all:0, nv_pay_base_1099:0, contractor_cost_actual:0,
                contractor_cost_unknown:0 };
    for (const d of classified) {
      t.all++;
      if (d.classification === "w2") t.w2++;
      else if (d.classification === "1099") { t.contractor_1099++; t.nv_pay_base_1099 += d.nv_pay_base; t.contractor_cost_actual += d.nv_pay_at_40; }
      else { t.unknown++; t.contractor_cost_unknown += d.nv_pay_at_40; }
      t.nv_pay_base_all += d.nv_pay_base;
    }
    return t;
  }, [classified]);

  if (loading) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading drivers...</div>;

  if (drivers.length === 0) {
    return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="👥" text="Drivers" />
      <EmptyState icon="👥" title="No Drivers Found" sub="Upload NuVizz or Payroll files in Data Ingest. Drivers appearing there will show up here for classification." />
    </div>;
  }

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="👥" text="Drivers — W2 / 1099 Classification" />

    <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
      <div style={{fontSize:12,color:T.text,lineHeight:1.6}}>
        <strong>Why this matters:</strong> NuVizz "Stop SealNbr" is the base for 1099 contractor pay (they get 40%). W2 drivers are paid hourly via CyberPay, not per stop. Tag each driver once — MarginIQ applies the right cost formula automatically. Historical drivers who've left still need to be tagged so past-period margins are accurate.
      </div>
    </div>

    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginTop:12}}>
      <KPI label="Total Drivers" value={totals.all} sub={`${totals.w2} W2 • ${totals.contractor_1099} 1099 • ${totals.unknown} unknown`} />
      <KPI label="Actual 1099 Cost" value={fmtK(totals.contractor_cost_actual)} sub="40% of their SealNbr totals" subColor={T.green} />
      <KPI label="Unclassified Pay Base" value={fmtK(totals.contractor_cost_unknown)} sub="cost if these are 1099" subColor={totals.unknown>0?T.yellowText:T.textMuted} />
      <KPI label="Upper Bound (All 1099)" value={fmtK(totals.nv_pay_base_all * 0.40)} sub="ceiling if every driver were 1099" />
    </div>

    <div style={{display:"flex",gap:8,marginTop:16,alignItems:"center",flexWrap:"wrap"}}>
      <input type="text" value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search driver..." style={{flex:"1 1 200px",padding:"8px 12px",borderRadius:8,border:`1px solid ${T.border}`,fontSize:12}} />
      {[["all","All"],["unknown","Unknown"],["w2","W2"],["1099","1099"]].map(([v,l]) => (
        <button key={v} onClick={()=>setFilter(v)} style={{padding:"7px 12px",borderRadius:8,border:`1px solid ${filter===v?T.brand:T.border}`,background:filter===v?T.brand:T.bgWhite,color:filter===v?"#fff":T.text,fontSize:12,fontWeight:600,cursor:"pointer"}}>{l}</button>
      ))}
    </div>

    <div style={{...cardStyle, padding:0, marginTop:12, overflow:"hidden"}}>
      <table style={{width:"100%",fontSize:12,borderCollapse:"collapse"}}>
        <thead>
          <tr style={{background:T.bgSurface,borderBottom:`1px solid ${T.border}`}}>
            {["Driver","Stops","Pay Base (SealNbr)","@40% (if 1099)","Payroll $","Sources","Last Seen","Classification"].map(h =>
              <th key={h} style={{textAlign:"left",padding:"8px 10px",fontWeight:700,color:T.textMuted,fontSize:11}}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {filtered.map(d => {
            const isSaving = saving[d.key];
            const cls = d.classification;
            return <tr key={d.key} style={{borderBottom:`1px solid ${T.borderLight}`}}>
              <td style={{padding:"8px 10px",fontWeight:600}}>{d.name}</td>
              <td style={{padding:"8px 10px"}}>{fmtNum(d.nv_stops)}</td>
              <td style={{padding:"8px 10px"}}>{fmt(d.nv_pay_base)}</td>
              <td style={{padding:"8px 10px",color:cls==="1099"?T.green:T.textMuted}}>{fmt(d.nv_pay_at_40)}</td>
              <td style={{padding:"8px 10px"}}>{d.pay_gross>0?fmt(d.pay_gross):"—"}</td>
              <td style={{padding:"8px 10px",fontSize:10,color:T.textMuted}}>{d.sources.join(", ")}</td>
              <td style={{padding:"8px 10px",fontSize:10,color:T.textMuted}}>{d.last_seen || "—"}</td>
              <td style={{padding:"6px 10px"}}>
                <div style={{display:"flex",gap:4}}>
                  {[["w2","W2",T.blue],["1099","1099",T.green],["unknown","?",T.textMuted]].map(([v,l,c]) => (
                    <button key={v} onClick={()=>setClass(d.key, d.name, v)} disabled={isSaving}
                      style={{padding:"4px 10px",borderRadius:6,border:`1px solid ${cls===v?c:T.border}`,background:cls===v?c:T.bgWhite,color:cls===v?"#fff":T.text,fontSize:11,fontWeight:700,cursor:isSaving?"wait":"pointer",opacity:isSaving?0.5:1}}>{l}</button>
                  ))}
                </div>
              </td>
            </tr>;
          })}
        </tbody>
      </table>
      {filtered.length === 0 && (
        <div style={{padding:"24px",textAlign:"center",color:T.textMuted,fontSize:12}}>No drivers match the filter.</div>
      )}
    </div>

    <div style={{...cardStyle, background:T.bgSurface, marginTop:12}}>
      <div style={{fontSize:11,color:T.textMuted,lineHeight:1.6}}>
        <strong>How cost is calculated per driver:</strong><br/>
        • <strong>1099:</strong> NuVizz SealNbr × 40% per stop<br/>
        • <strong>W2:</strong> Hours from CyberPay × hourly rate (from cost structure)<br/>
        • <strong>Unknown:</strong> Excluded from margin calcs until classified — shown above as "Unclassified Pay Base"<br/>
        Current Fleet Management drivers can be auto-classified in a future update (we read from the shared Firebase).
      </div>
    </div>
  </div>;
}

function Settings({ qboConnected, motiveConnected, reconMeta, weeklyRollups }) {
  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="⚙️" text="Settings & Connections" />
    <div style={cardStyle}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Data Connections</div>
      {[
        { name:"QuickBooks Online", on:qboConnected, sub:qboConnected?"Connected":"Not connected", action:!qboConnected?"/.netlify/functions/marginiq-qbo-auth":null },
        { name:"NuVizz", on:true, sub:"Connected" },
        { name:"CyberPay (Payroll)", on:true, sub:"Auto-pulls Mondays" },
        { name:"Motive (Fleet)", on:motiveConnected, sub:motiveConnected?"Connected":"Checking..." },
      ].map((s,i) => <div key={i} style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:i<3?`1px solid ${T.border}`:"none"}}>
        <div style={{width:10,height:10,borderRadius:"50%",background:s.on?T.green:T.textDim,boxShadow:s.on?`0 0 8px ${T.green}`:"none",flexShrink:0}} />
        <div style={{flex:1}}>
          <div style={{fontSize:13,fontWeight:600}}>{s.name}</div>
          <div style={{fontSize:11,color:s.on?T.green:T.textDim}}>{s.sub}</div>
        </div>
        {s.action && <a href={s.action} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.brand}`,color:T.brand,fontSize:12,fontWeight:600,textDecoration:"none"}}>Connect</a>}
      </div>)}
    </div>
    <div style={cardStyle}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>System Info</div>
      {[["Firebase","davismarginiq"],["Netlify","davis-marginiq.netlify.app"],["Version",APP_VERSION],["Weekly Rollups",weeklyRollups.length],["DDIS Files Loaded",reconMeta?reconMeta.files_count:0],["Last Upload",reconMeta?.last_upload?new Date(reconMeta.last_upload).toLocaleString():"Never"]].map(([l,v])=>
        <DataRow key={l} label={l} value={String(v)} />
      )}
    </div>
  </div>;
}

// ═══ MAIN ═══════════════════════════════════════════════════
function MarginIQ() {
  const [tab, setTab] = useState("command");
  const [costs, setCosts] = useState(DEFAULT_COSTS);
  const [weeklyRollups, setWeeklyRollups] = useState([]);
  const [reconWeekly, setReconWeekly] = useState([]);
  const [fileLog, setFileLog] = useState([]);
  const [reconMeta, setReconMeta] = useState(null);
  const [qboConnected, setQboConnected] = useState(false);
  const [motiveConnected, setMotiveConnected] = useState(false);
  const [loading, setLoading] = useState(true);

  const refreshData = async () => {
    const [weekly, recon, meta, log] = await Promise.all([
      FS.getWeeklyRollups(), FS.getReconWeekly(), FS.getReconMeta(), FS.getFileLog(100),
    ]);
    setWeeklyRollups(weekly.sort((a,b) => a.week_ending.localeCompare(b.week_ending)));
    setReconWeekly(recon);
    setReconMeta(meta);
    setFileLog(log);
  };

  useEffect(() => {
    const init = async () => {
      setLoading(true);
      const savedCosts = await FS.getCosts();
      if (savedCosts) setCosts(prev => ({...prev, ...savedCosts}));
      await refreshData();
      try {
        const r = await fetch("/.netlify/functions/marginiq-qbo-data?action=status");
        const d = await r.json();
        if (d.connected) setQboConnected(true);
      } catch(e) {}
      const params = new URLSearchParams(window.location.search);
      if (params.get("qbo")==="connected") { setQboConnected(true); window.history.replaceState({},"","/"); }
      try {
        const r = await fetch("/.netlify/functions/marginiq-motive?action=vehicles");
        if (r.ok) setMotiveConnected(true);
      } catch(e) {}
      setLoading(false);
    };
    init();
  }, []);

  // Margin uses last 12 full weeks of BILLED revenue (source of truth)
  const margins = useMemo(() => {
    const now = new Date().toISOString().slice(0,10);
    const usable = weeklyRollups.filter(r => r.week_ending < now).slice(-52);
    let weeklyWindow = null;
    if (usable.length > 0) {
      weeklyWindow = {
        totalRevenue: usable.reduce((s,r) => s + (r.revenue||0), 0),
        totalStops: usable.reduce((s,r) => s + (r.stops||0), 0),
        weeksCount: usable.length,
      };
    }
    return calculateMargins(costs, weeklyWindow);
  }, [costs, weeklyRollups]);

  // Data completeness scan
  const completeness = useMemo(() => {
    if (weeklyRollups.length === 0) return null;
    return scanCompleteness(weeklyRollups, null, BUSINESS_START);
  }, [weeklyRollups]);

  const tabs = [
    { id:"command", icon:"🎯", label:"Command" },
    { id:"revenue", icon:"💰", label:"Uline Revenue" },
    { id:"recon", icon:"🧾", label:"Audit" },
    { id:"drivers", icon:"👥", label:"Drivers" },
    { id:"timeclock", icon:"⏰", label:"Time Clock" },
    { id:"fuel", icon:"⛽", label:"Fuel" },
    { id:"completeness", icon:"✅", label:"Data Health" },
    { id:"ingest", icon:"📤", label:"Data Ingest" },
    { id:"gmail", icon:"📧", label:"Gmail Sync" },
    { id:"costs", icon:"⚙️", label:"Costs" },
    { id:"settings", icon:"🔧", label:"Settings" },
  ];

  return <div style={{minHeight:"100vh",background:T.bg,color:T.text,fontFamily:"'DM Sans',sans-serif"}}>
    <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"12px 20px",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.95)",backdropFilter:"blur(12px)",position:"sticky",top:0,zIndex:100,boxShadow:"0 1px 4px rgba(0,0,0,0.04)"}}>
      <div style={{display:"flex",alignItems:"center",gap:10}}>
        <div style={{width:36,height:36,borderRadius:"10px",background:`linear-gradient(135deg,${T.brand},${T.brandLight})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:"15px",fontWeight:800,color:"#fff",boxShadow:"0 2px 8px rgba(30,91,146,0.3)"}}>M</div>
        <div>
          <div style={{fontSize:"15px",fontWeight:800,letterSpacing:"-0.02em"}}>Davis MarginIQ</div>
          <div style={{fontSize:"9px",color:T.textDim,letterSpacing:"0.1em",textTransform:"uppercase"}}>Cost Intelligence Platform</div>
        </div>
      </div>
      <div style={{display:"flex",alignItems:"center",gap:8}}>
        {completeness && completeness.gaps.length > 0 && <Badge text={`${completeness.gaps.length} gaps`} color={T.yellowText} bg={T.yellowBg} />}
        {qboConnected && <Badge text="QBO ✓" color={T.greenText} bg={T.greenBg} />}
        <span style={{fontSize:"9px",color:T.textDim,padding:"3px 8px",background:T.bgSurface,borderRadius:"6px",fontWeight:600}}>v{APP_VERSION}</span>
      </div>
    </div>
    <div style={{display:"flex",gap:"3px",padding:"8px 12px",overflowX:"auto",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.7)"}}>
      {tabs.map(t => <button key={t.id} onClick={()=>setTab(t.id)} style={{padding:"7px 14px",borderRadius:"8px",border:"none",background:tab===t.id?T.brand:"transparent",color:tab===t.id?"#fff":T.textMuted,fontSize:"12px",fontWeight:tab===t.id?700:500,cursor:"pointer",whiteSpace:"nowrap",transition:"all 0.2s"}}>{t.icon} {t.label}</button>)}
    </div>
    {loading && <div style={{textAlign:"center",padding:"60px 20px"}}>
      <div className="loading-pulse" style={{fontSize:48,marginBottom:12}}>📊</div>
      <div style={{fontSize:14,fontWeight:600}}>Loading MarginIQ...</div>
    </div>}
    {!loading && tab==="command" && <CommandCenter margins={margins} weeklyRollups={weeklyRollups} completeness={completeness} qboConnected={qboConnected} reconMeta={reconMeta} connections={{nuvizz:true,motive:motiveConnected,cyberpay:true}} setTab={setTab} />}
    {!loading && tab==="revenue" && <UlineRevenue weeklyRollups={weeklyRollups} />}
    {!loading && tab==="recon" && <Audit reconWeekly={reconWeekly} weeklyRollups={weeklyRollups} />}
    {!loading && tab==="drivers" && <Drivers />}
    {!loading && tab==="timeclock" && (typeof window !== "undefined" && window.TimeClockTab ? React.createElement(window.TimeClockTab) : <EmptyState icon="⏰" title="Time Clock module not loaded" sub="TimeClockTab.jsx did not load. Check console." />)}
    {!loading && tab==="fuel" && <Fuel />}
    {!loading && tab==="completeness" && <DataCompleteness weeklyRollups={weeklyRollups} completeness={completeness} fileLog={fileLog} />}
    {!loading && tab==="ingest" && <DataIngest weeklyRollups={weeklyRollups} reconMeta={reconMeta} onRefresh={refreshData} />}
    {!loading && tab==="gmail" && <GmailSync onRefresh={refreshData} />}
    {!loading && tab==="costs" && <CostStructure costs={costs} onSave={setCosts} margins={margins} />}
    {!loading && tab==="settings" && <Settings qboConnected={qboConnected} motiveConnected={motiveConnected} reconMeta={reconMeta} weeklyRollups={weeklyRollups} />}
  </div>;
}

if (typeof ReactDOM !== "undefined" && document.getElementById("root")) {
  const root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(React.createElement(MarginIQ));
}
