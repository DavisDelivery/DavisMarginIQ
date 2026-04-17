// Davis MarginIQ v2.3 — Cost Intelligence Platform
// Revenue (billed) drives margins. Reconciliation (paid) is separate monitoring.
// Data completeness scanner flags missing weeks.
// v2.3: Multi-source ingest (Uline + NuVizz + Time Clock + Payroll + QBO)

const { useState, useEffect, useCallback, useRef, useMemo } = React;
const APP_VERSION = "2.3.0";

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
function parseDateMDYFlexible(s) {
  if (!s) return null;
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
function parseTimeClock(rows) {
  const entries = [];
  for (const r of rows) {
    const name = r["employee"] || r["employee name"] || r["driver"] || r["driver name"] || r["name"];
    if (!name) continue;
    const dateRaw = r["date"] || r["punch date"] || r["work date"] || r["day"];
    const date = parseDateMDYFlexible(dateRaw) || (dateRaw ? parseDateMDY(dateRaw) : null);
    const clockIn = r["clock in"] || r["punch in"] || r["time in"] || r["in"] || r["start time"];
    const clockOut = r["clock out"] || r["punch out"] || r["time out"] || r["out"] || r["end time"];
    const hoursRaw = r["hours"] || r["total hours"] || r["worked"] || r["duration"];
    const hours = parseHours(hoursRaw);
    entries.push({
      employee: normalizeName(name),
      date,
      week_ending: date ? weekEndingFriday(date) : null,
      month: date ? dateToMonth(date) : null,
      clock_in: clockIn ? String(clockIn).trim() : null,
      clock_out: clockOut ? String(clockOut).trim() : null,
      hours,
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
        total_hours: 0, days_worked: 0,
        unique_employees: new Set(),
        employees: {},
      };
    }
    const bw = byWeek[w];
    bw.total_hours += e.hours || 0;
    bw.days_worked++;
    bw.unique_employees.add(e.employee);
    if (!bw.employees[e.employee]) bw.employees[e.employee] = { hours:0, days:0 };
    bw.employees[e.employee].hours += e.hours || 0;
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
          <li><strong>Uline</strong> (master / originals / accessorials / DDIS) → weekly revenue + reconciliation</li>
          <li><strong>NuVizz</strong> (driver stops export) → weekly driver rollups + 1099 contractor pay (40% of SealNbr)</li>
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

// ═══ RECONCILIATION (separate from revenue) ═══════════════════
function Reconciliation({ reconWeekly, weeklyRollups }) {
  const [unpaid, setUnpaid] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("weekly");
  const [searchFilter, setSearchFilter] = useState("");

  useEffect(() => {
    (async () => {
      setLoading(true);
      const u = await FS.getUnpaidStops(500);
      setUnpaid(u);
      setLoading(false);
    })();
  }, []);

  // Only show reconciliation for weeks that HAVE any paid data
  // This avoids false alarms on weeks with no DDIS yet
  const reconWithPayments = reconWeekly.filter(r => r.paid_matched > 0);
  const reconWeeks = reconWithPayments.sort((a,b) => a.week_ending.localeCompare(b.week_ending));

  const totalBilled = reconWithPayments.reduce((s,r) => s + (r.billed||0), 0);
  const totalPaid = reconWithPayments.reduce((s,r) => s + (r.paid_matched||0), 0);
  const overallCollectionRate = totalBilled > 0 ? (totalPaid / totalBilled * 100) : 0;
  const totalUnpaidAmount = reconWithPayments.reduce((s,r) => s + (r.unpaid_amount||0), 0);
  const totalUnpaidCount = reconWithPayments.reduce((s,r) => s + (r.unpaid_count||0), 0);

  const reconStart = reconWeeks[0]?.week_ending;
  const reconEnd = reconWeeks[reconWeeks.length-1]?.week_ending;

  // By customer
  const byCustomer = {};
  for (const u of unpaid) {
    const c = u.customer || "Unknown";
    if (!byCustomer[c]) byCustomer[c] = { customer:c, count:0, amount:0 };
    byCustomer[c].count++;
    byCustomer[c].amount += u.billed || 0;
  }
  const topUnpaidCustomers = Object.values(byCustomer).sort((a,b) => b.amount-a.amount).slice(0,15);

  const filteredUnpaid = unpaid.filter(u => {
    if (!searchFilter) return true;
    const q = searchFilter.toLowerCase();
    return (u.customer||"").toLowerCase().includes(q) || (u.city||"").toLowerCase().includes(q) || (u.pro||"").toLowerCase().includes(q);
  });

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="🧾" text="Reconciliation (Going Forward)" right={reconStart?<span style={{fontSize:10,color:T.textDim}}>Window: {weekLabel(reconStart)} → {weekLabel(reconEnd)}</span>:null} />

    {reconWithPayments.length === 0 ? (
      <div style={cardStyle}>
        <EmptyState icon="🧾" title="No Reconciliation Data Yet" sub="Upload DDIS payment files (CSV) in Data Ingest to begin monitoring collections going forward." />
        <div style={{fontSize:12,color:T.textMuted,textAlign:"center",marginTop:12,padding:"0 20px"}}>
          Reconciliation tracks which Uline bills actually got paid. This data only exists for the window where you have DDIS files — it's NOT used for total revenue calculations.
        </div>
      </div>
    ) : (
      <>
        <div style={{...cardStyle, background:T.blueBg, borderColor:T.blue, marginBottom:12}}>
          <div style={{fontSize:12,color:T.blueText,lineHeight:1.5}}>
            <strong>ℹ️ About this tab:</strong> Reconciliation only covers weeks where DDIS payment files have been uploaded. It's a monitoring tool for going forward — total revenue and margins in the other tabs use the full billed amount (source of truth), not paid amount.
          </div>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
          <KPI label="Collection Rate" value={fmtPct(overallCollectionRate)} subColor={overallCollectionRate>=95?T.green:overallCollectionRate>=85?T.yellow:T.red} sub={`${fmtK(totalPaid)} / ${fmtK(totalBilled)}`} />
          <KPI label="Total Leakage" value={fmt(totalBilled-totalPaid)} subColor={T.red} sub={`${reconWeeks.length} weeks tracked`} />
          <KPI label="Unpaid Stops" value={fmtNum(totalUnpaidCount)} subColor={T.red} sub={`${fmt(totalUnpaidAmount)} outstanding`} />
          <KPI label="Avg Weekly Leak" value={fmt(reconWeeks.length>0?(totalBilled-totalPaid)/reconWeeks.length:0)} subColor={T.red} />
        </div>

        <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
          {[["weekly","📅 By Week"],["customers","🏢 By Customer"],["unpaid","📋 Unpaid Queue"]].map(([id,l])=>
            <TabButton key={id} active={view===id} label={l} onClick={()=>setView(id)} />
          )}
        </div>

        {view === "weekly" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Billed vs Paid by Week</div>
            <LineTrend data={reconWeeks} xKey="week_ending" yKey="billed" y2Key="paid_matched" label="Billed" y2Label="Paid" height={240} />
            <div style={{marginTop:16,overflowX:"auto",maxHeight:500}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["Week Ending","Billed","Paid","Gap","Collect %","Unpaid #","Unpaid $"].map(h=>
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
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{r.unpaid_count||0}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{r.unpaid_amount?fmt(r.unpaid_amount):"—"}</td>
                    </tr>;
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === "customers" && (
          <div style={cardStyle}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Top Customers With Unpaid Stops</div>
            <BarChart data={topUnpaidCustomers} labelKey="customer" valueKey="amount" color={T.red} maxBars={15} />
            <div style={{marginTop:16,overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead><tr>{["Customer","Unpaid Stops","Total Unpaid","Avg/Stop"].map(h=>
                  <th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>
                )}</tr></thead>
                <tbody>
                  {topUnpaidCustomers.map((c,i) => (
                    <tr key={i}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{c.count}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:700}}>{fmt(c.amount)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(c.amount/c.count)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === "unpaid" && (
          <div style={cardStyle}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12,flexWrap:"wrap",gap:8}}>
              <div style={{fontSize:13,fontWeight:700}}>Unpaid Queue ({filteredUnpaid.length})</div>
              <input type="text" placeholder="Search PRO, customer, city..." value={searchFilter} onChange={e=>setSearchFilter(e.target.value)} style={{...inputStyle,maxWidth:260,fontSize:12}} />
            </div>
            {loading ? <EmptyState icon="⏳" title="Loading..." sub="" /> : (
              <div style={{maxHeight:500,overflowY:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <thead><tr>{["PRO","Customer","City","Pickup","Billed","Code"].map(h=>
                    <th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite,zIndex:1}}>{h}</th>
                  )}</tr></thead>
                  <tbody>
                    {filteredUnpaid.slice(0,500).map((u,i) => (
                      <tr key={i}>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontWeight:600}}>{u.pro}</td>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{u.customer||"—"}</td>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{u.city||"—"}</td>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{u.pu_date||"—"}</td>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:700}}>{fmt(u.billed)}</td>
                        <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted,fontSize:10}}>{u.code||"—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </>
    )}
  </div>;
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

// ═══ SETTINGS ═══════════════════════════════════════════════
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
    { id:"recon", icon:"🧾", label:"Reconciliation" },
    { id:"completeness", icon:"✅", label:"Data Health" },
    { id:"ingest", icon:"📤", label:"Data Ingest" },
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
    {!loading && tab==="recon" && <Reconciliation reconWeekly={reconWeekly} weeklyRollups={weeklyRollups} />}
    {!loading && tab==="completeness" && <DataCompleteness weeklyRollups={weeklyRollups} completeness={completeness} fileLog={fileLog} />}
    {!loading && tab==="ingest" && <DataIngest weeklyRollups={weeklyRollups} reconMeta={reconMeta} onRefresh={refreshData} />}
    {!loading && tab==="costs" && <CostStructure costs={costs} onSave={setCosts} margins={margins} />}
    {!loading && tab==="settings" && <Settings qboConnected={qboConnected} motiveConnected={motiveConnected} reconMeta={reconMeta} weeklyRollups={weeklyRollups} />}
  </div>;
}

if (typeof ReactDOM !== "undefined" && document.getElementById("root")) {
  const root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(React.createElement(MarginIQ));
}
