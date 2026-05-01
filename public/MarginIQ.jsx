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
const APP_VERSION = "2.50.1";

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

// Earliest week we actively track for gaps and alerts. Data before this is
// historical / spotty (pre-MarginIQ era) and shouldn't raise flags.
const TRACKING_START = "2025-01-01";

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
  const ddisPayments = []; // per-PRO payment rows to persist (audit queue needs these)
  const nuvizzStops = [];
  const timeClockEntries = [];
  const payrollEntries = [];
  const qboEntries = [];
  const fileLogs = [];
  const sourceFilesLog = [];
  const countsByKind = { master:0, original:0, accessorials:0, ddis:0, nuvizz:0, timeclock:0, payroll:0, qbo_pl:0, qbo_tb:0, qbo_gl:0, unknown:0 };
  const unknownFiles = [];
  // Track which sources touched each week_ending — used to emit sourceConflicts.
  // weekSources[weekKey] = Set of sources (e.g. "uline", "davis", "manual")
  const weekSources = {};
  // per-file breakdown of weeks touched: [{ filename, source, weeks: [...], kind, service_type }]
  const fileWeekImpact = [];

  for (let i=0; i<files.length; i++) {
    // v2.40.26: yield between files so the UI stays responsive during bulk
    // imports (22 DDIS files × 3K rows each would otherwise block Safari's
    // main thread long enough to trigger the "unresponsive page" prompt)
    if (i > 0) await new Promise(r => setTimeout(r, 0));
    const file = files[i];
    onStatus({ phase:"read", current:i+1, total:files.length, name:file.name });
    const fileId = file.name.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);
    const source = file._source || "manual"; // "uline" | "davis" | "gmail" | "manual"
    const emailFrom = file._emailFrom || null;
    try {
      let rows;
      const isCSV = file.name.toLowerCase().endsWith(".csv");
      if (isCSV) rows = await readCSV(file);
      else rows = await readWorkbook(file);
      if (!rows || rows.length === 0) { unknownFiles.push(file.name + " (empty)"); countsByKind.unknown++; continue; }
      const kind = detectFileType(file.name, rows[0]);
      const group = sourceGroup(kind);
      const serviceType = detectServiceType(file.name, kind);
      countsByKind[kind] = (countsByKind[kind]||0) + 1;
      const logEntry = { file_id: fileId, filename: file.name, kind, group, service_type: serviceType, row_count: rows.length, source, email_from: emailFrom, uploaded_at: new Date().toISOString() };
      fileLogs.push(logEntry);
      sourceFilesLog.push(logEntry);
      if (kind === "master" || kind === "original" || kind === "accessorials") {
        const stops = parseOriginalOrAccessorial(rows, serviceType);
        const weeksTouchedByThisFile = new Set();
        for (const s of stops) {
          if (!s.pro || !s.week_ending) continue;
          weeksTouchedByThisFile.add(s.week_ending);
          // Tag the stop with its source for traceability (keeps if not already set)
          if (!s._source) s._source = source;
          const key = `${s.pro}|${s.service_type}`; // key by PRO + service_type so truckload and delivery don't clobber each other
          const existing = stopsByPro[key];
          if (!existing || (s.new_cost > existing.new_cost)) stopsByPro[key] = s;
        }
        // Record which weeks this specific file contributed to (for conflict report)
        for (const we of weeksTouchedByThisFile) {
          if (!weekSources[we]) weekSources[we] = new Set();
          weekSources[we].add(source);
        }
        fileWeekImpact.push({
          filename: file.name, source, kind, service_type: serviceType,
          weeks: Array.from(weeksTouchedByThisFile).sort(),
          stop_count: stops.length,
        });
      } else if (kind === "ddis") {
        const payments = parseDDIS(rows);
        const billDates = payments.map(p => p.bill_date).filter(Boolean).sort();
        const totalPaid = payments.reduce((s,p) => s + p.paid, 0);
        const wk = computeBillWeekEnding(billDates);
        ddisFileRecords.push({
          file_id: fileId, filename: file.name,
          record_count: payments.length, total_paid: totalPaid,
          earliest_bill_date: billDates[0] || null,
          latest_bill_date: billDates[billDates.length-1] || null,
          bill_week_ending: wk.bill_week_ending,
          week_ambiguous: wk.week_ambiguous,
          ambiguous_candidates: wk.ambiguous_candidates,
          top5_bill_dates: wk.top5,
          covers_weeks: wk.covers_weeks,
          checks: [...new Set(payments.map(p => p.check).filter(Boolean))],
          uploaded_at: new Date().toISOString(),
        });
        for (const p of payments) {
          paymentByPro[p.pro] = (paymentByPro[p.pro] || 0) + p.paid;
          // Persist per-PRO payment so the audit rebuild can do real
          // billed-vs-paid variance matching without re-ingesting files.
          // ID keyed by pro+bill_date+check so the same PRO paid in two
          // different checks writes two distinct rows (merge-safe on dup).
          if (p.pro && p.paid > 0) {
            const payId = `${p.pro}_${p.bill_date || "nodate"}_${p.check || "nocheck"}`;
            ddisPayments.push({
              id: payId, pro: p.pro, paid_amount: p.paid,
              bill_date: p.bill_date || null, check: p.check || null,
              voucher: p.voucher || null, source_file: file.name,
              uploaded_at: new Date().toISOString(),
            });
          }
        }
      } else if (kind === "nuvizz") {
        for (const s of parseNuVizz(rows)) nuvizzStops.push(s);
      } else if (kind === "timeclock") {
        for (const e of parseTimeClock(rows)) timeClockEntries.push(e);
      } else if (kind === "payroll") {
        for (const p of parsePayroll(rows)) payrollEntries.push(p);
      } else if (kind === "qbo_pl" || kind === "qbo_tb" || kind === "qbo_gl") {
        for (const e of parseQBO(rows, kind)) qboEntries.push({...e, source_file: file.name});
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
  // If we have no DDIS payment data yet for the weeks in this batch, audit
  // generation produces meaningless output (every stop looks unpaid). Skip
  // the heavy per-stop audit work when paymentByPro is empty — it massively
  // speeds up first-time ingest of a single weekly file. AuditIQ will
  // populate correctly the next time DDIS files are added via the normal flow.
  const hasPaymentData = Object.keys(paymentByPro).length > 0;
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
          code: s.code, weight: s.weight, order: s.order,
          service_type: s.service_type || "delivery", // v2.40.14: carry service_type for TK filter
        });
      }
    }
    // v2.5 AuditIQ — only generate audit items when we have payment data to
    // compare against. Without DDIS, every stop's variance == billed amount
    // (all "unpaid") which is noise, not a real audit signal.
    if (hasPaymentData && billed > 0 && variance > 1) {
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
          service_type: s.service_type || "delivery", // v2.40.14
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

  onStatus({ phase:"save", message:"Saving weekly rollups..." });
  let savedWeeks = 0, savedRecon = 0, savedAudit = 0;

  // Helper to run Firestore writes in parallel batches (cuts multi-minute
  // mobile hangs down to seconds). 25 concurrent writes works reliably.
  const batchWrite = async (items, writer, batchSize = 25, statusMsg) => {
    let saved = 0;
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      const results = await Promise.all(batch.map(writer));
      saved += results.filter(r => r).length;
      if (statusMsg) onStatus({ phase:"save", message: `${statusMsg} (${Math.min(i+batchSize, items.length)}/${items.length})` });
    }
    return saved;
  };

  savedWeeks = await batchWrite(rollups,
    r => FS.saveWeeklyRollup(r.week_ending, {...r, updated_at:new Date().toISOString()}),
    25, "Saving weekly rollups");
  savedRecon = await batchWrite(Object.values(reconByWeek),
    r => FS.saveReconWeekly(r.week_ending, {...r, updated_at:new Date().toISOString()}),
    25, "Saving reconciliation");
  // v2.40.26: Yield helper — releases the main thread so the UI can repaint.
  // Mobile Safari will mark a tab unresponsive and prompt to close it if the
  // main thread is busy for ~10+ seconds. Inserting these between hot loops
  // keeps the phone responsive even on large DDIS ingests.
  const yieldToUI = () => new Promise(r => setTimeout(r, 0));

  // v2.40.27: Per-operation timeout. Firebase SDK on mobile Safari has a
  // known failure mode where a batch.commit() promise neither resolves nor
  // rejects — the request vanishes into the SDK's internal queue and the
  // await hangs forever. This blocked 3 of Chad's 4 most recent DDIS imports
  // (175/3272, 200/3064, 335/3815 payments landed before each hung). A
  // 30s race unsticks us: if the commit hasn't come back by then, we throw
  // and let the outer catch fall through so importing[refKey] clears and
  // the UI stops showing "..." eternally.
  const withTimeout = (promise, ms, label) => Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);

  await yieldToUI();
  onStatus({ phase:"save", message:`Saving DDIS file records (${ddisFileRecords.length})...` });
  await batchWrite(ddisFileRecords, f => FS.saveDDISFile(f.file_id, f), 25);
  // v2.40.25: REVERTED v2.40.17 server-side dispatch. Root cause: Firestore
  // security rules block REST API writes with just FIREBASE_API_KEY (403
  // PERMISSION_DENIED, verified live). The v2.40.17 dispatch returned 202
  // from the dispatcher but the background function silently failed every
  // write. Net effect: ZERO ddis_payments rows landed for weeks.
  //
  // Client-side writes work fine (Firebase SDK auth). The original mobile
  // stall v2.40.17 tried to solve was caused by 25-at-a-time Promise.all
  // batching. Use db.batch() atomic commits — the same pattern the v2.40.24
  // client-side purge uses, which flies through 2K docs in a couple seconds.
  //
  // v2.40.26: reduced batch size 500→200, added yieldToUI between batches,
  // per-batch try/catch so one bad commit doesn't wedge the whole import.
  // v2.40.27: wrapped every commit in a 30s timeout because partial writes
  // (3 of 4 recent imports hung after 1-2 batches) proved the SDK itself
  // can silently stall on mobile.
  if (ddisPayments.length > 0) {
    const toSave = ddisPayments.slice(0, 10000);
    onStatus({ phase:"save", message:`Saving ${toSave.length} DDIS payments (0 done)...` });
    let written = 0;
    let batchesFailed = 0;
    let batchesTimedOut = 0;
    const BATCH_SIZE = 200;
    for (let i = 0; i < toSave.length; i += BATCH_SIZE) {
      await yieldToUI();
      const chunk = toSave.slice(i, i + BATCH_SIZE);
      try {
        const batch = window.db.batch();
        for (const p of chunk) {
          batch.set(window.db.collection("ddis_payments").doc(String(p.id)), p, { merge: true });
        }
        await withTimeout(batch.commit(), 30000, `DDIS batch ${i}-${i+chunk.length}`);
        written += chunk.length;
        onStatus({ phase:"save", message:`Saving DDIS payments (${written}/${toSave.length})...` });
      } catch(e) {
        const isTimeout = /timed out/i.test(e.message || "");
        console.error(`DDIS batch ${i}-${i+BATCH_SIZE} ${isTimeout?"TIMED OUT":"failed"}:`, e);
        if (isTimeout) batchesTimedOut++; else batchesFailed++;
        // After a timeout, the SDK's internal write queue may still be
        // processing. Give it a short breather before trying the next batch
        // instead of piling on another 200 ops. Moving to one-by-one writes
        // for THIS chunk ONLY — with per-write timeout so one stuck doc
        // can't hang another 200 seconds.
        onStatus({ phase:"save", message:`Batch ${i}-${i+BATCH_SIZE} stalled, recovering...` });
        for (const p of chunk) {
          try {
            await withTimeout(
              window.db.collection("ddis_payments").doc(String(p.id)).set(p, { merge: true }),
              5000,
              `set ${p.id}`,
            );
            written++;
          } catch(_) { /* skip bad/stuck row, keep going */ }
        }
        onStatus({ phase:"save", message:`Recovered from stall (${written}/${toSave.length})...` });
      }
    }
    if (batchesFailed > 0 || batchesTimedOut > 0) {
      console.warn(`DDIS ingest: ${batchesFailed} failed, ${batchesTimedOut} timed out out of ${Math.ceil(toSave.length/BATCH_SIZE)} batches`);
    }
    // v2.40.27: Write file_log for successfully imported DDIS files HERE,
    // not at the bottom of ingestFiles. If something downstream hangs (NuVizz,
    // audit build, etc.), the dedup state still reflects that the DDIS file
    // IS imported, so Chad isn't tricked into re-importing it next session.
    if (written > 0 && ddisFileRecords.length > 0) {
      try {
        for (const rec of ddisFileRecords) {
          const matchingLog = fileLogs.find(l => l.filename === rec.filename);
          if (matchingLog) {
            await withTimeout(
              FS.saveFileLog(matchingLog.file_id, { ...matchingLog, payments_written: written, total_payments: toSave.length }),
              10000,
              `file_log ${matchingLog.filename}`,
            );
          }
        }
      } catch(e) {
        console.warn("Early file_log write failed (will retry at end):", e.message);
      }
    }
  }
  // Without DDIS payment data, every stop looks unpaid — cap hard to avoid
  // writing 500 meaningless rows. With payment data, keep full 500 for audit.
  const topUnpaid = unpaidStops.sort((a,b) => b.billed - a.billed).slice(0, hasPaymentData ? 500 : 100);
  if (topUnpaid.length > 0) {
    onStatus({ phase:"save", message:`Saving unpaid stops...` });
    await batchWrite(topUnpaid, s => FS.saveUnpaidStop(s.pro, s), 25, "Saving unpaid stops");
  }
  // v2.5 save audit_items, top 1500 by variance
  const topAudit = auditItems.sort((a,b) => b.variance - a.variance).slice(0, 1500);
  if (topAudit.length > 0) {
    onStatus({ phase:"save", message:`Saving audit items...` });
    savedAudit = await batchWrite(topAudit, a => FS.saveAuditItem(a.pro, a), 25, "Saving audit items");
  }
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
  const customerContactEntries = Array.from(uniqueCustomers.entries());
  if (customerContactEntries.length > 0) {
    await batchWrite(customerContactEntries,
      ([key, stats]) => FS.saveAPContact(key, {
        customer: stats.customer,
        customer_key: key,
        total_owed_cached: stats.total_owed,
        item_count_cached: stats.item_count,
        billing_email: "",
        ap_contact_name: "",
        ap_contact_phone: "",
        dispute_portal_url: "",
      }),
      25);
  }

  // NuVizz
  // v2.40.34: NuVizz saves now happen server-side via marginiq-nuvizz-ingest.
  // The client-side ingest was dropping ~73% of stops (mobile Safari timeouts,
  // Firebase SDK hangs). Server function writes 500-doc batches via :commit
  // REST endpoint with 15-min budget — no drop.
  let nvWeeksSaved = 0, nvStopsSaved = 0;
  if (nuvizzStops.length > 0) {
    try {
      const { run_id, saved_ok, status } = await serverSaveNuVizzStops(
        nuvizzStops, "gmail_ingest", onStatus
      );
      nvStopsSaved = saved_ok || 0;
      nvWeeksSaved = status?.weeks_rebuilt || 0;
    } catch (e) {
      onStatus({ phase:"save", message:`✗ NuVizz server ingest failed: ${e.message}` });
    }
  }

  // Time Clock

  let tcWeeksSaved = 0, tcDaysSaved = 0;
  if (timeClockEntries.length > 0) {
    const tcWeekly = buildTimeClockWeekly(timeClockEntries);
    tcWeeksSaved = await batchWrite(tcWeekly,
      w => FS.saveTimeClockWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()}),
      25);
    // v2.47.0: also persist per-shift entries to timeclock_daily for the
    // clock-in→first-stop forensics on DriverPerformanceTab.
    const tcDaily = buildTimeClockDaily(timeClockEntries);
    tcDaysSaved = await batchWrite(tcDaily,
      d => {
        const { doc_id, ...fields } = d;
        return FS.saveTimeClockDaily(doc_id, {...fields, updated_at:new Date().toISOString()});
      },
      25);
  }

  // Payroll
  let payWeeksSaved = 0;
  if (payrollEntries.length > 0) {
    const payWeekly = buildPayrollWeekly(payrollEntries);
    payWeeksSaved = await batchWrite(payWeekly,
      w => FS.savePayrollWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()}),
      25);
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
    const periodEntries = Object.entries(byPeriod);
    qboPeriodsSaved = await batchWrite(periodEntries,
      ([pid, data]) => FS.saveQBOHistory(pid, {...data, uploaded_at: new Date().toISOString()}),
      25);
  }

  // File logs
  if (fileLogs.length > 0) {
    await batchWrite(fileLogs, l => FS.saveFileLog(l.file_id, l), 25);
  }
  if (sourceFilesLog.length > 0) {
    await batchWrite(sourceFilesLog, sf => FS.saveSourceFile(sf.file_id, sf), 25);
  }

  const existingMeta = await FS.getReconMeta() || {};
  await FS.saveReconMeta({
    files_count: (existingMeta.files_count || 0) + ddisFileRecords.length,
    last_upload: new Date().toISOString(),
    total_stops_processed: (existingMeta.total_stops_processed || 0) + allStops.length,
  });

  // Compute source conflicts: any week where files from more than one source
  // contributed data during this ingest batch. Gives Chad a review list.
  // For each conflict we compute per-source aggregates (stops, revenue, acc)
  // so the resolver UI can show a side-by-side comparison.
  const sourceConflicts = [];

  // Index stops by (week, source) to compute per-source aggregates
  const byWeekSource = {};  // `${we}|${src}` → { stops: [], delivery_stops, truckload_stops, accessorial_stops, revenue, acc_revenue, delivery_revenue, truckload_revenue }
  for (const s of allStops) {
    const src = s._source || "manual";
    const key = `${s.week_ending}|${src}`;
    if (!byWeekSource[key]) byWeekSource[key] = {
      stop_count: 0, delivery_stops: 0, truckload_stops: 0, accessorial_stops: 0,
      revenue: 0, delivery_revenue: 0, truckload_revenue: 0, accessorial_revenue: 0,
    };
    const b = byWeekSource[key];
    const nc = s.new_cost || 0;
    b.stop_count++;
    b.revenue += nc;
    if (s.service_type === "truckload") { b.truckload_stops++; b.truckload_revenue += nc; }
    else if (s.service_type === "accessorial") { b.accessorial_stops++; b.accessorial_revenue += nc; }
    else { b.delivery_stops++; b.delivery_revenue += nc; }
  }

  for (const [we, sources] of Object.entries(weekSources)) {
    if (sources.size > 1) {
      const filesForWeek = fileWeekImpact
        .filter(f => f.weeks.includes(we))
        .map(f => ({ filename: f.filename, source: f.source, kind: f.kind, service_type: f.service_type, stop_count: f.stop_count }));
      // Per-source aggregates for this week
      const summaries = {};
      for (const src of sources) {
        summaries[src] = byWeekSource[`${we}|${src}`] || {
          stop_count: 0, delivery_stops: 0, truckload_stops: 0, accessorial_stops: 0,
          revenue: 0, delivery_revenue: 0, truckload_revenue: 0, accessorial_revenue: 0,
        };
      }
      sourceConflicts.push({
        week_ending: we,
        sources: Array.from(sources),
        files: filesForWeek,
        summaries,
      });
    }
  }
  sourceConflicts.sort((a, b) => a.week_ending.localeCompare(b.week_ending));

  // Persist unresolved conflicts to Firestore so they survive page refresh
  // and the user can review them on the Data Ingest tab even after closing
  // the result modal.
  for (const c of sourceConflicts) {
    await FS.saveSourceConflict(c.week_ending, {
      week_ending: c.week_ending,
      sources: c.sources,
      files: c.files,
      summaries: c.summaries,
      detected_at: new Date().toISOString(),
      resolved: null,         // null | "davis" | "uline" | "merge"
      resolved_at: null,
      resolved_by: null,
    });
  }

  return {
    files_processed: files.length,
    counts: countsByKind,
    uline: { stops: allStops.length, weeks_saved: savedWeeks, recon_saved: savedRecon, unpaid_saved: topUnpaid.length, audit_saved: savedAudit, payments: Object.keys(paymentByPro).length },
    nuvizz: { stops: nuvizzStops.length, weeks_saved: nvWeeksSaved, stops_saved: nvStopsSaved },
    timeclock: { entries: timeClockEntries.length, weeks_saved: tcWeeksSaved, days_saved: tcDaysSaved },
    payroll: { entries: payrollEntries.length, weeks_saved: payWeeksSaved },
    qbo: { lines: qboEntries.length, periods_saved: qboPeriodsSaved },
    unknown: unknownFiles,
    source_conflicts: sourceConflicts,
  };
}

// ═══ GMAIL SYNC — Auto-import weekly reports from inbox ═════
// ═══ FUEL — Per-Vendor Spend & Rate Tracking (v2.6) ════════
// Two sources: FuelFox + Quick Fuel. Weekly rollups by vendor so you can
// compare which vendor is actually cheaper per gallon when you include
// all fees/taxes/delivery. True rate = (fuel + taxes + delivery) / gallons.
const FUEL_VENDORS = [
  { key: "fuelfox", label: "FuelFox", color: "#dc2626", supported: true },
  { key: "quickfuel", label: "Quick Fuel", color: "#2563eb", supported: true },
];

function Fuel() {
  const [view, setView] = useState("weekly"); // weekly | invoices | upload | trucks
  const [loading, setLoading] = useState(true);
  const [invoices, setInvoices] = useState([]);
  const [weekly, setWeekly] = useState([]);
  const [byTruck, setByTruck] = useState([]);
  const [refreshTick, setRefreshTick] = useState(0);
  const invSort = useSortable(invoices, "invoice_date", "desc");
  const trSort = useSortable(byTruck, "service_date", "desc");

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
            {/* v2.40.33: sortable invoices table */}
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead>
                <tr style={{background:T.bgSurface,position:"sticky",top:0}}>
                  {[["Date","invoice_date"],["Vendor","vendor"],["Invoice #","invoice_number"],["Gallons","total_gallons"],["Fuel","fuel_cost"],["Tax","tax"],["Delivery","delivery_fee"],["Grand Total","grand_total"],["True $/Gal","true_rate"],["Trucks","truck_count"]].map(([label,key]) => (
                    <SortableTh key={key} label={label} col={key} sortKey={invSort.sortKey} sortDir={invSort.sortDir} onSort={invSort.toggleSort}
                      style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:invSort.sortKey===key?T.brand:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",background:T.bgSurface}} />
                  ))}
                </tr>
              </thead>
              <tbody>
                {invSort.sorted.map(inv => {
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
          ); })()}
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
            {/* v2.40.33: sortable by-truck table */}
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead>
                <tr style={{background:T.bgSurface,position:"sticky",top:0}}>
                  {[["Service Date","service_date"],["Unit","unit"],["Gallons","gallons"],["Posted $","posted_charge"],["True $","true_cost"],["Uplift","uplift"],["Vendor","vendor"],["Invoice","invoice_number"]].map(([label,key]) => (
                    <SortableTh key={key} label={label} col={key} sortKey={trSort.sortKey} sortDir={trSort.sortDir} onSort={trSort.toggleSort}
                      style={{textAlign:"left",padding:"6px 10px",borderBottom:`1px solid ${T.border}`,color:trSort.sortKey===key?T.brand:T.textDim,fontSize:9,fontWeight:600,textTransform:"uppercase",background:T.bgSurface}} />
                  ))}
                </tr>
              </thead>
              <tbody>
                {trSort.sorted.slice(0, 500).map(t => {
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
          ); })()}
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

// Lazy-load jsPDF from CDN for client-side PDF generation. pdf.js above is
// read-only; jsPDF handles drawing new PDFs (used by the Run Sheet tab). Only
// loaded on first use to keep the initial page bundle light.
async function loadJsPdf() {
  if (window.jspdf && window.jspdf.jsPDF) return window.jspdf.jsPDF;
  if (window.jsPDF) return window.jsPDF;
  await new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js";
    s.onload = resolve;
    s.onerror = reject;
    document.head.appendChild(s);
  });
  return window.jspdf.jsPDF;
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

v2.40.29 — Three-section extraction: truck recap (backward compat), card→driver map, transaction-level detail rows.

RULES FOR QUICK FUEL / FLYERS ENERGY:
- Vendor is "Quick Fuel". Invoice number starts with "CFS-" (e.g. "CFS-4582698").

SECTION 1 — "Recap by Additional Info 2" (truck-level rollup, REQUIRED):
- Each row: [Truck#] [Units] [Amount] [Taxes] [Total]. Truck "0" means unassigned — use truckId "INVENTORY".
- Emit ONE object per row into rows[]:
  - truckId: the truck # (or "INVENTORY"), preserve leading zeros as string
  - gallons: Units column
  - total: Total column (INCLUDES tax)
  - pricePerGallon: total / gallons, rounded to 4 decimals
  - invoiceNum: "<base>-<truckId>" e.g. "CFS-4582698-0294"
  - date: invoice date YYYY-MM-DD
  - notes: "Weekly fuel card - Truck X" or "Unassigned fuel"

SECTION 2 — "Recap by Card" (card→driver map, REQUIRED):
- Each row looks like: "1194374 - STEVIE ROBINSON 144.52 736.64 36.05 772.69"
- Emit into card_drivers[] as { card: "1194374", driver: "STEVIE ROBINSON" }.
- Strip whitespace, preserve full driver name as shown (all caps is fine).

SECTION 3 — Transaction detail rows (each card swipe, REQUIRED):
- Found at the top of the invoice BEFORE the recap sections. Grouped by truck# (the truck subheaders like "1478", "1606" etc).
- Each row format (columns): [MM/DD/YY H:MMa/p] [Card#] [Site: "City, ST - SiteNum"] [Product] [Veh] [Manual] [Odometer] [MPG] [Units] [UnitPrice] [Amount]
- Emit ONE object per row into transactions[]:
  - date: "YYYY-MM-DD" (convert MM/DD/YY)
  - time: "HH:MM" 24-hour (convert from 12-hour)
  - card: card number string (e.g. "5493550")
  - site_city: e.g. "Hoschton, GA"
  - site_number: e.g. "756498"
  - product_raw: exactly as shown, e.g. "ULSD #2", "DEF", "REG CONV GAS", "PREM CONV GAS", "MID CONV GAS"
  - product_category: classify as EXACTLY one of: "diesel" (ULSD #2 or any ULSD), "def" (DEF), "gas" (any CONV GAS)
  - truck_id: the parent truck grouping (from subheader row like "1478"). For rows under the "0" group, use "INVENTORY".
  - odometer: number (strip commas)
  - gallons: number
  - unit_price: number (posted/rack, before invoice-level tax)
  - amount: number (gallons × unit_price)
- INCLUDE EVERY detail row. A typical invoice has 40-100+ rows. DO NOT skip, merge, or summarize.
- If a row's card# doesn't appear in card_drivers, driver will be resolved client-side as "Unknown".

SECTION 4 — Invoice fees:
- "Invoice Fees Total" section (e.g. "Regulatory Compliance Fee"). Sum these into invoice_fees (number, 0 if absent).

SECTION 5 — Grand total:
- invoice_total = the amount at "Invoice Total" / balance due at bottom.

Return ONLY JSON (no markdown, no prose):
{
  "vendor": "Quick Fuel",
  "invoice_number": "CFS-xxx",
  "invoice_date": "YYYY-MM-DD",
  "invoice_total": N,
  "invoice_fees": N,
  "rows": [...],
  "card_drivers": [{"card":"xxx","driver":"NAME"}, ...],
  "transactions": [...]
}`;

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


// ─── Attachment dedup key (v2.40.3, revised v2.40.6 + v2.40.7) ────────
// Uline DAS filenames carry a date range + a variant. Uline sends three
// kinds of files per week:
//   - main delivery billing   ~250-280 KB  "das YYYYMMDD-YYYYMMDD.xlsx"
//   - accessorials companion    10-20 KB  "das YYYYMMDD-YYYYMMDD accessorials.xlsx"
//   - truckload (TK) report      5-15 KB  "das YYYYMMDD-YYYYMMDD TK.xlsx"
// Each of these is a distinct document; treating them as one corrupts the
// dedup and blocks legitimate imports.
//
// v2.40.7 adjustments vs v2.40.3:
//   - Added third variant "truckload" for TK / TL / truckload tags
//   - Accessorials detection tolerates the common typo "accessiorials"
//     (extra 'i' between the s and the o)
//
// Re-forwarded/re-saved copies with reviewer-initial tags (-JO, -KP, etc.)
// still fall into the "main" bucket, which is correct — they're the same
// billing document.
//
// v2.40.6 date-format handling:
//   Uline also sends the same week under two date formats:
//     YYYYMMDD-YYYYMMDD  (ISO)
//     MMDDYYYY-MMDDYYYY  (US)
//   toCanonicalDate() normalizes both to YYYYMMDD so they collide correctly.
//
// Examples (v2.40.7 final):
//   das 20260216-20260220.xlsx                   → das_20260216_20260220_main
//   das 02162026-02202026.xlsx                   → das_20260216_20260220_main
//   das 20260216-20260220-JO.xlsx                → das_20260216_20260220_main
//   das 20260216-20260220 accessorials.xlsx      → das_20260216_20260220_accessorials
//   das 20260216-20260220 accessiorials.xlsx     → das_20260216_20260220_accessorials (typo)
//   das 20260216-20260220 TK.xlsx                → das_20260216_20260220_truckload
//   das 20260216-20260220-TK.xlsx                → das_20260216_20260220_truckload
function toCanonicalDate(s) {
  if (!s || s.length !== 8 || !/^\d{8}$/.test(s)) return null;
  const y1 = parseInt(s.slice(0, 4), 10);
  const m1 = parseInt(s.slice(4, 6), 10);
  const d1 = parseInt(s.slice(6, 8), 10);
  if (y1 >= 2000 && y1 <= 2099 && m1 >= 1 && m1 <= 12 && d1 >= 1 && d1 <= 31) {
    return s;
  }
  const m2 = parseInt(s.slice(0, 2), 10);
  const d2 = parseInt(s.slice(2, 4), 10);
  const y2 = parseInt(s.slice(4, 8), 10);
  if (y2 >= 2000 && y2 <= 2099 && m2 >= 1 && m2 <= 12 && d2 >= 1 && d2 <= 31) {
    return String(y2) + String(m2).padStart(2, "0") + String(d2).padStart(2, "0");
  }
  return null;
}
function classifyDasVariant(rest) {
  const s = (rest || "").toLowerCase();
  // Accessorials — accepts "accessorial", "accessorials", and the common
  // typo "accessiorial(s)" with an extra 'i'. The regex allows either 'o',
  // 'i', or both between 'access' and 'rial'.
  if (/access[io]+ri?als?/.test(s)) return "accessorials";
  // Truckload — TK, TL, or "truckload" as a bounded tag. Word boundary on
  // both sides so it can't false-match inside some longer word.
  if (/\b(tk|tl|truckload)\b/.test(s)) return "truckload";
  // Everything else is the main delivery file. This bucket also absorbs
  // reviewer-initial tags like -JO, double-extension artifacts, etc.
  return "main";
}
function parseDasFilename(filename) {
  const fn = (filename || "").toLowerCase().trim();
  const m = fn.match(/^das[\s_-]*(\d{8})[\s_-]+(\d{8})(.*)\.xlsx?$/);
  if (!m) return { ok: false };
  const [, raw1, raw2, rest] = m;
  const start = toCanonicalDate(raw1);
  const end = toCanonicalDate(raw2);
  if (!start || !end) return { ok: false };
  const variant = classifyDasVariant(rest);
  return { ok: true, start, end, variant, key: `das_${start}_${end}_${variant}` };
}
function dedupKey(filename) {
  const p = parseDasFilename(filename);
  return p.ok ? p.key : (filename || "").toLowerCase();
}

function GmailSync({ onRefresh }) {
  // v2.40: multi-account Gmail. `gmailAccounts` is the array of all connected
  // inboxes (legacy singleton counts as one). `gmailConn` kept as a derived
  // alias of the first account so the existing render paths stay simple.
  const [gmailAccounts, setGmailAccounts] = useState([]);
  const gmailConn = gmailAccounts.length > 0 ? gmailAccounts[0] : null;
  const [loadingConn, setLoadingConn] = useState(true);
  const [results, setResults] = useState({}); // vendor -> emails array
  const [loading, setLoading] = useState({}); // vendor -> bool
  const [importing, setImporting] = useState({}); // emailId:attachmentId -> bool
  const [imported, setImported] = useState({}); // emailId:attachmentId -> result summary
  const [importStatus, setImportStatus] = useState("");
  const [oauthMsg, setOauthMsg] = useState(null); // { kind: 'success'|'error', text }

  // Import All state — tracks when a bulk import is running per vendor
  const [bulkImporting, setBulkImporting] = useState({});     // vendor -> { current, total, skipped, failed, running }
  const [alreadyImportedFilenames, setAlreadyImportedFilenames] = useState(new Set());
  // v2.40.11: per-vendor toggle to hide already-imported emails from the list.
  // Useful after pagination fix when DDIS search returns 100+ results, most
  // already ingested — flipping this to show just the gaps makes them obvious.
  const [showMissingOnly, setShowMissingOnly] = useState({});  // vendor -> bool

  // Date range controls — shared across all vendor searches.
  // rangePreset values: "30d" | "60d" | "90d" | "6mo" | "12mo" | "ytd" | "all" | "custom"
  const [rangePreset, setRangePreset] = useState("60d");
  const [customFrom, setCustomFrom] = useState(""); // YYYY-MM-DD
  const [customTo, setCustomTo] = useState("");     // YYYY-MM-DD

  // Load the filenames we've already ingested so Import All can skip them.
  // Loaded once on mount + refreshed after each bulk import.
  const loadImportedFilenames = async () => {
    if (!hasFirebase) return;
    try {
      const log = await FS.getFileLog(2000);
      const set = new Set();
      for (const l of log) {
        if (l.filename) set.add(dedupKey(l.filename));
      }
      setAlreadyImportedFilenames(set);
    } catch(e) { console.error("loadImportedFilenames err:", e); }
  };
  useEffect(() => { loadImportedFilenames(); }, []);

  // Load Gmail connection state
  useEffect(() => {
    // Handle OAuth callback redirect (?gmail=connected OR ?gmail=error)
    const params = new URLSearchParams(window.location.search);
    const gmailParam = params.get("gmail");
    if (gmailParam === "connected") {
      const email = params.get("email") || "unknown";
      setOauthMsg({ kind: "success", text: `Gmail connected as ${email}` });
      // Strip params so refreshes don't re-show the toast
      window.history.replaceState({}, "", window.location.pathname);
    } else if (gmailParam === "error") {
      const reason = params.get("reason") || "unknown";
      const detail = params.get("detail") || "";
      setOauthMsg({ kind: "error", text: `Gmail OAuth failed: ${reason}${detail ? " — " + detail : ""}` });
      window.history.replaceState({}, "", window.location.pathname);
    }

    (async () => {
      if (!hasFirebase) { setLoadingConn(false); return; }
      try {
        // v2.40: scan marginiq_config for all gmail_tokens* docs. Per-account
        // doc (gmail_tokens_{slug}) wins over legacy singleton when both exist
        // for the same email.
        const snap = await window.db.collection("marginiq_config").get();
        const byEmail = {};
        snap.forEach((d) => {
          const id = d.id;
          if (id !== "gmail_tokens" && !id.startsWith("gmail_tokens_")) return;
          const data = d.data() || {};
          if (!data.refresh_token && !data.email) return; // skip empty shells
          const email = data.email || "unknown";
          const entry = { docId: id, email, connected_at: data.connected_at };
          const existing = byEmail[email];
          if (!existing || (existing.docId === "gmail_tokens" && id !== "gmail_tokens")) {
            byEmail[email] = entry;
          }
        });
        setGmailAccounts(Object.values(byEmail));
      } catch(e) {
        setOauthMsg({ kind: "error", text: "Could not read Gmail tokens from Firestore: " + e.message });
      }
      setLoadingConn(false);
    })();
  }, []);

  const disconnect = async (docId = "gmail_tokens", email = "") => {
    const who = email || "this Gmail account";
    if (!confirm(`Disconnect ${who}? You'll need to reconnect to pull new reports from it.`)) return;
    try { await window.db.collection("marginiq_config").doc(docId).delete(); } catch(e) {}
    setGmailAccounts(prev => prev.filter(a => a.docId !== docId));
    setResults({});
  };

  // Convert current rangePreset + customFrom/customTo into {afterDate, beforeDate}
  // that Gmail query understands (YYYY/MM/DD, slashes). Returns empty strings
  // for "no bound" (used by "all time" and when user leaves custom fields blank).
  const resolveDateRange = () => {
    const toGmail = (d) => `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()}`;
    const isoToGmail = (iso) => {
      if (!iso) return "";
      const [y,m,day] = iso.split("-").map(Number);
      return `${y}/${m}/${day}`;
    };
    const today = new Date();
    if (rangePreset === "custom") {
      return { afterDate: isoToGmail(customFrom), beforeDate: isoToGmail(customTo) };
    }
    if (rangePreset === "all") return { afterDate: "", beforeDate: "" };
    if (rangePreset === "ytd") {
      return { afterDate: `${today.getFullYear()}/1/1`, beforeDate: "" };
    }
    const d = new Date();
    const daysMap = { "30d": 30, "60d": 60, "90d": 90 };
    const monthsMap = { "6mo": 6, "12mo": 12 };
    if (daysMap[rangePreset]) d.setDate(d.getDate() - daysMap[rangePreset]);
    else if (monthsMap[rangePreset]) d.setMonth(d.getMonth() - monthsMap[rangePreset]);
    return { afterDate: toGmail(d), beforeDate: "" };
  };

  // Human-readable label for the current range (shown in UI)
  const rangeLabel = () => {
    if (rangePreset === "custom") {
      if (customFrom && customTo) return `${customFrom} → ${customTo}`;
      if (customFrom) return `${customFrom} → now`;
      if (customTo) return `all time → ${customTo}`;
      return "Custom (no dates set)";
    }
    const labels = { "30d":"Last 30 days","60d":"Last 60 days","90d":"Last 90 days","6mo":"Last 6 months","12mo":"Last 12 months","ytd":"Year to date","all":"All time" };
    return labels[rangePreset] || rangePreset;
  };

  const searchVendor = async (vendor, accountFilter = "") => {
    setLoading(prev => ({...prev, [vendor]: true}));
    try {
      const { afterDate, beforeDate } = resolveDateRange();
      // v2.40.11: longer ranges need more headroom. DDIS alone has 100+
      // weekly files over 2+ years. Server now pages Gmail API and caps at
      // 1000 so we can ask for the high end without being clipped.
      const maxResults = rangePreset === "all" ? 1000
        : (rangePreset === "12mo" || rangePreset === "custom" || rangePreset === "6mo") ? 500
        : 100;
      // v2.40.1: accountFilter pins the search to a single connected inbox
      // (useful for "Billing@ Outbox" which should only show billing@'s sent).
      const body = { vendor, afterDate, beforeDate, maxResults };
      if (accountFilter) body.account_email = accountFilter;
      const resp = await fetch("/.netlify/functions/marginiq-gmail-search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
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

  // Shared helper — download a Gmail attachment with retry + clearer error surfacing.
  // Common failure modes on mobile: Netlify cold-start timeout, 5G flakiness,
  // brief Gmail API hiccup. A single retry with a short backoff clears most.
  const downloadGmailAttachment = async (email, attachmentId, filenameForLogs) => {
    const MAX_ATTEMPTS = 3;
    let lastErr = null;
    const emailId = email?.emailId || email; // backward-compat if a plain id is passed
    const accountDocId = email?.account_doc_id || "";
    const accountEmail = email?.account_email || "";
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        // AbortController with 45s timeout — long enough for Netlify function
        // cold-start + Gmail fetch, short enough that user sees a real error
        // rather than waiting 3+ minutes on a hung request.
        const ctrl = new AbortController();
        const timeoutId = setTimeout(() => ctrl.abort(), 45000);
        const resp = await fetch("/.netlify/functions/marginiq-gmail-attachment", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messageId: emailId,
            attachmentId,
            account_doc_id: accountDocId,
            account_email: accountEmail,
          }),
          signal: ctrl.signal,
        });
        clearTimeout(timeoutId);
        if (!resp.ok) {
          // Got HTTP error — try to read body for useful message
          let bodyTxt = "";
          try { bodyTxt = await resp.text(); } catch {}
          throw new Error(`HTTP ${resp.status}${bodyTxt ? ": " + bodyTxt.substring(0,120) : ""}`);
        }
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        if (!data.data) throw new Error("Empty response");
        return data;
      } catch (e) {
        lastErr = e;
        const isAbort = e.name === "AbortError";
        console.warn(`Attachment DL attempt ${attempt}/${MAX_ATTEMPTS} failed for ${filenameForLogs}:`, isAbort ? "timeout" : e.message);
        if (attempt < MAX_ATTEMPTS) {
          await new Promise(r => setTimeout(r, 1500 * attempt)); // 1.5s, 3s backoff
        }
      }
    }
    throw new Error(`Download failed after ${MAX_ATTEMPTS} tries: ${lastErr?.message || "unknown"}`);
  };

  const importAttachment = async (email, attachment) => {
    const refKey = `${email.emailId}:${attachment.attachmentId}`;
    setImporting(prev => ({...prev, [refKey]: true}));
    setImportStatus(`Downloading ${attachment.filename}...`);
    try {
      // 1. Download attachment bytes (with retry)
      const dlData = await downloadGmailAttachment(email, attachment.attachmentId, attachment.filename);

      // 2. Base64 → File
      const binary = atob(dlData.data);
      const bytes = new Uint8Array(binary.length);
      for (let i=0; i<binary.length; i++) bytes[i] = binary.charCodeAt(i);
      const mimeType = attachment.mimeType || (attachment.filename.endsWith(".csv") ? "text/csv" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      const blob = new Blob([bytes], { type: mimeType });
      const file = new File([blob], attachment.filename, { type: mimeType });

      // Tag source based on email sender — downstream ingest reads file._source
      // to detect conflicting Davis-corrected vs Uline-original files.
      const fromLower = (email.from || "").toLowerCase();
      if (fromLower.includes("billing@davisdelivery.com")) file._source = "davis";
      else if (fromLower.includes("@uline.com")) file._source = "uline";
      else file._source = "gmail";
      file._emailFrom = email.from;
      file._emailDate = email.emailDate;

      // 3. Route through shared ingest pipeline
      setImportStatus(`Parsing ${attachment.filename}...`);
      const result = await ingestFiles([file], (s) => {
        if (s.message) setImportStatus(s.message);
      });

      setImported(prev => ({...prev, [refKey]: result}));
      setImportStatus(`✓ Imported ${attachment.filename}`);
      // Update the in-memory dedupe set so the UI reflects this file as a
      // duplicate on the next search without waiting for a full file_log reload.
      setAlreadyImportedFilenames(prev => new Set([...prev, dedupKey(attachment.filename)]));
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
          body: JSON.stringify({
            messageId: email.emailId,
            attachmentId: a.attachmentId,
            account_doc_id: email.account_doc_id || "",
            account_email: email.account_email || "",
          }),
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
      // Mark both PDFs as duplicates in-memory — pair is only a duplicate
      // when ALL its PDFs are flagged, so adding both here handles it.
      setAlreadyImportedFilenames(prev => {
        const next = new Set(prev);
        for (const a of pdfs) next.add(dedupKey(a.filename));
        return next;
      });
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
        body: JSON.stringify({
          messageId: email.emailId,
          attachmentId: attachment.attachmentId,
          account_doc_id: email.account_doc_id || "",
          account_email: email.account_email || "",
        }),
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

      // v2.40.29: Save per-transaction rows with driver names joined from Recap by Card.
      // This enables the "Rebuild Invoice" view and proper DEF/diesel/gas breakout.
      // Each transaction gets its invoice-level fee share by gallons, same as fuel_by_truck.
      const txns = Array.isArray(data.transactions) ? data.transactions : [];
      const cardDrivers = Array.isArray(data.card_drivers) ? data.card_drivers : [];
      if (txns.length > 0) {
        const driverByCard = {};
        for (const cd of cardDrivers) {
          if (cd?.card) driverByCard[String(cd.card)] = cd.driver || "Unknown";
        }
        setImportStatus(`Saving ${txns.length} transactions (${cardDrivers.length} drivers)...`);
        let txnIdx = 0;
        for (const t of txns) {
          txnIdx++;
          const gal = Number(t.gallons) || 0;
          const unitPrice = Number(t.unit_price) || 0;
          const amount = Number(t.amount) || 0;
          const feeShare = gal * feePerGal;
          const trueAmt = Math.round((amount + feeShare) * 100) / 100;
          const trueCpg = gal > 0 ? Math.round((trueAmt / gal) * 10000) / 10000 : null;
          // Stable doc ID: invoice + index (transactions come in order from the PDF)
          const txnId = `quickfuel_${invBase}_${String(txnIdx).padStart(4, "0")}`;
          const card = String(t.card || "");
          await FS.saveFuelTransaction(txnId, {
            txn_id: txnId,
            vendor: "quickfuel",
            invoice_id: `quickfuel_${invBase}`,
            invoice_number: invBase,
            week_ending: weekKey,
            txn_date: t.date || isoDate,
            txn_time: t.time || null,
            card: card,
            driver: driverByCard[card] || "Unknown",
            site_city: t.site_city || null,
            site_number: t.site_number || null,
            product_raw: t.product_raw || null,
            product_category: t.product_category || "unknown",  // diesel | def | gas
            truck_id: String(t.truck_id || "INVENTORY"),
            odometer: Number(t.odometer) || null,
            gallons: gal,
            unit_price: unitPrice,
            amount: amount,
            fee_share: Math.round(feeShare * 10000) / 10000,
            true_amount: trueAmt,
            true_cpg: trueCpg,
          });
        }
      }

      setImported(prev => ({...prev, [refKey]: { ok: true, trucks: data.rows.length, total: grandTotal, rate: avgRate }}));
      setImportStatus(`✓ Quick Fuel ${invBase}: ${data.rows.length} trucks, $${grandTotal.toFixed(2)} @ $${avgRate?.toFixed(4)}/gal${invoiceFees > 0 ? ` (includes $${invoiceFees.toFixed(2)} redistributed fees)` : ""}`);
      setAlreadyImportedFilenames(prev => new Set([...prev, dedupKey(attachment.filename)]));
      if (onRefresh) onRefresh();
    } catch(e) {
      setImported(prev => ({...prev, [refKey]: { error: e.message }}));
      setImportStatus(`✗ Quick Fuel failed: ${e.message}`);
    }
    setImporting(prev => ({...prev, [refKey]: false}));
  };

  // ─── AMP CPAs audited-financial PDFs ──────────────────────────────────────
  // PDF → pages → Claude vision → structured P&L/BS/CF JSON → audited_financials
  // collection. Mirrors the dedicated processor previously in
  // AuditedFinancialsTab::GmailSyncPanel. Lives here in the main Gmail Sync
  // tab so all email parsers are in one place — Audited Financials is now
  // a pure read-only dashboard.
  const importAuditedFinancial = async (email, attachment) => {
    const refKey = `${email.emailId}:${attachment.attachmentId}`;
    setImporting(prev => ({...prev, [refKey]: true}));
    setImportStatus(`Downloading ${attachment.filename}...`);
    try {
      // 1. Download PDF bytes
      const dlData = await downloadGmailAttachment(email, attachment.attachmentId, attachment.filename);
      const binary = atob(dlData.data);
      const pdfBytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) pdfBytes[i] = binary.charCodeAt(i);

      // 2. Render PDF pages to PNGs via pdf.js
      setImportStatus(`Rendering PDF (${attachment.filename})...`);
      await loadPdfJs();
      const loadingTask = window.pdfjsLib.getDocument({ data: pdfBytes });
      const pdf = await loadingTask.promise;
      const numPages = Math.min(pdf.numPages, 30);
      const pages = [];
      for (let p = 1; p <= numPages; p++) {
        const page = await pdf.getPage(p);
        const viewport = page.getViewport({ scale: 1.6 }); // v2.49.0: 1.6 with background-worker (no 26s pressure); compromise between 1.2 (legibility risk) and 1.8 (slow)
        const canvas = document.createElement("canvas");
        canvas.width = viewport.width;
        canvas.height = viewport.height;
        const ctx = canvas.getContext("2d");
        await page.render({ canvasContext: ctx, viewport }).promise;
        pages.push(canvas.toDataURL("image/png").split(",")[1]);
      }

      // 3. Claude vision extraction
      setImportStatus(`Extracting financials (${pages.length} pages)...`);
      const EXTRACTION_PROMPT = `You are extracting financial data from audited financial statements prepared by a CPA firm for a trucking/delivery company (Davis Delivery Services Inc).

Read every page carefully. Return ONLY valid JSON, no markdown, no explanation.

CRITICAL COLUMN RULES — these statements typically have multiple numeric columns. You MUST distinguish them:
  - "1 Month Ended" or "Current Month" or "Month" column → store as "month" value
  - "12 Months Ended" or "Year-To-Date" or "YTD" or "Year to Date" column → store as "ytd" value
  - "%" columns → store as "month_pct" or "ytd_pct" (these are percent of revenue)
  - Prior-year comparison columns ("December 31, 2024" when statement date is December 31, 2025) → store as "prior_month" or "prior_ytd"
  - "Variance" columns → store as "month_variance" or "ytd_variance"

If a column does not exist on the page, omit that field (do not invent zero). If only ONE numeric column exists per line, store it as "month" (NOT "ytd").

Schema:

{
  "period": "YYYY-MM",
  "period_end_date": "YYYY-MM-DD",
  "company": "...",

  "pl_line_items": [
    {
      "label": "exact label from the PDF",
      "section": "revenue | cost_of_sales | operating_expense | other_income | other_expense",
      "month": number | null,
      "month_pct": number | null,
      "ytd": number | null,
      "ytd_pct": number | null,
      "prior_month": number | null,
      "prior_ytd": number | null,
      "month_variance": number | null,
      "ytd_variance": number | null
    }
  ],

  "pl_totals": {
    "total_revenue":          { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_cost_of_sales":    { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "gross_profit":           { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_operating_expenses": { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "operating_income":       { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_other_income":     { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_other_expense":    { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "net_income":             { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... }
  },

  "ebitda_inputs": {
    "depreciation_month": number | null,
    "depreciation_ytd": number | null,
    "amortization_month": number | null,
    "amortization_ytd": number | null,
    "interest_expense_month": number | null,
    "interest_expense_ytd": number | null,
    "income_tax_month": number | null,
    "income_tax_ytd": number | null
  },

  "balance_sheet": {
    "as_of_date": "YYYY-MM-DD",
    "line_items": [
      {
        "label": "exact label from the PDF",
        "section": "current_asset | fixed_asset | other_asset | current_liability | long_term_liability | equity",
        "amount": number
      }
    ],
    "subtotals": {
      "total_current_assets": number | null,
      "total_fixed_assets": number | null,
      "total_other_assets": number | null,
      "total_assets": number | null,
      "total_current_liabilities": number | null,
      "total_long_term_liabilities": number | null,
      "total_liabilities": number | null,
      "total_equity": number | null,
      "total_liabilities_and_equity": number | null
    }
  },

  "cash_flow": {
    "operating_activities": number | null,
    "investing_activities": number | null,
    "financing_activities": number | null,
    "net_change_in_cash": number | null,
    "beginning_cash": number | null,
    "ending_cash": number | null
  },

  "notes": "anything noteworthy: comparative pages present? cash flow statement present? unusual items?"
}

EXTRACTION RULES:
- All amounts in dollars as plain numbers (no $ signs, no commas, no parentheses)
- Negative numbers stay negative: a loss of (341,118.33) becomes -341118.33
- Accumulated Depreciation in balance sheet typically shows as negative
- Net Income (Loss): if the line shows (loss) or parentheses, it is negative
- pl_line_items must include EVERY line on the income statement, including zero-value rows. Do not aggregate, do not skip lines, do not normalize labels.
- For ebitda_inputs: pull these specific values from the operating expenses (Depreciation, Amortization) and other income/expense (Interest Expense, Income Tax) line items. Set null if the line is not present.
- For income_tax: include only federal/state INCOME taxes. Do NOT include payroll taxes, property taxes, sales tax, or franchise tax — those stay in regular operating expenses. If the company is an S-corp with no income tax line, set null.
- period: YYYY-MM derived from the period end date (e.g., December 31, 2025 → "2025-12")
- If the comparative pages do not show prior-period columns, set prior_month and prior_ytd to null on every line item rather than guessing.

Cross-check before responding: total_revenue.ytd should equal the sum of all pl_line_items where section="revenue" of their .ytd values. If they don't match, recheck your column assignment.`;
      const imageBlocks = pages.map(b64 => ({
        type: "image",
        source: { type: "base64", media_type: "image/png", data: b64 },
      }));
      // v2.49.0: scan-financials is now a dispatcher+background-worker pair.
      // Synchronous Netlify functions max out at 26s but vision routinely
      // takes 30-60s on multi-page CPA PDFs. Background functions get a
      // 15-min wall, fully covering the worst case.
      //
      // Flow: POST messages → get jobId → poll status until terminal →
      //       decode response_b64 → parse JSON.
      const totalB64Bytes = pages.reduce((acc, p) => acc + p.length, 0);
      console.log(`[scan-financials] ${pages.length} page(s), ~${(totalB64Bytes/1024/1024).toFixed(1)}MB base64 payload`);
      const dispatchResp = await fetch("/.netlify/functions/marginiq-scan-financials", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          max_tokens: 8192,
          messages: [{ role: "user", content: [...imageBlocks, { type: "text", text: EXTRACTION_PROMPT }] }],
        }),
      });
      const dispatchBody = await dispatchResp.text();
      if (!dispatchResp.ok) {
        let errMsg = `Dispatch ${dispatchResp.status}`;
        try {
          const j = JSON.parse(dispatchBody);
          if (j.error) errMsg += `: ${typeof j.error === "string" ? j.error : JSON.stringify(j.error).slice(0,200)}`;
        } catch { errMsg += `: ${dispatchBody.slice(0,200)}`; }
        throw new Error(errMsg);
      }
      let dispatchJson;
      try { dispatchJson = JSON.parse(dispatchBody); }
      catch (e) { throw new Error(`Dispatch returned non-JSON: ${dispatchBody.slice(0,200)}`); }
      const jobId = dispatchJson.job_id;
      if (!jobId) throw new Error(`Dispatch missing job_id: ${dispatchBody.slice(0,200)}`);

      setImportStatus(`Scanning ${attachment.filename} (job ${jobId.slice(5,21)})...`);
      // Poll scan_jobs/{jobId} until terminal. Background worker has 15min
      // wall; we cap our wait at 5min (financial PDFs typically extract in
      // 20-60s, anything past 5min indicates a stuck job).
      const POLL_INTERVAL_MS = 2500;
      const MAX_POLL_MS = 5 * 60 * 1000;
      const startedAt = Date.now();
      let job = null;
      while (Date.now() - startedAt < MAX_POLL_MS) {
        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
        const pollResp = await fetch(`/.netlify/functions/marginiq-scan-financials?action=status&job_id=${encodeURIComponent(jobId)}`);
        if (!pollResp.ok) {
          // 404 right after dispatch can be Firestore eventual-consistency;
          // tolerate the first 10s then bail.
          if (pollResp.status === 404 && Date.now() - startedAt > 10000) {
            throw new Error(`Scan job ${jobId} disappeared (404 after 10s)`);
          }
          continue;
        }
        const pollJson = await pollResp.json();
        if (pollJson.state === "complete" || pollJson.state === "failed") {
          job = pollJson;
          break;
        }
      }
      if (!job) throw new Error(`Scan timed out after ${MAX_POLL_MS / 1000}s (job ${jobId})`);
      if (job.state === "failed") {
        // Tag rate-limit failures distinctly so Re-import All can pause+retry
        // instead of giving up on this file. usage_exceeded is Anthropic's
        // monthly cap; 429 is short-window throttling; 503 is general overload.
        const err = job.error || "Unknown scan failure";
        if (err.includes("usage_exceeded") || err.includes("rate_limit") || err.includes("rate limit") || job.http_status === 429 || job.http_status === 503) {
          throw new Error(`RATE_LIMITED: ${err}`);
        }
        throw new Error(err);
      }
      // state === "complete" — decode response_b64 → Anthropic JSON → extracted
      if (!job.response_b64) throw new Error("Job complete but no response_b64 in scan_jobs doc");
      const respText = atob(job.response_b64);
      let sfData;
      try { sfData = JSON.parse(respText); }
      catch (e) { throw new Error(`Vision response was not JSON: ${respText.slice(0,200)}`); }
      if (!sfData.content || !Array.isArray(sfData.content)) {
        throw new Error(`Vision response missing content array: ${JSON.stringify(sfData).slice(0,200)}`);
      }
      const text = sfData.content.map(b => b.text || "").join("").trim();
      if (!text) throw new Error(`Vision returned empty text. stop_reason=${sfData.stop_reason || "unknown"}`);
      let extracted;
      try {
        extracted = JSON.parse(text.replace(/```json|```/g, "").trim());
      } catch (e) {
        console.error("[scan-financials] JSON parse failed. Raw text:", text);
        throw new Error(`Vision returned text but not JSON. First 300 chars: ${text.slice(0,300)}`);
      }

      // 4. Determine period — extracted JSON wins, fallback to filename/subject
      const monthFromString = (s) => {
        if (!s) return null;
        const lower = s.toLowerCase();
        const monthsFull  = ["january","february","march","april","may","june","july","august","september","october","november","december"];
        const monthsShort = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"];
        for (let i = 0; i < 12; i++) {
          if (lower.includes(monthsFull[i]) || lower.includes(monthsShort[i])) {
            const yMatch = s.match(/\b(20\d{2})\b/);
            const y = yMatch ? yMatch[1] : new Date().getFullYear().toString();
            return `${y}-${String(i+1).padStart(2,"0")}`;
          }
        }
        const m1 = s.match(/\b(\d{1,2})[\/-](20\d{2})\b/); if (m1) return `${m1[2]}-${m1[1].padStart(2,"0")}`;
        const m2 = s.match(/\b(20\d{2})[\/-](\d{1,2})\b/); if (m2) return `${m2[1]}-${m2[2].padStart(2,"0")}`;
        return null;
      };
      const period = (extracted.period && /^\d{4}-\d{2}$/.test(extracted.period))
        ? extracted.period
        : (monthFromString(attachment.filename) || monthFromString(email.emailSubject));
      if (!period) throw new Error(`Could not determine period from "${attachment.filename}"`);

      // 5. Upload PDF to Storage (best effort) + write to Firestore
      setImportStatus(`Saving ${period} financials...`);
      let pdfMeta = null;
      if (window.fbStorage) {
        try {
          const path = `audited_financials_v2/${period}.pdf`;
          const ref = window.fbStorage.ref(path);
          const blob = new Blob([pdfBytes], { type: "application/pdf" });
          await ref.put(blob, { contentType: "application/pdf", customMetadata: { original_filename: attachment.filename } });
          const url = await ref.getDownloadURL();
          pdfMeta = { storage_path: path, download_url: url };
        } catch (e) { console.warn("PDF storage upload failed:", e); }
      }
      const record = {
        ...extracted,
        period,
        email_id: email.emailId,
        email_subject: email.emailSubject,
        email_date: email.emailDate,
        from: email.from,
        filename: attachment.filename,
        pdf_storage_path: pdfMeta?.storage_path || null,
        pdf_download_url: pdfMeta?.download_url || null,
        updated_at: new Date().toISOString(),
      };
      await window.db.collection("audited_financials_v2").doc(period).set(record, { merge: true });

      // 6. Log filename for dedupe + processed-emails ledger
      await FS.saveFileLog(`audited_${period}_${email.emailId}`, {
        file_id: `audited_${period}_${email.emailId}`,
        filename: attachment.filename,
        kind: "audited-financials",
        group: "financials",
        row_count: 0,
        source: "ampcpas",
        email_from: email.from,
        period,
        uploaded_at: new Date().toISOString(),
      });
      try {
        await window.db.collection("audited_financials_emails").doc(email.emailId)
          .set({ period, filename: attachment.filename, subject: email.emailSubject, processed_at: new Date().toISOString() }, { merge: true });
      } catch(e) { console.warn("audited_financials_emails write failed:", e); }

      setImported(prev => ({...prev, [refKey]: { ok: true, period }}));
      setImportStatus(`✓ ${period} extracted (${attachment.filename})`);
      if (onRefresh) onRefresh();
    } catch(e) {
      console.error("importAuditedFinancial failed:", e);
      setImported(prev => ({...prev, [refKey]: { error: e.message }}));
      setImportStatus(`✗ Audited financials failed: ${e.message}`);
    }
    setImporting(prev => ({...prev, [refKey]: false}));
  };

  // Import All — bulk-import every email in the current vendor's search results.
  // Skips anything whose filename is already in file_log (idempotent on re-runs).
  // Honors vendor-specific handlers: FuelFox uses pair import, Quick Fuel PDFs
  // use importQuickFuel, everything else uses per-attachment importAttachment.
  const importAllForVendor = async (vendor) => {
    const r = results[vendor];
    if (!r?.list || r.list.length === 0) return;

    const emails = r.list;
    let current = 0;
    let imported = 0;
    let skipped = 0;
    let failed = 0;
    const total = emails.length;
    setBulkImporting(prev => ({...prev, [vendor]: { current: 0, total, skipped: 0, failed: 0, imported: 0, running: true }}));

    for (const em of emails) {
      current++;
      setBulkImporting(prev => ({...prev, [vendor]: { current, total, skipped, failed, imported, running: true }}));

      try {
        // FuelFox is pair-mode — one email = 2 PDFs processed together
        if (vendor === "fuelfox") {
          const pdfs = (em.attachments || []).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
          if (pdfs.length < 2) { skipped++; continue; }
          // Check if ALL attachments already imported
          const allDone = pdfs.every(p => alreadyImportedFilenames.has(dedupKey(p.filename)));
          if (allDone) { skipped++; continue; }
          setImportStatus(`[${current}/${total}] ${em.emailSubject || "FuelFox"}...`);
          await importFuelFoxPair(em);
          imported++;
        } else {
          // Per-attachment vendors (NuVizz, Uline, DDIS, Quick Fuel)
          const atts = (em.attachments || []);
          if (atts.length === 0) { skipped++; continue; }
          for (const a of atts) {
            const lcName = a.filename.toLowerCase();
            const dk = dedupKey(a.filename);
            // Skip if already imported (structural dedup — same week same variant)
            if (alreadyImportedFilenames.has(dk)) { skipped++; continue; }
            // Only handle data file types the vendor parses
            const isData = lcName.endsWith(".xlsx") || lcName.endsWith(".xls") || lcName.endsWith(".csv") || lcName.endsWith(".pdf");
            if (!isData) { skipped++; continue; }
            // For ampcpas, also gate on filename containing "financial" so we
            // don't accidentally process invoices from the same sender.
            if (vendor === "ampcpas" && !lcName.includes("financial")) { skipped++; continue; }
            setImportStatus(`[${current}/${total}] ${a.filename}...`);
            try {
              const isQuickFuelPdf = vendor === "quickfuel" && lcName.endsWith(".pdf");
              const isAuditedPdf = vendor === "ampcpas" && lcName.endsWith(".pdf");
              if (isAuditedPdf) await importAuditedFinancial(em, a);
              else if (isQuickFuelPdf) await importQuickFuel(em, a);
              else await importAttachment(em, a);
              imported++;
              setAlreadyImportedFilenames(prev => new Set([...prev, dk]));
            } catch(e) {
              console.error("Import All: attachment failed", a.filename, e);
              failed++;
            }
          }
        }
      } catch(e) {
        console.error("Import All: email failed", em.emailSubject, e);
        failed++;
      }
      setBulkImporting(prev => ({...prev, [vendor]: { current, total, skipped, failed, imported, running: true }}));
    }

    setBulkImporting(prev => ({...prev, [vendor]: { current, total, skipped, failed, imported, running: false }}));
    setImportStatus(`✓ Import All done — ${imported} imported, ${skipped} skipped (already loaded), ${failed} failed`);
    await loadImportedFilenames(); // Refresh the already-imported Set
    if (onRefresh) onRefresh();
  };

  if (loadingConn) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading Gmail...</div>;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="📧" text="Gmail Sync" />

    {oauthMsg && (
      <div style={{
        padding: "10px 14px",
        marginBottom: 12,
        borderRadius: 8,
        border: `1px solid ${oauthMsg.kind === "success" ? T.green : T.red}`,
        background: oauthMsg.kind === "success" ? "#ecfdf5" : "#fef2f2",
        color: oauthMsg.kind === "success" ? "#065f46" : T.redText,
        fontSize: 12,
        fontWeight: 600,
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        gap: 10,
      }}>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{fontSize:16}}>{oauthMsg.kind === "success" ? "✓" : "⚠️"}</span>
          <span style={{wordBreak:"break-word"}}>{oauthMsg.text}</span>
        </div>
        <button onClick={() => setOauthMsg(null)} style={{
          border: "none",
          background: "transparent",
          color: "inherit",
          cursor: "pointer",
          fontSize: 18,
          padding: "0 4px",
          fontWeight: 700,
        }}>×</button>
      </div>
    )}

    {gmailAccounts.length === 0 ? (
      <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:8,color:T.brand}}>Connect Gmail</div>
        <div style={{fontSize:12,color:T.text,lineHeight:1.6,marginBottom:12}}>
          Auto-import weekly reports directly from your inbox. MarginIQ will search your Gmail (read-only) for attachments from known vendors and route them through the same parsers as manual upload.
          <br/><br/>
          <strong>Tip:</strong> connect <code>billing@davisdelivery.com</code> in addition to <code>chad@</code> to see every accessorial + TK file Uline sends, even when chad@ wasn't CC'd.
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
          <div style={{fontSize:12,fontWeight:700,color:T.greenText,marginBottom:8}}>
            ✓ Connected ({gmailAccounts.length} {gmailAccounts.length === 1 ? "account" : "accounts"})
          </div>
          {gmailAccounts.map((acct) => (
            <div key={acct.docId} style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,padding:"6px 0",borderTop:`1px solid ${T.borderLight}`}}>
              <div>
                <div style={{fontSize:13,fontWeight:600}}>{acct.email}</div>
                {acct.connected_at && <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>Connected {new Date(acct.connected_at).toLocaleString()}</div>}
              </div>
              <button onClick={() => disconnect(acct.docId, acct.email)} style={{padding:"6px 12px",borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,fontSize:11,cursor:"pointer"}}>Disconnect</button>
            </div>
          ))}
          <div style={{marginTop:10,paddingTop:10,borderTop:`1px solid ${T.borderLight}`}}>
            <a href="/.netlify/functions/marginiq-gmail-auth" style={{display:"inline-block",padding:"6px 12px",background:T.brand,color:"#fff",borderRadius:6,textDecoration:"none",fontSize:11,fontWeight:700}}>
              + Add another account
            </a>
            <span style={{marginLeft:10,fontSize:10,color:T.textMuted}}>
              Sign into the other Gmail in a new tab first, then click this.
            </span>
          </div>
        </div>

        {importStatus && (
          <div style={{...cardStyle, background:T.yellowBg, borderColor:T.yellow, fontSize:12, color:T.yellowText, fontWeight:600}}>
            {importStatus}
          </div>
        )}

        {/* Date range picker — applies to every vendor search below */}
        <div style={{...cardStyle, padding:"12px 14px"}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10,flexWrap:"wrap",gap:8}}>
            <div>
              <div style={{fontSize:12,fontWeight:700,color:T.text}}>📅 Search date range</div>
              <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>
                Applied to every vendor search. Current: <strong style={{color:T.text}}>{rangeLabel()}</strong>
              </div>
            </div>
          </div>
          <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
            {[
              { k:"30d",   l:"Last 30d" },
              { k:"60d",   l:"Last 60d" },
              { k:"90d",   l:"Last 90d" },
              { k:"6mo",   l:"Last 6mo" },
              { k:"12mo",  l:"Last 12mo" },
              { k:"ytd",   l:"Year to date" },
              { k:"all",   l:"All time" },
              { k:"custom",l:"Custom…" },
            ].map(p => (
              <button key={p.k}
                onClick={() => setRangePreset(p.k)}
                style={{
                  padding: "5px 10px",
                  borderRadius: 6,
                  border: `1px solid ${rangePreset === p.k ? T.brand : T.border}`,
                  background: rangePreset === p.k ? T.brandPale : T.bgWhite,
                  color: rangePreset === p.k ? T.brand : T.textMuted,
                  cursor: "pointer",
                  fontWeight: rangePreset === p.k ? 700 : 500,
                  fontSize: 11,
                }}>{p.l}</button>
            ))}
          </div>
          {rangePreset === "custom" && (
            <div style={{marginTop:10,display:"flex",alignItems:"center",gap:10,flexWrap:"wrap"}}>
              <div style={{display:"flex",flexDirection:"column",gap:2}}>
                <label style={{fontSize:10,color:T.textMuted,fontWeight:600,textTransform:"uppercase",letterSpacing:"0.06em"}}>From</label>
                <input type="date" value={customFrom} onChange={e=>setCustomFrom(e.target.value)}
                  style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${T.border}`,fontSize:12,fontFamily:"inherit"}} />
              </div>
              <div style={{display:"flex",flexDirection:"column",gap:2}}>
                <label style={{fontSize:10,color:T.textMuted,fontWeight:600,textTransform:"uppercase",letterSpacing:"0.06em"}}>To</label>
                <input type="date" value={customTo} onChange={e=>setCustomTo(e.target.value)}
                  style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${T.border}`,fontSize:12,fontFamily:"inherit"}} />
              </div>
              {(customFrom || customTo) && (
                <button onClick={() => { setCustomFrom(""); setCustomTo(""); }}
                  style={{padding:"6px 10px",borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,color:T.textMuted,cursor:"pointer",fontSize:11,alignSelf:"flex-end"}}>
                  Clear
                </button>
              )}
            </div>
          )}
        </div>

        {/* Vendor Panels */}
        {[
          { key:"nuvizz", icon:"🚚", label:"NuVizz", desc:"Weekly driver stops from nuvizzapps@nuvizzapps.com", color:T.blue, mode:"per-attachment" },
          { key:"uline", icon:"📦", label:"Uline Billing", desc:"Weekly DAS xlsx files from @uline.com + billing@davisdelivery.com corrections (delivery + truckload + accessorials)", color:T.brand, mode:"per-attachment" },
          { key:"ddis", icon:"💰", label:"Uline DDIS Payments", desc:"Payment remittance CSVs from APFreight@uline.com (paid PROs for reconciliation)", color:T.green, mode:"per-attachment" },
          { key:"fuelfox", icon:"⛽", label:"FuelFox", desc:"Fuel delivery — summary + service log PDFs from accounting@fuelfox.net", color:"#dc2626", mode:"pair" },
          { key:"quickfuel", icon:"⛽", label:"Quick Fuel", desc:"Fuel card statements from ebilling@4flyers.com", color:"#2563eb", mode:"quickfuel" },
          { key:"billing_sent", icon:"📤", label:"Billing@ → Uline", desc:"Emails billing@davisdelivery.com sent to any @uline.com recipient with an attachment. Disputes, corrections, POD replies, reshipments — outbound Uline correspondence only.", color:"#8b5cf6", mode:"per-attachment", accountFilter:"billing@davisdelivery.com" },
          { key:"ampcpas", icon:"📊", label:"AMP CPAs (Audited Financials)", desc:"Monthly audited P&L, Balance Sheet, and Cash Flow PDFs from @ampcpas.com. Each PDF is rendered, scanned by Claude vision, and stored in audited_financials. Visible in the 📋 Financials tab.", color:"#0d9488", mode:"audited-financials" },
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
                    {v.accountFilter && <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:T.brandPale,color:T.brand,marginLeft:8}}>pinned to {v.accountFilter}</span>}
                  </div>
                  <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>{v.desc}</div>
                </div>
                <PrimaryBtn text={isLoading ? "Searching..." : `Search · ${rangeLabel()}`} onClick={() => searchVendor(v.key, v.accountFilter || "")} loading={isLoading} />
              </div>

              {r?.error && <div style={{fontSize:12,color:T.redText,background:T.redBg,padding:"8px 10px",borderRadius:6,marginTop:8}}>✗ {r.error}</div>}

              {r?.list && r.list.length === 0 && (
                <div style={{fontSize:12,color:T.textMuted,padding:8}}>No matching emails in this range ({rangeLabel()}).</div>
              )}

              {r?.list && r.list.length > 0 && (() => {
                // Count totals and collect duplicate filenames for display. Dedup
                // detection is based on filename match against file_log (loaded on
                // mount as alreadyImportedFilenames). Gmail message IDs aren't
                // tracked in file_log, so filename is the practical dedupe key.
                let totalData = 0, alreadyDone = 0;
                const duplicateFiles = []; // filenames flagged as already ingested
                for (const em of r.list) {
                  if (v.mode === "pair") {
                    const pdfs = (em.attachments||[]).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
                    if (pdfs.length > 0) {
                      totalData++;
                      if (pdfs.every(p => alreadyImportedFilenames.has(dedupKey(p.filename)))) {
                        alreadyDone++;
                        duplicateFiles.push(pdfs.map(p => p.filename).join(" + "));
                      }
                    }
                  } else {
                    for (const a of (em.attachments||[])) {
                      const lc = a.filename.toLowerCase();
                      if (!(lc.endsWith(".xlsx") || lc.endsWith(".xls") || lc.endsWith(".csv") || lc.endsWith(".pdf"))) continue;
                      totalData++;
                      if (alreadyImportedFilenames.has(dedupKey(a.filename))) {
                        alreadyDone++;
                        duplicateFiles.push(a.filename);
                      }
                    }
                  }
                }
                const pending = totalData - alreadyDone;
                const bulk = bulkImporting[v.key];
                const bulkRunning = bulk?.running;

                // v2.40.11: Missing-only filter — hide emails whose every data
                // attachment is already in file_log. Pair-mode emails are
                // filtered out only when ALL PDFs in the pair are already in.
                const missingOnly = !!showMissingOnly[v.key];
                const emailIsFullyImported = (em) => {
                  if (v.mode === "pair") {
                    const pdfs = (em.attachments||[]).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
                    if (pdfs.length === 0) return false;
                    return pdfs.every(p => alreadyImportedFilenames.has(dedupKey(p.filename)));
                  }
                  const atts = (em.attachments||[]).filter(a => {
                    const lc = a.filename.toLowerCase();
                    return lc.endsWith(".xlsx") || lc.endsWith(".xls") || lc.endsWith(".csv") || lc.endsWith(".pdf");
                  });
                  if (atts.length === 0) return false;
                  return atts.every(a => alreadyImportedFilenames.has(dedupKey(a.filename)));
                };
                // v2.40.15: hide emails with ZERO data attachments from the
                // list. Gmail's has:attachment matches inline images/sigs, so
                // with 1000 results per search (post-v2.40.11 pagination) the
                // tail is a wall of "No data attachments" rows that add
                // nothing. Count how many we're hiding so the user knows.
                const hasDataAttachment = (em) => {
                  if (v.mode === "pair") {
                    return (em.attachments||[]).some(a => a.filename.toLowerCase().endsWith(".pdf"));
                  }
                  return (em.attachments||[]).some(a => {
                    const lc = a.filename.toLowerCase();
                    return lc.endsWith(".xlsx") || lc.endsWith(".xls") || lc.endsWith(".csv") || lc.endsWith(".pdf");
                  });
                };
                const emptyEmailCount = r.list.filter(em => !hasDataAttachment(em)).length;
                let displayList = r.list.filter(hasDataAttachment);
                if (missingOnly) displayList = displayList.filter(em => !emailIsFullyImported(em));

                return (
                  <div style={{marginTop:8}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,marginBottom:8,padding:"8px 10px",background:T.bgSurface,borderRadius:6}}>
                      <div style={{fontSize:11,color:T.textDim}}>
                        <strong style={{color:T.text}}>{r.list.length} email{r.list.length>1?"s":""}</strong>
                        {" · "}
                        <span>{totalData} {v.mode === "pair" ? "pair(s)" : "file(s)"}</span>
                        {alreadyDone > 0 && <span style={{color:T.greenText}}> · {alreadyDone} already imported</span>}
                        {pending > 0 && <span style={{color:T.brand,fontWeight:600}}> · {pending} pending</span>}
                        {emptyEmailCount > 0 && <span style={{color:T.textMuted}}> · {emptyEmailCount} hidden (no data attachment)</span>}
                        {missingOnly && <span style={{color:T.textMuted}}> · showing {displayList.length} missing</span>}
                      </div>
                      <div style={{display:"flex",alignItems:"center",gap:8,flexWrap:"wrap"}}>
                        {alreadyDone > 0 && (
                          <button
                            onClick={() => setShowMissingOnly(prev => ({...prev, [v.key]: !prev[v.key]}))}
                            style={{
                              padding:"5px 10px",borderRadius:6,
                              border:`1px solid ${missingOnly ? v.color : T.border}`,
                              background: missingOnly ? v.color : "transparent",
                              color: missingOnly ? "#fff" : T.textMuted,
                              fontSize:10,fontWeight:700,cursor:"pointer",
                            }}
                          >
                            {missingOnly ? "✓ Missing only" : "Show missing only"}
                          </button>
                        )}
                        {pending > 0 && !bulkRunning && (
                          <button
                            onClick={() => {
                              if (!confirm(`Import ${pending} ${v.mode==="pair"?"email pair(s)":"file(s)"} from ${v.label}?\n\nAlready-imported files will be skipped automatically.`)) return;
                              importAllForVendor(v.key);
                            }}
                            style={{
                              padding: "6px 14px",
                              borderRadius: 6,
                              border: "none",
                              background: `linear-gradient(135deg,${v.color},${v.color})`,
                              color: "#fff",
                              fontSize: 11,
                              fontWeight: 700,
                              cursor: "pointer",
                            }}
                          >🚀 Import All ({pending})</button>
                        )}
                        {bulkRunning && (
                          <div style={{fontSize:11,color:v.color,fontWeight:700}}>
                            ⏳ {bulk.current}/{bulk.total} · ✓{bulk.imported} ⏭{bulk.skipped} ✗{bulk.failed}
                          </div>
                        )}
                        {!bulkRunning && bulk && bulk.current > 0 && (
                          <div style={{fontSize:11,color:T.greenText,fontWeight:700}}>
                            ✓ Done — imported {bulk.imported}, skipped {bulk.skipped}, failed {bulk.failed}
                          </div>
                        )}
                      </div>
                    </div>
                    {duplicateFiles.length > 0 && (
                      <details style={{marginBottom:8,padding:"6px 10px",background:"#fef9c3",borderRadius:6,border:`1px solid ${T.yellow}40`,fontSize:11}}>
                        <summary style={{cursor:"pointer",fontWeight:700,color:"#78350f",listStyle:"none",display:"flex",alignItems:"center",gap:6,flexWrap:"wrap"}}>
                          <span>⚠ {duplicateFiles.length} duplicate{duplicateFiles.length>1?"s":""} detected — already in file_log, will be auto-skipped</span>
                          <span style={{fontSize:10,fontWeight:400,color:T.textMuted}}>(click to view)</span>
                          <button
                            onClick={(e) => {
                              e.preventDefault(); e.stopPropagation();
                              const dupCount = duplicateFiles.length;
                              if (!confirm(`Re-run all ${dupCount} duplicate-flagged file${dupCount>1?"s":""} through the parser?\n\nUse this when the parser has been updated since the original import (e.g. v2.48.0 added the audited-financials extractor) and existing file_log entries don't have the data they should.\n\nMax-merge means this is safe — no data will be destroyed.`)) return;
                              (async () => {
                                let done = 0, failed = 0;
                                const total = displayList.reduce((acc, em) => {
                                  return acc + (em.attachments || []).filter(a => alreadyImportedFilenames.has(dedupKey(a.filename))).length;
                                }, 0);
                                setImportStatus(`Re-importing ${total} duplicate${total>1?"s":""}...`);
                                // v2.49.0: vision-call vendors (ampcpas, quickfuel) require sequential
                                // processing with cooldowns. The Anthropic API has aggressive rate limits
                                // and a usage cap; parallel scans hit 'usage_exceeded' (HTTP 503) within
                                // seconds. CSV-parser vendors (uline, nuvizz, ddis) don't need throttling.
                                const isVisionVendor = v.mode === "audited-financials" || v.mode === "quickfuel";
                                const COOLDOWN_MS = isVisionVendor ? 8000 : 0;
                                const RATE_LIMIT_PAUSE_MS = 60000;
                                const MAX_RETRIES = 5;
                                for (const em of displayList) {
                                  for (const a of (em.attachments || [])) {
                                    const dk = dedupKey(a.filename);
                                    if (!alreadyImportedFilenames.has(dk)) continue;
                                    const lcName = a.filename.toLowerCase();
                                    // Skip non-data attachments
                                    const isData = lcName.endsWith(".xlsx") || lcName.endsWith(".xls") || lcName.endsWith(".csv") || lcName.endsWith(".pdf");
                                    if (!isData) continue;
                                    // Vendor-specific gate (same as Import All)
                                    if (v.key === "ampcpas" && !lcName.includes("financial")) continue;
                                    done++;
                                    setImportStatus(`Re-importing [${done}/${total}] ${a.filename}...`);
                                    let attempt = 0;
                                    while (attempt < MAX_RETRIES) {
                                      attempt++;
                                      try {
                                        const isQuickFuelPdf = v.mode === "quickfuel" && lcName.endsWith(".pdf");
                                        const isAuditedPdf = v.mode === "audited-financials" && lcName.endsWith(".pdf");
                                        if (isAuditedPdf) await importAuditedFinancial(em, a);
                                        else if (isQuickFuelPdf) await importQuickFuel(em, a);
                                        else await importAttachment(em, a);
                                        break; // success
                                      } catch(err) {
                                        const msg = String(err?.message || err);
                                        if (msg.startsWith("RATE_LIMITED") && attempt < MAX_RETRIES) {
                                          setImportStatus(`⏸ Rate limited — pausing ${RATE_LIMIT_PAUSE_MS/1000}s before retrying ${a.filename}...`);
                                          await new Promise(r => setTimeout(r, RATE_LIMIT_PAUSE_MS));
                                          continue;
                                        }
                                        console.error("Re-import failed:", a.filename, err);
                                        failed++;
                                        break;
                                      }
                                    }
                                    // Cooldown between scans (vision vendors only)
                                    if (COOLDOWN_MS && done < total) {
                                      await new Promise(r => setTimeout(r, COOLDOWN_MS));
                                    }
                                  }
                                }
                                setImportStatus(`✓ Re-import done — ${done - failed}/${done} succeeded${failed?`, ${failed} failed`:""}`);
                                await loadImportedFilenames();
                                if (onRefresh) onRefresh();
                              })();
                            }}
                            style={{padding:"3px 10px",fontSize:10,fontWeight:700,borderRadius:6,border:`1px solid ${T.yellow}`,background:"#fde68a",color:"#78350f",cursor:"pointer",marginLeft:"auto"}}
                            title="Re-run every duplicate-flagged file through the current parser. Use after a parser upgrade.">
                            🔁 Re-import All ({duplicateFiles.length})
                          </button>
                        </summary>
                        <div style={{marginTop:6,paddingLeft:4,display:"flex",flexDirection:"column",gap:2,fontFamily:"monospace",fontSize:10,color:"#78350f"}}>
                          {duplicateFiles.map((fn, i) => <div key={i}>• {fn}</div>)}
                        </div>
                        <div style={{marginTop:6,fontSize:10,color:T.textMuted,fontStyle:"italic"}}>
                          Import All automatically skips these. Max-merge ensures re-importing can't corrupt data if you do choose to re-import. Use the Re-import All button above when the parser has been updated and existing data needs to be re-extracted.
                        </div>
                      </details>
                    )}
                    {v.mode === "pair" && <div style={{fontSize:10,color:T.textMuted,marginBottom:6,fontStyle:"italic"}}>FuelFox sends summary + service log PDFs together — each pair imports as one unit.</div>}

                    {displayList.map((em, idx) => {
                    // For FuelFox: single "Import Pair" button per email
                    if (v.mode === "pair") {
                      const pdfs = (em.attachments || []).filter(a => a.filename.toLowerCase().endsWith(".pdf"));
                      const refKey = `${em.emailId}:fuelfox_pair`;
                      const isImp = importing[refKey];
                      const imp = imported[refKey];
                      // Pair-level dedupe: the pair is a duplicate only when
                      // every PDF in it is already in file_log. If only one
                      // of the two showed up before (rare partial-import
                      // scenario), allow a normal import rather than blocking.
                      const isDuplicate = pdfs.length > 0 && pdfs.every(p =>
                        alreadyImportedFilenames.has(dedupKey(p.filename))
                      );
                      return (
                        <div key={em.emailId} style={{padding:"8px 10px",borderTop:idx>0?`1px solid ${T.borderLight}`:"none"}}>
                          <div style={{fontSize:12,fontWeight:600}}>{em.emailSubject || "(no subject)"}</div>
                          <div style={{fontSize:10,color:T.textMuted}}>{em.from} • {em.emailDate ? new Date(em.emailDate).toLocaleString() : "—"}</div>
                          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8,marginTop:8,padding:"6px 10px",background:isDuplicate?"#fef9c3":T.bgSurface,borderRadius:6,border:isDuplicate?`1px solid ${T.yellow}40`:"1px solid transparent"}}>
                            <div style={{flex:1,fontSize:11}}>
                              <div style={{display:"flex",alignItems:"center",gap:6,flexWrap:"wrap"}}>
                                <span>📎 {pdfs.length} PDF{pdfs.length!==1?"s":""}{pdfs.length === 2 ? " (summary + log)" : pdfs.length < 2 ? " ⚠️ expected 2" : ""}</span>
                                {isDuplicate && !imp && <span title="Both PDFs already in file_log — safe to skip" style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:T.yellow,color:"#78350f"}}>⚠ DUPLICATE</span>}
                              </div>
                              <div style={{fontSize:9,color:T.textDim,marginTop:2}}>{pdfs.map(p => p.filename).join(" · ")}</div>
                            </div>
                            {imp?.error ? (
                              <span style={{fontSize:10,color:T.redText,maxWidth:150}}>✗ {imp.error.substring(0,60)}</span>
                            ) : imp?.ok ? (
                              <div style={{textAlign:"right"}}>
                                <div style={{fontSize:10,color:T.greenText,fontWeight:700}}>✓ {imp.trucks} trucks</div>
                                <div style={{fontSize:9,color:T.textMuted}}>${imp.total.toFixed(2)} @ ${imp.rate.toFixed(4)}/gal</div>
                              </div>
                            ) : isDuplicate ? (
                              <button
                                onClick={() => {
                                  if (!confirm(`Both PDFs in this FuelFox pair have already been imported. Re-importing is safe (max-merge guarantees no data is destroyed), but it's usually redundant.\n\nAre you sure you want to re-import?`)) return;
                                  importFuelFoxPair(em);
                                }}
                                disabled={isImp || pdfs.length < 2}
                                title="Already imported — click to re-import anyway (safe, max-merge)"
                                style={{padding:"6px 12px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.yellow}`,background:isImp?T.bgSurface:"#fde68a",color:"#78350f",cursor:isImp?"wait":(pdfs.length < 2?"not-allowed":"pointer"),opacity:(isImp || pdfs.length < 2)?0.6:1}}>
                                {isImp ? "Processing..." : "↻ Re-import"}
                              </button>
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
                    // For Uline: show source badge so Chad can see whether
                    // this is from Uline or from his own billing@davisdelivery.com
                    // correction address.
                    const sourceBadge = (() => {
                      if (v.key !== "uline") return null;
                      const fromLower = (em.from || "").toLowerCase();
                      if (fromLower.includes("billing@davisdelivery.com")) {
                        return <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:"#fef3c7",color:"#92400e",marginLeft:6,whiteSpace:"nowrap"}}>✏️ DAVIS CORRECTION</span>;
                      }
                      if (fromLower.includes("@uline.com")) {
                        return <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:T.brandPale,color:T.brand,marginLeft:6,whiteSpace:"nowrap"}}>📦 ULINE</span>;
                      }
                      return null;
                    })();
                    return (
                      <div key={em.emailId} style={{padding:"8px 10px",borderTop:idx>0?`1px solid ${T.borderLight}`:"none"}}>
                        <div style={{fontSize:12,fontWeight:600,display:"flex",alignItems:"center",flexWrap:"wrap",gap:4}}>
                          <span>{em.emailSubject || "(no subject)"}</span>
                          {sourceBadge}
                        </div>
                        <div style={{fontSize:10,color:T.textMuted}}>{em.from} • {em.emailDate ? new Date(em.emailDate).toLocaleString() : "—"}</div>
                        {em.attachments?.length > 0 ? (
                          <div style={{marginTop:6,display:"flex",flexDirection:"column",gap:4}}>
                            {em.attachments.map(a => {
                              const refKey = `${em.emailId}:${a.attachmentId}`;
                              const isImp = importing[refKey];
                              const imp = imported[refKey];
                              const disabled = v.comingSoon;
                              const isQuickFuelPdf = v.mode === "quickfuel" && a.filename.toLowerCase().endsWith(".pdf");
                              const isAuditedPdf = v.mode === "audited-financials" && a.filename.toLowerCase().endsWith(".pdf");
                              // Detect if this filename has already been ingested in a prior session.
                              // Gmail message IDs aren't stored in file_log, so filename is the
                              // practical dedupe key. v2.40.3 uses a structural
                              // key (das_<start>_<end>_<variant>) for DAS files
                              // so -JO and other re-forwarded variants collapse
                              // correctly. v2.25 max-merge makes accidental
                              // re-import non-destructive, but surfacing this
                              // visibly gives the user confidence that nothing
                              // was double-entered.
                              const isDuplicate = alreadyImportedFilenames.has(dedupKey(a.filename));
                              return (
                                <div key={a.attachmentId} style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8,padding:"4px 8px",background:isDuplicate?"#fef9c3":T.bgSurface,borderRadius:6,border:isDuplicate?`1px solid ${T.yellow}40`:"1px solid transparent"}}>
                                  <div style={{flex:1,fontSize:11,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:6}}>
                                    <span>📎 {a.filename} <span style={{color:T.textDim}}>({Math.round(a.size/1024)} KB)</span></span>
                                    {isDuplicate && !imp && <span title="This exact filename has already been ingested — safe to skip" style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:4,background:T.yellow,color:"#78350f",whiteSpace:"nowrap"}}>⚠ DUPLICATE</span>}
                                  </div>
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
                                  ) : isDuplicate ? (
                                    <button
                                      onClick={() => {
                                        if (!confirm(`"${a.filename}" has already been imported. Re-importing is safe (max-merge guarantees no data is destroyed), but it's usually redundant.\n\nAre you sure you want to re-import?`)) return;
                                        if (isAuditedPdf) importAuditedFinancial(em, a);
                                        else if (isQuickFuelPdf) importQuickFuel(em, a);
                                        else importAttachment(em, a);
                                      }}
                                      disabled={isImp}
                                      title="Already imported — click to re-import anyway (safe, max-merge)"
                                      style={{padding:"4px 10px",fontSize:10,fontWeight:700,borderRadius:6,border:`1px solid ${T.yellow}`,background:isImp?T.bgSurface:"#fde68a",color:"#78350f",cursor:isImp?"wait":"pointer",opacity:isImp?0.6:1}}>
                                      {isImp ? "..." : "↻ Re-import"}
                                    </button>
                                  ) : (
                                    <button
                                      onClick={() => {
                                        if (isAuditedPdf) importAuditedFinancial(em, a);
                                        else if (isQuickFuelPdf) importQuickFuel(em, a);
                                        else importAttachment(em, a);
                                      }}
                                      disabled={isImp}
                                      style={{padding:"4px 10px",fontSize:10,fontWeight:700,borderRadius:6,border:`1px solid ${v.color}`,background:isImp?T.bgSurface:v.color,color:isImp?T.text:"#fff",cursor:isImp?"wait":"pointer",opacity:isImp?0.6:1}}>
                                      {isImp ? "..." : isAuditedPdf ? "📊 Scan" : isQuickFuelPdf ? "⛽ Scan" : "→ Import"}
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
                );
              })()}
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

// Compute the Friday week-ending for a YYYY-MM-DD date (used for Uline Revenue)
// Compute Uline's Friday week-ending for a given date. Uline invoices on a
// SATURDAY-TO-FRIDAY cycle. That means:
//   Sat pickup → invoiced on the UPCOMING Friday (6 days later), not the prior Fri
//   Sun pickup → invoiced on the UPCOMING Friday (5 days later), not the prior Fri
//   Mon–Fri pickup → invoiced on THIS week's Friday
// Previously this function rolled Sat/Sun BACKWARD to the previous Friday, which
// put those stops on the wrong invoice. The rule is "always advance to the next
// Friday (or today if already Friday)." The formula (5 - day + 7) % 7 gives:
//   Sun(0) → +5 days   Mon(1) → +4   Tue(2) → +3   Wed(3) → +2
//   Thu(4) → +1        Fri(5) → +0   Sat(6) → +6 days (to next Friday)
function weekEndingFriday(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr + "T00:00:00");
  if (isNaN(d)) return null;
  const day = d.getDay(); // Sun=0...Sat=6
  const add = (5 - day + 7) % 7;
  const f = new Date(d);
  f.setDate(d.getDate() + add);
  return f.toISOString().slice(0,10);
}
// Compute the Saturday week-ending (Sun-Sat week) for a YYYY-MM-DD date.
// Used by Time Clock ingestion to match B600's native "Last Week" convention
// (Sun → Sat). A Sunday date rolls forward to the following Saturday (6 days).
function weekEndingSaturday(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr + "T00:00:00");
  if (isNaN(d)) return null;
  const day = d.getDay(); // Sun=0...Sat=6
  // Saturday = 6. Roll forward to next Saturday (or 0 days if already Sat).
  const add = (6 - day + 7) % 7;
  const s = new Date(d);
  s.setDate(d.getDate() + add);
  return s.toISOString().slice(0,10);
}
function addDays(dateStr, n) { const d = new Date(dateStr+"T00:00:00"); d.setDate(d.getDate()+n); return d.toISOString().slice(0,10); }
function weekLabel(friday) { if (!friday) return "—"; const d = new Date(friday+"T00:00:00"); return d.toLocaleDateString("en-US",{month:"2-digit",day:"2-digit",year:"2-digit"}); }

// ─── Firebase Helpers ────────────────────────────────────────
const hasFirebase = typeof window !== "undefined" && window.db;
const FS = {
  async getCosts() { if (!hasFirebase) return null; try { const d=await window.db.collection("marginiq_config").doc("cost_structure").get(); return d.exists?d.data():null; } catch(e) { return null; } },
  async saveCosts(data) { if (!hasFirebase) return false; try { await window.db.collection("marginiq_config").doc("cost_structure").set({...data, updated_at:new Date().toISOString()}); return true; } catch(e) { return false; } },
  async getWeeklyRollups() { if (!hasFirebase) return []; try { const s=await window.db.collection("uline_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  // CRITICAL FIX (v2.25): Per-service-type merging.
  // Uline weekly files come in 3 flavors (delivery/truckload/accessorials) and
  // are ingested as SEPARATE files per week. When Gmail Sync imports each file
  // individually (different ingestFiles() calls), each call builds a rollup from
  // only its own stops and the naive {merge:true} write would overwrite the
  // other service-types' numbers with zeros. That's how a week ends up showing
  // 47 stops $3K (accessorials only) even though the 3,248-row delivery file
  // was ingested 1 second before.
  //
  // Fix: read existing doc, then for each service-type bucket, TAKE THE MAX
  // of the existing value and the incoming value. That way:
  //   - First file (delivery) writes delivery_stops=3248, truckload_stops=0, accessorial_stops=0
  //   - Second file (accessorials) writes delivery_stops=0, truckload_stops=0, accessorial_stops=47
  //   - Merged result: delivery_stops=3248 (keep), accessorial_stops=47 (take), total stops=3295
  //
  // Non-service-type fields (month, week_ending, customers, cities) use naive merge.
  async saveWeeklyRollup(weekId, data) {
    if (!hasFirebase) return false;
    try {
      const ref = window.db.collection("uline_weekly").doc(weekId);
      const existing = await ref.get();
      const cur = existing.exists ? existing.data() : {};
      // Per-service-type fields: take max of existing vs incoming (non-destructive)
      const maxField = (k) => Math.max(Number(cur[k] || 0), Number(data[k] || 0));
      const merged = {
        ...cur, ...data,
        // Per-service-type counts and revenue — never reduce below existing
        delivery_stops:    maxField("delivery_stops"),
        delivery_revenue:  maxField("delivery_revenue"),
        truckload_stops:   maxField("truckload_stops"),
        truckload_revenue: maxField("truckload_revenue"),
        accessorial_stops: maxField("accessorial_stops"),
        accessorial_revenue: maxField("accessorial_revenue"),
        // Total stops = sum of the three service-type stops (recomputed from merged)
        stops: 0, // placeholder — recomputed below
        revenue: 0, // placeholder — recomputed below
        base_revenue: maxField("base_revenue"),
        accessorial_count: maxField("accessorial_count"),
        weight: maxField("weight"),
        skids: maxField("skids"),
      };
      // Now recompute totals from merged per-service-type
      merged.stops = merged.delivery_stops + merged.truckload_stops + merged.accessorial_stops;
      merged.revenue = Math.round((merged.delivery_revenue + merged.truckload_revenue + merged.accessorial_revenue) * 100) / 100;
      await ref.set(merged, { merge: true });
      return true;
    } catch(e) { console.error("saveWeeklyRollup failed:", weekId, e); return false; }
  },
  async getReconWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("recon_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  // Same merging concern as saveWeeklyRollup — multiple files per week can
  // each produce recon deltas and we can't let them zero each other out.
  async saveReconWeekly(weekId, data) {
    if (!hasFirebase) return false;
    try {
      const ref = window.db.collection("recon_weekly").doc(weekId);
      const existing = await ref.get();
      const cur = existing.exists ? existing.data() : {};
      const maxField = (k) => Math.max(Number(cur[k] || 0), Number(data[k] || 0));
      const merged = {
        ...cur, ...data,
        billed: maxField("billed"),
        paid_matched: maxField("paid_matched"),
        unpaid_count: maxField("unpaid_count"),
        unpaid_amount: maxField("unpaid_amount"),
      };
      merged.collection_rate = merged.billed > 0 ? (merged.paid_matched / merged.billed * 100) : null;
      await ref.set(merged, { merge: true });
      return true;
    } catch(e) { console.error("saveReconWeekly failed:", weekId, e); return false; }
  },
  async getReconMeta() { if (!hasFirebase) return null; try { const d = await window.db.collection("marginiq_config").doc("recon_meta").get(); return d.exists?d.data():null; } catch(e) { return null; } },
  async saveReconMeta(data) { if (!hasFirebase) return false; try { await window.db.collection("marginiq_config").doc("recon_meta").set(data, {merge:true}); return true; } catch(e) { return false; } },
  async saveUnpaidStop(proKey, data) { if (!hasFirebase) return false; try { await window.db.collection("unpaid_stops").doc(String(proKey)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getUnpaidStops(limit=500) { if (!hasFirebase) return []; try { const s=await window.db.collection("unpaid_stops").orderBy("billed","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveDDISFile(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("ddis_files").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getDDISFiles() { if (!hasFirebase) return []; try { const s=await window.db.collection("ddis_files").orderBy("latest_bill_date","desc").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  // Per-PRO payment records. Keyed by pro+bill_date+amount so the same PRO
  // getting paid twice (partial, or adjustment) writes two rows. The audit
  // rebuild reads this collection to compute accurate billed-vs-paid variance.
  // Without these records, audit can only do file-level totals which misses
  // per-PRO short-pays — the main category the audit queue exists to surface.
  async saveDDISPayment(id, data) { if (!hasFirebase) return false; try { await window.db.collection("ddis_payments").doc(String(id)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async saveFileLog(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("file_log").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getFileLog(limit=500) { if (!hasFirebase) return []; try { const s=await window.db.collection("file_log").orderBy("uploaded_at","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },

  // ─── v2.3 Multi-source helpers ───────────────────────────────
  async saveNuVizzStop(proKey, data) { if (!hasFirebase) return false; try { await window.db.collection("nuvizz_stops").doc(String(proKey)).set(data, {merge:true}); return true; } catch(e) { return false; } },
  // v2.40.30: per-ingest diagnostic log so we can see exactly where rows drop
  async saveNuVizzIngestLog(runId, data) { if (!hasFirebase) return false; try { await window.db.collection("nuvizz_ingest_logs").doc(String(runId)).set(data, {merge:true}); return true; } catch(e) { console.error("saveNuVizzIngestLog failed:", e); return false; } },
  async getNuVizzIngestLogs(limit=10) { if (!hasFirebase) return []; try { const s=await window.db.collection("nuvizz_ingest_logs").orderBy("started_at","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveNuVizzWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("nuvizz_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getNuVizzWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("nuvizz_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveTimeClockDaily(id, data) { if (!hasFirebase) return false; try { await window.db.collection("timeclock_daily").doc(id).set(data, {merge:true}); return true; } catch(e) { console.error("saveTimeClockDaily failed:", id, e); return false; } },
  async saveTimeClockWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("timeclock_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { console.error("saveTimeClockWeekly failed:", weekId, e); return false; } },
  async getTimeClockWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("timeclock_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async savePayrollWeekly(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("payroll_weekly").doc(weekId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getPayrollWeekly() { if (!hasFirebase) return []; try { const s=await window.db.collection("payroll_weekly").orderBy("week_ending","desc").limit(260).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveQBOHistory(periodId, data) { if (!hasFirebase) return false; try { await window.db.collection("qbo_history").doc(periodId).set(data, {merge:true}); return true; } catch(e) { return false; } },
  async getQBOHistory() { if (!hasFirebase) return []; try { const s=await window.db.collection("qbo_history").orderBy("period","desc").limit(120).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveSourceFile(fileId, data) { if (!hasFirebase) return false; try { await window.db.collection("source_files").doc(fileId).set(data, {merge:true}); return true; } catch(e) { return false; } },

  // ─── Source Conflict Resolution (Uline vs Davis corrections) ───────────
  // When the same week has files from both @uline.com AND billing@davisdelivery.com,
  // the ingest pipeline writes a conflict doc. User reviews + picks winner in the
  // Data Ingest tab. Resolved conflicts stay in the collection for audit history.
  async saveSourceConflict(weekId, data) { if (!hasFirebase) return false; try { await window.db.collection("source_conflicts").doc(weekId).set(data, {merge:true}); return true; } catch(e) { console.error("saveSourceConflict failed:", weekId, e); return false; } },
  async getSourceConflicts() { if (!hasFirebase) return []; try { const s=await window.db.collection("source_conflicts").orderBy("week_ending","desc").limit(200).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async updateSourceConflict(weekId, patch) { if (!hasFirebase) return false; try { await window.db.collection("source_conflicts").doc(weekId).update(patch); return true; } catch(e) { console.error("updateSourceConflict failed:", weekId, e); return false; } },

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
  // v2.40.29: transaction-level storage for per-card / per-driver / per-product (diesel/DEF/gas) analysis
  async getFuelTransactions(limit=5000) { if (!hasFirebase) return []; try { const s=await window.db.collection("fuel_transactions").orderBy("txn_date","desc").limit(limit).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async getFuelTransactionsByInvoice(invoiceId) { if (!hasFirebase) return []; try { const s=await window.db.collection("fuel_transactions").where("invoice_id","==",invoiceId).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveFuelTransaction(id, data) { if (!hasFirebase) return false; try { await window.db.collection("fuel_transactions").doc(String(id)).set(data, {merge:true}); return true; } catch(e) { return false; } },
};

// ─── File Type Detection ────────────────────────────────────
function detectFileType(filename, firstRow) {
  const fn = filename.toLowerCase();
  // Uline family (existing)
  if (fn.startsWith("ddis820") || fn.includes("ddis")) return "ddis";
  if (fn.startsWith("master")) return "master";
  if (fn.includes("accessorial") || fn.includes("acesorial") || fn.includes("acessorial") || fn.includes("accesorial") || fn.includes("acceessorial") || fn.includes("accessiorial")) return "accessorials";
  if (fn.startsWith("das") || fn.startsWith("das ")) return "original";
  // NuVizz family
  if (fn.includes("driver_stops") || fn.includes("driver stops") || fn.includes("nuvizz")) return "nuvizz";
  // Time clock / SENTINEL — accept space, underscore, hyphen, or no separator
  if (/sentinel|time[\s_-]?clock|timeclock|b600|punch/i.test(fn)) return "timeclock";
  // Payroll (CyberPay)
  if (/cyberpay|payroll|pay[\s_-]?detail|paydetail|pay[\s_-]?register|payregister/i.test(fn)) return "payroll";
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
    // Uline original/accessorial — accepts both modern ("new cost" present) and
    // legacy ("cost" only, no "new cost" column) format variants. The parser in
    // parseOriginalOrAccessorial already falls back to cost when new_cost is
    // absent, so detection just needs to confirm this is Uline-shaped data.
    // Required signature: pro/pro# + at least a few revenue-related columns.
    if (keys.some(k => k === "pro" || k === "pro#") && (keySet.has("new cost") || keySet.has("cost"))) {
      // Accessorials are identified either by an explicit "code" column value
      // on the first data row, or by a filename-level hint (checked upstream
      // in detectServiceType/filename routing). Absent both, treat as original.
      return firstRow.code ? "accessorials" : "original";
    }
  }
  return "unknown";
}

// Distinguish Uline service type from the filename. Does NOT affect what
// detectFileType returns — this is an orthogonal axis used for revenue
// categorization and audit grouping.
//   "truckload"   → filename has " TK" or "-TK" suffix (case-insensitive)
//   "accessorial" → accessorial file (extra charges)
//   "delivery"    → regular stop-by-stop weekly file (default)
// Note: "JO" suffix is intentionally NOT detected — per user rule, JO files
// are just regular weekly files (different employee initials in filename),
// so they're treated as "delivery" by default.
function detectServiceType(filename, kind) {
  if (kind === "accessorials") return "accessorial";
  const fn = filename.toLowerCase();
  // Match " TK" or "-TK" followed by word boundary/extension/end — avoid false
  // positives on "TK" embedded in other words. Handles cases like:
  //   "das 20250503 - 20250509 TK.xlsx" → match
  //   "das 20250503-20250509TK.xlsx"    → match
  //   "das 20250503 - 20250509 STK.xlsx" → no match (preceded by S)
  if (/(?:^|[\s\-_])tk(?=\.[a-z]+$|[\s\-_])/i.test(fn)) return "truckload";
  return "delivery";
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
        // Check for Uline-style meta row on row 0 (cells contain patterns like
        // "Num,0 (No Blanks)", "CAPS (COD,$$$)", etc). If detected, skip one row
        // so the REAL headers (pro, order, customer, cost, new cost, code, ...)
        // become the sheet_to_json keys.
        const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
        let skipRows = 0;
        if (raw.length >= 2) {
          const r0 = (raw[0] || []).filter(v => v != null).map(v => String(v).toLowerCase());
          const metaHints = r0.filter(v =>
            /^num\s*,/.test(v) || /no blanks/.test(v) || /^caps\s/.test(v) || v.startsWith("not blank")
          ).length;
          // If most of row 0 looks like meta-descriptors, skip it
          if (metaHints >= 3 && r0.length >= 5) skipRows = 1;
        }
        const rows = XLSX.utils.sheet_to_json(ws, { defval: null, range: skipRows });
        const norm = rows.map(r => { const o = {}; Object.keys(r).forEach(k => o[String(k).toLowerCase().trim()] = r[k]); return o; });
        resolve(norm);
      } catch(err) { reject(err); }
    };
    r.onerror = reject;
    r.readAsArrayBuffer(file);
  });
}
async function readCSV(file) {
  // Native CSV parser — avoids XLSX.read which recursively traverses the whole
  // parsed workbook and overflows the stack on files >~10MB / >100K rows.
  // Handles: quoted fields, embedded commas/quotes/newlines, CRLF, BOM, empty lines.
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = e => {
      try {
        let text = e.target.result;
        // Strip UTF-8 BOM
        if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
        const rows = parseCSVStream(text);
        if (rows.length === 0) return resolve([]);
        const headers = rows[0].map(h => String(h || "").toLowerCase().trim());
        const out = new Array(rows.length - 1);
        for (let i = 1; i < rows.length; i++) {
          const row = rows[i];
          // Skip completely empty lines
          if (row.length === 1 && row[0] === "") { out[i-1] = null; continue; }
          const obj = {};
          for (let j = 0; j < headers.length; j++) {
            const v = row[j];
            obj[headers[j]] = (v === undefined || v === "") ? null : v;
          }
          out[i-1] = obj;
        }
        resolve(out.filter(Boolean));
      } catch(err) { reject(err); }
    };
    r.onerror = reject;
    r.readAsText(file);
  });
}

// Iterative RFC-4180 CSV parser. Handles quoted fields, embedded commas,
// escaped quotes ("" inside quoted), and both \n and \r\n line endings.
// Returns an array of arrays.
function parseCSVStream(text) {
  const rows = [];
  let cur = [];
  let field = "";
  let inQuotes = false;
  const len = text.length;
  for (let i = 0; i < len; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i+1] === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else {
        field += c;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
      } else if (c === ",") {
        cur.push(field); field = "";
      } else if (c === "\n") {
        cur.push(field); field = "";
        rows.push(cur); cur = [];
      } else if (c === "\r") {
        // Swallow \r; the following \n (if any) will push the row.
        // If standalone \r (old Mac), treat as line terminator.
        if (text[i+1] !== "\n") {
          cur.push(field); field = "";
          rows.push(cur); cur = [];
        }
      } else {
        field += c;
      }
    }
  }
  // Flush final field/row
  if (field !== "" || cur.length > 0) {
    cur.push(field);
    rows.push(cur);
  }
  return rows;
}

// ─── Auto-rename typo'd files in memory ─────────────────────
// For each file whose audit result has a suggestedRename, wrap it in a fresh
// File object with the corrected name. The file on disk is unchanged — only
// the in-memory copy used for this ingest has the clean name. This means the
// detection logic downstream sees clean filenames and duplicates/gaps analysis
// produces accurate results.
function applyTypoFixes(files, audit) {
  const renameMap = new Map(); // originalName -> suggestedRename
  const fixed = [];
  for (const c of audit.classified) {
    if (c.parseStatus.startsWith("typo") && c.suggestedRename && c.suggestedRename !== c.name) {
      renameMap.set(c.name, c.suggestedRename);
    }
  }
  for (const f of files) {
    const newName = renameMap.get(f.name);
    if (newName) {
      // File constructor requires the fetch as Blob. Copy the underlying bytes.
      const rewrapped = new File([f], newName, { type: f.type || "application/octet-stream" });
      // Preserve our custom tags
      if (f._zipSource) rewrapped._zipSource = f._zipSource;
      if (f._isPdf) rewrapped._isPdf = f._isPdf;
      rewrapped._originalName = f.name; // for audit history so we can show the fix
      fixed.push(rewrapped);
    } else {
      fixed.push(f);
    }
  }
  return { files: fixed, renameMap };
}

// ─── Zip unpacking ──────────────────────────────────────────
// If the user drops a .zip into Data Ingest, we unpack it in the browser and
// return an array of File-shaped blobs — same interface readCSV/readWorkbook
// already accept, so nothing downstream has to change.
//
// Also extracts CSV/XLSX/PDF attachments from .eml (email) files, so raw email
// saves from Gmail that contain data attachments work without manual extraction.
async function unzipIfNeeded(files) {
  const out = [];
  for (const f of files) {
    const name = (f.name || "").toLowerCase();
    if (name.endsWith(".eml")) {
      // Email file — try to extract data attachments directly
      try {
        const extracted = await extractAttachmentsFromEml(f);
        for (const e of extracted) out.push(e);
      } catch(err) {
        console.warn("Failed to extract from .eml:", f.name, err);
        // Push the .eml through so audit can flag it
        out.push(f);
      }
      continue;
    }
    if (!name.endsWith(".zip")) { out.push(f); continue; }
    if (typeof JSZip === "undefined") {
      throw new Error("JSZip not loaded — cannot unpack .zip file");
    }
    const zip = await JSZip.loadAsync(f);
    const entries = Object.values(zip.files);
    for (const entry of entries) {
      if (entry.dir) continue;
      const entryName = entry.name;
      // Skip macOS resource fork files and hidden files
      if (entryName.includes("__MACOSX/")) continue;
      const base = entryName.split("/").pop();
      if (!base || base.startsWith(".") || base.startsWith("._")) continue;
      const lower = base.toLowerCase();
      const isData = lower.endsWith(".xlsx") || lower.endsWith(".xls") || lower.endsWith(".csv");
      const isPdf = lower.endsWith(".pdf");
      const isEml = lower.endsWith(".eml");
      if (!isData && !isPdf && !isEml) continue;
      const blob = await entry.async("blob");
      if (isEml) {
        // Zip contained an .eml — extract its attachments too
        try {
          const emlFile = new File([blob], base, { type: "message/rfc822" });
          const extracted = await extractAttachmentsFromEml(emlFile);
          for (const e of extracted) {
            e._zipSource = f.name;
            out.push(e);
          }
        } catch(err) {
          console.warn("Failed to extract .eml in zip:", base, err);
        }
        continue;
      }
      // Wrap in a File so the existing FileReader code paths work unchanged
      const fakeFile = new File([blob], base, { type: blob.type || "application/octet-stream" });
      fakeFile._zipSource = f.name;
      fakeFile._isPdf = isPdf;
      out.push(fakeFile);
    }
  }
  return out;
}

// Parse an .eml file and extract data-type attachments (csv/xlsx/xls/pdf).
// Handles multipart MIME messages with base64 or quoted-printable encoded
// attachments. Returns an array of File objects.
async function extractAttachmentsFromEml(emlFile) {
  const text = await emlFile.text();
  // Split headers from body
  const headerEnd = text.search(/\r?\n\r?\n/);
  if (headerEnd === -1) return [];
  const headers = text.slice(0, headerEnd);
  const body = text.slice(headerEnd).replace(/^\r?\n\r?\n/, "");

  // Find the top-level boundary
  const boundaryMatch = headers.match(/boundary="?([^";\r\n]+)"?/i);
  if (!boundaryMatch) return [];

  const extracted = [];
  // Recursively walk parts — attachments can be nested in multipart/mixed → multipart/alternative etc.
  walkParts(body, boundaryMatch[1], extracted);
  return extracted;
}

function walkParts(body, boundary, outList) {
  const parts = body.split(new RegExp(`--${boundary.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?:--)?\\s*`));
  for (const part of parts) {
    if (!part.trim()) continue;
    const headerEnd = part.search(/\r?\n\r?\n/);
    if (headerEnd === -1) continue;
    const partHeaders = part.slice(0, headerEnd);
    let partBody = part.slice(headerEnd).replace(/^\r?\n\r?\n/, "");

    // Is this a nested multipart?
    const nestedBoundary = partHeaders.match(/content-type:[^;]*multipart[^;]*;\s*[\s\S]*?boundary="?([^";\r\n]+)"?/i);
    if (nestedBoundary) {
      walkParts(partBody, nestedBoundary[1], outList);
      continue;
    }

    // Look for an attachment filename
    const fnMatch = partHeaders.match(/filename\*?=\s*"?([^";\r\n]+)"?/i);
    if (!fnMatch) continue;
    const filename = fnMatch[1].trim();
    const lower = filename.toLowerCase();
    const isData = lower.endsWith(".csv") || lower.endsWith(".xlsx") || lower.endsWith(".xls");
    const isPdf = lower.endsWith(".pdf");
    if (!isData && !isPdf) continue;

    // Decode based on Content-Transfer-Encoding
    const encMatch = partHeaders.match(/content-transfer-encoding:\s*([^\r\n]+)/i);
    const encoding = encMatch ? encMatch[1].trim().toLowerCase() : "7bit";
    let bytes;
    if (encoding === "base64") {
      // Strip whitespace from base64
      const b64 = partBody.replace(/\s+/g, "");
      try {
        const bin = atob(b64);
        bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      } catch(e) { continue; }
    } else if (encoding === "quoted-printable") {
      const decoded = partBody
        .replace(/=\r?\n/g, "")
        .replace(/=([0-9A-F]{2})/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
      bytes = new TextEncoder().encode(decoded);
    } else {
      // 7bit, 8bit, binary — use as-is
      bytes = new TextEncoder().encode(partBody);
    }

    const blob = new Blob([bytes], { type: isPdf ? "application/pdf" : "text/csv" });
    const fakeFile = new File([blob], filename, { type: blob.type });
    fakeFile._isPdf = isPdf;
    fakeFile._extractedFromEml = emlFile => emlFile;
    outList.push(fakeFile);
  }
}

// ─── Filename audit ─────────────────────────────────────────
// Runs BEFORE ingestion. Classifies every file by parseable date range,
// flags typos/duplicates/gaps, so the user can review before anything hits
// Firestore. Returns a structured audit report.
function auditFilenames(files) {
  // Parse: "DAS 20250104-20250110" or "DAS 20250104 - 20250110" variants.
  // Use (?<!\d) / (?!\d) instead of \b so boundaries between letters and digits
  // still match (e.g. "das2025012-20250718" where no space separates prefix from date).
  const patternGood = /(?<!\d)(\d{8})\s*-\s*(\d{8})(?!\d)/;
  // Typo patterns we want to explicitly surface:
  const patternBad7To8 = /(?<!\d)(\d{7})\s*-\s*(\d{8})(?!\d)/;              // "0240608-20240614" or "das2025012-20250718"
  const patternBad8To9 = /(?<!\d)(\d{8})\s*-\s*(\d{9})(?!\d)/;              // "20250315 - 220250321"
  const patternDoubleDash = /(?<!\d)(\d{8})\s*-\s*-\s*(\d{8})(?!\d)/;       // "20250324- -20250328"
  const patternSlashDate = /(?<!\d)(\d{2})(\d{2})(\d{4})\s*-\s*(\d{2})(\d{2})(\d{4})(?!\d)/; // "03012025 - 03072025"

  const parseYMD = (s) => {
    try {
      const y = parseInt(s.slice(0, 4), 10), m = parseInt(s.slice(4, 6), 10), d = parseInt(s.slice(6, 8), 10);
      if (y < 2000 || y > 2100 || m < 1 || m > 12 || d < 1 || d > 31) return null;
      return `${y}-${String(m).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
    } catch { return null; }
  };

  const classified = [];
  for (const f of files) {
    const name = f.name;
    const lower = name.toLowerCase();
    const ext = lower.split(".").pop();
    const isPdf = ext === "pdf";
    const isBackup = /backup/i.test(name);
    const isAccessorial = /accessorial|acessorial|accesorial|acesorial|acceessorial|accessiorial/i.test(name);
    const isDispute = /dispute/i.test(name);
    const isInvoicePdf = /^ra#|_invoice|backup accessorials|back accessorials/i.test(name);

    let startDate = null, endDate = null, weekEnding = null;
    let parseStatus = "ok"; // ok | typo-7digit | typo-9digit | typo-doubledash | typo-slashdate | unparsed | pdf-no-date
    let suggestedRename = null;

    // Try good pattern first
    let m = name.match(patternGood);
    if (m) {
      startDate = parseYMD(m[1]);
      endDate = parseYMD(m[2]);
      // If YYYYMMDD parse failed (e.g. "02092026" → year 0209 invalid), try
      // interpreting both as MMDDYYYY: "02092026" → Feb 09 2026 → "20260209"
      if (!startDate || !endDate) {
        const asMMDDYYYY = (s) => {
          const mm = s.slice(0,2), dd = s.slice(2,4), yyyy = s.slice(4,8);
          return parseYMD(yyyy + mm + dd);
        };
        const s2 = asMMDDYYYY(m[1]);
        const e2 = asMMDDYYYY(m[2]);
        if (s2 && e2) {
          parseStatus = "typo-mmddyyyy";
          startDate = s2;
          endDate = e2;
          // Rename: replace each 8-digit block with its YYYYMMDD equivalent
          const startYMD = s2.replace(/-/g, "");
          const endYMD = e2.replace(/-/g, "");
          suggestedRename = name.replace(m[1], startYMD).replace(m[2], endYMD);
        }
      }
      // If still valid but end < start, check for year-off-by-one typo
      // (e.g. "20251229 - 20250102" where end should be 20260102)
      if (startDate && endDate && endDate < startDate) {
        // Try bumping end year by 1
        const endParts = m[2];
        const bumpedYear = String(parseInt(endParts.slice(0,4),10) + 1);
        const bumpedEnd = parseYMD(bumpedYear + endParts.slice(4));
        if (bumpedEnd) {
          // Sanity check: bumped end should be within ~2 weeks of start
          const days = (new Date(bumpedEnd) - new Date(startDate)) / 86400000;
          if (days > 0 && days < 15) {
            parseStatus = "typo-yearoff";
            endDate = bumpedEnd;
            const fixedEnd = bumpedYear + endParts.slice(4);
            suggestedRename = name.replace(m[2], fixedEnd);
          }
        }
      }
    } else if ((m = name.match(patternBad7To8))) {
      parseStatus = "typo-7digit";
      // Heuristic 1: leading-zero typo. "0240608" → "20240608"
      let fixedStart = null;
      if (m[1].length === 7 && m[1].startsWith("0")) fixedStart = "2" + m[1];
      // Heuristic 2: try parsing end date, subtract 6 days, see if that matches 7 digits of truth
      if (!fixedStart) {
        const endParsed = parseYMD(m[2]);
        if (endParsed) {
          const startFromEnd = addDays(endParsed, -6).replace(/-/g, ""); // YYYYMMDD
          // Does startFromEnd look similar to m[1]? e.g. m[1]="2025012", startFromEnd="20250712"
          // Count matching characters
          let matches = 0;
          for (let i = 0; i < Math.min(m[1].length, startFromEnd.length); i++) {
            if (m[1][i] === startFromEnd[i]) matches++;
          }
          if (matches >= 5) fixedStart = startFromEnd;
        }
      }
      if (fixedStart) {
        startDate = parseYMD(fixedStart);
        endDate = parseYMD(m[2]);
        // Build a sensible suggested rename
        const before = name.indexOf(m[1]);
        if (before >= 0) {
          suggestedRename = name.slice(0, before) + fixedStart + name.slice(before + m[1].length);
          // Also normalize "das2025012" style (no space between das and date)
          suggestedRename = suggestedRename.replace(/^das(\d)/i, "das $1");
        }
      }
    } else if ((m = name.match(patternBad8To9))) {
      parseStatus = "typo-9digit";
      // 9-digit end date likely has duplicated digit. e.g. "220250321" → "20250321"
      const maybeFix = m[2].length === 9 && m[2].startsWith("2") && m[2][1] === "2" ? m[2].slice(1) : null;
      if (maybeFix) {
        startDate = parseYMD(m[1]);
        endDate = parseYMD(maybeFix);
        suggestedRename = name.replace("- " + m[2], "- " + maybeFix).replace("-" + m[2], "-" + maybeFix);
      }
    } else if ((m = name.match(patternDoubleDash))) {
      parseStatus = "typo-doubledash";
      startDate = parseYMD(m[1]);
      endDate = parseYMD(m[2]);
      suggestedRename = name.replace(/(\d{8})-\s*-\s*(\d{8})/, "$1-$2");
    } else if ((m = name.match(patternSlashDate))) {
      parseStatus = "typo-slashdate";
      // "03012025" MM/DD/YYYY → rebuild as YYYYMMDD
      const s = `${m[3]}${m[1]}${m[2]}`;
      const e = `${m[6]}${m[4]}${m[5]}`;
      startDate = parseYMD(s);
      endDate = parseYMD(e);
      suggestedRename = name.replace(m[0], `${s}-${e}`);
    } else {
      // No date range found. Some file types legitimately don't have one (DDIS
      // files, dispute logs, PDF backups, time clock CSVs, payroll exports,
      // QBO statements) — those aren't "unparsed", they just don't need a
      // week-ending. Only flag as unparsed if we can't otherwise classify
      // the file.
      const hasDdisName = /^ddis820|\bddis\b/i.test(lower);
      const hasDisputeName = /dispute/i.test(name);
      const hasNuVizzName = /nuvizz|driver.?stops/i.test(name);
      const hasTimeClockName = /sentinel|time[\s_-]?clock|timeclock|b600|punch/i.test(name);
      const hasPayrollName = /cyberpay|payroll|pay[\s_-]?detail|paydetail|pay[\s_-]?register|payregister/i.test(name);
      const hasQboName = /profit.+loss|p&l|p_l|trial[\s_-]?balance|tb_|general[\s_-]?ledger|_gl_|quickbooks|qbo_/i.test(name);
      if (hasDdisName || hasDisputeName || hasNuVizzName || hasTimeClockName || hasPayrollName || hasQboName || isPdf) parseStatus = "no-date-ok";
      else parseStatus = "unparsed";
    }

    // Compute Friday week-ending if we have an end date
    if (endDate) {
      weekEnding = weekEndingFriday(endDate);
    }

    // Classify kind
    let kind;
    if (isDispute) kind = "uline-dispute";
    else if (isBackup || isInvoicePdf) kind = "pdf-backup";
    else if (isPdf) kind = "pdf-other";
    else if (/^ddis820|\bddis\b/i.test(lower)) kind = "ddis";
    else if (/sentinel|time[\s_-]?clock|timeclock|b600|punch/i.test(name)) kind = "timeclock";
    else if (/cyberpay|payroll|pay[\s_-]?detail|paydetail|pay[\s_-]?register|payregister/i.test(name)) kind = "payroll";
    else if (/profit.+loss|p&l|p_l|trial[\s_-]?balance|tb_|general[\s_-]?ledger|_gl_|quickbooks|qbo_/i.test(name)) kind = "qbo";
    else if (/nuvizz|driver.?stops/i.test(name)) kind = "nuvizz";
    else if (isAccessorial) kind = "accessorial";
    else kind = "original";

    // Classify service_type: "truckload" if TK suffix, "accessorial" if accessorial file,
    // else "delivery" (regular weekly — includes JO-suffixed files since those are
    // just different employee initials per user rule).
    let serviceType;
    if (kind === "accessorial") serviceType = "accessorial";
    else if (/(?:^|[\s\-_])tk(?=\.[a-z]+$|[\s\-_])/i.test(lower)) serviceType = "truckload";
    else serviceType = "delivery";

    classified.push({
      file: f, name,
      kind, isPdf, serviceType,
      startDate, endDate, weekEnding,
      parseStatus, suggestedRename,
      size: f.size || 0,
    });
  }

  // Group data files by week-ending AND service_type. A regular + TK for the
  // same week is NOT a duplicate — they're different billing streams that both
  // need to be ingested for that week.
  const byWeek = {};
  for (const c of classified) {
    if (c.isPdf) continue;
    if (c.kind !== "original" && c.kind !== "accessorial") continue;
    if (!c.weekEnding) continue;
    if (!byWeek[c.weekEnding]) byWeek[c.weekEnding] = { delivery: [], truckload: [], accessorial: [] };
    byWeek[c.weekEnding][c.serviceType].push(c);
  }

  // Find duplicates: multiple files with same (week, serviceType).
  // Regular + TK for the same week is NOT a duplicate.
  const duplicates = [];
  for (const [we, g] of Object.entries(byWeek)) {
    if (g.delivery.length > 1) duplicates.push({ weekEnding: we, serviceType: "delivery", kind: "original (regular delivery)", files: g.delivery });
    if (g.truckload.length > 1) duplicates.push({ weekEnding: we, serviceType: "truckload", kind: "original (truckload)", files: g.truckload });
    if (g.accessorial.length > 1) duplicates.push({ weekEnding: we, serviceType: "accessorial", kind: "accessorial", files: g.accessorial });
  }

  // Find missing accessorials: weeks where we have ANY delivery or truckload
  // file but NO accessorial file. (A week can still be "complete" for billing
  // purposes without an accessorial — it just means no extra charges that week.)
  const missingAccessorials = Object.entries(byWeek)
    .filter(([, g]) => (g.delivery.length > 0 || g.truckload.length > 0) && g.accessorial.length === 0)
    .map(([we]) => we)
    .sort();

  // Find week gaps using contiguous-run detection. Ignores isolated stray files
  // (e.g., a lone 2023 file when the main data is 2025), which otherwise would
  // make every week between them look "missing".
  const weeksCovered = Object.keys(byWeek).sort();
  const missingWeeks = [];
  if (weeksCovered.length >= 4) {
    // Split sorted weeks into runs — consecutive = no gap > 28 days
    const runs = [];
    let cur = [weeksCovered[0]];
    for (let i = 1; i < weeksCovered.length; i++) {
      const prev = weeksCovered[i-1];
      const w = weeksCovered[i];
      const gapDays = (new Date(w+"T00:00:00") - new Date(prev+"T00:00:00")) / 86400000;
      if (gapDays <= 28) cur.push(w);
      else { runs.push(cur); cur = [w]; }
    }
    runs.push(cur);
    // Only look for gaps inside substantial runs (4+ weeks) — single stray
    // files or short runs don't generate noisy "missing week" alerts.
    for (const run of runs) {
      if (run.length < 4) continue;
      let x = run[0];
      const end = run[run.length - 1];
      while (x <= end) {
        if (!byWeek[x]) missingWeeks.push(x);
        x = addDays(x, 7);
      }
    }
  }

  const typos = classified.filter(c => c.parseStatus.startsWith("typo"));
  const unparsed = classified.filter(c => c.parseStatus === "unparsed");

  return {
    classified,
    byWeek,
    summary: {
      total: classified.length,
      // Break down 'originals' by service type. "delivery" = regular weekly files
      // (includes JO-suffixed variants). "truckload" = TK-suffixed files.
      delivery: classified.filter(c => c.kind === "original" && c.serviceType === "delivery" && c.weekEnding).length,
      truckload: classified.filter(c => c.kind === "original" && c.serviceType === "truckload" && c.weekEnding).length,
      originals: classified.filter(c => c.kind === "original" && c.weekEnding).length, // = delivery + truckload
      accessorials: classified.filter(c => c.kind === "accessorial" && c.weekEnding).length,
      ddis: classified.filter(c => c.kind === "ddis").length,
      pdfs: classified.filter(c => c.isPdf).length,
      typos: typos.length,
      unparsed: unparsed.length,
      duplicates: duplicates.length,
      missingAccessorials: missingAccessorials.length,
      missingWeeks: missingWeeks.length,
      weeksCovered: weeksCovered.length,
    },
    typos,
    unparsed,
    duplicates,
    missingAccessorials,
    missingWeeks,
  };
}

function normalizePro(v) {
  if (v == null) return null;
  let s = String(v).trim();
  if (s === "") return null;
  s = s.replace(/\.0+$/,"");
  const stripped = s.replace(/^0+/,"");
  return stripped || s;
}

// Parse a Uline delivery/accessorial/truckload spreadsheet into stop records.
//
// Revenue source of truth: NEW COST column. For every row produced here,
// stop.new_cost is the authoritative billed amount that will flow into the
// weekly rollup. If a row only has the legacy "cost" column (older file
// format), that value is used as the fallback. Downstream rollup code
// (bw.revenue, bw.delivery_revenue, etc.) sums new_cost — never extra_cost
// or base cost — so "new cost" is the universal revenue figure.
//
// Service-type determination is PER-ROW, not per-file. This matters because
// Uline's going-forward format is a single file per week containing delivery
// rows AND accessorial rows AND (sometimes) truckload rows, all mixed. Old
// multi-file weeks still work because each file is single-type by nature.
//
// Per-row rule:
//   - Truckload file (filename has TK suffix): every row is truckload.
//     This is file-level because TK rows don't have a distinguishing
//     content marker beyond the filename.
//   - Row has a non-empty `code` field: accessorial (e.g. "DET", "INS",
//     "LIF", any non-blank charge code). Covers both the old accessorials-
//     only files (every row has a code) and combined-file accessorial
//     lines interleaved with delivery lines.
//   - Otherwise: delivery. This is the default for stop-by-stop freight.
function parseOriginalOrAccessorial(rows, serviceType) {
  const stops = [];
  const fileST = serviceType || "delivery";
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
    const codeStr = r.code ? String(r.code).trim() : null;
    const hasCode = !!(codeStr && codeStr.length > 0);
    // Per-row service type. TK wins if the file is a truckload file; otherwise
    // code presence classifies accessorial vs delivery on a per-row basis.
    let rowST;
    if (fileST === "truckload") rowST = "truckload";
    else if (hasCode) rowST = "accessorial";
    else rowST = "delivery";
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
      // new_cost is ALWAYS the source of truth for revenue. cost is retained
      // for base-vs-accessorial analysis in older per-row files; new_cost
      // falls back to cost if empty (older legacy files have no new_cost col).
      cost, new_cost: newCost || cost, extra_cost: extraCost,
      warehouse: r.wh ? String(r.wh).trim() : null,
      skid, loose, weight: wgt,
      via: r.via ? String(r.via).trim() : null,
      code: codeStr,
      is_accessorial: hasCode,
      service_type: rowST, // "delivery" | "truckload" | "accessorial" — per row
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

// ─── DDIS bill-week computation (v2.40.9, extended v2.40.12) ──────────
// Uline's weekly 820 remit: one file = one Friday-week, BUT each file also
// includes straggler PROs from older invoices paid in the same settlement.
// Date spans on a file can be anything from 0 days to 900+ days of stragglers.
//
// Rule (per Chad): take the top 5 most-common bill_dates in the file —
// those are the week this file is primarily settling. Bucket each of those
// 5 dates to its Sat→Fri envelope (Friday-ending), pick the envelope that
// holds the most top-5 rows. That envelope's Friday = file's bill_week_ending.
//
// v2.40.12 adds `covers_weeks`: the list of Friday-envelopes (inclusive of
// bill_week_ending) where this file contributes ≥ COVERS_PCT of its TOTAL
// row count. This catches consolidation files like Thanksgiving where one
// DDIS pays two DAS weeks — the winning week is the bulk, but a secondary
// week can still have 30%+ of the rows. Normal files: covers_weeks ==
// [bill_week_ending]. Consolidation files: more than one entry.
//
// Inputs: billDates = array of "YYYY-MM-DD" strings (may be empty, may have dupes).
// Output: { bill_week_ending, week_ambiguous, ambiguous_candidates, top5, covers_weeks }
//   bill_week_ending: "YYYY-MM-DD" (Friday) or null if undeterminable/ambiguous
//   week_ambiguous: true only on tie (requires user resolution)
//   ambiguous_candidates: [{friday, rows}] when ambiguous, else []
//   top5: [{date, count}] for debugging/transparency on the file card
//   covers_weeks: ["YYYY-MM-DD", …] Friday ends where file has ≥20% of rows
function fridayEndOf(iso) {
  const d = new Date(iso + "T00:00:00");
  if (isNaN(d.getTime())) return null;
  const day = d.getDay(); // 0=Sun..6=Sat
  const daysTo = (5 - day + 7) % 7;
  d.setDate(d.getDate() + daysTo);
  return d.toISOString().slice(0, 10);
}
const COVERS_PCT = 0.20; // v2.40.12: ≥20% of total rows = file covers that week
function computeBillWeekEnding(billDates) {
  const result = {
    bill_week_ending: null,
    week_ambiguous: false,
    ambiguous_candidates: [],
    top5: [],
    covers_weeks: [],
  };
  if (!Array.isArray(billDates) || billDates.length === 0) return result;
  // Count rows per date
  const dateCounts = new Map();
  for (const bd of billDates) {
    if (!bd) continue;
    dateCounts.set(bd, (dateCounts.get(bd) || 0) + 1);
  }
  if (dateCounts.size === 0) return result;
  const totalRows = billDates.filter(Boolean).length;
  // Top 5 most-common dates
  const sorted = [...dateCounts.entries()].sort((a, b) => b[1] - a[1]);
  const top5 = sorted.slice(0, 5).map(([date, count]) => ({ date, count }));
  result.top5 = top5;
  // Bucket each top-5 date to its Fri-ending envelope, sum row counts per envelope
  const envelopeCounts = new Map();
  for (const { date, count } of top5) {
    const fri = fridayEndOf(date);
    if (!fri) continue;
    envelopeCounts.set(fri, (envelopeCounts.get(fri) || 0) + count);
  }
  if (envelopeCounts.size === 0) return result;
  // Winner = envelope with most rows. Tie → flag ambiguous.
  const envs = [...envelopeCounts.entries()].sort((a, b) => b[1] - a[1]);
  if (envs.length > 1 && envs[0][1] === envs[1][1]) {
    result.week_ambiguous = true;
    const tieCount = envs[0][1];
    result.ambiguous_candidates = envs.filter(([, n]) => n === tieCount).map(([friday, rows]) => ({ friday, rows }));
  } else {
    result.bill_week_ending = envs[0][0];
  }
  // v2.40.12: compute covers_weeks across ALL bill_dates (not just top 5)
  // so we catch consolidation files where a secondary week has 20-40% of rows.
  const allEnvelopeCounts = new Map();
  for (const [bd, count] of dateCounts.entries()) {
    const fri = fridayEndOf(bd);
    if (!fri) continue;
    allEnvelopeCounts.set(fri, (allEnvelopeCounts.get(fri) || 0) + count);
  }
  const threshold = Math.max(1, Math.ceil(totalRows * COVERS_PCT));
  const coversSet = new Set();
  for (const [fri, count] of allEnvelopeCounts.entries()) {
    if (count >= threshold) coversSet.add(fri);
  }
  // Always include the winning bill_week_ending even if slightly under threshold
  // (e.g. ambiguous files still get their top candidates included so stops for
  // those weeks aren't orphaned from the audit).
  if (result.bill_week_ending) coversSet.add(result.bill_week_ending);
  for (const c of result.ambiguous_candidates) coversSet.add(c.friday);
  result.covers_weeks = [...coversSet].sort();
  return result;
}


// v2.40.34: Server-side NuVizz ingest — replaces client-side browser saves.
// Sends all parsed stops to marginiq-nuvizz-ingest function which writes
// to Firestore using the :commit REST endpoint in 500-doc batches.
// Background function has 15-min budget; no mobile Safari timeouts, no SDK hangs.
async function serverSaveNuVizzStops(stops, source, onStatus) {
  if (stops.length === 0) return { run_id: null, saved_ok: 0 };
  onStatus({ phase:"save", message:`Sending ${stops.length.toLocaleString()} stops to server for ingest...` });
  const resp = await fetch("/.netlify/functions/marginiq-nuvizz-ingest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stops, source }),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Server ingest dispatch failed (${resp.status}): ${err}`);
  }
  const { run_id, stop_count } = await resp.json();
  onStatus({ phase:"save", message:`Server ingest started — ${stop_count?.toLocaleString()} stops queued. Polling for progress...` });

  // Poll status until complete
  let attempts = 0;
  const MAX_WAIT_MS = 15 * 60 * 1000; // 15 min
  const POLL_INTERVAL_MS = 3000;
  const started = Date.now();
  while (Date.now() - started < MAX_WAIT_MS) {
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
    attempts++;
    try {
      const sResp = await fetch(`/.netlify/functions/marginiq-nuvizz-ingest?action=status&run_id=${run_id}`);
      if (!sResp.ok) continue;
      const status = await sResp.json();
      if (status.progress_text) {
        onStatus({ phase:"save", message:status.progress_text });
      }
      if (status.state === "complete" || status.state === "failed") {
        return { run_id, saved_ok: status.saved_ok || 0, status };
      }
    } catch(e) {
      // Transient network error during poll — keep trying
    }
  }
  return { run_id, saved_ok: 0, status: { state:"timeout" } };
}

// ─── NuVizz Parser ──────────────────────────────────────────
// Captures stop-level data from the NuVizz manifest export.
// As of v2.46.0 every CSV column is preserved verbatim under the `raw` field
// — addresses, weights, accessorial codes, schedule windows, etc. — so future
// analytics (mileage calc, on-time, etc.) don't require a re-ingest.
// Known columns historically: Delivery End, Delivery Start, Stop Number,
// Stop Status, Driver Name, Ship To Name, Ship To, Ship To - City,
// Ship To - Zip Code, Stop SealNbr. NuVizz is free to add more — we capture
// whatever's there.
// "Stop SealNbr" is the base dollar amount used to calculate contractor pay.
// 1099 contractors get 40% of SealNbr per stop. W2 drivers are paid hourly (CyberPay).
// Driver W2/1099 classification lives in Fleet Management.
const CONTRACTOR_PAY_PCT = 0.40;

// Like parseDateMDYFlexible but preserves time-of-day when present.
// Returns YYYY-MM-DD if no time in source, or YYYY-MM-DDTHH:MM:SS if time
// component is included. Handles Excel serial fractions (where the
// fractional part encodes time) AND string formats with 12/24-hour clock.
function parseDateTimeMDYFlexible(s) {
  if (s == null || s === "") return null;
  if (typeof s === "number" && isFinite(s)) {
    const ms = Math.round((s - 25569) * 86400 * 1000);
    const d = new Date(ms);
    if (isNaN(d)) return null;
    const y = d.getUTCFullYear();
    const mo = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    const hh = String(d.getUTCHours()).padStart(2, "0");
    const mi = String(d.getUTCMinutes()).padStart(2, "0");
    const ss = String(d.getUTCSeconds()).padStart(2, "0");
    // If serial has no fractional part, return date-only
    const hasTime = (hh !== "00" || mi !== "00" || ss !== "00");
    return hasTime ? `${y}-${mo}-${dd}T${hh}:${mi}:${ss}` : `${y}-${mo}-${dd}`;
  }
  const str = String(s).trim();
  const m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:[\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM|am|pm)?)?/);
  if (!m) return null;
  let [, mo, d, y, hh, mi, ss, ampm] = m;
  if (y.length === 2) y = (parseInt(y) >= 70 ? "19" : "20") + y;
  const datePart = `${y}-${mo.padStart(2,"0")}-${d.padStart(2,"0")}`;
  if (!hh) return datePart;
  let h = parseInt(hh);
  if (ampm) {
    const isPM = /pm/i.test(ampm);
    if (isPM && h < 12) h += 12;
    else if (!isPM && h === 12) h = 0;
  }
  return `${datePart}T${String(h).padStart(2,"0")}:${mi}:${ss || "00"}`;
}

function parseNuVizz(rows) {
  const stops = [];
  for (const r of rows) {
    const stopNumRaw = r["stop number"];
    const driver = r["driver name"];
    if (!stopNumRaw && !driver) continue; // skip blank rows
    const stopNum = stopNumRaw ? String(stopNumRaw).trim() : null;
    // Datetime parse — preserves time-of-day for delivery-window analysis.
    // NOTE: there is no separate "delivery start" column. The first delivery
    // of a driver's day = MIN(delivery_end_at) across that driver's stops on
    // that date; the last delivery = MAX. Both come from this single column.
    const deliveryEndDt = parseDateTimeMDYFlexible(r["delivery end"]);
    // delivery_date stays date-only (YYYY-MM-DD) for backwards compat with
    // existing readers (week rollups, recon, etc.)
    const deliveryDate = deliveryEndDt ? deliveryEndDt.slice(0, 10) : null;
    // Only emit *_at field when an actual time component was found
    const hasEndTime = !!(deliveryEndDt && deliveryEndDt.includes("T"));
    const status = r["stop status"] ? String(r["stop status"]).trim() : null;
    const shipTo = r["ship to name"] ? String(r["ship to name"]).trim() : null;
    const city = r["ship to - city"] ? String(r["ship to - city"]).trim() : null;
    const zip = r["ship to - zip code"] ? String(r["ship to - zip code"]).trim() : null;
    const payBase = parseMoney(r["stop sealnbr"]);
    // NOTE: We do NOT pre-compute a 40% contractor pay value here. The 40%
    // rate is only meaningful for drivers actually classified 1099 in the
    // driver_classifications collection. Storing pay_at_40 on every stop
    // would be a hypothetical "if this driver were 1099" figure for W2
    // drivers (who are paid hourly via CyberPay) — misleading and unused.
    // The real cost calculation happens at query time by joining stop pay
    // against driver_classifications. The weekly rollup keeps a hypothetical
    // pay_at_40 per driver since downstream LaborReality already gates it
    // by classification before summing.
    const pro = normalizePro(stopNum); // cross-reference to Uline PRO
    // Redelivery detection — two independent signals:
    //   1. stop_number ends in "-N" (e.g. "12345-1")  → re-attempt suffix
    //   2. PRO begins with "ATT"  (e.g. "ATT12345")   → NuVizz redelivery prefix
    // Either one is sufficient. NuVizz may not currently emit the ATT prefix;
    // operator will add it to the export when available.
    const isRedeliveryByStop = !!(stopNum && /-\d+$/.test(stopNum));
    const isRedeliveryByPro  = !!(pro && /^ATT/i.test(pro));
    const isRedelivery = isRedeliveryByStop || isRedeliveryByPro;
    stops.push({
      stop_number: stopNum,
      pro,
      driver_name: normalizeName(driver),
      driver_key: driverKey(normalizeName(driver)), // v2.47.0: join key matching driver_classifications + timeclock_daily
      delivery_date: deliveryDate,
      delivery_end_at: hasEndTime ? deliveryEndDt : null,
      week_ending: deliveryDate ? weekEndingFriday(deliveryDate) : null,
      month: deliveryDate ? dateToMonth(deliveryDate) : null,
      status,
      ship_to: shipTo,
      city, zip,
      contractor_pay_base: payBase,         // raw $ from SealNbr column
      is_redelivery: isRedelivery,          // v2.46.1: see redelivery comment above
      raw: r,                               // v2.46.0: every CSV column verbatim — for
                                            //         addresses (mileage), weight,
                                            //         accessorial codes, schedule windows
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
        stops_total: 0,            // every row seen
        stops_completed: 0,        // status = "Completed"
        stops_manually_completed: 0, // status = "Manually Completed"
        stops_effective: 0,        // Completed + Manually Completed (what we count for pay)
        pay_base_total: 0,             // sum of SealNbr for EFFECTIVE stops only
        contractor_pay_if_all_1099: 0, // 40% of pay_base_total
        unique_drivers: new Set(),
        unique_customers: new Set(),
        drivers: {},
      };
    }
    const bw = byWeek[w];
    bw.stops_total++;
    const st = (s.status || "").toLowerCase().trim();
    const isCompleted = st === "completed";
    const isManual = st === "manually completed";
    const isEffective = isCompleted || isManual;
    if (isCompleted) bw.stops_completed++;
    if (isManual) bw.stops_manually_completed++;
    if (isEffective) {
      bw.stops_effective++;
      bw.pay_base_total += s.contractor_pay_base || 0;
      bw.contractor_pay_if_all_1099 += (s.contractor_pay_base || 0) * CONTRACTOR_PAY_PCT;
    }
    if (s.driver_name && isEffective) {
      bw.unique_drivers.add(s.driver_name);
      if (!bw.drivers[s.driver_name]) bw.drivers[s.driver_name] = { stops:0, pay_base:0, pay_at_40:0 };
      bw.drivers[s.driver_name].stops++;
      bw.drivers[s.driver_name].pay_base += s.contractor_pay_base || 0;
      // pay_at_40 is the per-driver "if 1099" figure; LaborReality &
      // DriverPerformanceTab filter it through driver_classifications
      // before treating it as actual cost.
      bw.drivers[s.driver_name].pay_at_40 += (s.contractor_pay_base || 0) * CONTRACTOR_PAY_PCT;
    }
    if (s.ship_to && isEffective) bw.unique_customers.add(s.ship_to);
  }
  return Object.values(byWeek).map(w => ({
    week_ending: w.week_ending,
    month: w.month,
    stops_total: w.stops_total,
    stops_completed: w.stops_completed,
    stops_manually_completed: w.stops_manually_completed,
    stops_effective: w.stops_effective,
    pay_base_total: Number(w.pay_base_total.toFixed(2)),
    contractor_pay_if_all_1099: Number(w.contractor_pay_if_all_1099.toFixed(2)),
    unique_drivers: w.unique_drivers.size,
    unique_customers: w.unique_customers.size,
    top_drivers: Object.entries(w.drivers).sort((a,b) => b[1].stops - a[1].stops).slice(0,60).map(([name, v]) => ({
      name,
      stops: v.stops,
      pay_base: Number(v.pay_base.toFixed(2)),
      pay_at_40: Number(v.pay_at_40.toFixed(2)),
    })),
  }));
}

// ─── Time Clock Parser ──────────────────────────────────────
// Flexible column mapping: employee/driver, date, clock in, clock out, hours
// Supports both generic timeclock format AND CyberPay/B600 format (Display Name, In Time, Out Time, REG, OT1, OT2, Total)
// v2.47.0: emits driver_key for joining to driver_classifications and
//          (downstream) nuvizz_stops; preserves raw row for future use.
// v2.47.4: extracts display_id, payroll_id, day_of_week so the union schema
//          matches what marginiq-b600-timeclock.mts produces. Multiple punches
//          per driver per day collapse into a punches[] array on the daily doc.
function parseTimeClock(rows) {
  const entries = [];
  for (const r of rows) {
    const rawName = r["employee"] || r["employee name"] || r["driver"] || r["driver name"] || r["name"] || r["display name"] || r["payroll id"];
    if (!rawName) continue;
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
    const employee = normalizeName(rawName);
    const displayName = (r["display name"] || rawName || "").toString().trim();
    const displayId = (r["display id"] || "").toString().trim() || displayName;
    const payrollId = (r["payroll id"] || "").toString().trim();
    const dayOfWeek = (r["in day"] || r["day of week"] || "").toString().trim();
    entries.push({
      employee,
      driver_key: driverKey(employee), // join key — same slug used in driver_classifications
      display_name: displayName,
      display_id: displayId,
      payroll_id: payrollId,
      day_of_week: dayOfWeek,
      date,
      week_ending: date ? weekEndingSaturday(date) : null,
      month: date ? dateToMonth(date) : null,
      clock_in: clockIn ? String(clockIn).trim() : null,
      clock_out: clockOut ? String(clockOut).trim() : null,
      hours,
      reg_hours: regHrs,
      ot_hours: ot1Hrs + ot2Hrs,
      department: r["department"] ? String(r["department"]).trim() : null,
      raw: r, // every CSV column verbatim — for future use
    });
  }
  return entries;
}

// Per-shift docs for clock-in→first-stop / last-stop→clock-out forensics.
// Doc ID  : `${driver_key}_${date}` — same slug used by driver_classifications,
//           so consumer-side joins are exact.
// Strategy: emit one doc per (driver_key, date); merge:true upserts on re-ingest.
//
// v2.47.4: schema unified with marginiq-b600-timeclock.mts. Multiple punches
// (lunch breaks, splits) collapse into a punches[] array on the single doc.
// clock_in = earliest "in" across all punches; clock_out = latest "out".
function buildTimeClockDaily(entries) {
  const map = {}; // doc_id -> aggregated daily entry
  for (const e of entries) {
    if (!e.driver_key || !e.date) continue;
    const docId = `${e.driver_key}_${e.date}`;
    if (!map[docId]) {
      map[docId] = {
        doc_id: docId,
        employee: e.employee,
        driver_key: e.driver_key,
        display_name: e.display_name || e.employee || null,
        display_id: e.display_id || null,
        payroll_id: e.payroll_id || null,
        day_of_week: e.day_of_week || null,
        date: e.date,
        week_ending: e.week_ending,
        month: e.month,
        clock_in: e.clock_in || null,
        clock_out: e.clock_out || null,
        punches: [],
        hours: 0,
        total_hours: 0, // alias for cross-writer compat
        reg_hours: 0,
        ot_hours: 0,
        department: e.department || null,
        raw_first: e.raw, // first row's raw — for inspection; punches[].raw has all
      };
    }
    const d = map[docId];
    d.punches.push({
      in: e.clock_in || null,
      out: e.clock_out || null,
      hours: e.hours || 0,
      raw: e.raw || null,
    });
    d.hours += e.hours || 0;
    d.total_hours += e.hours || 0;
    d.reg_hours += e.reg_hours || 0;
    d.ot_hours += e.ot_hours || 0;
    // Earliest in / latest out across all punches
    if (e.clock_in && (!d.clock_in || e.clock_in < d.clock_in)) d.clock_in = e.clock_in;
    if (e.clock_out && (!d.clock_out || e.clock_out > d.clock_out)) d.clock_out = e.clock_out;
  }
  return Object.values(map);
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
    week_ending: w.week_ending,
    month: w.month,
    total_hours: w.total_hours,
    reg_hours: w.reg_hours,
    ot_hours: w.ot_hours,
    days_worked: w.days_worked,
    unique_employees: w.unique_employees.size,
    top_employees: Object.entries(w.employees).sort((a,b) => b[1].hours - a[1].hours).slice(0,60).map(([name, v]) => ({name, ...v})),
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
        // NEW: per-service-type subtotals. Delivery = regular stop-based billing.
        // Truckload = full truckload billing (TK suffix files). Accessorial =
        // extra-charges files. All three roll up into this one weekly doc so a
        // week's reconciliation can check all streams at once.
        delivery_stops: 0, delivery_revenue: 0,
        truckload_stops: 0, truckload_revenue: 0,
        accessorial_stops: 0,
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
    bw.weight += s.weight || 0;
    bw.skids += s.skid || 0;
    bw.unique_pros.add(s.pro);
    if (s.is_accessorial) bw.accessorial_count++;
    // Route this stop's revenue to the right service-type bucket. new_cost is
    // the source of truth for every bucket — never extra_cost or base cost.
    // accessorial_revenue is now driven by new_cost of accessorial-type rows
    // (previously a misleading sum of extra_cost across all rows; that approach
    // under-reported combined-file accessorials where the full line amount
    // sits in new_cost with extra_cost blank).
    const st = s.service_type || "delivery";
    if (st === "truckload") {
      bw.truckload_stops++;
      bw.truckload_revenue += s.new_cost || 0;
    } else if (st === "accessorial") {
      bw.accessorial_stops++;
      bw.accessorial_revenue += s.new_cost || 0;
    } else {
      bw.delivery_stops++;
      bw.delivery_revenue += s.new_cost || 0;
    }
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
    week_ending: w.week_ending,
    month: w.month,
    stops: w.stops,
    revenue: Number((w.revenue || 0).toFixed(2)),
    base_revenue: Number((w.base_revenue || 0).toFixed(2)),
    accessorial_revenue: Number((w.accessorial_revenue || 0).toFixed(2)),
    delivery_stops: w.delivery_stops,
    delivery_revenue: Number((w.delivery_revenue || 0).toFixed(2)),
    truckload_stops: w.truckload_stops,
    truckload_revenue: Number((w.truckload_revenue || 0).toFixed(2)),
    accessorial_stops: w.accessorial_stops,
    weight: Math.round(w.weight || 0),
    skids: w.skids,
    accessorial_count: w.accessorial_count,
    unique_pros: w.unique_pros.size,
    top_customers: Object.entries(w.customers).sort((a,b) => b[1].revenue - a[1].revenue).slice(0,20).map(([name, v]) => ({ name, stops: v.stops, revenue: Number(v.revenue.toFixed(2)) })),
    top_cities: Object.entries(w.cities).sort((a,b) => b[1].revenue - a[1].revenue).slice(0,20).map(([name, v]) => ({ name, stops: v.stops, revenue: Number(v.revenue.toFixed(2)) })),
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

// ═══ SORTABLE TABLE UTILITIES ═════════════════════════════════════════
// One pattern used across every table in the app so all columns are sortable
// and any new table we add gets sortability by default.
//
// Usage pattern:
//   const { sorted, sortKey, sortDir, toggleSort } = useSortable(rows, "week_ending", "desc");
//   <thead><tr>
//     <SortableTh label="Week" col="week_ending" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
//     <SortableTh label="Revenue" col="revenue" align="right" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
//   </tr></thead>
//   <tbody>{sorted.map(r => ...)}</tbody>
//
// For computed/derived columns (e.g. filename→coverage date), pass a custom
// extractor via the `colAccessors` option:
//   const { sorted, ... } = useSortable(rows, "coverage", "desc", {
//     coverage: (r) => parseCoverage(r.filename)?.startISO || ""
//   });
//
// Clicking a column header toggles asc/desc. Clicking a different column
// starts at a sensible default (desc for numbers/dates, asc for strings).

function useSortable(rows, defaultKey = null, defaultDir = "desc", accessors = {}) {
  const [sortKey, setSortKey] = useState(defaultKey);
  const [sortDir, setSortDir] = useState(defaultDir);

  const toggleSort = (col) => {
    if (sortKey === col) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortKey(col);
      // Sensible default direction based on value type of first non-null row
      const sample = (rows || []).find(r => {
        const v = accessors[col] ? accessors[col](r) : r?.[col];
        return v !== null && v !== undefined && v !== "";
      });
      const sampleVal = sample ? (accessors[col] ? accessors[col](sample) : sample[col]) : null;
      // Numbers and dates default desc (biggest/newest first); strings default asc
      const isNumLike = typeof sampleVal === "number" || (typeof sampleVal === "string" && /^\d{4}-\d{2}-\d{2}/.test(sampleVal));
      setSortDir(isNumLike ? "desc" : "asc");
    }
  };

  const sorted = useMemo(() => {
    if (!rows || !Array.isArray(rows) || !sortKey) return rows || [];
    const arr = [...rows];
    const getVal = (r) => accessors[sortKey] ? accessors[sortKey](r) : r?.[sortKey];
    arr.sort((a, b) => {
      const va = getVal(a); const vb = getVal(b);
      // null/undefined/empty-string always sort last regardless of direction
      const aEmpty = va === null || va === undefined || va === "";
      const bEmpty = vb === null || vb === undefined || vb === "";
      if (aEmpty && bEmpty) return 0;
      if (aEmpty) return 1;
      if (bEmpty) return -1;
      // Numeric comparison
      if (typeof va === "number" && typeof vb === "number") {
        return sortDir === "asc" ? va - vb : vb - va;
      }
      // Boolean
      if (typeof va === "boolean" || typeof vb === "boolean") {
        const na = va ? 1 : 0; const nb = vb ? 1 : 0;
        return sortDir === "asc" ? na - nb : nb - na;
      }
      // Date-like strings (YYYY-MM-DD...) work naturally with localeCompare
      const sa = String(va); const sb = String(vb);
      const cmp = sa.localeCompare(sb, undefined, { numeric: true, sensitivity: "base" });
      return sortDir === "asc" ? cmp : -cmp;
    });
    return arr;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows, sortKey, sortDir]);

  return { sorted, sortKey, sortDir, toggleSort, setSortKey, setSortDir };
}

// Sortable column header. Drop-in replacement for a plain <th>.
function SortableTh({ label, col, sortKey, sortDir, onSort, align = "left", style = {}, title }) {
  const active = sortKey === col;
  const arrow = !active ? "↕" : (sortDir === "asc" ? "▲" : "▼");
  return (
    <th
      onClick={() => onSort(col)}
      title={title || `Sort by ${label}`}
      style={{
        textAlign: align,
        padding: "8px 10px",
        borderBottom: `1px solid ${T.border}`,
        color: active ? T.brand : T.textMuted,
        fontSize: 10,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        position: "sticky",
        top: 0,
        background: T.bgSurface,
        zIndex: 1,
        cursor: "pointer",
        userSelect: "none",
        whiteSpace: "nowrap",
        ...style,
      }}
    >
      <span style={{display:"inline-flex",alignItems:"center",gap:4}}>
        {label}
        <span style={{fontSize:9,opacity:active?1:0.35,color:active?T.brand:T.textDim}}>{arrow}</span>
      </span>
    </th>
  );
}

// ═══ AI INSIGHT PANEL ═════════════════════════════════════════════════
// Shared analyzer component. Any tab can drop <AIInsight context="X" data={{...}} />
// and get two passes: (1) instant deterministic sanity checks computed from the
// data provided, (2) Claude-powered interpretation via /marginiq-ai-analyze
// that adds pattern recognition and recommendations on top.
//
// Current contexts supported:
//   - "uline-revenue"  → checks accessorial %, rev/stop stability, week coverage
//   - "command-center" → overall business-health signals
//   - "data-health"    → missing weeks, gap severity, suggested next uploads
//   - "audit"          → reconciliation gaps, unmatched PROs
// Add new contexts by extending runSanityChecks() below.

// --- Sanity check engine (deterministic, no AI, instant) ---
// Returns { signal: 'ok'|'warn'|'alarm', findings: [{severity, title, detail, metric?}] }
function runSanityChecks(context, data) {
  const findings = [];
  const push = (severity, title, detail, metric) => findings.push({ severity, title, detail, metric });

  if (context === "uline-revenue" && data?.weeklyRollups) {
    const weeks = data.weeklyRollups.filter(w => w.week_ending >= "2025-01-01");
    if (weeks.length === 0) {
      push("info", "No 2025+ data loaded yet", "Upload your Uline weekly files in Data Ingest to start analysis.");
      return { signal: "info", findings };
    }

    // Check 1: accessorial ratio. LTL accessorials typically 5-15% of revenue.
    // If it's < 3%, almost certainly missing accessorial files.
    const totalRev = weeks.reduce((s,w) => s + (w.revenue||0), 0);
    const totalAcc = weeks.reduce((s,w) => s + (w.accessorial_revenue||0), 0);
    const accPct = totalRev > 0 ? (totalAcc / totalRev * 100) : 0;
    if (accPct < 3 && totalRev > 100000) {
      const weeksWithAcc = weeks.filter(w => (w.accessorial_revenue||0) > 0).length;
      const weeksMissingAcc = weeks.length - weeksWithAcc;
      push("alarm",
        `Accessorial revenue is only ${accPct.toFixed(1)}% of total — too low`,
        `LTL accessorials typically run 5–15% of revenue. Yours is ${accPct.toFixed(1)}%, suggesting missing accessorial files. ${weeksMissingAcc} of ${weeks.length} weeks have zero accessorial revenue.`,
        { actual: `${accPct.toFixed(1)}%`, expected: "5–15%", missing_weeks: weeksMissingAcc });
    } else if (accPct >= 3 && accPct < 5) {
      push("warn",
        `Accessorial revenue is ${accPct.toFixed(1)}% of total — on the low end`,
        `Below the typical LTL range of 5-15%. Worth spot-checking a few weeks to confirm accessorial files are complete.`,
        { actual: `${accPct.toFixed(1)}%`, expected: "5–15%" });
    }

    // Check 2: week coverage — expected weeks vs actual
    const sorted = [...weeks].sort((a,b) => a.week_ending.localeCompare(b.week_ending));
    const firstWE = sorted[0].week_ending;
    const lastWE = sorted[sorted.length-1].week_ending;
    const msPerWeek = 7*86400000;
    const expectedWeekCount = Math.round((new Date(lastWE) - new Date(firstWE)) / msPerWeek) + 1;
    const actualWeekCount = weeks.length;
    if (actualWeekCount < expectedWeekCount) {
      push("alarm",
        `${expectedWeekCount - actualWeekCount} week(s) missing between ${firstWE} and ${lastWE}`,
        `Expected ${expectedWeekCount} weeks of data based on date range; have ${actualWeekCount}. Check Data Health tab for the exact missing Fridays.`,
        { expected: expectedWeekCount, actual: actualWeekCount });
    }

    // Check 3: rev/stop stability — outliers suggest partial weeks
    const revPerStop = weeks.filter(w => (w.stops||0) > 100).map(w => (w.revenue||0) / (w.stops||1));
    if (revPerStop.length >= 5) {
      const sortedRPS = [...revPerStop].sort((a,b) => a-b);
      const median = sortedRPS[Math.floor(sortedRPS.length/2)];
      const outlierLow = weeks.filter(w => (w.stops||0) > 100 && ((w.revenue||0)/(w.stops||1)) < median * 0.5);
      if (outlierLow.length > 0) {
        push("warn",
          `${outlierLow.length} week(s) have rev/stop < 50% of median (\$${median.toFixed(0)})`,
          `Weeks: ${outlierLow.slice(0,5).map(w=>w.week_ending).join(", ")}${outlierLow.length>5?"…":""}. These weeks may be partial uploads (delivery file without accessorials).`,
          { median_rps: `$${median.toFixed(0)}`, outliers: outlierLow.length });
      }
    }

    // Check 4: truckload presence
    const withTK = weeks.filter(w => (w.truckload_stops||0) > 0).length;
    if (withTK === 0 && weeks.length > 20) {
      push("info",
        "No truckload (TK) weeks detected",
        "If you run truckload alongside delivery, the TK files may not be ingested. Filenames contain 'TK' (e.g. das 20251201-20251205 TK.xlsx).");
    }
  }

  if (context === "command-center" && data?.completeness) {
    const c = data.completeness;
    if (c.gaps?.length > 0) {
      push("alarm",
        `${c.gaps.length} missing week(s) of Uline data`,
        `These weeks have zero data in the system from 2025 onward. Your revenue, margin, and rev/stop metrics are understated.`,
        { gaps: c.gaps.length });
    }
    if (c.sparseWeeks?.length > 5) {
      push("warn",
        `${c.sparseWeeks.length} suspiciously low-volume weeks`,
        `These weeks have far fewer stops than average. Could be partial uploads or legitimate slow weeks (holidays).`,
        { sparse: c.sparseWeeks.length });
    }
    if (c.missingAccessorials?.length > 3) {
      push("warn",
        `${c.missingAccessorials.length} weeks have delivery data but no accessorial file`,
        `Accessorial charges from these weeks are missing from revenue totals.`,
        { missing: c.missingAccessorials.length });
    }
  }

  if (context === "data-health") {
    const streamCov = data?.streamCoverage || {};
    const comp = data?.completeness;
    const fileLog = data?.fileLog || [];
    const unknownFiles = data?.unknownFiles || [];

    // Check 1 — Delivery coverage (the primary revenue stream)
    const delivery = streamCov.delivery;
    if (delivery) {
      const missing = delivery.total - delivery.covered;
      if (missing > 0) {
        push(missing > 4 ? "alarm" : "warn",
          `${missing} delivery week${missing>1?"s":""} missing (${delivery.pct}% coverage)`,
          `Delivery is your primary revenue stream — each missing week directly understates revenue. ${delivery.covered} of ${delivery.total} weeks present in 2025+.`,
          { coverage: `${delivery.pct}%`, missing });
      } else if (delivery.total > 0) {
        push("ok", `Delivery coverage complete`, `All ${delivery.total} expected weeks have delivery data.`);
      }
    }

    // Check 2 — Accessorial coverage
    const acc = streamCov.accessorials;
    if (acc && acc.total > 0) {
      const missingAcc = acc.total - acc.covered;
      const accPct = acc.pct;
      if (accPct < 70) {
        push("warn",
          `${missingAcc} weeks missing accessorial data (${accPct}%)`,
          `Missing accessorial files directly reduce billed revenue — typical LTL accessorials run 5-15% of delivery revenue.`,
          { coverage: `${accPct}%`, missing: missingAcc });
      }
    }

    // Check 3 — Truckload presence
    const tk = streamCov.truckload;
    if (tk && tk.covered === 0 && delivery?.covered > 5) {
      push("info",
        `No truckload (TK) data found`,
        `Either you're not running TK routes, or TK files aren't being ingested. Check Gmail Sync for 'TK' variant filenames.`);
    }

    // Check 4 — DDIS (payment) coverage vs delivery
    const ddis = streamCov.ddis;
    if (ddis && delivery && delivery.covered > 10) {
      const ratio = ddis.covered / delivery.covered;
      if (ratio < 0.5) {
        push("warn",
          `DDIS payment coverage is sparse (${ddis.covered} weeks vs ${delivery.covered} delivery weeks)`,
          `Reconciliation and AuditIQ only work where DDIS remittance files exist. Pull more from Gmail.`);
      }
    }

    // Check 5 — NuVizz coverage (driver pay basis)
    const nv = streamCov.nuvizz;
    if (nv && delivery && delivery.covered > 10) {
      const nvMissing = delivery.covered - nv.covered;
      if (nvMissing > 4) {
        push("warn",
          `NuVizz data missing for ${nvMissing} week${nvMissing>1?"s":""}`,
          `Driver pay calculations (1099 SealNbr × 40%) require NuVizz stops. Without them, those weeks show no contractor pay.`);
      }
    }

    // Check 6 — Time Clock coverage (W2 payroll basis)
    const tc = streamCov.timeclock;
    if (tc && delivery && delivery.covered > 10) {
      const tcMissing = delivery.covered - tc.covered;
      if (tcMissing > 2) {
        push("warn",
          `Time Clock data missing for ${tcMissing} week${tcMissing>1?"s":""}`,
          `W2 driver payroll costs can't be computed without time clock entries. Check B600 export or Gmail Sync.`);
      }
    }

    // Check 7 — Sparse weeks (present but likely partial)
    if (comp?.sparseWeeks?.length > 0) {
      push("warn",
        `${comp.sparseWeeks.length} sparse week${comp.sparseWeeks.length>1?"s":""} detected`,
        `These weeks have data but stop counts are <50% of your average (${comp.avgStops}/week). Likely partial ingest — verify by re-searching Gmail for that date range.`);
    }

    // Check 8 — Unknown/unparsed files
    if (unknownFiles.length > 0) {
      push("info",
        `${unknownFiles.length} file${unknownFiles.length>1?"s":""} couldn't be categorized`,
        `Files that didn't match any known format. Either new formats, typos in filenames, or truly unknown sources.`,
        { examples: unknownFiles.slice(0, 3) });
    }

    // Check 9 — File log staleness
    if (fileLog.length > 0) {
      const latest = fileLog[0]; // fileLog is ordered by uploaded_at desc
      if (latest?.uploaded_at) {
        const daysSince = Math.floor((Date.now() - new Date(latest.uploaded_at).getTime()) / 86400000);
        if (daysSince > 14) {
          push("warn",
            `Last file uploaded ${daysSince} days ago`,
            `If you're still running deliveries, new Uline files should be arriving weekly. Check Gmail Sync.`);
        }
      }
    }

    // If nothing flagged and we have data, celebrate briefly
    if (findings.length === 0 && delivery?.covered > 0) {
      push("ok",
        `Data looks complete`,
        `All primary streams (delivery, accessorials, DDIS, NuVizz, time clock) present with good coverage across the loaded date range.`);
    }
  }

  if (context === "data-health-legacy-removed") {
    // placeholder to keep diff minimal — old single-rule block replaced above
  }

  // Overall signal: max severity seen
  let signal = "ok";
  if (findings.some(f => f.severity === "alarm")) signal = "alarm";
  else if (findings.some(f => f.severity === "warn")) signal = "warn";
  else if (findings.some(f => f.severity === "info")) signal = "info";
  return { signal, findings };
}

function AIInsight({ context, data, title, compact }) {
  const [expanded, setExpanded] = useState(!compact);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResponse, setAiResponse] = useState(null);
  const [aiError, setAiError] = useState(null);

  const { signal, findings } = useMemo(() => runSanityChecks(context, data), [context, data]);

  const runAI = async () => {
    setAiLoading(true);
    setAiError(null);
    try {
      const resp = await fetch("/.netlify/functions/marginiq-ai-analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ context, data, sanityFindings: findings }),
      });
      const result = await resp.json();
      if (result.error) throw new Error(result.error);
      setAiResponse(result);
    } catch(e) {
      setAiError(e.message);
    }
    setAiLoading(false);
  };

  // Color theming by severity
  const sigColors = {
    alarm: { border: T.red,    bg: "#fef2f2", accent: T.redText,    icon: "🚨", label: "Needs attention" },
    warn:  { border: T.yellow, bg: "#fffbeb", accent: T.yellowText, icon: "⚠️",  label: "Review" },
    info:  { border: T.blue,   bg: "#eff6ff", accent: "#1d4ed8",    icon: "ℹ️",  label: "Note" },
    ok:    { border: T.green,  bg: "#ecfdf5", accent: "#065f46",    icon: "✓",  label: "Looks good" },
  };
  const sc = sigColors[signal];

  return (
    <div style={{
      borderRadius: 10,
      border: `1px solid ${sc.border}`,
      background: sc.bg,
      padding: "12px 14px",
      marginBottom: 12,
    }}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",gap:8,flexWrap:"wrap"}}>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{fontSize:16}}>{sc.icon}</span>
          <div>
            <div style={{fontSize:12,fontWeight:700,color:sc.accent,textTransform:"uppercase",letterSpacing:"0.06em"}}>
              🤖 AI Insight · {sc.label}
            </div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>
              {title || "Automated analysis of the data shown above."} {findings.length > 0 && `${findings.length} finding${findings.length>1?"s":""}.`}
            </div>
          </div>
        </div>
        <button onClick={() => setExpanded(!expanded)} style={{
          border: "none", background: "transparent", color: sc.accent, cursor: "pointer",
          fontSize: 11, fontWeight: 700,
        }}>{expanded ? "Hide ▲" : "Show ▼"}</button>
      </div>

      {expanded && (
        <>
          {findings.length === 0 ? (
            <div style={{marginTop:10,fontSize:12,color:T.textMuted,fontStyle:"italic"}}>
              No anomalies detected in the current data.
            </div>
          ) : (
            <div style={{marginTop:10,display:"flex",flexDirection:"column",gap:8}}>
              {findings.map((f, i) => (
                <div key={i} style={{
                  padding: "8px 10px",
                  borderRadius: 6,
                  background: "white",
                  borderLeft: `3px solid ${sigColors[f.severity].border}`,
                  fontSize: 12,
                }}>
                  <div style={{fontWeight:700,color:sigColors[f.severity].accent,marginBottom:2}}>
                    {f.title}
                  </div>
                  <div style={{color:T.text,lineHeight:1.4}}>{f.detail}</div>
                  {f.metric && (
                    <div style={{marginTop:4,display:"flex",gap:8,flexWrap:"wrap"}}>
                      {Object.entries(f.metric).map(([k,v]) => (
                        <span key={k} style={{
                          fontSize: 10, padding: "1px 6px", borderRadius: 4,
                          background: T.bgSurface, color: T.textMuted, fontFamily: "monospace",
                        }}>{k.replace(/_/g," ")}: <strong style={{color:T.text}}>{v}</strong></span>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* AI Deep Analysis button — available once sanity pass is done */}
          <div style={{marginTop:12,paddingTop:10,borderTop:`1px dashed ${sc.border}50`}}>
            {!aiResponse && !aiLoading && (
              <button onClick={runAI} style={{
                padding: "6px 12px", borderRadius: 6,
                border: `1px solid ${sc.border}`,
                background: "white", color: sc.accent,
                fontSize: 11, fontWeight: 700, cursor: "pointer",
              }}>🧠 Ask Claude for deeper analysis</button>
            )}
            {aiLoading && <div style={{fontSize:11,color:T.textMuted}}>Running deep analysis…</div>}
            {aiError && <div style={{fontSize:11,color:T.redText}}>AI error: {aiError}</div>}
            {aiResponse && (
              <div style={{fontSize:12,color:T.text,lineHeight:1.5,whiteSpace:"pre-wrap"}}>
                {aiResponse.analysis || JSON.stringify(aiResponse)}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
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
        <div style={{fontSize:11,color:T.textMuted}}>Upload NuVizz + Payroll files (or connect Gmail) to see actual labor cost vs your cost-structure estimate. Then classify employees in the Employees tab to unlock accurate margins.</div>
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
          {" "}from employees not yet tagged W2 or 1099. Classify them in the Employees tab for accurate actual-cost math.
        </div>
      )}
    </div>
  );
}

function AuditedKpiStrip({ setTab }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      if (!hasFirebase) { setLoading(false); return; }
      try {
        const s = await window.db.collection("audited_financials").limit(60).get();
        const records = s.docs.map(d => ({ id: d.id, period: d.id, ...d.data() }))
          .filter(r => r.pl)
          .sort((a,b) => (b.period||"").localeCompare(a.period||""));
        if (records.length === 0) { setData({ empty: true }); setLoading(false); return; }
        const latest = records[0];
        const previous = records[1] || null;
        const ttm = records.slice(0, 12);
        const ttmRev = ttm.reduce((s,r) => s + (r.pl?.revenue||0), 0);
        const ttmNet = ttm.reduce((s,r) => s + (r.pl?.net_income||0), 0);
        const ttmMargin = ttmRev > 0 ? (ttmNet/ttmRev) * 100 : 0;
        const latestNet = latest.pl?.net_income || 0;
        const previousNet = previous?.pl?.net_income || 0;
        const momChange = previous && previousNet !== 0 ? ((latestNet - previousNet) / Math.abs(previousNet)) * 100 : null;
        setData({ records, latest, previous, ttm, ttmRev, ttmNet, ttmMargin, latestNet, previousNet, momChange });
      } catch (e) { console.error("AuditedKpiStrip load err:", e); }
      setLoading(false);
    })();
  }, []);

  if (loading) return null;

  if (data?.empty) {
    return (
      <div style={{...cardStyle, background:T.bgSurface, borderLeft:`3px solid ${T.textDim}`, marginBottom:12}}>
        <div style={{display:"flex", justifyContent:"space-between", alignItems:"center", gap:8, flexWrap:"wrap"}}>
          <div>
            <div style={{fontSize:13, fontWeight:700, marginBottom:4}}>📋 Financials</div>
            <div style={{fontSize:11, color:T.textMuted}}>No CPA-audited statements imported yet. Connect Gmail Sync to pull from @ampcpas.com.</div>
          </div>
          <button onClick={() => setTab("gmail")} style={{padding:"6px 14px", borderRadius:8, border:`1px solid ${T.brand}`, background:"transparent", color:T.brand, fontSize:12, fontWeight:600, cursor:"pointer"}}>Connect →</button>
        </div>
      </div>
    );
  }
  if (!data) return null;

  const marginColor = data.ttmMargin >= 20 ? T.green : data.ttmMargin >= 10 ? T.yellow : T.red;
  const momColor = data.momChange == null ? T.textMuted : data.momChange >= 0 ? T.green : T.red;
  const momSign = data.momChange != null && data.momChange >= 0 ? "+" : "";
  const latestLabel = (() => {
    const parts = (data.latest.period||"").split("-");
    if (parts.length < 2) return data.latest.period||"—";
    const months = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return months[parseInt(parts[1])||0] + " " + parts[0];
  })();

  return (
    <div style={{...cardStyle, borderLeft:`3px solid ${T.green}`, marginBottom:12}}>
      <div style={{display:"flex", justifyContent:"space-between", alignItems:"center", flexWrap:"wrap", gap:8, marginBottom:10}}>
        <div>
          <div style={{fontSize:13, fontWeight:700, display:"flex", alignItems:"center", gap:6}}>
            📋 Financials
            <span style={{fontSize:9, fontWeight:700, padding:"2px 6px", borderRadius:10, background:T.green, color:"#fff"}}>AUDITED</span>
          </div>
          <div style={{fontSize:10, color:T.textMuted, marginTop:2}}>TTM ({data.ttm.length} months) · latest: {latestLabel}</div>
        </div>
        <button onClick={() => setTab("audited")} style={{padding:"6px 12px", borderRadius:8, border:`1px solid ${T.border}`, background:"transparent", color:T.text, fontSize:11, fontWeight:600, cursor:"pointer"}}>Details →</button>
      </div>
      <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:10}}>
        <KPI label="TTM Revenue"    value={fmtK(data.ttmRev)} sub={`${data.records.length} mo audited`} subColor={T.brand} />
        <KPI label="TTM Net Income" value={fmtK(data.ttmNet)} subColor={data.ttmNet>=0?T.green:T.red} />
        <KPI label="TTM Margin"     value={fmtPct(data.ttmMargin)} sub="net income / revenue" subColor={marginColor} />
        <KPI label={`${latestLabel} Net`} value={fmtK(data.latestNet)} sub={data.momChange != null ? `${momSign}${fmtPct(data.momChange)} MoM` : "—"} subColor={momColor} />
      </div>
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

    <AIInsight
      context="command-center"
      data={{ completeness, margins: m }}
      title="Scanning KPIs + data completeness for anomalies."
      compact
    />

    <AuditedKpiStrip setTab={setTab} />

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

  // Sortable column hooks — one per table. Derived columns (avg/stop, pct of total)
  // use custom accessors so the numbers sort correctly.
  const weeklySort = useSortable(filtered, "week_ending", "desc", {
    week_ending: (w) => w.week_ending || "",
    stops: (w) => Number(w.stops || 0),
    revenue: (w) => Number(w.revenue || 0),
    base_revenue: (w) => Number(w.base_revenue || 0),
    accessorial_revenue: (w) => Number(w.accessorial_revenue || 0),
    avg_stop: (w) => w.stops > 0 ? (w.revenue || 0) / w.stops : 0,
    weight: (w) => Number(w.weight || 0),
  });
  const monthlySort = useSortable(monthly, "month", "desc", {
    month: (m) => m.month || "",
    weeks: (m) => Number(m.weeks || 0),
    stops: (m) => Number(m.stops || 0),
    revenue: (m) => Number(m.revenue || 0),
    accessorial_revenue: (m) => Number(m.accessorial_revenue || 0),
    avg_stop: (m) => m.stops > 0 ? (m.revenue || 0) / m.stops : 0,
    avg_week: (m) => m.weeks > 0 ? (m.revenue || 0) / m.weeks : 0,
  });
  const customersSort = useSortable(topCustomers, "revenue", "desc", {
    customer: (c) => (c.customer || "").toLowerCase(),
    stops: (c) => Number(c.stops || 0),
    revenue: (c) => Number(c.revenue || 0),
    avg_stop: (c) => c.stops > 0 ? (c.revenue || 0) / c.stops : 0,
    pct: (c) => totalRevenue > 0 ? (c.revenue || 0) / totalRevenue : 0,
  });
  const citiesSort = useSortable(topCities, "revenue", "desc", {
    city: (c) => (c.city || "").toLowerCase(),
    stops: (c) => Number(c.stops || 0),
    revenue: (c) => Number(c.revenue || 0),
    avg_stop: (c) => c.stops > 0 ? (c.revenue || 0) / c.stops : 0,
  });

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

        <AIInsight
          context="uline-revenue"
          data={{ weeklyRollups: filtered }}
          title={`Analyzing ${filtered.length} week(s) of Uline revenue data.`}
        />

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
                <thead><tr>
                  <SortableTh label="Week Ending" col="week_ending" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Stops" col="stops" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Revenue" col="revenue" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Base" col="base_revenue" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Accessorials" col="accessorial_revenue" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Avg/Stop" col="avg_stop" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                  <SortableTh label="Weight" col="weight" sortKey={weeklySort.sortKey} sortDir={weeklySort.sortDir} onSort={weeklySort.toggleSort} />
                </tr></thead>
                <tbody>
                  {weeklySort.sorted.map((w,i) => (
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
                <thead><tr>
                  <SortableTh label="Month" col="month" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Weeks" col="weeks" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Stops" col="stops" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Revenue" col="revenue" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Accessorials" col="accessorial_revenue" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Avg/Stop" col="avg_stop" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                  <SortableTh label="Avg/Week" col="avg_week" sortKey={monthlySort.sortKey} sortDir={monthlySort.sortDir} onSort={monthlySort.toggleSort} />
                </tr></thead>
                <tbody>
                  {monthlySort.sorted.map((m,i) => (
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
                <thead><tr>
                  <SortableTh label="Customer" col="customer" sortKey={customersSort.sortKey} sortDir={customersSort.sortDir} onSort={customersSort.toggleSort} />
                  <SortableTh label="Stops" col="stops" sortKey={customersSort.sortKey} sortDir={customersSort.sortDir} onSort={customersSort.toggleSort} />
                  <SortableTh label="Revenue" col="revenue" sortKey={customersSort.sortKey} sortDir={customersSort.sortDir} onSort={customersSort.toggleSort} />
                  <SortableTh label="Avg/Stop" col="avg_stop" sortKey={customersSort.sortKey} sortDir={customersSort.sortDir} onSort={customersSort.toggleSort} />
                  <SortableTh label="% of Total" col="pct" sortKey={customersSort.sortKey} sortDir={customersSort.sortDir} onSort={customersSort.toggleSort} />
                </tr></thead>
                <tbody>
                  {customersSort.sorted.map((c,i) => (
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
                <thead><tr>
                  <SortableTh label="City" col="city" sortKey={citiesSort.sortKey} sortDir={citiesSort.sortDir} onSort={citiesSort.toggleSort} />
                  <SortableTh label="Stops" col="stops" sortKey={citiesSort.sortKey} sortDir={citiesSort.sortDir} onSort={citiesSort.toggleSort} />
                  <SortableTh label="Revenue" col="revenue" sortKey={citiesSort.sortKey} sortDir={citiesSort.sortDir} onSort={citiesSort.toggleSort} />
                  <SortableTh label="Avg/Stop" col="avg_stop" sortKey={citiesSort.sortKey} sortDir={citiesSort.sortDir} onSort={citiesSort.toggleSort} />
                </tr></thead>
                <tbody>
                  {citiesSort.sorted.map((c,i) => (
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
// ─── Coverage Timeline ─────────────────────────────────────
// Shows a per-stream timeline of data coverage so gaps across Uline /
// NuVizz / Time Clock / DDIS are visible at a glance. One row per stream,
// one cell per week. Green = complete, yellow = partial, grey = gap.
function CoverageTimeline() {
  const [loading, setLoading] = useState(true);
  const [streams, setStreams] = useState(null);
  const [hover, setHover] = useState(null); // { streamIdx, weekISO, cell }
  const [rangeMode, setRangeMode] = useState("auto"); // "auto" | "2024" | "2025" | "last12"

  useEffect(() => {
    (async () => {
      if (!hasFirebase) { setLoading(false); return; }
      try {
        const [uline, nuvizz, timeclock, ddisFiles] = await Promise.all([
          FS.getWeeklyRollups(),
          FS.getNuVizzWeekly(),
          FS.getTimeClockWeekly(),
          FS.getDDISFiles(),
        ]);

        // Build coverage map per stream: weekISO → {present, value}
        // All streams normalize to Friday-ending weeks for visual alignment.
        const fridayOf = (iso) => {
          // Given any YYYY-MM-DD, return the week-ending Friday (on or after)
          const d = new Date(iso + "T00:00:00");
          const day = d.getDay();
          const daysTo = (5 - day + 7) % 7;
          d.setDate(d.getDate() + daysTo);
          return d.toISOString().slice(0, 10);
        };

        const deliveryMap = {};
        const truckloadMap = {};
        const accessorialMap = {};
        for (const w of uline) {
          const fri = fridayOf(w.week_ending);
          if ((w.delivery_stops || 0) > 0 || (w.stops || 0) > 0) {
            // Fallback: older rollups don't have service_type split, treat as delivery
            deliveryMap[fri] = {
              stops: w.delivery_stops || w.stops || 0,
              revenue: w.delivery_revenue || w.revenue || 0,
            };
          }
          if ((w.truckload_stops || 0) > 0) {
            truckloadMap[fri] = {
              stops: w.truckload_stops || 0,
              revenue: w.truckload_revenue || 0,
            };
          }
          if ((w.accessorial_stops || 0) > 0 || (w.accessorial_revenue || 0) > 0) {
            accessorialMap[fri] = {
              stops: w.accessorial_stops || w.accessorial_count || 0,
              revenue: w.accessorial_revenue || 0,
            };
          }
        }

        const nuvizzMap = {};
        for (const w of nuvizz) {
          if ((w.stops_effective || w.stops_total || 0) > 0) {
            nuvizzMap[w.week_ending] = {
              stops: w.stops_effective || w.stops_total || 0,
              revenue: w.pay_base_total || 0,
            };
          }
        }

        // Time Clock uses Saturday week-endings; shift -1 day to align Friday-visual
        const timeclockMap = {};
        for (const w of timeclock) {
          const satISO = w.week_ending;
          const satDate = new Date(satISO + "T00:00:00");
          satDate.setDate(satDate.getDate() - 1);
          const fri = satDate.toISOString().slice(0, 10);
          if ((w.total_hours || 0) > 0) {
            timeclockMap[fri] = {
              hours: w.total_hours || 0,
              entries: w.entries_count || 0,
            };
          }
        }

        // DDIS: v2.40.9 — one file = one week, keyed off bill_week_ending
        // (the Fri envelope of the top-5 most-common bill_dates). Falls back
        // to earliest_bill_date's Friday for pre-backfill files so the viz
        // doesn't go blank while the backfill is pending.
        const ddisMap = {};
        for (const f of ddisFiles) {
          let fri = f.bill_week_ending;
          if (!fri && f.earliest_bill_date) fri = fridayOf(f.earliest_bill_date);
          if (!fri) continue; // ambiguous or no dates
          if (!ddisMap[fri]) ddisMap[fri] = { files: 0, paid: 0 };
          ddisMap[fri].files = (ddisMap[fri].files || 0) + 1;
          ddisMap[fri].paid = (ddisMap[fri].paid || 0) + (f.total_paid || 0);
        }

        setStreams([
          { key: "delivery",    label: "Uline · Delivery",    color: T.brand,   map: deliveryMap,    primary: "stops", isPrimary: true,  fmtValue: (v) => `${fmtNum(v.stops)} stops · ${fmt(v.revenue)}` },
          { key: "truckload",   label: "Uline · Truckload",   color: T.purple,  map: truckloadMap,   primary: "stops", isPrimary: false, fmtValue: (v) => `${fmtNum(v.stops)} stops · ${fmt(v.revenue)}` },
          { key: "accessorial", label: "Uline · Accessorial", color: T.yellow,  map: accessorialMap, primary: "stops", isPrimary: false, fmtValue: (v) => `${v.stops ? fmtNum(v.stops)+" stops · " : ""}${fmt(v.revenue)}` },
          { key: "nuvizz",      label: "NuVizz",              color: T.blue,    map: nuvizzMap,      primary: "stops", isPrimary: false, fmtValue: (v) => `${fmtNum(v.stops)} stops · ${fmt(v.revenue)} SealNbr` },
          { key: "timeclock",   label: "Time Clock",          color: T.green,   map: timeclockMap,   primary: "hours", isPrimary: false, fmtValue: (v) => `${fmtNum(Math.round(v.hours))} hrs` },
          { key: "ddis",        label: "DDIS Payments",       color: T.red,     map: ddisMap,        primary: "files", isPrimary: false, fmtValue: (v) => `${v.files} file${v.files===1?"":"s"} · ${fmt(v.paid)}` },
        ]);
        setLoading(false);
      } catch (e) {
        console.error("CoverageTimeline load err:", e);
        setLoading(false);
      }
    })();
  }, []);

  if (loading) return <div style={cardStyle}><div style={{textAlign:"center",padding:20,color:T.textMuted,fontSize:12}}>Loading coverage data…</div></div>;
  if (!streams) return null;

  // Compute min/max week across all streams (plus date controls override)
  const allWeeks = new Set();
  for (const s of streams) for (const w of Object.keys(s.map)) allWeeks.add(w);
  if (allWeeks.size === 0) {
    return (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:6}}>📊 Coverage Timeline</div>
        <EmptyState icon="📤" title="No data yet" sub="Upload files in Data Ingest to see coverage across streams." />
      </div>
    );
  }

  let firstWeek, lastWeek;
  const sortedWeeks = Array.from(allWeeks).sort();
  const dataEarliest = sortedWeeks[0];
  const dataLatest = sortedWeeks[sortedWeeks.length - 1];

  const todayFri = (() => {
    const d = new Date();
    const day = d.getDay();
    const daysTo = (5 - day + 7) % 7;
    d.setDate(d.getDate() + daysTo);
    return d.toISOString().slice(0, 10);
  })();

  if (rangeMode === "2024") { firstWeek = "2024-01-05"; lastWeek = todayFri; }
  else if (rangeMode === "2025") { firstWeek = "2025-01-03"; lastWeek = todayFri; }
  else if (rangeMode === "last12") {
    const d = new Date();
    d.setMonth(d.getMonth() - 12);
    const day = d.getDay();
    const daysTo = (5 - day + 7) % 7;
    d.setDate(d.getDate() + daysTo);
    firstWeek = d.toISOString().slice(0, 10);
    lastWeek = todayFri;
  }
  else {
    // auto: data earliest, or 2024-01 if data reaches there, else 2025-01
    const dataFirstYear = parseInt(dataEarliest.slice(0,4), 10);
    if (dataFirstYear <= 2024) firstWeek = "2024-01-05";
    else firstWeek = "2025-01-03";
    lastWeek = dataLatest > todayFri ? dataLatest : todayFri;
  }

  // Build column array (weeks from firstWeek → lastWeek inclusive)
  const columns = [];
  const addDaysISO = (iso, n) => { const d = new Date(iso + "T00:00:00"); d.setDate(d.getDate()+n); return d.toISOString().slice(0,10); };
  {
    let cur = firstWeek;
    while (cur <= lastWeek) {
      columns.push(cur);
      cur = addDaysISO(cur, 7);
    }
  }

  // Figure out month boundaries for axis labels
  const monthLabels = [];
  let lastMonth = "";
  columns.forEach((w, i) => {
    const mo = w.slice(0, 7);
    if (mo !== lastMonth) {
      monthLabels.push({ i, label: new Date(w+"T00:00:00").toLocaleString("en-US",{month:"short",year:"2-digit"}) });
      lastMonth = mo;
    }
  });

  const CELL_W = 10;
  const CELL_H = 18;
  const GAP = 2;
  const ROW_GAP = 6;
  const LABEL_W = 150;
  const gridWidth = columns.length * (CELL_W + GAP);

  const coverageStats = streams.map(s => {
    const covered = columns.filter(w => s.map[w]).length;
    return { key: s.key, covered, total: columns.length, pct: columns.length > 0 ? Math.round(covered/columns.length*100) : 0 };
  });

  // Uline Delivery hero is ALWAYS calculated from 2025-01-03 onwards —
  // independent of the range selector. User explicitly doesn't want to
  // track 2024 gaps and doesn't want to be warned about them.
  const PRIMARY_FROM = "2025-01-03"; // first Friday of 2025
  const deliveryStream = streams.find(s => s.key === "delivery");
  const primaryColumns = [];
  {
    const effectiveEnd = lastWeek;
    let cur = PRIMARY_FROM;
    while (cur <= effectiveEnd) {
      primaryColumns.push(cur);
      cur = addDaysISO(cur, 7);
    }
  }
  const deliveryGaps = primaryColumns.filter(w => !deliveryStream.map[w]);
  const deliveryCovered = primaryColumns.length - deliveryGaps.length;
  const deliveryPct = primaryColumns.length > 0 ? Math.round(deliveryCovered / primaryColumns.length * 100) : 0;
  const deliveryStats = { covered: deliveryCovered, total: primaryColumns.length, pct: deliveryPct };
  const deliveryRevenue = Object.entries(deliveryStream.map)
    .filter(([w]) => w >= PRIMARY_FROM)
    .reduce((sum, [, v]) => sum + (v.revenue || 0), 0);

  return (
    <div style={{...cardStyle, marginBottom:16}}>
      {/* Uline Delivery hero block — this is 90% of revenue, gaps here hurt most.
          Always scoped to 2025-01-03+ regardless of timeline range selector. */}
      <div style={{
        padding: "14px 16px",
        marginBottom: 16,
        borderRadius: 10,
        background: deliveryStats.pct >= 100 ? "#ecfdf5" : deliveryStats.pct >= 85 ? "#fffbeb" : "#fef2f2",
        border: `1px solid ${deliveryStats.pct >= 100 ? T.green : deliveryStats.pct >= 85 ? T.yellow : T.red}`,
      }}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:10,flexWrap:"wrap"}}>
          <div>
            <div style={{fontSize:11,fontWeight:700,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>
              Primary Revenue Stream
            </div>
            <div style={{fontSize:15,fontWeight:700,color:T.text}}>Uline · Delivery</div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>
              90%+ of revenue. Gaps here understate reported revenue. <strong style={{color:T.text}}>Tracked from Jan 2025.</strong>
            </div>
          </div>
          <div style={{display:"flex",gap:16,alignItems:"center"}}>
            <div style={{textAlign:"center"}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em"}}>Coverage</div>
              <div style={{fontSize:26,fontWeight:800,color: deliveryStats.pct >= 100 ? T.green : deliveryStats.pct >= 85 ? T.yellowText : T.redText, lineHeight:1.1}}>
                {deliveryStats.pct}%
              </div>
              <div style={{fontSize:10,color:T.textMuted}}>{deliveryStats.covered}/{deliveryStats.total} wks (2025+)</div>
            </div>
            <div style={{textAlign:"center"}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em"}}>Gaps</div>
              <div style={{fontSize:26,fontWeight:800,color: deliveryGaps.length === 0 ? T.green : T.redText, lineHeight:1.1}}>
                {deliveryGaps.length}
              </div>
              <div style={{fontSize:10,color:T.textMuted}}>missing (2025+)</div>
            </div>
            <div style={{textAlign:"center"}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em"}}>Revenue</div>
              <div style={{fontSize:26,fontWeight:800,color:T.text,lineHeight:1.1}}>
                {deliveryRevenue >= 1000000 ? `$${(deliveryRevenue/1000000).toFixed(1)}M` : deliveryRevenue >= 1000 ? `$${Math.round(deliveryRevenue/1000)}K` : fmt(deliveryRevenue)}
              </div>
              <div style={{fontSize:10,color:T.textMuted}}>2025+</div>
            </div>
          </div>
        </div>
        {deliveryGaps.length > 0 && (
          <div style={{marginTop:12,paddingTop:10,borderTop:`1px solid ${deliveryStats.pct >= 85 ? T.yellow+"50" : T.red+"40"}`}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",gap:8,flexWrap:"wrap",marginBottom:6}}>
              <div style={{fontSize:11,fontWeight:700,color:T.redText}}>🚨 {deliveryGaps.length} Missing Delivery Week{deliveryGaps.length===1?"":"s"} — find and upload these</div>
              <button
                onClick={() => {
                  const allNames = deliveryGaps.map(we => {
                    const fri = new Date(we + "T00:00:00");
                    const sat = new Date(fri); sat.setDate(sat.getDate() - 6);
                    const fmt = (d) => d.toISOString().slice(0,10).replace(/-/g,"");
                    return `das ${fmt(sat)}-${fmt(fri)}.xlsx`;
                  }).join("\n");
                  navigator.clipboard?.writeText(allNames).then(() => {
                    alert(`Copied ${deliveryGaps.length} filenames to clipboard.`);
                  }).catch(() => alert("Copy failed — your browser may block clipboard access."));
                }}
                style={{padding:"4px 10px",borderRadius:6,border:`1px solid ${T.red}`,background:"white",color:T.redText,fontSize:10,fontWeight:700,cursor:"pointer"}}
              >📋 Copy all {deliveryGaps.length}</button>
            </div>
            <div style={{fontSize:10,color:T.textMuted,marginBottom:8}}>Tap any filename to copy it. Filenames encode Sat start → Fri end (Uline naming convention).</div>
            <div style={{maxHeight:400,overflowY:"auto",background:"white",borderRadius:6,border:`1px solid ${T.red}20`}}>
              {deliveryGaps.map(we => {
                // Compute Sat start = Friday WE - 6 days
                const friDate = new Date(we + "T00:00:00");
                const satDate = new Date(friDate);
                satDate.setDate(satDate.getDate() - 6);
                const fmt = (d) => d.toISOString().slice(0,10).replace(/-/g,"");
                const expectedFilename = `das ${fmt(satDate)}-${fmt(friDate)}.xlsx`;
                const humanRange = `${satDate.toLocaleDateString("en-US",{month:"short",day:"numeric"})} – ${friDate.toLocaleDateString("en-US",{month:"short",day:"numeric",year:"numeric"})}`;
                return (
                  <div
                    key={we}
                    onClick={() => {
                      navigator.clipboard?.writeText(expectedFilename).catch(()=>{});
                    }}
                    style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:11,cursor:"pointer"}}
                    title="Click to copy filename"
                  >
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",gap:8,flexWrap:"wrap"}}>
                      <div style={{fontWeight:700,color:T.redText}}>WE {we}</div>
                      <div style={{fontSize:10,color:T.textMuted}}>{humanRange}</div>
                    </div>
                    <div style={{fontFamily:"monospace",fontSize:11,color:T.text,marginTop:4,wordBreak:"break-all"}}>
                      <strong>{expectedFilename}</strong>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12,flexWrap:"wrap",gap:10}}>
        <div style={{fontSize:13,fontWeight:700,color:T.text}}>📊 Coverage Timeline — All Streams</div>
        <div style={{display:"flex",gap:4,fontSize:11}}>
          {[
            { k: "auto", l: "Auto" },
            { k: "2024", l: "2024→" },
            { k: "2025", l: "2025→" },
            { k: "last12", l: "Last 12mo" },
          ].map(b => (
            <button key={b.k}
              onClick={() => setRangeMode(b.k)}
              style={{
                padding: "4px 10px",
                borderRadius: 6,
                border: `1px solid ${rangeMode === b.k ? T.brand : T.border}`,
                background: rangeMode === b.k ? T.brandPale : "white",
                color: rangeMode === b.k ? T.brand : T.textMuted,
                cursor: "pointer",
                fontWeight: rangeMode === b.k ? 600 : 400,
                fontSize: 11,
              }}>{b.l}</button>
          ))}
        </div>
      </div>

      <div style={{fontSize:11,color:T.textMuted,marginBottom:12}}>
        Each cell = one week (Friday-ending). Coloured = data present, grey = gap. Hover for details. <strong style={{color:T.text}}>Delivery row is emphasized</strong> — it's the primary revenue stream.
      </div>

      <div style={{overflowX:"auto",paddingBottom:8}}>
        <div style={{minWidth: LABEL_W + gridWidth + 40}}>
          {/* Month axis labels */}
          <div style={{display:"flex",position:"relative",height:18,marginBottom:6,marginLeft:LABEL_W}}>
            {monthLabels.map((m, idx) => (
              <div key={idx} style={{
                position:"absolute",
                left: m.i * (CELL_W + GAP),
                fontSize: 10,
                color: T.textMuted,
                whiteSpace: "nowrap",
                borderLeft: `1px solid ${T.borderLight}`,
                paddingLeft: 3,
                height: 18,
              }}>{m.label}</div>
            ))}
          </div>

          {/* Stream rows — primary stream (Delivery) rendered larger & bolder */}
          {streams.map((s, sIdx) => {
            const stats = coverageStats[sIdx];
            const cellH = s.isPrimary ? 28 : 14;
            const cellW = CELL_W; // keep column alignment
            const rowGap = s.isPrimary ? 12 : ROW_GAP;
            const labelFontSize = s.isPrimary ? 13 : 11;
            const gapBg = s.isPrimary ? "#fca5a5" : T.borderLight; // red-tinted for primary gaps
            const gapOpacity = s.isPrimary ? 0.5 : 0.35;
            return (
              <div key={s.key} style={{display:"flex",alignItems:"center",marginBottom: rowGap, paddingBottom: s.isPrimary ? 10 : 0, borderBottom: s.isPrimary ? `2px solid ${T.borderLight}` : "none"}}>
                <div style={{width: LABEL_W, paddingRight: 10, fontSize: labelFontSize, color: T.text}}>
                  <div style={{fontWeight: s.isPrimary ? 700 : 600}}>
                    {s.isPrimary && <span style={{color: T.brand, marginRight: 4}}>★</span>}
                    {s.label}
                  </div>
                  <div style={{fontSize: 10, color: stats.pct >= 95 ? T.green : stats.pct >= 50 ? T.yellowText : T.textMuted, fontWeight: s.isPrimary ? 700 : 400}}>
                    {stats.covered}/{stats.total} weeks ({stats.pct}%)
                  </div>
                </div>
                <div style={{display:"flex",gap: GAP}}>
                  {columns.map((w, i) => {
                    const cell = s.map[w];
                    const present = !!cell;
                    return (
                      <div
                        key={i}
                        onMouseEnter={() => setHover({ streamIdx: sIdx, weekISO: w, cell })}
                        onMouseLeave={() => setHover(null)}
                        title={`${s.label} · WE ${w}${present ? " · " + s.fmtValue(cell) : " · no data"}`}
                        style={{
                          width: cellW,
                          height: cellH,
                          borderRadius: 2,
                          background: present ? s.color : gapBg,
                          opacity: present ? 1 : gapOpacity,
                          cursor: "pointer",
                          transition: "transform 0.1s",
                        }}
                      />
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {hover && (
        <div style={{marginTop:10,padding:"8px 12px",background:T.bgSurface,borderRadius:6,fontSize:11,color:T.text,border:`1px solid ${T.borderLight}`}}>
          <div style={{fontWeight:700}}>{streams[hover.streamIdx].label} · WE {hover.weekISO}</div>
          <div style={{color: hover.cell ? T.textMuted : T.red, marginTop:2}}>
            {hover.cell ? streams[hover.streamIdx].fmtValue(hover.cell) : "No data for this week"}
          </div>
        </div>
      )}
    </div>
  );
}

function DataCompleteness({ weeklyRollups, completeness, fileLog }) {
  const hasUline = weeklyRollups && weeklyRollups.length > 0 && completeness;
  const [streamCoverage, setStreamCoverage] = useState(null);
  const [inspectingWeek, setInspectingWeek] = useState(null); // week_ending string
  const [inspectorData, setInspectorData] = useState(null);
  const [inspectorLoading, setInspectorLoading] = useState(false);

  // When a week is selected for inspection, pull everything we know about it:
  // the rollup doc, any matching file_log entries, and any audit/unpaid records.
  const inspectWeek = async (weekEnding) => {
    setInspectingWeek(weekEnding);
    setInspectorData(null);
    setInspectorLoading(true);
    try {
      // The rollup doc
      const rollup = weeklyRollups.find(w => w.week_ending === weekEnding);

      // Find files whose date range overlaps OR is adjacent to this week.
      // Adjacency matters: Uline files cover Sat→Fri, but our week_ending is the
      // Friday end-of-week. A file covering the NEXT week (Sat X → Fri X+7) can
      // still contribute stops to THIS week because pickups dated Sat/Sun in that
      // file get bucketed back to THIS week's Friday via weekEndingFriday's
      // Sat→prior Fri rollback rule. So for WE 2025-01-03, the file
      // "das 20250104-20250110.xlsx" is a legitimate source even though no date
      // in its name matches 01/03.
      //
      // Strategy: parse the YYYYMMDD-YYYYMMDD range from each filename, compute
      // the file's covered date span (as an interval of dates), and match if the
      // file's span touches the target week OR the week immediately after
      // (adjacent-week rollback).
      const parseFilenameRange = (fn) => {
        if (!fn) return null;
        const m = fn.match(/(\d{8})\s*-\s*(\d{8})/);
        if (!m) return null;
        const parse8 = (s) => {
          // YYYYMMDD variant
          for (const y of ["2023","2024","2025","2026","2027"]) {
            if (s.startsWith(y)) {
              const mo = parseInt(s.slice(4,6),10), d = parseInt(s.slice(6,8),10);
              if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31) return `${y}-${String(mo).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
            }
          }
          // MMDDYYYY variant
          const mo = parseInt(s.slice(0,2),10), d = parseInt(s.slice(2,4),10), y = parseInt(s.slice(4,8),10);
          if (y >= 2023 && y <= 2027 && mo >= 1 && mo <= 12 && d >= 1 && d <= 31) {
            return `${y}-${String(mo).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
          }
          return null;
        };
        const start = parse8(m[1]);
        let end = parse8(m[2]);
        if (!start || !end) return null;
        // Year-off-by-one typo fix — if end < start, try same-year or next-year wrap
        if (end < start) {
          const sY = parseInt(start.slice(0,4),10);
          const fixed = `${sY}${end.slice(4)}`;
          end = fixed >= start ? fixed : `${sY+1}${end.slice(4)}`;
        }
        return { start, end };
      };

      // Compute the week's date span: Sat = WE - 6 days, Fri = WE.
      // With the corrected weekEndingFriday (forward-rolling), every stop that
      // a file produces lands inside the Sat-Fri span the filename describes —
      // there's no longer any boundary spillover to neighboring weeks.
      const addDays = (iso, n) => {
        const [y,m,d] = iso.split("-").map(Number);
        const dt = new Date(Date.UTC(y, m-1, d));
        dt.setUTCDate(dt.getUTCDate() + n);
        return dt.toISOString().slice(0,10);
      };
      const weekStart = addDays(weekEnding, -6);  // Saturday that starts this week
      const weekEnd = weekEnding;                  // Friday that ends this week

      const matchingFiles = (fileLog || []).map(f => {
        const range = parseFilenameRange(f.filename || "");
        if (!range) {
          // No date range in filename — fall back to substring match on the WE date itself
          const targetYMD = weekEnding.replace(/-/g, "");
          const fn = (f.filename || "").toLowerCase();
          if (fn.includes(targetYMD) || fn.includes(weekEnding)) {
            return { ...f, match_reason: "filename contains week date" };
          }
          return null;
        }
        // File's covered span overlaps this Sat-Fri window
        const overlapsThis = !(range.end < weekStart || range.start > weekEnd);
        if (overlapsThis) return { ...f, match_reason: "file covers this week", file_range: range };
        return null;
      }).filter(Boolean);

      // Pull any recon doc for this week
      let reconDoc = null;
      try {
        if (hasFirebase) {
          const d = await window.db.collection("recon_weekly").doc(weekEnding).get();
          if (d.exists) reconDoc = d.data();
        }
      } catch (e) { /* ignore */ }

      setInspectorData({
        weekEnding,
        rollup: rollup || null,
        matchingFiles,
        reconDoc,
      });
    } catch(e) {
      console.error("inspectWeek error:", e);
      setInspectorData({ weekEnding, error: e.message });
    }
    setInspectorLoading(false);
  };

  // Load per-stream coverage counts in parallel so AIInsight gets a full picture.
  // Same data sources CoverageTimeline uses, but summarized to counts (not cells).
  useEffect(() => {
    (async () => {
      if (!hasFirebase) return;
      try {
        const [uline, nuvizz, timeclock, payroll, ddisFiles] = await Promise.all([
          FS.getWeeklyRollups(),
          FS.getNuVizzWeekly(),
          FS.getTimeClockWeekly(),
          FS.getPayrollWeekly(),
          FS.getDDISFiles(),
        ]);
        const start = "2025-01-01";
        const inRange = (w) => w >= start;

        // Expected Friday weeks from Jan 3, 2025 through the most recent past Friday
        const today = new Date();
        const dow = today.getDay();
        const daysBackToFri = dow >= 5 ? dow - 5 : dow + 2;
        const lastFri = new Date(today);
        lastFri.setDate(today.getDate() - daysBackToFri);
        const firstFri = new Date("2025-01-03T00:00:00");
        const expectedWeeks = [];
        for (let d = new Date(firstFri); d <= lastFri; d.setDate(d.getDate() + 7)) {
          expectedWeeks.push(d.toISOString().slice(0, 10));
        }
        const total = expectedWeeks.length;
        const pct = (covered) => total > 0 ? Math.round((covered / total) * 100) : 0;

        // Uline split by service_type
        const ulineIn = uline.filter(w => inRange(w.week_ending));
        const delWeeks = new Set(ulineIn.filter(w => (w.delivery_stops||w.stops||0) > 0).map(w => w.week_ending));
        const tkWeeks = new Set(ulineIn.filter(w => (w.truckload_stops||0) > 0).map(w => w.week_ending));
        const accWeeks = new Set(ulineIn.filter(w => (w.accessorial_stops||0) > 0 || (w.accessorial_revenue||0) > 0).map(w => w.week_ending));

        // DDIS: v2.40.9 — each file claims one week (bill_week_ending).
        // Files in the ambiguous state (week_ambiguous: true) don't count
        // until the user resolves them in Settings.
        const expectedWeeksSet = new Set(expectedWeeks);
        const ddisWeekSet = new Set();
        for (const f of ddisFiles) {
          let fri = f.bill_week_ending;
          if (!fri && f.earliest_bill_date && !f.week_ambiguous) fri = fridayOf(f.earliest_bill_date); // pre-backfill fallback
          if (fri && expectedWeeksSet.has(fri)) ddisWeekSet.add(fri);
        }

        const nvWeeks = new Set(nuvizz.filter(w => inRange(w.week_ending)).map(w => w.week_ending));
        const tcWeeks = new Set(timeclock.filter(w => inRange(w.week_ending)).map(w => w.week_ending));
        const payWeeks = new Set(payroll.filter(w => inRange(w.week_ending)).map(w => w.week_ending));

        setStreamCoverage({
          delivery:     { covered: delWeeks.size,  total, pct: pct(delWeeks.size) },
          truckload:    { covered: tkWeeks.size,   total, pct: pct(tkWeeks.size) },
          accessorials: { covered: accWeeks.size,  total, pct: pct(accWeeks.size) },
          ddis:         { covered: ddisWeekSet.size, total, pct: pct(ddisWeekSet.size) },
          nuvizz:       { covered: nvWeeks.size,   total, pct: pct(nvWeeks.size) },
          timeclock:    { covered: tcWeeks.size,   total, pct: pct(tcWeeks.size) },
          payroll:      { covered: payWeeks.size,  total, pct: pct(payWeeks.size) },
        });
      } catch(e) { console.error("streamCoverage err:", e); }
    })();
  }, []);

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="✅" text="Data Health" />

    {/* Coverage timeline — always rendered; loads its own data */}
    <CoverageTimeline />

    {/* AI Insight — deterministic checks + optional Claude analysis */}
    <AIInsight
      context="data-health"
      title="🔍 Health Check"
      data={{
        streamCoverage,
        completeness,
        fileLog: (fileLog || []).slice(0, 200),
        unknownFiles: (fileLog || []).filter(f => f.kind === "unknown").map(f => f.filename).slice(0, 20),
      }}
    />

    {/* Existing Uline Completeness view — only shown if Uline data exists */}
    {!hasUline && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:6}}>📋 Uline Completeness</div>
        <EmptyState icon="📤" title="No Uline revenue data yet" sub="Upload Uline weekly files in Data Ingest to see week-by-week coverage metrics." />
      </div>
    )}
    {hasUline && (() => {
      const { expected, gaps, sparseWeeks, missingAccessorials, avgStops, firstWE, lastWE } = completeness;
      const completePct = expected.length > 0 ? ((expected.length - gaps.length) / expected.length * 100) : 100;
      return (
        <>
          <div style={{fontSize:13,fontWeight:700,margin:"8px 0 12px",color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.08em"}}>Uline Completeness</div>
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
              <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These weeks have far fewer stops than your average of <strong>{fmtNum(avgStops)}</strong>. Tap a row to see what files are loaded for that week.</div>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                  <thead><tr>
                    <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgSurface}}>Week Ending</th>
                    <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgSurface}}>Stops</th>
                    <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgSurface}}>Revenue</th>
                    <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgSurface}}>Expected Stops</th>
                    <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgSurface}}>Gap</th>
                    <th style={{padding:"8px 10px",borderBottom:`1px solid ${T.border}`}}></th>
                  </tr></thead>
                  <tbody>
                    {sparseWeeks.map((w,i) => {
                      const isOpen = inspectingWeek === w.week_ending;
                      const handleInspect = (e) => {
                        e?.stopPropagation?.();
                        if (isOpen) setInspectingWeek(null);
                        else inspectWeek(w.week_ending);
                      };
                      return (
                      <tr key={i} style={{background: isOpen ? T.yellowBg : "transparent"}}>
                        <td onClick={handleInspect} style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600,cursor:"pointer"}}>WE {weekLabel(w.week_ending)}</td>
                        <td onClick={handleInspect} style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,cursor:"pointer"}}>{fmtNum(w.stops)}</td>
                        <td onClick={handleInspect} style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600,color:T.green,cursor:"pointer"}}>{fmt(w.revenue||0)}</td>
                        <td onClick={handleInspect} style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,cursor:"pointer"}}>{fmtNum(avgStops)}</td>
                        <td onClick={handleInspect} style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:600,cursor:"pointer"}}>{fmtNum(avgStops-w.stops)}</td>
                        <td style={{padding:"4px 6px",borderBottom:`1px solid ${T.borderLight}`}}>
                          <button type="button" onClick={handleInspect} style={{background:isOpen?T.yellow:T.brand,color:"#fff",border:"none",padding:"6px 10px",borderRadius:6,fontSize:10,fontWeight:700,cursor:"pointer"}}>{isOpen?"▼ Close":"🔬 Inspect"}</button>
                        </td>
                      </tr>
                    );})}
                  </tbody>
                </table>
              </div>

              {inspectingWeek && (
                <div style={{marginTop:12,padding:"12px 14px",background:T.bgSurface,borderRadius:8,border:`2px solid ${T.yellow}50`}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
                    <div style={{fontSize:13,fontWeight:700}}>🔬 Week Inspector — WE {weekLabel(inspectingWeek)}</div>
                    <button onClick={()=>setInspectingWeek(null)} style={{background:"none",border:"none",color:T.textMuted,fontSize:14,cursor:"pointer"}}>✕</button>
                  </div>

                  {inspectorLoading && <div style={{fontSize:12,color:T.textMuted}}>Loading data for this week…</div>}

                  {inspectorData && !inspectorLoading && (() => {
                    const d = inspectorData;
                    const r = d.rollup;
                    const hasDel = r && ((r.delivery_stops||0) > 0);
                    const hasTk = r && ((r.truckload_stops||0) > 0);
                    const hasAcc = r && ((r.accessorial_stops||0) > 0 || (r.accessorial_revenue||0) > 0);

                    // Diagnose what's missing
                    const diagnosis = [];
                    if (!r) {
                      diagnosis.push({ severity: "alarm", text: "❌ No rollup document at all for this week" });
                    } else {
                      if (!hasDel) diagnosis.push({ severity: "alarm", text: "❌ Missing delivery data — main das xlsx was never ingested" });
                      if (hasAcc && !hasDel) diagnosis.push({ severity: "alarm", text: "⚠️ Only accessorials are loaded — this is your bug: accessorials ingested but main delivery file was not" });
                      if (!hasTk && !hasDel) diagnosis.push({ severity: "info", text: "No truckload data either (may not run TK this week)" });
                      if (hasDel && r.delivery_stops < 500) diagnosis.push({ severity: "warn", text: `Delivery stops (${r.delivery_stops}) are well below average — could be a holiday or partial ingest` });
                    }

                    return (
                      <div style={{display:"flex",flexDirection:"column",gap:12}}>
                        {/* Diagnosis */}
                        {diagnosis.length > 0 && (
                          <div style={{display:"flex",flexDirection:"column",gap:6}}>
                            {diagnosis.map((dx, i) => (
                              <div key={i} style={{
                                padding:"8px 10px",borderRadius:6,fontSize:12,
                                background: dx.severity === "alarm" ? T.redBg : dx.severity === "warn" ? T.yellowBg : T.bgSurface,
                                color: dx.severity === "alarm" ? T.redText : dx.severity === "warn" ? T.yellowText : T.text,
                                border: `1px solid ${dx.severity === "alarm" ? T.red : dx.severity === "warn" ? T.yellow : T.borderLight}40`,
                              }}>{dx.text}</div>
                            ))}
                          </div>
                        )}

                        {/* Rollup breakdown */}
                        <div>
                          <div style={{fontSize:10,fontWeight:700,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:6}}>What's in the Rollup</div>
                          {!r ? (
                            <div style={{fontSize:12,color:T.textMuted,fontStyle:"italic"}}>No rollup doc exists for this week.</div>
                          ) : (
                            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:8}}>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid ${hasDel?T.green+"40":T.red+"40"}`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>Delivery</div>
                                <div style={{fontSize:14,fontWeight:700,color:hasDel?T.green:T.red}}>{fmtNum(r.delivery_stops||0)} stops</div>
                                <div style={{fontSize:10,color:T.textMuted}}>{fmt(r.delivery_revenue||0)}</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid ${hasTk?T.green+"40":T.borderLight}`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>Truckload</div>
                                <div style={{fontSize:14,fontWeight:700,color:hasTk?T.green:T.textDim}}>{fmtNum(r.truckload_stops||0)} stops</div>
                                <div style={{fontSize:10,color:T.textMuted}}>{fmt(r.truckload_revenue||0)}</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid ${hasAcc?T.green+"40":T.yellow+"40"}`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>Accessorials</div>
                                <div style={{fontSize:14,fontWeight:700,color:hasAcc?T.green:T.yellowText}}>{fmtNum(r.accessorial_stops||0)} items</div>
                                <div style={{fontSize:10,color:T.textMuted}}>{fmt(r.accessorial_revenue||0)}</div>
                              </div>
                            </div>
                          )}
                        </div>

                        {/* Files matching this week */}
                        <div>
                          <div style={{fontSize:10,fontWeight:700,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:6}}>Files contributing to this week ({d.matchingFiles.length})</div>
                          {d.matchingFiles.length === 0 ? (
                            <div style={{fontSize:12,color:T.redText,fontStyle:"italic"}}>⚠️ No files in file_log touch this week, even checking adjacent weeks. The rollup may have been built from files that are no longer logged.</div>
                          ) : (
                            <div style={{fontSize:11,display:"flex",flexDirection:"column",gap:4}}>
                              {d.matchingFiles.map((f,i) => {
                                const rangeLabel = f.file_range ? ` • covers ${f.file_range.start} → ${f.file_range.end}` : "";
                                return (
                                <div key={i} style={{padding:"6px 10px",background:"white",borderRadius:6,border:`1px solid ${f.adjacent?T.yellow+"40":T.borderLight}`,display:"flex",flexDirection:"column",gap:2}}>
                                  <div style={{display:"flex",justifyContent:"space-between",gap:8,flexWrap:"wrap",alignItems:"center"}}>
                                    <span style={{fontFamily:"monospace",fontWeight:600,fontSize:11}}>📎 {f.filename}</span>
                                    <span style={{color:T.textMuted,fontSize:10,whiteSpace:"nowrap"}}>{f.kind}{f.service_type?`/${f.service_type}`:""} • {fmtNum(f.row_count||0)} rows</span>
                                  </div>
                                  <div style={{fontSize:10,color:f.adjacent?T.yellowText:T.textMuted,fontStyle:f.adjacent?"italic":"normal"}}>
                                    {f.adjacent ? "⤴️ " : "✓ "}{f.match_reason}{rangeLabel}
                                    {f.uploaded_at && <span style={{color:T.textDim}}> • uploaded {new Date(f.uploaded_at).toLocaleString("en-US",{month:"short",day:"numeric",year:"numeric",hour:"numeric",minute:"2-digit"})}</span>}
                                  </div>
                                </div>
                              );})}
                            </div>
                          )}
                        </div>

                        {/* Expected filename for re-upload */}
                        <div style={{padding:"8px 10px",background:"#eff6ff",borderRadius:6,fontSize:11,border:`1px solid #3b82f640`}}>
                          <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",marginBottom:4}}>To fix — find this file in Gmail or backups</div>
                          <div style={{fontFamily:"monospace",fontWeight:600}}>
                            {(() => {
                              // Compute the WE-6days → WE filename: das YYYYMMDD-YYYYMMDD.xlsx (Sat→Fri)
                              const fri = new Date(inspectingWeek + "T00:00:00");
                              const sat = new Date(fri);
                              sat.setDate(fri.getDate() - 6);
                              const ymd = (x) => x.toISOString().slice(0,10).replace(/-/g,"");
                              return `das ${ymd(sat)}-${ymd(fri)}.xlsx`;
                            })()}
                          </div>
                        </div>
                      </div>
                    );
                  })()}
                </div>
              )}
            </div>
          )}
          {missingAccessorials.length > 0 && (
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12,color:T.yellowText}}>⚠️ Weeks Missing Accessorial File ({missingAccessorials.length})</div>
              <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These weeks have an original file but no accessorial (extra charges) file uploaded.</div>
              <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(140px,1fr))",gap:6}}>
                {missingAccessorials.map(w => (
                  <div key={w} style={{padding:"8px 12px",borderRadius:6,background:T.yellowBg,color:T.yellowText,fontSize:12,fontWeight:600,textAlign:"center",border:`1px solid ${T.yellow}50`}}>
                    WE {weekLabel(w)}
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      );
    })()}

    {fileLog.length > 0 && (
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Upload Log (last {Math.min(fileLog.length,50)})</div>
        <div style={{overflowX:"auto",maxHeight:400}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><tr>
              <th style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>Filename</th>
              <th style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>Type</th>
              <th style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>Rows</th>
              <th style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>Uploaded</th>
            </tr></thead>
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
function DataIngest({ weeklyRollups, reconMeta, fileLog, onRefresh }) {
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState("");
  const [progress, setProgress] = useState({ current:0, total:0 });
  const [lastResult, setLastResult] = useState(null);
  const [sourceStats, setSourceStats] = useState(null);
  const [pendingReview, setPendingReview] = useState(null); // {audit, expandedFiles} while awaiting user confirmation
  const [sourceConflicts, setSourceConflicts] = useState([]); // Uline vs Davis conflicts pending review
  const [resolvingWeek, setResolvingWeek] = useState(null); // week_ending currently being resolved
  const [dragOver, setDragOver] = useState(false); // drop-zone visual state
  const fileRef = useRef(null);

  // Upload History sort — hook must live at component top level (not inside the
  // conditional IIFE that renders the table). Covers column uses the filename-
  // parsing helper defined there, duplicated here to keep the accessor local.
  const historyAccessors = {
    filename: (f) => (f.filename || "").toLowerCase(),
    covers:   (f) => {
      if (!f.filename) return null;
      const m = f.filename.match(/(\d{8})\s*-\s*(\d{8})/);
      if (!m) return null;
      const s = m[1];
      for (const y of ["2023","2024","2025","2026","2027"]) {
        if (s.startsWith(y)) return `${y}-${s.slice(4,6)}-${s.slice(6,8)}`;
      }
      return `${s.slice(4,8)}-${s.slice(0,2)}-${s.slice(2,4)}`;
    },
    kind:     (f) => (f.kind || "").toLowerCase(),
    group:    (f) => (f.group || "").toLowerCase(),
    row_count:(f) => Number(f.row_count || 0),
    uploaded_at: (f) => f.uploaded_at || null,
  };
  const historySort = useSortable(fileLog || [], "uploaded_at", "desc", historyAccessors);

  // Load source conflicts from Firestore — survive across page loads
  const loadSourceConflicts = async () => {
    if (!hasFirebase) return;
    try {
      const all = await FS.getSourceConflicts();
      // Only show unresolved (resolved === null)
      setSourceConflicts(all.filter(c => !c.resolved));
    } catch(e) { console.error("loadSourceConflicts err:", e); }
  };
  useEffect(() => { loadSourceConflicts(); }, []);

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
    setStatus(`Preparing ${files.length} file${files.length>1?"s":""}...`);
    setProgress({ current:0, total:0 });

    let expanded;
    try {
      expanded = await unzipIfNeeded(Array.from(files));
    } catch(e) {
      setStatus("");
      setUploading(false);
      alert("Could not unpack zip: " + e.message);
      return;
    }

    // PASS 1: initial audit to find typos
    const firstPass = auditFilenames(expanded);

    // Auto-rename any files with suggestedRename (in memory only — disk untouched)
    const { files: correctedFiles, renameMap } = applyTypoFixes(expanded, firstPass);

    // PASS 2: re-audit with the corrected names so duplicates/gaps reflect reality
    const audit = auditFilenames(correctedFiles);
    // Attach rename info so the review UI can show what was auto-fixed
    audit.autoRenamed = Array.from(renameMap.entries()).map(([from, to]) => ({ from, to }));

    // Decide whether to show the pre-upload review
    const anyZip = Array.from(files).some(f => (f.name || "").toLowerCase().endsWith(".zip"));
    const needsReview = anyZip || audit.summary.typos > 0 || audit.summary.unparsed > 0 ||
                        audit.summary.duplicates > 0 || audit.summary.missingWeeks > 0 ||
                        audit.summary.missingAccessorials > 0 || audit.autoRenamed.length > 0;

    if (needsReview) {
      setPendingReview({ audit, expandedFiles: correctedFiles });
      setUploading(false);
      setStatus("");
      return;
    }
    // No issues — straight to ingest
    await doIngest(correctedFiles);
  };

  const proceedWithPending = async () => {
    if (!pendingReview) return;
    const { expandedFiles } = pendingReview;
    setPendingReview(null);
    setUploading(true);
    await doIngest(expandedFiles);
  };

  const cancelPending = () => {
    setPendingReview(null);
    setStatus("");
  };

  const doIngest = async (files) => {
    if (!files || files.length === 0) { setUploading(false); return; }
    setUploading(true);
    setStatus(`Processing ${files.length} file${files.length>1?"s":""}...`);
    setProgress({ current:0, total:files.length });

    // Per-group accumulators
    const stopsByPro = {};         // Uline: pro -> merged stop
    const paymentByPro = {};       // DDIS
    const ddisFileRecords = [];
    const ddisPayments = [];       // per-PRO rows to persist for audit rebuild
    const nuvizzStops = [];
    const timeClockEntries = [];
    const payrollEntries = [];
    const qboEntries = [];
    const fileLogs = [];
    const sourceFiles = [];
    const countsByKind = { master:0, original:0, accessorials:0, ddis:0, nuvizz:0, timeclock:0, payroll:0, qbo_pl:0, qbo_tb:0, qbo_gl:0, unknown:0 };
    const unknownFiles = [];
    // Source conflict tracking — mirrors the logic in ingestFiles
    const weekSources = {};        // weekKey → Set of sources
    const fileWeekImpact = [];     // [{filename, source, kind, weeks, stop_count}]

    for (let i=0; i<files.length; i++) {
      const file = files[i];
      setProgress({ current: i+1, total: files.length });
      setStatus(`[${i+1}/${files.length}] ${file.name}...`);
      const fileId = file.name.replace(/[^a-z0-9._-]/gi,"_").slice(0,140);
      const source = file._source || "manual";
      const emailFrom = file._emailFrom || null;

      // PDFs are backup/evidence docs — log for audit trail, don't parse
      if (file._isPdf || file.name.toLowerCase().endsWith(".pdf")) {
        const pdfKind = /backup/i.test(file.name) ? "pdf-backup" : "pdf-other";
        fileLogs.push({ file_id: fileId, filename: file.name, kind: pdfKind, group: "evidence", row_count: 0, source, email_from: emailFrom, uploaded_at: new Date().toISOString() });
        continue;
      }

      try {
        let rows;
        const isCSV = file.name.toLowerCase().endsWith(".csv");
        if (isCSV) rows = await readCSV(file);
        else rows = await readWorkbook(file);
        if (!rows || rows.length === 0) { unknownFiles.push(file.name + " (empty)"); countsByKind.unknown++; continue; }

        const kind = detectFileType(file.name, rows[0]);
        const group = sourceGroup(kind);
        const serviceType = detectServiceType(file.name, kind);
        countsByKind[kind] = (countsByKind[kind]||0) + 1;

        const logEntry = { file_id: fileId, filename: file.name, kind, group, service_type: serviceType, row_count: rows.length, source, email_from: emailFrom, uploaded_at: new Date().toISOString() };
        fileLogs.push(logEntry);
        sourceFiles.push(logEntry);

        // ─── Uline family ───
        if (kind === "master" || kind === "original" || kind === "accessorials") {
          const stops = parseOriginalOrAccessorial(rows, serviceType);
          const weeksTouched = new Set();
          for (const s of stops) {
            if (!s.pro || !s.week_ending) continue;
            weeksTouched.add(s.week_ending);
            if (!s._source) s._source = source;
            const key = `${s.pro}|${s.service_type}`; // key by PRO + service_type
            const existing = stopsByPro[key];
            if (!existing || (s.new_cost > existing.new_cost)) stopsByPro[key] = s;
          }
          for (const we of weeksTouched) {
            if (!weekSources[we]) weekSources[we] = new Set();
            weekSources[we].add(source);
          }
          fileWeekImpact.push({
            filename: file.name, source, kind, service_type: serviceType,
            weeks: Array.from(weeksTouched).sort(),
            stop_count: stops.length,
          });
        } else if (kind === "ddis") {
          const payments = parseDDIS(rows);
          const billDates = payments.map(p => p.bill_date).filter(Boolean).sort();
          const totalPaid = payments.reduce((s,p) => s + p.paid, 0);
          const wk = computeBillWeekEnding(billDates);
          ddisFileRecords.push({
            file_id: fileId, filename: file.name,
            record_count: payments.length, total_paid: totalPaid,
            earliest_bill_date: billDates[0] || null,
            latest_bill_date: billDates[billDates.length-1] || null,
            bill_week_ending: wk.bill_week_ending,
            week_ambiguous: wk.week_ambiguous,
            ambiguous_candidates: wk.ambiguous_candidates,
            top5_bill_dates: wk.top5,
            covers_weeks: wk.covers_weeks,
            checks: [...new Set(payments.map(p => p.check).filter(Boolean))],
            uploaded_at: new Date().toISOString(),
          });
          for (const p of payments) {
            paymentByPro[p.pro] = (paymentByPro[p.pro] || 0) + p.paid;
            if (p.pro && p.paid > 0) {
              const payId = `${p.pro}_${p.bill_date || "nodate"}_${p.check || "nocheck"}`;
              ddisPayments.push({
                id: payId, pro: p.pro, paid_amount: p.paid,
                bill_date: p.bill_date || null, check: p.check || null,
                voucher: p.voucher || null, source_file: file.name,
                uploaded_at: new Date().toISOString(),
              });
            }
          }
        }
        // ─── NuVizz ───
        else if (kind === "nuvizz") {
          for (const s of parseNuVizz(rows)) nuvizzStops.push(s);
        }
        // ─── Time Clock ───
        else if (kind === "timeclock") {
          for (const e of parseTimeClock(rows)) timeClockEntries.push(e);
        }
        // ─── Payroll ───
        else if (kind === "payroll") {
          for (const p of parsePayroll(rows)) payrollEntries.push(p);
        }
        // ─── QBO ───
        else if (kind === "qbo_pl" || kind === "qbo_tb" || kind === "qbo_gl") {
          for (const e of parseQBO(rows, kind)) qboEntries.push({...e, source_file: file.name});
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

    // ─── Source conflict detection (Uline vs Davis) ───
    // Same logic as ingestFiles() — any week where files from more than one
    // source contributed gets a conflict doc written to Firestore for review.
    const byWeekSource = {};
    for (const s of allStops) {
      const src = s._source || "manual";
      const key = `${s.week_ending}|${src}`;
      if (!byWeekSource[key]) byWeekSource[key] = {
        stop_count: 0, delivery_stops: 0, truckload_stops: 0, accessorial_stops: 0,
        revenue: 0, delivery_revenue: 0, truckload_revenue: 0, accessorial_revenue: 0,
      };
      const b = byWeekSource[key];
      const nc = s.new_cost || 0;
      b.stop_count++;
      b.revenue += nc;
      if (s.service_type === "truckload") { b.truckload_stops++; b.truckload_revenue += nc; }
      else if (s.service_type === "accessorial") { b.accessorial_stops++; b.accessorial_revenue += nc; }
      else { b.delivery_stops++; b.delivery_revenue += nc; }
    }
    const detectedConflicts = [];
    for (const [we, sources] of Object.entries(weekSources)) {
      if (sources.size > 1) {
        const filesForWeek = fileWeekImpact
          .filter(f => f.weeks.includes(we))
          .map(f => ({ filename: f.filename, source: f.source, kind: f.kind, service_type: f.service_type, stop_count: f.stop_count }));
        const summaries = {};
        for (const src of sources) {
          summaries[src] = byWeekSource[`${we}|${src}`] || {
            stop_count: 0, delivery_stops: 0, truckload_stops: 0, accessorial_stops: 0,
            revenue: 0, delivery_revenue: 0, truckload_revenue: 0, accessorial_revenue: 0,
          };
        }
        detectedConflicts.push({
          week_ending: we, sources: Array.from(sources), files: filesForWeek, summaries,
        });
      }
    }

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
            code: s.code, weight: s.weight, order: s.order,
            service_type: s.service_type || "delivery", // v2.40.14: carry service_type for TK filter
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
    // Persist per-PRO payments so audit rebuild has real match data
    if (ddisPayments.length > 0) {
      const toSave = ddisPayments.slice(0, 10000);
      for (let i = 0; i < toSave.length; i += 25) {
        const batch = toSave.slice(i, i + 25);
        await Promise.all(batch.map(p => FS.saveDDISPayment(p.id, p)));
      }
    }
    const topUnpaid = unpaidStops.sort((a,b) => b.billed - a.billed).slice(0, 500);
    for (const s of topUnpaid) await FS.saveUnpaidStop(s.pro, s);

    // ─── NuVizz: server-side ingest (v2.40.34) ───
    let nvWeeksSaved = 0, nvStopsSaved = 0;
    if (nuvizzStops.length > 0) {
      try {
        const { run_id, saved_ok, status } = await serverSaveNuVizzStops(
          nuvizzStops, "direct_upload",
          (s) => setStatus(s.message || "Saving NuVizz...")
        );
        nvStopsSaved = saved_ok || 0;
        nvWeeksSaved = status?.weeks_rebuilt || 0;
      } catch (e) {
        setStatus(`✗ NuVizz server ingest failed: ${e.message}`);
      }
    }

    // ─── Time Clock: weekly rollups + daily shifts ───
    let tcWeeksSaved = 0, tcDaysSaved = 0;
    if (timeClockEntries.length > 0) {
      setStatus(`Building time clock weekly rollups (${timeClockEntries.length} entries)...`);
      const tcWeekly = buildTimeClockWeekly(timeClockEntries);
      for (const w of tcWeekly) {
        const ok = await FS.saveTimeClockWeekly(w.week_ending, {...w, updated_at:new Date().toISOString()});
        if (ok) tcWeeksSaved++;
      }
      // v2.47.0: also persist per-shift entries to timeclock_daily for the
      // clock-in→first-stop forensics on DriverPerformanceTab.
      // v2.47.2: parallel batches of 25 with UI yield between batches —
      // matches the pattern used for ddisPayments above. The earlier
      // sequential for-loop locked up mobile Safari for ~8 min on a
      // 3,408-entry file.
      const tcDaily = buildTimeClockDaily(timeClockEntries);
      setStatus(`Saving time clock daily shifts (0/${tcDaily.length})...`);
      const TC_BATCH = 25;
      for (let i = 0; i < tcDaily.length; i += TC_BATCH) {
        const batch = tcDaily.slice(i, i + TC_BATCH);
        const results = await Promise.all(batch.map(d => {
          const { doc_id, ...fields } = d;
          return FS.saveTimeClockDaily(doc_id, {...fields, updated_at:new Date().toISOString()});
        }));
        tcDaysSaved += results.filter(r => r).length;
        // Status + UI yield every batch so the tab stays responsive on mobile
        setStatus(`Saving time clock daily shifts (${Math.min(i+TC_BATCH, tcDaily.length)}/${tcDaily.length})...`);
        await new Promise(r => setTimeout(r, 0));
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

    // Persist source conflicts so the user can review them across sessions
    for (const c of detectedConflicts) {
      await FS.saveSourceConflict(c.week_ending, {
        week_ending: c.week_ending,
        sources: c.sources,
        files: c.files,
        summaries: c.summaries,
        detected_at: new Date().toISOString(),
        resolved: null,
        resolved_at: null,
        resolved_by: null,
      });
    }

    setLastResult({
      files_processed: files.length,
      counts: countsByKind,
      uline: { stops: allStops.length, weeks_saved: savedWeeks, recon_saved: savedRecon, unpaid_saved: topUnpaid.length, payments: Object.keys(paymentByPro).length },
      nuvizz: { stops: nuvizzStops.length, weeks_saved: nvWeeksSaved, stops_saved: nvStopsSaved },
      timeclock: { entries: timeClockEntries.length, weeks_saved: tcWeeksSaved, days_saved: tcDaysSaved },
      payroll: { entries: payrollEntries.length, weeks_saved: payWeeksSaved },
      qbo: { lines: qboEntries.length, periods_saved: qboPeriodsSaved },
      unknown: unknownFiles,
      source_conflicts: detectedConflicts,
    });
    setStatus(`✓ Processed ${files.length} files across ${Object.values(countsByKind).filter(c=>c>0).length} source types`);
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
    onRefresh();
    loadSourceConflicts(); // Check for new source conflicts from this batch

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
        <input ref={fileRef} type="file" accept=".xlsx,.xls,.csv,.zip,.pdf,.eml" multiple onChange={e=>handleFiles(e.target.files)} style={{display:"none"}} />
        <PrimaryBtn text={uploading?"Processing...":"Upload Files"} onClick={()=>fileRef.current?.click()} loading={uploading} />
      </div>
    } />

    {sourceConflicts.length > 0 && (
      <div style={{...cardStyle, borderColor: "#f59e0b", borderWidth: 2, background: "#fffbeb", marginBottom: 16}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,marginBottom:10}}>
          <div>
            <div style={{fontSize:14,fontWeight:700,color:"#92400e"}}>⚠️ Source Conflicts — {sourceConflicts.length} week{sourceConflicts.length>1?"s":""} have files from both Uline and Davis</div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:4,lineHeight:1.5}}>
              Both files contributed to each week's rollup (highest $/PRO wins per stop). Davis corrections typically run higher than Uline originals — review and confirm, or override.
            </div>
          </div>
        </div>

        {sourceConflicts.map(c => {
          const davis = c.summaries?.davis || { stop_count:0, delivery_stops:0, truckload_stops:0, accessorial_stops:0, revenue:0, accessorial_revenue:0, delivery_revenue:0, truckload_revenue:0 };
          const uline = c.summaries?.uline || { stop_count:0, delivery_stops:0, truckload_stops:0, accessorial_stops:0, revenue:0, accessorial_revenue:0, delivery_revenue:0, truckload_revenue:0 };
          const davisWins = davis.revenue >= uline.revenue;
          const resolving = resolvingWeek === c.week_ending;
          return (
            <div key={c.week_ending} style={{
              background: "white",
              borderRadius: 8,
              border: `1px solid ${davisWins ? T.green+"40" : T.red+"40"}`,
              padding: "10px 12px",
              marginBottom: 8,
            }}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",gap:8,flexWrap:"wrap",marginBottom:6}}>
                <div style={{fontWeight:700,fontSize:13}}>WE {c.week_ending}</div>
                <div style={{fontSize:10,color:T.textMuted}}>{(c.files||[]).length} file{(c.files||[]).length>1?"s":""} from {c.sources?.join(" + ")}</div>
              </div>

              {/* Side-by-side summaries */}
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,fontSize:11}}>
                <div style={{padding:"8px 10px",borderRadius:6,background: davisWins ? "#ecfdf5" : T.bgSurface, border:`1px solid ${davisWins ? T.green+"50" : T.borderLight}`}}>
                  <div style={{fontSize:10,fontWeight:700,color:"#92400e",textTransform:"uppercase",letterSpacing:"0.05em",marginBottom:4}}>
                    ✏️ Davis correction {davisWins && <span style={{color:T.green,marginLeft:4}}>✓ winning</span>}
                  </div>
                  <div>Stops: <strong>{fmtNum(davis.stop_count)}</strong></div>
                  <div>Revenue: <strong>{fmt(davis.revenue)}</strong></div>
                  <div style={{color:T.textMuted,fontSize:10,marginTop:2}}>
                    Del: {fmtNum(davis.delivery_stops)} · TK: {fmtNum(davis.truckload_stops)} · Acc: {fmtNum(davis.accessorial_stops)} ({fmt(davis.accessorial_revenue)})
                  </div>
                </div>
                <div style={{padding:"8px 10px",borderRadius:6,background: !davisWins ? "#fef2f2" : T.bgSurface, border:`1px solid ${!davisWins ? T.red+"50" : T.borderLight}`}}>
                  <div style={{fontSize:10,fontWeight:700,color:T.brand,textTransform:"uppercase",letterSpacing:"0.05em",marginBottom:4}}>
                    📦 Uline original {!davisWins && <span style={{color:T.red,marginLeft:4}}>⚠️ winning</span>}
                  </div>
                  <div>Stops: <strong>{fmtNum(uline.stop_count)}</strong></div>
                  <div>Revenue: <strong>{fmt(uline.revenue)}</strong></div>
                  <div style={{color:T.textMuted,fontSize:10,marginTop:2}}>
                    Del: {fmtNum(uline.delivery_stops)} · TK: {fmtNum(uline.truckload_stops)} · Acc: {fmtNum(uline.accessorial_stops)} ({fmt(uline.accessorial_revenue)})
                  </div>
                </div>
              </div>

              {/* Outcome narrative */}
              <div style={{marginTop:8,padding:"6px 10px",borderRadius:6,background: davisWins?"#ecfdf5":"#fef2f2",fontSize:11,color: davisWins?"#065f46":T.redText}}>
                {davisWins
                  ? `✓ Davis revenue is ${fmt(davis.revenue - uline.revenue)} higher — correction applied successfully.`
                  : `⚠️ Uline revenue is ${fmt(uline.revenue - davis.revenue)} higher than Davis. Unusual — verify the Davis file wasn't a partial correction.`}
              </div>

              {/* Files list */}
              <details style={{marginTop:6}}>
                <summary style={{fontSize:10,color:T.textMuted,cursor:"pointer"}}>Show {(c.files||[]).length} file{(c.files||[]).length>1?"s":""}</summary>
                <div style={{marginTop:4,fontSize:10,fontFamily:"monospace",color:T.textMuted,lineHeight:1.6}}>
                  {(c.files||[]).map((f, i) => (
                    <div key={i}>
                      <span style={{color: f.source==="davis"?"#92400e":T.brand,fontWeight:600}}>[{f.source}]</span> {f.filename} ({f.kind}{f.service_type?`/${f.service_type}`:""}, {fmtNum(f.stop_count||0)} rows)
                    </div>
                  ))}
                </div>
              </details>

              {/* Action buttons */}
              <div style={{marginTop:8,display:"flex",gap:6,flexWrap:"wrap"}}>
                <button
                  disabled={resolving}
                  onClick={async () => {
                    setResolvingWeek(c.week_ending);
                    await FS.updateSourceConflict(c.week_ending, {
                      resolved: "accept-current",
                      resolved_at: new Date().toISOString(),
                      resolved_note: davisWins ? "Davis winning — accepted as-is" : "Uline winning — accepted as-is",
                    });
                    setSourceConflicts(prev => prev.filter(x => x.week_ending !== c.week_ending));
                    setResolvingWeek(null);
                  }}
                  style={{padding:"6px 12px",borderRadius:6,border:`1px solid ${T.green}`,background:"white",color:"#065f46",fontSize:11,fontWeight:700,cursor:resolving?"wait":"pointer"}}
                >✓ Mark reviewed · accept current rollup</button>
                <button
                  disabled={resolving}
                  onClick={async () => {
                    if (!confirm(`Flag WE ${c.week_ending} for manual re-ingestion?\n\nThis marks the conflict as needing attention. You'll need to delete or re-upload files to fully override the rollup.`)) return;
                    setResolvingWeek(c.week_ending);
                    await FS.updateSourceConflict(c.week_ending, {
                      resolved: "needs-manual",
                      resolved_at: new Date().toISOString(),
                      resolved_note: "User flagged for manual follow-up",
                    });
                    setSourceConflicts(prev => prev.filter(x => x.week_ending !== c.week_ending));
                    setResolvingWeek(null);
                  }}
                  style={{padding:"6px 12px",borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,color:T.textMuted,fontSize:11,fontWeight:600,cursor:resolving?"wait":"pointer"}}
                >Flag for manual follow-up</button>
              </div>
            </div>
          );
        })}
      </div>
    )}

    {pendingReview && (() => {
      const a = pendingReview.audit;
      const weekLabels = (ws) => ws.map(w => {
        try { const d = new Date(w+"T00:00:00"); return `WE ${d.getMonth()+1}/${d.getDate()}/${String(d.getFullYear()).slice(-2)}`; }
        catch { return w; }
      });
      const canProceed = a.summary.unparsed === 0 && a.summary.typos === 0 && a.summary.duplicates === 0;
      return (
        <div style={{...cardStyle, borderColor:T.yellow, borderWidth:2, background:"#fffbeb"}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
            <div style={{fontSize:14,fontWeight:700,color:T.yellowText}}>🔍 Pre-Upload Review</div>
            <div style={{fontSize:11,color:T.textMuted}}>{a.summary.total} files detected</div>
          </div>

          {/* Summary grid */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:10,marginBottom:16}}>
            <div style={{background:T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Delivery (regular)</div>
              <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.delivery || 0}</div>
            </div>
            <div style={{background:T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Truckload (TK)</div>
              <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.truckload || 0}</div>
            </div>
            <div style={{background:T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Accessorials</div>
              <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.accessorials}</div>
            </div>
            {(a.summary.ddis || 0) > 0 && (
              <div style={{background:T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>DDIS Payments</div>
                <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.ddis}</div>
              </div>
            )}
            <div style={{background:T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>PDF Backups</div>
              <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.pdfs}</div>
            </div>
            <div style={{background:a.summary.weeksCovered>0?T.bgSurface:"#fef2f2",padding:"10px 12px",borderRadius:8,border:`1px solid ${T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Weeks Covered</div>
              <div style={{fontSize:18,fontWeight:700,color:T.text}}>{a.summary.weeksCovered}</div>
            </div>
            <div style={{background:a.autoRenamed && a.autoRenamed.length>0 ? "#ecfdf5" : T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${a.autoRenamed && a.autoRenamed.length>0 ? T.green : T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Auto-Fixed</div>
              <div style={{fontSize:18,fontWeight:700,color:a.autoRenamed && a.autoRenamed.length>0 ? T.green : T.text}}>{a.autoRenamed ? a.autoRenamed.length : 0}</div>
            </div>
            <div style={{background:a.summary.duplicates>0?"#fffbeb":T.bgSurface,padding:"10px 12px",borderRadius:8,border:`1px solid ${a.summary.duplicates>0?T.yellow:T.borderLight}`}}>
              <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase",letterSpacing:"0.05em"}}>Duplicates</div>
              <div style={{fontSize:18,fontWeight:700,color:a.summary.duplicates>0?T.yellowText:T.text}}>{a.summary.duplicates}</div>
            </div>
          </div>

          {a.autoRenamed && a.autoRenamed.length > 0 && (
            <div style={{background:"#ecfdf5",border:`1px solid ${T.green}50`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.green,marginBottom:8}}>✅ Auto-Corrected Filenames ({a.autoRenamed.length}) — will be ingested with clean names</div>
              <div style={{fontSize:11,color:T.textMuted,marginBottom:8}}>These files had parseable date typos. The files on disk weren't modified — the corrections apply only to this ingest.</div>
              <div style={{maxHeight:200,overflowY:"auto"}}>
                {a.autoRenamed.map((r,i) => (
                  <div key={i} style={{padding:"6px 0",borderBottom:i<a.autoRenamed.length-1?`1px solid ${T.borderLight}`:"none",fontSize:11}}>
                    <div style={{fontFamily:"monospace",color:T.textMuted,textDecoration:"line-through"}}>{r.from}</div>
                    <div style={{fontFamily:"monospace",color:T.green,marginTop:2}}>→ {r.to}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {a.typos.length > 0 && (
            <div style={{background:"#fef2f2",border:`1px solid ${T.red}40`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.redText,marginBottom:8}}>🔴 Unfixable Filenames ({a.typos.length})</div>
              <div style={{fontSize:11,color:T.textMuted,marginBottom:8}}>Could not auto-correct these. Rename in Finder and re-upload.</div>
              <div style={{maxHeight:200,overflowY:"auto"}}>
                {a.typos.map((t,i) => (
                  <div key={i} style={{padding:"6px 0",borderBottom:i<a.typos.length-1?`1px solid ${T.borderLight}`:"none",fontSize:11}}>
                    <div style={{fontFamily:"monospace",color:T.redText}}>❌ {t.name}</div>
                    <div style={{color:T.textDim,fontSize:10,marginTop:2}}>Issue: {t.parseStatus.replace("typo-","")}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {a.duplicates.length > 0 && (
            <div style={{background:"#fffbeb",border:`1px solid ${T.yellow}60`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.yellowText,marginBottom:6}}>🟡 True Duplicates ({a.duplicates.length})</div>
              <div style={{fontSize:11,color:T.textMuted,marginBottom:8}}>Multiple files for the same week AND same service type. (Regular + Truckload for the same week is OK — those are different billing streams.)</div>
              <div style={{maxHeight:200,overflowY:"auto"}}>
                {a.duplicates.map((d,i) => (
                  <div key={i} style={{padding:"6px 0",borderBottom:i<a.duplicates.length-1?`1px solid ${T.borderLight}`:"none",fontSize:11}}>
                    <div style={{fontWeight:600,color:T.text}}>{weekLabels([d.weekEnding])[0]} — {d.kind}</div>
                    {d.files.map((f,j) => (
                      <div key={j} style={{fontFamily:"monospace",color:T.textMuted,fontSize:10,marginLeft:12,marginTop:2}}>• {f.name}</div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          )}

          {a.missingWeeks.length > 0 && (
            <div style={{background:"#fef2f2",border:`1px solid ${T.red}40`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.redText,marginBottom:8}}>🔴 Missing Weeks ({a.missingWeeks.length}) — no files found for these dates</div>
              <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
                {weekLabels(a.missingWeeks).map((w,i) => (
                  <div key={i} style={{padding:"4px 10px",background:"white",borderRadius:4,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText,fontWeight:600}}>{w}</div>
                ))}
              </div>
            </div>
          )}

          {a.missingAccessorials.length > 0 && (
            <div style={{background:"#fffbeb",border:`1px solid ${T.yellow}60`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.yellowText,marginBottom:8}}>🟡 Weeks Missing Accessorials ({a.missingAccessorials.length})</div>
              <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
                {weekLabels(a.missingAccessorials).map((w,i) => (
                  <div key={i} style={{padding:"4px 10px",background:"white",borderRadius:4,border:`1px solid ${T.yellow}60`,fontSize:11,color:T.yellowText,fontWeight:600}}>{w}</div>
                ))}
              </div>
            </div>
          )}

          {a.unparsed.length > 0 && (
            <div style={{background:"#f9fafb",border:`1px solid ${T.border}`,borderRadius:8,padding:12,marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:T.textMuted,marginBottom:8}}>⚪ Unrecognized ({a.unparsed.length}) — will be skipped</div>
              <div style={{maxHeight:140,overflowY:"auto"}}>
                {a.unparsed.map((u,i) => (
                  <div key={i} style={{fontFamily:"monospace",fontSize:10,color:T.textMuted,padding:"3px 0"}}>{u.name}</div>
                ))}
              </div>
            </div>
          )}

          {/* Action buttons */}
          <div style={{display:"flex",gap:10,alignItems:"center",marginTop:16,paddingTop:12,borderTop:`1px solid ${T.borderLight}`}}>
            <button onClick={cancelPending} style={{padding:"10px 20px",borderRadius:8,background:"white",border:`1px solid ${T.border}`,color:T.text,fontSize:13,fontWeight:600,cursor:"pointer"}}>Cancel</button>
            <button onClick={proceedWithPending} style={{padding:"10px 20px",borderRadius:8,background:canProceed?T.brand:T.yellow,border:"none",color:"white",fontSize:13,fontWeight:600,cursor:"pointer"}}>
              {canProceed ? "Proceed with Ingest →" : `Ingest Anyway (${a.summary.typos + a.summary.duplicates} issues) →`}
            </button>
            {!canProceed && (
              <div style={{fontSize:11,color:T.textMuted,fontStyle:"italic"}}>
                Recommended: fix typos and dedupe, then re-upload
              </div>
            )}
          </div>
        </div>
      );
    })()}

    <div
      onDragEnter={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
      onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
      onDragLeave={(e) => {
        e.preventDefault(); e.stopPropagation();
        // Only clear dragOver if we're truly leaving the zone (not just crossing a child element)
        if (e.currentTarget.contains(e.relatedTarget)) return;
        setDragOver(false);
      }}
      onDrop={(e) => {
        e.preventDefault(); e.stopPropagation();
        setDragOver(false);
        const dropped = [];
        if (e.dataTransfer.items) {
          for (let i = 0; i < e.dataTransfer.items.length; i++) {
            const item = e.dataTransfer.items[i];
            if (item.kind === "file") {
              const f = item.getAsFile();
              if (f) dropped.push(f);
            }
          }
        } else if (e.dataTransfer.files) {
          for (let i = 0; i < e.dataTransfer.files.length; i++) dropped.push(e.dataTransfer.files[i]);
        }
        if (dropped.length > 0 && !uploading) handleFiles(dropped);
      }}
      onClick={() => !uploading && fileRef.current?.click()}
      style={{
        ...cardStyle,
        background: dragOver ? "#dbeafe" : T.brandPale,
        borderColor: dragOver ? T.brand : T.brand,
        borderWidth: 2,
        borderStyle: dragOver ? "solid" : "dashed",
        cursor: uploading ? "wait" : "pointer",
        transition: "background 0.15s, border-color 0.15s",
      }}
    >
      <div style={{fontSize:13,fontWeight:700,marginBottom:8,color:T.brand,display:"flex",alignItems:"center",gap:8}}>
        <span style={{fontSize:24}}>{dragOver ? "⬇️" : "📤"}</span>
        <span>{dragOver ? "Drop files to upload" : "Drag & Drop or Click to Upload"}</span>
      </div>
      <div style={{fontSize:12,color:T.text,lineHeight:1.6}}>
        Drop any combination of files here — individual <code>.xlsx</code>/<code>.csv</code>/<code>.pdf</code> or a <code>.zip</code> archive (e.g., a year's worth of Uline weeklies). MarginIQ unpacks zips, audits for typos/duplicates/gaps, shows you a review before ingesting, and auto-detects each type:
        <ul style={{marginTop:8,marginLeft:20,fontSize:12}}>
          <li><strong>Uline</strong> (master / originals / accessorials / DDIS) → weekly revenue (source of truth) + reconciliation</li>
          <li><strong>NuVizz</strong> (driver stops export) → weekly driver rollups + 1099 contractor pay base (40% per stop). <em>Not revenue.</em></li>
          <li><strong>Time Clock</strong> (SENTINEL / B600 punches) → weekly hours by employee</li>
          <li><strong>Payroll</strong> (CyberPay register) → weekly gross, hours, OT by employee</li>
          <li><strong>QuickBooks</strong> (P&L, Trial Balance, GL exports) → financial history</li>
        </ul>
        <div style={{fontSize:11,color:T.textMuted,marginTop:8}}>
          Files auto-detected by filename and column headers. On mobile, click here or the Upload Files button at the top.
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

    {fileLog && fileLog.length > 0 && (() => {
      // Parse the date range out of a filename. Handles Uline's YYYYMMDD-YYYYMMDD
      // and MMDDYYYY-MMDDYYYY variants, with or without spaces around the dash,
      // and an 'accessiorial' typo. Returns {startISO, endISO, label} or null.
      const parseCoverage = (filename) => {
        if (!filename) return null;
        const m = filename.match(/(\d{8})\s*-\s*(\d{8})/);
        if (!m) return null;
        const parse8 = (s) => {
          // Try YYYYMMDD
          for (const y of ["2023","2024","2025","2026","2027"]) {
            if (s.startsWith(y)) {
              const mo = parseInt(s.slice(4,6),10), d = parseInt(s.slice(6,8),10);
              if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31) return new Date(parseInt(y,10), mo-1, d);
            }
          }
          // Try MMDDYYYY
          const mo = parseInt(s.slice(0,2),10), d = parseInt(s.slice(2,4),10), y = parseInt(s.slice(4,8),10);
          if (y >= 2023 && y <= 2027 && mo >= 1 && mo <= 12 && d >= 1 && d <= 31) return new Date(y, mo-1, d);
          return null;
        };
        const start = parse8(m[1]);
        let end = parse8(m[2]);
        if (!start || !end) return null;
        // Year-off-by-one fix: if end < start, infer same-year or next-year wrap
        if (end < start) {
          const fixed = new Date(end); fixed.setFullYear(start.getFullYear());
          end = fixed >= start ? fixed : new Date(start.getFullYear()+1, end.getMonth(), end.getDate());
        }
        const fmt = (d) => d.toLocaleDateString("en-US", { month:"short", day:"numeric" });
        const yrs = start.getFullYear() === end.getFullYear();
        const label = yrs
          ? `${fmt(start)} – ${fmt(end)}, ${end.getFullYear()}`
          : `${fmt(start)} ${start.getFullYear()} – ${fmt(end)} ${end.getFullYear()}`;
        return { startISO: start.toISOString().slice(0,10), endISO: end.toISOString().slice(0,10), label };
      };

      // Shared sort logic — historySort is computed at component top level
      // (hooks can't run inside this conditional IIFE). parseCoverage here
      // builds the display label; the sort accessor (defined at the top) does
      // its own YYYYMMDD parse to avoid a closure capture.
      const { sorted: sortedFileLog, sortKey, sortDir, toggleSort } = historySort;

      return (
      <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:T.radius,padding:"16px 20px",marginTop:16,boxShadow:T.shadow}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
          <div style={{fontSize:13,fontWeight:700,color:T.text}}>📋 Upload History</div>
          <div style={{fontSize:11,color:T.textDim}}>Showing {Math.min(fileLog.length,100)} most recent {fileLog.length > 100 ? `of ${fmtNum(fileLog.length)}` : ""} · Tap column to sort</div>
        </div>
        <div style={{overflowX:"auto",maxHeight:500,borderRadius:T.radiusSm,border:`1px solid ${T.borderLight}`}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead>
              <tr>
                <SortableTh label="Filename" col="filename" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortableTh label="Covers" col="covers" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortableTh label="Type" col="kind" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortableTh label="Source" col="group" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortableTh label="Rows" col="row_count" align="right" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortableTh label="When" col="uploaded_at" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
              </tr>
            </thead>
            <tbody>
              {sortedFileLog.slice(0,100).map((f,i) => {
                const cov = parseCoverage(f.filename);
                return (
                <tr key={f.file_id || i} style={{transition:"background 0.15s"}} onMouseEnter={e => e.currentTarget.style.background=T.bgSurface} onMouseLeave={e => e.currentTarget.style.background="transparent"}>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontSize:11,color:T.text,maxWidth:340,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={f.filename}>{f.filename}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:11,color:cov ? T.text : T.textDim,whiteSpace:"nowrap",fontWeight: cov ? 600 : 400}} title={cov ? `${cov.startISO} → ${cov.endISO}` : "Could not parse date range from filename"}>{cov ? cov.label : "—"}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{f.kind ? <Badge text={f.kind} color={T.brand} bg={T.brandPale} /> : <span style={{color:T.textDim,fontSize:11}}>—</span>}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:11,color:T.textMuted}}>{f.group || "—"}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:12,fontWeight:600,color:T.text,textAlign:"right"}}>{fmtNum(f.row_count || 0)}</td>
                  <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontSize:11,color:T.textMuted,whiteSpace:"nowrap"}}>{f.uploaded_at ? new Date(f.uploaded_at).toLocaleString("en-US",{month:"short",day:"numeric",year:"numeric",hour:"numeric",minute:"2-digit"}) : "—"}</td>
                </tr>
              );})}
            </tbody>
          </table>
        </div>
      </div>
      );
    })()}
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
  const dispSort = useSortable(disputes, "submitted_date", "desc");
  const custSort = useSortable(topCustomers, "amount", "desc");
  const [detailItem, setDetailItem] = useState(null);
  const [editingContact, setEditingContact] = useState(null);

  // Filters for items view
  const [filterCategory, setFilterCategory] = useState("all");
  const [filterAge, setFilterAge] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const [filterCustomer, setFilterCustomer] = useState("");
  const [filterMinVar, setFilterMinVar] = useState(10);
  const [sortBy, setSortBy] = useState("variance");
  // v2.40.14: service_type filter — "all" | "delivery" | "truckload" | "accessorial"
  const [filterService, setFilterService] = useState("all");
  // v2.40.16: date-range filter — scopes the WHOLE Audit dashboard (tiles + items)
  // rangePreset: "all" | "this_month" | "30d" | "90d" | "ytd" | "custom"
  const [rangePreset, setRangePreset] = useState("all");
  const [customFrom, setCustomFrom] = useState(""); // YYYY-MM-DD
  const [customTo, setCustomTo] = useState("");     // YYYY-MM-DD
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

  // Rebuild audit_items from existing Firestore data WITHOUT re-ingesting
  // files. Joins unpaid_stops (which already has PRO-level billed amounts,
  // produced at Uline ingest time) with ddis_files / DDIS payment records
  // (which have paid amounts per PRO). Writes a fresh audit_items doc for
  // every PRO with a positive variance after applying the match.
  //
  // Why this exists: audit_items are produced only during Uline ingest, and
  // only when DDIS payment data was already present at that ingest moment.
  // After a purge + re-ingest where the DDIS file was imported AFTER the
  // Uline files (or not in the same session), audit_items end up empty
  // even though recon_weekly + unpaid_stops are fully populated. This
  // rebuild reads what's already in Firestore and catches up.
  const [rebuilding, setRebuilding] = useState(false);
  const [rebuildStatus, setRebuildStatus] = useState("");
  const [rebuildResult, setRebuildResult] = useState(null);

  const rebuildAuditItems = async () => {
    if (rebuilding) return;
    if (!hasFirebase) { alert("Firebase not connected"); return; }
    setRebuilding(true);
    setRebuildResult(null);
    setRebuildStatus("Queuing rebuild…");
    try {
      // v2.40.10: The rebuild moved server-side. Old client path was reading
      // only 20K of 122K ddis_payments rows and 2K of 3K unpaid_stops, so
      // every PRO whose payment row was beyond row 20K got variance=billed
      // (looked fully unpaid even when paid). That's fixed by the background
      // function which pages through everything with no caps.
      const resp = await fetch("/.netlify/functions/marginiq-audit-rebuild", { method: "POST" });
      if (resp.status !== 202 && !resp.ok) {
        const body = await resp.text();
        throw new Error(`Dispatch failed: HTTP ${resp.status} ${body.slice(0, 200)}`);
      }
      // Poll status every 5s for up to 10 minutes.
      const deadline = Date.now() + 10 * 60 * 1000;
      let lastPhase = "";
      while (Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 5000));
        try {
          const sresp = await fetch("/.netlify/functions/marginiq-audit-rebuild?action=status");
          const sbody = await sresp.json();
          const st = sbody?.status;
          if (!st) { setRebuildStatus("Starting…"); continue; }
          setRebuildStatus(st.progress_text || `Working (${st.phase || "unknown phase"})…`);
          if (st.phase && st.phase !== lastPhase) {
            console.log(`audit-rebuild phase: ${st.phase}`);
            lastPhase = st.phase;
          }
          if (st.state === "complete") {
            setRebuildResult({
              ok: true,
              generated: st.items_generated || 0,
              saved: st.items_written || 0,
              withPayments: !!st.with_payments,
              ddisFiles: null,
              totalVariance: st.total_variance || 0,
              // v2.40.12 sideline fields
              sidelinedRecentStops: st.sidelined_recent_stops || 0,
              sidelinedRecentBilled: st.sidelined_recent_billed || 0,
              sidelinedRecentWeeks: st.sidelined_recent_weeks || [],
              sidelinedAwaitingStops: st.sidelined_awaiting_stops || 0,
              sidelinedAwaitingBilled: st.sidelined_awaiting_billed || 0,
              sidelinedAwaitingWeeks: st.sidelined_awaiting_weeks || [],
              sidelinedNoWeekStops: st.sidelined_noweek_stops || 0,
              coveredWeeksCount: st.covered_weeks_count || 0,
              recentCutoffISO: st.recent_cutoff_iso || null,
            });
            await loadData();
            setRebuilding(false);
            setRebuildStatus("");
            return;
          }
          if (st.state === "failed") {
            setRebuildResult({ error: st.error || "Rebuild failed" });
            setRebuilding(false);
            setRebuildStatus("");
            return;
          }
        } catch (pollErr) {
          // Transient poll error — keep trying.
          console.warn("status poll failed:", pollErr);
        }
      }
      setRebuildResult({ error: "Rebuild timed out after 10 minutes. Check Netlify logs — it may still be running in the background." });
    } catch(e) {
      console.error("rebuildAuditItems failed:", e);
      setRebuildResult({ error: e.message });
    }
    setRebuilding(false);
    setRebuildStatus("");
  };

  // v2.40.18: Purge audit queue — nukes ALL audit_items. Use sparingly. The
  // rebuild now preserves dispute history across runs, so in normal operation
  // you shouldn't need this. It exists to clean up pre-v2.40.18 ghost items
  // accumulated from old rebuilds that never deleted stale docs.
  const [purging, setPurging] = useState(false);
  const [purgeResult, setPurgeResult] = useState(null);
  const purgeAuditItems = async () => {
    if (purging || rebuilding) return;
    if (!hasFirebase || !window.db) { alert("Firebase not connected"); return; }
    const count = itemsWithFreshAge.length;
    const confirmed = window.confirm(
      `Purge will DELETE all ${count.toLocaleString()} audit items.\n\n` +
      `Dispute history (won/lost/partial/written_off/recovered_paid) will also be lost — if you want to preserve those, cancel and just run Rebuild instead.\n\n` +
      `Type OK in the next prompt to confirm.`
    );
    if (!confirmed) return;
    const typed = window.prompt(`Type "PURGE" (all caps) to confirm deletion of ${count.toLocaleString()} audit items:`);
    if (typed !== "PURGE") { alert("Cancelled — confirmation phrase didn't match."); return; }
    setPurging(true);
    setPurgeResult(null);
    const t0 = Date.now();
    try {
      // v2.40.24: Purge runs client-side, not via a Netlify function.
      // Firestore security rules allow authenticated client deletes (proved
      // by the existing working delete at marginiq_config/{docId} in this
      // same file) but block deletes via REST API key. The server-side purge
      // kept returning 403 PERMISSION_DENIED — confirmed via direct probe.
      // Client SDK uses the user's Firebase auth context, so it just works.
      //
      // Pagination: Firestore client .get() on a collection returns at most
      // 10K docs by default and slows down on large reads. We page through
      // with .orderBy(...).startAfter(lastDoc).limit(500) so arbitrarily
      // large audit_items collections still work.
      let totalDeleted = 0;
      let lastDoc = null;
      const BATCH_SIZE = 500; // Firestore batch() cap
      while (true) {
        let q = window.db.collection("audit_items").orderBy(firebase.firestore.FieldPath.documentId()).limit(BATCH_SIZE);
        if (lastDoc) q = q.startAfter(lastDoc);
        const snap = await q.get();
        if (snap.empty) break;
        const batch = window.db.batch();
        for (const doc of snap.docs) batch.delete(doc.ref);
        await batch.commit();
        totalDeleted += snap.docs.length;
        lastDoc = snap.docs[snap.docs.length - 1];
        setPurgeResult({ ok: null, in_progress: true, deleted: totalDeleted });
        // Safety bound — if a collection somehow had 50K+ docs we stop.
        if (totalDeleted >= 100000) { console.warn("purge: hit 100K safety bound"); break; }
        if (snap.docs.length < BATCH_SIZE) break; // last page
      }
      const elapsed_s = Math.round((Date.now() - t0) / 1000);
      setPurgeResult({ ok: true, deleted: totalDeleted, failed: 0, elapsed_s });
      // Reload audit data so the UI reflects the empty state.
      await loadData();
    } catch(e) {
      console.error("purgeAuditItems failed:", e);
      setPurgeResult({ error: e.message || String(e) });
    }
    setPurging(false);
  };

  // Recompute age_days on load (they were stored at ingest time and may be stale)
  const itemsWithFreshAge = useMemo(() => {
    const today = new Date().toISOString().slice(0,10);
    return auditItems.map(i => {
      const age = daysBetween(i.pu_date, today);
      return { ...i, age_days: age, age_bucket: ageBucket(age) };
    });
  }, [auditItems]);

  // v2.40.16: resolve the active date range → { from, to } ISO strings, or null = unbounded.
  // Used by rangedItems below to scope the ENTIRE audit dashboard (KPIs, aging, by-customer,
  // by-category, filtered list). Chad confirmed whole-dashboard scoping.
  const rangeBounds = useMemo(() => {
    const pad = (n) => String(n).padStart(2, "0");
    const iso = (d) => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
    const today = new Date();
    if (rangePreset === "all") return null;
    if (rangePreset === "this_month") {
      const from = iso(new Date(today.getFullYear(), today.getMonth(), 1));
      return { from, to: iso(today) };
    }
    if (rangePreset === "30d") {
      const d = new Date(today); d.setDate(d.getDate() - 30);
      return { from: iso(d), to: iso(today) };
    }
    if (rangePreset === "90d") {
      const d = new Date(today); d.setDate(d.getDate() - 90);
      return { from: iso(d), to: iso(today) };
    }
    if (rangePreset === "ytd") {
      return { from: `${today.getFullYear()}-01-01`, to: iso(today) };
    }
    if (rangePreset === "custom") {
      if (!customFrom && !customTo) return null;
      return { from: customFrom || "0000-01-01", to: customTo || "9999-12-31" };
    }
    return null;
  }, [rangePreset, customFrom, customTo]);

  // v2.40.16: items scoped to the selected date range. Falls back to pu_date when week_ending
  // is missing (older audit rows written before v2.40.9 week-derivation logic).
  const rangedItems = useMemo(() => {
    if (!rangeBounds) return itemsWithFreshAge;
    const { from, to } = rangeBounds;
    return itemsWithFreshAge.filter(i => {
      const d = i.week_ending || i.pu_date;
      if (!d) return false;
      return d >= from && d <= to;
    });
  }, [itemsWithFreshAge, rangeBounds]);

  // Dashboard stats
  const stats = useMemo(() => {
    // v2.40.18: exclude recovered_paid (Uline paid after we saw the variance)
    // from Outstanding. v2.40.20 note: TK (ULI-) shipments are now included in
    // the main queue and matched via numeric-core join in the rebuild.
    const active = rangedItems.filter(i =>
      i.dispute_status !== "written_off" &&
      i.dispute_status !== "won" &&
      i.dispute_status !== "recovered_paid"
    );
    const outstanding = active.reduce((s,i) => s + (i.variance||0), 0);
    const outstandingCount = active.length;
    const thisMonth = new Date().toISOString().slice(0,7);
    const wonThisMonth = disputes.filter(d => d.outcome === "won" && (d.response_date||"").startsWith(thisMonth));
    // v2.40.18: passively-recovered items (Uline paid without a formal dispute)
    // also count toward "Recovered this month" — their recovered_at stamp is
    // written by the background rebuild when variance drops to ≤ $1.
    const recoveredPaidThisMonth = rangedItems.filter(i =>
      i.dispute_status === "recovered_paid" &&
      (i.recovered_at||"").startsWith(thisMonth)
    );
    const recoveredFromDisputes = wonThisMonth.reduce((s,d) => s + (d.amount_recovered||0), 0);
    const recoveredFromPassive = recoveredPaidThisMonth.reduce((s,i) => s + (i.recovered_amount||0), 0);
    const recovered = recoveredFromDisputes + recoveredFromPassive;
    const recoveredCount = wonThisMonth.length + recoveredPaidThisMonth.length;
    const allDisputed = disputes.filter(d => d.submitted_date);
    const won = allDisputed.filter(d => d.outcome === "won" || d.outcome === "partial");
    const winRate = allDisputed.length > 0 ? (won.length / allDisputed.length * 100) : null;
    // Avg days to recover
    const turnarounds = allDisputed
      .filter(d => d.submitted_date && d.response_date && (d.outcome === "won" || d.outcome === "partial"))
      .map(d => daysBetween(d.submitted_date.slice(0,10), d.response_date.slice(0,10)) || 0);
    const avgTurnaround = turnarounds.length > 0 ? Math.round(turnarounds.reduce((s,t)=>s+t,0)/turnarounds.length) : null;
    return { outstanding, outstandingCount, recovered, wonThisMonthCount: recoveredCount, winRate, avgTurnaround };
  }, [rangedItems, disputes]);

  // Aging bucket breakdown (active only)
  const agingBuckets = useMemo(() => {
    const buckets = {};
    for (const b of AGE_BUCKETS) buckets[b] = { bucket: b, count: 0, amount: 0 };
    for (const i of rangedItems) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won" || i.dispute_status === "recovered_paid") continue;
      const b = i.age_bucket || "unknown";
      if (!buckets[b]) buckets[b] = { bucket: b, count: 0, amount: 0 };
      buckets[b].count++;
      buckets[b].amount += i.variance || 0;
    }
    return AGE_BUCKETS.map(b => buckets[b]).filter(b => b.count > 0);
  }, [rangedItems]);

  // Top customers by outstanding
  const topCustomers = useMemo(() => {
    const byC = {};
    for (const i of rangedItems) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won" || i.dispute_status === "recovered_paid") continue;
      const c = i.customer || "Unknown";
      if (!byC[c]) byC[c] = { customer: c, customer_key: i.customer_key, count: 0, amount: 0, oldest_age: 0 };
      byC[c].count++;
      byC[c].amount += i.variance || 0;
      if ((i.age_days||0) > byC[c].oldest_age) byC[c].oldest_age = i.age_days || 0;
    }
    return Object.values(byC).sort((a,b) => b.amount - a.amount).slice(0, 15);
  }, [rangedItems]);

  // Category breakdown
  const byCategory = useMemo(() => {
    const out = {};
    for (const c of CATEGORIES) out[c] = { category: c, count: 0, amount: 0 };
    for (const i of rangedItems) {
      if (i.dispute_status === "written_off" || i.dispute_status === "won" || i.dispute_status === "recovered_paid") continue;
      const c = i.category || "short_paid";
      if (!out[c]) out[c] = { category: c, count: 0, amount: 0 };
      out[c].count++;
      out[c].amount += i.variance || 0;
    }
    return CATEGORIES.map(c => out[c]).filter(x => x.count > 0);
  }, [rangedItems]);

  // Filtered item list
  const filteredItems = useMemo(() => {
    let arr = rangedItems;
    if (filterCategory !== "all") arr = arr.filter(i => i.category === filterCategory);
    if (filterAge !== "all") arr = arr.filter(i => i.age_bucket === filterAge);
    if (filterStatus !== "all") arr = arr.filter(i => (i.dispute_status || "new") === filterStatus);
    // v2.40.14: service_type filter (delivery / truckload / accessorial)
    if (filterService !== "all") arr = arr.filter(i => (i.service_type || "delivery") === filterService);
    if (filterCustomer.trim()) {
      const q = filterCustomer.toLowerCase();
      arr = arr.filter(i => (i.customer||"").toLowerCase().includes(q) || (i.pro||"").toLowerCase().includes(q));
    }
    arr = arr.filter(i => (i.variance||0) >= filterMinVar);
    arr = arr.slice().sort((a,b) => {
      if (sortBy === "variance") return (b.variance||0) - (a.variance||0);
      if (sortBy === "-variance") return (a.variance||0) - (b.variance||0);
      if (sortBy === "age") return (b.age_days||0) - (a.age_days||0);
      if (sortBy === "-age") return (a.age_days||0) - (b.age_days||0);
      if (sortBy === "date") return (b.pu_date||"").localeCompare(a.pu_date||"");
      if (sortBy === "-date") return (a.pu_date||"").localeCompare(b.pu_date||"");
      if (sortBy === "customer") return (a.customer||"").localeCompare(b.customer||"");
      if (sortBy === "-customer") return (b.customer||"").localeCompare(a.customer||"");
      if (sortBy === "billed") return (b.billed||0) - (a.billed||0);
      if (sortBy === "-billed") return (a.billed||0) - (b.billed||0);
      if (sortBy === "paid") return (b.paid||0) - (a.paid||0);
      if (sortBy === "-paid") return (a.paid||0) - (b.paid||0);
      if (sortBy === "pro") return (a.pro||"").localeCompare(b.pro||"", undefined, {numeric:true});
      if (sortBy === "-pro") return (b.pro||"").localeCompare(a.pro||"", undefined, {numeric:true});
      if (sortBy === "status") return (a.dispute_status||"").localeCompare(b.dispute_status||"");
      if (sortBy === "-status") return (b.dispute_status||"").localeCompare(a.dispute_status||"");
      if (sortBy === "category") return (a.category||"").localeCompare(b.category||"");
      if (sortBy === "-category") return (b.category||"").localeCompare(a.category||"");
      return 0;
    });
    return arr;
  }, [rangedItems, filterCategory, filterAge, filterStatus, filterService, filterCustomer, filterMinVar, sortBy]);

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

  // v2.40.18: Uline export — dumps selected (or filtered) audit items to CSV/XLSX/PDF
  // for sending to Uline AP. Distinct from generateDisputePdf below:
  //   - No dispute record created (this is a discovery-stage export, not a formal submission)
  //   - No item statuses changed
  //   - Supports three formats (user picks per export)
  //   - Scope: if anything is selected, export selected; else export current filter result
  const [exportOpen, setExportOpen] = useState(false);
  const [exportBusy, setExportBusy] = useState(false);
  const [exportStatus, setExportStatus] = useState("");

  // Shared column definitions for CSV + XLSX. PDF uses its own layout server-side.
  const EXPORT_COLUMNS = [
    { key: "pro",           label: "PRO" },
    { key: "customer",      label: "End Customer" },
    { key: "pu_date",       label: "PU Date" },
    { key: "week_ending",   label: "Week Ending" },
    { key: "city",          label: "City" },
    { key: "state",         label: "ST" },
    { key: "zip",           label: "ZIP" },
    { key: "service_type",  label: "Service" },
    { key: "code",          label: "Accessorial Code" },
    { key: "weight",        label: "Weight (lb)" },
    { key: "order",         label: "Order #" },
    { key: "billed",        label: "Billed ($)" },
    { key: "paid",          label: "Paid ($)" },
    { key: "variance",      label: "Variance ($)" },
    { key: "variance_pct",  label: "Variance %" },
    { key: "age_days",      label: "Age (days)" },
    { key: "category",      label: "Category" },
    { key: "dispute_status",label: "Status" },
  ];

  const getExportScope = () => {
    if (selectedItems.length > 0) return { rows: selectedItems, scopeLabel: `${selectedItems.length} selected` };
    return { rows: filteredItems, scopeLabel: `${filteredItems.length} filtered` };
  };

  const buildExportRows = (rows) => rows.map(r => {
    const out = {};
    for (const col of EXPORT_COLUMNS) {
      let v = r[col.key];
      if (v === undefined || v === null) v = "";
      // Round money + pct fields for readability
      if (["billed","paid","variance"].includes(col.key) && typeof v === "number") v = Math.round(v * 100) / 100;
      if (col.key === "variance_pct" && typeof v === "number") v = Math.round(v * 10) / 10;
      out[col.label] = v;
    }
    return out;
  });

  const exportFilename = (ext) => {
    const today = new Date().toISOString().slice(0,10);
    return `davis-uline-audit-${today}.${ext}`;
  };

  const exportCSV = () => {
    const { rows, scopeLabel } = getExportScope();
    if (rows.length === 0) { setExportStatus("✗ Nothing to export."); return; }
    setExportBusy(true);
    try {
      const headers = EXPORT_COLUMNS.map(c => c.label);
      const escape = (v) => {
        if (v === null || v === undefined) return "";
        const s = String(v);
        // Standard CSV escaping: wrap in quotes if it contains ", comma, newline, or leading space.
        if (/[",\n\r]/.test(s) || s !== s.trim()) return `"${s.replace(/"/g, '""')}"`;
        return s;
      };
      const lines = [headers.map(escape).join(",")];
      for (const r of buildExportRows(rows)) {
        lines.push(headers.map(h => escape(r[h])).join(","));
      }
      const csv = "\uFEFF" + lines.join("\r\n"); // BOM for Excel compatibility
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = exportFilename("csv");
      link.click();
      URL.revokeObjectURL(link.href);
      setExportStatus(`✓ Exported ${rows.length} items to CSV (${scopeLabel})`);
      setExportOpen(false);
    } catch (e) {
      console.error("exportCSV failed:", e);
      setExportStatus(`✗ CSV export failed: ${e.message}`);
    }
    setExportBusy(false);
  };

  const exportXLSX = () => {
    const { rows, scopeLabel } = getExportScope();
    if (rows.length === 0) { setExportStatus("✗ Nothing to export."); return; }
    if (typeof window.XLSX === "undefined") {
      setExportStatus("✗ SheetJS not loaded — use CSV instead.");
      return;
    }
    setExportBusy(true);
    try {
      const data = buildExportRows(rows);
      const totals = {};
      for (const c of EXPORT_COLUMNS) totals[c.label] = "";
      totals[EXPORT_COLUMNS[0].label] = "TOTAL";
      totals["Billed ($)"] = Math.round(rows.reduce((s,r)=>s+(r.billed||0),0) * 100) / 100;
      totals["Paid ($)"]   = Math.round(rows.reduce((s,r)=>s+(r.paid||0),0) * 100) / 100;
      totals["Variance ($)"] = Math.round(rows.reduce((s,r)=>s+(r.variance||0),0) * 100) / 100;
      data.push(totals);
      const ws = window.XLSX.utils.json_to_sheet(data);
      // Column widths — pick something reasonable for the main columns
      ws["!cols"] = [
        { wch: 12 }, // PRO
        { wch: 24 }, // Customer
        { wch: 11 }, { wch: 11 }, // dates
        { wch: 16 }, { wch: 4 }, { wch: 7 }, // city/st/zip
        { wch: 10 }, { wch: 8 }, { wch: 8 }, { wch: 12 }, // service/code/weight/order
        { wch: 11 }, { wch: 10 }, { wch: 12 }, { wch: 10 }, // billed/paid/variance/pct
        { wch: 9 }, { wch: 16 }, { wch: 14 }, // age/category/status
      ];
      const wb = window.XLSX.utils.book_new();
      window.XLSX.utils.book_append_sheet(wb, ws, "Uline Audit");
      window.XLSX.writeFile(wb, exportFilename("xlsx"));
      setExportStatus(`✓ Exported ${rows.length} items to XLSX (${scopeLabel})`);
      setExportOpen(false);
    } catch (e) {
      console.error("exportXLSX failed:", e);
      setExportStatus(`✗ XLSX export failed: ${e.message}`);
    }
    setExportBusy(false);
  };

  const exportPDF = async () => {
    const { rows, scopeLabel } = getExportScope();
    if (rows.length === 0) { setExportStatus("✗ Nothing to export."); return; }
    setExportBusy(true);
    setExportStatus("Generating PDF…");
    try {
      // Reuse the dispute-pdf endpoint but label recipient as Uline. This is
      // intentionally NOT tied to a dispute record — it's a discovery export.
      const resp = await fetch("/.netlify/functions/marginiq-dispute-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: rows,
          customer: "Uline",
          ap_contact: { billing_email: "APFreight@uline.com", ap_contact_name: "Uline AP", ap_contact_phone: "" },
        }),
      });
      const data = await resp.json();
      if (data.error) throw new Error(data.error);
      const link = document.createElement("a");
      link.href = "data:application/pdf;base64," + data.data;
      link.download = data.filename || exportFilename("pdf");
      link.click();
      setExportStatus(`✓ Exported ${rows.length} items to PDF (${scopeLabel}, $${(data.total_claim||0).toFixed(2)} claim)`);
      setExportOpen(false);
    } catch (e) {
      console.error("exportPDF failed:", e);
      setExportStatus(`✗ PDF export failed: ${e.message}`);
    }
    setExportBusy(false);
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
      <div style={{fontSize:10,color:T.textDim}}>{rangedItems.length} items tracked</div>
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

    {/* v2.40.16: Date range filter — scopes the WHOLE audit dashboard (tiles + items). */}
    {/* v2.40.31: Strengthened visibility. Desktop screenshot showed it blending into     */}
    {/* page background to the point users thought it was missing. Brand-pale bg +       */}
    {/* left border accent + larger label make it unmissable.                             */}
    <div style={{
      background: T.brandPale,
      border: `1px solid ${T.brand}33`,
      borderLeft: `4px solid ${T.brand}`,
      borderRadius: 8,
      padding: "12px 14px",
      marginBottom: 12,
    }}>
      <div style={{display:"flex",gap:6,flexWrap:"wrap",alignItems:"center"}}>
        <div style={{fontSize:13,fontWeight:700,color:T.brand,marginRight:8}}>📅 Date Range</div>
        {[
          ["all","All time"],
          ["this_month","This month"],
          ["30d","Last 30d"],
          ["90d","Last 90d"],
          ["ytd","YTD"],
          ["custom","Custom"],
        ].map(([id,l]) =>
          <TabButton key={id} active={rangePreset===id} label={l} onClick={()=>setRangePreset(id)} />
        )}
        {rangePreset === "custom" && (
          <>
            <input type="date" value={customFrom} onChange={e=>setCustomFrom(e.target.value)}
              style={{...inputStyle,fontSize:12,width:"auto"}} />
            <span style={{fontSize:11,color:T.textMuted}}>→</span>
            <input type="date" value={customTo} onChange={e=>setCustomTo(e.target.value)}
              style={{...inputStyle,fontSize:12,width:"auto"}} />
          </>
        )}
      </div>
      {rangeBounds && (
        <div style={{fontSize:11,color:T.brand,marginTop:8,fontWeight:600}}>
          Scoped to <b>{rangeBounds.from}</b> → <b>{rangeBounds.to}</b> • <span style={{color:T.text}}>{rangedItems.length}</span> of {itemsWithFreshAge.length} items
        </div>
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

        {rangedItems.length === 0 ? (
          <div style={cardStyle}>
            <EmptyState icon="🧾" title="No Audit Data Yet" sub="Audit tracks billed-vs-paid discrepancies. The button below reads existing Uline + DDIS data from Firestore and builds the audit queue — no re-ingest needed." />
            <div style={{marginTop:14,padding:"12px 14px",background:T.brandPale,borderRadius:8,border:`1px solid ${T.brand}30`}}>
              <div style={{fontSize:12,fontWeight:700,color:T.brand,marginBottom:4}}>🔧 Build audit queue from existing data</div>
              <div style={{fontSize:11,color:T.text,lineHeight:1.5,marginBottom:10}}>
                Joins <code>unpaid_stops</code> (PRO-level billed amounts from Uline ingest) with <code>ddis_files</code> (Uline payment files). Every PRO with a variance ≥ $1 gets an audit item you can chase, categorize, and build dispute packets against.
              </div>
              <button
                onClick={rebuildAuditItems}
                disabled={rebuilding}
                style={{
                  padding: "9px 18px",
                  borderRadius: 8,
                  border: "none",
                  background: rebuilding ? T.bgSurface : `linear-gradient(135deg,${T.brand},${T.brandLight})`,
                  color: rebuilding ? T.text : "#fff",
                  fontSize: 12,
                  fontWeight: 700,
                  cursor: rebuilding ? "wait" : "pointer",
                }}
              >
                {rebuilding ? "⏳ Building…" : "🚀 Build Audit Queue Now"}
              </button>
              {rebuildStatus && <div style={{marginTop:8,fontSize:11,color:T.textMuted}}>{rebuildStatus}</div>}
              {rebuildResult?.error && (
                <div style={{marginTop:10,padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText}}>
                  ✗ {rebuildResult.error}
                </div>
              )}
              {rebuildResult?.ok && (
                <div style={{marginTop:10,padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText}}>
                  ✓ Generated {rebuildResult.generated} audit items ({fmtK(rebuildResult.totalVariance)} total variance)
                  {!rebuildResult.withPayments && <div style={{marginTop:4,fontSize:10}}>Note: no PRO-level payment matching possible — only DDIS file-level totals are stored. Re-ingesting DDIS will improve match fidelity.</div>}
                </div>
              )}
            </div>
          </div>
        ) : (
          <>
            {/* v2.40.8: Rebuild Audit Queue banner — visible even when items
                already exist. Previously this was only shown on an empty
                queue, which meant users who'd imported more DDIS data after
                the first build had no obvious way to refresh the variance
                numbers. Most common reason to click this: you just imported
                more DDIS files and want audit_items re-computed. */}
            <div style={{...cardStyle, marginBottom:12, background:T.brandPale, borderColor:T.brand}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:10,flexWrap:"wrap"}}>
                <div style={{flex:"1 1 260px"}}>
                  <div style={{fontSize:12,fontWeight:700,color:T.brand,marginBottom:2}}>🔄 Rebuild audit queue</div>
                  <div style={{fontSize:11,color:T.text,lineHeight:1.5}}>
                    Recomputes every audit item from the current <code>unpaid_stops</code> ↔ <code>ddis_payments</code> join. Click this after importing new DDIS files or if variance numbers look stale.
                  </div>
                </div>
                <button
                  onClick={rebuildAuditItems}
                  disabled={rebuilding || purging}
                  style={{
                    padding: "9px 16px",
                    borderRadius: 8,
                    border: "none",
                    background: (rebuilding || purging) ? T.bgSurface : `linear-gradient(135deg,${T.brand},${T.brandLight})`,
                    color: (rebuilding || purging) ? T.text : "#fff",
                    fontSize: 12,
                    fontWeight: 700,
                    cursor: (rebuilding || purging) ? "wait" : "pointer",
                    whiteSpace: "nowrap",
                  }}
                >
                  {rebuilding ? "⏳ Rebuilding…" : "🔄 Rebuild Now"}
                </button>
              </div>
              {rebuildStatus && <div style={{marginTop:8,fontSize:11,color:T.textMuted}}>{rebuildStatus}</div>}
              {rebuildResult?.error && (
                <div style={{marginTop:10,padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText}}>
                  ✗ {rebuildResult.error}
                </div>
              )}
              {rebuildResult?.ok && (
                <div style={{marginTop:10}}>
                  {/* v2.40.25: if the background computed items but wrote zero, surface that
                      as an error, not a success. The audit-rebuild background function's writes
                      are blocked by Firestore security rules (403 PERMISSION_DENIED via REST API
                      key). Until we wire a service account / firebase-admin auth path, items
                      generated here are not being persisted to audit_items. */}
                  {rebuildResult.generated > 0 && rebuildResult.saved === 0 ? (
                    <div style={{padding:"10px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}`,fontSize:11,color:T.redText}}>
                      <div style={{fontWeight:700,marginBottom:4}}>⚠️ Rebuild computed {rebuildResult.generated} items but wrote 0 to Firestore.</div>
                      <div style={{lineHeight:1.5,fontSize:10}}>
                        The background function's write permissions are blocked (403 PERMISSION_DENIED).
                        Your audit queue still shows the pre-existing stale data. Chad: this is a known issue
                        being tracked — tell Claude "fix audit-rebuild writes" in next session. For now, DDIS
                        import IS working (writes happen client-side), so import your remits normally and
                        the payment matching will activate once rebuild writes are fixed.
                      </div>
                    </div>
                  ) : (
                    <div style={{padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText}}>
                      ✓ Rebuilt {rebuildResult.saved || rebuildResult.generated} audit items ({fmtK(rebuildResult.totalVariance)} total variance)
                      {!rebuildResult.withPayments && <div style={{marginTop:4,fontSize:10}}>Note: no PRO-level payment matching — re-ingest a DDIS file to enable.</div>}
                    </div>
                  )}
                  {/* v2.40.12: sideline summary — stops exempted from the audit queue */}
                  {(rebuildResult.sidelinedRecentStops > 0 || rebuildResult.sidelinedAwaitingStops > 0) && (
                    <div style={{marginTop:6,padding:"8px 12px",background:T.bgSurface,borderRadius:6,border:`1px solid ${T.border}`,fontSize:11,color:T.textMuted}}>
                      <div style={{fontWeight:700,color:T.text,marginBottom:4}}>ℹ️ Sidelined from audit queue</div>
                      {rebuildResult.sidelinedRecentStops > 0 && (
                        <div>
                          <strong style={{color:T.text}}>{rebuildResult.sidelinedRecentStops.toLocaleString()} stops</strong>
                          {" "}({fmtK(rebuildResult.sidelinedRecentBilled)}) too recent — {rebuildResult.sidelinedRecentWeeks.length} week{rebuildResult.sidelinedRecentWeeks.length===1?"":"s"} after {rebuildResult.recentCutoffISO}, waiting on DDIS file
                          {rebuildResult.sidelinedRecentWeeks.length > 0 && (
                            <div style={{fontSize:10,marginTop:2,color:T.textDim}}>
                              {rebuildResult.sidelinedRecentWeeks.slice(0,6).map(w => `${w.week} (${w.stops})`).join(" · ")}
                              {rebuildResult.sidelinedRecentWeeks.length > 6 ? ` · +${rebuildResult.sidelinedRecentWeeks.length-6} more` : ""}
                            </div>
                          )}
                        </div>
                      )}
                      {rebuildResult.sidelinedAwaitingStops > 0 && (
                        <div style={{marginTop:rebuildResult.sidelinedRecentStops>0?6:0}}>
                          <strong style={{color:T.text}}>{rebuildResult.sidelinedAwaitingStops.toLocaleString()} stops</strong>
                          {" "}({fmtK(rebuildResult.sidelinedAwaitingBilled)}) awaiting DDIS — {rebuildResult.sidelinedAwaitingWeeks.length} older week{rebuildResult.sidelinedAwaitingWeeks.length===1?"":"s"} with no matching payment file yet
                          {rebuildResult.sidelinedAwaitingWeeks.length > 0 && (
                            <div style={{fontSize:10,marginTop:2,color:T.textDim}}>
                              {rebuildResult.sidelinedAwaitingWeeks.slice(0,6).map(w => `${w.week} (${w.stops})`).join(" · ")}
                              {rebuildResult.sidelinedAwaitingWeeks.length > 6 ? ` · +${rebuildResult.sidelinedAwaitingWeeks.length-6} more` : ""}
                            </div>
                          )}
                          <div style={{fontSize:10,marginTop:4,fontStyle:"italic"}}>
                            Try Gmail Sync → DDIS → &quot;Show missing only&quot; → Import All to fill these in.
                          </div>
                        </div>
                      )}
                      {rebuildResult.sidelinedNoWeekStops > 0 && (
                        <div style={{marginTop:6,fontSize:10,color:T.textDim}}>
                          {rebuildResult.sidelinedNoWeekStops} stops have no week_ending field set (re-ingest the source DAS file to fix).
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
              {/* v2.40.18: Danger-zone purge. Below the rebuild UI, visually separated. */}
              <div style={{marginTop:14,paddingTop:12,borderTop:`1px dashed ${T.border}`}}>
                <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:10,flexWrap:"wrap"}}>
                  <div style={{flex:"1 1 260px"}}>
                    <div style={{fontSize:11,fontWeight:700,color:T.red,marginBottom:2}}>⚠️ Purge audit queue</div>
                    <div style={{fontSize:10,color:T.textMuted,lineHeight:1.5}}>
                      Nukes all <code>{itemsWithFreshAge.length.toLocaleString()}</code> audit items. Use this to clean up stale data from pre-v2.40.18 rebuilds (which didn't delete ghost items). A fresh Rebuild after purging will start from the current <code>unpaid_stops</code> ↔ <code>ddis_payments</code> truth.
                    </div>
                  </div>
                  <button
                    onClick={purgeAuditItems}
                    disabled={purging || rebuilding || itemsWithFreshAge.length === 0}
                    style={{
                      padding: "7px 14px",
                      borderRadius: 8,
                      border: `1px solid ${T.red}`,
                      background: (purging || rebuilding) ? T.bgSurface : "transparent",
                      color: (purging || rebuilding || itemsWithFreshAge.length === 0) ? T.textMuted : T.red,
                      fontSize: 11,
                      fontWeight: 700,
                      cursor: (purging || rebuilding || itemsWithFreshAge.length === 0) ? "not-allowed" : "pointer",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {purging ? `⏳ Purging… ${purgeResult?.in_progress ? purgeResult.deleted?.toLocaleString() : ""}` : "🗑️ Purge All"}
                  </button>
                </div>
                {purgeResult?.in_progress && (
                  <div style={{marginTop:8,padding:"8px 12px",background:T.bgSurface,borderRadius:6,border:`1px solid ${T.border}`,fontSize:11,color:T.textMuted}}>
                    Deleted {purgeResult.deleted?.toLocaleString()} so far…
                  </div>
                )}
                {purgeResult?.error && (
                  <div style={{marginTop:8,padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText}}>
                    ✗ {purgeResult.error}
                  </div>
                )}
                {purgeResult?.ok && (
                  <div style={{marginTop:8,padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText}}>
                    ✓ Deleted {purgeResult.deleted?.toLocaleString()} audit items in {purgeResult.elapsed_s}s
                    {purgeResult.failed > 0 && ` (${purgeResult.failed} failed)`}. Click Rebuild Now to regenerate from current data.
                  </div>
                )}
              </div>
            </div>

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
                    <tr>{[["Customer","customer"],["Items","count"],["Outstanding","amount"],["Oldest Age","oldest_age"],["",""]].map(([label,key]) => key ? (
                      <SortableTh key={key} label={label} col={key} sortKey={custSort.sortKey} sortDir={custSort.sortDir} onSort={custSort.toggleSort}
                        style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:custSort.sortKey===key?T.brand:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}} />
                    ) : <th key="actions" style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600}}></th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {custSort.sorted.map(c => (
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
            {/* v2.40.14: service_type filter — TK / delivery / accessorial */}
            <select value={filterService} onChange={e=>setFilterService(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All services</option>
              <option value="delivery">🚚 Delivery only</option>
              <option value="truckload">📦 Truckload only</option>
              <option value="accessorial">➕ Accessorial only</option>
            </select>
            <select value={filterAge} onChange={e=>setFilterAge(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All ages</option>
              {AGE_BUCKETS.map(a => <option key={a} value={a}>{a} days</option>)}
            </select>
            <select value={filterStatus} onChange={e=>setFilterStatus(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="all">All statuses</option>
              {["new","queued","sent","won","lost","partial","written_off","recovered_paid"].map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <select value={sortBy} onChange={e=>setSortBy(e.target.value)} style={{...inputStyle,fontSize:12,width:"auto"}}>
              <option value="variance">Sort: $ owed</option>
              <option value="age">Sort: age</option>
              <option value="date">Sort: date</option>
              <option value="customer">Sort: customer</option>
            </select>
          </div>
          {/* v2.40.31: Totals strip. Prior version only showed a count. For the
              "how much am I owed right now?" question this page exists to answer,
              the user needed to eyeball 53+ variance cells and do the arithmetic
              in their head. The three sums below collapse that to one glance and
              update live as filters change. */}
          {(() => {
            const totBilled = filteredItems.reduce((s, i) => s + (i.billed || 0), 0);
            const totPaid = filteredItems.reduce((s, i) => s + (i.paid || 0), 0);
            const totVar = filteredItems.reduce((s, i) => s + (i.variance || 0), 0);
            const selCount = Object.keys(selectedIds).filter(k => selectedIds[k]).length;
            const selTot = selCount > 0 ? selectedItems.reduce((s, i) => s + (i.variance || 0), 0) : 0;
            return (
              <div style={{display:"flex",flexWrap:"wrap",gap:16,marginTop:10,paddingTop:10,borderTop:`1px solid ${T.borderLight}`,alignItems:"baseline"}}>
                <div style={{fontSize:11,color:T.textMuted}}>
                  <strong style={{color:T.text,fontSize:13}}>{filteredItems.length}</strong> items
                  {filterMinVar > 0 && <span style={{color:T.textDim}}> (min ${filterMinVar})</span>}
                </div>
                <div style={{fontSize:11,color:T.textMuted}}>
                  Billed <strong style={{color:T.text,fontSize:13}}>{fmt(totBilled)}</strong>
                </div>
                <div style={{fontSize:11,color:T.textMuted}}>
                  Paid <strong style={{color:T.textDim,fontSize:13}}>{fmt(totPaid)}</strong>
                </div>
                <div style={{fontSize:11,color:T.textMuted}}>
                  Owed <strong style={{color:T.red,fontSize:15,fontWeight:800}}>{fmt(totVar)}</strong>
                </div>
                {selCount > 0 && (
                  <div style={{fontSize:11,color:T.textMuted,marginLeft:"auto",padding:"2px 10px",background:T.brandPale,borderRadius:4}}>
                    <strong style={{color:T.brand,fontSize:12}}>{selCount}</strong> selected • <strong style={{color:T.brand}}>{fmt(selTot)}</strong> owed
                  </div>
                )}
              </div>
            );
          })()}
        </div>

        {/* v2.40.18: Uline export panel — dumps selected (or filtered if nothing selected)
            items to CSV / XLSX / PDF for sending to Uline AP. Unlike the bulk-actions
            "Generate Dispute PDF" below, this is a discovery-stage export: no dispute
            record is created, no item statuses are changed. */}
        <div style={{...cardStyle, padding:"10px 12px", marginBottom:8}}>
          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8,flexWrap:"wrap"}}>
            <div style={{flex:"1 1 200px"}}>
              <div style={{fontSize:12,fontWeight:700,color:T.text,marginBottom:2}}>📤 Export for Uline</div>
              <div style={{fontSize:10,color:T.textMuted,lineHeight:1.5}}>
                {selectedItems.length > 0
                  ? <>Will export <strong>{selectedItems.length}</strong> selected item{selectedItems.length===1?"":"s"} ({fmtK(selectedItems.reduce((s,i)=>s+(i.variance||0),0))} variance).</>
                  : <>Will export <strong>{filteredItems.length}</strong> filtered item{filteredItems.length===1?"":"s"} ({fmtK(filteredItems.reduce((s,i)=>s+(i.variance||0),0))} variance). Select rows to narrow.</>}
              </div>
            </div>
            <button
              onClick={() => setExportOpen(v => !v)}
              disabled={exportBusy || (filteredItems.length === 0 && selectedItems.length === 0)}
              style={{
                padding: "7px 14px",
                borderRadius: 8,
                border: `1px solid ${T.brand}`,
                background: exportOpen ? T.brand : "transparent",
                color: exportOpen ? "#fff" : T.brand,
                fontSize: 11,
                fontWeight: 700,
                cursor: (exportBusy || (filteredItems.length === 0 && selectedItems.length === 0)) ? "not-allowed" : "pointer",
                whiteSpace: "nowrap",
              }}
            >
              {exportOpen ? "▼ Hide formats" : "📤 Export…"}
            </button>
          </div>
          {exportOpen && (
            <div style={{marginTop:10,paddingTop:10,borderTop:`1px dashed ${T.border}`,display:"flex",gap:8,flexWrap:"wrap"}}>
              <button onClick={exportCSV} disabled={exportBusy}
                style={{padding:"8px 14px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.border}`,background:T.bgWhite,color:T.text,cursor:exportBusy?"wait":"pointer",flex:"1 1 140px"}}>
                📄 CSV
                <div style={{fontSize:9,color:T.textMuted,fontWeight:500,marginTop:2}}>Universal, opens in Excel</div>
              </button>
              <button onClick={exportXLSX} disabled={exportBusy}
                style={{padding:"8px 14px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.green}`,background:T.greenBg,color:T.greenText,cursor:exportBusy?"wait":"pointer",flex:"1 1 140px"}}>
                📊 Excel (.xlsx)
                <div style={{fontSize:9,color:T.textMuted,fontWeight:500,marginTop:2}}>Formatted + totals row</div>
              </button>
              <button onClick={exportPDF} disabled={exportBusy}
                style={{padding:"8px 14px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.brand}`,background:T.brandPale,color:T.brand,cursor:exportBusy?"wait":"pointer",flex:"1 1 140px"}}>
                📕 PDF
                <div style={{fontSize:9,color:T.textMuted,fontWeight:500,marginTop:2}}>Letter-style, send to APFreight@uline.com</div>
              </button>
            </div>
          )}
          {exportStatus && (
            <div style={{marginTop:8,fontSize:11,color:exportStatus.startsWith("✓")?T.greenText:exportStatus.startsWith("✗")?T.redText:T.textMuted}}>
              {exportStatus}
            </div>
          )}
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
                  // All disputes go to Uline — generate ONE combined PDF
                  (async () => {
                    await generateDisputePdf(selectedItems, "Uline");
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
                  {[
                    ["PRO","pro"],["Customer","customer"],["Billed","billed"],
                    ["Paid","paid"],["Variance","variance"],["Age","age"],
                    ["Category","category"],["Status","status"],
                  ].map(([label, key]) => {
                    const active = sortBy === key || sortBy === "-" + key;
                    const asc = sortBy === key;
                    const chevron = active ? (asc ? " ↑" : " ↓") : " ↕";
                    return (
                      <th key={key} onClick={() => setSortBy(sortBy === key ? "-"+key : key)}
                        style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,
                          color: active ? T.brand : T.textDim,fontSize:9,fontWeight:600,
                          textTransform:"uppercase",cursor:"pointer",userSelect:"none",whiteSpace:"nowrap"}}>
                        {label}<span style={{opacity:active?1:0.35,fontSize:"0.8em",marginLeft:2}}>{chevron}</span>
                      </th>
                    );
                  })}
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
                    {[["Customer","customer"],["Items","count"],["Claimed","total_claimed"],["Recovered","total_recovered"],["Submitted","submitted_date"],["Outcome","outcome"],["Actions",""]].map(([label,key]) => key ? (
                      <SortableTh key={key} label={label} col={key} sortKey={dispSort.sortKey} sortDir={dispSort.sortDir} onSort={dispSort.toggleSort}
                        style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:dispSort.sortKey===key?T.brand:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}} />
                    ) : <th key="act" style={{padding:"8px 10px",borderBottom:`1px solid ${T.border}`}}></th>)}
                  </tr>
                </thead>
                <tbody>
                  {dispSort.sorted.map(d => {
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
  // v2.40.21: DDIS payment trace — diagnostic breakdown of why this PRO is
  // showing up as paid/unpaid/awaiting. Loads lazily on tap so we don't pay
  // a Firestore round trip every time the detail modal opens.
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceData, setTraceData] = useState(null);
  const [traceError, setTraceError] = useState(null);
  const [traceOpen, setTraceOpen] = useState(false);
  const loadTrace = async () => {
    if (traceLoading) return;
    setTraceOpen(true);
    if (traceData) return; // already loaded
    setTraceLoading(true);
    setTraceError(null);
    try {
      const resp = await fetch(`/.netlify/functions/marginiq-audit-trace?pro=${encodeURIComponent(item.pro)}`);
      if (!resp.ok) {
        const t = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${t.slice(0, 200)}`);
      }
      const data = await resp.json();
      if (data.error) throw new Error(data.error);
      setTraceData(data);
    } catch (e) {
      console.error("trace failed:", e);
      setTraceError(e.message);
    }
    setTraceLoading(false);
  };
  const verdictColor = (v) =>
    v === "paid" || v === "paid_under_core" ? T.green :
    v === "short_paid" ? T.yellow :
    v === "unpaid_ddis_present" ? T.red :
    v === "awaiting_ddis" ? T.blue : T.textMuted;
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

        {/* v2.40.21: DDIS Payment Trace — on-demand diagnostic for billed-vs-paid */}
        <div style={{padding:"10px",background:T.bgSurface,borderRadius:8,marginBottom:12,border:`1px solid ${T.border}`}}>
          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:8}}>
            <div style={{fontSize:12,fontWeight:700,color:T.text}}>🔍 DDIS Payment Trace</div>
            <button onClick={loadTrace} disabled={traceLoading}
              style={{padding:"6px 12px",fontSize:11,fontWeight:700,borderRadius:6,border:`1px solid ${T.brand}`,
                background: traceOpen ? T.brand : "transparent",
                color: traceOpen ? "#fff" : T.brand,
                cursor: traceLoading ? "wait" : "pointer"}}>
              {traceLoading ? "⏳ Running…" : traceOpen ? "▼ Loaded" : "Run trace"}
            </button>
          </div>
          {traceOpen && (
            <div style={{marginTop:10,paddingTop:10,borderTop:`1px dashed ${T.border}`,fontSize:11,color:T.text,lineHeight:1.6}}>
              {traceError && (
                <div style={{padding:"8px 10px",background:T.redBg,color:T.redText,borderRadius:6,fontSize:11}}>
                  ✗ {traceError}
                </div>
              )}
              {traceData && (
                <>
                  {/* Verdict banner */}
                  <div style={{padding:"8px 10px",background:verdictColor(traceData.verdict)+"18",borderRadius:6,border:`1px solid ${verdictColor(traceData.verdict)}55`,marginBottom:10}}>
                    <div style={{fontSize:11,fontWeight:700,color:verdictColor(traceData.verdict),textTransform:"uppercase",letterSpacing:"0.05em",marginBottom:3}}>
                      Verdict: {traceData.verdict.replace(/_/g, " ")}
                    </div>
                    <div style={{fontSize:11,color:T.text,lineHeight:1.5}}>{traceData.explanation}</div>
                  </div>

                  {/* Lookup keys */}
                  <div style={{marginBottom:10,padding:"6px 8px",background:T.bgWhite,borderRadius:6,border:`1px solid ${T.border}`,fontSize:10,fontFamily:"monospace",color:T.textMuted}}>
                    Tried PRO: <span style={{color:T.text}}>{traceData.pro}</span>
                    {traceData.numeric_core && <> · numeric core: <span style={{color:T.text}}>{traceData.numeric_core}</span></>}
                  </div>

                  {/* Direct payments */}
                  <div style={{marginBottom:10}}>
                    <div style={{fontSize:11,fontWeight:700,marginBottom:4}}>
                      Direct matches in <code>ddis_payments</code>: {traceData.direct_payments.length} · total ${(traceData.total_paid||0).toFixed(2)}
                    </div>
                    {traceData.direct_payments.length === 0 ? (
                      <div style={{fontSize:10,color:T.textMuted,fontStyle:"italic"}}>No payment rows found for either key.</div>
                    ) : (
                      <div style={{overflowX:"auto"}}>
                        <table style={{width:"100%",fontSize:10,borderCollapse:"collapse"}}>
                          <thead><tr style={{background:T.bgSurface}}>
                            {["PRO","Paid","Bill Date","Check","Voucher","Source File"].map(h =>
                              <th key={h} style={{textAlign:"left",padding:"4px 6px",fontWeight:600,color:T.textDim,fontSize:9,textTransform:"uppercase"}}>{h}</th>
                            )}
                          </tr></thead>
                          <tbody>
                            {traceData.direct_payments.map((p,i) =>
                              <tr key={i} style={{borderBottom:`1px solid ${T.border}`}}>
                                <td style={{padding:"4px 6px",fontFamily:"monospace"}}>{p.pro}</td>
                                <td style={{padding:"4px 6px",color:T.green,fontWeight:600}}>${Number(p.paid_amount||0).toFixed(2)}</td>
                                <td style={{padding:"4px 6px"}}>{p.bill_date || "—"}</td>
                                <td style={{padding:"4px 6px",fontFamily:"monospace",fontSize:9}}>{p.check || "—"}</td>
                                <td style={{padding:"4px 6px",fontFamily:"monospace",fontSize:9}}>{p.voucher || "—"}</td>
                                <td style={{padding:"4px 6px",fontSize:9,color:T.textMuted}}>{p.source_file || "—"}</td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>

                  {/* Candidate files */}
                  <div style={{marginBottom:10}}>
                    <div style={{fontSize:11,fontWeight:700,marginBottom:4}}>
                      DDIS files checked: {traceData.candidate_files.length}
                    </div>
                    {traceData.candidate_files.length === 0 ? (
                      <div style={{fontSize:10,color:T.redText,fontStyle:"italic"}}>
                        ⚠️ No DDIS files ingested cover this stop's week or pickup date. Import the remit via Gmail Sync → DDIS.
                      </div>
                    ) : (
                      <div style={{maxHeight:160,overflowY:"auto"}}>
                        {traceData.candidate_files.slice(0, 10).map((f,i) =>
                          <div key={i} style={{fontSize:10,padding:"4px 6px",borderBottom:`1px solid ${T.border}`,display:"flex",justifyContent:"space-between",gap:6}}>
                            <div style={{fontFamily:"monospace",flex:"1 1 auto",minWidth:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>
                              {f.covers_this_week && <span style={{color:T.green,marginRight:4}}>✓</span>}
                              {f.filename}
                            </div>
                            <div style={{color:T.textMuted,fontSize:9,whiteSpace:"nowrap"}}>
                              bwe {f.bill_week_ending || "?"} · {f.earliest_bill_date || "?"}→{f.latest_bill_date || "?"}
                            </div>
                          </div>
                        )}
                        {traceData.candidate_files.length > 10 && (
                          <div style={{fontSize:9,color:T.textDim,padding:"4px 6px",fontStyle:"italic"}}>
                            +{traceData.candidate_files.length - 10} more files in range…
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Near misses */}
                  {traceData.near_misses.length > 0 && (
                    <div style={{marginBottom:4}}>
                      <div style={{fontSize:11,fontWeight:700,marginBottom:4,color:T.yellow}}>
                        ⚠️ Near-miss payments in same files (amount ±$5 of billed, different PRO):
                      </div>
                      <div style={{overflowX:"auto"}}>
                        <table style={{width:"100%",fontSize:10,borderCollapse:"collapse"}}>
                          <thead><tr style={{background:T.bgSurface}}>
                            {["PRO","Amount","Δ","Bill Date","File"].map(h =>
                              <th key={h} style={{textAlign:"left",padding:"4px 6px",fontWeight:600,color:T.textDim,fontSize:9,textTransform:"uppercase"}}>{h}</th>
                            )}
                          </tr></thead>
                          <tbody>
                            {traceData.near_misses.map((n,i) =>
                              <tr key={i} style={{borderBottom:`1px solid ${T.border}`}}>
                                <td style={{padding:"4px 6px",fontFamily:"monospace"}}>{n.pro}</td>
                                <td style={{padding:"4px 6px"}}>${Number(n.paid_amount||0).toFixed(2)}</td>
                                <td style={{padding:"4px 6px",color:T.yellow}}>±${n.delta.toFixed(2)}</td>
                                <td style={{padding:"4px 6px"}}>{n.bill_date || "—"}</td>
                                <td style={{padding:"4px 6px",fontSize:9,color:T.textMuted}}>{n.source_file || "—"}</td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                      <div style={{fontSize:9,color:T.textDim,marginTop:3,fontStyle:"italic"}}>
                        Could indicate a PRO-number encoding difference or data-entry error at Uline. Worth a look.
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          )}
        </div>

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
  const drvSort = useSortable([], "nv_stops", "desc");

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

  // v2.40.33: sortable by column click via drvSort
  const filtered = useMemo(() => {
    let arr = classified;
    if (filter !== "all") arr = arr.filter(d => d.classification === filter);
    if (search.trim()) {
      const q = search.toLowerCase();
      arr = arr.filter(d => d.name.toLowerCase().includes(q));
    }
    // Apply manual sort (drvSort.sorted only works when rows are passed at init;
    // here we sort the already-filtered arr inline using drvSort's state)
    const { sortKey, sortDir } = drvSort;
    if (!sortKey) return arr;
    return [...arr].sort((a, b) => {
      const av = sortKey === "sources" ? a.sources.length : a[sortKey];
      const bv = sortKey === "sources" ? b.sources.length : b[sortKey];
      if (av == null) return 1; if (bv == null) return -1;
      const cmp = typeof av === "string"
        ? av.localeCompare(bv, undefined, { numeric: true, sensitivity: "base" })
        : av - bv;
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [classified, filter, search, drvSort.sortKey, drvSort.sortDir]);

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
      <SectionTitle icon="👥" text="Employees" />
      <EmptyState icon="👥" title="No Employees Found" sub="Upload NuVizz or Payroll files in Data Ingest. Employees appearing there will show up here for classification." />
    </div>;
  }

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="👥" text="Employees — W2 / 1099 Classification" />

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
            {[["Driver","name"],["Stops","nv_stops"],["Pay Base","nv_pay_base"],["@40%","nv_pay_at_40"],["Payroll $","pay_gross"],["Sources","sources"],["Last Seen","last_seen"],["Classification","classification"]].map(([label,key]) => (
              <SortableTh key={key} label={label} col={key} sortKey={drvSort.sortKey} sortDir={drvSort.sortDir} onSort={drvSort.toggleSort}
                style={{textAlign:"left",padding:"8px 10px",fontWeight:700,color:drvSort.sortKey===key?T.brand:T.textMuted,fontSize:11,background:T.bgSurface}} />
            ))}
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

function Settings({ qboConnected, motiveConnected, reconMeta, weeklyRollups, onRefresh, setTab }) {
  const [purgeConfirmText, setPurgeConfirmText] = useState("");
  const [purgeToken, setPurgeToken] = useState("");
  const [purging, setPurging] = useState(false);
  const [purgeResult, setPurgeResult] = useState(null);

  // Backups state
  const [backups, setBackups] = useState([]);
  const [backupsLoading, setBackupsLoading] = useState(true);
  const [backupAction, setBackupAction] = useState(null); // "run" | "restore" | null
  const [backupMsg, setBackupMsg] = useState(null);
  const [restoreDate, setRestoreDate] = useState(null);
  const [restoreConfirm, setRestoreConfirm] = useState("");
  const [adminToken, setAdminToken] = useState("");

  // v2.40.9: DDIS bill-week backfill state
  const [backfillRunning, setBackfillRunning] = useState(false);
  const [backfillMsg, setBackfillMsg] = useState(null);
  // Resolve-ambiguous state (manual week pick for tie files)
  const [resolvingFile, setResolvingFile] = useState(null); // file_id being resolved
  const [resolveMsg, setResolveMsg] = useState(null);

  // v2.40.8: Live DDIS stats pulled from the ddis_files collection itself.
  // Previous versions read reconMeta.files_count, a counter kept in a side
  // doc inside marginiq_config that got out of sync with reality (user had
  // 42 real files but the counter read 1). Ground-truthing against the
  // collection avoids that drift entirely.
  const [ddisStats, setDdisStats] = useState(null);
  const [ddisAmbiguous, setDdisAmbiguous] = useState([]);
  const [ddisNeedingBackfill, setDdisNeedingBackfill] = useState(0);
  // v2.40.13: explicit missing-week lists for the gaps card
  const [dataGaps, setDataGaps] = useState(null); // { dasMissing: [], ddisMissing: [], window: {from, to} }
  useEffect(() => {
    (async () => {
      if (!hasFirebase) return;
      try {
        const files = await FS.getDDISFiles();
        if (!files || files.length === 0) { setDdisStats({ count: 0, totalPaid: 0, rows: 0, weeksCovered: 0 }); return; }
        let totalPaid = 0, rows = 0, earliest = null, latest = null;
        const weeks = new Set();
        const ambiguous = [];
        let needBackfill = 0;
        // v2.40.12 covers_weeks — union across all files, used for gap detection below
        const ddisCoveredWeeks = new Set();
        for (const f of files) {
          totalPaid += Number(f.total_paid || 0);
          rows += Number(f.record_count || 0);
          const eb = f.earliest_bill_date, lb = f.latest_bill_date;
          if (eb && (!earliest || eb < earliest)) earliest = eb;
          if (lb && (!latest || lb > latest)) latest = lb;
          if (f.week_ambiguous) {
            ambiguous.push(f);
            for (const c of (f.ambiguous_candidates || [])) {
              if (c?.friday) ddisCoveredWeeks.add(c.friday);
            }
          } else if (f.bill_week_ending) {
            weeks.add(f.bill_week_ending);
          } else {
            needBackfill += 1;
          }
          // Prefer covers_weeks when present (handles Thanksgiving-style double-weeks)
          if (Array.isArray(f.covers_weeks) && f.covers_weeks.length > 0) {
            for (const w of f.covers_weeks) ddisCoveredWeeks.add(w);
          } else if (f.bill_week_ending) {
            ddisCoveredWeeks.add(f.bill_week_ending);
          }
        }
        setDdisStats({
          count: files.length,
          totalPaid,
          rows,
          earliest,
          latest,
          weeksCovered: weeks.size,
          ambiguousCount: ambiguous.length,
          backfillNeeded: needBackfill,
        });
        setDdisAmbiguous(ambiguous);
        setDdisNeedingBackfill(needBackfill);

        // v2.40.13: compute missing-week sets (DAS + DDIS) for the gaps card.
        // Window: from earliest week we have ANY data for, through the most
        // recent complete Friday that's older than 21 days (so we don't flag
        // the current week as missing while files are still en route).
        const dasWeeks = new Set((weeklyRollups || []).map(r => r.week_ending).filter(Boolean));
        if (dasWeeks.size === 0 && ddisCoveredWeeks.size === 0) {
          setDataGaps(null);
          return;
        }
        const allKnown = [...dasWeeks, ...ddisCoveredWeeks].sort();
        const fromDate = allKnown[0];
        // Cutoff: last Friday ≥ 21 days ago (align with audit rebuild cutoff)
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - 21);
        const cutoffDay = cutoff.getDay();
        const daysBackToFri = cutoffDay >= 5 ? cutoffDay - 5 : cutoffDay + 2;
        cutoff.setDate(cutoff.getDate() - daysBackToFri);
        const toDate = cutoff.toISOString().slice(0, 10);
        // Build expected Friday sequence fromDate → toDate
        const expected = [];
        let d = new Date(fromDate + "T00:00:00");
        const end = new Date(toDate + "T00:00:00");
        while (d <= end) {
          expected.push(d.toISOString().slice(0, 10));
          d.setDate(d.getDate() + 7);
        }
        const dasMissing = expected.filter(w => !dasWeeks.has(w));
        const ddisMissing = expected.filter(w => !ddisCoveredWeeks.has(w));
        setDataGaps({
          dasMissing, ddisMissing,
          window: { from: fromDate, to: toDate },
          totalWeeks: expected.length,
          dasCoveredCount: dasWeeks.size,
          ddisCoveredCount: ddisCoveredWeeks.size,
        });
      } catch(e) { console.error("DDIS stats load failed:", e); }
    })();
  }, [weeklyRollups]);

  const loadBackups = async () => {
    setBackupsLoading(true);
    try {
      const resp = await fetch("/.netlify/functions/marginiq-backup?action=list");
      const data = await resp.json();
      if (data.ok) setBackups(data.backups || []);
    } catch(e) { /* non-fatal */ }
    setBackupsLoading(false);
  };
  useEffect(() => { loadBackups(); }, []);

  const runBackfill = async () => {
    if (backfillRunning) return;
    setBackfillRunning(true);
    setBackfillMsg(null);
    try {
      const resp = await fetch("/.netlify/functions/marginiq-ddis-week-backfill-background", {
        method: "POST",
      });
      if (resp.status === 202 || resp.ok) {
        setBackfillMsg({ ok: true, text: "✓ Backfill queued. Recomputes bill_week_ending for every DDIS file. Reload Settings in ~60s to see updated counts." });
      } else {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data.error || `HTTP ${resp.status}`);
      }
    } catch (e) {
      setBackfillMsg({ error: e.message });
    }
    // Keep the spinner on for ~60s so user doesn't double-click
    setTimeout(() => setBackfillRunning(false), 60000);
  };

  const resolveAmbiguousFile = async (fileId, friday) => {
    setResolvingFile(fileId);
    setResolveMsg(null);
    try {
      await FS.saveDDISFile(fileId, {
        bill_week_ending: friday,
        week_ambiguous: false,
        week_resolved_manually: true,
        week_resolved_at: new Date().toISOString(),
      });
      setResolveMsg({ ok: true, text: `✓ ${fileId} assigned to week ending ${friday}. Reload to refresh counters.` });
    } catch (e) {
      setResolveMsg({ error: `Failed to save: ${e.message}` });
    }
    setResolvingFile(null);
  };

  const runBackupNow = async () => {
    setBackupAction("run");
    setBackupMsg(null);
    try {
      const resp = await fetch(`/.netlify/functions/marginiq-backup`);
      // v2.40.5: the backup function now dispatches to a background function
      // and returns 202 immediately. 200 is the legacy synchronous response
      // (keeps this code backward-compatible if the old behavior ever comes
      // back). v2.40.4-style empty-body 502/504 still possible on dispatcher
      // errors so we keep the defensive parsing.
      const text = await resp.text();
      if (!text) {
        if (resp.status === 502 || resp.status === 504) {
          throw new Error("Backup dispatcher timed out. Try again in a minute.");
        }
        throw new Error(`Empty response (HTTP ${resp.status})`);
      }
      let data;
      try { data = JSON.parse(text); }
      catch { throw new Error(`HTTP ${resp.status}: ${text.substring(0, 200)}`); }
      if (!resp.ok && resp.status !== 202) throw new Error(data.error || `HTTP ${resp.status}`);
      if (data.error) throw new Error(data.error);

      if (data.queued) {
        // Background function is doing the work. Poll the snapshot list a
        // couple of times with backoff so the user sees the new snapshot
        // appear without a manual refresh.
        setBackupMsg({ ok: true, text: data.message || "✓ Backup queued — running in the background." });
        const poll = async (delaySec) => {
          await new Promise(r => setTimeout(r, delaySec * 1000));
          await loadBackups();
        };
        // 30s, 60s, 120s — covers typical backup wall time (~20-60s)
        await poll(30);
        await poll(30);
        await poll(60);
      } else if (data.manifest) {
        // Legacy synchronous response path
        setBackupMsg({ ok: true, text: `✓ Backup created: ${data.manifest.total_docs} docs across ${data.manifest.collections_captured} collections (${Math.round(data.manifest.compressed_bytes/1024)} KB compressed)` });
        await loadBackups();
      } else {
        setBackupMsg({ ok: true, text: "✓ Backup requested. Check the snapshots list." });
        await loadBackups();
      }
    } catch(e) {
      setBackupMsg({ error: e.message });
    }
    setBackupAction(null);
  };

  const confirmRestore = async () => {
    if (restoreConfirm !== `RESTORE ${restoreDate}`) {
      setBackupMsg({ error: `Confirmation text must be exactly: RESTORE ${restoreDate}` });
      return;
    }
    if (!adminToken.trim()) { setBackupMsg({ error: "Admin token required" }); return; }
    setBackupAction("restore");
    setBackupMsg(null);
    try {
      const resp = await fetch(`/.netlify/functions/marginiq-backup?action=restore&token=${encodeURIComponent(adminToken.trim())}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ date: restoreDate }),
      });
      const data = await resp.json();
      if (!resp.ok || data.error) throw new Error(data.error || `HTTP ${resp.status}`);
      const collCount = Object.keys(data.results).length;
      const totalRestored = Object.values(data.results).reduce((s, r) => s + r.restored, 0);
      const totalDeleted = Object.values(data.results).reduce((s, r) => s + r.deleted, 0);
      setBackupMsg({ ok: true, text: `✓ Restored snapshot from ${restoreDate}: ${totalRestored} docs written, ${totalDeleted} extraneous docs removed across ${collCount} collections` });
      setRestoreDate(null);
      setRestoreConfirm("");
      if (onRefresh) await onRefresh();
    } catch(e) {
      setBackupMsg({ error: e.message });
    }
    setBackupAction(null);
  };

  const handlePurge = async () => {
    if (purgeConfirmText !== "PURGE ULINE") {
      setPurgeResult({ error: "Confirmation text must be exactly: PURGE ULINE" });
      return;
    }
    if (!purgeToken.trim()) {
      setPurgeResult({ error: "Admin token required" });
      return;
    }
    setPurging(true);
    setPurgeResult(null);
    try {
      const resp = await fetch(`/.netlify/functions/marginiq-purge-uline?token=${encodeURIComponent(purgeToken.trim())}`);
      const data = await resp.json();
      if (!resp.ok || data.error) throw new Error(data.error || `HTTP ${resp.status}`);
      setPurgeResult(data);
      setPurgeConfirmText("");
      setPurgeToken("");
      if (onRefresh) await onRefresh();
    } catch(e) {
      setPurgeResult({ error: e.message });
    }
    setPurging(false);
  };

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

    {/* v2.40.9: DDIS bill-week backfill banner — appears when any file lacks bill_week_ending */}
    {ddisNeedingBackfill > 0 && (
      <div style={{...cardStyle, border:`1px solid ${T.yellow}60`, background:T.yellowBg || T.bgSurface}}>
        <div style={{display:"flex",alignItems:"center",gap:10,flexWrap:"wrap"}}>
          <div style={{fontSize:14}}>⚠️</div>
          <div style={{flex:1,minWidth:200}}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:4}}>DDIS Bill-Week Backfill Needed</div>
            <div style={{fontSize:11,color:T.textMuted,lineHeight:1.5}}>
              <strong>{ddisNeedingBackfill}</strong> DDIS file{ddisNeedingBackfill===1?"":"s"} {ddisNeedingBackfill===1?"was":"were"} uploaded before v2.40.9 and need{ddisNeedingBackfill===1?"s":""} the <code>bill_week_ending</code> field computed. The counter will stay stale until this runs. Background job — takes ~30–90s, safe to run anytime.
            </div>
          </div>
          <button
            onClick={runBackfill}
            disabled={backfillRunning}
            style={{
              padding:"9px 16px",borderRadius:8,border:"none",
              background: backfillRunning ? T.bgSurface : T.brand,
              color: backfillRunning ? T.textMuted : "#fff",
              fontSize:12,fontWeight:700,
              cursor: backfillRunning ? "wait" : "pointer",
              whiteSpace:"nowrap",
            }}
          >
            {backfillRunning ? "Running…" : "🔁 Run Backfill"}
          </button>
        </div>
        {backfillMsg?.ok && (
          <div style={{marginTop:10,padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText}}>
            {backfillMsg.text}
          </div>
        )}
        {backfillMsg?.error && (
          <div style={{marginTop:10,padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText}}>
            ✗ {backfillMsg.error}
          </div>
        )}
      </div>
    )}

    {/* v2.40.9: Needs Review — files with ambiguous bill-week (tie on top-5 row count) */}
    {ddisAmbiguous && ddisAmbiguous.length > 0 && (
      <div style={{...cardStyle, border:`1px solid ${T.yellow}60`}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:6}}>🔎 Needs Review — Ambiguous Bill Week</div>
        <div style={{fontSize:11,color:T.textMuted,marginBottom:12,lineHeight:1.5}}>
          The top-5 most-common bill dates in {ddisAmbiguous.length===1?"this file":"these files"} split evenly across two or more Sat→Fri envelopes — no single week has the bulk of the payments. Pick which week {ddisAmbiguous.length===1?"this file":"each file"} should count toward, or leave it null (it won't contribute to the coverage count).
        </div>
        {ddisAmbiguous.map(f => (
          <div key={f.file_id} style={{padding:"10px 12px",border:`1px solid ${T.border}`,borderRadius:6,marginBottom:8}}>
            <div style={{fontSize:12,fontWeight:600,marginBottom:4}}>{f.filename || f.file_id}</div>
            <div style={{fontSize:10,color:T.textMuted,marginBottom:8}}>
              {(f.record_count||0).toLocaleString()} rows · ${Number(f.total_paid||0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})} · {f.earliest_bill_date} → {f.latest_bill_date}
            </div>
            <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
              {(f.ambiguous_candidates || []).map(c => (
                <button
                  key={c.friday}
                  onClick={() => resolveAmbiguousFile(f.file_id, c.friday)}
                  disabled={resolvingFile === f.file_id}
                  style={{
                    padding:"6px 12px",borderRadius:6,border:`1px solid ${T.brand}`,
                    background: "transparent", color: T.brand,
                    fontSize:11, fontWeight:600,
                    cursor: resolvingFile === f.file_id ? "wait" : "pointer",
                  }}
                >
                  Week ending {c.friday} ({c.rows} rows)
                </button>
              ))}
            </div>
          </div>
        ))}
        {resolveMsg?.ok && (
          <div style={{padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText}}>
            {resolveMsg.text}
          </div>
        )}
        {resolveMsg?.error && (
          <div style={{padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText}}>
            ✗ {resolveMsg.error}
          </div>
        )}
      </div>
    )}

    {/* v2.40.13: Data Gaps card — explicit list of weeks missing DAS or DDIS files */}
    {dataGaps && (dataGaps.dasMissing.length > 0 || dataGaps.ddisMissing.length > 0) && (
      <div style={{...cardStyle, border:`1px solid ${T.red}60`}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:6}}>🚨 Data Coverage Gaps</div>
        <div style={{fontSize:11,color:T.textMuted,marginBottom:12,lineHeight:1.5}}>
          Weeks between <strong>{dataGaps.window.from}</strong> and <strong>{dataGaps.window.to}</strong> (cutoff is 21 days ago to avoid flagging files still in transit) that are missing a DAS billing file, a DDIS payment file, or both. These gaps make audit math incomplete — fill them in and rebuild.
        </div>
        <div style={{display:"flex",gap:8,marginBottom:12,flexWrap:"wrap"}}>
          <div style={{flex:1,minWidth:180,padding:"8px 10px",background:T.bgSurface,borderRadius:6,fontSize:11}}>
            <div style={{color:T.textMuted,marginBottom:2}}>Total expected weeks</div>
            <div style={{fontSize:14,fontWeight:700}}>{dataGaps.totalWeeks}</div>
          </div>
          <div style={{flex:1,minWidth:180,padding:"8px 10px",background:T.bgSurface,borderRadius:6,fontSize:11}}>
            <div style={{color:T.textMuted,marginBottom:2}}>DAS coverage</div>
            <div style={{fontSize:14,fontWeight:700,color:dataGaps.dasMissing.length===0?T.greenText:T.text}}>
              {dataGaps.dasCoveredCount}/{dataGaps.totalWeeks} ({Math.round(dataGaps.dasCoveredCount/Math.max(1,dataGaps.totalWeeks)*100)}%)
            </div>
          </div>
          <div style={{flex:1,minWidth:180,padding:"8px 10px",background:T.bgSurface,borderRadius:6,fontSize:11}}>
            <div style={{color:T.textMuted,marginBottom:2}}>DDIS coverage</div>
            <div style={{fontSize:14,fontWeight:700,color:dataGaps.ddisMissing.length===0?T.greenText:T.text}}>
              {dataGaps.ddisCoveredCount}/{dataGaps.totalWeeks} ({Math.round(dataGaps.ddisCoveredCount/Math.max(1,dataGaps.totalWeeks)*100)}%)
            </div>
          </div>
        </div>

        {dataGaps.ddisMissing.length > 0 && (
          <div style={{padding:"10px 12px",background:T.yellowBg||T.bgSurface,borderRadius:6,border:`1px solid ${T.yellow}40`,marginBottom:8}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,marginBottom:6}}>
              <div style={{fontSize:12,fontWeight:700,color:"#78350f"}}>💰 Missing DDIS (payment) files — {dataGaps.ddisMissing.length} week{dataGaps.ddisMissing.length===1?"":"s"}</div>
              {setTab && (
                <button
                  onClick={() => setTab("gmail")}
                  style={{
                    padding:"6px 14px",borderRadius:6,border:"none",
                    background:T.green,color:"#fff",
                    fontSize:11,fontWeight:700,cursor:"pointer",whiteSpace:"nowrap",
                  }}
                >🚀 Fetch Missing DDIS →</button>
              )}
            </div>
            <div style={{fontSize:10,color:T.textMuted,marginBottom:8,lineHeight:1.5}}>
              No ingested DDIS file covers these Fridays. Stops for these weeks can't be scored by the audit rebuild (they sideline as &quot;awaiting DDIS&quot;). The button above jumps to Gmail Sync — hit <strong>Uline DDIS Payments</strong>, set the range to <strong>All time</strong>, toggle <strong>Show missing only</strong>, then <strong>Import All</strong>.
            </div>
            <div style={{fontFamily:"monospace",fontSize:10,lineHeight:1.6,color:"#78350f",display:"flex",flexWrap:"wrap",gap:6}}>
              {dataGaps.ddisMissing.slice(0, 30).map(w => <span key={w} style={{padding:"2px 8px",background:"#fff",borderRadius:4,border:"1px solid #fde68a"}}>{w}</span>)}
              {dataGaps.ddisMissing.length > 30 && <span style={{color:T.textMuted}}>+{dataGaps.ddisMissing.length-30} more</span>}
            </div>
          </div>
        )}

        {dataGaps.dasMissing.length > 0 && (
          <div style={{padding:"10px 12px",background:T.bgSurface,borderRadius:6,border:`1px solid ${T.blue}40`}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8,marginBottom:6}}>
              <div style={{fontSize:12,fontWeight:700,color:T.text}}>📦 Missing DAS (billing) files — {dataGaps.dasMissing.length} week{dataGaps.dasMissing.length===1?"":"s"}</div>
              {setTab && (
                <button
                  onClick={() => setTab("gmail")}
                  style={{
                    padding:"6px 14px",borderRadius:6,border:"none",
                    background:T.blue,color:"#fff",
                    fontSize:11,fontWeight:700,cursor:"pointer",whiteSpace:"nowrap",
                  }}
                >🚀 Fetch Missing DAS →</button>
              )}
            </div>
            <div style={{fontSize:10,color:T.textMuted,marginBottom:8,lineHeight:1.5}}>
              No uline_weekly rollup exists for these Fridays. Unpaid stops from these weeks never got ingested, so they aren't in the audit at all. The button above jumps to Gmail Sync — hit <strong>Uline Weekly Billing</strong>, set range to <strong>All time</strong>, toggle <strong>Show missing only</strong>, then <strong>Import All</strong>.
            </div>
            <div style={{fontFamily:"monospace",fontSize:10,lineHeight:1.6,color:T.text,display:"flex",flexWrap:"wrap",gap:6}}>
              {dataGaps.dasMissing.slice(0, 30).map(w => <span key={w} style={{padding:"2px 8px",background:"#fff",borderRadius:4,border:`1px solid ${T.borderLight}`}}>{w}</span>)}
              {dataGaps.dasMissing.length > 30 && <span style={{color:T.textMuted}}>+{dataGaps.dasMissing.length-30} more</span>}
            </div>
          </div>
        )}
      </div>
    )}

    {/* v2.40.13: all-clear banner when nothing is missing in the covered window */}
    {dataGaps && dataGaps.dasMissing.length === 0 && dataGaps.ddisMissing.length === 0 && dataGaps.totalWeeks > 0 && (
      <div style={{...cardStyle, border:`1px solid ${T.green}60`, background:T.greenBg}}>
        <div style={{fontSize:12,fontWeight:700,color:T.greenText}}>
          ✅ Data Coverage Complete — {dataGaps.totalWeeks} weeks of DAS + DDIS, no gaps between {dataGaps.window.from} and {dataGaps.window.to}.
        </div>
      </div>
    )}

    <div style={cardStyle}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>System Info</div>
      {(() => {
        // v2.40.9: DDIS stats. Files Loaded counts docs; Coverage counts
        // distinct bill_week_ending values (each file = one week). Ambiguous
        // files don't contribute to the week count until resolved.
        const ddisCount  = ddisStats ? ddisStats.count : "—";
        const ddisWeeks  = ddisStats?.count ? `${ddisStats.weeksCovered} week${ddisStats.weeksCovered===1?"":"s"}` : "—";
        const ddisAmbig  = ddisStats ? (ddisStats.ambiguousCount || 0) : 0;
        const ddisPaid   = ddisStats?.count ? `$${(ddisStats.totalPaid || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—";
        const ddisRange  = ddisStats?.earliest && ddisStats?.latest ? `${ddisStats.earliest} → ${ddisStats.latest}` : "—";
        const ddisRows   = ddisStats?.count ? `${(ddisStats.rows || 0).toLocaleString()} payment rows` : "—";
        const lastUpload = reconMeta?.last_upload ? new Date(reconMeta.last_upload).toLocaleString() : "Never";
        const rows = [
          ["Firebase", "davismarginiq"],
          ["Netlify", "davis-marginiq.netlify.app"],
          ["Version", APP_VERSION],
          ["Weekly Rollups", String(weeklyRollups.length)],
          ["DDIS Files Loaded", String(ddisCount)],
          ["DDIS Coverage", ddisWeeks],
        ];
        if (ddisAmbig > 0) rows.push(["DDIS Needs Review", `${ddisAmbig} file${ddisAmbig===1?"":"s"}`]);
        rows.push(
          ["DDIS Total Paid", ddisPaid],
          ["DDIS Date Range", ddisRange],
          ["DDIS Payment Rows", ddisRows],
          ["Last DDIS Upload", lastUpload],
        );
        return rows.map(([l, v]) => <DataRow key={l} label={l} value={v} />);
      })()}
    </div>

    {/* DAILY BACKUPS */}
    <div style={cardStyle}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10,flexWrap:"wrap",gap:8}}>
        <div style={{fontSize:13,fontWeight:700}}>💾 Daily Backups</div>
        <div style={{fontSize:10,color:T.textMuted}}>Auto-runs 3AM EST · kept 30 days + monthly archives</div>
      </div>
      <div style={{fontSize:11,color:T.textMuted,marginBottom:12,lineHeight:1.5}}>
        Every night at 3AM EST, MarginIQ snapshots every Firestore collection to the <code>backups</code> collection as gzipped chunks. If an ingest goes sideways or data looks wrong, you can restore to any recent daily snapshot. Older snapshots (beyond 30 days) are pruned down to one per month for long-term history.
      </div>

      <div style={{display:"flex",gap:8,marginBottom:12,flexWrap:"wrap",alignItems:"center"}}>
        <button
          onClick={runBackupNow}
          disabled={backupAction === "run"}
          style={{
            padding:"9px 16px",borderRadius:8,border:"none",
            background: backupAction === "run" ? T.bgSurface : T.brand,
            color: backupAction === "run" ? T.textMuted : "#fff",
            fontSize:12,fontWeight:700,
            cursor: backupAction === "run" ? "wait" : "pointer",
          }}
        >
          {backupAction === "run" ? "Backing up…" : "💾 Backup Now"}
        </button>
        <span style={{fontSize:10,color:T.textMuted}}>No auth needed — backup is read-only. Restore requires admin token.</span>
      </div>

      {backupMsg?.ok && (
        <div style={{padding:"8px 12px",background:T.greenBg,borderRadius:6,border:`1px solid ${T.green}40`,fontSize:11,color:T.greenText,marginBottom:10}}>
          {backupMsg.text}
        </div>
      )}
      {backupMsg?.error && (
        <div style={{padding:"8px 12px",background:T.redBg,borderRadius:6,border:`1px solid ${T.red}40`,fontSize:11,color:T.redText,marginBottom:10}}>
          ✗ {backupMsg.error}
        </div>
      )}

      <div style={{fontSize:11,fontWeight:700,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:6}}>
        Available Snapshots
      </div>
      {backupsLoading ? (
        <div style={{fontSize:12,color:T.textMuted,padding:"8px 0"}}>Loading…</div>
      ) : backups.length === 0 ? (
        <div style={{padding:"12px",background:T.bgSurface,borderRadius:6,fontSize:12,color:T.textMuted,textAlign:"center"}}>
          No backups yet. Click "Backup Now" to create the first snapshot, or wait until 3AM EST for the automatic run.
        </div>
      ) : (
        <div style={{maxHeight:300,overflowY:"auto",border:`1px solid ${T.borderLight}`,borderRadius:8}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead>
              <tr style={{background:T.bgSurface}}>
                <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textMuted,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>Date</th>
                <th style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textMuted,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>Taken</th>
                <th style={{textAlign:"right",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textMuted,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>Docs</th>
                <th style={{textAlign:"right",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textMuted,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>Size</th>
                <th style={{padding:"8px 10px",borderBottom:`1px solid ${T.border}`}}></th>
              </tr>
            </thead>
            <tbody>
              {backups.map(b => {
                const isToday = b.date === new Date().toISOString().slice(0,10);
                return (
                  <tr key={b.date}>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontWeight:600,color:isToday?T.brand:T.text}}>
                      {b.date}{isToday && <span style={{marginLeft:6,fontSize:9,color:T.brand,fontWeight:700}}>TODAY</span>}
                    </td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted}}>
                      {b.taken_at ? new Date(b.taken_at).toLocaleString("en-US",{hour:"numeric",minute:"2-digit",month:"short",day:"numeric"}) : "—"}
                    </td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,textAlign:"right",fontWeight:600}}>
                      {fmtNum(b.total_docs || 0)}
                    </td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,textAlign:"right",color:T.textMuted,fontSize:10}}>
                      {b.compressed_bytes ? `${Math.round(b.compressed_bytes/1024)} KB` : "—"}
                    </td>
                    <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,display:"flex",gap:6,justifyContent:"flex-end"}}>
                      <button
                        onClick={() => { setRestoreDate(b.date); setRestoreConfirm(""); setBackupMsg(null); }}
                        style={{padding:"4px 10px",fontSize:10,fontWeight:700,borderRadius:6,border:`1px solid ${T.yellow}`,background:"#fef9c3",color:"#78350f",cursor:"pointer"}}
                      >↻ Restore</button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {restoreDate && (
        <div style={{marginTop:12,padding:"12px 14px",background:"#fef9c3",border:`2px solid ${T.yellow}`,borderRadius:8}}>
          <div style={{fontSize:13,fontWeight:700,color:"#78350f",marginBottom:8}}>
            ⚠ Restore Snapshot from {restoreDate}?
          </div>
          <div style={{fontSize:11,color:T.text,lineHeight:1.5,marginBottom:12}}>
            This will <strong>overwrite every Firestore collection</strong> with the state from {restoreDate}. Any docs added or changed since that backup will be reverted. Docs that didn't exist in the backup will be <strong>deleted</strong>. This is a full system rollback — use only if you're sure you want to undo everything since the backup date.
          </div>
          <div style={{marginBottom:10}}>
            <label style={{fontSize:10,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600,display:"block",marginBottom:4}}>
              Type <code style={{background:"#fde68a",padding:"1px 5px",borderRadius:3}}>RESTORE {restoreDate}</code> to confirm
            </label>
            <input
              type="text"
              value={restoreConfirm}
              onChange={e => setRestoreConfirm(e.target.value)}
              placeholder={`RESTORE ${restoreDate}`}
              style={inputStyle}
            />
          </div>
          <div style={{marginBottom:10}}>
            <label style={{fontSize:10,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600,display:"block",marginBottom:4}}>
              Admin token (MARGINIQ_ADMIN_TOKEN)
            </label>
            <input
              type="password"
              value={adminToken}
              onChange={e => setAdminToken(e.target.value)}
              placeholder="required for restore"
              autoCapitalize="off"
              autoCorrect="off"
              autoComplete="off"
              spellCheck={false}
              style={inputStyle}
            />
          </div>
          <div style={{display:"flex",gap:8}}>
            <button
              onClick={confirmRestore}
              disabled={backupAction === "restore" || restoreConfirm !== `RESTORE ${restoreDate}` || !adminToken.trim()}
              style={{
                padding:"9px 16px",borderRadius:8,border:"none",
                background: (backupAction === "restore" || restoreConfirm !== `RESTORE ${restoreDate}` || !adminToken.trim()) ? T.bgSurface : T.red,
                color: (backupAction === "restore" || restoreConfirm !== `RESTORE ${restoreDate}` || !adminToken.trim()) ? T.textMuted : "#fff",
                fontSize:12,fontWeight:700,
                cursor: (backupAction === "restore" || restoreConfirm !== `RESTORE ${restoreDate}` || !adminToken.trim()) ? "not-allowed" : "pointer",
              }}
            >
              {backupAction === "restore" ? "Restoring…" : `↻ Yes, Restore from ${restoreDate}`}
            </button>
            <button
              onClick={() => { setRestoreDate(null); setRestoreConfirm(""); }}
              style={{padding:"9px 16px",borderRadius:8,border:`1px solid ${T.border}`,background:"transparent",color:T.text,fontSize:12,fontWeight:600,cursor:"pointer"}}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>

    {/* DANGER ZONE: Purge Uline data */}
    <div style={{...cardStyle, borderColor: T.red, borderWidth: 2, background: "#fef2f2"}}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:8,color:T.redText}}>⚠️ Danger Zone — Purge Uline Data</div>
      <div style={{fontSize:12,color:T.text,marginBottom:12,lineHeight:1.5}}>
        Wipes <strong>uline_weekly</strong>, <strong>recon_weekly</strong>, <strong>unpaid_stops</strong>, <strong>audit_items</strong>, <strong>source_conflicts</strong>, and all Uline-kind entries in <strong>file_log</strong> + <strong>source_files</strong>.
        <br/><br/>
        <strong style={{color:T.greenText}}>Preserved:</strong> DDIS payments, NuVizz, Time Clock, Payroll, QBO, Fuel, Driver Classifications, AP Contacts, Disputes, Gmail connection.
        <br/><br/>
        Use this when Uline rollup data is corrupted (e.g. accessorial-only weeks merged into real weeks) and you want to re-ingest clean. You'll need to re-run ingestion from Gmail Sync or the Data Ingest tab after.
      </div>

      {purgeResult?.ok && (
        <div style={{padding:"10px 12px",background:"#ecfdf5",border:`1px solid ${T.green}`,borderRadius:6,marginBottom:10,fontSize:12,color:"#065f46"}}>
          <div style={{fontWeight:700,marginBottom:6}}>✓ Purge complete</div>
          {Object.entries(purgeResult.results||{}).map(([coll, r]) => (
            <div key={coll} style={{fontFamily:"monospace",fontSize:11}}>{coll}: {r.deleted}/{r.found} deleted</div>
          ))}
        </div>
      )}
      {purgeResult?.error && (
        <div style={{padding:"10px 12px",background:T.redBg,border:`1px solid ${T.red}`,borderRadius:6,marginBottom:10,fontSize:12,color:T.redText}}>
          ✗ {purgeResult.error}
        </div>
      )}

      <div style={{display:"flex",flexDirection:"column",gap:8,marginTop:12}}>
        <label style={{fontSize:11,fontWeight:600,color:T.text}}>
          Admin token (MARGINIQ_ADMIN_TOKEN)
          <input type="password" value={purgeToken} onChange={e=>setPurgeToken(e.target.value)} placeholder="enter admin token"
            autoCapitalize="off"
            autoCorrect="off"
            autoComplete="off"
            spellCheck={false}
            style={{display:"block",marginTop:4,padding:"8px 10px",border:`1px solid ${T.border}`,borderRadius:6,fontSize:12,width:"100%",maxWidth:300}} />
        </label>
        <label style={{fontSize:11,fontWeight:600,color:T.text}}>
          Type <code style={{background:"#fff",padding:"2px 6px",borderRadius:3,color:T.redText,fontWeight:700}}>PURGE ULINE</code> to confirm:
          <input type="text" value={purgeConfirmText} onChange={e=>setPurgeConfirmText(e.target.value)} placeholder="PURGE ULINE"
            style={{display:"block",marginTop:4,padding:"8px 10px",border:`1px solid ${T.border}`,borderRadius:6,fontSize:12,width:"100%",maxWidth:300}} />
        </label>
        <button onClick={handlePurge}
          disabled={purging || purgeConfirmText !== "PURGE ULINE" || !purgeToken.trim()}
          style={{
            padding:"10px 16px",borderRadius:8,border:"none",fontSize:13,fontWeight:700,
            background: (purging || purgeConfirmText !== "PURGE ULINE" || !purgeToken.trim()) ? T.bgSurface : T.red,
            color: (purging || purgeConfirmText !== "PURGE ULINE" || !purgeToken.trim()) ? T.textDim : "#fff",
            cursor: purging ? "wait" : (purgeConfirmText === "PURGE ULINE" && purgeToken.trim() ? "pointer" : "not-allowed"),
            maxWidth:300,
          }}>
          {purging ? "Purging..." : "🗑️ Purge Uline Data"}
        </button>
      </div>
    </div>
  </div>;
}

// ═══ ULINE ↔ NUVIZZ WEEKLY RECONCILIATION ═══════════════════
// Cross-check Uline billed stops vs NuVizz delivered stops for each
// Friday-ending week in 2025. Designed to surface:
//   - Weeks where NuVizz shows deliveries but Uline didn't bill (lost revenue)
//   - Weeks where Uline billed but NuVizz has no delivery record (driver log gap)
//   - Gross count mismatches in either direction (suggests data quality issue)
//
// Starts with a weekly count comparison using only the *_weekly rollup docs
// (fast, works with existing data). Stop-level PRO matching ("which specific
// PROs are missing") can be added later if nuvizz_stops is fully populated.
function UlineNuVizzRecon({ weeklyRollups }) {
  const [nvWeekly, setNvWeekly] = useState([]);
  const [loading, setLoading] = useState(true);
  const [detailWeek, setDetailWeek] = useState(null); // expanded row
  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    (async () => {
      if (!hasFirebase) { setLoading(false); return; }
      try {
        const nv = await FS.getNuVizzWeekly();
        setNvWeekly(nv);
      } catch(e) { console.error("nvWeekly err:", e); }
      setLoading(false);
    })();
  }, []);

  // Fetch stop-level PRO lists for a specific week when user expands it
  const loadDetail = async (weekEnding) => {
    setDetailWeek(weekEnding);
    setDetail(null);
    setDetailLoading(true);
    try {
      if (!hasFirebase) return;
      // Pull NuVizz stops for this week
      const nvSnap = await window.db.collection("nuvizz_stops")
        .where("week_ending", "==", weekEnding).limit(5000).get();
      const nvStops = nvSnap.docs.map(d => d.data()).filter(s => s.pro);
      const nvProSet = new Set(nvStops.map(s => String(s.pro)));

      // Pull Uline audit_items + unpaid_stops for this week (they each store PRO)
      // Note: Uline stop-level data isn't stored as a flat collection — we have
      // audit_items (variance-flagged stops) and unpaid_stops. For full PRO list
      // we'd need to re-parse, but these two cover most actionable cases.
      const audSnap = await window.db.collection("audit_items")
        .where("week_ending", "==", weekEnding).limit(5000).get();
      const upSnap = await window.db.collection("unpaid_stops")
        .where("week_ending", "==", weekEnding).limit(5000).get();
      const ulineAudit = audSnap.docs.map(d => d.data());
      const ulineUnpaid = upSnap.docs.map(d => d.data());
      const ulineProSet = new Set([
        ...ulineAudit.map(s => String(s.pro)),
        ...ulineUnpaid.map(s => String(s.pro)),
      ]);

      // Compare
      const onlyNv = [];
      const onlyUline = [];
      const matched = [];
      for (const p of nvProSet) {
        if (ulineProSet.has(p)) matched.push(p);
        else onlyNv.push(p);
      }
      for (const p of ulineProSet) {
        if (!nvProSet.has(p)) onlyUline.push(p);
      }

      setDetail({
        nv_count: nvProSet.size,
        uline_count: ulineProSet.size,
        matched: matched.length,
        only_nv: onlyNv.slice(0, 100),
        only_uline: onlyUline.slice(0, 100),
        only_nv_count: onlyNv.length,
        only_uline_count: onlyUline.length,
        note: ulineProSet.size < 500 ? "⚠️ Uline PRO coverage is limited — only audit_items and unpaid_stops are in Firestore. For full PRO-level matching, the app would need to store every Uline stop individually (not currently done)." : null,
      });
    } catch(e) {
      console.error("loadDetail err:", e);
      setDetail({ error: e.message });
    }
    setDetailLoading(false);
  };

  // Build weekly comparison BEFORE the early return so `useSortable` (a hook)
  // is called on every render — required by the Rules of Hooks. When loading
  // is true, nvWeekly is [] and weeklyRollups may also be [], producing an
  // empty rows array — but useSortable still runs, keeping hook order stable
  // across the loading→loaded transition.
  const ulineByWeek = {};
  for (const w of (weeklyRollups || [])) ulineByWeek[w.week_ending] = w;
  const nvByWeek = {};
  for (const w of nvWeekly) nvByWeek[w.week_ending] = w;

  // Union of both week sets, filtered to 2025+
  const allWeeks = new Set([
    ...Object.keys(ulineByWeek).filter(w => w >= "2025-01-01"),
    ...Object.keys(nvByWeek).filter(w => w >= "2025-01-01"),
  ]);
  const rows = Array.from(allWeeks).sort().map(we => {
    const u = ulineByWeek[we];
    const n = nvByWeek[we];
    const ulineStops = u ? (u.delivery_stops ?? u.stops ?? 0) : 0;
    const ulineRev = u ? (u.delivery_revenue ?? u.revenue ?? 0) : 0;
    const nvStops = n ? (n.stops_effective ?? n.stops_total ?? 0) : 0;
    const nvTotal = n ? (n.stops_total ?? 0) : 0;
    const delta = ulineStops - nvStops;
    const absDelta = Math.abs(delta);
    const pctDelta = nvStops > 0 ? (delta / nvStops * 100) : (ulineStops > 0 ? 100 : 0);
    let status, hint;
    if (!u && !n) { status = "none"; hint = "No data either side"; }
    else if (!u) { status = "uline_missing"; hint = "NuVizz ran but no Uline billing"; }
    else if (!n) { status = "nv_missing"; hint = "Uline billed but no NuVizz record"; }
    else if (absDelta <= 5) { status = "match"; hint = "✓ In agreement"; }
    else if (absDelta <= 30) { status = "close"; hint = `${delta > 0 ? "+" : ""}${delta} diff`; }
    else { status = "diverge"; hint = `${delta > 0 ? "Uline higher" : "NuVizz higher"} by ${absDelta}`; }
    return { we, u, n, ulineStops, ulineRev, nvStops, nvTotal, delta, pctDelta, status, hint };
  });

  // Sortable weekly reconciliation rows — MUST be called before any early return
  const reconSort = useSortable(rows, "we", "desc", {
    we: (r) => r.we || "",
    ulineStops: (r) => r.u ? Number(r.ulineStops || 0) : -1,
    nvStops: (r) => r.n ? Number(r.nvStops || 0) : -1,
    nvTotal: (r) => r.n ? Number(r.nvTotal || 0) : -1,
    delta: (r) => (r.u && r.n) ? r.delta : null,
    status: (r) => r.status || "",
  });

  if (loading) return <div style={{padding:40,textAlign:"center",color:T.textMuted}}>Loading reconciliation data…</div>;

  // Summary metrics
  const totals = rows.reduce((acc, r) => {
    acc.uline_total += r.ulineStops;
    acc.nv_total += r.nvStops;
    acc.weeks_match += r.status === "match" ? 1 : 0;
    acc.weeks_close += r.status === "close" ? 1 : 0;
    acc.weeks_diverge += r.status === "diverge" ? 1 : 0;
    acc.weeks_uline_missing += r.status === "uline_missing" ? 1 : 0;
    acc.weeks_nv_missing += r.status === "nv_missing" ? 1 : 0;
    return acc;
  }, { uline_total: 0, nv_total: 0, weeks_match: 0, weeks_close: 0, weeks_diverge: 0, weeks_uline_missing: 0, weeks_nv_missing: 0 });
  const overallDelta = totals.uline_total - totals.nv_total;

  const statusColor = (s) => ({
    match: T.green,
    close: T.yellow,
    diverge: T.red,
    uline_missing: T.red,
    nv_missing: "#8b5cf6",
    none: T.textDim,
  })[s] || T.textDim;
  const statusBg = (s) => ({
    match: "#ecfdf5",
    close: "#fef9c3",
    diverge: "#fef2f2",
    uline_missing: "#fef2f2",
    nv_missing: "#f5f3ff",
    none: T.bgSurface,
  })[s] || T.bgSurface;

  return <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="🔄" text="Uline ↔ NuVizz Reconciliation" />

    {/* Summary KPIs */}
    <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:10,marginBottom:16}}>
      <KPI label="Uline stops" value={fmtNum(totals.uline_total)} sub={`${rows.filter(r=>r.u).length} weeks with data`} />
      <KPI label="NuVizz stops" value={fmtNum(totals.nv_total)} sub={`${rows.filter(r=>r.n).length} weeks with data`} />
      <KPI label="Net delta" value={(overallDelta >= 0 ? "+" : "") + fmtNum(overallDelta)}
        subColor={Math.abs(overallDelta) < 200 ? T.green : Math.abs(overallDelta) < 1000 ? T.yellow : T.red}
        sub={overallDelta > 0 ? "Uline has more" : overallDelta < 0 ? "NuVizz has more" : "Match"} />
      <KPI label="Weeks in agreement" value={fmtNum(totals.weeks_match)} subColor={T.green} sub={`${rows.length} total weeks`} />
      <KPI label="Weeks diverging" value={fmtNum(totals.weeks_diverge + totals.weeks_uline_missing + totals.weeks_nv_missing)} subColor={T.red} sub="Worth investigating" />
    </div>

    {/* Legend */}
    <div style={{fontSize:10,color:T.textDim,marginBottom:8,display:"flex",gap:12,flexWrap:"wrap"}}>
      <span><span style={{display:"inline-block",width:10,height:10,borderRadius:2,background:T.green,marginRight:4}}></span>Match (±5)</span>
      <span><span style={{display:"inline-block",width:10,height:10,borderRadius:2,background:T.yellow,marginRight:4}}></span>Close (±6-30)</span>
      <span><span style={{display:"inline-block",width:10,height:10,borderRadius:2,background:T.red,marginRight:4}}></span>Diverge (&gt;30)</span>
      <span><span style={{display:"inline-block",width:10,height:10,borderRadius:2,background:"#8b5cf6",marginRight:4}}></span>NuVizz missing</span>
    </div>

    {/* Weekly table */}
    <div style={cardStyle}>
      <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>Week-by-Week Comparison ({rows.length} weeks)</div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
          <thead><tr>
            <SortableTh label="Week Ending" col="we" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <SortableTh label="Uline Stops" col="ulineStops" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <SortableTh label="NuVizz Effective" col="nvStops" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <SortableTh label="NuVizz Total" col="nvTotal" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <SortableTh label="Delta" col="delta" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <SortableTh label="Status" col="status" sortKey={reconSort.sortKey} sortDir={reconSort.sortDir} onSort={reconSort.toggleSort} />
            <th style={{padding:"8px 10px",borderBottom:`1px solid ${T.border}`,background:T.bgSurface,position:"sticky",top:0,zIndex:1}}></th>
          </tr></thead>
          <tbody>
            {reconSort.sorted.map((r, i) => {
              const isOpen = detailWeek === r.we;
              return (
                <React.Fragment key={r.we}>
                  <tr style={{background: isOpen ? T.yellowBg : "transparent"}}>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>WE {weekLabel(r.we)}</td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{r.u ? fmtNum(r.ulineStops) : <span style={{color:T.textDim,fontStyle:"italic"}}>—</span>}</td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{r.n ? fmtNum(r.nvStops) : <span style={{color:T.textDim,fontStyle:"italic"}}>—</span>}</td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted}}>{r.n ? fmtNum(r.nvTotal) : "—"}</td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,color:statusColor(r.status),fontWeight:600}}>
                      {r.u && r.n ? `${r.delta > 0 ? "+" : ""}${fmtNum(r.delta)}` : "—"}
                    </td>
                    <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>
                      <span style={{fontSize:10,fontWeight:700,padding:"3px 8px",borderRadius:4,background:statusBg(r.status),color:statusColor(r.status)}}>{r.hint}</span>
                    </td>
                    <td style={{padding:"4px 6px",borderBottom:`1px solid ${T.borderLight}`}}>
                      <button type="button" onClick={() => isOpen ? setDetailWeek(null) : loadDetail(r.we)}
                        style={{background:isOpen?T.yellow:T.brand,color:"#fff",border:"none",padding:"4px 10px",borderRadius:6,fontSize:10,fontWeight:700,cursor:"pointer"}}>
                        {isOpen?"▼ Close":"🔬 Details"}
                      </button>
                    </td>
                  </tr>
                  {isOpen && (
                    <tr>
                      <td colSpan={7} style={{padding:"10px 12px",background:T.bgSurface,borderBottom:`1px solid ${T.borderLight}`}}>
                        {detailLoading && <div style={{fontSize:12,color:T.textMuted}}>Loading PRO-level match…</div>}
                        {detail?.error && <div style={{fontSize:12,color:T.redText}}>✗ {detail.error}</div>}
                        {detail && !detail.error && !detailLoading && (
                          <div style={{display:"flex",flexDirection:"column",gap:10}}>
                            {detail.note && <div style={{fontSize:11,color:T.yellowText,background:T.yellowBg,padding:"6px 10px",borderRadius:6}}>{detail.note}</div>}
                            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:8}}>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid ${T.borderLight}`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>Matched PROs</div>
                                <div style={{fontSize:16,fontWeight:700,color:T.green}}>{fmtNum(detail.matched)}</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid ${T.red}40`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>NuVizz only</div>
                                <div style={{fontSize:16,fontWeight:700,color:T.red}}>{fmtNum(detail.only_nv_count)}</div>
                                <div style={{fontSize:9,color:T.textMuted}}>delivered but not billed?</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"white",borderRadius:6,border:`1px solid #8b5cf640`}}>
                                <div style={{fontSize:10,color:T.textDim,textTransform:"uppercase"}}>Uline only</div>
                                <div style={{fontSize:16,fontWeight:700,color:"#8b5cf6"}}>{fmtNum(detail.only_uline_count)}</div>
                                <div style={{fontSize:9,color:T.textMuted}}>billed but no driver record?</div>
                              </div>
                            </div>
                            {detail.only_nv.length > 0 && (
                              <details>
                                <summary style={{fontSize:11,fontWeight:600,cursor:"pointer",color:T.redText}}>Sample NuVizz-only PROs ({detail.only_nv.length} shown)</summary>
                                <div style={{fontSize:10,fontFamily:"monospace",color:T.textMuted,padding:"6px 0",lineHeight:1.6}}>
                                  {detail.only_nv.join(", ")}
                                </div>
                              </details>
                            )}
                            {detail.only_uline.length > 0 && (
                              <details>
                                <summary style={{fontSize:11,fontWeight:600,cursor:"pointer",color:"#6d28d9"}}>Sample Uline-only PROs ({detail.only_uline.length} shown)</summary>
                                <div style={{fontSize:10,fontFamily:"monospace",color:T.textMuted,padding:"6px 0",lineHeight:1.6}}>
                                  {detail.only_uline.join(", ")}
                                </div>
                              </details>
                            )}
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>

    {/* Help panel */}
    <div style={{...cardStyle, background:"#eff6ff", borderColor:"#3b82f640"}}>
      <div style={{fontSize:12,fontWeight:700,marginBottom:8,color:"#1e40af"}}>📖 How to read this</div>
      <div style={{fontSize:11,color:T.text,lineHeight:1.6}}>
        <p style={{margin:"0 0 8px"}}><strong>NuVizz effective stops</strong> = stops marked "Completed" or "Manually Completed" in NuVizz. That's the apples-to-apples comparison to Uline's billed stops.</p>
        <p style={{margin:"0 0 8px"}}><strong>Red "Diverge"</strong> weeks are the real concern — you either billed for stops that weren't delivered (invoice problem) or delivered stops that didn't get billed (lost revenue).</p>
        <p style={{margin:0}}><strong>PRO-level matching in Details</strong> uses stops stored in Firestore — which only includes audit_items and unpaid_stops, so count mismatches here are normal. For true 1:1 PRO matching, NuVizz ingestion needs to run with the full stop set saved.</p>
      </div>
    </div>
  </div>;
}

// ═══ RUN SHEET GENERATOR ══════════════════════════════════
// Takes a NuVizz stop export (xlsx) and generates a printable Davis Delivery
// run sheet PDF for a specific driver. Ports the standalone Flask/reportlab
// app (davis_runsheet.zip) to a native MarginIQ tab — client-side rendering
// via jsPDF, no server round-trip.
//
// The NuVizz export has Ship To Name + Stop SealNbr per stop (plus Delivery
// End date). Driver pay = 40% of summed SealNbr (the standard 1099 contractor
// rate). The PDF layout mirrors the reportlab template: brand blue header,
// driver share callout, stop table with alternating row shading, and a
// totals block at the bottom.
function RunSheet() {
  const [driver, setDriver] = useState("");
  const [shipper, setShipper] = useState("ULINE");
  const [file, setFile] = useState(null);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState(null);
  const [lastGenerated, setLastGenerated] = useState(null);
  const [dragOver, setDragOver] = useState(false);
  const fileRef = useRef(null);

  // Convenience: let user pick a driver they've seen in past NuVizz ingestion
  // (reads from nuvizz_weekly rollups which keep a driver list per week).
  // Populates the driver dropdown with unique names seen recently.
  const [knownDrivers, setKnownDrivers] = useState([]);
  useEffect(() => {
    (async () => {
      if (!hasFirebase) return;
      try {
        const nvW = await FS.getNuVizzWeekly();
        const names = new Set();
        for (const w of nvW) {
          if (w.drivers) {
            for (const d of Object.keys(w.drivers)) names.add(d);
          }
        }
        setKnownDrivers(Array.from(names).sort());
      } catch(e) { /* non-fatal */ }
    })();
  }, []);

  const handleFileChange = (f) => {
    if (!f) return;
    if (!/\.(xlsx?|csv)$/i.test(f.name)) {
      setError(`Unsupported file type: ${f.name}. Upload an .xlsx / .xls / .csv.`);
      return;
    }
    setFile(f);
    setError(null);
  };

  const handleGenerate = async () => {
    setError(null);
    if (!driver.trim()) { setError("Driver name is required."); return; }
    if (!file) { setError("Upload a NuVizz stop export first."); return; }
    setGenerating(true);
    try {
      // Read + parse the NuVizz export. readWorkbook handles meta-hint row
      // skipping automatically (same logic used for Uline files).
      const rows = await readWorkbook(file);
      if (!rows || rows.length === 0) {
        throw new Error("File is empty or couldn't be parsed.");
      }

      // Filter to just this driver's stops. NuVizz column "driver name" is
      // normalized upstream; match case-insensitively with whitespace tolerance.
      const driverNorm = driver.trim().toLowerCase().replace(/\s+/g, " ");
      const driverRows = rows.filter(r => {
        const n = String(r["driver name"] || "").trim().toLowerCase().replace(/\s+/g, " ");
        return n === driverNorm;
      });
      if (driverRows.length === 0) {
        // If no exact match, fall back to contains — helps when the file has
        // "FRANK OKINE" and user typed "Frank Okine" but also "Okine, Frank"
        const loose = rows.filter(r => {
          const n = String(r["driver name"] || "").toLowerCase();
          return n.includes(driverNorm) || driverNorm.split(" ").every(p => p && n.includes(p));
        });
        if (loose.length === 0) {
          const available = Array.from(new Set(rows.map(r => r["driver name"]).filter(Boolean))).slice(0, 10);
          throw new Error(`No stops found for driver "${driver}". Drivers in file: ${available.join(", ") || "(none)"}`);
        }
        driverRows.push(...loose);
      }

      // Build the stop list — name + amount. Skip blank names.
      const stops = [];
      for (const r of driverRows) {
        const name = String(r["ship to name"] || "").trim();
        if (!name || name.toLowerCase() === "nan") continue;
        const amt = parseMoney(r["stop sealnbr"]) || 0;
        stops.push({ name, amt });
      }
      if (stops.length === 0) {
        throw new Error(`Driver "${driver}" has no valid stops in this file.`);
      }

      // Pick a representative delivery-end date for the sheet header
      let dateStr = "";
      for (const r of driverRows) {
        if (r["delivery end"]) {
          const iso = parseDateMDYFlexible(r["delivery end"]);
          if (iso) {
            const [y,m,d] = iso.split("-");
            dateStr = `${m}/${d}/${y.slice(2)}`;
            break;
          }
        }
      }
      if (!dateStr) {
        const today = new Date();
        dateStr = `${String(today.getMonth()+1).padStart(2,"0")}/${String(today.getDate()).padStart(2,"0")}/${String(today.getFullYear()).slice(2)}`;
      }

      const total = stops.reduce((s, r) => s + r.amt, 0);
      const share = total * 0.40;

      // Generate the PDF — layout matches the Flask/reportlab template 1:1
      // but rendered via jsPDF. Note: jsPDF's origin is top-left (Y increases
      // downward), so coordinates are simpler than reportlab's bottom-left.
      const jsPDFCtor = await loadJsPdf();
      const doc = new jsPDFCtor({ unit: "pt", format: "letter" });
      const W = 612, H = 792, L = 36, R = W - 36, T = 36;
      const brandRgb = [30, 91, 146];
      const textRgb = [20, 20, 20];

      // Header band
      doc.setFillColor(...brandRgb);
      doc.rect(0, T, W, 50, "F");
      doc.setTextColor(255, 255, 255);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(20);
      doc.text("DAVIS", W/2, T + 26, { align: "center" });
      doc.setFont("helvetica", "normal");
      doc.setFontSize(9);
      doc.text("DELIVERY SERVICE", W/2, T + 39, { align: "center" });

      // Driver share callout (light gray panel on right)
      doc.setFillColor(237, 237, 237);
      doc.rect(R - 145, T, 145, 50, "F");
      doc.setTextColor(...brandRgb);
      doc.setFont("helvetica", "normal");
      doc.setFontSize(8);
      doc.text("Driver Share: $", R - 140, T + 18);
      doc.setTextColor(...textRgb);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(13);
      doc.text(`${share.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}`, R - 6, T + 42, { align: "right" });

      // Info row: Name / Date
      const infoY = T + 72;
      doc.setTextColor(...textRgb);
      doc.setFont("helvetica", "normal");
      doc.setFontSize(10);
      doc.text("Name:", L, infoY);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(12);
      doc.text(driver, L + 40, infoY);
      doc.setFont("helvetica", "normal");
      doc.setFontSize(10);
      doc.text("Date:", W/2 - 20, infoY);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(12);
      doc.text(dateStr, W/2 + 10, infoY);
      doc.setDrawColor(0, 0, 0);
      doc.setLineWidth(0.5);
      doc.line(L + 38, infoY + 3, L + 200, infoY + 3);
      doc.line(W/2 + 8, infoY + 3, W/2 + 160, infoY + 3);

      // Table header
      const tableY = infoY + 24;
      const colH = 16;
      doc.setFillColor(220, 220, 220);
      doc.rect(L, tableY, R - L, colH, "F");
      doc.setDrawColor(150, 150, 150);
      doc.setLineWidth(0.5);
      doc.rect(L, tableY, R - L, colH);
      const cx = { num:L+2, ship:L+24, cons:L+92, skids:L+288, del:L+328, det:L+368, amt:R-5 };
      doc.setTextColor(...textRgb);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(8);
      doc.text("#", cx.num, tableY + 11);
      doc.text("Shipper", cx.ship, tableY + 11);
      doc.text("Consignee", cx.cons, tableY + 11);
      doc.text("Skids/Weight", cx.skids, tableY + 11);
      doc.text("Del/Pu", cx.del, tableY + 11);
      doc.text("Detention/Inside Del", cx.det, tableY + 11);
      doc.text("Amount", cx.amt, tableY + 11, { align: "right" });

      // Table rows with alternating shading
      const rowH = 20;
      let rowY = tableY + colH;
      stops.forEach((s, i) => {
        if (i % 2 === 0) {
          doc.setFillColor(247, 247, 247);
          doc.rect(L, rowY, R - L, rowH, "F");
        }
        doc.setDrawColor(204, 204, 204);
        doc.setLineWidth(0.3);
        doc.line(L, rowY + rowH, R, rowY + rowH);
        for (const x of [L+22, L+90, L+286, L+326, L+366, R-70]) {
          doc.line(x, rowY, x, rowY + rowH);
        }
        doc.setTextColor(...textRgb);
        doc.setFont("helvetica", "normal");
        doc.setFontSize(9);
        doc.text(String(i + 1), cx.num, rowY + 14);
        doc.text(shipper || "", cx.ship, rowY + 14);
        const name = s.name.length > 40 ? s.name.slice(0, 40) + "…" : s.name;
        doc.text(name, cx.cons, rowY + 14);
        doc.text(`$${s.amt.toFixed(2)}`, cx.amt, rowY + 14, { align: "right" });
        rowY += rowH;
      });

      // Pad blank rows to always show 16 minimum (matches printed form height)
      const minRows = 16;
      const padRows = Math.max(0, minRows - stops.length);
      for (let i = 0; i < padRows; i++) {
        doc.setDrawColor(204, 204, 204);
        doc.setLineWidth(0.3);
        doc.line(L, rowY + rowH, R, rowY + rowH);
        for (const x of [L+22, L+90, L+286, L+326, L+366, R-70]) {
          doc.line(x, rowY, x, rowY + rowH);
        }
        rowY += rowH;
      }

      // Totals block
      rowY += 8;
      doc.setDrawColor(...brandRgb);
      doc.setLineWidth(1.2);
      doc.line(L, rowY, R, rowY);
      rowY += 18;
      doc.setFont("helvetica", "bold");
      doc.setFontSize(10);
      doc.setTextColor(...textRgb);
      doc.text("TOTAL:", R - 80, rowY, { align: "right" });
      doc.text(`$${total.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}`, R - 5, rowY, { align: "right" });
      rowY += 20;
      doc.setTextColor(...brandRgb);
      doc.setFont("helvetica", "bold");
      doc.setFontSize(10);
      doc.text("DRIVER SHARE (40%):", L + 4, rowY);
      doc.setFontSize(13);
      doc.text(`$${share.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}`, R - 5, rowY, { align: "right" });

      // Outer border around the main content area
      const topBorder = T + 54;
      doc.setDrawColor(...brandRgb);
      doc.setLineWidth(1.5);
      doc.rect(L - 4, topBorder, R - L + 8, (rowY + 10) - topBorder);

      // Footer
      doc.setFont("helvetica", "normal");
      doc.setFontSize(8);
      doc.setTextColor(153, 153, 153);
      doc.text("Davis Delivery Service Inc.  |  davisdelivery.com", W/2, H - 20, { align: "center" });

      const safeName = driver.replace(/\s+/g, "_") || "driver";
      const safeDate = dateStr.replace(/\//g, "-");
      const filename = `RunSheet_${safeName}_${safeDate}.pdf`;
      doc.save(filename);
      setLastGenerated({ filename, stops: stops.length, total, share, driver, dateStr });
    } catch(e) {
      console.error("RunSheet generate failed:", e);
      setError(e.message || String(e));
    }
    setGenerating(false);
  };

  return <div style={{padding:"16px",maxWidth:700,margin:"0 auto"}} className="fade-in">
    <SectionTitle icon="📋" text="Run Sheet Generator" />

    <div style={{fontSize:12,color:T.textMuted,marginBottom:16,lineHeight:1.5}}>
      Upload a NuVizz stop export and a printable driver run sheet PDF will be generated. Driver share auto-calculates at 40% of total SealNbr — the standard 1099 contractor rate.
    </div>

    <div style={cardStyle}>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:14}}>
        <div>
          <label style={{fontSize:10,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600,display:"block",marginBottom:5}}>Driver Name</label>
          {knownDrivers.length > 0 ? (
            <div style={{display:"flex",gap:4}}>
              <input
                type="text"
                value={driver}
                onChange={e => setDriver(e.target.value)}
                placeholder="e.g. Frank Okine"
                list="known-drivers"
                style={{...inputStyle, flex:1}}
              />
              <datalist id="known-drivers">
                {knownDrivers.map(d => <option key={d} value={d} />)}
              </datalist>
            </div>
          ) : (
            <input
              type="text"
              value={driver}
              onChange={e => setDriver(e.target.value)}
              placeholder="e.g. Frank Okine"
              style={inputStyle}
            />
          )}
          {knownDrivers.length > 0 && (
            <div style={{fontSize:10,color:T.textDim,marginTop:3}}>{knownDrivers.length} driver(s) from past NuVizz data auto-suggest as you type</div>
          )}
        </div>
        <div>
          <label style={{fontSize:10,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600,display:"block",marginBottom:5}}>Shipper</label>
          <input
            type="text"
            value={shipper}
            onChange={e => setShipper(e.target.value)}
            style={inputStyle}
          />
        </div>
      </div>

      <label style={{fontSize:10,color:T.textMuted,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600,display:"block",marginBottom:5}}>Stop Export File (XLS / XLSX / CSV)</label>
      <div
        onDragEnter={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
        onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
        onDragLeave={(e) => {
          e.preventDefault(); e.stopPropagation();
          if (e.currentTarget.contains(e.relatedTarget)) return;
          setDragOver(false);
        }}
        onDrop={(e) => {
          e.preventDefault(); e.stopPropagation();
          setDragOver(false);
          const f = e.dataTransfer.files?.[0];
          if (f) handleFileChange(f);
        }}
        onClick={() => !generating && fileRef.current?.click()}
        style={{
          border: `2px dashed ${dragOver ? T.brand : T.border}`,
          borderRadius: 10,
          padding: "26px 20px",
          textAlign: "center",
          cursor: generating ? "wait" : "pointer",
          background: dragOver ? "#dbeafe" : T.bgSurface,
          transition: "background 0.15s, border-color 0.15s",
        }}
      >
        <input
          ref={fileRef}
          type="file"
          accept=".xlsx,.xls,.csv"
          onChange={e => handleFileChange(e.target.files?.[0])}
          style={{display:"none"}}
        />
        <div style={{fontSize:28,marginBottom:6}}>{dragOver ? "⬇️" : "📂"}</div>
        <div style={{fontSize:14,color:T.text,marginBottom:3}}>
          {file ? file.name : (dragOver ? "Drop to upload" : "Click to browse or drag & drop")}
        </div>
        <div style={{fontSize:11,color:T.textDim}}>NuVizz .xls / .xlsx stop export</div>
      </div>

      <button
        onClick={handleGenerate}
        disabled={generating || !driver.trim() || !file}
        style={{
          width: "100%",
          marginTop: 16,
          padding: "12px",
          borderRadius: 10,
          border: "none",
          background: (generating || !driver.trim() || !file) ? "#94a3b8" : `linear-gradient(135deg,${T.brand},${T.brandLight})`,
          color: "#fff",
          fontSize: 14,
          fontWeight: 700,
          cursor: (generating || !driver.trim() || !file) ? "not-allowed" : "pointer",
        }}
      >
        {generating ? "Generating…" : "Generate & Download PDF"}
      </button>

      {error && (
        <div style={{marginTop:12,padding:"10px 14px",background:T.redBg,borderRadius:8,border:`1px solid ${T.red}40`,fontSize:12,color:T.redText}}>
          ✗ {error}
        </div>
      )}

      {lastGenerated && !error && (
        <div style={{marginTop:12,padding:"10px 14px",background:T.greenBg,borderRadius:8,border:`1px solid ${T.green}40`,fontSize:12,color:T.greenText}}>
          ✓ Generated <strong>{lastGenerated.filename}</strong> — {lastGenerated.stops} stops, ${lastGenerated.total.toFixed(2)} total, ${lastGenerated.share.toFixed(2)} driver share
        </div>
      )}
    </div>

    <div style={{...cardStyle, background:"#eff6ff", borderColor:"#3b82f640", marginTop:12}}>
      <div style={{fontSize:12,fontWeight:700,marginBottom:6,color:"#1e40af"}}>📖 How it works</div>
      <div style={{fontSize:11,color:T.text,lineHeight:1.6}}>
        <p style={{margin:"0 0 6px"}}>The NuVizz export is expected to contain one row per stop with columns <code>Ship To Name</code>, <code>Stop SealNbr</code>, <code>Driver Name</code>, and <code>Delivery End</code>. Stops with a blank Ship To are skipped.</p>
        <p style={{margin:"0 0 6px"}}>Driver share is computed as <strong>40% of the summed Stop SealNbr</strong>. This matches the business rule for 1099 contractors (W2 drivers are paid hourly and should not use this sheet).</p>
        <p style={{margin:0}}>If you've already ingested NuVizz data in MarginIQ, the driver name field will auto-suggest from the drivers the system has seen.</p>
      </div>
    </div>
  </div>;
}

// ═══ MAIN ═══════════════════════════════════════════════════
function MarginIQ() {
  // Allow URL to pre-select a tab (used by OAuth callbacks etc: ?tab=gmail)
  const initialTab = (() => {
    try {
      const t = new URLSearchParams(window.location.search).get("tab");
      if (t) return t;
    } catch {}
    return "command";
  })();
  const [tab, setTab] = useState(initialTab);

  // Expose setTab globally so DataHubTab.jsx (and other sister tabs) can navigate
  useEffect(() => {
    if (typeof window !== "undefined") {
      window.__marginiqSetTab = setTab;
    }
    return () => { if (typeof window !== "undefined") window.__marginiqSetTab = null; };
  }, []);

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
      FS.getWeeklyRollups(), FS.getReconWeekly(), FS.getReconMeta(), FS.getFileLog(500),
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

  // Data completeness scan. Starts from TRACKING_START (2025-01-01) so old
  // pre-MarginIQ data doesn't raise flags.
  const completeness = useMemo(() => {
    if (weeklyRollups.length === 0) return null;
    return scanCompleteness(weeklyRollups, null, TRACKING_START);
  }, [weeklyRollups]);

  const tabs = [
    { id:"command", icon:"🎯", label:"Command" },
    { id:"audited", icon:"📋", label:"Financials" },
    { id:"customers", icon:"📊", label:"Customers" },
    { id:"revenue", icon:"💰", label:"Uline Revenue" },
    { id:"recon", icon:"🧾", label:"Audit" },
    { id:"nvrecon", icon:"🔄", label:"Uline↔NuVizz" },
    { id:"employees", icon:"👥", label:"Employees" },
    { id:"drivers-perf", icon:"🏁", label:"Driver Perf" },
    { id:"timeclock", icon:"⏰", label:"Time Clock" },
    { id:"fuel", icon:"⛽", label:"Fuel" },
    { id:"completeness", icon:"✅", label:"Data Health" },
    { id:"datahub", icon:"🗄️", label:"Data Hub" },
    { id:"ingest", icon:"📤", label:"Data Ingest" },
    { id:"gmail", icon:"📧", label:"Gmail Sync" },
    { id:"runsheet", icon:"📋", label:"Run Sheet" },
    { id:"costs", icon:"⚙️", label:"Costs" },
    { id:"phone", icon:"📞", label:"Phone Calls" },
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
    {!loading && tab==="audited" && (typeof window !== "undefined" && window.AuditedFinancialsTab ? React.createElement(window.AuditedFinancialsTab) : <EmptyState icon="📋" title="Financials module not loaded" sub="AuditedFinancialsTab.jsx did not load. Check console." />)}
    {!loading && tab==="customers" && (typeof window !== "undefined" && window.CustomerAnalysisTab ? React.createElement(window.CustomerAnalysisTab) : <EmptyState icon="📊" title="Customer Analysis module not loaded" sub="CustomerAnalysisTab.jsx did not load. Check console." />)}
    {!loading && tab==="drivers-perf" && (typeof window !== "undefined" && window.DriverPerformanceTab ? React.createElement(window.DriverPerformanceTab) : <EmptyState icon="🏁" title="Driver Performance module not loaded" sub="DriverPerformanceTab.jsx did not load. Check console." />)}
    {!loading && tab==="revenue" && <UlineRevenue weeklyRollups={weeklyRollups} />}
    {!loading && tab==="recon" && <Audit reconWeekly={reconWeekly} weeklyRollups={weeklyRollups} />}
    {!loading && tab==="nvrecon" && <UlineNuVizzRecon weeklyRollups={weeklyRollups} />}
    {!loading && tab==="runsheet" && <RunSheet />}
    {!loading && tab==="employees" && <Drivers />}
    {!loading && tab==="timeclock" && (typeof window !== "undefined" && window.TimeClockTab ? React.createElement(window.TimeClockTab) : <EmptyState icon="⏰" title="Time Clock module not loaded" sub="TimeClockTab.jsx did not load. Check console." />)}
    {!loading && tab==="fuel" && <Fuel />}
    {!loading && tab==="completeness" && <DataCompleteness weeklyRollups={weeklyRollups} completeness={completeness} fileLog={fileLog} />}
    {!loading && tab==="datahub" && (typeof window !== "undefined" && window.DataHubTab ? React.createElement(window.DataHubTab) : <EmptyState icon="🗄️" title="Data Hub module not loaded" sub="DataHubTab.jsx did not load. Check console." />)}
    {!loading && tab==="ingest" && <DataIngest weeklyRollups={weeklyRollups} reconMeta={reconMeta} fileLog={fileLog} onRefresh={refreshData} />}
    {!loading && tab==="gmail" && <GmailSync onRefresh={refreshData} />}
    {!loading && tab==="costs" && <CostStructure costs={costs} onSave={setCosts} margins={margins} />}
    {!loading && tab==="phone" && <ZoomPhoneTab />}
    {!loading && tab==="settings" && <Settings qboConnected={qboConnected} motiveConnected={motiveConnected} reconMeta={reconMeta} weeklyRollups={weeklyRollups} onRefresh={refreshData} setTab={setTab} />}
  </div>;
}

// ─── ZoomPhoneTab v4 — Stable, mobile-first, sortable ───────────────────────
// Top-level helpers (stable refs across renders)
function _phoneFmtDur(s) {
  if (!s || isNaN(s)) return "—";
  const m = Math.floor(s/60), sec = s%60;
  return m + ":" + String(sec).padStart(2,"0");
}
function _phoneFmtDT(s) {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d)) return s;
  return d.toLocaleDateString("en-US",{month:"2-digit",day:"2-digit",year:"2-digit"})
    + " " + d.toLocaleTimeString("en-US",{hour:"2-digit",minute:"2-digit",hour12:true});
}
function _phoneFmtTime(s) {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d)) return s;
  return d.toLocaleTimeString("en-US",{hour:"2-digit",minute:"2-digit",hour12:true});
}
function _phoneInitials(name) {
  if (!name || typeof name !== "string") return "?";
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "?";
  return (parts[0][0] + (parts[1]?.[0] || "")).toUpperCase();
}
function _phoneRateColor(rate) {
  if (rate >= 80) return T.green;
  if (rate >= 50) return T.yellow;
  return T.red;
}
function _phonePeriodKey(date, groupBy) {
  const d = new Date(date);
  if (isNaN(d)) return null;
  if (groupBy === "day") {
    return d.toISOString().slice(0,10); // YYYY-MM-DD for stable sort
  }
  if (groupBy === "week") {
    const m = new Date(d);
    m.setDate(d.getDate() - ((d.getDay()+6)%7));
    return m.toISOString().slice(0,10);
  }
  return d.toISOString().slice(0,7); // YYYY-MM
}
function _phonePeriodLabel(key, groupBy) {
  if (!key) return "—";
  if (groupBy === "day") {
    const d = new Date(key+"T00:00:00");
    return d.toLocaleDateString("en-US",{month:"short",day:"numeric"});
  }
  if (groupBy === "week") {
    const d = new Date(key+"T00:00:00");
    return "Wk " + d.toLocaleDateString("en-US",{month:"short",day:"numeric"});
  }
  const [y,m] = key.split("-");
  return new Date(+y, +m-1, 1).toLocaleDateString("en-US",{month:"short",year:"numeric"});
}

// Stable child components (defined outside parent so they're not recreated)
function _PhoneStatPill({ label, value, sub, color }) {
  return (
    <div style={{
      background: T.bgCard, border: `1px solid ${T.border}`,
      borderRadius: T.radiusSm, padding: "10px 12px",
      minWidth: 0, boxShadow: T.shadow,
    }}>
      <div style={{
        fontSize: 9, fontWeight: 700, color: T.textMuted,
        textTransform: "uppercase", letterSpacing: "0.06em",
        marginBottom: 4, whiteSpace: "nowrap",
        overflow: "hidden", textOverflow: "ellipsis",
      }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 800, color: color || T.text, lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: T.textMuted, marginTop: 3, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{sub}</div>}
    </div>
  );
}

function _PhoneRateBar({ rate }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
      <div style={{ flex: "1 1 40px", maxWidth: 80, height: 5, background: T.borderLight, borderRadius: 3, minWidth: 24 }}>
        <div style={{ width: `${Math.max(0, Math.min(100, rate))}%`, height: "100%", background: _phoneRateColor(rate), borderRadius: 3, transition: "width 0.4s" }} />
      </div>
      <span style={{ fontSize: 11, color: T.textMuted, fontWeight: 600, width: 32, textAlign: "right" }}>{rate}%</span>
    </div>
  );
}

function _PhoneEmptyState({ icon, title, body }) {
  return (
    <div style={{ textAlign: "center", padding: "48px 20px", color: T.textMuted }}>
      <div style={{ fontSize: 36, marginBottom: 10, opacity: 0.6 }}>{icon}</div>
      <div style={{ fontWeight: 700, fontSize: 14, color: T.text, marginBottom: 4 }}>{title}</div>
      {body && <div style={{ fontSize: 12, maxWidth: 360, margin: "0 auto", lineHeight: 1.5 }}>{body}</div>}
    </div>
  );
}

function ZoomPhoneTab() {
  // ── State ──────────────────────────────────────────────────────────────
  const [status, setStatus]               = React.useState(null);
  const [activeTab, setActiveTab]         = React.useState("live");

  // Live state
  const [liveCalls, setLiveCalls]         = React.useState([]);
  const [recentCalls, setRecentCalls]     = React.useState([]);
  const [liveConnected, setLiveConnected] = React.useState(false);
  const [livePaused, setLivePaused]       = React.useState(false);
  const [lastEvent, setLastEvent]         = React.useState(null);

  // History state
  const today  = React.useMemo(() => new Date(), []);
  const day30  = React.useMemo(() => { const d=new Date(); d.setDate(d.getDate()-30); return d; }, []);
  const [histFrom, setHistFrom]       = React.useState(day30.toISOString().slice(0,10));
  const [histTo, setHistTo]           = React.useState(today.toISOString().slice(0,10));
  const [histGroupBy, setHistGroupBy] = React.useState("day");
  const [histEmp, setHistEmp]         = React.useState("");
  const [histDir, setHistDir]         = React.useState("");
  const [histCalls, setHistCalls]     = React.useState([]);
  const [histLoading, setHistLoading] = React.useState(false);
  const [histError, setHistError]     = React.useState("");
  const [histInfo, setHistInfo]       = React.useState("");
  const [syncedAt, setSyncedAt]       = React.useState(null);
  const [syncProgress, setSyncProgress] = React.useState(null); // { current, total, name }
  const [hasFetched, setHasFetched]   = React.useState(false);

  const unsubRef = React.useRef(null);

  // ── Mount: check credentials ───────────────────────────────────────────
  React.useEffect(() => {
    let cancelled = false;
    fetch("/.netlify/functions/marginiq-zoom-phone?action=status")
      .then(r => r.ok ? r.json() : { configured: false, missing: ["unknown"] })
      .then(d => { if (!cancelled) setStatus(d); })
      .catch(() => { if (!cancelled) setStatus({ configured: false, missing: ["unknown"] }); });
    return () => { cancelled = true; };
  }, []);

  // ── Live: Firebase listener ────────────────────────────────────────────
  React.useEffect(() => {
    if (livePaused || activeTab !== "live") {
      if (unsubRef.current) { try { unsubRef.current(); } catch(_){} unsubRef.current = null; }
      return;
    }
    const db = window.db;
    if (!db) { setLiveConnected(false); return; }

    setLiveConnected(false);
    let unsub = null;
    try {
      unsub = db.collection("zoom_calls")
        .orderBy("ts","desc").limit(150)
        .onSnapshot(
          snap => {
            try {
              setLiveConnected(true);
              const docs = snap.docs.map(d => {
                try { return { ...d.data(), _docId: d.id }; } catch(_) { return null; }
              }).filter(Boolean);

              const active = docs.filter(c => c && (c.status === "ringing" || c.status === "active"));
              const done   = docs.filter(c => c && c.status !== "ringing" && c.status !== "active");

              setLiveCalls(active);
              setRecentCalls(done.slice(0, 30).map(c => ({
                id:         c.call_id || c._docId,
                date:       c.ts || c.updated_at || "",
                answeredBy: c.employee || "Unknown",
                callerNum:  c.caller_num || "",
                callerName: c.caller_name || "",
                direction:  (c.direction || "inbound").toLowerCase(),
                result:     c.status === "ended" ? "answered"
                          : c.status === "missed" ? "missed"
                          : c.status === "voicemail" ? "voicemail"
                          : "unknown",
                talkTime:   Number(c.duration) || 0,
              })));

              if (docs.length > 0 && docs[0]) {
                setLastEvent({
                  ts: docs[0].updated_at || docs[0].ts,
                  employee: docs[0].employee || "—",
                });
              }
            } catch(err) {
              console.warn("[ZoomPhone] snapshot processing error:", err?.message);
            }
          },
          err => {
            console.warn("[ZoomPhone] listener error:", err?.message);
            setLiveConnected(false);
          }
        );
      unsubRef.current = unsub;
    } catch(err) {
      console.warn("[ZoomPhone] subscribe error:", err?.message);
      setLiveConnected(false);
    }
    return () => { if (unsub) { try { unsub(); } catch(_){} } unsubRef.current = null; };
  }, [livePaused, activeTab]);

  // ── History: fetch from cache ──────────────────────────────────────────
  const fetchHistory = React.useCallback(async () => {
    setHistLoading(true);
    setHistError("");
    setHistInfo("");
    setHasFetched(true);
    try {
      const params = new URLSearchParams({
        action: "history",
        from:   histFrom,
        to:     histTo,
      });
      if (histDir) params.set("direction", histDir);

      const r = await fetch("/.netlify/functions/marginiq-zoom-phone?" + params.toString());
      if (!r.ok) {
        const txt = await r.text().catch(() => "");
        let msg = "HTTP " + r.status;
        try { msg = JSON.parse(txt).error || msg; } catch(_) {}
        setHistError(msg);
        setHistCalls([]);
        return;
      }
      const d = await r.json().catch(() => ({}));
      const records = Array.isArray(d.records) ? d.records : [];
      setHistCalls(records);
      setSyncedAt(d.synced_at || null);
      if (d.source === "warming" || (records.length === 0 && !d.synced_at)) {
        setHistInfo("No data in cache yet. Tap “Sync from Zoom” to load it for the first time. After that, this loads instantly.");
      } else if (records.length === 0) {
        setHistInfo("No calls match the selected filters.");
      }
    } catch(e) {
      setHistError("Couldn't load: " + String(e?.message || e));
      setHistCalls([]);
    } finally {
      setHistLoading(false);
    }
  }, [histFrom, histTo, histDir]);

  // ── History: sync from Zoom (manual, per-user with progress) ───────────
  const syncFromZoom = React.useCallback(async () => {
    setHistLoading(true);
    setHistError("");
    setHistInfo("");
    setSyncProgress({ current: 0, total: 0, name: "Getting employee list…" });
    try {
      const ur = await fetch("/.netlify/functions/marginiq-zoom-phone?action=users");
      if (!ur.ok) {
        const e = await ur.json().catch(() => ({}));
        throw new Error(e.error || ("Failed to load employees: HTTP " + ur.status));
      }
      const ud = await ur.json();
      const users = Array.isArray(ud.users) ? ud.users : [];
      if (!users.length) throw new Error("No phone users found in your Zoom account.");

      for (let i = 0; i < users.length; i++) {
        const u = users[i] || {};
        setSyncProgress({ current: i + 1, total: users.length, name: u.name || "Unknown" });
        try {
          const params = new URLSearchParams({
            action:  "sync-user",
            userId:  u.id || "",
            name:    u.name || "",
            email:   u.email || "",
            ext:     u.ext || "",
          });
          const sr = await fetch("/.netlify/functions/marginiq-zoom-phone?" + params.toString());
          if (!sr.ok) {
            const e = await sr.json().catch(() => ({}));
            console.warn("[ZoomPhone] sync failed for " + u.name + ":", e.error);
          }
        } catch(err) {
          console.warn("[ZoomPhone] sync error for " + u.name + ":", err?.message);
        }
      }

      setSyncProgress(null);
      setHistInfo("Sync complete. Loading report…");
      await fetchHistory();
    } catch(e) {
      setHistError("Sync failed: " + String(e?.message || e));
      setSyncProgress(null);
      setHistLoading(false);
    }
  }, [fetchHistory]);

  // ── Derived: filtered + grouped history ────────────────────────────────
  const histFiltered = React.useMemo(() => {
    if (!Array.isArray(histCalls)) return [];
    if (!histEmp) return histCalls;
    return histCalls.filter(c => c && c.answeredBy === histEmp);
  }, [histCalls, histEmp]);

  const histEmployees = React.useMemo(() => {
    const set = new Set();
    histCalls.forEach(c => { if (c?.answeredBy) set.add(c.answeredBy); });
    return Array.from(set).sort();
  }, [histCalls]);

  const periodGroups = React.useMemo(() => {
    const map = new Map();
    histFiltered.forEach(c => {
      if (!c || !c.date) return;
      const key = _phonePeriodKey(c.date, histGroupBy);
      if (!key) return;
      let g = map.get(key);
      if (!g) {
        g = { key, label: _phonePeriodLabel(key, histGroupBy), total: 0, answered: 0, missed: 0, voicemail: 0, talk: 0 };
        map.set(key, g);
      }
      g.total++;
      if (c.result === "answered") { g.answered++; g.talk += Number(c.talkTime) || 0; }
      else if (c.result === "missed")    g.missed++;
      else if (c.result === "voicemail") g.voicemail++;
    });
    return Array.from(map.values()).map(g => ({
      ...g,
      rate: g.total ? Math.round((g.answered/g.total)*100) : 0,
      avgTalk: g.answered ? Math.round(g.talk / g.answered) : 0,
    }));
  }, [histFiltered, histGroupBy]);

  const empGroups = React.useMemo(() => {
    const map = new Map();
    histFiltered.forEach(c => {
      if (!c) return;
      const k = c.answeredBy || "Unknown";
      let g = map.get(k);
      if (!g) {
        g = { name: k, ext: c.answeredExt || "", total: 0, answered: 0, missed: 0, voicemail: 0, talk: 0 };
        map.set(k, g);
      }
      g.total++;
      if (c.result === "answered") { g.answered++; g.talk += Number(c.talkTime) || 0; }
      else if (c.result === "missed")    g.missed++;
      else if (c.result === "voicemail") g.voicemail++;
    });
    return Array.from(map.values()).map(g => ({
      ...g,
      rate: g.total ? Math.round((g.answered/g.total)*100) : 0,
      avgTalk: g.answered ? Math.round(g.talk / g.answered) : 0,
    }));
  }, [histFiltered]);

  const histMetrics = React.useMemo(() => {
    const tot = histFiltered.length;
    let ans = 0, mis = 0, vm = 0, talkSum = 0, talkCount = 0;
    histFiltered.forEach(c => {
      if (!c) return;
      if (c.result === "answered") {
        ans++;
        const t = Number(c.talkTime) || 0;
        if (t > 0) { talkSum += t; talkCount++; }
      }
      else if (c.result === "missed")    mis++;
      else if (c.result === "voicemail") vm++;
    });
    const busiest = periodGroups.length
      ? periodGroups.reduce((max, g) => g.total > max.total ? g : max, periodGroups[0])
      : null;
    return {
      tot, ans, mis, vm,
      avgTalk:  talkCount ? Math.round(talkSum / talkCount) : 0,
      ansRate:  tot ? Math.round((ans/tot)*100) : 0,
      missRate: tot ? Math.round((mis/tot)*100) : 0,
      busiest,
    };
  }, [histFiltered, periodGroups]);

  // Sortable hooks for the two history tables
  const periodSort = useSortable(periodGroups, "key", "desc");
  const empSort    = useSortable(empGroups, "total", "desc");

  // ── CSV exports ────────────────────────────────────────────────────────
  const exportPeriodCSV = () => {
    const rows = [
      ["Period","Total","Answered","Missed","Voicemail","Answer Rate %","Avg Talk (sec)"],
      ...periodGroups.map(p => [p.label, p.total, p.answered, p.missed, p.voicemail, p.rate, p.avgTalk]),
    ];
    const csv = rows.map(r => r.map(v => `"${String(v??"").replace(/"/g,'""')}"`).join(",")).join("\n");
    const a = Object.assign(document.createElement("a"), {
      href: URL.createObjectURL(new Blob([csv], { type: "text/csv" })),
      download: `phone_history_by_${histGroupBy}_${histFrom}_${histTo}.csv`,
    });
    a.click();
  };
  const exportEmpCSV = () => {
    const rows = [
      ["Employee","Ext","Total","Answered","Missed","Voicemail","Answer Rate %","Avg Talk (sec)"],
      ...empGroups.map(e => [e.name, e.ext, e.total, e.answered, e.missed, e.voicemail, e.rate, e.avgTalk]),
    ];
    const csv = rows.map(r => r.map(v => `"${String(v??"").replace(/"/g,'""')}"`).join(",")).join("\n");
    const a = Object.assign(document.createElement("a"), {
      href: URL.createObjectURL(new Blob([csv], { type: "text/csv" })),
      download: `phone_history_by_employee_${histFrom}_${histTo}.csv`,
    });
    a.click();
  };

  // ── Styles ─────────────────────────────────────────────────────────────
  const card = {
    background: T.bgCard, border: `1px solid ${T.border}`,
    borderRadius: T.radius, padding: 14, boxShadow: T.shadow,
  };
  const inputStyle = {
    padding: "9px 11px", border: `1px solid ${T.border}`,
    borderRadius: T.radiusSm, fontSize: 14,
    background: T.bgWhite, color: T.text,
    fontFamily: "inherit", outline: "none",
    width: "100%", minHeight: 38,
  };
  const labelStyle = {
    fontSize: 10, fontWeight: 700, color: T.textMuted,
    textTransform: "uppercase", letterSpacing: "0.06em",
    marginBottom: 4, display: "block",
  };
  const btnPrimary = {
    padding: "10px 18px", background: T.brand, color: "#fff",
    border: "none", borderRadius: T.radiusSm,
    fontWeight: 700, fontSize: 13, cursor: "pointer",
    minHeight: 40, whiteSpace: "nowrap",
  };
  const btnSec = {
    padding: "10px 14px", background: T.bgSurface, color: T.textMuted,
    border: `1px solid ${T.border}`, borderRadius: T.radiusSm,
    fontWeight: 600, fontSize: 12, cursor: "pointer",
    minHeight: 40, whiteSpace: "nowrap",
  };
  const tdStyle = { padding: "10px 10px", fontSize: 12, borderBottom: `1px solid ${T.borderLight}`, verticalAlign: "middle" };

  // ── Not configured ─────────────────────────────────────────────────────
  if (status && !status.configured) {
    return (
      <div style={{ padding: 16, maxWidth: 600 }}>
        <div style={{ ...card, borderLeft: `4px solid ${T.accentWarn}` }}>
          <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 12 }}>🔑 Zoom Credentials Not Configured</div>
          <p style={{ color: T.textMuted, fontSize: 13, marginBottom: 12, lineHeight: 1.6 }}>
            Add these environment variables to Netlify:
          </p>
          {["ZOOM_ACCOUNT_ID","ZOOM_CLIENT_ID","ZOOM_CLIENT_SECRET","FIREBASE_PROJECT_ID","FIREBASE_API_KEY"].map(k => (
            <div key={k} style={{
              fontFamily: "monospace", fontSize: 12, background: T.bgSurface,
              border: `1px solid ${T.border}`, borderRadius: T.radiusSm,
              padding: "7px 11px", marginBottom: 5, color: T.brand,
            }}>
              <span style={{ color: status.missing?.includes(k) ? T.red : T.green, marginRight: 6 }}>
                {status.missing?.includes(k) ? "✗" : "✓"}
              </span>{k}
            </div>
          ))}
        </div>
      </div>
    );
  }

  // ── Main render ────────────────────────────────────────────────────────
  return (
    <div style={{ padding: "12px clamp(12px, 3vw, 20px) 24px", maxWidth: 1400, margin: "0 auto" }}>

      {/* Tab nav */}
      <div style={{
        display: "flex", borderBottom: `2px solid ${T.border}`,
        marginBottom: 14, overflowX: "auto", WebkitOverflowScrolling: "touch",
      }}>
        {[
          { id: "live", label: "Live Feed", icon: "📞" },
          { id: "history", label: "History & Analysis", icon: "📊" },
        ].map(t => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            style={{
              padding: "10px 14px", background: "transparent",
              border: "none", borderBottom: `3px solid ${activeTab === t.id ? T.brand : "transparent"}`,
              color: activeTab === t.id ? T.brand : T.textMuted,
              fontWeight: activeTab === t.id ? 700 : 500,
              fontSize: 13, cursor: "pointer", marginBottom: -2,
              whiteSpace: "nowrap", transition: "all 0.15s",
            }}
          >
            <span style={{ marginRight: 6 }}>{t.icon}</span>{t.label}
          </button>
        ))}
      </div>

      {/* ═══════════ LIVE FEED ═══════════ */}
      {activeTab === "live" && (
        <div>
          {/* Connection status */}
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12, flexWrap: "wrap", gap: 8 }}>
            <div style={{ fontSize: 12, color: T.textMuted, display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{
                display: "inline-block", width: 8, height: 8, borderRadius: "50%",
                background: livePaused ? T.textDim : (liveConnected ? T.green : T.yellow),
                boxShadow: liveConnected && !livePaused ? `0 0 6px ${T.green}` : "none",
              }} />
              {livePaused ? "Paused" : liveConnected ? "Live · auto-updating" : "Connecting…"}
              {lastEvent && !livePaused && (
                <span style={{ marginLeft: 8 }}>· last: {_phoneFmtTime(lastEvent.ts)} {lastEvent.employee}</span>
              )}
            </div>
            <button
              style={{ ...btnSec, color: livePaused ? T.brand : T.green, borderColor: livePaused ? T.brand : T.green }}
              onClick={() => setLivePaused(p => !p)}
            >
              {livePaused ? "▶ Resume" : "⏸ Pause"}
            </button>
          </div>

          {/* Active calls */}
          <div style={{ ...card, borderLeft: `3px solid ${liveCalls.length ? T.green : T.border}`, marginBottom: 12 }}>
            <div style={{ fontWeight: 700, fontSize: 13, marginBottom: liveCalls.length ? 10 : 0 }}>
              {liveCalls.length
                ? <><span style={{ display:"inline-block", width:8, height:8, borderRadius:"50%", background:T.green, marginRight:6, verticalAlign:"middle", animation:"zmPulse 1.5s infinite" }} />{liveCalls.length} Active Call{liveCalls.length !== 1 ? "s" : ""}</>
                : <span style={{ color: T.textMuted, fontWeight: 500 }}>📵 No active calls</span>
              }
            </div>
            {liveCalls.length > 0 && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 8 }}>
                {liveCalls.map(c => (
                  <div key={c.call_id || c._docId} style={{
                    background: c.status === "ringing" ? T.yellowBg : T.greenBg,
                    border: `1px solid ${c.status === "ringing" ? T.yellow : T.green}`,
                    borderRadius: T.radiusSm, padding: "10px 12px",
                  }}>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 5 }}>
                      <div style={{ fontWeight: 700, fontSize: 12, color: c.status === "ringing" ? T.yellowText : T.greenText }}>
                        {c.status === "ringing" ? "📲 Ringing" : "📞 On Call"}
                      </div>
                      <span style={{ fontSize: 10, color: T.textMuted }}>{_phoneFmtTime(c.ts)}</span>
                    </div>
                    <div style={{ fontWeight: 600, fontSize: 14 }}>{c.employee || "—"}</div>
                    <div style={{ fontSize: 12, color: T.textMuted, marginTop: 2 }}>
                      {c.caller_name || c.caller_num || "Unknown caller"}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Recent calls (compact list, no horizontal scroll on mobile) */}
          <div style={{ ...card }}>
            <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 10 }}>
              Recent Calls
              <span style={{ fontWeight: 400, color: T.textMuted, fontSize: 12, marginLeft: 8 }}>
                ({recentCalls.length})
              </span>
            </div>
            {recentCalls.length === 0 ? (
              <_PhoneEmptyState
                icon="📞"
                title="No recent calls yet"
                body="Calls will appear here as they complete. Active calls show up at the top."
              />
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {recentCalls.map((c, i) => (
                  <div key={c.id || i} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 10px",
                    background: i % 2 === 0 ? T.bgWhite : T.bgSurface,
                    borderRadius: T.radiusSm, border: `1px solid ${T.borderLight}`,
                  }}>
                    {/* Result dot */}
                    <span style={{
                      flexShrink: 0, width: 8, height: 8, borderRadius: "50%",
                      background: c.result === "answered" ? T.green
                                 : c.result === "missed" ? T.red
                                 : c.result === "voicemail" ? T.yellow
                                 : T.textDim,
                    }} />

                    {/* Employee & caller */}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 600, fontSize: 13, color: T.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {c.answeredBy || "—"}
                        {c.direction === "outbound" && <span style={{ fontSize: 10, color: T.textMuted, marginLeft: 6 }}>→ out</span>}
                      </div>
                      <div style={{ fontSize: 11, color: T.textMuted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {c.callerName || c.callerNum || "Unknown"}
                      </div>
                    </div>

                    {/* Talk + time */}
                    <div style={{ flexShrink: 0, textAlign: "right" }}>
                      <div style={{ fontSize: 12, fontFamily: "monospace", color: T.text, fontWeight: 600 }}>{_phoneFmtDur(c.talkTime)}</div>
                      <div style={{ fontSize: 10, color: T.textMuted }}>{_phoneFmtTime(c.date)}</div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ═══════════ HISTORY ═══════════ */}
      {activeTab === "history" && (
        <div>
          {/* Controls — mobile-first stack */}
          <div style={{ ...card, marginBottom: 12 }}>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 10, marginBottom: 10 }}>
              <div>
                <div style={labelStyle}>From</div>
                <input type="date" value={histFrom} onChange={e => setHistFrom(e.target.value)} max={histTo} style={inputStyle} />
              </div>
              <div>
                <div style={labelStyle}>To</div>
                <input type="date" value={histTo} onChange={e => setHistTo(e.target.value)} min={histFrom} style={inputStyle} />
              </div>
              <div>
                <div style={labelStyle}>Group by</div>
                <select value={histGroupBy} onChange={e => setHistGroupBy(e.target.value)} style={inputStyle}>
                  <option value="day">Day</option>
                  <option value="week">Week</option>
                  <option value="month">Month</option>
                </select>
              </div>
              <div>
                <div style={labelStyle}>Employee</div>
                <select value={histEmp} onChange={e => setHistEmp(e.target.value)} style={inputStyle}>
                  <option value="">All</option>
                  {histEmployees.map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
              <div>
                <div style={labelStyle}>Direction</div>
                <select value={histDir} onChange={e => setHistDir(e.target.value)} style={inputStyle}>
                  <option value="">All</option>
                  <option value="inbound">Inbound</option>
                  <option value="outbound">Outbound</option>
                </select>
              </div>
            </div>

            <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
              <button style={btnPrimary} onClick={fetchHistory} disabled={histLoading}>
                {histLoading && !syncProgress ? "Loading…" : "Run Report"}
              </button>
              <button style={{ ...btnSec, color: T.brand, borderColor: T.brand }} onClick={syncFromZoom} disabled={histLoading}>
                🔄 Sync from Zoom
              </button>
              {syncedAt && (
                <div style={{ fontSize: 11, color: T.textMuted, display: "flex", alignItems: "center", gap: 5 }}>
                  <span style={{ display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: T.green }} />
                  Synced {new Date(syncedAt).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}
                </div>
              )}
            </div>
          </div>

          {/* Sync progress */}
          {syncProgress && (
            <div style={{ ...card, borderLeft: `3px solid ${T.brand}`, marginBottom: 12 }}>
              <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 6 }}>
                Syncing from Zoom · {syncProgress.current} / {syncProgress.total || "?"}
              </div>
              <div style={{ fontSize: 12, color: T.textMuted, marginBottom: 8 }}>{syncProgress.name}</div>
              <div style={{ height: 6, background: T.borderLight, borderRadius: 3, overflow: "hidden" }}>
                <div style={{
                  width: syncProgress.total ? `${(syncProgress.current / syncProgress.total) * 100}%` : "0%",
                  height: "100%", background: T.brand, transition: "width 0.3s",
                }} />
              </div>
            </div>
          )}

          {/* Error / Info banners */}
          {histError && (
            <div style={{
              background: T.redBg, border: `1px solid ${T.red}`,
              borderRadius: T.radiusSm, padding: "10px 14px",
              color: T.redText, fontSize: 13, marginBottom: 12,
            }}>⚠ {histError}</div>
          )}
          {histInfo && !histError && (
            <div style={{
              background: T.blueBg, border: `1px solid ${T.blue}`,
              borderRadius: T.radiusSm, padding: "10px 14px",
              color: T.blueText, fontSize: 13, marginBottom: 12,
            }}>{histInfo}</div>
          )}

          {/* Loading state */}
          {histLoading && !syncProgress && (
            <_PhoneEmptyState icon="📊" title="Loading…" />
          )}

          {/* Empty before first fetch */}
          {!histLoading && !hasFetched && (
            <_PhoneEmptyState
              icon="📊"
              title="Ready to analyze"
              body="Pick a date range and tap Run Report. First time? Tap Sync from Zoom to load your data."
            />
          )}

          {/* Results */}
          {!histLoading && histCalls.length > 0 && (
            <>
              {/* Summary tiles — auto-fit, no fixed 6-col grid */}
              <div style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(110px, 1fr))",
                gap: 8, marginBottom: 12,
              }}>
                <_PhoneStatPill label="Total Calls" value={histMetrics.tot.toLocaleString()} color={T.brand} />
                <_PhoneStatPill label="Answered"   value={histMetrics.ans.toLocaleString()} sub={`${histMetrics.ansRate}% rate`} color={T.green} />
                <_PhoneStatPill label="Missed"     value={histMetrics.mis.toLocaleString()} sub={`${histMetrics.missRate}% rate`} color={T.red} />
                <_PhoneStatPill label="Voicemail"  value={histMetrics.vm.toLocaleString()} color={T.yellow} />
                <_PhoneStatPill label="Avg Talk"   value={_phoneFmtDur(histMetrics.avgTalk)} color={T.text} />
                {histMetrics.busiest && (
                  <_PhoneStatPill
                    label={`Busiest ${histGroupBy}`}
                    value={String(histMetrics.busiest.total)}
                    sub={histMetrics.busiest.label}
                    color={T.brand}
                  />
                )}
              </div>

              {/* By Period */}
              <div style={{ ...card, marginBottom: 12, padding: 0, overflow: "hidden" }}>
                <div style={{ padding: "12px 14px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: `1px solid ${T.borderLight}`, flexWrap: "wrap", gap: 8 }}>
                  <div style={{ fontWeight: 700, fontSize: 13 }}>
                    By {histGroupBy.charAt(0).toUpperCase() + histGroupBy.slice(1)}
                    {histEmp && <span style={{ fontWeight: 400, color: T.textMuted, fontSize: 12, marginLeft: 6 }}>· {histEmp}</span>}
                  </div>
                  <button style={{ ...btnSec, padding: "6px 12px", fontSize: 11, minHeight: 32 }} onClick={exportPeriodCSV}>⬇ CSV</button>
                </div>
                <div style={{ overflowX: "auto", WebkitOverflowScrolling: "touch" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 520 }}>
                    <thead>
                      <tr>
                        <SortableTh label="Period"      col="key"      sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} />
                        <SortableTh label="Total"       col="total"    sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} align="right" />
                        <SortableTh label="Answered"    col="answered" sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} align="right" />
                        <SortableTh label="Missed"      col="missed"   sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} align="right" />
                        <SortableTh label="Answer Rate" col="rate"     sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} />
                        <SortableTh label="Avg Talk"    col="avgTalk"  sortKey={periodSort.sortKey} sortDir={periodSort.sortDir} onSort={periodSort.toggleSort} align="right" />
                      </tr>
                    </thead>
                    <tbody>
                      {periodSort.sorted.map((p, i) => (
                        <tr key={p.key} style={{ background: i % 2 === 0 ? T.bgWhite : T.bgSurface }}>
                          <td style={{ ...tdStyle, fontWeight: 600 }}>{p.label}</td>
                          <td style={{ ...tdStyle, textAlign: "right", fontWeight: 700, color: T.brand }}>{p.total}</td>
                          <td style={{ ...tdStyle, textAlign: "right", color: T.green, fontWeight: 600 }}>{p.answered}</td>
                          <td style={{ ...tdStyle, textAlign: "right", color: T.red, fontWeight: 600 }}>{p.missed}</td>
                          <td style={tdStyle}><_PhoneRateBar rate={p.rate} /></td>
                          <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{p.answered ? _phoneFmtDur(p.avgTalk) : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* By Employee */}
              <div style={{ ...card, padding: 0, overflow: "hidden" }}>
                <div style={{ padding: "12px 14px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: `1px solid ${T.borderLight}`, flexWrap: "wrap", gap: 8 }}>
                  <div style={{ fontWeight: 700, fontSize: 13 }}>
                    By Employee
                    <span style={{ fontWeight: 400, color: T.textMuted, fontSize: 12, marginLeft: 6 }}>
                      · {empGroups.length} {empGroups.length === 1 ? "person" : "people"}
                    </span>
                  </div>
                  <button style={{ ...btnSec, padding: "6px 12px", fontSize: 11, minHeight: 32 }} onClick={exportEmpCSV}>⬇ CSV</button>
                </div>
                <div style={{ overflowX: "auto", WebkitOverflowScrolling: "touch" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 600 }}>
                    <thead>
                      <tr>
                        <SortableTh label="Employee"    col="name"     sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} />
                        <SortableTh label="Total"       col="total"    sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} align="right" />
                        <SortableTh label="Answered"    col="answered" sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} align="right" />
                        <SortableTh label="Missed"      col="missed"   sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} align="right" />
                        <SortableTh label="Voicemail"   col="voicemail" sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} align="right" />
                        <SortableTh label="Answer Rate" col="rate"     sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} />
                        <SortableTh label="Avg Talk"    col="avgTalk"  sortKey={empSort.sortKey} sortDir={empSort.sortDir} onSort={empSort.toggleSort} align="right" />
                      </tr>
                    </thead>
                    <tbody>
                      {empSort.sorted.map((e, i) => (
                        <tr
                          key={e.name}
                          style={{ background: i % 2 === 0 ? T.bgWhite : T.bgSurface, cursor: "pointer" }}
                          onClick={() => setHistEmp(histEmp === e.name ? "" : e.name)}
                          title={histEmp === e.name ? "Click to clear filter" : "Click to filter to this employee"}
                        >
                          <td style={{ ...tdStyle, fontWeight: 700 }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                              <div style={{
                                width: 28, height: 28, background: T.brandPale,
                                border: `1px solid ${T.brand}33`, borderRadius: 6,
                                display: "flex", alignItems: "center", justifyContent: "center",
                                fontSize: 11, fontWeight: 800, color: T.brand, flexShrink: 0,
                              }}>{_phoneInitials(e.name)}</div>
                              <div style={{ minWidth: 0 }}>
                                <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 180 }}>{e.name}</div>
                                {e.ext && <div style={{ fontSize: 10, color: T.textMuted, fontWeight: 500 }}>Ext {e.ext}</div>}
                              </div>
                            </div>
                          </td>
                          <td style={{ ...tdStyle, textAlign: "right", fontWeight: 700, color: T.brand }}>{e.total}</td>
                          <td style={{ ...tdStyle, textAlign: "right", color: T.green, fontWeight: 600 }}>{e.answered}</td>
                          <td style={{ ...tdStyle, textAlign: "right", color: T.red, fontWeight: 600 }}>{e.missed}</td>
                          <td style={{ ...tdStyle, textAlign: "right", color: T.yellow }}>{e.voicemail}</td>
                          <td style={tdStyle}><_PhoneRateBar rate={e.rate} /></td>
                          <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{e.answered ? _phoneFmtDur(e.avgTalk) : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </div>
      )}

      <style>{`
        @keyframes zmPulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.6; transform: scale(1.3); }
        }
      `}</style>
    </div>
  );
}


if (typeof ReactDOM !== "undefined" && document.getElementById("root")) {
  const root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(React.createElement(MarginIQ));
}
