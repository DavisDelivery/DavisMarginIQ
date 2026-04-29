// CustomerAnalysisTab.jsx — Customer Profitability & Rate Benchmarking
// Anchor: Uline rate card (per stop / per lb / per skid) — every other
// customer is benchmarked against it. Modeled vs Actual cost columns.
// v1.0.0
//
// Data sources:
//   uline_weekly        → Uline stops, revenue, weight, skids
//   qbo_invoices        → all customers (revenue + invoice count)
//   qbo_pl_monthly      → actual monthly costs (for actual cost/stop)
//   marginiq_config/cost_structure → modeled costs
//   payroll_weekly      → actual labor (cross-check)
//   fuel_weekly         → actual fuel (cross-check)

(function () {
"use strict";

const { useState, useEffect, useMemo, useCallback } = React;

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

const fmt   = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmt2  = n => n==null||isNaN(n)?"$0.00":"$"+Number(n).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtK  = n => { if(n==null||isNaN(n))return"$0";const v=Number(n);if(Math.abs(v)>=1000000)return"$"+(v/1000000).toFixed(2)+"M";if(Math.abs(v)>=1000)return"$"+(v/1000).toFixed(1)+"K";return"$"+v.toFixed(0);};
const fmtPct= (n,d=1) => n==null||isNaN(n)?"0%":Number(n).toFixed(d)+"%";
const fmtNum= n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

const card  = { background:T.bgCard, borderRadius:T.radius, border:`1px solid ${T.border}`, padding:"16px", boxShadow:T.shadow };
const tblH  = { padding:"8px 12px", textAlign:"left", fontSize:10, fontWeight:700, textTransform:"uppercase", color:T.textDim, background:T.bgSurface, borderBottom:`1px solid ${T.border}`, whiteSpace:"nowrap" };
const tblD  = { padding:"8px 12px", fontSize:12, color:T.text, borderBottom:`1px solid ${T.borderLight}` };
const tblDR = { ...tblD, textAlign:"right", fontVariantNumeric:"tabular-nums" };

const db = () => window.db;

// ─── Time window options ──────────────────────────────────────────────────────
const WINDOWS = [
  { id:"l4w",  label:"Last 4 weeks",   weeks:4   },
  { id:"l13w", label:"Last 13 weeks",  weeks:13  },
  { id:"l26w", label:"Last 26 weeks",  weeks:26  },
  { id:"l52w", label:"Last 52 weeks",  weeks:52  },
  { id:"all",  label:"All time",       weeks:null},
];

// ─── Firestore loaders ────────────────────────────────────────────────────────
async function loadAll() {
  if (!db()) return null;
  const fetch = async (coll, ord, lim) => {
    try {
      let q = db().collection(coll);
      if (ord) q = q.orderBy(ord, "desc");
      if (lim) q = q.limit(lim);
      const s = await q.get();
      return s.docs.map(d => ({ id:d.id, ...d.data() }));
    } catch(e) { return []; }
  };
  const getCfg = async (doc) => {
    try {
      const d = await db().collection("marginiq_config").doc(doc).get();
      return d.exists ? d.data() : null;
    } catch(e) { return null; }
  };

  const [ulineWeekly, invoices, plMonthly, payrollWeekly, fuelWeekly, costStructure] =
    await Promise.all([
      fetch("uline_weekly",     "week_ending", 260),
      fetch("qbo_invoices",     "date",       2000),
      fetch("qbo_pl_monthly",   "month",        60),
      fetch("payroll_weekly",   "week_ending", 260),
      fetch("fuel_weekly",      "week_ending", 260),
      getCfg("cost_structure"),
    ]);

  return { ulineWeekly, invoices, plMonthly, payrollWeekly, fuelWeekly, costStructure };
}

// ─── Filter rollups by time window ────────────────────────────────────────────
function filterByWindow(rollups, weeks) {
  if (!weeks || !rollups.length) return rollups;
  const sorted = [...rollups].sort((a,b)=> (b.week_ending||"").localeCompare(a.week_ending||""));
  return sorted.slice(0, weeks);
}
function filterMonthsByWindow(months, weeks) {
  if (!weeks || !months.length) return months;
  // approximate: weeks/4.33 = months
  const n = Math.max(1, Math.round(weeks / 4.33));
  return [...months].sort((a,b)=>(b.month||"").localeCompare(a.month||"")).slice(0, n);
}
function filterInvoicesByWindow(invoices, weeks) {
  if (!weeks || !invoices.length) return invoices;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - weeks*7);
  return invoices.filter(i => i.date && new Date(i.date) >= cutoff);
}

// ─── Modeled cost structure → annualized + per-stop ──────────────────────────
function computeModeledCosts(cs) {
  if (!cs) return null;
  const labor =
    (cs.count_box_drivers||0)    * (cs.rate_box_driver||0)    * (cs.avg_hours_per_shift||10) * (cs.working_days_year||260) +
    (cs.count_tractor_drivers||0)* (cs.rate_tractor_driver||0)* (cs.avg_hours_per_shift||10) * (cs.working_days_year||260) +
    (cs.count_dispatchers||0)    * (cs.rate_dispatcher||0)    * (cs.avg_hours_per_shift||10) * (cs.working_days_year||260) +
    (cs.count_admin||0)          * (cs.rate_admin||0)         * (cs.avg_hours_per_shift||10) * (cs.working_days_year||260) +
    (cs.count_mechanics||0)      * (cs.rate_mechanic||0)      * (cs.avg_hours_per_shift||10) * (cs.working_days_year||260) +
    (cs.forklift_operators||0); // forklift_operators is stored as annual $ already

  const truckCount = (cs.truck_count_box||0) + (cs.truck_count_tractor||0);
  const insurance  = truckCount * (cs.truck_insurance_monthly||0) * 12;
  // Estimated fuel using cost structure assumptions
  const fuelBox     = (cs.truck_count_box||0)     * (cs.working_days_year||260) * 100/((cs.mpg_box||8))     * (cs.fuel_price||3.5); // 100mi/day estimate
  const fuelTractor = (cs.truck_count_tractor||0) * (cs.working_days_year||260) * 200/((cs.mpg_tractor||6)) * (cs.fuel_price||3.5); // 200mi/day estimate
  const fuel = fuelBox + fuelTractor;
  const facility = (cs.warehouse||0) + (cs.forklifts||0);

  const total = labor + insurance + fuel + facility;
  return {
    annual: total,
    labor, insurance, fuel, facility,
    weekly: total/52, daily: total/(cs.working_days_year||260), monthly: total/12,
  };
}

// ─── Customer name normalization ──────────────────────────────────────────────
const norm = s => String(s||"").trim().toLowerCase().replace(/\s+/g," ");
function isUlineName(s) {
  const n = norm(s);
  return n.includes("uline");
}

// ─── Aggregate: Uline metrics from weekly rollups ─────────────────────────────
function aggregateUline(rollups) {
  let revenue = 0, stops = 0, weight = 0, skids = 0;
  let deliveryRevenue = 0, deliveryStops = 0;
  let accessorialRevenue = 0, accessorialStops = 0;
  let truckloadRevenue = 0, truckloadStops = 0;
  for (const r of rollups) {
    revenue += r.revenue || 0;
    stops   += r.stops || 0;
    weight  += r.weight || 0;
    skids   += r.skids || 0;
    deliveryRevenue    += r.delivery_revenue    || 0;
    deliveryStops      += r.delivery_stops      || 0;
    accessorialRevenue += r.accessorial_revenue || 0;
    accessorialStops   += r.accessorial_stops   || 0;
    truckloadRevenue   += r.truckload_revenue   || 0;
    truckloadStops     += r.truckload_stops     || 0;
  }
  return {
    revenue, stops, weight, skids,
    deliveryRevenue, deliveryStops,
    accessorialRevenue, accessorialStops,
    truckloadRevenue, truckloadStops,
    revPerStop:   stops  ? revenue/stops  : 0,
    revPerLb:     weight ? revenue/weight : 0,
    revPerSkid:   skids  ? revenue/skids  : 0,
    deliveryRevPerStop: deliveryStops ? deliveryRevenue/deliveryStops : 0,
  };
}

// ─── Aggregate: per-customer from QBO invoices ────────────────────────────────
function aggregateCustomers(invoices) {
  const byCust = {};
  for (const inv of invoices) {
    const name = (inv.customer || "Unknown").trim();
    if (!name) continue;
    if (!byCust[name]) byCust[name] = { name, revenue:0, count:0, months:new Set(), invoices:[] };
    byCust[name].revenue += inv.total || 0;
    byCust[name].count++;
    if (inv.month) byCust[name].months.add(inv.month);
    byCust[name].invoices.push(inv);
  }
  return Object.values(byCust).map(c => ({
    ...c,
    monthsActive: c.months.size,
    avgInvoice: c.count ? c.revenue/c.count : 0,
    monthlyAvg: c.months.size ? c.revenue/c.months.size : 0,
  }));
}

// ─── Compute "actual" cost per stop ───────────────────────────────────────────
// Strategy: total expenses (QBO P&L) over the window / total stops over the window
function computeActualCostPerStop(plMonthly, ulineAgg, payrollWeekly, fuelWeekly) {
  if (ulineAgg.stops === 0) return null;

  // Prefer QBO P&L total (income + cogs + expenses cover the full picture)
  let totalCost = 0, hasPL = false;
  for (const m of plMonthly) {
    totalCost += (m.cogs||0) + (m.expenses||0);
    hasPL = true;
  }
  if (hasPL && totalCost > 0) {
    return {
      source: "QBO P&L (actual)",
      total: totalCost,
      perStop: totalCost / ulineAgg.stops,
    };
  }

  // Fallback: payroll + fuel only (incomplete but better than nothing)
  let actualLabor = 0;
  for (const w of payrollWeekly) actualLabor += w.gross_pay || w.total_pay || 0;
  let actualFuel = 0;
  for (const f of fuelWeekly) actualFuel += f.spend || 0;

  if (actualLabor > 0 || actualFuel > 0) {
    const total = actualLabor + actualFuel;
    return {
      source: "Payroll + Fuel only (partial)",
      total,
      perStop: total / ulineAgg.stops,
      partial: true,
      labor: actualLabor,
      fuel: actualFuel,
    };
  }
  return null;
}

// ─── KPI Card ─────────────────────────────────────────────────────────────────
const KPI = ({ label, value, sub, color, bg, hint }) => (
  <div style={{ ...card, flex:1, minWidth:160, background:bg||T.bgCard }}>
    <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.05em", marginBottom:4 }}>{label}</div>
    <div style={{ fontSize:22, fontWeight:800, color:color||T.text }}>{value}</div>
    {sub && <div style={{ fontSize:10, color:T.textDim, marginTop:2 }}>{sub}</div>}
    {hint && <div style={{ fontSize:10, color:T.textMuted, marginTop:6, lineHeight:1.4 }}>{hint}</div>}
  </div>
);

const Section = ({ icon, title, sub, children }) => (
  <div style={{ marginBottom:24 }}>
    <div style={{ display:"flex", alignItems:"baseline", gap:10, marginBottom:12 }}>
      <span style={{ fontSize:18 }}>{icon}</span>
      <span style={{ fontSize:14, fontWeight:800, color:T.text }}>{title}</span>
      {sub && <span style={{ fontSize:11, color:T.textMuted }}>{sub}</span>}
    </div>
    {children}
  </div>
);

const Pill = ({ active, children, onClick }) => (
  <button onClick={onClick} style={{
    padding:"6px 14px", borderRadius:T.radiusSm, border:"none",
    background: active ? T.brand : T.bgSurface,
    color: active ? "#fff" : T.textMuted,
    fontSize:12, fontWeight:active?700:500, cursor:"pointer", transition:"all 0.2s",
  }}>{children}</button>
);

// ─── Uline Benchmark Card ─────────────────────────────────────────────────────
function UlineBenchmark({ uline, modeled, actual, weeks }) {
  if (uline.stops === 0) return (
    <div style={{ ...card, background:T.yellowBg, borderColor:T.yellow }}>
      <div style={{ fontSize:13, fontWeight:700, color:T.yellowText, marginBottom:6 }}>⚠️ No Uline data in this window</div>
      <div style={{ fontSize:12, color:T.text }}>Try widening the time window or check Data Health for missing weeks.</div>
    </div>
  );

  // Cost per stop: modeled
  const modCostPerStop = modeled && uline.stops ? (modeled.annual * (weeks||52) / 52) / uline.stops : null;
  const modMargin      = modCostPerStop != null ? uline.revPerStop - modCostPerStop : null;
  const modMarginPct   = modCostPerStop != null && uline.revPerStop > 0 ? (modMargin/uline.revPerStop)*100 : null;

  const actCostPerStop = actual ? actual.perStop : null;
  const actMargin      = actCostPerStop != null ? uline.revPerStop - actCostPerStop : null;
  const actMarginPct   = actCostPerStop != null && uline.revPerStop > 0 ? (actMargin/uline.revPerStop)*100 : null;

  return (
    <div style={{ ...card, borderLeft:`4px solid ${T.brand}`, background:T.brandPale }}>
      <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:14 }}>
        <span style={{ fontSize:22 }}>🎯</span>
        <div>
          <div style={{ fontSize:14, fontWeight:800, color:T.brandDark }}>Uline Benchmark</div>
          <div style={{ fontSize:11, color:T.textMuted }}>Your largest customer — every other rate is compared to this</div>
        </div>
      </div>

      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(150px, 1fr))", gap:12 }}>
        <KPI label="Total Revenue"   value={fmtK(uline.revenue)} sub={`${fmtNum(uline.stops)} stops`} color={T.brand} />
        <KPI label="Revenue / Stop"  value={fmt2(uline.revPerStop)} color={T.brand} sub="THE benchmark rate" />
        <KPI label="Revenue / Pound" value={fmt2(uline.revPerLb)}  color={T.brand} sub={`${fmtNum(uline.weight)} lbs total`} />
        <KPI label="Revenue / Skid"  value={fmt2(uline.revPerSkid)} color={T.brand} sub={`${fmtNum(uline.skids)} skids`} />
      </div>

      <div style={{ marginTop:16, padding:"12px 14px", background:T.bgWhite, borderRadius:T.radiusSm, border:`1px solid ${T.border}` }}>
        <div style={{ fontSize:11, fontWeight:700, color:T.textMuted, textTransform:"uppercase", marginBottom:10 }}>Cost per Stop — Modeled vs Actual</div>
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:14 }}>
          <div>
            <div style={{ fontSize:10, color:T.textDim, fontWeight:600, marginBottom:4 }}>MODELED (cost structure)</div>
            <div style={{ fontSize:18, fontWeight:800, color:T.text }}>{modCostPerStop!=null?fmt2(modCostPerStop):"—"}</div>
            {modMargin!=null && (
              <div style={{ fontSize:11, marginTop:4 }}>
                Margin: <span style={{ color:modMargin>=0?T.green:T.red, fontWeight:700 }}>{fmt2(modMargin)} ({fmtPct(modMarginPct)})</span>
              </div>
            )}
          </div>
          <div>
            <div style={{ fontSize:10, color:T.textDim, fontWeight:600, marginBottom:4 }}>ACTUAL ({actual?.source||"no source"})</div>
            <div style={{ fontSize:18, fontWeight:800, color:T.text }}>{actCostPerStop!=null?fmt2(actCostPerStop):"—"}</div>
            {actMargin!=null ? (
              <div style={{ fontSize:11, marginTop:4 }}>
                Margin: <span style={{ color:actMargin>=0?T.green:T.red, fontWeight:700 }}>{fmt2(actMargin)} ({fmtPct(actMarginPct)})</span>
              </div>
            ) : (
              <div style={{ fontSize:10, color:T.textDim, marginTop:4, lineHeight:1.4 }}>Import QBO P&L (Costs tab → QuickBooks → Profit & Loss) to compute actual cost/stop</div>
            )}
          </div>
        </div>
        {actual?.partial && (
          <div style={{ fontSize:10, color:T.yellowText, marginTop:8, fontStyle:"italic" }}>
            ⚠️ Actual = payroll + fuel only. Doesn't include fixed costs, insurance, etc. Import QBO P&L for a complete number.
          </div>
        )}
      </div>

      <div style={{ marginTop:14, fontSize:11, color:T.textMuted, lineHeight:1.5 }}>
        <strong>Service-type breakdown:</strong>{" "}
        Delivery <strong>{fmt2(uline.deliveryRevPerStop)}/stop</strong>{" "}({fmtNum(uline.deliveryStops)} stops, {fmtK(uline.deliveryRevenue)})
        {uline.accessorialStops > 0 && <> · Accessorials {fmtK(uline.accessorialRevenue)} ({fmtNum(uline.accessorialStops)} stops)</>}
        {uline.truckloadStops > 0   && <> · Truckload {fmtK(uline.truckloadRevenue)} ({fmtNum(uline.truckloadStops)} stops)</>}
      </div>
    </div>
  );
}

// ─── Customer Comparison Table ────────────────────────────────────────────────
function CustomerTable({ customers, ulineRevPerStop, modeledCostPerStop, actualCostPerStop, totalRevenue }) {
  const [sortKey, setSortKey] = useState("revenue");
  const [sortDir, setSortDir] = useState(-1);

  const sorted = useMemo(() => {
    const arr = [...customers].sort((a,b) => {
      const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
      if (typeof av === "string") return sortDir * (av < bv ? -1 : av > bv ? 1 : 0);
      return sortDir * (bv - av);
    });
    return arr;
  }, [customers, sortKey, sortDir]);

  const SortTh = ({ k, label, right, hint }) => (
    <th style={{ ...tblH, textAlign:right?"right":"left", cursor:"pointer" }} title={hint}
        onClick={() => { if(sortKey===k) setSortDir(-sortDir); else { setSortKey(k); setSortDir(-1); } }}>
      {label}{sortKey===k?(sortDir===-1?" ↓":" ↑"):""}
    </th>
  );

  if (!customers.length) return (
    <div style={{ ...card, textAlign:"center", padding:"40px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:8 }}>📥</div>
      <div style={{ fontSize:13, fontWeight:600 }}>No QBO invoice data yet</div>
      <div style={{ fontSize:11, marginTop:6, lineHeight:1.5 }}>
        Import a QBO Invoice List CSV (Reports → Invoice List → Export to CSV)<br/>
        in the <strong>QuickBooks tab</strong> to populate this comparison.
      </div>
    </div>
  );

  return (
    <div style={{ ...card, padding:0, overflow:"auto" }}>
      <table style={{ width:"100%", borderCollapse:"collapse", minWidth:900 }}>
        <thead>
          <tr>
            <SortTh k="name"        label="Customer" />
            <SortTh k="revenue"     label="Revenue"    right hint="Total revenue across all invoices in window" />
            <SortTh k="count"       label="# Inv"      right />
            <SortTh k="avgInvoice"  label="Avg / Inv"  right />
            <SortTh k="monthsActive" label="Months"    right />
            <SortTh k="monthlyAvg"  label="Avg / Mo"   right />
            <th style={{ ...tblH, textAlign:"right" }} title="Customer's avg invoice ÷ Uline's avg revenue/stop. >100% means they pay more per touch than Uline">vs Uline /Stop</th>
            <th style={{ ...tblH, textAlign:"right" }} title="Modeled gross profit % using blended cost-per-stop">Modeled %</th>
            <th style={{ ...tblH, textAlign:"right" }} title="Actual gross profit % using QBO actual cost-per-stop">Actual %</th>
            <th style={{ ...tblH, textAlign:"right" }} title="Share of total tracked revenue">Share</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((c, i) => {
            const isUline = isUlineName(c.name);
            // vs Uline: compare avgInvoice to Uline rev/stop (rough — assumes similar work per touch)
            const vsUline = ulineRevPerStop > 0 ? (c.avgInvoice / ulineRevPerStop) * 100 : null;
            // Modeled margin estimate: assume invoices ≈ stops (worst case, gives conservative number)
            // For Uline we use stops directly
            const stopsEstimate = isUline ? null : c.count;
            const modeledMargin = (modeledCostPerStop && stopsEstimate)
              ? ((c.revenue - modeledCostPerStop * stopsEstimate) / c.revenue) * 100
              : null;
            const actualMargin = (actualCostPerStop && stopsEstimate)
              ? ((c.revenue - actualCostPerStop * stopsEstimate) / c.revenue) * 100
              : null;
            const share = totalRevenue > 0 ? (c.revenue/totalRevenue)*100 : 0;
            return (
              <tr key={c.name} style={{ background: isUline ? T.brandPale : (i%2 ? T.bgSurface : undefined) }}>
                <td style={{ ...tblD, fontWeight:isUline?700:500 }}>
                  {isUline && "🎯 "}{c.name}
                  {isUline && <span style={{ marginLeft:6, fontSize:9, color:T.brand, fontWeight:700, padding:"1px 6px", background:T.bgWhite, borderRadius:10 }}>BENCHMARK</span>}
                </td>
                <td style={{ ...tblDR, fontWeight:600 }}>{fmtK(c.revenue)}</td>
                <td style={tblDR}>{fmtNum(c.count)}</td>
                <td style={tblDR}>{fmt(c.avgInvoice)}</td>
                <td style={tblDR}>{c.monthsActive}</td>
                <td style={tblDR}>{fmtK(c.monthlyAvg)}</td>
                <td style={{ ...tblDR, color: vsUline==null?T.textDim : vsUline>=100?T.green:T.red, fontWeight:600 }}>
                  {isUline ? "—" : vsUline!=null ? fmtPct(vsUline,0) : "—"}
                </td>
                <td style={{ ...tblDR, color:(modeledMargin||0)>=0?T.greenText:T.redText }}>
                  {modeledMargin!=null?fmtPct(modeledMargin):"—"}
                </td>
                <td style={{ ...tblDR, color:(actualMargin||0)>=0?T.greenText:T.redText }}>
                  {actualMargin!=null?fmtPct(actualMargin):"—"}
                </td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtPct(share)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ─── Monthly Trend Chart (top N customers) ────────────────────────────────────
function CustomerTrendChart({ invoices, customers, topN=8 }) {
  const top = [...customers].sort((a,b)=>b.revenue-a.revenue).slice(0, topN).map(c=>c.name);
  if (!top.length || !invoices.length) return null;

  // Build matrix [customer][month] = revenue
  const monthSet = new Set();
  const data = {};
  for (const c of top) data[c] = {};
  for (const inv of invoices) {
    if (!inv.month) continue;
    monthSet.add(inv.month);
    const cust = inv.customer || "Unknown";
    if (!top.includes(cust)) continue;
    data[cust][inv.month] = (data[cust][inv.month] || 0) + (inv.total||0);
  }
  const months = Array.from(monthSet).sort();
  if (months.length < 2) return null;

  // Find max for scaling
  let max = 0;
  for (const c of top) for (const m of months) max = Math.max(max, data[c][m]||0);
  if (!max) return null;

  // Color palette
  const palette = [T.brand, T.green, T.red, T.purple, T.yellow, T.blue, "#0d9488", "#db2777"];

  const W = 780, H = 280, pad = { l:54, r:140, t:14, b:36 };
  const innerW = W - pad.l - pad.r;
  const innerH = H - pad.t - pad.b;
  const xAt = i => pad.l + (months.length<2?0:(i/(months.length-1))*innerW);
  const yAt = v => pad.t + innerH - (v/max)*innerH;

  const monthLabel = m => {
    const [y,mm] = m.split("-");
    return ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][parseInt(mm)] + " '" + y.slice(2);
  };

  return (
    <div style={{ ...card, overflow:"auto" }}>
      <div style={{ fontSize:13, fontWeight:700, marginBottom:10 }}>📈 Top {top.length} Customers — Monthly Revenue Trend</div>
      <svg width={W} height={H} style={{ display:"block" }}>
        {/* Y axis gridlines */}
        {[0, 0.25, 0.5, 0.75, 1].map(p => (
          <g key={p}>
            <line x1={pad.l} y1={pad.t+innerH-p*innerH} x2={pad.l+innerW} y2={pad.t+innerH-p*innerH} stroke={T.borderLight} strokeDasharray="2,4" />
            <text x={pad.l-8} y={pad.t+innerH-p*innerH+3} fontSize="9" fill={T.textDim} textAnchor="end">{fmtK(max*p)}</text>
          </g>
        ))}
        {/* X axis labels (every Nth) */}
        {months.map((m, i) => {
          const skip = Math.ceil(months.length/8);
          if (i%skip!==0 && i!==months.length-1) return null;
          return <text key={m} x={xAt(i)} y={H-pad.b+16} fontSize="9" fill={T.textDim} textAnchor="middle">{monthLabel(m)}</text>;
        })}
        {/* Lines */}
        {top.map((c, ci) => {
          const path = months.map((m, i) => {
            const v = data[c][m] || 0;
            return `${i===0?"M":"L"} ${xAt(i)} ${yAt(v)}`;
          }).join(" ");
          const color = palette[ci % palette.length];
          return (
            <g key={c}>
              <path d={path} stroke={color} strokeWidth="2" fill="none" />
              {months.map((m, i) => (
                <circle key={i} cx={xAt(i)} cy={yAt(data[c][m]||0)} r="2.5" fill={color} />
              ))}
            </g>
          );
        })}
        {/* Legend */}
        {top.map((c, ci) => (
          <g key={"legend-"+c} transform={`translate(${pad.l+innerW+10}, ${pad.t+ci*22})`}>
            <rect width="10" height="10" fill={palette[ci % palette.length]} rx="2" />
            <text x="14" y="9" fontSize="10" fill={T.text}>{c.length>16?c.slice(0,15)+"…":c}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}

// ─── Cost Source Breakdown (small table) ──────────────────────────────────────
function CostSourceCard({ modeled, actualSource, weeks }) {
  if (!modeled) return null;
  const factor = (weeks||52) / 52;
  const m = (v) => v * factor;
  return (
    <div style={card}>
      <div style={{ fontSize:13, fontWeight:700, marginBottom:10 }}>💰 Modeled Cost Breakdown <span style={{ fontSize:11, color:T.textMuted, fontWeight:500 }}>({weeks||52} weeks pro-rata)</span></div>
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10 }}>
        <div style={{ display:"flex", justifyContent:"space-between", padding:"8px 0", borderBottom:`1px solid ${T.borderLight}` }}>
          <span style={{ fontSize:12, color:T.textMuted }}>Labor</span>
          <span style={{ fontSize:12, fontWeight:600 }}>{fmtK(m(modeled.labor))}</span>
        </div>
        <div style={{ display:"flex", justifyContent:"space-between", padding:"8px 0", borderBottom:`1px solid ${T.borderLight}` }}>
          <span style={{ fontSize:12, color:T.textMuted }}>Insurance</span>
          <span style={{ fontSize:12, fontWeight:600 }}>{fmtK(m(modeled.insurance))}</span>
        </div>
        <div style={{ display:"flex", justifyContent:"space-between", padding:"8px 0", borderBottom:`1px solid ${T.borderLight}` }}>
          <span style={{ fontSize:12, color:T.textMuted }}>Fuel (estimated)</span>
          <span style={{ fontSize:12, fontWeight:600 }}>{fmtK(m(modeled.fuel))}</span>
        </div>
        <div style={{ display:"flex", justifyContent:"space-between", padding:"8px 0", borderBottom:`1px solid ${T.borderLight}` }}>
          <span style={{ fontSize:12, color:T.textMuted }}>Facility</span>
          <span style={{ fontSize:12, fontWeight:600 }}>{fmtK(m(modeled.facility))}</span>
        </div>
        <div style={{ display:"flex", justifyContent:"space-between", padding:"8px 0", gridColumn:"1/3", borderTop:`2px solid ${T.border}`, marginTop:4 }}>
          <span style={{ fontSize:12, fontWeight:700 }}>Total Modeled Cost</span>
          <span style={{ fontSize:13, fontWeight:800, color:T.brand }}>{fmtK(m(modeled.annual))}</span>
        </div>
      </div>
      <div style={{ fontSize:10, color:T.textDim, marginTop:10, lineHeight:1.5 }}>
        Adjust these inputs in the <strong>Costs</strong> tab. Actual cost source: <strong>{actualSource||"none — import QBO P&L"}</strong>.
      </div>
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
function CustomerAnalysisTab() {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [windowId, setWindowId] = useState("l52w");

  useEffect(() => {
    (async () => {
      setLoading(true);
      const d = await loadAll();
      setData(d);
      setLoading(false);
    })();
  }, []);

  const win = WINDOWS.find(w => w.id === windowId) || WINDOWS[3];

  const computed = useMemo(() => {
    if (!data) return null;

    const ulineWeekly  = filterByWindow(data.ulineWeekly, win.weeks);
    const invoices     = filterInvoicesByWindow(data.invoices, win.weeks);
    const plMonthly    = filterMonthsByWindow(data.plMonthly, win.weeks);
    const payrollWeekly= filterByWindow(data.payrollWeekly, win.weeks);
    const fuelWeekly   = filterByWindow(data.fuelWeekly, win.weeks);

    const uline    = aggregateUline(ulineWeekly);
    const customers= aggregateCustomers(invoices);
    const modeled  = computeModeledCosts(data.costStructure);
    const actual   = computeActualCostPerStop(plMonthly, uline, payrollWeekly, fuelWeekly);

    const modeledCostPerStop = (modeled && uline.stops)
      ? (modeled.annual * (win.weeks||52) / 52) / uline.stops
      : null;
    const actualCostPerStop = actual ? actual.perStop : null;

    const totalRevenue = customers.reduce((s,c)=>s+(c.revenue||0), 0);
    const ulineRevenue = customers.filter(c=>isUlineName(c.name)).reduce((s,c)=>s+c.revenue, 0)
                      || uline.revenue; // fall back to weekly rollups if Uline not in QBO yet

    return { uline, customers, modeled, actual, modeledCostPerStop, actualCostPerStop, totalRevenue, ulineRevenue, plMonthly, invoices };
  }, [data, win]);

  if (loading) return (
    <div style={{ textAlign:"center", padding:"60px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:12 }}>📊</div>
      <div style={{ fontSize:14 }}>Loading customer analysis...</div>
    </div>
  );

  if (!computed) return (
    <div style={{ textAlign:"center", padding:"60px 20px", color:T.red }}>Failed to load data — check console</div>
  );

  const c = computed;
  const ulineSharePct = c.totalRevenue > 0 ? (c.ulineRevenue/c.totalRevenue)*100 : null;

  return (
    <div style={{ padding:"20px", maxWidth:1400, margin:"0 auto" }} className="fade-in">
      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16, flexWrap:"wrap", gap:12 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:20 }}>📊</span>
          <span style={{ fontSize:15, fontWeight:800, color:T.text }}>Customer Analysis</span>
          <span style={{ fontSize:11, color:T.textMuted, marginLeft:6 }}>Rate benchmarking · modeled vs actual margins</span>
        </div>
        <div style={{ display:"flex", gap:6, flexWrap:"wrap" }}>
          {WINDOWS.map(w => <Pill key={w.id} active={windowId===w.id} onClick={()=>setWindowId(w.id)}>{w.label}</Pill>)}
        </div>
      </div>

      {/* Top KPIs */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(180px, 1fr))", gap:12, marginBottom:24 }}>
        <KPI label="Tracked Revenue" value={fmtK(Math.max(c.totalRevenue, c.uline.revenue))} sub={`${c.customers.length} QBO customer(s) · Uline rollups separate`} color={T.brand} />
        <KPI label="Uline Share"     value={ulineSharePct!=null?fmtPct(ulineSharePct,0):"—"} sub={ulineSharePct!=null?fmtK(c.ulineRevenue):"Uline data not yet in QBO"} color={T.purple} />
        <KPI label="Modeled $/Stop"  value={c.modeledCostPerStop!=null?fmt2(c.modeledCostPerStop):"—"} sub="from cost structure" />
        <KPI label="Actual $/Stop"   value={c.actualCostPerStop!=null?fmt2(c.actualCostPerStop):"—"}  sub={c.actual?.source||"import QBO P&L"} />
        <KPI label="Uline Avg Rate"  value={fmt2(c.uline.revPerStop)} sub="revenue per Uline stop" color={T.brand} />
      </div>

      {/* Uline benchmark */}
      <Section icon="🎯" title="Uline Benchmark" sub={`${win.label} window`}>
        <UlineBenchmark uline={c.uline} modeled={c.modeled} actual={c.actual} weeks={win.weeks} />
      </Section>

      {/* Cost structure breakdown */}
      <Section icon="💰" title="Cost Sources">
        <CostSourceCard modeled={c.modeled} actualSource={c.actual?.source} weeks={win.weeks} />
      </Section>

      {/* Customer comparison table */}
      <Section icon="📋" title="All Customers — Ranked" sub="from QBO invoices · click any column to sort">
        <CustomerTable
          customers={c.customers}
          ulineRevPerStop={c.uline.revPerStop}
          modeledCostPerStop={c.modeledCostPerStop}
          actualCostPerStop={c.actualCostPerStop}
          totalRevenue={c.totalRevenue}
        />
      </Section>

      {/* Trends */}
      {c.invoices.length > 0 && c.customers.length >= 2 && (
        <Section icon="📈" title="Monthly Revenue Trend" sub="top 8 customers">
          <CustomerTrendChart invoices={c.invoices} customers={c.customers} topN={8} />
        </Section>
      )}

      {/* Footer hints */}
      <div style={{ marginTop:24, padding:"14px 16px", background:T.bgSurface, borderRadius:T.radius, fontSize:11, color:T.textMuted, lineHeight:1.7 }}>
        <strong style={{ color:T.text }}>Reading the table:</strong>{" "}
        <em>vs Uline /Stop</em> compares each customer's average invoice to Uline's revenue-per-stop. Above 100% (green) means the customer pays more per touch than Uline; below 100% (red) means they pay less.{" "}
        <em>Modeled %</em> uses your Costs-tab structure; <em>Actual %</em> uses real costs from QBO P&L. Margin estimates for non-Uline customers assume one invoice ≈ one stop — refine as you learn customer profiles.
      </div>
    </div>
  );
}

window.CustomerAnalysisTab = CustomerAnalysisTab;

})();
