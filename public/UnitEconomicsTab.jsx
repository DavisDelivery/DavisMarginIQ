// UnitEconomicsTab.jsx — Cross-source unit economics
// v1.0.0 — Joins nuvizz_weekly + audited_financials_v2 + timeclock + payroll
//
// Answers the questions a CFO actually asks when revenue moves:
//   • "Is the revenue drop volume or price?"
//      → Decomposes Δrevenue into Δstops × $/stop and Δ$/stop × stops
//   • "Which drivers move the most revenue?"
//      → Stop counts × effective $/stop (from audited rev / total stops)
//   • "Which ZIPs / customers concentrate revenue and what's their margin?"
//      → Top-N concentration with revenue density
//   • "What's the true unit economics by week?"
//      → Audited revenue / NuVizz stops, with a labor cost overlay
//
// Data sources used:
//   • nuvizz_weekly       — weekly stops, top_drivers, top_customers (ifany)
//   • nuvizz_stops        — granular per-stop (loaded lazily for drill-down)
//   • audited_financials_v2 — true monthly revenue, line-item costs
//   • timeclock_weekly    — driver hours per week
//   • payroll_weekly      — W2 gross by driver per week
//   • ddis_payments       — payment $ per PRO (loaded lazily for true rev/stop)
//   • driver_classifications — w2 vs 1099 split
//
// Lazy-load pattern: page mount loads the small/medium aggregates only.
// Driver / ZIP / customer drill-downs query Firestore on click.

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

const fmt    = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmt2   = n => n==null||isNaN(n)?"$0.00":"$"+Number(n).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtK   = n => { if(n==null||isNaN(n))return"$0";const v=Number(n);if(Math.abs(v)>=1000000)return"$"+(v/1000000).toFixed(2)+"M";if(Math.abs(v)>=1000)return"$"+(v/1000).toFixed(1)+"K";return"$"+v.toFixed(0);};
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":(Number(n)>=0?"":"")+Number(n).toFixed(d)+"%";
const fmtPctSigned = (n,d=1) => n==null||isNaN(n)?"—":(n>=0?"+":"")+Number(n).toFixed(d)+"%";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

const card = { background:T.bgCard, borderRadius:T.radius, border:`1px solid ${T.border}`, padding:"16px", boxShadow:T.shadow };
const tblH = { padding:"8px 12px", textAlign:"left", fontSize:10, fontWeight:700, textTransform:"uppercase", color:T.textDim, background:T.bgSurface, borderBottom:`1px solid ${T.border}`, whiteSpace:"nowrap" };
const tblD = { padding:"8px 12px", fontSize:12, color:T.text, borderBottom:`1px solid ${T.borderLight}` };
const tblDR= { ...tblD, textAlign:"right", fontVariantNumeric:"tabular-nums" };

const db = () => window.db;
const hasFirebase = () => !!window.db;

// ── Loaders ────────────────────────────────────────────────────────────────

async function loadAggregates() {
  if (!hasFirebase()) return null;
  const [nvWeekly, audited, tcWeekly, prWeekly, classifications] = await Promise.all([
    db().collection("nuvizz_weekly").orderBy("week_ending", "asc").limit(200).get()
      .then(s => s.docs.map(d => ({ id:d.id, ...d.data() }))).catch(() => []),
    db().collection("audited_financials_v2").limit(120).get()
      .then(s => s.docs.map(d => ({ id:d.id, period:d.id, ...d.data() }))).catch(() => []),
    db().collection("timeclock_weekly").orderBy("week_ending", "asc").limit(200).get()
      .then(s => s.docs.map(d => ({ id:d.id, ...d.data() }))).catch(() => []),
    db().collection("payroll_weekly").orderBy("week_ending", "asc").limit(200).get()
      .then(s => s.docs.map(d => ({ id:d.id, ...d.data() }))).catch(() => []),
    db().collection("driver_classifications").get()
      .then(s => { const m={}; s.docs.forEach(d => m[d.id] = d.data()); return m; }).catch(() => ({})),
  ]);
  return { nvWeekly, audited, tcWeekly, prWeekly, classifications };
}

// Loads stops for a single driver, optionally limited to a date range.
// Used for driver drill-down. nuvizz_stops is large — must query by driver_name.
async function loadDriverStops(driverName, startDate, endDate) {
  if (!hasFirebase() || !driverName) return [];
  try {
    let q = db().collection("nuvizz_stops").where("driver_name", "==", driverName);
    if (startDate) q = q.where("delivery_date", ">=", startDate);
    if (endDate)   q = q.where("delivery_date", "<=", endDate);
    const s = await q.limit(5000).get();
    return s.docs.map(d => ({ id:d.id, ...d.data() }));
  } catch (e) {
    console.error("loadDriverStops failed:", e);
    return [];
  }
}

// Match DDIS payments by PRO. Stops carry pro; payments carry pro+paid_amount.
async function loadPaymentsForPros(pros) {
  if (!pros || !pros.length) return {};
  const proSet = new Set(pros.map(String));
  // Firestore "in" max is 30. Chunk and parallel.
  const chunks = [];
  const proArr = [...proSet];
  for (let i = 0; i < proArr.length; i += 30) chunks.push(proArr.slice(i, i+30));
  const result = {};
  await Promise.all(chunks.map(async chunk => {
    try {
      const s = await db().collection("ddis_payments").where("pro", "in", chunk).get();
      s.docs.forEach(d => {
        const v = d.data();
        const k = String(v.pro);
        if (!result[k]) result[k] = 0;
        result[k] += (v.paid_amount || 0);
      });
    } catch (e) { /* skip chunk */ }
  }));
  return result;
}

// ── Aggregation ────────────────────────────────────────────────────────────

// Roll weekly stops to monthly to align with audited financials grain.
// Each week_ending = Friday. We'll attribute the entire week to its month
// (the month of the Friday). Imperfect for week-spanning months but good
// enough for the kind of attribution analysis we're doing.
function weeksToMonthlyStops(nvWeekly) {
  const byMonth = {};
  for (const w of nvWeekly) {
    const we = w.week_ending; if (!we) continue;
    const month = we.slice(0, 7); // YYYY-MM
    if (!byMonth[month]) byMonth[month] = { month, stops:0, contractor_pay:0, weeks:0 };
    byMonth[month].stops += w.stops_completed || w.stops_total || 0;
    byMonth[month].contractor_pay += w.pay_base_total || 0;
    byMonth[month].weeks += 1;
  }
  return Object.values(byMonth).sort((a,b) => a.month.localeCompare(b.month));
}

// Build month-level join of audited revenue + stops + costs.
function buildMonthlyJoin(audited, monthlyStops) {
  const stopsBy = {};
  for (const m of monthlyStops) stopsBy[m.month] = m;

  return audited
    .filter(a => a.pl_totals)
    .map(a => {
      const period = a.period || a.id;
      const stops = stopsBy[period]?.stops || 0;
      const rev = a.pl_totals?.total_revenue?.month;
      const revVal = typeof rev === "number" ? rev
                  : (period.endsWith("-01") && typeof a.pl_totals?.total_revenue?.ytd === "number"
                     ? a.pl_totals.total_revenue.ytd : 0);
      // For costs: pull key categories at single-month granularity
      const ni = (() => {
        const m = a.pl_totals?.net_income?.month;
        if (typeof m === "number") return m;
        if (period.endsWith("-01") && typeof a.pl_totals?.net_income?.ytd === "number") return a.pl_totals.net_income.ytd;
        return 0;
      })();
      // Variable cost approximation: subcontractors + fuel + truck maintenance
      // (variable lines that scale with operations). Pull from line_items.
      const items = a.pl_line_items_v2 || [];
      const sumLine = (regex) => items
        .filter(li => regex.test(li.label || ""))
        .reduce((s, li) => {
          const v = (typeof li.month === "number") ? li.month
                : (period.endsWith("-01") && typeof li.ytd === "number") ? li.ytd : 0;
          return s + v;
        }, 0);
      const subcontractorCost = sumLine(/subcontractor/i);
      const fuelCost  = sumLine(/^fuel$/i);
      const truckMaint = sumLine(/truck maintenance/i);
      const insurance = sumLine(/^insurance$/i);
      const salaries  = sumLine(/^salaries$/i) + sumLine(/^salaries.*officer/i);
      return {
        period,
        stops,
        contractorPay: stopsBy[period]?.contractor_pay || 0,
        revenue: revVal,
        revPerStop: stops > 0 ? revVal / stops : null,
        netIncome: ni,
        marginPct: revVal > 0 ? ni / revVal * 100 : null,
        subcontractorCost,
        fuelCost,
        truckMaint,
        insurance,
        salaries,
      };
    })
    .sort((a,b) => a.period.localeCompare(b.period));
}

// Volume × Price decomposition between two periods for the revenue change.
// Δrev = Δstops × revPerStop_prior + Δ(rev/stop) × stops_current
// Splits into "volume effect" and "price effect".
function decomposeRevenueChange(monthly, periodA, periodB) {
  const a = monthly.find(m => m.period === periodA);
  const b = monthly.find(m => m.period === periodB);
  if (!a || !b) return null;
  const dStops = b.stops - a.stops;
  const volumeEffect = dStops * (a.revPerStop || 0);
  const dRevPerStop = (b.revPerStop || 0) - (a.revPerStop || 0);
  const priceEffect = dRevPerStop * b.stops;
  const totalDelta = b.revenue - a.revenue;
  // Reconciliation gap (rounding from the linear approximation)
  const gap = totalDelta - (volumeEffect + priceEffect);
  return {
    a, b, dStops, volumeEffect, dRevPerStop, priceEffect, totalDelta, gap,
  };
}

// Roll weekly nuvizz top_drivers field (list per week) into all-time totals
// for use without loading the full stops collection.
function aggregateDriversFromWeekly(nvWeekly, classifications, tcWeekly, prWeekly) {
  const byKey = {};
  const driverKey = (name) => String(name||"").trim().toLowerCase().replace(/[^a-z0-9]+/g,"_").replace(/^_+|_+$/g,"");

  for (const w of nvWeekly) {
    const top = w.top_drivers; // array of { driver_name, stops, pay_base } typically
    if (!Array.isArray(top)) continue;
    for (const td of top) {
      const name = td.driver_name || td.name; if (!name) continue;
      const key = driverKey(name);
      if (!byKey[key]) byKey[key] = { key, name, stops:0, contractor_pay:0, weeks_active: new Set() };
      byKey[key].stops += td.stops || 0;
      byKey[key].contractor_pay += td.pay_base || td.contractor_pay || 0;
      byKey[key].weeks_active.add(w.week_ending);
    }
  }

  // Join W2 hours/pay
  for (const w of tcWeekly) {
    if (!w.by_driver) continue;
    for (const [name, info] of Object.entries(w.by_driver)) {
      const key = driverKey(name);
      if (!byKey[key]) byKey[key] = { key, name, stops:0, contractor_pay:0, weeks_active: new Set() };
      byKey[key].hours = (byKey[key].hours || 0) + (info.total_hours || 0);
      byKey[key].ot_hours = (byKey[key].ot_hours || 0) + (info.ot_hours || 0);
    }
  }
  for (const w of prWeekly) {
    if (!w.by_driver) continue;
    for (const [name, info] of Object.entries(w.by_driver)) {
      const key = driverKey(name);
      if (!byKey[key]) byKey[key] = { key, name, stops:0, contractor_pay:0, weeks_active: new Set() };
      byKey[key].w2_gross = (byKey[key].w2_gross || 0) + (info.gross || 0);
    }
  }

  // Classification + cost-per-stop estimate
  return Object.values(byKey).map(d => {
    const cls = classifications[d.key]?.classification || (d.contractor_pay > 0 ? "1099" : (d.w2_gross > 0 ? "w2" : "?"));
    const total_cost = (d.contractor_pay || 0) + (d.w2_gross || 0);
    const cost_per_stop = d.stops > 0 ? total_cost / d.stops : null;
    return {
      ...d,
      classification: cls,
      weeks_active: d.weeks_active.size,
      total_cost,
      cost_per_stop,
      hours: d.hours || 0,
      ot_hours: d.ot_hours || 0,
      w2_gross: d.w2_gross || 0,
    };
  }).sort((a,b) => b.stops - a.stops);
}

// ── Components ─────────────────────────────────────────────────────────────

function KPI({ label, value, sub, subColor=T.textMuted, accent=T.text }) {
  return (
    <div style={{ ...card, padding:14, minWidth:120 }}>
      <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", marginBottom:4 }}>{label}</div>
      <div style={{ fontSize:20, fontWeight:800, color:accent }}>{value}</div>
      {sub && <div style={{ fontSize:10, color:subColor, marginTop:2 }}>{sub}</div>}
    </div>
  );
}

// Bar chart: monthly revenue + stops side-by-side dual axis
function MonthlyRevenueStopsChart({ monthly }) {
  if (monthly.length < 2) return null;
  const W = 900, H = 280, P = 50;
  const data = monthly.filter(m => m.revenue > 0 || m.stops > 0);
  const maxRev = Math.max(...data.map(d => d.revenue), 1);
  const maxStops = Math.max(...data.map(d => d.stops), 1);
  const xStep = (W - P*2) / Math.max(1, data.length);
  const barW = Math.max(2, xStep * 0.6);

  const yRevFor = (v) => H - P - (v / maxRev) * (H - P*2);
  const yStopsFor = (v) => H - P - (v / maxStops) * (H - P*2);
  const stopsPath = data.map((d, i) => `${i===0?"M":"L"} ${P + i*xStep + xStep/2} ${yStopsFor(d.stops)}`).join(" ");

  return (
    <div style={{ ...card, padding:16 }}>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"baseline", marginBottom:8 }}>
        <div>
          <div style={{ fontSize:13, fontWeight:700 }}>Monthly Revenue vs Stops</div>
          <div style={{ fontSize:11, color:T.textMuted }}>Bars = audited revenue · Line = NuVizz stops · {data.length} months</div>
        </div>
        <div style={{ display:"flex", gap:14 }}>
          <span style={{ fontSize:10, color:T.brand, fontWeight:600 }}>■ Revenue</span>
          <span style={{ fontSize:10, color:T.green, fontWeight:600 }}>━ Stops</span>
        </div>
      </div>
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display:"block" }}>
        {/* Y axis labels - revenue */}
        <text x={P-6} y={yRevFor(maxRev)+3} fontSize="9" fill={T.brand} textAnchor="end">${(maxRev/1e6).toFixed(1)}M</text>
        <text x={P-6} y={H-P+3} fontSize="9" fill={T.brand} textAnchor="end">$0</text>
        <text x={W-P+6} y={yStopsFor(maxStops)+3} fontSize="9" fill={T.green} textAnchor="start">{fmtNum(maxStops)}</text>
        <text x={W-P+6} y={H-P+3} fontSize="9" fill={T.green} textAnchor="start">0</text>
        {/* baseline */}
        <line x1={P} y1={H-P} x2={W-P} y2={H-P} stroke={T.borderLight} />
        {/* Revenue bars */}
        {data.map((d, i) => (
          <rect key={i} x={P + i*xStep + (xStep-barW)/2} y={yRevFor(d.revenue)} width={barW} height={H-P-yRevFor(d.revenue)}
                fill={T.brand} opacity={0.7}>
            <title>{d.period}: {fmt(d.revenue)} rev · {fmtNum(d.stops)} stops · {d.revPerStop != null ? fmt(d.revPerStop) : "—"}/stop</title>
          </rect>
        ))}
        {/* Stops line */}
        <path d={stopsPath} fill="none" stroke={T.green} strokeWidth="1.8" />
        {data.map((d, i) => (
          <circle key={i} cx={P + i*xStep + xStep/2} cy={yStopsFor(d.stops)} r="2.5" fill={T.green} />
        ))}
        {/* X axis labels - first of each year + last */}
        {data.map((d, i) => {
          const m = d.period.split("-")[1];
          const y = d.period.split("-")[0];
          if (m === "01" || i === 0 || i === data.length-1) {
            return <text key={`x${i}`} x={P + i*xStep + xStep/2} y={H-P+14} fontSize="9" fill={T.textMuted} textAnchor="middle">{y}{m==="01"?`-${m}`:""}</text>;
          }
          return null;
        })}
      </svg>
    </div>
  );
}

// Volume × Price decomposition card
function VolumePriceDecomposition({ monthly }) {
  const [periodA, setPeriodA] = useState(null);
  const [periodB, setPeriodB] = useState(null);

  useEffect(() => {
    if (monthly.length < 2) return;
    const sorted = [...monthly].sort((a,b) => b.period.localeCompare(a.period));
    const latest = sorted[0];
    const oneYearAgo = sorted.find(m => {
      const [ya, ma] = m.period.split("-").map(x => parseInt(x,10));
      const [yb, mb] = latest.period.split("-").map(x => parseInt(x,10));
      return ma === mb && ya === yb - 1;
    });
    setPeriodA(oneYearAgo?.period || sorted[1]?.period);
    setPeriodB(latest.period);
  }, [monthly]);

  if (!periodA || !periodB) return null;

  const decomp = decomposeRevenueChange(monthly, periodA, periodB);
  if (!decomp) return null;

  const { a, b, dStops, volumeEffect, dRevPerStop, priceEffect, totalDelta, gap } = decomp;
  const allPeriods = [...monthly].map(m => m.period).sort();

  // Bar chart showing the decomposition
  const W = 600, H = 200, P = 40;
  const maxAbs = Math.max(Math.abs(volumeEffect), Math.abs(priceEffect), Math.abs(totalDelta), 1);
  const barH = 30;
  const yFor = (i) => 30 + i*45;
  const xFor = (v) => {
    const center = W/2;
    const halfRange = (W - P*2) / 2;
    return center + (v/maxAbs) * halfRange;
  };

  const renderBar = (label, value, color, y) => (
    <g key={label}>
      <text x={P-4} y={y+barH/2+4} fontSize="11" fill={T.text} textAnchor="end" fontWeight="600">{label}</text>
      <line x1={W/2} y1={y} x2={W/2} y2={y+barH} stroke={T.border} />
      <rect x={value >= 0 ? W/2 : xFor(value)} y={y} width={Math.abs(xFor(value) - W/2)} height={barH} fill={color} opacity="0.85" />
      <text x={value >= 0 ? xFor(value)+6 : xFor(value)-6} y={y+barH/2+4} fontSize="11" fontWeight="700"
            textAnchor={value >= 0 ? "start" : "end"} fill={T.text}>
        {value >= 0 ? "+" : "-"}{fmtK(Math.abs(value))}
      </text>
    </g>
  );

  return (
    <div style={{ ...card, padding:16 }}>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"baseline", marginBottom:12 }}>
        <div>
          <div style={{ fontSize:13, fontWeight:700 }}>Why Did Revenue Change? — Volume × Price Decomposition</div>
          <div style={{ fontSize:11, color:T.textMuted }}>
            Splits Δrevenue into {"\""}did we do more/fewer stops{"\""} (volume) vs {"\""}did each stop pay more/less{"\""} (price)
          </div>
        </div>
      </div>

      {/* Period selectors */}
      <div style={{ display:"flex", gap:12, alignItems:"center", marginBottom:12, fontSize:11 }}>
        <span style={{ color:T.textMuted, fontWeight:600 }}>Compare</span>
        <select value={periodA} onChange={e => setPeriodA(e.target.value)} style={{ padding:"4px 8px", border:`1px solid ${T.border}`, borderRadius:6, fontSize:11 }}>
          {allPeriods.map(p => <option key={p} value={p}>{p}</option>)}
        </select>
        <span style={{ color:T.textMuted, fontWeight:600 }}>vs</span>
        <select value={periodB} onChange={e => setPeriodB(e.target.value)} style={{ padding:"4px 8px", border:`1px solid ${T.border}`, borderRadius:6, fontSize:11 }}>
          {allPeriods.map(p => <option key={p} value={p}>{p}</option>)}
        </select>
      </div>

      {/* Side-by-side period summary */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:10, marginBottom:14 }}>
        <div style={{ ...card, padding:10, background:T.bgSurface }}>
          <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase" }}>{periodA} (baseline)</div>
          <div style={{ fontSize:14, fontWeight:700 }}>{fmt(a.revenue)} rev · {fmtNum(a.stops)} stops</div>
          <div style={{ fontSize:11, color:T.textMuted }}>{a.revPerStop != null ? fmt(a.revPerStop) : "—"}/stop</div>
        </div>
        <div style={{ ...card, padding:10, background:T.bgSurface }}>
          <div style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase" }}>{periodB}</div>
          <div style={{ fontSize:14, fontWeight:700 }}>{fmt(b.revenue)} rev · {fmtNum(b.stops)} stops</div>
          <div style={{ fontSize:11, color:T.textMuted }}>{b.revPerStop != null ? fmt(b.revPerStop) : "—"}/stop</div>
        </div>
      </div>

      <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display:"block" }}>
        {renderBar(`Volume effect (${dStops>=0?"+":""}${fmtNum(dStops)} stops)`, volumeEffect, dStops >= 0 ? T.green : T.red, yFor(0))}
        {renderBar(`Price effect (${dRevPerStop>=0?"+":""}${fmt(dRevPerStop)}/stop)`, priceEffect, dRevPerStop >= 0 ? T.green : T.red, yFor(1))}
        {renderBar("Total Δ revenue", totalDelta, totalDelta >= 0 ? T.green : T.red, yFor(2))}
      </svg>

      <div style={{ paddingTop:8, borderTop:`1px solid ${T.borderLight}`, fontSize:11, color:T.textMuted }}>
        {Math.abs(volumeEffect) > Math.abs(priceEffect) ? (
          <>Revenue change is mostly driven by <strong style={{color: dStops >= 0 ? T.green : T.red}}>volume</strong> ({((Math.abs(volumeEffect)/(Math.abs(volumeEffect)+Math.abs(priceEffect)))*100).toFixed(0)}% of move). {dStops >= 0 ? "Did more stops" : "Did fewer stops"}.</>
        ) : (
          <>Revenue change is mostly driven by <strong style={{color: dRevPerStop >= 0 ? T.green : T.red}}>price/mix</strong> ({((Math.abs(priceEffect)/(Math.abs(volumeEffect)+Math.abs(priceEffect)))*100).toFixed(0)}% of move). {dRevPerStop >= 0 ? "Each stop paid more" : "Each stop paid less"}.</>
        )}
        {Math.abs(gap) > 100 && <span style={{ color:T.textDim, marginLeft:8 }}>(±{fmtK(gap)} reconciliation rounding)</span>}
      </div>
    </div>
  );
}

// Driver economics matrix
function DriverEconomicsTable({ drivers, totalRev, totalStops, onSelectDriver }) {
  const [sortKey, setSortKey] = useState("stops");
  const [sortDir, setSortDir] = useState(-1);

  const avgRevPerStop = totalStops > 0 ? totalRev / totalStops : null;

  // Each driver's estimated revenue contribution = stops × company-wide avg rev/stop
  const enriched = useMemo(() => drivers.map(d => {
    const estRev = avgRevPerStop != null ? d.stops * avgRevPerStop : null;
    const grossMargin = estRev != null ? estRev - d.total_cost : null;
    const grossMarginPct = (estRev && estRev > 0) ? grossMargin / estRev * 100 : null;
    return { ...d, estRev, grossMargin, grossMarginPct };
  }), [drivers, avgRevPerStop]);

  const sorted = useMemo(() => {
    return [...enriched].sort((a,b) => {
      const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
      if (typeof av === "string") return sortDir * (av < bv ? -1 : av > bv ? 1 : 0);
      return sortDir * (bv - av);
    });
  }, [enriched, sortKey, sortDir]);

  const SortTh = ({ k, label, hint }) => (
    <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} title={hint}
        onClick={() => { if(sortKey===k) setSortDir(-sortDir); else { setSortKey(k); setSortDir(-1); } }}>
      {label}{sortKey===k?(sortDir===-1?" ↓":" ↑"):""}
    </th>
  );

  return (
    <div style={{ ...card, padding:0, overflow:"auto" }}>
      <div style={{ padding:"12px 16px", borderBottom:`1px solid ${T.border}` }}>
        <div style={{ fontSize:13, fontWeight:700 }}>Driver Economics</div>
        <div style={{ fontSize:11, color:T.textMuted }}>
          Estimated revenue = stops × company-wide avg ${avgRevPerStop != null ? avgRevPerStop.toFixed(2) : "—"}/stop. Click any driver for true revenue from DDIS payments.
        </div>
      </div>
      <table style={{ width:"100%", borderCollapse:"collapse", minWidth:1100 }}>
        <thead>
          <tr>
            <th style={{ ...tblH, cursor:"pointer" }} onClick={() => { if(sortKey==="name") setSortDir(-sortDir); else { setSortKey("name"); setSortDir(1); } }}>Driver{sortKey==="name"?(sortDir===-1?" ↓":" ↑"):""}</th>
            <th style={tblH}>Class</th>
            <SortTh k="stops" label="Stops" />
            <SortTh k="weeks_active" label="Weeks" hint="Distinct weeks with stops" />
            <SortTh k="estRev" label="Est Revenue" hint="stops × company avg $/stop" />
            <SortTh k="contractor_pay" label="1099 Pay" hint="Contractor pay base from NuVizz" />
            <SortTh k="w2_gross" label="W2 Gross" hint="Total W2 gross from payroll" />
            <SortTh k="hours" label="Hours" />
            <SortTh k="cost_per_stop" label="Cost/Stop" hint="(1099 pay + W2 gross) ÷ stops" />
            <SortTh k="grossMargin" label="Gross Margin $" hint="Est revenue − total driver cost" />
            <SortTh k="grossMarginPct" label="Margin %" />
          </tr>
        </thead>
        <tbody>
          {sorted.slice(0, 60).map(d => {
            const clsColor = d.classification === "1099" ? T.purple : d.classification === "w2" ? T.brand : T.textMuted;
            const marginColor = d.grossMarginPct == null ? T.textMuted : d.grossMarginPct >= 70 ? T.green : d.grossMarginPct >= 50 ? T.yellow : T.red;
            return (
              <tr key={d.key} style={{ cursor:"pointer" }} onClick={() => onSelectDriver(d)}>
                <td style={{ ...tblD, fontWeight:600 }}>{d.name}</td>
                <td style={tblD}>
                  <span style={{ fontSize:9, fontWeight:700, padding:"2px 6px", borderRadius:8, background:clsColor, color:"#fff" }}>{(d.classification||"?").toUpperCase()}</span>
                </td>
                <td style={tblDR}>{fmtNum(d.stops)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{d.weeks_active}</td>
                <td style={{ ...tblDR, fontWeight:600 }}>{d.estRev != null ? fmtK(d.estRev) : "—"}</td>
                <td style={tblDR}>{d.contractor_pay > 0 ? fmtK(d.contractor_pay) : "—"}</td>
                <td style={tblDR}>{d.w2_gross > 0 ? fmtK(d.w2_gross) : "—"}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{d.hours > 0 ? d.hours.toFixed(0)+"h" : "—"}</td>
                <td style={tblDR}>{d.cost_per_stop != null ? fmt2(d.cost_per_stop) : "—"}</td>
                <td style={{ ...tblDR, color: d.grossMargin != null ? (d.grossMargin >= 0 ? T.green : T.red) : T.textMuted, fontWeight:600 }}>
                  {d.grossMargin != null ? fmtK(d.grossMargin) : "—"}
                </td>
                <td style={{ ...tblDR, color:marginColor, fontWeight:600 }}>{d.grossMarginPct != null ? d.grossMarginPct.toFixed(0)+"%" : "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// Driver drill-down modal: loads stops + DDIS payments to show TRUE revenue per stop
function DriverDrillModal({ driver, onClose }) {
  const [loading, setLoading] = useState(true);
  const [stops, setStops] = useState([]);
  const [paymentsByPro, setPaymentsByPro] = useState({});
  const [loadErr, setLoadErr] = useState("");

  useEffect(() => {
    if (!driver) return;
    (async () => {
      setLoading(true); setLoadErr("");
      try {
        // Last 90 days of stops to keep load reasonable
        const today = new Date();
        const end = today.toISOString().slice(0,10);
        const startD = new Date(today.getTime() - 90*86400000);
        const start = startD.toISOString().slice(0,10);
        const s = await loadDriverStops(driver.name, start, end);
        setStops(s);
        // Match payments
        const pros = s.map(x => x.pro).filter(Boolean);
        const payments = await loadPaymentsForPros(pros);
        setPaymentsByPro(payments);
      } catch (e) {
        setLoadErr(e.message || "Failed to load");
      }
      setLoading(false);
    })();
  }, [driver]);

  if (!driver) return null;

  // Compute true unit economics for the loaded window
  let trueRev = 0, matchedStops = 0, unmatchedStops = 0;
  let totalContractorPay = 0;
  const stopsByZip = {};
  const stopsByCity = {};
  const stopsByCustomer = {};
  for (const s of stops) {
    const proKey = String(s.pro || "");
    const matched = paymentsByPro[proKey];
    if (matched != null) {
      trueRev += matched;
      matchedStops += 1;
    } else {
      unmatchedStops += 1;
    }
    totalContractorPay += s.contractor_pay_base || 0;
    if (s.zip) {
      if (!stopsByZip[s.zip]) stopsByZip[s.zip] = { zip: s.zip, stops:0, rev:0, contractor_pay:0 };
      stopsByZip[s.zip].stops += 1;
      stopsByZip[s.zip].rev += matched || 0;
      stopsByZip[s.zip].contractor_pay += s.contractor_pay_base || 0;
    }
    if (s.city) {
      if (!stopsByCity[s.city]) stopsByCity[s.city] = { city: s.city, stops:0, rev:0 };
      stopsByCity[s.city].stops += 1;
      stopsByCity[s.city].rev += matched || 0;
    }
    if (s.ship_to) {
      if (!stopsByCustomer[s.ship_to]) stopsByCustomer[s.ship_to] = { customer: s.ship_to, stops:0, rev:0 };
      stopsByCustomer[s.ship_to].stops += 1;
      stopsByCustomer[s.ship_to].rev += matched || 0;
    }
  }
  const matchRate = stops.length > 0 ? matchedStops / stops.length * 100 : 0;
  const trueRevPerStop = matchedStops > 0 ? trueRev / matchedStops : null;
  const trueGrossMargin = trueRev - totalContractorPay;
  const trueGrossMarginPct = trueRev > 0 ? trueGrossMargin / trueRev * 100 : null;

  const topZips = Object.values(stopsByZip).sort((a,b) => b.stops - a.stops).slice(0, 10);
  const topCities = Object.values(stopsByCity).sort((a,b) => b.stops - a.stops).slice(0, 10);
  const topCustomers = Object.values(stopsByCustomer).sort((a,b) => b.stops - a.stops).slice(0, 10);

  return (
    <div onClick={onClose} style={{
      position:"fixed", top:0, left:0, right:0, bottom:0, background:"rgba(0,0,0,0.5)",
      display:"flex", alignItems:"center", justifyContent:"center", zIndex:9999, padding:20,
    }}>
      <div onClick={e => e.stopPropagation()} style={{
        background:T.bgWhite, borderRadius:T.radius, padding:24,
        maxWidth:1100, width:"100%", maxHeight:"90vh", overflow:"auto",
      }}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:16 }}>
          <div>
            <div style={{ fontSize:20, fontWeight:700 }}>{driver.name}</div>
            <div style={{ fontSize:12, color:T.textMuted, marginTop:2 }}>
              Last 90 days · {fmtNum(stops.length)} stops loaded · {matchRate.toFixed(0)}% matched to DDIS payments
            </div>
          </div>
          <button onClick={onClose} style={{ border:"none", background:"transparent", fontSize:20, color:T.textMuted, cursor:"pointer" }}>✕</button>
        </div>

        {loading ? (
          <div style={{ textAlign:"center", padding:40, color:T.textMuted }}>Loading stops + payments…</div>
        ) : loadErr ? (
          <div style={{ ...card, color:T.red, fontSize:12 }}>⚠️ {loadErr}</div>
        ) : (
          <>
            {/* True economics KPIs */}
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:8, marginBottom:16 }}>
              <KPI label="Stops (90d)" value={fmtNum(stops.length)} sub={`${matchedStops} matched, ${unmatchedStops} unmatched`} />
              <KPI label="True Revenue" value={fmtK(trueRev)} sub={trueRevPerStop != null ? fmt(trueRevPerStop)+"/stop" : "—"} accent={T.brand} />
              <KPI label="Contractor Pay" value={fmtK(totalContractorPay)} sub={stops.length > 0 ? fmt(totalContractorPay/stops.length)+"/stop avg" : ""} accent={T.red} />
              <KPI label="Gross Margin" value={fmtK(trueGrossMargin)} sub={trueGrossMarginPct != null ? trueGrossMarginPct.toFixed(0)+"% margin" : "—"} accent={trueGrossMargin >= 0 ? T.green : T.red} />
            </div>

            {/* Top ZIPs / Cities / Customers */}
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(280px, 1fr))", gap:12 }}>
              <div style={{ ...card, padding:0, overflow:"hidden" }}>
                <div style={{ padding:"10px 14px", fontSize:11, fontWeight:700, background:T.bgSurface, borderBottom:`1px solid ${T.border}` }}>Top ZIPs</div>
                <table style={{ width:"100%", borderCollapse:"collapse" }}>
                  <thead><tr>
                    <th style={tblH}>ZIP</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Stops</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
                    <th style={{ ...tblH, textAlign:"right" }}>$/Stop</th>
                  </tr></thead>
                  <tbody>
                    {topZips.map(z => (
                      <tr key={z.zip}>
                        <td style={tblD}>{z.zip}</td>
                        <td style={tblDR}>{fmtNum(z.stops)}</td>
                        <td style={tblDR}>{z.rev > 0 ? fmtK(z.rev) : "—"}</td>
                        <td style={{ ...tblDR, color:T.textMuted }}>{z.rev > 0 ? fmt(z.rev/z.stops) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div style={{ ...card, padding:0, overflow:"hidden" }}>
                <div style={{ padding:"10px 14px", fontSize:11, fontWeight:700, background:T.bgSurface, borderBottom:`1px solid ${T.border}` }}>Top Cities</div>
                <table style={{ width:"100%", borderCollapse:"collapse" }}>
                  <thead><tr>
                    <th style={tblH}>City</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Stops</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
                  </tr></thead>
                  <tbody>
                    {topCities.map(c => (
                      <tr key={c.city}>
                        <td style={tblD}>{c.city}</td>
                        <td style={tblDR}>{fmtNum(c.stops)}</td>
                        <td style={tblDR}>{c.rev > 0 ? fmtK(c.rev) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div style={{ ...card, padding:0, overflow:"hidden" }}>
                <div style={{ padding:"10px 14px", fontSize:11, fontWeight:700, background:T.bgSurface, borderBottom:`1px solid ${T.border}` }}>Top Customers</div>
                <table style={{ width:"100%", borderCollapse:"collapse" }}>
                  <thead><tr>
                    <th style={tblH}>Customer</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Stops</th>
                    <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
                  </tr></thead>
                  <tbody>
                    {topCustomers.map(c => (
                      <tr key={c.customer}>
                        <td style={tblD}>{c.customer.length > 28 ? c.customer.slice(0,28)+"…" : c.customer}</td>
                        <td style={tblDR}>{fmtNum(c.stops)}</td>
                        <td style={tblDR}>{c.rev > 0 ? fmtK(c.rev) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ── Stop Economics Rollup Panel ───────────────────────────────────────────
// Reads from stop_economics_zip / stop_economics_driver / stop_economics_customer
// (computed server-side by marginiq-stop-economics function). Provides ZIP-level
// profitability matrix and a "Run Rollup" button to trigger fresh computation.

function StopEconomicsPanel({ availableMonths }) {
  const [month, setMonth] = useState(availableMonths[0] || "");
  const [zipData, setZipData] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [statusMsg, setStatusMsg] = useState("");
  const [view, setView] = useState("zip"); // zip | driver | customer
  const [sortKey, setSortKey] = useState("stops_total");
  const [sortDir, setSortDir] = useState(-1);

  const loadRollup = useCallback(async (m) => {
    if (!hasFirebase() || !m) return;
    setLoading(true); setStatusMsg("");
    try {
      // Query zip/driver/customer rollups for this month using `month` field
      const [zipSnap, drvSnap, custSnap, sumSnap] = await Promise.all([
        db().collection("stop_economics_zip").where("month", "==", m).limit(2000).get().catch(() => null),
        db().collection("stop_economics_driver").where("month", "==", m).limit(500).get().catch(() => null),
        db().collection("stop_economics_customer").where("month", "==", m).limit(2000).get().catch(() => null),
        db().collection("stop_economics_summary").doc(m).get().catch(() => null),
      ]);
      const zips = zipSnap ? zipSnap.docs.map(d => ({ id:d.id, ...d.data() })) : [];
      const drvs = drvSnap ? drvSnap.docs.map(d => ({ id:d.id, ...d.data() })) : [];
      const csts = custSnap ? custSnap.docs.map(d => ({ id:d.id, ...d.data() })) : [];
      const sm   = sumSnap && sumSnap.exists ? sumSnap.data() : null;
      setZipData({ zip: zips, driver: drvs, customer: csts });
      setSummary(sm);
      if (zips.length === 0 && drvs.length === 0 && !sm) {
        setStatusMsg(`No rollup data for ${m}. Click "Run Rollup" to compute.`);
      }
    } catch (e) {
      setStatusMsg(`Load failed: ${e.message || e}`);
    }
    setLoading(false);
  }, []);

  useEffect(() => { if (month) loadRollup(month); }, [month, loadRollup]);

  const runRollup = useCallback(async (testMode) => {
    if (!month) return;
    setRunning(true); setStatusMsg(testMode ? "Dispatching dry-run…" : "Dispatching rollup…");
    try {
      const r = await fetch("/.netlify/functions/marginiq-stop-economics", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ month, test: testMode }),
      });
      const j = await r.json();
      if (!j.ok && r.status !== 202) {
        setStatusMsg(`Failed: ${j.error || "unknown"}`);
        setRunning(false);
        return;
      }
      // Background dispatched. Poll the status doc every 5s up to 3 minutes.
      setStatusMsg(`Background ${testMode ? "dry-run" : "rollup"} started for ${month}. Polling for completion…`);
      const maxAttempts = 36; // 3 min @ 5s
      for (let i = 0; i < maxAttempts; i++) {
        await new Promise(r => setTimeout(r, 5000));
        try {
          const stat = await db().collection("marginiq_config").doc("stop_economics_status").get();
          if (stat.exists) {
            const sd = stat.data() || {};
            if (sd.month === month && sd.state === "complete") {
              const summary = JSON.parse(sd.result_summary || "{}");
              setStatusMsg(`✓ Done in ${(summary.elapsed_ms/1000).toFixed(1)}s · ${summary.stops_read} stops · ${summary.zips} ZIPs · ${summary.drivers} drivers · ${summary.customers} customers · ${summary.payments_matched} Uline PROs paid`);
              if (!testMode) await loadRollup(month);
              break;
            }
            if (sd.month === month && sd.state === "failed") {
              setStatusMsg(`Failed: ${sd.error || "unknown"}`);
              break;
            }
            if (sd.month === month && sd.state === "running") {
              setStatusMsg(`Running ${month}… (poll ${i+1}/${maxAttempts})`);
            }
          }
        } catch (e) { /* keep polling */ }
        if (i === maxAttempts - 1) {
          setStatusMsg(`Still running after 3 minutes. Click Refresh to check later.`);
        }
      }
    } catch (e) {
      setStatusMsg(`Run failed: ${e.message || e}`);
    }
    setRunning(false);
  }, [month, loadRollup]);

  const rows = (zipData[view] || []);
  const sorted = useMemo(() => {
    return [...rows].sort((a,b) => {
      const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
      if (typeof av === "string") return sortDir * (av < bv ? -1 : av > bv ? 1 : 0);
      return sortDir * (bv - av);
    });
  }, [rows, sortKey, sortDir]);

  const setSort = (k) => {
    if (sortKey === k) setSortDir(-sortDir);
    else { setSortKey(k); setSortDir(-1); }
  };
  const sortIcon = (k) => sortKey === k ? (sortDir === -1 ? " ↓" : " ↑") : "";

  // Threshold flags for ZIPs needing price increases (margin < 50% or rev/stop < $50)
  const threshold = (r) => {
    const lowMargin = r.gross_margin_pct != null && r.gross_margin_pct < 50;
    const lowRev = r.rev_per_stop != null && r.rev_per_stop < 50;
    if (lowMargin && lowRev) return { color: T.red, label: "❗ Price increase needed" };
    if (lowMargin || lowRev) return { color: T.yellow, label: "⚠️ Watch" };
    return null;
  };

  return (
    <div style={{ ...card, padding:0, marginBottom:16, overflow:"hidden" }}>
      <div style={{ padding:"14px 16px", borderBottom:`1px solid ${T.border}` }}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", flexWrap:"wrap", gap:12 }}>
          <div>
            <div style={{ fontSize:14, fontWeight:700 }}>📐 Stop Economics — ZIP / Driver / Customer Profitability</div>
            <div style={{ fontSize:11, color:T.textMuted, marginTop:2 }}>
              Server-side rollup joining stops × payments × classifications. <strong>Uline revenue from DDIS payments + unpaid_stops</strong>; <strong>non-Uline revenue implied from contractor pay ÷ 0.4</strong> (no rate card yet). Revenue attributed to the month the <strong>stop was delivered</strong> (not when it was billed) — won't tie exactly to audited financials month-to-month due to billing timing.
            </div>
          </div>
          <div style={{ display:"flex", gap:8, alignItems:"center", flexWrap:"wrap" }}>
            <select value={month} onChange={e => setMonth(e.target.value)}
                    style={{ padding:"6px 10px", border:`1px solid ${T.border}`, borderRadius:6, fontSize:11 }}>
              {availableMonths.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
            <button onClick={() => runRollup(true)} disabled={running}
                    style={{ padding:"6px 12px", borderRadius:6, border:`1px solid ${T.border}`, background:T.bgWhite, color:T.text, fontSize:11, fontWeight:600, cursor: running ? "wait" : "pointer", opacity: running ? 0.6 : 1 }}>
              👁 Dry-Run
            </button>
            <button onClick={() => runRollup(false)} disabled={running}
                    style={{ padding:"6px 12px", borderRadius:6, border:"none", background:T.brand, color:"#fff", fontSize:11, fontWeight:700, cursor: running ? "wait" : "pointer", opacity: running ? 0.6 : 1 }}>
              {running ? "Running…" : "▶ Run Rollup"}
            </button>
          </div>
        </div>
        {statusMsg && (
          <div style={{ marginTop:10, padding:"8px 12px", background:T.bgSurface, borderRadius:6, fontSize:11, color:T.textMuted }}>
            {statusMsg}
          </div>
        )}
      </div>

      {/* Summary KPIs from monthly rollup */}
      {summary && (
        <div style={{ padding:"12px 16px", borderBottom:`1px solid ${T.border}`, background:T.bgSurface }}>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:10 }}>
            <KPI label="Total Stops" value={fmtNum(summary.stops_total)} sub={`${summary.uline_stops} Uline / ${summary.nonuline_stops} other`} />
            <KPI label="Uline Revenue" value={fmtK(summary.uline_revenue)} sub={`${summary.uline_revenue_paid > 0 ? fmtK(summary.uline_revenue_paid)+" paid" : ""}${summary.uline_revenue_unpaid > 0 ? " + "+fmtK(summary.uline_revenue_unpaid)+" unpaid" : ""}`} accent={T.brand} />
            <KPI label="Non-Uline Implied" value={fmtK(summary.nonuline_revenue_implied)} sub="contractor_pay ÷ 0.4" />
            <KPI label="Total Revenue" value={fmtK(summary.total_revenue)} sub={summary.rev_per_stop != null ? fmt(summary.rev_per_stop)+"/stop" : ""} accent={T.green} />
            <KPI label="Gross Margin" value={fmtK(summary.gross_margin)} sub={summary.gross_margin_pct != null ? summary.gross_margin_pct.toFixed(0)+"%" : ""} accent={summary.gross_margin >= 0 ? T.green : T.red} />
          </div>
        </div>
      )}

      {/* View tabs */}
      {(zipData.zip?.length > 0 || zipData.driver?.length > 0 || zipData.customer?.length > 0) && (
        <>
          <div style={{ display:"flex", gap:4, padding:"10px 16px", borderBottom:`1px solid ${T.border}` }}>
            {[
              { id:"zip", label:`ZIPs (${zipData.zip?.length || 0})` },
              { id:"driver", label:`Drivers (${zipData.driver?.length || 0})` },
              { id:"customer", label:`Customers (${zipData.customer?.length || 0})` },
            ].map(v => (
              <button key={v.id} onClick={() => setView(v.id)} style={{
                padding:"6px 12px", borderRadius:6, border:"none",
                background: view === v.id ? T.brand : "transparent",
                color: view === v.id ? "#fff" : T.textMuted,
                fontSize:11, fontWeight: view === v.id ? 700 : 500, cursor:"pointer",
              }}>
                {v.label}
              </button>
            ))}
          </div>

          {/* Matrix */}
          <div style={{ overflow:"auto", maxHeight:600 }}>
            <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
              <thead style={{ position:"sticky", top:0, zIndex:1 }}>
                <tr>
                  <th style={{ ...tblH, cursor:"pointer" }} onClick={() => setSort(view === "zip" ? "zip" : view === "driver" ? "driver_name" : "customer")}>
                    {view === "zip" ? "ZIP" : view === "driver" ? "Driver" : "Customer"}
                    {sortIcon(view === "zip" ? "zip" : view === "driver" ? "driver_name" : "customer")}
                  </th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("stops_total")}>Stops{sortIcon("stops_total")}</th>
                  <th style={{ ...tblH, textAlign:"right" }}>Uline / Other</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("uline_revenue")}>Uline Rev{sortIcon("uline_revenue")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("nonuline_revenue_implied")}>Non-U Implied{sortIcon("nonuline_revenue_implied")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("total_revenue")}>Total Rev{sortIcon("total_revenue")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("rev_per_stop")}>$/Stop{sortIcon("rev_per_stop")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("contractor_cost_at_40")}>Driver Cost{sortIcon("contractor_cost_at_40")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("gross_margin")}>Margin $ {sortIcon("gross_margin")}</th>
                  <th style={{ ...tblH, textAlign:"right", cursor:"pointer" }} onClick={() => setSort("gross_margin_pct")}>Margin %{sortIcon("gross_margin_pct")}</th>
                  <th style={tblH}>Flag</th>
                </tr>
              </thead>
              <tbody>
                {sorted.slice(0, 200).map((r, i) => {
                  const flag = threshold(r);
                  return (
                    <tr key={r.id || i}>
                      <td style={{ ...tblD, fontWeight:600 }}>
                        {view === "zip" ? r.zip : view === "driver" ? r.driver_name : (r.customer || r.customer_key)}
                      </td>
                      <td style={tblDR}>{fmtNum(r.stops_total)}</td>
                      <td style={{ ...tblDR, color:T.textMuted, fontSize:10 }}>{r.uline_stops || 0} / {r.nonuline_stops || 0}</td>
                      <td style={{ ...tblDR, color: r.uline_revenue > 0 ? T.brand : T.textDim }}>{r.uline_revenue > 0 ? fmtK(r.uline_revenue) : "—"}</td>
                      <td style={{ ...tblDR, color: r.nonuline_revenue_implied > 0 ? T.textMuted : T.textDim, fontStyle:"italic" }}>{r.nonuline_revenue_implied > 0 ? fmtK(r.nonuline_revenue_implied) : "—"}</td>
                      <td style={{ ...tblDR, fontWeight:600 }}>{fmtK(r.total_revenue)}</td>
                      <td style={{ ...tblDR, fontWeight:600 }}>{r.rev_per_stop != null ? fmt(r.rev_per_stop) : "—"}</td>
                      <td style={{ ...tblDR, color:T.red }}>{fmtK(r.contractor_cost_at_40)}</td>
                      <td style={{ ...tblDR, color: r.gross_margin >= 0 ? T.green : T.red, fontWeight:600 }}>{fmtK(r.gross_margin)}</td>
                      <td style={{ ...tblDR, color: r.gross_margin_pct == null ? T.textMuted : r.gross_margin_pct >= 70 ? T.green : r.gross_margin_pct >= 50 ? T.yellow : T.red, fontWeight:700 }}>
                        {r.gross_margin_pct != null ? r.gross_margin_pct.toFixed(0) + "%" : "—"}
                      </td>
                      <td style={tblD}>
                        {flag ? <span style={{ fontSize:9, fontWeight:700, color:flag.color }}>{flag.label}</span> : ""}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {sorted.length > 200 && (
            <div style={{ padding:"8px 16px", fontSize:10, color:T.textMuted, borderTop:`1px solid ${T.borderLight}` }}>
              Showing top 200 of {sorted.length}. Sort columns to see different slices.
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── Main tab ──────────────────────────────────────────────────────────────

function UnitEconomicsTab() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadErr, setLoadErr] = useState("");
  const [selectedDriver, setSelectedDriver] = useState(null);

  const load = useCallback(async () => {
    setLoading(true); setLoadErr("");
    try {
      const d = await loadAggregates();
      setData(d);
    } catch (e) {
      setLoadErr(e.message || "Failed to load");
    }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading) return (
    <div style={{ textAlign:"center", padding:"60px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:12 }}>🔄</div>
      <div style={{ fontSize:14 }}>Loading unit economics…</div>
    </div>
  );

  if (loadErr) return (
    <div style={{ padding:20 }}>
      <div style={{ ...card, color:T.red, fontSize:12 }}>⚠️ {loadErr}</div>
    </div>
  );

  if (!data) return null;

  const monthlyStops = weeksToMonthlyStops(data.nvWeekly);
  const monthly = buildMonthlyJoin(data.audited, monthlyStops);
  const drivers = aggregateDriversFromWeekly(data.nvWeekly, data.classifications, data.tcWeekly, data.prWeekly);

  const totalRev = monthly.reduce((s, m) => s + (m.revenue || 0), 0);
  const totalStops = monthly.reduce((s, m) => s + (m.stops || 0), 0);

  // KPI summary
  const recentMonths = monthly.slice(-3);
  const avgRevPerStop = totalStops > 0 ? totalRev / totalStops : null;
  const recentRev = recentMonths.reduce((s, m) => s + m.revenue, 0);
  const recentStops = recentMonths.reduce((s, m) => s + m.stops, 0);
  const recentRevPerStop = recentStops > 0 ? recentRev / recentStops : null;

  return (
    <div style={{ padding:"20px", maxWidth:1400, margin:"0 auto" }} className="fade-in">
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:18 }}>🧮</span>
          <span style={{ fontSize:15, fontWeight:700 }}>Unit Economics</span>
          <span style={{ fontSize:11, color:T.textDim, marginLeft:4 }}>
            {monthly.length} months · {fmtNum(totalStops)} stops · {drivers.length} drivers
          </span>
        </div>
        <button onClick={load} style={{ padding:"6px 12px", borderRadius:T.radiusSm, border:`1px solid ${T.border}`, background:T.bgWhite, color:T.textMuted, fontSize:11, fontWeight:600, cursor:"pointer" }}>🔄 Refresh</button>
      </div>

      {/* Top KPIs */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(160px, 1fr))", gap:10, marginBottom:16 }}>
        <KPI label="Total Audited Revenue" value={fmtK(totalRev)} sub={`${monthly.length} months`} accent={T.brand} />
        <KPI label="Total Stops Delivered" value={fmtNum(totalStops)} sub="from NuVizz" accent={T.green} />
        <KPI label="All-Time $/Stop" value={avgRevPerStop != null ? fmt(avgRevPerStop) : "—"} sub="audited rev / NuVizz stops" />
        <KPI label="Last 3mo $/Stop" value={recentRevPerStop != null ? fmt(recentRevPerStop) : "—"} sub={`${fmtNum(recentStops)} stops in window`} accent={recentRevPerStop != null && avgRevPerStop != null && recentRevPerStop >= avgRevPerStop ? T.green : T.red} />
      </div>

      {/* Volume × Price decomposition */}
      <div style={{ marginBottom:16 }}>
        <VolumePriceDecomposition monthly={monthly} />
      </div>

      {/* Monthly trend chart */}
      <div style={{ marginBottom:16 }}>
        <MonthlyRevenueStopsChart monthly={monthly} />
      </div>

      {/* Monthly detail table */}
      <div style={{ marginBottom:16, ...card, padding:0, overflow:"auto" }}>
        <div style={{ padding:"12px 16px", borderBottom:`1px solid ${T.border}` }}>
          <div style={{ fontSize:13, fontWeight:700 }}>Monthly Unit Economics</div>
          <div style={{ fontSize:11, color:T.textMuted }}>True audited revenue ÷ NuVizz stops, with key cost lines per month</div>
        </div>
        <table style={{ width:"100%", borderCollapse:"collapse" }}>
          <thead>
            <tr>
              <th style={tblH}>Period</th>
              <th style={{ ...tblH, textAlign:"right" }}>Revenue</th>
              <th style={{ ...tblH, textAlign:"right" }}>Stops</th>
              <th style={{ ...tblH, textAlign:"right" }}>$/Stop</th>
              <th style={{ ...tblH, textAlign:"right" }}>Net Income</th>
              <th style={{ ...tblH, textAlign:"right" }}>Margin %</th>
              <th style={{ ...tblH, textAlign:"right" }}>Subcontractors</th>
              <th style={{ ...tblH, textAlign:"right" }}>Salaries</th>
              <th style={{ ...tblH, textAlign:"right" }}>Insurance</th>
              <th style={{ ...tblH, textAlign:"right" }}>Fuel</th>
              <th style={{ ...tblH, textAlign:"right" }}>Truck Maint</th>
            </tr>
          </thead>
          <tbody>
            {[...monthly].reverse().map(m => (
              <tr key={m.period}>
                <td style={{ ...tblD, fontWeight:600 }}>{m.period}</td>
                <td style={tblDR}>{fmtK(m.revenue)}</td>
                <td style={tblDR}>{m.stops > 0 ? fmtNum(m.stops) : <span style={{color:T.textDim}}>—</span>}</td>
                <td style={{ ...tblDR, fontWeight:600 }}>{m.revPerStop != null ? fmt(m.revPerStop) : "—"}</td>
                <td style={{ ...tblDR, color: m.netIncome >= 0 ? T.green : T.red }}>{fmtK(m.netIncome)}</td>
                <td style={{ ...tblDR, color: m.marginPct == null ? T.textMuted : m.marginPct >= 10 ? T.green : m.marginPct >= 0 ? T.yellow : T.red }}>{m.marginPct != null ? m.marginPct.toFixed(1)+"%" : "—"}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtK(m.subcontractorCost)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtK(m.salaries)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtK(m.insurance)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtK(m.fuelCost)}</td>
                <td style={{ ...tblDR, color:T.textMuted }}>{fmtK(m.truckMaint)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Driver economics */}
      <div style={{ marginBottom:16 }}>
        <DriverEconomicsTable drivers={drivers} totalRev={totalRev} totalStops={totalStops} onSelectDriver={setSelectedDriver} />
      </div>

      {/* Stop Economics rollup (ZIP / driver / customer profitability matrices) */}
      <StopEconomicsPanel availableMonths={[...monthly].map(m => m.period).sort().reverse()} />

      {/* Note about route-level analysis */}
      <div style={{ ...card, padding:14, background:T.bgSurface, fontSize:11, color:T.textMuted }}>
        <strong style={{ color:T.text }}>About the data joins above:</strong> The Stop Economics matrices read from server-side rollups (collections <code style={{fontSize:10,background:T.borderLight,padding:"1px 4px",borderRadius:3}}>stop_economics_zip / _driver / _customer</code>). Click "Run Rollup" for a month before viewing. Uline revenue comes from DDIS payments + unpaid_stops; non-Uline revenue is implied from contractor pay × 2.5 (since 1099 drivers are paid 40%) and is flagged as estimated until a customer rate card is loaded.
      </div>

      {selectedDriver && <DriverDrillModal driver={selectedDriver} onClose={() => setSelectedDriver(null)} />}
    </div>
  );
}

window.UnitEconomicsTab = UnitEconomicsTab;

})();
