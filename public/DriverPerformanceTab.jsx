// DriverPerformanceTab.jsx — Driver Performance & Productivity
// v1.0.0 — Joins timeclock_weekly + payroll_weekly + nuvizz_weekly per driver.
//
// What this v1.0.0 surfaces NOW (data we have today):
//   • Hours worked, regular vs OT, OT % of total
//   • Days worked per driver in window
//   • Avg shift length (total hours ÷ days worked)
//   • Total gross pay (W2) and total 1099 pay (NuVizz × 40%)
//   • Effective hourly rate (gross ÷ hours)
//   • Effective per-stop earnings (1099_pay ÷ stops)
//   • Stops per day (NuVizz stops ÷ days worked) — uses timeclock days when available
//
// What's COMING in Phase 2 (data plumbing required):
//   • Clock-in → first delivery (pre-trip minutes)
//      needs: daily timeclock entries + raw NuVizz stop timestamps
//   • Last delivery → clock-out (post-trip minutes)
//      needs: same
//   • First delivery → last delivery span vs baseline
//      needs: same; baseline computed as P50 / P75 across same truck type
//   • Daily miles per driver (route efficiency, miles/stop)
//      needs: Motive daily mileage + driver→truck assignment
//   • Tractor vs box truck segmentation
//      needs: truck_type field on driver_classifications doc
//   • Idle gaps between stops > N minutes (time-theft signal)
//      needs: raw NuVizz stop timestamps
//   • On-time delivery rate vs scheduled windows
//      needs: NuVizz scheduled-window field

(function () {
"use strict";

const { useState, useEffect, useMemo } = React;

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
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":Number(n).toFixed(d)+"%";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtNum1= n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:1});
const fmtHr  = n => n==null||isNaN(n)?"0":Number(n).toFixed(1)+"h";

const card = { background:T.bgCard, borderRadius:T.radius, border:`1px solid ${T.border}`, padding:"16px", boxShadow:T.shadow };
const tblH = { padding:"8px 12px", textAlign:"left", fontSize:10, fontWeight:700, textTransform:"uppercase", color:T.textDim, background:T.bgSurface, borderBottom:`1px solid ${T.border}`, whiteSpace:"nowrap" };
const tblD = { padding:"8px 12px", fontSize:12, color:T.text, borderBottom:`1px solid ${T.borderLight}` };
const tblDR= { ...tblD, textAlign:"right", fontVariantNumeric:"tabular-nums" };

const db = () => window.db;

// Use the same normalizer MarginIQ.jsx uses, so cross-collection joins line up.
function driverKey(name) {
  if (!name) return null;
  return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g,"_").replace(/^_+|_+$/g,"").slice(0, 140) || null;
}

const WINDOWS = [
  { id:"l4w",  label:"Last 4 weeks",  weeks:4   },
  { id:"l13w", label:"Last 13 weeks", weeks:13  },
  { id:"l26w", label:"Last 26 weeks", weeks:26  },
  { id:"l52w", label:"Last 52 weeks", weeks:52  },
  { id:"all",  label:"All time",      weeks:null},
];

async function loadAll() {
  if (!db()) return null;
  const fetchColl = async (coll, ord, lim) => {
    try {
      let q = db().collection(coll);
      if (ord) q = q.orderBy(ord, "desc");
      if (lim) q = q.limit(lim);
      const s = await q.get();
      return s.docs.map(d => ({ id:d.id, ...d.data() }));
    } catch(e) { return []; }
  };
  const fetchClassifications = async () => {
    try {
      const s = await db().collection("driver_classifications").get();
      const map = {};
      for (const d of s.docs) map[d.id] = d.data();
      return map;
    } catch(e) { return {}; }
  };

  const [timeclockWeekly, payrollWeekly, nvWeekly, classifications] = await Promise.all([
    fetchColl("timeclock_weekly", "week_ending", 260),
    fetchColl("payroll_weekly",   "week_ending", 260),
    fetchColl("nuvizz_weekly",    "week_ending", 260),
    fetchClassifications(),
  ]);
  return { timeclockWeekly, payrollWeekly, nvWeekly, classifications };
}

function filterByWindow(rollups, weeks) {
  if (!weeks || !rollups || !rollups.length) return rollups || [];
  const sorted = [...rollups].sort((a,b)=> (b.week_ending||"").localeCompare(a.week_ending||""));
  return sorted.slice(0, weeks);
}

// Build the per-driver record by joining the three weekly streams.
function aggregateDrivers(timeclockWeekly, payrollWeekly, nvWeekly, classifications) {
  const agg = {};

  const ensure = (key, displayName) => {
    if (!agg[key]) {
      agg[key] = {
        key, name: displayName,
        // timeclock (CyberPay)
        tc_hours: 0, tc_reg: 0, tc_ot: 0, tc_days: 0, tc_weeks: 0,
        // payroll (gross W2)
        pay_gross: 0, pay_hours: 0, pay_weeks: 0,
        // nuvizz (1099 / contractor)
        nv_stops: 0, nv_pay_base: 0, nv_pay_at_40: 0, nv_weeks: 0,
        last_seen: null,
        sources: new Set(),
      };
    }
    return agg[key];
  };

  // 1. Timeclock — actual hours worked (most accurate for hour metrics)
  for (const w of (timeclockWeekly||[])) {
    if (!w.top_employees) continue;
    for (const e of w.top_employees) {
      const k = driverKey(e.name);
      if (!k) continue;
      const r = ensure(k, e.name);
      r.tc_hours += e.hours || 0;
      r.tc_reg   += e.reg   || 0;
      r.tc_ot    += e.ot    || 0;
      r.tc_days  += e.days  || 0;
      r.tc_weeks += 1;
      r.sources.add("timeclock");
      if (!r.last_seen || w.week_ending > r.last_seen) r.last_seen = w.week_ending;
    }
  }

  // 2. Payroll — gross pay (W2 dollars). Hours from payroll are less reliable
  //    than timeclock (CyberPay rounds), so we keep them only as a fallback.
  for (const w of (payrollWeekly||[])) {
    if (!w.employees) continue;
    for (const e of w.employees) {
      const k = driverKey(e.name);
      if (!k) continue;
      const r = ensure(k, e.name);
      r.pay_gross += e.gross || 0;
      r.pay_hours += e.hours || 0;
      r.pay_weeks += 1;
      r.sources.add("payroll");
      if (!r.last_seen || w.week_ending > r.last_seen) r.last_seen = w.week_ending;
    }
  }

  // 3. NuVizz — stops + 1099 contractor pay (40% of SealNbr)
  for (const w of (nvWeekly||[])) {
    if (!w.top_drivers) continue;
    for (const d of w.top_drivers) {
      const k = driverKey(d.name);
      if (!k) continue;
      const r = ensure(k, d.name);
      r.nv_stops      += d.stops || 0;
      r.nv_pay_base   += d.pay_base || 0;
      r.nv_pay_at_40  += d.pay_at_40 || 0;
      r.nv_weeks      += 1;
      r.sources.add("nuvizz");
      if (!r.last_seen || w.week_ending > r.last_seen) r.last_seen = w.week_ending;
    }
  }

  // Apply classifications + derive metrics
  return Object.values(agg).map(r => {
    const cls = classifications[r.key];
    const classification = cls?.classification || "unknown";
    const truck_type     = cls?.truck_type     || "unknown"; // Phase-2 field
    // Hours: prefer timeclock; fall back to payroll-reported.
    const total_hours = r.tc_hours > 0 ? r.tc_hours : r.pay_hours;
    const reg_hours   = r.tc_reg;
    const ot_hours    = r.tc_ot;
    const days_worked = r.tc_days; // direct count from CyberPay shifts
    const ot_pct = total_hours > 0 ? (ot_hours / total_hours) * 100 : 0;
    const avg_shift = days_worked > 0 ? total_hours / days_worked : (r.tc_weeks > 0 ? total_hours / r.tc_weeks / 5 : 0);
    // Earnings & rates
    const total_earnings = (r.pay_gross || 0) + (r.nv_pay_at_40 || 0);
    const eff_hourly = total_hours > 0 ? total_earnings / total_hours : 0;
    const eff_per_stop = r.nv_stops > 0 ? r.nv_pay_at_40 / r.nv_stops : 0;
    const stops_per_day = days_worked > 0 ? r.nv_stops / days_worked : 0;
    return {
      ...r,
      sources: Array.from(r.sources),
      classification, truck_type,
      total_hours, reg_hours, ot_hours, days_worked, ot_pct, avg_shift,
      total_earnings, eff_hourly, eff_per_stop, stops_per_day,
    };
  });
}

// ─── UI helpers ────────────────────────────────────────────────────────────────
const KPI = ({ label, value, sub, color, subColor, hint, badge }) => (
  <div style={{ ...card, flex:1, minWidth:140 }}>
    <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:4 }}>
      <span style={{ fontSize:10, color:T.textMuted, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.05em" }}>{label}</span>
      {badge && <span style={{ fontSize:9, fontWeight:700, padding:"2px 6px", borderRadius:10, background:T.brand, color:"#fff" }}>{badge}</span>}
    </div>
    <div style={{ fontSize:22, fontWeight:800, color:color||T.text }}>{value}</div>
    {sub && <div style={{ fontSize:10, color:subColor||T.textDim, marginTop:2 }}>{sub}</div>}
    {hint && <div style={{ fontSize:10, color:T.textMuted, marginTop:6, lineHeight:1.4 }}>{hint}</div>}
  </div>
);

const Section = ({ icon, title, sub, right, children }) => (
  <div style={{ marginBottom:24 }}>
    <div style={{ display:"flex", alignItems:"baseline", gap:10, marginBottom:12, justifyContent:"space-between", flexWrap:"wrap" }}>
      <div style={{ display:"flex", alignItems:"baseline", gap:10 }}>
        <span style={{ fontSize:18 }}>{icon}</span>
        <span style={{ fontSize:14, fontWeight:800, color:T.text }}>{title}</span>
        {sub && <span style={{ fontSize:11, color:T.textMuted }}>{sub}</span>}
      </div>
      {right}
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

const ClassBadge = ({ c }) => {
  const cfg = c === "w2" ? { bg:T.blueBg, fg:T.blueText, t:"W2" }
            : c === "1099" ? { bg:T.purpleBg, fg:"#6b21a8", t:"1099" }
            : { bg:T.bgSurface, fg:T.textMuted, t:"—" };
  return <span style={{ fontSize:9, fontWeight:700, padding:"2px 8px", borderRadius:10, background:cfg.bg, color:cfg.fg }}>{cfg.t}</span>;
};

function DriverTable({ drivers }) {
  const [sortKey, setSortKey] = useState("total_hours");
  const [sortDir, setSortDir] = useState(-1);

  const sorted = useMemo(() => {
    const arr = [...drivers].sort((a,b) => {
      const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
      if (typeof av === "string") return sortDir * (av < bv ? -1 : av > bv ? 1 : 0);
      return sortDir * (bv - av);
    });
    return arr;
  }, [drivers, sortKey, sortDir]);

  const SortTh = ({ k, label, right, hint }) => (
    <th style={{ ...tblH, textAlign:right?"right":"left", cursor:"pointer" }} title={hint}
        onClick={() => { if(sortKey===k) setSortDir(-sortDir); else { setSortKey(k); setSortDir(-1); } }}>
      {label}{sortKey===k?(sortDir===-1?" ↓":" ↑"):""}
    </th>
  );

  if (!drivers.length) return (
    <div style={{ ...card, textAlign:"center", padding:"40px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:8 }}>🚚</div>
      <div style={{ fontSize:13, fontWeight:600 }}>No drivers in this window</div>
      <div style={{ fontSize:11, marginTop:6, lineHeight:1.5 }}>Try widening the time window or check that timeclock / payroll / NuVizz data has been ingested.</div>
    </div>
  );

  return (
    <div style={{ ...card, padding:0, overflow:"auto" }}>
      <table style={{ width:"100%", borderCollapse:"collapse", minWidth:1100 }}>
        <thead>
          <tr>
            <SortTh k="name"          label="Driver" />
            <th style={{...tblH, textAlign:"left"}}>Class</th>
            <SortTh k="days_worked"   label="Days"     right hint="Days worked in window (from CyberPay daily shifts)" />
            <SortTh k="total_hours"   label="Hours"    right hint="Total hours worked (timeclock; falls back to payroll-reported)" />
            <SortTh k="ot_hours"      label="OT Hrs"   right />
            <SortTh k="ot_pct"        label="OT %"     right hint="OT hours as % of total hours" />
            <SortTh k="avg_shift"     label="Avg Shift" right hint="Total hours ÷ days worked" />
            <SortTh k="nv_stops"      label="Stops"    right hint="Total NuVizz stops in window" />
            <SortTh k="stops_per_day" label="Stops/Day" right />
            <SortTh k="pay_gross"     label="W2 Gross" right hint="Gross W2 pay from CyberPay payroll" />
            <SortTh k="nv_pay_at_40"  label="1099 Pay" right hint="40% of NuVizz SealNbr — contractor pay" />
            <SortTh k="eff_hourly"    label="Eff $/hr" right hint="(W2 gross + 1099 pay) ÷ total hours" />
            <SortTh k="eff_per_stop"  label="$/Stop"   right hint="1099 pay ÷ NuVizz stops (contractor effective rate)" />
          </tr>
        </thead>
        <tbody>
          {sorted.map((d, i) => (
            <tr key={d.key} style={{ background: i%2 ? T.bgSurface : undefined }}>
              <td style={{ ...tblD, fontWeight:600 }}>{d.name}</td>
              <td style={tblD}><ClassBadge c={d.classification} /></td>
              <td style={tblDR}>{fmtNum(d.days_worked)}</td>
              <td style={tblDR}>{fmtNum1(d.total_hours)}</td>
              <td style={{ ...tblDR, color: d.ot_hours > 0 ? T.yellowText : T.textDim }}>{fmtNum1(d.ot_hours)}</td>
              <td style={{ ...tblDR, color: d.ot_pct >= 20 ? T.red : d.ot_pct >= 10 ? T.yellowText : T.textMuted }}>{fmtPct(d.ot_pct, 0)}</td>
              <td style={tblDR}>{d.avg_shift > 0 ? fmtHr(d.avg_shift) : "—"}</td>
              <td style={tblDR}>{fmtNum(d.nv_stops)}</td>
              <td style={tblDR}>{d.stops_per_day > 0 ? fmtNum1(d.stops_per_day) : "—"}</td>
              <td style={tblDR}>{d.pay_gross > 0 ? fmtK(d.pay_gross) : "—"}</td>
              <td style={tblDR}>{d.nv_pay_at_40 > 0 ? fmtK(d.nv_pay_at_40) : "—"}</td>
              <td style={{ ...tblDR, color: d.eff_hourly > 0 ? T.text : T.textDim, fontWeight:600 }}>{d.eff_hourly > 0 ? fmt2(d.eff_hourly) : "—"}</td>
              <td style={tblDR}>{d.eff_per_stop > 0 ? fmt2(d.eff_per_stop) : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Phase2Card() {
  const items = [
    { icon:"🚀", title:"Clock-in → first delivery time", needs:"daily timeclock entries + raw NuVizz stops with timestamps" },
    { icon:"🏁", title:"Last delivery → clock-out time", needs:"same as above" },
    { icon:"⏱️", title:"First → last delivery span vs baseline", needs:"raw NuVizz stops; baseline = P50/P75 across same truck type" },
    { icon:"🛣️", title:"Daily miles per driver, miles/stop", needs:"Motive daily mileage by truck + driver→truck assignment" },
    { icon:"🚛", title:"Tractor vs box truck segmentation", needs:"truck_type field on driver_classifications doc" },
    { icon:"💤", title:"Idle gaps between stops > N min (time-theft signal)", needs:"raw NuVizz stop timestamps" },
    { icon:"📅", title:"On-time delivery rate vs scheduled windows", needs:"NuVizz scheduled-window field" },
    { icon:"📈", title:"Personal rolling baseline (last 13 wks for that driver) vs peer baseline", needs:"all of the above" },
  ];
  return (
    <div style={{ ...card, background:T.bgSurface, borderLeft:`3px solid ${T.purple}` }}>
      <div style={{ fontSize:13, fontWeight:700, marginBottom:6 }}>🔮 Coming in Phase 2</div>
      <div style={{ fontSize:11, color:T.textMuted, marginBottom:12, lineHeight:1.5 }}>
        Each metric below needs a specific data source wired in. They'll populate this tab automatically once the underlying collection is being written to Firestore.
      </div>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(280px, 1fr))", gap:10 }}>
        {items.map((it, i) => (
          <div key={i} style={{ background:T.bgWhite, border:`1px solid ${T.border}`, borderRadius:T.radiusSm, padding:"10px 12px" }}>
            <div style={{ fontSize:12, fontWeight:700, color:T.text, marginBottom:4 }}>{it.icon} {it.title}</div>
            <div style={{ fontSize:10, color:T.textMuted, lineHeight:1.5 }}><strong style={{ color:T.textMuted }}>Needs:</strong> {it.needs}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Main component ──────────────────────────────────────────────────────────
function DriverPerformanceTab() {
  const [data, setData]         = useState(null);
  const [loading, setLoading]   = useState(true);
  const [windowId, setWindowId] = useState("l13w");
  const [classFilter, setClassFilter] = useState("all"); // all / w2 / 1099 / unknown

  useEffect(() => {
    (async () => {
      setLoading(true);
      const d = await loadAll();
      setData(d);
      setLoading(false);
    })();
  }, []);

  const win = WINDOWS.find(w => w.id === windowId) || WINDOWS[1];

  const drivers = useMemo(() => {
    if (!data) return [];
    const tcW = filterByWindow(data.timeclockWeekly, win.weeks);
    const payW = filterByWindow(data.payrollWeekly,  win.weeks);
    const nvW  = filterByWindow(data.nvWeekly,       win.weeks);
    const all = aggregateDrivers(tcW, payW, nvW, data.classifications || {});
    // drop ghost rows with literally no activity
    return all.filter(d => d.total_hours > 0 || d.nv_stops > 0 || d.pay_gross > 0);
  }, [data, win]);

  const filtered = useMemo(() => {
    if (classFilter === "all") return drivers;
    return drivers.filter(d => d.classification === classFilter);
  }, [drivers, classFilter]);

  // Summary KPIs across the filtered set
  const summary = useMemo(() => {
    const s = { drivers:filtered.length, hours:0, ot:0, days:0, stops:0, w2_gross:0, c_pay:0 };
    for (const d of filtered) {
      s.hours    += d.total_hours;
      s.ot       += d.ot_hours;
      s.days     += d.days_worked;
      s.stops    += d.nv_stops;
      s.w2_gross += d.pay_gross;
      s.c_pay    += d.nv_pay_at_40;
    }
    s.ot_pct        = s.hours > 0 ? (s.ot / s.hours) * 100 : 0;
    s.avg_shift     = s.days  > 0 ? s.hours / s.days : 0;
    s.stops_per_day = s.days  > 0 ? s.stops / s.days : 0;
    s.eff_hourly    = s.hours > 0 ? (s.w2_gross + s.c_pay) / s.hours : 0;
    return s;
  }, [filtered]);

  // Per-class counts (for filter pill labels)
  const classCounts = useMemo(() => {
    const c = { all: drivers.length, w2:0, "1099":0, unknown:0 };
    for (const d of drivers) {
      if (d.classification === "w2") c.w2++;
      else if (d.classification === "1099") c["1099"]++;
      else c.unknown++;
    }
    return c;
  }, [drivers]);

  if (loading) return (
    <div style={{ textAlign:"center", padding:"60px 20px", color:T.textMuted }}>
      <div style={{ fontSize:36, marginBottom:12 }}>🏁</div>
      <div style={{ fontSize:14 }}>Loading driver performance...</div>
    </div>
  );

  return (
    <div style={{ padding:"20px", maxWidth:1400, margin:"0 auto" }} className="fade-in">
      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16, flexWrap:"wrap", gap:12 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:20 }}>🏁</span>
          <span style={{ fontSize:15, fontWeight:800, color:T.text }}>Driver Performance</span>
          <span style={{ fontSize:11, color:T.textMuted, marginLeft:6 }}>per-driver hours, productivity & effective rate</span>
        </div>
        <div style={{ display:"flex", gap:6, flexWrap:"wrap" }}>
          {WINDOWS.map(w => <Pill key={w.id} active={windowId===w.id} onClick={()=>setWindowId(w.id)}>{w.label}</Pill>)}
        </div>
      </div>

      {/* Summary KPIs */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(160px, 1fr))", gap:12, marginBottom:24 }}>
        <KPI label="Active Drivers"  value={fmtNum(summary.drivers)} sub={`${classCounts.w2} W2 · ${classCounts["1099"]} 1099 · ${classCounts.unknown} unclass.`} color={T.brand} />
        <KPI label="Total Hours"     value={fmtNum1(summary.hours)} sub={`${fmtNum1(summary.ot)} OT (${fmtPct(summary.ot_pct,0)})`} subColor={summary.ot_pct >= 15 ? T.red : T.green} />
        <KPI label="Days Worked"     value={fmtNum(summary.days)} sub={summary.avg_shift > 0 ? `avg shift ${fmtHr(summary.avg_shift)}` : "—"} />
        <KPI label="Total Stops"     value={fmtNum(summary.stops)} sub={summary.stops_per_day > 0 ? `${fmtNum1(summary.stops_per_day)} / day` : "—"} color={T.brand} />
        <KPI label="Effective $/hr"  value={summary.eff_hourly > 0 ? fmt2(summary.eff_hourly) : "—"} sub={`${fmtK(summary.w2_gross)} W2 + ${fmtK(summary.c_pay)} 1099`} subColor={T.green} />
      </div>

      {/* Class filter pills */}
      <div style={{ display:"flex", gap:6, marginBottom:14, flexWrap:"wrap", alignItems:"center" }}>
        <span style={{ fontSize:11, color:T.textMuted, marginRight:4, fontWeight:600 }}>Filter:</span>
        <Pill active={classFilter==="all"}     onClick={()=>setClassFilter("all")}>All ({classCounts.all})</Pill>
        <Pill active={classFilter==="w2"}      onClick={()=>setClassFilter("w2")}>W2 ({classCounts.w2})</Pill>
        <Pill active={classFilter==="1099"}    onClick={()=>setClassFilter("1099")}>1099 ({classCounts["1099"]})</Pill>
        <Pill active={classFilter==="unknown"} onClick={()=>setClassFilter("unknown")}>Unclassified ({classCounts.unknown})</Pill>
      </div>

      {/* Driver table */}
      <Section icon="👥" title="Drivers" sub={`${filtered.length} in window · click any column to sort`}>
        <DriverTable drivers={filtered} />
        {classCounts.unknown > 0 && (
          <div style={{ marginTop:10, padding:"10px 14px", background:T.yellowBg, border:`1px solid ${T.yellow}`, borderRadius:T.radiusSm, fontSize:11, color:T.yellowText, lineHeight:1.5 }}>
            ⚠️ <strong>{classCounts.unknown} driver(s) are unclassified.</strong> Tag them as W2 or 1099 in the <strong>Drivers</strong> tab so margin and effective-rate math is accurate.
          </div>
        )}
      </Section>

      {/* Phase-2 roadmap */}
      <Section icon="🔮" title="Phase 2 — what's next" sub="metrics that need additional data plumbing">
        <Phase2Card />
      </Section>

      {/* Reading guide */}
      <div style={{ padding:"14px 16px", background:T.bgSurface, borderRadius:T.radius, fontSize:11, color:T.textMuted, lineHeight:1.7 }}>
        <strong style={{ color:T.text }}>Reading the table:</strong>{" "}
        Hours are pulled from CyberPay timeclock when available (most accurate); payroll-reported hours are used as a fallback when timeclock is missing.{" "}
        <em>Eff $/hr</em> combines W2 gross + 1099 contractor pay across the same hours, useful for drivers who wear both hats.{" "}
        <em>$/Stop</em> is contractor-only — what each NuVizz stop pays out at the 40% rate.{" "}
        Days worked is a true count of distinct shift dates from CyberPay, not weeks × 5.
      </div>
    </div>
  );
}

window.DriverPerformanceTab = DriverPerformanceTab;

})();
