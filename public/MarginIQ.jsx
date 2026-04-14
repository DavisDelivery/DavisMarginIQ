// Davis MarginIQ v2.0 — Cost Intelligence Platform
// Firebase loaded globally via compat SDK in index.html (window.db, window.fbStorage)
// SheetJS loaded globally via CDN (window.XLSX)

const { useState, useEffect, useCallback, useRef, useMemo } = React;
const APP_VERSION = "2.0.2";

// ─── Design Tokens (Davis Brand Blue) ────────────────────────
const T = {
  brand:"#1e5b92", brandLight:"#2a7bc8", brandDark:"#143f66", brandPale:"#e8f0f8",
  accent:"#10b981", accentWarn:"#f59e0b", accentDanger:"#ef4444",
  bg:"#f0f4f8", bgWhite:"#ffffff", bgCard:"#ffffff", bgSurface:"#f8fafc", bgHover:"#f1f5f9",
  text:"#0f172a", textMuted:"#64748b", textDim:"#94a3b8",
  border:"#e2e8f0", borderLight:"#f1f5f9",
  green:"#10b981", greenBg:"#ecfdf5", greenText:"#065f46",
  red:"#ef4444", redBg:"#fef2f2", redText:"#991b1b",
  yellow:"#f59e0b", yellowBg:"#fffbeb", yellowText:"#92400e",
  blue:"#3b82f6", blueBg:"#eff6ff", blueText:"#1e40af",
  purple:"#8b5cf6", purpleBg:"#f5f3ff",
  radius:"12px", radiusSm:"8px", radiusLg:"16px",
  shadow:"0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
  shadowMd:"0 4px 12px rgba(0,0,0,0.08)",
  shadowLg:"0 10px 30px rgba(0,0,0,0.1)",
};

// ─── Default Cost Structure (all zeros — populate from real data) ─
const DEFAULT_COSTS = {
  // Annual fixed costs
  warehouse: 0,
  forklifts: 0,
  forklift_operators: 0,
  truck_insurance_monthly: 0,
  truck_count_box: 0,
  truck_count_tractor: 0,
  // Hourly rates
  rate_box_driver: 0,
  rate_tractor_driver: 0,
  rate_dispatcher: 0,
  rate_admin: 0,
  rate_mechanic: 0,
  // Headcounts
  count_box_drivers: 0,
  count_tractor_drivers: 0,
  count_dispatchers: 0,
  count_admin: 0,
  count_mechanics: 0,
  count_forklift_ops: 0,
  // Vehicle
  mpg_box: 0,
  mpg_tractor: 0,
  fuel_price: 0,
  // Working days
  working_days_year: 260,
  avg_hours_per_shift: 10,
  // Contractor payout
  contractor_pct: 0.40,
};

// ─── Formatters ──────────────────────────────────────────────
const fmt = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1000000) return "$"+( v/1000000).toFixed(1)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtDec = (n,d=2) => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":fmtDec(n,d)+"%";
const fmtDate = d => d?new Date(d).toLocaleDateString("en-US",{month:"short",day:"numeric"}):"—";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

// ─── Firebase Helpers ────────────────────────────────────────
const hasFirebase = typeof window !== "undefined" && window.db;
const FS = {
  async getCosts() {
    if (!hasFirebase) return null;
    try { const d=await window.db.collection("marginiq_config").doc("cost_structure").get(); return d.exists?d.data():null; } catch(e) { return null; }
  },
  async saveCosts(data) {
    if (!hasFirebase) return false;
    try { await window.db.collection("marginiq_config").doc("cost_structure").set({...data, updated_at:new Date().toISOString()}); return true; } catch(e) { return false; }
  },
  async getUlineWeeks() {
    if (!hasFirebase) return [];
    try { const s=await window.db.collection("uline_audits").orderBy("upload_date","desc").limit(52).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; }
  },
  async saveUlineWeek(weekId, data) {
    if (!hasFirebase) return false;
    try { await window.db.collection("uline_audits").doc(weekId).set(data); return true; } catch(e) { return false; }
  },
  async getPayrolls(n=10) {
    if (!hasFirebase) return [];
    try { const s=await window.db.collection("payroll_runs").orderBy("check_date","desc").limit(n).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; }
  },
  async getDrivers() {
    if (!hasFirebase) return [];
    try { const s=await window.db.collection("drivers").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; }
  },
};

// ─── Uline XLSX Parser ──────────────────────────────────────
function parseUlineXlsx(file) {
  return new Promise((resolve, reject) => {
    if (typeof XLSX === "undefined") return reject(new Error("XLSX library not loaded — deploy to Netlify or add SheetJS CDN"));
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const wb = XLSX.read(e.target.result, { type: "array" });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const raw = XLSX.utils.sheet_to_json(ws, { defval: "" });
        // Normalize column names (Uline files have varying case)
        const rows = raw.map(r => {
          const n = {};
          Object.keys(r).forEach(k => { n[k.toLowerCase().trim()] = r[k]; });
          return n;
        });
        // Extract stop-level data
        const stops = rows.filter(r => r.pro || r.order).map(r => ({
          pro: String(r.pro||"").replace(/^0+/,""),
          order: String(r.order||""),
          customer: String(r.customer||r.consignee||""),
          city: String(r.city||""),
          state: String(r.st||r.state||""),
          zip: String(r.zip||""),
          pickup: String(r.pu||r.pickup||""),
          cost: parseFloat(r.cost||r.amount||0) || 0,
          weight: parseFloat(r.wgt||r.weight||0) || 0,
          skids: parseInt(r.skid||r.skids||r.pallets||0) || 0,
          loose: parseInt(r.loose||0) || 0,
          pieces: (parseInt(r.skid||r.skids||0)||0) + (parseInt(r.loose||0)||0),
          via: String(r.via||""),
          extraCost: parseFloat(r["extra cost"]||r.extra_cost||0) || 0,
          code: String(r.code||""),
          newCost: parseFloat(r["new cost"]||r.new_cost||0) || 0,
          notes: String(r.notes||""),
          warehouse: String(r.wh||r.warehouse||""),
        }));
        // Summary
        const totalRevenue = stops.reduce((s,r) => s + (r.newCost || r.cost), 0);
        const totalWeight = stops.reduce((s,r) => s + r.weight, 0);
        const totalStops = stops.length;
        const byCity = {};
        stops.forEach(s => { const c=s.city||"Unknown"; if(!byCity[c]) byCity[c]={city:c,stops:0,revenue:0,weight:0}; byCity[c].stops++; byCity[c].revenue+=(s.newCost||s.cost); byCity[c].weight+=s.weight; });
        const byZip = {};
        stops.forEach(s => { const z=s.zip||"Unknown"; if(!byZip[z]) byZip[z]={zip:z,stops:0,revenue:0}; byZip[z].stops++; byZip[z].revenue+=(s.newCost||s.cost); });

        resolve({
          stops, totalRevenue, totalWeight, totalStops,
          byCity: Object.values(byCity).sort((a,b)=>b.revenue-a.revenue),
          byZip: Object.values(byZip).sort((a,b)=>b.revenue-a.revenue),
          avgRevenuePerStop: totalStops>0 ? totalRevenue/totalStops : 0,
          avgWeightPerStop: totalStops>0 ? totalWeight/totalStops : 0,
        });
      } catch(e) { reject(e); }
    };
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });
}

// ─── NuVizz API Helper ──────────────────────────────────────
async function fetchNuVizz(path) {
  try {
    const r = await fetch(`/api/nuvizz/${path}`);
    if (!r.ok) throw new Error(`NuVizz ${r.status}`);
    return await r.json();
  } catch(e) { console.warn("NuVizz:", e); return null; }
}

async function fetchNuVizzStops(from, to) {
  return fetchNuVizz(`stops?fromDTTM=${encodeURIComponent(from+"T00:00:00")}&toDTTM=${encodeURIComponent(to+"T23:59:59")}`);
}

// ─── Motive API Helper ──────────────────────────────────────
async function fetchMotive(action) {
  try {
    const r = await fetch(`/.netlify/functions/marginiq-motive?action=${action}`);
    if (!r.ok) throw new Error(`Motive ${r.status}`);
    return await r.json();
  } catch(e) { console.warn("Motive:", e); return null; }
}

// ─── QBO API Helper ─────────────────────────────────────────
async function fetchQBO(action, start, end) {
  try {
    let url = `/.netlify/functions/marginiq-qbo-data?action=${action}`;
    if (start) url += `&start=${start}`;
    if (end) url += `&end=${end}`;
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch(e) { return null; }
}

// ─── MARGIN ENGINE ──────────────────────────────────────────
function calculateMargins(costs, ulineData, nuvizzStops, qboData) {
  const c = { ...DEFAULT_COSTS, ...costs };
  const wd = c.working_days_year || 260;

  // ── Annual labor costs ──
  const annualBoxDrivers = c.count_box_drivers * c.rate_box_driver * c.avg_hours_per_shift * wd;
  const annualTractorDrivers = c.count_tractor_drivers * c.rate_tractor_driver * c.avg_hours_per_shift * wd;
  const annualDispatchers = c.count_dispatchers * c.rate_dispatcher * 8 * wd;
  const annualAdmin = c.count_admin * c.rate_admin * 8 * wd;
  const annualMechanics = c.count_mechanics * c.rate_mechanic * 8 * wd;
  const annualForkliftOps = c.count_forklift_ops * 20 * 8 * wd;
  const totalAnnualLabor = annualBoxDrivers + annualTractorDrivers + annualDispatchers + annualAdmin + annualMechanics + annualForkliftOps;

  // ── Annual fixed costs ──
  const totalTrucks = (c.truck_count_box||0) + (c.truck_count_tractor||0);
  const annualInsurance = c.truck_insurance_monthly * totalTrucks * 12;
  const annualWarehouse = c.warehouse || 0;
  const annualForklifts = c.forklifts || 0;
  const totalAnnualFixed = annualWarehouse + annualForklifts + annualInsurance;

  // ── Total annual cost ──
  const totalAnnualCost = totalAnnualLabor + totalAnnualFixed;
  const dailyCost = totalAnnualCost / wd;
  const monthlyCost = totalAnnualCost / 12;

  // ── Per-stop costs (using Uline volume as baseline) ──
  const dailyStops = ulineData ? (ulineData.totalStops / (ulineData.weekCount||1) / 5) : 600;
  const annualStops = dailyStops * wd;
  const costPerStop = dailyCost / dailyStops;

  // ── Revenue analysis ──
  const ulineRevenue = ulineData ? ulineData.totalRevenue : 0;
  const ulineWeeklyRevenue = ulineData ? ulineRevenue / (ulineData.weekCount||1) : 0;
  const ulineDailyRevenue = ulineWeeklyRevenue / 5;
  const ulineAnnualRevenue = ulineWeeklyRevenue * 52;

  // ── QBO revenue (all customers) ──
  const qboRevenue = qboData?.revenue || 0;
  const qboCosts = qboData?.costs || 0;

  // ── Margin calculations ──
  const dailyRevenue = ulineDailyRevenue || (qboRevenue / wd) || 0;
  const dailyMargin = dailyRevenue - dailyCost;
  const dailyMarginPct = dailyRevenue > 0 ? (dailyMargin / dailyRevenue * 100) : 0;

  const revenuePerStop = dailyStops > 0 ? dailyRevenue / dailyStops : 0;
  const marginPerStop = revenuePerStop - costPerStop;
  const marginPerStopPct = revenuePerStop > 0 ? (marginPerStop / revenuePerStop * 100) : 0;

  // ── Per-driver economics ──
  const totalDrivers = c.count_box_drivers + c.count_tractor_drivers;
  const stopsPerDriver = dailyStops / (totalDrivers || 1);
  const revenuePerDriver = dailyRevenue / (totalDrivers || 1);
  const costPerDriver = dailyCost / (totalDrivers || 1);
  const marginPerDriver = revenuePerDriver - costPerDriver;

  // ── Per-truck economics ──
  const revenuePerTruck = dailyRevenue / (totalTrucks || 1);
  const costPerTruck = dailyCost / (totalTrucks || 1);

  // ── Cost breakdown ──
  const laborPct = totalAnnualCost > 0 ? (totalAnnualLabor / totalAnnualCost * 100) : 0;
  const fixedPct = totalAnnualCost > 0 ? (totalAnnualFixed / totalAnnualCost * 100) : 0;

  // ── Break-even ──
  const breakEvenStopsDaily = revenuePerStop > 0 ? (dailyCost / revenuePerStop) : 0;

  return {
    // Annual
    totalAnnualCost, totalAnnualLabor, totalAnnualFixed,
    annualBoxDrivers, annualTractorDrivers, annualDispatchers, annualAdmin, annualMechanics, annualForkliftOps,
    annualInsurance, annualWarehouse, annualForklifts,
    ulineAnnualRevenue, qboRevenue,
    // Monthly
    monthlyCost,
    // Daily
    dailyCost, dailyRevenue, dailyMargin, dailyMarginPct, dailyStops,
    // Per-stop
    costPerStop, revenuePerStop, marginPerStop, marginPerStopPct,
    // Per-driver
    totalDrivers, stopsPerDriver, revenuePerDriver, costPerDriver, marginPerDriver,
    // Per-truck
    totalTrucks, revenuePerTruck, costPerTruck,
    // Breakdown
    laborPct, fixedPct,
    // Break-even
    breakEvenStopsDaily,
    // Cost components for pie chart
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

// ─── Shared Components ──────────────────────────────────────
const cardStyle = { background:T.bgCard, borderRadius:T.radius, padding:"16px", border:`1px solid ${T.border}`, boxShadow:T.shadow, marginBottom:"12px" };
const inputStyle = { width:"100%", padding:"8px 12px", borderRadius:"8px", border:`1px solid ${T.border}`, background:T.bgSurface, color:T.text, fontSize:"13px", outline:"none", fontFamily:"inherit" };

function KPI({ label, value, sub, subColor, icon }) {
  return (
    <div style={{...cardStyle, padding:"14px 16px", marginBottom:0}}>
      <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
        {icon && <span style={{fontSize:14}}>{icon}</span>}
        <span style={{fontSize:"10px",color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600}}>{label}</span>
      </div>
      <div style={{fontSize:"22px",fontWeight:700,color:T.text,letterSpacing:"-0.02em"}}>{value}</div>
      {sub && <div style={{fontSize:"11px",marginTop:"3px",fontWeight:500,color:subColor||T.textMuted}}>{sub}</div>}
    </div>
  );
}

function Badge({ text, color, bg }) {
  return <span style={{fontSize:"10px",fontWeight:700,color:color||T.brand,background:bg||T.brandPale,padding:"2px 8px",borderRadius:"5px",whiteSpace:"nowrap"}}>{text}</span>;
}

function MiniBar({ pct, color, height=6 }) {
  return (
    <div style={{width:"100%",height,borderRadius:3,background:T.borderLight,overflow:"hidden"}}>
      <div style={{height:"100%",borderRadius:3,background:color||T.brand,width:`${Math.min(Math.max(pct||0,0),100)}%`,transition:"width 0.6s"}} />
    </div>
  );
}

function SectionTitle({ icon, text, right }) {
  return (
    <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:"12px"}}>
      <div style={{fontSize:"15px",fontWeight:700,color:T.text,display:"flex",alignItems:"center",gap:"8px"}}>
        {icon && <span>{icon}</span>}{text}
      </div>
      {right}
    </div>
  );
}

function DataRow({ label, value, valueColor, bold }) {
  return (
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.borderLight}`}}>
      <span style={{fontSize:"12px",color:T.textMuted}}>{label}</span>
      <span style={{fontSize:"13px",fontWeight:bold?700:600,color:valueColor||T.text}}>{value}</span>
    </div>
  );
}

function EmptyState({ icon, title, sub }) {
  return (
    <div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}>
      <div style={{fontSize:36,marginBottom:8}}>{icon||"📊"}</div>
      <div style={{fontSize:14,fontWeight:600,color:T.text,marginBottom:4}}>{title}</div>
      <div style={{fontSize:12}}>{sub}</div>
    </div>
  );
}

function TabButton({ active, label, onClick }) {
  return (
    <button onClick={onClick} style={{
      padding:"8px 16px", borderRadius:"8px", border:"none",
      background:active?T.brand:"transparent", color:active?"#fff":T.textMuted,
      fontSize:"12px", fontWeight:active?700:500, cursor:"pointer", whiteSpace:"nowrap", transition:"all 0.2s",
    }}>{label}</button>
  );
}

function PrimaryBtn({ text, onClick, loading, style:sx }) {
  return (
    <button onClick={onClick} disabled={loading} style={{
      padding:"10px 20px", borderRadius:"10px", border:"none",
      background:loading?"#94a3b8":`linear-gradient(135deg,${T.brand},${T.brandLight})`,
      color:"#fff", fontSize:"13px", fontWeight:700, cursor:loading?"wait":"pointer", ...sx,
    }}>{loading?"Loading...":text}</button>
  );
}

// ─── Simple Bar Chart (no external lib) ─────────────────────
function BarChart({ data, labelKey, valueKey, color, maxBars=15, formatValue }) {
  const items = data.slice(0, maxBars);
  const max = Math.max(...items.map(d => d[valueKey] || 0), 1);
  const fv = formatValue || fmt;
  return (
    <div>
      {items.map((d, i) => {
        const pct = (d[valueKey] || 0) / max * 100;
        return (
          <div key={i} style={{display:"flex",alignItems:"center",gap:10,marginBottom:6}}>
            <div style={{width:90,fontSize:11,color:T.textMuted,flexShrink:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={d[labelKey]}>{d[labelKey]}</div>
            <div style={{flex:1,height:22,background:T.borderLight,borderRadius:4,overflow:"hidden"}}>
              <div style={{height:"100%",width:`${pct}%`,background:color||`linear-gradient(90deg,${T.brand},${T.brandLight})`,borderRadius:4,display:"flex",alignItems:"center",paddingLeft:8,transition:"width 0.4s"}}>
                {pct>20&&<span style={{fontSize:10,color:"#fff",fontWeight:700}}>{fv(d[valueKey])}</span>}
              </div>
            </div>
            <div style={{width:60,fontSize:12,fontWeight:700,textAlign:"right",color:T.text}}>{fv(d[valueKey])}</div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Donut Chart (SVG) ──────────────────────────────────────
function DonutChart({ data, size=180 }) {
  const total = data.reduce((s,d) => s+d.value, 0);
  if (total === 0) return null;
  const cx=size/2, cy=size/2, r=size*0.35, strokeW=size*0.15;
  let cumAngle = -90;
  const arcs = data.map(d => {
    const pct = d.value / total;
    const angle = pct * 360;
    const startAngle = cumAngle;
    cumAngle += angle;
    const endAngle = cumAngle;
    const largeArc = angle > 180 ? 1 : 0;
    const rad = Math.PI / 180;
    const x1 = cx + r * Math.cos(startAngle * rad);
    const y1 = cy + r * Math.sin(startAngle * rad);
    const x2 = cx + r * Math.cos(endAngle * rad);
    const y2 = cy + r * Math.sin(endAngle * rad);
    return { ...d, pct, path: `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}` };
  });
  return (
    <div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        {arcs.map((a,i) => (
          <path key={i} d={a.path} fill="none" stroke={a.color} strokeWidth={strokeW} strokeLinecap="butt" />
        ))}
        <text x={cx} y={cy-6} textAnchor="middle" fill={T.text} fontSize="16" fontWeight="800" fontFamily="DM Sans">{fmtK(total)}</text>
        <text x={cx} y={cy+10} textAnchor="middle" fill={T.textMuted} fontSize="9" fontFamily="DM Sans">ANNUAL</text>
      </svg>
      <div style={{flex:1,minWidth:120}}>
        {arcs.filter(a=>a.pct>0.02).map((a,i) => (
          <div key={i} style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}>
            <div style={{width:8,height:8,borderRadius:2,background:a.color,flexShrink:0}} />
            <span style={{fontSize:11,color:T.textMuted,flex:1}}>{a.name}</span>
            <span style={{fontSize:11,fontWeight:700,color:T.text}}>{fmtPct(a.pct*100,0)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: COMMAND CENTER
// ═══════════════════════════════════════════════════════════════
function CommandCenter({ margins, ulineData, qboConnected, qboData, connections }) {
  const m = margins;
  const marginColor = m.dailyMarginPct >= 30 ? T.green : m.dailyMarginPct >= 20 ? T.yellow : T.red;
  const marginBg = m.dailyMarginPct >= 30 ? T.greenBg : m.dailyMarginPct >= 20 ? T.yellowBg : T.redBg;

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🎯" text="Command Center" right={<span style={{fontSize:10,color:T.textDim}}>{new Date().toLocaleDateString("en-US",{weekday:"long",month:"long",day:"numeric",year:"numeric"})}</span>} />

      {/* Top KPI Row */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
        <KPI icon="💰" label="Daily Revenue" value={fmt(m.dailyRevenue)} sub={`${fmtK(m.ulineAnnualRevenue||m.qboRevenue)}/yr projected`} subColor={T.green} />
        <KPI icon="📉" label="Daily Cost" value={fmt(m.dailyCost)} sub={`${fmtK(m.totalAnnualCost)}/yr total`} subColor={T.red} />
        <KPI icon="📊" label="Daily Margin" value={fmt(m.dailyMargin)} sub={fmtPct(m.dailyMarginPct)+" margin"} subColor={marginColor} />
        <KPI icon="🚚" label="Daily Stops" value={fmtNum(m.dailyStops)} sub={`${fmtNum(m.breakEvenStopsDaily)} break-even`} />
        <KPI icon="🎯" label="Rev/Stop" value={fmt(m.revenuePerStop)} sub={`${fmt(m.costPerStop)} cost`} subColor={T.blue} />
        <KPI icon="👤" label="Rev/Driver" value={fmt(m.revenuePerDriver)} sub={`${fmtNum(m.stopsPerDriver)} stops/day`} />
      </div>

      {/* Margin Health */}
      <div style={{...cardStyle, borderLeft:`4px solid ${marginColor}`, marginBottom:16}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}>
          <div>
            <div style={{fontSize:13,fontWeight:700,color:T.text}}>Margin Health</div>
            <div style={{fontSize:11,color:T.textMuted}}>Target: 30% gross margin</div>
          </div>
          <div style={{fontSize:28,fontWeight:800,color:marginColor}}>{fmtPct(m.dailyMarginPct)}</div>
        </div>
        <MiniBar pct={m.dailyMarginPct * (100/50)} color={marginColor} height={10} />
        <div style={{display:"flex",justifyContent:"space-between",marginTop:6}}>
          <span style={{fontSize:10,color:T.textDim}}>0%</span>
          <span style={{fontSize:10,color:T.textDim,position:"relative",left:"-10%"}}>|30% target</span>
          <span style={{fontSize:10,color:T.textDim}}>50%</span>
        </div>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:"12px"}}>
        {/* Cost Breakdown Donut */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Annual Cost Breakdown</div>
          <DonutChart data={m.costBreakdown} />
        </div>

        {/* Per-Unit Economics */}
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
          <DataRow label="Revenue per truck/day" value={fmt(m.revenuePerTruck)} valueColor={T.green} />
          <DataRow label="Cost per truck/day" value={fmt(m.costPerTruck)} valueColor={T.red} />
          <DataRow label="Trucks in fleet" value={m.totalTrucks} />
          <DataRow label="Drivers" value={m.totalDrivers} />
        </div>
      </div>

      {/* Data Sources Status */}
      <div style={{...cardStyle, marginTop:12}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Data Sources</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:8}}>
          {[
            { name:"Uline XLSX", on:!!ulineData, sub:ulineData?`${ulineData.weekCount} weeks loaded`:"Upload weekly audit files" },
            { name:"NuVizz", on:connections.nuvizz, sub:connections.nuvizz?"Connected":"Check API" },
            { name:"QuickBooks", on:qboConnected, sub:qboConnected?"Connected":"Connect in Settings" },
            { name:"Motive", on:connections.motive, sub:connections.motive?"Connected":"Check API" },
            { name:"CyberPay", on:connections.cyberpay, sub:"Auto-scrape Mondays" },
          ].map(s => (
            <div key={s.name} style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",borderRadius:8,background:T.bgSurface}}>
              <div style={{width:8,height:8,borderRadius:"50%",background:s.on?T.green:T.textDim,boxShadow:s.on?`0 0 6px ${T.green}`:"none"}} />
              <div>
                <div style={{fontSize:12,fontWeight:600}}>{s.name}</div>
                <div style={{fontSize:10,color:s.on?T.green:T.textDim}}>{s.sub}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: ULINE ANALYSIS
// ═══════════════════════════════════════════════════════════════
function UlineAnalysis({ ulineWeeks, onUpload, margins }) {
  const [uploading, setUploading] = useState(false);
  const [selectedWeek, setSelectedWeek] = useState(null);
  const fileRef = useRef(null);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const parsed = await parseUlineXlsx(file);
      // Extract date range from filename or use today
      const match = file.name.match(/(\d{1,2}[-\/]\d{1,2}).*?(\d{1,2}[-\/]\d{1,2})/);
      const weekId = new Date().toISOString().slice(0,10);
      const weekData = {
        ...parsed,
        filename: file.name,
        upload_date: new Date().toISOString(),
        week_id: weekId,
      };
      await FS.saveUlineWeek(weekId, weekData);
      onUpload(weekData);
    } catch(e) { alert("Error parsing file: " + e.message); }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
  };

  // Aggregate all weeks
  const allStops = ulineWeeks.flatMap(w => w.stops || []);
  const totalRevenue = ulineWeeks.reduce((s,w) => s + (w.totalRevenue||0), 0);
  const totalStops = ulineWeeks.reduce((s,w) => s + (w.totalStops||0), 0);
  const weekCount = ulineWeeks.length;

  // City breakdown across all weeks
  const byCity = {};
  allStops.forEach(s => { const c=s.city||"Unknown"; if(!byCity[c]) byCity[c]={city:c,stops:0,revenue:0,weight:0}; byCity[c].stops++; byCity[c].revenue+=(s.newCost||s.cost); byCity[c].weight+=s.weight; });
  const cityData = Object.values(byCity).sort((a,b)=>b.revenue-a.revenue);

  // Weight band analysis
  const weightBands = [{label:"0-200 lbs",min:0,max:200},{label:"201-500",min:201,max:500},{label:"501-1000",min:501,max:1000},{label:"1001-2000",min:1001,max:2000},{label:"2000+",min:2001,max:99999}];
  const byWeight = weightBands.map(b => {
    const stops = allStops.filter(s => s.weight >= b.min && s.weight <= b.max);
    return { label:b.label, stops:stops.length, revenue:stops.reduce((s,r)=>s+(r.newCost||r.cost),0), avgRate:stops.length>0?stops.reduce((s,r)=>s+(r.newCost||r.cost),0)/stops.length:0 };
  });

  const detail = selectedWeek || (ulineWeeks.length>0 ? ulineWeeks[0] : null);

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📦" text="Uline Revenue Analysis" right={
        <div>
          <input ref={fileRef} type="file" accept=".xlsx,.xls,.csv" onChange={handleUpload} style={{display:"none"}} />
          <PrimaryBtn text={uploading?"Parsing...":"Upload Uline XLSX"} onClick={()=>fileRef.current?.click()} loading={uploading} />
        </div>
      } />

      {totalStops === 0 ? (
        <EmptyState icon="📤" title="No Uline Data Yet" sub="Upload your weekly Uline audit XLSX files to see revenue analysis" />
      ) : (
        <>
          {/* Summary KPIs */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}>
            <KPI label="Total Revenue" value={fmtK(totalRevenue)} sub={`${weekCount} week${weekCount>1?"s":""} loaded`} subColor={T.green} />
            <KPI label="Total Stops" value={fmtNum(totalStops)} sub={`${fmtNum(Math.round(totalStops/weekCount/5))}/day avg`} />
            <KPI label="Avg Rev/Stop" value={fmt(totalStops>0?totalRevenue/totalStops:0)} />
            <KPI label="Weekly Revenue" value={fmtK(weekCount>0?totalRevenue/weekCount:0)} sub="average" />
            <KPI label="Margin/Stop" value={fmt(margins.marginPerStop)} subColor={margins.marginPerStop>0?T.green:T.red} sub={fmtPct(margins.marginPerStopPct)} />
            <KPI label="Cost/Stop" value={fmt(margins.costPerStop)} sub="fully loaded" subColor={T.red} />
          </div>

          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}>
            {/* Revenue by City */}
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by City (Top 15)</div>
              <BarChart data={cityData} labelKey="city" valueKey="revenue" maxBars={15} />
            </div>

            {/* Revenue by Weight Band */}
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by Weight Band</div>
              <BarChart data={byWeight} labelKey="label" valueKey="revenue" color={T.green} />
              <div style={{marginTop:12,fontSize:12,fontWeight:700,color:T.text}}>Avg Rate by Weight</div>
              {byWeight.map((b,i) => (
                <DataRow key={i} label={b.label} value={`${fmt(b.avgRate)} × ${fmtNum(b.stops)} stops`} />
              ))}
            </div>
          </div>

          {/* Week-by-week table */}
          <div style={{...cardStyle, marginTop:12}}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Uploaded Weeks</div>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead>
                  <tr>{["File","Stops","Revenue","Avg/Stop","Weight"].map(h=><th key={h} style={{textAlign:"left",padding:"8px 10px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {ulineWeeks.map((w,i) => (
                    <tr key={i} onClick={()=>setSelectedWeek(w)} style={{cursor:"pointer",background:detail===w?T.brandPale:"transparent"}}>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis"}}>{w.filename||w.id}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(w.totalStops)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:T.green}}>{fmt(w.totalRevenue)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.avgRevenuePerStop)}</td>
                      <td style={{padding:"8px 10px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(w.totalWeight)} lbs</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: OPERATIONS (NuVizz)
// ═══════════════════════════════════════════════════════════════
function Operations({ margins }) {
  const today = new Date();
  const fmtD = d => d.toISOString().slice(0,10);
  const buildRange = p => {
    const now = new Date();
    if (p==="today") return {from:fmtD(now),to:fmtD(now)};
    if (p==="yesterday") { const y=new Date(now); y.setDate(y.getDate()-1); return {from:fmtD(y),to:fmtD(y)}; }
    if (p==="week") { const m=new Date(now); m.setDate(now.getDate()-now.getDay()+(now.getDay()===0?-6:1)); return {from:fmtD(m),to:fmtD(now)}; }
    if (p==="month") return {from:`${fmtD(now).slice(0,7)}-01`,to:fmtD(now)};
    return {from:fmtD(now),to:fmtD(now)};
  };
  const [period, setPeriod] = useState("week");
  const [from, setFrom] = useState(()=>buildRange("week").from);
  const [to, setTo] = useState(()=>buildRange("week").to);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [view, setView] = useState("overview");

  const applyPeriod = p => { setPeriod(p); const r=buildRange(p); setFrom(r.from); setTo(r.to); };

  const run = async () => {
    setLoading(true); setError(null);
    try {
      const j = await fetchNuVizzStops(from, to);
      if (!j) throw new Error("No response from NuVizz");
      const stops = Array.isArray(j) ? j : (j.stopList||j.stops||j.data||[]);
      setData(stops);
    } catch(e) { setError(e.message); }
    setLoading(false);
  };

  const stats = useMemo(() => {
    if (!data) return null;
    const total = data.length;
    const delivered = data.filter(s=>(s.stopStatus||"").toLowerCase().match(/deliver|complet/)).length;
    const failed = data.filter(s=>(s.stopStatus||"").toLowerCase().match(/fail|cancel/)).length;
    const byDriver = {};
    data.forEach(s => {
      const d = s.driverName||s.driver||"Unassigned";
      if (!byDriver[d]) byDriver[d]={name:d,stops:0,delivered:0,weight:0};
      byDriver[d].stops++;
      byDriver[d].weight += parseFloat(s.weight||s.totalWeight||0)||0;
      if ((s.stopStatus||"").toLowerCase().match(/deliver|complet/)) byDriver[d].delivered++;
    });
    const byDay = {};
    data.forEach(s => {
      const day = (s.schedDTTM||s.scheduledDate||"").slice(0,10);
      if (day) { if(!byDay[day]) byDay[day]={day,stops:0,delivered:0}; byDay[day].stops++; if((s.stopStatus||"").toLowerCase().match(/deliver|complet/)) byDay[day].delivered++; }
    });
    const byRoute = {};
    data.forEach(s => {
      const r = s.loadNbr||s.routeName||"Unknown";
      if (!byRoute[r]) byRoute[r]={route:r,stops:0,driver:s.driverName||"",weight:0};
      byRoute[r].stops++;
      byRoute[r].weight += parseFloat(s.weight||0)||0;
    });
    const driverArr = Object.values(byDriver).sort((a,b)=>b.stops-a.stops);
    const dayArr = Object.values(byDay).sort((a,b)=>a.day.localeCompare(b.day));
    const routeArr = Object.values(byRoute).sort((a,b)=>b.stops-a.stops);
    const daysWorked = dayArr.length || 1;
    return {
      total, delivered, failed, pending:total-delivered-failed,
      rate:total>0?(delivered/total*100):0,
      byDriver:driverArr, byDay:dayArr, byRoute:routeArr,
      avgPerDay:Math.round(total/daysWorked),
      daysWorked,
      // Profitability overlay using margin engine
      estRevenuePerStop: margins.revenuePerStop,
      estCostPerStop: margins.costPerStop,
      estTotalRevenue: total * margins.revenuePerStop,
      estTotalCost: total * margins.costPerStop,
      estMargin: total * margins.marginPerStop,
      estMarginPct: margins.marginPerStopPct,
    };
  }, [data, margins]);

  const periods = [{id:"today",l:"Today"},{id:"yesterday",l:"Yesterday"},{id:"week",l:"This Week"},{id:"month",l:"This Month"}];

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🚚" text="Operations (NuVizz)" />

      {/* Date controls */}
      <div style={{...cardStyle}}>
        <div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:10}}>
          {periods.map(p => <TabButton key={p.id} active={period===p.id} label={p.l} onClick={()=>applyPeriod(p.id)} />)}
        </div>
        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
          <input type="date" value={from} onChange={e=>{setFrom(e.target.value);setPeriod("custom");}} style={{...inputStyle,width:145,fontSize:12}} />
          <span style={{color:T.textDim}}>→</span>
          <input type="date" value={to} onChange={e=>{setTo(e.target.value);setPeriod("custom");}} style={{...inputStyle,width:145,fontSize:12}} />
          <PrimaryBtn text="Run Report" onClick={run} loading={loading} />
        </div>
      </div>

      {error && <div style={{background:T.redBg,color:T.redText,padding:"10px 14px",borderRadius:8,marginBottom:12,fontSize:12}}>⚠️ {error}</div>}

      {stats && (
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Total Stops" value={fmtNum(stats.total)} sub={`${fmtNum(stats.avgPerDay)}/day avg`} />
            <KPI label="Delivered" value={fmtNum(stats.delivered)} subColor={T.green} sub={fmtPct(stats.rate)+" rate"} />
            <KPI label="Failed" value={fmtNum(stats.failed)} subColor={T.red} />
            <KPI label="Est. Revenue" value={fmtK(stats.estTotalRevenue)} subColor={T.green} sub="from stop volume" />
            <KPI label="Est. Cost" value={fmtK(stats.estTotalCost)} subColor={T.red} />
            <KPI label="Est. Margin" value={fmtK(stats.estMargin)} subColor={stats.estMargin>0?T.green:T.red} sub={fmtPct(stats.estMarginPct)} />
          </div>

          <div style={{display:"flex",gap:6,marginBottom:12}}>
            {[["overview","📊 Overview"],["drivers","👤 Drivers"],["routes","🛣 Routes"],["stops","📋 Stops"]].map(([id,l])=>(
              <TabButton key={id} active={view===id} label={l} onClick={()=>setView(id)} />
            ))}
          </div>

          {view==="overview" && (
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}>
              <div style={cardStyle}>
                <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Stops by Day</div>
                <BarChart data={stats.byDay} labelKey="day" valueKey="stops" formatValue={fmtNum} />
              </div>
              <div style={cardStyle}>
                <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Top Drivers by Volume</div>
                <BarChart data={stats.byDriver.slice(0,10)} labelKey="name" valueKey="stops" color={T.green} formatValue={fmtNum} />
              </div>
            </div>
          )}

          {view==="drivers" && (
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Driver Performance</div>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                  <thead><tr>{["Driver","Stops","Delivered","Rate","Est. Revenue","Est. Cost","Est. Margin"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                  <tbody>
                    {stats.byDriver.map((d,i) => {
                      const rate=d.stops>0?(d.delivered/d.stops*100):0;
                      const estRev = d.stops * margins.revenuePerStop;
                      const estCost = d.stops * margins.costPerStop;
                      const estMargin = estRev - estCost;
                      return (
                        <tr key={i}>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.name}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.stops}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.delivered}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={fmtPct(rate)} color={rate>=90?T.greenText:T.redText} bg={rate>=90?T.greenBg:T.redBg} /></td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(estRev)}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(estCost)}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:estMargin>0?T.green:T.red}}>{fmt(estMargin)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {view==="routes" && (
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Route Analysis</div>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                  <thead><tr>{["Route/Load","Driver","Stops","Weight","Est. Revenue","Est. Cost","Est. Margin"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                  <tbody>
                    {stats.byRoute.slice(0,50).map((r,i) => {
                      const estRev = r.stops * margins.revenuePerStop;
                      const estCost = r.stops * margins.costPerStop;
                      return (
                        <tr key={i}>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{r.route}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.driver||"—"}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.stops}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(r.weight)} lbs</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(estRev)}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(estCost)}</td>
                          <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:(estRev-estCost)>0?T.green:T.red}}>{fmt(estRev-estCost)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {view==="stops" && (
            <div style={cardStyle}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Stop Detail ({fmtNum(stats.total)} total — showing first 200)</div>
              <div style={{maxHeight:400,overflowY:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <thead><tr>{["Stop#","Customer","City","Driver","Status","Weight"].map(h=><th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead>
                  <tbody>
                    {data.slice(0,200).map((s,i) => {
                      const st=(s.stopStatus||"").toLowerCase();
                      const isOk=st.match(/deliver|complet/);
                      return (
                        <tr key={i}>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.stopNbr||"—"}</td>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{s.custName||s.customerName||"—"}</td>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.city||"—"}</td>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.driverName||s.driver||"—"}</td>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={s.stopStatus||"—"} color={isOk?T.greenText:T.yellowText} bg={isOk?T.greenBg:T.yellowBg} /></td>
                          <td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.weight||"—"}</td>
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

      {!stats && !loading && <EmptyState icon="🚚" title="Pull NuVizz Data" sub="Select a date range and hit Run Report to see operations data" />}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: FLEET (Motive)
// ═══════════════════════════════════════════════════════════════
function Fleet({ margins }) {
  const [vehicles, setVehicles] = useState(null);
  const [loading, setLoading] = useState(false);
  const [drivers, setDrivers] = useState([]);

  const loadFleet = async () => {
    setLoading(true);
    const [vData, dData] = await Promise.all([fetchMotive("vehicles"), FS.getDrivers()]);
    if (vData?.vehicles || vData?.data) setVehicles(vData.vehicles || vData.data || []);
    setDrivers(dData);
    setLoading(false);
  };

  useEffect(() => { loadFleet(); }, []);

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🚛" text="Fleet & Drivers" right={<PrimaryBtn text="Refresh" onClick={loadFleet} loading={loading} style={{padding:"6px 14px",fontSize:11}} />} />

      {/* Fleet summary KPIs */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}>
        <KPI label="Total Vehicles" value={vehicles?fmtNum(vehicles.length):"—"} />
        <KPI label="Box Trucks" value={fmtNum(margins.totalTrucks - (margins.totalTrucks>20?20:0))} sub="8 MPG" />
        <KPI label="Tractors" value={fmtNum(margins.totalTrucks>20?20:0)} sub="6 MPG" />
        <KPI label="Rev/Truck/Day" value={fmt(margins.revenuePerTruck)} subColor={T.green} />
        <KPI label="Cost/Truck/Day" value={fmt(margins.costPerTruck)} subColor={T.red} />
        <KPI label="Drivers" value={fmtNum(margins.totalDrivers)} />
      </div>

      {/* Vehicles from Motive */}
      {vehicles && vehicles.length > 0 && (
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Motive Vehicles ({vehicles.length})</div>
          <div style={{maxHeight:400,overflowY:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead><tr>{["Vehicle #","Make/Model","Year","Status","Driver"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead>
              <tbody>
                {vehicles.map((v,i) => (
                  <tr key={i}>
                    <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{v.number||v.vehicle_number||v.id||"—"}</td>
                    <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{[v.make,v.model].filter(Boolean).join(" ")||"—"}</td>
                    <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{v.year||"—"}</td>
                    <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={v.status||"active"} color={T.greenText} bg={T.greenBg} /></td>
                    <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{v.current_driver?.first_name?`${v.current_driver.first_name} ${v.current_driver.last_name||""}`:(v.driver_name||"—")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Driver Roster from Firebase */}
      {drivers.length > 0 && (
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Driver Roster ({drivers.length})</div>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead><tr>{["Name","Type","Pay Rate","Status"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
            <tbody>
              {drivers.map(d => (
                <tr key={d.id}>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.name}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={d.type||"W2"} color={d.type==="1099"?T.greenText:T.blueText} bg={d.type==="1099"?T.greenBg:T.blueBg} /></td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.pay_rate?fmt(d.pay_rate)+"/wk":"—"}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={d.active!==false?"Active":"Inactive"} color={d.active!==false?T.greenText:T.textDim} bg={d.active!==false?T.greenBg:T.borderLight} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!vehicles && !loading && <EmptyState icon="🚛" title="Loading Fleet Data" sub="Fetching from Motive..." />}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: QUICKBOOKS
// ═══════════════════════════════════════════════════════════════
function QuickBooksTab({ connected }) {
  const [action, setAction] = useState("pnl");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const year = new Date().getFullYear();
  const [start, setStart] = useState(`${year}-01-01`);
  const [end, setEnd] = useState(new Date().toISOString().slice(0,10));

  const run = async () => {
    setLoading(true);
    const d = await fetchQBO(action, start, end);
    setData(d);
    setLoading(false);
  };

  if (!connected) {
    return (
      <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
        <SectionTitle icon="💰" text="QuickBooks Online" />
        <div style={{...cardStyle, borderColor:T.yellow, background:T.yellowBg, textAlign:"center", padding:30}}>
          <div style={{fontSize:28,marginBottom:8}}>🔗</div>
          <div style={{fontSize:14,fontWeight:700,color:T.yellowText,marginBottom:4}}>QuickBooks Not Connected</div>
          <div style={{fontSize:12,color:T.textMuted,marginBottom:16}}>Connect your QuickBooks Online account to pull live P&L, invoices, expenses, and payroll data.</div>
          <a href="/.netlify/functions/marginiq-qbo-auth" style={{display:"inline-block",padding:"10px 24px",borderRadius:10,background:T.brand,color:"#fff",fontSize:13,fontWeight:700,textDecoration:"none"}}>Connect QuickBooks →</a>
        </div>
      </div>
    );
  }

  const actions = [
    {id:"pnl",l:"P&L Report"},{id:"invoices",l:"Invoices"},{id:"bills",l:"Bills"},
    {id:"expenses",l:"Expenses"},{id:"customers",l:"Customers"},{id:"vendors",l:"Vendors"},
    {id:"employees",l:"Employees"},{id:"accounts",l:"Chart of Accounts"},
  ];

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="💰" text="QuickBooks Online" right={<Badge text="Connected" color={T.greenText} bg={T.greenBg} />} />
      <div style={cardStyle}>
        <div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:10}}>
          {actions.map(a => <TabButton key={a.id} active={action===a.id} label={a.l} onClick={()=>setAction(a.id)} />)}
        </div>
        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
          <input type="date" value={start} onChange={e=>setStart(e.target.value)} style={{...inputStyle,width:145,fontSize:12}} />
          <span style={{color:T.textDim}}>→</span>
          <input type="date" value={end} onChange={e=>setEnd(e.target.value)} style={{...inputStyle,width:145,fontSize:12}} />
          <PrimaryBtn text="Pull Data" onClick={run} loading={loading} />
        </div>
      </div>

      {data && (
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Results — {actions.find(a=>a.id===action)?.l}</div>
          <pre style={{background:T.bgSurface,padding:14,borderRadius:8,overflow:"auto",maxHeight:500,fontSize:11,color:T.text,fontFamily:"'DM Mono','Courier New',monospace",whiteSpace:"pre-wrap",wordBreak:"break-word"}}>
            {JSON.stringify(data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: COST STRUCTURE
// ═══════════════════════════════════════════════════════════════
function CostStructure({ costs, onSave, margins }) {
  const [c, setC] = useState({...DEFAULT_COSTS, ...costs});
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const upd = (k, v) => setC(prev => ({...prev, [k]: v}));

  const save = async () => {
    setSaving(true);
    await FS.saveCosts(c);
    onSave(c);
    setSaving(false);
    setSaved(true);
    setTimeout(()=>setSaved(false), 2000);
  };

  const Field = ({ label, field, prefix, suffix, type="number", step }) => (
    <div style={{marginBottom:8}}>
      <label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:3}}>{label}</label>
      <div style={{display:"flex",alignItems:"center",gap:4}}>
        {prefix && <span style={{fontSize:12,color:T.textDim}}>{prefix}</span>}
        <input type={type} value={c[field]||""} step={step||"any"} onChange={e=>upd(field, parseFloat(e.target.value)||0)} style={{...inputStyle,width:"100%"}} />
        {suffix && <span style={{fontSize:12,color:T.textDim,whiteSpace:"nowrap"}}>{suffix}</span>}
      </div>
    </div>
  );

  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="⚙️" text="Cost Structure" right={
        <div style={{display:"flex",gap:8,alignItems:"center"}}>
          {saved && <Badge text="✓ Saved" color={T.greenText} bg={T.greenBg} />}
          <PrimaryBtn text="Save Changes" onClick={save} loading={saving} style={{padding:"8px 16px",fontSize:12}} />
        </div>
      } />

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
        {/* Facility */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,display:"flex",alignItems:"center",gap:6}}>🏭 Facility Costs</div>
          <Field label="Warehouse (annual)" field="warehouse" prefix="$" />
          <Field label="Forklifts (annual)" field="forklifts" prefix="$" />
        </div>

        {/* Headcounts */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,display:"flex",alignItems:"center",gap:6}}>👥 Headcounts</div>
          <Field label="Box Truck Drivers" field="count_box_drivers" />
          <Field label="Tractor Trailer Drivers" field="count_tractor_drivers" />
          <Field label="Dispatchers" field="count_dispatchers" />
          <Field label="Admin/Office" field="count_admin" />
          <Field label="Mechanics" field="count_mechanics" />
          <Field label="Forklift Operators" field="count_forklift_ops" />
        </div>

        {/* Pay Rates */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,display:"flex",alignItems:"center",gap:6}}>💵 Hourly Rates</div>
          <Field label="Box Truck Driver" field="rate_box_driver" prefix="$" suffix="/hr" step="0.50" />
          <Field label="Tractor Driver" field="rate_tractor_driver" prefix="$" suffix="/hr" step="0.50" />
          <Field label="Dispatcher" field="rate_dispatcher" prefix="$" suffix="/hr" step="0.50" />
          <Field label="Admin" field="rate_admin" prefix="$" suffix="/hr" step="0.50" />
          <Field label="Mechanic" field="rate_mechanic" prefix="$" suffix="/hr" step="0.50" />
        </div>

        {/* Vehicles */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,display:"flex",alignItems:"center",gap:6}}>🚛 Fleet & Fuel</div>
          <Field label="Box Truck Count" field="truck_count_box" />
          <Field label="Tractor Count" field="truck_count_tractor" />
          <Field label="Insurance per truck/month" field="truck_insurance_monthly" prefix="$" />
          <Field label="Box Truck MPG" field="mpg_box" suffix="MPG" />
          <Field label="Tractor MPG" field="mpg_tractor" suffix="MPG" />
          <Field label="Fuel Price" field="fuel_price" prefix="$" suffix="/gal" step="0.01" />
        </div>

        {/* Operations */}
        <div style={cardStyle}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,display:"flex",alignItems:"center",gap:6}}>📅 Operations</div>
          <Field label="Working Days/Year" field="working_days_year" />
          <Field label="Avg Hours/Shift" field="avg_hours_per_shift" />
          <Field label="Contractor Payout %" field="contractor_pct" step="0.01" />
        </div>

        {/* Calculated Summary */}
        <div style={{...cardStyle, background:T.brandPale, borderColor:T.brand}}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,color:T.brand}}>📊 Calculated Totals</div>
          <DataRow label="Annual Labor" value={fmtK(margins.totalAnnualLabor)} valueColor={T.brand} bold />
          <DataRow label="  Box Truck Drivers" value={fmtK(margins.annualBoxDrivers)} />
          <DataRow label="  Tractor Drivers" value={fmtK(margins.annualTractorDrivers)} />
          <DataRow label="  Dispatch" value={fmtK(margins.annualDispatchers)} />
          <DataRow label="  Admin" value={fmtK(margins.annualAdmin)} />
          <DataRow label="  Mechanics" value={fmtK(margins.annualMechanics)} />
          <DataRow label="  Forklift Ops" value={fmtK(margins.annualForkliftOps)} />
          <div style={{height:8}} />
          <DataRow label="Annual Fixed" value={fmtK(margins.totalAnnualFixed)} valueColor={T.brand} bold />
          <DataRow label="  Warehouse" value={fmtK(margins.annualWarehouse)} />
          <DataRow label="  Forklifts" value={fmtK(margins.annualForklifts)} />
          <DataRow label="  Insurance" value={fmtK(margins.annualInsurance)} />
          <div style={{height:8}} />
          <DataRow label="TOTAL ANNUAL COST" value={fmtK(margins.totalAnnualCost)} valueColor={T.red} bold />
          <DataRow label="Daily Cost" value={fmt(margins.dailyCost)} valueColor={T.red} bold />
          <DataRow label="Monthly Cost" value={fmtK(margins.monthlyCost)} valueColor={T.red} bold />
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TAB: SETTINGS
// ═══════════════════════════════════════════════════════════════
function Settings({ qboConnected, motiveConnected }) {
  return (
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="⚙️" text="Settings & Connections" />
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Data Connections</div>
        {[
          { name:"QuickBooks Online", on:qboConnected, sub:qboConnected?"Connected — pulling P&L, invoices, expenses":"Not connected", action:!qboConnected?"/.netlify/functions/marginiq-qbo-auth":null },
          { name:"NuVizz", on:true, sub:"Connected — stops, loads, driver data" },
          { name:"CyberPay (Payroll)", on:true, sub:"Auto-pulls every Monday 9am ET → Firebase" },
          { name:"Motive (Fleet)", on:motiveConnected, sub:motiveConnected?"Connected":"Checking..." },
        ].map((s,i) => (
          <div key={i} style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:i<3?`1px solid ${T.border}`:"none"}}>
            <div style={{width:10,height:10,borderRadius:"50%",background:s.on?T.green:T.textDim,boxShadow:s.on?`0 0 8px ${T.green}`:"none",flexShrink:0}} />
            <div style={{flex:1}}>
              <div style={{fontSize:13,fontWeight:600}}>{s.name}</div>
              <div style={{fontSize:11,color:s.on?T.green:T.textDim}}>{s.sub}</div>
            </div>
            {s.action && <a href={s.action} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.brand}`,color:T.brand,fontSize:12,fontWeight:600,textDecoration:"none"}}>Connect</a>}
          </div>
        ))}
      </div>
      <div style={cardStyle}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>System Info</div>
        {[["Firebase","davismarginiq"],["Netlify","davis-marginiq.netlify.app"],["Version",APP_VERSION],["Firestore","nam5 (US multi-region)"]].map(([l,v])=>(
          <DataRow key={l} label={l} value={v} />
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════
function MarginIQ() {
  const [tab, setTab] = useState("command");
  const [costs, setCosts] = useState(DEFAULT_COSTS);
  const [ulineWeeks, setUlineWeeks] = useState([]);
  const [qboConnected, setQboConnected] = useState(false);
  const [qboData, setQboData] = useState(null);
  const [motiveConnected, setMotiveConnected] = useState(false);
  const [loading, setLoading] = useState(true);

  // Initial data load
  useEffect(() => {
    const init = async () => {
      setLoading(true);
      // Load saved cost structure
      const savedCosts = await FS.getCosts();
      if (savedCosts) setCosts(prev => ({...prev, ...savedCosts}));

      // Load Uline weeks
      const weeks = await FS.getUlineWeeks();
      setUlineWeeks(weeks);

      // Check QBO connection
      try {
        const r = await fetch("/.netlify/functions/marginiq-qbo-data?action=status");
        const d = await r.json();
        if (d.connected) {
          setQboConnected(true);
          // Pull dashboard data
          const yr = new Date().getFullYear();
          const dash = await fetchQBO("dashboard", `${yr}-01-01`, new Date().toISOString().slice(0,10));
          if (dash) setQboData(dash);
        }
      } catch(e) {}

      // Check URL for QBO callback
      const params = new URLSearchParams(window.location.search);
      if (params.get("qbo")==="connected") { setQboConnected(true); window.history.replaceState({},"","/"); }
      if (params.get("qbo")==="error") {
        const reason = params.get("reason") || "unknown";
        const detail = params.get("detail") || "";
        alert("QBO Connect Failed: " + reason + (detail ? "\n" + decodeURIComponent(detail) : ""));
        window.history.replaceState({},"","/");
      }

      // Check Motive
      try {
        const r = await fetch("/.netlify/functions/marginiq-motive?action=vehicles");
        if (r.ok) setMotiveConnected(true);
      } catch(e) {}

      setLoading(false);
    };
    init();
  }, []);

  // Calculate margins whenever costs or Uline data changes
  const margins = useMemo(() => {
    const ulineAgg = ulineWeeks.length > 0 ? {
      totalRevenue: ulineWeeks.reduce((s,w) => s+(w.totalRevenue||0), 0),
      totalStops: ulineWeeks.reduce((s,w) => s+(w.totalStops||0), 0),
      weekCount: ulineWeeks.length,
    } : null;
    return calculateMargins(costs, ulineAgg, null, qboData);
  }, [costs, ulineWeeks, qboData]);

  const handleUlineUpload = (weekData) => {
    setUlineWeeks(prev => [weekData, ...prev.filter(w => w.week_id !== weekData.week_id)]);
  };

  const tabs = [
    { id:"command", icon:"🎯", label:"Command Center" },
    { id:"uline", icon:"📦", label:"Uline" },
    { id:"operations", icon:"🚚", label:"Operations" },
    { id:"fleet", icon:"🚛", label:"Fleet" },
    { id:"quickbooks", icon:"💰", label:"QuickBooks" },
    { id:"costs", icon:"⚙️", label:"Costs" },
    { id:"settings", icon:"🔧", label:"Settings" },
  ];

  return (
    <div style={{minHeight:"100vh",background:T.bg,color:T.text,fontFamily:"'DM Sans',sans-serif"}}>
      {/* Header */}
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"12px 20px",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.95)",backdropFilter:"blur(12px)",position:"sticky",top:0,zIndex:100,boxShadow:"0 1px 4px rgba(0,0,0,0.04)"}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{width:36,height:36,borderRadius:"10px",background:`linear-gradient(135deg,${T.brand},${T.brandLight})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:"15px",fontWeight:800,color:"#fff",boxShadow:`0 2px 8px rgba(30,91,146,0.3)`}}>M</div>
          <div>
            <div style={{fontSize:"15px",fontWeight:800,letterSpacing:"-0.02em",color:T.text}}>Davis MarginIQ</div>
            <div style={{fontSize:"9px",color:T.textDim,letterSpacing:"0.1em",textTransform:"uppercase"}}>Cost Intelligence Platform</div>
          </div>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          {qboConnected && <Badge text="QBO ✓" color={T.greenText} bg={T.greenBg} />}
          <span style={{fontSize:"9px",color:T.textDim,padding:"3px 8px",background:T.bgSurface,borderRadius:"6px",fontWeight:600}}>v{APP_VERSION}</span>
        </div>
      </div>

      {/* Nav */}
      <div style={{display:"flex",gap:"3px",padding:"8px 12px",overflowX:"auto",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.7)"}}>
        {tabs.map(t => (
          <button key={t.id} onClick={()=>setTab(t.id)} style={{
            padding:"7px 14px", borderRadius:"8px", border:"none",
            background:tab===t.id?T.brand:"transparent", color:tab===t.id?"#fff":T.textMuted,
            fontSize:"12px", fontWeight:tab===t.id?700:500, cursor:"pointer", whiteSpace:"nowrap", transition:"all 0.2s",
          }}>{t.icon} {t.label}</button>
        ))}
      </div>

      {/* Loading state */}
      {loading && (
        <div style={{textAlign:"center",padding:"60px 20px"}}>
          <div className="loading-pulse" style={{fontSize:48,marginBottom:12}}>📊</div>
          <div style={{fontSize:14,fontWeight:600,color:T.text}}>Loading MarginIQ...</div>
          <div style={{fontSize:12,color:T.textMuted,marginTop:4}}>Connecting to data sources</div>
        </div>
      )}

      {/* Pages */}
      {!loading && tab==="command" && <CommandCenter margins={margins} ulineData={ulineWeeks.length>0?{weekCount:ulineWeeks.length,totalRevenue:ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0)}:null} qboConnected={qboConnected} qboData={qboData} connections={{nuvizz:true,motive:motiveConnected,cyberpay:true}} />}
      {!loading && tab==="uline" && <UlineAnalysis ulineWeeks={ulineWeeks} onUpload={handleUlineUpload} margins={margins} />}
      {!loading && tab==="operations" && <Operations margins={margins} />}
      {!loading && tab==="fleet" && <Fleet margins={margins} />}
      {!loading && tab==="quickbooks" && <QuickBooksTab connected={qboConnected} />}
      {!loading && tab==="costs" && <CostStructure costs={costs} onSave={setCosts} margins={margins} />}
      {!loading && tab==="settings" && <Settings qboConnected={qboConnected} motiveConnected={motiveConnected} />}
    </div>
  );
}

// Default export for artifact/preview environments
export default MarginIQ;

// Standalone mount for Netlify deployment (index.html loads React + ReactDOM via CDN)
if (typeof ReactDOM !== "undefined" && document.getElementById("root")) {
  const root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(React.createElement(MarginIQ));
}
