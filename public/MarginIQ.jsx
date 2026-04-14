// Davis MarginIQ — Cost Intelligence Platform
// Firebase loaded globally via compat SDK in index.html (window.db, window.fbStorage)

const { useState, useEffect, useCallback, useRef } = React;
const APP_VERSION = "1.3.0";

// ─── Firebase helpers ─────────────────────────────────────────
async function fsGetDrivers() {
  try {
    const snap = await window.db.collection("drivers").get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch(e) { console.warn("fsGetDrivers:", e); return []; }
}
async function fsSetDriver(id, data) {
  try { await window.db.collection("drivers").doc(id).set(data, { merge: true }); return true; }
  catch(e) { console.warn("fsSetDriver:", e); return false; }
}
async function fsGetLatestPayrolls(n = 10) {
  try {
    const snap = await window.db.collection("payroll_runs").orderBy("check_date","desc").limit(n).get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch(e) { console.warn("fsGetPayrolls:", e); return []; }
}
async function fsGetMarginSummaries(n = 12) {
  try {
    const snap = await window.db.collection("margin_summary").orderBy("week_of","desc").limit(n).get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch(e) { console.warn("fsGetMarginSummaries:", e); return []; }
}

// ─── Design tokens ────────────────────────────────────────────
const T = {
  brand:"#1e5b92", brandLight:"#2a7bc8", brandDark:"#143f66",
  accent:"#10b981", accentWarn:"#f59e0b", accentDanger:"#ef4444",
  bg:"#0c1220", bgCard:"#111827", bgCardHover:"#1a2332", bgSurface:"#1f2937",
  text:"#f1f5f9", textMuted:"#94a3b8", textDim:"#64748b",
  border:"#1e293b", borderLight:"#334155",
  green:"#10b981", greenBg:"rgba(16,185,129,0.12)",
  red:"#ef4444", redBg:"rgba(239,68,68,0.12)",
  yellow:"#f59e0b", yellowBg:"rgba(245,158,11,0.12)",
  blue:"#3b82f6", blueBg:"rgba(59,130,246,0.12)",
  radius:"12px", radiusSm:"8px",
  shadow:"0 4px 24px rgba(0,0,0,0.3)",
};

// ─── Formatters ───────────────────────────────────────────────
const fmt = n => n == null || isNaN(n) ? "$0" : "$" + Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtDec = (n,d=2) => n == null || isNaN(n) ? "0" : Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const pctColor = (pct, target=30) => pct >= target ? {color:T.green,bg:T.greenBg} : pct >= target*0.75 ? {color:T.yellow,bg:T.yellowBg} : {color:T.red,bg:T.redBg};

// ─── Styles ───────────────────────────────────────────────────
const S = {
  app: { minHeight:"100vh", background:`linear-gradient(135deg,${T.bg} 0%,#0f1729 50%,#111827 100%)`, color:T.text, fontFamily:"'DM Sans',sans-serif" },
  header: { display:"flex", alignItems:"center", justifyContent:"space-between", padding:"14px 20px", borderBottom:`1px solid ${T.border}`, background:"rgba(12,18,32,0.95)", backdropFilter:"blur(12px)", position:"sticky", top:0, zIndex:100 },
  nav: { display:"flex", gap:"3px", padding:"10px 12px", overflowX:"auto", borderBottom:`1px solid ${T.border}`, background:"rgba(12,18,32,0.6)" },
  navBtn: a => ({ padding:"7px 14px", borderRadius:"8px", border:"none", background:a?T.brand:"transparent", color:a?"#fff":T.textMuted, fontSize:"12px", fontWeight:a?600:500, cursor:"pointer", whiteSpace:"nowrap", transition:"all 0.2s" }),
  page: { padding:"16px", maxWidth:"1200px", margin:"0 auto" },
  kpiRow: { display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))", gap:"10px", marginBottom:"20px" },
  kpiCard: { background:T.bgCard, borderRadius:T.radius, padding:"14px", border:`1px solid ${T.border}` },
  kpiLabel: { fontSize:"10px", color:T.textDim, textTransform:"uppercase", letterSpacing:"0.06em", marginBottom:"4px", fontWeight:600 },
  kpiValue: { fontSize:"20px", fontWeight:700, color:T.text, letterSpacing:"-0.02em" },
  kpiSub: { fontSize:"10px", marginTop:"3px", fontWeight:500 },
  secTitle: { fontSize:"14px", fontWeight:700, color:T.text, marginBottom:"12px", display:"flex", alignItems:"center", gap:"8px" },
  card: { background:T.bgCard, borderRadius:T.radius, padding:"14px", border:`1px solid ${T.border}`, marginBottom:"10px" },
  cardHead: { display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:"8px" },
  cardTitle: { fontSize:"13px", fontWeight:600, color:T.text },
  row: { display:"flex", justifyContent:"space-between", alignItems:"center", padding:"5px 0", borderTop:`1px solid ${T.border}` },
  rowLabel: { fontSize:"11px", color:T.textMuted },
  rowVal: { fontSize:"12px", fontWeight:600, color:T.text },
  input: { width:"100%", padding:"9px 12px", borderRadius:"8px", border:`1px solid ${T.borderLight}`, background:T.bgSurface, color:T.text, fontSize:"13px", outline:"none" },
  primaryBtn: { width:"100%", padding:"12px", borderRadius:"10px", border:"none", background:`linear-gradient(135deg,${T.brand},${T.brandLight})`, color:"#fff", fontSize:"14px", fontWeight:700, cursor:"pointer" },
  table: { width:"100%", borderCollapse:"collapse", fontSize:"12px" },
  th: { textAlign:"left", padding:"8px 10px", borderBottom:`1px solid ${T.borderLight}`, color:T.textDim, fontSize:"10px", fontWeight:600, textTransform:"uppercase", letterSpacing:"0.05em" },
  td: { padding:"8px 10px", borderBottom:`1px solid ${T.border}`, color:T.text },
  empty: { textAlign:"center", padding:"36px 20px", color:T.textMuted, fontSize:"13px" },
  tabToggle: { display:"flex", background:T.bgSurface, borderRadius:"8px", padding:"3px", marginBottom:"14px" },
  tabToggleBtn: a => ({ flex:1, padding:"7px", borderRadius:"6px", border:"none", background:a?T.brand:"transparent", color:a?"#fff":T.textMuted, fontSize:"12px", fontWeight:600, cursor:"pointer" }),
  badge: (c,bg) => ({ fontSize:"10px", fontWeight:700, color:c, background:bg, padding:"2px 7px", borderRadius:"5px" }),
  connBtn: { display:"flex", alignItems:"center", gap:"10px", padding:"12px 16px", borderRadius:T.radius, border:`1px solid ${T.border}`, background:T.bgCard, color:T.text, fontSize:"13px", fontWeight:600, cursor:"pointer", width:"100%", transition:"all 0.2s", marginBottom:"8px" },
  connDot: on => ({ width:9, height:9, borderRadius:"50%", background:on?T.green:T.textDim, boxShadow:on?`0 0 8px ${T.green}`:"none", flexShrink:0 }),
};

// ─── Shared components ────────────────────────────────────────
function KPI({ label, value, sub, subColor }) {
  return (
    <div style={S.kpiCard}>
      <div style={S.kpiLabel}>{label}</div>
      <div style={S.kpiValue}>{value}</div>
      {sub && <div style={{...S.kpiSub, color:subColor||T.textMuted}}>{sub}</div>}
    </div>
  );
}
function MarginBadge({ pct, target=30 }) {
  const c = pctColor(pct, target);
  return <span style={S.badge(c.color,c.bg)}>{fmtDec(pct,1)}%</span>;
}
function ProgressBar({ pct, target=30 }) {
  const c = pctColor(pct, target);
  return (
    <div style={{width:"100%",height:"5px",borderRadius:"3px",background:T.bgSurface,overflow:"hidden"}}>
      <div style={{height:"100%",borderRadius:"3px",background:c.color,width:`${Math.min(Math.max(pct,0),100)}%`,transition:"width 0.6s"}} />
    </div>
  );
}

// ─── Dashboard ────────────────────────────────────────────────
function Dashboard({ qboConnected, setTab }) {
  const [qboData, setQboData] = useState(null);
  const [payrolls, setPayrolls] = useState([]);
  const [loading, setLoading] = useState(true);
  const year = new Date().getFullYear();

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const [pr] = await Promise.all([fsGetLatestPayrolls(5)]);
        setPayrolls(pr);
        if (qboConnected) {
          const r = await fetch(`/.netlify/functions/marginiq-qbo-data?action=dashboard&start=${year}-01-01&end=${new Date().toISOString().slice(0,10)}`);
          if (r.ok) setQboData(await r.json());
        }
      } catch(e) { console.warn(e); }
      setLoading(false);
    };
    load();
  }, [qboConnected]);

  return (
    <div style={S.page}>
      <div style={S.secTitle}>📊 Dashboard — {new Date().getFullYear()}</div>

      {/* QBO KPIs */}
      {qboData && (
        <div style={S.kpiRow}>
          <KPI label="Revenue YTD" value={fmt(qboData.revenue)} subColor={T.green} />
          <KPI label="Total Costs" value={fmt(qboData.costs)} subColor={T.red} />
          <KPI label="Gross Profit" value={fmt(qboData.gross_profit)} subColor={qboData.gross_profit > 0 ? T.green : T.red} />
          <KPI label="Margin %" value={`${fmtDec(qboData.margin_pct,1)}%`} subColor={qboData.margin_pct >= 30 ? T.green : T.red} />
          <KPI label="Invoices" value={qboData.invoice_count} sub="this year" />
          <KPI label="Bills + Expenses" value={qboData.bill_count + qboData.expense_count} sub="this year" />
        </div>
      )}

      {!qboConnected && (
        <div style={{...S.card, borderColor:T.yellow, background:"rgba(245,158,11,0.06)", marginBottom:16}}>
          <div style={{display:"flex",alignItems:"center",gap:12}}>
            <span style={{fontSize:20}}>⚠️</span>
            <div>
              <div style={{fontSize:13,fontWeight:600,color:T.yellow}}>QuickBooks not connected</div>
              <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>Connect QBO in Settings to see live revenue, costs, and margin data</div>
            </div>
            <button onClick={() => setTab("settings")} style={{marginLeft:"auto", padding:"6px 14px", borderRadius:8, border:`1px solid ${T.yellow}`, background:"transparent", color:T.yellow, fontSize:12, fontWeight:600, cursor:"pointer", flexShrink:0}}>Connect →</button>
          </div>
        </div>
      )}

      {/* Recent payroll runs from Firebase */}
      <div style={S.card}>
        <div style={S.cardHead}>
          <div style={S.cardTitle}>📄 Recent Payroll Runs (CyberPay)</div>
          <span style={{fontSize:10,color:T.textDim}}>Auto-pulls every Monday</span>
        </div>
        {payrolls.length === 0 && <div style={S.empty}>{loading ? "Loading..." : "No payroll data yet — runs Monday 9am"}</div>}
        {payrolls.length > 0 && (
          <table style={S.table}>
            <thead><tr>
              <th style={S.th}>Company</th>
              <th style={S.th}>Check Date</th>
              <th style={S.th}>Period</th>
              <th style={S.th}>Run ID</th>
            </tr></thead>
            <tbody>
              {payrolls.map(p => (
                <tr key={p.id}>
                  <td style={S.td}>{p.company_code}</td>
                  <td style={S.td}>{p.check_date}</td>
                  <td style={S.td}>{p.from_date} → {p.to_date}</td>
                  <td style={S.td}>{p.run_id}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Quick links */}
      <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))", gap:10}}>
        {[
          {icon:"📊", label:"Analytics", sub:"NuVizz stops & loads", tab:"analytics"},
          {icon:"💰", label:"QuickBooks", sub:"P&L, invoices, expenses", tab:"quickbooks"},
          {icon:"🚛", label:"Fleet", sub:"Trucks & drivers", tab:"fleet"},
          {icon:"⚙️", label:"Settings", sub:"Connections & config", tab:"settings"},
        ].map(item => (
          <button key={item.tab} onClick={() => setTab(item.tab)} style={{...S.card, textAlign:"left", cursor:"pointer", border:`1px solid ${T.border}`, width:"100%", marginBottom:0}}>
            <div style={{fontSize:24,marginBottom:6}}>{item.icon}</div>
            <div style={{fontSize:13,fontWeight:700,color:T.text}}>{item.label}</div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>{item.sub}</div>
          </button>
        ))}
      </div>
    </div>
  );
}

// ─── Analytics (NuVizz) ───────────────────────────────────────
function Analytics() {
  const today = new Date();
  const fmtDate = d => d.toISOString().slice(0,10);
  const buildRange = p => {
    const now = new Date();
    if (p==="today") return { from:fmtDate(now), to:fmtDate(now) };
    if (p==="yesterday") { const y=new Date(now); y.setDate(y.getDate()-1); return {from:fmtDate(y),to:fmtDate(y)}; }
    if (p==="week") { const m=new Date(now); m.setDate(now.getDate()-now.getDay()+(now.getDay()===0?-6:1)); return {from:fmtDate(m),to:fmtDate(now)}; }
    if (p==="lastweek") { const m=new Date(now); m.setDate(now.getDate()-now.getDay()-6); const s=new Date(m); s.setDate(m.getDate()+6); return {from:fmtDate(m),to:fmtDate(s)}; }
    if (p==="month") return { from:`${fmtDate(now).slice(0,7)}-01`, to:fmtDate(now) };
    if (p==="lastmonth") { const lm=new Date(now.getFullYear(),now.getMonth()-1,1); const lme=new Date(now.getFullYear(),now.getMonth(),0); return {from:fmtDate(lm),to:fmtDate(lme)}; }
    if (p==="year") return { from:`${now.getFullYear()}-01-01`, to:fmtDate(now) };
    return { from:fmtDate(now), to:fmtDate(now) };
  };

  const [period, setPeriod] = useState("week");
  const [from, setFrom] = useState(() => buildRange("week").from);
  const [to, setTo] = useState(() => buildRange("week").to);
  const [view, setView] = useState("stops");
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const applyPeriod = p => { setPeriod(p); const r=buildRange(p); setFrom(r.from); setTo(r.to); };

  const run = async () => {
    setLoading(true); setError(null);
    try {
      const res = await fetch(`/api/nuvizz/stops?fromDTTM=${encodeURIComponent(from+"T00:00:00")}&toDTTM=${encodeURIComponent(to+"T23:59:59")}`);
      if (!res.ok) throw new Error(`NuVizz ${res.status}`);
      const j = await res.json();
      const stops = Array.isArray(j) ? j : (j.stopList||j.stops||j.data||[]);
      setData(stops);
    } catch(e) { setError(e.message); }
    setLoading(false);
  };

  const stats = data ? (() => {
    const total = data.length;
    const delivered = data.filter(s=>(s.stopStatus||"").toLowerCase().match(/deliver|complet/)).length;
    const failed = data.filter(s=>(s.stopStatus||"").toLowerCase().match(/fail|cancel/)).length;
    const byDriver = {};
    data.forEach(s => {
      const d = s.driverName||s.driver||"Unassigned";
      if (!byDriver[d]) byDriver[d] = {name:d, stops:0, delivered:0};
      byDriver[d].stops++;
      if ((s.stopStatus||"").toLowerCase().match(/deliver|complet/)) byDriver[d].delivered++;
    });
    const byDay = {};
    data.forEach(s => {
      const day = (s.schedDTTM||s.scheduledDate||"").slice(0,10);
      if (day) { byDay[day]=(byDay[day]||0)+1; }
    });
    return { total, delivered, failed, pending:total-delivered-failed,
      rate:total>0?(delivered/total*100):0,
      byDriver:Object.values(byDriver).sort((a,b)=>b.stops-a.stops),
      byDay:Object.entries(byDay).sort((a,b)=>a[0].localeCompare(b[0])),
    };
  })() : null;

  const periods = [{id:"today",l:"Today"},{id:"yesterday",l:"Yesterday"},{id:"week",l:"This Week"},{id:"lastweek",l:"Last Week"},{id:"month",l:"This Month"},{id:"lastmonth",l:"Last Month"},{id:"year",l:"This Year"}];

  return (
    <div style={S.page}>
      <div style={S.secTitle}>📊 Operations Analytics</div>
      <div style={S.card}>
        <div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:12}}>
          {periods.map(p => (
            <button key={p.id} onClick={()=>applyPeriod(p.id)} style={{padding:"5px 11px",borderRadius:7,border:`1px solid ${period===p.id?T.brand:T.border}`,background:period===p.id?T.brand:"transparent",color:period===p.id?"#fff":T.textMuted,fontSize:11,fontWeight:600,cursor:"pointer"}}>{p.l}</button>
          ))}
        </div>
        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
          <input type="date" value={from} onChange={e=>{setFrom(e.target.value);setPeriod("custom");}} style={{...S.input,width:145,fontSize:12}} />
          <span style={{color:T.textDim}}>→</span>
          <input type="date" value={to} onChange={e=>{setTo(e.target.value);setPeriod("custom");}} style={{...S.input,width:145,fontSize:12}} />
          <button onClick={run} style={{...S.primaryBtn,width:"auto",padding:"9px 22px",fontSize:12}}>{loading?"Loading...":"Run Report"}</button>
        </div>
      </div>

      {error && <div style={{background:T.redBg,color:T.red,padding:"10px 14px",borderRadius:8,marginBottom:12,fontSize:12}}>⚠️ {error}</div>}

      {stats && (
        <>
          <div style={S.kpiRow}>
            <KPI label="Total Stops" value={stats.total} />
            <KPI label="Delivered" value={stats.delivered} subColor={T.green} sub={`${fmtDec(stats.rate,1)}% rate`} />
            <KPI label="Failed/Cancelled" value={stats.failed} subColor={T.red} />
            <KPI label="Pending" value={stats.pending} subColor={T.yellow} />
          </div>
          <div style={{display:"flex",gap:6,marginBottom:12}}>
            {[["stops","🚚 Stops"],["drivers","👤 Drivers"]].map(([id,l])=>(
              <button key={id} onClick={()=>setView(id)} style={{padding:"6px 16px",borderRadius:7,border:`1px solid ${view===id?T.brand:T.border}`,background:view===id?T.brand:"transparent",color:view===id?"#fff":T.textMuted,fontSize:12,fontWeight:600,cursor:"pointer"}}>{l}</button>
            ))}
          </div>

          {view==="stops" && (
            <div style={S.card}>
              <div style={S.cardTitle}>Stops by Day</div>
              <div style={{marginTop:12,marginBottom:16}}>
                {stats.byDay.map(([day,count])=>{
                  const max = Math.max(...stats.byDay.map(([,c])=>c));
                  const pct = max>0?(count/max*100):0;
                  return (
                    <div key={day} style={{display:"flex",alignItems:"center",gap:10,marginBottom:6}}>
                      <div style={{width:60,fontSize:11,color:T.textMuted,flexShrink:0}}>{day.slice(5)}</div>
                      <div style={{flex:1,height:20,background:T.bgSurface,borderRadius:4,overflow:"hidden"}}>
                        <div style={{height:"100%",width:`${pct}%`,background:`linear-gradient(90deg,${T.brand},${T.brandLight})`,borderRadius:4,display:"flex",alignItems:"center",paddingLeft:6}}>
                          {pct>15&&<span style={{fontSize:10,color:"#fff",fontWeight:700}}>{count}</span>}
                        </div>
                      </div>
                      <div style={{width:28,fontSize:12,fontWeight:700,textAlign:"right"}}>{count}</div>
                    </div>
                  );
                })}
              </div>
              <div style={{maxHeight:320,overflowY:"auto"}}>
                <table style={S.table}>
                  <thead><tr><th style={S.th}>Stop #</th><th style={S.th}>Customer</th><th style={S.th}>City</th><th style={S.th}>Driver</th><th style={S.th}>Status</th></tr></thead>
                  <tbody>
                    {data.slice(0,200).map((s,i)=>{
                      const st = (s.stopStatus||"").toLowerCase();
                      const sc = st.match(/deliver|complet/)?{c:T.green,bg:T.greenBg}:st.match(/fail|cancel/)?{c:T.red,bg:T.redBg}:{c:T.yellow,bg:T.yellowBg};
                      return (
                        <tr key={i}>
                          <td style={S.td}>{s.stopNbr||s.id||"—"}</td>
                          <td style={S.td}>{s.custName||s.customerName||"—"}</td>
                          <td style={S.td}>{s.city||"—"}</td>
                          <td style={S.td}>{s.driverName||s.driver||"—"}</td>
                          <td style={S.td}><span style={{...S.badge(sc.c,sc.bg),fontSize:10}}>{s.stopStatus||"—"}</span></td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {view==="drivers" && (
            <div style={S.card}>
              <div style={S.cardTitle}>Driver Performance</div>
              <table style={{...S.table,marginTop:12}}>
                <thead><tr><th style={S.th}>Driver</th><th style={S.th}>Stops</th><th style={S.th}>Delivered</th><th style={S.th}>Rate</th></tr></thead>
                <tbody>
                  {stats.byDriver.map((d,i)=>{
                    const rate = d.stops>0?(d.delivered/d.stops*100):0;
                    const c = rate>=90?{c:T.green,bg:T.greenBg}:rate>=70?{c:T.yellow,bg:T.yellowBg}:{c:T.red,bg:T.redBg};
                    return (
                      <tr key={i}>
                        <td style={{...S.td,fontWeight:600}}>{d.name}</td>
                        <td style={S.td}>{d.stops}</td>
                        <td style={{...S.td,color:T.green}}>{d.delivered}</td>
                        <td style={S.td}><span style={{...S.badge(c.c,c.bg)}}>{fmtDec(rate,0)}%</span></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {!data && !loading && (
        <div style={{...S.card,textAlign:"center",padding:"48px 20px"}}>
          <div style={{fontSize:36,marginBottom:12}}>📊</div>
          <div style={{fontSize:14,color:T.textMuted}}>Select a date range and click Run Report</div>
          <div style={{fontSize:11,color:T.textDim,marginTop:4}}>Pulls live stop data from NuVizz</div>
        </div>
      )}
    </div>
  );
}

// ─── QuickBooks Page ──────────────────────────────────────────
function QuickBooks({ connected }) {
  const [action, setAction] = useState("pnl");
  const [start, setStart] = useState(`${new Date().getFullYear()}-01-01`);
  const [end, setEnd] = useState(new Date().toISOString().slice(0,10));
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const run = async () => {
    setLoading(true); setError(null); setData(null);
    try {
      const r = await fetch(`/.netlify/functions/marginiq-qbo-data?action=${action}&start=${start}&end=${end}`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.error||`HTTP ${r.status}`);
      setData(j);
    } catch(e) { setError(e.message); }
    setLoading(false);
  };

  if (!connected) return (
    <div style={S.page}>
      <div style={S.secTitle}>💰 QuickBooks Online</div>
      <div style={{...S.card,textAlign:"center",padding:"48px 20px"}}>
        <div style={{fontSize:36,marginBottom:12}}>🔌</div>
        <div style={{fontSize:14,color:T.textMuted,marginBottom:16}}>QuickBooks not connected</div>
        <a href="/.netlify/functions/marginiq-qbo-auth" style={{...S.primaryBtn,display:"inline-block",textDecoration:"none",padding:"12px 24px",width:"auto"}}>Connect QuickBooks →</a>
      </div>
    </div>
  );

  const reports = [
    {id:"dashboard",l:"Dashboard Summary"},
    {id:"pnl",l:"P&L Report"},
    {id:"invoices",l:"Invoices"},
    {id:"bills",l:"Bills"},
    {id:"expenses",l:"Expenses"},
    {id:"payroll",l:"Payroll"},
    {id:"customers",l:"Customers"},
    {id:"vendors",l:"Vendors"},
    {id:"employees",l:"Employees"},
    {id:"accounts",l:"Chart of Accounts"},
  ];

  return (
    <div style={S.page}>
      <div style={S.secTitle}>💰 QuickBooks Online</div>
      <div style={S.card}>
        <div style={{display:"flex",gap:8,flexWrap:"wrap",marginBottom:12}}>
          <select value={action} onChange={e=>setAction(e.target.value)} style={{...S.input,width:"auto",flex:1}}>
            {reports.map(r=><option key={r.id} value={r.id}>{r.l}</option>)}
          </select>
          <input type="date" value={start} onChange={e=>setStart(e.target.value)} style={{...S.input,width:140}} />
          <span style={{color:T.textDim,alignSelf:"center"}}>→</span>
          <input type="date" value={end} onChange={e=>setEnd(e.target.value)} style={{...S.input,width:140}} />
          <button onClick={run} style={{...S.primaryBtn,width:"auto",padding:"9px 22px",fontSize:12}}>{loading?"Loading...":"Pull Data"}</button>
        </div>
      </div>

      {error && <div style={{background:T.redBg,color:T.red,padding:"10px 14px",borderRadius:8,marginBottom:12,fontSize:12}}>⚠️ {error}</div>}

      {/* Dashboard summary */}
      {data && action==="dashboard" && (
        <>
          <div style={S.kpiRow}>
            <KPI label="Revenue" value={fmt(data.revenue)} subColor={T.green} />
            <KPI label="Costs" value={fmt(data.costs)} subColor={T.red} />
            <KPI label="Gross Profit" value={fmt(data.gross_profit)} subColor={data.gross_profit>0?T.green:T.red} />
            <KPI label="Margin" value={`${fmtDec(data.margin_pct,1)}%`} subColor={data.margin_pct>=30?T.green:T.red} />
            <KPI label="Invoices" value={data.invoice_count} />
            <KPI label="Bills" value={data.bill_count} />
            <KPI label="Expenses" value={data.expense_count} />
          </div>
          <div style={S.card}>
            <div style={S.cardTitle}>Period: {data.period?.start} → {data.period?.end}</div>
            {[["Total Revenue",fmt(data.revenue)],["Total Bills",fmt(data.bills)],["Total Expenses",fmt(data.expenses)],["Total Costs",fmt(data.costs)],["Gross Profit",fmt(data.gross_profit)],["Margin %",`${fmtDec(data.margin_pct,1)}%`]].map(([l,v])=>(
              <div key={l} style={S.row}><span style={S.rowLabel}>{l}</span><span style={S.rowVal}>{v}</span></div>
            ))}
          </div>
        </>
      )}

      {/* Invoice / bill / expense list */}
      {data && ["invoices","bills","expenses","payroll"].includes(action) && (
        <div style={S.card}>
          <div style={S.cardTitle}>{reports.find(r=>r.id===action)?.l}</div>
          <div style={{maxHeight:500,overflowY:"auto",marginTop:12}}>
            <pre style={{fontSize:11,color:T.textMuted,whiteSpace:"pre-wrap",wordBreak:"break-all"}}>{JSON.stringify(data,null,2).slice(0,5000)}</pre>
          </div>
        </div>
      )}

      {/* Generic data display */}
      {data && !["dashboard","invoices","bills","expenses","payroll"].includes(action) && (
        <div style={S.card}>
          <div style={S.cardTitle}>{reports.find(r=>r.id===action)?.l}</div>
          <div style={{maxHeight:500,overflowY:"auto",marginTop:12}}>
            <pre style={{fontSize:11,color:T.textMuted,whiteSpace:"pre-wrap",wordBreak:"break-all"}}>{JSON.stringify(data,null,2).slice(0,8000)}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Fleet Page ───────────────────────────────────────────────
function Fleet() {
  const [drivers, setDrivers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [form, setForm] = useState({name:"",type:"W2",pay_rate:"",active:true});

  useEffect(() => {
    fsGetDrivers().then(d=>{ setDrivers(d); setLoading(false); });
  }, []);

  const save = async () => {
    if (!form.name) return;
    const id = form.name.toLowerCase().replace(/\s+/g,"-")+"-"+Date.now();
    await fsSetDriver(id, {...form, pay_rate:parseFloat(form.pay_rate)||0, updated_at:new Date().toISOString()});
    setDrivers(prev=>[...prev,{id,...form,pay_rate:parseFloat(form.pay_rate)||0}]);
    setForm({name:"",type:"W2",pay_rate:"",active:true});
    setAdding(false);
  };

  return (
    <div style={S.page}>
      <div style={S.secTitle}>🚛 Fleet Management</div>
      <div style={{...S.card,marginBottom:16}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:adding?12:0}}>
          <div style={S.cardTitle}>Driver Roster</div>
          <button onClick={()=>setAdding(!adding)} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.brand}`,background:"transparent",color:T.brand,fontSize:12,fontWeight:600,cursor:"pointer"}}>{adding?"Cancel":"+ Add Driver"}</button>
        </div>
        {adding && (
          <div style={{display:"flex",gap:8,flexWrap:"wrap",paddingTop:12,borderTop:`1px solid ${T.border}`}}>
            <input placeholder="Driver name" value={form.name} onChange={e=>setForm(f=>({...f,name:e.target.value}))} style={{...S.input,flex:2,minWidth:140}} />
            <select value={form.type} onChange={e=>setForm(f=>({...f,type:e.target.value}))} style={{...S.input,flex:1,minWidth:100}}>
              <option value="W2">W2</option>
              <option value="1099">1099</option>
            </select>
            <input type="number" placeholder="Pay rate" value={form.pay_rate} onChange={e=>setForm(f=>({...f,pay_rate:e.target.value}))} style={{...S.input,flex:1,minWidth:100}} />
            <button onClick={save} style={{...S.primaryBtn,width:"auto",padding:"9px 20px",fontSize:12}}>Save</button>
          </div>
        )}
      </div>

      {loading && <div style={S.empty}>Loading drivers...</div>}
      {!loading && drivers.length===0 && <div style={S.empty}>No drivers yet — add your first driver above</div>}
      {drivers.length>0 && (
        <div style={S.card}>
          <table style={S.table}>
            <thead><tr><th style={S.th}>Name</th><th style={S.th}>Type</th><th style={S.th}>Pay Rate</th><th style={S.th}>Status</th></tr></thead>
            <tbody>
              {drivers.map(d=>(
                <tr key={d.id}>
                  <td style={{...S.td,fontWeight:600}}>{d.name}</td>
                  <td style={S.td}><span style={S.badge(d.type==="W2"?T.blue:T.green,d.type==="W2"?T.blueBg:T.greenBg)}>{d.type}</span></td>
                  <td style={S.td}>{d.pay_rate?fmt(d.pay_rate)+"/wk":"—"}</td>
                  <td style={S.td}><span style={S.badge(d.active!==false?T.green:T.textDim,d.active!==false?T.greenBg:"rgba(100,116,139,0.15)")}>{d.active!==false?"Active":"Inactive"}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─── Settings Page ────────────────────────────────────────────
function Settings({ qboConnected, motiveConnected }) {
  return (
    <div style={S.page}>
      <div style={S.secTitle}>⚙️ Settings & Connections</div>
      <div style={S.card}>
        <div style={S.cardTitle}>Data Connections</div>
        <div style={{marginTop:12}}>
          {/* QuickBooks */}
          <div style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:`1px solid ${T.border}`}}>
            <div style={S.connDot(qboConnected)} />
            <div style={{flex:1}}>
              <div style={{fontSize:13,fontWeight:600}}>QuickBooks Online</div>
              <div style={{fontSize:11,color:qboConnected?T.green:T.textDim}}>{qboConnected?"Connected — pulling P&L, invoices, expenses":"Not connected"}</div>
            </div>
            {!qboConnected && <a href="/.netlify/functions/marginiq-qbo-auth" style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.brand}`,color:T.brand,fontSize:12,fontWeight:600,textDecoration:"none"}}>Connect</a>}
          </div>
          {/* NuVizz */}
          <div style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:`1px solid ${T.border}`}}>
            <div style={S.connDot(true)} />
            <div style={{flex:1}}>
              <div style={{fontSize:13,fontWeight:600}}>NuVizz</div>
              <div style={{fontSize:11,color:T.green}}>Connected — stops, loads, driver data</div>
            </div>
          </div>
          {/* CyberPay */}
          <div style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:`1px solid ${T.border}`}}>
            <div style={S.connDot(true)} />
            <div style={{flex:1}}>
              <div style={{fontSize:13,fontWeight:600}}>CyberPay (Payroll)</div>
              <div style={{fontSize:11,color:T.green}}>Auto-pulls every Monday 9am ET → Firebase</div>
            </div>
          </div>
          {/* Motive */}
          <div style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0"}}>
            <div style={S.connDot(motiveConnected)} />
            <div style={{flex:1}}>
              <div style={{fontSize:13,fontWeight:600}}>Motive (Fleet)</div>
              <div style={{fontSize:11,color:motiveConnected?T.green:T.textDim}}>{motiveConnected?"Connected":"Checking..."}</div>
            </div>
          </div>
        </div>
      </div>
      <div style={S.card}>
        <div style={S.cardTitle}>Firebase Project</div>
        <div style={{marginTop:8}}>
          {[["Project","davismarginiq"],["Firestore","nam5 (US multi-region)"],["Storage","davismarginiq.firebasestorage.app"],["Plan","Blaze (pay-as-you-go)"]].map(([l,v])=>(
            <div key={l} style={S.row}><span style={S.rowLabel}>{l}</span><span style={{...S.rowVal,color:T.textMuted,fontFamily:"monospace",fontSize:11}}>{v}</span></div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────
function MarginIQ() {
  const [tab, setTab] = useState("dashboard");
  const [qboConnected, setQboConnected] = useState(false);
  const [motiveConnected, setMotiveConnected] = useState(false);

  useEffect(() => {
    // Check QBO connection
    fetch("/.netlify/functions/marginiq-qbo-data?action=status")
      .then(r=>r.json())
      .then(d=>{ if(d.connected) setQboConnected(true); })
      .catch(()=>{});
    // Check URL for QBO callback
    const params = new URLSearchParams(window.location.search);
    if (params.get("qbo")==="connected") { setQboConnected(true); window.history.replaceState({},""," /"); }
    // Check Motive
    fetch("/.netlify/functions/marginiq-motive?action=vehicles")
      .then(r=>{ if(r.ok) setMotiveConnected(true); })
      .catch(()=>{});
  }, []);

  const tabs = [
    {id:"dashboard",l:"Dashboard"},
    {id:"analytics",l:"📊 Analytics"},
    {id:"quickbooks",l:"💰 QuickBooks"},
    {id:"fleet",l:"🚛 Fleet"},
    {id:"settings",l:"⚙️ Settings"},
  ];

  return (
    <div style={S.app}>
      {/* Header */}
      <div style={S.header}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{width:34,height:34,borderRadius:"9px",background:`linear-gradient(135deg,${T.brand},${T.brandLight})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:"16px",fontWeight:800,color:"#fff"}}>M</div>
          <div>
            <div style={{fontSize:"15px",fontWeight:700,letterSpacing:"-0.02em"}}>Davis MarginIQ</div>
            <div style={{fontSize:"9px",color:T.textDim,letterSpacing:"0.08em",textTransform:"uppercase"}}>Cost Intelligence</div>
          </div>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          {qboConnected && <span style={{...S.badge(T.green,T.greenBg),fontSize:9}}>QBO ✓</span>}
          <span style={{fontSize:"9px",color:T.textDim,padding:"2px 6px",background:T.bgSurface,borderRadius:"5px"}}>v{APP_VERSION}</span>
        </div>
      </div>

      {/* Nav */}
      <div style={S.nav}>
        {tabs.map(t=><button key={t.id} style={S.navBtn(tab===t.id)} onClick={()=>setTab(t.id)}>{t.l}</button>)}
      </div>

      {/* Pages */}
      {tab==="dashboard" && <Dashboard qboConnected={qboConnected} setTab={setTab} />}
      {tab==="analytics" && <Analytics />}
      {tab==="quickbooks" && <QuickBooks connected={qboConnected} />}
      {tab==="fleet" && <Fleet />}
      {tab==="settings" && <Settings qboConnected={qboConnected} motiveConnected={motiveConnected} />}
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(React.createElement(MarginIQ));
