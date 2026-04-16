// Davis MarginIQ v3.0 — Complete Cost Intelligence Platform
// Firebase loaded globally via compat SDK in index.html (window.db, window.fbStorage)
// SheetJS loaded globally via CDN (window.XLSX)

const { useState, useEffect, useCallback, useRef, useMemo } = React;
const APP_VERSION = "5.2.0";

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
  orange:"#f97316", orangeBg:"#fff7ed",
  radius:"12px", radiusSm:"8px",
  shadow:"0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
  shadowMd:"0 4px 12px rgba(0,0,0,0.08)",
};

// ─── Default Cost Structure (zeros — populate from real data) ─
const DEFAULT_COSTS = {
  warehouse:0, forklifts:0, forklift_operators:0,
  truck_insurance_monthly:0, truck_count_box:0, truck_count_tractor:0,
  rate_box_driver:0, rate_tractor_driver:0, rate_dispatcher:0, rate_admin:0, rate_mechanic:0,
  count_box_drivers:0, count_tractor_drivers:0, count_dispatchers:0, count_admin:0, count_mechanics:0, count_forklift_ops:0,
  mpg_box:8, mpg_tractor:6, fuel_price:3.50,
  working_days_year:260, avg_hours_per_shift:10, contractor_pct:0.40,
};

// ─── Formatters ──────────────────────────────────────────────
const fmt = n => n==null||isNaN(n)?"$0":"$"+Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const fmtK = n => { if(n==null||isNaN(n)) return "$0"; const v=Number(n); if(Math.abs(v)>=1e6) return "$"+(v/1e6).toFixed(1)+"M"; if(Math.abs(v)>=1000) return "$"+(v/1000).toFixed(1)+"K"; return "$"+v.toFixed(0); };
const fmtDec = (n,d=2) => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtPct = (n,d=1) => n==null||isNaN(n)?"0%":fmtDec(n,d)+"%";
const fmtNum = n => n==null||isNaN(n)?"0":Number(n).toLocaleString("en-US",{maximumFractionDigits:0});

// ─── Firebase Helpers ────────────────────────────────────────
const hasFirebase = typeof window !== "undefined" && window.db;
const FS = {
  async getCosts() { if(!hasFirebase) return null; try { const d=await window.db.collection("marginiq_config").doc("cost_structure").get(); return d.exists?d.data():null; } catch(e) { return null; } },
  async saveCosts(data) { if(!hasFirebase) return false; try { await window.db.collection("marginiq_config").doc("cost_structure").set({...data,updated_at:new Date().toISOString()}); return true; } catch(e) { return false; } },
  async getUlineWeeks() { if(!hasFirebase) return []; try { const s=await window.db.collection("uline_audits").orderBy("upload_date","desc").limit(52).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async saveUlineWeek(weekId,data) { if(!hasFirebase) return false; try { await window.db.collection("uline_audits").doc(weekId).set(data); return true; } catch(e) { return false; } },
  async getPayrolls(n=10) { if(!hasFirebase) return []; try { const s=await window.db.collection("payroll_runs").orderBy("check_date","desc").limit(n).get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
  async getDrivers() { if(!hasFirebase) return []; try { const s=await window.db.collection("drivers").get(); return s.docs.map(d=>({id:d.id,...d.data()})); } catch(e) { return []; } },
};

// ─── API Helpers ─────────────────────────────────────────────
async function fetchNuVizzStops(from,to) {
  try { const r=await fetch(`/api/nuvizz/stops?fromDTTM=${encodeURIComponent(from+"T00:00:00")}&toDTTM=${encodeURIComponent(to+"T23:59:59")}`); if(!r.ok) throw new Error(`NuVizz ${r.status}`); return await r.json(); } catch(e) { console.warn("NuVizz:",e); return null; }
}
async function fetchMotive(action,params="") {
  try { const r=await fetch(`/.netlify/functions/marginiq-motive?action=${action}${params}`); if(!r.ok) return null; return await r.json(); } catch(e) { return null; }
}
async function fetchQBO(action,start,end) {
  try { let url=`/.netlify/functions/marginiq-qbo-data?action=${action}`; if(start) url+=`&start=${start}`; if(end) url+=`&end=${end}`; const r=await fetch(url); if(!r.ok) return null; return await r.json(); } catch(e) { return null; }
}

// ─── Uline XLSX Parser ──────────────────────────────────────
function parseUlineXlsx(file) {
  return new Promise((resolve, reject) => {
    if (typeof XLSX==="undefined") return reject(new Error("XLSX library not loaded"));
    const reader=new FileReader();
    reader.onload=e => {
      try {
        const wb=XLSX.read(e.target.result,{type:"array"});
        const ws=wb.Sheets[wb.SheetNames[0]];
        const raw=XLSX.utils.sheet_to_json(ws,{defval:""});
        const rows=raw.map(r => { const n={}; Object.keys(r).forEach(k => { n[k.toLowerCase().trim()]=r[k]; }); return n; });
        const stops=rows.filter(r=>r.pro||r.order).map(r => ({
          pro:String(r.pro||"").replace(/^0+/,""), order:String(r.order||""),
          customer:String(r.customer||r.consignee||""), city:String(r.city||""),
          state:String(r.st||r.state||""), zip:String(r.zip||""),
          cost:parseFloat(r.cost||r.amount||0)||0, weight:parseFloat(r.wgt||r.weight||0)||0,
          skids:parseInt(r.skid||r.skids||r.pallets||0)||0, loose:parseInt(r.loose||0)||0,
          via:String(r.via||""), extraCost:parseFloat(r["extra cost"]||r.extra_cost||0)||0,
          newCost:parseFloat(r["new cost"]||r.new_cost||0)||0,
          notes:String(r.notes||""), warehouse:String(r.wh||r.warehouse||""),
          sealNbr:String(r.seal||r.sealnbr||r["seal nbr"]||""),
        }));
        const totalRevenue=stops.reduce((s,r)=>s+(r.newCost||r.cost),0);
        const totalWeight=stops.reduce((s,r)=>s+r.weight,0);
        const contractorStops=stops.filter(s=>s.sealNbr);
        const contractorPayout=contractorStops.reduce((s,r)=>s+(r.newCost||r.cost),0)*0.40;
        resolve({ stops, totalRevenue, totalWeight, totalStops:stops.length, avgRevenuePerStop:stops.length>0?totalRevenue/stops.length:0, avgWeightPerStop:stops.length>0?totalWeight/stops.length:0, contractorStops:contractorStops.length, contractorPayout });
      } catch(e) { reject(e); }
    };
    reader.onerror=reject;
    reader.readAsArrayBuffer(file);
  });
}

// ─── MARGIN ENGINE ──────────────────────────────────────────
function calculateMargins(costs, ulineData, qboData) {
  const c={...DEFAULT_COSTS,...costs}; const wd=c.working_days_year||260;
  const annualBoxDrivers=c.count_box_drivers*c.rate_box_driver*c.avg_hours_per_shift*wd;
  const annualTractorDrivers=c.count_tractor_drivers*c.rate_tractor_driver*c.avg_hours_per_shift*wd;
  const annualDispatchers=c.count_dispatchers*c.rate_dispatcher*8*wd;
  const annualAdmin=c.count_admin*c.rate_admin*8*wd;
  const annualMechanics=c.count_mechanics*c.rate_mechanic*8*wd;
  const annualForkliftOps=c.count_forklift_ops*20*8*wd;
  const totalAnnualLabor=annualBoxDrivers+annualTractorDrivers+annualDispatchers+annualAdmin+annualMechanics+annualForkliftOps;
  const totalTrucks=(c.truck_count_box||0)+(c.truck_count_tractor||0);
  const annualInsurance=c.truck_insurance_monthly*totalTrucks*12;
  const totalAnnualFixed=(c.warehouse||0)+(c.forklifts||0)+annualInsurance;
  const totalAnnualCost=totalAnnualLabor+totalAnnualFixed;
  const dailyCost=wd>0?totalAnnualCost/wd:0;
  const dailyStops=ulineData?(ulineData.totalStops/(ulineData.weekCount||1)/5):0;
  const costPerStop=dailyStops>0?dailyCost/dailyStops:0;
  const ulineWeeklyRev=ulineData?ulineData.totalRevenue/(ulineData.weekCount||1):0;
  const dailyRevenue=ulineWeeklyRev/5||(qboData?.revenue>0?qboData.revenue/wd:0);
  const dailyMargin=dailyRevenue-dailyCost;
  const dailyMarginPct=dailyRevenue>0?(dailyMargin/dailyRevenue*100):0;
  const revenuePerStop=dailyStops>0?dailyRevenue/dailyStops:0;
  const marginPerStop=revenuePerStop-costPerStop;
  const marginPerStopPct=revenuePerStop>0?(marginPerStop/revenuePerStop*100):0;
  const totalDrivers=c.count_box_drivers+c.count_tractor_drivers;
  const dailyFuelEst=c.fuel_price>0?((c.truck_count_box*120/(c.mpg_box||8))+(c.truck_count_tractor*200/(c.mpg_tractor||6)))*c.fuel_price:0;
  const breakEvenStops=revenuePerStop>0?dailyCost/revenuePerStop:0;
  return {
    totalAnnualCost,totalAnnualLabor,totalAnnualFixed,annualBoxDrivers,annualTractorDrivers,annualDispatchers,annualAdmin,annualMechanics,annualForkliftOps,annualInsurance,annualWarehouse:c.warehouse||0,annualForklifts:c.forklifts||0,annualFuelEst:dailyFuelEst*wd,
    ulineAnnualRevenue:ulineWeeklyRev*52,qboRevenue:qboData?.revenue||0,monthlyCost:totalAnnualCost/12,
    dailyCost,dailyRevenue,dailyMargin,dailyMarginPct,dailyStops,dailyFuelEst,
    costPerStop,revenuePerStop,marginPerStop,marginPerStopPct,
    totalDrivers,stopsPerDriver:totalDrivers>0?dailyStops/totalDrivers:0,revenuePerDriver:totalDrivers>0?dailyRevenue/totalDrivers:0,costPerDriver:totalDrivers>0?dailyCost/totalDrivers:0,
    totalTrucks,revenuePerTruck:totalTrucks>0?dailyRevenue/totalTrucks:0,costPerTruck:totalTrucks>0?dailyCost/totalTrucks:0,
    breakEvenStops,otExposure:(c.avg_hours_per_shift*5>40?(c.avg_hours_per_shift*5-40)*totalDrivers:0),weeklyHours:c.avg_hours_per_shift*5,
    costBreakdown:[{name:"Box Drivers",value:annualBoxDrivers,color:"#3b82f6"},{name:"Tractor Drivers",value:annualTractorDrivers,color:"#6366f1"},{name:"Warehouse",value:c.warehouse||0,color:"#f59e0b"},{name:"Forklift Ops",value:annualForkliftOps,color:"#10b981"},{name:"Insurance",value:annualInsurance,color:"#ef4444"},{name:"Fuel (est)",value:dailyFuelEst*wd,color:"#f97316"},{name:"Forklifts",value:c.forklifts||0,color:"#8b5cf6"},{name:"Dispatch",value:annualDispatchers,color:"#ec4899"},{name:"Admin",value:annualAdmin,color:"#14b8a6"},{name:"Mechanics",value:annualMechanics,color:"#78716c"}].filter(x=>x.value>0),
  };
}

// ═══════════════════════════════════════════════════════════════
// SHARED UI COMPONENTS
// ═══════════════════════════════════════════════════════════════
const CS={background:T.bgCard,borderRadius:T.radius,padding:"16px",border:`1px solid ${T.border}`,boxShadow:T.shadow,marginBottom:"12px"};
const IS={width:"100%",padding:"8px 12px",borderRadius:"8px",border:`1px solid ${T.border}`,background:T.bgSurface,color:T.text,fontSize:"13px",outline:"none",fontFamily:"inherit"};
function KPI({label,value,sub,subColor,icon}){return(<div style={{...CS,padding:"14px 16px",marginBottom:0}}><div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>{icon&&<span style={{fontSize:14}}>{icon}</span>}<span style={{fontSize:"10px",color:T.textDim,textTransform:"uppercase",letterSpacing:"0.06em",fontWeight:600}}>{label}</span></div><div style={{fontSize:"22px",fontWeight:700,color:T.text,letterSpacing:"-0.02em"}}>{value}</div>{sub&&<div style={{fontSize:"11px",marginTop:"3px",fontWeight:500,color:subColor||T.textMuted}}>{sub}</div>}</div>);}
function Badge({text,color,bg}){return <span style={{fontSize:"10px",fontWeight:700,color:color||T.brand,background:bg||T.brandPale,padding:"2px 8px",borderRadius:"5px",whiteSpace:"nowrap"}}>{text}</span>;}
function MiniBar({pct,color,height=6}){return(<div style={{width:"100%",height,borderRadius:3,background:T.borderLight,overflow:"hidden"}}><div style={{height:"100%",borderRadius:3,background:color||T.brand,width:`${Math.min(Math.max(pct||0,0),100)}%`,transition:"width 0.6s"}}/></div>);}
function SectionTitle({icon,text,right}){return(<div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:"12px"}}><div style={{fontSize:"15px",fontWeight:700,color:T.text,display:"flex",alignItems:"center",gap:"8px"}}>{icon&&<span>{icon}</span>}{text}</div>{right}</div>);}
function DataRow({label,value,valueColor,bold}){return(<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.borderLight}`}}><span style={{fontSize:"12px",color:T.textMuted}}>{label}</span><span style={{fontSize:"13px",fontWeight:bold?700:600,color:valueColor||T.text}}>{value}</span></div>);}
function EmptyState({icon,title,sub}){return(<div style={{textAlign:"center",padding:"40px 20px",color:T.textMuted}}><div style={{fontSize:36,marginBottom:8}}>{icon||"📊"}</div><div style={{fontSize:14,fontWeight:600,color:T.text,marginBottom:4}}>{title}</div><div style={{fontSize:12}}>{sub}</div></div>);}
function TabBtn({active,label,onClick}){return(<button onClick={onClick} style={{padding:"8px 16px",borderRadius:"8px",border:"none",background:active?T.brand:"transparent",color:active?"#fff":T.textMuted,fontSize:"12px",fontWeight:active?700:500,cursor:"pointer",whiteSpace:"nowrap"}}>{label}</button>);}
function PrimaryBtn({text,onClick,loading,style:sx}){return(<button onClick={onClick} disabled={loading} style={{padding:"10px 20px",borderRadius:"10px",border:"none",background:loading?"#94a3b8":`linear-gradient(135deg,${T.brand},${T.brandLight})`,color:"#fff",fontSize:"13px",fontWeight:700,cursor:loading?"wait":"pointer",...sx}}>{loading?"Loading...":text}</button>);}
function BarChart({data,labelKey,valueKey,color,maxBars=15,formatValue}){const items=data.slice(0,maxBars);const max=Math.max(...items.map(d=>d[valueKey]||0),1);const fv=formatValue||fmt;return(<div>{items.map((d,i)=>{const pct=(d[valueKey]||0)/max*100;return(<div key={i} style={{display:"flex",alignItems:"center",gap:10,marginBottom:6}}><div style={{width:90,fontSize:11,color:T.textMuted,flexShrink:0,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={d[labelKey]}>{d[labelKey]}</div><div style={{flex:1,height:22,background:T.borderLight,borderRadius:4,overflow:"hidden"}}><div style={{height:"100%",width:`${pct}%`,background:color||`linear-gradient(90deg,${T.brand},${T.brandLight})`,borderRadius:4,display:"flex",alignItems:"center",paddingLeft:8,transition:"width 0.4s"}}>{pct>20&&<span style={{fontSize:10,color:"#fff",fontWeight:700}}>{fv(d[valueKey])}</span>}</div></div><div style={{width:60,fontSize:12,fontWeight:700,textAlign:"right"}}>{fv(d[valueKey])}</div></div>);})}</div>);}
function DonutChart({data,size=180}){const total=data.reduce((s,d)=>s+d.value,0);if(total===0)return null;const cx=size/2,cy=size/2,r=size*0.35,sw=size*0.15;let ca=-90;const arcs=data.map(d=>{const pct=d.value/total;const a=pct*360;const sa=ca;ca+=a;const ea=ca;const la=a>180?1:0;const rd=Math.PI/180;return{...d,pct,path:`M ${cx+r*Math.cos(sa*rd)} ${cy+r*Math.sin(sa*rd)} A ${r} ${r} 0 ${la} 1 ${cx+r*Math.cos(ea*rd)} ${cy+r*Math.sin(ea*rd)}`};});return(<div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}><svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>{arcs.map((a,i)=><path key={i} d={a.path} fill="none" stroke={a.color} strokeWidth={sw} strokeLinecap="butt"/>)}<text x={cx} y={cy-6} textAnchor="middle" fill={T.text} fontSize="16" fontWeight="800" fontFamily="DM Sans">{fmtK(total)}</text><text x={cx} y={cy+10} textAnchor="middle" fill={T.textMuted} fontSize="9" fontFamily="DM Sans">ANNUAL</text></svg><div style={{flex:1,minWidth:120}}>{arcs.filter(a=>a.pct>0.02).map((a,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}><div style={{width:8,height:8,borderRadius:2,background:a.color,flexShrink:0}}/><span style={{fontSize:11,color:T.textMuted,flex:1}}>{a.name}</span><span style={{fontSize:11,fontWeight:700}}>{fmtPct(a.pct*100,0)}</span></div>)}</div></div>);}

// ═══════════════════════════════════════════════════════════════
// COMMAND CENTER
// ═══════════════════════════════════════════════════════════════
function CommandCenter({margins:m,ulineData,qboConnected,qboData,qbFinancials,connections}){
  const mc=m.dailyMarginPct>=30?T.green:m.dailyMarginPct>=20?T.yellow:T.red;
  const hasData=m.totalAnnualCost>0||m.dailyRevenue>0||qbFinancials;
  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🎯" text="Command Center" right={<span style={{fontSize:10,color:T.textDim}}>{new Date().toLocaleDateString("en-US",{weekday:"long",month:"long",day:"numeric",year:"numeric"})}</span>}/>
      {!hasData&&<div style={{...CS,borderColor:T.yellow,background:T.yellowBg}}><div style={{display:"flex",alignItems:"center",gap:12}}><span style={{fontSize:20}}>📊</span><div><div style={{fontSize:13,fontWeight:600,color:T.yellowText}}>No data yet</div><div style={{fontSize:11,color:T.textMuted}}>Upload Uline XLSX files, connect QuickBooks, or enter costs to see real numbers</div></div></div></div>}

      {/* QB Financial Summary — real numbers from GL/P&L */}
      {qbFinancials&&qbFinancials.totalRevenue>0&&<div style={{...CS,background:T.brandPale,borderLeft:`4px solid ${T.brand}`,marginBottom:16}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:10,flexWrap:"wrap",gap:8}}>
          <div>
            <div style={{fontSize:13,fontWeight:800,color:T.brand}}>📁 QuickBooks Financial Snapshot</div>
            <div style={{fontSize:10,color:T.textMuted}}>From {qbFinancials.source==="gl"?"General Ledger":"P&L Report"} • {qbFinancials.dateStart||"?"} → {qbFinancials.dateEnd||"?"} • {fmtNum(qbFinancials.daysInRange)} days</div>
          </div>
          <Badge text={qbFinancials.source==="gl"?"GL-driven":"P&L-driven"} color={T.brand} bg="#fff"/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:8}}>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Period Revenue</div><div style={{fontSize:18,fontWeight:800,color:T.green}}>{fmtK(qbFinancials.totalRevenue)}</div></div>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Period Expenses</div><div style={{fontSize:18,fontWeight:800,color:T.red}}>{fmtK(qbFinancials.totalExpenses)}</div></div>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Net Income</div><div style={{fontSize:18,fontWeight:800,color:qbFinancials.netIncome>0?T.green:T.red}}>{fmtK(qbFinancials.netIncome)}</div></div>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Margin</div><div style={{fontSize:18,fontWeight:800,color:qbFinancials.marginPct>=15?T.green:qbFinancials.marginPct>=5?T.yellow:T.red}}>{fmtPct(qbFinancials.marginPct)}</div></div>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Annualized Rev</div><div style={{fontSize:14,fontWeight:700,color:T.green}}>{fmtK(qbFinancials.annualRevenue)}/yr</div></div>
          <div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Annualized Cost</div><div style={{fontSize:14,fontWeight:700,color:T.red}}>{fmtK(qbFinancials.annualExpenses)}/yr</div></div>
          {qbFinancials.totalAssets>0&&<div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Total Assets</div><div style={{fontSize:14,fontWeight:700}}>{fmtK(qbFinancials.totalAssets)}</div></div>}
          {qbFinancials.totalLiabilities>0&&<div><div style={{fontSize:10,color:T.textDim,fontWeight:600,textTransform:"uppercase"}}>Total Liabilities</div><div style={{fontSize:14,fontWeight:700}}>{fmtK(qbFinancials.totalLiabilities)}</div></div>}
        </div>
      </div>}

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
        <KPI icon="💰" label="Daily Revenue" value={m.dailyRevenue>0?fmt(m.dailyRevenue):"—"} sub={m.ulineAnnualRevenue>0?`${fmtK(m.ulineAnnualRevenue)}/yr`:"No data"} subColor={m.dailyRevenue>0?T.green:T.textDim}/>
        <KPI icon="📉" label="Daily Cost" value={m.dailyCost>0?fmt(m.dailyCost):"—"} sub={m.totalAnnualCost>0?`${fmtK(m.totalAnnualCost)}/yr`:"Enter costs"} subColor={m.dailyCost>0?T.red:T.textDim}/>
        <KPI icon="📊" label="Daily Margin" value={m.dailyRevenue>0?fmt(m.dailyMargin):"—"} sub={m.dailyRevenue>0?fmtPct(m.dailyMarginPct):"—"} subColor={mc}/>
        <KPI icon="🚚" label="Daily Stops" value={m.dailyStops>0?fmtNum(m.dailyStops):"—"} sub={m.breakEvenStops>0?`${fmtNum(m.breakEvenStops)} break-even`:"—"}/>
        <KPI icon="🎯" label="Rev/Stop" value={m.revenuePerStop>0?fmt(m.revenuePerStop):"—"} sub={m.costPerStop>0?`${fmt(m.costPerStop)} cost`:"—"} subColor={T.blue}/>
        <KPI icon="⛽" label="Daily Fuel" value={m.dailyFuelEst>0?fmt(m.dailyFuelEst):"—"} sub={m.annualFuelEst>0?`${fmtK(m.annualFuelEst)}/yr`:"Set MPG"} subColor={T.orange}/>
        <KPI icon="👤" label="Drivers" value={m.totalDrivers>0?fmtNum(m.totalDrivers):"—"} sub={m.stopsPerDriver>0?`${fmtDec(m.stopsPerDriver,0)} stops/day ea`:"—"}/>
        <KPI icon="⏰" label="OT Risk" value={m.otExposure>0?`${fmtNum(m.otExposure)} hrs/wk`:"None"} sub={`${m.weeklyHours}hr work weeks`} subColor={m.otExposure>0?T.red:T.green}/>
      </div>
      {m.totalAnnualCost>0&&<div style={{...CS,borderLeft:`4px solid ${mc}`,marginBottom:16}}><div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8}}><div><div style={{fontSize:13,fontWeight:700}}>Margin Health</div><div style={{fontSize:11,color:T.textMuted}}>Target: 30%</div></div><div style={{fontSize:28,fontWeight:800,color:mc}}>{m.dailyRevenue>0?fmtPct(m.dailyMarginPct):"—"}</div></div><MiniBar pct={m.dailyMarginPct*(100/50)} color={mc} height={10}/></div>}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:"12px"}}>
        {m.costBreakdown.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:4}}>Annual Cost Breakdown</div>{qbFinancials?<div style={{fontSize:10,color:T.textMuted,marginBottom:8}}>From QuickBooks GL</div>:null}<DonutChart data={m.costBreakdown}/></div>}
        {(m.revenuePerStop>0||m.costPerStop>0)&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Per-Unit Economics</div><DataRow label="Revenue/stop" value={fmt(m.revenuePerStop)} valueColor={T.green}/><DataRow label="Cost/stop" value={fmt(m.costPerStop)} valueColor={T.red}/><DataRow label="Margin/stop" value={fmt(m.marginPerStop)} valueColor={m.marginPerStop>0?T.green:T.red} bold/><div style={{height:8}}/><DataRow label="Revenue/driver/day" value={fmt(m.revenuePerDriver)}/><DataRow label="Cost/driver/day" value={fmt(m.costPerDriver)}/><DataRow label="Revenue/truck/day" value={fmt(m.revenuePerTruck)}/></div>}
      </div>

      {/* Top QB expense categories */}
      {qbFinancials?.categoryTotals&&Object.keys(qbFinancials.categoryTotals).length>0&&<div style={{...CS,marginTop:12}}>
        <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Top Expense Categories (QuickBooks)</div>
        <BarChart data={Object.entries(qbFinancials.categoryTotals).filter(([,v])=>v>0).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([name,value])=>({name,value}))} labelKey="name" valueKey="value" color={T.red} maxBars={10}/>
      </div>}

      <div style={{...CS,marginTop:12}}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Data Sources</div><div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:8}}>{[{name:"Uline",on:!!ulineData,sub:ulineData?`${ulineData.weekCount} wks`:"Upload xlsx"},{name:"QuickBooks Files",on:!!qbFinancials,sub:qbFinancials?`${qbFinancials.source==="gl"?"GL loaded":"P&L loaded"}`:"Upload in QB Import"},{name:"NuVizz",on:connections.nuvizz,sub:"Configured"},{name:"QBO API",on:qboConnected,sub:qboConnected?"Connected":"Not connected"},{name:"Motive",on:connections.motive,sub:connections.motive?"Connected":"Check API"},{name:"CyberPay",on:true,sub:"Auto Monday"}].map(s=><div key={s.name} style={{display:"flex",alignItems:"center",gap:8,padding:"8px 12px",borderRadius:8,background:T.bgSurface}}><div style={{width:8,height:8,borderRadius:"50%",background:s.on?T.green:T.textDim}}/><div><div style={{fontSize:12,fontWeight:600}}>{s.name}</div><div style={{fontSize:10,color:s.on?T.green:T.textDim}}>{s.sub}</div></div></div>)}</div></div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ULINE TAB
// ═══════════════════════════════════════════════════════════════
function UlineTab({ulineWeeks,onUpload,margins}){
  const [uploading,setUploading]=useState(false);const [view,setView]=useState("overview");const fileRef=useRef(null);
  const handleUpload=async e=>{const f=e.target.files?.[0];if(!f)return;setUploading(true);try{
    const p=await parseUlineXlsx(f);
    // PRO dedup check
    const existingPros=new Set();ulineWeeks.forEach(w=>(w.stops||[]).forEach(s=>{if(s.pro)existingPros.add(s.pro);}));
    const dupCount=p.stops.filter(s=>s.pro&&existingPros.has(s.pro)).length;
    const newStops=p.stops.filter(s=>!s.pro||!existingPros.has(s.pro));
    if(newStops.length===0&&dupCount>0){alert(`All ${dupCount} stops already imported (duplicate PROs). File skipped.`);setUploading(false);return;}
    const stopsToSave=newStops.length<p.stops.length?newStops:p.stops;
    const rev=stopsToSave.reduce((s,r)=>s+(r.newCost||r.cost),0);const wt=stopsToSave.reduce((s,r)=>s+r.weight,0);
    const cs=stopsToSave.filter(s=>s.sealNbr);const cp=cs.reduce((s,r)=>s+(r.newCost||r.cost),0)*0.40;
    const weekId=`${f.name}_${Date.now()}`.replace(/[\/\s\.]/g,"_").substring(0,200);
    const weekData={stops:stopsToSave,totalRevenue:rev,totalWeight:wt,totalStops:stopsToSave.length,avgRevenuePerStop:stopsToSave.length>0?rev/stopsToSave.length:0,avgWeightPerStop:stopsToSave.length>0?wt/stopsToSave.length:0,contractorStops:cs.length,contractorPayout:cp,filename:f.name,upload_date:new Date().toISOString(),week_id:weekId};
    await FS.saveUlineWeek(weekId,weekData);onUpload(weekData);
    if(dupCount>0)alert(`Imported ${stopsToSave.length} new stops, skipped ${dupCount} duplicate PROs.`);
  }catch(e){alert("Error: "+e.message);}setUploading(false);if(fileRef.current)fileRef.current.value="";};
  const allStops=ulineWeeks.flatMap(w=>w.stops||[]);const totalRev=ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0);const totalStops=ulineWeeks.reduce((s,w)=>s+(w.totalStops||0),0);const wc=ulineWeeks.length;const totalContr=ulineWeeks.reduce((s,w)=>s+(w.contractorPayout||0),0);
  const byCity={};allStops.forEach(s=>{const c=s.city||"?";if(!byCity[c])byCity[c]={city:c,stops:0,revenue:0,weight:0};byCity[c].stops++;byCity[c].revenue+=(s.newCost||s.cost);byCity[c].weight+=s.weight;});const cityData=Object.values(byCity).sort((a,b)=>b.revenue-a.revenue);
  const byWeight=[{l:"0-200",mn:0,mx:200},{l:"201-500",mn:201,mx:500},{l:"501-1K",mn:501,mx:1000},{l:"1K-2K",mn:1001,mx:2000},{l:"2K+",mn:2001,mx:99999}].map(b=>{const st=allStops.filter(s=>s.weight>=b.mn&&s.weight<=b.mx);return{label:b.l,stops:st.length,revenue:st.reduce((s,r)=>s+(r.newCost||r.cost),0)};});
  const byCust={};allStops.forEach(s=>{const c=s.customer||"?";if(!byCust[c])byCust[c]={customer:c,stops:0,revenue:0};byCust[c].stops++;byCust[c].revenue+=(s.newCost||s.cost);});const custData=Object.values(byCust).sort((a,b)=>b.revenue-a.revenue);
  const byZip={};allStops.forEach(s=>{const z=s.zip||"?";if(!byZip[z])byZip[z]={zip:z,city:s.city,stops:0,revenue:0};byZip[z].stops++;byZip[z].revenue+=(s.newCost||s.cost);});const zipData=Object.values(byZip).sort((a,b)=>b.revenue-a.revenue);

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📦" text="Uline Analysis" right={<div><input ref={fileRef} type="file" accept=".xlsx,.xls" onChange={handleUpload} style={{display:"none"}}/><PrimaryBtn text={uploading?"Parsing...":"Upload XLSX"} onClick={()=>fileRef.current?.click()} loading={uploading}/></div>}/>
      {totalStops===0?<EmptyState icon="📤" title="No Uline Data" sub="Upload weekly audit XLSX files"/>:(
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Revenue" value={fmtK(totalRev)} sub={`${wc} weeks`} subColor={T.green}/><KPI label="Stops" value={fmtNum(totalStops)} sub={`${fmtNum(Math.round(totalStops/wc/5))}/day`}/><KPI label="Avg/Stop" value={fmt(totalStops>0?totalRev/totalStops:0)}/><KPI label="Weekly Avg" value={fmtK(wc>0?totalRev/wc:0)}/><KPI label="Contractor Pay" value={fmtK(totalContr)} sub="40% seal" subColor={T.red}/><KPI label="Net Retained" value={fmtK(totalRev-totalContr)} subColor={T.green}/>
          </div>
          <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>{[["overview","📊 Overview"],["cities","🏙 Cities"],["customers","🏢 Customers"],["weight","⚖️ Weight"],["zips","📍 ZIPs"],["weeks","📅 Weeks"]].map(([id,l])=><TabBtn key={id} active={view===id} label={l} onClick={()=>setView(id)}/>)}</div>
          {view==="overview"&&<div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}><div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by City</div><BarChart data={cityData} labelKey="city" valueKey="revenue" maxBars={15}/></div><div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by Weight</div><BarChart data={byWeight} labelKey="label" valueKey="revenue" color={T.green}/></div></div>}
          {view==="cities"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>All Cities ({cityData.length})</div><div style={{maxHeight:500,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["City","Stops","Revenue","Avg/Stop","Weight"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{cityData.map((c,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.city}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{c.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(c.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(c.stops>0?c.revenue/c.stops:0)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(c.weight)}</td></tr>)}</tbody></table></div></div>}
          {view==="customers"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Customer Breakdown ({custData.length})</div><div style={{maxHeight:500,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Customer","Stops","Revenue","% Total","Avg/Stop"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{custData.map((c,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{c.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(c.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(totalRev>0?c.revenue/totalRev*100:0)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(c.stops>0?c.revenue/c.stops:0)}</td></tr>)}</tbody></table></div></div>}
          {view==="weight"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Weight Band Analysis</div><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Band","Stops","Revenue","% Rev"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{byWeight.map((b,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{b.label}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(b.stops)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(b.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(totalRev>0?b.revenue/totalRev*100:0)}</td></tr>)}</tbody></table></div>}
          {view==="zips"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Top 50 ZIP Codes</div><div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["ZIP","City","Stops","Revenue"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{zipData.slice(0,50).map((z,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{z.zip}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{z.city}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{z.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(z.revenue)}</td></tr>)}</tbody></table></div></div>}
          {view==="weeks"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Uploaded Weeks</div><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["File","Stops","Revenue","Avg/Stop","Contractor"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{ulineWeeks.map((w,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{w.filename||w.id}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(w.totalStops)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(w.totalRevenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.avgRevenuePerStop)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(w.contractorPayout||0)}</td></tr>)}</tbody></table></div>}
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ROUTE ANALYSIS
// ═══════════════════════════════════════════════════════════════
function RouteTab({margins}){
  const [from,setFrom]=useState(()=>new Date().toISOString().slice(0,10));const [to,setTo]=useState(()=>new Date().toISOString().slice(0,10));
  const [loading,setLoading]=useState(false);const [data,setData]=useState(null);const [error,setError]=useState(null);const [view,setView]=useState("overview");
  const applyPeriod=p=>{const now=new Date();const fd=d=>d.toISOString().slice(0,10);if(p==="today"){setFrom(fd(now));setTo(fd(now));}if(p==="week"){const m=new Date(now);m.setDate(now.getDate()-now.getDay()+(now.getDay()===0?-6:1));setFrom(fd(m));setTo(fd(now));}if(p==="month"){setFrom(`${fd(now).slice(0,7)}-01`);setTo(fd(now));}};
  const run=async()=>{setLoading(true);setError(null);try{const j=await fetchNuVizzStops(from,to);if(!j)throw new Error("No response");const stops=Array.isArray(j)?j:(j.stopList||j.stops||j.data||[]);setData(stops);}catch(e){setError(e.message);}setLoading(false);};
  const stats=useMemo(()=>{if(!data)return null;const t=data.length;const del=data.filter(s=>(s.stopStatus||"").toLowerCase().match(/deliver|complet/)).length;
    const byD={};data.forEach(s=>{const d=s.driverName||s.driver||"Unassigned";if(!byD[d])byD[d]={name:d,stops:0,delivered:0,weight:0,routes:new Set()};byD[d].stops++;byD[d].weight+=parseFloat(s.weight||0)||0;if((s.stopStatus||"").toLowerCase().match(/deliver|complet/))byD[d].delivered++;if(s.loadNbr)byD[d].routes.add(s.loadNbr);});
    const byR={};data.forEach(s=>{const r=s.loadNbr||s.routeName||"?";if(!byR[r])byR[r]={route:r,driver:s.driverName||"",stops:0,del:0,weight:0};byR[r].stops++;byR[r].weight+=parseFloat(s.weight||0)||0;if((s.stopStatus||"").toLowerCase().match(/deliver|complet/))byR[r].del++;});
    const byDay={};data.forEach(s=>{const day=(s.schedDTTM||s.scheduledDate||"").slice(0,10);if(day){if(!byDay[day])byDay[day]={day,stops:0};byDay[day].stops++;}});
    return{total:t,delivered:del,rate:t>0?del/t*100:0,byDriver:Object.values(byD).map(d=>({...d,routes:d.routes.size,rate:d.stops>0?d.delivered/d.stops*100:0})).sort((a,b)=>b.stops-a.stops),byRoute:Object.values(byR).sort((a,b)=>b.stops-a.stops),byDay:Object.values(byDay).sort((a,b)=>a.day.localeCompare(b.day)),estRev:t*margins.revenuePerStop,estMargin:t*margins.marginPerStop};
  },[data,margins]);

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🛣️" text="Route Analysis"/>
      <div style={CS}><div style={{display:"flex",gap:6,marginBottom:10}}>{[["today","Today"],["week","Week"],["month","Month"]].map(([id,l])=><TabBtn key={id} label={l} onClick={()=>applyPeriod(id)}/>)}</div><div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}><input type="date" value={from} onChange={e=>setFrom(e.target.value)} style={{...IS,width:145,fontSize:12}}/><span style={{color:T.textDim}}>→</span><input type="date" value={to} onChange={e=>setTo(e.target.value)} style={{...IS,width:145,fontSize:12}}/><PrimaryBtn text="Run" onClick={run} loading={loading}/></div></div>
      {error&&<div style={{background:T.redBg,color:T.redText,padding:"10px 14px",borderRadius:8,marginBottom:12,fontSize:12}}>⚠️ {error}</div>}
      {stats&&(<>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:"10px",marginBottom:"14px"}}><KPI label="Stops" value={fmtNum(stats.total)}/><KPI label="Delivered" value={fmtNum(stats.delivered)} sub={fmtPct(stats.rate)} subColor={T.green}/><KPI label="Routes" value={fmtNum(stats.byRoute.length)}/><KPI label="Drivers" value={fmtNum(stats.byDriver.length)}/><KPI label="Est Revenue" value={fmtK(stats.estRev)} subColor={T.green}/><KPI label="Est Margin" value={fmtK(stats.estMargin)} subColor={stats.estMargin>0?T.green:T.red}/></div>
        <div style={{display:"flex",gap:6,marginBottom:12}}>{[["overview","📊 Overview"],["routes","🛣️ Routes"],["drivers","👤 Drivers"],["stops","📋 Stops"]].map(([id,l])=><TabBtn key={id} active={view===id} label={l} onClick={()=>setView(id)}/>)}</div>
        {view==="overview"&&<div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}><div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Daily Volume</div><BarChart data={stats.byDay} labelKey="day" valueKey="stops" formatValue={fmtNum}/></div><div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Top Drivers</div><BarChart data={stats.byDriver.slice(0,10)} labelKey="name" valueKey="stops" color={T.green} formatValue={fmtNum}/></div></div>}
        {view==="routes"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Routes ({stats.byRoute.length})</div><div style={{maxHeight:500,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Route","Driver","Stops","Del","Weight","Est Rev","Est Margin"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{stats.byRoute.slice(0,100).map((r,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{r.route}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.driver}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.del}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(r.weight)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green}}>{fmt(r.stops*margins.revenuePerStop)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:r.stops*margins.marginPerStop>0?T.green:T.red,fontWeight:600}}>{fmt(r.stops*margins.marginPerStop)}</td></tr>)}</tbody></table></div></div>}
        {view==="drivers"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Driver Performance</div><div style={{maxHeight:500,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Driver","Stops","Delivered","Rate","Routes","Est Revenue","Est Cost","Margin"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{stats.byDriver.map((d,i)=>{const rev=d.stops*margins.revenuePerStop;const cost=d.stops*margins.costPerStop;return(<tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.name}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.delivered}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={fmtPct(d.rate)} color={d.rate>=90?T.greenText:T.redText} bg={d.rate>=90?T.greenBg:T.redBg}/></td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.routes}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green}}>{fmt(rev)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(cost)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:(rev-cost)>0?T.green:T.red}}>{fmt(rev-cost)}</td></tr>);})}</tbody></table></div></div>}
        {view==="stops"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Stops (first 200)</div><div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}><thead><tr>{["Stop#","Customer","City","Driver","Status"].map(h=><th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead><tbody>{data.slice(0,200).map((s,i)=>{const ok=(s.stopStatus||"").toLowerCase().match(/deliver|complet/);return(<tr key={i}><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.stopNbr||"—"}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{s.custName||s.customerName||"—"}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.city||"—"}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.driverName||"—"}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={s.stopStatus||"—"} color={ok?T.greenText:T.yellowText} bg={ok?T.greenBg:T.yellowBg}/></td></tr>);})}</tbody></table></div></div>}
      </>)}
      {!stats&&!loading&&<EmptyState icon="🛣️" title="Pull Route Data" sub="Select dates and run"/>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CUSTOMER PROFITABILITY
// ═══════════════════════════════════════════════════════════════
function CustomerTab({ulineWeeks,margins,qbFinancials}){
  const allStops=ulineWeeks.flatMap(w=>w.stops||[]);const byC={};allStops.forEach(s=>{const c=s.customer||"?";if(!byC[c])byC[c]={customer:c,ulineStops:0,ulineRevenue:0,weight:0};byC[c].ulineStops++;byC[c].ulineRevenue+=(s.newCost||s.cost);byC[c].weight+=s.weight;});
  // Merge QB customer revenue
  const qbCustMap={};(qbFinancials?.customerRevenue||[]).forEach(qc=>{qbCustMap[qc.customer.toLowerCase()]=qc.revenue;});
  // Combine all customer sources
  const allCustomerNames=new Set([...Object.keys(byC),...(qbFinancials?.customerRevenue||[]).map(qc=>qc.customer)]);
  const customers=[...allCustomerNames].map(name=>{
    const uline=byC[name]||{customer:name,ulineStops:0,ulineRevenue:0,weight:0};
    const qbRev=qbCustMap[name.toLowerCase()]||0;
    const totalRev=uline.ulineRevenue+qbRev;
    return{
      customer:name,
      stops:uline.ulineStops,
      ulineRevenue:uline.ulineRevenue,
      qbRevenue:qbRev,
      revenue:totalRev,
      weight:uline.weight,
      avg:uline.ulineStops>0?uline.ulineRevenue/uline.ulineStops:0,
      estCost:uline.ulineStops*margins.costPerStop,
      estMargin:totalRev-(uline.ulineStops*margins.costPerStop),
      marginPct:totalRev>0?((totalRev-(uline.ulineStops*margins.costPerStop))/totalRev*100):0,
      sources:[uline.ulineStops>0?"Uline":null,qbRev>0?"QB":null].filter(Boolean).join("+"),
    };
  }).filter(c=>c.revenue>0).sort((a,b)=>b.revenue-a.revenue);
  const totalRev=customers.reduce((s,c)=>s+c.revenue,0);
  const ulineOnlyTotal=customers.reduce((s,c)=>s+c.ulineRevenue,0);
  const qbOnlyTotal=customers.reduce((s,c)=>s+c.qbRevenue,0);

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🏢" text="Customer Profitability" right={qbFinancials&&<Badge text="QB + Uline merged" color={T.greenText} bg={T.greenBg}/>}/>
      {customers.length===0?<EmptyState icon="🏢" title="No Customer Data" sub="Upload Uline XLSX files or QuickBooks Sales by Customer / General Ledger"/>:(
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Customers" value={fmtNum(customers.length)}/>
            <KPI label="Total Revenue" value={fmtK(totalRev)} subColor={T.green}/>
            {qbOnlyTotal>0&&<KPI label="From QuickBooks" value={fmtK(qbOnlyTotal)} sub="all sources" subColor={T.blue}/>}
            {ulineOnlyTotal>0&&<KPI label="From Uline Files" value={fmtK(ulineOnlyTotal)} sub="weekly audits" subColor={T.orange}/>}
            <KPI label="#1 Customer" value={fmtK(customers[0]?.revenue)} sub={customers[0]?.customer?.slice(0,20)}/>
            {customers[0]&&<KPI label="Top customer %" value={fmtPct(totalRev>0?customers[0].revenue/totalRev*100:0)} sub="concentration risk" subColor={customers[0].revenue/totalRev>0.5?T.red:T.yellow}/>}
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Rankings by Revenue ({customers.length} customers)</div>
            <div style={{maxHeight:600,overflowY:"auto",overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:700}}>
              <thead><tr>{["#","Customer","Source","Uline Rev","QB Rev","Total","% Total","Stops","Margin","Margin %"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead>
              <tbody>{customers.map((c,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textDim}}>{i+1}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={c.sources} color={T.blueText} bg={T.blueBg}/></td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:c.ulineRevenue>0?T.orange:T.textDim}}>{c.ulineRevenue>0?fmt(c.ulineRevenue):"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:c.qbRevenue>0?T.blue:T.textDim}}>{c.qbRevenue>0?fmt(c.qbRevenue):"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(c.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(totalRev>0?c.revenue/totalRev*100:0)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{c.stops||"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:c.estMargin>0?T.green:T.red}}>{c.stops>0?fmt(c.estMargin):"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{c.stops>0?<Badge text={fmtPct(c.marginPct)} color={c.marginPct>=30?T.greenText:c.marginPct>=15?T.yellowText:T.redText} bg={c.marginPct>=30?T.greenBg:c.marginPct>=15?T.yellowBg:T.redBg}/>:<span style={{color:T.textDim,fontSize:11}}>—</span>}</td></tr>)}</tbody>
            </table></div>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue Distribution (Top 20)</div><BarChart data={customers.slice(0,20)} labelKey="customer" valueKey="revenue" maxBars={20}/></div>
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// FLEET TAB
// ═══════════════════════════════════════════════════════════════
function FleetTab({margins}){
  const [vehicles,setVehicles]=useState(null);const [drivers,setDrivers]=useState([]);const [loading,setLoading]=useState(false);
  const load=async()=>{setLoading(true);const [v,d]=await Promise.all([fetchMotive("vehicles"),FS.getDrivers()]);if(v?.vehicles||v?.data)setVehicles(v.vehicles||v.data||[]);setDrivers(d);setLoading(false);};
  useEffect(()=>{load();},[]);
  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🚛" text="Fleet & Trucks" right={<PrimaryBtn text="Refresh" onClick={load} loading={loading} style={{padding:"6px 14px",fontSize:11}}/>}/>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}><KPI label="Vehicles" value={vehicles?fmtNum(vehicles.length):"—"}/><KPI label="Fleet" value={fmtNum(margins.totalTrucks)}/><KPI label="Rev/Truck" value={margins.revenuePerTruck>0?fmt(margins.revenuePerTruck):"—"} subColor={T.green}/><KPI label="Cost/Truck" value={margins.costPerTruck>0?fmt(margins.costPerTruck):"—"} subColor={T.red}/><KPI label="Drivers" value={fmtNum(margins.totalDrivers)}/></div>
      {vehicles&&vehicles.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Motive Vehicles ({vehicles.length})</div><div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Unit #","Make/Model","Year","Type","Driver"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{vehicles.map((v,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{v.number||v.id||"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{[v.make,v.model].filter(Boolean).join(" ")||"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{v.year||"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={v.type||"box"}/></td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{v.current_driver?.first_name?`${v.current_driver.first_name} ${v.current_driver.last_name||""}`:"—"}</td></tr>)}</tbody></table></div></div>}
      {drivers.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Driver Roster</div><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["Name","Type","Pay","Status"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{drivers.map(d=><tr key={d.id}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.name}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={d.type||"W2"}/></td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.pay_rate?fmt(d.pay_rate)+"/wk":"—"}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={d.active!==false?"Active":"Inactive"} color={d.active!==false?T.greenText:T.textDim} bg={d.active!==false?T.greenBg:T.borderLight}/></td></tr>)}</tbody></table></div>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// QUICKBOOKS TAB
// ═══════════════════════════════════════════════════════════════
function QBTab({connected}){
  const [action,setAction]=useState("pnl");const [data,setData]=useState(null);const [loading,setLoading]=useState(false);
  const yr=new Date().getFullYear();const [start,setStart]=useState(`${yr}-01-01`);const [end,setEnd]=useState(new Date().toISOString().slice(0,10));
  const run=async()=>{setLoading(true);setData(await fetchQBO(action,start,end));setLoading(false);};
  if(!connected)return(<div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}}><SectionTitle icon="💰" text="QuickBooks"/><div style={{...CS,borderColor:T.yellow,background:T.yellowBg,textAlign:"center",padding:30}}><div style={{fontSize:28,marginBottom:8}}>🔗</div><div style={{fontSize:14,fontWeight:700,color:T.yellowText}}>Not Connected</div><div style={{fontSize:12,color:T.textMuted,marginBottom:16}}>Note: Production keys from Intuit required for live data</div><a href="/.netlify/functions/marginiq-qbo-auth" style={{display:"inline-block",padding:"10px 24px",borderRadius:10,background:T.brand,color:"#fff",fontSize:13,fontWeight:700,textDecoration:"none"}}>Connect →</a></div></div>);
  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="💰" text="QuickBooks" right={<Badge text="Connected" color={T.greenText} bg={T.greenBg}/>}/>
      <div style={CS}><div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:10}}>{[["pnl","P&L"],["dashboard","Dashboard"],["invoices","Invoices"],["bills","Bills"],["expenses","Expenses"],["customers","Customers"],["vendors","Vendors"]].map(([id,l])=><TabBtn key={id} active={action===id} label={l} onClick={()=>setAction(id)}/>)}</div><div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}><input type="date" value={start} onChange={e=>setStart(e.target.value)} style={{...IS,width:145,fontSize:12}}/><span style={{color:T.textDim}}>→</span><input type="date" value={end} onChange={e=>setEnd(e.target.value)} style={{...IS,width:145,fontSize:12}}/><PrimaryBtn text="Pull" onClick={run} loading={loading}/></div></div>
      {data&&<div style={CS}><pre style={{background:T.bgSurface,padding:14,borderRadius:8,overflow:"auto",maxHeight:500,fontSize:11,whiteSpace:"pre-wrap",wordBreak:"break-word"}}>{JSON.stringify(data,null,2)}</pre></div>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// COST STRUCTURE
// ═══════════════════════════════════════════════════════════════
function CostsTab({costs,onSave,margins,qbFinancials}){
  const [c,setC]=useState({...DEFAULT_COSTS,...costs});const [saving,setSaving]=useState(false);const [saved,setSaved]=useState(false);
  const upd=(k,v)=>setC(p=>({...p,[k]:v}));const save=async()=>{setSaving(true);await FS.saveCosts(c);onSave(c);setSaving(false);setSaved(true);setTimeout(()=>setSaved(false),2000);};
  const F=({label,field,prefix,suffix,step})=>(<div style={{marginBottom:8}}><label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:3}}>{label}</label><div style={{display:"flex",alignItems:"center",gap:4}}>{prefix&&<span style={{fontSize:12,color:T.textDim}}>{prefix}</span>}<input type="number" value={c[field]||""} step={step||"any"} onChange={e=>upd(field,parseFloat(e.target.value)||0)} style={{...IS}}/>{suffix&&<span style={{fontSize:12,color:T.textDim,whiteSpace:"nowrap"}}>{suffix}</span>}</div></div>);

  // Auto-populate from QB categories
  const autoFillFromQB=()=>{
    if(!qbFinancials||!qbFinancials.categoryTotals) {alert("No QB data loaded. Upload General Ledger in QB Import tab first.");return;}
    const ann=qbFinancials.daysInRange>0?365/qbFinancials.daysInRange:1;
    const cat=qbFinancials.categoryTotals;
    const get=(name)=>Math.round((cat[name]||0)*ann);
    // Annual totals derived from GL categories
    const annualWarehouse=get("Rent")+get("Warehouse")+get("Utilities");
    const annualForklifts=get("Warehouse"); // forklift leases are in warehouse bucket
    const annualInsurance=get("Insurance");
    // Update cost fields based on QB data
    const newCosts={...c,
      warehouse:annualWarehouse,
      forklifts:0, // already in warehouse bucket from GL
      // Don't overwrite headcounts/rates (those are manual judgment), but show totals in UI
    };
    setC(newCosts);
    alert(`QB auto-fill:\n• Warehouse + Utilities + Rent: ${fmtK(annualWarehouse)}/yr\n• Insurance: ${fmtK(annualInsurance)}/yr\n• Salaries from GL: ${fmtK(get("Salaries & Wages")+get("Subcontractors")+get("Temp Labor")+get("Officer Salaries"))}/yr\n\nReview fields and click Save to persist.`);
  };

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="⚙️" text="Cost Structure" right={<div style={{display:"flex",gap:8,alignItems:"center"}}>{saved&&<Badge text="✓ Saved" color={T.greenText} bg={T.greenBg}/>}<PrimaryBtn text="Save" onClick={save} loading={saving} style={{padding:"8px 16px",fontSize:12}}/></div>}/>

      {qbFinancials&&qbFinancials.totalExpenses>0&&<div style={{...CS,background:T.brandPale,borderLeft:`4px solid ${T.brand}`,marginBottom:16}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:8}}>
          <div>
            <div style={{fontSize:13,fontWeight:700,color:T.brand}}>📁 QuickBooks Data Available</div>
            <div style={{fontSize:11,color:T.textMuted,marginTop:2}}>Annual expenses from QB: <strong style={{color:T.red}}>{fmtK(qbFinancials.annualExpenses)}</strong> • Annual revenue: <strong style={{color:T.green}}>{fmtK(qbFinancials.annualRevenue)}</strong></div>
          </div>
          <button onClick={autoFillFromQB} style={{padding:"8px 16px",fontSize:12,fontWeight:700,borderRadius:8,border:"none",background:T.brand,color:"#fff",cursor:"pointer",fontFamily:"inherit"}}>Auto-fill from QB →</button>
        </div>
      </div>}

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>🏭 Facility</div><F label="Warehouse (annual)" field="warehouse" prefix="$"/><F label="Forklifts (annual)" field="forklifts" prefix="$"/></div>
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>👥 Headcounts</div><F label="Box Truck Drivers" field="count_box_drivers"/><F label="Tractor Drivers" field="count_tractor_drivers"/><F label="Dispatchers" field="count_dispatchers"/><F label="Admin" field="count_admin"/><F label="Mechanics" field="count_mechanics"/><F label="Forklift Ops" field="count_forklift_ops"/></div>
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>💵 Rates</div><F label="Box Driver" field="rate_box_driver" prefix="$" suffix="/hr" step="0.50"/><F label="Tractor Driver" field="rate_tractor_driver" prefix="$" suffix="/hr" step="0.50"/><F label="Dispatcher" field="rate_dispatcher" prefix="$" suffix="/hr"/><F label="Admin" field="rate_admin" prefix="$" suffix="/hr"/><F label="Mechanic" field="rate_mechanic" prefix="$" suffix="/hr"/></div>
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>🚛 Fleet</div><F label="Box Trucks" field="truck_count_box"/><F label="Tractors" field="truck_count_tractor"/><F label="Insurance $/truck/mo" field="truck_insurance_monthly" prefix="$"/><F label="Box MPG" field="mpg_box" suffix="MPG"/><F label="Tractor MPG" field="mpg_tractor" suffix="MPG"/><F label="Fuel $/gal" field="fuel_price" prefix="$" step="0.01"/></div>
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>📅 Ops</div><F label="Work Days/Year" field="working_days_year"/><F label="Avg Hrs/Shift" field="avg_hours_per_shift"/><F label="Contractor %" field="contractor_pct" step="0.01"/></div>
        {margins.totalAnnualCost>0&&<div style={{...CS,background:T.brandPale,borderColor:T.brand}}><div style={{fontSize:13,fontWeight:700,marginBottom:10,color:T.brand}}>📊 Totals (Manual)</div><DataRow label="Annual Labor" value={fmtK(margins.totalAnnualLabor)} bold/><DataRow label="Annual Fixed" value={fmtK(margins.totalAnnualFixed)}/><DataRow label="Annual Fuel (est)" value={fmtK(margins.annualFuelEst)}/><div style={{height:8}}/><DataRow label="TOTAL ANNUAL" value={fmtK(margins.totalAnnualCost+margins.annualFuelEst)} valueColor={T.red} bold/><DataRow label="Daily" value={fmt(margins.dailyCost+margins.dailyFuelEst)} valueColor={T.red} bold/><DataRow label="Monthly" value={fmtK(margins.monthlyCost+margins.annualFuelEst/12)} valueColor={T.red}/></div>}
        {qbFinancials&&qbFinancials.categoryTotals&&<div style={{...CS,background:"#f0f9ff",borderColor:T.blue}}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:10,color:T.blue}}>📁 Actual from QB (Annualized)</div>
          {(()=>{const ann=qbFinancials.daysInRange>0?365/qbFinancials.daysInRange:1;const cat=qbFinancials.categoryTotals;const get=(n)=>Math.round((cat[n]||0)*ann);return<>
            <DataRow label="Salaries & Wages" value={fmtK(get("Salaries & Wages"))}/>
            <DataRow label="Subcontractors" value={fmtK(get("Subcontractors"))}/>
            <DataRow label="Temp Labor" value={fmtK(get("Temp Labor"))}/>
            <DataRow label="Officer Salaries" value={fmtK(get("Officer Salaries"))}/>
            <DataRow label="Payroll Taxes" value={fmtK(get("Payroll Taxes"))}/>
            <div style={{height:4}}/>
            <DataRow label="Fuel" value={fmtK(get("Fuel"))}/>
            <DataRow label="Truck Maintenance" value={fmtK(get("Truck Maintenance"))}/>
            <DataRow label="Truck Leases" value={fmtK(get("Truck Leases"))}/>
            <DataRow label="Insurance" value={fmtK(get("Insurance"))}/>
            <DataRow label="Health Insurance" value={fmtK(get("Health Insurance"))}/>
            <DataRow label="Rent" value={fmtK(get("Rent"))}/>
            <DataRow label="Warehouse" value={fmtK(get("Warehouse"))}/>
            <DataRow label="Utilities" value={fmtK(get("Utilities"))}/>
            <div style={{height:8}}/>
            <DataRow label="TOTAL QB ANNUAL" value={fmtK(qbFinancials.annualExpenses)} valueColor={T.red} bold/>
            <DataRow label="Daily (QB)" value={fmt(qbFinancials.annualExpenses/(c.working_days_year||260))} valueColor={T.red} bold/>
          </>;})()}
        </div>}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SETTINGS
// ═══════════════════════════════════════════════════════════════
function SettingsTab({qboConnected,motiveConnected}){
  const [history,setHistory]=useState(null);const [loading,setLoading]=useState(false);
  const load=async()=>{if(!hasFirebase)return;setLoading(true);const cols=[{n:"uline_audits",l:"Uline",i:"📦"},{n:"payroll_runs",l:"Payroll",i:"💵"},{n:"drivers",l:"Drivers",i:"👤"},{n:"marginiq_config",l:"Config",i:"⚙️"},{n:"costs",l:"Costs",i:"📉"}];const r=[];for(const c of cols){try{const s=await window.db.collection(c.n).get();r.push({...c,count:s.size,docs:s.docs.slice(0,15).map(d=>({id:d.id,...d.data()}))});}catch(e){r.push({...c,count:0,docs:[]});}}setHistory(r);setLoading(false);};
  useEffect(()=>{load();},[]);
  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🔧" text="Settings"/>
      <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Connections</div>{[{n:"QuickBooks",on:qboConnected,s:qboConnected?"Connected (Dev—need Production for live)":"Not connected",a:!qboConnected?"/.netlify/functions/marginiq-qbo-auth":null},{n:"NuVizz",on:true,s:"portal.nuvizz.com"},{n:"CyberPay",on:true,s:"Auto Monday"},{n:"Motive",on:motiveConnected,s:motiveConnected?"Connected":"Checking"}].map((s,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:12,padding:"12px 0",borderBottom:i<3?`1px solid ${T.border}`:"none"}}><div style={{width:10,height:10,borderRadius:"50%",background:s.on?T.green:T.textDim}}/><div style={{flex:1}}><div style={{fontSize:13,fontWeight:600}}>{s.n}</div><div style={{fontSize:11,color:s.on?T.green:T.textDim}}>{s.s}</div></div>{s.a&&<a href={s.a} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${T.brand}`,color:T.brand,fontSize:12,fontWeight:600,textDecoration:"none"}}>Connect</a>}</div>)}</div>
      <div style={CS}><div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}><div style={{fontSize:13,fontWeight:700}}>📂 Data History</div><PrimaryBtn text="Refresh" onClick={load} loading={loading} style={{padding:"6px 14px",fontSize:11}}/></div>{history&&history.map((c,i)=><div key={i} style={{marginBottom:10,padding:"10px",borderRadius:8,background:T.bgSurface}}><div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}><span style={{fontSize:13,fontWeight:600}}>{c.i} {c.l}</span><Badge text={`${c.count}`} color={c.count>0?T.greenText:T.textDim} bg={c.count>0?T.greenBg:T.borderLight}/></div>{c.count>0&&<div style={{marginTop:6,maxHeight:100,overflowY:"auto"}}>{c.docs.map((d,j)=><div key={j} style={{fontSize:11,padding:"2px 0",display:"flex",justifyContent:"space-between"}}><span>{d.filename||d.name||d.check_date||d.id}</span><span style={{color:T.textDim}}>{(d.upload_date||d.updated_at||"").slice(0,10)}</span></div>)}</div>}</div>)}</div>
      <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:8}}>System</div>{[["Firebase","davismarginiq"],["Netlify","davis-marginiq.netlify.app"],["Version",APP_VERSION]].map(([l,v])=><DataRow key={l} label={l} value={v}/>)}</div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// AI INSIGHTS (Claude API powered analysis)
// ═══════════════════════════════════════════════════════════════
function AIInsights({margins,ulineWeeks,costs,qbFinancials}){
  const [query,setQuery]=useState("");const [response,setResponse]=useState(null);const [loading,setLoading]=useState(false);const [history,setHistory]=useState([]);
  const [extraData,setExtraData]=useState(null);

  // Load all available data from Firebase on mount
  useEffect(()=>{(async()=>{
    if(!hasFirebase)return;
    const extra={};
    try{const s=await window.db.collection("b600_timeclock").orderBy("date","desc").limit(200).get();extra.b600=s.docs.map(d=>d.data());
      // Summarize B600
      const byDriver={};extra.b600.forEach(r=>{if(!byDriver[r.name])byDriver[r.name]={name:r.name,records:0,totalHours:0};byDriver[r.name].records++;byDriver[r.name].totalHours+=r.hours||0;});
      extra.b600Summary=Object.values(byDriver).sort((a,b)=>b.totalHours-a.totalHours);
    }catch(e){}
    try{const s=await window.db.collection("nuvizz_stops").limit(500).get();extra.nvStops=s.size;
      const byDriver={};s.docs.forEach(d=>{const data=d.data();const drv=data.driverName||data.driver||"?";if(!byDriver[drv])byDriver[drv]={name:drv,stops:0};byDriver[drv].stops++;});
      extra.nvDriverSummary=Object.values(byDriver).sort((a,b)=>b.stops-a.stops);
    }catch(e){}
    try{const s=await window.db.collection("payroll_runs").orderBy("check_date","desc").limit(20).get();extra.payrolls=s.docs.map(d=>d.data());}catch(e){}
    setExtraData(extra);
  })();},[]);

  const buildContext=()=>{
    const lines=[];
    if(margins.totalAnnualCost>0){lines.push(`COST STRUCTURE: Annual cost ${fmtK(margins.totalAnnualCost)}, daily cost ${fmt(margins.dailyCost)}, ${margins.totalDrivers} drivers, ${margins.totalTrucks} trucks`);lines.push(`Labor: Box drivers ${costs.count_box_drivers}x$${costs.rate_box_driver}/hr, Tractor ${costs.count_tractor_drivers}x$${costs.rate_tractor_driver}/hr, Dispatchers ${costs.count_dispatchers}x$${costs.rate_dispatcher}/hr, Admin ${costs.count_admin}x$${costs.rate_admin}/hr, Mechanics ${costs.count_mechanics}x$${costs.rate_mechanic}/hr, Forklift Ops ${costs.count_forklift_ops}x$20/hr`);lines.push(`Fixed: Warehouse $${costs.warehouse}/yr, Insurance $${costs.truck_insurance_monthly}/truck/mo x${margins.totalTrucks} trucks, Forklifts $${costs.forklifts}/yr`);lines.push(`Fleet: ${costs.truck_count_box} box trucks @${costs.mpg_box}MPG, ${costs.truck_count_tractor} tractors @${costs.mpg_tractor}MPG, fuel $${costs.fuel_price}/gal`);lines.push(`Operations: ${costs.working_days_year} work days/yr, ${costs.avg_hours_per_shift}hr shifts, contractor payout ${costs.contractor_pct*100}%`);}
    if(margins.dailyRevenue>0){lines.push(`MARGIN: Daily revenue ${fmt(margins.dailyRevenue)}, daily cost ${fmt(margins.dailyCost)}, daily margin ${fmt(margins.dailyMargin)} (${fmtPct(margins.dailyMarginPct)})`);lines.push(`Per stop: rev ${fmt(margins.revenuePerStop)}, cost ${fmt(margins.costPerStop)}, margin ${fmt(margins.marginPerStop)} (${fmtPct(margins.marginPerStopPct)})`);lines.push(`Per driver/day: rev ${fmt(margins.revenuePerDriver)}, cost ${fmt(margins.costPerDriver)}`);lines.push(`Break-even: ${fmtNum(margins.breakEvenStops)} stops/day. Current: ${fmtNum(margins.dailyStops)} stops/day`);lines.push(`Fuel estimate: ${fmt(margins.dailyFuelEst)}/day, ${fmtK(margins.annualFuelEst)}/yr`);if(margins.otExposure>0)lines.push(`OT EXPOSURE: ${fmtNum(margins.otExposure)} overtime hrs/wk at ${margins.weeklyHours}hr work weeks`);}
    if(ulineWeeks.length>0){
      const totalRev=ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0);const totalStops=ulineWeeks.reduce((s,w)=>s+(w.totalStops||0),0);const totalContr=ulineWeeks.reduce((s,w)=>s+(w.contractorPayout||0),0);
      lines.push(`ULINE DATA: ${ulineWeeks.length} weeks, ${fmtNum(totalStops)} stops, ${fmtK(totalRev)} revenue, avg ${fmt(totalStops>0?totalRev/totalStops:0)}/stop, weekly avg ${fmtK(totalRev/ulineWeeks.length)}`);
      if(totalContr>0) lines.push(`Contractor payouts: ${fmtK(totalContr)} (40% of seal stops), net retained: ${fmtK(totalRev-totalContr)}`);
      const allStops=ulineWeeks.flatMap(w=>w.stops||[]);
      const byCity={};allStops.forEach(s=>{const c=s.city||"?";byCity[c]=(byCity[c]||0)+(s.newCost||s.cost);});
      lines.push(`TOP 10 CITIES BY REVENUE: ${Object.entries(byCity).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([c,r])=>`${c}:${fmt(r)}`).join(", ")}`);
      const byCust={};allStops.forEach(s=>{const c=s.customer||"?";if(!byCust[c])byCust[c]={rev:0,stops:0};byCust[c].rev+=(s.newCost||s.cost);byCust[c].stops++;});
      lines.push(`TOP 10 CUSTOMERS: ${Object.entries(byCust).sort((a,b)=>b[1].rev-a[1].rev).slice(0,10).map(([c,d])=>`${c}:${fmt(d.rev)}(${d.stops}stops)`).join(", ")}`);
    }
    if(extraData){
      if(extraData.b600Summary?.length>0){lines.push(`B600 TIME CLOCK (${extraData.b600?.length||0} records): Top drivers by hours: ${extraData.b600Summary.slice(0,10).map(d=>`${d.name}:${fmtDec(d.totalHours,1)}hrs(${d.records}days)`).join(", ")}`);}
      if(extraData.nvStops>0){lines.push(`NUVIZZ STORED STOPS: ${extraData.nvStops} total. Top drivers: ${(extraData.nvDriverSummary||[]).slice(0,10).map(d=>`${d.name}:${d.stops}`).join(", ")}`);}
      if(extraData.payrolls?.length>0){lines.push(`CYBERPAY PAYROLL: ${extraData.payrolls.length} recent runs. Latest: ${extraData.payrolls[0]?.check_date||"?"} (${extraData.payrolls[0]?.from_date} to ${extraData.payrolls[0]?.to_date})`);}
    }
    // QB Financials — the authoritative financial picture
    if(qbFinancials){
      lines.push(`\n=== QUICKBOOKS FINANCIALS (${qbFinancials.source==="gl"?"from General Ledger":"from P&L"}) ===`);
      lines.push(`Date range: ${qbFinancials.dateStart} to ${qbFinancials.dateEnd} (${qbFinancials.daysInRange} days)`);
      lines.push(`Period revenue: ${fmtK(qbFinancials.totalRevenue)}, expenses: ${fmtK(qbFinancials.totalExpenses)}, net income: ${fmtK(qbFinancials.netIncome)}, margin: ${fmtPct(qbFinancials.marginPct)}`);
      lines.push(`ANNUALIZED: Revenue ${fmtK(qbFinancials.annualRevenue)}/yr, Expenses ${fmtK(qbFinancials.annualExpenses)}/yr, Net ${fmtK(qbFinancials.annualNetIncome)}/yr`);
      if(qbFinancials.totalAssets>0) lines.push(`Balance Sheet: Assets ${fmtK(qbFinancials.totalAssets)}, Liabilities ${fmtK(qbFinancials.totalLiabilities)}, Equity ${fmtK(qbFinancials.totalEquity)}`);
      if(Object.keys(qbFinancials.categoryTotals||{}).length>0){
        const ann=qbFinancials.daysInRange>0?365/qbFinancials.daysInRange:1;
        const cats=Object.entries(qbFinancials.categoryTotals).filter(([,v])=>v>0).sort((a,b)=>b[1]-a[1]).slice(0,15);
        lines.push(`TOP EXPENSE CATEGORIES (ANNUAL): ${cats.map(([n,v])=>`${n}:${fmtK(v*ann)}`).join(", ")}`);
      }
      if(qbFinancials.customerRevenue?.length>0){
        lines.push(`TOP 10 QB CUSTOMERS: ${qbFinancials.customerRevenue.slice(0,10).map(c=>`${c.customer}:${fmtK(c.revenue)}`).join(", ")}`);
      }
      if(qbFinancials.vendorSpend?.length>0){
        lines.push(`TOP 10 QB VENDORS: ${qbFinancials.vendorSpend.slice(0,10).map(v=>`${v.vendor}:${fmtK(v.total)}`).join(", ")}`);
      }
      // Monthly/quarterly trend
      if(qbFinancials.quarterlyPnL?.length>0){
        lines.push(`QUARTERLY P&L (last 8 quarters): ${qbFinancials.quarterlyPnL.slice(-8).map(p=>`${p.period}:Rev${fmtK(p.revenue)}/NI${fmtK(p.netIncome)}/M${fmtPct(p.margin)}`).join(" | ")}`);
      }
    }
    return lines.join("\n");
  };

  const ask=async()=>{
    if(!query.trim())return;setLoading(true);setResponse(null);
    const ctx=buildContext();
    const systemPrompt=`You are a freight logistics and financial analyst for Davis Delivery Service Inc., a family-owned LTL final mile carrier based in Buford, GA near I-85. They have ~50 trucks (box trucks and tractor trailers), ~$12M revenue, primary customer is Uline (~$13M/yr, ~600 stops/day). Analyze the data provided and give specific, actionable insights. Use exact numbers from the data. Be direct and concise. Format with clear sections.

COMPANY DATA:
${ctx||"No data loaded yet - recommend user upload Uline files and enter cost structure."}`;
    try{
      const resp=await fetch("https://api.anthropic.com/v1/messages",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({model:"claude-opus-4-20250514",max_tokens:4000,system:systemPrompt,messages:[...history.flatMap(h=>[{role:"user",content:h.q},{role:"assistant",content:h.a}]),{role:"user",content:query}]})});
      const data=await resp.json();
      const answer=data.content?.map(c=>c.text||"").join("\n")||"No response";
      setResponse(answer);setHistory(prev=>[...prev,{q:query,a:answer}]);setQuery("");
    }catch(e){setResponse("Error: "+e.message);}
    setLoading(false);
  };

  const suggestions=["What are my biggest cost reduction opportunities?","Which cities generate the most revenue per stop?","Am I staffed correctly for my volume?","What should I charge per stop to hit 30% margin?","Analyze my overtime exposure and recommend solutions","Compare my cost structure to industry benchmarks","What's my break-even analysis look like?","Where am I losing money?"];

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🤖" text="AI Analysis" right={<Badge text="Powered by Claude Opus" color={T.blueText} bg={T.blueBg}/>}/>
      {/* Quick suggestions */}
      <div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:12}}>
        {suggestions.map((s,i)=><button key={i} onClick={()=>{setQuery(s);}} style={{padding:"6px 12px",borderRadius:20,border:`1px solid ${T.border}`,background:T.bgSurface,color:T.textMuted,fontSize:11,cursor:"pointer",transition:"all 0.2s"}}>{s}</button>)}
      </div>
      {/* Input */}
      <div style={CS}>
        <div style={{display:"flex",gap:8}}>
          <input value={query} onChange={e=>setQuery(e.target.value)} onKeyDown={e=>{if(e.key==="Enter"&&!e.shiftKey)ask();}} placeholder="Ask about your costs, margins, routes, drivers..." style={{...IS,flex:1,fontSize:14,padding:"12px 16px"}}/>
          <PrimaryBtn text={loading?"Analyzing...":"Ask"} onClick={ask} loading={loading} style={{padding:"12px 24px"}}/>
        </div>
      </div>
      {/* Response */}
      {response&&<div style={{...CS,borderLeft:`4px solid ${T.brand}`}}>
        <div style={{fontSize:12,fontWeight:700,color:T.brand,marginBottom:8}}>🤖 AI Analysis</div>
        <div style={{fontSize:13,lineHeight:1.7,whiteSpace:"pre-wrap",color:T.text}}>{response}</div>
      </div>}
      {/* History */}
      {history.length>1&&<div style={CS}>
        <div style={{fontSize:12,fontWeight:700,marginBottom:8}}>Previous Questions</div>
        {history.slice(0,-1).reverse().map((h,i)=>(
          <div key={i} style={{marginBottom:12,padding:"10px",borderRadius:8,background:T.bgSurface}}>
            <div style={{fontSize:12,fontWeight:600,color:T.brand,marginBottom:4}}>Q: {h.q}</div>
            <div style={{fontSize:11,color:T.textMuted,maxHeight:80,overflow:"hidden"}}>{h.a.substring(0,200)}...</div>
          </div>
        ))}
      </div>}
      {!response&&history.length===0&&<div style={{...CS,textAlign:"center",padding:30,color:T.textMuted}}><div style={{fontSize:32,marginBottom:8}}>🤖</div><div style={{fontSize:13,fontWeight:600,color:T.text}}>Ask anything about your operation</div><div style={{fontSize:12,marginTop:4}}>AI has access to your cost structure, Uline data, and margin calculations. The more data you load, the better the analysis.</div></div>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// DATA IMPORT HUB (B600, Uline dedup, NuVizz bulk)
// ═══════════════════════════════════════════════════════════════
function DataImport({ulineWeeks,onUlineUpload}){
  const [view,setView]=useState("overview");
  const [importing,setImporting]=useState(false);
  const [importResult,setImportResult]=useState(null);
  const [b600Data,setB600Data]=useState([]);
  const [nvFrom,setNvFrom]=useState(()=>new Date().toISOString().slice(0,10));
  const [nvTo,setNvTo]=useState(()=>new Date().toISOString().slice(0,10));
  const b600Ref=useRef(null);const ulineRef=useRef(null);

  // ── B600 Time Clock CSV Parser ──
  const parseB600=async(file)=>{
    return new Promise((resolve,reject)=>{
      const reader=new FileReader();
      reader.onload=e=>{
        try{
          const text=e.target.result;
          const lines=text.split("\n").map(l=>l.trim()).filter(l=>l);
          if(lines.length<2) return reject(new Error("Empty CSV"));
          // Parse header
          const header=lines[0].toLowerCase().split(",").map(h=>h.trim().replace(/"/g,""));
          const nameIdx=header.findIndex(h=>h.match(/employee|name|driver|person/));
          const dateIdx=header.findIndex(h=>h.match(/^date$/));
          const inIdx=header.findIndex(h=>h.match(/in|clock.?in|start|punch.?in/));
          const outIdx=header.findIndex(h=>h.match(/out|clock.?out|end|punch.?out/));
          const hoursIdx=header.findIndex(h=>h.match(/hours|total|duration/));
          const records=[];
          for(let i=1;i<lines.length;i++){
            const cols=lines[i].split(",").map(c=>c.trim().replace(/"/g,""));
            if(cols.length<3) continue;
            const name=nameIdx>=0?cols[nameIdx]:(cols[0]||"");
            const date=dateIdx>=0?cols[dateIdx]:(cols[1]||"");
            const clockIn=inIdx>=0?cols[inIdx]:(cols[2]||"");
            const clockOut=outIdx>=0?cols[outIdx]:(cols[3]||"");
            const hours=hoursIdx>=0?parseFloat(cols[hoursIdx])||0:0;
            if(!name||!date) continue;
            records.push({name,date,clockIn,clockOut,hours,raw:lines[i]});
          }
          resolve(records);
        }catch(e){reject(e);}
      };
      reader.onerror=reject;
      reader.readAsText(file);
    });
  };

  const handleB600Upload=async(e)=>{
    const file=e.target.files?.[0];if(!file)return;
    setImporting(true);setImportResult(null);
    try{
      const records=await parseB600(file);
      // Dedup: use date_name as document ID
      let saved=0,skipped=0;
      if(hasFirebase){
        for(const r of records){
          const docId=`${r.date}_${r.name}`.replace(/[\/\s]/g,"_").substring(0,200);
          try{
            const existing=await window.db.collection("b600_timeclock").doc(docId).get();
            if(existing.exists){skipped++;continue;}
            await window.db.collection("b600_timeclock").doc(docId).set({...r,imported_at:new Date().toISOString(),source:file.name});
            saved++;
          }catch(err){console.warn("B600 save error:",err);}
        }
      }
      setB600Data(records);
      setImportResult({type:"b600",total:records.length,saved,skipped,filename:file.name});
    }catch(err){setImportResult({type:"error",message:err.message});}
    setImporting(false);
    if(b600Ref.current)b600Ref.current.value="";
  };

  // ── Uline with dedup ──
  const handleUlineUpload=async(e)=>{
    const file=e.target.files?.[0];if(!file)return;
    setImporting(true);setImportResult(null);
    try{
      const parsed=await parseUlineXlsx(file);
      // ── PRO-based dedup: check incoming PROs against all previously loaded weeks ──
      const existingPros=new Set();
      ulineWeeks.forEach(w=>(w.stops||[]).forEach(s=>{if(s.pro)existingPros.add(s.pro);}));
      const incomingPros=parsed.stops.filter(s=>s.pro).map(s=>s.pro);
      const dupPros=incomingPros.filter(p=>existingPros.has(p));
      const newStops=parsed.stops.filter(s=>!s.pro||!existingPros.has(s.pro));

      if(dupPros.length>0&&newStops.length===0){
        // ALL stops are duplicates — skip entirely
        setImportResult({type:"uline_dup",filename:file.name,totalStops:parsed.totalStops,dupCount:dupPros.length,samplePros:dupPros.slice(0,5).join(", ")});
      }else if(dupPros.length>0&&newStops.length>0){
        // Partial overlap — save only new stops
        const newRevenue=newStops.reduce((s,r)=>s+(r.newCost||r.cost),0);
        const newWeight=newStops.reduce((s,r)=>s+r.weight,0);
        const contractorStops=newStops.filter(s=>s.sealNbr);
        const contractorPayout=contractorStops.reduce((s,r)=>s+(r.newCost||r.cost),0)*0.40;
        const weekId=`${file.name}_${Date.now()}`.replace(/[\/\s\.]/g,"_").substring(0,200);
        const weekData={stops:newStops,totalRevenue:newRevenue,totalWeight:newWeight,totalStops:newStops.length,avgRevenuePerStop:newStops.length>0?newRevenue/newStops.length:0,avgWeightPerStop:newStops.length>0?newWeight/newStops.length:0,contractorStops:contractorStops.length,contractorPayout,filename:file.name,upload_date:new Date().toISOString(),week_id:weekId};
        await FS.saveUlineWeek(weekId,weekData);
        onUlineUpload(weekData);
        setImportResult({type:"uline_partial",filename:file.name,newCount:newStops.length,dupCount:dupPros.length,revenue:newRevenue,samplePros:dupPros.slice(0,5).join(", ")});
      }else{
        // No duplicates — save everything
        const weekId=`${file.name}_${Date.now()}`.replace(/[\/\s\.]/g,"_").substring(0,200);
        const weekData={...parsed,filename:file.name,upload_date:new Date().toISOString(),week_id:weekId};
        await FS.saveUlineWeek(weekId,weekData);
        onUlineUpload(weekData);
        setImportResult({type:"uline",filename:file.name,totalStops:parsed.totalStops,totalRevenue:parsed.totalRevenue});
      }
    }catch(err){setImportResult({type:"error",message:err.message});}
    setImporting(false);
    if(ulineRef.current)ulineRef.current.value="";
  };

  // ── NuVizz Bulk Import ──
  const handleNuVizzBulk=async()=>{
    setImporting(true);setImportResult(null);
    try{
      const j=await fetchNuVizzStops(nvFrom,nvTo);
      if(!j) throw new Error("No response from NuVizz API");
      const stops=Array.isArray(j)?j:(j.stopList||j.stops||j.data||[]);
      if(stops.length===0) throw new Error("No stops returned for date range");
      // Save to Firebase with dedup by stopNbr
      let saved=0,skipped=0;
      if(hasFirebase){
        for(const s of stops){
          const stopNbr=s.stopNbr||s.id||`${Date.now()}_${Math.random()}`;
          const docId=String(stopNbr).replace(/[\/\s]/g,"_").substring(0,200);
          try{
            const existing=await window.db.collection("nuvizz_stops").doc(docId).get();
            if(existing.exists){skipped++;continue;}
            await window.db.collection("nuvizz_stops").doc(docId).set({...s,imported_at:new Date().toISOString(),import_range:`${nvFrom}_${nvTo}`});
            saved++;
          }catch(err){console.warn("NuVizz save:",err);}
        }
      }
      setImportResult({type:"nuvizz",total:stops.length,saved,skipped,from:nvFrom,to:nvTo});
    }catch(err){setImportResult({type:"error",message:err.message});}
    setImporting(false);
  };

  // ── B600 History from Firebase ──
  const [b600History,setB600History]=useState([]);
  const loadB600History=async()=>{
    if(!hasFirebase)return;
    try{const s=await window.db.collection("b600_timeclock").orderBy("date","desc").limit(100).get();setB600History(s.docs.map(d=>({id:d.id,...d.data()})));}catch(e){}
  };
  useEffect(()=>{loadB600History();},[]);

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📥" text="Data Import" right={<Badge text="Dedup enabled" color={T.greenText} bg={T.greenBg}/>}/>

      {/* Import result banner */}
      {importResult&&(
        <div style={{...CS,borderLeft:`4px solid ${importResult.type==="error"?T.red:importResult.type==="uline_dup"?T.yellow:importResult.type==="uline_partial"?T.blue:T.green}`,marginBottom:16}}>
          {importResult.type==="error"&&<div style={{color:T.redText,fontSize:13}}>❌ Error: {importResult.message}</div>}
          {importResult.type==="uline_dup"&&<div style={{color:T.yellowText,fontSize:13}}>⚠️ All {fmtNum(importResult.totalStops)} stops already imported — {importResult.dupCount} duplicate PROs found (e.g. {importResult.samplePros})</div>}
          {importResult.type==="uline_partial"&&<div style={{color:T.blueText,fontSize:13}}>ℹ️ Imported {fmtNum(importResult.newCount)} new stops from <strong>{importResult.filename}</strong> ({fmt(importResult.revenue)} revenue). Skipped {importResult.dupCount} duplicate PROs (e.g. {importResult.samplePros})</div>}
          {importResult.type==="uline"&&<div style={{color:T.greenText,fontSize:13}}>✅ Imported <strong>{importResult.filename}</strong>: {fmtNum(importResult.totalStops)} stops, {fmt(importResult.totalRevenue)} revenue</div>}
          {importResult.type==="b600"&&<div style={{color:T.greenText,fontSize:13}}>✅ B600: {importResult.saved} records saved, {importResult.skipped} duplicates skipped (from {importResult.filename})</div>}
          {importResult.type==="nuvizz"&&<div style={{color:T.greenText,fontSize:13}}>✅ NuVizz: {importResult.saved} stops saved, {importResult.skipped} duplicates skipped ({importResult.from} → {importResult.to})</div>}
        </div>
      )}

      <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
        {[["overview","📊 Overview"],["uline","📦 Uline XLSX"],["b600","⏰ B600 Clock"],["nuvizz","🚚 NuVizz Bulk"],["history","📂 Import History"]].map(([id,l])=><TabBtn key={id} active={view===id} label={l} onClick={()=>setView(id)}/>)}
      </div>

      {view==="overview"&&(
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
          <div style={{...CS,cursor:"pointer",border:`2px dashed ${T.border}`}} onClick={()=>setView("uline")}>
            <div style={{textAlign:"center",padding:"20px 0"}}><div style={{fontSize:36,marginBottom:8}}>📦</div><div style={{fontSize:14,fontWeight:700}}>Uline Weekly Audit</div><div style={{fontSize:12,color:T.textMuted,marginTop:4}}>Upload .xlsx files from Uline billing emails</div><div style={{fontSize:11,color:T.green,marginTop:8}}>{ulineWeeks.length} weeks loaded</div></div>
          </div>
          <div style={{...CS,cursor:"pointer",border:`2px dashed ${T.border}`}} onClick={()=>setView("b600")}>
            <div style={{textAlign:"center",padding:"20px 0"}}><div style={{fontSize:36,marginBottom:8}}>⏰</div><div style={{fontSize:14,fontWeight:700}}>B600 Time Clock</div><div style={{fontSize:12,color:T.textMuted,marginTop:4}}>Upload CSV exports from TotalPass B600</div><div style={{fontSize:11,color:T.green,marginTop:8}}>{b600History.length} records stored</div></div>
          </div>
          <div style={{...CS,cursor:"pointer",border:`2px dashed ${T.border}`}} onClick={()=>setView("nuvizz")}>
            <div style={{textAlign:"center",padding:"20px 0"}}><div style={{fontSize:36,marginBottom:8}}>🚚</div><div style={{fontSize:14,fontWeight:700}}>NuVizz Delivery Data</div><div style={{fontSize:12,color:T.textMuted,marginTop:4}}>Pull stops by date range and save to Firebase</div><div style={{fontSize:11,color:T.textMuted,marginTop:8}}>Pulls from portal.nuvizz.com API</div></div>
          </div>
        </div>
      )}

      {view==="uline"&&(
        <div style={CS}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>📦 Upload Uline Weekly Audit XLSX</div>
          <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Upload the weekly audit xlsx from Uline billing emails. Duplicate files are automatically detected and skipped.</div>
          <input ref={ulineRef} type="file" accept=".xlsx,.xls" onChange={handleUlineUpload} style={{display:"none"}}/>
          <PrimaryBtn text={importing?"Importing...":"Choose Uline XLSX File"} onClick={()=>ulineRef.current?.click()} loading={importing}/>
          {ulineWeeks.length>0&&<div style={{marginTop:16}}><div style={{fontSize:12,fontWeight:700,marginBottom:8}}>Previously Loaded ({ulineWeeks.length} weeks)</div>{ulineWeeks.map((w,i)=><div key={i} style={{display:"flex",justifyContent:"space-between",padding:"6px 0",borderBottom:`1px solid ${T.borderLight}`,fontSize:12}}><span style={{fontWeight:500}}>{w.filename||w.id}</span><span style={{color:T.green,fontWeight:600}}>{fmt(w.totalRevenue)} ({fmtNum(w.totalStops)} stops)</span></div>)}</div>}
        </div>
      )}

      {view==="b600"&&(
        <div style={CS}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>⏰ Import B600 Time Clock Data</div>
          <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Export CSV from the TotalPass B600 time clock (b600.atlantafreightquotes.com). Expected columns: Employee/Name, Date, Clock In, Clock Out, Hours. Duplicates (same date + employee) are automatically skipped.</div>
          <input ref={b600Ref} type="file" accept=".csv,.txt" onChange={handleB600Upload} style={{display:"none"}}/>
          <PrimaryBtn text={importing?"Importing...":"Choose B600 CSV File"} onClick={()=>b600Ref.current?.click()} loading={importing}/>
          {b600Data.length>0&&<div style={{marginTop:16}}><div style={{fontSize:12,fontWeight:700,marginBottom:8}}>Just Imported ({b600Data.length} records)</div><div style={{maxHeight:300,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}><thead><tr>{["Name","Date","In","Out","Hours"].map(h=><th key={h} style={{textAlign:"left",padding:"6px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead><tbody>{b600Data.slice(0,50).map((r,i)=><tr key={i}><td style={{padding:"6px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{r.name}</td><td style={{padding:"6px",borderBottom:`1px solid ${T.borderLight}`}}>{r.date}</td><td style={{padding:"6px",borderBottom:`1px solid ${T.borderLight}`}}>{r.clockIn}</td><td style={{padding:"6px",borderBottom:`1px solid ${T.borderLight}`}}>{r.clockOut}</td><td style={{padding:"6px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{r.hours||"—"}</td></tr>)}</tbody></table></div></div>}
          {b600History.length>0&&b600Data.length===0&&<div style={{marginTop:16}}><div style={{fontSize:12,fontWeight:700,marginBottom:8}}>Stored Records ({b600History.length})</div><div style={{maxHeight:200,overflowY:"auto"}}>{b600History.slice(0,30).map((r,i)=><div key={i} style={{display:"flex",justifyContent:"space-between",padding:"4px 0",borderBottom:`1px solid ${T.borderLight}`,fontSize:11}}><span>{r.name}</span><span>{r.date}</span><span>{r.clockIn}→{r.clockOut}</span><span style={{fontWeight:600}}>{r.hours||"—"}h</span></div>)}</div></div>}
        </div>
      )}

      {view==="nuvizz"&&(
        <div style={CS}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>🚚 Pull NuVizz Delivery Data</div>
          <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Pull all stops for a date range from the NuVizz API and save to Firebase. Each stop is stored by its stop number — duplicates are automatically skipped.</div>
          <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
            <input type="date" value={nvFrom} onChange={e=>setNvFrom(e.target.value)} style={{...IS,width:150,fontSize:12}}/>
            <span style={{color:T.textDim}}>→</span>
            <input type="date" value={nvTo} onChange={e=>setNvTo(e.target.value)} style={{...IS,width:150,fontSize:12}}/>
            <PrimaryBtn text={importing?"Pulling...":"Pull & Save Stops"} onClick={handleNuVizzBulk} loading={importing}/>
          </div>
        </div>
      )}

      {view==="history"&&(
        <FullImportHistory ulineWeeks={ulineWeeks} onUlineDelete={(id)=>{setUlineWeeks(prev=>prev.filter(w=>(w.week_id||w.id)!==id));}} b600History={b600History} onB600Delete={(id)=>{setB600History(prev=>prev.filter(r=>r.id!==id));}}/>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// FULL IMPORT HISTORY (cross-source management)
// ═══════════════════════════════════════════════════════════════
function FullImportHistory({ulineWeeks,onUlineDelete,b600History,onB600Delete}){
  const [source,setSource]=useState("all");
  const [nvStops,setNvStops]=useState([]);
  const [qboImports,setQboImports]=useState([]);
  const [payrollRuns,setPayrollRuns]=useState([]);
  const [loading,setLoading]=useState(false);

  const loadAll=async()=>{
    if(!hasFirebase) return;
    setLoading(true);
    try{
      const [nv,qbo,pay]=await Promise.all([
        window.db.collection("nuvizz_stops").limit(500).get().catch(()=>({docs:[]})),
        window.db.collection("qbo_imports").orderBy("imported_at","desc").limit(100).get().catch(()=>({docs:[]})),
        window.db.collection("payroll_runs").orderBy("check_date","desc").limit(50).get().catch(()=>({docs:[]})),
      ]);
      setNvStops(nv.docs.map(d=>({id:d.id,...d.data()})));
      setQboImports(qbo.docs.map(d=>({id:d.id,...d.data()})));
      setPayrollRuns(pay.docs.map(d=>({id:d.id,...d.data()})));
    }catch(e){}
    setLoading(false);
  };
  useEffect(()=>{loadAll();},[]);

  const deleteDoc=async(collection,id,label,onDelete)=>{
    if(!confirm(`Delete "${label}"? This permanently removes it from MarginIQ. Cannot be undone.`)) return;
    try{
      await window.db.collection(collection).doc(id).delete();
      if(onDelete) onDelete(id);
      loadAll();
    }catch(e){alert("Delete failed: "+e.message);}
  };

  const deleteAll=async(collection,label,count)=>{
    if(!confirm(`Delete ALL ${count} ${label} records? This permanently wipes this entire data source from MarginIQ. Cannot be undone.`)) return;
    if(!confirm(`Are you absolutely sure? This will delete every ${label} import. Type cancel to stop, OK to proceed.`)) return;
    try{
      const snap=await window.db.collection(collection).get();
      const batch=window.db.batch();
      snap.docs.forEach(d=>batch.delete(d.ref));
      await batch.commit();
      loadAll();
      alert(`Deleted ${snap.size} records from ${label}`);
    }catch(e){alert("Bulk delete failed: "+e.message);}
  };

  // Build unified records list
  const allRecords=[
    ...ulineWeeks.map(w=>({id:w.week_id||w.id,type:"uline",label:"Uline",icon:"📦",filename:w.filename,date:w.upload_date,detail:`${fmtNum(w.totalStops||0)} stops, ${fmt(w.totalRevenue||0)}`,color:T.orange,collection:"uline_audits",onDelete:onUlineDelete})),
    ...qboImports.map(q=>({id:q.id,type:"qb",label:`QB ${q.type==="gl"?"GL":q.type==="pnl"?"P&L":q.type==="bs"?"Bal Sheet":q.type==="customers"?"Customers":q.type==="payroll"?"Payroll":q.type}`,icon:"📁",filename:q.filename,date:q.imported_at,detail:q.txnCount?`${fmtNum(q.txnCount)} txns`:q.totalRevenue?`${fmtK(q.totalRevenue)} revenue`:q.rowCount?`${q.rowCount} rows`:"",color:T.blue,collection:"qbo_imports"})),
    ...b600History.map(r=>({id:r.id,type:"b600",label:"B600",icon:"⏰",filename:`${r.name} — ${r.date}`,date:r.imported_at||r.date,detail:`${r.clockIn}→${r.clockOut} (${r.hours||"?"}hrs)`,color:T.purple,collection:"b600_timeclock",onDelete:onB600Delete})),
    ...nvStops.map(s=>({id:s.id,type:"nuvizz",label:"NuVizz",icon:"🚚",filename:`Stop ${s.stopNbr||s.id}`,date:s.imported_at,detail:`${s.driverName||""} ${s.custName||s.customerName||""}`.trim(),color:T.green,collection:"nuvizz_stops"})),
    ...payrollRuns.map(p=>({id:p.id,type:"payroll",label:"CyberPay",icon:"💵",filename:`Run ${p.run_id||p.id}`,date:p.check_date||p.scraped_at,detail:`${p.from_date}→${p.to_date}`,color:T.accent,collection:"payroll_runs"})),
  ].filter(r=>source==="all"||r.type===source);
  allRecords.sort((a,b)=>(b.date||"").localeCompare(a.date||""));

  const counts={
    uline:ulineWeeks.length,
    qb:qboImports.length,
    b600:b600History.length,
    nuvizz:nvStops.length,
    payroll:payrollRuns.length,
  };
  const total=counts.uline+counts.qb+counts.b600+counts.nuvizz+counts.payroll;

  return(
    <div>
      <div style={CS}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12,flexWrap:"wrap",gap:8}}>
          <div style={{fontSize:13,fontWeight:700}}>📂 All Imported Data ({total} records)</div>
          <PrimaryBtn text={loading?"Loading...":"Refresh"} onClick={loadAll} loading={loading} style={{padding:"6px 14px",fontSize:11}}/>
        </div>
        <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
          <TabBtn active={source==="all"} label={`All (${total})`} onClick={()=>setSource("all")}/>
          <TabBtn active={source==="uline"} label={`📦 Uline (${counts.uline})`} onClick={()=>setSource("uline")}/>
          <TabBtn active={source==="qb"} label={`📁 QB (${counts.qb})`} onClick={()=>setSource("qb")}/>
          <TabBtn active={source==="b600"} label={`⏰ B600 (${counts.b600})`} onClick={()=>setSource("b600")}/>
          <TabBtn active={source==="nuvizz"} label={`🚚 NuVizz (${counts.nuvizz})`} onClick={()=>setSource("nuvizz")}/>
          <TabBtn active={source==="payroll"} label={`💵 Payroll (${counts.payroll})`} onClick={()=>setSource("payroll")}/>
        </div>
        {source!=="all"&&allRecords.length>0&&(
          <div style={{display:"flex",justifyContent:"flex-end",marginBottom:10}}>
            <button onClick={()=>{
              const srcMap={uline:["uline_audits",ulineWeeks.length],qb:["qbo_imports",qboImports.length],b600:["b600_timeclock",b600History.length],nuvizz:["nuvizz_stops",nvStops.length],payroll:["payroll_runs",payrollRuns.length]};
              const [col,cnt]=srcMap[source];
              deleteAll(col,source,cnt);
            }} style={{padding:"6px 12px",fontSize:11,borderRadius:6,border:`1px solid ${T.red}`,background:T.redBg,color:T.redText,fontWeight:600,cursor:"pointer",fontFamily:"inherit"}}>🗑️ Delete ALL {source} records</button>
          </div>
        )}
        <div style={{maxHeight:500,overflowY:"auto"}}>
          {allRecords.length===0?<div style={{textAlign:"center",padding:30,color:T.textMuted,fontSize:13}}>No records found.</div>:allRecords.map((r,i)=>(
            <div key={`${r.type}_${r.id}`} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"10px 12px",borderBottom:`1px solid ${T.borderLight}`,background:T.bgSurface,borderRadius:8,marginBottom:4}}>
              <div style={{flex:1,minWidth:0}}>
                <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:3}}>
                  <Badge text={`${r.icon} ${r.label}`} color="#fff" bg={r.color}/>
                  <span style={{fontSize:12,fontWeight:600,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{r.filename}</span>
                </div>
                <div style={{fontSize:10,color:T.textMuted}}>
                  {r.date?new Date(r.date).toLocaleDateString():"?"} • {r.detail}
                </div>
              </div>
              <button onClick={()=>deleteDoc(r.collection,r.id,r.filename,r.onDelete)} title="Delete this import" style={{marginLeft:8,padding:"6px 10px",borderRadius:6,border:`1px solid ${T.red}`,background:"transparent",color:T.red,fontSize:14,cursor:"pointer",fontFamily:"inherit",flexShrink:0}}>🗑️</button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// DATA INTEGRITY ENGINE
// ═══════════════════════════════════════════════════════════════
function DataIntegrity({ulineWeeks}){
  const [scanning,setScanning]=useState(false);
  const [report,setReport]=useState(null);
  const [cleaning,setCleaning]=useState(false);
  const [cleanResult,setCleanResult]=useState(null);
  const [view,setView]=useState("overview");

  const runScan=async()=>{
    if(!hasFirebase) return;
    setScanning(true);setReport(null);setCleanResult(null);
    const r={uline:{},nuvizz:{},b600:{},cross:{}};

    try{
      // ── 1. Uline PRO scan ──
      // Collect all PROs across all uploaded weeks
      const allPros=new Map(); // pro → [{weekId, cost, customer, city}]
      ulineWeeks.forEach(w=>{
        (w.stops||[]).forEach(s=>{
          if(!s.pro) return;
          if(!allPros.has(s.pro)) allPros.set(s.pro,[]);
          allPros.get(s.pro).push({weekId:w.week_id||w.id,filename:w.filename,cost:s.newCost||s.cost,customer:s.customer,city:s.city});
        });
      });
      const dupPros=[...allPros.entries()].filter(([k,v])=>v.length>1);
      const totalUlineStops=ulineWeeks.reduce((s,w)=>s+(w.totalStops||0),0);
      r.uline={
        totalPros:allPros.size,
        totalStops:totalUlineStops,
        duplicatePros:dupPros.length,
        duplicateRevenue:dupPros.reduce((s,[k,v])=>{const extra=v.slice(1);return s+extra.reduce((ss,e)=>ss+e.cost,0);},0),
        topDups:dupPros.slice(0,20).map(([pro,entries])=>({pro,count:entries.length,files:entries.map(e=>e.filename).filter((v,i,a)=>a.indexOf(v)===i),revenue:entries.reduce((s,e)=>s+e.cost,0),customer:entries[0].customer})),
        health:dupPros.length===0?"clean":"has_duplicates",
      };

      // ── 2. NuVizz stop scan ──
      let nvTotal=0,nvDups=0;
      try{
        const snap=await window.db.collection("nuvizz_stops").get();
        nvTotal=snap.size;
        const nvKeys=new Map();
        snap.docs.forEach(d=>{
          const data=d.data();
          const key=data.stopNbr||d.id;
          if(!nvKeys.has(key)) nvKeys.set(key,[]);
          nvKeys.get(key).push(d.id);
        });
        nvDups=[...nvKeys.values()].filter(v=>v.length>1).length;
        r.nuvizz={total:nvTotal,uniqueStops:nvKeys.size,duplicates:nvDups,health:nvDups===0?"clean":"has_duplicates"};
      }catch(e){r.nuvizz={total:0,error:e.message};}

      // ── 3. B600 scan ──
      let b6Total=0,b6Dups=0;
      try{
        const snap=await window.db.collection("b600_timeclock").get();
        b6Total=snap.size;
        const b6Keys=new Map();
        snap.docs.forEach(d=>{
          const data=d.data();
          const key=`${data.date}_${data.name}`;
          if(!b6Keys.has(key)) b6Keys.set(key,[]);
          b6Keys.get(key).push(d.id);
        });
        b6Dups=[...b6Keys.values()].filter(v=>v.length>1).length;
        r.b600={total:b6Total,uniqueRecords:b6Keys.size,duplicates:b6Dups,health:b6Dups===0?"clean":"has_duplicates"};
      }catch(e){r.b600={total:0,error:e.message};}

      // ── 4. Cross-source match (Uline PRO ↔ NuVizz stopNbr) ──
      try{
        const nvSnap=await window.db.collection("nuvizz_stops").get();
        const nvStopNbrs=new Set();
        nvSnap.docs.forEach(d=>{const data=d.data();if(data.stopNbr) nvStopNbrs.add(String(data.stopNbr).replace(/^0+/,""));});
        const ulinePros=new Set([...allPros.keys()]);
        const matched=[...ulinePros].filter(p=>nvStopNbrs.has(p));
        const ulineOnly=[...ulinePros].filter(p=>!nvStopNbrs.has(p));
        const nvOnly=[...nvStopNbrs].filter(n=>!ulinePros.has(n));
        r.cross={
          ulinePros:ulinePros.size, nuvizzStops:nvStopNbrs.size,
          matched:matched.length, ulineOnly:ulineOnly.length, nuvizzOnly:nvOnly.length,
          matchRate:ulinePros.size>0?(matched.length/ulinePros.size*100):0,
          sampleMatched:matched.slice(0,10),
          sampleUlineOnly:ulineOnly.slice(0,10),
        };
      }catch(e){r.cross={error:e.message};}

    }catch(e){console.warn("Integrity scan error:",e);}
    setReport(r);setScanning(false);
  };

  // ── Clean duplicates ──
  const cleanDuplicates=async(source)=>{
    if(!hasFirebase) return;
    setCleaning(true);setCleanResult(null);
    let removed=0;
    try{
      if(source==="uline"){
        // Rebuild all weeks removing duplicate PROs (keep first occurrence)
        const seenPros=new Set();
        const cleanedWeeks=[];
        // Process weeks oldest first so earliest upload wins
        const sorted=[...ulineWeeks].sort((a,b)=>(a.upload_date||"").localeCompare(b.upload_date||""));
        for(const w of sorted){
          const cleanStops=(w.stops||[]).filter(s=>{
            if(!s.pro) return true; // keep stops without PROs
            if(seenPros.has(s.pro)){removed++;return false;}
            seenPros.add(s.pro);return true;
          });
          if(cleanStops.length!==(w.stops||[]).length){
            // Recalculate totals
            const rev=cleanStops.reduce((s,r)=>s+(r.newCost||r.cost),0);const wt=cleanStops.reduce((s,r)=>s+r.weight,0);
            const cs=cleanStops.filter(s=>s.sealNbr);const cp=cs.reduce((s,r)=>s+(r.newCost||r.cost),0)*0.40;
            const updated={...w,stops:cleanStops,totalStops:cleanStops.length,totalRevenue:rev,totalWeight:wt,avgRevenuePerStop:cleanStops.length>0?rev/cleanStops.length:0,contractorStops:cs.length,contractorPayout:cp,cleaned_at:new Date().toISOString()};
            await FS.saveUlineWeek(w.week_id||w.id,updated);
            cleanedWeeks.push(updated);
          }else{cleanedWeeks.push(w);}
        }
        setCleanResult({source:"uline",removed,message:`Removed ${removed} duplicate PROs across ${ulineWeeks.length} weeks. Earliest occurrence of each PRO was kept.`});
      }
      if(source==="nuvizz"){
        const snap=await window.db.collection("nuvizz_stops").get();
        const seen=new Map();
        for(const doc of snap.docs){
          const data=doc.data();const key=data.stopNbr||doc.id;
          if(seen.has(key)){await window.db.collection("nuvizz_stops").doc(doc.id).delete();removed++;}
          else{seen.set(key,doc.id);}
        }
        setCleanResult({source:"nuvizz",removed,message:`Removed ${removed} duplicate NuVizz stops. First occurrence kept.`});
      }
      if(source==="b600"){
        const snap=await window.db.collection("b600_timeclock").get();
        const seen=new Map();
        for(const doc of snap.docs){
          const data=doc.data();const key=`${data.date}_${data.name}`;
          if(seen.has(key)){await window.db.collection("b600_timeclock").doc(doc.id).delete();removed++;}
          else{seen.set(key,doc.id);}
        }
        setCleanResult({source:"b600",removed,message:`Removed ${removed} duplicate B600 records. First occurrence kept.`});
      }
    }catch(e){setCleanResult({source,removed,message:`Error: ${e.message}`});}
    setCleaning(false);
  };

  useEffect(()=>{runScan();},[ulineWeeks]);

  const healthBadge=(h)=>h==="clean"?<Badge text="✓ Clean" color={T.greenText} bg={T.greenBg}/>:<Badge text="⚠ Duplicates" color={T.redText} bg={T.redBg}/>;

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🛡️" text="Data Integrity" right={<PrimaryBtn text={scanning?"Scanning...":"Run Full Scan"} onClick={runScan} loading={scanning} style={{padding:"8px 16px",fontSize:12}}/>}/>

      {cleanResult&&<div style={{...CS,borderLeft:`4px solid ${cleanResult.removed>0?T.green:T.yellow}`,marginBottom:16}}><div style={{fontSize:13,color:cleanResult.removed>0?T.greenText:T.yellowText}}>{cleanResult.removed>0?"✅":"ℹ️"} {cleanResult.message}</div></div>}

      <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>{[["overview","📊 Overview"],["uline","📦 Uline PROs"],["cross","🔗 Cross-Match"],["clean","🧹 Cleanup"]].map(([id,l])=><TabBtn key={id} active={view===id} label={l} onClick={()=>setView(id)}/>)}</div>

      {!report&&<div style={{...CS,textAlign:"center",padding:30}}><div className="loading-pulse" style={{fontSize:32}}>🔍</div><div style={{fontSize:13,marginTop:8}}>Scanning all data sources...</div></div>}

      {report&&view==="overview"&&(
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
          {/* Uline Health */}
          <div style={CS}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}><div style={{fontSize:13,fontWeight:700}}>📦 Uline Stops</div>{healthBadge(report.uline.health)}</div>
            <DataRow label="Unique PROs" value={fmtNum(report.uline.totalPros)}/>
            <DataRow label="Total stops (all weeks)" value={fmtNum(report.uline.totalStops)}/>
            <DataRow label="Duplicate PROs" value={fmtNum(report.uline.duplicatePros)} valueColor={report.uline.duplicatePros>0?T.red:T.green}/>
            {report.uline.duplicateRevenue>0&&<DataRow label="Duplicate revenue (inflated)" value={fmt(report.uline.duplicateRevenue)} valueColor={T.red} bold/>}
          </div>
          {/* NuVizz Health */}
          <div style={CS}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}><div style={{fontSize:13,fontWeight:700}}>🚚 NuVizz Stops</div>{report.nuvizz.health?healthBadge(report.nuvizz.health):<Badge text="No data" color={T.textDim} bg={T.borderLight}/>}</div>
            <DataRow label="Total records" value={fmtNum(report.nuvizz.total||0)}/>
            <DataRow label="Unique stop #s" value={fmtNum(report.nuvizz.uniqueStops||0)}/>
            <DataRow label="Duplicates" value={fmtNum(report.nuvizz.duplicates||0)} valueColor={(report.nuvizz.duplicates||0)>0?T.red:T.green}/>
          </div>
          {/* B600 Health */}
          <div style={CS}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}><div style={{fontSize:13,fontWeight:700}}>⏰ B600 Time Clock</div>{report.b600.health?healthBadge(report.b600.health):<Badge text="No data" color={T.textDim} bg={T.borderLight}/>}</div>
            <DataRow label="Total records" value={fmtNum(report.b600.total||0)}/>
            <DataRow label="Unique entries" value={fmtNum(report.b600.uniqueRecords||0)}/>
            <DataRow label="Duplicates" value={fmtNum(report.b600.duplicates||0)} valueColor={(report.b600.duplicates||0)>0?T.red:T.green}/>
          </div>
          {/* Cross-source */}
          {report.cross.ulinePros>0&&<div style={{...CS,borderColor:T.brand}}>
            <div style={{fontSize:13,fontWeight:700,marginBottom:10}}>🔗 Uline ↔ NuVizz Match</div>
            <DataRow label="Uline PROs" value={fmtNum(report.cross.ulinePros)}/>
            <DataRow label="NuVizz stop #s" value={fmtNum(report.cross.nuvizzStops)}/>
            <DataRow label="Matched" value={fmtNum(report.cross.matched)} valueColor={T.green} bold/>
            <DataRow label="Match rate" value={fmtPct(report.cross.matchRate)} valueColor={report.cross.matchRate>80?T.green:T.yellow}/>
            <DataRow label="Uline only (no NuVizz)" value={fmtNum(report.cross.ulineOnly)} valueColor={T.yellow}/>
            <DataRow label="NuVizz only (no Uline)" value={fmtNum(report.cross.nuvizzOnly)}/>
          </div>}
        </div>
      )}

      {report&&view==="uline"&&(
        <div style={CS}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Uline PRO Duplicate Detail</div>
          {report.uline.duplicatePros===0?<div style={{color:T.green,fontSize:13,padding:"20px 0",textAlign:"center"}}>✅ No duplicate PROs found across {ulineWeeks.length} uploaded weeks</div>:(
            <>
              <div style={{background:T.redBg,padding:"10px 14px",borderRadius:8,marginBottom:12,fontSize:12,color:T.redText}}>Found {fmtNum(report.uline.duplicatePros)} PROs that appear in multiple uploads. This inflates your revenue by {fmt(report.uline.duplicateRevenue)}.</div>
              <div style={{maxHeight:400,overflowY:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                  <thead><tr>{["PRO","Count","Customer","Files","Revenue"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                  <tbody>{report.uline.topDups.map((d,i)=><tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600,fontFamily:"monospace"}}>{d.pro}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:700}}>{d.count}x</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.customer}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontSize:10,color:T.textMuted}}>{d.files.join(", ")}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{fmt(d.revenue)}</td></tr>)}</tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}

      {report&&view==="cross"&&(
        <div style={CS}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Uline PRO ↔ NuVizz Stop Cross-Reference</div>
          <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Shows how well Uline billing data lines up with NuVizz delivery records. A matched PRO means the same stop exists in both systems — that's confirmed delivered revenue.</div>
          {report.cross.matched>0&&<><div style={{fontSize:12,fontWeight:700,marginTop:12,marginBottom:6}}>Sample Matched PROs (confirmed in both systems)</div><div style={{display:"flex",flexWrap:"wrap",gap:6}}>{report.cross.sampleMatched.map((p,i)=><span key={i} style={{padding:"3px 10px",borderRadius:12,background:T.greenBg,color:T.greenText,fontSize:11,fontFamily:"monospace"}}>{p}</span>)}</div></>}
          {report.cross.ulineOnly>0&&<><div style={{fontSize:12,fontWeight:700,marginTop:16,marginBottom:6,color:T.yellow}}>Sample Uline-Only PROs (billed but no NuVizz record)</div><div style={{display:"flex",flexWrap:"wrap",gap:6}}>{report.cross.sampleUlineOnly.map((p,i)=><span key={i} style={{padding:"3px 10px",borderRadius:12,background:T.yellowBg,color:T.yellowText,fontSize:11,fontFamily:"monospace"}}>{p}</span>)}</div></>}
        </div>
      )}

      {report&&view==="clean"&&(
        <div>
          <div style={{...CS,background:T.redBg,borderColor:T.red}}>
            <div style={{fontSize:13,fontWeight:700,color:T.redText,marginBottom:8}}>⚠️ Cleanup Tools</div>
            <div style={{fontSize:12,color:T.textMuted,marginBottom:16}}>These tools remove duplicate records. First occurrence is always kept. Run a scan first to see what will be affected.</div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:"12px"}}>
            <div style={CS}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>📦 Clean Uline Duplicates</div>
              <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Removes duplicate PROs across all uploaded weeks. Keeps the earliest occurrence of each PRO. Recalculates revenue totals for affected weeks.</div>
              <div style={{fontSize:12,marginBottom:8}}>Duplicates found: <strong style={{color:report.uline.duplicatePros>0?T.red:T.green}}>{report.uline.duplicatePros}</strong></div>
              <PrimaryBtn text={cleaning?"Cleaning...":"Clean Uline Duplicates"} onClick={()=>cleanDuplicates("uline")} loading={cleaning} style={{background:report.uline.duplicatePros>0?T.red:"#94a3b8",padding:"8px 16px",fontSize:12}}/>
            </div>
            <div style={CS}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>🚚 Clean NuVizz Duplicates</div>
              <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Removes duplicate stop numbers from Firebase. Keeps first occurrence.</div>
              <div style={{fontSize:12,marginBottom:8}}>Duplicates found: <strong style={{color:(report.nuvizz.duplicates||0)>0?T.red:T.green}}>{report.nuvizz.duplicates||0}</strong></div>
              <PrimaryBtn text={cleaning?"Cleaning...":"Clean NuVizz Duplicates"} onClick={()=>cleanDuplicates("nuvizz")} loading={cleaning} style={{background:(report.nuvizz.duplicates||0)>0?T.red:"#94a3b8",padding:"8px 16px",fontSize:12}}/>
            </div>
            <div style={CS}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>⏰ Clean B600 Duplicates</div>
              <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Removes duplicate date+employee records. Keeps first occurrence.</div>
              <div style={{fontSize:12,marginBottom:8}}>Duplicates found: <strong style={{color:(report.b600.duplicates||0)>0?T.red:T.green}}>{report.b600.duplicates||0}</strong></div>
              <PrimaryBtn text={cleaning?"Cleaning...":"Clean B600 Duplicates"} onClick={()=>cleanDuplicates("b600")} loading={cleaning} style={{background:(report.b600.duplicates||0)>0?T.red:"#94a3b8",padding:"8px 16px",fontSize:12}}/>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
// ═══════════════════════════════════════════════════════════════
// ULINE ↔ NUVIZZ RECONCILIATION
// ═══════════════════════════════════════════════════════════════
function ReconciliationTab({ulineWeeks}){
  const [loading,setLoading]=useState(false);
  const [nvStops,setNvStops]=useState(null);
  const [report,setReport]=useState(null);
  const [view,setView]=useState("summary");
  const [pullDates,setPullDates]=useState({from:"",to:""});

  // Load stored NuVizz stops from Firebase
  const loadNvStops=async()=>{
    if(!hasFirebase) return {};
    try{const s=await window.db.collection("nuvizz_stops").get();const map={};s.docs.forEach(d=>{const data=d.data();const key=String(data.stopNbr||d.id).replace(/^0+/,"");map[key]=data;});return map;}catch(e){return {};}
  };

  // Also try live NuVizz API for a date range
  const pullLiveNuVizz=async(from,to)=>{
    try{const j=await fetchNuVizzStops(from,to);if(!j) return {};const stops=Array.isArray(j)?j:(j.stopList||j.stops||j.data||[]);const map={};stops.forEach(s=>{const key=String(s.stopNbr||s.id||"").replace(/^0+/,"");if(key) map[key]=s;});return map;}catch(e){return {};}
  };

  const runReconciliation=async(useLive)=>{
    setLoading(true);setReport(null);
    try{
      // Get all Uline PROs
      const allStops=ulineWeeks.flatMap(w=>w.stops||[]);
      const ulinePros=new Map();
      allStops.forEach(s=>{
        if(!s.pro) return;
        const key=String(s.pro).replace(/^0+/,"");
        if(!ulinePros.has(key)) ulinePros.set(key,{pro:key,customer:s.customer,city:s.city,state:s.state,zip:s.zip,weight:s.weight,cost:s.newCost||s.cost,skids:s.skids,order:s.order,warehouse:s.warehouse});
        else{const existing=ulinePros.get(key);existing.cost+=(s.newCost||s.cost);} // aggregate if dup PRO
      });

      // Get NuVizz data (stored + optionally live)
      let nvMap=await loadNvStops();
      if(useLive&&pullDates.from&&pullDates.to){
        const liveMap=await pullLiveNuVizz(pullDates.from,pullDates.to);
        nvMap={...nvMap,...liveMap};
      }
      const nvKeys=new Set(Object.keys(nvMap));

      // Build reconciliation
      const matched=[];const ulineOnly=[];const nvOnly=[];
      ulinePros.forEach((uData,pro)=>{
        if(nvKeys.has(pro)){
          const nv=nvMap[pro];
          matched.push({
            pro, source:"both",
            // Uline side
            uCustomer:uData.customer, uCity:uData.city, uState:uData.state, uZip:uData.zip,
            uWeight:uData.weight, uCost:uData.cost, uSkids:uData.skids, uOrder:uData.order,
            // NuVizz side
            nDriver:nv.driverName||nv.driver||"",
            nRoute:nv.loadNbr||nv.routeName||"",
            nStatus:nv.stopStatus||(nv.stopExecutionInfo?.stopStatus)||"",
            nCity:nv.city||(nv.to?.address?.city)||"",
            nVehicle:nv.vehicleNbr||"",
            nArrival:nv.actualArrival||(nv.stopExecutionInfo?.to?.actualArrival)||"",
            nDeparture:nv.actualDeparture||(nv.stopExecutionInfo?.to?.actualDeparture)||"",
            nWeight:nv.weight||0,
            nPallets:nv.totalPallets||0,
          });
          nvKeys.delete(pro);
        }else{
          ulineOnly.push({pro,source:"uline_only",...uData});
        }
      });
      // Remaining NuVizz stops not in Uline
      nvKeys.forEach(key=>{
        const nv=nvMap[key];
        nvOnly.push({
          pro:key,source:"nuvizz_only",
          nDriver:nv.driverName||nv.driver||"",
          nRoute:nv.loadNbr||nv.routeName||"",
          nStatus:nv.stopStatus||(nv.stopExecutionInfo?.stopStatus)||"",
          nCity:nv.city||(nv.to?.address?.city)||"",
          nCustomer:nv.custName||nv.customerName||(nv.to?.address?.name)||"",
          nWeight:nv.weight||0,
        });
      });

      // Revenue analysis
      const matchedRevenue=matched.reduce((s,r)=>s+r.uCost,0);
      const ulineOnlyRevenue=ulineOnly.reduce((s,r)=>s+r.cost,0);
      const totalUlineRevenue=matchedRevenue+ulineOnlyRevenue;
      const matchRate=ulinePros.size>0?(matched.length/ulinePros.size*100):0;

      // Status analysis on matched stops
      const delivered=matched.filter(m=>{const st=String(m.nStatus).toLowerCase();return st.match(/90|91|deliver|complet/);});
      const notDelivered=matched.filter(m=>{const st=String(m.nStatus).toLowerCase();return !st.match(/90|91|deliver|complet/);});

      // Driver revenue from matched stops
      const driverRevenue={};
      matched.forEach(m=>{
        const drv=m.nDriver||"Unknown";
        if(!driverRevenue[drv]) driverRevenue[drv]={driver:drv,stops:0,revenue:0,weight:0,routes:new Set()};
        driverRevenue[drv].stops++;driverRevenue[drv].revenue+=m.uCost;driverRevenue[drv].weight+=m.uWeight;
        if(m.nRoute) driverRevenue[drv].routes.add(m.nRoute);
      });
      const driverArr=Object.values(driverRevenue).map(d=>({...d,routes:d.routes.size,avgPerStop:d.stops>0?d.revenue/d.stops:0})).sort((a,b)=>b.revenue-a.revenue);

      // Route revenue from matched stops
      const routeRevenue={};
      matched.forEach(m=>{
        const rt=m.nRoute||"Unknown";
        if(!routeRevenue[rt]) routeRevenue[rt]={route:rt,driver:m.nDriver,stops:0,revenue:0,weight:0};
        routeRevenue[rt].stops++;routeRevenue[rt].revenue+=m.uCost;routeRevenue[rt].weight+=m.uWeight;
      });
      const routeArr=Object.values(routeRevenue).sort((a,b)=>b.revenue-a.revenue);

      setReport({
        ulinePros:ulinePros.size,nvStops:Object.keys(nvMap).length+nvKeys.size,
        matched,ulineOnly,nvOnly,
        matchedRevenue,ulineOnlyRevenue,totalUlineRevenue,matchRate,
        delivered,notDelivered,
        byDriver:driverArr,byRoute:routeArr,
      });
    }catch(e){console.warn("Reconciliation error:",e);}
    setLoading(false);
  };

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="🔗" text="Uline ↔ NuVizz Reconciliation"/>

      {/* Controls */}
      <div style={CS}>
        <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>Match Uline billing PROs against NuVizz delivery records. Uses stored NuVizz data from Firebase. Optionally pull live NuVizz data for a date range.</div>
        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap",marginBottom:10}}>
          <span style={{fontSize:12,color:T.textMuted}}>Live pull (optional):</span>
          <input type="date" value={pullDates.from} onChange={e=>setPullDates(p=>({...p,from:e.target.value}))} style={{...IS,width:140,fontSize:12}}/>
          <span style={{color:T.textDim}}>→</span>
          <input type="date" value={pullDates.to} onChange={e=>setPullDates(p=>({...p,to:e.target.value}))} style={{...IS,width:140,fontSize:12}}/>
        </div>
        <div style={{display:"flex",gap:8}}>
          <PrimaryBtn text={loading?"Reconciling...":"Reconcile (Stored Data)"} onClick={()=>runReconciliation(false)} loading={loading}/>
          {pullDates.from&&pullDates.to&&<PrimaryBtn text={loading?"Pulling...":"Reconcile + Live Pull"} onClick={()=>runReconciliation(true)} loading={loading} style={{background:`linear-gradient(135deg,${T.green},#059669)`}}/>}
        </div>
      </div>

      {ulineWeeks.length===0&&<EmptyState icon="📦" title="No Uline Data" sub="Upload Uline XLSX files first, then run reconciliation"/>}

      {report&&(
        <>
          {/* Summary KPIs */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Uline PROs" value={fmtNum(report.ulinePros)}/>
            <KPI label="NuVizz Stops" value={fmtNum(report.nvStops)}/>
            <KPI label="Matched" value={fmtNum(report.matched.length)} sub={fmtPct(report.matchRate)+" match rate"} subColor={report.matchRate>80?T.green:T.yellow}/>
            <KPI label="Matched Revenue" value={fmtK(report.matchedRevenue)} sub="confirmed delivered" subColor={T.green}/>
            <KPI label="Uline Only" value={fmtNum(report.ulineOnly.length)} sub={fmtK(report.ulineOnlyRevenue)} subColor={report.ulineOnly.length>0?T.yellow:T.green}/>
            <KPI label="NuVizz Only" value={fmtNum(report.nvOnly.length)} sub="delivered, not billed?" subColor={report.nvOnly.length>0?T.red:T.green}/>
          </div>

          {/* Revenue health bar */}
          <div style={{...CS,borderLeft:`4px solid ${report.matchRate>80?T.green:T.yellow}`,marginBottom:16}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
              <div style={{fontSize:13,fontWeight:700}}>Revenue Reconciliation</div>
              <div style={{fontSize:20,fontWeight:800,color:report.matchRate>80?T.green:T.yellow}}>{fmtPct(report.matchRate)}</div>
            </div>
            <MiniBar pct={report.matchRate} color={report.matchRate>80?T.green:T.yellow} height={10}/>
            <div style={{display:"flex",justifyContent:"space-between",marginTop:8,fontSize:11}}>
              <span style={{color:T.green}}>✅ Confirmed: {fmt(report.matchedRevenue)}</span>
              <span style={{color:T.yellow}}>⚠️ Unmatched: {fmt(report.ulineOnlyRevenue)}</span>
              <span style={{color:T.red}}>❓ Unbilled: {fmtNum(report.nvOnly.length)} stops</span>
            </div>
          </div>

          <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
            {[["summary","📊 Summary"],["drivers","👤 Driver Revenue"],["routes","🛣️ Route Revenue"],["uline_only","⚠️ Billed Not Delivered"],["nv_only","❓ Delivered Not Billed"],["matched","✅ All Matched"]].map(([id,l])=><TabBtn key={id} active={view===id} label={l} onClick={()=>setView(id)}/>)}
          </div>

          {view==="summary"&&(
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}>
              <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by Driver (from matched PROs)</div><BarChart data={report.byDriver.slice(0,15)} labelKey="driver" valueKey="revenue" maxBars={15}/></div>
              <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:12}}>Revenue by Route (from matched PROs)</div><BarChart data={report.byRoute.slice(0,15)} labelKey="route" valueKey="revenue" color={T.green} maxBars={15}/></div>
            </div>
          )}

          {view==="drivers"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Actual Revenue per Driver (Uline billing × NuVizz delivery)</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:500}}>
              <thead><tr>{["#","Driver","Stops","Revenue","Avg/Stop","Weight","Routes"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{report.byDriver.map((d,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textDim}}>{i+1}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.driver}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(d.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(d.avgPerStop)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(d.weight)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.routes}</td></tr>
              ))}</tbody>
            </table></div>
          </div>}

          {view==="routes"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Actual Revenue per Route</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:500}}>
              <thead><tr>{["Route","Driver","Stops","Revenue","Weight"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{report.byRoute.map((r,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{r.route}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.driver}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{r.stops}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:700}}>{fmt(r.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(r.weight)}</td></tr>
              ))}</tbody>
            </table></div>
          </div>}

          {view==="uline_only"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:4}}>⚠️ Billed in Uline but No NuVizz Delivery Record ({report.ulineOnly.length})</div>
            <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These PROs appear in Uline billing but have no matching stop in NuVizz. Could be: different stop number format, stop not yet imported, or billing error.</div>
            {report.ulineOnly.length===0?<div style={{color:T.green,padding:"20px 0",textAlign:"center",fontSize:13}}>✅ All Uline PROs have matching NuVizz records</div>:(
              <div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:400}}>
                <thead><tr>{["PRO","Customer","City","Weight","Revenue"].map(h=><th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                <tbody>{report.ulineOnly.slice(0,100).map((s,i)=>(
                  <tr key={i} style={{background:T.yellowBg}}><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontWeight:600}}>{s.pro}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.customer}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.city}, {s.state}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(s.weight)}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.yellow,fontWeight:600}}>{fmt(s.cost)}</td></tr>
                ))}</tbody>
              </table>{report.ulineOnly.length>100&&<div style={{fontSize:11,color:T.textDim,marginTop:8}}>...and {report.ulineOnly.length-100} more</div>}</div>
            )}
          </div>}

          {view==="nv_only"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:4}}>❓ Delivered in NuVizz but No Uline Billing ({report.nvOnly.length})</div>
            <div style={{fontSize:12,color:T.textMuted,marginBottom:12}}>These stops were delivered according to NuVizz but don't appear in any uploaded Uline billing file. Potential revenue you haven't been paid for.</div>
            {report.nvOnly.length===0?<div style={{color:T.green,padding:"20px 0",textAlign:"center",fontSize:13}}>✅ All NuVizz deliveries have matching Uline billing</div>:(
              <div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:400}}>
                <thead><tr>{["Stop #","Customer","City","Driver","Route","Status"].map(h=><th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
                <tbody>{report.nvOnly.slice(0,100).map((s,i)=>(
                  <tr key={i} style={{background:T.redBg}}><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontWeight:600}}>{s.pro}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.nCustomer}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.nCity}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.nDriver}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.nRoute}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{s.nStatus}</td></tr>
                ))}</tbody>
              </table>{report.nvOnly.length>100&&<div style={{fontSize:11,color:T.textDim,marginTop:8}}>...and {report.nvOnly.length-100} more</div>}</div>
            )}
          </div>}

          {view==="matched"&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>✅ All Matched PROs ({report.matched.length})</div>
            <div style={{maxHeight:400,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11,minWidth:600}}>
              <thead><tr>{["PRO","Customer","City","Driver","Route","Revenue","Status"].map(h=><th key={h} style={{textAlign:"left",padding:"6px 8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{report.matched.slice(0,200).map((m,i)=>{
                const isDelivered=String(m.nStatus).match(/90|91/);
                return(<tr key={i}><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontFamily:"monospace",fontSize:10}}>{m.pro}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500}}>{m.uCustomer}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{m.uCity}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{m.nDriver}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}>{m.nRoute}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(m.uCost)}</td><td style={{padding:"6px 8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={isDelivered?"Delivered":m.nStatus||"?"} color={isDelivered?T.greenText:T.yellowText} bg={isDelivered?T.greenBg:T.yellowBg}/></td></tr>);
              })}</tbody>
            </table>{report.matched.length>200&&<div style={{fontSize:11,color:T.textDim,marginTop:8}}>Showing first 200 of {report.matched.length}</div>}</div>
          </div>}
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// QB FILE IMPORT + FINANCIAL ANALYZER (v5 — quarterly/monthly/yearly)
// ═══════════════════════════════════════════════════════════════
function QBOImport(){
  const [view,setView]=useState("upload");
  const [importing,setImporting]=useState(false);
  const [result,setResult]=useState(null);
  const [qboFiles,setQboFiles]=useState([]);
  const [glData,setGlData]=useState(null);
  const [period,setPeriod]=useState("quarter"); // month | quarter | year
  const [selectedCat,setSelectedCat]=useState(null);
  const glRef=useRef(null);const pnlRef=useRef(null);const bsRef=useRef(null);const custRef=useRef(null);const payRef=useRef(null);

  // Load existing imports from Firebase
  useEffect(()=>{(async()=>{
    if(!hasFirebase) return;
    try{const s=await window.db.collection("qbo_imports").orderBy("imported_at","desc").limit(50).get();
      const files=s.docs.map(d=>({id:d.id,...d.data()}));
      setQboFiles(files);
      // If we have a GL import, load it
      const glImport=files.find(f=>f.type==="gl");
      if(glImport?.transactions) setGlData(glImport);
    }catch(e){}
  })();},[]);

  // ── Generic XLSX parser ──
  const parseXLSX=(file)=>new Promise((resolve,reject)=>{
    if(typeof XLSX==="undefined")return reject(new Error("XLSX not loaded"));
    const reader=new FileReader();
    reader.onload=e=>{
      try{const wb=XLSX.read(e.target.result,{type:"array",cellDates:true});const ws=wb.Sheets[wb.SheetNames[0]];const raw=XLSX.utils.sheet_to_json(ws,{defval:"",header:1,raw:false});resolve(raw);}
      catch(err){reject(err);}
    };
    reader.onerror=reject;reader.readAsArrayBuffer(file);
  });

  // ── General Ledger Parser (the big one) ──
  const parseGL=async(file)=>{
    const raw=await parseXLSX(file);
    // Find header row (row with "Date", "Debit", "Credit" columns)
    let headerIdx=-1;let cols={};
    for(let i=0;i<Math.min(20,raw.length);i++){
      const lc=raw[i].map(c=>String(c||"").toLowerCase().trim());
      if(lc.includes("date")&&(lc.includes("debit")||lc.includes("credit"))){
        headerIdx=i;
        lc.forEach((h,j)=>{cols[h]=j;});
        break;
      }
    }
    if(headerIdx<0) throw new Error("Could not find GL header row. Expected 'Date', 'Debit', 'Credit' columns.");

    const dateCol=cols["date"];const typeCol=cols["transaction type"]||cols["type"];const numCol=cols["num"];
    const nameCol=cols["name"];const memoCol=cols["memo/description"]||cols["memo"];
    const accountCol=cols["account"];const debitCol=cols["debit"];const creditCol=cols["credit"];

    const txns=[];let currentAcct=null;
    for(let i=headerIdx+1;i<raw.length;i++){
      const row=raw[i];if(!row||row.length===0) continue;
      const firstCell=String(row[0]||"").trim();
      const dateCell=row[dateCol];
      // Account header row: something in col 0, no date
      if(firstCell&&!dateCell&&!String(firstCell).toLowerCase().startsWith("total")){
        currentAcct=firstCell;continue;
      }
      if(String(firstCell).toLowerCase().startsWith("total")){currentAcct=null;continue;}
      if(!dateCell) continue;
      // Parse date (MM/DD/YYYY)
      let date=null;
      if(dateCell instanceof Date) date=dateCell;
      else if(typeof dateCell==="string"&&dateCell.includes("/")){
        const p=dateCell.split("/");
        if(p.length===3){date=new Date(parseInt(p[2]),parseInt(p[0])-1,parseInt(p[1]));}
      }
      if(!date||isNaN(date.getTime())) continue;
      const account=row[accountCol]||currentAcct||"";
      if(!account) continue;
      const debit=parseFloat(String(row[debitCol]||"0").replace(/[$,]/g,""))||0;
      const credit=parseFloat(String(row[creditCol]||"0").replace(/[$,]/g,""))||0;
      txns.push({
        date:date.toISOString().slice(0,10),
        year:date.getFullYear(),
        month:date.getMonth()+1,
        quarter:Math.floor(date.getMonth()/3)+1,
        yearMonth:`${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,"0")}`,
        yearQuarter:`${date.getFullYear()}-Q${Math.floor(date.getMonth()/3)+1}`,
        type:String(row[typeCol]||""),
        num:String(row[numCol]||""),
        name:String(row[nameCol]||""),
        memo:String(row[memoCol]||""),
        account:String(account).trim(),
        debit,credit,
        net:debit-credit, // positive = debit (expense/asset), negative = credit (income/liability)
      });
    }
    return txns;
  };

  // ── Handle GL upload (the big one) ──
  const handleGL=async(e)=>{
    const file=e.target.files?.[0];if(!file)return;
    setImporting(true);setResult(null);
    try{
      const txns=await parseGL(file);
      if(txns.length===0) throw new Error("No transactions found in file");
      // Store metadata + transactions. Firebase doc limit is 1MB, so compress if needed
      const totalDebit=txns.reduce((s,t)=>s+t.debit,0);
      const totalCredit=txns.reduce((s,t)=>s+t.credit,0);
      const dates=txns.map(t=>t.date).sort();
      const summary={type:"gl",filename:file.name,txnCount:txns.length,totalDebit,totalCredit,dateStart:dates[0],dateEnd:dates[dates.length-1],imported_at:new Date().toISOString()};
      // Store transactions in chunks if large (Firestore doc limit ~1MB)
      if(hasFirebase){
        const docId=`gl_${file.name}`.replace(/[\/\s\.]/g,"_").substring(0,200);
        // Check dedup
        const existing=await window.db.collection("qbo_imports").doc(docId).get();
        if(existing.exists){setResult({type:"dup",filename:file.name,reportType:"gl"});setImporting(false);return;}
        // Compact transaction storage — we save the full list for analysis
        try{
          await window.db.collection("qbo_imports").doc(docId).set({...summary,transactions:txns});
        }catch(err){
          // Too large — save summary only
          await window.db.collection("qbo_imports").doc(docId).set({...summary,note:"Transactions too large for single doc, stored in memory only"});
        }
      }
      setGlData({...summary,transactions:txns});
      setResult({type:"success",reportType:"gl",filename:file.name,...summary});
      setView("analyzer");
    }catch(err){setResult({type:"error",message:err.message});}
    setImporting(false);if(glRef.current)glRef.current.value="";
  };

  // ── Handle P&L, BS, Customers, Payroll uploads (summary-style) ──
  const handleSimpleReport=async(e,type)=>{
    const file=e.target.files?.[0];if(!file)return;
    setImporting(true);setResult(null);
    try{
      const raw=await parseXLSX(file);
      const rows=raw.filter(r=>r.some(c=>c!==""));
      let summary={type,filename:file.name,rowCount:rows.length,imported_at:new Date().toISOString()};

      if(type==="pnl"){
        const items=[];let section=null;
        for(const row of rows){
          const label=String(row[0]||"").trim();const val=parseFloat(String(row[1]||"0").replace(/[$,]/g,""));
          if(!label) continue;
          if(label==="Income") section="income";
          else if(label==="Expenses") section="expense";
          else if(label==="Other Income") section="other_income";
          else if(label==="Other Expenses") section="other_expense";
          else if(label==="Gross Profit"||label==="Net Income"||label==="Net Operating Income"||label==="Net Other Income") continue;
          else if(!isNaN(val)&&val!==0&&section){
            items.push({label,value:val,section,isTotal:label.toLowerCase().startsWith("total ")});
          }
        }
        const totalIncome=items.filter(i=>i.section==="income"&&i.isTotal).reduce((s,i)=>s+i.value,0)||items.filter(i=>i.section==="income"&&!i.isTotal).reduce((s,i)=>s+i.value,0);
        const totalExpense=items.filter(i=>i.section==="expense"&&i.isTotal).reduce((s,i)=>s+i.value,0)||items.filter(i=>i.section==="expense"&&!i.isTotal).reduce((s,i)=>s+i.value,0);
        summary.items=items;summary.totalRevenue=totalIncome;summary.totalExpenses=totalExpense;summary.netIncome=totalIncome-totalExpense;summary.margin=totalIncome>0?((totalIncome-totalExpense)/totalIncome*100):0;
      }
      if(type==="bs"){
        const items=[];let section=null;
        for(const row of rows){
          const label=String(row[0]||"").trim();const val=parseFloat(String(row[1]||"0").replace(/[$,]/g,""));
          if(!label) continue;
          if(label==="Assets") section="asset";
          else if(label==="Liabilities") section="liability";
          else if(label==="Equity") section="equity";
          else if(label.startsWith("Total for")) continue;
          else if(!isNaN(val)&&val!==0&&section) items.push({label,value:val,section});
        }
        summary.items=items;summary.totalAssets=items.filter(i=>i.section==="asset").reduce((s,i)=>s+i.value,0);
        summary.totalLiabilities=items.filter(i=>i.section==="liability").reduce((s,i)=>s+i.value,0);
        summary.totalEquity=items.filter(i=>i.section==="equity").reduce((s,i)=>s+i.value,0);
      }
      if(type==="customers"){
        const customers=[];
        for(const row of rows){
          const name=String(row[0]||"").trim();const val=parseFloat(String(row[1]||"0").replace(/[$,]/g,""));
          if(name&&!isNaN(val)&&val!==0&&!name.startsWith("Total")&&name!=="Sales by Customer Summary"&&name!=="Davis Delivery Service"&&!name.match(/^\d{4}/)) customers.push({name,revenue:val});
        }
        customers.sort((a,b)=>b.revenue-a.revenue);
        summary.customers=customers;summary.totalRevenue=customers.reduce((s,c)=>s+c.revenue,0);
      }
      if(type==="payroll"){
        const employees=[];
        for(const row of rows){
          const name=String(row[0]||"").trim();if(!name||name.startsWith("Total")||name==="TOTAL") continue;
          const vals=row.slice(1).map(c=>parseFloat(String(c||"0").replace(/[$,]/g,""))||0);
          if(vals.some(v=>v>0)) employees.push({employee:name,grossPay:vals[0]||0,totalPay:vals[vals.length-1]||vals[0]||0});
        }
        summary.employees=employees;summary.totalGross=employees.reduce((s,e)=>s+e.grossPay,0);summary.headcount=employees.length;
      }

      // Dedup
      const docId=`${type}_${file.name}`.replace(/[\/\s\.]/g,"_").substring(0,200);
      if(hasFirebase){
        const existing=await window.db.collection("qbo_imports").doc(docId).get();
        if(existing.exists){setResult({type:"dup",filename:file.name,reportType:type});setImporting(false);return;}
        await window.db.collection("qbo_imports").doc(docId).set(summary);
      }
      setResult({type:"success",reportType:type,...summary});
      setQboFiles(prev=>[{id:docId,...summary},...prev]);
    }catch(err){setResult({type:"error",message:err.message});}
    setImporting(false);
    [pnlRef,bsRef,custRef,payRef].forEach(r=>{if(r?.current)r.current.value="";});
  };

  // ═══ GL-based Financial Analysis ═══
  // Expense/income categorization from account names
  const classifyAccount=(account)=>{
    const a=String(account).toLowerCase();
    // Income
    if(a.includes("delivery sales")||a.includes("sales income")||a.match(/^income/)) return {category:"revenue",subcat:"Delivery Sales"};
    if(a.includes("misc") && a.includes("income")) return {category:"revenue",subcat:"Other Income"};
    // Expenses — payroll
    if(a.includes("salaries")||a.includes("wages")) return {category:"expense",subcat:"Salaries & Wages"};
    if(a.includes("subcontractor")||a.includes("3rd party delivery")) return {category:"expense",subcat:"Subcontractors"};
    if(a.includes("temporary services")) return {category:"expense",subcat:"Temp Labor"};
    if(a.includes("officers salaries")) return {category:"expense",subcat:"Officer Salaries"};
    if(a.includes("payroll tax")) return {category:"expense",subcat:"Payroll Taxes"};
    if(a.includes("payroll fees")||a.includes("payroll deduction")||a.includes("contract labor")) return {category:"expense",subcat:"Payroll Other"};
    // Fleet
    if(a.includes("truck fuel")||a.includes("fuel - gas")) return {category:"expense",subcat:"Fuel"};
    if(a.includes("truck repair")||a.includes("truck maintenance")||a.includes("truck parts")||a.includes("truck tires")||a.includes("truck wash")||a.includes("truck - misc")||a.includes("trailer repair")||a.includes("towing")) return {category:"expense",subcat:"Truck Maintenance"};
    if(a.includes("truck lease")||a.includes("trailer rental")||a.includes("rent - equipment")) return {category:"expense",subcat:"Truck Leases"};
    if(a.includes("taxes & licenses - trucks")||a.includes("licenses & permits")) return {category:"expense",subcat:"Truck Taxes/Licenses"};
    // Insurance
    if(a.includes("insurance")&&!a.includes("health")) return {category:"expense",subcat:"Insurance"};
    if(a.includes("health plan")||a.includes("fsa")||a.includes("health & dental")||a.includes("medical")) return {category:"expense",subcat:"Health Insurance"};
    // Facilities
    if(a.includes("rent - office")||a.includes("rent office")) return {category:"expense",subcat:"Rent"};
    if(a.includes("warehouse")||a.includes("propane")||a.includes("fork lift")||a.includes("forklift")) return {category:"expense",subcat:"Warehouse"};
    if(a.includes("utilities")||a.includes("electric")||a.includes("cell phone")||a.includes("telephone")||a.includes("trash")) return {category:"expense",subcat:"Utilities"};
    // Admin
    if(a.includes("computer")||a.includes("internet")) return {category:"expense",subcat:"IT"};
    if(a.includes("bank charge")||a.includes("interest")) return {category:"expense",subcat:"Bank/Interest"};
    if(a.includes("legal")||a.includes("professional")) return {category:"expense",subcat:"Legal/Professional"};
    if(a.includes("depreciation")) return {category:"expense",subcat:"Depreciation"};
    if(a.includes("office")||a.includes("admin")) return {category:"expense",subcat:"Office/Admin"};
    if(a.includes("meals")||a.includes("travel")||a.includes("seminars")) return {category:"expense",subcat:"Travel/Meals"};
    if(a.includes("damages")||a.includes("claims")||a.includes("penalties")) return {category:"expense",subcat:"Damages/Claims"};
    if(a.includes("uniform")) return {category:"expense",subcat:"Uniforms"};
    if(a.includes("uline purchase")) return {category:"expense",subcat:"Uline Supplies"};
    if(a.includes("advertis")||a.includes("promotion")) return {category:"expense",subcat:"Marketing"};
    if(a.includes("state tax")||a.includes("property tax")||a.includes("other taxes")) return {category:"expense",subcat:"Other Taxes"};
    if(a.includes("repair")||a.includes("maintenance")) return {category:"expense",subcat:"Repairs"};
    if(a.includes("supplies")||a.includes("materials")||a.includes("stationery")) return {category:"expense",subcat:"Supplies"};
    if(a.includes("retirement")||a.includes("401k")) return {category:"expense",subcat:"Retirement"};
    if(a.includes("dues")||a.includes("subscription")) return {category:"expense",subcat:"Dues/Subs"};
    if(a.includes("bad debt")) return {category:"expense",subcat:"Bad Debts"};
    // Bank/equity/etc - not for P&L
    return {category:"other",subcat:a};
  };

  const analysis=useMemo(()=>{
    if(!glData?.transactions) return null;
    const txns=glData.transactions;
    // Bucket key depends on period
    const bucketKey=t=>period==="year"?String(t.year):period==="quarter"?t.yearQuarter:t.yearMonth;
    // Build: periodBuckets with income + expense by subcat
    const buckets={};
    txns.forEach(t=>{
      const cls=classifyAccount(t.account);
      if(cls.category==="other") return;
      const bk=bucketKey(t);
      if(!buckets[bk]) buckets[bk]={period:bk,revenue:0,expense:0,byCategory:{}};
      // For revenue accounts: CREDITS increase revenue (sales booked as credits)
      // For expense accounts: DEBITS increase expense
      if(cls.category==="revenue"){
        const amt=t.credit-t.debit; // net credit = new revenue
        buckets[bk].revenue+=amt;
        buckets[bk].byCategory[cls.subcat]=(buckets[bk].byCategory[cls.subcat]||0)+amt;
      }else{
        const amt=t.debit-t.credit; // net debit = new expense
        buckets[bk].expense+=amt;
        buckets[bk].byCategory[cls.subcat]=(buckets[bk].byCategory[cls.subcat]||0)+amt;
      }
    });
    const periods=Object.values(buckets).sort((a,b)=>a.period.localeCompare(b.period));
    periods.forEach(p=>{p.netIncome=p.revenue-p.expense;p.margin=p.revenue>0?(p.netIncome/p.revenue*100):0;});

    // All-time category totals
    const catTotals={};
    txns.forEach(t=>{
      const cls=classifyAccount(t.account);
      if(cls.category==="other") return;
      if(!catTotals[cls.subcat]) catTotals[cls.subcat]={subcat:cls.subcat,category:cls.category,total:0,count:0};
      const amt=cls.category==="revenue"?(t.credit-t.debit):(t.debit-t.credit);
      catTotals[cls.subcat].total+=amt;
      catTotals[cls.subcat].count++;
    });
    const expCats=Object.values(catTotals).filter(c=>c.category==="expense").sort((a,b)=>b.total-a.total);
    const revCats=Object.values(catTotals).filter(c=>c.category==="revenue").sort((a,b)=>b.total-a.total);

    // Vendor spending (from txn names on expense accounts)
    const byVendor={};
    txns.forEach(t=>{
      const cls=classifyAccount(t.account);
      if(cls.category!=="expense"||!t.name) return;
      const vendor=t.name.trim();
      if(!byVendor[vendor]) byVendor[vendor]={vendor,total:0,count:0};
      byVendor[vendor].total+=(t.debit-t.credit);
      byVendor[vendor].count++;
    });
    const topVendors=Object.values(byVendor).filter(v=>v.total>0).sort((a,b)=>b.total-a.total).slice(0,50);

    // Customer revenue (from txn names on revenue accounts)
    const byCust={};
    txns.forEach(t=>{
      const cls=classifyAccount(t.account);
      if(cls.category!=="revenue"||!t.name) return;
      const cust=t.name.trim();
      if(!byCust[cust]) byCust[cust]={customer:cust,revenue:0,count:0};
      byCust[cust].revenue+=(t.credit-t.debit);
      byCust[cust].count++;
    });
    const topCustomers=Object.values(byCust).filter(c=>c.revenue>0).sort((a,b)=>b.revenue-a.revenue).slice(0,50);

    const totalRev=periods.reduce((s,p)=>s+p.revenue,0);
    const totalExp=periods.reduce((s,p)=>s+p.expense,0);

    // Category time-series for drill-down
    const catTimeSeries={};
    txns.forEach(t=>{
      const cls=classifyAccount(t.account);
      if(cls.category==="other") return;
      const bk=bucketKey(t);
      if(!catTimeSeries[cls.subcat]) catTimeSeries[cls.subcat]={};
      if(!catTimeSeries[cls.subcat][bk]) catTimeSeries[cls.subcat][bk]=0;
      const amt=cls.category==="revenue"?(t.credit-t.debit):(t.debit-t.credit);
      catTimeSeries[cls.subcat][bk]+=amt;
    });

    return{periods,expCats,revCats,topVendors,topCustomers,totalRev,totalExp,netIncome:totalRev-totalExp,catTimeSeries};
  },[glData,period]);

  // Build the UI
  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📁" text="QuickBooks Analyzer" right={glData?<Badge text={`GL: ${fmtNum(glData.txnCount)} txns`} color={T.greenText} bg={T.greenBg}/>:<Badge text="No GL loaded" color={T.textDim} bg={T.borderLight}/>}/>

      {result&&<div style={{...CS,borderLeft:`4px solid ${result.type==="error"?T.red:result.type==="dup"?T.yellow:T.green}`,marginBottom:16}}>
        {result.type==="error"&&<div style={{color:T.redText,fontSize:13}}>❌ {result.message}</div>}
        {result.type==="dup"&&<div style={{color:T.yellowText,fontSize:13}}>⚠️ {result.filename} already imported — skipped</div>}
        {result.type==="success"&&result.reportType==="gl"&&<div style={{color:T.greenText,fontSize:13}}>✅ GL imported: {fmtNum(result.txnCount)} transactions, {result.dateStart} → {result.dateEnd}</div>}
        {result.type==="success"&&result.reportType==="pnl"&&<div style={{color:T.greenText,fontSize:13}}>✅ P&L: Revenue {fmtK(result.totalRevenue)}, Expenses {fmtK(result.totalExpenses)}, Net {fmtK(result.netIncome)}, Margin {fmtPct(result.margin)}</div>}
        {result.type==="success"&&result.reportType==="bs"&&<div style={{color:T.greenText,fontSize:13}}>✅ Balance Sheet: Assets {fmtK(result.totalAssets)}, Liab {fmtK(result.totalLiabilities)}, Equity {fmtK(result.totalEquity)}</div>}
        {result.type==="success"&&result.reportType==="customers"&&<div style={{color:T.greenText,fontSize:13}}>✅ {result.customers?.length} customers, {fmtK(result.totalRevenue)} total revenue</div>}
        {result.type==="success"&&result.reportType==="payroll"&&<div style={{color:T.greenText,fontSize:13}}>✅ {result.headcount} employees, {fmtK(result.totalGross)} gross pay</div>}
      </div>}

      <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
        <TabBtn active={view==="upload"} label="📤 Upload" onClick={()=>setView("upload")}/>
        {glData&&<TabBtn active={view==="analyzer"} label="📊 Analyzer" onClick={()=>setView("analyzer")}/>}
        {glData&&<TabBtn active={view==="trends"} label="📈 Trends" onClick={()=>setView("trends")}/>}
        {glData&&<TabBtn active={view==="categories"} label="🏷️ Categories" onClick={()=>setView("categories")}/>}
        {glData&&<TabBtn active={view==="vendors"} label="🏭 Vendors" onClick={()=>setView("vendors")}/>}
        {glData&&<TabBtn active={view==="customers"} label="🏢 Customers" onClick={()=>setView("customers")}/>}
        {qboFiles.length>0?<TabBtn active={view==="history"} label={`📂 History (${qboFiles.length})`} onClick={()=>setView("history")}/>:<TabBtn active={view==="history"} label="📂 History" onClick={()=>setView("history")}/>}
      </div>

      {view==="upload"&&(
        <div>
          <div style={{...CS,borderLeft:`4px solid ${T.brand}`,background:T.brandPale,marginBottom:16}}>
            <div style={{fontSize:13,fontWeight:700,color:T.brand,marginBottom:4}}>📂 Upload General Ledger for Full Analysis</div>
            <div style={{fontSize:12,color:T.textMuted}}>The GL contains every transaction. MarginIQ auto-generates P&L by month, quarter, and year — no need to export separately.</div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(260px,1fr))",gap:"12px"}}>
            <div style={{...CS,border:`2px solid ${T.brand}`}}>
              <div style={{fontSize:13,fontWeight:700,marginBottom:4}}>📒 General Ledger <Badge text="PRIMARY" color={T.brand} bg={T.brandPale}/></div>
              <div style={{fontSize:11,color:T.textMuted,marginBottom:12}}>Reports → Accountant → General Ledger. Last 12+ months.</div>
              <input ref={glRef} type="file" accept=".xlsx,.xls,.csv" onChange={handleGL} style={{display:"none"}}/>
              <PrimaryBtn text={importing?"Importing...":"Upload GL"} onClick={()=>glRef.current?.click()} loading={importing} style={{width:"100%",fontSize:12}}/>
            </div>
            {[{type:"pnl",icon:"📊",title:"Profit & Loss",sub:"Summary totals",ref:pnlRef},
              {type:"bs",icon:"⚖️",title:"Balance Sheet",sub:"Assets/liabilities snapshot",ref:bsRef},
              {type:"customers",icon:"🏢",title:"Sales by Customer",sub:"Revenue per customer",ref:custRef},
              {type:"payroll",icon:"💵",title:"Payroll Summary",sub:"Pay per employee",ref:payRef},
            ].map(item=>(
              <div key={item.type} style={CS}>
                <div style={{fontSize:13,fontWeight:700,marginBottom:4}}>{item.icon} {item.title}</div>
                <div style={{fontSize:11,color:T.textMuted,marginBottom:12}}>{item.sub}</div>
                <input ref={item.ref} type="file" accept=".xlsx,.xls,.csv" onChange={e=>handleSimpleReport(e,item.type)} style={{display:"none"}}/>
                <PrimaryBtn text={importing?"Importing...":`Upload ${item.title}`} onClick={()=>item.ref.current?.click()} loading={importing} style={{width:"100%",fontSize:12}}/>
              </div>
            ))}
          </div>
        </div>
      )}

      {view==="analyzer"&&analysis&&(
        <div>
          {/* Period selector */}
          <div style={{...CS,marginBottom:16}}>
            <div style={{fontSize:12,color:T.textMuted,marginBottom:8}}>Break out by:</div>
            <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
              {[["month","Monthly"],["quarter","Quarterly"],["year","Yearly"]].map(([k,l])=><TabBtn key={k} active={period===k} label={l} onClick={()=>setPeriod(k)}/>)}
            </div>
          </div>
          {/* All-time KPIs */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Total Revenue" value={fmtK(analysis.totalRev)} sub={`${analysis.periods.length} ${period}s`} subColor={T.green}/>
            <KPI label="Total Expenses" value={fmtK(analysis.totalExp)} subColor={T.red}/>
            <KPI label="Net Income" value={fmtK(analysis.netIncome)} sub={fmtPct(analysis.totalRev>0?(analysis.netIncome/analysis.totalRev*100):0)+" margin"} subColor={analysis.netIncome>0?T.green:T.red}/>
            <KPI label="Transactions" value={fmtNum(glData.txnCount)}/>
          </div>
          {/* P&L by period table */}
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>P&L by {period==="month"?"Month":period==="quarter"?"Quarter":"Year"}</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:600}}>
              <thead><tr>{["Period","Revenue","Expenses","Net Income","Margin %"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead>
              <tbody>{analysis.periods.map((p,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{p.period}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(p.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(p.expense)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:p.netIncome>0?T.green:T.red}}>{fmt(p.netIncome)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={fmtPct(p.margin)} color={p.margin>=30?T.greenText:p.margin>=15?T.yellowText:T.redText} bg={p.margin>=30?T.greenBg:p.margin>=15?T.yellowBg:T.redBg}/></td></tr>
              ))}</tbody>
            </table></div>
          </div>
          {/* Chart */}
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Revenue by {period==="month"?"Month":period==="quarter"?"Quarter":"Year"}</div>
            <BarChart data={analysis.periods} labelKey="period" valueKey="revenue" color={T.green} maxBars={Math.min(analysis.periods.length,50)}/>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Net Income by {period==="month"?"Month":period==="quarter"?"Quarter":"Year"}</div>
            <BarChart data={analysis.periods} labelKey="period" valueKey="netIncome" color={T.brand} maxBars={Math.min(analysis.periods.length,50)}/>
          </div>
        </div>
      )}

      {view==="categories"&&analysis&&(
        <div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Expense Categories (All-Time)</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:500}}>
              <thead><tr>{["Category","Total Spend","% of Expenses","Transactions","Drill"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{analysis.expCats.map((c,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.subcat}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:600}}>{fmt(c.total)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(analysis.totalExp>0?c.total/analysis.totalExp*100:0)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textMuted}}>{c.count}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><button onClick={()=>setSelectedCat(c.subcat)} style={{padding:"3px 10px",fontSize:10,borderRadius:6,border:`1px solid ${T.brand}`,background:"transparent",color:T.brand,cursor:"pointer"}}>View trend →</button></td></tr>
              ))}</tbody>
            </table></div>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Expense Distribution</div>
            <BarChart data={analysis.expCats.slice(0,20)} labelKey="subcat" valueKey="total" color={T.red} maxBars={20}/>
          </div>
          {selectedCat&&analysis.catTimeSeries[selectedCat]&&(
            <div style={CS}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
                <div style={{fontSize:13,fontWeight:700}}>📈 {selectedCat} — Trend by {period==="month"?"Month":period==="quarter"?"Quarter":"Year"}</div>
                <button onClick={()=>setSelectedCat(null)} style={{padding:"3px 10px",fontSize:10,borderRadius:6,border:`1px solid ${T.border}`,background:"transparent",color:T.textMuted,cursor:"pointer"}}>Close</button>
              </div>
              <BarChart data={Object.entries(analysis.catTimeSeries[selectedCat]).map(([period,value])=>({period,value})).sort((a,b)=>a.period.localeCompare(b.period))} labelKey="period" valueKey="value" color={T.orange} maxBars={50}/>
            </div>
          )}
        </div>
      )}

      {view==="trends"&&analysis&&(
        <div>
          <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
            {[["month","Monthly"],["quarter","Quarterly"],["year","Yearly"]].map(([k,l])=><TabBtn key={k} active={period===k} label={l} onClick={()=>setPeriod(k)}/>)}
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}>
            <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Revenue Trend</div><BarChart data={analysis.periods} labelKey="period" valueKey="revenue" color={T.green} maxBars={50}/></div>
            <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Expense Trend</div><BarChart data={analysis.periods} labelKey="period" valueKey="expense" color={T.red} maxBars={50}/></div>
            <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Net Income Trend</div><BarChart data={analysis.periods} labelKey="period" valueKey="netIncome" color={T.brand} maxBars={50}/></div>
            <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Margin % Trend</div><BarChart data={analysis.periods} labelKey="period" valueKey="margin" color={T.purple} formatValue={v=>fmtPct(v)} maxBars={50}/></div>
          </div>
        </div>
      )}

      {view==="vendors"&&analysis&&(
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Top Vendors by Spend ({analysis.topVendors.length})</div>
          <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:400}}>
            <thead><tr>{["#","Vendor","Total Spend","Transactions"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
            <tbody>{analysis.topVendors.map((v,i)=>(
              <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textDim}}>{i+1}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{v.vendor}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:600}}>{fmt(v.total)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{v.count}</td></tr>
            ))}</tbody>
          </table></div>
        </div>
      )}

      {view==="customers"&&analysis&&(
        <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Top Customers by Revenue ({analysis.topCustomers.length})</div>
          <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:400}}>
            <thead><tr>{["#","Customer","Revenue","% of Total","Invoices"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
            <tbody>{analysis.topCustomers.map((c,i)=>(
              <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.textDim}}>{i+1}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{c.customer}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(c.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtPct(analysis.totalRev>0?c.revenue/analysis.totalRev*100:0)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{c.count}</td></tr>
            ))}</tbody>
          </table></div>
        </div>
      )}

      {view==="history"&&(
        <div style={CS}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
            <div style={{fontSize:13,fontWeight:700}}>Import History ({qboFiles.length})</div>
            <div style={{fontSize:11,color:T.textMuted}}>Click 🗑️ to delete an import and its data</div>
          </div>
          <div style={{maxHeight:500,overflowY:"auto"}}>{qboFiles.map((f,i)=>(
            <div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"12px",borderBottom:`1px solid ${T.borderLight}`,background:T.bgSurface,borderRadius:8,marginBottom:6}}>
              <div style={{flex:1,minWidth:0}}>
                <div style={{fontSize:12,fontWeight:600,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{f.filename}</div>
                <div style={{fontSize:10,color:T.textMuted,marginTop:2}}>
                  <Badge text={f.type==="gl"?"General Ledger":f.type==="pnl"?"P&L":f.type==="bs"?"Balance Sheet":f.type==="customers"?"Customers":f.type==="payroll"?"Payroll":f.type} color={T.blueText} bg={T.blueBg}/>
                  {" "}• {new Date(f.imported_at).toLocaleDateString()} {new Date(f.imported_at).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})}
                  {f.txnCount&&` • ${fmtNum(f.txnCount)} transactions`}
                  {f.totalRevenue&&` • ${fmtK(f.totalRevenue)} rev`}
                </div>
              </div>
              <button onClick={async()=>{
                if(!confirm(`Delete "${f.filename}"? This permanently removes the import and all its data from MarginIQ. This cannot be undone.`)) return;
                try{
                  if(hasFirebase) await window.db.collection("qbo_imports").doc(f.id).delete();
                  setQboFiles(prev=>prev.filter(x=>x.id!==f.id));
                  if(f.type==="gl"&&glData?.id===f.id) setGlData(null);
                  setResult({type:"success",reportType:"delete",filename:f.filename,message:`Deleted ${f.filename}`});
                }catch(err){alert("Delete failed: "+err.message);}
              }} title="Delete this import" style={{marginLeft:8,padding:"6px 10px",borderRadius:6,border:`1px solid ${T.red}`,background:"transparent",color:T.red,fontSize:14,cursor:"pointer",fontFamily:"inherit",flexShrink:0}}>🗑️</button>
            </div>
          ))}</div>
          {qboFiles.length===0&&<div style={{textAlign:"center",padding:30,color:T.textMuted,fontSize:13}}>No imports yet. Upload files from the 📤 Upload tab to get started.</div>}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PAYROLL OVERLAY (CyberPay + QBO payroll vs driver revenue)
// ═══════════════════════════════════════════════════════════════
function PayrollTab({ulineWeeks,margins}){
  const [payrollData,setPayrollData]=useState([]);
  const [qboPayroll,setQboPayroll]=useState([]);
  const [b600Data,setB600Data]=useState([]);
  const [loading,setLoading]=useState(true);

  useEffect(()=>{(async()=>{
    setLoading(true);
    if(hasFirebase){
      try{const s=await window.db.collection("payroll_runs").orderBy("check_date","desc").limit(20).get();setPayrollData(s.docs.map(d=>({id:d.id,...d.data()})));}catch(e){}
      try{const s=await window.db.collection("qbo_imports").where("type","==","payroll").limit(10).get();setQboPayroll(s.docs.map(d=>d.data()));}catch(e){}
      try{const s=await window.db.collection("b600_timeclock").orderBy("date","desc").limit(500).get();setB600Data(s.docs.map(d=>d.data()));}catch(e){}
    }
    setLoading(false);
  })();},[]);

  // Merge B600 hours by driver
  const driverHours={};
  b600Data.forEach(r=>{if(!driverHours[r.name])driverHours[r.name]={name:r.name,totalHours:0,days:0,records:[]};driverHours[r.name].totalHours+=r.hours||0;driverHours[r.name].days++;driverHours[r.name].records.push(r);});
  const driverArr=Object.values(driverHours).sort((a,b)=>b.totalHours-a.totalHours);

  // Estimate revenue per driver from Uline data
  const allStops=ulineWeeks.flatMap(w=>w.stops||[]);
  const totalUlineRev=ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0);
  const totalUlineStops=ulineWeeks.reduce((s,w)=>s+(w.totalStops||0),0);
  const weekCount=ulineWeeks.length||1;

  // QBO payroll employees
  const qboEmployees=qboPayroll.flatMap(p=>p.employees||[]);

  // Cross-reference: if we have QBO payroll employees AND B600 hours, overlay them
  const overlay=driverArr.map(d=>{
    const qboMatch=qboEmployees.find(e=>e.employee?.toLowerCase().includes(d.name.split(" ")[0]?.toLowerCase()));
    const avgHoursPerDay=d.days>0?d.totalHours/d.days:0;
    const weeklyHours=avgHoursPerDay*5;
    const estWeeklyPay=weeklyHours*(margins.totalDrivers>0?(margins.totalAnnualLabor/margins.totalDrivers/52/weeklyHours||0):0);
    const estStopsPerDay=margins.stopsPerDriver;
    const estDailyRevenue=estStopsPerDay*margins.revenuePerStop;
    const estDailyCost=avgHoursPerDay*(d.name.toLowerCase().match(/tractor/)?margins.costPerDriver/(margins.totalDrivers||1):23);
    return{...d,avgHoursPerDay,weeklyHours,qboPay:qboMatch?.grossPay||0,estDailyRevenue,estDailyCost,roi:estDailyRevenue>0&&estDailyCost>0?((estDailyRevenue-estDailyCost)/estDailyCost*100):0};
  });

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="💵" text="Payroll & Driver Economics"/>
      {loading?<EmptyState icon="💵" title="Loading payroll data..." sub="Pulling from Firebase"/>:(
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}>
            <KPI label="CyberPay Runs" value={fmtNum(payrollData.length)} sub="on file"/>
            <KPI label="QBO Payroll" value={qboPayroll.length>0?fmtNum(qboEmployees.length)+" employees":"Not imported"} sub={qboPayroll.length>0?fmtK(qboEmployees.reduce((s,e)=>s+e.grossPay,0))+" gross":"Upload in QB Import"}/>
            <KPI label="B600 Drivers" value={fmtNum(driverArr.length)} sub={`${fmtNum(b600Data.length)} clock records`}/>
            <KPI label="Avg Hours/Day" value={driverArr.length>0?fmtDec(driverArr.reduce((s,d)=>s+d.avgHoursPerDay,0)/driverArr.length,1):"—"} sub="from B600"/>
          </div>

          {/* Driver hours from B600 */}
          {driverArr.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Driver Hours (B600 Time Clock)</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:600}}>
              <thead><tr>{["Driver","Total Hours","Days","Avg Hrs/Day","Weekly Est","OT Risk"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{overlay.map((d,i)=>{const ot=d.weeklyHours>40;return(
                <tr key={i} style={{background:ot?T.redBg:"transparent"}}>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{d.name}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtDec(d.totalHours,1)}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{d.days}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{fmtDec(d.avgHoursPerDay,1)}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtDec(d.weeklyHours,0)}</td>
                  <td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{ot?<Badge text="⚠️ OT" color={T.redText} bg={T.redBg}/>:<Badge text="OK" color={T.greenText} bg={T.greenBg}/>}</td>
                </tr>
              );})}</tbody>
            </table></div>
          </div>}

          {/* QBO Payroll overlay */}
          {qboEmployees.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>QBO Payroll Data</div>
            <BarChart data={qboEmployees.slice(0,15)} labelKey="employee" valueKey="grossPay" color={T.purple} maxBars={15}/>
          </div>}

          {/* CyberPay runs */}
          {payrollData.length>0&&<div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>CyberPay Payroll Runs</div>
            <div style={{maxHeight:200,overflowY:"auto"}}>{payrollData.map((p,i)=>(
              <div key={i} style={{display:"flex",justifyContent:"space-between",padding:"6px 0",borderBottom:`1px solid ${T.borderLight}`,fontSize:12}}>
                <span style={{fontWeight:500}}>Check: {p.check_date}</span>
                <span style={{color:T.textMuted}}>Period: {p.from_date} → {p.to_date}</span>
                <span>{p.run_id}</span>
              </div>
            ))}</div>
          </div>}

          {driverArr.length===0&&qboEmployees.length===0&&payrollData.length===0&&<EmptyState icon="💵" title="No Payroll Data" sub="Upload B600 time clock CSV, import QBO payroll report, or wait for CyberPay Monday auto-pull"/>}
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MOTIVE MILEAGE & FUEL COST
// ═══════════════════════════════════════════════════════════════
function MileageTab({margins,costs}){
  const [vehicles,setVehicles]=useState(null);
  const [trips,setTrips]=useState(null);
  const [loading,setLoading]=useState(false);
  const [from,setFrom]=useState(()=>{const d=new Date();d.setDate(d.getDate()-7);return d.toISOString().slice(0,10);});
  const [to,setTo]=useState(()=>new Date().toISOString().slice(0,10));

  const loadData=async()=>{
    setLoading(true);
    const [vRes,tRes]=await Promise.all([
      fetchMotive("vehicles"),
      fetchMotive("ifta_trips",`&start=${from}&end=${to}`),
    ]);
    if(vRes?.vehicles||vRes?.data) setVehicles(vRes.vehicles||vRes.data||[]);
    if(tRes) setTrips(tRes);
    setLoading(false);
  };

  useEffect(()=>{loadData();},[]);

  // Process trip data
  const tripData=useMemo(()=>{
    if(!trips) return null;
    const rawTrips=trips.ifta_trips||trips.trips||trips.data||[];
    if(!Array.isArray(rawTrips)||rawTrips.length===0) return null;
    const byVehicle={};
    rawTrips.forEach(t=>{
      const trip=t.ifta_trip||t.trip||t;
      const vNum=trip.vehicle?.number||trip.vehicle_number||"?";
      const miles=parseFloat(trip.distance)||parseFloat(trip.odometer_distance)||0;
      const fuelGal=parseFloat(trip.fuel_consumption)||0;
      if(!byVehicle[vNum])byVehicle[vNum]={vehicle:vNum,totalMiles:0,totalFuel:0,tripCount:0,type:trip.vehicle?.vehicle_type||"box"};
      byVehicle[vNum].totalMiles+=miles;
      byVehicle[vNum].totalFuel+=fuelGal;
      byVehicle[vNum].tripCount++;
    });
    const vArr=Object.values(byVehicle).map(v=>({
      ...v,
      mpg:v.totalFuel>0?(v.totalMiles/v.totalFuel):0,
      fuelCost:v.totalFuel*(costs.fuel_price||3.50),
      costPerMile:v.totalMiles>0?(v.totalFuel*(costs.fuel_price||3.50)/v.totalMiles):0,
    })).sort((a,b)=>b.totalMiles-a.totalMiles);
    const totalMiles=vArr.reduce((s,v)=>s+v.totalMiles,0);
    const totalFuel=vArr.reduce((s,v)=>s+v.totalFuel,0);
    const totalFuelCost=totalFuel*(costs.fuel_price||3.50);
    return{byVehicle:vArr,totalMiles,totalFuel,totalFuelCost,avgMPG:totalFuel>0?totalMiles/totalFuel:0,tripCount:rawTrips.length};
  },[trips,costs]);

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="⛽" text="Mileage & Fuel Cost"/>
      <div style={CS}>
        <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
          <input type="date" value={from} onChange={e=>setFrom(e.target.value)} style={{...IS,width:145,fontSize:12}}/>
          <span style={{color:T.textDim}}>→</span>
          <input type="date" value={to} onChange={e=>setTo(e.target.value)} style={{...IS,width:145,fontSize:12}}/>
          <PrimaryBtn text={loading?"Loading...":"Pull Motive Data"} onClick={loadData} loading={loading}/>
        </div>
      </div>
      {tripData?(
        <>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"16px"}}>
            <KPI label="Total Miles" value={fmtNum(Math.round(tripData.totalMiles))} sub={`${tripData.tripCount} trips`}/>
            <KPI label="Total Fuel" value={`${fmtNum(Math.round(tripData.totalFuel))} gal`}/>
            <KPI label="Fuel Cost" value={fmt(tripData.totalFuelCost)} sub={`$${costs.fuel_price||3.50}/gal`} subColor={T.red}/>
            <KPI label="Fleet MPG" value={fmtDec(tripData.avgMPG,1)} sub="actual average"/>
            <KPI label="Cost/Mile" value={`$${fmtDec(tripData.totalMiles>0?tripData.totalFuelCost/tripData.totalMiles:0,2)}`}/>
            <KPI label="Vehicles Active" value={fmtNum(tripData.byVehicle.length)}/>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Miles by Vehicle</div>
            <BarChart data={tripData.byVehicle.slice(0,20)} labelKey="vehicle" valueKey="totalMiles" formatValue={v=>fmtNum(Math.round(v))+" mi"} maxBars={20}/>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Fuel Cost by Vehicle</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:500}}>
              <thead><tr>{["Vehicle","Miles","Fuel (gal)","Cost","MPG","$/Mile"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{tripData.byVehicle.map((v,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{v.vehicle}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(Math.round(v.totalMiles))}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(Math.round(v.totalFuel))}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red,fontWeight:600}}>{fmt(v.fuelCost)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtDec(v.mpg,1)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>${fmtDec(v.costPerMile,2)}</td></tr>
              ))}</tbody>
            </table></div>
          </div>
        </>
      ):(!loading&&<EmptyState icon="⛽" title="Pull Mileage Data" sub="Select a date range and pull from Motive to see actual miles and fuel cost per vehicle"/>)}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// TREND CHARTS (Week-over-week analysis)
// ═══════════════════════════════════════════════════════════════
function TrendsTab({ulineWeeks,margins,qbFinancials}){
  const [tab,setT]=useState(qbFinancials?"qb":"uline");
  const [qbPeriod,setQbPeriod]=useState("quarter");
  // Build weekly trends from Uline data (sorted chronologically)
  const sorted=[...ulineWeeks].sort((a,b)=>(a.upload_date||a.id||"").localeCompare(b.upload_date||b.id||""));
  const weeklyData=sorted.map((w,i)=>({
    label:w.filename?.replace(/\.xlsx?$/i,"").slice(-12)||`Week ${i+1}`,
    revenue:w.totalRevenue||0,
    stops:w.totalStops||0,
    avgPerStop:w.avgRevenuePerStop||0,
    contractorPay:w.contractorPayout||0,
    netRevenue:(w.totalRevenue||0)-(w.contractorPayout||0),
    weight:w.totalWeight||0,
    costEst:(w.totalStops||0)*margins.costPerStop,
    marginEst:(w.totalRevenue||0)-((w.totalStops||0)*margins.costPerStop),
    marginPctEst:w.totalRevenue>0?((w.totalRevenue-((w.totalStops||0)*margins.costPerStop))/w.totalRevenue*100):0,
  }));

  // Calculate week-over-week changes
  const wow=weeklyData.length>=2?{
    revChange:weeklyData[weeklyData.length-1].revenue-weeklyData[weeklyData.length-2].revenue,
    revPctChange:weeklyData[weeklyData.length-2].revenue>0?((weeklyData[weeklyData.length-1].revenue-weeklyData[weeklyData.length-2].revenue)/weeklyData[weeklyData.length-2].revenue*100):0,
    stopChange:weeklyData[weeklyData.length-1].stops-weeklyData[weeklyData.length-2].stops,
    avgChange:weeklyData[weeklyData.length-1].avgPerStop-weeklyData[weeklyData.length-2].avgPerStop,
  }:null;

  // SVG line chart
  function TrendLine({data,valueKey,color,height=120,label}){
    if(data.length<2) return null;
    const vals=data.map(d=>d[valueKey]);
    const min=Math.min(...vals);const max=Math.max(...vals);const range=max-min||1;
    const w=300;const h=height;const padY=10;
    const points=vals.map((v,i)=>({x:(i/(vals.length-1))*w,y:h-padY-((v-min)/range)*(h-padY*2)}));
    const path=points.map((p,i)=>i===0?`M ${p.x} ${p.y}`:`L ${p.x} ${p.y}`).join(" ");
    return(
      <div style={{marginBottom:16}}>
        <div style={{fontSize:12,fontWeight:700,marginBottom:6}}>{label}</div>
        <svg viewBox={`-10 0 ${w+20} ${h}`} style={{width:"100%",maxWidth:400,height}}>
          <path d={path} fill="none" stroke={color} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
          {points.map((p,i)=><circle key={i} cx={p.x} cy={p.y} r="3" fill={color}/>)}
          {/* Labels on first and last */}
          <text x={points[0].x} y={points[0].y-8} textAnchor="start" fontSize="10" fill={T.textMuted}>{valueKey.includes("Pct")||valueKey.includes("margin")?fmtPct(vals[0]):valueKey.includes("stop")?fmtNum(vals[0]):fmtK(vals[0])}</text>
          <text x={points[points.length-1].x} y={points[points.length-1].y-8} textAnchor="end" fontSize="10" fill={color} fontWeight="700">{valueKey.includes("Pct")||valueKey.includes("margin")?fmtPct(vals[vals.length-1]):valueKey.includes("stop")?fmtNum(vals[vals.length-1]):fmtK(vals[vals.length-1])}</text>
        </svg>
      </div>
    );
  }

  // Get QB trend data based on selected period
  const qbTrendData=qbFinancials?(qbPeriod==="month"?qbFinancials.monthlyPnL:qbPeriod==="quarter"?qbFinancials.quarterlyPnL:qbFinancials.yearlyPnL):[];

  return(
    <div style={{padding:"16px",maxWidth:1200,margin:"0 auto"}} className="fade-in">
      <SectionTitle icon="📈" text="Trends & Performance Over Time"/>

      {/* Source tabs */}
      <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
        {qbFinancials&&<TabBtn active={tab==="qb"} label="📁 QuickBooks P&L Trends" onClick={()=>setT("qb")}/>}
        <TabBtn active={tab==="uline"} label="📦 Uline Weekly Trends" onClick={()=>setT("uline")}/>
      </div>

      {/* QB TRENDS VIEW */}
      {tab==="qb"&&qbFinancials&&qbTrendData.length>0&&(
        <>
          <div style={{display:"flex",gap:6,marginBottom:12}}>
            {[["month","Monthly"],["quarter","Quarterly"],["year","Yearly"]].map(([k,l])=><TabBtn key={k} active={qbPeriod===k} label={l} onClick={()=>setQbPeriod(k)}/>)}
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:"10px",marginBottom:"14px"}}>
            <KPI label="Periods" value={fmtNum(qbTrendData.length)} sub={qbPeriod+"s tracked"}/>
            <KPI label="Avg Revenue" value={fmtK(qbTrendData.reduce((s,p)=>s+p.revenue,0)/qbTrendData.length)} sub={`per ${qbPeriod}`} subColor={T.green}/>
            <KPI label="Avg Expenses" value={fmtK(qbTrendData.reduce((s,p)=>s+p.expense,0)/qbTrendData.length)} sub={`per ${qbPeriod}`} subColor={T.red}/>
            <KPI label="Avg Net Income" value={fmtK(qbTrendData.reduce((s,p)=>s+p.netIncome,0)/qbTrendData.length)} subColor={T.brand}/>
            {qbTrendData.length>=2&&<KPI label="Latest Change" value={(()=>{const a=qbTrendData[qbTrendData.length-1].netIncome;const b=qbTrendData[qbTrendData.length-2].netIncome;return(a-b>=0?"+":"")+fmtK(a-b);})()} sub="vs prior period" subColor={qbTrendData[qbTrendData.length-1].netIncome-qbTrendData[qbTrendData.length-2].netIncome>=0?T.green:T.red}/>}
          </div>

          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px",marginBottom:12}}>
            <div style={CS}><TrendLine data={qbTrendData.map(p=>({...p,label:p.period}))} valueKey="revenue" color={T.green} label={`Revenue by ${qbPeriod}`}/></div>
            <div style={CS}><TrendLine data={qbTrendData.map(p=>({...p,label:p.period}))} valueKey="expense" color={T.red} label={`Expenses by ${qbPeriod}`}/></div>
            <div style={CS}><TrendLine data={qbTrendData.map(p=>({...p,label:p.period}))} valueKey="netIncome" color={T.brand} label={`Net Income by ${qbPeriod}`}/></div>
            <div style={CS}><TrendLine data={qbTrendData.map(p=>({...p,label:p.period}))} valueKey="margin" color={T.purple} label={`Margin % by ${qbPeriod}`}/></div>
          </div>

          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>P&L by {qbPeriod==="month"?"Month":qbPeriod==="quarter"?"Quarter":"Year"}</div>
            <div style={{overflowX:"auto",maxHeight:500,overflowY:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:600}}>
              <thead><tr>{["Period","Revenue","Expenses","Net Income","Margin %"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase",position:"sticky",top:0,background:T.bgWhite}}>{h}</th>)}</tr></thead>
              <tbody>{qbTrendData.map((p,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{p.period}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(p.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(p.expense)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:p.netIncome>0?T.green:T.red}}>{fmt(p.netIncome)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={fmtPct(p.margin)} color={p.margin>=15?T.greenText:p.margin>=5?T.yellowText:T.redText} bg={p.margin>=15?T.greenBg:p.margin>=5?T.yellowBg:T.redBg}/></td></tr>
              ))}</tbody>
            </table></div>
          </div>
        </>
      )}

      {tab==="qb"&&(!qbFinancials||qbTrendData.length===0)&&<EmptyState icon="📈" title="No QB Trend Data" sub="Upload General Ledger in QB Import tab to see monthly/quarterly/yearly trends"/>}

      {/* ULINE TRENDS VIEW */}
      {tab==="uline"&&(weeklyData.length<2?<EmptyState icon="📈" title="Need More Data" sub="Upload at least 2 Uline weekly files to see trends"/>:(
        <>
          {wow&&<div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(150px,1fr))",gap:"10px",marginBottom:"16px"}}>
            <KPI label="Revenue Change" value={(wow.revChange>=0?"+":"")+fmtK(wow.revChange)} sub={`${wow.revPctChange>=0?"+":""}${fmtDec(wow.revPctChange,1)}% WoW`} subColor={wow.revChange>=0?T.green:T.red}/>
            <KPI label="Stop Change" value={(wow.stopChange>=0?"+":"")+fmtNum(wow.stopChange)} sub="vs last week" subColor={wow.stopChange>=0?T.green:T.red}/>
            <KPI label="Avg/Stop Change" value={(wow.avgChange>=0?"+$":"-$")+fmtDec(Math.abs(wow.avgChange),2)} sub="rate movement" subColor={wow.avgChange>=0?T.green:T.red}/>
            <KPI label="Weeks Tracked" value={fmtNum(weeklyData.length)}/>
          </div>}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:"12px"}}>
            <div style={CS}><TrendLine data={weeklyData} valueKey="revenue" color={T.green} label="Weekly Revenue Trend"/></div>
            <div style={CS}><TrendLine data={weeklyData} valueKey="stops" color={T.brand} label="Weekly Stop Count Trend"/></div>
            <div style={CS}><TrendLine data={weeklyData} valueKey="avgPerStop" color={T.purple} label="Average Revenue Per Stop"/></div>
            <div style={CS}><TrendLine data={weeklyData} valueKey="marginEst" color={weeklyData[weeklyData.length-1]?.marginEst>0?T.green:T.red} label="Estimated Weekly Margin"/></div>
          </div>
          <div style={CS}><div style={{fontSize:13,fontWeight:700,marginBottom:10}}>Weekly Detail</div>
            <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12,minWidth:600}}>
              <thead><tr>{["Week","Revenue","Stops","Avg/Stop","Contractor","Net Rev","Est Margin","Margin %"].map(h=><th key={h} style={{textAlign:"left",padding:"8px",borderBottom:`1px solid ${T.border}`,color:T.textDim,fontSize:10,fontWeight:600,textTransform:"uppercase"}}>{h}</th>)}</tr></thead>
              <tbody>{weeklyData.map((w,i)=>(
                <tr key={i}><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:500,maxWidth:120,overflow:"hidden",textOverflow:"ellipsis"}}>{w.label}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.green,fontWeight:600}}>{fmt(w.revenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmtNum(w.stops)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}>{fmt(w.avgPerStop)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,color:T.red}}>{fmt(w.contractorPay)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:600}}>{fmt(w.netRevenue)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`,fontWeight:700,color:w.marginEst>0?T.green:T.red}}>{fmt(w.marginEst)}</td><td style={{padding:"8px",borderBottom:`1px solid ${T.borderLight}`}}><Badge text={fmtPct(w.marginPctEst)} color={w.marginPctEst>=30?T.greenText:w.marginPctEst>=15?T.yellowText:T.redText} bg={w.marginPctEst>=30?T.greenBg:w.marginPctEst>=15?T.yellowBg:T.redBg}/></td></tr>
              ))}</tbody>
            </table></div>
          </div>
        </>
      ))}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// LOGIN SCREEN
// ═══════════════════════════════════════════════════════════════
function LoginScreen({onLogin}){
  const [email,setEmail]=useState("");const [password,setPassword]=useState("");
  const [loading,setLoading]=useState(false);const [error,setError]=useState("");
  const submit=async()=>{
    if(!email||!password){setError("Enter email and password");return;}
    setLoading(true);setError("");
    try{
      if(!window.fbAuth){setError("Firebase Auth not loaded. Refresh the page.");setLoading(false);return;}
      await window.fbAuth.signInWithEmailAndPassword(email,password);
      // onAuthStateChanged in parent will handle the rest
    }catch(e){
      const code=e.code||"";
      if(code.includes("user-not-found")||code.includes("wrong-password")||code.includes("invalid-credential")) setError("Invalid email or password");
      else if(code.includes("too-many-requests")) setError("Too many attempts. Try again in a few minutes.");
      else if(code.includes("network")) setError("Network error. Check your connection.");
      else setError(e.message||"Login failed");
    }
    setLoading(false);
  };
  return(
    <div style={{minHeight:"100vh",background:`linear-gradient(135deg,${T.brand} 0%,${T.brandDark} 100%)`,display:"flex",alignItems:"center",justifyContent:"center",padding:"20px",fontFamily:"'DM Sans',sans-serif"}}>
      <div style={{background:"#fff",borderRadius:"16px",padding:"32px 28px",maxWidth:400,width:"100%",boxShadow:"0 20px 60px rgba(0,0,0,0.3)"}}>
        <div style={{textAlign:"center",marginBottom:24}}>
          <img src="/images/davis-logo.jpg" alt="Davis Delivery" style={{height:70,width:"auto",objectFit:"contain",display:"inline-block",marginBottom:12}}/>
          <div style={{fontSize:"22px",fontWeight:800,color:T.text,letterSpacing:"-0.02em"}}>MarginIQ</div>
          <div style={{fontSize:"11px",color:T.textDim,letterSpacing:"0.1em",textTransform:"uppercase",marginTop:4}}>Cost Intelligence</div>
        </div>
        <div style={{marginBottom:12}}>
          <label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:4,fontWeight:600}}>Email</label>
          <input type="email" value={email} onChange={e=>setEmail(e.target.value)} onKeyDown={e=>e.key==="Enter"&&submit()} placeholder="your@email.com" style={{width:"100%",padding:"12px 14px",borderRadius:10,border:`1px solid ${T.border}`,fontSize:14,outline:"none",fontFamily:"inherit"}} autoFocus/>
        </div>
        <div style={{marginBottom:16}}>
          <label style={{fontSize:11,color:T.textMuted,display:"block",marginBottom:4,fontWeight:600}}>Password</label>
          <input type="password" value={password} onChange={e=>setPassword(e.target.value)} onKeyDown={e=>e.key==="Enter"&&submit()} placeholder="••••••••" style={{width:"100%",padding:"12px 14px",borderRadius:10,border:`1px solid ${T.border}`,fontSize:14,outline:"none",fontFamily:"inherit"}}/>
        </div>
        {error&&<div style={{padding:"10px 14px",borderRadius:8,background:T.redBg,color:T.redText,fontSize:12,marginBottom:12}}>⚠️ {error}</div>}
        <button onClick={submit} disabled={loading} style={{width:"100%",padding:"13px",borderRadius:10,border:"none",background:loading?"#94a3b8":`linear-gradient(135deg,${T.brand},${T.brandLight})`,color:"#fff",fontSize:14,fontWeight:700,cursor:loading?"wait":"pointer",fontFamily:"inherit"}}>{loading?"Signing in...":"Sign In"}</button>
        <div style={{marginTop:16,fontSize:10,color:T.textDim,textAlign:"center",lineHeight:1.5}}>Secured by Firebase Authentication.<br/>Accounts created by admin in Firebase Console.</div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN APP v4.0
// ═══════════════════════════════════════════════════════════════
function MarginIQ(){
  const [user,setUser]=useState(null);
  const [authChecking,setAuthChecking]=useState(true);

  // Auth state listener
  useEffect(()=>{
    if(!window.fbAuth){setAuthChecking(false);return;}
    const unsub=window.fbAuth.onAuthStateChanged(u=>{
      setUser(u);setAuthChecking(false);
    });
    return ()=>unsub();
  },[]);

  const logout=async()=>{try{await window.fbAuth.signOut();}catch(e){console.warn(e);}};

  if(authChecking) return <div style={{minHeight:"100vh",background:T.bg,display:"flex",alignItems:"center",justifyContent:"center",flexDirection:"column",gap:12,fontFamily:"'DM Sans',sans-serif"}}><div className="loading-pulse" style={{fontSize:48}}>📊</div><div style={{fontSize:13,color:T.textMuted}}>Loading...</div></div>;
  if(!user) return <LoginScreen/>;

  return <MarginIQApp user={user} onLogout={logout}/>;
}

// ═══════════════════════════════════════════════════════════════
// APP SHELL (authenticated)
// ═══════════════════════════════════════════════════════════════
function MarginIQApp({user,onLogout}){
  const [tab,setTab]=useState("command");const [costs,setCosts]=useState(DEFAULT_COSTS);const [ulineWeeks,setUlineWeeks]=useState([]);
  const [qboConnected,setQboConnected]=useState(false);const [qboData,setQboData]=useState(null);const [motiveConnected,setMotiveConnected]=useState(false);const [loading,setLoading]=useState(true);
  const [qbImports,setQbImports]=useState([]); // ALL QB file imports (GL, P&L, BS, customers, payroll)
  const reloadQB=async()=>{if(!hasFirebase)return;try{const s=await window.db.collection("qbo_imports").orderBy("imported_at","desc").get();setQbImports(s.docs.map(d=>({id:d.id,...d.data()})));}catch(e){console.warn("QB load:",e);}};
  useEffect(()=>{(async()=>{setLoading(true);const sc=await FS.getCosts();if(sc)setCosts(p=>({...p,...sc}));setUlineWeeks(await FS.getUlineWeeks());
    await reloadQB();
    try{const r=await fetch("/.netlify/functions/marginiq-qbo-data?action=status");const d=await r.json();if(d.connected){setQboConnected(true);const dash=await fetchQBO("dashboard",`${new Date().getFullYear()}-01-01`,new Date().toISOString().slice(0,10));if(dash)setQboData(dash);}}catch(e){}
    const p=new URLSearchParams(window.location.search);if(p.get("qbo")==="connected"){setQboConnected(true);window.history.replaceState({},"","/");}if(p.get("qbo")==="error"){alert("QBO Failed: "+(p.get("reason")||"unknown")+"\n"+(p.get("detail")?decodeURIComponent(p.get("detail")):""));window.history.replaceState({},"","/");}
    try{const r=await fetch("/.netlify/functions/marginiq-motive?action=vehicles");if(r.ok)setMotiveConnected(true);}catch(e){}setLoading(false);})();},[]);

  // Derive consolidated QB financials from all imports
  const qbFinancials=useMemo(()=>{
    if(qbImports.length===0)return null;
    const gl=qbImports.find(q=>q.type==="gl");
    const pnl=qbImports.find(q=>q.type==="pnl");
    const bs=qbImports.find(q=>q.type==="bs");
    const customersImport=qbImports.find(q=>q.type==="customers");
    const payrollImport=qbImports.find(q=>q.type==="payroll");

    // Prefer GL-derived totals over P&L if we have GL transactions
    let totalRevenue=0,totalExpenses=0,netIncome=0,dateStart=null,dateEnd=null,source="";
    let monthlyPnL=[],quarterlyPnL=[],yearlyPnL=[];
    let categoryTotals={},customerRevenue=[],vendorSpend=[];

    if(gl?.transactions&&gl.transactions.length>0){
      source="gl";
      const txns=gl.transactions;
      dateStart=gl.dateStart;dateEnd=gl.dateEnd;
      // Inline classify (match the QBOImport logic)
      const classify=(account)=>{
        const a=String(account||"").toLowerCase();
        if(a.includes("delivery sales")||a.includes("sales income")||a.match(/^income/)) return {cat:"revenue",sub:"Delivery Sales"};
        if(a.includes("misc")&&a.includes("income")) return {cat:"revenue",sub:"Other Income"};
        if(a.includes("salaries")||a.includes("wages")) return {cat:"expense",sub:"Salaries & Wages"};
        if(a.includes("subcontractor")||a.includes("3rd party delivery")) return {cat:"expense",sub:"Subcontractors"};
        if(a.includes("temporary services")) return {cat:"expense",sub:"Temp Labor"};
        if(a.includes("officers salaries")) return {cat:"expense",sub:"Officer Salaries"};
        if(a.includes("payroll tax")) return {cat:"expense",sub:"Payroll Taxes"};
        if(a.includes("payroll fees")||a.includes("payroll deduction")||a.includes("contract labor")) return {cat:"expense",sub:"Payroll Other"};
        if(a.includes("truck fuel")||a.includes("fuel - gas")) return {cat:"expense",sub:"Fuel"};
        if(a.includes("truck repair")||a.includes("truck maintenance")||a.includes("truck parts")||a.includes("truck tires")||a.includes("truck wash")||a.includes("truck - misc")||a.includes("trailer repair")||a.includes("towing")) return {cat:"expense",sub:"Truck Maintenance"};
        if(a.includes("truck lease")||a.includes("trailer rental")||a.includes("rent - equipment")) return {cat:"expense",sub:"Truck Leases"};
        if(a.includes("taxes & licenses - trucks")||a.includes("licenses & permits")) return {cat:"expense",sub:"Truck Taxes/Licenses"};
        if(a.includes("insurance")&&!a.includes("health")) return {cat:"expense",sub:"Insurance"};
        if(a.includes("health plan")||a.includes("fsa")||a.includes("health & dental")||a.includes("medical")) return {cat:"expense",sub:"Health Insurance"};
        if(a.includes("rent - office")||a.includes("rent office")) return {cat:"expense",sub:"Rent"};
        if(a.includes("warehouse")||a.includes("propane")||a.includes("fork lift")||a.includes("forklift")) return {cat:"expense",sub:"Warehouse"};
        if(a.includes("utilities")||a.includes("electric")||a.includes("cell phone")||a.includes("telephone")||a.includes("trash")) return {cat:"expense",sub:"Utilities"};
        if(a.includes("computer")||a.includes("internet")) return {cat:"expense",sub:"IT"};
        if(a.includes("bank charge")||a.includes("interest")) return {cat:"expense",sub:"Bank/Interest"};
        if(a.includes("legal")||a.includes("professional")) return {cat:"expense",sub:"Legal/Professional"};
        if(a.includes("depreciation")) return {cat:"expense",sub:"Depreciation"};
        if(a.includes("office")||a.includes("admin")) return {cat:"expense",sub:"Office/Admin"};
        if(a.includes("meals")||a.includes("travel")||a.includes("seminars")) return {cat:"expense",sub:"Travel/Meals"};
        if(a.includes("damages")||a.includes("claims")||a.includes("penalties")) return {cat:"expense",sub:"Damages/Claims"};
        if(a.includes("uniform")) return {cat:"expense",sub:"Uniforms"};
        if(a.includes("uline purchase")) return {cat:"expense",sub:"Uline Supplies"};
        if(a.includes("advertis")||a.includes("promotion")) return {cat:"expense",sub:"Marketing"};
        if(a.includes("state tax")||a.includes("property tax")||a.includes("other taxes")) return {cat:"expense",sub:"Other Taxes"};
        if(a.includes("repair")||a.includes("maintenance")) return {cat:"expense",sub:"Repairs"};
        if(a.includes("supplies")||a.includes("materials")||a.includes("stationery")) return {cat:"expense",sub:"Supplies"};
        if(a.includes("retirement")||a.includes("401k")) return {cat:"expense",sub:"Retirement"};
        if(a.includes("dues")||a.includes("subscription")) return {cat:"expense",sub:"Dues/Subs"};
        if(a.includes("bad debt")) return {cat:"expense",sub:"Bad Debts"};
        return {cat:"other",sub:a};
      };
      const monthly={},quarterly={},yearly={};
      const vendors={},customers={};
      txns.forEach(t=>{
        const c=classify(t.account);
        if(c.cat==="other")return;
        const amt=c.cat==="revenue"?(t.credit-t.debit):(t.debit-t.credit);
        if(c.cat==="revenue") totalRevenue+=amt; else totalExpenses+=amt;
        categoryTotals[c.sub]=(categoryTotals[c.sub]||0)+amt;
        if(!monthly[t.yearMonth]) monthly[t.yearMonth]={period:t.yearMonth,revenue:0,expense:0};
        if(!quarterly[t.yearQuarter]) quarterly[t.yearQuarter]={period:t.yearQuarter,revenue:0,expense:0};
        const y=String(t.year);if(!yearly[y]) yearly[y]={period:y,revenue:0,expense:0};
        if(c.cat==="revenue"){monthly[t.yearMonth].revenue+=amt;quarterly[t.yearQuarter].revenue+=amt;yearly[y].revenue+=amt;}
        else{monthly[t.yearMonth].expense+=amt;quarterly[t.yearQuarter].expense+=amt;yearly[y].expense+=amt;}
        // Vendor/customer breakdown
        if(t.name){
          if(c.cat==="expense"){if(!vendors[t.name])vendors[t.name]={vendor:t.name,total:0,count:0};vendors[t.name].total+=amt;vendors[t.name].count++;}
          if(c.cat==="revenue"){if(!customers[t.name])customers[t.name]={customer:t.name,revenue:0,count:0};customers[t.name].revenue+=amt;customers[t.name].count++;}
        }
      });
      const addMeta=p=>({...p,netIncome:p.revenue-p.expense,margin:p.revenue>0?(p.revenue-p.expense)/p.revenue*100:0});
      monthlyPnL=Object.values(monthly).map(addMeta).sort((a,b)=>a.period.localeCompare(b.period));
      quarterlyPnL=Object.values(quarterly).map(addMeta).sort((a,b)=>a.period.localeCompare(b.period));
      yearlyPnL=Object.values(yearly).map(addMeta).sort((a,b)=>a.period.localeCompare(b.period));
      vendorSpend=Object.values(vendors).filter(v=>v.total>0).sort((a,b)=>b.total-a.total).slice(0,100);
      customerRevenue=Object.values(customers).filter(c=>c.revenue>0).sort((a,b)=>b.revenue-a.revenue).slice(0,200);
      netIncome=totalRevenue-totalExpenses;
    }else if(pnl){
      source="pnl";
      totalRevenue=pnl.totalRevenue||0;totalExpenses=pnl.totalExpenses||0;netIncome=pnl.netIncome||0;
      // Extract categories from P&L items
      (pnl.items||[]).forEach(it=>{if(it.section==="expense"&&!it.isTotal) categoryTotals[it.label]=it.value;});
    }

    // Overlay customers import if present (may be more structured)
    if(customersImport?.customers) customerRevenue=customersImport.customers.map(c=>({customer:c.name,revenue:c.revenue,count:0}));

    // Calculate date-range days for annualizing
    let daysInRange=365;
    if(dateStart&&dateEnd){try{daysInRange=Math.max(1,Math.round((new Date(dateEnd)-new Date(dateStart))/(1000*60*60*24)));}catch(e){}}
    const annualizer=daysInRange>0?365/daysInRange:1;

    return{
      source, // "gl" or "pnl"
      hasGL:!!gl,hasPnL:!!pnl,hasBS:!!bs,hasCustomers:!!customersImport,hasPayroll:!!payrollImport,
      totalRevenue,totalExpenses,netIncome,
      marginPct:totalRevenue>0?(netIncome/totalRevenue*100):0,
      dateStart,dateEnd,daysInRange,
      annualRevenue:totalRevenue*annualizer,
      annualExpenses:totalExpenses*annualizer,
      annualNetIncome:netIncome*annualizer,
      monthlyPnL,quarterlyPnL,yearlyPnL,
      categoryTotals, // sub => $
      customerRevenue, // [{customer,revenue,count}]
      vendorSpend, // [{vendor,total,count}]
      bs, // raw balance sheet data
      pnlRaw:pnl,
      totalAssets:bs?.totalAssets||0,totalLiabilities:bs?.totalLiabilities||0,totalEquity:bs?.totalEquity||0,
      payrollEmployees:payrollImport?.employees||[],
    };
  },[qbImports]);

  // Enhanced margins that use QB data when available
  const margins=useMemo(()=>{
    const ulineAgg=ulineWeeks.length>0?{totalRevenue:ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0),totalStops:ulineWeeks.reduce((s,w)=>s+(w.totalStops||0),0),weekCount:ulineWeeks.length}:null;
    const m=calculateMargins(costs,ulineAgg,qboData);
    // Enhance with QB data if available
    if(qbFinancials){
      const wd=costs.working_days_year||260;
      // Use QB annualized numbers as the authoritative financial picture
      m.qbAnnualRevenue=qbFinancials.annualRevenue;
      m.qbAnnualExpenses=qbFinancials.annualExpenses;
      m.qbAnnualNetIncome=qbFinancials.annualNetIncome;
      m.qbMarginPct=qbFinancials.marginPct;
      m.qbDailyRevenue=qbFinancials.annualRevenue/wd;
      m.qbDailyExpenses=qbFinancials.annualExpenses/wd;
      m.qbDailyNetIncome=qbFinancials.annualNetIncome/wd;
      // When Uline isn't loaded, use QB daily revenue
      if(!ulineAgg&&qbFinancials.annualRevenue>0){m.dailyRevenue=m.qbDailyRevenue;m.ulineAnnualRevenue=qbFinancials.annualRevenue;}
      // Use QB expenses as authoritative cost if cost structure isn't set
      if(!costs.count_box_drivers&&qbFinancials.annualExpenses>0){
        m.totalAnnualCost=qbFinancials.annualExpenses;
        m.dailyCost=qbFinancials.annualExpenses/wd;
        m.monthlyCost=qbFinancials.annualExpenses/12;
        m.dailyMargin=m.dailyRevenue-m.dailyCost;
        m.dailyMarginPct=m.dailyRevenue>0?(m.dailyMargin/m.dailyRevenue*100):0;
      }
      // Real cost breakdown from QB categories
      if(Object.keys(qbFinancials.categoryTotals).length>0){
        const annualizer=qbFinancials.daysInRange>0?365/qbFinancials.daysInRange:1;
        const colors=["#3b82f6","#6366f1","#f59e0b","#10b981","#ef4444","#f97316","#8b5cf6","#ec4899","#14b8a6","#78716c","#06b6d4","#84cc16"];
        m.qbCostBreakdown=Object.entries(qbFinancials.categoryTotals).filter(([,v])=>v>0).sort((a,b)=>b[1]-a[1]).slice(0,12).map(([name,value],i)=>({name,value:value*annualizer,color:colors[i%colors.length]}));
        // If cost structure isn't manually set, show QB breakdown on command center
        if(m.costBreakdown.length===0||!costs.count_box_drivers) m.costBreakdown=m.qbCostBreakdown;
      }
    }
    return m;
  },[costs,ulineWeeks,qboData,qbFinancials]);
  const onUline=w=>{setUlineWeeks(p=>[w,...p.filter(x=>x.week_id!==w.week_id)]);};
  const tabs=[{id:"command",i:"🎯",l:"Command"},{id:"ai",i:"🤖",l:"AI"},{id:"reconcile",i:"🔗",l:"Reconcile"},{id:"uline",i:"📦",l:"Uline"},{id:"routes",i:"🛣️",l:"Routes"},{id:"trends",i:"📈",l:"Trends"},{id:"payroll",i:"💵",l:"Payroll"},{id:"mileage",i:"⛽",l:"Mileage"},{id:"customers",i:"🏢",l:"Customers"},{id:"fleet",i:"🚛",l:"Fleet"},{id:"integrity",i:"🛡️",l:"Integrity"},{id:"costs",i:"⚙️",l:"Costs"},{id:"qbimport",i:"📁",l:"QB Import"},{id:"import",i:"📥",l:"Import"},{id:"settings",i:"🔧",l:"Settings"}];
  return(
    <div style={{minHeight:"100vh",background:T.bg,color:T.text,fontFamily:"'DM Sans',sans-serif"}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 16px",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.95)",backdropFilter:"blur(12px)",position:"sticky",top:0,zIndex:100}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <img src="/images/davis-logo.jpg" alt="Davis Delivery" style={{height:40,width:"auto",objectFit:"contain",display:"block"}}/>
          <div style={{borderLeft:`1px solid ${T.border}`,paddingLeft:10}}><div style={{fontSize:"14px",fontWeight:800}}>MarginIQ</div><div style={{fontSize:"8px",color:T.textDim,letterSpacing:"0.1em",textTransform:"uppercase"}}>Cost Intelligence</div></div>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:6}}>
          {qboConnected&&<Badge text="QBO" color={T.greenText} bg={T.greenBg}/>}
          <span style={{fontSize:"8px",color:T.textDim,padding:"2px 6px",background:T.bgSurface,borderRadius:"5px",fontWeight:600}}>v{APP_VERSION}</span>
          {user&&<div style={{display:"flex",alignItems:"center",gap:6,padding:"3px 4px 3px 8px",borderRadius:"18px",background:T.brandPale,border:`1px solid ${T.border}`}}>
            <div style={{width:20,height:20,borderRadius:"50%",background:T.brand,color:"#fff",fontSize:10,fontWeight:700,display:"flex",alignItems:"center",justifyContent:"center"}}>{(user.email||"?").charAt(0).toUpperCase()}</div>
            <span style={{fontSize:11,color:T.brand,fontWeight:600,maxWidth:120,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={user.email}>{user.email?.split("@")[0]}</span>
            <button onClick={onLogout} title="Log out" style={{padding:"3px 8px",borderRadius:"12px",border:"none",background:T.brand,color:"#fff",fontSize:10,fontWeight:700,cursor:"pointer",fontFamily:"inherit"}}>Log out</button>
          </div>}
        </div>
      </div>
      <div style={{display:"flex",gap:"2px",padding:"6px 8px",overflowX:"auto",borderBottom:`1px solid ${T.border}`,background:"rgba(255,255,255,0.7)",WebkitOverflowScrolling:"touch"}} className="hide-scrollbar">
        {tabs.map(t=><button key={t.id} onClick={()=>setTab(t.id)} style={{padding:"7px 12px",borderRadius:"7px",border:"none",background:tab===t.id?T.brand:"transparent",color:tab===t.id?"#fff":T.textMuted,fontSize:"12px",fontWeight:tab===t.id?700:500,cursor:"pointer",whiteSpace:"nowrap",flexShrink:0,display:"flex",alignItems:"center",gap:"6px",fontFamily:"inherit"}}><span style={{fontSize:"14px"}}>{t.i}</span><span>{t.l}</span></button>)}
      </div>
      {loading&&<div style={{textAlign:"center",padding:"60px 20px"}}><div className="loading-pulse" style={{fontSize:48,marginBottom:12}}>📊</div><div style={{fontSize:14,fontWeight:600}}>Loading...</div></div>}
      {!loading&&tab==="command"&&<CommandCenter margins={margins} ulineData={ulineWeeks.length>0?{weekCount:ulineWeeks.length,totalRevenue:ulineWeeks.reduce((s,w)=>s+(w.totalRevenue||0),0)}:null} qboConnected={qboConnected} qboData={qboData} qbFinancials={qbFinancials} connections={{nuvizz:true,motive:motiveConnected,cyberpay:true}}/>}
      {!loading&&tab==="ai"&&<AIInsights margins={margins} ulineWeeks={ulineWeeks} costs={costs} qbFinancials={qbFinancials}/>}
      {!loading&&tab==="import"&&<DataImport ulineWeeks={ulineWeeks} onUlineUpload={onUline}/>}
      {!loading&&tab==="reconcile"&&<ReconciliationTab ulineWeeks={ulineWeeks}/>}
      {!loading&&tab==="integrity"&&<DataIntegrity ulineWeeks={ulineWeeks}/>}
      {!loading&&tab==="uline"&&<UlineTab ulineWeeks={ulineWeeks} onUpload={onUline} margins={margins}/>}
      {!loading&&tab==="routes"&&<RouteTab margins={margins}/>}
      {!loading&&tab==="customers"&&<CustomerTab ulineWeeks={ulineWeeks} margins={margins} qbFinancials={qbFinancials}/>}
      {!loading&&tab==="fleet"&&<FleetTab margins={margins}/>}
      {!loading&&tab==="qbimport"&&<QBOImport onChange={reloadQB}/>}
      {!loading&&tab==="payroll"&&<PayrollTab ulineWeeks={ulineWeeks} margins={margins} qbFinancials={qbFinancials}/>}
      {!loading&&tab==="mileage"&&<MileageTab margins={margins} costs={costs}/>}
      {!loading&&tab==="trends"&&<TrendsTab ulineWeeks={ulineWeeks} margins={margins} qbFinancials={qbFinancials}/>}
      {!loading&&tab==="costs"&&<CostsTab costs={costs} onSave={setCosts} margins={margins} qbFinancials={qbFinancials}/>}
      {!loading&&tab==="settings"&&<SettingsTab qboConnected={qboConnected} motiveConnected={motiveConnected}/>}
    </div>
  );
}

export default MarginIQ;
if(typeof ReactDOM!=="undefined"&&document.getElementById("root")){ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(MarginIQ));}
