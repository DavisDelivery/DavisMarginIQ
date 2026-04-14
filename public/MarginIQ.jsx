import { useState, useEffect, useCallback, useRef } from "react";
import { initializeApp } from "firebase/app";
import {
  getFirestore, collection, doc, getDoc, getDocs,
  setDoc, addDoc, updateDoc, query, orderBy, limit, onSnapshot, serverTimestamp
} from "firebase/firestore";
import { getStorage, ref as storageRef, uploadBytes, getDownloadURL } from "firebase/storage";

// ─── Firebase Config ──────────────────────────────────────────
const firebaseConfig = {
  apiKey: "AIzaSyDyRyjuiP_UD8T_2xmW2xLjvqx9RLCYCmo",
  authDomain: "davismarginiq.firebaseapp.com",
  projectId: "davismarginiq",
  storageBucket: "davismarginiq.firebasestorage.app",
  messagingSenderId: "131773007635",
  appId: "1:131773007635:web:be408aab03d843333afce6",
  measurementId: "G-SRN8BVXXDB",
};

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);
const storage = getStorage(firebaseApp);

// ─── Firestore Helpers ────────────────────────────────────────
// Collections:
//   payroll_runs/{runId}         — CyberPay weekly payroll metadata
//   reports_1099/{dateRange}     — 1099 report metadata
//   uline_audits/{YYYY-MM-DD}    — Uline weekly audit rows
//   nuvizz_manifests/{YYYY-MM-DD}/stops/{stopId} — NuVizz stop data
//   drivers/{driverId}           — Driver roster (W2/1099)
//   costs/{YYYY-MM-DD}           — Daily cost roll-ups
//   margin_summary/{weekOf}      — Weekly margin snapshots

async function fsGetLatestPayrolls(limitN = 10) {
  try {
    const q = query(collection(db, "payroll_runs"), orderBy("check_date", "desc"), limit(limitN));
    const snap = await getDocs(q);
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) { console.error("fsGetLatestPayrolls:", e); return []; }
}

async function fsGetDrivers() {
  try {
    const snap = await getDocs(collection(db, "drivers"));
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) { console.error("fsGetDrivers:", e); return []; }
}

async function fsSetDriver(driverId, data) {
  try {
    await setDoc(doc(db, "drivers", driverId), { ...data, updated_at: serverTimestamp() }, { merge: true });
    return true;
  } catch (e) { console.error("fsSetDriver:", e); return false; }
}

async function fsGetUlineAudit(dateStr) {
  try {
    const d = await getDoc(doc(db, "uline_audits", dateStr));
    return d.exists() ? { id: d.id, ...d.data() } : null;
  } catch (e) { console.error("fsGetUlineAudit:", e); return null; }
}

async function fsGetMarginSummaries(limitN = 12) {
  try {
    const q = query(collection(db, "margin_summary"), orderBy("week_of", "desc"), limit(limitN));
    const snap = await getDocs(q);
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) { console.error("fsGetMarginSummaries:", e); return []; }
}

async function fsGetCosts(dateStr) {
  try {
    const d = await getDoc(doc(db, "costs", dateStr));
    return d.exists() ? { id: d.id, ...d.data() } : null;
  } catch (e) { console.error("fsGetCosts:", e); return null; }
}

// Storage: upload a file and return its download URL
// path examples: "payroll/pdfs/2026/file.pdf", "uline/raw/2026-04-14/file.xlsx"
async function fsUploadFile(path, file) {
  try {
    const fileRef = storageRef(storage, path);
    const snap = await uploadBytes(fileRef, file);
    return await getDownloadURL(snap.ref);
  } catch (e) { console.error("fsUploadFile:", e); return null; }
}

const APP_VERSION = "1.1.1";

// ─── Design Tokens ───────────────────────────────────────────
const T = {
  brand: "#1e5b92", brandLight: "#2a7bc8", brandDark: "#143f66",
  accent: "#10b981", accentWarn: "#f59e0b", accentDanger: "#ef4444",
  bg: "#0c1220", bgCard: "#111827", bgCardHover: "#1a2332", bgSurface: "#1f2937",
  text: "#f1f5f9", textMuted: "#94a3b8", textDim: "#64748b",
  border: "#1e293b", borderLight: "#334155",
  green: "#10b981", greenBg: "rgba(16,185,129,0.12)",
  red: "#ef4444", redBg: "rgba(239,68,68,0.12)",
  yellow: "#f59e0b", yellowBg: "rgba(245,158,11,0.12)",
  blue: "#3b82f6", blueBg: "rgba(59,130,246,0.12)",
  purple: "#8b5cf6", purpleBg: "rgba(139,92,246,0.12)",
  radius: "12px", radiusSm: "8px",
  shadow: "0 4px 24px rgba(0,0,0,0.3)",
};

// ─── Helpers ─────────────────────────────────────────────────
const fmt = (n) => {
  if (n == null || isNaN(n)) return "$0";
  return "$" + Number(n).toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 });
};
const fmtDec = (n, d = 2) => {
  if (n == null || isNaN(n)) return "0";
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
};
const pctColor = (pct, target = 30) => {
  if (pct >= target) return { color: T.green, bg: T.greenBg };
  if (pct >= target * 0.75) return { color: T.yellow, bg: T.yellowBg };
  return { color: T.red, bg: T.redBg };
};

// ─── Base Styles ─────────────────────────────────────────────
const S = {
  app: { minHeight: "100vh", background: `linear-gradient(135deg, ${T.bg} 0%, #0f1729 50%, #111827 100%)`, color: T.text, fontFamily: "'DM Sans', sans-serif" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 20px", borderBottom: `1px solid ${T.border}`, background: "rgba(12,18,32,0.95)", backdropFilter: "blur(12px)", position: "sticky", top: 0, zIndex: 100 },
  nav: { display: "flex", gap: "3px", padding: "10px 12px", overflowX: "auto", borderBottom: `1px solid ${T.border}`, background: "rgba(12,18,32,0.6)" },
  navBtn: (a) => ({ padding: "7px 14px", borderRadius: "8px", border: "none", background: a ? T.brand : "transparent", color: a ? "#fff" : T.textMuted, fontSize: "12px", fontWeight: a ? 600 : 500, cursor: "pointer", whiteSpace: "nowrap", transition: "all 0.2s" }),
  page: { padding: "16px", maxWidth: "1200px", margin: "0 auto" },
  kpiRow: { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: "10px", marginBottom: "20px" },
  kpiCard: { background: T.bgCard, borderRadius: T.radius, padding: "14px", border: `1px solid ${T.border}` },
  kpiLabel: { fontSize: "10px", color: T.textDim, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: "4px", fontWeight: 600 },
  kpiValue: { fontSize: "20px", fontWeight: 700, color: T.text, letterSpacing: "-0.02em" },
  kpiSub: { fontSize: "10px", marginTop: "3px", fontWeight: 500 },
  secTitle: { fontSize: "14px", fontWeight: 700, color: T.text, marginBottom: "12px", display: "flex", alignItems: "center", gap: "8px" },
  card: { background: T.bgCard, borderRadius: T.radius, padding: "14px", border: `1px solid ${T.border}`, marginBottom: "10px" },
  cardHead: { display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" },
  cardTitle: { fontSize: "13px", fontWeight: 600, color: T.text },
  badge: (c, bg) => ({ fontSize: "11px", fontWeight: 700, color: c, background: bg, padding: "2px 8px", borderRadius: "6px" }),
  row: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "5px 0", borderTop: `1px solid ${T.border}` },
  rowLabel: { fontSize: "11px", color: T.textMuted },
  rowVal: { fontSize: "12px", fontWeight: 600, color: T.text },
  connBtn: { display: "flex", alignItems: "center", gap: "10px", padding: "12px 16px", borderRadius: T.radius, border: `1px solid ${T.border}`, background: T.bgCard, color: T.text, fontSize: "13px", fontWeight: 600, cursor: "pointer", width: "100%", transition: "all 0.2s" },
  connDot: (on) => ({ width: 9, height: 9, borderRadius: "50%", background: on ? T.green : T.textDim, boxShadow: on ? `0 0 8px ${T.green}` : "none", flexShrink: 0 }),
  input: { width: "100%", padding: "9px 12px", borderRadius: "8px", border: `1px solid ${T.borderLight}`, background: T.bgSurface, color: T.text, fontSize: "13px" },
  primaryBtn: { width: "100%", padding: "12px", borderRadius: "10px", border: "none", background: `linear-gradient(135deg, ${T.brand}, ${T.brandLight})`, color: "#fff", fontSize: "14px", fontWeight: 700, cursor: "pointer" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px" },
  th: { textAlign: "left", padding: "8px 10px", borderBottom: `1px solid ${T.borderLight}`, color: T.textDim, fontSize: "10px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" },
  td: { padding: "8px 10px", borderBottom: `1px solid ${T.border}`, color: T.text },
  empty: { textAlign: "center", padding: "36px 20px", color: T.textMuted, fontSize: "13px" },
  tabToggle: { display: "flex", background: T.bgSurface, borderRadius: "8px", padding: "3px", marginBottom: "14px" },
  tabToggleBtn: (a) => ({ flex: 1, padding: "7px", borderRadius: "6px", border: "none", background: a ? T.brand : "transparent", color: a ? "#fff" : T.textMuted, fontSize: "12px", fontWeight: 600, cursor: "pointer" }),
};

// ─── Subcomponents ───────────────────────────────────────────
function KPI({ label, value, sub, subColor }) {
  return <div style={S.kpiCard}><div style={S.kpiLabel}>{label}</div><div style={S.kpiValue}>{value}</div>{sub && <div style={{ ...S.kpiSub, color: subColor || T.textMuted }}>{sub}</div>}</div>;
}

function MarginBadge({ pct, target = 30 }) {
  const c = pctColor(pct, target);
  return <span style={S.badge(c.color, c.bg)}>{fmtDec(pct, 1)}%</span>;
}

function ProgressBar({ pct, target = 30 }) {
  const c = pctColor(pct, target);
  return <div style={{ width: "100%", height: "5px", borderRadius: "3px", background: T.bgSurface, overflow: "hidden" }}><div style={{ height: "100%", borderRadius: "3px", background: c.color, width: `${Math.min(Math.max(pct, 0), 100)}%`, transition: "width 0.6s" }} /></div>;
}

function ConnCard({ name, icon, on, detail, onClick }) {
  return <button style={S.connBtn} onClick={onClick}><div style={S.connDot(on)} /><span style={{ fontSize: "16px" }}>{icon}</span><div style={{ flex: 1, textAlign: "left" }}><div>{name}</div><div style={{ fontSize: "10px", color: on ? T.green : T.textDim, fontWeight: 400 }}>{detail}</div></div><span style={{ color: T.textDim }}>→</span></button>;
}

// ─── Truck Profitability Card ────────────────────────────────
function TruckCard({ truck }) {
  const margin = truck.revenue > 0 ? ((truck.revenue - truck.totalCost) / truck.revenue) * 100 : 0;
  const c = pctColor(margin);
  return (
    <div style={S.card}>
      <div style={S.cardHead}>
        <div><div style={S.cardTitle}>🚛 Truck {truck.number}</div><div style={{ fontSize: "10px", color: T.textDim, marginTop: "2px" }}>{truck.driver || "Unassigned"} • {fmtDec(truck.miles, 0)} mi</div></div>
        <MarginBadge pct={margin} />
      </div>
      <ProgressBar pct={margin} />
      <div style={{ marginTop: "8px" }}>
        {[
          ["Revenue", fmt(truck.revenue), T.green],
          ["Total Cost", fmt(truck.totalCost), T.red],
          [`Fuel (${fmtDec(truck.miles, 0)} mi ÷ ${truck.mpg} MPG)`, fmt(truck.fuelCost)],
          ["Maintenance", fmt(truck.maintenance)],
          ["Insurance", fmt(truck.insurance)],
          ["Labor", fmt(truck.laborCost)],
        ].map(([l, v, c], i) => (
          <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={{ ...S.rowVal, color: c || T.text }}>{v}</span></div>
        ))}
        <div style={{ ...S.row, borderTop: `2px solid ${T.borderLight}` }}>
          <span style={{ ...S.rowLabel, fontWeight: 700, color: T.text }}>Net Profit</span>
          <span style={{ ...S.rowVal, color: c.color }}>{fmt(truck.revenue - truck.totalCost)}</span>
        </div>
      </div>
    </div>
  );
}

// ─── W2 Driver Card ──────────────────────────────────────────
function W2DriverCard({ driver }) {
  const margin = driver.revenue > 0 ? ((driver.revenue - driver.totalCost) / driver.revenue) * 100 : 0;
  return (
    <div style={S.card}>
      <div style={S.cardHead}>
        <div>
          <div style={S.cardTitle}>👤 {driver.name} <span style={S.badge(T.blue, T.blueBg)}>W2</span></div>
          <div style={{ fontSize: "10px", color: T.textDim, marginTop: "2px" }}>{driver.type === "tt" ? "Tractor Trailer" : "Box Truck"} • {fmtDec(driver.hours, 1)} hrs</div>
        </div>
        <MarginBadge pct={margin} />
      </div>
      <ProgressBar pct={margin} />
      <div style={{ marginTop: "8px" }}>
        {[
          ["Revenue Generated", fmt(driver.revenue), T.green],
          [`Labor (${fmtDec(driver.hours, 1)}h × $${driver.rate}/hr)`, fmt(driver.laborCost), T.red],
          ["Truck Cost Allocation", fmt(driver.truckCostAlloc || 0)],
          ["Stops", driver.stops || "—"],
          ["Revenue/Stop", driver.stops ? fmt(driver.revenue / driver.stops) : "—"],
          ["OT Hours", driver.otHours ? fmtDec(driver.otHours, 1) : "0"],
          ["OT Cost", driver.otCost ? fmt(driver.otCost) : "$0"],
        ].map(([l, v, c], i) => (
          <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={{ ...S.rowVal, color: c || T.text }}>{v}</span></div>
        ))}
      </div>
    </div>
  );
}

// ─── Contractor Card ─────────────────────────────────────────
function ContractorCard({ contractor }) {
  const margin = contractor.revenue > 0
    ? ((contractor.revenue - contractor.payout) / contractor.revenue) * 100 : 0;
  return (
    <div style={S.card}>
      <div style={S.cardHead}>
        <div>
          <div style={S.cardTitle}>👤 {contractor.name} <span style={S.badge(T.purple, T.purpleBg)}>1099</span></div>
          <div style={{ fontSize: "10px", color: T.textDim, marginTop: "2px" }}>{contractor.loads || 0} loads • {contractor.stops || 0} stops</div>
        </div>
        <MarginBadge pct={margin} />
      </div>
      <ProgressBar pct={margin} />
      <div style={{ marginTop: "8px" }}>
        {[
          ["Revenue from Deliveries", fmt(contractor.revenue), T.green],
          ["Payout to Contractor", fmt(contractor.payout), T.red],
          ["Net Margin", fmt(contractor.revenue - contractor.payout), pctColor(margin).color],
          ["Stops Completed", contractor.stops || "—"],
          ["Revenue/Stop", contractor.stops ? fmt(contractor.revenue / contractor.stops) : "—"],
          ["Payout/Stop", contractor.stops ? fmt(contractor.payout / contractor.stops) : "—"],
          ["Revenue/Load", contractor.loads ? fmt(contractor.revenue / contractor.loads) : "—"],
        ].map(([l, v, c], i) => (
          <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={{ ...S.rowVal, color: c || T.text }}>{v}</span></div>
        ))}
      </div>
    </div>
  );
}

// ─── Customer Card ───────────────────────────────────────────
function CustomerCard({ customer }) {
  const margin = customer.revenue > 0
    ? ((customer.revenue - customer.costToServe) / customer.revenue) * 100 : 0;
  return (
    <div style={S.card}>
      <div style={S.cardHead}>
        <div><div style={S.cardTitle}>{customer.name}</div><div style={{ fontSize: "10px", color: T.textDim, marginTop: "2px" }}>{customer.loads || 0} loads • {fmtDec(customer.totalMiles, 0)} mi</div></div>
        <MarginBadge pct={margin} />
      </div>
      <ProgressBar pct={margin} />
      <div style={{ marginTop: "8px" }}>
        {[
          ["Revenue", fmt(customer.revenue), T.green],
          ["Cost to Serve", fmt(customer.costToServe), T.red],
          ["Revenue/Mile", customer.totalMiles > 0 ? "$" + fmtDec(customer.revenue / customer.totalMiles) : "—"],
          ["Cost/Mile", customer.totalMiles > 0 ? "$" + fmtDec(customer.costToServe / customer.totalMiles) : "—"],
        ].map(([l, v, c], i) => (
          <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={{ ...S.rowVal, color: c || T.text }}>{v}</span></div>
        ))}
      </div>
    </div>
  );
}

// ─── Drivers Page (W2 + 1099) ────────────────────────────────
function DriversPage({ w2Drivers, contractors }) {
  const [view, setView] = useState("all");

  const totalW2 = w2Drivers.length;
  const totalContractors = contractors.length;
  const w2Rev = w2Drivers.reduce((s, d) => s + d.revenue, 0);
  const w2Cost = w2Drivers.reduce((s, d) => s + d.totalCost, 0);
  const conRev = contractors.reduce((s, c) => s + c.revenue, 0);
  const conPay = contractors.reduce((s, c) => s + c.payout, 0);
  const w2Margin = w2Rev > 0 ? ((w2Rev - w2Cost) / w2Rev) * 100 : 0;
  const conMargin = conRev > 0 ? ((conRev - conPay) / conRev) * 100 : 0;

  return (
    <div style={S.page}>
      <div style={S.secTitle}>👥 Driver & Contractor Profitability</div>

      {/* W2 vs 1099 Summary */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", marginBottom: "14px" }}>
        <div style={{ ...S.kpiCard, borderLeft: `3px solid ${T.blue}` }}>
          <div style={S.kpiLabel}>W2 Drivers ({totalW2})</div>
          <div style={{ fontSize: "16px", fontWeight: 700 }}>{fmt(w2Rev)}</div>
          <div style={{ fontSize: "10px", color: pctColor(w2Margin).color }}>Margin: {fmtDec(w2Margin, 1)}%</div>
        </div>
        <div style={{ ...S.kpiCard, borderLeft: `3px solid ${T.purple}` }}>
          <div style={S.kpiLabel}>Contractors ({totalContractors})</div>
          <div style={{ fontSize: "16px", fontWeight: 700 }}>{fmt(conRev)}</div>
          <div style={{ fontSize: "10px", color: pctColor(conMargin).color }}>Margin: {fmtDec(conMargin, 1)}%</div>
        </div>
      </div>

      {/* Toggle */}
      <div style={S.tabToggle}>
        <button style={S.tabToggleBtn(view === "all")} onClick={() => setView("all")}>All</button>
        <button style={S.tabToggleBtn(view === "w2")} onClick={() => setView("w2")}>W2</button>
        <button style={S.tabToggleBtn(view === "1099")} onClick={() => setView("1099")}>1099</button>
        <button style={S.tabToggleBtn(view === "compare")} onClick={() => setView("compare")}>Compare</button>
      </div>

      {/* Compare View */}
      {view === "compare" && (
        <div style={S.card}>
          <div style={{ ...S.cardTitle, marginBottom: "10px" }}>📊 W2 vs 1099 Comparison</div>
          {[
            ["Headcount", totalW2, totalContractors],
            ["Total Revenue", fmt(w2Rev), fmt(conRev)],
            ["Total Cost/Payout", fmt(w2Cost), fmt(conPay)],
            ["Net Profit", fmt(w2Rev - w2Cost), fmt(conRev - conPay)],
            ["Avg Margin", fmtDec(w2Margin, 1) + "%", fmtDec(conMargin, 1) + "%"],
            ["Revenue/Driver", totalW2 > 0 ? fmt(w2Rev / totalW2) : "—", totalContractors > 0 ? fmt(conRev / totalContractors) : "—"],
            ["Has GPS Data", "✅ Motive", "❌ No"],
            ["Truck Costs", "✅ Tracked", "❌ Their truck"],
          ].map(([label, w2Val, conVal], i) => (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", padding: "6px 0", borderTop: `1px solid ${T.border}`, fontSize: "11px" }}>
              <span style={{ color: T.textMuted }}>{label}</span>
              <span style={{ color: T.blue, fontWeight: 600, textAlign: "center" }}>{w2Val}</span>
              <span style={{ color: T.purple, fontWeight: 600, textAlign: "center" }}>{conVal}</span>
            </div>
          ))}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", padding: "4px 0", fontSize: "10px", color: T.textDim }}>
            <span></span><span style={{ textAlign: "center" }}>W2</span><span style={{ textAlign: "center" }}>1099</span>
          </div>
        </div>
      )}

      {/* Driver/Contractor Lists */}
      {(view === "all" || view === "w2") && w2Drivers.length > 0 && (
        <>
          {view === "all" && <div style={{ ...S.secTitle, marginTop: "8px" }}><span style={S.badge(T.blue, T.blueBg)}>W2</span> Drivers</div>}
          {w2Drivers.map((d) => <W2DriverCard key={d.id} driver={d} />)}
        </>
      )}

      {(view === "all" || view === "1099") && contractors.length > 0 && (
        <>
          {view === "all" && <div style={{ ...S.secTitle, marginTop: "8px" }}><span style={S.badge(T.purple, T.purpleBg)}>1099</span> Contractors</div>}
          {contractors.map((c) => <ContractorCard key={c.id} contractor={c} />)}
        </>
      )}

      {w2Drivers.length === 0 && contractors.length === 0 && (
        <div style={S.empty}>
          <div style={{ fontSize: "32px", marginBottom: "10px" }}>👥</div>
          <div style={{ fontWeight: 600, marginBottom: "4px" }}>Awaiting Driver Data</div>
          <div>W2 drivers populate from Motive + CyberPay payroll.</div>
          <div>Contractors populate from 1099 payroll + NuVizz stops.</div>
        </div>
      )}
    </div>
  );
}

// ─── Quote Generator ─────────────────────────────────────────
function QuotePage({ costConfig }) {
  const [q, setQ] = useState({ origin: "", destination: "", weight: "", pallets: "", miles: "", liftgate: false, residential: false, insideDelivery: false });
  const [result, setResult] = useState(null);

  const calc = () => {
    const miles = parseFloat(q.miles) || 0;
    const fuel = (miles / (costConfig.boxTruckMPG || 8)) * (costConfig.dieselPrice || 3.50);
    const laborHrs = Math.max(miles / 35, 2);
    const labor = laborHrs * (costConfig.boxTruckRate || 23);
    const truckDaily = ((costConfig.insurancePerTruck || 1000) + (costConfig.leasePerTruck || 2000)) / 30;
    const overhead = (costConfig.warehouseCostAnnual || 450000) / 365 / 50;
    let acc = 0;
    if (q.liftgate) acc += 75;
    if (q.residential) acc += 25;
    if (q.insideDelivery) acc += 50;
    const totalCost = fuel + labor + truckDaily + overhead + acc;
    const targetM = (costConfig.marginTarget || 30) / 100;
    const price = totalCost / (1 - targetM);
    const fuelSur = price * 0.15;
    setResult({ baseCost: totalCost, price, fuelSur, total: price + fuelSur, margin: ((price + fuelSur - totalCost) / (price + fuelSur)) * 100, breakdown: { fuel, labor, truckDaily, overhead, acc } });
  };

  return (
    <div style={S.page}>
      <div style={S.secTitle}>💲 Quote Generator</div>
      <div style={{ ...S.card, marginBottom: "14px" }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
          {[
            ["ORIGIN ZIP", "origin", "30518"],
            ["DESTINATION ZIP", "destination", "30601"],
            ["WEIGHT (lbs)", "weight", "2500"],
            ["PALLETS", "pallets", "2"],
          ].map(([l, k, ph]) => (
            <div key={k}><label style={{ fontSize: "10px", color: T.textDim, display: "block", marginBottom: "3px" }}>{l}</label><input style={S.input} placeholder={ph} type={k === "origin" || k === "destination" ? "text" : "number"} value={q[k]} onChange={(e) => setQ((p) => ({ ...p, [k]: e.target.value }))} /></div>
          ))}
          <div style={{ gridColumn: "1 / -1" }}><label style={{ fontSize: "10px", color: T.textDim, display: "block", marginBottom: "3px" }}>MILES (one way)</label><input style={S.input} type="number" placeholder="45" value={q.miles} onChange={(e) => setQ((p) => ({ ...p, miles: e.target.value }))} /></div>
        </div>
        <div style={{ marginTop: "10px", borderTop: `1px solid ${T.border}`, paddingTop: "10px" }}>
          <div style={{ fontSize: "10px", color: T.textDim, marginBottom: "4px" }}>ACCESSORIALS</div>
          {[["liftgate", "Liftgate (+$75)"], ["residential", "Residential / Gravel (+$25)"], ["insideDelivery", "Inside Delivery (+$50)"]].map(([k, l]) => (
            <label key={k} style={{ display: "flex", alignItems: "center", gap: "8px", padding: "5px 0", fontSize: "12px", color: T.textMuted }}>
              <input type="checkbox" checked={q[k]} onChange={(e) => setQ((p) => ({ ...p, [k]: e.target.checked }))} style={{ width: 16, height: 16, accentColor: T.brand }} />{l}
            </label>
          ))}
        </div>
        <button onClick={calc} style={{ ...S.primaryBtn, marginTop: "14px" }}>Calculate Quote</button>
      </div>

      {result && (
        <div style={{ ...S.card, border: `1px solid ${T.borderLight}` }}>
          <div style={{ textAlign: "center", marginBottom: "14px" }}>
            <div style={{ fontSize: "10px", color: T.textDim, textTransform: "uppercase", letterSpacing: "0.08em" }}>Recommended Quote</div>
            <div style={{ fontSize: "30px", fontWeight: 800, color: T.green }}>{fmt(result.total)}</div>
            <div style={{ fontSize: "11px", color: T.textMuted }}>at {fmtDec(result.margin, 1)}% margin</div>
          </div>
          {[
            ["Base Quote", fmt(result.price)],
            ["Fuel Surcharge (15%)", fmt(result.fuelSur)],
          ].map(([l, v], i) => <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={S.rowVal}>{v}</span></div>)}
          <div style={{ ...S.row, borderTop: `2px solid ${T.borderLight}` }}><span style={{ ...S.rowLabel, fontWeight: 700, color: T.text }}>Total Quote</span><span style={{ ...S.rowVal, color: T.green, fontSize: "15px" }}>{fmt(result.total)}</span></div>

          <div style={{ marginTop: "14px", paddingTop: "10px", borderTop: `1px solid ${T.border}` }}>
            <div style={{ fontSize: "10px", color: T.textDim, marginBottom: "6px", textTransform: "uppercase", letterSpacing: "0.06em" }}>Cost Breakdown</div>
            {[["Fuel", result.breakdown.fuel], ["Labor", result.breakdown.labor], ["Truck Fixed", result.breakdown.truckDaily], ["Overhead", result.breakdown.overhead], result.breakdown.acc > 0 && ["Accessorials", result.breakdown.acc]].filter(Boolean).map(([l, v], i) => (
              <div key={i} style={S.row}><span style={S.rowLabel}>{l}</span><span style={S.rowVal}>{fmt(v)}</span></div>
            ))}
            <div style={{ ...S.row, borderTop: `2px solid ${T.borderLight}` }}><span style={{ ...S.rowLabel, fontWeight: 700, color: T.text }}>Total Cost</span><span style={{ ...S.rowVal, color: T.red }}>{fmt(result.baseCost)}</span></div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Alerts Page ─────────────────────────────────────────────
function AlertsPage({ trucks, contractors, costConfig }) {
  const alerts = [];
  const target = costConfig.marginTarget || 30;
  trucks.forEach((t) => {
    const m = t.revenue > 0 ? ((t.revenue - t.totalCost) / t.revenue) * 100 : 0;
    if (m < target * 0.75 && t.revenue > 0) alerts.push({ type: "danger", icon: "🔴", msg: `Truck ${t.number} margin at ${fmtDec(m, 1)}% — below ${target}% target` });
    if (t.maintenance > 500) alerts.push({ type: "warn", icon: "🟡", msg: `Truck ${t.number} maintenance at ${fmt(t.maintenance)} this period` });
  });
  contractors.forEach((c) => {
    const m = c.revenue > 0 ? ((c.revenue - c.payout) / c.revenue) * 100 : 0;
    if (m < target * 0.75 && c.revenue > 0) alerts.push({ type: "danger", icon: "🔴", msg: `Contractor ${c.name} margin at ${fmtDec(m, 1)}%` });
  });
  if (alerts.length === 0) alerts.push({ type: "good", icon: "🟢", msg: "All operations within targets" });

  return (
    <div style={S.page}>
      <div style={S.secTitle}>🔔 Alerts</div>
      {alerts.map((a, i) => (
        <div key={i} style={{ ...S.card, borderLeft: `4px solid ${a.type === "danger" ? T.red : a.type === "warn" ? T.yellow : T.green}` }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}><span style={{ fontSize: "16px" }}>{a.icon}</span><span style={{ fontSize: "12px" }}>{a.msg}</span></div>
        </div>
      ))}
    </div>
  );
}

// ─── Settings Page ───────────────────────────────────────────
function SettingsPage({ connections, onConnectQBO, costConfig, setCostConfig }) {
  return (
    <div style={S.page}>
      <div style={S.secTitle}>⚡ Data Connections</div>
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", marginBottom: "20px" }}>
        <ConnCard name="Motive" icon="📡" on={connections.motive} detail={connections.motive ? `${connections.motiveVehicles || 0} vehicles` : "Not connected"} />
        <ConnCard name="QuickBooks Online" icon="📗" on={connections.qbo} detail={connections.qbo ? "Synced" : "Click to connect"} onClick={onConnectQBO} />
        <ConnCard name="Fleet Management" icon="🔧" on={connections.fleet} detail={connections.fleet ? "Firebase synced" : "Not connected"} />
        <ConnCard name="CyberPay Payroll" icon="💰" on={connections.payroll} detail="Gmail auto-pull" />
        <ConnCard name="NuVizz" icon="🗺️" on={connections.nuvizz} detail="CSV upload" />
      </div>

      <div style={S.secTitle}>⚙️ Cost Configuration</div>
      <div style={{ ...S.card }}>
        {[
          ["dieselPrice", "Diesel $/gal", "0.01"],
          ["boxTruckMPG", "Box Truck MPG", "0.5"],
          ["ttMPG", "Tractor Trailer MPG", "0.5"],
          ["boxTruckRate", "Box Truck Driver $/hr", "0.50"],
          ["ttRate", "TT Driver $/hr", "0.50"],
          ["insurancePerTruck", "Insurance $/truck/mo", "50"],
          ["leasePerTruck", "Lease $/truck/mo", "50"],
          ["warehouseCostAnnual", "Warehouse $/yr", "1000"],
          ["marginTarget", "Target Margin %", "1"],
        ].map(([k, l, step]) => (
          <div key={k} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0", borderBottom: `1px solid ${T.border}` }}>
            <label style={{ fontSize: "12px", color: T.textMuted }}>{l}</label>
            <input type="number" step={step} value={costConfig[k] ?? ""} onChange={(e) => setCostConfig((p) => ({ ...p, [k]: parseFloat(e.target.value) || 0 }))} style={{ width: "90px", padding: "5px 8px", borderRadius: "6px", border: `1px solid ${T.borderLight}`, background: T.bgSurface, color: T.text, fontSize: "12px", fontWeight: 600, textAlign: "right" }} />
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── NuVizz Upload Component ─────────────────────────────────
function NuVizzUpload({ onData }) {
  const fileRef = useRef();
  const [status, setStatus] = useState(null);

  const handleFile = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setStatus("Parsing...");
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const text = ev.target.result;
        const lines = text.split("\n").filter((l) => l.trim());
        if (lines.length < 2) { setStatus("Empty file"); return; }
        const headers = lines[0].split(",").map((h) => h.trim().toLowerCase().replace(/['"]/g, ""));
        const rows = lines.slice(1).map((line) => {
          const vals = line.split(",").map((v) => v.trim().replace(/^["']|["']$/g, ""));
          const obj = {};
          headers.forEach((h, i) => { obj[h] = vals[i] || ""; });
          return obj;
        });
        onData?.(rows, headers);
        setStatus(`✅ ${rows.length} stops loaded`);
      } catch (err) {
        setStatus("Parse error: " + err.message);
      }
    };
    reader.readAsText(file);
  };

  return (
    <div style={{ ...S.card, marginBottom: "14px" }}>
      <div style={{ fontSize: "12px", fontWeight: 600, marginBottom: "8px" }}>📤 Upload NuVizz Route Export</div>
      <div style={{ fontSize: "11px", color: T.textMuted, marginBottom: "8px" }}>Upload a CSV with stop data to populate contractor deliveries and route analysis.</div>
      <input ref={fileRef} type="file" accept=".csv,.tsv,.txt" onChange={handleFile} style={{ fontSize: "12px", color: T.textMuted }} />
      {status && <div style={{ marginTop: "6px", fontSize: "11px", color: status.startsWith("✅") ? T.green : T.yellow }}>{status}</div>}
    </div>
  );
}

// ─── Main App ────────────────────────────────────────────────
export default function MarginIQ() {
  const [tab, setTab] = useState("dashboard");
  const [connections, setConnections] = useState({ motive: false, motiveVehicles: 0, qbo: false, fleet: false, payroll: false, nuvizz: false });
  const [trucks, setTrucks] = useState([]);
  const [w2Drivers, setW2Drivers] = useState([]);
  const [contractors, setContractors] = useState([]);
  const [customers, setCustomers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [costConfig, setCostConfig] = useState({
    dieselPrice: 3.50, boxTruckMPG: 8, ttMPG: 6.5,
    boxTruckRate: 23, ttRate: 27.5,
    insurancePerTruck: 1000, leasePerTruck: 2000,
    warehouseCostAnnual: 450000, marginTarget: 30,
  });

  // Fetch Motive
  useEffect(() => {
    (async () => {
      try {
        const r = await fetch("/.netlify/functions/marginiq-motive?action=vehicles");
        if (r.ok) {
          const data = await r.json();
          if (data.vehicles) {
            setConnections((c) => ({ ...c, motive: true, motiveVehicles: data.count }));
            setTrucks(data.vehicles.map((v) => {
              const miles = v.odometer_miles || 0;
              const mpg = v.type === "tractor" ? costConfig.ttMPG : costConfig.boxTruckMPG;
              const rate = v.type === "tractor" ? costConfig.ttRate : costConfig.boxTruckRate;
              const hours = v.driving_hours || 0;
              const fuel = (miles / mpg) * costConfig.dieselPrice;
              const labor = hours * rate;
              const maint = v.maintenance_cost || 0;
              const ins = costConfig.insurancePerTruck;
              return { id: v.id, number: v.number, driver: v.current_driver?.first_name ? `${v.current_driver.first_name} ${v.current_driver.last_name || ""}` : "Unassigned", type: v.type || "box", miles, mpg, hours, fuelCost: fuel, laborCost: labor, maintenance: maint, insurance: ins, totalCost: fuel + labor + maint + ins, revenue: 0 };
            }));
          }
        }
      } catch (e) { console.error("Motive:", e); }
      setLoading(false);
    })();
  }, []);

  // Check QBO callback
  useEffect(() => {
    const p = new URLSearchParams(window.location.search);
    if (p.get("qbo") === "connected") {
      setConnections((c) => ({ ...c, qbo: true }));
      window.history.replaceState({}, "", window.location.pathname);
    }
  }, []);

  // Check QBO status
  useEffect(() => {
    (async () => {
      try {
        const r = await fetch("/.netlify/functions/marginiq-qbo-data?action=status");
        if (r.ok) {
          const d = await r.json();
          if (d.connected) setConnections((c) => ({ ...c, qbo: true }));
        }
      } catch (e) {}
    })();
  }, []);

  const handleConnectQBO = () => { window.location.href = "/.netlify/functions/marginiq-qbo-auth"; };

  const handleNuVizzData = (rows, headers) => {
    setConnections((c) => ({ ...c, nuvizz: true }));
    // NuVizz data can populate contractor stop counts and route analysis
    console.log("NuVizz data loaded:", rows.length, "rows, columns:", headers);
  };

  // Aggregates
  const totalRev = trucks.reduce((s, t) => s + t.revenue, 0) + contractors.reduce((s, c) => s + c.revenue, 0);
  const totalCost = trucks.reduce((s, t) => s + t.totalCost, 0) + contractors.reduce((s, c) => s + c.payout, 0);
  const totalMiles = trucks.reduce((s, t) => s + t.miles, 0);
  const margin = totalRev > 0 ? ((totalRev - totalCost) / totalRev) * 100 : 0;
  const cpm = totalMiles > 0 ? trucks.reduce((s, t) => s + t.totalCost, 0) / totalMiles : 0;
  const truckCount = trucks.length;
  const empCount = w2Drivers.length + contractors.length || "64";

  const tabs = [
    { id: "dashboard", label: "Dashboard" },
    { id: "trucks", label: "Trucks" },
    { id: "drivers", label: "Drivers" },
    { id: "customers", label: "Customers" },
    { id: "quote", label: "Quote" },
    { id: "alerts", label: "Alerts" },
    { id: "settings", label: "Settings" },
  ];

  return (
    <div style={S.app}>
      {/* Header */}
      <div style={S.header}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <div style={{ width: 34, height: 34, borderRadius: "9px", background: `linear-gradient(135deg, ${T.brand}, ${T.brandLight})`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: "16px", fontWeight: 800, color: "#fff" }}>M</div>
          <div><div style={{ fontSize: "15px", fontWeight: 700, letterSpacing: "-0.02em" }}>Davis MarginIQ</div><div style={{ fontSize: "9px", color: T.textDim, letterSpacing: "0.08em", textTransform: "uppercase" }}>Cost Intelligence</div></div>
        </div>
        <span style={{ fontSize: "9px", color: T.textDim, padding: "2px 6px", background: T.bgSurface, borderRadius: "5px" }}>v{APP_VERSION}</span>
      </div>

      {/* Nav */}
      <div style={S.nav}>{tabs.map((t) => <button key={t.id} style={S.navBtn(tab === t.id)} onClick={() => setTab(t.id)}>{t.label}</button>)}</div>

      {/* Dashboard */}
      {tab === "dashboard" && (
        <div style={S.page}>
          <div style={S.kpiRow}>
            <KPI label="Revenue" value={totalRev > 0 ? fmt(totalRev) : "—"} sub={totalRev === 0 ? "Connect QBO" : undefined} />
            <KPI label="Total Cost" value={fmt(totalCost)} />
            <KPI label="Margin" value={totalRev > 0 ? fmtDec(margin, 1) + "%" : "—"} sub={totalRev > 0 ? (margin >= 30 ? "✓ On target" : "⚠ Below") : "Awaiting"} subColor={margin >= 30 ? T.green : T.yellow} />
            <KPI label="Cost/Mile" value={cpm > 0 ? "$" + fmtDec(cpm) : "—"} />
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", marginBottom: "16px" }}>
            <KPI label="Fleet" value={truckCount > 0 ? truckCount : "—"} sub="trucks" />
            <KPI label="Workforce" value={empCount} sub="W2 + 1099" />
          </div>

          <div style={S.secTitle}>⚡ Data Sources</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", marginBottom: "20px" }}>
            <ConnCard name="Motive" icon="📡" on={connections.motive} detail={connections.motive ? `${connections.motiveVehicles} vehicles` : "—"} />
            <ConnCard name="QuickBooks" icon="📗" on={connections.qbo} detail={connections.qbo ? "Synced" : "Connect →"} onClick={handleConnectQBO} />
          </div>

          {trucks.length > 0 && (
            <>
              <div style={S.secTitle}>🚛 Fleet ({trucks.length})</div>
              <div style={{ overflowX: "auto" }}>
                <table style={S.table}>
                  <thead><tr><th style={S.th}>Truck</th><th style={S.th}>Driver</th><th style={S.th}>Miles</th><th style={S.th}>Cost</th><th style={S.th}>Margin</th></tr></thead>
                  <tbody>
                    {trucks.slice(0, 10).map((t) => {
                      const m = t.revenue > 0 ? ((t.revenue - t.totalCost) / t.revenue) * 100 : 0;
                      return <tr key={t.id} onClick={() => setTab("trucks")} style={{ cursor: "pointer" }}><td style={S.td}>{t.number}</td><td style={{ ...S.td, color: T.textMuted }}>{t.driver}</td><td style={S.td}>{fmtDec(t.miles, 0)}</td><td style={S.td}>{fmt(t.totalCost)}</td><td style={S.td}>{t.revenue > 0 ? <MarginBadge pct={m} /> : <span style={{ color: T.textDim }}>—</span>}</td></tr>;
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}

          {trucks.length === 0 && !loading && (
            <div style={S.empty}><div style={{ fontSize: "32px", marginBottom: "10px" }}>📊</div><div style={{ fontWeight: 600, marginBottom: "4px" }}>Awaiting Data</div><div>Connect Motive and QuickBooks in Settings.</div></div>
          )}
        </div>
      )}

      {/* Trucks */}
      {tab === "trucks" && (
        <div style={S.page}>
          <div style={S.secTitle}>🚛 Truck Profitability</div>
          {trucks.length === 0 ? <div style={S.empty}>Connect Motive in Settings.</div> : trucks.map((t) => <TruckCard key={t.id} truck={t} />)}
        </div>
      )}

      {/* Drivers (W2 + 1099) */}
      {tab === "drivers" && <DriversPage w2Drivers={w2Drivers} contractors={contractors} />}

      {/* Customers */}
      {tab === "customers" && (
        <div style={S.page}>
          <div style={S.secTitle}>🏢 Customer Profitability</div>
          <NuVizzUpload onData={handleNuVizzData} />
          {customers.length === 0 ? (
            <div style={S.empty}><div style={{ fontSize: "32px", marginBottom: "10px" }}>📋</div><div>Customer data populates from QBO invoices + route data.</div></div>
          ) : customers.map((c) => <CustomerCard key={c.id} customer={c} />)}
        </div>
      )}

      {/* Quote */}
      {tab === "quote" && <QuotePage costConfig={costConfig} />}

      {/* Alerts */}
      {tab === "alerts" && <AlertsPage trucks={trucks} contractors={contractors} costConfig={costConfig} />}

      {/* Settings */}
      {tab === "settings" && <SettingsPage connections={connections} onConnectQBO={handleConnectQBO} costConfig={costConfig} setCostConfig={setCostConfig} />}
    </div>
  );
}
