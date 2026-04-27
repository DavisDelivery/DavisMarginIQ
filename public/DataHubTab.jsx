/* global React, firebase */
// Davis MarginIQ — Data Hub tab
// Central control panel for all ingestion sources and the canonical master
// collections (employees, vehicles, customers). Other apps (SENTINEL, Fleet
// Management, Dispatch) read these masters instead of importing data themselves.
//
// Master collections this tab manages:
//   employees   — canonical roster, externalIds map links to all systems
//   vehicles    — truck/trailer roster, externalIds.motive/samsara
//   customers   — canonical customer list (extends customer_ap_contacts)
//
// Existing collections this tab READS (does not write):
//   payroll_weekly, timeclock_weekly, nuvizz_weekly, nuvizz_stops,
//   uline_weekly, fuel_weekly, fuel_by_truck, recon_weekly, ddis_files,
//   driver_classifications, customer_ap_contacts, marginiq_config

const { useState, useEffect, useMemo, useCallback } = React;

// ── Theme (kept in sync with MarginIQ.jsx) ───────────────────────
const DH_T = {
  brand: "#1e5b92", brandLight: "#2a7bc8", brandDark: "#143f66", brandPale: "#e8f0f8",
  bg: "#f0f4f8", bgCard: "#ffffff", bgSurface: "#f8fafc",
  text: "#0f172a", textMuted: "#64748b", textDim: "#94a3b8",
  border: "#e2e8f0", borderLight: "#f1f5f9",
  green: "#10b981", greenBg: "#ecfdf5", greenText: "#065f46",
  red: "#ef4444", redBg: "#fef2f2", redText: "#991b1b",
  yellow: "#f59e0b", yellowBg: "#fffbeb", yellowText: "#92400e",
  blue: "#3b82f6", blueBg: "#eff6ff", blueText: "#1e40af",
  purple: "#8b5cf6", purpleBg: "#f5f3ff", purpleText: "#5b21b6",
  cyan: "#06b6d4", cyanBg: "#ecfeff", cyanText: "#0e7490",
  orange: "#ea580c", orangeBg: "#fff7ed", orangeText: "#9a3412",
  radius: "12px", radiusSm: "8px",
  shadow: "0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
};

// ── Helpers ────────────────────────────────────────────────────
const dhFmt = n => n == null || isNaN(n) ? "0" : Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
const dhFmtMoney = n => n == null || isNaN(n) ? "$0" : "$" + Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });

// Slug an arbitrary name into a Firestore-safe id (matches MarginIQ's driverKey)
function dhKey(name) {
  if (!name) return null;
  return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140) || null;
}

// Title-case a "First Last" or "LAST, FIRST" string (matches MarginIQ's normalizeName)
function dhNormalizeName(v) {
  if (!v) return null;
  let s = String(v).trim();
  if (!s) return null;
  if (s.includes(",")) {
    const [last, first] = s.split(",").map(x => x.trim());
    if (first && last) s = `${first} ${last}`;
  }
  return s.replace(/\s+/g, " ").split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(" ");
}

function relativeTime(iso) {
  if (!iso) return "never";
  const d = new Date(iso);
  if (isNaN(d)) return "never";
  const mins = Math.floor((Date.now() - d.getTime()) / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs} hr${hrs === 1 ? "" : "s"} ago`;
  const days = Math.floor(hrs / 24);
  if (days < 7) return `${days} day${days === 1 ? "" : "s"} ago`;
  return d.toISOString().slice(0, 10);
}

const ROLE_OPTIONS = [
  { id: "management", label: "Management", color: DH_T.purple, bg: DH_T.purpleBg, text: DH_T.purpleText },
  { id: "office", label: "Office", color: "#64748b", bg: "#f1f5f9", text: "#475569" },
  { id: "driver", label: "Driver", color: DH_T.blue, bg: DH_T.blueBg, text: DH_T.blueText },
  { id: "owner_op", label: "Owner Op", color: "#6366f1", bg: "#eef2ff", text: "#4338ca" },
  { id: "shuttle_driver", label: "Shuttle", color: DH_T.cyan, bg: DH_T.cyanBg, text: DH_T.cyanText },
  { id: "yard_jockey", label: "Yard", color: "#0d9488", bg: "#f0fdfa", text: "#0f766e" },
  { id: "warehouse", label: "Warehouse", color: DH_T.yellow, bg: DH_T.yellowBg, text: DH_T.yellowText },
  { id: "mechanic", label: "Mechanic", color: DH_T.orange, bg: DH_T.orangeBg, text: DH_T.orangeText },
  { id: "unknown", label: "Unmapped", color: DH_T.textDim, bg: DH_T.bgSurface, text: DH_T.textMuted },
];
const ROLE_BY_ID = Object.fromEntries(ROLE_OPTIONS.map(r => [r.id, r]));

// ── Firestore helpers (DataHub-scoped) ─────────────────────────
const DH = {
  // Existing collections (READ ONLY here)
  async getPayrollWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("payroll_weekly").orderBy("week_ending", "desc").limit(52).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getPayrollWeekly:", e); return []; }
  },
  async getTimeclockWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("timeclock_weekly").orderBy("week_ending", "desc").limit(52).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getTimeclockWeekly:", e); return []; }
  },
  async getNuvizzWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("nuvizz_weekly").orderBy("week_ending", "desc").limit(26).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getNuvizzWeekly:", e); return []; }
  },
  async getUlineWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("uline_weekly").orderBy(firebase.firestore.FieldPath.documentId(), "desc").limit(8).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getUlineWeekly:", e); return []; }
  },
  async getFuelWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("fuel_weekly").orderBy(firebase.firestore.FieldPath.documentId(), "desc").limit(8).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getFuelWeekly:", e); return []; }
  },
  async getFuelByTruck() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("fuel_by_truck").limit(2000).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getFuelByTruck:", e); return []; }
  },
  async getDriverClassifications() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("driver_classifications").get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getDriverClassifications:", e); return []; }
  },
  async getCustomerApContacts() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("customer_ap_contacts").get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getCustomerApContacts:", e); return []; }
  },
  async getConfig(docId) {
    if (!window.db) return null;
    try {
      const s = await window.db.collection("marginiq_config").doc(docId).get();
      return s.exists ? s.data() : null;
    } catch (e) { return null; }
  },
  async getReconWeekly() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("recon_weekly").orderBy(firebase.firestore.FieldPath.documentId(), "desc").limit(8).get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { return []; }
  },

  // ─── Master collections (READ + WRITE) ────────────────────────
  async getEmployees() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("employees").get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { console.error("getEmployees:", e); return []; }
  },
  async saveEmployee(id, data) {
    if (!window.db) return false;
    try {
      await window.db.collection("employees").doc(id).set({
        ...data,
        updatedAt: new Date().toISOString(),
        updatedBy: "datahub_ui",
      }, { merge: true });
      return true;
    } catch (e) { console.error("saveEmployee:", e); return false; }
  },
  async getVehicles() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("vehicles").get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { return []; }
  },
  async saveVehicle(id, data) {
    if (!window.db) return false;
    try {
      await window.db.collection("vehicles").doc(id).set({
        ...data,
        updatedAt: new Date().toISOString(),
        updatedBy: "datahub_ui",
      }, { merge: true });
      return true;
    } catch (e) { console.error("saveVehicle:", e); return false; }
  },
  async getCustomers() {
    if (!window.db) return [];
    try {
      const s = await window.db.collection("customers").get();
      return s.docs.map(d => ({ id: d.id, ...d.data() }));
    } catch (e) { return []; }
  },
  async saveCustomer(id, data) {
    if (!window.db) return false;
    try {
      await window.db.collection("customers").doc(id).set({
        ...data,
        updatedAt: new Date().toISOString(),
        updatedBy: "datahub_ui",
      }, { merge: true });
      return true;
    } catch (e) { console.error("saveCustomer:", e); return false; }
  },
};

// ═══ MAIN COMPONENT ═════════════════════════════════════════════
function DataHubTab() {
  const [section, setSection] = useState("dashboard");
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState({
    employees: [],
    vehicles: [],
    customers: [],
    payroll: [],
    timeclock: [],
    nuvizz: [],
    uline: [],
    fuel: [],
    fuelByTruck: [],
    recon: [],
    classifications: [],
    apContacts: [],
    config: { b600: null, qbo: null },
  });
  const [refreshKey, setRefreshKey] = useState(0);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const [
        employees, vehicles, customers,
        payroll, timeclock, nuvizz, uline, fuel, fuelByTruck, recon,
        classifications, apContacts,
        b600Config, qboConfig,
      ] = await Promise.all([
        DH.getEmployees(), DH.getVehicles(), DH.getCustomers(),
        DH.getPayrollWeekly(), DH.getTimeclockWeekly(), DH.getNuvizzWeekly(),
        DH.getUlineWeekly(), DH.getFuelWeekly(), DH.getFuelByTruck(), DH.getReconWeekly(),
        DH.getDriverClassifications(), DH.getCustomerApContacts(),
        DH.getConfig("b600_last_pull"), DH.getConfig("qbo_tokens"),
      ]);
      setData({
        employees, vehicles, customers,
        payroll, timeclock, nuvizz, uline, fuel, fuelByTruck, recon,
        classifications, apContacts,
        config: { b600: b600Config, qbo: qboConfig },
      });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll, refreshKey]);

  const refresh = () => setRefreshKey(k => k + 1);

  // Derive ingestion source statuses from the loaded data
  const sources = useMemo(() => buildIngestionStatus(data), [data]);
  const unmappedEmployees = useMemo(() => {
    return data.employees.filter(e => {
      if (!e.role || e.role === "unknown") return true;
      const ext = e.externalIds || {};
      // For drivers: need motive + b600 + nuvizz
      if (e.role === "driver" || e.role === "owner_op" || e.role === "shuttle_driver") {
        return !ext.motive || !ext.b600 || !ext.nuvizz;
      }
      // For warehouse: need b600
      if (e.role === "warehouse" || e.role === "yard_jockey") {
        return !ext.b600;
      }
      return false;
    });
  }, [data.employees]);

  const navItems = [
    { id: "dashboard", icon: "📊", label: "Ingestion Status" },
    { id: "employees", icon: "👥", label: "Employee Mapping", badge: unmappedEmployees.length || null, badgeColor: unmappedEmployees.length > 0 ? "yellow" : null },
    { id: "vehicles", icon: "🚛", label: "Vehicle Mapping" },
    { id: "customers", icon: "🏢", label: "Customer Master" },
    { id: "upload", icon: "📤", label: "Manual Upload" },
    { id: "errors", icon: "⚠️", label: "Error Log" },
    { id: "reprocess", icon: "🔄", label: "Reprocess Tools" },
    { id: "settings", icon: "🔧", label: "Settings" },
  ];

  return React.createElement("div", { style: { display: "flex", gap: 0, minHeight: "calc(100vh - 110px)" } },
    // ── Sidebar ──
    React.createElement("aside", {
      style: {
        width: 220, flexShrink: 0, background: DH_T.bgCard,
        borderRight: `1px solid ${DH_T.border}`, padding: "16px 0",
      }
    },
      React.createElement("div", { style: { padding: "0 16px 12px", borderBottom: `1px solid ${DH_T.borderLight}`, marginBottom: 8 } },
        React.createElement("div", {
          style: { fontSize: 10, fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: DH_T.brand, display: "flex", alignItems: "center", gap: 6 }
        }, "🗄️ Data Hub"),
        React.createElement("div", { style: { fontSize: 10, color: DH_T.textDim, marginTop: 2 } }, "Central source of truth")
      ),
      navItems.map(item => {
        const active = section === item.id;
        return React.createElement("button", {
          key: item.id,
          onClick: () => setSection(item.id),
          style: {
            width: "100%", border: "none", background: active ? DH_T.brandPale : "transparent",
            color: active ? DH_T.brand : DH_T.textMuted,
            padding: "10px 16px", fontSize: 12, fontWeight: active ? 700 : 500,
            cursor: "pointer", textAlign: "left", display: "flex", alignItems: "center", justifyContent: "space-between",
            borderRight: active ? `2px solid ${DH_T.brand}` : "2px solid transparent",
            fontFamily: "inherit",
          }
        },
          React.createElement("span", null, `${item.icon}  ${item.label}`),
          item.badge && React.createElement("span", {
            style: {
              fontSize: 9, fontWeight: 700, padding: "2px 6px", borderRadius: 99,
              background: item.badgeColor === "yellow" ? DH_T.yellowBg : DH_T.redBg,
              color: item.badgeColor === "yellow" ? DH_T.yellowText : DH_T.redText,
            }
          }, item.badge)
        );
      })
    ),
    // ── Main content ──
    React.createElement("main", { style: { flex: 1, padding: 20, overflow: "auto", minWidth: 0 } },
      loading
        ? React.createElement(LoadingState)
        : section === "dashboard" ? React.createElement(IngestionStatus, { sources, data, onRefresh: refresh })
        : section === "employees" ? React.createElement(EmployeeMapping, { data, unmapped: unmappedEmployees, onRefresh: refresh })
        : section === "vehicles" ? React.createElement(VehicleMapping, { data, onRefresh: refresh })
        : section === "customers" ? React.createElement(CustomerMaster, { data, onRefresh: refresh })
        : section === "upload" ? React.createElement(ManualUpload)
        : section === "errors" ? React.createElement(ErrorLog, { data })
        : section === "reprocess" ? React.createElement(ReprocessTools, { onRefresh: refresh })
        : section === "settings" ? React.createElement(SettingsPanel, { data })
        : null
    )
  );
}

// ═══ Ingestion Status ════════════════════════════════════════════
function buildIngestionStatus(data) {
  const sources = [];

  // Payroll
  const pLatest = data.payroll[0];
  sources.push({
    name: "Payroll PDF",
    sourceLabel: "Jessica Sage @ Southern Payroll",
    trigger: "Gmail Sync",
    lastRun: pLatest?.imported_at || pLatest?.updated_at || null,
    status: pLatest ? "green" : "amber",
    records: pLatest ? `${pLatest.employees?.length || 0} employees · week of ${pLatest.week_ending}` : "No data ingested yet",
    schedule: "Weekly · Mondays",
    icon: "💰",
    note: pLatest ? null : "Use Gmail Sync tab to import",
  });

  // NuVizz
  const nLatest = data.nuvizz[0];
  sources.push({
    name: "NuVizz Manifests",
    sourceLabel: "login.nuvizz.com (davis)",
    trigger: "Gmail / manual upload",
    lastRun: nLatest?.imported_at || nLatest?.updated_at || null,
    status: nLatest ? "green" : "amber",
    records: nLatest ? `${nLatest.top_drivers?.length || 0} drivers · week ${nLatest.week_ending}` : "No data ingested yet",
    schedule: "Weekly",
    icon: "📦",
  });

  // B600 Time Clock
  const tcLatest = data.timeclock[0];
  const b600Cfg = data.config.b600;
  sources.push({
    name: "B600 Time Clock",
    sourceLabel: "b600.atlantafreightquotes.com",
    trigger: "Auto-pull (cron) · manual fallback",
    lastRun: b600Cfg?.last_pull || tcLatest?.imported_at || null,
    status: tcLatest ? "green" : "amber",
    records: tcLatest ? `Week ${tcLatest.week_ending} · ${dhFmt(tcLatest.total_hours)} hrs` : "No data ingested yet",
    schedule: "Weekly · Mondays 9 AM ET",
    icon: "⏰",
    note: b600Cfg?.last_error ? `Last error: ${b600Cfg.last_error}` : null,
  });

  // QuickBooks
  const qbo = data.config.qbo;
  sources.push({
    name: "QuickBooks Online",
    sourceLabel: "QBO OAuth (refresh token)",
    trigger: "On-demand",
    lastRun: qbo?.updated_at || null,
    status: qbo?.access_token ? "green" : "amber",
    records: qbo?.access_token ? "Connected · token refreshes auto" : "Not connected",
    schedule: "On-demand",
    icon: "📊",
  });

  // Uline
  const uLatest = data.uline[0];
  sources.push({
    name: "Uline Audit XLSX",
    sourceLabel: "billing@davisdelivery.com",
    trigger: "Gmail Sync",
    lastRun: uLatest?.imported_at || null,
    status: uLatest ? "green" : "amber",
    records: uLatest ? `Week ${uLatest.id} · ${dhFmtMoney(uLatest.total_revenue || 0)}` : "No data ingested yet",
    schedule: "Weekly · Friday",
    icon: "📋",
  });

  // Fuel
  const fLatest = data.fuel[0];
  sources.push({
    name: "Fuel Receipts",
    sourceLabel: "FuelFox + Quick Fuel PDFs",
    trigger: "Gmail Sync",
    lastRun: fLatest?.imported_at || null,
    status: fLatest ? "green" : "amber",
    records: fLatest ? `Week ${fLatest.id} · ${dhFmtMoney(fLatest.total_cost || 0)}` : "No data ingested yet",
    schedule: "Weekly",
    icon: "⛽",
  });

  // DDIS reconciliation
  const rLatest = data.recon[0];
  sources.push({
    name: "Uline DDIS820 Recon",
    sourceLabel: "DDIS files (irregular)",
    trigger: "Manual upload",
    lastRun: rLatest?.imported_at || null,
    status: rLatest ? "green" : "amber",
    records: rLatest ? `Week ${rLatest.id} · ${dhFmtMoney(rLatest.total_paid || 0)} paid` : "No data ingested yet",
    schedule: "Irregular",
    icon: "🧾",
  });

  return sources;
}

function IngestionStatus({ sources, data, onRefresh }) {
  return React.createElement("div", null,
    // Header
    React.createElement("div", { style: { marginBottom: 20, display: "flex", alignItems: "end", justifyContent: "space-between" } },
      React.createElement("div", null,
        React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, letterSpacing: "-0.02em", margin: 0 } }, "Ingestion Status"),
        React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
          "All sources flowing into the hub. Apps consume from these collections.")
      ),
      React.createElement("button", {
        onClick: onRefresh,
        style: {
          padding: "6px 12px", fontSize: 11, fontWeight: 600, color: DH_T.textMuted,
          background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6,
          cursor: "pointer", fontFamily: "inherit",
        }
      }, "🔄 Refresh")
    ),

    // Top metrics
    React.createElement("div", { style: { display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 16 } },
      [
        { label: "Master Employees", value: dhFmt(data.employees.length), sub: data.employees.length === 0 ? "Run bootstrap below" : `${data.employees.filter(e => e.role && e.role !== "unknown").length} mapped`, color: DH_T.brand },
        { label: "Master Vehicles", value: dhFmt(data.vehicles.length), sub: data.vehicles.length === 0 ? "Add first" : `${data.vehicles.filter(v => v.status === "active").length} active`, color: "#0d9488" },
        { label: "Active Customers", value: dhFmt(data.customers.length), sub: data.customers.length === 0 ? "Add first" : `${data.apContacts.length} AP contacts`, color: DH_T.purple },
        { label: "Records (recent)", value: dhFmt((data.payroll[0]?.employees?.length || 0) + (data.timeclock[0]?.employees?.length || 0) + (data.nuvizz[0]?.top_drivers?.length || 0)), sub: "Latest week", color: DH_T.orange },
      ].map(m => React.createElement("div", {
        key: m.label,
        style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, padding: 14 }
      },
        React.createElement("div", { style: { fontSize: 9, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: DH_T.textMuted } }, m.label),
        React.createElement("div", { style: { fontSize: 26, fontWeight: 800, color: m.color, marginTop: 4, letterSpacing: "-0.02em" } }, m.value),
        React.createElement("div", { style: { fontSize: 10, color: DH_T.textDim, marginTop: 2 } }, m.sub)
      ))
    ),

    // Source cards
    React.createElement("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 10 } },
      sources.map(src => React.createElement(SourceCard, { key: src.name, src }))
    )
  );
}

function SourceCard({ src }) {
  const cfg = {
    green: { bg: DH_T.greenBg, text: DH_T.greenText, border: "#a7f3d0", label: "Healthy" },
    amber: { bg: DH_T.yellowBg, text: DH_T.yellowText, border: "#fde68a", label: "Pending" },
    red: { bg: DH_T.redBg, text: DH_T.redText, border: "#fecaca", label: "Failed" },
  }[src.status] || { bg: DH_T.bgSurface, text: DH_T.textMuted, border: DH_T.border, label: "Unknown" };

  return React.createElement("div", {
    style: {
      background: DH_T.bgCard, border: `1px solid ${cfg.border}`, borderRadius: DH_T.radius, padding: 14,
    }
  },
    React.createElement("div", { style: { display: "flex", alignItems: "start", justifyContent: "space-between", marginBottom: 10 } },
      React.createElement("div", { style: { display: "flex", gap: 10, alignItems: "start" } },
        React.createElement("div", {
          style: { width: 32, height: 32, borderRadius: 8, background: DH_T.bgSurface, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16, flexShrink: 0 }
        }, src.icon),
        React.createElement("div", null,
          React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, src.name),
          React.createElement("div", { style: { fontSize: 10, color: DH_T.textDim, marginTop: 2 } }, src.sourceLabel)
        )
      ),
      React.createElement("span", {
        style: { fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 99, background: cfg.bg, color: cfg.text, textTransform: "uppercase", letterSpacing: "0.05em" }
      }, cfg.label)
    ),
    React.createElement("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, fontSize: 10, paddingTop: 10, borderTop: `1px solid ${DH_T.borderLight}` } },
      React.createElement("div", null,
        React.createElement("div", { style: { color: DH_T.textDim, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", fontSize: 9 } }, "Last Run"),
        React.createElement("div", { style: { color: DH_T.text, marginTop: 2 } }, relativeTime(src.lastRun))
      ),
      React.createElement("div", null,
        React.createElement("div", { style: { color: DH_T.textDim, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", fontSize: 9 } }, "Schedule"),
        React.createElement("div", { style: { color: DH_T.text, marginTop: 2 } }, src.schedule)
      ),
      React.createElement("div", { style: { gridColumn: "1 / -1" } },
        React.createElement("div", { style: { color: DH_T.textDim, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", fontSize: 9 } }, "Latest"),
        React.createElement("div", { style: { color: DH_T.text, marginTop: 2 } }, src.records),
        src.note && React.createElement("div", { style: { color: DH_T.redText, fontSize: 10, marginTop: 4, fontWeight: 600 } }, "⚠ " + src.note)
      )
    )
  );
}

// ═══ Employee Mapping (rebuilt — typeahead spreadsheet editor) ═════════
// Two modes:
//   1. BOOTSTRAP MODE (only when employees collection is empty)
//      → upload W2 + 1099 payroll PDFs, parse via /api/scan-payroll,
//        review extracted rows inline, write 80 employees to Firestore.
//   2. EDITOR MODE (after bootstrap, permanent)
//      → spreadsheet table where every cell is a typeahead dropdown that
//        pulls live values from each data source. Claimed values graylisted.

// ─── Distinct-value harvesters ─────────────────────────────────────
// Walk every existing collection once and pull every distinct value
// each data source has used to identify a person/vehicle. These become
// the dropdown options for each cell.
function harvestSourceValues(data, fleetMgmt) {
  const harvested = {
    payroll: new Set(),
    payrollNames: new Map(),
    b600: new Set(),
    b600Names: new Map(),
    nuvizz: new Set(),
    nuvizzDisplay: new Map(),
    motive: new Set(),
    motiveNames: new Map(),
    samsara: new Set(),
    truck: new Set(),
    truckAssignments: new Map(), // driverName -> latest truck (from Fleet Mgmt)
    fleetDrivers: [],           // [{ name, role, category }] — definitive role list
  };

  // Payroll — IDs from payroll_weekly.employees[].payroll_id (or top_employees fallback)
  for (const wk of (data.payroll || [])) {
    const list = wk.employees || wk.top_employees || [];
    for (const e of list) {
      const id = e.payroll_id || e.id || e.employee_id;
      if (id) {
        harvested.payroll.add(String(id));
        if (e.name) harvested.payrollNames.set(String(id), e.name);
      }
    }
  }

  // B600 — from timeclock_weekly.top_employees[].name (the actual stored shape)
  for (const wk of (data.timeclock || [])) {
    const list = wk.top_employees || wk.employees || [];
    for (const e of list) {
      const nm = e.name || e.employee_name;
      if (nm) {
        const key = String(nm).trim();
        harvested.b600.add(key);
        // No separate ID in current schema — display the name itself
        harvested.b600Names.set(key, key);
      }
    }
  }

  // NuVizz — driver names from nuvizz_weekly.top_drivers[].name
  for (const wk of (data.nuvizz || [])) {
    for (const d of (wk.top_drivers || [])) {
      if (d.name) {
        const key = String(d.name).trim();
        if (key) {
          harvested.nuvizz.add(key);
          harvested.nuvizzDisplay.set(key, key);
        }
      }
    }
  }

  // Trucks — fuel_by_truck uses truck_id, NOT truck_number
  for (const r of (data.fuelByTruck || [])) {
    const t = r.truck_id || r.truck_number;
    if (t && t !== "INVENTORY") harvested.truck.add(String(t));
  }
  for (const v of (data.vehicles || [])) {
    const t = v.truckNumber || v.truck_id;
    if (t) harvested.truck.add(String(t));
  }

  // Fleet Management — definitive truck list + driver→truck assignments + driver roles
  if (fleetMgmt) {
    for (const t of (fleetMgmt.trucks || [])) {
      harvested.truck.add(String(t));
    }
    for (const [driver, truck] of Object.entries(fleetMgmt.assignments || {})) {
      harvested.truckAssignments.set(driver, String(truck));
    }
    harvested.fleetDrivers = fleetMgmt.drivers || [];
  }

  // Motive — populated lazily via /api/marginiq-motive?action=drivers
  // Samsara — future
  return harvested;
}

// Map Fleet Management role string -> our canonical role id
function fleetMgmtRoleToCanonical(role) {
  if (!role) return null;
  const r = String(role).toLowerCase();
  if (r.includes("uline") && r.includes("shuttle")) return "shuttle_driver";
  if (r.includes("owner")) return "owner_op";
  if (r.includes("tractor") || r.includes("straight")) return "driver";
  return null;
}

// Find the Fleet Management entry that best matches a driver name
function matchFleetDriver(name, fleetDrivers) {
  if (!name || !fleetDrivers || fleetDrivers.length === 0) return null;
  const norm = s => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const target = norm(name);
  // 1. exact match
  let m = fleetDrivers.find(d => norm(d.name) === target);
  if (m) return m;
  // 2. last-name + first-initial match
  const targetParts = target.split(" ");
  const targetLast = targetParts[targetParts.length - 1];
  const targetFI = targetParts[0]?.charAt(0);
  m = fleetDrivers.find(d => {
    const p = norm(d.name).split(" ");
    return p[p.length - 1] === targetLast && p[0]?.charAt(0) === targetFI;
  });
  if (m) return m;
  // 3. unique last name
  const lastMatches = fleetDrivers.filter(d => {
    const p = norm(d.name).split(" ");
    return p[p.length - 1] === targetLast;
  });
  if (lastMatches.length === 1) return lastMatches[0];
  return null;
}

// Index claimed values per source: { payroll: Map<id, employeeId>, ... }
function indexClaimedValues(employees) {
  const claimed = {
    payroll: new Map(), b600: new Map(), nuvizz: new Map(),
    motive: new Map(), samsara: new Map(), truck: new Map(),
  };
  for (const e of employees) {
    const ext = e.externalIds || {};
    if (ext.payroll) claimed.payroll.set(String(ext.payroll), e.id);
    if (ext.b600) claimed.b600.set(String(ext.b600), e.id);
    if (ext.nuvizz) claimed.nuvizz.set(String(ext.nuvizz), e.id);
    if (ext.motive) claimed.motive.set(String(ext.motive), e.id);
    if (ext.samsara) claimed.samsara.set(String(ext.samsara), e.id);
    if (e.defaultTruck) claimed.truck.set(String(e.defaultTruck), e.id);
  }
  return claimed;
}

// ─── PDF text extraction (client-side, layout preserved) ───────────
async function ensurePdfJs() {
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

// Extract text from PDF preserving column alignment (parsePayroll.mjs needs
// layout-aware text — items at similar y get joined into the same line).
// Uses 2pt y-bucketing to handle small font-baseline differences within a
// row (CyberPay PDFs often render labels and values 0.1-0.5pt apart vertically).
async function pdfFileToLayoutText(file) {
  const pdfjs = await ensurePdfJs();
  const buffer = await file.arrayBuffer();
  const pdf = await pdfjs.getDocument({ data: buffer }).promise;
  const lines = [];
  for (let p = 1; p <= pdf.numPages; p++) {
    const page = await pdf.getPage(p);
    const tc = await page.getTextContent();
    // Group items by y-coordinate (round to nearest 2pt).
    const rowsByY = new Map();
    for (const it of tc.items) {
      const y = Math.round((it.transform?.[5] ?? 0) / 2) * 2;
      if (!rowsByY.has(y)) rowsByY.set(y, []);
      rowsByY.get(y).push(it);
    }
    const sortedYs = [...rowsByY.keys()].sort((a, b) => b - a);
    for (const y of sortedYs) {
      const row = rowsByY.get(y).sort((a, b) => (a.transform?.[4] ?? 0) - (b.transform?.[4] ?? 0));
      // Reconstruct line with whitespace approximating column gaps
      let line = "";
      let prevEndX = 0;
      for (const it of row) {
        const x = it.transform?.[4] ?? 0;
        if (line.length > 0) {
          const gap = x - prevEndX;
          // Roughly: 3 PDF units ~ 1 space character for our payroll PDFs
          const spaces = Math.max(1, Math.round(gap / 3));
          line += " ".repeat(Math.min(spaces, 12));
        }
        line += it.str;
        prevEndX = x + (it.width ?? 0);
      }
      lines.push(line.trimEnd());
    }
    lines.push("\f"); // form feed between pages
  }
  return lines.join("\n");
}

// ═══ MAIN: EmployeeMapping ════════════════════════════════════════════
function EmployeeMapping({ data, unmapped, onRefresh }) {
  const [showBootstrap, setShowBootstrap] = useState(false);
  const hasEmployees = data.employees.length > 0;

  // Show bootstrap CTA panel when no employees yet, OR explicitly invoked
  if (!hasEmployees || showBootstrap) {
    return React.createElement(BootstrapPanel, {
      data,
      onComplete: () => { setShowBootstrap(false); onRefresh(); },
      onCancel: hasEmployees ? () => setShowBootstrap(false) : null,
    });
  }

  return React.createElement(EmployeeEditor, {
    data, unmapped, onRefresh,
    onBootstrap: () => setShowBootstrap(true),
  });
}

// ─── Bootstrap Panel ───────────────────────────────────────────────
// One-time flow: drag-drop W2 + 1099 payroll PDFs, parse, review, save.
function BootstrapPanel({ data, onComplete, onCancel }) {
  const [phase, setPhase] = useState("upload"); // upload | parsing | review | saving | done
  const [w2File, setW2File] = useState(null);
  const [p1099File, setP1099File] = useState(null);
  const [parseError, setParseError] = useState(null);
  const [rows, setRows] = useState([]); // parsed employee candidates
  const [progress, setProgress] = useState("");
  const [fleetMgmt, setFleetMgmt] = useState(null);   // { drivers, trucks, assignments }
  const [motiveDrivers, setMotiveDrivers] = useState(null); // [{ id, first_name, last_name }]

  // Pre-load Fleet Management + Motive on mount so dropdowns are populated
  // immediately (don't wait for click).
  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await fetch("/api/fleet-management?action=all");
        if (r.ok && !cancelled) {
          const j = await r.json();
          setFleetMgmt(j);
        }
      } catch (e) { /* silent */ }
      try {
        const r = await fetch("/.netlify/functions/marginiq-motive?action=drivers");
        if (r.ok && !cancelled) {
          const j = await r.json();
          setMotiveDrivers(j.drivers || []);
        }
      } catch (e) { /* silent */ }
    })();
    return () => { cancelled = true; };
  }, []);

  // Extract per-employee payroll ID from raw text using SSN as anchor.
  // - 1099 PDF: data row format is "<payrollId> xxx-xx-NNNN ..." (ID may differ
  //   from SSN last-4, e.g. "37 xxx-xx-7893" for George Leonard).
  // - W2 PDF: data row format is "xxx-xx-NNNN ..." (no separate ID column);
  //   we use SSN last-4 as the effective payroll ID.
  function extractPayrollIds(rawText) {
    const map = {}; // ssn -> payroll_id
    const lines = rawText.split("\n");
    for (const line of lines) {
      // 1099 form: "<id> xxx-xx-NNNN ..."
      const m1099 = line.match(/^\s*([A-Za-z0-9-]+)\s+xxx-xx-(\d{4})/);
      if (m1099 && !/^xxx-xx-/.test(m1099[1])) {
        map[`xxx-xx-${m1099[2]}`] = m1099[1];
        continue;
      }
      // W2 form: "xxx-xx-NNNN ..." (no ID column) — use SSN last-4
      const mW2 = line.match(/^\s*xxx-xx-(\d{4})/);
      if (mW2) {
        const ssn = `xxx-xx-${mW2[1]}`;
        if (!(ssn in map)) map[ssn] = mW2[1];
      }
    }
    return map;
  }

  // Infer rate, pay type, and YTD gross from the employee's payRows.
  function inferPayInfo(emp, company) {
    const rows = emp.payRows || [];
    if (company === "0189") {
      // 1099 — single 1099 row with rate=net amount, ytd=cumulative
      const row1099 = rows.find(r => r.type === "1099");
      return {
        payType: "percentage",
        payRate: 0,  // rate per check varies; not meaningful for 1099 LLC
        ytdGross: row1099?.ytd || emp.pay || 0,
      };
    }
    // W2 priority: Salary > Hourly
    const salary = rows.find(r => r.type === "Salary");
    const hourly = rows.find(r => r.type === "Hourly");
    let payType, payRate;
    if (salary && salary.rate) {
      payType = "salary"; payRate = salary.rate;
    } else if (hourly && hourly.rate) {
      payType = "hourly"; payRate = hourly.rate;
    } else {
      payType = "hourly"; payRate = 0;
    }
    // YTD = sum of every payRow's ytd
    const ytdGross = rows.reduce((s, r) => s + (r.ytd || 0), 0);
    return { payType, payRate, ytdGross };
  }

  const parsePdfs = async () => {
    if (!w2File && !p1099File) {
      setParseError("Upload at least one payroll PDF (W2 or 1099)");
      return;
    }
    setPhase("parsing");
    setParseError(null);
    const candidates = [];
    const diagnostics = [];
    try {
      for (const [file, label] of [[w2File, "W2"], [p1099File, "1099"]]) {
        if (!file) continue;
        setProgress(`Reading ${label} PDF (${file.name})…`);
        const rawText = await pdfFileToLayoutText(file);
        diagnostics.push(`${label}: extracted ${rawText.length.toLocaleString()} chars from ${file.name}`);
        setProgress(`Parsing ${label} payroll structure…`);
        const resp = await fetch("/api/scan-payroll", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ rawText, filename: file.name }),
        });
        if (!resp.ok) {
          const err = await resp.text();
          throw new Error(`${label} parse failed (${resp.status}): ${err.slice(0, 200)}`);
        }
        const result = await resp.json();
        const company = result.company;
        const parsedEmployees = result.employees || [];
        diagnostics.push(`${label}: parser returned company=${company || "?"}, employees=${parsedEmployees.length}`);
        if (parsedEmployees.length === 0) {
          diagnostics.push(`${label}: ⚠ Zero employees extracted — likely PDF format mismatch. Check console for raw text dump.`);
          // eslint-disable-next-line no-console
          console.warn(`[BootstrapPanel] ${label} PDF returned 0 employees. First 1500 chars of extracted text:\n` + rawText.slice(0, 1500));
        }
        const idMap = extractPayrollIds(rawText);

        for (const e of parsedEmployees) {
          const name = dhNormalizeName(e.rawName || e.name);
          if (!name) continue;
          const id = dhKey(name);
          const payrollId = idMap[e.ssn] || (e.ssn ? e.ssn.replace("xxx-xx-", "") : "");
          const payInfo = inferPayInfo(e, company);
          let role = "unknown";

          // First try Fleet Management — it's the truth list for drivers/contractors
          const fleetMatch = matchFleetDriver(name, fleetMgmt?.drivers);
          const fleetRole = fleetMatch ? fleetMgmtRoleToCanonical(fleetMatch.role) : null;
          // Pre-assigned truck from Fleet Management's most-recent weekly assignment
          const fleetTruck = fleetMatch
            ? (fleetMgmt?.assignments?.[fleetMatch.name] || null)
            : (fleetMgmt?.assignments?.[name] || null);

          if (company === "0189") {
            role = fleetRole || "owner_op"; // Fleet Mgmt may say "Owner Tractor Driver"
            const isLLC = /LLC|INC|CORP|TRANSPORTATION|DELIVERY|EXPRESS|LOGISTICS|FREIGHT|TRANSPORT|INVESTORS|NETWORK|ENTERPRISE|SERVICES?|DBA/i.test(name);
            candidates.push({
              id, fullName: name,
              firstName: name.split(" ")[0],
              lastName: name.split(" ").slice(1).join(" "),
              role, payType: payInfo.payType, payRate: payInfo.payRate,
              ytdGross: payInfo.ytdGross,
              externalIds: { payroll: payrollId },
              defaultTruck: fleetTruck,
              source: "1099",
              isLLC,
              llcName: isLLC ? name : null,
              actualDriverName: isLLC ? null : name,
              fleetRoleString: fleetMatch?.role || null,
            });
          } else {
            // W2 — Fleet Management role wins; otherwise fall back to pay heuristic
            if (fleetRole) {
              role = fleetRole;
            } else {
              const rate = payInfo.payRate;
              if (payInfo.payType === "salary" && rate >= 1500) role = "management";
              else if (payInfo.payType === "salary" && rate >= 1100) role = "driver";
              else if (payInfo.payType === "salary") role = "office";
              else if (rate >= 30) role = "mechanic";
              else if (rate >= 22) role = "driver";
              else if (rate >= 18) role = "warehouse";
              else role = "warehouse";
            }

            candidates.push({
              id, fullName: name,
              firstName: name.split(" ")[0],
              lastName: name.split(" ").slice(1).join(" "),
              role, payType: payInfo.payType, payRate: payInfo.payRate,
              ytdGross: payInfo.ytdGross,
              externalIds: { payroll: payrollId },
              defaultTruck: fleetTruck,
              source: "W2",
              isLLC: false,
              actualDriverName: name,
              fleetRoleString: fleetMatch?.role || null,
            });
          }
        }
      }
      // eslint-disable-next-line no-console
      console.log("[BootstrapPanel] " + diagnostics.join(" · "));
      if (candidates.length === 0) {
        throw new Error("No employees extracted. " + diagnostics.join("; ") + ". Make sure these are CyberPay payroll PDFs from Southern Payroll Services.");
      }
      setRows(candidates);
      setPhase("review");
    } catch (e) {
      setParseError(String(e.message || e));
      setPhase("upload");
    }
  };

  const updateRow = (i, patch) => setRows(r => r.map((row, idx) => idx === i ? { ...row, ...patch } : row));
  const removeRow = i => setRows(r => r.filter((_, idx) => idx !== i));

  const saveAll = async () => {
    setPhase("saving");
    try {
      let saved = 0;
      for (const row of rows) {
        if (!row.actualDriverName) continue; // skip unresolved LLCs
        const id = dhKey(row.actualDriverName);
        const aliases = [];
        if (row.isLLC && row.llcName) aliases.push(row.llcName);
        if (row.externalIds?.b600 && row.externalIds.b600 !== row.actualDriverName) {
          aliases.push(row.externalIds.b600);
        }
        if (row.externalIds?.nuvizz && row.externalIds.nuvizz !== row.actualDriverName) {
          aliases.push(row.externalIds.nuvizz);
        }
        await DH.saveEmployee(id, {
          fullName: row.actualDriverName,
          firstName: row.actualDriverName.split(" ")[0],
          lastName: row.actualDriverName.split(" ").slice(1).join(" "),
          role: row.role,
          status: "active",
          payRate: row.payRate || 0,
          payType: row.payType,
          externalIds: row.externalIds || {},
          defaultTruck: row.defaultTruck || null,
          aliases: [...new Set(aliases)],
          source: row.source,
          ytdGross: row.ytdGross,
          createdAt: new Date().toISOString(),
          createdBy: "datahub_bootstrap_payroll",
        });
        saved++;
      }
      setPhase("done");
      setTimeout(() => onComplete(), 1500);
    } catch (e) {
      setParseError(`Save failed: ${e.message || e}`);
      setPhase("review");
    }
  };

  if (phase === "done") {
    return React.createElement("div", { style: { padding: 40, textAlign: "center" } },
      React.createElement("div", { style: { fontSize: 48, marginBottom: 12 } }, "✅"),
      React.createElement("div", { style: { fontSize: 18, fontWeight: 700, color: DH_T.text } }, "Roster created"),
      React.createElement("div", { style: { fontSize: 12, color: DH_T.textMuted, marginTop: 6 } }, "Loading editor…")
    );
  }

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16, display: "flex", justifyContent: "space-between", alignItems: "start" } },
      React.createElement("div", null,
        React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Bootstrap Roster from Payroll PDFs"),
        React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
          "One-time setup. Upload the latest W2 + 1099 payroll PDFs from Southern Payroll. After this, the editor below is the only place you'll touch the roster.")
      ),
      onCancel && React.createElement("button", {
        onClick: onCancel,
        style: { padding: "5px 11px", fontSize: 11, fontWeight: 600, color: DH_T.textMuted, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
      }, "← Back to editor")
    ),

    phase === "upload" && React.createElement("div", null,
      React.createElement("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 } },
        React.createElement(PdfDropzone, { label: "W2 Payroll (Company 0190)", file: w2File, onChange: setW2File, hint: "All hourly + salary employees" }),
        React.createElement(PdfDropzone, { label: "1099 Payroll (Company 0189)", file: p1099File, onChange: setP1099File, hint: "Owner-operators and contractors" })
      ),
      parseError && React.createElement("div", {
        style: { background: DH_T.redBg, border: `1px solid #fecaca`, borderRadius: DH_T.radius, padding: 12, marginBottom: 12, fontSize: 12, color: DH_T.redText }
      }, "❌ " + parseError),
      React.createElement("button", {
        onClick: parsePdfs, disabled: !w2File && !p1099File,
        style: {
          padding: "10px 18px", fontSize: 13, fontWeight: 700, color: "#fff",
          background: (!w2File && !p1099File) ? DH_T.textDim : DH_T.brand,
          border: "none", borderRadius: 6, cursor: (!w2File && !p1099File) ? "not-allowed" : "pointer", fontFamily: "inherit",
        }
      }, "Parse payroll →")
    ),

    phase === "parsing" && React.createElement("div", { style: { padding: 40, textAlign: "center" } },
      React.createElement("div", { className: "loading-pulse", style: { fontSize: 36, marginBottom: 12 } }, "📄"),
      React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, progress || "Parsing payroll…"),
      React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 4 } }, "This takes 5-15 seconds")
    ),

    phase === "review" && React.createElement(BootstrapReview, {
      data, rows, updateRow, removeRow,
      fleetMgmt, motiveDrivers,
      onSave: saveAll,
      onBack: () => setPhase("upload"),
      onCancel,
    }),

    phase === "saving" && React.createElement("div", { style: { padding: 40, textAlign: "center" } },
      React.createElement("div", { className: "loading-pulse", style: { fontSize: 36, marginBottom: 12 } }, "💾"),
      React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, `Saving ${rows.filter(r => r.actualDriverName).length} employees…`)
    )
  );
}

function PdfDropzone({ label, file, onChange, hint }) {
  const [dragOver, setDragOver] = useState(false);
  // Unique id so each dropzone's <label htmlFor> targets its own input
  const inputId = React.useMemo(() => `pdf-dz-${Math.random().toString(36).slice(2, 9)}`, []);

  const onDrop = e => {
    e.preventDefault(); setDragOver(false);
    const f = [...(e.dataTransfer.files || [])].find(x => x.name.toLowerCase().endsWith(".pdf"));
    if (f) onChange(f);
  };

  // Use a <label> as the clickable container — when the user taps anywhere
  // inside the label, the browser natively triggers the input's file picker.
  // This is the only reliable way to get the picker to open on iOS Safari;
  // programmatic input.click() from a parent's onClick handler often gets
  // swallowed by iOS's user-gesture rules.
  return React.createElement("label", {
    htmlFor: inputId,
    onDragOver: e => { e.preventDefault(); setDragOver(true); },
    onDragLeave: () => setDragOver(false),
    onDrop,
    style: {
      display: "block",
      background: file ? DH_T.greenBg : DH_T.bgCard,
      border: `2px dashed ${file ? "#a7f3d0" : (dragOver ? DH_T.brand : DH_T.border)}`,
      borderRadius: DH_T.radius, padding: 22, textAlign: "center", cursor: "pointer",
      transition: "all 0.15s",
      // Prevent iOS callout/long-press menu and ensure tap is responsive
      WebkitTapHighlightColor: "rgba(30,91,146,0.15)",
      WebkitTouchCallout: "none",
      userSelect: "none",
    }
  },
    React.createElement("div", { style: { fontSize: 28, marginBottom: 8 } }, file ? "✅" : "📄"),
    React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text, marginBottom: 3 } }, label),
    file
      ? React.createElement("div", { style: { fontSize: 11, color: DH_T.greenText, fontWeight: 600 } }, file.name)
      : React.createElement("div", { style: { fontSize: 10, color: DH_T.textMuted } }, "Tap to choose · or drag-drop"),
    React.createElement("div", { style: { fontSize: 10, color: DH_T.textDim, marginTop: 4 } }, hint),
    // Visually hidden but still clickable — using a position-absolute trick
    // rather than display:none so iOS still treats it as a focusable input.
    React.createElement("input", {
      id: inputId,
      type: "file",
      accept: "application/pdf,.pdf",
      style: {
        position: "absolute",
        width: 1, height: 1,
        opacity: 0,
        overflow: "hidden",
        pointerEvents: "none",
      },
      onChange: e => { const f = e.target.files?.[0]; if (f) onChange(f); }
    })
  );
}

function BootstrapReview({ data, rows, updateRow, removeRow, fleetMgmt, motiveDrivers, onSave, onBack, onCancel }) {
  const w2Count = rows.filter(r => r.source === "W2").length;
  const llcUnresolved = rows.filter(r => r.isLLC && !r.actualDriverName).length;

  // Harvest distinct values from real source collections + Fleet Management
  const harvested = useMemo(() => {
    const h = harvestSourceValues(data, fleetMgmt);
    // Eagerly populate Motive options from preloaded driver list
    if (motiveDrivers && motiveDrivers.length > 0) {
      for (const d of motiveDrivers) {
        const id = String(d.id || "");
        const nm = d.first_name && d.last_name ? `${d.first_name} ${d.last_name}` : "";
        if (id) {
          h.motive.add(id);
          if (nm) h.motiveNames.set(id, nm);
        }
      }
    }
    return h;
  }, [data, fleetMgmt, motiveDrivers]);

  // Track what's claimed across pending rows (so dropdowns graylist correctly).
  // Build claim maps from the rows themselves (not from the saved employees,
  // because nothing's saved yet during bootstrap).
  const claimed = useMemo(() => {
    const c = { payroll: new Map(), b600: new Map(), nuvizz: new Map(), motive: new Map(), samsara: new Map(), truck: new Map() };
    for (const r of rows) {
      const ext = r.externalIds || {};
      if (ext.payroll) c.payroll.set(String(ext.payroll), r.id);
      if (ext.b600) c.b600.set(String(ext.b600), r.id);
      if (ext.nuvizz) c.nuvizz.set(String(ext.nuvizz), r.id);
      if (ext.motive) c.motive.set(String(ext.motive), r.id);
      if (r.defaultTruck) c.truck.set(String(r.defaultTruck), r.id);
    }
    return c;
  }, [rows]);

  // Auto-match name -> source values (one-shot on mount; manual edits override)
  const [autoMatched, setAutoMatched] = useState(false);
  React.useEffect(() => {
    if (autoMatched) return;
    if (rows.length === 0) return;

    const nameKey = s => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
    const lastTokens = s => {
      const k = nameKey(s);
      const parts = k.split(" ");
      return { full: k, last: parts[parts.length - 1], first: parts[0] };
    };

    // Build searchable indexes for each source
    const buildIndex = (set, displayMap) => {
      const list = [...set];
      // For sources where the value is an ID (Motive), match against the display name
      return list.map(v => {
        const display = displayMap?.get(v) || v;
        return { raw: v, ...lastTokens(display) };
      });
    };
    const b600Index = buildIndex(harvested.b600, harvested.b600Names);
    const nuvizzIndex = buildIndex(harvested.nuvizz, harvested.nuvizzDisplay);
    const motiveIndex = buildIndex(harvested.motive, harvested.motiveNames);

    const findMatch = (driverName, index, alreadyClaimed) => {
      if (!driverName) return null;
      const t = lastTokens(driverName);
      // 1. exact full-name match
      let m = index.find(o => o.full === t.full);
      if (m && !alreadyClaimed.has(m.raw)) return m.raw;
      // 2. last name + first initial
      const firstInitial = t.first.charAt(0);
      m = index.find(o => o.last === t.last && o.first.charAt(0) === firstInitial);
      if (m && !alreadyClaimed.has(m.raw)) return m.raw;
      // 3. last name only (only if unique)
      const lastMatches = index.filter(o => o.last === t.last);
      if (lastMatches.length === 1 && !alreadyClaimed.has(lastMatches[0].raw)) return lastMatches[0].raw;
      return null;
    };

    const claimedB600 = new Set();
    const claimedNuvizz = new Set();
    const claimedMotive = new Set();
    let anyChange = false;
    rows.forEach((r, i) => {
      const driver = r.actualDriverName;
      if (!driver) return;
      const ext = r.externalIds || {};
      const patch = { externalIds: { ...ext } };
      let rowChanged = false;
      if (!ext.b600) {
        const m = findMatch(driver, b600Index, claimedB600);
        if (m) { patch.externalIds.b600 = m; claimedB600.add(m); rowChanged = true; }
      } else { claimedB600.add(ext.b600); }
      if (!ext.nuvizz) {
        const m = findMatch(driver, nuvizzIndex, claimedNuvizz);
        if (m) { patch.externalIds.nuvizz = m; claimedNuvizz.add(m); rowChanged = true; }
      } else { claimedNuvizz.add(ext.nuvizz); }
      if (!ext.motive) {
        const m = findMatch(driver, motiveIndex, claimedMotive);
        if (m) { patch.externalIds.motive = m; claimedMotive.add(m); rowChanged = true; }
      } else { claimedMotive.add(ext.motive); }
      if (rowChanged) { updateRow(i, patch); anyChange = true; }
    });
    setAutoMatched(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows.length, harvested]);

  const willSave = rows.filter(r => r.actualDriverName).length;
  const matchStats = useMemo(() => {
    let b600 = 0, nuvizz = 0, motive = 0, truck = 0;
    for (const r of rows) {
      if (r.actualDriverName) {
        if (r.externalIds?.b600) b600++;
        if (r.externalIds?.nuvizz) nuvizz++;
        if (r.externalIds?.motive) motive++;
        if (r.defaultTruck) truck++;
      }
    }
    return { b600, nuvizz, motive, truck };
  }, [rows]);

  const headStyle = {
    padding: "6px 10px", fontSize: 9, fontWeight: 700, textTransform: "uppercase",
    letterSpacing: "0.05em", color: DH_T.textMuted, background: DH_T.bgSurface,
    textAlign: "left", borderBottom: `1px solid ${DH_T.border}`,
    position: "sticky", top: 0, zIndex: 2, whiteSpace: "nowrap",
  };
  const cellStyle = {
    padding: "5px 8px", fontSize: 11, borderBottom: `1px solid ${DH_T.borderLight}`,
    verticalAlign: "middle",
  };
  const numStyle = { ...cellStyle, fontFamily: "ui-monospace, monospace", fontSize: 10, color: DH_T.textMuted, textAlign: "right" };

  const updateExt = (i, system, value) => {
    const r = rows[i];
    const newExt = { ...(r.externalIds || {}), [system]: value || null };
    updateRow(i, { externalIds: newExt });
  };

  return React.createElement("div", null,
    // Header summary
    React.createElement("div", {
      style: { background: DH_T.brandPale, border: `1px solid ${DH_T.brand}`, borderRadius: DH_T.radius, padding: 14, marginBottom: 12, display: "flex", gap: 16, alignItems: "center", flexWrap: "wrap" }
    },
      React.createElement("div", { style: { fontSize: 24 } }, "🔍"),
      React.createElement("div", { style: { flex: 1, minWidth: 240 } },
        React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text } },
          `Extracted ${rows.length} entries · ${w2Count} W2 · ${rows.length - w2Count} 1099`),
        React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 2 } },
          `Auto-matched ${matchStats.b600} B600 · ${matchStats.nuvizz} NuVizz · ${matchStats.motive} Motive · ${matchStats.truck} Trucks (from Fleet Mgmt).`,
          llcUnresolved > 0 ? ` ⚠️ ${llcUnresolved} LLC payees still need driver names.` : "")
      ),
      React.createElement("div", { style: { display: "flex", gap: 12, fontSize: 10, color: DH_T.textMuted } },
        React.createElement(StatPill, { label: "B600 source", value: harvested.b600.size }),
        React.createElement(StatPill, { label: "NuVizz source", value: harvested.nuvizz.size }),
        React.createElement(StatPill, { label: "Motive source", value: harvested.motive.size }),
        React.createElement(StatPill, { label: "Trucks source", value: harvested.truck.size }),
      )
    ),

    // Big table
    React.createElement("div", {
      style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, overflow: "auto", maxHeight: "calc(100vh - 320px)" }
    },
      React.createElement("table", { style: { width: "100%", borderCollapse: "collapse", minWidth: 1500 } },
        React.createElement("thead", null,
          React.createElement("tr", null,
            React.createElement("th", { style: { ...headStyle, minWidth: 60 } }, "Src"),
            React.createElement("th", { style: { ...headStyle, minWidth: 200 } }, "Payee on Check"),
            React.createElement("th", { style: { ...headStyle, minWidth: 200 } }, "Driver / Person Name"),
            React.createElement("th", { style: { ...headStyle, minWidth: 120 } }, "Role"),
            React.createElement("th", { style: { ...headStyle, minWidth: 90, textAlign: "right" } }, "Pay"),
            React.createElement("th", { style: { ...headStyle, minWidth: 90, textAlign: "right" } }, "YTD"),
            React.createElement("th", { style: { ...headStyle, minWidth: 80 } }, "Payroll"),
            React.createElement("th", { style: { ...headStyle, minWidth: 180 } }, "B600"),
            React.createElement("th", { style: { ...headStyle, minWidth: 180 } }, "NuVizz"),
            React.createElement("th", { style: { ...headStyle, minWidth: 130 } }, "Motive"),
            React.createElement("th", { style: { ...headStyle, minWidth: 100 } }, "Truck"),
            React.createElement("th", { style: { ...headStyle, minWidth: 32 } }, "")
          )
        ),
        React.createElement("tbody", null,
          rows.map((r, i) => {
            const ext = r.externalIds || {};
            const needsName = r.isLLC && !r.actualDriverName;
            return React.createElement("tr", {
              key: i,
              style: needsName ? { background: DH_T.yellowBg + "40" } : {}
            },
              // Source badge
              React.createElement("td", { style: cellStyle },
                React.createElement("span", {
                  style: { fontSize: 9, fontWeight: 700, padding: "2px 6px", borderRadius: 4, background: r.source === "W2" ? DH_T.blueBg : DH_T.purpleBg, color: r.source === "W2" ? DH_T.blueText : DH_T.purpleText }
                }, r.source)
              ),
              // Payee on Check (read-only label)
              React.createElement("td", {
                style: { ...cellStyle, fontWeight: 600, color: r.isLLC ? DH_T.purpleText : DH_T.text }
              }, r.fullName),
              // Driver / Person Name
              //   - W2 / personal-name 1099: read-only label
              //   - LLC 1099: typeahead pulling from NuVizz drivers (the only
              //     source that links a contractor LLC to actual stops/pay)
              React.createElement("td", { style: cellStyle },
                r.isLLC
                  ? React.createElement(NuVizzNameTypeahead, {
                      value: r.actualDriverName,
                      options: harvested.nuvizz,
                      claimedNames: claimed.nuvizz,
                      employeeId: r.id,
                      onChange: nm => {
                        // Setting the driver name from NuVizz also claims the
                        // NuVizz column (they're literally the same string).
                        updateRow(i, {
                          actualDriverName: nm || null,
                          externalIds: {
                            ...(r.externalIds || {}),
                            nuvizz: nm || null,
                          },
                        });
                      },
                    })
                  : React.createElement("span", { style: { color: DH_T.text } }, r.actualDriverName)
              ),
              // Role
              React.createElement("td", { style: cellStyle },
                React.createElement("select", {
                  value: r.role, onChange: e => updateRow(i, { role: e.target.value }),
                  style: { padding: "3px 5px", fontSize: 10, border: `1px solid ${DH_T.border}`, borderRadius: 4, fontFamily: "inherit", background: "#fff" }
                }, ROLE_OPTIONS.map(o => React.createElement("option", { key: o.id, value: o.id }, o.label)))
              ),
              // Pay
              React.createElement("td", { style: numStyle },
                r.payRate
                  ? `$${r.payRate.toFixed(r.payType === "hourly" ? 2 : 0)}${r.payType === "hourly" ? "/hr" : "/wk"}`
                  : "—"
              ),
              // YTD
              React.createElement("td", { style: numStyle },
                r.ytdGross ? "$" + Math.round(r.ytdGross).toLocaleString() : "—"
              ),
              // Payroll ID (read-only — comes from PDF)
              React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace", fontSize: 10, color: DH_T.text } },
                ext.payroll || "—"
              ),
              // B600 typeahead
              React.createElement("td", { style: cellStyle },
                React.createElement(TypeaheadCell, {
                  value: ext.b600, employeeId: r.id,
                  options: harvested.b600, displayMap: harvested.b600Names,
                  claimed: claimed.b600,
                  onChange: v => updateExt(i, "b600", v),
                  placeholder: "—",
                })
              ),
              // NuVizz typeahead
              React.createElement("td", { style: cellStyle },
                React.createElement(TypeaheadCell, {
                  value: ext.nuvizz, employeeId: r.id,
                  options: harvested.nuvizz, displayMap: harvested.nuvizzDisplay,
                  claimed: claimed.nuvizz,
                  onChange: v => updateExt(i, "nuvizz", v),
                  placeholder: "—",
                })
              ),
              // Motive typeahead (preloaded eagerly on mount)
              React.createElement("td", { style: cellStyle },
                React.createElement(TypeaheadCell, {
                  value: ext.motive, employeeId: r.id,
                  options: harvested.motive, displayMap: harvested.motiveNames,
                  claimed: claimed.motive,
                  onChange: v => updateExt(i, "motive", v),
                  placeholder: "—", monoFont: true,
                })
              ),
              // Truck typeahead
              React.createElement("td", { style: cellStyle },
                React.createElement(TypeaheadCell, {
                  value: r.defaultTruck, employeeId: r.id,
                  options: harvested.truck, displayMap: new Map(),
                  claimed: claimed.truck,
                  onChange: v => updateRow(i, { defaultTruck: v }),
                  placeholder: "—", monoFont: true,
                })
              ),
              // Remove
              React.createElement("td", { style: cellStyle },
                React.createElement("button", {
                  onClick: () => removeRow(i),
                  title: "Remove this row",
                  style: { padding: "2px 6px", fontSize: 10, color: DH_T.textMuted, background: "transparent", border: "none", cursor: "pointer", fontFamily: "inherit" }
                }, "✕")
              )
            );
          })
        )
      )
    ),

    React.createElement("div", { style: { marginTop: 14, display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 } },
      React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted } },
        `Will save ${willSave} of ${rows.length} entries`,
        rows.length - willSave > 0 ? ` (${rows.length - willSave} skipped — set driver names above)` : ""),
      React.createElement("div", { style: { display: "flex", gap: 8 } },
        React.createElement("button", {
          onClick: onBack,
          style: { padding: "7px 13px", fontSize: 12, fontWeight: 600, color: DH_T.textMuted, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
        }, "← Re-upload"),
        React.createElement("button", {
          onClick: onSave, disabled: willSave === 0,
          style: { padding: "8px 16px", fontSize: 12, fontWeight: 700, color: "#fff", background: willSave === 0 ? DH_T.textDim : DH_T.brand, border: "none", borderRadius: 6, cursor: willSave === 0 ? "not-allowed" : "pointer", fontFamily: "inherit" }
        }, `Create roster (${willSave}) →`)
      )
    )
  );
}

function StatPill({ label, value }) {
  return React.createElement("div", { style: { display: "flex", flexDirection: "column", alignItems: "end" } },
    React.createElement("div", { style: { fontSize: 14, fontWeight: 700, color: DH_T.text } }, value),
    React.createElement("div", { style: { fontSize: 9, color: DH_T.textMuted, textTransform: "uppercase", letterSpacing: "0.05em" } }, label)
  );
}

// ─── Editor mode (post-bootstrap, permanent) ────────────────────────
function EmployeeEditor({ data, unmapped, onRefresh, onBootstrap }) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("all");
  const [pendingChanges, setPendingChanges] = useState({}); // { employeeId: patch }
  const [saving, setSaving] = useState(false);
  const [adding, setAdding] = useState(false);
  const [fleetMgmt, setFleetMgmt] = useState(null);
  const [motiveDrivers, setMotiveDrivers] = useState(null);

  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await fetch("/api/fleet-management?action=all");
        if (r.ok && !cancelled) setFleetMgmt(await r.json());
      } catch (e) { /* silent */ }
      try {
        const r = await fetch("/.netlify/functions/marginiq-motive?action=drivers");
        if (r.ok && !cancelled) {
          const j = await r.json();
          setMotiveDrivers(j.drivers || []);
        }
      } catch (e) { /* silent */ }
    })();
    return () => { cancelled = true; };
  }, []);

  const harvested = useMemo(() => {
    const h = harvestSourceValues(data, fleetMgmt);
    if (motiveDrivers && motiveDrivers.length > 0) {
      for (const d of motiveDrivers) {
        const id = String(d.id || "");
        const nm = d.first_name && d.last_name ? `${d.first_name} ${d.last_name}` : "";
        if (id) {
          h.motive.add(id);
          if (nm) h.motiveNames.set(id, nm);
        }
      }
    }
    return h;
  }, [data, fleetMgmt, motiveDrivers]);
  const claimed = useMemo(() => indexClaimedValues(data.employees), [data.employees]);

  // Apply pending edits on top of saved data for instant feedback
  const liveEmployees = useMemo(() => {
    return data.employees.map(e => {
      const patch = pendingChanges[e.id];
      if (!patch) return e;
      return { ...e, ...patch, externalIds: { ...(e.externalIds || {}), ...(patch.externalIds || {}) } };
    });
  }, [data.employees, pendingChanges]);

  const filtered = useMemo(() => {
    let list = liveEmployees;
    if (filter === "unmapped") {
      list = list.filter(e => unmapped.some(u => u.id === e.id));
    } else if (filter.startsWith("role:")) {
      list = list.filter(e => e.role === filter.slice(5));
    } else if (filter === "drivers") {
      list = list.filter(e => ["driver", "owner_op", "shuttle_driver"].includes(e.role));
    }
    if (search.trim()) {
      const q = search.toLowerCase();
      list = list.filter(e => (e.fullName || "").toLowerCase().includes(q));
    }
    return list.sort((a, b) => (a.fullName || "").localeCompare(b.fullName || ""));
  }, [liveEmployees, filter, search, unmapped]);

  const updateField = (employeeId, field, value) => {
    setPendingChanges(p => ({ ...p, [employeeId]: { ...(p[employeeId] || {}), [field]: value } }));
  };
  const updateExternal = (employeeId, system, value) => {
    setPendingChanges(p => ({
      ...p,
      [employeeId]: {
        ...(p[employeeId] || {}),
        externalIds: { ...((p[employeeId] || {}).externalIds || {}), [system]: value || null },
      },
    }));
  };

  const dirtyCount = Object.keys(pendingChanges).length;

  const saveAll = async () => {
    if (dirtyCount === 0) return;
    setSaving(true);
    try {
      for (const [id, patch] of Object.entries(pendingChanges)) {
        const existing = data.employees.find(e => e.id === id);
        await DH.saveEmployee(id, {
          ...patch,
          externalIds: { ...(existing?.externalIds || {}), ...(patch.externalIds || {}) },
        });
      }
      setPendingChanges({});
      onRefresh();
    } finally { setSaving(false); }
  };

  return React.createElement("div", null,
    // Header
    React.createElement("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "end", marginBottom: 14 } },
      React.createElement("div", null,
        React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Employee Mapping"),
        React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
          `${data.employees.length} employees · click any cell to map it to a value from that data source`)
      ),
      React.createElement("div", { style: { display: "flex", gap: 8, alignItems: "center" } },
        dirtyCount > 0 && React.createElement("div", { style: { fontSize: 11, color: DH_T.yellowText, fontWeight: 600 } }, `${dirtyCount} unsaved`),
        dirtyCount > 0 && React.createElement("button", {
          onClick: saveAll, disabled: saving,
          style: { padding: "6px 12px", fontSize: 11, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: saving ? "wait" : "pointer", fontFamily: "inherit", opacity: saving ? 0.6 : 1 }
        }, saving ? "Saving…" : "Save all"),
        React.createElement("button", {
          onClick: () => setAdding(true),
          style: { padding: "6px 12px", fontSize: 11, fontWeight: 600, color: DH_T.brand, background: "transparent", border: `1px solid ${DH_T.brand}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
        }, "+ Add row")
      )
    ),

    // Filter bar
    React.createElement("div", { style: { display: "flex", gap: 8, marginBottom: 10, alignItems: "center" } },
      React.createElement("input", {
        type: "text", placeholder: "Search employees…",
        value: search, onChange: e => setSearch(e.target.value),
        style: { padding: "6px 10px", fontSize: 12, border: `1px solid ${DH_T.border}`, borderRadius: 6, width: 240, fontFamily: "inherit", outline: "none" }
      }),
      React.createElement("select", {
        value: filter, onChange: e => setFilter(e.target.value),
        style: { padding: "6px 8px", fontSize: 12, border: `1px solid ${DH_T.border}`, borderRadius: 6, fontFamily: "inherit" }
      },
        React.createElement("option", { value: "all" }, "All employees"),
        React.createElement("option", { value: "drivers" }, "Drivers (all types)"),
        React.createElement("option", { value: "unmapped" }, "Missing system links"),
        ROLE_OPTIONS.filter(r => r.id !== "unknown").map(r =>
          React.createElement("option", { key: r.id, value: `role:${r.id}` }, r.label))
      ),
      React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted } },
        `${filtered.length} shown`),
      React.createElement("div", { style: { flex: 1 } }),
      React.createElement("button", {
        onClick: onBootstrap,
        title: "Re-import from payroll PDFs",
        style: { padding: "5px 10px", fontSize: 10, fontWeight: 600, color: DH_T.textMuted, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
      }, "📄 Import payroll PDFs")
    ),

    // The big spreadsheet table
    React.createElement(SpreadsheetTable, {
      employees: filtered, harvested, claimed,
      updateField, updateExternal,
      data,
    }),

    // Add row modal
    adding && React.createElement(AddRowModal, {
      onClose: () => setAdding(false),
      onSave: async (newEmployee) => {
        await DH.saveEmployee(newEmployee.id, newEmployee);
        setAdding(false);
        onRefresh();
      },
    })
  );
}

// ─── Spreadsheet Table ──────────────────────────────────────────────
function SpreadsheetTable({ employees, harvested, claimed, updateField, updateExternal, data }) {
  if (employees.length === 0) {
    return React.createElement("div", {
      style: { padding: 30, textAlign: "center", background: DH_T.bgCard, border: `1px dashed ${DH_T.border}`, borderRadius: DH_T.radius, fontSize: 12, color: DH_T.textMuted }
    }, "No employees match the current filter.");
  }

  const headStyle = {
    padding: "6px 10px", fontSize: 9, fontWeight: 700, textTransform: "uppercase",
    letterSpacing: "0.05em", color: DH_T.textMuted, background: DH_T.bgSurface,
    textAlign: "left", borderBottom: `1px solid ${DH_T.border}`,
    position: "sticky", top: 0, zIndex: 2,
  };
  const cellStyle = {
    padding: "6px 8px", fontSize: 11, borderBottom: `1px solid ${DH_T.borderLight}`,
    verticalAlign: "middle",
  };

  return React.createElement("div", {
    style: {
      background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius,
      overflow: "auto", maxHeight: "calc(100vh - 280px)",
    }
  },
    React.createElement("table", { style: { width: "100%", borderCollapse: "collapse", minWidth: 1300 } },
      React.createElement("thead", null,
        React.createElement("tr", null,
          React.createElement("th", { style: { ...headStyle, minWidth: 200 } }, "Driver / Employee"),
          React.createElement("th", { style: { ...headStyle, minWidth: 130 } }, "Role"),
          React.createElement("th", { style: { ...headStyle, minWidth: 110 } }, "Payroll"),
          React.createElement("th", { style: { ...headStyle, minWidth: 180 } }, "B600"),
          React.createElement("th", { style: { ...headStyle, minWidth: 180 } }, "NuVizz"),
          React.createElement("th", { style: { ...headStyle, minWidth: 120 } }, "Motive"),
          React.createElement("th", { style: { ...headStyle, minWidth: 100 } }, "Truck #"),
          React.createElement("th", { style: { ...headStyle, minWidth: 90 } }, "Status")
        )
      ),
      React.createElement("tbody", null,
        employees.map(emp => React.createElement(SpreadsheetRow, {
          key: emp.id, emp, harvested, claimed,
          updateField, updateExternal, data,
          cellStyle,
        }))
      )
    )
  );
}

function SpreadsheetRow({ emp, harvested, claimed, updateField, updateExternal, data, cellStyle }) {
  const role = ROLE_BY_ID[emp.role] || ROLE_BY_ID.unknown;
  const ext = emp.externalIds || {};

  return React.createElement("tr", { style: { transition: "background 0.1s" } },
    // Driver name (editable inline)
    React.createElement("td", { style: { ...cellStyle, fontWeight: 600, color: DH_T.text } },
      React.createElement("input", {
        type: "text", value: emp.fullName || "",
        onChange: e => updateField(emp.id, "fullName", e.target.value),
        style: { padding: "3px 6px", fontSize: 11, border: "1px solid transparent", borderRadius: 4, fontFamily: "inherit", width: "100%", background: "transparent", fontWeight: 600, outline: "none" },
        onFocus: e => e.target.style.border = `1px solid ${DH_T.border}`,
        onBlur: e => e.target.style.border = "1px solid transparent",
      })
    ),

    // Role (select)
    React.createElement("td", { style: cellStyle },
      React.createElement("select", {
        value: emp.role || "unknown", onChange: e => updateField(emp.id, "role", e.target.value),
        style: { padding: "3px 6px", fontSize: 10, border: `1px solid ${role.bg === DH_T.bgSurface ? DH_T.yellow : DH_T.border}`, borderRadius: 4, fontFamily: "inherit", background: role.bg, color: role.text, fontWeight: 600, cursor: "pointer" }
      }, ROLE_OPTIONS.map(o => React.createElement("option", { key: o.id, value: o.id }, o.label)))
    ),

    // Payroll ID (typeahead)
    React.createElement("td", { style: cellStyle },
      React.createElement(TypeaheadCell, {
        value: ext.payroll, employeeId: emp.id,
        options: harvested.payroll, displayMap: harvested.payrollNames,
        claimed: claimed.payroll,
        onChange: v => updateExternal(emp.id, "payroll", v),
        placeholder: "—", monoFont: true,
      })
    ),

    // B600
    React.createElement("td", { style: cellStyle },
      React.createElement(TypeaheadCell, {
        value: ext.b600, employeeId: emp.id,
        options: harvested.b600, displayMap: harvested.b600Names,
        claimed: claimed.b600,
        onChange: v => updateExternal(emp.id, "b600", v),
        placeholder: "—",
      })
    ),

    // NuVizz
    React.createElement("td", { style: cellStyle },
      React.createElement(TypeaheadCell, {
        value: ext.nuvizz, employeeId: emp.id,
        options: harvested.nuvizz, displayMap: harvested.nuvizzDisplay,
        claimed: claimed.nuvizz,
        onChange: v => updateExternal(emp.id, "nuvizz", v),
        placeholder: "—",
      })
    ),

    // Motive (preloaded eagerly in EmployeeEditor)
    React.createElement("td", { style: cellStyle },
      React.createElement(TypeaheadCell, {
        value: ext.motive, employeeId: emp.id,
        options: harvested.motive, displayMap: harvested.motiveNames,
        claimed: claimed.motive,
        onChange: v => updateExternal(emp.id, "motive", v),
        placeholder: "—", monoFont: true,
      })
    ),

    // Truck (assigned default)
    React.createElement("td", { style: cellStyle },
      React.createElement(TypeaheadCell, {
        value: emp.defaultTruck, employeeId: emp.id,
        options: harvested.truck, displayMap: new Map(),
        claimed: claimed.truck,
        onChange: v => updateField(emp.id, "defaultTruck", v),
        placeholder: "—", monoFont: true,
      })
    ),

    // Status
    React.createElement("td", { style: cellStyle },
      React.createElement("select", {
        value: emp.status || "active", onChange: e => updateField(emp.id, "status", e.target.value),
        style: { padding: "3px 6px", fontSize: 10, border: `1px solid ${DH_T.border}`, borderRadius: 4, fontFamily: "inherit", background: emp.status === "terminated" ? DH_T.redBg : DH_T.greenBg, color: emp.status === "terminated" ? DH_T.redText : DH_T.greenText, fontWeight: 600 }
      },
        React.createElement("option", { value: "active" }, "Active"),
        React.createElement("option", { value: "inactive" }, "Inactive"),
        React.createElement("option", { value: "terminated" }, "Terminated"),
        React.createElement("option", { value: "on_leave" }, "On Leave"),
      )
    )
  );
}

// ─── Typeahead Cell (the heart of the editor) ───────────────────────
function TypeaheadCell({ value, employeeId, options, displayMap, claimed, onChange, placeholder, monoFont, loadOptions }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [dynamicOpts, setDynamicOpts] = useState(null);
  const [dynamicDisplay, setDynamicDisplay] = useState(null);
  const [loading, setLoading] = useState(false);
  const cellRef = React.useRef();
  const inputRef = React.useRef();

  const effectiveOptions = dynamicOpts || options;
  const effectiveDisplay = dynamicDisplay || displayMap;

  React.useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
    if (open && loadOptions && !dynamicOpts) {
      setLoading(true);
      loadOptions().then(result => {
        if (result) {
          setDynamicOpts(result.options);
          setDynamicDisplay(result.displayMap);
        }
        setLoading(false);
      });
    }
  }, [open]);

  React.useEffect(() => {
    if (!open) return;
    const handler = e => {
      if (cellRef.current && !cellRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Filter and rank options
  const filtered = useMemo(() => {
    const q = query.toLowerCase().trim();
    const arr = [...effectiveOptions];
    const matches = q
      ? arr.filter(o => {
          const display = effectiveDisplay.get(o) || "";
          return o.toLowerCase().includes(q) || display.toLowerCase().includes(q);
        })
      : arr;
    // Sort: current value first, unclaimed before claimed, alpha within
    return matches.sort((a, b) => {
      if (a === value) return -1;
      if (b === value) return 1;
      const aClaimedByOther = claimed.has(a) && claimed.get(a) !== employeeId;
      const bClaimedByOther = claimed.has(b) && claimed.get(b) !== employeeId;
      if (aClaimedByOther !== bClaimedByOther) return aClaimedByOther ? 1 : -1;
      return String(a).localeCompare(String(b));
    });
  }, [effectiveOptions, effectiveDisplay, query, value, claimed, employeeId]);

  const display = value
    ? (effectiveDisplay.get(value) ? `${value}` : value)
    : null;

  const cellInner = open
    ? React.createElement("div", { ref: cellRef, style: { position: "relative" } },
        React.createElement("input", {
          ref: inputRef, type: "text", value: query,
          onChange: e => setQuery(e.target.value),
          placeholder: "Type to filter…",
          style: { width: "100%", padding: "3px 6px", fontSize: 11, border: `1px solid ${DH_T.brand}`, borderRadius: 4, fontFamily: "inherit", outline: "none", background: "#fff" }
        }),
        React.createElement("div", {
          style: {
            position: "absolute", top: "100%", left: 0, marginTop: 2, minWidth: 240, maxWidth: 360, maxHeight: 280,
            overflowY: "auto", background: "#fff", border: `1px solid ${DH_T.border}`, borderRadius: 6,
            boxShadow: "0 8px 24px rgba(0,0,0,0.12)", zIndex: 50,
          }
        },
          loading && React.createElement("div", { style: { padding: 10, fontSize: 10, color: DH_T.textMuted, textAlign: "center" } }, "Loading…"),
          !loading && filtered.length === 0 && React.createElement("div", { style: { padding: 10, fontSize: 10, color: DH_T.textMuted, textAlign: "center" } },
            effectiveOptions.size === 0 ? "No values from this source yet" : "No matches"),
          !loading && filtered.slice(0, 50).map(opt => {
            const claimedBy = claimed.get(opt);
            const claimedByOther = claimedBy && claimedBy !== employeeId;
            const isCurrent = opt === value;
            const dispName = effectiveDisplay.get(opt);
            return React.createElement("button", {
              key: opt,
              onClick: () => { onChange(opt); setOpen(false); setQuery(""); },
              style: {
                display: "block", width: "100%", textAlign: "left",
                padding: "5px 9px", fontSize: 11, fontFamily: "inherit",
                background: isCurrent ? DH_T.brandPale : "transparent",
                color: claimedByOther ? DH_T.textDim : DH_T.text,
                border: "none", borderBottom: `1px solid ${DH_T.borderLight}`,
                cursor: "pointer",
              }
            },
              React.createElement("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 } },
                React.createElement("div", null,
                  isCurrent && React.createElement("span", { style: { color: DH_T.brand, fontWeight: 700, marginRight: 4 } }, "✓"),
                  React.createElement("span", { style: { fontFamily: monoFont ? "ui-monospace, monospace" : "inherit" } }, opt),
                  dispName && dispName !== opt && React.createElement("span", { style: { color: DH_T.textMuted, marginLeft: 6, fontSize: 10 } }, dispName)
                ),
                claimedByOther && React.createElement("span", {
                  style: { fontSize: 9, color: DH_T.textDim, fontStyle: "italic" }
                }, "claimed")
              )
            );
          }),
          value && React.createElement("button", {
            onClick: () => { onChange(null); setOpen(false); setQuery(""); },
            style: { display: "block", width: "100%", textAlign: "left", padding: "5px 9px", fontSize: 10, fontFamily: "inherit", background: DH_T.bgSurface, color: DH_T.redText, border: "none", cursor: "pointer", fontWeight: 600 }
          }, "× Clear value")
        )
      )
    : React.createElement("button", {
        onClick: () => setOpen(true),
        style: {
          width: "100%", padding: "3px 6px", fontSize: 11, fontFamily: monoFont ? "ui-monospace, monospace" : "inherit",
          textAlign: "left", background: "transparent", border: "1px solid transparent", borderRadius: 4,
          cursor: "pointer", color: value ? DH_T.text : DH_T.textDim,
        },
        onMouseEnter: e => e.target.style.border = `1px solid ${DH_T.border}`,
        onMouseLeave: e => e.target.style.border = "1px solid transparent",
      }, display || placeholder || "—");

  return cellInner;
}

// ─── NuVizz Name Typeahead ─────────────────────────────────────────
// Specialized typeahead for the LLC contractor name field. Pulls names
// from NuVizz (the only source that ties a 1099 LLC to actual stops/pay).
// Picking a name here ALSO claims the NuVizz column for that row.
function NuVizzNameTypeahead({ value, options, claimedNames, employeeId, onChange }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const cellRef = React.useRef();
  const inputRef = React.useRef();

  React.useEffect(() => {
    if (open && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [open]);

  React.useEffect(() => {
    if (!open) return;
    const handler = e => {
      if (cellRef.current && !cellRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const filtered = useMemo(() => {
    const q = query.toLowerCase().trim();
    const arr = [...options];
    const matches = q ? arr.filter(o => o.toLowerCase().includes(q)) : arr;
    // Sort: current value first, unclaimed before claimed, alpha within
    return matches.sort((a, b) => {
      if (a === value) return -1;
      if (b === value) return 1;
      const aClaimedByOther = claimedNames.has(a) && claimedNames.get(a) !== employeeId;
      const bClaimedByOther = claimedNames.has(b) && claimedNames.get(b) !== employeeId;
      if (aClaimedByOther !== bClaimedByOther) return aClaimedByOther ? 1 : -1;
      return String(a).localeCompare(String(b));
    });
  }, [options, query, value, claimedNames, employeeId]);

  if (!open) {
    const hasValue = !!value;
    return React.createElement("button", {
      onClick: () => setOpen(true),
      style: {
        width: "100%", padding: "3px 7px", fontSize: 11, fontFamily: "inherit",
        textAlign: "left",
        background: hasValue ? "#fff" : DH_T.yellowBg,
        border: `1px solid ${hasValue ? DH_T.border : DH_T.yellow}`,
        borderRadius: 4, cursor: "pointer",
        color: hasValue ? DH_T.text : DH_T.yellowText, fontWeight: hasValue ? 400 : 600,
      }
    }, value || "Pick from NuVizz…");
  }

  return React.createElement("div", { ref: cellRef, style: { position: "relative" } },
    React.createElement("input", {
      ref: inputRef, type: "text", value: query,
      onChange: e => setQuery(e.target.value),
      placeholder: "Search NuVizz drivers…",
      style: { width: "100%", padding: "3px 7px", fontSize: 11, border: `1px solid ${DH_T.brand}`, borderRadius: 4, fontFamily: "inherit", outline: "none", background: "#fff" }
    }),
    React.createElement("div", {
      style: {
        position: "absolute", top: "100%", left: 0, marginTop: 2, minWidth: 240, maxWidth: 360, maxHeight: 280,
        overflowY: "auto", background: "#fff", border: `1px solid ${DH_T.border}`, borderRadius: 6,
        boxShadow: "0 8px 24px rgba(0,0,0,0.12)", zIndex: 50,
      }
    },
      filtered.length === 0 && React.createElement("div", {
        style: { padding: 10, fontSize: 10, color: DH_T.textMuted, textAlign: "center" }
      }, options.size === 0 ? "No NuVizz drivers loaded" : "No matches"),
      filtered.slice(0, 100).map(opt => {
        const claimedBy = claimedNames.get(opt);
        const claimedByOther = claimedBy && claimedBy !== employeeId;
        const isCurrent = opt === value;
        return React.createElement("button", {
          key: opt,
          onClick: () => { onChange(opt); setOpen(false); setQuery(""); },
          style: {
            display: "block", width: "100%", textAlign: "left",
            padding: "5px 9px", fontSize: 11, fontFamily: "inherit",
            background: isCurrent ? DH_T.brandPale : "transparent",
            color: claimedByOther ? DH_T.textDim : DH_T.text,
            border: "none", borderBottom: `1px solid ${DH_T.borderLight}`, cursor: "pointer",
          }
        },
          React.createElement("div", { style: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 } },
            React.createElement("div", null,
              isCurrent && React.createElement("span", { style: { color: DH_T.brand, fontWeight: 700, marginRight: 4 } }, "✓"),
              opt
            ),
            claimedByOther && React.createElement("span", {
              style: { fontSize: 9, color: DH_T.textDim, fontStyle: "italic" }
            }, "claimed")
          )
        );
      }),
      value && React.createElement("button", {
        onClick: () => { onChange(null); setOpen(false); setQuery(""); },
        style: { display: "block", width: "100%", textAlign: "left", padding: "5px 9px", fontSize: 10, fontFamily: "inherit", background: DH_T.bgSurface, color: DH_T.redText, border: "none", cursor: "pointer", fontWeight: 600 }
      }, "× Clear value")
    )
  );
}

// ─── Add Row modal ──────────────────────────────────────────────────
function AddRowModal({ onClose, onSave }) {
  const [name, setName] = useState("");
  const [role, setRole] = useState("driver");
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    const trimmed = name.trim();
    if (!trimmed) { alert("Name is required"); return; }
    setSaving(true);
    const id = dhKey(trimmed);
    await onSave({
      id,
      fullName: trimmed,
      firstName: trimmed.split(" ")[0],
      lastName: trimmed.split(" ").slice(1).join(" "),
      role, status: "active",
      externalIds: {},
      createdAt: new Date().toISOString(),
      createdBy: "datahub_ui_add_row",
    });
    setSaving(false);
  };

  return React.createElement("div", {
    onClick: onClose,
    style: { position: "fixed", inset: 0, background: "rgba(15,23,42,0.5)", backdropFilter: "blur(4px)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 200, padding: 20 }
  },
    React.createElement("div", {
      onClick: e => e.stopPropagation(),
      style: { background: DH_T.bgCard, borderRadius: DH_T.radius, padding: 22, maxWidth: 420, width: "100%", boxShadow: "0 20px 60px rgba(0,0,0,0.3)" }
    },
      React.createElement("div", { style: { fontSize: 16, fontWeight: 700, color: DH_T.text, marginBottom: 4 } }, "Add Employee"),
      React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginBottom: 14 } },
        "Add a new row. After saving, click cells in the editor to map external system IDs."),

      React.createElement("div", { style: { marginBottom: 12 } },
        React.createElement("label", { style: { fontSize: 10, fontWeight: 700, color: DH_T.textMuted, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 4, display: "block" } }, "Full Name"),
        React.createElement("input", {
          type: "text", value: name, onChange: e => setName(e.target.value),
          autoFocus: true, placeholder: "e.g. Marcus Williams",
          style: { padding: "7px 10px", fontSize: 13, border: `1px solid ${DH_T.border}`, borderRadius: 6, fontFamily: "inherit", width: "100%", boxSizing: "border-box", outline: "none" }
        })
      ),

      React.createElement("div", { style: { marginBottom: 18 } },
        React.createElement("label", { style: { fontSize: 10, fontWeight: 700, color: DH_T.textMuted, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 4, display: "block" } }, "Role"),
        React.createElement("select", {
          value: role, onChange: e => setRole(e.target.value),
          style: { padding: "7px 10px", fontSize: 13, border: `1px solid ${DH_T.border}`, borderRadius: 6, fontFamily: "inherit", width: "100%", boxSizing: "border-box" }
        }, ROLE_OPTIONS.filter(o => o.id !== "unknown").map(o =>
          React.createElement("option", { key: o.id, value: o.id }, o.label)
        ))
      ),

      React.createElement("div", { style: { display: "flex", gap: 8, justifyContent: "end" } },
        React.createElement("button", {
          onClick: onClose,
          style: { padding: "8px 14px", fontSize: 12, fontWeight: 600, color: DH_T.textMuted, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
        }, "Cancel"),
        React.createElement("button", {
          onClick: handleSave, disabled: saving || !name.trim(),
          style: { padding: "8px 16px", fontSize: 12, fontWeight: 700, color: "#fff", background: !name.trim() ? DH_T.textDim : DH_T.brand, border: "none", borderRadius: 6, cursor: !name.trim() ? "not-allowed" : "pointer", fontFamily: "inherit", opacity: saving ? 0.6 : 1 }
        }, saving ? "Adding…" : "Add Employee")
      )
    )
  );
}


// ═══ Vehicle Mapping ═══════════════════════════════════════════════
function VehicleMapping({ data, onRefresh }) {
  const [editing, setEditing] = useState(null);
  const [bootstrapping, setBootstrapping] = useState(false);

  // Discover trucks from fuel_by_truck data
  const discoveredTrucks = useMemo(() => {
    const set = new Set();
    for (const row of data.fuelByTruck) {
      if (row.truck_number) set.add(String(row.truck_number));
    }
    return Array.from(set).sort();
  }, [data.fuelByTruck]);

  const knownTrucks = new Set(data.vehicles.map(v => String(v.truckNumber || "")));
  const newTrucks = discoveredTrucks.filter(t => !knownTrucks.has(t));

  const runBootstrap = async () => {
    if (!newTrucks.length) return;
    if (!window.confirm(`Add ${newTrucks.length} trucks discovered in fuel data?\n\nYou can edit each one (type, VIN, Motive ID) after.`)) return;
    setBootstrapping(true);
    try {
      let saved = 0;
      for (const t of newTrucks) {
        const id = `truck_${t}`;
        const ok = await DH.saveVehicle(id, {
          truckNumber: t,
          unitType: "box_truck",
          status: "active",
          homeBase: "Buford",
          externalIds: {},
          createdAt: new Date().toISOString(),
          createdBy: "datahub_bootstrap",
        });
        if (ok) saved++;
      }
      alert(`Added ${saved} trucks. Set unit type, VIN, and Motive IDs in each row.`);
      onRefresh();
    } finally {
      setBootstrapping(false);
    }
  };

  const cellStyle = { padding: "8px 10px", fontSize: 11, borderBottom: `1px solid ${DH_T.borderLight}`, verticalAlign: "middle" };
  const headStyle = { padding: "6px 10px", fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: DH_T.textMuted, background: DH_T.bgSurface, textAlign: "left", borderBottom: `1px solid ${DH_T.border}` };

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16, display: "flex", justifyContent: "space-between", alignItems: "end" } },
      React.createElement("div", null,
        React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Vehicle Mapping"),
        React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
          `${data.vehicles.length} master vehicles · ${discoveredTrucks.length} truck numbers seen in fuel data`)
      ),
      data.vehicles.length > 0 && React.createElement("button", {
        onClick: () => setEditing("__new__"),
        style: { padding: "6px 12px", fontSize: 12, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
      }, "+ Add Vehicle")
    ),

    newTrucks.length > 0 && React.createElement("div", {
      style: { background: DH_T.brandPale, border: `1px solid ${DH_T.brand}`, borderRadius: DH_T.radius, padding: 14, marginBottom: 16, display: "flex", gap: 12, alignItems: "center" }
    },
      React.createElement("div", { style: { fontSize: 24 } }, "🚛"),
      React.createElement("div", { style: { flex: 1 } },
        React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text } },
          `${newTrucks.length} trucks discovered in fuel data, not yet in master`),
        React.createElement("div", { style: { fontSize: 10, color: DH_T.textMuted, marginTop: 2 } },
          "Trucks: " + newTrucks.slice(0, 8).join(", ") + (newTrucks.length > 8 ? `, +${newTrucks.length - 8} more` : ""))
      ),
      React.createElement("button", {
        onClick: runBootstrap, disabled: bootstrapping,
        style: { padding: "7px 12px", fontSize: 12, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: bootstrapping ? "wait" : "pointer", fontFamily: "inherit", opacity: bootstrapping ? 0.6 : 1 }
      }, bootstrapping ? "Adding…" : `Add ${newTrucks.length} →`)
    ),

    data.vehicles.length === 0 && newTrucks.length === 0 && React.createElement(EmptyHubState, {
      icon: "🚛", title: "No vehicles yet",
      subtitle: "Ingest fuel data to auto-discover trucks, or click + Add Vehicle above.",
    }),

    data.vehicles.length > 0 && React.createElement("div", { style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, overflow: "hidden" } },
      React.createElement("div", { style: { overflowX: "auto" } },
        React.createElement("table", { style: { width: "100%", borderCollapse: "collapse" } },
          React.createElement("thead", null,
            React.createElement("tr", null,
              React.createElement("th", { style: headStyle }, "Truck #"),
              React.createElement("th", { style: headStyle }, "Type"),
              React.createElement("th", { style: headStyle }, "VIN"),
              React.createElement("th", { style: headStyle }, "Motive ID"),
              React.createElement("th", { style: headStyle }, "Samsara"),
              React.createElement("th", { style: headStyle }, "Default Driver"),
              React.createElement("th", { style: headStyle }, "Status"),
              React.createElement("th", { style: headStyle }, "")
            )
          ),
          React.createElement("tbody", null,
            data.vehicles.sort((a, b) => String(a.truckNumber).localeCompare(String(b.truckNumber))).map(v => {
              const ext = v.externalIds || {};
              const driver = data.employees.find(e => e.id === v.defaultDriverId);
              const statusColors = {
                active: { bg: DH_T.greenBg, color: DH_T.greenText },
                maintenance: { bg: DH_T.yellowBg, color: DH_T.yellowText },
                oos: { bg: DH_T.redBg, color: DH_T.redText },
                sold: { bg: DH_T.bgSurface, color: DH_T.textMuted },
                spare: { bg: DH_T.bgSurface, color: DH_T.textMuted },
              };
              const sc = statusColors[v.status] || statusColors.active;
              return React.createElement("tr", { key: v.id },
                React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace", fontWeight: 700 } }, v.truckNumber),
                React.createElement("td", { style: cellStyle },
                  React.createElement("span", {
                    style: { fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 99, background: DH_T.bgSurface, color: DH_T.textMuted, textTransform: "uppercase", letterSpacing: "0.04em" }
                  }, (v.unitType || "—").replace("_", " "))
                ),
                React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace", fontSize: 10, color: DH_T.textMuted } }, v.vin || "—"),
                React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace", fontSize: 10 } }, ext.motive || React.createElement("span", { style: { color: DH_T.yellowText, fontWeight: 700 } }, "?")),
                React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace", fontSize: 10 } }, ext.samsara || React.createElement("span", { style: { color: DH_T.textDim } }, "—")),
                React.createElement("td", { style: cellStyle }, driver?.fullName || React.createElement("span", { style: { color: DH_T.textDim } }, "—")),
                React.createElement("td", { style: cellStyle },
                  React.createElement("span", {
                    style: { fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 99, background: sc.bg, color: sc.color, textTransform: "uppercase", letterSpacing: "0.04em" }
                  }, v.status || "active")
                ),
                React.createElement("td", { style: { ...cellStyle, textAlign: "right" } },
                  React.createElement("button", {
                    onClick: () => setEditing(v.id),
                    style: { padding: "3px 9px", fontSize: 10, fontWeight: 600, color: DH_T.brand, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 4, cursor: "pointer", fontFamily: "inherit" }
                  }, "Edit")
                )
              );
            })
          )
        )
      )
    ),

    editing && React.createElement(VehicleEditModal, {
      vehicle: editing === "__new__" ? null : data.vehicles.find(v => v.id === editing),
      employees: data.employees,
      onClose: () => setEditing(null),
      onSave: async (id, patch) => {
        await DH.saveVehicle(id, patch);
        setEditing(null);
        onRefresh();
      },
    })
  );
}

function VehicleEditModal({ vehicle, employees, onClose, onSave }) {
  const isNew = !vehicle;
  const [draft, setDraft] = useState({
    truckNumber: vehicle?.truckNumber || "",
    unitType: vehicle?.unitType || "box_truck",
    vin: vehicle?.vin || "",
    status: vehicle?.status || "active",
    defaultDriverId: vehicle?.defaultDriverId || "",
    externalIds: { ...(vehicle?.externalIds || {}) },
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    if (!draft.truckNumber) { alert("Truck number is required"); return; }
    setSaving(true);
    const id = vehicle?.id || `truck_${draft.truckNumber}`;
    await onSave(id, {
      truckNumber: draft.truckNumber,
      unitType: draft.unitType,
      vin: draft.vin,
      status: draft.status,
      defaultDriverId: draft.defaultDriverId || null,
      externalIds: draft.externalIds,
      ...(isNew ? { createdAt: new Date().toISOString(), createdBy: "datahub_ui", homeBase: "Buford" } : {}),
    });
    setSaving(false);
  };

  const inputStyle = { padding: "6px 8px", fontSize: 12, border: `1px solid ${DH_T.border}`, borderRadius: 6, fontFamily: "inherit", width: "100%", boxSizing: "border-box", outline: "none" };
  const labelStyle = { fontSize: 10, fontWeight: 700, color: DH_T.textMuted, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 4, display: "block" };

  return React.createElement("div", {
    onClick: onClose,
    style: { position: "fixed", inset: 0, background: "rgba(15,23,42,0.5)", backdropFilter: "blur(4px)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 200, padding: 20 }
  },
    React.createElement("div", {
      onClick: e => e.stopPropagation(),
      style: { background: DH_T.bgCard, borderRadius: DH_T.radius, padding: 20, maxWidth: 480, width: "100%", boxShadow: "0 20px 60px rgba(0,0,0,0.3)" }
    },
      React.createElement("div", { style: { fontSize: 16, fontWeight: 700, color: DH_T.text, marginBottom: 16 } }, isNew ? "Add Vehicle" : `Truck ${vehicle.truckNumber}`),

      React.createElement("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 12 } },
        React.createElement("div", null,
          React.createElement("label", { style: labelStyle }, "Truck #"),
          React.createElement("input", { type: "text", value: draft.truckNumber, onChange: e => setDraft(d => ({ ...d, truckNumber: e.target.value })), style: inputStyle, disabled: !isNew })
        ),
        React.createElement("div", null,
          React.createElement("label", { style: labelStyle }, "Type"),
          React.createElement("select", { value: draft.unitType, onChange: e => setDraft(d => ({ ...d, unitType: e.target.value })), style: inputStyle },
            React.createElement("option", { value: "box_truck" }, "Box Truck"),
            React.createElement("option", { value: "tractor" }, "Tractor"),
            React.createElement("option", { value: "trailer" }, "Trailer"),
            React.createElement("option", { value: "pickup" }, "Pickup"),
            React.createElement("option", { value: "forklift" }, "Forklift"),
          )
        )
      ),

      React.createElement("div", { style: { marginBottom: 12 } },
        React.createElement("label", { style: labelStyle }, "VIN"),
        React.createElement("input", { type: "text", value: draft.vin, onChange: e => setDraft(d => ({ ...d, vin: e.target.value })), style: { ...inputStyle, fontFamily: "ui-monospace, monospace" } })
      ),

      React.createElement("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 16 } },
        React.createElement("div", null,
          React.createElement("label", { style: labelStyle }, "Status"),
          React.createElement("select", { value: draft.status, onChange: e => setDraft(d => ({ ...d, status: e.target.value })), style: inputStyle },
            React.createElement("option", { value: "active" }, "Active"),
            React.createElement("option", { value: "maintenance" }, "Maintenance"),
            React.createElement("option", { value: "oos" }, "Out of Service"),
            React.createElement("option", { value: "spare" }, "Spare"),
            React.createElement("option", { value: "sold" }, "Sold"),
          )
        ),
        React.createElement("div", null,
          React.createElement("label", { style: labelStyle }, "Default Driver"),
          React.createElement("select", { value: draft.defaultDriverId, onChange: e => setDraft(d => ({ ...d, defaultDriverId: e.target.value })), style: inputStyle },
            React.createElement("option", { value: "" }, "—"),
            employees.filter(e => ["driver", "owner_op", "shuttle_driver"].includes(e.role)).map(e =>
              React.createElement("option", { key: e.id, value: e.id }, e.fullName))
          )
        )
      ),

      React.createElement("div", { style: { borderTop: `1px solid ${DH_T.borderLight}`, paddingTop: 14, marginBottom: 14 } },
        React.createElement("div", { style: { fontSize: 11, fontWeight: 700, color: DH_T.text, marginBottom: 8 } }, "External IDs"),
        React.createElement("div", { style: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 } },
          ["motive", "samsara"].map(key =>
            React.createElement("div", { key },
              React.createElement("label", { style: { ...labelStyle, fontSize: 9 } }, key),
              React.createElement("input", {
                type: "text", value: draft.externalIds[key] || "",
                onChange: e => setDraft(d => ({ ...d, externalIds: { ...d.externalIds, [key]: e.target.value } })),
                placeholder: "—",
                style: { ...inputStyle, fontFamily: "ui-monospace, monospace", fontSize: 11 },
              })
            )
          )
        )
      ),

      React.createElement("div", { style: { display: "flex", gap: 8, justifyContent: "end", paddingTop: 12, borderTop: `1px solid ${DH_T.borderLight}` } },
        React.createElement("button", { onClick: onClose, style: { padding: "7px 14px", fontSize: 12, fontWeight: 600, color: DH_T.textMuted, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 6, cursor: "pointer", fontFamily: "inherit" } }, "Cancel"),
        React.createElement("button", { onClick: handleSave, disabled: saving, style: { padding: "7px 14px", fontSize: 12, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: saving ? "wait" : "pointer", fontFamily: "inherit", opacity: saving ? 0.6 : 1 } }, saving ? "Saving…" : "Save")
      )
    )
  );
}

// ═══ Customer Master ═══════════════════════════════════════════════
function CustomerMaster({ data, onRefresh }) {
  const [editing, setEditing] = useState(null);

  // Reconcile customers — discover from existing data sources
  const discoveredCustomers = useMemo(() => {
    const set = new Set();
    // From customer_ap_contacts
    for (const c of data.apContacts) {
      if (c.customer_name) set.add(c.customer_name);
    }
    // From uline_weekly (always Uline if records exist)
    if (data.uline.length > 0) set.add("Uline");
    return Array.from(set);
  }, [data.apContacts, data.uline]);

  const knownNames = new Set(data.customers.map(c => c.name));
  const newCustomers = discoveredCustomers.filter(n => !knownNames.has(n));

  const runBootstrap = async () => {
    if (!newCustomers.length) return;
    if (!window.confirm(`Add ${newCustomers.length} customers from existing AP contacts and Uline data?`)) return;
    for (const name of newCustomers) {
      const id = dhKey(name);
      const apContact = data.apContacts.find(c => c.customer_name === name);
      await DH.saveCustomer(id, {
        name,
        accountCode: apContact?.account_code || "",
        customerType: name === "Uline" ? "contract" : "contract",
        status: "active",
        primaryContact: apContact ? { name: apContact.contact_name, email: apContact.email } : null,
        createdAt: new Date().toISOString(),
        createdBy: "datahub_bootstrap",
      });
    }
    onRefresh();
  };

  const cellStyle = { padding: "8px 10px", fontSize: 11, borderBottom: `1px solid ${DH_T.borderLight}` };
  const headStyle = { padding: "6px 10px", fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.05em", color: DH_T.textMuted, background: DH_T.bgSurface, textAlign: "left", borderBottom: `1px solid ${DH_T.border}` };

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16, display: "flex", justifyContent: "space-between", alignItems: "end" } },
      React.createElement("div", null,
        React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Customer Master"),
        React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
          `${data.customers.length} master customers · ${data.apContacts.length} AP contacts (legacy)`)
      )
    ),

    newCustomers.length > 0 && React.createElement("div", {
      style: { background: DH_T.brandPale, border: `1px solid ${DH_T.brand}`, borderRadius: DH_T.radius, padding: 14, marginBottom: 16, display: "flex", gap: 12, alignItems: "center" }
    },
      React.createElement("div", { style: { fontSize: 24 } }, "🏢"),
      React.createElement("div", { style: { flex: 1 } },
        React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text } },
          `${newCustomers.length} customers found in existing data`),
        React.createElement("div", { style: { fontSize: 10, color: DH_T.textMuted, marginTop: 2 } },
          newCustomers.slice(0, 8).join(", ") + (newCustomers.length > 8 ? `, +${newCustomers.length - 8} more` : ""))
      ),
      React.createElement("button", {
        onClick: runBootstrap,
        style: { padding: "7px 12px", fontSize: 12, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: "pointer", fontFamily: "inherit" }
      }, `Bootstrap ${newCustomers.length} →`)
    ),

    data.customers.length === 0 && newCustomers.length === 0 && React.createElement(EmptyHubState, {
      icon: "🏢", title: "No customers yet",
      subtitle: "Ingest Uline or set up customer AP contacts first.",
    }),

    data.customers.length > 0 && React.createElement("div", { style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, overflow: "hidden" } },
      React.createElement("table", { style: { width: "100%", borderCollapse: "collapse" } },
        React.createElement("thead", null,
          React.createElement("tr", null,
            React.createElement("th", { style: headStyle }, "Customer"),
            React.createElement("th", { style: headStyle }, "Code"),
            React.createElement("th", { style: headStyle }, "Type"),
            React.createElement("th", { style: headStyle }, "Status"),
            React.createElement("th", { style: headStyle }, "")
          )
        ),
        React.createElement("tbody", null,
          data.customers.sort((a, b) => (a.name || "").localeCompare(b.name || "")).map(c =>
            React.createElement("tr", { key: c.id },
              React.createElement("td", { style: { ...cellStyle, fontWeight: 600, color: DH_T.text } }, c.name),
              React.createElement("td", { style: { ...cellStyle, fontFamily: "ui-monospace, monospace" } }, c.accountCode || "—"),
              React.createElement("td", { style: cellStyle }, c.customerType || "contract"),
              React.createElement("td", { style: cellStyle },
                React.createElement("span", {
                  style: { fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 99, background: DH_T.greenBg, color: DH_T.greenText, textTransform: "uppercase" }
                }, c.status || "active")
              ),
              React.createElement("td", { style: { ...cellStyle, textAlign: "right" } },
                React.createElement("button", {
                  onClick: () => setEditing(c.id),
                  style: { padding: "3px 9px", fontSize: 10, fontWeight: 600, color: DH_T.brand, background: "transparent", border: `1px solid ${DH_T.border}`, borderRadius: 4, cursor: "pointer", fontFamily: "inherit" }
                }, "Edit")
              )
            )
          )
        )
      )
    )
  );
}

// ═══ Manual Upload (links to existing tabs) ════════════════════════
function ManualUpload() {
  const links = [
    { label: "NuVizz, Uline, DDIS, Payroll, Fuel uploads", desc: "Use the existing Data Ingest tab in MarginIQ — drag-drop CSVs and PDFs.", target: "ingest", cta: "Open Data Ingest →" },
    { label: "Auto-sync from Gmail", desc: "Pull NuVizz, Uline, payroll PDFs and FuelFox PDFs directly from your inbox.", target: "gmail", cta: "Open Gmail Sync →" },
    { label: "B600 Time Clock", desc: "Auto-pulls weekly. Manual CSV upload if API is offline — use Data Ingest.", target: "ingest", cta: "Open Data Ingest →" },
  ];

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16 } },
      React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Manual Upload"),
      React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
        "Existing ingestion tabs handle file uploads. The Data Hub will integrate them inline in v2.")
    ),
    React.createElement("div", { style: { display: "grid", gap: 10 } },
      links.map(link => React.createElement("div", {
        key: link.label,
        style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, padding: 16, display: "flex", justifyContent: "space-between", alignItems: "center", gap: 14 }
      },
        React.createElement("div", { style: { flex: 1 } },
          React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, link.label),
          React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 3 } }, link.desc)
        ),
        React.createElement("button", {
          onClick: () => { if (window.__marginiqSetTab) window.__marginiqSetTab(link.target); },
          style: { padding: "7px 12px", fontSize: 11, fontWeight: 700, color: "#fff", background: DH_T.brand, border: "none", borderRadius: 6, cursor: "pointer", fontFamily: "inherit", whiteSpace: "nowrap" }
        }, link.cta)
      ))
    )
  );
}

// ═══ Error Log ═════════════════════════════════════════════════════
function ErrorLog({ data }) {
  // Pull errors from b600 config + check for missing recent ingests
  const errors = [];
  if (data.config.b600?.last_error) {
    errors.push({
      severity: "error",
      time: data.config.b600.last_error_at || "Unknown",
      source: "B600 Time Clock auto-pull",
      msg: data.config.b600.last_error,
      action: "Check B600 credentials in Netlify env vars",
    });
  }
  // Detect stale ingests
  const staleness = (latestRecord, days) => {
    if (!latestRecord) return null;
    const t = latestRecord.imported_at || latestRecord.updated_at;
    if (!t) return null;
    const ageDays = (Date.now() - new Date(t).getTime()) / (1000 * 60 * 60 * 24);
    return ageDays > days ? Math.floor(ageDays) : null;
  };
  const checks = [
    { source: "Payroll", record: data.payroll[0], threshold: 9 },
    { source: "NuVizz", record: data.nuvizz[0], threshold: 10 },
    { source: "Uline Audit", record: data.uline[0], threshold: 10 },
    { source: "Time Clock", record: data.timeclock[0], threshold: 9 },
  ];
  for (const c of checks) {
    const stale = staleness(c.record, c.threshold);
    if (stale != null) {
      errors.push({
        severity: "warning",
        time: c.record?.imported_at || c.record?.updated_at,
        source: c.source,
        msg: `Latest ingest is ${stale} days old (expected weekly)`,
        action: `Run ${c.source} ingest from Gmail Sync or Data Ingest`,
      });
    }
  }
  // Detect employees without external IDs
  const driversWithoutMotive = data.employees.filter(e =>
    ["driver", "owner_op", "shuttle_driver"].includes(e.role) && !(e.externalIds || {}).motive
  );
  if (driversWithoutMotive.length > 0) {
    errors.push({
      severity: "info",
      time: null,
      source: "Employee Mapping",
      msg: `${driversWithoutMotive.length} drivers missing Motive IDs (GPS data won't link)`,
      action: "Open Employee Mapping → Edit each driver",
    });
  }

  const sevConfig = {
    error: { bg: DH_T.redBg, border: "#fecaca", text: DH_T.redText, icon: "❌" },
    warning: { bg: DH_T.yellowBg, border: "#fde68a", text: DH_T.yellowText, icon: "⚠️" },
    info: { bg: DH_T.blueBg, border: "#bfdbfe", text: DH_T.blueText, icon: "ℹ️" },
  };

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16 } },
      React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Error Log"),
      React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
        "Failed ingestions, stale data, and unresolved mappings.")
    ),

    errors.length === 0
      ? React.createElement(EmptyHubState, { icon: "✅", title: "All clear", subtitle: "No errors or stale ingests detected." })
      : React.createElement("div", { style: { display: "grid", gap: 8 } },
          errors.map((err, i) => {
            const cfg = sevConfig[err.severity];
            return React.createElement("div", {
              key: i,
              style: { background: DH_T.bgCard, border: `1px solid ${cfg.border}`, borderRadius: DH_T.radius, padding: 14 }
            },
              React.createElement("div", { style: { display: "flex", gap: 10, alignItems: "start" } },
                React.createElement("div", { style: { fontSize: 16, flexShrink: 0, paddingTop: 1 } }, cfg.icon),
                React.createElement("div", { style: { flex: 1 } },
                  React.createElement("div", { style: { display: "flex", alignItems: "center", gap: 8, marginBottom: 4 } },
                    React.createElement("span", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text } }, err.source),
                    React.createElement("span", { style: { fontSize: 9, fontWeight: 700, padding: "1px 6px", borderRadius: 4, background: cfg.bg, color: cfg.text, textTransform: "uppercase", letterSpacing: "0.05em" } }, err.severity),
                    err.time && React.createElement("span", { style: { fontSize: 10, color: DH_T.textDim, marginLeft: "auto" } }, relativeTime(err.time))
                  ),
                  React.createElement("div", { style: { fontSize: 12, color: DH_T.text, lineHeight: 1.4 } }, err.msg),
                  err.action && React.createElement("div", { style: { fontSize: 10, color: DH_T.textMuted, marginTop: 6 } },
                    "→ ", err.action)
                )
              )
            );
          })
        )
  );
}

// ═══ Reprocess Tools ═══════════════════════════════════════════════
function ReprocessTools({ onRefresh }) {
  const tools = [
    { title: "Re-run B600 pull", desc: "Trigger weekly time clock auto-pull manually", endpoint: "/.netlify/functions/marginiq-b600-timeclock" },
    { title: "Refresh Motive vehicles", desc: "Pull current vehicle list from Motive API", endpoint: "/.netlify/functions/marginiq-motive" },
    { title: "Trigger Audit rebuild", desc: "Recompute audit_items from billed vs paid", endpoint: "/.netlify/functions/marginiq-audit-rebuild" },
    { title: "Backup Firestore", desc: "Export all collections to Storage", endpoint: "/.netlify/functions/marginiq-backup" },
  ];

  const [running, setRunning] = useState(null);
  const [result, setResult] = useState(null);

  const run = async (tool) => {
    if (!window.confirm(`Run "${tool.title}"?`)) return;
    setRunning(tool.title);
    setResult(null);
    try {
      const r = await fetch(tool.endpoint);
      const text = await r.text();
      let parsed;
      try { parsed = JSON.parse(text); } catch { parsed = { raw: text.slice(0, 500) }; }
      setResult({ ok: r.ok, tool: tool.title, status: r.status, data: parsed });
      if (r.ok) onRefresh();
    } catch (e) {
      setResult({ ok: false, tool: tool.title, error: String(e) });
    } finally {
      setRunning(null);
    }
  };

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16 } },
      React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Reprocess Tools"),
      React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
        "Manually trigger ingestion functions or recompute derived data.")
    ),
    React.createElement("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 10, marginBottom: 16 } },
      tools.map(tool => React.createElement("div", {
        key: tool.title,
        style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, padding: 14 }
      },
        React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, tool.title),
        React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 4 } }, tool.desc),
        React.createElement("div", { style: { fontSize: 9, color: DH_T.textDim, marginTop: 6, fontFamily: "ui-monospace, monospace" } }, tool.endpoint),
        React.createElement("button", {
          onClick: () => run(tool), disabled: running != null,
          style: { marginTop: 10, padding: "5px 11px", fontSize: 11, fontWeight: 600, color: DH_T.text, background: DH_T.bgSurface, border: `1px solid ${DH_T.border}`, borderRadius: 4, cursor: running ? "wait" : "pointer", fontFamily: "inherit", opacity: running ? 0.6 : 1 }
        }, running === tool.title ? "Running…" : "▶ Run")
      ))
    ),
    result && React.createElement("div", {
      style: { background: result.ok ? DH_T.greenBg : DH_T.redBg, border: `1px solid ${result.ok ? "#a7f3d0" : "#fecaca"}`, borderRadius: DH_T.radius, padding: 14 }
    },
      React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: result.ok ? DH_T.greenText : DH_T.redText, marginBottom: 6 } },
        `${result.ok ? "✅" : "❌"} ${result.tool} — ${result.status || "error"}`),
      React.createElement("pre", {
        style: { fontSize: 10, fontFamily: "ui-monospace, monospace", color: DH_T.text, background: "#fff", border: `1px solid ${DH_T.border}`, borderRadius: 4, padding: 8, margin: 0, whiteSpace: "pre-wrap", maxHeight: 200, overflow: "auto" }
      }, JSON.stringify(result.data || result.error, null, 2))
    )
  );
}

// ═══ Settings ══════════════════════════════════════════════════════
function SettingsPanel({ data }) {
  const items = [
    { label: "Firebase Project", value: "davismarginiq (acting as data hub)", status: "connected" },
    { label: "QuickBooks OAuth", value: data.config.qbo?.access_token ? "Connected · auto-refresh" : "Not connected", status: data.config.qbo?.access_token ? "connected" : "disconnected", meta: "env: QBO_CLIENT_ID, QBO_CLIENT_SECRET" },
    { label: "B600 Time Clock", value: "b600.atlantafreightquotes.com", status: "connected", meta: "env: B600_USERNAME, B600_PASSWORD, B600_BASE_URL" },
    { label: "Motive API", value: "71 vehicles · proxy at netlify/functions/marginiq-motive", status: "connected", meta: "env: MOTIVE_API_KEY" },
    { label: "NuVizz Login", value: "company code: davis", status: "connected" },
    { label: "Gmail Watch (billing@)", value: "Active for payroll + Uline + fuel", status: "connected" },
  ];

  return React.createElement("div", null,
    React.createElement("div", { style: { marginBottom: 16 } },
      React.createElement("h1", { style: { fontSize: 22, fontWeight: 700, color: DH_T.text, margin: 0, letterSpacing: "-0.02em" } }, "Hub Settings"),
      React.createElement("p", { style: { fontSize: 12, color: DH_T.textMuted, margin: "4px 0 0 0" } },
        "Connection details for ingestion sources. Credentials stored in Netlify env vars.")
    ),
    React.createElement("div", { style: { display: "grid", gap: 8 } },
      items.map(item => React.createElement("div", {
        key: item.label,
        style: { background: DH_T.bgCard, border: `1px solid ${DH_T.border}`, borderRadius: DH_T.radius, padding: 14, display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }
      },
        React.createElement("div", null,
          React.createElement("div", { style: { fontSize: 12, fontWeight: 700, color: DH_T.text } }, item.label),
          React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 2 } }, item.value),
          item.meta && React.createElement("div", { style: { fontSize: 10, color: DH_T.textDim, marginTop: 4, fontFamily: "ui-monospace, monospace" } }, item.meta)
        ),
        React.createElement("span", {
          style: {
            fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 99,
            background: item.status === "connected" ? DH_T.greenBg : DH_T.yellowBg,
            color: item.status === "connected" ? DH_T.greenText : DH_T.yellowText,
            textTransform: "uppercase", letterSpacing: "0.05em",
          }
        }, item.status)
      ))
    )
  );
}

// ═══ Shared mini components ════════════════════════════════════════
function LoadingState() {
  return React.createElement("div", { style: { textAlign: "center", padding: "60px 20px" } },
    React.createElement("div", { className: "loading-pulse", style: { fontSize: 36, marginBottom: 8 } }, "🗄️"),
    React.createElement("div", { style: { fontSize: 13, fontWeight: 600, color: DH_T.text } }, "Loading hub…"),
    React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 4 } }, "Reading master and source collections")
  );
}

function EmptyHubState({ icon, title, subtitle }) {
  return React.createElement("div", {
    style: { background: DH_T.bgCard, border: `1px dashed ${DH_T.border}`, borderRadius: DH_T.radius, padding: "40px 20px", textAlign: "center" }
  },
    React.createElement("div", { style: { fontSize: 36, marginBottom: 8 } }, icon),
    React.createElement("div", { style: { fontSize: 13, fontWeight: 700, color: DH_T.text } }, title),
    React.createElement("div", { style: { fontSize: 11, color: DH_T.textMuted, marginTop: 4, maxWidth: 360, marginInline: "auto" } }, subtitle)
  );
}

// ── Expose globally so MarginIQ.jsx can mount it ───────────────
if (typeof window !== "undefined") {
  window.DataHubTab = DataHubTab;
}
