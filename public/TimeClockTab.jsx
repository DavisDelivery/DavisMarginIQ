/* global React, firebase */
// Davis MarginIQ — Time Clock tab
// Reads timeclock_weekly collection and provides: overview, weekly drill-in,
// anomaly detection, and an AI chat that answers questions about the data.

const { useState, useEffect, useMemo, useRef } = React;

// ── Theme (kept in sync with MarginIQ.jsx) ───────────────────────
const TC_T = {
  brand:"#1e5b92", brandLight:"#2a7bc8", brandPale:"#e8f0f8",
  bg:"#f0f4f8", bgCard:"#ffffff", bgSurface:"#f8fafc",
  text:"#0f172a", textMuted:"#64748b", textDim:"#94a3b8",
  border:"#e2e8f0", borderLight:"#f1f5f9",
  green:"#10b981", greenBg:"#ecfdf5", greenText:"#065f46",
  red:"#ef4444", redBg:"#fef2f2", redText:"#991b1b",
  yellow:"#f59e0b", yellowBg:"#fffbeb", yellowText:"#92400e",
  blue:"#3b82f6", blueBg:"#eff6ff", blueText:"#1e40af",
  purple:"#8b5cf6", purpleBg:"#f5f3ff", purpleText:"#5b21b6",
  radius:"12px", radiusSm:"8px",
  shadow:"0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06)",
};

const tcFmt = n => n==null||isNaN(n) ? "0" : Number(n).toLocaleString("en-US",{maximumFractionDigits:0});
const tcFmtDec = (n,d=1) => n==null||isNaN(n) ? "0" : Number(n).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const tcFmtPct = n => n==null||isNaN(n) ? "0%" : (n*100).toFixed(1)+"%";

// ── Firestore loader ────────────────────────────────────────────
async function loadWeekly() {
  if (typeof window === "undefined" || !window.db) return [];
  try {
    const snap = await window.db.collection("timeclock_weekly")
      .orderBy("week_ending","desc")
      .limit(260)
      .get();
    return snap.docs.map(d => ({ id:d.id, ...d.data() }));
  } catch (e) {
    console.error("loadWeekly error:", e);
    return [];
  }
}

// ── Anomaly detection (pure local stats, no AI call) ────────────
function detectAnomalies(weeks) {
  if (!weeks || weeks.length === 0) return [];
  const sorted = [...weeks].sort((a,b) => (a.week_ending||"").localeCompare(b.week_ending||""));
  const anomalies = [];

  // 1) Total-hours weeks that deviate >2σ from the trailing 8-week mean
  for (let i = 8; i < sorted.length; i++) {
    const window = sorted.slice(i-8, i);
    const mean = window.reduce((s,w) => s+(w.total_hours||0), 0) / 8;
    const variance = window.reduce((s,w) => s + Math.pow((w.total_hours||0) - mean, 2), 0) / 8;
    const std = Math.sqrt(variance);
    const cur = sorted[i].total_hours || 0;
    if (std > 0 && Math.abs(cur - mean) > 2 * std) {
      anomalies.push({
        type: cur > mean ? "spike" : "drop",
        week: sorted[i].week_ending,
        severity: "warning",
        message: `Total hours ${cur > mean ? "spiked" : "dropped"} to ${tcFmt(cur)} (trailing-8-week avg: ${tcFmt(mean)})`,
      });
    }
  }

  // 2) OT ratio exceeds 20%
  for (const w of sorted) {
    const tot = w.total_hours || 0;
    const ot = w.ot_hours || 0;
    if (tot > 0 && ot/tot > 0.20) {
      anomalies.push({
        type: "ot_high",
        week: w.week_ending,
        severity: "warning",
        message: `OT ratio ${tcFmtPct(ot/tot)} (${tcFmtDec(ot)} OT / ${tcFmtDec(tot)} total)`,
      });
    }
  }

  // 3) Employee counts that drop sharply
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i-1].unique_employees || 0;
    const cur = sorted[i].unique_employees || 0;
    if (prev > 20 && cur < prev * 0.7) {
      anomalies.push({
        type: "staff_drop",
        week: sorted[i].week_ending,
        severity: "info",
        message: `Employee count dropped to ${cur} (prior week: ${prev})`,
      });
    }
  }

  // Most recent anomalies first
  return anomalies.sort((a,b) => (b.week||"").localeCompare(a.week||""));
}

// ── AI chat via Netlify proxy ───────────────────────────────────
async function askAI(question, weeks, history=[]) {
  // Build a compact data summary so we don't blow context budget
  const sorted = [...weeks].sort((a,b) => (b.week_ending||"").localeCompare(a.week_ending||""));
  const recent = sorted.slice(0, 52); // last ~52 weeks = 1 year
  const summary = recent.map(w => ({
    week: w.week_ending,
    total: w.total_hours,
    reg: w.reg_hours,
    ot: w.ot_hours,
    employees: w.unique_employees,
    days_worked: w.days_worked,
    top5: (w.top_employees || []).slice(0, 5).map(e => ({ name:e.name, hrs:e.hours, ot:e.ot })),
  }));

  const systemPrompt = `You are a business analyst for Davis Delivery Service, a LTL trucking company. You analyze time clock data to help the owner (Chad Davis) understand labor costs, overtime trends, and anomalies. Be concise, direct, and focus on actionable insight. Use numbers from the provided data — never invent figures. When calling out an employee, use their name exactly as it appears. Always reference specific week-ending dates when discussing trends.`;

  const userContent = `Here is the time clock data (most recent 52 weeks, each row = one Saturday-week-ending rollup matching the B600 Sun-Sat work week):

${JSON.stringify(summary, null, 2)}

Question: ${question}`;

  const messages = [
    ...history,
    { role: "user", content: userContent },
  ];

  const resp = await fetch("/.netlify/functions/marginiq-analyze-timeclock", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ system: systemPrompt, messages, max_tokens: 1500 }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`AI call failed: ${resp.status} ${text}`);
  }
  const data = await resp.json();
  // Extract text from content blocks
  const text = (data.content || []).filter(b => b.type === "text").map(b => b.text).join("\n");
  return text || "(no response)";
}

// ── Sub-components ──────────────────────────────────────────────
function StatCard({ label, value, sub, color }) {
  return <div style={{background:TC_T.bgCard, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radius, padding:"14px 16px", boxShadow:TC_T.shadow}}>
    <div style={{fontSize:10, color:TC_T.textMuted, textTransform:"uppercase", letterSpacing:"0.08em", fontWeight:600}}>{label}</div>
    <div style={{fontSize:22, fontWeight:700, color:color||TC_T.text, marginTop:4, letterSpacing:"-0.02em"}}>{value}</div>
    {sub && <div style={{fontSize:11, color:TC_T.textDim, marginTop:2}}>{sub}</div>}
  </div>;
}

function AnomalyCard({ anomaly }) {
  const colors = anomaly.severity === "warning"
    ? { bg:TC_T.yellowBg, text:TC_T.yellowText, border:"#fde68a" }
    : { bg:TC_T.blueBg, text:TC_T.blueText, border:"#bfdbfe" };
  const icon = anomaly.type === "spike" ? "📈" : anomaly.type === "drop" ? "📉" : anomaly.type === "ot_high" ? "⚠️" : "ℹ️";
  return <div style={{background:colors.bg, border:`1px solid ${colors.border}`, borderRadius:TC_T.radiusSm, padding:"10px 12px", display:"flex", alignItems:"flex-start", gap:10}}>
    <span style={{fontSize:18}}>{icon}</span>
    <div style={{flex:1}}>
      <div style={{fontSize:11, color:colors.text, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.04em"}}>Week ending {anomaly.week}</div>
      <div style={{fontSize:13, color:TC_T.text, marginTop:2}}>{anomaly.message}</div>
    </div>
  </div>;
}

function WeekRow({ week, onClick, expanded }) {
  const otPct = week.total_hours ? (week.ot_hours||0)/week.total_hours : 0;
  const otColor = otPct > 0.20 ? TC_T.redText : otPct > 0.15 ? TC_T.yellowText : TC_T.textMuted;
  return <>
    <tr onClick={onClick} style={{cursor:"pointer", borderBottom:`1px solid ${TC_T.borderLight}`, background: expanded ? TC_T.bgSurface : "transparent"}}>
      <td style={{padding:"10px 12px", fontSize:13, fontWeight:600, color:TC_T.text}}>{week.week_ending}</td>
      <td style={{padding:"10px 12px", fontSize:13, textAlign:"right", fontWeight:600}}>{tcFmtDec(week.total_hours)}</td>
      <td style={{padding:"10px 12px", fontSize:13, textAlign:"right", color:TC_T.textMuted}}>{tcFmtDec(week.reg_hours)}</td>
      <td style={{padding:"10px 12px", fontSize:13, textAlign:"right", color:otColor, fontWeight:600}}>{tcFmtDec(week.ot_hours)} <span style={{fontSize:10, fontWeight:500}}>({tcFmtPct(otPct)})</span></td>
      <td style={{padding:"10px 12px", fontSize:13, textAlign:"right", color:TC_T.textMuted}}>{week.unique_employees || 0}</td>
      <td style={{padding:"10px 12px", fontSize:16, textAlign:"center", color:TC_T.textDim}}>{expanded ? "▼" : "▸"}</td>
    </tr>
    {expanded && <tr>
      <td colSpan={6} style={{background:TC_T.bgSurface, padding:"12px 16px"}}>
        <div style={{fontSize:11, color:TC_T.textMuted, fontWeight:700, textTransform:"uppercase", letterSpacing:"0.06em", marginBottom:8}}>Top employees this week</div>
        {(week.top_employees || []).length === 0
          ? <div style={{fontSize:12, color:TC_T.textDim}}>No employee detail available for this week.</div>
          : <table style={{width:"100%", borderCollapse:"collapse"}}>
              <thead>
                <tr style={{borderBottom:`1px solid ${TC_T.border}`}}>
                  <th style={{padding:"6px 8px", fontSize:10, color:TC_T.textMuted, textAlign:"left", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:600}}>Employee</th>
                  <th style={{padding:"6px 8px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:600}}>Hours</th>
                  <th style={{padding:"6px 8px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:600}}>Reg</th>
                  <th style={{padding:"6px 8px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:600}}>OT</th>
                  <th style={{padding:"6px 8px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:600}}>Days</th>
                </tr>
              </thead>
              <tbody>
                {week.top_employees.slice(0, 20).map((e, idx) => <tr key={idx}>
                  <td style={{padding:"6px 8px", fontSize:12, color:TC_T.text}}>{e.name}</td>
                  <td style={{padding:"6px 8px", fontSize:12, textAlign:"right", fontWeight:600}}>{tcFmtDec(e.hours)}</td>
                  <td style={{padding:"6px 8px", fontSize:12, textAlign:"right", color:TC_T.textMuted}}>{tcFmtDec(e.reg)}</td>
                  <td style={{padding:"6px 8px", fontSize:12, textAlign:"right", color:(e.ot||0)>10?TC_T.redText:TC_T.textMuted, fontWeight:(e.ot||0)>10?600:400}}>{tcFmtDec(e.ot)}</td>
                  <td style={{padding:"6px 8px", fontSize:12, textAlign:"right", color:TC_T.textMuted}}>{e.days}</td>
                </tr>)}
              </tbody>
            </table>}
      </td>
    </tr>}
  </>;
}

function AIChat({ weeks }) {
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState([]); // { role, content, isError? }
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [messages, loading]);

  const suggestions = [
    "What's my overtime trend over the last 12 weeks?",
    "Which employees consistently work the most hours?",
    "Summarize the last 4 weeks in plain English.",
    "Are there any weeks that look unusual compared to the average?",
  ];

  async function submit(q) {
    const ask = (q || question).trim();
    if (!ask || loading) return;
    setQuestion("");
    const newMessages = [...messages, { role: "user", content: ask }];
    setMessages(newMessages);
    setLoading(true);
    try {
      // Pass prior assistant/user turns as history (strip isError flags)
      const history = messages.map(m => ({ role: m.role, content: m.content }));
      const answer = await askAI(ask, weeks, history);
      setMessages([...newMessages, { role: "assistant", content: answer }]);
    } catch (err) {
      setMessages([...newMessages, { role: "assistant", content: `Error: ${err.message}`, isError: true }]);
    } finally {
      setLoading(false);
    }
  }

  return <div style={{background:TC_T.bgCard, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radius, boxShadow:TC_T.shadow, overflow:"hidden", display:"flex", flexDirection:"column", height:"500px"}}>
    <div style={{padding:"12px 16px", borderBottom:`1px solid ${TC_T.border}`, background:TC_T.bgSurface, display:"flex", alignItems:"center", gap:8}}>
      <span style={{fontSize:16}}>🤖</span>
      <div style={{fontSize:13, fontWeight:700, color:TC_T.text}}>Ask about your time clock data</div>
    </div>

    <div ref={scrollRef} style={{flex:1, overflowY:"auto", padding:"16px"}}>
      {messages.length === 0 && <div>
        <div style={{fontSize:12, color:TC_T.textMuted, marginBottom:10}}>Try one of these:</div>
        {suggestions.map((s,i) => <button key={i} onClick={() => submit(s)} disabled={loading} style={{display:"block", width:"100%", textAlign:"left", padding:"10px 12px", marginBottom:6, background:TC_T.bgSurface, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radiusSm, fontSize:12, color:TC_T.text, cursor:loading?"not-allowed":"pointer", fontFamily:"inherit"}}>{s}</button>)}
      </div>}
      {messages.map((m, i) => <div key={i} style={{marginBottom:12, display:"flex", justifyContent: m.role === "user" ? "flex-end" : "flex-start"}}>
        <div style={{maxWidth:"85%", padding:"10px 12px", borderRadius:TC_T.radiusSm, background: m.role === "user" ? TC_T.brand : (m.isError ? TC_T.redBg : TC_T.bgSurface), color: m.role === "user" ? "#fff" : (m.isError ? TC_T.redText : TC_T.text), fontSize:13, lineHeight:1.5, whiteSpace:"pre-wrap"}}>
          {m.content}
        </div>
      </div>)}
      {loading && <div style={{display:"flex", justifyContent:"flex-start"}}>
        <div style={{padding:"10px 12px", background:TC_T.bgSurface, borderRadius:TC_T.radiusSm, fontSize:12, color:TC_T.textMuted}}>Thinking…</div>
      </div>}
    </div>

    <div style={{padding:"10px 12px", borderTop:`1px solid ${TC_T.border}`, background:TC_T.bgSurface, display:"flex", gap:8}}>
      <input
        value={question}
        onChange={e => setQuestion(e.target.value)}
        onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); submit(); } }}
        placeholder="Ask anything about your time clock data..."
        disabled={loading}
        style={{flex:1, padding:"10px 12px", borderRadius:TC_T.radiusSm, border:`1px solid ${TC_T.border}`, fontSize:13, fontFamily:"inherit", outline:"none"}}
      />
      <button onClick={() => submit()} disabled={loading || !question.trim()} style={{padding:"10px 16px", borderRadius:TC_T.radiusSm, border:"none", background: (loading || !question.trim()) ? TC_T.textDim : TC_T.brand, color:"#fff", fontSize:13, fontWeight:600, cursor:(loading || !question.trim()) ? "not-allowed" : "pointer"}}>Send</button>
    </div>
  </div>;
}

// ── Main tab component ──────────────────────────────────────────
function TimeClockTab() {
  const [weeks, setWeeks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedWeek, setExpandedWeek] = useState(null);
  const [view, setView] = useState("overview"); // overview | weeks | anomalies | ai

  useEffect(() => {
    (async () => {
      setLoading(true);
      const data = await loadWeekly();
      setWeeks(data);
      setLoading(false);
    })();
  }, []);

  const stats = useMemo(() => {
    if (weeks.length === 0) return null;
    const sorted = [...weeks].sort((a,b) => (b.week_ending||"").localeCompare(a.week_ending||""));
    const totalHrs = weeks.reduce((s,w) => s+(w.total_hours||0), 0);
    const totalReg = weeks.reduce((s,w) => s+(w.reg_hours||0), 0);
    const totalOt = weeks.reduce((s,w) => s+(w.ot_hours||0), 0);
    const last4 = sorted.slice(0,4);
    const last4Hrs = last4.reduce((s,w) => s+(w.total_hours||0), 0);
    const last4OtPct = last4.reduce((s,w) => s+(w.ot_hours||0), 0) / (last4Hrs || 1);
    const avgEmps = weeks.reduce((s,w) => s+(w.unique_employees||0), 0) / weeks.length;
    return {
      totalHrs, totalReg, totalOt,
      otPct: totalOt/(totalHrs||1),
      weekCount: weeks.length,
      firstWeek: sorted[sorted.length-1]?.week_ending,
      lastWeek: sorted[0]?.week_ending,
      last4Hrs,
      last4OtPct,
      avgEmps,
    };
  }, [weeks]);

  const anomalies = useMemo(() => detectAnomalies(weeks), [weeks]);

  if (loading) {
    return <div style={{padding:"60px 20px", textAlign:"center", color:TC_T.textMuted}}>
      <div style={{fontSize:36, marginBottom:8}}>⏰</div>
      <div style={{fontSize:14, fontWeight:600}}>Loading time clock data…</div>
    </div>;
  }

  if (weeks.length === 0) {
    return <div style={{padding:"40px 20px"}}>
      <div style={{background:TC_T.bgCard, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radius, padding:"40px 20px", textAlign:"center", boxShadow:TC_T.shadow}}>
        <div style={{fontSize:36, marginBottom:8}}>⏰</div>
        <div style={{fontSize:15, fontWeight:700, color:TC_T.text, marginBottom:4}}>No Time Clock Data Yet</div>
        <div style={{fontSize:13, color:TC_T.textMuted}}>Upload CyberPay CSV exports in the Data Ingest tab.</div>
      </div>
    </div>;
  }

  return <div style={{padding:"16px 20px"}}>
    <div style={{display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:16}}>
      <div style={{fontSize:18, fontWeight:700, color:TC_T.text, display:"flex", alignItems:"center", gap:8}}>
        <span>⏰</span>Time Clock
      </div>
      <div style={{fontSize:10, color:TC_T.textDim}}>
        {stats?.firstWeek} → {stats?.lastWeek} · {stats?.weekCount} weeks
      </div>
    </div>

    {/* Sub-nav */}
    <div style={{display:"flex", gap:6, marginBottom:16, borderBottom:`1px solid ${TC_T.border}`, paddingBottom:8, overflowX:"auto"}}>
      {[
        { id:"overview", label:"Overview", icon:"📊" },
        { id:"weeks", label:"Weekly Detail", icon:"📅" },
        { id:"anomalies", label:`Anomalies${anomalies.length?` (${anomalies.length})`:""}`, icon:"⚠️" },
        { id:"ai", label:"Ask AI", icon:"🤖" },
      ].map(v => <button key={v.id} onClick={() => setView(v.id)} style={{padding:"6px 12px", borderRadius:TC_T.radiusSm, border:"none", background: view === v.id ? TC_T.brand : "transparent", color: view === v.id ? "#fff" : TC_T.textMuted, fontSize:12, fontWeight: view === v.id ? 700 : 500, cursor:"pointer", whiteSpace:"nowrap"}}>{v.icon} {v.label}</button>)}
    </div>

    {/* Overview */}
    {view === "overview" && stats && <div>
      <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(180px, 1fr))", gap:12, marginBottom:20}}>
        <StatCard label="Total Hours (all time)" value={tcFmt(stats.totalHrs)} sub={`${stats.weekCount} weeks`} />
        <StatCard label="Regular / OT Split" value={`${tcFmtPct(1-stats.otPct)} / ${tcFmtPct(stats.otPct)}`} sub={`${tcFmt(stats.totalReg)} REG + ${tcFmt(stats.totalOt)} OT`} />
        <StatCard label="Last 4 Weeks" value={tcFmt(stats.last4Hrs)} sub={`OT ratio: ${tcFmtPct(stats.last4OtPct)}`} color={stats.last4OtPct > 0.20 ? TC_T.redText : TC_T.text} />
        <StatCard label="Avg Employees / Week" value={tcFmtDec(stats.avgEmps, 0)} sub="Across all weeks" />
      </div>

      {anomalies.length > 0 && <div>
        <div style={{fontSize:13, fontWeight:700, color:TC_T.text, marginBottom:10}}>Recent Anomalies ({anomalies.slice(0,3).length} of {anomalies.length})</div>
        <div style={{display:"flex", flexDirection:"column", gap:8}}>
          {anomalies.slice(0, 3).map((a, i) => <AnomalyCard key={i} anomaly={a} />)}
        </div>
        {anomalies.length > 3 && <div style={{marginTop:8}}>
          <button onClick={() => setView("anomalies")} style={{background:"none", border:"none", color:TC_T.brand, fontSize:12, fontWeight:600, cursor:"pointer", padding:"4px 0"}}>See all {anomalies.length} anomalies →</button>
        </div>}
      </div>}
    </div>}

    {/* Weekly detail */}
    {view === "weeks" && <div style={{background:TC_T.bgCard, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radius, boxShadow:TC_T.shadow, overflow:"hidden"}}>
      <table style={{width:"100%", borderCollapse:"collapse"}}>
        <thead>
          <tr style={{background:TC_T.bgSurface, borderBottom:`1px solid ${TC_T.border}`}}>
            <th style={{padding:"10px 12px", fontSize:10, color:TC_T.textMuted, textAlign:"left", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:700}}>Week Ending</th>
            <th style={{padding:"10px 12px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:700}}>Total Hours</th>
            <th style={{padding:"10px 12px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:700}}>Regular</th>
            <th style={{padding:"10px 12px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:700}}>Overtime</th>
            <th style={{padding:"10px 12px", fontSize:10, color:TC_T.textMuted, textAlign:"right", textTransform:"uppercase", letterSpacing:"0.06em", fontWeight:700}}>Employees</th>
            <th style={{padding:"10px 12px", width:30}}></th>
          </tr>
        </thead>
        <tbody>
          {[...weeks].sort((a,b) => (b.week_ending||"").localeCompare(a.week_ending||"")).map(w => <WeekRow
            key={w.week_ending}
            week={w}
            expanded={expandedWeek === w.week_ending}
            onClick={() => setExpandedWeek(expandedWeek === w.week_ending ? null : w.week_ending)}
          />)}
        </tbody>
      </table>
    </div>}

    {/* Anomalies */}
    {view === "anomalies" && <div>
      {anomalies.length === 0
        ? <div style={{background:TC_T.bgCard, border:`1px solid ${TC_T.border}`, borderRadius:TC_T.radius, padding:"40px 20px", textAlign:"center", color:TC_T.textMuted}}>
            <div style={{fontSize:36, marginBottom:8}}>✅</div>
            <div style={{fontSize:14, fontWeight:600, color:TC_T.text}}>No anomalies detected</div>
            <div style={{fontSize:12, marginTop:4}}>Hours and OT ratios look consistent with your trailing-8-week averages.</div>
          </div>
        : <div style={{display:"flex", flexDirection:"column", gap:8}}>
            {anomalies.map((a, i) => <AnomalyCard key={i} anomaly={a} />)}
          </div>}
    </div>}

    {/* AI chat */}
    {view === "ai" && <AIChat weeks={weeks} />}
  </div>;
}

// Expose globally so MarginIQ.jsx can mount it
if (typeof window !== "undefined") {
  window.TimeClockTab = TimeClockTab;
}
