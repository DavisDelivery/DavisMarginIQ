// MarginIQ Payroll Tab — React component
// Mirrors the Fuel tab pattern: Weekly Comparison | Employees | Contractors | By Driver | Upload
//
// Uses deterministic text-based parser (Path B).
// Invariants surface format-change alerts automatically on every import.

import React, { useState, useMemo } from 'react';
import { extractLayoutText } from './lib/extractPdfText.mjs';

// Expects window.marginiqFirebase to be initialized (same pattern as Fuel tab)
// Expects props: { identityMap, onIdentityMapChange, weeks }

// ============================================================================
// UPLOAD VIEW
// ============================================================================

function UploadView({ onScanned }) {
  const [status, setStatus] = useState('idle'); // idle | extracting | scanning | done | error
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const onFileChange = async (e) => {
    const files = Array.from(e.target.files || []);
    if (!files.length) return;

    const results = [];
    for (const file of files) {
      try {
        setStatus('extracting');
        setError(null);
        const rawText = await extractLayoutText(file);

        setStatus('scanning');
        const res = await fetch('/api/scan-payroll', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ rawText, filename: file.name }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.error || `HTTP ${res.status}`);
        }
        const parsed = await res.json();
        results.push(parsed);
      } catch (err) {
        setError(`${file.name}: ${err.message}`);
        setStatus('error');
        return;
      }
    }
    setResult(results);
    setStatus('done');
    onScanned?.(results);
  };

  return (
    <div className="payroll-upload">
      <h3>Upload CyberPay PDFs</h3>
      <p className="hint">
        Drag the weekly payroll PDFs from Southern Payroll Services —
        typically <code>Paper_Delivery_0190_Combined_XXX.pdf</code> (W2)
        and <code>Paper_Delivery_0189_Combined_XXX.pdf</code> (1099).
      </p>
      <input
        type="file"
        accept=".pdf,application/pdf"
        multiple
        onChange={onFileChange}
        disabled={status === 'extracting' || status === 'scanning'}
      />
      {status === 'extracting' && <div className="status">Extracting PDF text…</div>}
      {status === 'scanning' && <div className="status">Parsing & validating…</div>}
      {error && <div className="status error">❌ {error}</div>}
      {status === 'done' && result && (
        <div className="results">
          {result.map((r, i) => (
            <ScanResultBanner key={i} result={r} />
          ))}
        </div>
      )}
    </div>
  );
}

// ============================================================================
// INVARIANT BANNER
// ============================================================================

function ScanResultBanner({ result }) {
  const inv = result.invariants;
  const cls = inv.ok
    ? (inv.warnings.length ? 'banner-warn' : 'banner-ok')
    : 'banner-fail';
  return (
    <div className={`invariant-banner ${cls}`}>
      <div className="banner-header">
        {inv.ok ? '✅' : '❌'} {result.filename || result.company} —
        {' '}{result.employees.length} {result.summary.population} records,
        {' '}${result.summary.totals.grossPay?.toFixed(2)} gross
      </div>
      {!inv.ok && (
        <div className="banner-failures">
          <strong>Invariant failures ({inv.failed.length})</strong> — this usually
          means Southern Payroll changed their PDF format. Email
          chad@davisdelivery.com with this message so the parser can be updated.
          <ul>
            {inv.failed.map((f, i) => (
              <li key={i}><code>{f.label}</code>: {f.detail}</li>
            ))}
          </ul>
        </div>
      )}
      {inv.warnings.length > 0 && (
        <details className="banner-warnings">
          <summary>{inv.warnings.length} warning(s) to review</summary>
          <ul>
            {inv.warnings.map((w, i) => (
              <li key={i}><code>{w.label}</code>: {w.detail}</li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}

// ============================================================================
// UNMAPPED PEOPLE BANNER
// ============================================================================

function UnmappedBanner({ employees, identityMap, onOpenMapping }) {
  const unmapped = useMemo(() => {
    const known = new Set(identityMap.map(m => m.canonicalKey));
    return employees.filter(e => !known.has(e.canonicalKey));
  }, [employees, identityMap]);

  const needsTruck = useMemo(() => {
    const mapByKey = new Map(identityMap.map(m => [m.canonicalKey, m]));
    return employees.filter(e => {
      const m = mapByKey.get(e.canonicalKey);
      return m && m.kind === 'W2' && !m.truckNumber;
    });
  }, [employees, identityMap]);

  if (unmapped.length === 0 && needsTruck.length === 0) return null;

  return (
    <div className="mapping-banner">
      {unmapped.length > 0 && (
        <div className="new-people">
          <strong>🆕 {unmapped.length} new {unmapped.length === 1 ? 'person' : 'people'} this week:</strong>
          {' '}{unmapped.slice(0, 5).map(e => e.rawName).join(', ')}
          {unmapped.length > 5 ? `, +${unmapped.length - 5} more` : ''}
          <button onClick={onOpenMapping}>Assign identity & truck</button>
        </div>
      )}
      {needsTruck.length > 0 && (
        <div className="needs-truck">
          <strong>🚚 {needsTruck.length} driver{needsTruck.length === 1 ? '' : 's'} need truck assignment</strong>
          <button onClick={onOpenMapping}>Assign</button>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// WEEKLY COMPARISON
// ============================================================================

function WeeklyComparison({ weeks }) {
  if (!weeks || weeks.length === 0) {
    return <div className="empty">No payroll weeks imported yet. Go to Upload tab.</div>;
  }
  return (
    <table className="weekly-comparison">
      <thead>
        <tr>
          <th>Week Ending</th>
          <th>W2 Gross</th>
          <th>W2 Net</th>
          <th>W2 Taxes (ER)</th>
          <th>1099 Paid</th>
          <th>Total Labor Cost</th>
        </tr>
      </thead>
      <tbody>
        {weeks.map(w => (
          <tr key={w.weekEnding}>
            <td>{w.weekEnding}</td>
            <td>${w.w2?.grossPay?.toFixed(2) || '—'}</td>
            <td>${w.w2?.directDeposit?.toFixed(2) || '—'}</td>
            <td>${w.w2?.employerTaxes?.toFixed(2) || '—'}</td>
            <td>${w.c1099?.grossPay?.toFixed(2) || '—'}</td>
            <td className="total">${w.totalLaborCost?.toFixed(2) || '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ============================================================================
// EMPLOYEE / CONTRACTOR DETAIL
// ============================================================================

function EmployeeList({ employees, identityMap, kind }) {
  const mapByKey = new Map(identityMap.map(m => [m.canonicalKey, m]));
  const rows = employees
    .filter(e => {
      const m = mapByKey.get(e.canonicalKey);
      return !kind || (m?.kind || '1099') === kind;
    })
    .sort((a, b) => b.pay - a.pay);

  return (
    <table className="employee-list">
      <thead>
        <tr>
          <th>Name</th>
          <th>Truck</th>
          <th>Hours</th>
          <th>Pay</th>
          <th>Ded</th>
          <th>Tax</th>
          <th>Net</th>
          <th>Check #</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(e => {
          const m = mapByKey.get(e.canonicalKey);
          const displayName = m?.personDisplayName || m?.entityName || e.rawName;
          const dedSum = (e.deductions || []).reduce((a, d) => a + d.current, 0);
          const taxSum = (e.taxes || []).reduce((a, t) => a + t.current, 0);
          return (
            <tr key={e.ssn}>
              <td>
                {displayName}
                {m?.entityName && m?.personDisplayName && (
                  <span className="dba"> (dba: {m.entityName})</span>
                )}
              </td>
              <td>{m?.truckNumber || <span className="unknown">—</span>}</td>
              <td>{e.hours?.toFixed(2) || '—'}</td>
              <td>${e.pay?.toFixed(2)}</td>
              <td>${dedSum.toFixed(2)}</td>
              <td>${taxSum.toFixed(2)}</td>
              <td>${e.net?.toFixed(2)}</td>
              <td>{e.checkNumber || ''}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ============================================================================
// MAIN TAB COMPONENT
// ============================================================================

export default function PayrollTab({ identityMap = [], onIdentityMapChange, weeks = [] }) {
  const [subtab, setSubtab] = useState('comparison');
  const [lastScan, setLastScan] = useState(null);

  const currentEmployees = lastScan
    ? lastScan.flatMap(r => r.employees)
    : (weeks[0]?.allEmployees || []);

  return (
    <div className="payroll-tab">
      <nav className="subnav">
        <button className={subtab === 'comparison' ? 'active' : ''} onClick={() => setSubtab('comparison')}>Weekly Comparison</button>
        <button className={subtab === 'w2' ? 'active' : ''} onClick={() => setSubtab('w2')}>W2 Employees</button>
        <button className={subtab === '1099' ? 'active' : ''} onClick={() => setSubtab('1099')}>1099 Contractors</button>
        <button className={subtab === 'mapping' ? 'active' : ''} onClick={() => setSubtab('mapping')}>Mapping</button>
        <button className={subtab === 'upload' ? 'active' : ''} onClick={() => setSubtab('upload')}>Upload</button>
      </nav>

      {currentEmployees.length > 0 && (
        <UnmappedBanner
          employees={currentEmployees}
          identityMap={identityMap}
          onOpenMapping={() => setSubtab('mapping')}
        />
      )}

      <div className="subtab-content">
        {subtab === 'comparison' && <WeeklyComparison weeks={weeks} />}
        {subtab === 'w2' && <EmployeeList employees={currentEmployees} identityMap={identityMap} kind="W2" />}
        {subtab === '1099' && <EmployeeList employees={currentEmployees} identityMap={identityMap} kind="1099" />}
        {subtab === 'mapping' && (
          <div className="mapping-pane">
            <p>Mapping editor — edit canonical names, entity → person, truck assignments.</p>
            {/* TODO: full CRUD UI for identityMap, persist to Firebase */}
            <pre>{JSON.stringify(identityMap.slice(0, 3), null, 2)}</pre>
          </div>
        )}
        {subtab === 'upload' && <UploadView onScanned={setLastScan} />}
      </div>
    </div>
  );
}
