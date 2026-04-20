// MarginIQ Payroll Invariant Checks
// Runs cross-validation on parser output to detect:
//   (1) PDF format changes (Southern Payroll alters their layout)
//   (2) Parser bugs (regression detection)
//   (3) Data anomalies (unusual payroll patterns)
//
// Returns { passed: [], failed: [], warnings: [] }
//   - failed  = hard invariant break (import should be rejected or flagged red)
//   - warnings = soft anomaly (import succeeds but flagged yellow for review)

const PENNY = 0.01;
const near = (a, b, tol = PENNY) => Math.abs((a || 0) - (b || 0)) < tol;
const fmt = v => (v == null ? 'null' : Number(v).toFixed(2));

export function runInvariants(parsed) {
  const passed = [];
  const failed = [];
  const warnings = [];

  const check = (label, ok, detail) => {
    if (ok) passed.push(label);
    else failed.push({ label, detail });
  };
  const warn = (label, detail) => warnings.push({ label, detail });

  const s = parsed.summary;
  const emps = parsed.employees || [];
  const checks = parsed.checks || [];
  const prChecks = checks.filter(c => c.sourceCode === 'PR');

  // ------------------------------------------------------------
  // HARD INVARIANTS — any failure means format change or bug
  // ------------------------------------------------------------

  // 1. Sum of employee pay must equal reported gross pay
  const sumPay = emps.reduce((a, e) => a + (e.pay || 0), 0);
  check(
    'sum_employee_pay_equals_gross',
    near(sumPay, s.totals.grossPay),
    `sum(employee.pay)=${fmt(sumPay)}  gross=${fmt(s.totals.grossPay)}  diff=${fmt(sumPay - s.totals.grossPay)}`
  );

  // 2. Sum of net + deductions + employee taxes = gross (W2 only)
  if (s.population === 'W2') {
    const sumNet = emps.reduce((a, e) => a + (e.net || 0), 0);
    const sumDed = emps.reduce((a, e) => a + (e.deductions || []).reduce((x, d) => x + (d.current || 0), 0), 0);
    const sumTax = emps.reduce((a, e) => a + (e.taxes || []).reduce((x, t) => x + (t.current || 0), 0), 0);
    check(
      'net_plus_ded_plus_tax_equals_gross_W2',
      near(sumNet + sumDed + sumTax, s.totals.grossPay),
      `net=${fmt(sumNet)} + ded=${fmt(sumDed)} + tax=${fmt(sumTax)} = ${fmt(sumNet+sumDed+sumTax)}  gross=${fmt(s.totals.grossPay)}`
    );
    // 3. EE tax sum matches summary totals
    check(
      'employee_tax_sum_matches_summary_W2',
      near(sumTax, s.totals.employeeTaxes),
      `sum(employee.taxes)=${fmt(sumTax)}  summary.employeeTaxes=${fmt(s.totals.employeeTaxes)}`
    );
    // 4. Deduction sum matches
    check(
      'deduction_sum_matches_summary_W2',
      near(sumDed, s.totals.deductions),
      `sum(employee.deductions)=${fmt(sumDed)}  summary.deductions=${fmt(s.totals.deductions)}`
    );
  }

  // 5. Employee count matches check count matches PR lines
  check(
    'employee_count_matches_check_count',
    emps.length === prChecks.length,
    `employees=${emps.length}  PR checks=${prChecks.length}`
  );

  // 6. Check listing total reconciles to directDeposit + netPayChecks + tax/fee items
  // For a clean run: sum(PR checks) should equal directDeposit + netPayChecks
  const sumPrCheck = prChecks.reduce((a, c) => a + c.amount, 0);
  const expectedNet = (s.totals.directDeposit || 0) + (s.totals.netPayChecks || 0);
  check(
    'PR_check_sum_matches_net_plus_dd',
    near(sumPrCheck, expectedNet),
    `sum(PR checks)=${fmt(sumPrCheck)}  directDeposit+netPayChecks=${fmt(expectedNet)}`
  );

  // 7. Total cost = gross + fees + employer taxes (W2) or gross + fees (1099)
  if (s.population === 'W2' && s.totals.totalCost != null) {
    const expected = s.totals.grossPay + (s.totals.fees || 0) + (s.totals.employerTaxes || 0);
    check(
      'total_cost_equals_gross_plus_fees_plus_ertax',
      near(s.totals.totalCost, expected, 1.00), // allow $1 rounding
      `totalCost=${fmt(s.totals.totalCost)}  gross+fees+erTax=${fmt(expected)}`
    );
  }

  // ------------------------------------------------------------
  // SOFT WARNINGS — unusual but not necessarily wrong
  // ------------------------------------------------------------

  // Abnormal employee count shift (if we have historical context)
  // -> handled at application layer, not here

  // Any employee with 0 hours AND 0 pay
  const zeros = emps.filter(e => e.pay === 0);
  if (zeros.length > 0) {
    warn('zero_pay_employees', `${zeros.length} employees with $0 pay: ${zeros.map(e => e.rawName).slice(0, 5).join(', ')}`);
  }

  // Any employee where payRows sum doesn't match their pay field
  for (const e of emps) {
    if (!e.payRows || !e.payRows.length) continue;
    const rowSum = e.payRows.reduce((a, r) => a + (r.current || 0), 0);
    if (!near(rowSum, e.pay)) {
      warn('employee_payrows_mismatch', `${e.rawName} (${e.ssn}): payRows sum=${fmt(rowSum)}  pay field=${fmt(e.pay)}`);
    }
  }

  // Overtime hours >= regular hours (unusual, might indicate data entry error)
  for (const e of emps) {
    const hourly = (e.payRows || []).find(r => r.type === 'Hourly');
    const ot = (e.payRows || []).find(r => r.type === 'OT1.5');
    if (hourly && ot && ot.quantity > hourly.quantity && ot.quantity > 0) {
      warn('ot_exceeds_regular', `${e.rawName}: OT hrs=${ot.quantity}  regular hrs=${hourly.quantity}`);
    }
  }

  // Overall result
  const ok = failed.length === 0;
  return { ok, passed, failed, warnings };
}

// Summary formatter for the UI banner
export function invariantSummaryText(result) {
  if (result.ok && result.warnings.length === 0) {
    return `✅ ${result.passed.length} invariant checks passed, no warnings.`;
  }
  if (result.ok) {
    return `✅ ${result.passed.length} invariant checks passed, ${result.warnings.length} warnings to review.`;
  }
  return `❌ ${result.failed.length} invariant check(s) FAILED — import flagged. Likely cause: PDF format change or parser regression.`;
}
