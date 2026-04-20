// MarginIQ CyberPay Payroll Parser v2.0 — column-aware
// Southern Payroll Services / CyberPay pdftotext -layout output → structured JSON

const money = s => {
  if (s === null || s === undefined || s === '') return 0;
  const n = parseFloat(String(s).replace(/[$,]/g, '').trim());
  return isNaN(n) ? 0 : n;
};
const normWS = s => String(s || '').replace(/\s+/g, ' ').trim();

export function canonicalKey(raw) {
  return normWS(raw).toUpperCase().replace(/[^\w ]/g, '').replace(/\s+/g, '_');
}

export function detectCompany(rawText) {
  const t = String(rawText);
  if (/\b0190,/.test(t) && /Hourly|401KEe/.test(t)) return '0190';
  if (/\b0189,/.test(t) && /\b1099\b/.test(t)) return '0189';
  return null;
}

function parsePeriod(text) {
  const m = text.match(/Pay Period From (\d{1,2}\/\d{1,2}\/\d{4}) to (\d{1,2}\/\d{1,2}\/\d{4})/);
  return m ? { from: m[1], to: m[2] } : { from: null, to: null };
}
function parseCheckDate(text) {
  const m = text.match(/Check Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/);
  return m ? m[1] : null;
}
function parseRunId(text) {
  const m = text.match(/Weekly Processing\s+(\d+)\s+\d+\s+\$/);
  return m ? m[1] : null;
}

// Net Pay Summary is a header row followed by a values row, column-aligned.
// e.g. "Net Pay Cks  Manual Cks  Direct Dep.  Deductions  Benefits  Taxes  Fees  Total Cost"
//      "       0.00        0.00    62,257.31    5,136.48      0.00 22,291.65 144.50  89,829.94"
// We detect the header line and parse the following values line by splitting both on 2+ spaces.
function parseNetPaySummary(text) {
  const r = {};
  const lines = text.split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (/Net Pay Cks.*Total Cost/.test(lines[i])) {
      // Find next non-empty line
      let valLine = null;
      for (let j = i + 1; j < Math.min(i + 5, lines.length); j++) {
        if (lines[j].trim()) { valLine = lines[j]; break; }
      }
      if (!valLine) continue;

      // Split header and value lines on 2+ spaces
      const headers = lines[i].split(/\s{2,}/).map(s => s.trim()).filter(Boolean);
      const values = valLine.split(/\s{2,}/).map(s => s.trim()).filter(Boolean);

      // Map by position
      const keyMap = {
        'Net Pay Cks':  'netPayChecks',
        'Manual Cks':   'manualChecks',
        'Direct Dep.':  'directDeposit',
        'Deductions':   'deductionsSum',
        'Benefits':     'benefitsSum',
        'Taxes':        'taxesSum',
        'Fees':         'feesSum',
        'Total Cost':   'totalCost',
      };
      for (let k = 0; k < headers.length && k < values.length; k++) {
        const key = keyMap[headers[k]];
        if (key) r[key] = money(values[k]);
      }
      break;
    }
  }
  return r;
}

function parse0190Summary(text) {
  const s = {
    company: '0190', companyName: 'Davis Delivery Service Inc.', population: 'W2',
    payPeriod: parsePeriod(text), checkDate: parseCheckDate(text), runId: parseRunId(text),
    payTypes: {}, deductions: {}, employeeTaxes: {}, employerTaxes: {}, fees: {}, totals: {},
  };

  for (const [name, re] of [
    ['Salary',  /^Salary\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['Hourly',  /^Hourly\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['OT1.5',   /^OT1\.5\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['PTO',     /^PTO\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['Holiday', /^Holiday\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['Bonus',   /^Bonus\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
    ['Comm',    /^Comm\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m],
  ]) {
    const m = text.match(re);
    if (m) s.payTypes[name] = { quantity: money(m[1]), hours: money(m[2]), amount: money(m[3]) };
  }

  const tp = text.match(/Total Pay:\s*([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/);
  if (tp) s.totals.grossPay = money(tp[3]);
  const td = text.match(/Total Deductions:\s*([\d,.]+)/);
  if (td) s.totals.deductions = money(td[1]);

  for (const d of ['401KEe','Accident','Advance','Crit Ill','Dental','FSA','Hlth PT','Life','Misc Ded','PerformD','Support1','Vision']) {
    const safe = d.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = text.match(new RegExp(`^${safe}\\s+([\\d,.]+)\\s+([\\d,.]+)\\s*$`, 'm'));
    if (m) s.deductions[d] = { dedWages: money(m[1]), amount: money(m[2]) };
  }

  for (const t of ['FICA Med', 'FICA SS', 'FIT', 'GA SIT']) {
    const safe = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = text.match(new RegExp(`^${safe}\\s+([\\d,.]+)\\s+([\\d,.]+)\\s+([\\d,.]+)\\s*$`, 'm'));
    if (m) s.employeeTaxes[t] = { totalWages: money(m[1]), taxableWages: money(m[2]), amount: money(m[3]) };
  }
  const tet = text.match(/Total Employee Taxes:\s*([\d,.]+)/);
  if (tet) s.totals.employeeTaxes = money(tet[1]);

  const erSection = text.split(/Employer Taxes/)[1] || '';
  for (const t of ['FICA MED', 'FICA SS', 'FUTA', 'GA SUPP', 'GA SUTA']) {
    const safe = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = erSection.match(new RegExp(`^${safe}\\s+([\\d,.]+)\\s+([\\d,.]+)\\s+([\\d,.]+)\\s*$`, 'm'));
    if (m) s.employerTaxes[t] = { totalWages: money(m[1]), taxableWages: money(m[2]), amount: money(m[3]) };
  }
  const tert = text.match(/Total Employer Taxes:\s*([\d,.]+)/);
  if (tert) s.totals.employerTaxes = money(tert[1]);

  for (const f of ['Agency Checks', 'Direct Deposit Items', 'Employee Direct Deposits', 'Online Access', 'Weekly Processing', 'Payroll Checks', 'Mid Georgia']) {
    const safe = f.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = text.match(new RegExp(`${safe}\\s+\\d+\\s+(\\d+)\\s+\\$?([\\d,.]+)`));
    if (m) s.fees[f] = { quantity: parseInt(m[1]), amount: money(m[2]) };
  }
  const tf = text.match(/Total Fees:\s*\$?([\d,.]+)/);
  if (tf) s.totals.fees = money(tf[1]);

  Object.assign(s.totals, parseNetPaySummary(text));
  return s;
}

function parse0189Summary(text) {
  const s = {
    company: '0189', companyName: 'Davis Delivery Service Inc.', population: '1099',
    payPeriod: parsePeriod(text), checkDate: parseCheckDate(text), runId: parseRunId(text),
    payTypes: {}, deductions: {}, employerTaxes: {}, fees: {}, totals: {},
  };
  const pm = text.match(/^1099\s+\d+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/m);
  if (pm) s.payTypes['1099'] = { quantity: money(pm[1]), hours: money(pm[2]), amount: money(pm[3]) };

  const tp = text.match(/Total Pay:\s*([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)/);
  if (tp) s.totals.grossPay = money(tp[3]);

  for (const d of ['Advance', 'GOAL 1', 'INSURANC', 'INSURAN']) {
    const safe = d.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = text.match(new RegExp(`^${safe}\\s+([\\d,.]+)\\s+([\\d,.]+)\\s*$`, 'm'));
    if (m) s.deductions[d === 'INSURAN' ? 'INSURANC' : d] = { dedWages: money(m[1]), amount: money(m[2]) };
  }
  const td = text.match(/Total Deductions:\s*([\d,.]+)/);
  if (td) s.totals.deductions = money(td[1]);

  for (const f of ['Direct Deposit Items','Employee Direct Deposits','Mid Georgia','Online Access','Payroll Checks','Weekly Processing']) {
    const safe = f.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = text.match(new RegExp(`${safe}\\s+\\d+\\s+(\\d+)\\s+\\$?([\\d,.]+)`));
    if (m) s.fees[f] = { quantity: parseInt(m[1]), amount: money(m[2]) };
  }
  const tf = text.match(/Total Fees:\s*\$?([\d,.]+)/);
  if (tf) s.totals.fees = money(tf[1]);

  Object.assign(s.totals, parseNetPaySummary(text));
  return s;
}

function parseCheckListing(text) {
  const checks = [];
  const re = /^(\d{4,12})\s+(.+?)\s+(\d{1,2}\/\d{1,2}\/\d{4})\s+.+?\s+(PR|PD|PT|TS|PF|MC|SC|AG|SD)\s+([\d,.]+)\s*$/gm;
  let m;
  while ((m = re.exec(text)) !== null) {
    checks.push({
      checkNumber: m[1],
      payeeName: normWS(m[2]),
      checkDate: m[3],
      sourceCode: m[4],
      amount: money(m[5]),
    });
  }
  return checks;
}

// ============================================================================
// CHECK REGISTER — column-aware employee parser
// ============================================================================

const PAY_KEYWORDS = ['Hourly','OT1.5','Salary','PTO','Holiday','Bonus','Comm','1099'];
const TAX_KEYWORDS = ['FIT','FICA Med','GA SIT']; // FICA SS handled specially
const DED_KEYWORDS_W2 = ['401KEe','Dental','FSA','Hlth PT','Vision','Advance','Support1','Accident','Crit Ill','Life','Misc Ded','PerformD'];
const DED_KEYWORDS_1099 = ['Advance','GOAL 1','INSURAN','INSURANC'];
const ERTAX_KEYWORDS = ['FICA MED','FICA SS','FUTA','GA SUTA','GA SUPP'];

function splitEmployeeBlocks(registerText) {
  const lines = registerText.split('\n');
  const blocks = [];
  let current = null;
  for (const line of lines) {
    if (/\bID#:.*SS#:.*Pay:.*Net:.*Check #:/.test(line)) {
      if (current) blocks.push(current);
      const nameM = line.match(/^(.+?)\s+ID#:/);
      current = {
        rawName: nameM ? normWS(nameM[1]) : null,
        lines: [line],
      };
    } else if (current) {
      current.lines.push(line);
    }
  }
  if (current) blocks.push(current);
  return blocks;
}

function parseDataRow(lines) {
  for (const line of lines) {
    const m = line.match(
      /xxx-xx-(\d{4})\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(\d{1,2}\/\d{1,2}\/\d{4})\s+(\d{6,12})\s+([\d,.]+)/
    );
    if (m) {
      return {
        ssn: `xxx-xx-${m[1]}`,
        pay: money(m[2]),
        tips: money(m[3]),
        reimburse: money(m[4]),
        net: money(m[5]),
        checkDate: m[6],
        checkNumber: m[7],
        amount: money(m[8]),
      };
    }
  }
  return null;
}

function tryMatchSegment(seg, keywords, expectNums) {
  const sorted = [...keywords].sort((a,b) => b.length - a.length);
  for (const kw of sorted) {
    const safe = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const num = `([\\d,.-]+)`;
    const re = new RegExp(`^${safe}\\s+${Array(expectNums).fill(num).join('\\s+')}$`);
    const m = seg.match(re);
    if (m) {
      return { keyword: kw, nums: m.slice(1).map(money) };
    }
  }
  return null;
}

// Scan a line for all keyword occurrences and grab the N numbers following each.
// Uses keyword POSITIONS as anchors rather than whitespace-splitting, which
// breaks down because pdftotext uses variable whitespace within columns.
function scanLineForKeywords(line, keywordPools) {
  const hits = [];
  for (const pool of keywordPools) {
    for (const kw of pool.keywords) {
      const safe = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(`(^|\\s)(${safe})(?=\\s)`, 'g');
      let m;
      while ((m = re.exec(line)) !== null) {
        const kwStart = m.index + m[1].length;
        // Case-sensitivity: "FICA MED" (employer) vs "FICA Med" (employee) must not collide.
        const actual = line.substring(kwStart, kwStart + kw.length);
        if (actual !== kw) continue;
        hits.push({
          keyword: kw, pool: pool.kind, expectNums: pool.expectNums,
          start: kwStart, end: kwStart + kw.length,
        });
      }
    }
  }
  hits.sort((a, b) => a.start - b.start);

  // Dedupe overlapping hits (e.g. PAY "PTO" inside an accrual column text).
  // If two hits overlap, keep the earlier one only.
  const deduped = [];
  for (const h of hits) {
    const last = deduped[deduped.length - 1];
    if (last && h.start < last.end) continue;
    deduped.push(h);
  }

  const results = [];
  for (let i = 0; i < deduped.length; i++) {
    const h = deduped[i];
    const nextStart = i + 1 < deduped.length ? deduped[i + 1].start : line.length;
    const segment = line.substring(h.end, nextStart);
    const nums = [];
    const numRe = /[\d][\d,.]*/g;  // starts with digit
    let nm;
    while ((nm = numRe.exec(segment)) !== null) {
      nums.push(money(nm[0]));
    }
    if (nums.length >= h.expectNums) {
      results.push({ keyword: h.keyword, pool: h.pool, start: h.start, nums: nums.slice(0, h.expectNums) });
    }
  }
  return results;
}

function parseBlockColumns(lines, population) {
  const payRows = [];
  const taxes = [];
  const deductions = [];
  const employerTaxes = [];
  const dedKeywords = population === '1099' ? DED_KEYWORDS_1099 : DED_KEYWORDS_W2;

  // Order matters for case-insensitive disambiguation: put ERTAX before TAX
  // so "FICA MED" is claimed by ERTAX pool before TAX pool sees it.
  const keywordPools = [
    { keywords: PAY_KEYWORDS,   kind: 'pay',   expectNums: 4 },
    { keywords: ERTAX_KEYWORDS, kind: 'ertax', expectNums: 2 },
    { keywords: TAX_KEYWORDS,   kind: 'tax',   expectNums: 2 },
    { keywords: dedKeywords,    kind: 'ded',   expectNums: 2 },
  ];

  for (const line of lines) {
    if (!line.trim()) continue;
    if (/^\s*Pay\s+Rate\s+Quantity/.test(line)) continue;
    if (/Southern Payroll Services/.test(line)) continue;
    if (/Payroll Check Register/.test(line)) continue;
    if (/Page \d+ of \d+/.test(line)) continue;
    if (/ID#:.*SS#:.*Pay:.*Net:.*Check #:/.test(line)) continue;
    if (/xxx-xx-\d{4}.*\d{1,2}\/\d{1,2}\/\d{4}/.test(line)) continue;

    const hits = scanLineForKeywords(line, keywordPools);
    // Disambiguate FICA SS by absolute column position. Register layout has
    // EE Taxes column ~60-100 and Er Tax column ~130-165. Col 115 cleanly splits them.
    const EETAX_ERTAX_BOUNDARY = 115;

    for (const h of hits) {
      if (h.pool === 'pay') {
        payRows.push({
          type: h.keyword, rate: h.nums[0], quantity: h.nums[1],
          current: h.nums[2], ytd: h.nums[3],
        });
      } else if (h.pool === 'tax') {
        taxes.push({ type: h.keyword, current: h.nums[0], ytd: h.nums[1] });
      } else if (h.pool === 'ertax') {
        if (h.keyword === 'FICA SS' && h.start < EETAX_ERTAX_BOUNDARY) {
          taxes.push({ type: 'FICA SS', current: h.nums[0], ytd: h.nums[1] });
        } else {
          employerTaxes.push({ type: h.keyword, current: h.nums[0], ytd: h.nums[1] });
        }
      } else if (h.pool === 'ded') {
        const type = h.keyword === 'INSURAN' ? 'INSURANC' : h.keyword;
        deductions.push({ type, current: h.nums[0], ytd: h.nums[1] });
      }
    }
  }

  return { payRows, taxes, deductions, employerTaxes };
}

function parseEmployeeBlock(block, population) {
  const dataRow = parseDataRow(block.lines);
  if (!dataRow) return null;
  const cols = parseBlockColumns(block.lines, population);

  const timeTypes = new Set(['Hourly','OT1.5','PTO','Holiday']);
  const hours = cols.payRows.filter(r => timeTypes.has(r.type)).reduce((s, r) => s + r.quantity, 0);

  return {
    ssn: dataRow.ssn,
    rawName: block.rawName,
    canonicalKey: block.rawName ? canonicalKey(block.rawName) : null,
    pay: dataRow.pay,
    net: dataRow.net,
    tips: dataRow.tips,
    reimburse: dataRow.reimburse,
    checkNumber: dataRow.checkNumber,
    checkDate: dataRow.checkDate,
    hours,
    payRows: cols.payRows,
    taxes: cols.taxes,
    deductions: cols.deductions,
    employerTaxes: cols.employerTaxes,
  };
}

export function parsePayrollPDF(rawText) {
  const company = detectCompany(rawText);
  if (!company) return { error: 'Could not detect 0189 or 0190', company: null };

  const summary = company === '0190' ? parse0190Summary(rawText) : parse0189Summary(rawText);
  const registerStart = rawText.indexOf('Payroll Check Register');
  const registerEnd = rawText.indexOf('Report Totals:', registerStart);
  const registerText = registerStart >= 0
    ? rawText.slice(registerStart, registerEnd > 0 ? registerEnd : undefined)
    : '';
  const blocks = splitEmployeeBlocks(registerText);
  const employees = blocks.map(b => parseEmployeeBlock(b, summary.population)).filter(Boolean);
  const checks = parseCheckListing(rawText);
  const prChecks = checks.filter(c => c.sourceCode === 'PR');

  return {
    company,
    summary,
    employees,
    checks,
    validation: {
      employeeCount: employees.length,
      prCheckCount: prChecks.length,
      prCheckTotal: Math.round(prChecks.reduce((s, c) => s + c.amount, 0) * 100) / 100,
      grossPayFromSummary: summary.totals.grossPay,
    },
  };
}
