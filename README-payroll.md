# MarginIQ Payroll Module — Install Instructions

Deterministic CyberPay PDF parser with invariant cross-validation.
Validated at 21/21 checks against real 4/17/2026 payroll files.

## Files in this bundle

```
netlify/functions/
  marginiq-scan-payroll.mts    → /api/scan-payroll endpoint (Netlify function)
  lib/
    parsePayroll.mjs           → Deterministic CyberPay parser (407 lines)
    invariants.mjs             → Cross-validation tripwires (139 lines)

public/
  PayrollTab.jsx               → React component for the Payroll tab
  lib/
    extractPdfText.mjs         → Client-side pdf.js layout extractor
    identityMap.mjs            → Seed identity map + enrichment functions
```

## Install (run from your DavisMarginIQ repo root)

```bash
# 1. Extract this zip inside your local clone of DavisDelivery/DavisMarginIQ
#    (the folder paths inside the zip mirror the repo structure exactly)
cd ~/path/to/DavisMarginIQ
unzip ~/Downloads/marginiq-payroll.zip
# This creates: marginiq-payroll/
# The files inside already match repo paths — copy them into place:
rsync -av marginiq-payroll/ ./
rm -rf marginiq-payroll

# 2. Verify files landed in the right spots
ls netlify/functions/marginiq-scan-payroll.mts
ls netlify/functions/lib/parsePayroll.mjs
ls netlify/functions/lib/invariants.mjs
ls public/PayrollTab.jsx
ls public/lib/extractPdfText.mjs
ls public/lib/identityMap.mjs

# 3. Wire PayrollTab into MarginIQ.jsx
#    - Add at top:  import PayrollTab from './PayrollTab.jsx';
#    - Add 'payroll' to your tabs array (between Costs and Settings)
#    - Render: <PayrollTab identityMap={...} weeks={...} />

# 4. Commit and push
git add netlify/functions/marginiq-scan-payroll.mts \
        netlify/functions/lib/parsePayroll.mjs \
        netlify/functions/lib/invariants.mjs \
        public/PayrollTab.jsx \
        public/lib/extractPdfText.mjs \
        public/lib/identityMap.mjs
git commit -m "Add payroll module: deterministic CyberPay parser + Payroll tab"
git push origin main

# Netlify auto-deploys within ~90s to https://davis-marginiq.netlify.app
```

## Smoke test after deploy

1. Open https://davis-marginiq.netlify.app
2. Navigate to the Payroll tab → Upload sub-tab
3. Drop the `Paper_Delivery_0190_Combined_*.pdf` and `Paper_Delivery_0189_Combined_*.pdf` files
4. Expected result:
   - Green banner: "✅ 7 invariant checks passed" (W2)
   - Green banner: "✅ 3 invariant checks passed" (1099)
   - Yellow banner: "🆕 80 new people this week" — click to map

## What to do if the parser breaks later

If Southern Payroll ever changes their PDF format, at least one invariant
check will fail and the banner will show exactly which one. Examples:

- `sum_employee_pay_equals_gross` fail → pay row scanning is off
- `employee_count_matches_check_count` fail → block splitting broke
- `total_cost_equals_gross_plus_fees_plus_ertax` fail → Net Pay Summary box moved

Send the specific invariant label to Claude in a new chat and it'll pinpoint
the fix in the parser in minutes.
