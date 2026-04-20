// Client-side PDF → layout-preserving text extraction for MarginIQ Payroll tab.
// Uses pdf.js (already a dependency from the Fuel tab).
//
// The parser expects pdftotext -layout output style. pdf.js gives us positioned
// text items per page; we reconstruct lines by clustering Y-coordinates, then
// pad items within each line using their X coordinates so column alignment is
// preserved — exactly what pdftotext -layout produces.

// pdf.js is loaded on-demand in the Fuel tab via CDN; reuse the same pattern.
let pdfjsLib = null;
async function loadPdfJs() {
  if (pdfjsLib) return pdfjsLib;
  // eslint-disable-next-line no-undef
  if (window.pdfjsLib) {
    pdfjsLib = window.pdfjsLib;
    return pdfjsLib;
  }
  // Load via CDN
  await new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js';
    s.onload = resolve;
    s.onerror = reject;
    document.head.appendChild(s);
  });
  // eslint-disable-next-line no-undef
  pdfjsLib = window.pdfjsLib;
  pdfjsLib.GlobalWorkerOptions.workerSrc =
    'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
  return pdfjsLib;
}

/**
 * Extract layout-preserving text from a PDF File or ArrayBuffer.
 * Groups items by Y coordinate (line clusters), sorts each line by X,
 * then reconstructs a single-line string using character-position padding
 * so columns align the way pdftotext -layout would render them.
 *
 * Tuning constants chosen to match pdftotext's default behavior on the
 * CyberPay PDFs we've validated against.
 */
export async function extractLayoutText(fileOrBuffer) {
  const pdfjs = await loadPdfJs();
  const buf = fileOrBuffer instanceof ArrayBuffer
    ? fileOrBuffer
    : await fileOrBuffer.arrayBuffer();
  const doc = await pdfjs.getDocument({ data: buf }).promise;

  // Average character width in points for the font used by Southern Payroll
  // PDFs (roughly 5.5pt for 8pt monospace). Tuned so the output matches the
  // column alignment our parser was validated against.
  const CHAR_WIDTH_PT = 5.4;
  const LINE_TOLERANCE_PT = 3; // items within 3pt Y are on the same visual line

  const pages = [];
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();

    // Each item: { str, transform: [a,b,c,d,x,y], width, height }
    // Group by Y coordinate
    const lineMap = new Map();
    for (const item of content.items) {
      if (!item.str) continue;
      const y = Math.round(item.transform[5]);
      // Find a bucket within tolerance
      let bucketKey = null;
      for (const k of lineMap.keys()) {
        if (Math.abs(k - y) < LINE_TOLERANCE_PT) { bucketKey = k; break; }
      }
      if (bucketKey === null) bucketKey = y;
      if (!lineMap.has(bucketKey)) lineMap.set(bucketKey, []);
      lineMap.get(bucketKey).push(item);
    }

    // Sort lines top-down (Y decreases going down in PDF coords)
    const lines = [...lineMap.entries()].sort((a, b) => b[0] - a[0]);

    const pageLines = [];
    for (const [, items] of lines) {
      // Sort items within line by X
      items.sort((a, b) => a.transform[4] - b.transform[4]);
      // Build the line by padding with spaces based on X coordinate
      let rendered = '';
      for (const it of items) {
        const targetCol = Math.floor(it.transform[4] / CHAR_WIDTH_PT);
        while (rendered.length < targetCol) rendered += ' ';
        rendered += it.str;
      }
      pageLines.push(rendered);
    }
    pages.push(pageLines.join('\n'));
  }

  // Form feed between pages matches pdftotext behavior
  return pages.join('\n\f\n');
}
