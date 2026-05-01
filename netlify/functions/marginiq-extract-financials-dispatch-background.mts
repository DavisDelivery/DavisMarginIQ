import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Worker fanout coordinator (background, v2.50.1).
 *
 * Receives a list of periods to extract and fires the per-period
 * background workers in waves so that we stay under Anthropic's
 * input-tokens-per-minute (ITPM) limit.
 *
 * Why a separate background function:
 *   - The synchronous dispatcher has a 26s wall-clock cap. With 43 PDFs
 *     at ~21K input tokens each and a Tier 2 ITPM limit of 450K, we can
 *     only fire ~20 workers per minute without throttling. A 43-PDF run
 *     therefore needs ~2-3 minutes of paced firing, which doesn't fit.
 *   - context.waitUntil only extends as far as the function's own
 *     execution limit (26s for sync). So we move the staggered firing
 *     into its own background function (15-min budget).
 *
 * Body shape (POST JSON):
 *   {
 *     batch_id: "batch_...",
 *     periods: ["2025-12", "2025-11", ...],
 *     wave_size: 18,            // optional, default 18 (safety margin under 20)
 *     wave_pause_seconds: 65,   // optional, default 65 (safety margin over 60)
 *   }
 *
 * Returns 202 immediately to the dispatcher; runs in background until done.
 */

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return new Response("POST required", { status: 405 });

  const origin = new URL(req.url).origin;
  const workerUrl = `${origin}/.netlify/functions/marginiq-extract-financial-background`;

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const batchId: string = body.batch_id;
  const periods: string[] = Array.isArray(body.periods) ? body.periods : [];
  const waveSize: number = Math.max(1, Math.min(50, body.wave_size || 18));
  const wavePauseSeconds: number = Math.max(0, Math.min(300, body.wave_pause_seconds || 65));

  if (!batchId || periods.length === 0) {
    return new Response("batch_id and periods required", { status: 400 });
  }

  console.log(`[dispatch-bg ${batchId}] starting ${periods.length} periods in waves of ${waveSize}, ${wavePauseSeconds}s pause`);

  const fireOne = async (period: string): Promise<void> => {
    try {
      const r = await fetch(workerUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ period, batch_id: batchId }),
      });
      // Background functions return 202 fast. Anything else logs but
      // doesn't abort the run — the worker also writes its own
      // failure state to extract_jobs/{period} on platform errors.
      if (r.status !== 202 && r.status !== 200) {
        console.error(`[dispatch-bg ${batchId}] worker fire ${period}: status ${r.status}`);
      }
    } catch (e: any) {
      console.error(`[dispatch-bg ${batchId}] worker fire ${period} error:`, e.message || e);
    }
  };

  // Fire in waves
  for (let i = 0; i < periods.length; i += waveSize) {
    const wave = periods.slice(i, i + waveSize);
    const waveNum = Math.floor(i / waveSize) + 1;
    const totalWaves = Math.ceil(periods.length / waveSize);
    console.log(`[dispatch-bg ${batchId}] wave ${waveNum}/${totalWaves}: firing ${wave.length} periods`);

    // Fire the wave with a tiny intra-wave stagger (50ms) to avoid
    // firing 18 simultaneous TCP connects from one process.
    const fires: Promise<void>[] = [];
    for (const period of wave) {
      fires.push(fireOne(period));
      await new Promise(r => setTimeout(r, 50));
    }
    await Promise.all(fires);

    // Pause before next wave (unless this was the last wave)
    if (i + waveSize < periods.length) {
      console.log(`[dispatch-bg ${batchId}] wave ${waveNum} fired, sleeping ${wavePauseSeconds}s`);
      await new Promise(r => setTimeout(r, wavePauseSeconds * 1000));
    }
  }

  console.log(`[dispatch-bg ${batchId}] all ${periods.length} workers fired`);
  return new Response(JSON.stringify({ ok: true, batch_id: batchId, fired: periods.length }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};
