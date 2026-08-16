/**
 * Pure math for byte-weighted age histograms (spec: viz-widgets.md §4).
 *
 * The bins come from disk-tree's `/api/histogram` (or any equivalent query):
 * shared `edges` in epoch seconds, and per-item `bytes[i]` for the interval
 * `[edges[i], edges[i+1])`. Because the weight is bytes rather than file
 * count, the area under a threshold *is* the reclaimable byte total — which
 * is the number a "delete everything older than X" decision actually needs.
 */

/**
 * Bytes older than `t` (epoch seconds).
 *
 * Whole bins below `t` count fully; the bin straddling `t` contributes the
 * fraction of its time span that lies below `t`. That linear split assumes
 * bytes are spread evenly within a bin — the same assumption the bars
 * already draw, so the readout can't disagree with the picture.
 */
export function bytesOlderThan(edges: number[], bins: number[], t: number): number {
  let total = 0
  for (let i = 0; i < bins.length; i++) {
    const lo = edges[i]
    const hi = edges[i + 1]
    if (t >= hi) {
      total += bins[i]
    } else if (t > lo) {
      const span = hi - lo
      total += span > 0 ? (bins[i] * (t - lo)) / span : bins[i]
    }
  }
  return total
}

/** Σ of every bin. */
export function totalBytes(bins: number[]): number {
  return bins.reduce((a, b) => a + b, 0)
}

/**
 * Largest single bin across all items — the shared scale that keeps bar
 * *area* proportional to bytes across items, not just within one.
 */
export function peakBin(allBins: number[][]): number {
  let peak = 0
  for (const bins of allBins) for (const v of bins) if (v > peak) peak = v
  return peak
}

/**
 * `count` evenly-spaced tick values spanning `[lo, hi]` inclusive.
 * A degenerate range yields a single tick.
 */
export function timeTicks(lo: number, hi: number, count = 4): number[] {
  if (!(hi > lo)) return [lo]
  const n = Math.max(2, Math.round(count))
  return Array.from({ length: n }, (_, i) => lo + ((hi - lo) * i) / (n - 1))
}
