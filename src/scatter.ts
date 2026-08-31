/**
 * Pure log-log scatter math (spec: viz-widgets.md §3) — the whole
 * `<StalenessScatter>` layout reduces to these functions, so they're
 * unit-tested directly rather than through the DOM.
 *
 * The axes are decades: x = age in years, y = size in TB. On *those* units
 * the product `x·y` is exactly the sum-TB·years score (see `stats.ts`), so
 * an iso-score contour is the line `x·y = C` — a −1 slope in log-log space,
 * and the labeled diagonals are true iso-score lines rather than a visual
 * approximation.
 */

import { pow10 } from './stats'

// Guards decade arithmetic against log10 landing a hair off an integer.
const EPS = 1e-9

/**
 * Decade-snapped `[lo, hi]` covering the positive values (non-positive /
 * non-finite / nullish are skipped — log axes have nothing to say about
 * them). `null` when nothing is plottable; a single distinct value gets a
 * decade of headroom either side.
 */
export function logDomain(values: Iterable<number | null | undefined>): [number, number] | null {
  let min = Infinity
  let max = -Infinity
  for (const v of values) {
    if (v == null || !Number.isFinite(v) || v <= 0) continue
    if (v < min) min = v
    if (v > max) max = v
  }
  if (min > max) return null
  const lo = pow10(Math.floor(Math.log10(min) + EPS))
  const hi = pow10(Math.ceil(Math.log10(max) - EPS))
  return lo < hi ? [lo, hi] : [lo / 10, lo * 10]
}

/** Fraction along a log axis: `lo` → 0, `hi` → 1, clamped outside. */
export function logPos(v: number, [lo, hi]: [number, number]): number {
  if (!(v > 0) || !(hi > lo)) return 0
  const t = (Math.log10(v) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo))
  return Math.max(0, Math.min(1, t))
}

/** Decade ticks inside `[lo, hi]`, thinned by whole decades to at most `maxTicks`. */
export function logTicks([lo, hi]: [number, number], maxTicks = 6): number[] {
  const k0 = Math.ceil(Math.log10(lo) - EPS)
  const k1 = Math.floor(Math.log10(hi) + EPS)
  if (k1 < k0) return []
  const step = Math.max(1, Math.ceil((k1 - k0 + 1) / maxTicks))
  const out: number[] = []
  for (let k = k0; k <= k1; k += step) out.push(pow10(k))
  return out
}

/**
 * The iso-score contour `x·y = score`, clipped to the plot box — returned as
 * its two endpoints. `null` when the contour misses the box or only grazes a
 * corner (a zero-length segment is not worth a label).
 */
export function isoScoreSegment(
  score: number,
  [x0, x1]: [number, number],
  [y0, y1]: [number, number],
): [[number, number], [number, number]] | null {
  if (!(score > 0)) return null
  const xa = Math.max(x0, score / y1)
  const xb = Math.min(x1, score / y0)
  if (!(xa < xb)) return null
  return [[xa, score / xa], [xb, score / xb]]
}

/**
 * Decades strictly between `lo` and `hi`, thinned to at most `maxCount` by
 * stepping up whole decades from the lowest.
 */
export function decadesBetween(lo: number, hi: number, maxCount = 5): number[] {
  if (!(lo > 0) || !(hi > 0)) return []
  const k0 = Math.ceil(Math.log10(lo) + EPS)
  const k1 = Math.floor(Math.log10(hi) - EPS)
  if (k1 < k0) return []
  const step = Math.max(1, Math.ceil((k1 - k0 + 1) / maxCount))
  const out: number[] = []
  for (let k = k0; k <= k1; k += step) out.push(pow10(k))
  return out
}

/**
 * Decade scores (…, 0.1, 1, 10, …) whose contours cross the box's interior.
 * The box's corners bound the score range: `x0·y0` at the bottom-left,
 * `x1·y1` at the top-right.
 *
 * Prefer `isoScoresForData` when you have the points: a box corner is the
 * *combination* of extremes (smallest age × smallest size), which usually no
 * point occupies, so corner-derived decades cluster in an empty corner
 * instead of running through the data.
 */
export function isoScoreDecades(
  [x0, x1]: [number, number],
  [y0, y1]: [number, number],
  maxLines = 5,
): number[] {
  return decadesBetween(x0 * y0, x1 * y1, maxLines)
}

/**
 * Iso-score decades spanning the *data's* own score range, kept only where
 * the contour actually crosses the box. Falls back to the box-corner
 * decades when the scores span less than a decade (so a tightly-clustered
 * level still gets a reference line).
 */
export function isoScoresForData(
  scores: number[],
  xDomain: [number, number],
  yDomain: [number, number],
  maxLines = 5,
): number[] {
  const pos = scores.filter(s => s > 0 && Number.isFinite(s))
  const inBox = (s: number) => isoScoreSegment(s, xDomain, yDomain) !== null
  if (pos.length) {
    const found = decadesBetween(Math.min(...pos), Math.max(...pos), maxLines).filter(inBox)
    if (found.length) return found
    // Sub-decade spread: label the decade the data sits on, if it fits.
    const mid = pow10(Math.round(Math.log10(Math.max(...pos))))
    if (inBox(mid)) return [mid]
  }
  return isoScoreDecades(xDomain, yDomain, maxLines).filter(inBox)
}

/**
 * Marker radius with **area ∝ weight** (the honest encoding for a count
 * channel), floored at `rMin` so zero / missing weights stay clickable.
 */
export function radiusFor(
  weight: number | null | undefined,
  maxWeight: number,
  rMin = 2.5,
  rMax = 14,
): number {
  if (weight == null || !(weight > 0) || !(maxWeight > 0)) return rMin
  return Math.max(rMin, rMax * Math.sqrt(Math.min(1, weight / maxWeight)))
}
