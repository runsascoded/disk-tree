// Pure helpers for the store root's size-over-time (specs/root-geneses.md
// §2): per-root traces stacked into bands, and each root's genesis — the
// first scan its trace has a point for.

export interface Pt { x: number; y: number }
export interface Band extends Pt { y0: number }
export interface Trace { key: string; points: Pt[] }
export interface Callout { x: number; y: number; below: boolean }

/** Interior points that read as prominent peaks (`maxes`) / valleys (`mins`),
 * for size callouts. Ranks each by topographic **prominence** — its drop to the
 * highest saddle on the path to higher terrain (min-space: to lower terrain) —
 * not "tallest within a window". Prominence matches the eye's 2-D sense of how
 * much a point stands out: a sharp spike beside a taller-but-distant peak still
 * scores high (you must descend into the trough between them to reach the taller
 * one), where a literal 2-D disk would mislabel steep slopes. `r` (x-units) is a
 * non-max-suppression radius: of two peaks within `r`, only the more prominent
 * survives (valleys likewise; a peak never suppresses a valley). Endpoints and
 * global extremes are the caller's job. O(n·peaks), fine at ~hundreds of scans. */
export function prominentExtrema(pts: Pt[], r: number): { maxes: Pt[]; mins: Pt[] } {
  // Peaks of `sign*y`: sign=1 finds maxima, sign=-1 finds minima.
  const peaks = (sign: number): Pt[] => {
    const v = (i: number) => sign * pts[i].y
    const cand: { i: number; prom: number }[] = []
    for (let i = 1; i < pts.length - 1; i++) {
      if (!(v(i) > v(i - 1) && v(i) > v(i + 1))) continue
      // Lowest saddle each way until terrain rises above this peak (or the end).
      let left = v(i)
      for (let j = i - 1; j >= 0 && v(j) <= v(i); j--) left = Math.min(left, v(j))
      let right = v(i)
      for (let j = i + 1; j < pts.length && v(j) <= v(i); j++) right = Math.min(right, v(j))
      cand.push({ i, prom: v(i) - Math.max(left, right) })
    }
    cand.sort((a, b) => b.prom - a.prom)
    const kept: number[] = []
    for (const c of cand) {
      if (kept.some(k => Math.abs(pts[k].x - pts[c.i].x) < r)) continue
      kept.push(c.i)
    }
    return kept.sort((a, b) => a - b).map(i => pts[i])
  }
  return { maxes: peaks(1), mins: peaks(-1) }
}

/** Which points earn a size callout on the over-time line: the series' first &
 * last, its global min & max, and — when a range is selected (`winX` = the
 * window edges' x's) — the point nearest each edge, so a selection reads the
 * size at its own start and end. When `radius` > 0, prominent interior
 * peaks/valleys at that suppression radius (`prominentExtrema`) are added too.
 * Points coinciding in a role (first is also max; a window edge lands on the
 * min; a prominent peak is the global max) collapse to one callout, keeping the
 * earliest-assigned role's placement. `below` puts the label under the point
 * (lows) rather than above it (highs). Ordered by first assignment (max, min,
 * first, last, window edges, then prominent peaks, valleys) for a stable render. */
export function pickAnnotations(pts: Pt[], winX?: readonly [number, number], radius?: number): Callout[] {
  if (pts.length < 2) return []
  let lo = pts[0]
  let hi = pts[0]
  for (const p of pts) {
    if (p.y < lo.y) lo = p
    if (p.y > hi.y) hi = p
  }
  const mid = (lo.y + hi.y) / 2
  const picks = new Map<Pt, boolean>() // point → below?
  picks.set(hi, false)
  picks.set(lo, true)
  for (const p of [pts[0], pts[pts.length - 1]]) if (!picks.has(p)) picks.set(p, p.y < mid)
  if (winX) {
    const nearest = (x: number) => pts.reduce((b, p) => (Math.abs(p.x - x) < Math.abs(b.x - x) ? p : b), pts[0])
    for (const x of winX) {
      const p = nearest(x)
      if (!picks.has(p)) picks.set(p, p.y < mid)
    }
  }
  if (radius && radius > 0) {
    const { maxes, mins } = prominentExtrema(pts, radius)
    for (const p of maxes) if (!picks.has(p)) picks.set(p, false)
    for (const p of mins) if (!picks.has(p)) picks.set(p, true)
  }
  return [...picks].map(([p, below]) => ({ x: p.x, y: p.y, below }))
}

/** Each trace's genesis x (its first point). */
export function geneses(traces: Trace[]): Map<string, number> {
  return new Map(traces.filter(t => t.points.length).map(t => [t.key, Math.min(...t.points.map(p => p.x))]))
}

/** The latest genesis among the traces (the x before which the total is
 * missing a root), or null with fewer than two traces. */
export function youngestGenesis(traces: Trace[]): number | null {
  const g = [...geneses(traces).values()]
  return g.length > 1 ? Math.max(...g) : null
}

/** Stack traces in order: each band runs from the running sum below it to
 * the sum including it, at every x any trace has. A trace without a point
 * at an x contributes 0 there (and gets no band point before its genesis). */
export function stackSeries(traces: Trace[]): { key: string; points: Band[] }[] {
  const xs = [...new Set(traces.flatMap(t => t.points.map(p => p.x)))].sort((a, b) => a - b)
  const running = new Map<number, number>(xs.map(x => [x, 0]))
  return traces.map(t => {
    const at = new Map(t.points.map(p => [p.x, p.y]))
    const g = t.points.length ? Math.min(...t.points.map(p => p.x)) : Infinity
    const points: Band[] = []
    for (const x of xs) {
      if (x < g) continue
      const y0 = running.get(x)!
      const y = y0 + (at.get(x) ?? 0)
      running.set(x, y)
      points.push({ x, y0, y })
    }
    return { key: t.key, points }
  })
}
