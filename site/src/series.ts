// Pure helpers for the store root's size-over-time (specs/root-geneses.md
// §2): per-root traces stacked into bands, and each root's genesis — the
// first scan its trace has a point for.

export interface Pt { x: number; y: number }
export interface Band extends Pt { y0: number }
export interface Trace { key: string; points: Pt[] }

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
