/**
 * Color primitives for disk-tree widgets. All CSS-var-friendly so consumers
 * can theme without touching component code.
 */

import type { CellStyle } from './Treemap'

/**
 * Diverging red-negative / green-positive scale for delta coloring
 * (`Δbytes`, `Δcount`). Returns an rgb-triple string, always at max
 * saturation to keep signs unambiguous; the *intensity* comes from
 * background/opacity, not from the hue.
 *
 * `t` should be in `[-1, 1]`. Values outside are clamped.
 */
export function divergingColor(t: number): string {
  const c = Math.max(-1, Math.min(1, t))
  if (c === 0) return 'rgb(139, 148, 158)' // neutral gray
  if (c > 0) {
    // grew (positive) → red gradient, brightening with magnitude
    const a = 0.15 + 0.85 * c
    return `rgba(248, 81, 73, ${a.toFixed(3)})`
  }
  const a = 0.15 + 0.85 * -c
  return `rgba(63, 185, 80, ${a.toFixed(3)})`
}

/**
 * Ink color that reads well on top of `divergingColor(t)`.
 * Roughly: switches to white once the delta is intense enough that the
 * background is dark enough to need it.
 */
export function divergingInk(t: number): string {
  return Math.abs(t) > 0.35 ? '#fff' : 'var(--dt-treemap-ink, #d0d0d8)'
}

export interface AgeFadeOpts {
  /**
   * Max fraction of the panel color mixed in at `age01 = 1` — the fade
   * *floor* clamp, so the oldest cells stay legible. Default 0.72.
   */
  floor?: number
  /** Color faded toward. Default: the themable panel background var. */
  panel?: string
}

/**
 * Age lens: fade a resolved cell style toward the panel background —
 * "fading from memory". Older (`age01` → 1) ⇒ more faded.
 *
 * Uses `color-mix(in oklch, …)` so the fade is *perceptual lightness*, even
 * across hues (equal age reads as equal fade — an RGB-alpha ramp doesn't),
 * and works with CSS-var colors the JS can't parse. Ink fades at half the
 * background's rate, preserving label contrast on faded cells.
 *
 * Composable: takes and returns a `CellStyle`, so it stacks on the default
 * categorical palette or any `colorForCell` output — pass via the treemap's
 * `lens` slot rather than replacing the color logic.
 */
export function ageFade(style: CellStyle, age01: number, opts?: AgeFadeOpts): CellStyle {
  const t = Math.max(0, Math.min(1, age01))
  const floor = opts?.floor ?? 0.72
  const p = t * floor * 100
  if (p < 0.5 || !style.bg) return style
  const panel = opts?.panel ?? 'var(--dt-treemap-fade-panel, #1a1a1f)'
  const mix = (c: string, pct: number) => `color-mix(in oklch, ${c}, ${panel} ${pct.toFixed(0)}%)`
  return {
    ...style,
    bg: mix(style.bg, p),
    ...(style.ink && { ink: mix(style.ink, p / 2) }),
  }
}

/**
 * [oldest, newest] age extent over `nodes` (ignoring null/undefined ages) —
 * feed to `age01`. Returns `null` when no node has an age (lens should no-op).
 */
export function ageDomain<T>(nodes: T[], getAge: (n: T) => number | null | undefined): [number, number] | null {
  let lo = Infinity
  let hi = -Infinity
  for (const n of nodes) {
    const a = getAge(n)
    if (a == null) continue
    if (a < lo) lo = a
    if (a > hi) hi = a
  }
  return lo <= hi ? [lo, hi] : null
}

/**
 * Normalize an age (epoch seconds, days — any monotonic "older = smaller"
 * unit like mtime) into the `[0, 1]` fade domain: the *newest* value in the
 * domain maps to 0 (no fade), the oldest to 1. Degenerate domains map to 0.
 */
export function age01(age: number, [oldest, newest]: [number, number]): number {
  if (newest <= oldest) return 0
  return Math.max(0, Math.min(1, (newest - age) / (newest - oldest)))
}
