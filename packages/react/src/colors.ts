/**
 * Color primitives for disk-tree widgets. All CSS-var-friendly so consumers
 * can theme without touching component code.
 */

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
