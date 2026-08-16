/**
 * Sum-TB·years — the additive staleness score (spec: viz-widgets.md §0).
 *
 * Each dir's score is Σ over descendant files of `size_i × age_i`. By the
 * identity Σ size·age = now·Σsize − Σ size·mtime, it reduces to
 * `size × (now − mtime_mean)` — no per-file data needed, just the
 * size-weighted mean mtime the disk-tree engines already emit
 * (`--mean-mtime` / `mtime_mean`). It cascades like `size` (node score =
 * Σ children's scores), so it's treemap-able with honest part-of-whole
 * semantics, and iso-score diagonals on a log-log (age, bytes) scatter are
 * exact.
 */

export const SEC_PER_YEAR = 365.25 * 86_400
export const TB = 1e12

/**
 * Exact, platform-stable `10 ** k` for integer `k`.
 *
 * `**` / `Math.pow` are implementation-approximated, not correctly rounded:
 * `10 ** -4` is `0.0001` on some engines and `0.00009999999999999999` on
 * others (this bit CI on decade tick values). Repeated multiplication is
 * exact through 1e22, and dividing 1 by an exact integer is correctly
 * rounded — so this always returns the same double as the decimal literal.
 */
export function pow10(k: number): number {
  let v = 1
  const n = Math.abs(Math.trunc(k))
  for (let i = 0; i < n; i++) v *= 10
  return k < 0 ? 1 / v : v
}

/**
 * Σ descendant-file size·age in TB·years, from a node's total size and
 * size-weighted mean mtime. `null` in (no mean available — e.g. zero-byte
 * dirs) → `null` out.
 */
export function sumTbYears(
  sizeBytes: number,
  mtimeMeanSec: number | null | undefined,
  nowSec: number,
): number | null {
  if (mtimeMeanSec == null) return null
  return (sizeBytes / TB) * ((nowSec - mtimeMeanSec) / SEC_PER_YEAR)
}

const SCORE_UNITS = ['B·yr', 'KB·yr', 'MB·yr', 'GB·yr', 'TB·yr', 'PB·yr']

/**
 * Format a TB·years score, stepping the *byte* half of the unit so real
 * values stay readable: `1.5 TB·yr`, `123 GB·yr`, `10 MB·yr`. (Fixed TB·yr
 * would render most directory-level scores as `1e-5 TB·yr`.)
 */
export function formatTbYears(v: number, sigFigs = 3): string {
  if (!(v > 0)) return '0 TB·yr'
  const byteYears = v * TB
  const e = Math.max(0, Math.min(SCORE_UNITS.length - 1, Math.floor(Math.log10(byteYears) / 3)))
  return `${Number((byteYears / pow10(3 * e)).toPrecision(sigFigs))} ${SCORE_UNITS[e]}`
}
