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

/** `0.123 TB·yr` — sig-fig formatting (Number() strips trailing zeros / exponent artifacts). */
export function formatTbYears(v: number, sigFigs = 3): string {
  return `${Number(v.toPrecision(sigFigs))} TB·yr`
}
