/**
 * Squarified treemap layout (Bruls et al., "Squarified Treemaps", 2000).
 *
 * Ported from marin-gcs-usage's `site/src/squarify.ts`, generified to work on
 * any `{ size, ... }` node instead of hardcoded `{ b }`. The algorithm is:
 *
 *   walk items in decreasing-weight order; keep adding to the current row
 *   while it improves the worst-case aspect ratio; when it doesn't, lay
 *   out the row along the shorter side of the remaining rectangle and
 *   start a new row on what's left.
 *
 * This is the shape everyone expects when they say "treemap"; the classic
 * slice-and-dice alternative gives ugly stripes for skewed distributions.
 */

export interface Rect<T> {
  /** The input node this rect represents. */
  it: T
  x: number
  y: number
  w: number
  h: number
}

/**
 * Lay out `items` inside `[x, y, w, h]`, returning one Rect per item.
 *
 * @param items    non-negative-weighted items; zero-weight items are dropped
 * @param getSize  extracts the weight (bytes, count, whatever) from an item
 */
export function squarify<T>(
  items: T[],
  x: number,
  y: number,
  w: number,
  h: number,
  getSize: (it: T) => number,
): Rect<T>[] {
  const out: Rect<T>[] = []
  items = items.filter(it => getSize(it) > 0)
  const total = items.reduce((s, it) => s + getSize(it), 0)
  if (!total || w <= 0 || h <= 0) return out
  const scale = (w * h) / total
  let row: T[] = []
  let rowSum = 0
  const rest = items.slice()

  const worst = (r: T[], len: number): number => {
    const s = r.reduce((a, it) => a + getSize(it) * scale, 0)
    let mn = Infinity
    let mx = 0
    for (const it of r) {
      const a = getSize(it) * scale
      mn = Math.min(mn, a)
      mx = Math.max(mx, a)
    }
    const s2 = s * s
    const l2 = len * len
    return Math.max((l2 * mx) / s2, s2 / (l2 * mn))
  }

  const layoutRow = () => {
    const len = Math.min(w, h)
    const thick = rowSum / len
    let off = 0
    for (const it of row) {
      const l = (getSize(it) * scale) / thick
      if (w <= h) out.push({ it, x: x + off, y, w: l, h: thick })
      else out.push({ it, x, y: y + off, w: thick, h: l })
      off += l
    }
    if (w <= h) {
      y += thick
      h -= thick
    } else {
      x += thick
      w -= thick
    }
  }

  while (rest.length) {
    const len = Math.min(w, h)
    const it = rest[0]
    if (!row.length || worst([...row, it], len) <= worst(row, len)) {
      row.push(rest.shift()!)
      rowSum += getSize(it) * scale
    } else {
      layoutRow()
      row = []
      rowSum = 0
    }
  }
  if (row.length) layoutRow()
  return out
}

/**
 * Fold items too small to render at this scale into one synthetic node, so
 * their combined area shows as a single tile instead of dropped rows of
 * sub-6px cells (which read as dead space).
 *
 * The synthetic node is built by `mergeSmall`, which receives the folded
 * items and returns a stand-in of the same shape `T` (label like "(+3)",
 * summed size, and whatever else the consumer needs to preserve).
 *
 * @param items       candidate items
 * @param w, h        available viewport
 * @param getSize     size accessor
 * @param mergeSmall  builds the synthetic node from the folded items
 * @param minArea     min area in px² below which an item is "too small"
 *                    (default 16 = a 4×4 tile — below what a user can hit)
 */
export function foldSmall<T>(
  items: T[],
  w: number,
  h: number,
  getSize: (it: T) => number,
  mergeSmall: (small: T[]) => T,
  minArea = 16,
): T[] {
  const vis = items.filter(it => getSize(it) > 0)
  const total = vis.reduce((s, it) => s + getSize(it), 0)
  if (!total || w <= 0 || h <= 0) return vis
  const scale = (w * h) / total
  const kept = vis.filter(it => getSize(it) * scale >= minArea)
  const tiny = vis.filter(it => getSize(it) * scale < minArea)
  if (tiny.length < 2) return vis
  return [...kept, mergeSmall(tiny)]
}
