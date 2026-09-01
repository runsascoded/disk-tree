import { useLayoutEffect, useMemo, useRef } from 'react'
import { CONTAINER_BG, parseColor } from './colors'
import { drawDust } from './DustHatch'
import { resolveCellStyle, type StyleOpts } from './cellStyle'
import { flattenPlaced, hitTest, type FoldedNode, type PlacedCell } from './layout'
import { squarify } from './squarify'

/**
 * Whole-map canvas renderer for `<Treemap renderer="canvas">`.
 *
 * Paints the entire placed-cell tree to one `<canvas>` (parent-first, so
 * children draw over their container) and hit-tests the same retained tree —
 * one code path, one paint loop, no DOM node per cell. Interaction is not
 * re-implemented here: pointer hits resolve to `(node, path)` and are handed
 * straight back to `<Treemap>`'s drill/tooltip handlers, the identical path a
 * DOM cell's event takes.
 *
 * Parity is staged. This first cut draws base + faded bg + shared edge stroke +
 * dust tail + labels, and hit-tests cells (folded dust resolves to its specific
 * child, like the DOM tile). Segments (makeup stripes), chain-label rendering,
 * `cellHref` anchors, and the lazy-load overlay are still DOM-only.
 */

/** A resolved pointer hit: a real cell, or a specific child under a dust tile. */
export interface CanvasHit<T> {
  /** Always a real node — a folded tile resolves to the child under the cursor. */
  node: T
  path: T[]
  key: string
  drillable: boolean
  /** Whether this cell renders nested tiles (branch title-bar vs. leaf body). */
  branch: boolean
  /** This hit is a child resolved out of a dust tile (pins, never drills, and
   * bypasses `onCellClick` — matching the DOM dust tile). */
  foldChild: boolean
}

export interface TreemapCanvasProps<T> {
  cells: PlacedCell<T>[]
  width: number
  height: number
  styleOpts: StyleOpts<T>
  getSize: (n: T) => number
  getLabel: (n: T) => string
  formatSize: (n: number) => string
  idFor: (n: T, p: T[]) => string
  expandable: (n: T, p: T[]) => boolean
  dustTexture: boolean
  onHover: (hit: CanvasHit<T>, clientX: number, clientY: number) => void
  onClick: (hit: CanvasHit<T>, e: React.MouseEvent) => void
  onLeave: () => void
}

const CONTAINER_RGB = `rgb(${CONTAINER_BG[0]}, ${CONTAINER_BG[1]}, ${CONTAINER_BG[2]})`

/** A CSS color → `rgba()` at `alpha`, or null if it isn't a parseable solid
 * (a `var()`/gradient/`color-mix`) so the caller can skip or fall back. */
function rgbaAt(c: string | undefined, alpha: number): string | null {
  const p = parseColor(c ?? '')
  if (!p) return null
  return `rgba(${p[0]}, ${p[1]}, ${p[2]}, ${p[3] * alpha})`
}

export function TreemapCanvas<T>({
  cells,
  width,
  height,
  styleOpts,
  getSize,
  getLabel,
  formatSize,
  idFor,
  expandable,
  dustTexture,
  onHover,
  onClick,
  onLeave,
}: TreemapCanvasProps<T>) {
  const ref = useRef<HTMLCanvasElement>(null)
  // Parent-first flat list for painting; the same tree hit-tests deepest-first.
  const flat = useMemo(() => flattenPlaced(cells), [cells])

  useLayoutEffect(() => {
    const cv = ref.current
    if (!cv || width <= 0 || height <= 0) return
    const dpr = typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1
    cv.width = Math.round(width * dpr)
    cv.height = Math.round(height * dpr)
    const ctx = cv.getContext('2d')
    if (!ctx) return
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    // Map ground = container color; gaps gutters and empty space show through it.
    ctx.clearRect(0, 0, width, height)
    ctx.fillStyle = CONTAINER_RGB
    ctx.fillRect(0, 0, width, height)

    for (const cell of flat) {
      const { x, y, w, h, depth, mode, edge, dust, folded, showLbl, hasKids } = cell
      const shared = mode === 'shared'
      // Cell box: gaps leaves a 2px (1px dust) gutter to the container ground;
      // shared fills the exact rect.
      const cw = shared ? w : w - (dust ? 1 : 2)
      const ch = shared ? h : h - (dust ? 1 : 2)
      if (cw <= 0 || ch <= 0) continue

      const { style, builtinEdge } = resolveCellStyle(cell, styleOpts)
      const fade = styleOpts.fadeAt(depth)
      const alpha = fade * (style.opacity ?? 1)

      // Opaque base under the translucent bg layer (matches the DOM cell's
      // container-color base): so a faded bg recedes toward the container tone.
      ctx.fillStyle = CONTAINER_RGB
      ctx.fillRect(x, y, cw, ch)

      // Faded bg layer, inset by the half-stroke in shared mode.
      const inset = shared ? edge : 0
      const bx = x + inset
      const by = y + inset
      const bw = cw - 2 * inset
      const bh = ch - 2 * inset
      if (bw > 0 && bh > 0) {
        const fill = rgbaAt(style.bg, alpha)
        if (fill) {
          ctx.fillStyle = fill
          ctx.fillRect(bx, by, bw, bh)
        }
      }

      // Shared edge: this cell's half-stroke ring (contrast default or pinned).
      if (shared && edge > 0) {
        const stroke = style.edge ? rgbaAt(style.edge, 1) : builtinEdge ? rgbaAt(builtinEdge, 1) : null
        if (stroke) {
          ctx.strokeStyle = stroke
          ctx.lineWidth = edge
          ctx.strokeRect(x + edge / 2, y + edge / 2, cw - edge, ch - edge)
        }
      }

      // Dust tail: dashed frame + the tightening cross-hatch.
      if (folded && dustTexture && Math.min(w, h) >= 6) {
        if (Math.min(w, h) >= 8) {
          ctx.strokeStyle = 'rgba(150, 150, 165, 0.55)'
          ctx.lineWidth = 1
          ctx.setLineDash([2, 2])
          ctx.strokeRect(bx + 0.5, by + 0.5, bw - 1, bh - 1)
          ctx.setLineDash([])
        }
        const count = (cell.node as FoldedNode<T>).count
        drawDust(ctx, bx, by, bw, bh, count)
      }

      // Label: name (+ inline size when there's room). Ink stays full-strength.
      if (showLbl) {
        const ink = rgbaAt(style.ink, 1) ?? 'rgba(230, 230, 238, 1)'
        const small = w < 64
        const fs = small ? 11.5 : 13.5
        ctx.font = `${fs}px system-ui, -apple-system, sans-serif`
        ctx.textBaseline = 'top'
        ctx.fillStyle = ink
        const label = folded
          ? `(+${(cell.node as FoldedNode<T>).count})`
          : cell.chainLabels
            ? (cell.chainLabels.length > 3
                ? `${cell.chainLabels[0]}/…/${cell.chainLabels[cell.chainLabels.length - 1]}`
                : cell.chainLabels.join('/'))
            : getLabel(cell.node as T)
        // Clip text to the cell so long names don't bleed across neighbors.
        ctx.save()
        ctx.beginPath()
        ctx.rect(x, y, cw, ch)
        ctx.clip()
        const pad = 4
        const kidSize = folded ? (cell.node as FoldedNode<T>).size : getSize(cell.node as T)
        const inlineSize = (hasKids || h <= 34) && w > 90
        let nameMax = cw - 2 * pad
        if (inlineSize) {
          const szText = formatSize(kidSize)
          const szW = ctx.measureText(szText).width
          nameMax -= szW + 8
          ctx.globalAlpha = 0.75
          ctx.fillText(szText, x + cw - pad - szW, y + 2)
          ctx.globalAlpha = 1
        }
        ctx.fillText(fit(ctx, label, Math.max(0, nameMax)), x + pad, y + 2)
        // Second-line size for a tall leaf (name owns the first line).
        if (!hasKids && !inlineSize && h > 34 && w > 40) {
          ctx.globalAlpha = 0.75
          ctx.font = `11.5px system-ui, -apple-system, sans-serif`
          ctx.fillText(fit(ctx, formatSize(kidSize), cw - 2 * pad), x + pad, y + 2 + fs + 3)
          ctx.globalAlpha = 1
        }
        ctx.restore()
      }
    }
  }, [flat, width, height, styleOpts, getSize, getLabel, formatSize, dustTexture])

  /** Squarify a folded tile's children over its box, to resolve a dust hover. */
  const dustChildAt = (
    cell: PlacedCell<T>,
    lx: number,
    ly: number,
  ): { node: T; path: T[]; key: string } | null => {
    const fn = cell.node as FoldedNode<T>
    if (!dustTexture || fn.children.length <= 1 || fn.children.length > 4000) return null
    if (Math.min(cell.w, cell.h) < 10) return null
    const rects = squarify<T>(fn.children, 0, 0, cell.boxW, cell.boxH, getSize)
    const rx = lx - cell.x
    const ry = ly - cell.y
    const hit = rects.find(rc => rx >= rc.x && rx < rc.x + rc.w && ry >= rc.y && ry < rc.y + rc.h)
    if (!hit) return null
    const path = [...cell.path, hit.it]
    return { node: hit.it, path, key: idFor(hit.it, path) }
  }

  const toHit = (cell: PlacedCell<T>, lx: number, ly: number): CanvasHit<T> | null => {
    if (cell.folded) {
      const child = dustChildAt(cell, lx, ly)
      if (!child) return null
      return { node: child.node, path: child.path, key: child.key, drillable: false, branch: false, foldChild: true }
    }
    const node = cell.node as T
    return {
      node,
      path: cell.path,
      key: idFor(node, cell.path),
      drillable: expandable(node, cell.path),
      branch: cell.hasKids,
      foldChild: false,
    }
  }

  const localXY = (e: React.MouseEvent) => {
    const box = (e.currentTarget as HTMLElement).getBoundingClientRect()
    return { lx: e.clientX - box.left, ly: e.clientY - box.top }
  }

  return (
    <canvas
      ref={ref}
      width={Math.max(1, Math.round(width))}
      height={Math.max(1, Math.round(height))}
      style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', cursor: 'default' }}
      onMouseMove={e => {
        const { lx, ly } = localXY(e)
        const cell = hitTest(cells, lx, ly)
        // No-hit (a gutter between cells): leave the current tip alone, matching
        // the DOM renderer where sweeping a gap doesn't clear. Real exit clears.
        if (!cell) return
        const hit = toHit(cell, lx, ly)
        if (hit) onHover(hit, e.clientX, e.clientY)
      }}
      onMouseLeave={onLeave}
      onClick={e => {
        const { lx, ly } = localXY(e)
        const cell = hitTest(cells, lx, ly)
        if (!cell) return
        const hit = toHit(cell, lx, ly)
        if (hit) onClick(hit, e)
      }}
    />
  )
}

/** Truncate `text` with an ellipsis to fit `maxW` px in the current ctx font. */
function fit(ctx: CanvasRenderingContext2D, text: string, maxW: number): string {
  if (maxW <= 0) return ''
  if (ctx.measureText(text).width <= maxW) return text
  let lo = 0
  let hi = text.length
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1
    if (ctx.measureText(text.slice(0, mid) + '…').width <= maxW) lo = mid
    else hi = mid - 1
  }
  return lo > 0 ? text.slice(0, lo) + '…' : ''
}
