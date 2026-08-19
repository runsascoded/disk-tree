import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, ReactNode } from 'react'
import { DEFAULT_PALETTE } from './colors'
import { foldSmall, squarify } from './squarify'
import { useHoverPin } from './useHoverPin'

/**
 * Generic squarified treemap.
 *
 * Data shape is opaque: all extraction happens through accessors. That's
 * what lets marin's dense per-path `{ n, b, o, tm, sh, us, cb }` node and
 * disk-tree's `{ path, size, kind, children }` node share this component.
 *
 * The presentation surface is all slot-based, and every slot has a working
 * default:
 *   - `colorForCell` — override cell background/text/hatch. Default: an
 *     8-slot categorical hue by top-level index (stable across renders).
 *   - `renderTooltip` — return a React node for the hover/pin tooltip.
 *     Default: label + size (via `formatSize`).
 *   - `renderRollup` — extra row rendered above the map (e.g. team/user
 *     rollup + $/mo). Default: none.
 *   - `renderLegend` — legend row (right side of the crumbs bar).
 *   - `onCellClick` — override the built-in drill-on-branch,
 *     pin-tooltip-on-leaf behavior.
 *
 * Interaction is baked-in but overridable: click a branch to drill; click a
 * leaf to pin its tooltip; Backspace/Escape pops the drill stack; unpin
 * with Escape or an outside click.
 */

export interface TreemapProps<T> {
  root: T
  /** Extract this node's *own* aggregated size (bytes, count, whatever). */
  getSize: (n: T) => number
  /** Extract children; return `undefined` or `[]` for leaves. */
  getChildren: (n: T) => T[] | undefined
  /**
   * Whether this node has children *that may not be loaded yet*. Only
   * consulted alongside `loadChildren`; without it, "has children" is just
   * `getChildren(n)?.length`.
   *
   * This is the distinction a lazily-loaded tree needs and `getChildren`
   * can't express: no children in hand means "leaf" to a fully-materialized
   * tree but "not fetched yet" to a paged one. A server that answers with a
   * bounded depth (disk-tree's `/api/scan?uri=…&depth=N`) knows the
   * difference and usually says so — `n_children > 0`, `kind === 'dir'`.
   */
  hasChildren?: (n: T) => boolean
  /**
   * Fetch a node's children on demand. Called when the *viewed* node's
   * children aren't in hand and `hasChildren` says it has some — one fetch
   * per drill, never per rendered cell, so drilling costs one request rather
   * than fanning out over everything on screen.
   *
   * Resolved children are cached inside the component (keyed by `getId`) for
   * as long as `root` is unchanged; `onChildrenLoaded` lets a consumer that
   * owns its own tree persist them too. Rejections surface through
   * `renderLoadError` with a retry.
   */
  loadChildren?: (n: T, path: T[]) => Promise<T[]>
  /** Called after `loadChildren` resolves, before the cells render. */
  onChildrenLoaded?: (n: T, path: T[], children: T[]) => void
  /** Map-area content while the viewed node's children load. Default: "Loading…". */
  renderLoading?: (n: T, path: T[]) => ReactNode
  /** Map-area content when a load rejects. Default: the message + a Retry button. */
  renderLoadError?: (n: T, path: T[], error: Error, retry: () => void) => ReactNode
  /** Human label shown in the cell (and in breadcrumbs). */
  getLabel: (n: T) => string
  /** Stable key for this node — defaults to the joined path label. */
  getId?: (n: T, path: T[]) => string
  /** Format `getSize`'s return value for display. Default: raw number toLocaleString. */
  formatSize?: (n: number) => string

  /**
   * Cell background + text color + optional hatch overlay. Return `null`
   * (or `undefined`) to defer to the default categorical palette for that
   * cell — useful for callers that only want to override a subset of cells
   * (e.g. only synthetic "…" placeholders).
   *
   * `ctx` carries render-time facts the data alone can't know: the cell's
   * on-screen dims and whether it renders nested child tiles (`hasKids`) —
   * containers usually want a neutral bg so the nested tiles carry the data
   * colors, and leaf-only treatments (hatch, highlight-dim) key off it.
   */
  colorForCell?: (n: T, path: T[], depth: number, ctx: CellCtx) => CellStyle | null | undefined
  /**
   * Post-resolution style transform, applied to whatever `colorForCell` or
   * the default palette produced — so lenses (e.g. the age fade in
   * `colors.ts`) *stack* on any color mode instead of replacing it. Return
   * null/undefined to leave the resolved style untouched. Not called for
   * synthetic folded tiles.
   */
  lens?: (n: T, path: T[], depth: number, ctx: CellCtx, style: CellStyle) => CellStyle | null | undefined
  /** Optional extra content rendered inside the cell after the label. */
  renderCellExtra?: (n: T, path: T[], dims: CellDims) => ReactNode
  /** Tooltip body; return null to suppress the tooltip. */
  renderTooltip?: (n: T, path: T[]) => ReactNode
  /** Extra row above the map (e.g. rollup / totals). */
  renderRollup?: (n: T, path: T[]) => ReactNode
  /** Right side of the breadcrumbs bar. */
  renderLegend?: (n: T, path: T[]) => ReactNode
  /**
   * Replaces the default `— <size>` suffix after the breadcrumbs (e.g. to
   * add object counts / $-estimates). Return null to render no suffix.
   */
  renderCrumbSuffix?: (n: T, path: T[]) => ReactNode
  /** Row rendered below the map (e.g. a usage-hint footer). */
  renderFooter?: (n: T, path: T[]) => ReactNode
  /**
   * Right side of the cell size line — e.g. "$1.2/mo". Rendered inline with
   * the size when the cell is big enough to fit a subtitle.
   */
  renderCellSubtitle?: (n: T, path: T[]) => ReactNode

  /**
   * Override the click handler. Return `true` to skip the built-in
   * drill/pin behavior (i.e. the consumer handled it).
   */
  onCellClick?: (n: T, path: T[], event: React.MouseEvent) => boolean | void
  /** Called whenever the drill path changes (drill in, drill back). */
  onPathChange?: (path: T[]) => void
  /**
   * Fold small-area cells (below `minCellArea` in px²) into a synthetic
   * "…" tile. Pass `null` to disable folding.
   */
  minCellArea?: number | null
  /**
   * Build the folded stand-in as a *first-class* `T` (label, aggregated
   * size, and whatever the consumer's tooltip/colors need). When given, the
   * folded tile gets normal label/tooltip/click treatment; when omitted, a
   * synthetic gray `(+n)` tile with no tooltip is used.
   */
  mergeSmall?: (small: T[]) => T
  /** Show the fullscreen toggle button. Default: true. */
  fullscreen?: boolean
  /** Render the breadcrumbs/legend bar. Default: true. */
  chrome?: boolean
  /** Render in-cell labels. Default: true. (`false` + `chrome={false}` ≈ a redacted/og render.) */
  showLabels?: boolean
  /** Extra className on the outer wrapper. */
  className?: string
  /** Style overrides on the map area. */
  mapStyle?: CSSProperties
  /**
   * Opacity applied to every nested (depth > 0) cell. Default: 0.82.
   *
   * Cells are nested DOM nodes, so this *compounds*: at the default a depth-5
   * cell renders at 0.92 × 0.82⁴ ≈ 0.42, which reads as progressively washed
   * out the deeper you go. That is the intended "recede into the background"
   * effect for shallow trees, but a deep one loses its colors entirely — pass
   * `1` to keep saturation constant and lean on borders for structure.
   */
  depthFade?: number
  /** Opacity of the outermost (depth 0) cells. Default: 0.92. */
  rootFade?: number
}

export interface CellStyle {
  /** background CSS color/gradient */
  bg?: string
  /** ink color for the label */
  ink?: string
  /** repeating-gradient overlay (e.g. the class-lens hatch marin uses) */
  hatch?: string
  /** opacity multiplier (0–1). Combined with the built-in depth fade. */
  opacity?: number
}

export interface CellDims {
  w: number
  h: number
}

/** Render-time context passed to `colorForCell`. */
export interface CellCtx extends CellDims {
  /** Whether this cell renders nested child tiles at the current size. */
  hasKids: boolean
}

const DEFAULT_SLOTS = DEFAULT_PALETTE

const defaultFormat = (n: number) => n.toLocaleString('en-US')

const defaultId = <T,>(_n: T, path: T[], getLabel: (n: T) => string): string =>
  path.map(getLabel).join('/')

interface TipState<T> {
  x: number
  y: number
  key: string
  node: T
  path: T[]
}

/** Synthetic node returned by the default fold-small merger. */
interface FoldedNode<T> {
  __folded: true
  count: number
  size: number
  children: T[]
}

function isFolded<T>(n: T | FoldedNode<T>): n is FoldedNode<T> {
  return typeof n === 'object' && n !== null && (n as FoldedNode<T>).__folded === true
}

/** Centered overlay for the lazy-load loading/error states. */
const STATUS_STYLE: CSSProperties = {
  position: 'absolute',
  inset: 0,
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  gap: 2,
  textAlign: 'center',
  padding: 12,
  color: 'var(--dt-treemap-ink, #d0d0d8)',
  background: 'var(--dt-treemap-status-bg, rgba(0,0,0,0.35))',
  pointerEvents: 'auto',
}

export function Treemap<T>({
  root,
  getSize,
  getChildren,
  hasChildren,
  loadChildren,
  onChildrenLoaded,
  renderLoading,
  renderLoadError,
  getLabel,
  getId,
  formatSize = defaultFormat,
  colorForCell,
  lens,
  renderCellExtra,
  renderTooltip,
  renderRollup,
  renderLegend,
  renderCrumbSuffix,
  renderFooter,
  renderCellSubtitle,
  onCellClick,
  onPathChange,
  minCellArea = 16,
  mergeSmall,
  fullscreen = true,
  chrome = true,
  showLabels = true,
  className,
  mapStyle,
  depthFade = 0.82,
  rootFade = 0.92,
}: TreemapProps<T>) {
  const [path, setPath] = useState<T[]>([root])
  const [tip, setTip] = useState<TipState<T> | null>(null)
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 0, h: 0 })
  const mapRef = useRef<HTMLDivElement>(null)
  const wrapRef = useRef<HTMLDivElement>(null)
  const tipRef = useRef<HTMLDivElement>(null)

  const pin = useHoverPin<string>({ excludeRefs: [tipRef] })
  const [pinnedTip, setPinnedTip] = useState<TipState<T> | null>(null)
  useEffect(() => {
    if (pin.pinned === null) setPinnedTip(null)
  }, [pin.pinned])

  const node = path[path.length - 1]

  // Reset drill path when root changes.
  useEffect(() => {
    setPath([root])
    setTip(null)
  }, [root])

  // Notify consumer on path change.
  useEffect(() => {
    onPathChange?.(path)
    // path is a fresh array on every drill — safe to depend on it
  }, [path, onPathChange])

  // Track container size: measure synchronously, then observe for changes.
  // The initial ResizeObserver delivery is not guaranteed to arrive — in a
  // hidden/background tab it never did, leaving a correctly-sized container
  // rendering zero cells until something resized it.
  useLayoutEffect(() => {
    const el = mapRef.current
    if (!el) return
    setSize({ w: el.clientWidth, h: el.clientHeight })
    const ro = new ResizeObserver(() => setSize({ w: el.clientWidth, h: el.clientHeight }))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Backspace/Escape pops the drill stack.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.key === 'Backspace' || e.key === 'Escape') && path.length > 1) {
        setPath(p => p.slice(0, -1))
        setTip(null)
      }
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [path.length])

  const idFor = useCallback(
    (n: T, p: T[]) => (getId ? getId(n, p) : defaultId(n, p, getLabel)),
    [getId, getLabel],
  )

  // Lazily-fetched children, keyed by node id. Dropped when `root` changes,
  // since a new root is a different tree (a different scan, in disk-tree's
  // case) and its ids mean nothing here.
  const [fetched, setFetched] = useState<Map<string, T[]>>(() => new Map())
  const [pending, setPending] = useState<string | null>(null)
  const [failed, setFailed] = useState<{ key: string; error: Error } | null>(null)
  const [retries, setRetries] = useState(0)
  useEffect(() => {
    setFetched(new Map())
    setPending(null)
    setFailed(null)
  }, [root])

  const childrenOf = useCallback(
    (n: T, p: T[]): T[] | undefined => getChildren(n) ?? fetched.get(idFor(n, p)),
    [getChildren, fetched, idFor],
  )
  /** Drillable: has children in hand, or says it has some we can fetch. */
  const expandable = useCallback(
    (n: T, p: T[]): boolean =>
      (childrenOf(n, p)?.length ?? 0) > 0 || (!!loadChildren && !!hasChildren?.(n)),
    [childrenOf, loadChildren, hasChildren],
  )

  // One fetch per drill: only the *viewed* node loads, never the cells it
  // renders. Nested tiles come from the depth the server already returned.
  const viewKey = idFor(node, path)
  const viewNeedsLoad =
    !!loadChildren && !getChildren(node)?.length && !fetched.has(viewKey) && !!hasChildren?.(node)
  useEffect(() => {
    if (!viewNeedsLoad || !loadChildren) return
    let live = true
    setPending(viewKey)
    setFailed(null)
    loadChildren(node, path).then(
      kids => {
        if (!live) return
        // Cache even a superseded load — the data is valid, and it spares a
        // refetch if the user drills back in.
        setFetched(m => (m.has(viewKey) ? m : new Map(m).set(viewKey, kids)))
        setPending(p => (p === viewKey ? null : p))
        onChildrenLoaded?.(node, path, kids)
      },
      (e: unknown) => {
        if (!live) return
        setPending(p => (p === viewKey ? null : p))
        setFailed({ key: viewKey, error: e instanceof Error ? e : new Error(String(e)) })
      },
    )
    return () => { live = false }
    // `node`/`path` are pinned by `viewKey`; `retries` re-runs a failed load.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewKey, viewNeedsLoad, retries])

  const retry = useCallback(() => {
    setFailed(null)
    setRetries(r => r + 1)
  }, [])

  // Categorical color slots by top-level index (used when no colorForCell is given).
  const topLevelSlot = useMemo(() => {
    const kids = childrenOf(root, [root]) ?? []
    return new Map(kids.map((k, i) => [getLabel(k), DEFAULT_SLOTS[i % DEFAULT_SLOTS.length]]))
  }, [root, childrenOf, getLabel])

  // Fold small items at any level: consumer `mergeSmall` builds a first-class
  // T stand-in; the default builds a synthetic FoldedNode.
  const fold = useCallback(
    (raw: (T | FoldedNode<T>)[], w: number, h: number): (T | FoldedNode<T>)[] => {
      if (minCellArea == null) return raw
      const sz = (it: T | FoldedNode<T>) => (isFolded(it) ? it.size : getSize(it))
      const merge = mergeSmall
        ? (small: (T | FoldedNode<T>)[]) => mergeSmall(small as T[])
        : (small: (T | FoldedNode<T>)[]): FoldedNode<T> => {
            // Flatten any nested folds so `.count` is accurate.
            const flat: T[] = []
            let sum = 0
            for (const s of small) {
              if (isFolded(s)) {
                flat.push(...s.children)
                sum += s.size
              } else {
                flat.push(s)
                sum += getSize(s)
              }
            }
            return { __folded: true, count: flat.length, size: sum, children: flat }
          }
      return foldSmall<T | FoldedNode<T>>(raw, w, h, sz, merge, minCellArea)
    },
    [getSize, minCellArea, mergeSmall],
  )

  // Foldable children of the currently-viewed node.
  const children = useMemo(
    () => fold((childrenOf(node, path) ?? []).slice(), size.w, size.h),
    [node, path, size, childrenOf, fold],
  )

  const rects = useMemo(
    () =>
      squarify<T | FoldedNode<T>>(
        children,
        0, 0, size.w, size.h,
        n => (isFolded(n) ? n.size : getSize(n)),
      ),
    [children, size, getSize],
  )

  const cell = (
    kid: T | FoldedNode<T>,
    kidPath: T[],
    r: { x: number; y: number; w: number; h: number },
    depth: number,
  ): ReactNode => {
    const folded = isFolded(kid)
    const kidSize = folded ? kid.size : getSize(kid)
    const kidLabel = folded ? `(+${kid.count})` : getLabel(kid)
    const kidChildren = folded ? undefined : childrenOf(kid, kidPath)
    // Drillable even with nothing in hand, when a loader can go get it.
    const kidDrillable = !folded && expandable(kid, kidPath)
    const showLbl = showLabels && r.w > 36 && r.h > 13
    const kw = r.w - 6
    const kh = r.h - (showLbl ? 23 : 6)
    const kids = kidChildren && kidChildren.length > 0 && r.w > 90 && r.h > 44
      ? squarify<T | FoldedNode<T>>(
          fold(kidChildren.slice(), kw, kh),
          0, 0, kw, kh,
          n => (isFolded(n) ? n.size : getSize(n)),
        )
      : []

    // Cell color falls through: consumer override → default categorical.
    // A consumer's `colorForCell` may return null/undefined to defer, so we
    // can't just conditionally *call* it; we call it and then fall through
    // if the result is nullish. This lets a caller style only the cells it
    // cares about (e.g. synthetic placeholders) and inherit defaults for
    // the rest.
    const explicit = folded
      ? null
      : colorForCell?.(kid, kidPath, depth, { w: r.w, h: r.h, hasKids: kids.length > 0 })
    let style: CellStyle
    if (explicit) {
      style = explicit
    } else if (folded) {
      style = { bg: 'var(--dt-treemap-folded, #4a4a52)', ink: 'var(--dt-treemap-folded-ink, #d0d0d8)' }
    } else {
      const top = kidPath[1] // path[0] = root; [1] is the top-level bucket-of-the-current-drill
      const slot = top ? topLevelSlot.get(getLabel(top)) : undefined
      style = kids.length > 0
        ? { bg: 'var(--dt-treemap-container-bg, #202024)', ink: 'var(--dt-treemap-ink, #d0d0d8)' }
        : { bg: slot ?? DEFAULT_SLOTS[0], ink: '#fff' }
    }
    if (lens && !folded) {
      style = lens(kid, kidPath, depth, { w: r.w, h: r.h, hasKids: kids.length > 0 }, style) ?? style
    }

    const cellKey = folded
      ? `__folded_${depth}_${r.x}_${r.y}`
      : idFor(kid, kidPath)

    const showTip = (e: React.MouseEvent) => {
      e.stopPropagation()
      if (folded) return
      pin.hover(cellKey)
      setTip({ x: e.clientX, y: e.clientY, key: cellKey, node: kid, path: kidPath })
    }
    const onClick = (e: React.MouseEvent) => {
      e.stopPropagation()
      if (folded) return
      if (onCellClick && onCellClick(kid, kidPath, e)) return
      if (kidDrillable) {
        setTip(null)
        pin.clearPin()
        setPath(kidPath)
      } else {
        pin.togglePin(cellKey)
        setPinnedTip(p =>
          p?.key === cellKey ? null : { x: e.clientX, y: e.clientY, key: cellKey, node: kid, path: kidPath },
        )
      }
    }
    const dust = Math.min(r.w, r.h) < 14

    return (
      <div
        key={cellKey}
        className={'dt-treemap-cell' + (kidDrillable ? ' branch' : '') + (dust ? ' dust' : '')}
        style={{
          position: 'absolute',
          left: r.x,
          top: r.y,
          width: Math.max(0, r.w - (dust ? 1 : 2)),
          height: Math.max(0, r.h - (dust ? 1 : 2)),
          background: style.bg,
          color: style.ink,
          opacity: (depth > 0 ? depthFade : rootFade) * (style.opacity ?? 1),
          ...(style.hatch && { backgroundImage: style.hatch }),
          borderRadius: dust ? 1.5 : 3,
          overflow: 'hidden',
          boxSizing: 'border-box',
          cursor: kidDrillable ? 'pointer' : 'default',
        }}
        tabIndex={folded ? -1 : 0}
        // Leaf cells hover their whole body; branch cells hover only their
        // title-bar label (below) — so sweeping across a branch's children
        // never dips into the parent's tooltip through the inter-child gaps.
        // Clearing lives on the `.dt-treemap-map` container, not per-cell, so
        // child→child is smooth (the tip persists across the gap instead of
        // blinking off).
        onMouseMove={kids.length > 0 ? undefined : showTip}
        onClick={onClick}
        onKeyDown={e => e.key === 'Enter' && onClick(e as unknown as React.MouseEvent)}
      >
        {showLbl && (
          <div
            className={'dt-treemap-lbl' + (r.w < 64 ? ' sm' : '')}
            style={{
              padding: 'var(--dt-treemap-lbl-pad, 2px 4px)',
              fontSize: r.w < 64
                ? 'var(--dt-treemap-lbl-fs-sm, 0.72rem)'
                : 'var(--dt-treemap-lbl-fs, 0.85rem)',
              lineHeight: 1.2,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              // branch title bar is the parent's own hover target (leaf labels
              // stay pointer-events:none so the body handles them)
              pointerEvents: kids.length > 0 ? 'auto' : 'none',
            }}
            onMouseMove={kids.length > 0 ? showTip : undefined}
          >
            {kidLabel}
            {r.w > 90 && (
              <span style={{ opacity: 0.75, marginLeft: 6 }}>
                {formatSize(kidSize)}
                {!folded && renderCellSubtitle && (
                  <span style={{ marginLeft: 4 }}>{renderCellSubtitle(kid, kidPath)}</span>
                )}
              </span>
            )}
          </div>
        )}
        {!folded && renderCellExtra && renderCellExtra(kid, kidPath, { w: r.w, h: r.h })}
        {kids.length > 0 && kidChildren && (
          <div
            className="dt-treemap-inner"
            style={{ position: 'absolute', inset: `${showLbl ? 20 : 3}px 3px 3px 3px` }}
          >
            {kids
              .filter(s => s.w >= 3 && s.h >= 3)
              .map(s => cell(s.it, isFolded(s.it) ? kidPath : [...kidPath, s.it as T], s, depth + 1))}
          </div>
        )}
      </div>
    )
  }

  const goFullscreen = () => {
    const el = wrapRef.current
    if (!el) return
    if (document.fullscreenElement) void document.exitFullscreen()
    else void el.requestFullscreen()
  }

  const tipToShow = pinnedTip ?? tip
  const tipContent = tipToShow && renderTooltip
    ? renderTooltip(tipToShow.node, tipToShow.path)
    : tipToShow
      ? (
        <>
          <div style={{ fontWeight: 500 }}>{getLabel(tipToShow.node)}</div>
          <div style={{ opacity: 0.75, fontSize: '0.85em' }}>{formatSize(getSize(tipToShow.node))}</div>
        </>
      )
      : null

  return (
    <div
      className={'dt-treemap' + (className ? ` ${className}` : '')}
      ref={wrapRef}
      style={{ display: 'flex', flexDirection: 'column', width: '100%', height: '100%' }}
    >
      {chrome && <div
        className="dt-treemap-bar"
        style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '2px 6px', minHeight: 22 }}
      >
        <nav
          className="dt-treemap-crumbs"
          aria-label="Path"
          style={{ display: 'flex', gap: 3, alignItems: 'center', flexWrap: 'wrap', flex: 1, minWidth: 0 }}
        >
          {path.map((n, i) => (
            <span key={i} style={{ display: 'inline-flex', alignItems: 'center', gap: 3 }}>
              {i > 0 && <span style={{ opacity: 0.4 }}>/</span>}
              {i < path.length - 1 ? (
                <a
                  tabIndex={0}
                  role="link"
                  style={{ cursor: 'pointer', textDecoration: 'underline' }}
                  onClick={() => setPath(path.slice(0, i + 1))}
                >
                  {getLabel(n)}
                </a>
              ) : (
                <span style={{ fontWeight: 500 }}>{getLabel(n)}</span>
              )}
            </span>
          ))}
          <span style={{ opacity: 0.6, marginLeft: 6 }}>
            {renderCrumbSuffix ? renderCrumbSuffix(node, path) : <>— {formatSize(getSize(node))}</>}
          </span>
        </nav>
        {renderLegend && <div className="dt-treemap-legend">{renderLegend(node, path)}</div>}
        {fullscreen && (
          <button
            className="dt-treemap-fs"
            onClick={goFullscreen}
            title="Toggle fullscreen"
            style={{ background: 'transparent', border: 'none', color: 'inherit', cursor: 'pointer', fontSize: '1.1em' }}
          >
            ⛶
          </button>
        )}
      </div>}
      {(() => {
        const r = renderRollup?.(node, path)
        return r ? <div className="dt-treemap-rollup">{r}</div> : null
      })()}
      <div
        className="dt-treemap-map"
        ref={mapRef}
        role="application"
        aria-label="Treemap"
        style={{ position: 'relative', flex: 1, minHeight: 0, ...mapStyle }}
        onMouseLeave={() => {
          pin.hover(null)
          setTip(null)
        }}
      >
        {rects.filter(r => r.w >= 3 && r.h >= 3).map(r => cell(r.it, isFolded(r.it) ? path : [...path, r.it as T], r, 0))}
        {failed?.key === viewKey ? (
          <div className="dt-treemap-status error" style={STATUS_STYLE}>
            {renderLoadError
              ? renderLoadError(node, path, failed.error, retry)
              : (
                <>
                  <div>Couldn’t load {getLabel(node)}: {failed.error.message}</div>
                  <button onClick={retry} style={{ marginTop: 6, cursor: 'pointer' }}>Retry</button>
                </>
              )}
          </div>
        ) : pending === viewKey ? (
          <div className="dt-treemap-status loading" style={STATUS_STYLE}>
            {renderLoading ? renderLoading(node, path) : 'Loading…'}
          </div>
        ) : null}
      </div>
      {(() => {
        const f = renderFooter?.(node, path)
        return f ? <div className="dt-treemap-footer">{f}</div> : null
      })()}
      {tipContent && tipToShow && (
        <div
          ref={tipRef}
          className={'dt-treemap-tip' + (pinnedTip ? ' pinned' : '')}
          style={{
            position: 'fixed',
            left: Math.min(tipToShow.x + 14, (typeof window !== 'undefined' ? window.innerWidth : 1600) - 320),
            top: Math.min(tipToShow.y + 14, (typeof window !== 'undefined' ? window.innerHeight : 1200) - 80),
            background: 'var(--dt-treemap-tip-bg, #1a1a1e)',
            color: 'var(--dt-treemap-tip-ink, #e6e6ea)',
            border: '1px solid var(--dt-treemap-tip-border, #333)',
            borderRadius: 4,
            padding: '6px 10px',
            fontSize: '0.85rem',
            zIndex: 1000,
            maxWidth: 320,
            pointerEvents: pinnedTip ? 'auto' : 'none',
            boxShadow: '0 4px 10px rgba(0,0,0,0.35)',
          }}
        >
          {pinnedTip && (
            <button
              onClick={() => pin.clearPin()}
              title="Unpin (Esc)"
              style={{
                position: 'absolute', top: 2, right: 4,
                background: 'transparent', border: 'none', color: 'inherit', cursor: 'pointer',
                fontSize: '1em', opacity: 0.6,
              }}
            >
              ×
            </button>
          )}
          {tipContent}
        </div>
      )}
    </div>
  )
}
