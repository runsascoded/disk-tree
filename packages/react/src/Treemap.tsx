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
  /**
   * Start drilled to this path (must begin with `root`). Applied on mount and
   * whenever `root`'s identity changes — crumbs still show the full ancestry,
   * so a store with one meaningful top-level container can open inside it.
   */
  initialPath?: T[]
  /**
   * Controlled drill path (must begin with `root`). When given, the component
   * renders this path and reports every drill/crumb/Backspace gesture through
   * `onPathChange` instead of keeping its own state — so a consumer can put
   * the path in the URL, or command a drill from outside (a table row, a
   * search hit). Mutually exclusive with `initialPath`.
   */
  path?: T[]
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
  /**
   * Collapse single-child wrapper chains into one cell labeled `a/…/z` (the
   * cell is the deepest node; the chain is recorded in the drill path).
   * Off by default — chains carry real information in some trees.
   */
  collapseChains?: boolean
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
   * the size when the cell is big enough to fit a subtitle; `dims` is the
   * cell's box so the consumer can size its content to the room left after
   * the label (the size span never shrinks, so an over-long subtitle would
   * crush the name).
   */
  renderCellSubtitle?: (n: T, path: T[], dims: CellDims) => ReactNode

  /**
   * Override the click handler. Return `true` to skip the built-in
   * drill/pin behavior (i.e. the consumer handled it).
   */
  onCellClick?: (n: T, path: T[], event: React.MouseEvent) => boolean | void
  /**
   * Make leaf-rendered cells real anchors (`<a href>`): native cursor,
   * middle/cmd-click, link hints (Vimium), crawlability. Return `undefined`
   * for cells that shouldn't be links. Cells that render nested tiles stay
   * `<div>`s (anchors can't nest), so this suits shallow maps best. Plain
   * clicks are `preventDefault`ed and flow through `onCellClick`/drill/pin
   * as usual — an SPA router intercepts while the href keeps its native
   * affordances.
   */
  cellHref?: (n: T, path: T[]) => string | undefined
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
   * Per-level fade applied to nested (depth > 0) cell *backgrounds*.
   * Default: 0.82.
   *
   * Each cell paints its background on a dedicated layer at opacity
   * `max(rootFade × depthFade^depth, fadeFloor)` — deeper paint recedes, but
   * label ink never fades: text renders outside the faded layer at full
   * strength at every depth. Pass `1` to keep saturation constant and lean on
   * borders for structure.
   */
  depthFade?: number
  /** Background opacity of the outermost (depth 0) cells. Default: 0.92. */
  rootFade?: number
  /**
   * Floor on the depth fade, so deep backgrounds don't wash out entirely
   * (depth 4 at the defaults would otherwise paint at ~0.5). Default: 0.75.
   */
  fadeFloor?: number
  /**
   * How sibling cells tile a parent's canvas.
   *
   * - `'gaps'` (default): every cell insets 2px per side, leaving gutters
   *   in the container color and rounded corners. Reads comfortably, but
   *   painted area under-represents by ~perimeter/area — a 6×6px cell
   *   paints 4×4 (44% loss), so area-proportionality can't hold at the
   *   coarsest and finest displayed levels at once.
   * - `'shared'`: cells occupy their exact squarify rects and share edges;
   *   each boundary is one stroke (`borderWidth`), half drawn by each
   *   neighbor as an inset ring in `--dt-treemap-edge` (defaults to the
   *   container color, so it reads like a thin gutter) — the paint layer
   *   insets by that much to expose it. Near-exact areas; the stroke is the
   *   only residual.
   * - A callback decides per subtree — called for the node whose children
   *   are being tiled, with their laid-out density — e.g. `'shared'` for
   *   dense leaf fields (`medianChildArea < 100`), `'gaps'` elsewhere.
   */
  tiling?: Tiling | ((n: T, path: T[], depth: number, ctx: TilingCtx) => Tiling)
  /**
   * Shared-mode stroke width (CSS px) for cells at `depth` (0 = the viewed
   * node's children). Default: `max(1, 3 − depth)` — thicker at shallow
   * levels for hierarchy legibility, a hairline at the leaves. Dust cells
   * (<14px) are capped at 1px.
   */
  borderWidth?: (depth: number, ctx: CellDims) => number
}

export type Tiling = 'gaps' | 'shared'

/** Context for the `tiling` callback: the parent's canvas and its children's layout. */
export interface TilingCtx extends CellDims {
  nChildren: number
  /** median laid-out child area in px² — the density cue */
  medianChildArea: number
}

export interface CellStyle {
  /** background CSS color/gradient */
  bg?: string
  /** ink color for the label */
  ink?: string
  /** repeating-gradient overlay (e.g. the class-lens hatch marin uses) */
  hatch?: string
  /**
   * Shared-tiling stroke for THIS cell, overriding `--dt-treemap-edge`. Each
   * cell paints its own half of every boundary it shares, so neighbours may
   * choose different colors — each half then contrasts with the face it
   * borders (one stroke color can't serve a bright cell and a dark one).
   * `CellCtx.fade` gives the background opacity applied at this depth, so a
   * consumer can compute the cell's true composited luminance.
   */
  edge?: string
  /** opacity multiplier (0–1). Combined with the built-in depth fade; applies
   * to the cell's background layer, never its label ink. */
  opacity?: number
  /**
   * Proportional makeup stripes for a leaf-rendered cell (no nested tiles at
   * the current depth — folds and max-depth leaves): instead of one dominant
   * color, the cell renders these as inset slices along its longer axis,
   * proportional to `frac`. The inset frame (painted `bg`) plus the shared
   * outer border distinguish a composition breakdown from real child tiles.
   * Ignored when fewer than 2 segments, when the cell renders children, or
   * when the cell is too small to read.
   */
  segments?: { color: string; frac: number }[]
}

export interface CellDims {
  w: number
  h: number
}

/** Render-time context passed to `colorForCell`. */
export interface CellCtx extends CellDims {
  /** Whether this cell renders nested child tiles at the current size. */
  hasKids: boolean
  /** Background opacity applied at this depth (the depth fade), so consumers
   * can compute what their color actually composites to on screen. */
  fade: number
}

const DEFAULT_SLOTS = DEFAULT_PALETTE

const defaultFormat = (n: number) => n.toLocaleString('en-US')

const defaultBorderWidth = (depth: number) => Math.max(1, 3 - depth)

const medianArea = (rs: { w: number; h: number }[]): number => {
  if (!rs.length) return 0
  const a = rs.map(r => r.w * r.h).sort((x, y) => x - y)
  return a[Math.floor(a.length / 2)]
}

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
  initialPath,
  path: pathProp,
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
  collapseChains = false,
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
  cellHref,
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
  fadeFloor = 0.75,
  tiling = 'gaps',
  borderWidth = defaultBorderWidth,
}: TreemapProps<T>) {
  const [pathState, setPathState] = useState<T[]>(initialPath?.[0] === root ? initialPath : [root])
  const controlled = pathProp !== undefined
  const path = controlled && pathProp[0] === root ? pathProp : pathState
  const [tip, setTip] = useState<TipState<T> | null>(null)
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 0, h: 0 })
  const mapRef = useRef<HTMLDivElement>(null)
  const wrapRef = useRef<HTMLDivElement>(null)
  const tipRef = useRef<HTMLDivElement>(null)
  // Grace timer so the hover tip survives the cell→tip gap: leaving the map
  // schedules a clear, entering the tip cancels it. Lets you move into the tip
  // and use its controls/links without pinning (the tip is anchored, not
  // mouse-following, so it stays put while you reach for it).
  const tipClear = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const cancelTipClear = () => {
    if (tipClear.current) { clearTimeout(tipClear.current); tipClear.current = undefined }
  }

  const pin = useHoverPin<string>({ excludeRefs: [tipRef] })
  const [pinnedTip, setPinnedTip] = useState<TipState<T> | null>(null)
  useEffect(() => {
    if (pin.pinned === null) setPinnedTip(null)
  }, [pin.pinned])

  const node = path[path.length - 1]

  // Every drill/crumb/Backspace gesture routes through here: controlled mode
  // only reports (the consumer renders the new `path` prop back); uncontrolled
  // keeps its own state and reports from the effect below, so the mount and
  // root-reset paths — which no gesture produces — report too.
  const go = useCallback(
    (p: T[]) => {
      if (!controlled) setPathState(p)
      setTip(null)
      if (controlled) onPathChange?.(p)
    },
    [controlled, onPathChange],
  )

  // Reset drill path when root changes (respecting `initialPath` when it
  // belongs to the new root). Controlled mode skips this — the consumer's
  // `path` prop is recomputed against the new root by the consumer. Skipped on
  // mount too: `useState` already seeded `path`, and re-seeding it here would
  // report a second, identical `[root]` through the effect below.
  const mountedRoot = useRef(root)
  useEffect(() => {
    if (controlled || mountedRoot.current === root) return
    mountedRoot.current = root
    setPathState(initialPath?.[0] === root ? initialPath : [root])
    setTip(null)
    // eslint-disable-next-line react-hooks/exhaustive-deps -- initialPath applies per-root, not on its own changes
  }, [root])

  // Report the path this component owns: mount (incl. `initialPath`), drills,
  // and the root-change reset. Controlled mode reports from `go` instead, where
  // no local state changes and this effect would never fire.
  useEffect(() => {
    if (!controlled) onPathChange?.(path)
  }, [controlled, path, onPathChange])

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
      const t = e.target as HTMLElement | null
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)) return
      if ((e.key === 'Backspace' || e.key === 'Escape') && path.length > 1) {
        go(path.slice(0, -1))
      }
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [path, go])

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

  // Background opacity at a given nesting depth. Applied per-cell to the
  // `.dt-treemap-bg` layer (not the cell div), so ancestors' fades never
  // compound and label ink stays full-strength at every depth.
  const fadeAt = (d: number) => Math.max(rootFade * depthFade ** d, fadeFloor)

  // Tiling mode for a node's children (`depth` = the children's depth).
  const tilingFor = (
    n: T, p: T[], depth: number, w: number, h: number, rs: { w: number; h: number }[],
  ): Tiling =>
    typeof tiling === 'function'
      ? tiling(n, p, depth, { w, h, nChildren: rs.length, medianChildArea: medianArea(rs) })
      : tiling
  const rootMode = tilingFor(node, path, 0, size.w, size.h, rects)

  const cell = (
    kid0: T | FoldedNode<T>,
    kidPath0: T[],
    r: { x: number; y: number; w: number; h: number },
    depth: number,
    mode: Tiling,
  ): ReactNode => {
    const folded = isFolded(kid0)
    // Single-child wrapper chains render as ONE cell labeled `a/…/z`: each
    // wrapper level otherwise costs a title strip + gutter, so a deep
    // single-child spine eats area and reads as noise (five nested boxes
    // all holding the same bytes). The cell *is* the deepest node — its
    // children, drill target, and id — with the chain recorded in the path,
    // so crumbs and tooltips still show every level.
    const chained = collapseChains && !folded
      ? (() => {
          let cur = kid0 as T
          let p = kidPath0
          const labels = [getLabel(cur)]
          for (;;) {
            const only = childrenOf(cur, p)
            if (!only || only.length !== 1) break
            cur = only[0]
            p = [...p, cur]
            labels.push(getLabel(cur))
          }
          return labels.length > 1 ? { node: cur, path: p, labels } : null
        })()
      : null
    const kid: T | FoldedNode<T> = chained ? chained.node : kid0
    const kidPath = chained ? chained.path : kidPath0
    const chainLabels = chained?.labels ?? null
    const kidSize = isFolded(kid) ? kid.size : getSize(kid)
    const kidLabel = folded
      ? `(+${(kid as FoldedNode<T>).count})`
      : chainLabels
        ? (chainLabels.length > 3 ? `${chainLabels[0]}/…/${chainLabels[chainLabels.length - 1]}` : chainLabels.join('/'))
        : getLabel(kid as T)
    const kidChildren = folded ? undefined : childrenOf(kid as T, kidPath)
    // Drillable even with nothing in hand, when a loader can go get it.
    const kidDrillable = !folded && expandable(kid as T, kidPath)
    const showLbl = showLabels && r.w > 36 && r.h > 13
    const dust = Math.min(r.w, r.h) < 14
    const shared = mode === 'shared'
    // This cell's own stroke (shared mode): half of it is drawn inside this
    // cell as an inset ring, the neighbor draws the other half.
    const bw = shared ? Math.min(borderWidth(depth, { w: r.w, h: r.h }), dust ? 1 : Infinity) : 0
    const edge = bw / 2
    // Children canvas. Gaps mode pads 3px inside the cell; shared mode fills
    // to the cell's own half-stroke so the children's outer strokes meet it.
    // Their tiling mode is decided from a first layout's density, and the
    // layout redone at the mode's dims when they differ.
    // (Sized from the cell's *box* — in gaps mode that's the rect minus the
    // 2px gutter — so children never overrun the clipped cell.)
    const boxW = shared ? r.w : r.w - (dust ? 1 : 2)
    const boxH = shared ? r.h : r.h - (dust ? 1 : 2)
    let kw = boxW - 4
    let kh = boxH - (showLbl ? 21 : 4)
    let kidsMode: Tiling = 'gaps'
    let kids: ReturnType<typeof squarify<T | FoldedNode<T>>> = []
    if (kidChildren && kidChildren.length > 0 && r.w > 90 && r.h > 44) {
      const lay = (w: number, h: number) =>
        squarify<T | FoldedNode<T>>(
          fold(kidChildren.slice(), w, h),
          0, 0, w, h,
          n => (isFolded(n) ? n.size : getSize(n)),
        )
      kids = lay(kw, kh)
      kidsMode = tilingFor(kid as T, kidPath, depth + 1, r.w, r.h, kids)
      if (kidsMode === 'shared') {
        kw = boxW - 2 * edge
        kh = boxH - (showLbl ? 20 + edge : 2 * edge)
        kids = lay(kw, kh)
      }
    }

    // Cell color falls through: consumer override → default categorical.
    // A consumer's `colorForCell` may return null/undefined to defer, so we
    // can't just conditionally *call* it; we call it and then fall through
    // if the result is nullish. This lets a caller style only the cells it
    // cares about (e.g. synthetic placeholders) and inherit defaults for
    // the rest.
    const explicit = folded
      ? null
      : colorForCell?.(kid as T, kidPath, depth, { w: r.w, h: r.h, fade: fadeAt(depth), hasKids: kids.length > 0 })
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
      style = lens(kid as T, kidPath, depth, { w: r.w, h: r.h, fade: fadeAt(depth), hasKids: kids.length > 0 }, style) ?? style
    }

    const cellKey = folded
      ? `__folded_${depth}_${r.x}_${r.y}`
      : idFor(kid as T, kidPath)

    const showTip = (e: React.MouseEvent) => {
      e.stopPropagation()
      if (folded) return
      cancelTipClear()
      pin.hover(cellKey)
      // Anchor to the cell (frozen once per cell) instead of chasing the cursor:
      // a mouse-following tip can't be hovered into to click its contents.
      setTip(prev => (prev?.key === cellKey ? prev : { x: e.clientX, y: e.clientY, key: cellKey, node: kid as T, path: kidPath }))
    }
    // Real-anchor cells (`cellHref`): only leaf-rendered ones — a cell with
    // nested tiles would nest <a>s, which HTML forbids. Modified/middle
    // clicks keep native behavior (new tab); plain clicks preventDefault and
    // flow through the normal handler so SPA routers can intercept.
    const href = cellHref && !folded && kids.length === 0 ? cellHref(kid as T, kidPath) : undefined
    const onClick = (e: React.MouseEvent) => {
      e.stopPropagation()
      if (folded) return
      if (href) {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button === 1) return
        e.preventDefault()
      }
      if (onCellClick && onCellClick(kid as T, kidPath, e)) return
      if (kidDrillable) {
        pin.clearPin()
        go(kidPath)
      } else {
        pin.togglePin(cellKey)
        setPinnedTip(p =>
          p?.key === cellKey ? null : { x: e.clientX, y: e.clientY, key: cellKey, node: kid as T, path: kidPath },
        )
      }
    }
    // `branch` / `chain` chrome (consumers hang inset rings / doubled edges
    // off these) only when the cell is big enough for that treatment to read
    // — a dense field of small drillable or chain-collapsed tiles must not
    // all grow dark inner rings. `branch` also applies whenever children
    // actually render (a container needs its edge at any size).
    const chromeOk = Math.min(r.w, r.h) >= 28

    const CellTag: 'a' | 'div' = href ? 'a' : 'div'
    return (
      <CellTag
        key={cellKey}
        {...(href && { href })}
        className={'dt-treemap-cell' + (kidDrillable && (kids.length > 0 || chromeOk) ? ' branch' : '') + (dust ? ' dust' : '') + (chainLabels && chromeOk ? ' chain' : '') + (shared ? ' shared' : '')}
        style={{
          position: 'absolute',
          left: r.x,
          top: r.y,
          // Gaps: 2px gutter (1px for dust) around the squarify rect.
          // Shared: the exact rect — neighbors abut, the stroke is the gutter.
          width: Math.max(0, shared ? r.w : r.w - (dust ? 1 : 2)),
          height: Math.max(0, shared ? r.h : r.h - (dust ? 1 : 2)),
          // Opaque base under the faded paint layer: a cell's fade recedes
          // toward the container color, and ancestor bg never shows through
          // descendants — it surfaces only in title bars and gutters. Stays
          // the container color in both modes: the paint layer is translucent
          // (depth fade), so a tinted base would wash every cell toward it.
          background: 'var(--dt-treemap-container-bg, #202024)',
          // Shared: this cell's half of the shared stroke, painted in the ring
          // the inset paint layer leaves bare (an inset shadow under a
          // full-bleed paint layer would never show). Gaps: an outer ring in
          // the gutter, transparent by default so dark-palette consumers can
          // opt into brighter sibling separation via the var.
          // (boxShadow, not outline — :focus owns the outline.)
          boxShadow: shared
            ? `inset 0 0 0 ${edge}px ${style.edge ?? 'var(--dt-treemap-edge, var(--dt-treemap-container-bg, #202024))'}`
            : '0 0 0 1px var(--dt-treemap-cell-border, transparent)',
          // Anchors must not fall through to the page's link color when the
          // consumer sets no ink.
          color: style.ink ?? (href ? 'inherit' : undefined),
          borderRadius: shared ? 0 : dust ? 1.5 : 3,
          overflow: 'hidden',
          boxSizing: 'border-box',
          cursor: href || kidDrillable ? 'pointer' : 'default',
          ...(href && { textDecoration: 'none' }),
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
        {/* All paint (bg, hatch, makeup stripes) lives on this layer so the
            depth fade (and any per-cell `style.opacity`) dims backgrounds
            only — label ink renders outside it, full-strength at every
            depth. */}
        <div
          className="dt-treemap-bg"
          style={{
            position: 'absolute',
            inset: shared ? edge : 0,
            background: style.bg,
            ...(style.hatch && { backgroundImage: style.hatch }),
            opacity: fadeAt(depth) * (style.opacity ?? 1),
            pointerEvents: 'none',
          }}
        >
        {/* Makeup stripes: a leaf/fold cell with a mixed composition renders
            proportional inset slices (longer axis) instead of one dominant
            blob. The `bg` frame showing through the inset + the single outer
            border reads as "one blob, split by share" — visibly distinct from
            real child tiles (separate bordered cells with their own labels). */}
        {style.segments && style.segments.length > 1 && kids.length === 0 && !dust
          && Math.min(r.w, r.h) >= 18 && (() => {
            const inset = shared ? Math.max(1, bw) : 3
            const gap = 1
            const horiz = r.w >= r.h // slice along the longer axis
            const span = (horiz ? r.w : r.h) - 2 * inset - gap * (style.segments!.length - 1)
            if (span < style.segments!.length * 2) return null
            const total = style.segments!.reduce((s, x) => s + x.frac, 0) || 1
            let at = inset
            return style.segments!.map((s, i) => {
              const len = (s.frac / total) * span
              const rect = horiz
                ? { left: at, top: inset, width: len, height: Math.max(0, r.h - 2 * inset - 2) }
                : { left: inset, top: at, width: Math.max(0, r.w - 2 * inset - 2), height: len }
              at += len + gap
              return (
                <div key={i} style={{
                  position: 'absolute', ...rect, background: s.color,
                  borderRadius: 2, pointerEvents: 'none',
                }} />
              )
            })
          })()}
        </div>
        {showLbl && (
          <div
            className={'dt-treemap-lbl' + (r.w < 64 ? ' sm' : '')}
            style={{
              position: 'relative',
              zIndex: 1, // above makeup stripes (positioned siblings)
              padding: 'var(--dt-treemap-lbl-pad, 2px 4px)',
              fontSize: r.w < 64
                ? 'var(--dt-treemap-lbl-fs-sm, 0.72rem)'
                : 'var(--dt-treemap-lbl-fs, 0.85rem)',
              lineHeight: 1.2,
              // Flex, not one nowrap run: a long name ellipsizes while the
              // size span keeps its width, instead of pushing it out of view.
              display: 'flex',
              alignItems: 'baseline',
              gap: 6,
              overflow: 'hidden',
              // branch title bar is the parent's own hover target (leaf labels
              // stay pointer-events:none so the body handles them)
              pointerEvents: kids.length > 0 ? 'auto' : 'none',
            }}
            onMouseMove={kids.length > 0 ? showTip : undefined}
          >
            <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{kidLabel}</span>
            {/* Inline size only for branch title-bars and short leaves; a tall
                leaf drops it to a 2nd line (below) so the name gets the full
                first line and isn't crowded by the size. */}
            {(kids.length > 0 || r.h <= 34) && r.w > 90 && (
              <span className="sz" style={{ opacity: 0.75, whiteSpace: 'nowrap', flex: 'none' }}>
                {formatSize(kidSize)}
                {!folded && renderCellSubtitle && (
                  <span style={{ marginLeft: 4 }}>{renderCellSubtitle(kid as T, kidPath, { w: r.w, h: r.h })}</span>
                )}
              </span>
            )}
          </div>
        )}
        {/* Size on a second line for any leaf tall enough to hold one — the
            first line then belongs entirely to the (possibly long) name. */}
        {showLbl && kids.length === 0 && r.h > 34 && (
          <div
            className="dt-treemap-lbl2"
            style={{
              position: 'relative',
              zIndex: 1,
              padding: '0 4px',
              fontSize: 'var(--dt-treemap-lbl-fs-sm, 0.72rem)',
              lineHeight: 1.2,
              opacity: 0.75,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              pointerEvents: 'none',
            }}
          >
            {formatSize(kidSize)}
            {!folded && renderCellSubtitle && (
              <span style={{ marginLeft: 4 }}>{renderCellSubtitle(kid as T, kidPath, { w: r.w, h: r.h })}</span>
            )}
          </div>
        )}
        {!folded && renderCellExtra && renderCellExtra(kid as T, kidPath, { w: r.w, h: r.h })}
        {kids.length > 0 && kidChildren && (
          <div
            className="dt-treemap-inner"
            style={{
              position: 'absolute',
              inset: kidsMode === 'shared'
                ? `${showLbl ? 20 : edge}px ${edge}px ${edge}px ${edge}px`
                : `${showLbl ? 20 : 3}px 3px 3px 3px`,
            }}
          >
            {kids
              .filter(s => s.w >= 3 && s.h >= 3)
              .map(s => cell(s.it, isFolded(s.it) ? kidPath : [...kidPath, s.it as T], s, depth + 1, kidsMode))}
          </div>
        )}
      </CellTag>
    )
  }

  const goFullscreen = () => {
    const el = wrapRef.current
    if (!el) return
    if (document.fullscreenElement) void document.exitFullscreen()
    else void el.requestFullscreen()
  }

  const tipToShow = pinnedTip ?? tip
  // Clamp the tip to the viewport using its MEASURED size — consumers widen
  // it with CSS (max-width overrides), so a fixed guess overflows the edge.
  const [tipDims, setTipDims] = useState<{ w: number; h: number } | null>(null)
  useLayoutEffect(() => {
    const el = tipRef.current
    if (!el) return
    const r = el.getBoundingClientRect()
    setTipDims(d => (d && Math.abs(d.w - r.width) < 1 && Math.abs(d.h - r.height) < 1 ? d : { w: r.width, h: r.height }))
  })
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
                  onClick={() => go(path.slice(0, i + 1))}
                >
                  {getLabel(n)}
                </a>
              ) : (
                <span style={{ fontWeight: 500 }}>{getLabel(n)}</span>
              )}
            </span>
          ))}
          <span style={{ opacity: 0.6, marginLeft: 6, whiteSpace: 'nowrap' }}>
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
          // Don't clear immediately — give the pointer time to reach the tip
          // (cancelled by the tip's onMouseEnter).
          cancelTipClear()
          tipClear.current = setTimeout(() => { pin.hover(null); setTip(null) }, 180)
        }}
      >
        {rects.filter(r => r.w >= 3 && r.h >= 3).map(r => cell(r.it, isFolded(r.it) ? path : [...path, r.it as T], r, 0, rootMode))}
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
          onMouseEnter={cancelTipClear}
          onMouseLeave={() => { if (!pinnedTip) { pin.hover(null); setTip(null) } }}
          style={{
            position: 'fixed',
            left: Math.max(4, Math.min(tipToShow.x + 14, (typeof window !== 'undefined' ? window.innerWidth : 1600) - (tipDims?.w ?? 320) - 8)),
            top: Math.max(4, Math.min(tipToShow.y + 14, (typeof window !== 'undefined' ? window.innerHeight : 1200) - (tipDims?.h ?? 80) - 8)),
            background: 'var(--dt-treemap-tip-bg, #1a1a1e)',
            color: 'var(--dt-treemap-tip-ink, #e6e6ea)',
            border: '1px solid var(--dt-treemap-tip-border, #333)',
            borderRadius: 4,
            padding: '6px 10px',
            fontSize: '0.85rem',
            zIndex: 1000,
            maxWidth: 320,
            // Interactive whether hovered or pinned — you can move into it and
            // click its links/controls (the grace timer keeps it alive en route).
            pointerEvents: 'auto',
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
