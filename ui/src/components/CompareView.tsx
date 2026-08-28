import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react'
import { Link, useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
  Box,
  Button,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Tooltip,
  Typography,
} from '@mui/material'
import { FaArrowRight, FaFolder, FaFile, FaSortUp, FaSortDown, FaSync, FaList } from 'react-icons/fa'
import { Treemap as DTTreemap, divergingColor } from '@disk-tree/react'
import '@disk-tree/react/styles.css'
import { compareScans, compareScansRecursive, fetchDiffIndexStatus, fetchScanHistory, startScan } from '../api'
import type { CompareRecResult, CompareResult, CompareRow, ScanHistoryItem } from '../api'
import { useScanProgress } from '../hooks/useScanProgress'
import { useRecentPaths } from '../hooks/useRecentPaths'
import { formatSize, formatCount, timeAgo } from '../utils/format'
import { comparePathToUri, isSchemeRoot, uriToPath, type RouteType } from '../schemes'

type SortColumn = 'size_old' | 'size_new' | 'size_delta' | 'desc_old' | 'desc_new' | 'desc_delta'
type SortDirection = 'asc' | 'desc'

function formatDelta(bytes: number): string {
  const sign = bytes < 0 ? '−' : '+'  // U+2212: a hyphen reads as a dash at small sizes
  return sign + formatSize(Math.abs(bytes)).replace(' ', '')
}

/**
 * Diff polarity (git convention): green = added/grew, red = removed/shrank —
 * matching the added/removed row tints and the summary chips. This is a diff
 * view, not a cost alarm; a "growth = red" cost lens can be a toggle later.
 * `divergingColor` is red-positive, so negate on the way in.
 */
const GREW_GREEN = '#3fb950'
const SHRANK_RED = '#f85149'
const NEUTRAL = '#8b949e'
/** Unchanged-bytes fill: translucent dark grey (not `divergingColor(0)`'s mid
 * grey, which left the light ink low-contrast) — the colored bands pop and
 * labels read. */
const UNCHANGED_GREY = 'rgba(110, 118, 129, 0.28)'
/** touched (same bytes, mtime moved): the unchanged grey, hatched */
const TOUCHED_HATCH = 'repeating-linear-gradient(45deg, rgba(255, 255, 255, 0.11) 0 3px, transparent 3px 9px)'
const deltaColor = (t: number) => divergingColor(-t)
const deltaTextColor = (d: number) => (d > 0 ? GREW_GREEN : d < 0 ? SHRANK_RED : NEUTRAL)

/**
 * Δ-recolor treemap with two area modes:
 *
 * - `max` (default): one cell per row, sized by `max(old, new)` — deleted
 *   subtrees keep their old area, added ones their new area, and unchanged
 *   structure stays visible as neutral context. Color encodes Δ/max per cell:
 *   pure-add is fully red, pure-delete fully green, unchanged neutral.
 *   Caveat (labeled): cell areas sum to more than either side's true total.
 * - `Δ`: the churn view — only changed rows, sized by `|Δbytes|`, colored by
 *   Δ relative to the largest |Δ|.
 *
 * Clicking a directory drills into `/compare/<scheme>/<subpath>` so the
 * exploration matches the deep-link scheme.
 */
type AreaMode = 'max' | 'delta'

interface CompareTMNode {
  key: string
  label: string
  /** what the widget sizes by — `max(old, new)` or `|size_delta|` per mode;
   * parents take `max(own, Σ children)` so children can never overflow
   * (delete-X-add-Y churn makes Σ children max exceed the parent's max). */
  weight: number
  /** signed delta for coloring */
  delta: number
  /** Σ of positive / negative deltas across descendants (frontier-leaf
   * granularity): `grew ≥ 0`, `shrank ≤ 0`, `grew + shrank ≈ delta`. */
  grew: number
  shrank: number
  /** same, for entry counts (`n_desc_delta`): `nGrew ≥ 0`, `nShrank ≤ 0` */
  nGrew: number
  nShrank: number
  status: CompareRow['status'] | 'filler' | 'fold'
  size_old: number
  size_new: number
  n_desc_delta: number
  kind: CompareRow['kind'] | 'filler' | 'fold'
  uri: string
  /** fold cells: how many too-small-to-draw children were merged */
  nFolded?: number
  /** filler cells: the unenumerated unchanged children it stands for
   * (direct-child count and their descendants), when the server told us */
  nRest?: number
  nDescRest?: number
  /** Frontier dir with unexplored change below (budget/depth cut the walk). */
  pruned?: boolean
  children?: CompareTMNode[]
}

/**
 * Recursive-diff frontier rows + the depth-1 unchanged rows (from the plain
 * compare, for labeled grey context at the top level) → a nested tree.
 *
 * Weights are bottom-up: a leaf is `max(old, new)` (or `|Δ|` in Δ mode); a
 * parent is `max(its own max, Σ children)` — churn (delete X + add Y) makes
 * children sum past either side's bytes, and the parent honestly grows to
 * hold them. Where children under-fill a parent (unchanged bytes the walk
 * never enumerated), a grey filler cell absorbs the gap, so areas stay
 * truthful without shipping every unchanged row. The server ships each
 * expanded dir's biggest unchanged children (`rec.unchanged.top`, named grey
 * cells) and an aggregate of the rest (`rec.unchanged.rest`) — the filler's
 * tooltip counts what it stands for.
 *
 * Children order is signed: biggest adds first, unchanged middle, biggest
 * shrinks last (sort by -Δ).
 */
function buildCompareTree(
  flat: CompareResult,
  rec: CompareRecResult | undefined,
  areaMode: AreaMode,
  showUnchanged: boolean,
): { cells: CompareTMNode[]; maxAbsDelta: number } {
  const uriPrefix = flat.uri.replace(/\/$/, '') + '/'
  const byPath = new Map<string, CompareTMNode>()
  const pathOf = new Map<CompareTMNode, string>()
  const roots: CompareTMNode[] = []
  const attach = (node: CompareTMNode, path: string) => {
    byPath.set(path, node)
    pathOf.set(node, path)
    const i = path.lastIndexOf('/')
    if (i < 0) {
      roots.push(node)
      return
    }
    const parentPath = path.slice(0, i)
    let parent = byPath.get(parentPath)
    if (!parent) {
      // Expanded-but-unchanged dir (e.g. a net-zero rename inside it): its
      // children were emitted without it. Synthesize the intermediate.
      parent = {
        key: uriPrefix + parentPath,
        label: parentPath.split('/').pop()!,
        weight: 0,
        delta: 0,
        grew: 0,
        shrank: 0,
        nGrew: 0,
        nShrank: 0,
        status: 'unchanged',
        size_old: 0,
        size_new: 0,
        n_desc_delta: 0,
        kind: 'dir',
        uri: uriPrefix + parentPath,
        children: [],
      }
      attach(parent, parentPath)
    }
    ;(parent.children ??= []).push(node)
  }

  const recRows = [
    ...(rec?.rows ?? []),
    ...(showUnchanged ? rec?.unchanged?.top ?? [] : []),
  ].sort((a, b) => a.depth - b.depth || a.path.localeCompare(b.path))
  const rest = rec?.unchanged?.rest ?? {}
  for (const r of recRows) {
    attach({
      key: r.uri,
      label: r.path.split('/').pop() || r.path,
      weight: 0,
      delta: r.size_delta,
      grew: 0,
      shrank: 0,
      nGrew: 0,
      nShrank: 0,
      status: r.status,
      size_old: r.size_a,
      size_new: r.size_b,
      n_desc_delta: r.n_desc_delta,
      kind: r.kind,
      uri: r.uri,
      pruned: r.pruned,
      children: undefined,
    }, r.path)
  }
  // Labeled grey context at the top level (the recursive walk doesn't emit
  // unchanged rows; the plain depth-1 compare does). Hidden entirely in
  // hide-unchanged mode — only the changed frontier plots.
  for (const r of showUnchanged ? flat.rows : []) {
    if (r.status === 'unchanged' && !byPath.has(r.path)) {
      attach({
        key: r.uri,
        label: r.path,
        weight: 0,
        delta: 0,
        grew: 0,
        shrank: 0,
        nGrew: 0,
        nShrank: 0,
        status: 'unchanged',
        size_old: r.size_old ?? r.size ?? 0,
        size_new: r.size ?? 0,
        n_desc_delta: 0,
        kind: r.kind,
        uri: r.uri,
      }, r.path)
    }
  }

  let maxAbs = 0
  const finalize = (node: CompareTMNode): number => {
    maxAbs = Math.max(maxAbs, Math.abs(node.delta))
    const own = areaMode === 'max'
      ? Math.max(node.size_old, node.size_new)
      : Math.abs(node.delta)
    if (!node.children?.length) {
      node.grew = Math.max(node.delta, 0)
      node.shrank = Math.min(node.delta, 0)
      node.nGrew = Math.max(node.n_desc_delta, 0)
      node.nShrank = Math.min(node.n_desc_delta, 0)
      node.weight = own
      return node.weight
    }
    let kidSum = 0
    for (const k of node.children) {
      kidSum += finalize(k)
      node.grew += k.grew
      node.shrank += k.shrank
      node.nGrew += k.nGrew
      node.nShrank += k.nShrank
    }
    // Hide-unchanged: a parent occupies only its changed children's bytes —
    // no filler for the unenumerated remainder, so areas compare *changes*,
    // not directory sizes. (Frontier leaves still carry their full
    // max(old, new) — change granularity stops there.)
    node.weight = showUnchanged ? Math.max(own, kidSum) : Math.max(kidSum, areaMode === 'max' ? 0 : own)
    const gap = node.weight - kidSum
    if (areaMode === 'max' && showUnchanged && gap > Math.max(1_000_000, node.weight * 0.002)) {
      // No name: it aggregates children the walk never enumerated — the
      // uniform grey (and the tooltip, with the server's count of what it
      // stands for) do the talking.
      const r = rest[pathOf.get(node) ?? '']
      node.children.push({
        key: `${node.key}/__unchanged__`,
        label: '',
        weight: gap,
        delta: 0,
        grew: 0,
        shrank: 0,
        nGrew: 0,
        nShrank: 0,
        status: 'filler',
        size_old: gap,
        size_new: gap,
        n_desc_delta: 0,
        kind: 'filler',
        uri: node.uri,
        nRest: r?.count,
        nDescRest: r === undefined ? undefined : r.count + r.n_desc,
      })
    }
    node.children.sort((a, b) => (b.delta - a.delta) || (b.weight - a.weight))
    return node.weight
  }
  for (const r of roots) {
    finalize(r)
  }

  const cells = roots.filter(r => r.weight > 0)
  cells.sort(areaMode === 'max'
    ? (a, b) => (b.delta - a.delta) || (b.weight - a.weight)
    : (a, b) => b.weight - a.weight)
  return { cells, maxAbsDelta: maxAbs }
}

/**
 * `▲ 32.4 ▼ 6.9 Δ +25.5` — churn among a node's descendants, for title
 * strips / crumbs / tooltips. Units are omitted when every value shares the
 * unit of the total it follows (`totalBytes`), and shown per value otherwise
 * (`▲ 100M ▼ 200K Δ +100M`) — never a mixed line the reader has to parse.
 * Both directions when there's churn both ways, else just the Δ.
 */
function churn(
  n: { grew: number; shrank: number; delta: number },
  /** bytes: the total the line follows (drives unit elision); counts: `null` */
  totalBytes: number | null,
  /** omit the `Δ` part (the caller already shows the net elsewhere) */
  net: boolean = true,
): { text: string; node: ReactNode } | null {
  const both = n.grew > 0 && n.shrank < 0
  if (!both && n.delta === 0) return null
  if (!both && !net) return null
  let fmt: (v: number) => string
  if (totalBytes === null) {
    fmt = formatCount
  } else {
    const vals = both ? [n.grew, -n.shrank, Math.abs(n.delta)] : [Math.abs(n.delta)]
    const unitOf = (b: number) => formatSize(b).split(' ')[1]
    const shared = vals.every(v => v === 0 || unitOf(v) === unitOf(totalBytes))
    fmt = b => shared ? formatSize(b).split(' ')[0] : formatSize(b).replace(' ', '')
  }
  const d = (n.delta > 0 ? '+' : n.delta < 0 ? '−' : '±') + fmt(Math.abs(n.delta))
  const parts: [string, string, string][] = both
    ? [['▲', fmt(n.grew), GREW_GREEN], ['▼', fmt(-n.shrank), SHRANK_RED]]
    : []
  // Counts carry their sign, so the bare number is the net; bytes keep the
  // `Δ` glyph in title strips where it's the only cue the number is a delta.
  if (net) parts.push([totalBytes === null ? '' : 'Δ', d, deltaTextColor(n.delta)])
  return {
    text: parts.map(([g, v]) => (g ? `${g} ${v}` : v)).join(' '),
    node: parts.map(([g, v, color], i) => (
      <span key={g || 'net'} style={{ color, marginLeft: i ? 6 : 0 }}>{g ? `${g} ${v}` : v}</span>
    )),
  }
}

function CompareTreemap({
  result,
  rec,
  recState,
  onRecRetry,
  onDrill,
}: {
  result: CompareResult
  rec?: CompareRecResult
  recState: 'loading' | 'error' | 'ready'
  onRecRetry: () => void
  onDrill: (uri: string) => void
}) {
  const [areaMode, setAreaMode] = useState<AreaMode>('max')
  const [showUnchanged, setShowUnchanged] = useState(true)
  const { root, maxAbsDelta } = useMemo(() => {
    const { cells, maxAbsDelta: maxAbs } = buildCompareTree(result, rec, areaMode, showUnchanged)
    const totalWeight = cells.reduce((s, c) => s + c.weight, 0)
    // Root aggregates its cells so the widget's crumbs line reads correctly.
    const root: CompareTMNode & { children: CompareTMNode[] } = {
      key: result.uri,
      label: result.uri,
      weight: totalWeight,
      delta: result.summary.total_delta,
      grew: cells.reduce((s, c) => s + c.grew, 0),
      shrank: cells.reduce((s, c) => s + c.shrank, 0),
      nGrew: cells.reduce((s, c) => s + c.nGrew, 0),
      nShrank: cells.reduce((s, c) => s + c.nShrank, 0),
      status: 'changed',
      size_old: result.scan1.size ?? 0,
      size_new: result.scan2.size ?? 0,
      n_desc_delta: (result.scan2.n_desc ?? 0) - (result.scan1.n_desc ?? 0),
      kind: 'dir',
      uri: result.uri,
      children: cells,
    }
    return { root, maxAbsDelta: maxAbs }
  }, [result, rec, areaMode, showUnchanged])

  if (root.children.length === 0) {
    return (
      <Paper sx={{ p: 3, textAlign: 'center' }}>
        <Typography color="text.secondary" variant="body2">
          {areaMode === 'max' && showUnchanged
            ? 'Nothing to plot — no row has any bytes on either side.'
            : 'No size deltas to plot — every row is unchanged.'}
        </Typography>
        {!showUnchanged && (
          <Button size="small" sx={{ mt: 1 }} onClick={() => setShowUnchanged(true)}>
            show unchanged
          </Button>
        )}
      </Paper>
    )
  }

  return (
    <Paper sx={{ p: 0, mb: 3, overflow: 'hidden' }}>
      <Box sx={{ height: 340, position: 'relative' }}>
        <DTTreemap<CompareTMNode & { children?: CompareTMNode[] }>
          root={root}
          getSize={n => n.weight}
          getChildren={n => (n as { children?: CompareTMNode[] }).children}
          getLabel={n => n.label}
          // First-class fold cells: aggregate the dust's Δ so a green parent
          // whose change lives entirely in tiny children still shows where
          // (band + tooltip), instead of an inert "(+N)" tile.
          mergeSmall={small => ({
            key: `${small[0].key}__fold${small.length}`,
            label: `(+${small.length})`,
            weight: small.reduce((s, c) => s + c.weight, 0),
            delta: small.reduce((s, c) => s + c.delta, 0),
            grew: small.reduce((s, c) => s + c.grew, 0),
            shrank: small.reduce((s, c) => s + c.shrank, 0),
            nGrew: small.reduce((s, c) => s + c.nGrew, 0),
            nShrank: small.reduce((s, c) => s + c.nShrank, 0),
            status: 'fold',
            size_old: small.reduce((s, c) => s + c.size_old, 0),
            size_new: small.reduce((s, c) => s + c.size_new, 0),
            n_desc_delta: small.reduce((s, c) => s + c.n_desc_delta, 0),
            kind: 'fold',
            uri: small[0].uri,
            nFolded: small.reduce((s, c) => s + (c.nFolded ?? 1), 0),
          })}
          // Brighter sibling separation: the compare palette's dark neutrals
          // make the default (transparent) cell rings invisible.
          mapStyle={{ '--dt-treemap-cell-border': 'rgba(255, 255, 255, 0.14)' } as CSSProperties}
          // Displayed inline with the label — the raw |Δ| magnitude. Sign is
          // encoded by the cell color; exact old/new/Δ lives in the tooltip.
          formatSize={formatSize}
          colorForCell={n => {
            if (areaMode === 'max') {
              if (n.children?.length) {
                // Parent: children tile its interior, so only the title strip
                // and gutters show — tint them by the net trend Δ/weight (a
                // summary cue; magnitude lives in the leaf bands). Net-zero
                // parents get the same grey as every other unchanged rect.
                if (n.delta === 0) return { bg: UNCHANGED_GREY, ink: '#fff', ...(n.status === 'touched' && { hatch: TOUCHED_HATCH }) }
                const t = n.weight === 0 ? 0 : n.delta / n.weight
                return { bg: deltaColor(t), ink: '#fff' }
              }
              // Sub-rect encoding: a grey rect of min(old, new) bytes plus a
              // full-strength colored band of |Δ| bytes, filling from the
              // bottom — magnitude by *area*, not saturation.
              const f = n.weight === 0 ? 0 : Math.min(1, Math.abs(n.delta) / n.weight)
              if (f === 0) return { bg: UNCHANGED_GREY, ink: '#fff', ...(n.status === 'touched' && { hatch: TOUCHED_HATCH }) }
              const pct = `${(f * 100).toFixed(2)}%`
              const band = deltaColor(Math.sign(n.delta))
              return {
                bg: `linear-gradient(to top, ${band} ${pct}, ${UNCHANGED_GREY} ${pct})`,
                ink: '#fff', // uniform with every other label
              }
            }
            // Δ mode: full cell tinted by Δ relative to the largest |Δ|.
            if (n.delta === 0) return { bg: UNCHANGED_GREY, ink: '#fff' }
            const t = maxAbsDelta === 0 ? 0 : n.delta / maxAbsDelta
            return { bg: deltaColor(t), ink: '#fff' }
          }}
          renderCellExtra={areaMode === 'max' ? (n, _path, { w, h }) => {
            // Per-sub-rect size labels: Δ centered in the colored band, the
            // unchanged min(old, new) bytes centered in the grey rect above it.
            // Leaves only (a parent's interior belongs to its children), and
            // skip narrow slivers — a clipped "+128.0KB" is worse than none.
            if (n.children?.length || w < 56) return null
            const f = n.weight === 0 ? 0 : Math.min(1, Math.abs(n.delta) / n.weight)
            if (f === 0) return null
            const bandH = h * f
            const greyH = h - bandH
            const minBytes = Math.min(n.size_old, n.size_new)
            const lbl = (top: string, height: number, text: string, style: CSSProperties) => (
              <div style={{
                position: 'absolute', top, left: 0, right: 0, height,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                pointerEvents: 'none', fontSize: '0.75rem', ...style,
              }}>{text}</div>
            )
            return (
              <>
                {bandH >= 16 && lbl(`${(100 - f * 100).toFixed(2)}%`, bandH,
                  formatDelta(n.delta), { color: '#fff', fontWeight: 600 })}
                {f < 1 && greyH >= 44 && minBytes > 0 && lbl('0', greyH,
                  formatSize(minBytes), { color: 'var(--dt-treemap-ink, #d0d0d8)', opacity: 0.65 })}
              </>
            )
          } : undefined}
          // Branch title strips have room to spare: inline the ▲/▼/Δ (or
          // just the Δ) when it fits after the name and size — a rough
          // char-width estimate against the cell box, since the size span
          // never shrinks and would crush the name otherwise.
          renderCellSubtitle={(n, _path, { w }) => {
            if (!n.children?.length) return null
            // ▲/▼/Δ glyphs run wider than digits; err toward dropping the
            // stats over ellipsizing the name.
            const room = w - 8 - n.label.length * 7 - formatSize(n.weight).length * 6.5 - 12
            const fits = (c: { text: string }) => c.text.length * 7.5 + 12 <= room
            const full = churn(n, n.weight)
            if (!full) return null
            if (fits(full)) return full.node
            const net = churn({ grew: 0, shrank: 0, delta: n.delta }, n.weight)
            return net && fits(net) ? net.node : null
          }}
          // The crumbs row is the drilled node's own "title strip".
          renderCrumbSuffix={n => (
            <>— {formatSize(n.weight)}{(() => { const c = churn(n, n.weight); return c && <> {c.node}</> })()}</>
          )}
          renderTooltip={n => (
            <>
              <div style={{ fontWeight: 500 }}>
                {n.status === 'fold'
                  ? `${(n.nFolded ?? 0).toLocaleString()} smaller items`
                  : n.status === 'filler'
                  ? (n.nRest === undefined ? 'unchanged' : `${n.nRest.toLocaleString()} unchanged children`)
                  : n.label || 'unchanged'}
              </div>
              <div style={{ color: 'rgba(255, 255, 255, 0.75)', fontSize: '0.85em' }}>
                {n.delta === 0
                  ? formatSize(n.size_new)
                  : <>{formatSize(n.size_old)} → {formatSize(n.size_new)} (<span style={{ color: deltaTextColor(n.delta), fontWeight: 600, fontSize: '1.1em' }}>{formatDelta(n.delta)}</span>)</>}
                {n.status === 'unchanged' && ' — unchanged'}
                {n.status === 'touched' && ' — touched (same bytes & count, mtime moved)'}
                {n.status === 'filler' && n.nDescRest !== undefined && n.nDescRest !== n.nRest && (
                  <> · {formatCount(n.nDescRest)} entries below</>
                )}
              </div>
              {/* Aggregates with churn in both directions: the net alone
                  hides how much grew vs shrank among descendants. */}
              {n.grew > 0 && n.shrank < 0 && (
                <div style={{ fontSize: '0.8em' }}>{churn(n, n.size_new, false)?.node}</div>
              )}
              {/* Entry counts get the same ▲/▼/Δ treatment as bytes. */}
              {(n.n_desc_delta !== 0 || n.nGrew > 0) && (
                <div style={{ fontSize: '0.8em' }}>
                  <span style={{ opacity: 0.6 }}>count </span>
                  {churn({ grew: n.nGrew, shrank: n.nShrank, delta: n.n_desc_delta }, null)?.node}
                </div>
              )}
              {/* Only what the size line doesn't already say. */}
              {(n.status === 'filler' || n.status === 'fold' || n.status === 'added' || n.status === 'removed' || (n.pruned && rec?.index?.status !== 'done')) && (
                <div style={{ opacity: 0.5, fontSize: '0.75em', marginTop: 2 }}>
                  {n.status === 'filler' ? 'unchanged bytes the diff never needed to enumerate'
                    : n.status === 'fold' ? 'children too small to draw, aggregated'
                    : n.status === 'added' || n.status === 'removed' ? n.status
                    : null}
                  {n.pruned && rec?.index?.status !== 'done' && <>
                    {(n.status === 'added' || n.status === 'removed') && ' · '}
                    {n.delta === 0 && n.n_desc_delta === 0
                      ? 'not descended (walk budget): something inside moved — click to compare here'
                      : 'Δ not localized (walk budget) — click to compare here'}
                  </>}
                </div>
              )}
            </>
          )}
          renderLegend={() => (
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: '0.8rem', opacity: 0.85 }}>
              <span style={{ display: 'inline-block', width: 12, height: 12, background: deltaColor(1), borderRadius: 2 }} />
              grew
              <span style={{ display: 'inline-block', width: 12, height: 12, background: deltaColor(-1), borderRadius: 2 }} />
              shrank
              {areaMode === 'max' && (
                // Legend-item toggle (plot-legend style): click to hide/show
                // unchanged rows. Hidden = only changed entries plot (at
                // max(old, new) size); parents shrink to their changed
                // contents, fillers and grey context rows drop out.
                <button
                  onClick={e => { e.stopPropagation(); setShowUnchanged(s => !s) }}
                  title={showUnchanged
                    ? 'Hide unchanged rows: only changed entries plot; parents shrink to their changed contents'
                    : 'Show unchanged rows: grey context cells and fillers restore true directory proportions'}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 6, cursor: 'pointer',
                    background: 'none', border: 'none', padding: 0, margin: 0,
                    font: 'inherit', color: 'inherit',
                    opacity: showUnchanged ? 1 : 0.45,
                    textDecoration: showUnchanged ? 'none' : 'line-through',
                  }}
                >
                  <span style={{ display: 'inline-block', width: 12, height: 12, background: UNCHANGED_GREY, borderRadius: 2 }} />
                  unchanged
                </button>
              )}
              {areaMode === 'max' && (
                <span title="same bytes & count, mtime moved (rename / net-zero churn / touch)" style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ display: 'inline-block', width: 12, height: 12, background: UNCHANGED_GREY, backgroundImage: TOUCHED_HATCH, borderRadius: 2 }} />
                  touched
                </span>
              )}
              <span style={{ opacity: 0.6, marginLeft: 4 }}>
                {areaMode === 'max' ? 'area = max(old, new), band = |Δ|' : 'area = |Δ|'}
              </span>
              {rec?.index && rec.index.status !== 'done' && (
                <span
                  title={rec.index.status === 'failed'
                    ? `full diff index failed: ${rec.index.error ?? 'unknown error'} — showing the budgeted walk`
                    : 'showing a budgeted walk while the full diff index builds; the map completes itself when it lands'}
                  style={{ opacity: 0.6, marginLeft: 6, fontStyle: 'italic' }}
                >
                  {rec.index.status === 'failed' ? 'index failed' : 'indexing…'}
                </span>
              )}
              <span style={{ display: 'inline-flex', gap: 2, marginLeft: 6 }}>
                {(['max', 'delta'] as const).map(m => (
                  <button
                    key={m}
                    onClick={e => { e.stopPropagation(); setAreaMode(m) }}
                    title={m === 'max'
                      ? 'Size cells by max(old, new): deleted subtrees keep their old area; stable structure stays visible'
                      : 'Size cells by |Δbytes|: churn only, unchanged rows dropped'}
                    style={{
                      cursor: 'pointer', fontSize: '0.75rem', padding: '1px 7px', borderRadius: 3,
                      border: '1px solid var(--dt-border, #444)',
                      background: areaMode === m ? 'var(--dt-accent-bg, #30363d)' : 'transparent',
                      color: 'inherit', fontWeight: areaMode === m ? 600 : 400,
                    }}
                  >
                    {m === 'max' ? 'max' : 'Δ'}
                  </button>
                ))}
              </span>
            </span>
          )}
          onCellClick={(n) => {
            // Cells with in-tree children drill inside the widget (return
            // false); frontier/unchanged dirs navigate to /compare there.
            if (n.children?.length) return false
            if (n.kind !== 'dir') return false // let the widget pin the tooltip
            onDrill(n.uri)
            return true
          }}
        />
        {/* The colored Δ cells come from the recursive frontier; until it
            lands, the flat grey context alone reads as a broken all-grey
            map — put the state front and center, not in the legend. */}
        {recState !== 'ready' && (
          <Box
            sx={{
              position: 'absolute', inset: 0, zIndex: 2,
              display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
              gap: 1.5, bgcolor: 'rgba(8, 10, 12, 0.55)',
              pointerEvents: recState === 'error' ? 'auto' : 'none',
            }}
          >
            {recState === 'loading' ? (
              <>
                <CircularProgress size={40} />
                <Typography variant="body2" sx={{ opacity: 0.9 }}>computing Δ detail…</Typography>
              </>
            ) : (
              <Button size="small" color="error" variant="outlined" onClick={onRecRetry}>
                Δ detail failed — retry
              </Button>
            )}
          </Box>
        )}
      </Box>
    </Paper>
  )
}

function formatDateTime(dateStr: string): string {
  const date = new Date(dateStr)
  return date.toLocaleString()
}

const statusColors = {
  added: { bg: 'rgba(46, 160, 67, 0.15)' },
  removed: { bg: 'rgba(248, 81, 73, 0.15)' },
  changed: { bg: 'transparent' },
  touched: { bg: 'transparent' },
  unchanged: { bg: 'transparent' },
}

// Delta bar component - visual representation of size change
function DeltaBar({ delta, maxDelta }: { delta: number; maxDelta: number }) {
  if (maxDelta === 0) return <div style={{ width: '50px' }} />
  const pct = Math.min(Math.abs(delta) / maxDelta * 100, 100)
  const color = delta === 0 ? 'transparent' : deltaTextColor(delta)
  return (
    <div style={{
      width: '50px',
      height: '8px',
      backgroundColor: 'rgba(255,255,255,0.1)',
      borderRadius: '4px',
      overflow: 'hidden',
      flexShrink: 0,
    }}>
      <div style={{
        width: `${pct}%`,
        height: '100%',
        backgroundColor: color,
        borderRadius: '4px',
      }} />
    </div>
  )
}

function formatDeltaNumber(n: number): string {
  const sign = n > 0 ? '+' : ''
  return sign + formatCount(Math.abs(n))
}

// Check if a path is covered by a scan (path is at or below scan_path)
function isPathCoveredByScan(path: string, scanPath: string): boolean {
  if (!scanPath) return false
  // Normalize: ensure both have consistent trailing slash handling
  const normPath = path.endsWith('/') ? path.slice(0, -1) : path
  const normScan = scanPath.endsWith('/') ? scanPath.slice(0, -1) : scanPath
  return normPath === normScan || normPath.startsWith(normScan + '/')
}

// Breadcrumb component for compare view
function CompareBreadcrumbs({
  uri,
  routeType,
  scan1Path,
  scan2Path,
  scan1,
  scan2,
}: {
  uri: string
  routeType: RouteType
  scan1Path?: string
  scan2Path?: string
  scan1: number | ''
  scan2: number | ''
}) {
  // Split path into segments
  const isFile = routeType === 'file'
  const scheme = routeType // 's3' | 'gcs' | 'r2' | 'ssh' (file handled by isFile)
  let segments: { name: string; path: string }[] = []

  if (isFile) {
    // /Users/ryan/Library/...
    const parts = uri.split('/').filter(Boolean)
    let currentPath = ''
    for (const part of parts) {
      currentPath += '/' + part
      segments.push({ name: part, path: currentPath })
    }
  } else {
    // <scheme>://bucket/path/to/dir
    const withoutScheme = uri.slice(scheme.length + 3) // strip '<scheme>://'
    const parts = withoutScheme.split('/').filter(Boolean)
    let currentPath = `${scheme}:/`
    for (const part of parts) {
      currentPath += '/' + part
      segments.push({ name: part, path: currentPath })
    }
  }

  return (
    <Typography
      variant="body2"
      sx={{ mb: 3, fontFamily: 'monospace', display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}
    >
      {isFile && <span style={{ color: '#8b949e' }}>/</span>}
      {!isFile && <span style={{ color: '#8b949e' }}>{scheme}://</span>}
      {segments.map((seg, i) => {
        const basePath = `/compare${uriToPath(seg.path)}`
        const params = new URLSearchParams()
        if (scan1 !== '') params.set('scan1', String(scan1))
        if (scan2 !== '') params.set('scan2', String(scan2))
        const compareUrl = params.toString() ? `${basePath}?${params}` : basePath

        // Check if this segment is covered by both scans
        const coveredBy1 = scan1Path ? isPathCoveredByScan(seg.path, scan1Path) : true
        const coveredBy2 = scan2Path ? isPathCoveredByScan(seg.path, scan2Path) : true
        const fullyCovered = coveredBy1 && coveredBy2
        const partiallyCovered = coveredBy1 || coveredBy2

        // Style based on coverage - brighter colors for better visibility
        const color = fullyCovered ? '#e6edf3' : partiallyCovered ? '#b0b8c1' : '#8b949e'
        const opacity = 1

        return (
          <span key={seg.path} style={{ display: 'inline-flex', alignItems: 'center' }}>
            <Link
              to={compareUrl}
              style={{
                color,
                opacity,
                textDecoration: 'none',
              }}
              title={
                fullyCovered
                  ? 'Both scans cover this path'
                  : partiallyCovered
                    ? 'Only one scan covers this path'
                    : 'Neither scan covers this path'
              }
              onMouseEnter={(e) => (e.currentTarget.style.textDecoration = 'underline')}
              onMouseLeave={(e) => (e.currentTarget.style.textDecoration = 'none')}
            >
              {seg.name}
            </Link>
            {i < segments.length - 1 && (
              <span style={{ color: '#6e7681', margin: '0 2px' }}>/</span>
            )}
          </span>
        )
      })}
    </Typography>
  )
}

// Sortable column header component
function SortHeader({
  label,
  column,
  sortColumn,
  sortDirection,
  onSort,
  style,
}: {
  label: string
  column: SortColumn
  sortColumn: SortColumn | null
  sortDirection: SortDirection
  onSort: (col: SortColumn) => void
  style?: React.CSSProperties
}) {
  const isActive = sortColumn === column
  return (
    <th
      onClick={() => onSort(column)}
      style={{
        ...style,
        cursor: 'pointer',
        userSelect: 'none',
        fontWeight: 'normal',
        color: isActive ? '#e6edf3' : '#8b949e',
      }}
    >
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: '2px' }}>
        {label}
        {isActive && (sortDirection === 'desc' ? <FaSortDown size={10} /> : <FaSortUp size={10} />)}
      </span>
    </th>
  )
}

// Parent directory summary row - shows totals for the directory being compared
function ParentSummaryRow({
  result,
  maxSizeDelta,
  maxDescDelta,
  onScan,
  isScanning,
}: {
  result: CompareResult
  maxSizeDelta: number
  maxDescDelta: number
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
}) {
  const { scan1, scan2, uri } = result
  const sizeDelta = (scan2.size ?? 0) - (scan1.size ?? 0)
  const descDelta = (scan2.n_desc ?? 0) - (scan1.n_desc ?? 0)

  const sizeDeltaColor = deltaTextColor(sizeDelta)
  const descDeltaColor = deltaTextColor(descDelta)

  const td: React.CSSProperties = { padding: '8px 6px', textAlign: 'right', fontFamily: 'monospace', fontSize: '0.85em', whiteSpace: 'nowrap' }
  const dim: React.CSSProperties = { color: '#8b949e' }

  // Get the directory name for display
  const dirName = uri === '/' ? '/' : uri.split('/').pop() || uri

  return (
    <tr style={{ backgroundColor: 'rgba(88, 166, 255, 0.1)', borderBottom: '2px solid rgba(255,255,255,0.2)' }}>
      {/* Path */}
      <td style={{ padding: '8px', fontWeight: 'bold' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <FaFolder size={14} color="#54aeff" style={{ flexShrink: 0 }} />
          <span style={{ fontFamily: 'monospace', fontSize: '0.9em' }}>. ({dirName})</span>
        </div>
      </td>
      {/* Size: before */}
      <td style={{ ...td, ...dim, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{formatSize(scan1.size)}</td>
      {/* Size: after */}
      <td style={td}>{formatSize(scan2.size)}</td>
      {/* Size: delta */}
      <td style={{ ...td, color: sizeDeltaColor, fontWeight: sizeDelta !== 0 ? 'bold' : undefined }}>
        {formatDelta(sizeDelta)}
      </td>
      {/* Size: bar */}
      <td style={{ padding: '8px 12px 8px 4px' }}>
        <DeltaBar delta={sizeDelta} maxDelta={Math.max(maxSizeDelta, Math.abs(sizeDelta))} />
      </td>
      {/* Desc: before */}
      <td style={{ ...td, ...dim, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{formatCount(scan1.n_desc)}</td>
      {/* Desc: after */}
      <td style={td}>{formatCount(scan2.n_desc)}</td>
      {/* Desc: delta */}
      <td style={{ ...td, color: descDeltaColor, fontWeight: descDelta !== 0 ? 'bold' : undefined }}>
        {formatDeltaNumber(descDelta)}
      </td>
      {/* Desc: bar */}
      <td style={{ padding: '8px 4px' }}>
        <DeltaBar delta={descDelta} maxDelta={Math.max(maxDescDelta, Math.abs(descDelta))} />
      </td>
      {/* Scan button */}
      <td style={{ padding: '4px 8px', textAlign: 'center', borderLeft: '1px solid rgba(255,255,255,0.1)' }}>
        <Tooltip title={`Scan ${uri}`}>
          <span>
            <Button
              size="small"
              onClick={() => onScan(uri)}
              disabled={isScanning(uri)}
              sx={{ minWidth: 'auto', padding: '2px 6px' }}
            >
              {isScanning(uri) ? <CircularProgress size={12} /> : <FaSync size={10} />}
            </Button>
          </span>
        </Tooltip>
      </td>
    </tr>
  )
}

function CompareTable({
  result,
  onScan,
  isScanning,
  getProgress,
  scan1,
  scan2,
}: {
  result: CompareResult
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
  getProgress: (path: string) => { items_found?: number } | undefined
  scan1: number | ''
  scan2: number | ''
}) {
  const [sortColumn, setSortColumn] = useState<SortColumn | null>('size_delta')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')

  const handleSort = (col: SortColumn) => {
    if (sortColumn === col) {
      setSortDirection(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortColumn(col)
      setSortDirection('desc')
    }
  }

  // Filter out unchanged rows for cleaner view
  const changedRows = result.rows.filter(r => r.status !== 'unchanged')

  // Sort rows
  const sortedRows = useMemo(() => {
    if (!sortColumn) return changedRows
    return [...changedRows].sort((a, b) => {
      let aVal: number, bVal: number
      switch (sortColumn) {
        case 'size_old': aVal = a.size_old ?? a.size ?? 0; bVal = b.size_old ?? b.size ?? 0; break
        case 'size_new': aVal = a.size ?? 0; bVal = b.size ?? 0; break
        case 'size_delta': aVal = Math.abs(a.size_delta); bVal = Math.abs(b.size_delta); break
        case 'desc_old': aVal = a.n_desc_old ?? a.n_desc ?? 0; bVal = b.n_desc_old ?? b.n_desc ?? 0; break
        case 'desc_new': aVal = a.n_desc ?? 0; bVal = b.n_desc ?? 0; break
        case 'desc_delta': aVal = Math.abs(a.n_desc_delta ?? 0); bVal = Math.abs(b.n_desc_delta ?? 0); break
      }
      return sortDirection === 'desc' ? bVal - aVal : aVal - bVal
    })
  }, [changedRows, sortColumn, sortDirection])

  // Find max deltas for scaling bars
  const maxSizeDelta = Math.max(...changedRows.map(r => Math.abs(r.size_delta)), 1)
  const maxDescDelta = Math.max(...changedRows.map(r => Math.abs(r.n_desc_delta ?? 0)), 1)

  const subTh: React.CSSProperties = { padding: '4px 6px', textAlign: 'right', fontSize: '0.75em', whiteSpace: 'nowrap' }

  return (
    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
      <thead>
        <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <th style={{ textAlign: 'left', padding: '8px', width: '100%' }}>Path</th>
          <th colSpan={4} style={{ textAlign: 'center', padding: '8px 12px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}>Size</th>
          <th colSpan={4} style={{ textAlign: 'center', padding: '8px 12px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}>Descendants</th>
          <th style={{ padding: '8px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}></th>
        </tr>
        <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <th style={{ width: '100%' }}></th>
          <SortHeader label="old" column="size_old" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={{ ...subTh, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }} />
          <SortHeader label="new" column="size_new" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <SortHeader label="Δ" column="size_delta" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <th style={subTh}></th>
          <SortHeader label="old" column="desc_old" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={{ ...subTh, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }} />
          <SortHeader label="new" column="desc_new" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <SortHeader label="Δ" column="desc_delta" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <th style={subTh}></th>
          <th style={{ whiteSpace: 'nowrap' }}></th>
        </tr>
      </thead>
      <tbody>
        {/* Parent directory summary row */}
        <ParentSummaryRow
          result={result}
          maxSizeDelta={maxSizeDelta}
          maxDescDelta={maxDescDelta}
          onScan={onScan}
          isScanning={isScanning}
        />
        {sortedRows.map((row) => (
          <CompareRowComponent
            key={row.path}
            row={row}
            maxSizeDelta={maxSizeDelta}
            maxDescDelta={maxDescDelta}
            parentUri={result.uri}
            onScan={onScan}
            isScanning={isScanning}
            getProgress={getProgress}
            scan1={scan1}
            scan2={scan2}
          />
        ))}
        {sortedRows.length === 0 && (
          <tr>
            <td colSpan={10} style={{ padding: '24px', textAlign: 'center', color: '#8b949e' }}>
              No changes detected between scans
            </td>
          </tr>
        )}
        {/* The treemap draws unchanged children as grey context; the table
            lists changes only — say what it's leaving out so they agree. */}
        {result.summary.unchanged > 0 && (
          <tr>
            <td colSpan={10} style={{ padding: '6px 8px', color: '#8b949e', fontSize: '0.8em' }}>
              {result.summary.unchanged.toLocaleString()} unchanged {result.summary.unchanged === 1 ? 'entry' : 'entries'} not listed
            </td>
          </tr>
        )}
      </tbody>
    </table>
  )
}

function CompareRowComponent({
  row,
  maxSizeDelta,
  maxDescDelta,
  onScan,
  isScanning,
  getProgress: _getProgress,
  scan1,
  scan2,
}: {
  row: CompareRow
  maxSizeDelta: number
  maxDescDelta: number
  parentUri: string
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
  getProgress: (path: string) => { items_found?: number } | undefined
  scan1: number | ''
  scan2: number | ''
}) {
  const { bg } = statusColors[row.status]
  const Icon = row.kind === 'dir' ? FaFolder : FaFile
  const iconColor = row.kind === 'dir' ? '#54aeff' : '#8b949e'

  const sizeDeltaColor = deltaTextColor(row.size_delta)
  const descDelta = row.n_desc_delta ?? 0
  const descDeltaColor = deltaTextColor(descDelta)

  // Build link URL for drilling into subdirectory (preserving scan params)
  const childUri = row.uri
  const basePath = `/compare${uriToPath(childUri)}`
  const params = new URLSearchParams()
  if (scan1 !== '') params.set('scan1', String(scan1))
  if (scan2 !== '') params.set('scan2', String(scan2))
  const compareUrl = params.toString() ? `${basePath}?${params}` : basePath

  const td: React.CSSProperties = { padding: '8px 6px', textAlign: 'right', fontFamily: 'monospace', fontSize: '0.85em', whiteSpace: 'nowrap' }

  // Size values
  const sizeBefore = row.status === 'added' ? '-' : formatSize(row.size_old ?? row.size)
  const sizeAfter = row.status === 'removed' ? '-' : formatSize(row.size)
  const sizeBeforeColor = row.status === 'removed' ? '#f85149' : '#8b949e'
  const sizeAfterColor = row.status === 'added' ? '#3fb950' : undefined

  // Desc values
  const descBefore = row.status === 'added' ? '-' : formatCount(row.n_desc_old ?? row.n_desc)
  const descAfter = row.status === 'removed' ? '-' : formatCount(row.n_desc)
  const descBeforeColor = row.status === 'removed' ? '#f85149' : '#8b949e'
  const descAfterColor = row.status === 'added' ? '#3fb950' : undefined

  return (
    <tr style={{ backgroundColor: bg, borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
      {/* Path */}
      <td style={{ padding: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Icon size={14} color={iconColor} style={{ flexShrink: 0 }} />
          {row.kind === 'dir' ? (
            <Link to={compareUrl} style={{ fontFamily: 'monospace', fontSize: '0.9em', color: 'inherit', textDecoration: 'none' }}>
              {row.path}
            </Link>
          ) : (
            <span style={{ fontFamily: 'monospace', fontSize: '0.9em' }}>{row.path}</span>
          )}
          {row.status === 'touched' && (
            <span
              title="same bytes & count, mtime moved (rename / net-zero churn / touch)"
              style={{
                fontSize: '0.7em', color: '#8b949e', padding: '0 6px', borderRadius: 3,
                background: UNCHANGED_GREY, backgroundImage: TOUCHED_HATCH,
              }}
            >
              touched
            </span>
          )}
        </div>
      </td>
      {/* Size: before */}
      <td style={{ ...td, color: sizeBeforeColor, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{sizeBefore}</td>
      {/* Size: after */}
      <td style={{ ...td, color: sizeAfterColor }}>{sizeAfter}</td>
      {/* Size: delta */}
      <td style={{ ...td, color: sizeDeltaColor, fontWeight: row.size_delta !== 0 ? 'bold' : undefined }}>
        {formatDelta(row.size_delta)}
      </td>
      {/* Size: bar */}
      <td style={{ padding: '8px 12px 8px 4px' }}>
        <DeltaBar delta={row.size_delta} maxDelta={maxSizeDelta} />
      </td>
      {/* Desc: before */}
      <td style={{ ...td, color: descBeforeColor, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{row.kind === 'dir' ? descBefore : '-'}</td>
      {/* Desc: after */}
      <td style={{ ...td, color: descAfterColor }}>{row.kind === 'dir' ? descAfter : '-'}</td>
      {/* Desc: delta */}
      <td style={{ ...td, color: descDeltaColor, fontWeight: descDelta !== 0 ? 'bold' : undefined }}>
        {row.kind === 'dir' ? formatDeltaNumber(descDelta) : '-'}
      </td>
      {/* Desc: bar */}
      <td style={{ padding: '8px 4px' }}>
        {row.kind === 'dir' ? <DeltaBar delta={descDelta} maxDelta={maxDescDelta} /> : null}
      </td>
      {/* Scan button */}
      <td style={{ padding: '4px 8px', textAlign: 'center', borderLeft: '1px solid rgba(255,255,255,0.1)' }}>
        {row.kind === 'dir' && (
          <Tooltip title={`Scan ${row.path}`}>
            <span>
              <Button
                size="small"
                onClick={() => onScan(row.uri)}
                disabled={isScanning(row.uri)}
                sx={{ minWidth: 'auto', padding: '2px 6px' }}
              >
                {isScanning(row.uri) ? <CircularProgress size={12} /> : <FaSync size={10} />}
              </Button>
            </span>
          </Tooltip>
        )}
      </td>
    </tr>
  )
}

function Summary({ result }: { result: CompareResult }) {
  const { summary, scan1, scan2 } = result

  return (
    <Paper sx={{ p: 2, mb: 3 }}>
      <Box sx={{ display: 'flex', gap: 4, flexWrap: 'wrap', alignItems: 'center' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Box>
            <Typography variant="caption" color="text.secondary">From</Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {formatDateTime(scan1.time)}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {formatSize(scan1.size)}
            </Typography>
          </Box>
          <FaArrowRight color="#8b949e" />
          <Box>
            <Typography variant="caption" color="text.secondary">To</Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {formatDateTime(scan2.time)}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {formatSize(scan2.size)}
            </Typography>
          </Box>
        </Box>
        <Box sx={{ borderLeft: '1px solid rgba(255,255,255,0.1)', pl: 3, display: 'flex', gap: 3 }}>
          <Box>
            <Typography variant="caption" color="text.secondary">Added</Typography>
            <Typography variant="body1" sx={{ color: '#3fb950', fontWeight: 'bold' }}>
              {summary.added}
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" color="text.secondary">Removed</Typography>
            <Typography variant="body1" sx={{ color: '#f85149', fontWeight: 'bold' }}>
              {summary.removed}
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" color="text.secondary">Changed</Typography>
            <Typography variant="body1" sx={{ color: '#d29922', fontWeight: 'bold' }}>
              {summary.changed}
            </Typography>
          </Box>
        </Box>
        <Box sx={{ borderLeft: '1px solid rgba(255,255,255,0.1)', pl: 3 }}>
          <Typography variant="caption" color="text.secondary">Total Delta</Typography>
          <Typography
            variant="body1"
            sx={{
              fontWeight: 'bold',
              fontFamily: 'monospace',
              color: deltaTextColor(summary.total_delta),
            }}
          >
            {formatDelta(summary.total_delta)}
          </Typography>
        </Box>
      </Box>
    </Paper>
  )
}

export function CompareView() {
  const location = useLocation()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  // Extract URI and routeType from path: /compare/file/Users/ryan/…, /compare/s3/bucket/…,
  // /compare/gcs/bucket/…, /compare/r2/bucket/…, /compare/ssh/host/…
  const pathAfterCompare = location.pathname.replace(/^\/compare/, '') || '/'
  const { uri, routeType } = comparePathToUri(pathAfterCompare)

  // Get scan selections from URL params
  const urlScan1 = searchParams.get('scan1')
  const urlScan2 = searchParams.get('scan2')
  const scan1: number | '' = urlScan1 ? parseInt(urlScan1, 10) : ''
  const scan2: number | '' = urlScan2 ? parseInt(urlScan2, 10) : ''

  // Update URL params when selections change
  const setScan1 = (id: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id === '') {
        next.delete('scan1')
      } else {
        next.set('scan1', String(id))
      }
      return next
    }, { replace: true })
  }
  const setScan2 = (id: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id === '') {
        next.delete('scan2')
      } else {
        next.set('scan2', String(id))
      }
      return next
    }, { replace: true })
  }
  const setScans = (id1: number | '', id2: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id1 === '') {
        next.delete('scan1')
      } else {
        next.set('scan1', String(id1))
      }
      if (id2 === '') {
        next.delete('scan2')
      } else {
        next.set('scan2', String(id2))
      }
      return next
    }, { replace: true })
  }

  const [history, setHistory] = useState<ScanHistoryItem[]>([])
  const [historyLoading, setHistoryLoading] = useState(true)
  const [result, setResult] = useState<CompareResult | null>(null)
  const [recResult, setRecResult] = useState<CompareRecResult | null>(null)
  const [recState, setRecState] = useState<'loading' | 'error' | 'ready'>('loading')
  const [recAttempt, setRecAttempt] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Record visit to recent paths
  const { recordVisit } = useRecentPaths()
  useEffect(() => {
    if (uri && !isSchemeRoot(uri)) {
      recordVisit(uri, 'compare')
    }
  }, [uri, recordVisit])

  // Track previous URI to detect navigation
  const prevUriRef = useRef(uri)

  // Load scan history
  useEffect(() => {
    const isNavigation = prevUriRef.current !== uri
    prevUriRef.current = uri

    setHistory([])
    setHistoryLoading(true)
    setResult(null)
    setError(null)

    fetchScanHistory(uri)
      .then(h => {
        setHistory(h)

        // If we have URL scan params, keep them - they may be valid even if not in
        // this path's history (e.g., for a newly added directory that didn't exist
        // in the older scan, the older scan won't be in history, but it's still
        // a valid comparison showing the directory was added)
        if (scan1 !== '' && scan2 !== '') {
          // URL params present - keep them, let compare API handle the details
          // Just ensure correct order if both are in history
          const scan1Item = h.find(item => item.id === scan1)
          const scan2Item = h.find(item => item.id === scan2)
          if (scan1Item && scan2Item) {
            const time1 = new Date(scan1Item.time).getTime()
            const time2 = new Date(scan2Item.time).getTime()
            if (time1 > time2) {
              // Swap to ensure scan1 is older
              setScans(scan2, scan1)
            }
          }
          // If not both in history, keep them as-is - the compare API will handle it
        } else if (!isNavigation && h.length >= 2) {
          // Initial page load without URL params - auto-select most recent two
          setScans(h[1].id, h[0].id) // h[1] is older, h[0] is newer (sorted DESC)
        }
        // If navigating with no URL params and < 2 scans in history, leave empty
      })
      .catch(err => setError(err.message))
      .finally(() => setHistoryLoading(false))
  }, [uri])

  // Fetch comparison when both scans selected
  useEffect(() => {
    if (scan1 === '' || scan2 === '' || scan1 === scan2) {
      setResult(null)
      return
    }

    setLoading(true)
    setError(null)
    compareScans(uri, scan1 as number, scan2 as number)
      .then(setResult)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [uri, scan1, scan2])

  // The recursive frontier feeds the nested treemap; the table stays on the
  // depth-1 rows. The flat view stands alone without it, but its absence is
  // surfaced (legend spinner / retry) — never a silently-grey map.
  useEffect(() => {
    if (scan1 === '' || scan2 === '' || scan1 === scan2) {
      setRecResult(null)
      return
    }
    let stale = false
    let timer: ReturnType<typeof setTimeout> | undefined
    setRecResult(null)
    setRecState('loading')
    const s1 = scan1 as number, s2 = scan2 as number
    // While the server builds the pair's full diff index, the walk's answer
    // stands in; poll, then refetch silently (no spinner — the map just gets
    // complete) once it lands.
    const pollIndex = () => {
      timer = setTimeout(() => {
        fetchDiffIndexStatus(s1, s2)
          .then(st => {
            if (stale) return
            if (st.status === 'done') {
              compareScansRecursive(uri, s1, s2)
                .then(r => { if (!stale) setRecResult(r) })
                .catch(() => { /* keep the walk result */ })
            } else if (st.status === 'building' || st.status === 'none') {
              pollIndex()
            }
          })
          .catch(() => { if (!stale) pollIndex() })
      }, 3000)
    }
    compareScansRecursive(uri, s1, s2)
      .then(r => {
        if (stale) return
        setRecResult(r)
        setRecState('ready')
        if (r.index?.status === 'building' || r.index?.status === 'none') pollIndex()
      })
      .catch(() => { if (!stale) setRecState('error') })
    return () => { stale = true; if (timer) clearTimeout(timer) }
  }, [uri, scan1, scan2, recAttempt])

  // Get scan_path for selected scans (for breadcrumb coverage highlighting)
  const scan1Item = history.find(h => h.id === scan1)
  const scan2Item = history.find(h => h.id === scan2)
  const scan1Path = scan1Item?.scan_path ?? scan1Item?.path
  const scan2Path = scan2Item?.scan_path ?? scan2Item?.path

  // Scan progress tracking
  const scanProgress = useScanProgress()
  const [scanningPath, setScanningPath] = useState<string | null>(null)

  const handleStartScan = async (path: string) => {
    try {
      setScanningPath(path)
      await startScan(path)
      // Refresh history after scan completes (SSE will update progress)
    } catch (err) {
      console.error('Failed to start scan:', err)
      setScanningPath(null)
    }
  }

  // Check if there's an active scan for a path
  const isScanning = (path: string) => {
    return scanProgress.some(s => s.path === path && s.status === 'running')
  }

  // Get progress for a path
  const getProgress = (path: string) => {
    return scanProgress.find(s => s.path === path && s.status === 'running')
  }

  // Clear scanningPath when scan completes
  useEffect(() => {
    if (scanningPath && !isScanning(scanningPath)) {
      setScanningPath(null)
      // Refresh history and set new scan as "after"
      fetchScanHistory(uri).then(h => {
        setHistory(h)
        if (h.length >= 1) {
          // New scan becomes the "after" (scan2)
          // Keep current scan1 if it's valid, otherwise promote current scan2
          const currentScan1Valid = scan1 !== '' && h.some(item => item.id === scan1)
          if (currentScan1Valid) {
            setScans(scan1, h[0].id)
          } else if (scan2 !== '') {
            // Promote current scan2 to scan1 (shift the window)
            setScans(scan2, h[0].id)
          } else {
            // No prior selections, just set the new scan
            setScans('', h[0].id)
          }
        }
      })
    }
  }, [scanProgress, scanningPath, uri, scan1, scan2])

  return (
    <Box sx={{ p: 3, maxWidth: 1400, margin: '0 auto' }}>
      <Typography variant="h5" sx={{ mb: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <FaFolder color="#54aeff" />
        Compare Scans
      </Typography>
      <CompareBreadcrumbs
        uri={uri}
        routeType={routeType}
        scan1Path={scan1Path}
        scan2Path={scan2Path}
        scan1={scan1}
        scan2={scan2}
      />

      {/* Show comparison UI if we have URL params (even with 0-1 scans in local history,
          because the path may be newly added and the scans come from an ancestor) */}
      {historyLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      ) : history.length === 0 && (scan1 === '' || scan2 === '') ? (
        <Paper sx={{ p: 3, textAlign: 'center' }}>
          <Typography color="text.secondary" sx={{ mb: 2 }}>
            No scans found for this path.
          </Typography>
          <Button
            variant="outlined"
            onClick={() => handleStartScan(uri)}
            disabled={isScanning(uri)}
            startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
          >
            {isScanning(uri) ? 'Scanning...' : 'Scan Now'}
          </Button>
        </Paper>
      ) : history.length === 1 && (scan1 === '' || scan2 === '') ? (
        <Paper sx={{ p: 3 }}>
          <Typography color="text.secondary" sx={{ mb: 2 }}>
            Only one scan found for this path. This directory may have been added recently.
          </Typography>
          <Box sx={{ display: 'flex', gap: 3, alignItems: 'center', mb: 2 }}>
            <Box>
              <Typography variant="caption" color="text.secondary">Scanned</Typography>
              <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                {formatDateTime(history[0].time)} ({timeAgo(history[0].time)})
              </Typography>
            </Box>
            <Box>
              <Typography variant="caption" color="text.secondary">Size</Typography>
              <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                {formatSize(history[0].size)}
              </Typography>
            </Box>
            {history[0].n_desc != null && (
              <Box>
                <Typography variant="caption" color="text.secondary">Files</Typography>
                <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                  {formatCount(history[0].n_desc)}
                </Typography>
              </Box>
            )}
          </Box>
          <Button
            variant="outlined"
            onClick={() => handleStartScan(uri)}
            disabled={isScanning(uri)}
            startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
          >
            {isScanning(uri) ? 'Scanning...' : 'Rescan to Compare'}
          </Button>
        </Paper>
      ) : (
        <>
          {/* Show dropdowns only if history has enough scans; otherwise show simpler header */}
          {history.length >= 2 ? (
            <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center', flexWrap: 'wrap' }}>
              <FormControl sx={{ minWidth: 300 }}>
                <InputLabel>From (older)</InputLabel>
                <Select
                  value={scan1}
                  label="From (older)"
                  onChange={(e) => setScan1(e.target.value as number)}
                >
                  {history.map(h => (
                    <MenuItem key={h.id} value={h.id}>
                      {timeAgo(h.time)} — {formatSize(h.size)} — {formatDateTime(h.time)}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FaArrowRight color="#8b949e" />
              <FormControl sx={{ minWidth: 300 }}>
                <InputLabel>To (newer)</InputLabel>
                <Select
                  value={scan2}
                  label="To (newer)"
                  onChange={(e) => setScan2(e.target.value as number)}
                >
                  {history.map(h => (
                    <MenuItem key={h.id} value={h.id}>
                      {timeAgo(h.time)} — {formatSize(h.size)} — {formatDateTime(h.time)}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
                <Tooltip title="View directory tree">
                  <Button
                    component={Link}
                    to={`${uriToPath(uri)}${scan2 !== '' ? `?scan_id=${scan2}` : ''}`}
                    variant="outlined"
                    size="small"
                    startIcon={<FaList />}
                  >
                    Tree
                  </Button>
                </Tooltip>
                <Tooltip title={`Rescan ${uri}`}>
                  <span>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => handleStartScan(uri)}
                      disabled={isScanning(uri)}
                      startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
                    >
                      {isScanning(uri) ? (
                        getProgress(uri)?.items_found
                          ? `${getProgress(uri)!.items_found.toLocaleString()} items`
                          : 'Scanning...'
                      ) : 'Rescan'}
                    </Button>
                  </span>
                </Tooltip>
              </Box>
            </Box>
          ) : (
            /* History has < 2 scans but we have URL params - show simpler header
               (for newly added/removed directories where path doesn't have full history) */
            <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center', flexWrap: 'wrap' }}>
              {result ? (
                <>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                    Comparing scans from {formatDateTime(result.scan1.time)} → {formatDateTime(result.scan2.time)}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                    {/* No scans of this exact path: usually a subtree of a
                        broader scan (sizes on both sides), else it really
                        was added/removed. */}
                    {result.scan1.size != null && result.scan2.size != null
                      ? `(within scans of ${result.scan1.scan_path ?? 'an ancestor'})`
                      : result.scan2.size != null
                      ? '(this path was added between scans)'
                      : result.scan1.size != null
                      ? '(this path was removed between scans)'
                      : '(this path is absent from both scans)'}
                  </Typography>
                </>
              ) : loading ? null : (
                <Typography variant="body2" color="text.secondary">
                  Loading comparison...
                </Typography>
              )}
              <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
                <Tooltip title="View directory tree">
                  <Button
                    component={Link}
                    to={`${uriToPath(uri)}${scan2 !== '' ? `?scan_id=${scan2}` : ''}`}
                    variant="outlined"
                    size="small"
                    startIcon={<FaList />}
                  >
                    Tree
                  </Button>
                </Tooltip>
                <Tooltip title={`Rescan ${uri}`}>
                  <span>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => handleStartScan(uri)}
                      disabled={isScanning(uri)}
                      startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
                    >
                      {isScanning(uri) ? (
                        getProgress(uri)?.items_found
                          ? `${getProgress(uri)!.items_found.toLocaleString()} items`
                          : 'Scanning...'
                      ) : 'Rescan'}
                    </Button>
                  </span>
                </Tooltip>
              </Box>
            </Box>
          )}

          {error && (
            <Paper sx={{ p: 2, mb: 3, backgroundColor: 'rgba(248, 81, 73, 0.1)' }}>
              <Typography color="error">{error}</Typography>
            </Paper>
          )}

          {loading && (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
              <CircularProgress />
            </Box>
          )}

          {!loading && !result && history.length >= 2 && (scan1 === '' || scan2 === '') && (
            <Paper sx={{ p: 3, textAlign: 'center' }}>
              <Typography color="text.secondary">
                Select two scans to compare.
              </Typography>
            </Paper>
          )}

          {result && !loading && (
            <>
              <Summary result={result} />
              <CompareTreemap
                result={result}
                rec={recResult ?? undefined}
                recState={recState}
                onRecRetry={() => setRecAttempt(a => a + 1)}
                onDrill={childUri => {
                  // Preserve scan1/scan2 query params on drill so the sub-view
                  // shows the same snapshot pair.
                  const q = new URLSearchParams()
                  if (scan1 !== '') q.set('scan1', String(scan1))
                  if (scan2 !== '') q.set('scan2', String(scan2))
                  const suffix = q.toString() ? `?${q}` : ''
                  navigate(`/compare${uriToPath(childUri)}${suffix}`)
                }}
              />
              <Paper sx={{ overflow: 'auto' }}>
                <CompareTable
                  result={result}
                  onScan={handleStartScan}
                  isScanning={isScanning}
                  getProgress={getProgress}
                  scan1={scan1}
                  scan2={scan2}
                />
              </Paper>
            </>
          )}
        </>
      )}
    </Box>
  )
}
