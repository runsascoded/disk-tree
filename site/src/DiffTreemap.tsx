import { useMemo } from 'react'
import type { CSSProperties } from 'react'
import { Treemap as DtTreemap, divergingColor, divergingInk } from '@disk-tree/react'
import { stringParam, useUrlState } from 'use-prms'
import { useUnits } from './units'

const { abs, max, min, sign } = Math

// Ported from disk-tree's CompareView (ui/src/components/CompareView.tsx):
// `divergingColor` is red-positive, so negate on the way in.
const UNCHANGED_GREY = 'rgba(110, 118, 129, 0.28)'
const deltaColor = (t: number) => divergingColor(-t)

// `/api/diff` row (functions/_lib/view.ts `DiffRow`): p=path (relative to
// the diffed root) d=depth k=kind s=status a/b=bytes oa/ob=objects
// x=expanded l=read by a point lookup on side 1|2.
export interface DiffRow {
  p: string
  d: number
  k: string
  s: 'added' | 'removed' | 'changed' | 'unchanged'
  a: number
  b: number
  oa: number
  ob: number
  x?: boolean
  l?: 1 | 2
}

export interface DiffData {
  prev: string | null
  curr: string | null
  total_a: number
  total_b: number
  objects_a: number
  objects_b: number
  expansions: number
  truncated: boolean
  /** The shared byte floor both scans were read at. */
  threshold: number
  lookups: number
  lookups_capped: boolean
  rows: DiffRow[]
}

type AreaMode = 'max' | 'delta'

interface DiffNode {
  key: string
  label: string
  weight: number
  delta: number
  /** Bytes that arrived / left under this node (Σ over the frontier below
   * it): start − removed + added = end. A frontier row is all one or the other. */
  added: number
  removed: number
  status: DiffRow['s'] | 'filler' | 'root'
  size_old: number
  size_new: number
  n_desc_delta: number
  /** Object counts on each side (net only — the diff rows don't carry per-node adds/removes). */
  n_old: number
  n_new: number
  lookup?: 1 | 2
  children?: DiffNode[]
}

/**
 * Frontier rows → nested tree. Weights are bottom-up: a leaf is
 * `max(old, new)` (or `|Δ|` in Δ mode); a parent is `max(own, Σ children)` —
 * churn (delete X + add Y) makes children sum past either side's bytes.
 * Under-filled parents get a grey `(unchanged)` filler cell so areas stay
 * truthful without shipping every unchanged row.
 */
function buildTree(data: DiffData, areaMode: AreaMode): { cells: DiffNode[] } {
  const byPath = new Map<string, DiffNode>()
  const roots: DiffNode[] = []
  const attach = (node: DiffNode, path: string) => {
    byPath.set(path, node)
    const i = path.lastIndexOf('/')
    if (i < 0) {
      roots.push(node)
      return
    }
    const parentPath = path.slice(0, i)
    let parent = byPath.get(parentPath)
    if (!parent) {
      // Expanded-but-net-zero dir whose children were emitted without it.
      parent = {
        key: parentPath, label: parentPath.split('/').pop()!, weight: 0, delta: 0, added: 0, removed: 0,
        status: 'unchanged', size_old: 0, size_new: 0, n_desc_delta: 0, n_old: 0, n_new: 0, children: [],
      }
      attach(parent, parentPath)
    }
    ;(parent.children ??= []).push(node)
  }

  const rows = [...data.rows].sort((a, b) => a.d - b.d || a.p.localeCompare(b.p))
  for (const r of rows) {
    attach({
      key: r.p,
      label: r.p.split('/').pop() || r.p,
      weight: 0,
      delta: r.b - r.a,
      added: r.x ? 0 : max(0, r.b - r.a),
      removed: r.x ? 0 : max(0, r.a - r.b),
      status: r.s,
      size_old: r.a,
      size_new: r.b,
      n_desc_delta: r.ob - r.oa,
      n_old: r.oa,
      n_new: r.ob,
      lookup: r.l,
    }, r.p)
  }

  const finalize = (node: DiffNode): number => {
    const own = areaMode === 'max' ? max(node.size_old, node.size_new) : abs(node.delta)
    if (!node.children?.length) {
      node.weight = own
      return node.weight
    }
    let kidSum = 0
    for (const k of node.children) {
      kidSum += finalize(k)
      node.added += k.added
      node.removed += k.removed
    }
    node.weight = max(own, kidSum)
    const gap = node.weight - kidSum
    if (areaMode === 'max' && gap > max(1_000_000, node.weight * 0.002)) {
      node.children.push({
        key: `${node.key}/__unchanged__`, label: '(unchanged)', weight: gap, delta: 0, added: 0, removed: 0,
        status: 'filler', size_old: gap, size_new: gap, n_desc_delta: 0, n_old: 0, n_new: 0,
      })
    }
    node.children.sort((a, b) => (b.delta - a.delta) || (b.weight - a.weight))
    return node.weight
  }
  for (const r of roots) finalize(r)

  const cells = roots.filter(r => r.weight > 0)
  cells.sort(areaMode === 'max'
    ? (a, b) => (b.delta - a.delta) || (b.weight - a.weight)
    : (a, b) => b.weight - a.weight)
  return { cells }
}

export function DiffTreemap({ data, label }: { data: DiffData; label: string }) {
  const { fmtBytes } = useUnits()
  const fmtDelta = (d: number) => (d >= 0 ? '+' : '−') + fmtBytes(abs(d))
  // Area mode is shareable state: `?dm=max` switches to max(old,new) areas;
  // Δ (area = |delta|) is the default — the movement is what a diff view is
  // for — and stays out of the URL.
  const [dmP, setDmP] = useUrlState('dm', stringParam())
  const areaMode: AreaMode = dmP === 'max' ? 'max' : 'delta'
  const setAreaMode = (m: AreaMode) => setDmP(m === 'max' ? 'max' : undefined)
  // Root arithmetic for the crumb: start − removed + added = end. Bytes move
  // at the frontier (an expanded row's own Δ is carried by its children), so
  // the two terms are the tree's sums; a truncated walk leaves the smallest
  // movements unenumerated, in which case they're approximate — the
  // endpoints are always exact.
  const root = useMemo((): DiffNode => {
    const { cells } = buildTree(data, areaMode)
    return {
      key: label,
      label,
      weight: cells.reduce((s, c) => s + c.weight, 0),
      delta: data.total_b - data.total_a,
      added: cells.reduce((s, c) => s + c.added, 0),
      removed: cells.reduce((s, c) => s + c.removed, 0),
      status: 'root',
      size_old: data.total_a,
      size_new: data.total_b,
      n_desc_delta: data.objects_b - data.objects_a,
      n_old: data.objects_a,
      n_new: data.objects_b,
      children: cells,
    }
  }, [data, areaMode, label])
  const { added, removed } = root

  if (!root.children?.length) return null

  return (
    <div className="diff-tm">
      <DtTreemap<DiffNode>
        root={root}
        getSize={n => n.weight}
        getChildren={n => n.children}
        getLabel={n => n.label}
        getId={(_n, p) => p.map(x => x.key).join('|')}
        formatSize={n => fmtBytes(n)}
        tipMode="dock"
        // The core's default suffix is the node's *area weight* (Σ max(old,new),
        // or Σ|Δ|), which reads as a nonsense total next to the header's scan
        // size. Show the movement instead: start − removed + added = end.
        renderCrumbSuffix={n => n.status === 'root'
          ? <>
              — {fmtBytes(n.size_old)}{' '}
              <span className="shrank">− {fmtBytes(removed)}</span>{' '}
              <span className="grew">+ {fmtBytes(added)}</span>{' '}
              {data.truncated || added - removed !== n.delta ? '≈' : '='} {fmtBytes(n.size_new)}{' '}
              <span className={n.delta >= 0 ? 'grew' : 'shrank'}>({fmtDelta(n.delta)})</span>
            </>
          : <>— {fmtBytes(n.size_old)} → {fmtBytes(n.size_new)} <span className={n.delta >= 0 ? 'grew' : 'shrank'}>({fmtDelta(n.delta)})</span></>}
        collapseChains
        depthFade={1}
        rootFade={1}
        colorForCell={n => {
          if (areaMode === 'max') {
            if (n.children?.length) {
              const t = n.weight === 0 ? 0 : n.delta / n.weight
              return { bg: deltaColor(t), ink: divergingInk(t) }
            }
            const f = n.weight === 0 ? 0 : min(1, abs(n.delta) / n.weight)
            if (f === 0) return { bg: UNCHANGED_GREY, ink: divergingInk(0) }
            const pct = `${(f * 100).toFixed(2)}%`
            const band = deltaColor(sign(n.delta))
            return {
              bg: `linear-gradient(to top, ${band} ${pct}, ${UNCHANGED_GREY} ${pct})`,
              ink: divergingInk(f > 0.85 ? 1 : 0),
            }
          }
          // Δ mode: area already says how much moved; color says how much of
          // the node that was — a wholly added / removed directory is full
          // green / red however small, a 5% shrink is a faint red, a
          // net-zero churn is grey (its tooltip shows the churn).
          const base = max(n.size_old, n.size_new)
          const t = base === 0 ? 0 : n.delta / base
          return { bg: deltaColor(t), ink: divergingInk(t) }
        }}
        renderCellExtra={areaMode === 'max' ? (n, _path, { w, h }) => {
          if (n.children?.length || w < 56) return null
          const f = n.weight === 0 ? 0 : min(1, abs(n.delta) / n.weight)
          if (f === 0) return null
          const bandH = h * f
          const greyH = h - bandH
          const minBytes = min(n.size_old, n.size_new)
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
                fmtDelta(n.delta), { color: '#fff', fontWeight: 600 })}
              {f < 1 && greyH >= 44 && minBytes > 0 && lbl('0', greyH,
                fmtBytes(minBytes), { color: 'var(--ink-2)', opacity: 0.65 })}
            </>
          )
        } : undefined}
        renderTooltip={n => (
          <>
            <div style={{ fontWeight: 500 }}>{n.key}</div>
            <div style={{ opacity: 0.85, fontSize: '0.85em', fontVariantNumeric: 'tabular-nums' }}>
              {fmtBytes(n.size_old)}
              {n.removed > 0 && <> <span className="shrank">− {fmtBytes(n.removed)}</span></>}
              {n.added > 0 && <> <span className="grew">+ {fmtBytes(n.added)}</span></>}
              {' '}= {fmtBytes(n.size_new)}{' '}
              <span className={n.delta >= 0 ? 'grew' : 'shrank'}>({fmtDelta(n.delta)})</span>
            </div>
            {n.status !== 'filler' && (
              <div style={{ opacity: 0.75, fontSize: '0.85em', fontVariantNumeric: 'tabular-nums' }}>
                {n.n_old.toLocaleString('en-US')} → {n.n_new.toLocaleString('en-US')} objects{' '}
                <span className={n.n_desc_delta >= 0 ? 'grew' : 'shrank'}>({n.n_desc_delta > 0 ? '+' : ''}{n.n_desc_delta.toLocaleString('en-US')})</span>
              </div>
            )}
            <div style={{ opacity: 0.5, fontSize: '0.75em', marginTop: 2 }}>
              {n.status === 'filler' ? 'unchanged bytes the diff never needed to enumerate' : n.status}
              {n.lookup && ' · under the floor on one side, read exactly'}
            </div>
          </>
        )}
        renderLegend={() => (
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: '0.8rem', opacity: 0.85 }}>
            <span style={{ display: 'inline-block', width: 12, height: 12, background: deltaColor(1), borderRadius: 2 }} />
            grew
            <span style={{ display: 'inline-block', width: 12, height: 12, background: deltaColor(-1), borderRadius: 2 }} />
            shrank
            {areaMode === 'max' && <>
              <span style={{ display: 'inline-block', width: 12, height: 12, background: UNCHANGED_GREY, borderRadius: 2 }} />
              unchanged
            </>}
            <span style={{ opacity: 0.6, marginLeft: 4 }}>
              {areaMode === 'max' ? 'area = max(old, new), band = |Δ|' : 'area = |Δ| · color = Δ / size'}
            </span>
            <span style={{ display: 'inline-flex', gap: 2, marginLeft: 6 }}>
              {(['max', 'delta'] as const).map(m => (
                <button
                  key={m}
                  onClick={e => { e.stopPropagation(); setAreaMode(m) }}
                  className={'diff-mode' + (areaMode === m ? ' on' : '')}
                >
                  {m === 'max' ? 'max' : 'Δ'}
                </button>
              ))}
            </span>
          </span>
        )}
      />
    </div>
  )
}
