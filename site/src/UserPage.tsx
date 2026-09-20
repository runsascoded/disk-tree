import { Treemap, type CellStyle } from '@disk-tree/react'
import { useQuery } from '@tanstack/react-query'
import { stringParam, useUrlState } from 'use-prms'
import { Fragment, useEffect, useMemo, useState } from 'react'
import { useActions } from 'use-kbd'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Avatar } from './Avatar'
import { ACTION_COLORS, fmtMarkDate } from './MarkControls'
import { ACTION_LABELS, useMarkIndex, useMarks, type Mark, type MarkAction } from './marks'
import { DEFAULT_STORE } from './stores'
import { type MarkState, type UserStates } from './sweep'
import { Treemap as MarkTreemap } from './Treemap'
import { ScanPicker } from './ScanPicker'
import { SiteNav } from './SiteNav'
import { useScan, useScans, type Scan } from './scan'
import { Skeleton } from './Busy'
import { SiteKbd } from './SiteKbd'
import { useMarkTotals } from './markTotals'
import { useDocTitle, SITE } from './title'
import { Tooltip } from './Tooltip'
import { UserChip, canonId, ghHandle, shortName, shortUserKey } from './UserChip'
import {
  CLASS_NAMES, CLASS_PRICE_US,
  ratePerByte, fmtBytesIec, fmtN, fmtUsd,
  type Meta, type TreeNode, type UserInfo,
} from './types'

// Per-user estate pages (the view Ahmed went looking for and couldn't find):
// `/users` ranks everyone by attributed bytes; `/user/:id` answers "of my
// N TiB, what's keep-marked, what's sweep-marked, and what's still undecided?"
// — the state rollup the map's per-prefix chips never total up.
//
// Everything here is folded server-side from the index tiers and the live
// ledger (`/api/estate`, `/api/marks/totals`, the user lens of
// `/api/subtree`): the same numbers the map's rollup and `/users` show, with
// no scan tree on the client (specs/view-serving.md §2).

interface StateRow {
  uri: string          // marked prefix (decided) or maximal clean subtree (unmarked)
  state: MarkState
  b: number            // this user's bytes governed by the row
  mark: Mark | null
}

// `keep_last_ckpt` decomposes into real keep/sweep proportions server-side
// wherever the step dirs are in view; only bytes under *unresolvable* KLC
// marks reach this fold, where they count as keep. Individual mark rows still
// show the first-class amber "keep last ckpt".
export type ShownState = 'keep' | 'sweep' | 'unmarked'
const SHOWN_STATES: ShownState[] = ['keep', 'sweep', 'unmarked']
const ALL_STATES: MarkState[] = ['keep', 'keep_last_ckpt', 'sweep', 'unmarked']
const STATE_ORDER_TOTAL = (f: Record<MarkState, number>): number => ALL_STATES.reduce((s, k) => s + f[k], 0)
const foldStates = (f: Record<MarkState, number>): Record<ShownState, number> => ({
  keep: f.keep + f.keep_last_ckpt,
  sweep: f.sweep,
  unmarked: f.unmarked,
})
type ClassMix = Record<string, number>
const addMix = (into: ClassMix, m: ClassMix): ClassMix => {
  for (const [c, b] of Object.entries(m)) into[c] = (into[c] ?? 0) + b
  return into
}
// Same fold for the storage-class mixes behind each state (KLC's kept bytes
// price as keep).
const foldMixes = (f: UserStates): Record<ShownState, ClassMix> => ({
  keep: addMix({ ...f.mix.keep }, f.mix.keep_last_ckpt),
  sweep: f.mix.sweep,
  unmarked: f.mix.unmarked,
})
// Every state's mix together = the user's whole (claims-applied) estate mix.
const wholeMix = (f: UserStates): ClassMix => ALL_STATES.reduce((m, k) => addMix(m, f.mix[k]), {} as ClassMix)
const stateLabel = (f: MarkState): string => (f === 'unmarked' ? 'unmarked' : ACTION_LABELS[f])
// `unmarked` gets the regular secondary ink, not the unattributed-gray — as
// the most common column value it has to be readable, not washed out.
const stateColor = (f: MarkState): string => (f === 'unmarked' ? 'var(--ink-2)' : f === 'sweep' ? 'var(--mk-del-ink)' : ACTION_COLORS[f])

// `gs://marin-<bucket>/<path>/` → the treemap's URL path.
const prefixToPath = (prefix: string): string => {
  const m = /^[a-z0-9]+:\/\/(.*?)\/?$/.exec(prefix)
  return m ? m[1] : prefix
}

const store = DEFAULT_STORE

// The live Google Sheet mirror of this table (created 2026-08-27; re-seed
// with: `dt-cloud report -a <actions.json> -o mark-status.csv` then
// `gws drive files update --params '{"fileId":"<id>","uploadType":"multipart"}'
//   --upload mark-status.csv --upload-content-type text/csv`).
const SHEET_URL = 'https://docs.google.com/spreadsheets/d/1k_11LA21g8uqMckPhkKvwrnRENVKF8yHxbW5NnUiRFc/edit'

function useLatestScan() {
  return useScans(store).data?.[0] ?? null
}

function useScanFile<T>(name: string, asof: string | null) {
  return useQuery<T>({
    queryKey: [name, store.key, asof],
    queryFn: () => fetch(`${store.base}/${asof}/${name}.json`).then(r => r.json() as Promise<T>),
    enabled: !!asof,
    staleTime: Infinity,
  })
}

// Est. $/mo with the storage-class mix behind it on hover.
function DollarCell({ b, mix, color }: { b: number; mix?: Record<string, number>; color?: string }) {
  if (!mix || !b) return <>—</>
  const rows = Object.entries(mix)
    .sort(([a], [c]) => Number(a) - Number(c))
    .map(([cls, cb]) => ({
      name: CLASS_NAMES[cls] ?? cls,
      b: cb,
      usd: (cb / 1024 ** 3) * (CLASS_PRICE_US[cls] ?? 0.02),
    }))
  return (
    <Tooltip content={
      <table className="class-tt">
        <tbody>
          {rows.map(r => (
            <tr key={r.name}>
              <td>{r.name}</td>
              <td className="num">{fmtBytesIec(r.b)}</td>
              <td className="num">{fmtUsd(r.usd)}/mo</td>
            </tr>
          ))}
        </tbody>
      </table>
    }>
      <span className="has-tt" style={color ? { color } : undefined}>{fmtUsd(ratePerByte(mix) * b)}</span>
    </Tooltip>
  )
}

// One-level treemap of the whole estate by owner: every user, plus one
// "unowned" pool for everything no person owns.
interface OwnerCell {
  n: string
  b: number
  pool?: boolean   // the unclaimed pool (no user)
  id?: string      // canonical user id → /user/:id
  c?: OwnerCell[]
}

// The pool tile: the unclaimed gray pulled toward dark, so the full-strength
// keep/sweep stripes on user tiles read as *marks on* a tile, not more of it.
const POOL_TILE_BG = 'color-mix(in oklab, var(--t-unattr) 48%, #131311)'
const USER_TILE_BG = 'color-mix(in oklab, var(--ink) 7%, var(--panel))'

/** The owner tiles (users + the unclaimed pool) — ONE derivation shared by
 * the map and its legend. Live states (claims applied) win over scan meta when
 * loaded. */
function ownerCells(meta: Meta, states: Map<string, Record<MarkState, number>> | null): OwnerCell[] {
  const metaUsers: UserInfo[] = meta.users ?? []
  const users: { u: string; b: number }[] = states
    ? [...states.entries()]
        .map(([u, f]) => ({ u, b: STATE_ORDER_TOTAL(f) }))
        .filter(x => x.b > 0)
    : metaUsers
  const cells: OwnerCell[] = users.map(u => ({ n: shortName(u.u), id: u.u, b: u.b }))
  const userSum = users.reduce((s, u) => s + u.b, 0)
  const unattr = Math.max(0, meta.total_bytes - userSum)
  if (unattr > 0) cells.push({ n: 'unowned', b: unattr, pool: true })
  return cells.sort((a, b) => b.b - a.b)
}

function MapLegend({ cells, states }: {
  cells: OwnerCell[]
  states: Map<string, Record<MarkState, number>> | null
}) {
  // Only the pool tile carries its own color — user tiles are state-striped.
  const hasPool = cells.some(c => c.pool)
  const present: Record<ShownState, boolean> = { keep: false, sweep: false, unmarked: false }
  if (states) {
    for (const f of states.values()) {
      const s = foldStates(f)
      for (const k of SHOWN_STATES) if (s[k] > 0) present[k] = true
    }
  }
  return (
    <div className="map-legend">
      {hasPool && <span><i style={{ background: POOL_TILE_BG }} />unowned</span>}
      {(!states || present.keep || present.sweep) && <span className="sep" />}
      {(!states || present.keep) && <span><i style={{ background: 'var(--mk-keep)' }} />keep</span>}
      {(!states || present.sweep) && <span><i style={{ background: 'var(--mk-del)' }} />sweep</span>}
      {(!states || present.unmarked) && <span><i style={{ background: 'var(--other)' }} />undecided</span>}
    </div>
  )
}

function UsersMap({ meta, states, redact = false }: {
  meta: Meta
  states: Map<string, Record<MarkState, number>> | null
  /** og:image mode — names + stripes only: no sizes, no tooltips, no drill. */
  redact?: boolean
}) {
  const navigate = useNavigate()
  const root = useMemo(
    (): OwnerCell => ({ n: '', b: meta.total_bytes, c: ownerCells(meta, states) }),
    [meta, states],
  )
  return (
    <div className="users-map">
      <Treemap<OwnerCell>
        root={root}
        getSize={n => n.b}
        getChildren={n => n.c}
        getLabel={n => n.n}
        formatSize={redact ? () => '' : n => fmtBytesIec(n)}
        chrome={false}
        fullscreen={false}
        colorForCell={(n): CellStyle | null => {
          // ONE categorical axis per tile: user tiles carry their state makeup
          // (neutral base + full-strength keep/sweep/undecided stripes); the
          // unclaimed pool, which has no state stripes, keeps its own color.
          if (!n.id) return n.pool ? { bg: POOL_TILE_BG } : null
          const style: CellStyle = { bg: USER_TILE_BG }
          const raw = states?.get(n.id)
          const f = raw ? foldStates(raw) : undefined
          if (f) {
            const total = SHOWN_STATES.reduce((s, k) => s + f[k], 0)
            if (total > 0) {
              const segs = SHOWN_STATES.filter(k => f[k] > 0)
                .map(k => ({
                  color: k === 'unmarked' ? 'var(--other)' : stateColor(k),
                  frac: f[k] / total,
                }))
              if (segs.length > 1) style.segments = segs
              else if (segs.length === 1) style.bg = segs[0].color
            }
          }
          return style
        }}
        renderTooltip={(n) => {
          if (redact) return null
          const raw = n.id ? states?.get(n.id) : undefined
          const f = raw ? foldStates(raw) : undefined
          const total = f ? SHOWN_STATES.reduce((s, k) => s + f[k], 0) : 0
          return (
            <div>
              <b>{n.n}</b>
              <div>{fmtBytesIec(n.b, true)} · {meta.total_bytes ? ((100 * n.b) / meta.total_bytes).toFixed(1) : 0}%</div>
              {f && total > 0 && (
                <div>
                  {SHOWN_STATES.filter(k => f[k] > 0).map(k => (
                    <span key={k} style={{ color: stateColor(k), marginRight: 8 }}>{stateLabel(k)} {fmtBytesIec(f[k])}</span>
                  ))}
                </div>
              )}
              {n.id && <div className="tt-hint">click for breakdown</div>}
            </div>
          )
        }}
        // Real anchors (this map is one flat level, so every tile qualifies):
        // Vimium hints, cmd-click, native pointer all work; plain clicks
        // still route through the SPA below.
        cellHref={n =>
          n.id ? `/user/${n.id}`
          : n.pool ? '/?o=unowned'
          : undefined}
        onCellClick={(n) => {
          if (redact) return true
          // Every tile goes somewhere sane: users to their page, the
          // unattributed pool to the matching home lens.
          if (n.id) navigate(`/user/${n.id}`)
          else if (n.pool) navigate('/?o=unowned')
          return true
        }}
      />
    </div>
  )
}

/** Per-user keep / sweep / undecided (claims applied) from
 * `/api/marks/totals`, keyed by canonical id. */
function useUserStates(asof: string | null): Map<string, UserStates> | null {
  const totalsQ = useMarkTotals(asof)
  return useMemo(
    () => (totalsQ.data ? new Map(Object.entries(totalsQ.data.users).map(([u, f]) => [canonId(u), f])) : null),
    [totalsQ.data],
  )
}

interface Estate {
  user: string
  date: string
  head: number
  states: UserStates | null
  marks: { prefix: string; keep: MarkAction; eff: MarkState; who: string | null; ts: number; bytes: number; b: number; authored: boolean; repainted_by?: string }[]
  claims: { prefix: string; ts: number; bytes: number; objects: number; repainted_by?: string }[]
  undecided: { prefix: string; b: number }[]
}

/** `/users/og` — fixed 1200×630 unfurl render of the owner map: names + state
 * stripes only (no sizes, no $, no tooltips). Screenshot via `pnpm shots`. */
export function UsersOgPage() {
  const asof = useLatestScan()
  const metaQ = useScanFile<Meta>('meta', asof)
  const states = useUserStates(asof)
  useEffect(() => {
    const prev = document.documentElement.dataset.theme
    document.documentElement.dataset.theme = 'dark'
    return () => {
      if (prev) document.documentElement.dataset.theme = prev
      else delete document.documentElement.dataset.theme
    }
  }, [])
  return (
    <div className="og og-users">
      <div className="og-head">
        <h1>{SITE} — users</h1>
        <p>Who owns what, and where every user’s bytes stand: keep / sweep / undecided.</p>
      </div>
      <div className="og-map">
        {metaQ.data && states && <UsersMap meta={metaQ.data} states={states} redact />}
      </div>
      {metaQ.data && <MapLegend cells={ownerCells(metaQ.data, states)} states={states} />}
    </div>
  )
}

export function UsersPage() {
  useDocTitle('Users')
  const scan = useScan(store)
  const asof = scan.asof
  const metaQ = useScanFile<Meta>('meta', asof)
  const mixes = metaQ.data?.user_class_bytes
  // Per-user keep / sweep / undecided from /api/marks/totals — the ledger
  // folded server-side against the floor-free index (claims applied), so the
  // table, the map's root rollup and the digest agree, and no tree.json.
  const states = useUserStates(asof)
  // ONE basis for every column: once the state walk has run, Attributed is
  // its claims-applied total (same numbers as the tiles, the state columns,
  // and the CSV) — a scan-only Attributed next to walk-based Keep let a
  // user's keep exceed their "attributed" (Percy caught Michael at 67>41:
  // his claims added 40 Ti the old column ignored). Scan meta is only the
  // pre-load fallback.
  const users = useMemo(() => {
    const metaUsers = metaQ.data?.users ?? []
    if (!states) return [...metaUsers].sort((a, b) => b.b - a.b)
    return [...states.entries()]
      .map(([u, f]) => ({ u, b: STATE_ORDER_TOTAL(f) }))
      .filter(x => x.b > 1e9)
      .sort((a, b) => b.b - a.b)
  }, [metaQ.data, states])
  // Client-side CSV of exactly what the table shows (claims applied).
  const downloadCsv = () => {
    const rowsIter = states
      ? [...states.entries()].map(([u, f]) => ({ u, b: STATE_ORDER_TOTAL(f), f: foldStates(f), m: foldMixes(f), mix: wholeMix(f) }))
      : users.map(u => ({ u: u.u, b: u.b, f: null as Record<ShownState, number> | null, m: null as Record<ShownState, ClassMix> | null, mix: mixes?.[u.u] }))
    const usd = (mix: ClassMix | undefined, b: number) => (mix && b ? Math.round(ratePerByte(mix) * b) : '')
    const tib = 1024 ** 4
    const lines = [
      ['user', 'attributed_TiB', 'est_usd_mo', 'keep_TiB', 'keep_usd_mo', 'sweep_TiB', 'sweep_usd_mo', 'undecided_TiB', 'undecided_usd_mo', 'undecided_pct', 'page'],
      ...rowsIter
        .filter(r => r.b > 1e9)
        .sort((a, b) => (b.f?.unmarked ?? b.b) - (a.f?.unmarked ?? a.b))
        .map(r => [
          r.u,
          (r.b / tib).toFixed(1),
          usd(r.mix, r.b),
          ((r.f?.keep ?? 0) / tib).toFixed(1),
          usd(r.m?.keep, r.f?.keep ?? 0),
          ((r.f?.sweep ?? 0) / tib).toFixed(1),
          usd(r.m?.sweep, r.f?.sweep ?? 0),
          ((r.f?.unmarked ?? r.b) / tib).toFixed(1),
          usd(r.m?.unmarked, r.f?.unmarked ?? 0),
          r.b ? Math.round((100 * (r.f?.unmarked ?? r.b)) / r.b) : 0,
          `https://gcs.oa.dev/user/${r.u}`,
        ]),
    ]
    const blob = new Blob([lines.map(l => l.join(',')).join('\n') + '\n'], { type: 'text/csv' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `marin-gcs-mark-status-${asof ?? 'latest'}.csv`
    a.click()
    URL.revokeObjectURL(a.href)
  }
  // Each state is a (bytes, est. $/mo) pair; the $ prices that state's own
  // storage-class mix from the walk (a cold sweep is cheap, a hot one isn't).
  useActions({
    'users:csv': { label: 'Download CSV (this table, assignments applied)', group: 'Users page', handler: downloadCsv },
    'users:sheet': { label: 'Google Sheet mirror ↗', group: 'Users page', handler: () => window.open(SHEET_URL, '_blank', 'noreferrer') },
  })
  const cell = (u: string, f: ShownState) => {
    const raw = states?.get(u)
    const b = raw ? foldStates(raw)[f] : 0
    const dim = { color: 'var(--ink-2)', opacity: 0.5 }
    return (
      <>
        <td className="num" style={b ? { color: stateColor(f) } : dim}>
          {states ? (b ? fmtBytesIec(b) : '—') : '…'}
        </td>
        <td className="num usd" style={b ? undefined : dim}>
          {states ? <DollarCell b={b} mix={raw ? foldMixes(raw)[f] : undefined} color={stateColor(f)} /> : '…'}
        </td>
      </>
    )
  }
  // Footer totals over exactly the rows shown (same claims-applied basis);
  // $ only sums users whose class mix is known, so it's a floor, flagged as such.
  const totals = useMemo(() => {
    const t = { b: 0, usd: 0, priced: 0, keep: 0, sweep: 0, unmarked: 0, mix: { keep: {} as ClassMix, sweep: {} as ClassMix, unmarked: {} as ClassMix } }
    for (const u of users) {
      t.b += u.b
      const raw = states?.get(u.u)
      const mix = raw ? wholeMix(raw) : mixes?.[u.u]
      if (mix) { t.usd += ratePerByte(mix) * u.b; t.priced++ }
      if (raw) {
        const f = foldStates(raw); t.keep += f.keep; t.sweep += f.sweep; t.unmarked += f.unmarked
        const m = foldMixes(raw)
        for (const k of SHOWN_STATES) addMix(t.mix[k], m[k])
      }
    }
    return t
  }, [users, mixes, states])
  const totalCell = (f: ShownState) => (
    <>
      <td className="num" style={{ color: stateColor(f) }}>{states ? fmtBytesIec(totals[f]) : '…'}</td>
      <td className="num usd">{states ? <DollarCell b={totals[f]} mix={totals.mix[f]} color={stateColor(f)} /> : '…'}</td>
    </>
  )
  return (
    <main className="marks-page user-page">
      <SiteNav><ScanPicker scan={scan} /></SiteNav>
      <header>
        <div className="hrow">
          <h1>Users</h1>
          <span style={{ display: 'inline-flex', gap: '1.2em', alignItems: 'baseline' }}>
            <button type="button" className="csv-btn" onClick={downloadCsv}>Download&nbsp;CSV</button>
            <a className="nav-files" href={SHEET_URL} target="_blank" rel="noreferrer">Google&nbsp;Sheet&nbsp;↗</a>
          </span>
        </div>
        <p className="sub">Everyone who owns storage{asof && <> in the {asof} scan</>}, largest first — and where their bytes stand (keep / sweep / no decision yet). Click a user (row or tile) for the per-prefix breakdown.</p>
      </header>
      {metaQ.isLoading && <Skeleton height={300} label="loading users…" />}
      {metaQ.data && (
        <>
          <UsersMap meta={metaQ.data} states={states} />
          <MapLegend cells={ownerCells(metaQ.data, states)} states={states} />
        </>
      )}
      {users.length > 0 && (
        <table className="worklist">
          <thead>
            <tr className="groups">
              <th />
              <th className="num" colSpan={2}>Owned</th>
              <th className="num" colSpan={2}>Keep</th>
              <th className="num" colSpan={2}>Sweep</th>
              <th className="num" colSpan={2}>Unmarked</th>
            </tr>
            <tr className="subs">
              <th>User</th>
              {['attributed', ...SHOWN_STATES].map(k => (
                <Fragment key={k}><th className="num">bytes</th><th className="num usd">est. $/mo</th></Fragment>
              ))}
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.u}>
                <td className="user-td">
                  <Link className="user-link" to={`/user/${u.u}`}>
                    <UserChip who={u.u} />
                  </Link>
                </td>
                <td className="num">{fmtBytesIec(u.b)}</td>
                <td className="num usd"><DollarCell b={u.b} mix={states?.get(u.u) ? wholeMix(states.get(u.u)!) : mixes?.[u.u]} /></td>
                {cell(u.u, 'keep')}
                {cell(u.u, 'sweep')}
                {cell(u.u, 'unmarked')}
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="total-row">
              <td>
                <Tooltip content="Sum of the rows above — bytes owned by some user (inferred from the scan, or assigned in the ledger). Unowned bytes (no owner: shared datasets, communal pools) are in no row, so this is less than the estate-wide keep / sweep rollup on the map.">
                  <span className="dotted">Total</span>
                </Tooltip>
                {' '}<span style={{ fontWeight: 400, opacity: 0.7 }}>· {users.length} users</span>
              </td>
              <td className="num">{fmtBytesIec(totals.b)}</td>
              <td className="num usd">
                {totals.priced
                  ? <Tooltip content={totals.priced < users.length ? `${totals.priced} of ${users.length} users have a storage-class mix; the rest are unpriced, so this is a floor` : 'sum of the per-user estimates'}>
                      <span className="has-tt">{totals.priced < users.length ? '≥ ' : ''}{fmtUsd(totals.usd)}</span>
                    </Tooltip>
                  : '—'}
              </td>
              {totalCell('keep')}
              {totalCell('sweep')}
              {totalCell('unmarked')}
            </tr>
          </tfoot>
        </table>
      )}
      <SiteKbd />
    </main>
  )
}

const PAGE = 25

function StateTable({ rows, empty }: { rows: StateRow[]; empty: string }) {
  const [page, setPage] = useState(0)
  if (!rows.length) return <p className="tab-note">{empty}</p>
  const pages = Math.ceil(rows.length / PAGE)
  const p = Math.min(page, pages - 1)
  const slice = rows.slice(p * PAGE, p * PAGE + PAGE)
  return (
    <>
      <table className="worklist marks-feed">
        <thead>
          <tr><th>Mark</th><th className="num">Your data</th><th>Prefix</th><th>By</th><th>When</th></tr>
        </thead>
        <tbody>
          {slice.map(r => (
            <tr key={`${r.state}:${r.uri}`}>
              <td>
                <span className="chip" style={{ borderColor: stateColor(r.state), color: stateColor(r.state) }}>
                  {stateLabel(r.state)}
                </span>
              </td>
              <td className="num">{fmtBytesIec(r.b)}</td>
              <td className="prefix">
                <Link to={`/${prefixToPath(r.uri)}`}>{r.uri}</Link>
                {r.mark?.note && <span className="memo" title={r.mark.note}> — {r.mark.note}</span>}
              </td>
              <td>{r.mark ? <UserChip who={r.mark.who} /> : ''}</td>
              <td>{r.mark ? fmtMarkDate(r.mark.ts) : ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {pages > 1 && (
        <div className="pager">
          <button type="button" disabled={p === 0} onClick={() => setPage(p - 1)}>← prev</button>
          <span className="pager-count">{p * PAGE + 1}–{p * PAGE + slice.length} of {rows.length}</span>
          <button type="button" disabled={p >= pages - 1} onClick={() => setPage(p + 1)}>next →</button>
        </div>
      )}
    </>
  )
}

/** `/user/:id/og` — fixed 1200×630 per-user unfurl card: avatar, name, group
 * glyph, and the keep / sweep / undecided proportions (percentages only —
 * no bytes, no $). Screenshot by `scripts/shoot-user-ogs.mjs`. */
export function UserOgPage() {
  const { id = '' } = useParams()
  const asof = useLatestScan()
  const states = useUserStates(asof)
  useEffect(() => {
    const prev = document.documentElement.dataset.theme
    document.documentElement.dataset.theme = 'dark'
    return () => {
      if (prev) document.documentElement.dataset.theme = prev
      else delete document.documentElement.dataset.theme
    }
  }, [])
  const raw = states?.get(id)
  const f = raw ? foldStates(raw) : null
  const total = f ? SHOWN_STATES.reduce((s, k) => s + f[k], 0) : 0
  const pct = (k: ShownState): number => (f && total ? (100 * f[k]) / total : 0)
  const barColor = (k: ShownState): string => (k === 'unmarked' ? 'var(--other)' : stateColor(k))
  const barLabel: Record<ShownState, string> = { keep: 'keep', sweep: 'sweep', unmarked: 'undecided' }
  return (
    <div className="og og-user">
      <div className="og-head ogu-head">
        <Avatar github={ghHandle(id)} name={shortName(id)} size={110} />
        <div>
          <h1>{shortName(id)}</h1>
          <p>{SITE} — where their bytes stand.</p>
        </div>
      </div>
      {f && total > 0 && (
        <>
          <div className="ogu-bar">
            {SHOWN_STATES.filter(k => f[k] > 0).map(k => (
              <div key={k} style={{ width: `${pct(k)}%`, background: barColor(k) }} />
            ))}
          </div>
          <div className="ogu-legend">
            {SHOWN_STATES.filter(k => f[k] > 0).map(k => (
              <span key={k}>
                <i style={{ background: barColor(k) }} />
                {barLabel[k]} <b>{Math.round(pct(k))}%</b>
              </span>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

export function UserPage() {
  const { id = '' } = useParams()
  useDocTitle(shortName(id))
  const scan = useScan(store)
  const asof = scan.asof
  const metaQ = useScanFile<Meta>('meta', asof)
  const marksQ = useMarks(true)
  const idx = useMarkIndex(marksQ.data)
  // The estate, folded server-side: states (claims applied), the marks that
  // govern their bytes, their claims, and their undecided subtrees.
  const estateQ = useQuery<Estate>({
    queryKey: ['estate', asof, id],
    enabled: !!asof && !!id,
    staleTime: 30_000,
    refetchInterval: 30_000,
    queryFn: async () => {
      const r = await fetch(`/api/estate?date=${asof}&user=${encodeURIComponent(id)}`, { credentials: 'include' })
      if (!r.ok) throw new Error(`estate: ${r.status}`)
      return r.json()
    },
  })
  const estate = estateQ.data ?? null
  const mine = estate?.states ?? null
  // Drill state lives in `?p=` so deep views are shareable and the back
  // button walks back out (same contract as the homepage map).
  const [pP, setPP] = useUrlState('p', stringParam())
  // The user's bytes as a drillable map (the homepage's user lens), colored by
  // mark state.
  const mapQ = useQuery<{ tree: TreeNode }>({
    queryKey: ['user-map', asof, id],
    enabled: !!asof && !!id,
    staleTime: Infinity,
    retry: (n: number, e: Error) => !/^4\d\d/.test(e.message) && n < 3,
    queryFn: async () => {
      const r = await fetch(`/api/subtree?date=${asof}&path=&w=1200&h=720&lens=user:${encodeURIComponent(id)}`, { credentials: 'include' })
      if (!r.ok) throw new Error(`${r.status}`)
      return r.json()
    },
  })
  const scopedTree = mapQ.data?.tree && mapQ.data.tree.b > 0 ? mapQ.data.tree : null

  // Decided rows: every mark whose band holds some of their bytes (under its
  // effective state); undecided rows: their outermost mark-free subtrees.
  const rows = useMemo((): StateRow[] => {
    if (!estate) return []
    // A mark repainted to unmarked (a newer clear above it) decides nothing —
    // its bytes are in the undecided rows.
    const decided: StateRow[] = estate.marks
      .filter(m => m.b > 0 && m.eff !== 'unmarked')
      .map(m => ({ uri: m.prefix, state: m.eff, b: m.b, mark: { prefix: m.prefix, action: m.keep, who: m.who ?? '', ts: m.ts, note: null } }))
    const undecided: StateRow[] = estate.undecided.map(u => ({ uri: u.prefix, state: 'unmarked', b: u.b, mark: null }))
    return [...decided, ...undecided].sort((a, b) => b.b - a.b)
  }, [estate])
  const totals = useMemo(() => {
    const t = new Map<ShownState, { b: number; n: number }>(SHOWN_STATES.map(f => [f, { b: 0, n: 0 }]))
    for (const r of rows) {
      const cur = t.get(r.state === 'keep_last_ckpt' ? 'keep' : r.state)!
      cur.b += r.b
      cur.n++
    }
    return t
  }, [rows])
  const attributed = mine ? STATE_ORDER_TOTAL(mine) : rows.reduce((s, r) => s + r.b, 0)
  const stripBytes = mine ? foldStates(mine) : null
  const metaB = metaQ.data?.users?.find(u => u.u === id)?.b
  const mix = metaQ.data?.user_class_bytes?.[id]
  const authored = estate ? estate.marks.filter(m => m.authored).length : 0
  // Fallback content for a user with ledger activity but no attributed bytes
  // yet (fresh identity, or claims the attribution pipeline hasn't mapped):
  // their own latest live marks, sized from the index (each prefix's total
  // bytes, whoever owns them).
  const authoredRows = useMemo((): StateRow[] => {
    if (rows.length > 0 || !estate) return []
    return estate.marks
      .filter(m => m.authored)
      .map(m => ({ uri: m.prefix, state: m.keep, b: m.bytes, mark: { prefix: m.prefix, action: m.keep, who: m.who ?? '', ts: m.ts, note: null } }))
      .sort((a, b) => b.b - a.b)
  }, [rows.length, estate])
  const claimedRows = useMemo((): StateRow[] => {
    if (!estate) return []
    return estate.claims
      .map(c => {
        const st = idx.resolve(c.prefix)
        return { uri: c.prefix, state: (st.mark?.action ?? 'unmarked') as MarkState, b: c.bytes, mark: st.mark }
      })
      .sort((a, b) => b.b - a.b)
  }, [estate, idx])
  const claimed = claimedRows.length
  const decidedRows = rows.filter(r => r.state !== 'unmarked')
  const undecidedRows = rows.filter(r => r.state === 'unmarked')
  // Resolve `?p=` against the scoped tree each render; a vanished segment
  // truncates to its deepest surviving ancestor.
  const mapPath = useMemo((): TreeNode[] | undefined => {
    if (!scopedTree) return undefined
    const path = [scopedTree]
    let cur: TreeNode = scopedTree
    for (const s of (pP ?? '').split('/').filter(Boolean)) {
      const next = cur.c?.find(c => c.n === s)
      if (!next) break
      path.push(next)
      cur = next
    }
    return path
  }, [scopedTree, pP])
  const loading = !asof || estateQ.isLoading || marksQ.isLoading

  return (
    <main className="marks-page user-page">
      <SiteNav><ScanPicker scan={scan} /></SiteNav>
      <header>
        <div className="hrow">
          <h1 className="user-head">
            <Avatar github={ghHandle(id)} name={shortName(id)} size={36} />
            {shortName(id)}
          </h1>
          <span style={{ display: 'inline-flex', gap: '1.2em' }}>
            <Link className="nav-files" to={`/?o=${shortUserKey(id)}`} style={{ fontSize: '0.9em' }}>Home,&nbsp;filtered&nbsp;to&nbsp;{shortName(id)}&nbsp;→</Link>
            <Link className="nav-files" to="/users" style={{ fontSize: '0.9em' }}>All&nbsp;users</Link>
          </span>
        </div>
        <p className="sub">
          {fmtBytesIec(attributed, true)} owned ({asof ?? '…'} scan + live assignments)
          {mix != null && metaB != null && <> · est. {fmtUsd(ratePerByte(mix) * metaB)}/mo</>}
          {authored > 0 && <> · {fmtN(authored)} prefixes marked by {shortName(id)}</>}.
          Where every byte stands, resolved the way the map does it (most recent mark on an ancestor-or-equal prefix wins).
        </p>
      </header>

      {loading && <Skeleton height={200} label="loading estate…" />}
      {marksQ.error && <p className="tab-note" style={{ color: 'var(--s3)' }}>Couldn’t load marks: {marksQ.error.message}</p>}
      {estateQ.error && <p className="tab-note" style={{ color: 'var(--s3)' }}>Couldn’t load the estate: {estateQ.error.message}</p>}
      {!loading && !rows.length && attributed === 0 && (
        <>
          <p className="tab-note">
            No data owned by or assigned to “{id}”
            {authoredRows.length > 0 ? <>{' '}— but their marks are below.</> : '.'}
          </p>
          {authoredRows.length > 0 && (
            <>
              <h2>Marked by {shortName(id)}</h2>
              <p className="tab-note">Their keep / sweep decisions (sizes are each prefix’s total bytes, whoever owns them).</p>
              <StateTable rows={authoredRows} empty="No marks yet." />
            </>
          )}
        </>
      )}

      {(rows.length > 0 || attributed > 0) && (
        <>
          <div className="state-strip">
            {SHOWN_STATES.map(f => {
              const { b: rowB, n } = totals.get(f)!
              const b = stripBytes ? stripBytes[f] : rowB
              if (!b) return null
              return (
                <div className="state-cell" key={f} style={{ borderColor: stateColor(f) }}>
                  <b style={{ color: stateColor(f) }}>{stateLabel(f)}</b>
                  <span className="state-b">{fmtBytesIec(b)}</span>
                  <span className="state-n">{n > 0 && <>{fmtN(n)} prefix{n === 1 ? '' : 'es'} · </>}{attributed ? Math.round((b / attributed) * 100) : 0}%</span>
                </div>
              )
            })}
          </div>

          {mapQ.isPending && !!asof && <Skeleton height={320} className="user-mini-map" label="loading map…" />}
          {scopedTree && scopedTree.b > 0 && (
            <div className="user-mini-map">
              <MarkTreemap
                root={scopedTree}
                mode="marks"
                userIdx={new Map()}
                dateRange={null}
                scheme={store.scheme}
                markIdx={idx}
                path={mapPath}
                onPathChange={pth => setPP(pth.slice(1).map(n => n.n).join('/') || undefined)}
              />
            </div>
          )}

          {rows.length > 0 && (
            <>
              <h2>Decided</h2>
              <p className="tab-note">Prefixes whose mark governs your bytes — yours and anyone else’s marks both count.</p>
              <StateTable rows={decidedRows} empty="Nothing marked yet." />

              <h2>Undecided</h2>
              <p className="tab-note">Your largest subtrees with no keep / sweep decision anywhere above or below — the review backlog.</p>
              <StateTable rows={undecidedRows} empty="Every owned byte has a decision. 🎉" />
            </>
          )}

          {claimedRows.length > 0 && (
            <>
              <h2>Assigned</h2>
              <p className="tab-note">Prefixes assigned in the ledger — counted in the totals above immediately; the scan pipeline formalizes the ownership on its next run.</p>
              <StateTable rows={claimedRows} empty="" />
            </>
          )}
        </>
      )}
      <SiteKbd />
    </main>
  )
}
