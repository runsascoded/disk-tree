// /staged — the opt-in deletion console (specs/staged-delete.md; the OA build
// plan `sweep-plan-union.md` checkpoint 4). Trash gestures on the map's table
// stage prefixes into one shared open plan; this page shows that plan — a
// treemap of everything staged (and of the selection), then each gesture's
// batch with who/when/memo, its items sized at a scan — lets a stager take
// their own back, and lets an admin dry-run or really dispatch it to the
// deployment's executor (`EXEC_API`, `Store.executor`: cw's plan-first Batch
// bridge, or gcs's sweep bridge). Non-admins see everything read-only.
// Nothing is deleted by inaction: no deadline, no auto-sweep.
import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { SiteNav } from './SiteNav'
import { SiteKbd } from './SiteKbd'
import { Tooltip } from './Tooltip'
import { Treemap } from './Treemap'
import { UserChip } from './UserChip'
import { PrefixTable, TimeCell } from './PrefixTable'
import { useUnits } from './units'
import { fmtN, type Meta, type TreeNode } from './types'
import { DEFAULT_STORE } from './stores'
import { buildUserIndex } from './colors'
import { useCanStage, useIdent } from './auth'
import { applyLedger } from './ledgerOverlay'
import { useOwnerIndex, useOwners } from './owners'
import { useRowSelection, useRowSelectionKeys } from './rowSelection'
import { useDispatch, useExecJobs, useRunAction, useStagedPlan, useUnstage, LIVE_STATES } from './plans'
import type { DeletionRun, ExecJob, StagedItem } from './plans'
import { type PrefixSortKey, type PrefixStat, relAgo, sortPrefixRows, usePrefixes } from './prefixes'
import { stagedTree } from './stagedTree'

const iso = (ts: number): string => new Date(ts * 1000).toISOString()
const store = DEFAULT_STORE

// `<scheme><bucket>/<path>/` → the treemap's URL path (below the store root).
const prefixToPath = (prefix: string): string => {
  const m = /^[a-z0-9]+:\/\/(.*?)\/?$/.exec(prefix)
  return m ? m[1] : prefix
}

/** Rows per batch page. */
const PAGE = 20

/** Whoami's admin flag: the plan-first console keys on it server-side too. */
function useIsAdmin(): boolean {
  const [admin, setAdmin] = useState(false)
  useEffect(() => {
    let live = true
    void fetch('/api/whoami', { credentials: 'include' }).then(r => r.ok ? r.json() : null).then((w: { admin?: boolean } | null) => { if (live) setAdmin(!!w?.admin) }).catch(() => {})
    return () => { live = false }
  }, [])
  return admin
}

type Row = StagedItem & { name: string; to: string; stat?: PrefixStat }
type Group = { id: number | null; batch?: { created_by: string; created_ts: number; note: string | null }; rows: Row[] }

export function StagedPage() {
  const { fmtBytes } = useUnits()
  const ident = useIdent()
  const admin = useIsAdmin()
  const canStage = useCanStage()
  // Plan-first executors (cw's Batch bridge) share the
  // `/api/plan-sweep/*` console routes; gcs's `sweep` has its own.
  const planFirst = store.executor !== 'sweep'

  const [live, setLive] = useState(false)
  const staged = useStagedPlan(live)
  const jobs = useExecJobs(live)
  const plan = staged.data?.plan ?? null
  const items = useMemo(() => staged.data?.items ?? [], [staged.data])
  const batches = useMemo(() => staged.data?.batches ?? [], [staged.data])
  const runs = staged.data?.runs ?? []
  const anyLive = runs.some(r => LIVE_STATES.has(jobs.data?.[r.run_id]?.state ?? '') || (!r.finished_ts && !jobs.data?.[r.run_id]))
  useEffect(() => setLive(anyLive), [anyLive])

  const unstage = useUnstage(plan?.id ?? null)
  const dispatch = useDispatch(plan?.id ?? null)
  const runAction = useRunAction()
  const busy = unstage.isPending || dispatch.isPending || runAction.isPending

  // The scan everything on the page is sized at — and the one a dispatch reads.
  const [scans, setScans] = useState<string[]>([])
  const [date, setDate] = useState('')
  useEffect(() => {
    void fetch(`${store.base}/scans.json`, { credentials: 'include' }).then(r => r.json()).then((s: string[]) => { setScans(s); setDate(s[0] ?? '') }).catch(() => {})
  }, [])
  const prefixes = useMemo(() => items.map(it => it.prefix), [items])
  const statsQ = usePrefixes(date, prefixes)
  const stats = statsQ.data
  const metaQ = useQuery<Meta>({
    queryKey: ['meta', store.key, date],
    queryFn: () => fetch(`${store.base}/${date}/meta.json`).then(r => r.json() as Promise<Meta>),
    enabled: !!date,
    staleTime: Infinity,
  })
  const userIdx = useMemo(() => buildUserIndex(metaQ.data?.users ?? []), [metaQ.data])
  const ownerIdx = useOwnerIndex(useOwners(!!store.owners).data)
  const error = unstage.error ?? dispatch.error ?? runAction.error ?? staged.error ?? statsQ.error

  const rows: Row[] = useMemo(() => items.map(it => ({ ...it, name: it.prefix, to: `/${prefixToPath(it.prefix)}`, stat: stats?.[it.prefix] })), [items, stats])
  const [sort, setSort] = useState<{ k: PrefixSortKey; asc: boolean }>({ k: 'b', asc: false })
  const onSort = (k: PrefixSortKey) => setSort(s => ({ k, asc: s.k === k ? !s.asc : k === 'name' }))

  // Items grouped by gesture (newest first); items staged before batches
  // existed fall into one "earlier" group. Each group sorts by the table's key.
  const groups: Group[] = useMemo(() => {
    const byBatch = new Map<number | null, Row[]>()
    for (const r of rows) {
      if (!byBatch.has(r.batch_id)) byBatch.set(r.batch_id, [])
      byBatch.get(r.batch_id)!.push(r)
    }
    const known = new Map(batches.map(b => [b.id, b]))
    return [...byBatch.entries()]
      .map(([id, rs]) => ({ id, batch: id != null ? known.get(id) : undefined, rows: sortPrefixRows(rs, sort.k, sort.asc, r => r.added_ts) }))
      .sort((a, b) => (b.batch?.created_ts ?? 0) - (a.batch?.created_ts ?? 0))
  }, [rows, batches, sort])

  // Collapsed batches and each batch's page.
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const [pages, setPages] = useState<Record<string, number>>({})
  const gkey = (g: Group) => String(g.id ?? 'none')
  const pageOf = (g: Group) => Math.min(pages[gkey(g)] ?? 0, Math.max(0, Math.ceil(g.rows.length / PAGE) - 1))
  // What's on screen, in order: the rows selection and j/k walk.
  const visible = useMemo(
    () => groups.flatMap(g => collapsed.has(gkey(g)) ? [] : g.rows.slice(pageOf(g) * PAGE, (pageOf(g) + 1) * PAGE)),
    [groups, collapsed, pages], // eslint-disable-line react-hooks/exhaustive-deps
  )
  const sel = useRowSelection(visible, r => r.prefix)
  useRowSelectionKeys(sel, 'staged', 'Staged')
  const selected = items.filter(it => sel.selected.has(it.prefix)).map(it => it.prefix)
  const mine = (it: StagedItem) => !!ident && it.added_by === ident.email
  const canRemove = (it: StagedItem) => admin || (canStage && mine(it))
  const removable = selected.filter(p => { const it = items.find(i => i.prefix === p); return it ? canRemove(it) : false })

  const [armed, setArmed] = useState(false)
  useEffect(() => setArmed(false), [plan?.id, items.length])

  const total = (rs: Row[]) => rs.reduce((t, r) => ({ b: t.b + (r.stat?.b ?? 0), o: t.o + (r.stat?.o ?? 0), gone: t.gone + (stats && !r.stat ? 1 : 0) }), { b: 0, o: 0, gone: 0 })
  const all = total(rows)
  const selRows = rows.filter(r => sel.selected.has(r.prefix))
  const selTotal = total(selRows)

  const overlay = (t: TreeNode) => (ownerIdx.count ? applyLedger(t, ownerIdx, store.scheme) : t)
  const tree = useMemo(() => (stats ? overlay(stagedTree(prefixes, stats, 'staged')) : null), [prefixes, stats, ownerIdx]) // eslint-disable-line react-hooks/exhaustive-deps
  const selKey = selected.join('\n')
  const selTree = useMemo(() => (stats && selected.length ? overlay(stagedTree(selected, stats, 'selected')) : null), [selKey, stats, ownerIdx]) // eslint-disable-line react-hooks/exhaustive-deps

  const sizesNote = !date ? null : statsQ.isLoading ? 'sizing…' : stats ? null : 'sizes unavailable'

  return (
    <main className="staged-page">
      <SiteNav />
      <div className="staged-head">
        <h1>Staged for deletion</h1>
        <span className="who">
          {ident ? <>{admin ? 'admin' : canStage ? 'stager' : 'viewer'} · <UserChip who={ident.email} size={20} /></> : 'not signed in'}
        </span>
      </div>
      <p className="sub">
        Nothing here is deleted until an admin dispatches it — staging is opt-in, with no deadline. Stage prefixes
        from the table under the map (the trash icon); a memo travels with each gesture.
        {admin ? ' Dry-run first to see what a real run would delete; a real run deletes recoverably.' : ' An admin reviews and dispatches from here.'}
      </p>
      {error && <p className="staged-err" role="alert">{error.message}</p>}

      {staged.isLoading ? <p className="loading">loading…</p> : !plan || !items.length ? (
        <p className="staged-empty">Nothing is staged.</p>
      ) : (
        <>
          <div className="pp-head">
            <h2>
              {items.length} {items.length === 1 ? 'prefix' : 'prefixes'}
              {stats && <> · {fmtBytes(all.b)} · {fmtN(all.o)} objects</>}
              <span className="dim"> · plan #{plan.id}{plan.name !== 'Staged' && <> “{plan.name}”</>} · open since {relAgo(plan.created_ts).replace(/ ago$/, '')}</span>
            </h2>
            <label className="scan-pick">sized at scan <select value={date} onChange={e => setDate(e.target.value)} aria-label="scan">{scans.map(s => <option key={s}>{s}</option>)}</select>
              {sizesNote && <span className="dim"> {sizesNote}</span>}
              {all.gone > 0 && <Tooltip content="Staged prefixes with nothing under them at this scan (already deleted, or never there)."><span className="dim"> · {all.gone} empty</span></Tooltip>}
            </label>
          </div>

          <div className={`staged-maps${selTree ? ' two' : ''}`}>
            <section className="staged-map">
              <h3>everything staged</h3>
              <div className="map-box">
                {tree ? <Treemap key={`all:${date}`} root={tree} mode="user" userIdx={userIdx} dateRange={null} scheme={store.scheme} /> : <p className="dim">{sizesNote ?? 'nothing to draw'}</p>}
              </div>
            </section>
            {selTree && (
              <section className="staged-map">
                <h3>selected · {selected.length} · {fmtBytes(selTotal.b)}</h3>
                <div className="map-box">
                  <Treemap key={`sel:${date}:${selKey}`} root={selTree} mode="user" userIdx={userIdx} dateRange={null} scheme={store.scheme} />
                </div>
              </section>
            )}
          </div>

          <div className="staged-actions">
            <label className="sel-all"><input type="checkbox" checked={sel.pageAll} onChange={sel.togglePage} aria-label="select all shown" /> {sel.selected.size ? `${sel.selected.size} selected · ${fmtBytes(selTotal.b)}` : 'select'}</label>
            {sel.selected.size > 0 && <button type="button" onClick={sel.clear}>deselect</button>}
            {removable.length > 0 && (
              <button type="button" disabled={busy} onClick={() => unstage.mutate(removable, { onSuccess: () => sel.clear() })}>unstage {removable.length}</button>
            )}
            <span className="fold-all">
              <button type="button" disabled={collapsed.size === 0} onClick={() => setCollapsed(new Set())} aria-label="expand all batches">▾ all</button>
              <button type="button" disabled={collapsed.size === groups.length} onClick={() => setCollapsed(new Set(groups.map(gkey)))} aria-label="collapse all batches">▸ all</button>
            </span>
          </div>

          {groups.map(g => {
            const k = gkey(g)
            const open = !collapsed.has(k)
            const t = total(g.rows)
            const pg = pageOf(g)
            const np = Math.max(1, Math.ceil(g.rows.length / PAGE))
            const setPg = (p: number) => setPages(ps => ({ ...ps, [k]: p }))
            const shown = g.rows.slice(pg * PAGE, (pg + 1) * PAGE)
            return (
              <section key={k} className={`stage-batch${open ? '' : ' folded'}`}>
                <div className="batch-head">
                  <button type="button" className="fold" aria-expanded={open} aria-label={open ? 'collapse batch' : 'expand batch'}
                    onClick={() => setCollapsed(c => { const n = new Set(c); if (open) n.add(k); else n.delete(k); return n })}>{open ? '▾' : '▸'}</button>
                  {g.batch
                    ? <><UserChip who={g.batch.created_by} size={18} /> staged <Tooltip content={iso(g.batch.created_ts)}><span>{relAgo(g.batch.created_ts)}</span></Tooltip></>
                    : <span className="dim">staged earlier</span>}
                  <span className="dim">· {g.rows.length} {g.rows.length === 1 ? 'prefix' : 'prefixes'}{stats && <> · {fmtBytes(t.b)}</>}</span>
                  {g.batch?.note && <i className="memo">{g.batch.note}</i>}
                </div>
                {open && (
                  <div className="staged-wrap">
                    <PrefixTable
                      rows={shown}
                      sort={sort}
                      onSort={onSort}
                      shareOf={all.b}
                      userIdx={userIdx}
                      ownerIdx={ownerIdx}
                      loading={!stats}
                      extra={[{
                        key: 'staged', label: 'staged', className: 'nb staged-by', sort: r => r.added_ts,
                        cell: r => <>{r.added_by !== g.batch?.created_by && <UserChip who={r.added_by} size={16} />}<TimeCell ts={r.added_ts} /></>,
                      }]}
                      lead={{
                        header: null,
                        cell: r => <input type="checkbox" checked={sel.selected.has(r.prefix)} onChange={() => { const i = visible.indexOf(r); sel.toggle(i); sel.commit() }} aria-label={`select ${r.prefix}`} />,
                      }}
                      rowProps={r => { const i = visible.indexOf(r); return { ref: sel.rowRef(i), ...sel.rowProps(i) } }}
                      trail={r => canRemove(r) && <button type="button" className="rm" title="unstage" disabled={busy} onClick={() => unstage.mutate([r.prefix])}>×</button>}
                    />
                    {np > 1 && (
                      <div className="pg">
                        <button type="button" disabled={pg === 0} onClick={() => setPg(0)} aria-label="first page">«</button>
                        <button type="button" disabled={pg === 0} onClick={() => setPg(pg - 1)} aria-label="previous page">‹</button>
                        <span>{pg * PAGE + 1}–{Math.min(g.rows.length, (pg + 1) * PAGE)} of {g.rows.length.toLocaleString('en-US')}</span>
                        <button type="button" disabled={pg >= np - 1} onClick={() => setPg(pg + 1)} aria-label="next page">›</button>
                        <button type="button" disabled={pg >= np - 1} onClick={() => setPg(np - 1)} aria-label="last page">»</button>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )
          })}

          {admin && (
            <div className="dispatch">
              <h3>Dispatch</h3>
              <label>scan <select value={date} onChange={e => setDate(e.target.value)}>{scans.map(s => <option key={s}>{s}</option>)}</select></label>
              <div className="dispatch-btns">
                <button type="button" className="dry" disabled={busy || !date} onClick={() => dispatch.mutate({ mode: 'dry', date })}>dispatch dry-run</button>
                {!armed
                  ? <button type="button" className="danger" disabled={busy || !date} onClick={() => setArmed(true)}>real delete…</button>
                  : <>
                      <button type="button" className="danger armed" disabled={busy} onClick={() => dispatch.mutate({ mode: 'real', date }, { onSuccess: () => setArmed(false) })}>confirm REAL delete of {items.length} {items.length === 1 ? 'prefix' : 'prefixes'}</button>
                      <button type="button" onClick={() => setArmed(false)}>cancel</button>
                    </>}
              </div>
              <p className="dispatch-note dim">A run reads the scan you pick and deletes only what it listed; new objects since are left alone.</p>
            </div>
          )}
        </>
      )}

      {plan && runs.length > 0 && (
        <>
          <h3>Runs ({runs.length})</h3>
          <div className="runs-wrap">
            <table className="runs">
              <thead><tr><th>run</th><th>mode</th><th>state</th><th>deleted</th><th>gone</th><th>started</th><th></th></tr></thead>
              <tbody>
                {runs.map(r => (
                  <RunRow key={r.run_id} r={r} job={jobs.data?.[r.run_id]} admin={admin} planFirst={planFirst} busy={busy} fmtBytes={fmtBytes}
                    act={(action, run_id) => runAction.mutate({ action, run_id })} />
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      <SiteKbd />
    </main>
  )
}

function RunRow({ r, job, admin, planFirst, busy, fmtBytes, act }: {
  r: DeletionRun; job: ExecJob | undefined; admin: boolean; planFirst: boolean; busy: boolean
  fmtBytes: (b: number) => string
  act: (action: 'stop' | 'undo' | 'purge', run_id: string) => void
}) {
  const state = job?.state ?? (r.finished_ts ? 'done' : 'submitted')
  const live = LIVE_STATES.has(state)
  const now = Math.floor(Date.now() / 1000)
  const canUndo = planFirst && admin && r.mode === 'real' && r.undo_state !== 'full' && (!r.undo_deadline || now < r.undo_deadline) && !!r.finished_ts
  const canPurge = planFirst && admin && r.mode === 'real' && r.purge_state === 'pending' && r.undo_state !== 'full' && (!r.undo_deadline || now >= r.undo_deadline)
  return (
    <tr className={`run ${r.mode} ${state.toLowerCase()}`}>
      <td className="rid"><code>{r.run_id.replace(/^(cw|gcs)-sweep-(dry|real)-/, '')}</code>{job?.logs && <> <a href={job.logs} target="_blank" rel="noreferrer" className="logs">logs</a></>}</td>
      <td>{r.mode}</td>
      <td>
        <span className="rstate">{state}</span>
        {state === 'FAILED' && job?.last_event && <details className="why"><summary>why</summary><div>{job.last_event}</div></details>}
        {r.undo_state === 'full' && <span className="tag">undone</span>}
        {r.purge_state === 'done' && <span className="tag">purged</span>}
      </td>
      <td>
        {fmtBytes(r.deleted_bytes)} <span className="dim">/ {fmtN(r.deleted_objects)}</span>
      </td>
      <td>{fmtN(r.skipped_gone)}</td>
      <td><TimeCell ts={r.started_ts} /></td>
      <td className="actions">
        {planFirst && admin && live && <button type="button" disabled={busy} onClick={() => act('stop', r.run_id)}>stop</button>}
        {canUndo && <button type="button" disabled={busy} onClick={() => act('undo', r.run_id)}>undo</button>}
        {canPurge && <button type="button" className="danger" disabled={busy} onClick={() => act('purge', r.run_id)}>purge</button>}
      </td>
    </tr>
  )
}
