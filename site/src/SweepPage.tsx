import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { MdUndo } from 'react-icons/md'
import { useActions } from 'use-kbd'
import { useRowSelection, useRowSelectionKeys } from './rowSelection'
import { SiteNav, topbarH } from './SiteNav'

const SWEEP_SECTIONS = ['bands', 'dispatch', 'runs'] as const
import { Tooltip } from './Tooltip'
import { Avatar } from './Avatar'
import { MultiSelect } from './MultiSelect'
import { ghHandle, shortName, shortUserKey, UserChip } from './UserChip'
import { useUnits } from './units'
import { useHashSpy } from './hashSpy'
import { Busy, Skeleton } from './Busy'
import { useDocTitle } from './title'

// /sweep — the sweep console (specs/sweep-executor.md § Phase 2): review the
// candidate sweep-only bands with their ownership evidence, sign bands off
// (rows in `sweep_approvals`; admin scope — everyone else sees read-only),
// and follow executor runs (`deletion_runs`, written by `dt-cloud sweep
// execute`) through to their object-level logs in /files.

interface Candidate {
  prefix: string
  net_bytes: number
  net_objects: number
  sweepers: string[]
  top_user: string | null
  share: number | null
  owner_match: boolean
  /** Per-child sweeper-vs-attribution split (gross): the manifest's attr
   * gate only deletes the sweeper-attributed slice of an approved band. */
  attr_match_bytes?: number | null
  attr_other_bytes?: number | null
  attr_unattr_bytes?: number | null
}

/** What approving this band would let the executor delete (≈, gross-capped). */
const attrCap = (c: Candidate): number | null =>
  c.attr_match_bytes == null ? null : Math.min(c.attr_match_bytes, c.net_bytes)

const userMatch = (u: string | null | undefined, t: string): boolean =>
  !!u && (u.toLowerCase().includes(t) || shortName(u).toLowerCase().includes(t))

/** Filter-box query: whitespace-separated terms, all must match. A bare term
 * matches the path or any user on the row; `owner:`/`o:` and `sweeper:`/`s:`
 * (or `by:`) pin a user to one role; `is:approved` / `is:todo` / `is:unowned`
 * filter on state. */
const matchRow = (c: Candidate, approved: boolean, q: string): boolean =>
  q.toLowerCase().split(/\s+/).filter(Boolean).every(t => {
    const i = t.indexOf(':')
    const [k, v] = i > 0 ? [t.slice(0, i), t.slice(i + 1)] : ['', t]
    switch (k) {
      case 'o': case 'owner': return userMatch(c.top_user, v)
      case 's': case 'sweeper': case 'by': return c.sweepers.some(s => userMatch(s, v))
      case 'is': return v === 'approved' ? approved : v === 'todo' || v === 'unapproved' ? !approved : v === 'unowned' ? !c.top_user : true
      default: return c.prefix.toLowerCase().includes(t) || userMatch(c.top_user, t) || c.sweepers.some(s => userMatch(s, t))
    }
  })

interface DeletionRun {
  run_id: string
  plan: string
  scan: string
  mode: string
  actor: string
  started_ts: number
  finished_ts: number | null
  deleted_bytes: number
  deleted_objects: number
  skipped_gone: number
  skipped_overwritten: number
  drift_dirs: number
  ledger_drift_dirs: number
  undo_deadline: number | null
  undo_state: string
  log_dir: string
  /** Comma-separated `-b` cut; null = every bucket in the plan. */
  buckets?: string | null
}

/** A `gcs-sweep-*` Batch job as `/api/sweep/jobs` reports it (live state). */
interface SweepJob {
  job_id: string
  mode: 'dry' | 'real'
  state: string
  created: string
  updated: string | null
  run_secs: number | null
  by: string | null
  date: string | null
  buckets: string[]
  /** The Batch region it runs in (its bucket's, for a one-bucket cut). */
  region: string
  /** The bucket's own region, for a one-bucket cut (null: several buckets). */
  bucket_region: string | null
  plan: string
  last_event: string | null
  logs: string
}

const LIVE_STATES = new Set(['RUNNING', 'QUEUED', 'SCHEDULED'])
/** A runs-table row: the Batch job, its recorded D1 run, or both. */
interface RunRow { key: string; job?: SweepJob; run?: DeletionRun }

/** `marin-us-east5` → `us-east5`; a run's bucket cut, or "all" when it had none. */
const shortBuckets = (bs: readonly string[] | null | undefined): string =>
  bs && bs.length ? bs.map(b => b.replace(/^marin-/, '')).join(', ') : 'all'

const when = (ts: number | null) => (ts ? new Date(ts * 1000).toISOString().slice(0, 16).replace('T', ' ') : '—')
const fmtDur = (s: number): string => {
  const m = Math.floor(s / 60)
  return m < 60 ? `${m}m` : `${Math.floor(m / 60)}h ${String(m % 60).padStart(2, '0')}m`
}

/** Run `fn` over `xs` with at most `n` in flight; rejects on the first failure. */
const mapLimit = async <T,>(xs: T[], n: number, fn: (x: T) => Promise<void>): Promise<void> => {
  let i = 0
  const worker = async () => { while (i < xs.length) await fn(xs[i++]) }
  await Promise.all(Array.from({ length: Math.min(n, xs.length) }, worker))
}

type Status = 'approved' | 'todo'
const STATUSES: Status[] = ['approved', 'todo']

const PAGE_SIZES = [25, 50, 100, Infinity]
const fmtPageSize = (n: number) => (n === Infinity ? 'all' : String(n))
const clamp = (n: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, n))

const jfetch = async <T,>(url: string): Promise<T> => {
  const r = await fetch(url, { credentials: 'include' })
  if (!r.ok) throw new Error(`${url}: ${r.status}`)
  return r.json() as Promise<T>
}

/** The other-owner residue inside a band, fetched on demand when a row is
 * expanded: the users (besides the sweepers) whose data sits under the band,
 * and the unowned remainder — each a link to the homepage scoped to just their
 * paths here, plus one "inspect all" link to everything the sweepers don't own
 * (`?o=!<sweepers>`). This is what a *full* approval would additionally delete;
 * a slice approval defers it. */
function BandConflicts({ prefix, scan, sweepers }: { prefix: string; scan: string | undefined; sweepers: string[] }) {
  const { fmtBytes: tb } = useUnits()
  const path = prefix.replace('gs://', '').replace(/\/$/, '')
  const q = useQuery({
    queryKey: ['band-conflicts', scan, prefix],
    enabled: !!scan,
    staleTime: 60_000,
    queryFn: () => jfetch<{ tree: { b: number; us?: [string, number][] } }>(
      `/api/subtree?date=${scan}&path=${encodeURIComponent(path)}&w=600&h=360`),
  })
  if (q.isLoading) return <span className="dim">loading…</span>
  if (q.error || !q.data?.tree) return <span className="dim">—</span>
  const tree = q.data.tree
  const sw = new Set(sweepers)
  const others = (tree.us ?? []).filter(([u]) => !sw.has(u)).sort((a, b) => b[1] - a[1])
  const owned = (tree.us ?? []).reduce((s, [, b]) => s + b, 0)
  const unowned = Math.max(0, tree.b - owned)
  const bandPath = '/' + path
  const notQ = `?o=!${sweepers.map(s => encodeURIComponent(shortUserKey(s))).join(',')}`
  if (!others.length && unowned <= 0)
    return <span className="go-ink">clean — everything here is {sweepers.map(shortName).join(', ')}'s.</span>
  return (
    <div className="band-conflicts">
      <span className="dim">Not the sweeper's (deferred unless a <b>full</b> approval):</span>{' '}
      {others.map(([u, b]) => (
        <Link key={u} to={`${bandPath}?o=${encodeURIComponent(shortUserKey(u))}`} className="conflict-owner">
          <UserChip who={u} size={14} /> {tb(b)}
        </Link>
      ))}
      {unowned > 0 && <Link to={`${bandPath}?o=unowned`} className="conflict-owner dim">unowned {tb(unowned)}</Link>}
      <Link to={`${bandPath}${notQ}`} className="conflict-all">inspect all →</Link>
    </div>
  )
}

export function SweepPage() {
  useDocTitle('Sweep')
  const qc = useQueryClient()
  // Site-wide byte-unit preference (IEC TiB default; header toggle / `?si`) —
  // the treemap pages use the same formatter, so sizes agree across pages.
  const { fmtBytes: tb } = useUnits()
  const latestQ = useQuery({
    queryKey: ['sweep-latest'],
    queryFn: () => jfetch<{ plan: string }>('/v1/files/get?path=sweep/latest.json'),
    staleTime: 60_000,
  })
  const plan = latestQ.data?.plan
  const candsQ = useQuery({
    queryKey: ['sweep-candidates', plan],
    enabled: !!plan,
    queryFn: () => jfetch<{ plan: string; scan: string; head: number; bands: Candidate[] }>(`/v1/files/get?path=${encodeURIComponent(`sweep/${plan}/candidates.json`)}`),
    staleTime: 60_000,
  })
  const apprQ = useQuery({
    queryKey: ['sweep-approvals'],
    queryFn: () => jfetch<{ spec: { canWrite: boolean }; rows: { prefix: string; who: string; ts: number; mode?: string }[] }>('/api/db/sweep_approvals'),
  })
  const runsQ = useQuery({
    queryKey: ['deletion-runs'],
    queryFn: () => jfetch<{ rows: DeletionRun[] }>('/api/db/deletion_runs'),
    refetchInterval: 30_000,
  })
  // Live Batch state for every sweep job — visible from the moment it is
  // queued, hours before the executor writes its `deletion_runs` row.
  const jobsQ = useQuery({
    queryKey: ['sweep-jobs'],
    queryFn: () => jfetch<{ jobs: SweepJob[]; configured: boolean }>('/api/sweep/jobs'),
    refetchInterval: 30_000,
  })
  // What each dispatch planned: its manifest step's `plan-summary.json`,
  // summed over the buckets it was cut to (absent until that step ran).
  const jobList = jobsQ.data?.jobs ?? []
  const planQs = useQueries({
    queries: jobList.map(j => ({
      queryKey: ['sweep-plan-summary', j.job_id],
      staleTime: Infinity,
      retry: false,
      // Absent until the manifest step runs (minutes into a big bucket): keep
      // asking while the job is live; a finished job without one stays a dash.
      refetchInterval: (q: { state: { data?: unknown } }) => q.state.data || !/RUNNING|QUEUED|SCHEDULED/.test(j.state) ? false : 60_000,
      queryFn: () => jfetch<{ buckets: Record<string, { eligible?: { bytes: number; objects: number } }> }>(`/v1/files/get?path=${encodeURIComponent(`sweep/runs/${j.job_id}/plan-summary.json`)}`),
    })),
  })
  const planned = new Map<string, { bytes: number; objects: number }>()
  jobList.forEach((j, i) => {
    const b = planQs[i]?.data?.buckets
    if (!b) return
    const want = j.buckets.length ? j.buckets : Object.keys(b)
    const tot = { bytes: 0, objects: 0 }
    for (const k of want) { tot.bytes += b[k]?.eligible?.bytes ?? 0; tot.objects += b[k]?.eligible?.objects ?? 0 }
    planned.set(j.job_id, tot)
  })
  // Live progress: the executor's `progress/<bucket>.json` per bucket of a
  // running job, polled every 30 s; summed over the job's buckets.
  const liveJobs = jobList.filter(j => LIVE_STATES.has(j.state))
  const progTargets = liveJobs.flatMap(j => (j.buckets.length ? j.buckets : Object.keys(planQs[jobList.indexOf(j)]?.data?.buckets ?? {})).map(b => ({ job: j.job_id, bucket: b })))
  const progQs = useQueries({
    queries: progTargets.map(t => ({
      queryKey: ['sweep-progress', t.job, t.bucket],
      staleTime: 20_000,
      refetchInterval: 30_000,
      retry: false,
      queryFn: () => jfetch<{ roots: number; roots_done: number; decisions: Record<string, number>; delete_bytes: number; started: string; updated: string; done: boolean }>(`/v1/files/get?path=${encodeURIComponent(`sweep/runs/${t.job}/progress/${t.bucket}.json`)}`),
    })),
  })
  const progress = new Map<string, { deletes: number; gone: number; bytes: number; roots: number; roots_done: number; rate: number }>()
  progTargets.forEach((t, i) => {
    const d = progQs[i]?.data
    if (!d) return
    const cur = progress.get(t.job) ?? { deletes: 0, gone: 0, bytes: 0, roots: 0, roots_done: 0, rate: 0 }
    const secs = Math.max(1, (Date.parse(d.updated) - Date.parse(d.started)) / 1000)
    cur.deletes += d.decisions.delete ?? 0
    cur.gone += d.decisions.skipped_gone ?? 0
    cur.bytes += d.delete_bytes
    cur.roots += d.roots
    cur.roots_done += d.roots_done
    cur.rate = Math.round(cur.deletes / secs)
    progress.set(t.job, cur)
  })
  // One row per run: every Batch job (newest first), joined to its D1 run;
  // then the D1 runs no job accounts for (CLI runs, or older than the list).
  const runRows = runsQ.data?.rows ?? []
  const runList: RunRow[] = [
    ...jobList.map((j): RunRow => ({ key: j.job_id, job: j, run: runRows.find(r => r.log_dir.includes(j.job_id)) })),
    ...runRows.filter(r => !jobList.some(j => r.log_dir.includes(j.job_id))).map((r): RunRow => ({ key: r.run_id, run: r })),
  ].sort((a, b) => (b.run?.started_ts ?? Date.parse(b.job!.created) / 1000) - (a.run?.started_ts ?? Date.parse(a.job!.created) / 1000))
  const [runPage, setRunPage] = useState(0)
  const [runPageSize, setRunPageSize] = useState(PAGE_SIZES[0])
  const runPages = Math.max(1, Math.ceil(runList.length / runPageSize))
  const runPageRows = runList.slice(Math.min(runPage, runPages - 1) * runPageSize, (Math.min(runPage, runPages - 1) + 1) * runPageSize)
  const [stopped, setStopped] = useState<ReadonlySet<string>>(new Set())
  const stop = useMutation({
    mutationFn: async (job_id: string) => {
      const r = await fetch('/api/sweep/stop', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id }) })
      const j = await r.json() as { error?: string }
      if (!r.ok) throw new Error(j.error ?? `${r.status}`)
      return job_id
    },
    onSuccess: job_id => setStopped(s => new Set([...s, job_id])),
  })
  const canWrite = apprQ.data?.spec.canWrite ?? false
  // `#bands` / `#dispatch` / `#dispatches` / `#runs` (and a run row's own id):
  // a reload or a shared link lands where the reader was.
  useHashSpy({ ids: SWEEP_SECTIONS, hash: useLocation().hash, deps: [candsQ.data, jobsQ.data, runsQ.data], offset: topbarH })
  // Orientation text: open until the reader closes it once (per browser).
  const [introOpen, setIntroOpenRaw] = useState(() => { try { return localStorage.getItem('sweep-intro') !== 'closed' } catch { return true } })
  const setIntroOpen = (v: boolean) => { setIntroOpenRaw(v); try { localStorage.setItem('sweep-intro', v ? 'open' : 'closed') } catch { /* private mode */ } }
  const approvals = new Map((apprQ.data?.rows ?? []).map(r => [r.prefix, r]))

  // One row per approval on the wire (the /api/db route inserts one row per
  // POST); a bulk approve/revoke fans those out a few at a time and refetches
  // once. The first failure aborts the rest and surfaces as the error.
  const approve = useMutation({
    mutationFn: async ({ cs, mode }: { cs: Candidate[]; mode: 'slice' | 'full' }) => {
      await mapLimit(cs, 4, async c => {
        const r = await fetch('/api/db/sweep_approvals', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ values: {
            prefix: c.prefix,
            scan: candsQ.data!.scan,
            head: String(candsQ.data!.head),
            mode,
            note: `console: sweepers=${c.sweepers.join(',')} top=${c.top_user ?? '—'}${c.share != null ? ` ${(c.share * 100).toFixed(0)}%` : ''}`,
          } }),
        })
        if (!r.ok) throw new Error(`${c.prefix}: ${(await r.json() as { error?: string }).error ?? r.status}`)
      })
    },
    onSettled: () => qc.invalidateQueries({ queryKey: ['sweep-approvals'] }),
  })
  const revoke = useMutation({
    mutationFn: async (prefixes: string[]) => {
      await mapLimit(prefixes, 4, async prefix => {
        const r = await fetch(`/api/db/sweep_approvals?pk=${encodeURIComponent(prefix)}`, { method: 'DELETE', credentials: 'include' })
        if (!r.ok) throw new Error(`${prefix}: ${r.status}`)
      })
    },
    onSettled: () => qc.invalidateQueries({ queryKey: ['sweep-approvals'] }),
  })

  const bands = candsQ.data?.bands ?? []
  // Default view: only bands where an approve would actually delete something
  // (a non-zero sweeper-attributed slice, or already approved). Overly-broad
  // sweeps of other users' data are noise for the reviewer — folded away.
  const [showAll, setShowAll] = useState(false)
  const actionable = (c: Candidate) => approvals.has(c.prefix) || (attrCap(c) ?? 0) > 0
  const hidden = bands.filter(c => !actionable(c))
  const hiddenBytes = hidden.reduce((s, b) => s + b.net_bytes, 0)
  const shown = showAll ? bands : [...bands.filter(actionable)].sort((x, y) => (attrCap(y) ?? 0) - (attrCap(x) ?? 0))
  // ---- paging + selection ----
  // Selection is keyed by prefix (so it survives paging and the show-all
  // toggle); the cursor is a row index within the current page, as in the
  // use-kbd table demo. Click / j / k select a row; shift+click and shift+j/k
  // extend a range from the anchor; ⌘-click and the checkbox column add or
  // drop single rows; ⇧x toggles the page.
  const [q, setQ] = useState('')
  // Status axis: a multi-select over approved / todo; both (or neither) = all.
  const [stSel, setStSel] = useState<Status[]>(STATUSES)
  const st: 'all' | Status = stSel.length === 1 ? stSel[0] : 'all'
  const nApproved = shown.filter(c => approvals.has(c.prefix)).length
  const rows = shown.filter(c => {
    const a = approvals.has(c.prefix)
    return (st === 'all' || (st === 'approved') === a) && (!q.trim() || matchRow(c, a, q))
  })
  const [pageSize, setPageSize] = useState(PAGE_SIZES[0])
  const [pageRaw, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(rows.length / pageSize))
  const page = clamp(pageRaw, 0, pages - 1)
  const pageRows = rows.slice(page * pageSize, (page + 1) * pageSize)
  const sel = useRowSelection(pageRows, r => r.prefix)
  const { selected, toggle } = sel
  // Which bands are expanded to show their non-sweeper residue (on-demand fetch).
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const toggleExpand = (prefix: string) => setExpanded(prev => {
    const next = new Set(prev)
    next.has(prefix) ? next.delete(prefix) : next.add(prefix)
    return next
  })
  const cursorRow = sel.cursorRow as Candidate | undefined
  const selRows = bands.filter(b => selected.has(b.prefix))
  // A page turn or filter change swaps `pageRows`; the selection hook freezes
  // the active range into pins and drops the cursor by itself.
  useEffect(() => setPage(0), [pageSize, showAll, q, st])
  const clearSel = sel.clear
  const gotoPage = (p: number) => setPage(clamp(p, 0, pages - 1))

  // Bulk targets: the selection, else the cursor row. Approve skips bands
  // already approved; revoke skips the rest.
  const targets = selRows.length ? selRows : cursorRow ? [cursorRow] : []
  const toApprove = targets.filter(c => !approvals.has(c.prefix))
  const toRevoke = targets.filter(c => approvals.has(c.prefix))
  const busy = approve.isPending || revoke.isPending
  const selBytes = selRows.reduce((s, b) => s + b.net_bytes, 0)
  const selCap = selRows.reduce((s, b) => s + (attrCap(b) ?? 0), 0)

  const G = 'Sweep console'
  useRowSelectionKeys(sel, 'sweep', G)
  useActions({
    'sweep:next-page': { label: 'Next page', group: G, defaultBindings: [']'], handler: () => gotoPage(page + 1) },
    'sweep:prev-page': { label: 'Previous page', group: G, defaultBindings: ['['], handler: () => gotoPage(page - 1) },
    'sweep:approve': { label: 'Approve (slice) the selected bands', group: G, defaultBindings: ['a'], enabled: canWrite, handler: () => { if (toApprove.length && !busy) approve.mutate({ cs: toApprove, mode: 'slice' }) } },
    'sweep:revoke': { label: 'Revoke the selected approvals', group: G, defaultBindings: ['r'], enabled: canWrite, handler: () => { if (toRevoke.length && !busy) revoke.mutate(toRevoke.map(c => c.prefix)) } },
  })

  const approvedRows = bands.filter(b => approvals.has(b.prefix))
  const approvedBytes = approvedRows.reduce((s, b) => s + b.net_bytes, 0)
  // 'full'-mode approvals bypass the attr gate → count the whole band
  const approvedAttrBytes = approvedRows.reduce(
    (s, b) => s + (approvals.get(b.prefix)?.mode === 'full' ? b.net_bytes : attrCap(b) ?? b.net_bytes), 0)

  // A run can be cut to some of the approved bands' buckets (one bucket
  // first, then the rest): unchecked buckets are left out of the dispatch,
  // and `sweep manifest -b` / `sweep execute -b` plan and delete only the
  // checked ones. Default: every bucket with an approved band.
  const [bucketsOff, setBucketsOff] = useState<ReadonlySet<string>>(new Set())
  const bucketOf = (c: Candidate) => c.prefix.split('/')[2]
  const perBucket = new Map<string, { bands: number; bytes: number; attr: number; objects: number }>()
  for (const c of approvedRows) {
    const e = perBucket.get(bucketOf(c)) ?? { bands: 0, bytes: 0, attr: 0, objects: 0 }
    e.bands++
    e.bytes += c.net_bytes
    e.attr += approvals.get(c.prefix)?.mode === 'full' ? c.net_bytes : attrCap(c) ?? c.net_bytes
    e.objects += c.net_objects
    perBucket.set(bucketOf(c), e)
  }
  const allBuckets = [...perBucket.keys()].sort()
  const onBuckets = allBuckets.filter(b => !bucketsOff.has(b))
  const partial = onBuckets.length < allBuckets.length
  const runBytes = onBuckets.reduce((s, b) => s + perBucket.get(b)!.bytes, 0)
  const runAttrBytes = onBuckets.reduce((s, b) => s + perBucket.get(b)!.attr, 0)

  const [armed, setArmed] = useState(false)
  const dispatch = useMutation({
    mutationFn: async (mode: 'dry' | 'real') => {
      const r = await fetch('/api/sweep/dispatch', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode, date: candsQ.data!.scan, ...(partial ? { buckets: onBuckets } : {}) }),
      })
      // Read the body as text first: a Function that throws (or a Cloudflare
      // error page) answers with HTML, and the status + a snippet of it is
      // the whole clue — `r.json()` would only say "not valid JSON".
      const text = await r.text()
      let j: { error?: string; job_id?: string; plan?: string; mode?: string; detail?: unknown } | null = null
      try { j = JSON.parse(text) } catch { /* non-JSON body: reported below */ }
      if (!r.ok || !j) {
        console.error('dispatch failed', r.status, text)
        const snippet = j ? (j.error ?? '') + (j.detail ? ` — ${JSON.stringify(j.detail).slice(0, 300)}` : '') : text.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 300)
        throw new Error(`HTTP ${r.status}${snippet ? `: ${snippet}` : ''}`)
      }
      return j as { job_id: string; plan: string; mode: string }
    },
    onSuccess: () => setArmed(false),
  })

  return (
    <main className="sweep-page">
      <SiteNav />
      <h1>Sweep console</h1>
      {/* First-visit orientation: open until dismissed once, then a one-line
          summary. The live plan/approved numbers are their own line below. */}
      <details className="intro" open={introOpen} onToggle={e => setIntroOpen((e.target as HTMLDetailsElement).open)}>
        <summary>How approvals work</summary>
        <p className="sub">
          Candidate bands are <b>sweep-only under the vote model</b> (no keep votes anywhere). Two ways to sign one off:{' '}
          <b>approve</b> (slice) deletes only the <i>sweeper's own data</i> — other users' data caught in a broad sweep is
          deferred to their own votes (expand a band to see whose); <b>all</b> signs off the entire band regardless of
          ownership (for bands verified out-of-band). Approved bands feed <code>sweep manifest --approved-from-site</code>;
          runs land below with their logs.
        </p>
      </details>
      {plan && (
        <p className="plan-line">
          Plan <b>{plan}</b>{candsQ.data && <> · head {candsQ.data.head}</>}
          {candsQ.data && apprQ.data && <> · approved <b>{tb(approvedBytes)}</b>
            {approvedAttrBytes !== approvedBytes && <> (≈<b>{tb(approvedAttrBytes)}</b> after the ownership gate)</>}</>}
        </p>
      )}

      {latestQ.isError && (
        // Only a 404 means there's no plan; anything else is the backend.
        /: 404$/.test(String(latestQ.error))
          ? <p className="err">No plan baked yet — run <code>dt-cloud sweep plan -C</code>.</p>
          : <p className="err">Error loading the latest plan: {String(latestQ.error)}</p>
      )}
      {!candsQ.data && !latestQ.isError && !candsQ.isError && <Skeleton height={420} label="loading plan…" />}
      {candsQ.isError && <p className="err">Error loading the plan's candidates: {String(candsQ.error)}</p>}
      {candsQ.data && (<>
        <div className="sweep-tools" id="bands">
          <span className="tb-axis nb">
            <span className="lbl">status</span>
            <MultiSelect<Status>
              label="approval status"
              options={[
                { key: 'approved', label: `approved ${nApproved}`, glyph: '✓', color: '#3fb950', tip: 'Bands somebody has signed off (slice or full) — what a dispatch would plan.' },
                { key: 'todo', label: `todo ${shown.length - nApproved}`, glyph: '○', color: 'var(--ink-2)', tip: 'Bands still waiting for a decision.' },
              ]}
              selected={stSel}
              onChange={keys => setStSel(keys.length === 0 ? STATUSES : keys)}
            />
          </span>
          <input className="filter" type="search" value={q} onChange={e => setQ(e.target.value)}
                 placeholder="filter: path · user · owner:x · sweeper:x · is:unowned" />
          {(q.trim() || st !== 'all') && <span className="dim nb">{rows.length} of {shown.length} match</span>}
          {selected.size > 0 && (
            <span className="sel-bar">
              <b>{selRows.length}</b> selected · ≈<b>{tb(selCap)}</b> deletable{selCap !== selBytes && <span className="dim"> of {tb(selBytes)}</span>}
              {canWrite && (
                <>
                  <Tooltip content={<>Approve the <b>sweeper's slice</b> of every selected band not yet approved ({toApprove.length}). Other users' and unowned data in them is deferred, never deleted by this.</>}>
                    <button className="mini go" disabled={!toApprove.length || busy} onClick={() => approve.mutate({ cs: toApprove, mode: 'slice' })}>approve ×{toApprove.length}</button>
                  </Tooltip>
                  <Tooltip content={<>Approve the <b>ENTIRE band</b> — including other users' and unowned data — for every selected band not yet approved ({toApprove.length}). Skips the ownership gate.</>}>
                    <button className="mini warn" disabled={!toApprove.length || busy} onClick={() => approve.mutate({ cs: toApprove, mode: 'full' })}>all ×{toApprove.length}</button>
                  </Tooltip>
                  <button className="mini" disabled={!toRevoke.length || busy} onClick={() => revoke.mutate(toRevoke.map(c => c.prefix))}>revoke ×{toRevoke.length}</button>
                </>
              )}
              <button className="mini" onClick={clearSel}>clear</button>
            </span>
          )}
        </div>
        <div className="table-scroll"><table className="sweep-table">
          <thead>
            <tr>
              <th className="col-sel"><input type="checkbox" title="select / deselect this page (⇧x)" checked={sel.pageAll} onChange={sel.togglePage} /></th>
              <th>band</th>
              <th className="num">
                <Tooltip content={<>The band's net size. The <span className="warn-ink">yellow ✓</span> approves the <b>whole band</b> for deletion — other users' and unowned data included (skips the ownership gate).</>}>
                  <span className="hashelp">size</span>
                </Tooltip>
              </th>
              <th className="num">
                <Tooltip content={<>What an <b>approve slice</b> would delete: the <i>sweeper's own data</i> under the band (other users' and unowned data is deferred — expand a band to see whose). A gross estimate capped at the band size; the dry run gives exact numbers. The <span className="go-ink">green ✓</span> approves this slice.</>}>
                  <span className="hashelp">≈ deletable</span>
                </Tooltip>
              </th>
              <th className="num col-objects">objects</th><th>swept by</th><th>top owner</th><th>status</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.map((c, i) => {
              const a = approvals.get(c.prefix)
              const cap = attrCap(c)
              // Land on the band scoped to its sweeper's attributed slice —
              // the data an approval would actually delete — colored by read
              // recency (staleness is the case for deletion).
              const drill = '/' + c.prefix.replace(/^gs:\/\//, '').replace(/\/$/, '')
                + '?c=read' + (c.sweepers.length === 1 ? `&o=${encodeURIComponent(c.sweepers[0])}` : '')
              const rp = sel.rowProps(i)
              const cls = [a ? 'approved' : c.owner_match ? 'matched' : '', rp.className].filter(Boolean).join(' ')
              return (
                <Fragment key={c.prefix}>
                <tr ref={sel.rowRef(i)} {...rp} className={cls}>
                  <td className="col-sel"><input type="checkbox" checked={selected.has(c.prefix)} onChange={() => toggle(i)} /></td>
                  <td>
                    <button type="button" className={`caret${expanded.has(c.prefix) ? ' open' : ''}`} aria-expanded={expanded.has(c.prefix)}
                      title="show the non-sweeper data inside this band" onClick={e => { e.stopPropagation(); toggleExpand(c.prefix) }}>▸</button>
                    <Link to={drill}><code>{c.prefix.replace('gs://', '')}</code></Link>
                  </td>
                  <td className="num">
                    <span className="nb">
                      {tb(c.net_bytes)}
                      {canWrite && !a && (
                        <Tooltip content={<>Approve the <b>ENTIRE band</b> — all {tb(c.net_bytes)}, including data owned by other users or unowned. Skips the ownership gate. Use only after confirming out-of-band (e.g. with the affected users) that everything under this prefix can go.</>}>
                          <button className="mini ico warn" disabled={busy} onClick={() => approve.mutate({ cs: [c], mode: 'full' })}>✓</button>
                        </Tooltip>
                      )}
                    </span>
                  </td>
                  <td className="num" title={c.attr_other_bytes ? `${tb(c.attr_other_bytes)} owned by other users + ${tb(c.attr_unattr_bytes ?? 0)} unowned/mixed are deferred, not deleted` : undefined}>
                    <span className="nb">
                      {cap == null ? <span className="dim">—</span> : tb(cap)}
                      {canWrite && !a && (
                        <Tooltip content={<>Approve <b>{c.sweepers.map(shortName).join(', ')}</b>'s slice{cap != null && <> — ≈<b>{tb(cap)}</b> of {tb(c.net_bytes)}</>}. Everyone else's data is deferred. Click the band to see exactly what's inside.</>}>
                          <button className="mini ico go" disabled={busy} onClick={() => approve.mutate({ cs: [c], mode: 'slice' })}>✓</button>
                        </Tooltip>
                      )}
                    </span>
                  </td>
                  <td className="num col-objects"><span className="nb">{c.net_objects.toLocaleString()}</span></td>
                  <td>{c.sweepers.map(s => <UserChip key={s} who={s} size={16} />)}</td>
                  <td>
                    {c.owner_match ? (
                      // The sweeper owns the band: one chip (in "swept by") is enough.
                      <Tooltip content={<><b>{c.sweepers.map(shortName).join(', ')}</b> is the top owner here. Slice deletes ≈<b>{cap != null ? tb(cap) : tb(c.net_bytes)}</b> of {tb(c.net_bytes)}; the rest is deferred. Click the band to inspect what's inside.</>}>
                        <span className="match-tag nb">= sweeper{c.share != null && c.share < 0.995 && <> · {(c.share * 100).toFixed(0)}%</>}</span>
                      </Tooltip>
                    ) : c.top_user ? (
                      <Tooltip content={<>Top owner here is <b>{shortName(c.top_user)}</b>{c.share != null && <> ({(c.share * 100).toFixed(0)}% of the band)</>}, <b>not</b> the sweeper — so slice deletes little or none{cap != null && cap > 0 && <> (≈{tb(cap)})</>}. Deleting the rest needs a <b>full</b> approval (everyone's data). Click the band to inspect.</>}>
                        <span className="nb">
                          <UserChip who={c.top_user} size={16} />
                          {c.share != null && <span className="pct"> {(c.share * 100).toFixed(0)}%</span>}
                        </span>
                      </Tooltip>
                    ) : (
                      <Tooltip content={<>No attributed owner for this band's bytes — nothing is slice-deletable; it would only go under a <b>full</b> approval.</>}>
                        <span className="dim">unowned</span>
                      </Tooltip>
                    )}
                  </td>
                  <td>
                    {a ? (
                      <>
                        {a.mode === 'full'
                          ? <Tooltip content={<>Approved · <b>FULL</b> band: the ENTIRE band is deletable — including data owned by other users or unowned. The ownership gate is skipped for this band.</>}>
                              <span className="appr-ico warn" aria-label="Approved — full band">✓</span>
                            </Tooltip>
                          : <Tooltip content={<>Approved · <b>slice</b>: deletes {c.sweepers.map(shortName).join(', ')}'s data{cap != null && <> (≈{tb(cap)} of {tb(c.net_bytes)})</>}; everyone else's stays. Expand the band to see whose.</>}>
                              <span className="appr-ico go" aria-label="Approved — slice">✓</span>
                            </Tooltip>}
                        {' '}<Tooltip content={<>approved by <b>{shortName(a.who)}</b> · {when(a.ts)} UTC · {a.mode === 'full' ? 'full band' : 'slice'}</>}>
                          <span className="approver"><Avatar github={ghHandle(a.who)} name={shortName(a.who)} size={16} /></span>
                        </Tooltip>
                        {canWrite && (
                          <Tooltip content={<>Revoke this approval — the band goes back to the todo backlog.</>}>
                            <button className="mini ico danger revoke" disabled={busy} aria-label="Revoke approval" onClick={() => revoke.mutate([c.prefix])}><MdUndo /></button>
                          </Tooltip>
                        )}
                      </>
                    ) : (
                      <span className="dim">—</span>
                    )}
                  </td>
                </tr>
                {expanded.has(c.prefix) && (
                  <tr className="band-detail">
                    <td />
                    <td colSpan={7}><BandConflicts prefix={c.prefix} scan={candsQ.data?.scan} sweepers={c.sweepers} /></td>
                  </tr>
                )}
                </Fragment>
              )
            })}
          </tbody>
        </table></div>
        <div className="pager">
          <span className="dim nb">{rows.length ? `${page * pageSize + 1}–${Math.min(rows.length, (page + 1) * pageSize)} of ${rows.length}` : 'no bands match'}</span>
          {pages > 1 && (
            <span className="nb">
              <button className="mini" disabled={page === 0} onClick={() => gotoPage(page - 1)}>‹</button>
              {Array.from({ length: pages }, (_, p) => <button key={p} className={`mini${p === page ? ' on' : ''}`} onClick={() => gotoPage(p)}>{p + 1}</button>)}
              <button className="mini" disabled={page === pages - 1} onClick={() => gotoPage(page + 1)}>›</button>
            </span>
          )}
          <span className="nb"><span className="dim">per page</span>{PAGE_SIZES.map(n => <button key={n} className={`mini${n === pageSize ? ' on' : ''}`} onClick={() => setPageSize(n)}>{fmtPageSize(n)}</button>)}</span>
          <span className="dim kbd-hint"><kbd>j</kbd>/<kbd>k</kbd> select · <kbd>⇧j</kbd>/<kbd>⇧k</kbd> range · <kbd>⌘</kbd>-click add · <kbd>⇧x</kbd> page{canWrite && <> · <kbd>a</kbd> approve · <kbd>r</kbd> revoke</>} · <kbd>[</kbd>/<kbd>]</kbd> prev/next</span>
        </div>
      </>)}
      {candsQ.data && hidden.length > 0 && (
        <p className="dim table-fold">
          {showAll
            ? <>showing all {bands.length} bands (incl. {hidden.length} with nothing slice-deletable) </>
            : <>{hidden.length} bands hidden — nothing slice-deletable (≈{tb(hiddenBytes)}, mostly other users' data under overly-broad sweeps; deferred to their own votes) </>}
          <button className="mini" onClick={() => setShowAll(v => !v)}>{showAll ? 'hide them' : 'show anyway'}</button>
        </p>
      )}
      {(approve.error || revoke.error) && <p className="err">{String(approve.error ?? revoke.error)}</p>}

      {canWrite && candsQ.data && (() => {
        // What a dispatch operates on: every approved band, whoever approved
        // it — spelled out here so "dispatch" is never a leap of faith.
        const ap = bands.filter(c => approvals.has(c.prefix))
        const objs = ap.reduce((n, c) => n + c.net_objects, 0)
        const bySweeper = new Map<string, number>()
        for (const c of ap) for (const sw of c.sweepers) bySweeper.set(sw, (bySweeper.get(sw) ?? 0) + 1)
        const full = ap.filter(c => approvals.get(c.prefix)!.mode === 'full').length
        return (
          <div className="dispatch" id="dispatch">
            <p className="dispatch-sum">
              {ap.length === 0 ? <>Nothing approved yet — a dispatch would plan nothing.</> : (
                <>
                  <b>{ap.length}</b> approved band{ap.length === 1 ? '' : 's'} · <b>≈{tb(approvedAttrBytes)}</b> deletable
                  {approvedAttrBytes !== approvedBytes && <span className="dim"> (of {tb(approvedBytes)} in the bands; the rest is other users' or unowned data, deferred)</span>}
                  {' '}· {objs.toLocaleString()} objects in the bands
                  {full > 0 && <> · <span className="warn-tag">{full} in FULL mode</span></>}
                  {' '}· swept by {[...bySweeper].sort((x, y) => y[1] - x[1]).map(([sw, n], i) => <span key={sw}>{i > 0 && ', '}<UserChip who={sw} size={14} /> ×{n}</span>)}
                </>
              )}
            </p>
            <p className="dim dispatch-note">
              A dry-run walks the pinned scan listing under each approved band, plans the deletions under the ownership gate, and records the run below — it deletes nothing. The real run takes the same plan and deletes.
            </p>
            {allBuckets.length > 1 && (
              <details className="dispatch-buckets-wrap">
                <summary className="dim">
                  limit this run to some buckets{partial ? <> — <b>{onBuckets.length} of {allBuckets.length}</b> checked</> : ''}
                </summary>
              <p className="dispatch-buckets">
                {allBuckets.map(b => {
                  const e = perBucket.get(b)!
                  return (
                    <label key={b} className={bucketsOff.has(b) ? 'off' : undefined}>
                      <input type="checkbox" checked={!bucketsOff.has(b)} onChange={ev => setBucketsOff(s => { const n = new Set(s); if (ev.target.checked) n.delete(b); else n.add(b); return n })} />
                      {' '}<code>{b}</code> <span className="dim">{e.bands} band{e.bands === 1 ? '' : 's'} · ≈{tb(e.attr)} · {e.objects.toLocaleString()} objects in bands</span>
                    </label>
                  )
                })}
                {partial && <span className="dim"> — only the checked buckets are planned and swept</span>}
              </p>
              </details>
            )}
            <div className="dispatch-btns">
          {/* Dispatches a GCP Batch executor run: `sweep manifest -S` (reads
              the approvals above) → `sweep execute` — the run records itself
              into the table below. Dry-run is the default posture; "real"
              takes a second, armed click and shows what it will consume. */}
          <button className="mini go" disabled={dispatch.isPending || !onBuckets.length} onClick={() => dispatch.mutate('dry')}>dispatch dry-run</button>
          {!armed ? (
            <button className="mini danger" disabled={runBytes === 0 || dispatch.isPending} onClick={() => setArmed(true)}
                    title={approvedBytes === 0 ? 'approve at least one band first' : runBytes === 0 ? 'check at least one bucket' : undefined}>
              real delete…
            </button>
          ) : (
            <>
              <button className="mini danger armed" disabled={dispatch.isPending} onClick={() => dispatch.mutate('real')}>
                confirm REAL delete — ≈{tb(runAttrBytes)} (attr-gated, of {tb(runBytes)} approved{partial ? `, ${onBuckets.length} of ${allBuckets.length} buckets` : ''})
              </button>
              <button className="mini" onClick={() => setArmed(false)}>cancel</button>
            </>
          )}
          {dispatch.isPending && <span className="dim">submitting…</span>}
          {dispatch.data && (
            <span className="ok">
              submitted <code>{dispatch.data.job_id}</code> — appears below once the executor records it
              (streaming the listing takes a while; this page refetches runs every 30s)
            </span>
          )}
          {dispatch.error != null && <span className="err">{String(dispatch.error)}</span>}
            </div>
          </div>
        )
      })()}

      <h2 id="runs">Runs</h2>
      {/* One row per run, from the moment it is queued: Batch's job (state,
          region, buckets, elapsed) joined to the D1 run the executor records
          (totals, undo window) by the job id in the run's log dir. A row
          without a job is a CLI run or older than Batch's list; a live row
          draws its progress from the executor's `progress/<bucket>.json`. */}
      {(jobsQ.isPending || runsQ.isPending) && <Skeleton height={160} label="loading runs…" />}
      {jobsQ.data?.configured === false && <p className="dim">Dispatch isn't configured on this deployment (no <code>GCP_SA_KEY</code>): only recorded runs are listed.</p>}
      {jobsQ.isError && <p className="err">{String(jobsQ.error)}</p>}
      {runsQ.isError && <p className="err">{String(runsQ.error)}</p>}
      {jobsQ.data && runsQ.data && !runList.length && <p className="dim">None yet — a dispatch shows here from the moment it is queued; the executor fills in the totals as it runs.</p>}
      {!!runList.length && (
        <div className="table-scroll busy-host">{(jobsQ.isFetching || runsQ.isFetching) && <Busy corner label="refreshing…" />}<table className="sweep-table runs">
          <thead>
            <tr><th>run</th><th>by</th><th>mode</th><th>buckets</th><th className="num">planned</th><th className="num">deleted</th><th className="num">gone</th><th className="num">overwritten</th><th className="num">drift</th><th>state</th><th>started</th><th className="num">elapsed</th><th>undo by</th><th>links</th>{canWrite && <th></th>}</tr>
          </thead>
          <tbody>
            {runPageRows.map(({ key, job, run }) => {
              const mode = job?.mode ?? run!.mode
              const live = !!job && LIVE_STATES.has(job.state)
              const startedTs = run?.started_ts ?? (job ? Date.parse(job.created) / 1000 : 0)
              const secs = job?.run_secs ?? (run?.finished_ts ? run.finished_ts - run.started_ts : live ? Date.now() / 1000 - startedTs : null)
              const p = job ? planned.get(job.job_id) : undefined
              const prog = job ? progress.get(job.job_id) : undefined
              const bucketsOf = job ? job.buckets : run?.buckets?.split(',') ?? []
              const state = job ? job.state.toLowerCase() : run?.finished_ts ? 'recorded' : 'in progress'
              const stateCls = job?.state === 'SUCCEEDED' ? 'ok' : job?.state === 'FAILED' ? 'err' : live ? 'live-tag' : 'dim'
              const actor = run?.actor ?? job?.by ?? ''
              const logDir = (run?.log_dir ?? job?.plan ?? '').replace('gs://oa-gcs-usage-dvx/', '').replace(/\/?$/, '/')
              return (
                <tr key={key} id={run ? `run-${run.run_id.replace('/', '-')}` : undefined} className={[`mode-${mode}`, job?.state === 'FAILED' ? 'failed' : live ? 'live' : job?.state === 'SUCCEEDED' || run?.finished_ts ? 'ok' : ''].filter(Boolean).join(' ')}>
                  <td><code>{job?.job_id ?? run!.run_id}</code></td>
                  <td>{actor && (
                    <Tooltip content={<>{run ? 'run' : 'dispatched'} by <b>{shortName(actor)}</b></>}>
                      <span className="approver"><Avatar github={ghHandle(actor)} name={shortName(actor)} size={16} /></span>
                    </Tooltip>
                  )}</td>
                  <td className="mode">{mode === 'real' ? <span className="warn-tag">REAL</span> : 'dry-run'}</td>
                  <td><span className="nb">{shortBuckets(bucketsOf)}{job?.bucket_region && job.bucket_region !== job.region && (
                    <span className="dim" title={`Ran in ${job.region}, not the bucket's ${job.bucket_region}${job.bucket_region === 'us-central2' ? ' (Batch has no us-central2 location)' : ' (dispatched before jobs were colocated with their bucket)'}`}> · {job.region}</span>
                  )}</span></td>
                  <td className="num"><span className="nb">{p ? `${tb(p.bytes)} · ${p.objects.toLocaleString()}` : '—'}</span></td>
                  <td className="num">
                    {run?.finished_ts ? (
                      <span className="nb">{tb(run.deleted_bytes)} · {run.deleted_objects.toLocaleString()}</span>
                    ) : live && prog ? (
                      <span className="prog nb" title={`${prog.deletes.toLocaleString()} of ${(p?.objects ?? 0).toLocaleString()} · ${prog.rate.toLocaleString()}/s · ${prog.roots_done.toLocaleString()} / ${prog.roots.toLocaleString()} roots`}>
                        <progress max={p?.objects || undefined} value={prog.deletes} /> {tb(prog.bytes)} · {prog.deletes.toLocaleString()} · {prog.rate.toLocaleString()}/s
                      </span>
                    ) : live && !p ? (
                      <span className="dim" title="The manifest step is still streaming the scan listing; deletes start once it lands.">planning…</span>
                    ) : live ? (
                      <span className="dim" title="The job's image predates progress reporting (images built before 2026-09-11 12:00Z); deletes are landing, but only the final log will say how many.">no progress file</span>
                    ) : '—'}
                  </td>
                  <td className="num">{run ? run.skipped_gone.toLocaleString() : prog ? prog.gone.toLocaleString() : '—'}</td>
                  <td className="num">{run ? run.skipped_overwritten.toLocaleString() : '—'}</td>
                  <td className="num">{run ? run.drift_dirs + run.ledger_drift_dirs : '—'}</td>
                  <td>
                    <span className={stateCls}>{state}</span>
                    {job?.state === 'FAILED' && job.last_event && (
                      <details className="why"><summary className="dim small">why</summary><div className="dim small">{job.last_event}</div></details>
                    )}
                  </td>
                  <td><span className="nb">{when(startedTs)}</span></td>
                  <td className="num"><span className="nb">{secs == null ? '—' : fmtDur(secs)}</span></td>
                  <td>{mode === 'real' && run?.undo_deadline ? <span className="nb">{when(run.undo_deadline)}</span> : '—'}</td>
                  <td className="nb">
                    <Link to={`/files/${logDir}`}>plan →</Link>
                    {(run?.finished_ts || live) && <> · <Link to={`/files/${logDir}${mode === 'real' ? 'deleted' : 'would-delete'}/`}>log →</Link></>}
                    {job && <> · <a href={job.logs} target="_blank" rel="noreferrer">logs ↗</a></>}
                  </td>
                  {canWrite && (
                    <td>
                      {live && (
                        <button className="mini stop" disabled={stop.isPending || stopped.has(job!.job_id)} onClick={() => stop.mutate(job!.job_id)}
                                title="Drop the STOP file: roots already listing finish and log, the rest are left for a re-run; the job ends red.">
                          {stopped.has(job!.job_id) ? 'stopping' : 'stop'}
                        </button>
                      )}
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table></div>
      )}
      {runList.length > PAGE_SIZES[0] && (
        <div className="sweep-tools runs-pager">
          <span className="dim nb">{`${Math.min(runPage, runPages - 1) * runPageSize + 1}–${Math.min(runList.length, (Math.min(runPage, runPages - 1) + 1) * runPageSize)} of ${runList.length}`}</span>
          <span className="nb">
            <button className="mini" disabled={runPage <= 0} onClick={() => setRunPage(p => Math.max(0, p - 1))}>‹</button>
            {' '}<button className="mini" disabled={runPage >= runPages - 1} onClick={() => setRunPage(p => Math.min(runPages - 1, p + 1))}>›</button>
          </span>
          <span className="nb"><span className="dim">per page</span>{PAGE_SIZES.map(n => <button key={n} className={`mini${n === runPageSize ? ' on' : ''}`} onClick={() => { setRunPageSize(n); setRunPage(0) }}>{fmtPageSize(n)}</button>)}</span>
        </div>
      )}
      {stop.error != null && <p className="err">{String(stop.error)}</p>}
    </main>
  )
}
