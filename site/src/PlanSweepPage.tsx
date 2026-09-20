// /sweep — the mark & sweep console (specs/cw-sweep.md): curate first-class
// deletion plans, dispatch dry/real runs on GCP Batch, and track runs with
// undo/purge. Plain fetch/useState (cw-s3 has no react-query); admin controls
// gated on /api/whoami. Owner axis intentionally absent — the curated plan is
// the eligibility decision.
import { useCallback, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { Tooltip } from './Tooltip'
import { UserChip } from './UserChip'
import { useUnits } from './units'
import { fmtN } from './types'
import { DEFAULT_STORE } from './stores'

interface Whoami { email: string | null; admin: boolean }
interface PlanSummary {
  id: number; name: string; note: string | null; state: 'open' | 'closed'
  created_by: string; created_ts: number; closed_ts: number | null; items: number; runs: number
}
interface PlanItem { prefix: string; note: string | null; added_by: string; added_ts: number }
interface Run {
  run_id: string; plan_id: number; mode: 'dry' | 'real'; scan: string
  started_ts: number; finished_ts: number | null
  deleted_bytes: number; deleted_objects: number; skipped_gone: number; skipped_overwritten: number
  drift_dirs: number; undo_deadline: number | null; undo_state: string; purge_state: string; log_dir: string
}
interface PlanDetail { plan: PlanSummary; items: PlanItem[]; runs: Run[] }
interface Job {
  job_id: string; mode: string; state: string; created: string; updated: string | null
  run_secs: number | null; date: string | null; run: string; last_event: string | null; logs: string
}

async function getJson<T>(url: string): Promise<T> {
  const r = await fetch(url, { credentials: 'include' })
  if (!r.ok) throw new Error(`${url}: ${r.status}`)
  return r.json() as Promise<T>
}
async function send(url: string, method: string, body?: unknown): Promise<{ ok: boolean; status: number; data: unknown }> {
  const r = await fetch(url, {
    method, credentials: 'include',
    headers: body ? { 'content-type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  // CF returns HTML on a 5xx; read text first, then try JSON (per gcs's lesson).
  const text = await r.text()
  let data: unknown = { body: text.slice(0, 400) }
  try { data = JSON.parse(text) } catch { /* keep raw */ }
  return { ok: r.ok, status: r.status, data }
}

const ago = (ts: number): string => {
  const s = Math.max(0, Math.floor(Date.now() / 1000 - ts))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m`
  if (s < 86400) return `${Math.floor(s / 3600)}h`
  return `${Math.floor(s / 86400)}d`
}
const LIVE = new Set(['QUEUED', 'SCHEDULED', 'RUNNING'])

export function PlanSweepPage() {
  const { fmtBytes } = useUnits()
  const [who, setWho] = useState<Whoami | null>(null)
  const [plans, setPlans] = useState<PlanSummary[]>([])
  const [sel, setSel] = useState<number | null>(null)
  const [detail, setDetail] = useState<PlanDetail | null>(null)
  const [jobs, setJobs] = useState<Record<string, Job>>({})
  const [scans, setScans] = useState<string[]>([])
  const [date, setDate] = useState<string>('')
  const [err, setErr] = useState<string | null>(null)
  const admin = !!who?.admin

  const loadPlans = useCallback(() => getJson<{ plans: PlanSummary[] }>('/api/plans').then(d => setPlans(d.plans)).catch(e => setErr(String(e))), [])
  const loadDetail = useCallback((id: number) => getJson<PlanDetail>(`/api/plans/${id}`).then(setDetail).catch(e => setErr(String(e))), [])
  const loadJobs = useCallback(() => getJson<{ jobs: Job[] }>('/api/plan-sweep/jobs')
    .then(d => setJobs(Object.fromEntries(d.jobs.map(j => [j.job_id, j])))).catch(() => {}), [])

  useEffect(() => { void getJson<Whoami>('/api/whoami').then(setWho).catch(() => {}) }, [])
  useEffect(() => { void loadPlans(); void loadJobs() }, [loadPlans, loadJobs])
  useEffect(() => { void getJson<string[]>(`${DEFAULT_STORE.base}/scans.json`).then(s => { setScans(s); setDate(s[0] ?? '') }).catch(() => {}) }, [])
  useEffect(() => { if (sel != null) void loadDetail(sel) }, [sel, loadDetail])

  // Poll while any of the selected plan's runs is live.
  const anyLive = detail?.runs.some(r => LIVE.has(jobs[r.run_id]?.state ?? '')) ?? false
  const tick = useRef<number | undefined>(undefined)
  useEffect(() => {
    if (!anyLive) return
    tick.current = window.setInterval(() => { void loadJobs(); if (sel != null) void loadDetail(sel) }, 20_000)
    return () => window.clearInterval(tick.current)
  }, [anyLive, sel, loadJobs, loadDetail])

  const act = async (url: string, method: string, body?: unknown, reload = true) => {
    setErr(null)
    const { ok, data } = await send(url, method, body)
    if (!ok) { setErr(typeof data === 'object' && data && 'error' in data ? String((data as { error: unknown }).error) : 'request failed'); return false }
    if (reload) { await loadPlans(); if (sel != null) await loadDetail(sel); await loadJobs() }
    return true
  }

  return (
    <div className="sweep-page">
      <div className="sweep-head">
        <Link to="/" className="back">← treemap</Link>
        <h1>Sweep console</h1>
        <span className="who">
          {who?.email
            ? <>{admin ? 'admin' : 'viewer'} · <UserChip who={who.email} size={20} /></>
            : 'not signed in'}
        </span>
      </div>
      {!admin && <p className="note">Read-only: dispatching deletes and editing plans require admin.</p>}
      {err && <p className="sweep-err" role="alert">{err} <button onClick={() => setErr(null)}>dismiss</button></p>}

      <div className="sweep-grid">
        <PlansList plans={plans} sel={sel} onSelect={setSel} admin={admin}
          onCreate={async (name, note) => { const { ok, data } = await send('/api/plans', 'POST', { name, note })
            if (ok && data && typeof data === 'object' && 'id' in data) { await loadPlans(); setSel(Number((data as { id: number }).id)) }
            else setErr('create failed') }} />
        {detail
          ? <PlanPanel detail={detail} jobs={jobs} admin={admin} scans={scans} date={date} setDate={setDate}
              fmtBytes={fmtBytes} act={act} />
          : <p className="sweep-empty">Select a plan, or create one.</p>}
      </div>
    </div>
  )
}

function PlansList({ plans, sel, onSelect, admin, onCreate }: {
  plans: PlanSummary[]; sel: number | null; onSelect: (id: number) => void
  admin: boolean; onCreate: (name: string, note: string) => void
}) {
  const [name, setName] = useState('')
  const [note, setNote] = useState('')
  return (
    <div className="plans-list">
      <h2>Plans</h2>
      <ul>
        {plans.map(p => (
          <li key={p.id} className={`${p.id === sel ? 'on' : ''} ${p.state}`}>
            <button onClick={() => onSelect(p.id)}>
              <span className="pname">{p.name}</span>
              <span className="pmeta">{p.state} · {p.items} items · {p.runs} runs</span>
            </button>
          </li>
        ))}
        {!plans.length && <li className="dim">no plans yet</li>}
      </ul>
      {admin && (
        <form className="new-plan" onSubmit={e => { e.preventDefault(); if (name.trim()) { onCreate(name.trim(), note.trim()); setName(''); setNote('') } }}>
          <input placeholder="new plan name" value={name} onChange={e => setName(e.target.value)} />
          <input placeholder="note (optional)" value={note} onChange={e => setNote(e.target.value)} />
          <button type="submit" disabled={!name.trim()}>create</button>
        </form>
      )}
    </div>
  )
}

function PlanPanel({ detail, jobs, admin, scans, date, setDate, fmtBytes, act }: {
  detail: PlanDetail; jobs: Record<string, Job>; admin: boolean
  scans: string[]; date: string; setDate: (d: string) => void
  fmtBytes: (b: number) => string
  act: (url: string, method: string, body?: unknown, reload?: boolean) => Promise<boolean>
}) {
  const { plan, items, runs } = detail
  const open = plan.state === 'open'
  const [add, setAdd] = useState('')
  const [armed, setArmed] = useState(false)
  useEffect(() => setArmed(false), [plan.id])

  const addPrefixes = async () => {
    const prefixes = add.split('\n').map(s => s.trim()).filter(Boolean)
    if (prefixes.length && await act(`/api/plans/${plan.id}/items`, 'POST', { prefixes })) setAdd('')
  }
  const addMarked = async () => {
    const d = await getJson<{ marks: { prefix: string; keep: string }[] }>('/api/plan-marks').catch(() => ({ marks: [] }))
    const have = new Set(items.map(i => i.prefix))
    const prefixes = d.marks.filter(m => m.keep === 'sweep' && !have.has(m.prefix)).map(m => m.prefix)
    if (prefixes.length) await act(`/api/plans/${plan.id}/items`, 'POST', { prefixes })
  }

  return (
    <div className="plan-panel">
      <div className="pp-head">
        <h2>{plan.name} <span className={`state ${plan.state}`}>{plan.state}</span></h2>
        {admin && open && <button className="close-plan" onClick={() => act(`/api/plans/${plan.id}`, 'PATCH', { state: 'closed' })}>close plan</button>}
      </div>
      {plan.note && <p className="pp-note">{plan.note}</p>}

      <h3>Items ({items.length})</h3>
      <ul className="items">
        {items.map(it => (
          <li key={it.prefix}>
            <code>{it.prefix}</code>
            {admin && open && <button className="rm" title="remove" onClick={() => act(`/api/plans/${plan.id}/items`, 'DELETE', { prefixes: [it.prefix] })}>×</button>}
          </li>
        ))}
        {!items.length && <li className="dim">no prefixes yet</li>}
      </ul>
      {admin && open && (
        <div className="add-items">
          <textarea placeholder="s3://marin-us-east-02a/… prefixes, one per line" value={add} onChange={e => setAdd(e.target.value)} rows={3} />
          <div className="add-btns">
            <button onClick={addPrefixes} disabled={!add.trim()}>add prefixes</button>
            <button onClick={addMarked} title="add every prefix currently marked 'sweep'">add sweep-marked</button>
          </div>
        </div>
      )}

      {admin && open && (
        <div className="dispatch">
          <h3>Dispatch</h3>
          <label>scan <select value={date} onChange={e => setDate(e.target.value)}>{scans.map(s => <option key={s}>{s}</option>)}</select></label>
          <button className="dry" disabled={!items.length || !date} onClick={() => act('/api/plan-sweep/dispatch', 'POST', { plan_id: plan.id, mode: 'dry', date })}>dispatch dry-run</button>
          {!armed
            ? <button className="danger" disabled={!items.length || !date} onClick={() => setArmed(true)}>real delete…</button>
            : <>
                <button className="danger armed" onClick={async () => { if (await act('/api/plan-sweep/dispatch', 'POST', { plan_id: plan.id, mode: 'real', date })) setArmed(false) }}>confirm REAL delete on {date}</button>
                <button onClick={() => setArmed(false)}>cancel</button>
              </>}
        </div>
      )}

      <h3>Runs ({runs.length})</h3>
      <div className="runs-wrap">
        <table className="runs">
          <thead><tr><th>run</th><th>mode</th><th>state</th><th>deleted</th><th>gone</th><th>overwr</th><th>started</th><th></th></tr></thead>
          <tbody>
            {runs.map(r => <RunRow key={r.run_id} r={r} job={jobs[r.run_id]} admin={admin} fmtBytes={fmtBytes} act={act} />)}
            {!runs.length && <tr><td colSpan={8} className="dim">no runs</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function RunRow({ r, job, admin, fmtBytes, act }: {
  r: Run; job: Job | undefined; admin: boolean
  fmtBytes: (b: number) => string
  act: (url: string, method: string, body?: unknown, reload?: boolean) => Promise<boolean>
}) {
  const state = job?.state ?? (r.finished_ts ? 'done' : 'UNKNOWN')
  const live = LIVE.has(state)
  const now = Math.floor(Date.now() / 1000)
  const canUndo = admin && r.mode === 'real' && r.undo_state !== 'full' && (!r.undo_deadline || now < r.undo_deadline) && !!r.finished_ts
  const canPurge = admin && r.mode === 'real' && r.purge_state === 'pending' && r.undo_state !== 'full' && (!r.undo_deadline || now >= r.undo_deadline)
  return (
    <tr className={`run ${r.mode} ${state.toLowerCase()}`}>
      <td className="rid"><code>{r.run_id.replace(/^cw-sweep-(dry|real)-/, '')}</code>{job && <> <a href={job.logs} target="_blank" rel="noreferrer" className="logs">logs</a></>}</td>
      <td>{r.mode}</td>
      <td>
        <span className="rstate">{state}</span>
        {state === 'FAILED' && job?.last_event && <details className="why"><summary>why</summary><div>{job.last_event}</div></details>}
        {r.undo_state === 'full' && <span className="tag">undone</span>}
        {r.purge_state === 'done' && <span className="tag">purged</span>}
      </td>
      <td>{fmtBytes(r.deleted_bytes)} <span className="dim">/ {fmtN(r.deleted_objects)}</span></td>
      <td>{fmtN(r.skipped_gone)}</td>
      <td>{fmtN(r.skipped_overwritten)}</td>
      <td><Tooltip content={new Date(r.started_ts * 1000).toISOString()}><span>{ago(r.started_ts)} ago</span></Tooltip></td>
      <td className="actions">
        {admin && live && <button onClick={() => act('/api/plan-sweep/stop', 'POST', { job_id: r.run_id })}>stop</button>}
        {canUndo && <button onClick={() => act('/api/plan-sweep/undo', 'POST', { run_id: r.run_id })}>undo</button>}
        {canPurge && <button className="danger" onClick={() => act('/api/plan-sweep/purge', 'POST', { run_id: r.run_id })}>purge</button>}
      </td>
    </tr>
  )
}
