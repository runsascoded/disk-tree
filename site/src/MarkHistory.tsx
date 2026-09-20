import { Explain } from './Help'
import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { stringParam, useUrlState } from 'use-prms'
import { fmtMarkDate } from './MarkControls'
import { Tooltip } from './Tooltip'
import { UserChip } from './UserChip'
import type { NamePred } from './filterTree'
import { ActionChip, LOCAL_TZ, MARK_ACTS, eventsUnder, fmtWhen, useMarkEvents } from './markEvents'
import { fmtScan, scanTime } from './scan'
import { Busy } from './Busy'

// Path-scoped slice of the mark ledger: every keep/sweep/clear/assignment under the
// currently-drilled prefix, newest first. The map shows *current* state; this
// shows how it got there (and, when size-over-time can't, the change story).
// It reads the same page scope as the widgets around it: the drilled prefix,
// the `?f=` path filter, and (opt-in) the diff window.

const PAGE_SIZES = [10, 20, 50]

/** `gs://marin-<bucket>/<path>/` → the treemap's URL path segments. */
const prefixToPath = (prefix: string): string => {
  const m = /^[a-z0-9]+:\/\/(.*?)\/?$/.exec(prefix)
  return m ? m[1] : prefix
}

export function MarkHistory({ prefix, scope, pred, filterQ, window: win }: {
  prefix: string
  scope: string
  /** The page's `?f=` path filter: keep actions whose prefix has a matching segment. */
  pred?: NamePred | null
  filterQ?: string
  /** The page's diff window [before, after] (scan ids), offered as a time filter. */
  window?: [string, string]
}) {
  const { events, isLoading, isFetching } = useMarkEvents()
  const [page, setPage] = useState(0)
  // Action-type filter: `?mk=` ⊆ the MARK_ACTS letters (absent = all).
  const [mkP, setMkP] = useUrlState('mk', stringParam())
  const kinds = useMemo(() => {
    const on = new Set([...(mkP ?? '')].filter(k => MARK_ACTS.some(a => a.key === k)))
    return on.size ? on : new Set(MARK_ACTS.map(a => a.key))
  }, [mkP])
  const allKinds = kinds.size === MARK_ACTS.length
  const toggleKind = (key: string) => {
    const next = new Set(kinds)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    // Every kind on = the default (no param); switching the last one off would
    // hide everything, so it rolls back to all (same rule as the bar's axes).
    if (next.size === MARK_ACTS.length || next.size === 0) setMkP(undefined)
    else setMkP(MARK_ACTS.filter(a => next.has(a.key)).map(a => a.key).join(''))
  }
  // Page size: `?mn=` (10 default).
  const [mnP, setMnP] = useUrlState('mn', stringParam())
  const pageSize = PAGE_SIZES.includes(Number(mnP)) ? Number(mnP) : PAGE_SIZES[0]
  // Time filter: `?mw` restricts to the diff window (only offered when the
  // page has one).
  const [mwP, setMwP] = useUrlState('mw', stringParam())
  const inWindow = !!win && mwP === '1'

  const scoped = useMemo(() => {
    let evs = eventsUnder(events, prefix)
    if (!allKinds) evs = evs.filter(e => kinds.has(MARK_ACTS.find(a => a.act === e.act)!.key))
    if (pred) evs = evs.filter(e => prefixToPath(e.prefix).split('/').some(pred))
    if (inWindow && win) {
      // Scan ids are calendar dates (midnight UTC); the listing itself ran a
      // few hours later, so the edges are approximate by that much.
      const [t0, t1] = [scanTime(win[0]) / 1000, scanTime(win[1]) / 1000]
      evs = evs.filter(e => e.ts >= t0 && e.ts <= t1)
    }
    return evs
  }, [events, prefix, allKinds, kinds, pred, inWindow, win])
  const total = useMemo(() => eventsUnder(events, prefix).length, [events, prefix])
  if (total === 0) return null

  const pages = Math.max(1, Math.ceil(scoped.length / pageSize))
  const p = Math.min(page, pages - 1)
  const rows = scoped.slice(p * pageSize, p * pageSize + pageSize)
  const base = prefix.endsWith('/') ? prefix : prefix + '/'
  const filtered = scoped.length !== total

  return (
    <section id="marks" className="children-tbl busy-host">
      {/* The ledger polls every 30 s; the refresh shows as a corner marker. */}
      {isFetching && !isLoading && <Busy corner label="refreshing marks…" />}
      <div className="hrow">
        <h2>Mark history</h2>
        <Link className="nav-files" to="/marks" style={{ fontSize: '0.9em' }}>All&nbsp;marks&nbsp;→</Link>
      </div>
      <p className="sub">
        Actions under <code>{scope}</code>
        {filterQ && <> matching <code>{filterQ}</code></>}
        {inWindow && win && <>, {fmtScan(win[0])} → {fmtScan(win[1])}</>}
        , newest first — {filtered ? <>{scoped.length} of {total}</> : scoped.length} action{total === 1 ? '' : 's'}.
      </p>
      <div className="mark-filters">
        <span className="mark-kinds" role="group" aria-label="Action types">
          {MARK_ACTS.map(a => (
            <Tooltip key={a.key} content={<>{kinds.has(a.key) ? 'Hide' : 'Show'} <b>{a.label}</b> actions</>}>
              <button type="button" className={`kind${kinds.has(a.key) ? ' on' : ''}`} aria-pressed={kinds.has(a.key)}
                style={{ '--kind': a.color } as React.CSSProperties}
                onClick={() => { toggleKind(a.key); setPage(0) }}>
                <span className="glyph">{a.glyph}</span>{a.label}
              </button>
            </Tooltip>
          ))}
        </span>
        {win && (
          <Explain text={<>Only actions between the Diff section’s two scans ({fmtScan(win[0])} → {fmtScan(win[1])}). Drag on the size chart to change the window.</>}>
            <button type="button" className={`kind window${inWindow ? ' on' : ''}`} aria-pressed={inWindow}
              onClick={() => { setMwP(inWindow ? undefined : '1'); setPage(0) }}>
              in diff window
            </button>
          </Explain>
        )}
        <span className="gran" role="radiogroup" aria-label="Rows per page">
          <span className="lbl">rows</span>
          {PAGE_SIZES.map(n => (
            <button key={n} role="radio" aria-checked={pageSize === n} className={pageSize === n ? 'on' : ''}
              onClick={() => { setMnP(n === PAGE_SIZES[0] ? undefined : String(n)); setPage(0) }}>
              {n}
            </button>
          ))}
        </span>
      </div>
      {rows.length > 0 ? (
        <table className="worklist marks-feed">
          <thead>
            <tr><th>when ({LOCAL_TZ})</th><th>who</th><th>action</th><th>prefix</th></tr>
          </thead>
          <tbody>
            {rows.map(e => {
              // Show the prefix relative to the current view when it's inside it.
              const rel = e.prefix.startsWith(base) ? e.prefix.slice(base.length) || '(here)' : e.prefix
              return (
                <tr key={`${e.id}-${e.kind}-${e.prefix}`}>
                  <td title={fmtMarkDate(e.ts)}>{fmtWhen(e.ts)}</td>
                  <td><UserChip who={e.who} size={15} /></td>
                  <td><ActionChip e={e} /></td>
                  <td className="prefix">
                    <Link to={`/${prefixToPath(e.prefix)}`}>{rel}</Link>
                    {e.memo && <span className="memo" title={e.memo}> — {e.memo}</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      ) : (
        <p className="hint">No actions match these filters.</p>
      )}
      {pages > 1 && (
        <div className="pager">
          <button type="button" disabled={p === 0} onClick={() => setPage(p - 1)}>← prev</button>
          <span>{p + 1} / {pages}</span>
          <button type="button" disabled={p >= pages - 1} onClick={() => setPage(p + 1)}>next →</button>
        </div>
      )}
    </section>
  )
}
