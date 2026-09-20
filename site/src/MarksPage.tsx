import { useRules } from './rules'
import { Skeleton } from './Busy'
import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { whoToHandle } from './Avatar'
import { SiteNav } from './SiteNav'
import { SiteKbd } from './SiteKbd'
import { UserChip } from './UserChip'
import { fmtMarkDate } from './MarkControls'
import { ActionChip, LOCAL_TZ, fmtWhen, useMarkEvents } from './markEvents'
import { type RuleUser } from './types'
import { useDocTitle } from './title'

// Recent-marks activity feed (specs/actions-ledger.md): the ledger's keep +
// owner rows, newest first — who decided what, when. Read-only; the map is
// where marks are set. Clicking a prefix jumps the treemap to it.

const PAGE = 25

// `gs://marin-<bucket>/<path>/` → the treemap's URL path (below the store root).
const prefixToPath = (prefix: string): string => {
  const m = /^[a-z0-9]+:\/\/(.*?)\/?$/.exec(prefix)
  return m ? m[1] : prefix
}

// Avatar + short name, with the interactive identity hover card (UserChip). The
// rules.json row adds the "aka <aliases>" line the registry alone doesn't carry.
function WhoCell({ who, user }: { who: string; user?: RuleUser }) {
  const extra = user?.aliases?.length
    ? <div className="uc-sub">aka {user.aliases.join(', ')}</div>
    : undefined
  return <UserChip who={who} extra={extra} />
}

export function MarksPage() {
  useDocTitle('Marks')
  const { events, isLoading, error } = useMarkEvents()
  const [page, setPage] = useState(0)
  const { data: rules } = useRules()
  const userByHandle = useMemo(() => {
    const m = new Map<string, RuleUser>()
    for (const u of rules?.users ?? []) {
      m.set(u.u, u)
      for (const a of u.aliases) m.set(a.toLowerCase(), u)
    }
    return m
  }, [rules])

  return (
    <main className="marks-page">
      <SiteNav />
      <header>
        <div className="hrow">
          <h1>Recent marks</h1>
        </div>
        <p className="sub">
          Every keep / sweep / assignment in the ledger, newest first. Click a prefix to open it on the map.
          {events.length > 0 && <> {events.length} action{events.length === 1 ? '' : 's'}.</>}
        </p>
      </header>

      {error && <p className="tab-note" style={{ color: 'var(--s3)' }}>Couldn’t load marks: {error.message}</p>}
      {isLoading && events.length === 0 && <Skeleton height={400} label="loading marks…" />}
      {!isLoading && !error && events.length === 0 && <p className="tab-note">No marks yet — head to the map and start marking.</p>}

      {events.length > 0 && (() => {
        const pages = Math.ceil(events.length / PAGE)
        const p = Math.min(page, pages - 1)
        const rows = events.slice(p * PAGE, p * PAGE + PAGE)
        return (
          <>
            <table className="worklist marks-feed">
              <thead>
                <tr><th>when ({LOCAL_TZ})</th><th>who</th><th>action</th><th>prefix</th></tr>
              </thead>
              <tbody>
                {rows.map(e => (
                  <tr key={`${e.id}-${e.kind}-${e.prefix}`}>
                    <td title={fmtMarkDate(e.ts)}>{fmtWhen(e.ts)}</td>
                    <td><WhoCell who={e.who} user={userByHandle.get(whoToHandle(e.who))} /></td>
                    <td><ActionChip e={e} /></td>
                    <td className="prefix">
                      <Link to={`/${prefixToPath(e.prefix)}`}>{e.prefix}</Link>
                      {e.memo && <span className="memo" title={e.memo}> — {e.memo}</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {pages > 1 && (
              <div className="pager">
                <button type="button" disabled={p === 0} onClick={() => setPage(p - 1)}>← prev</button>
                <span className="pager-count">{p * PAGE + 1}–{p * PAGE + rows.length} of {events.length}</span>
                <button type="button" disabled={p >= pages - 1} onClick={() => setPage(p + 1)}>next →</button>
              </div>
            )}
          </>
        )
      })()}
      <SiteKbd />
    </main>
  )
}
