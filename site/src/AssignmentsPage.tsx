import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import { Link } from 'react-router-dom'
import { stringParam, useUrlState } from 'use-prms'
import { SiteNav } from './SiteNav'
import { SiteKbd } from './SiteKbd'
import { UserChip, shortName, shortUserKey } from './UserChip'
import { Tooltip } from './Tooltip'
import { DEFAULT_STORE } from './stores'
import { useScan } from './scan'
import { useUnits } from './units'
import { useDocTitle } from './title'
import { Busy, Skeleton } from './Busy'

interface Cell { by: string; to: string; bytes: number; prefixes: string[] }

// The two owner-provenance matrices, sharing one heatmap. `sweep` = sweeper ×
// owner (who marked data sweep × who owns it) — off-diagonal cells are the
// conflicts to vet before a deletion. `assign` = assigner × assignee (who
// assigned an owner to a prefix × to whom). Cells open the map scoped to the
// pair.
const MODES = {
  sweep: {
    api: '/api/sweep-owners', label: 'sweeper × owner', rowAxis: 'sweeper', colAxis: 'owner',
    blurb: <>Who marked data <b>sweep</b> (rows) × who <b>owns</b> it (columns). <b>Off the diagonal</b> = a sweeper marked data someone else owns — the conflicts to vet before dispatching a deletion.</>,
  },
  assign: {
    api: '/api/assignments', label: 'assigner × assignee', rowAxis: 'assigner', colAxis: 'assignee',
    blurb: <>Who <b>assigned</b> an owner to a prefix (rows) × the person it was assigned <b>to</b> (columns). Off the diagonal = someone assigned <i>another</i> person's data.</>,
  },
} as const
type Mode = keyof typeof MODES

export function AssignmentsPage() {
  useDocTitle('Assignments')
  const { fmtBytes } = useUnits()
  const { asof } = useScan(DEFAULT_STORE)
  const [mP, setMP] = useUrlState('m', stringParam())
  const mode: Mode = mP === 'assign' ? 'assign' : 'sweep'
  const cfg = MODES[mode]
  const q = useQuery<{ scan: string; head: number; cells: Cell[] }>({
    queryKey: ['matrix', mode, asof],
    enabled: !!asof,
    queryFn: async () => {
      const r = await fetch(`${cfg.api}?date=${asof}`, { credentials: 'include' })
      if (!r.ok) throw new Error(`${r.status}`)
      return r.json()
    },
  })
  const { rows, cols, at, max } = useMemo(() => {
    const cells = q.data?.cells ?? []
    const at = new Map<string, Cell>()
    const rowB = new Map<string, number>()
    const colB = new Map<string, number>()
    for (const c of cells) {
      at.set(`${c.by} ${c.to}`, c)
      rowB.set(c.by, (rowB.get(c.by) ?? 0) + c.bytes)
      colB.set(c.to, (colB.get(c.to) ?? 0) + c.bytes)
    }
    const rows = [...rowB.entries()].sort((a, b) => b[1] - a[1]).map(e => e[0])
    const cols = [...colB.entries()].sort((a, b) => b[1] - a[1]).map(e => e[0])
    const max = Math.max(1, ...cells.map(c => c.bytes))
    return { rows, cols, at, max }
  }, [q.data])

  // Cell colour: log scale of bytes over an accent ramp (0 → transparent).
  const bg = (b: number): string => {
    if (b <= 0) return 'transparent'
    const t = Math.log10(b + 1) / Math.log10(max + 1)
    return `color-mix(in srgb, var(--accent, #58a6ff) ${Math.round(12 + t * 78)}%, transparent)`
  }

  return (
    <main className="assignments-page">
      <SiteNav />
      <header>
        <div className="hrow">
          <h1>{cfg.label}</h1>
          <span className="mode-toggle">
            {(Object.keys(MODES) as Mode[]).map(m => (
              <button key={m} type="button" className={`mini${m === mode ? ' on' : ''}`}
                onClick={() => setMP(m === 'sweep' ? undefined : m)}>{MODES[m].label}</button>
            ))}
          </span>
        </div>
        <p className="sub">
          {cfg.blurb} By bytes at scan <b>{q.data?.scan ?? asof ?? '…'}</b>; a cell opens the map scoped to that pair.
          {mode === 'sweep' && ' Inferred attribution (W&B, path shapes) is a separate axis — not here yet.'}
        </p>
      </header>
      {q.isLoading && <Skeleton height={320} label="loading matrix…" />}
      {q.error && <p className="err">{(q.error as Error).message}</p>}
      {q.data && !q.data.cells.length && <p className="dim">{mode === 'sweep' ? 'No live sweep marks.' : 'No owner assignments in the ledger yet.'}</p>}
      {!!q.data?.cells.length && (
        <div className="table-scroll busy-host">
          {q.isFetching && <Busy corner label="refreshing…" />}
          <table className="heatmap">
            <thead>
              <tr>
                <th className="corner"><span className="dim">{cfg.rowAxis} ↓ / {cfg.colAxis} →</span></th>
                {cols.map(u => (
                  <th key={u} className="col-head"><UserChip who={u} size={16} /></th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map(by => (
                <tr key={by}>
                  <th className="row-head"><UserChip who={by} size={16} /></th>
                  {cols.map(to => {
                    const c = at.get(`${by} ${to}`)
                    const b = c?.bytes ?? 0
                    const conflict = mode === 'sweep' && by !== to && b > 0
                    const link = `/?o=${encodeURIComponent(shortUserKey(to))}${mode === 'assign' ? `&by=${encodeURIComponent(shortUserKey(by))}` : ''}`
                    return (
                      <td key={to} className={`cell${conflict ? ' conflict' : ''}`} style={{ background: bg(b) }}>
                        {c ? (
                          <Tooltip content={<><b>{shortName(by)}</b> {mode === 'sweep' ? 'swept' : '→'} <b>{shortName(to)}</b>: {fmtBytes(b)} across {c.prefixes.length} prefix{c.prefixes.length === 1 ? '' : 'es'}{by === to ? (mode === 'sweep' ? ' (own data)' : ' (self-assigned)') : (mode === 'sweep' ? ' — ⚠ not the sweeper’s' : '')}. Click to open the map.</>}>
                            <Link to={link} className="cell-link">{fmtBytes(b)}</Link>
                          </Tooltip>
                        ) : <span className="empty" />}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <SiteKbd />
    </main>
  )
}
