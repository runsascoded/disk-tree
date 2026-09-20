import { useMemo } from 'react'
import { useQuery, type UseQueryResult } from '@tanstack/react-query'
import { useUrlState } from 'use-prms'
import type { Store } from './stores'

// How often an unpinned tab re-checks for newly published scans.
export const SCANS_POLL_MS = 5 * 60_000

const HOUR = 3600_000
export const DAY = 24 * HOUR

// Scan labels: drop the redundant year for the current one, so a list of
// same-year scans reads as `8/17` rather than `2026-08-17`. Scan ids are
// `YYYY-MM-DD`, optionally sub-daily as `YYYY-MM-DDTHHMM`.
export function fmtScan(s: string, now = new Date()): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):?(\d{2}))?/.exec(s)
  if (!m) return s
  const [, y, mo, d, hh, mm] = m
  if (!hh) {
    // Date-only ids (the daily GCS job) are calendar dates, not instants —
    // rendering them through a timezone would shift some readers a day off.
    return Number(y) === now.getFullYear() ? `${Number(mo)}/${Number(d)}` : `${y}-${mo}-${d}`
  }
  // Sub-daily ids are UTC instants; display in the viewer's local time,
  // 12-hour with a bare a/p ("8/19 6:08a"). The `?d=` token stays UTC (see
  // decodeScan) — display converts, the URL doesn't.
  const dt = new Date(Date.UTC(+y, +mo - 1, +d, +hh, +(mm ?? '0')))
  const h = dt.getHours()
  const time = `${h % 12 || 12}:${String(dt.getMinutes()).padStart(2, '0')}${h < 12 ? 'a' : 'p'}`
  const md = `${dt.getMonth() + 1}/${dt.getDate()}`
  return dt.getFullYear() === now.getFullYear()
    ? `${md} ${time}`
    : `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')} ${time}`
}

/** A scan id's instant (UTC). Date-only ids read as midnight UTC. */
export const scanTime = (d: string): number => {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2})(\d{2})?)?/.exec(d)
  return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], +(m[4] ?? '0'), +(m[5] ?? '0')) : NaN
}

// `?d` is a *prefix* of a scan id (always UTC), accepted in several spellings —
// examples that all pin the 2026-08-19T1008 scan:
//   260819-1008 · 260819T10 (compact date; `-` or `T` before the time)
//   8-19-10 · 8-19-10-08    (M-D-H[-MM]; current year assumed)
//   26-8-19-10 · 2026-8-19-10 (year-first when the lead component can't be a month)
// The canonical/emitted form stays compact (`260819-1008`). A token matching
// several scans renders the newest plus a disambiguation strip listing the rest.
export const encodeScan = (v: string | undefined): string | undefined => {
  const m = v && /^\d{2}(\d{2})-(\d{2})-(\d{2})(?:T(\d{2})(\d{2})?)?$/.exec(v)
  if (!m) return v || undefined
  const [, y, mo, d, hh, mm] = m
  return `${y}${mo}${d}` + (hh ? `-${hh}${mm ?? ''}` : '')
}

export const decodeScan = (e: string | undefined, now = new Date()): string | undefined => {
  if (!e) return undefined
  const pad = (n: string) => n.padStart(2, '0')
  const compact = /^(\d{2})(\d{2})(\d{2})(?:[T-](\d{2})(\d{2})?)?$/.exec(e)
  if (compact) {
    const [, y, mo, d, hh, mm] = compact
    return `20${y}-${mo}-${d}` + (hh ? `T${hh}${mm ?? ''}` : '')
  }
  const parts = e.split(/[T-]/)
  if (parts.length < 2 || parts.length > 5 || parts.some(p => !/^(\d{1,2}|\d{4})$/.test(p))) return undefined
  // A lead component that can't be a month is a year (2- or 4-digit); 4-digit
  // components anywhere else are malformed.
  const yearFirst = parts[0].length === 4 || Number(parts[0]) > 12
  if (parts.slice(yearFirst ? 1 : 0).some(p => p.length > 2)) return undefined
  const y = yearFirst ? (parts[0].length === 4 ? parts[0] : `20${parts[0]}`) : String(now.getUTCFullYear())
  const [mo, d, hh, mm] = parts.slice(yearFirst ? 1 : 0)
  if (!mo || !d || Number(mo) < 1 || Number(mo) > 12 || Number(d) < 1 || Number(d) > 31) return undefined
  if ((hh && Number(hh) > 23) || (mm && Number(mm) > 59)) return undefined
  return `${y}-${pad(mo)}-${pad(d)}` + (hh ? `T${pad(hh)}${mm ? pad(mm) : ''}` : '')
}

// ---- span (the Changes section's look-back) ----
//
// `?d=[scan][-span]` — same shape as awair's `?t=`: the scan is the "after"
// endpoint (absent = latest, a sticky state that follows new scans), the span
// is how far back the "before" endpoint sits (absent = the baked previous
// scan). Spans are `Nd`, `Nh`, or both (`6d12h`); a span alone (`-7d`) keeps
// the page on latest.
//   ?d=-7d            latest, 7 days back
//   ?d=260904-0002    pinned to the 9/4 00:02Z scan, default look-back
//   ?d=260904-0002-7d pinned, 7 days back
// The span resolves to the *nearest* scan (scan times drift minutes past
// exact multiples), so an explicit dropdown pick round-trips as its own span.

export const encodeSpan = (ms: number): string => {
  const days = Math.floor(ms / DAY)
  const hours = Math.round((ms - days * DAY) / HOUR)
  return (days ? `${days}d` : '') + (hours ? `${hours}h` : '') || '0h'
}

export const decodeSpan = (s: string): number | undefined => {
  const m = /^(?:(\d+)d)?(?:(\d+)h)?$/.exec(s)
  if (!m || !s) return undefined
  const ms = (+(m[1] ?? 0)) * DAY + (+(m[2] ?? 0)) * HOUR
  return ms > 0 ? ms : undefined
}

export interface ScanSel {
  /** Scan-id prefix (decoded form, e.g. `2026-09-04T0002`); absent = latest. */
  d?: string
  /** Look-back in ms; absent = the baked previous scan. */
  span?: number
}

const SPAN_SUFFIX = /-(\d+d(?:\d+h)?|\d+h)$/

export const encodeSel = (v: ScanSel | undefined): string | undefined => {
  if (!v) return undefined
  const d = encodeScan(v.d) ?? ''
  const span = v.span ? `-${encodeSpan(v.span)}` : ''
  return d + span || undefined
}

export const decodeSel = (e: string | undefined, now = new Date()): ScanSel | undefined => {
  if (!e) return undefined
  const m = SPAN_SUFFIX.exec(e)
  const span = m ? decodeSpan(m[1]) : undefined
  const head = m ? e.slice(0, m.index) : e
  const d = head ? decodeScan(head, now) : undefined
  return d || span ? { ...(d ? { d } : {}), ...(span ? { span } : {}) } : undefined
}

/** The scan nearest to `t` among `scans` (any order); null when empty. */
export const nearestScan = (scans: string[], t: number): string | null => {
  let best: string | null = null
  for (const s of scans) if (!best || Math.abs(scanTime(s) - t) < Math.abs(scanTime(best) - t)) best = s
  return best
}

export interface Scan {
  asof: string | null
  scans: string[]
  dMatches: string[]
  dP: string | undefined
  /** Pin the "after" scan; the latest scan (or undefined) clears the pin. */
  setDP: (v: string | undefined) => void
  /** Diff look-back in ms; undefined = the previous scan. */
  span: number | undefined
  setSpan: (ms: number | undefined) => void
  /** Pin + look-back in one URL write (a chart brush sets both). */
  setRange: (d: string | undefined, ms: number | undefined) => void
  scansQ: UseQueryResult<string[]>
}

// Shared scan resolution: `?d=YYMMDD` (a prefix of a scan id) pins a scan;
// absent means "latest" (a first-class state, so a parked tab follows new scans
// via the poll rather than freezing on the day it opened). Every scan-scoped
// page (home map, /users, /user/:id) uses this so a scan pin is one shareable,
// page-independent dimension. `scans` is newest-first, so the first prefix match
// is the newest. See specs/scan-param-all-pages.md.
/** The store's scan list (newest first) — the one definition every page
 * shares, so the poll and error handling are the same wherever it mounts.
 * The list polls so an unpinned tab discovers new scans on its own; the
 * per-scan payloads are immutable once published, so they never refetch.
 * Store-scoped key, so switching stores swaps the whole payload set. */
export function useScans(store: Store): UseQueryResult<string[]> {
  return useQuery<string[]>({
    queryKey: ['scans', store.key],
    // Throw on non-OK (e.g. a 401 from the data proxy with no session) so
    // react-query holds it as an error rather than handing the error body
    // downstream — `scans` then stays `[]` and the error surfaces as a sign-in
    // prompt rather than crashing `scans.map`.
    queryFn: async () => {
      const r = await fetch(`${store.base}/scans.json`)
      if (!r.ok) throw Object.assign(new Error(`scans: ${r.status}`), { status: r.status })
      return r.json()
    },
    refetchInterval: SCANS_POLL_MS,
  })
}

export function useScan(store: Store): Scan {
  const [sel, setSel] = useUrlState('d', { encode: encodeSel, decode: decodeSel }, true)
  const scansQ = useScans(store)
  const scans = useMemo(() => scansQ.data ?? [], [scansQ.data])
  const dP = sel?.d
  const span = sel?.span
  const dMatches = useMemo(() => (dP ? scans.filter(s => s.startsWith(dP)) : []), [dP, scans])
  const asof = dMatches[0] ?? scans[0] ?? null
  const setRange = (v: string | undefined, ms: number | undefined) => {
    const d = v && v !== scans[0] ? v : undefined
    setSel(d || ms ? { ...(d ? { d } : {}), ...(ms ? { span: ms } : {}) } : undefined)
  }
  const setDP = (v: string | undefined) => setRange(v, span)
  const setSpan = (ms: number | undefined) => setRange(dP, ms)
  return { asof, scans, dMatches, dP, setDP, span, setSpan, setRange, scansQ }
}
