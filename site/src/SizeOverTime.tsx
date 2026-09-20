import { Explain } from './Help'
import { TimeSeries } from '@disk-tree/react'
import type { Annotation } from '@disk-tree/react'
import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import { boolParam, stringParam, useUrlState } from 'use-prms'
import { shortName } from './UserChip'
import { useUnits } from './units'
import { Skeleton } from './Busy'

// Stored bytes over the historical scans, scoped exactly like the map: the
// drilled prefix, a user, or an owner pool (`/api/series` — one row read per
// scan in that scan's index tiers; specs/view-serving.md §1). Nothing is
// precomputed per prefix and nothing is floored.

interface Pt { x: number; y: number }
interface Series { path: string; points: { date: string; b: number; o: number }[] }

// Nice y-ticks aligned to the *display* unit: a base-10-nice byte value (1e15)
// is an ugly binary label (909 TiB), so nice-tick in the unit's own base
// (1024 for IEC → 1024/2048/3072 TiB; 1000 for SI → round TB/PB).
// `min` > 0 = a fitted axis: ticks cover [min, max] at the same unit-nice step.
const unitTicks = (min: number, max: number, base: number, count = 4): number[] => {
  if (max <= 0) return [0]
  const span = Math.max(max - min, max * 1e-6)
  const scale = base ** Math.floor(Math.log(max) / Math.log(base))
  const rawStep = span / scale / count
  const mag = 10 ** Math.floor(Math.log10(rawStep))
  const norm = rawStep / mag
  const step = (norm < 1.5 ? 1 : norm < 3 ? 2 : norm < 7 ? 5 : 10) * mag * scale
  const out: number[] = []
  for (let v = Math.ceil(min / step) * step; v <= max + step / 100; v += step) out.push(v)
  return out
}

type YFrom = 'data' | 'zero'
type XRange = '1w' | '1m' | 'all'
const X_RANGES: [XRange, number | null][] = [['1w', 7], ['1m', 30], ['all', null]]

// The x-range (`?xr=1w|1m`; all = default): the last N days before the latest
// scan. A phone's chart is too narrow to read months of scans at the right
// edge, and a recent-only view sharpens the last week's movement.
function XRangeToggle({ v, set }: { v: XRange; set: (r: XRange) => void }) {
  return (
    <span className="gran" role="radiogroup" aria-label="X-axis range">
      <span className="lbl">range</span>
      {X_RANGES.map(([r, days]) => (
        <Explain key={r} text={days ? `Only the last ${days === 7 ? 'week' : '30 days'} of scans` : 'Every scan'}>
          <button role="radio" aria-checked={v === r} className={v === r ? 'on' : ''} onClick={() => set(r)}>{r}</button>
        </Explain>
      ))}
    </span>
  )
}

// y-axis origin toggle (`?y0` — from-zero on): fit the data (default — a ~1%
// wiggle on 3 PiB is invisible from zero) or anchor at zero (honest
// proportions).
function YFromToggle({ v, set }: { v: YFrom; set: (y: YFrom) => void }) {
  return (
    <span className="gran" role="radiogroup" aria-label="Y-axis range">
      <span className="lbl">y-axis</span>
      {(['data', 'zero'] as YFrom[]).map(y => (
        <Explain key={y} text={y === 'data' ? 'Fit the y-range to the data (a small movement in a large total stays visible)' : 'Start the y-axis at zero (honest proportions)'}>
          <button role="radio" aria-checked={v === y} className={v === y ? 'on' : ''} onClick={() => set(y)}>{y === 'data' ? 'fit' : 'from 0'}</button>
        </Explain>
      ))}
    </span>
  )
}

// Points sit at UTC midnight of each scan's calendar date, so labels format
// in UTC too — a local-time render shows the 8/23 scan as “Aug 22” in the US.
const dateOfX = (x: number) => new Date(x).toISOString().slice(0, 10)
const fmtX = (x: number) => new Date(x).toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })
const xOfScan = (d: string) => new Date(d.slice(0, 10)).getTime()

export function SizeOverTime({ scans, prefix, user, pool, onPickDate, onBrush, window: win }: {
  scans: string[]
  prefix: string
  /** The owner axis's user: their bytes under `prefix`, per scan. */
  user?: string | null
  /** The owner axis's pool — `unowned` = bytes no person owns, `owned`
   * = bytes some person owns — under `prefix`, per scan. `user` wins. */
  pool?: 'unowned' | 'owned' | null
  /** Click a point → view the page as of that scan (pins `?d=`). */
  onPickDate?: (date: string) => void
  /** Drag across the chart → make [from, to] the page's diff window. */
  onBrush?: (from: string, to: string) => void
  /** The page's current diff window (scan ids), shaded on the chart. */
  window?: [string, string]
}) {
  const { fmtBytes, units } = useUnits()
  const [y0P, setY0P] = useUrlState('y0', boolParam)
  const yFrom: YFrom = y0P ? 'zero' : 'data'
  const setYFrom = (y: YFrom) => setY0P(y === 'zero')
  const [xrP, setXrP] = useUrlState('xr', stringParam())
  const xRange: XRange = xrP === '1w' || xrP === '1m' ? xrP : 'all'
  const setXRange = (r: XRange) => setXrP(r === 'all' ? undefined : r)

  const scope = user ? `&lens=user:${encodeURIComponent(user)}` : pool ? `&o=${pool}` : ''
  const seriesQ = useQuery<Series>({
    queryKey: ['series', prefix, scope, scans.length],
    enabled: scans.length > 1,
    staleTime: 5 * 60_000,
    queryFn: async () => {
      const r = await fetch(`/api/series?path=${encodeURIComponent(prefix)}${scope}`, { credentials: 'include' })
      if (!r.ok) throw new Error(`series: ${r.status}`)
      return r.json()
    },
  })

  const label = user ? shortName(user) : pool ?? (prefix || 'total')
  // The x-range cut: points on or after (latest − N days).
  const xFrom = useMemo(() => {
    const days = X_RANGES.find(([r]) => r === xRange)![1]
    const last = seriesQ.data?.points.reduce((m, p) => Math.max(m, xOfScan(p.date)), 0) ?? 0
    return days ? last - days * 86_400_000 : -Infinity
  }, [seriesQ.data, xRange])
  const series = useMemo(() => {
    const pts = (seriesQ.data?.points ?? [])
      .map(p => ({ x: xOfScan(p.date), y: p.b }))
      .filter(p => p.x >= xFrom)
      .sort((a, b) => a.x - b.x)
    if (pts.length < 2) return []
    return [{ key: 'scoped', label, color: 'var(--s1)', points: pts }]
  }, [seriesQ.data, label, xFrom])
  // A scope that owns nothing here in any scan is a flat zero line — say so
  // instead of drawing an empty axis.
  const allZero = series.length === 1 && series[0].points.every(p => p.y === 0)

  // Callouts at the points a reader looks for first: the ends of the series
  // and its extremes. Coinciding roles (first is also max) share one label.
  const annotations = useMemo((): Annotation[] => {
    if (series.length !== 1) return []
    const pts = series[0].points
    if (pts.length < 2) return []
    let lo = pts[0]
    let hi = pts[0]
    for (const p of pts) {
      if (p.y < lo.y) lo = p
      if (p.y > hi.y) hi = p
    }
    const picks = new Map<Pt, boolean>() // point → below?
    picks.set(hi, false)
    picks.set(lo, true)
    for (const p of [pts[0], pts[pts.length - 1]]) if (!picks.has(p)) picks.set(p, p.y < (lo.y + hi.y) / 2)
    return [...picks].map(([p, below]) => ({ x: p.x, y: p.y, label: fmtBytes(p.y), below }))
  }, [series, fmtBytes])

  const yTickValues = useMemo(() => {
    const ys = series.flatMap(s => s.points.map(p => p.y))
    const max = Math.max(0, ...ys)
    const min = yFrom === 'data' && ys.length ? Math.min(...ys) : 0
    // Fit mode pads 5% each side (TimeSeries), so tick that slightly wider range.
    const pad = yFrom === 'data' ? (max - min) * 0.05 : 0
    return unitTicks(Math.max(0, min - pad), max + pad, units === 'iec' ? 1024 : 1000)
  }, [series, units, yFrom])

  if (scans.length < 2) return null
  return (
    <section id="over-time">
      <h2>Size over time <XRangeToggle v={xRange} set={setXRange} /><YFromToggle v={yFrom} set={setYFrom} /></h2>
      <p className="sub">
        {user
          ? <><b>{shortName(user)}</b>’s bytes{prefix ? <> under <code>{prefix}</code></> : ''} per scan.</>
          : pool === 'unowned'
            ? <>Bytes no person owns{prefix ? <> under <code>{prefix}</code></> : ''}, per scan.</>
            : pool === 'owned'
              ? <>Bytes owned by a person{prefix ? <> under <code>{prefix}</code></> : ''}, per scan.</>
              : prefix
                ? <>Stored bytes under <code>{prefix}</code> per scan.</>
                : <>Total stored bytes per scan (fleet-wide).</>}
        {' '}Each point is that scan’s own index row — exact, at any depth.
        {seriesQ.isError && <> <i>(series unavailable)</i></>}
      </p>
      {allZero ? (
        <p className="loading">
          {user ? <><b>{shortName(user)}</b> owns nothing{prefix ? <> under <code>{prefix}</code></> : ''} in any scan</>
            : pool === 'unowned' ? <>nothing{prefix ? <> under <code>{prefix}</code></> : ''} is unowned in any scan</>
              : <>nothing{prefix ? <> under <code>{prefix}</code></> : ''} in any scan</>}
        </p>
      ) : series.length > 0 ? (
        <TimeSeries<Pt>
          series={series}
          getX={p => p.x}
          getY={p => p.y}
          formatY={fmtBytes}
          formatX={fmtX}
          yTickValues={yTickValues}
          yFrom={yFrom}
          yLabel="stored bytes"
          height={220}
          annotations={annotations}
          onPickX={onPickDate && (x => onPickDate(dateOfX(x)))}
          onBrush={onBrush && ((x0, x1) => onBrush(dateOfX(x0), dateOfX(x1)))}
          window={win && [xOfScan(win[0]), xOfScan(win[1])]}
        />
      ) : (
        seriesQ.isLoading ? <Skeleton height={220} label="loading series…" /> : <p className="loading">fewer than two scans hold this path</p>
      )}
    </section>
  )
}
