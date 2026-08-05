import { useEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, ReactNode } from 'react'

/**
 * DIY SVG line/area chart. No Plotly, no recharts, no d3 — the whole widget
 * is under ~180 LOC. Designed for `/api/scans/history` (bytes vs. snapshot
 * date) but the accessor API keeps it usable for any `{x, y}` series.
 *
 * What ships:
 *   - Multi-series overlay (default: one series). Each series is an area
 *     under a line, colored from an 8-slot palette (mirrors the Treemap's).
 *   - Auto y-axis scaling from the data; optional log scale via `yScale`.
 *   - Hover-follow crosshair with a per-x tooltip listing each series's
 *     value at that x.
 *   - ResizeObserver-driven layout (no fixed pixel size in the component;
 *     wrap in a container with a height and it fills).
 *   - Zero external dep besides React.
 */

export interface Series<T> {
  key: string
  label?: string
  color?: string
  points: T[]
}

export interface TimeSeriesProps<T> {
  series: Series<T>[]
  getX: (p: T) => number
  getY: (p: T) => number
  /** Format an X tick (default: `new Date(x).toLocaleDateString()`). */
  formatX?: (x: number) => string
  /** Format a Y tick / tooltip value. */
  formatY?: (y: number) => string
  /** `linear` (default) or `log`. */
  yScale?: 'linear' | 'log'
  /** Y-axis label (rendered vertically). */
  yLabel?: string
  /** Approximate tick counts. */
  xTicks?: number
  yTicks?: number
  /** Show area fill under each line (default true). */
  area?: boolean
  /** Extra CSS on the outer wrapper. */
  className?: string
  style?: CSSProperties
  /** Chart height in px (default: fill parent via 100%). */
  height?: number | string
}

const DEFAULT_COLORS = [
  'hsl(210 70% 55%)',
  'hsl(30 80% 55%)',
  'hsl(160 55% 45%)',
  'hsl(350 65% 55%)',
  'hsl(280 55% 55%)',
  'hsl(50 75% 55%)',
  'hsl(180 50% 45%)',
  'hsl(120 45% 50%)',
]

const PAD = { top: 12, right: 16, bottom: 24, left: 56 }

function niceTicks(min: number, max: number, count: number, log = false): number[] {
  if (log) {
    const lmin = Math.log10(Math.max(1, min))
    const lmax = Math.log10(Math.max(10, max))
    const step = Math.max(1, Math.ceil((lmax - lmin) / count))
    const out: number[] = []
    for (let e = Math.ceil(lmin); e <= Math.floor(lmax); e += step) out.push(10 ** e)
    return out
  }
  if (min === max) return [min]
  const range = max - min
  const rawStep = range / Math.max(1, count)
  const mag = 10 ** Math.floor(Math.log10(rawStep))
  const norm = rawStep / mag
  const stepMult = norm < 1.5 ? 1 : norm < 3 ? 2 : norm < 7 ? 5 : 10
  const step = stepMult * mag
  const first = Math.ceil(min / step) * step
  const out: number[] = []
  for (let v = first; v <= max + step / 100; v += step) out.push(Number(v.toFixed(10)))
  return out
}

export function TimeSeries<T>({
  series,
  getX,
  getY,
  formatX = x => new Date(x).toLocaleDateString(),
  formatY = y => y.toLocaleString('en-US'),
  yScale = 'linear',
  yLabel,
  xTicks = 5,
  yTicks = 4,
  area = true,
  className,
  style,
  height,
}: TimeSeriesProps<T>) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 0, h: 0 })

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setDims({ w: el.clientWidth, h: el.clientHeight }))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const [hoverX, setHoverX] = useState<number | null>(null)

  const { xMin, xMax, yMin, yMax } = useMemo(() => {
    const xs: number[] = []
    const ys: number[] = []
    for (const s of series) for (const p of s.points) {
      xs.push(getX(p))
      ys.push(getY(p))
    }
    if (xs.length === 0) return { xMin: 0, xMax: 1, yMin: 0, yMax: 1 }
    const yMinRaw = Math.min(...ys)
    const yMaxRaw = Math.max(...ys)
    return {
      xMin: Math.min(...xs),
      xMax: Math.max(...xs),
      yMin: yScale === 'log' ? Math.max(1, yMinRaw) : 0,
      yMax: yMaxRaw > 0 ? yMaxRaw * 1.05 : 1,
    }
  }, [series, getX, getY, yScale])

  const plotW = Math.max(0, dims.w - PAD.left - PAD.right)
  const plotH = Math.max(0, dims.h - PAD.top - PAD.bottom)

  const xToPx = (x: number) => PAD.left + ((x - xMin) / Math.max(1, xMax - xMin)) * plotW
  const yToPx = (y: number) => {
    if (yScale === 'log') {
      const lmin = Math.log10(Math.max(1, yMin))
      const lmax = Math.log10(Math.max(10, yMax))
      const ly = Math.log10(Math.max(1, y))
      return PAD.top + plotH - ((ly - lmin) / Math.max(0.001, lmax - lmin)) * plotH
    }
    return PAD.top + plotH - ((y - yMin) / Math.max(0.001, yMax - yMin)) * plotH
  }

  const xTickVals = niceTicks(xMin, xMax, xTicks)
  const yTickVals = niceTicks(yMin, yMax, yTicks, yScale === 'log')

  // Collect all distinct x's across series for hover snapping.
  const allXs = useMemo(() => {
    const s = new Set<number>()
    for (const ser of series) for (const p of ser.points) s.add(getX(p))
    return [...s].sort((a, b) => a - b)
  }, [series, getX])

  const snapX = (px: number): number | null => {
    if (allXs.length === 0) return null
    // find nearest x in plot coords
    let best = allXs[0]
    let bestD = Math.abs(xToPx(best) - px)
    for (const x of allXs) {
      const d = Math.abs(xToPx(x) - px)
      if (d < bestD) { bestD = d; best = x }
    }
    return best
  }

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    setHoverX(snapX(e.clientX - rect.left))
  }

  const hoverPoints: { color: string; label: string; y: number | null }[] = hoverX == null
    ? []
    : series.map((s, i) => {
      const pt = s.points.find(p => getX(p) === hoverX)
      return {
        color: s.color ?? DEFAULT_COLORS[i % DEFAULT_COLORS.length],
        label: s.label ?? s.key,
        y: pt ? getY(pt) : null,
      }
    })

  return (
    <div
      ref={wrapRef}
      className={'dt-timeseries' + (className ? ` ${className}` : '')}
      style={{ position: 'relative', width: '100%', height: height ?? '100%', ...style }}
    >
      {dims.w > 0 && dims.h > 0 && (
        <svg
          width={dims.w}
          height={dims.h}
          onMouseMove={onMove}
          onMouseLeave={() => setHoverX(null)}
          style={{ display: 'block' }}
        >
          {/* Y grid + ticks */}
          {yTickVals.map((y, i) => (
            <g key={`y${i}`}>
              <line
                x1={PAD.left}
                x2={PAD.left + plotW}
                y1={yToPx(y)}
                y2={yToPx(y)}
                stroke="var(--dt-ts-grid, rgba(255,255,255,0.08))"
              />
              <text
                x={PAD.left - 6}
                y={yToPx(y)}
                textAnchor="end"
                dominantBaseline="middle"
                fontSize={10}
                fill="var(--dt-ts-axis-ink, #8b949e)"
              >
                {formatY(y)}
              </text>
            </g>
          ))}
          {/* X ticks */}
          {xTickVals.map((x, i) => (
            <g key={`x${i}`}>
              <line
                x1={xToPx(x)}
                x2={xToPx(x)}
                y1={PAD.top + plotH}
                y2={PAD.top + plotH + 4}
                stroke="var(--dt-ts-axis, rgba(255,255,255,0.2))"
              />
              <text
                x={xToPx(x)}
                y={PAD.top + plotH + 16}
                textAnchor="middle"
                fontSize={10}
                fill="var(--dt-ts-axis-ink, #8b949e)"
              >
                {formatX(x)}
              </text>
            </g>
          ))}
          {/* Axis lines */}
          <line
            x1={PAD.left}
            x2={PAD.left + plotW}
            y1={PAD.top + plotH}
            y2={PAD.top + plotH}
            stroke="var(--dt-ts-axis, rgba(255,255,255,0.2))"
          />
          <line
            x1={PAD.left}
            x2={PAD.left}
            y1={PAD.top}
            y2={PAD.top + plotH}
            stroke="var(--dt-ts-axis, rgba(255,255,255,0.2))"
          />
          {yLabel && (
            <text
              x={12}
              y={PAD.top + plotH / 2}
              textAnchor="middle"
              fontSize={10}
              fill="var(--dt-ts-axis-ink, #8b949e)"
              transform={`rotate(-90, 12, ${PAD.top + plotH / 2})`}
            >
              {yLabel}
            </text>
          )}
          {/* Series */}
          {series.map((s, si) => {
            if (s.points.length === 0) return null
            const color = s.color ?? DEFAULT_COLORS[si % DEFAULT_COLORS.length]
            const sortedPts = [...s.points].sort((a, b) => getX(a) - getX(b))
            const linePath = sortedPts
              .map((p, i) => `${i === 0 ? 'M' : 'L'} ${xToPx(getX(p))} ${yToPx(getY(p))}`)
              .join(' ')
            const areaPath = area
              ? `${linePath} L ${xToPx(getX(sortedPts[sortedPts.length - 1]))} ${PAD.top + plotH} L ${xToPx(getX(sortedPts[0]))} ${PAD.top + plotH} Z`
              : null
            return (
              <g key={s.key}>
                {areaPath && <path d={areaPath} fill={color} fillOpacity={0.15} />}
                <path d={linePath} fill="none" stroke={color} strokeWidth={1.75} />
                {sortedPts.map((p, i) => (
                  <circle
                    key={i}
                    cx={xToPx(getX(p))}
                    cy={yToPx(getY(p))}
                    r={2.5}
                    fill={color}
                  />
                ))}
              </g>
            )
          })}
          {/* Hover crosshair */}
          {hoverX != null && (
            <line
              x1={xToPx(hoverX)}
              x2={xToPx(hoverX)}
              y1={PAD.top}
              y2={PAD.top + plotH}
              stroke="var(--dt-ts-cross, rgba(255,255,255,0.4))"
              strokeDasharray="3 3"
              pointerEvents="none"
            />
          )}
        </svg>
      )}
      {/* Tooltip */}
      {hoverX != null && hoverPoints.some(p => p.y != null) && (
        <div
          style={{
            position: 'absolute',
            left: Math.min(xToPx(hoverX) + 8, dims.w - 180),
            top: PAD.top + 4,
            background: 'var(--dt-ts-tip-bg, rgba(20,20,24,0.94))',
            color: 'var(--dt-ts-tip-ink, #e6e6ea)',
            border: '1px solid var(--dt-ts-tip-border, #333)',
            borderRadius: 4,
            padding: '4px 8px',
            fontSize: 11,
            pointerEvents: 'none',
            whiteSpace: 'nowrap',
          }}
        >
          <div style={{ opacity: 0.7, marginBottom: 2 }}>{formatX(hoverX)}</div>
          {hoverPoints.map((p, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ display: 'inline-block', width: 8, height: 8, background: p.color, borderRadius: 2 }} />
              <span style={{ minWidth: 60 }}>{p.label}</span>
              <b>{p.y != null ? formatY(p.y) : '—'}</b>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/** Convenience wrapper: single-series bytes-over-time. Used by disk-tree. */
export function BytesOverTime({
  points,
  formatBytes,
  height,
}: {
  points: { time: string | number; bytes: number | null }[]
  formatBytes: (n: number) => string
  height?: number | string
}): ReactNode {
  const cleaned = points
    .map(p => ({ t: typeof p.time === 'number' ? p.time : Date.parse(p.time), bytes: p.bytes }))
    .filter((p): p is { t: number; bytes: number } => Number.isFinite(p.t) && typeof p.bytes === 'number')
  return (
    <TimeSeries
      series={[{ key: 'bytes', label: 'size', points: cleaned }]}
      getX={p => p.t}
      getY={p => p.bytes}
      formatY={formatBytes}
      height={height}
    />
  )
}
