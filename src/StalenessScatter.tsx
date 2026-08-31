import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, ReactNode } from 'react'
import { DEFAULT_PALETTE } from './colors'
import { isoScoresForData, isoScoreSegment, logDomain, logPos, logTicks, radiusFor } from './scatter'
import { formatTbYears, pow10, SEC_PER_YEAR, TB } from './stats'
import { useHoverPin } from './useHoverPin'

/**
 * Log-log "triage frontier" scatter (spec: viz-widgets.md §3): one marker per
 * node, x = age, y = bytes, marker area ∝ a count channel (n_files / n_desc —
 * the ops/listing-pain signal). DIY SVG, no chart lib, patterned on
 * `<TimeSeries>`.
 *
 * The point of log-log: iso-sum-TB·years contours become straight −1-slope
 * lines, and they're *exact* rather than decorative (`y·x` = bytes × age =
 * the additive score from `stats.ts`). Upper-right of a labeled diagonal is
 * the delete-candidate frontier — big and stale beats big or stale.
 *
 * Nodes without a positive age *and* size can't be placed on log axes; they
 * are counted and reported under the plot rather than silently dropped.
 */

export interface StalenessScatterProps<T> {
  nodes: T[]
  /** Age in **seconds** (consumers: `now − mtime_mean`). */
  getAge: (n: T) => number | null | undefined
  /** Size in **bytes**. */
  getSize: (n: T) => number | null | undefined
  getLabel: (n: T) => string
  /** Marker *area* ∝ this (default: uniform). `n_files` is the intended signal. */
  getWeight?: (n: T) => number | null | undefined
  /** Marker fill. Default: the shared 8-slot palette, by index — same hues as `<Treemap>`. */
  getColor?: (n: T, i: number) => string | null | undefined
  /** Format bytes for the y axis / tooltip. Default: SI (1 TB = 1e12 B). */
  formatSize?: (bytes: number) => string
  /** Format an age in seconds for the x axis / tooltip. Default: `4d` / `7mo` / `2.3y`. */
  formatAge?: (seconds: number) => string
  /** Format a sum-TB·years score for diagonal labels / tooltip. */
  formatScore?: (tbYears: number) => string
  /** Tooltip body. Default: label + size + age + score. */
  renderTooltip?: (n: T) => ReactNode
  /** Click a marker (drill / route). */
  onNodeClick?: (n: T, event: React.MouseEvent) => void
  /** Draw the labeled iso-score diagonals. Default: true. */
  isoLines?: boolean
  /** Axis titles. Pass `null` to omit. Defaults: `age` / `size`. */
  xLabel?: string | null
  yLabel?: string | null
  className?: string
  style?: CSSProperties
  height?: number | string
}

const PAD = { top: 14, right: 18, bottom: 44, left: 62 }

// Rendered before the first ResizeObserver callback (and in SSR / jsdom,
// where there is no layout at all). Real browsers deliver an observation
// before paint, so this is never seen there.
const FALLBACK_DIMS = { w: 640, h: 320 }

const SI_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

/** SI bytes — 1 TB = 1e12 B, matching the `TB` the score is denominated in. */
function defaultFormatSize(bytes: number): string {
  if (!(bytes > 0)) return '0 B'
  const e = Math.min(SI_UNITS.length - 1, Math.floor(Math.log10(bytes) / 3))
  const v = bytes / pow10(3 * e)
  return `${Number(v.toPrecision(3))} ${SI_UNITS[e]}`
}

function defaultFormatAge(seconds: number): string {
  const days = seconds / 86_400
  if (days < 1) return `${Number((seconds / 3600).toPrecision(2))}h`
  if (days < 30) return `${Number(days.toPrecision(2))}d`
  if (days < 365) return `${Math.round(days / 30)}mo`
  return `${Number((days / 365.25).toPrecision(2))}y`
}

export function StalenessScatter<T>({
  nodes,
  getAge,
  getSize,
  getLabel,
  getWeight,
  getColor,
  formatSize = defaultFormatSize,
  formatAge = defaultFormatAge,
  formatScore = formatTbYears,
  renderTooltip,
  onNodeClick,
  isoLines = true,
  xLabel = 'age',
  yLabel = 'size',
  className,
  style,
  height,
}: StalenessScatterProps<T>) {
  const wrapRef = useRef<HTMLDivElement>(null)
  // The plot is measured separately from the wrapper so the footer note can
  // sit *below* it without stealing height from (or overlapping) the chart.
  const plotRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 0, h: 0 })

  useLayoutEffect(() => {
    const el = plotRef.current
    if (!el) return
    // Measure synchronously first: a ResizeObserver's initial delivery is not
    // guaranteed to arrive promptly (background/throttled tabs never got one
    // in practice), which otherwise left the plot stuck at its fallback size.
    setDims({ w: el.clientWidth, h: el.clientHeight })
    const ro = new ResizeObserver(() => setDims({ w: el.clientWidth, h: el.clientHeight }))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const pin = useHoverPin<number>({ excludeRefs: [wrapRef] })

  /** Plottable points, in axis units: x = years, y = TB (so x·y = TB·years). */
  const { points, hidden } = useMemo(() => {
    const points: { i: number; x: number; y: number; ageSec: number; bytes: number; node: T }[] = []
    let hidden = 0
    nodes.forEach((node, i) => {
      const ageSec = getAge(node)
      const bytes = getSize(node)
      if (ageSec == null || bytes == null || !(ageSec > 0) || !(bytes > 0)) {
        hidden++
        return
      }
      points.push({ i, x: ageSec / SEC_PER_YEAR, y: bytes / TB, ageSec, bytes, node })
    })
    return { points, hidden }
  }, [nodes, getAge, getSize])

  const xDomain = useMemo(() => logDomain(points.map(p => p.x)), [points])
  const yDomain = useMemo(() => logDomain(points.map(p => p.y)), [points])
  const maxWeight = useMemo(
    () => (getWeight ? points.reduce((m, p) => Math.max(m, getWeight(p.node) ?? 0), 0) : 0),
    [points, getWeight],
  )

  const w = dims.w || FALLBACK_DIMS.w
  const h = dims.h || FALLBACK_DIMS.h
  const plotW = Math.max(0, w - PAD.left - PAD.right)
  const plotH = Math.max(0, h - PAD.top - PAD.bottom)

  const xToPx = (x: number) => PAD.left + (xDomain ? logPos(x, xDomain) : 0) * plotW
  const yToPx = (y: number) => PAD.top + plotH - (yDomain ? logPos(y, yDomain) : 0) * plotH

  const active = pin.active
  const activePoint = active == null ? null : points.find(p => p.i === active) ?? null

  return (
    <div
      ref={wrapRef}
      className={'dt-scatter' + (className ? ` ${className}` : '')}
      style={{
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        height: height ?? '100%',
        ...style,
      }}
    >
      <div ref={plotRef} style={{ position: 'relative', flex: 1, minHeight: 0 }}>
      {xDomain && yDomain && (
        <svg width={w} height={h} style={{ display: 'block' }} role="img" aria-label="staleness scatter">
          {/* Y grid + ticks (bytes) */}
          {logTicks(yDomain).map(y => (
            <g key={`y${y}`}>
              <line
                x1={PAD.left}
                x2={PAD.left + plotW}
                y1={yToPx(y)}
                y2={yToPx(y)}
                stroke="var(--dt-scatter-grid, rgba(255,255,255,0.08))"
              />
              <text
                x={PAD.left - 6}
                y={yToPx(y)}
                textAnchor="end"
                dominantBaseline="middle"
                fontSize={10}
                fill="var(--dt-scatter-axis-ink, #8b949e)"
              >
                {formatSize(y * TB)}
              </text>
            </g>
          ))}
          {/* X grid + ticks (age) */}
          {logTicks(xDomain).map(x => (
            <g key={`x${x}`}>
              <line
                x1={xToPx(x)}
                x2={xToPx(x)}
                y1={PAD.top}
                y2={PAD.top + plotH}
                stroke="var(--dt-scatter-grid, rgba(255,255,255,0.08))"
              />
              <text
                x={xToPx(x)}
                y={PAD.top + plotH + 16}
                textAnchor="middle"
                fontSize={10}
                fill="var(--dt-scatter-axis-ink, #8b949e)"
              >
                {formatAge(x * SEC_PER_YEAR)}
              </text>
            </g>
          ))}
          {/* Iso-sum-TB·years diagonals — exact contours of size × age. */}
          {isoLines &&
            isoScoresForData(points.map(p => p.x * p.y), xDomain, yDomain).map(score => {
              const seg = isoScoreSegment(score, xDomain, yDomain)
              if (!seg) return null
              const [[xa, ya], [xb, yb]] = seg
              return (
                <g key={`iso${score}`} pointerEvents="none">
                  <line
                    x1={xToPx(xa)}
                    y1={yToPx(ya)}
                    x2={xToPx(xb)}
                    y2={yToPx(yb)}
                    stroke="var(--dt-scatter-iso, rgba(255,255,255,0.18))"
                    strokeDasharray="4 4"
                  />
                  <text
                    x={xToPx(xb) - 4}
                    y={yToPx(yb) - 4}
                    textAnchor="end"
                    fontSize={9}
                    fill="var(--dt-scatter-iso-ink, #6e7681)"
                  >
                    {formatScore(score)}
                  </text>
                </g>
              )
            })}
          {/* Axis lines */}
          <line
            x1={PAD.left}
            x2={PAD.left + plotW}
            y1={PAD.top + plotH}
            y2={PAD.top + plotH}
            stroke="var(--dt-scatter-axis, rgba(255,255,255,0.2))"
          />
          <line
            x1={PAD.left}
            x2={PAD.left}
            y1={PAD.top}
            y2={PAD.top + plotH}
            stroke="var(--dt-scatter-axis, rgba(255,255,255,0.2))"
          />
          {/* Axis titles */}
          {xLabel && (
            <text
              x={PAD.left + plotW / 2}
              y={PAD.top + plotH + 36}
              textAnchor="middle"
              fontSize={10}
              fill="var(--dt-scatter-axis-ink, #8b949e)"
            >
              {xLabel} →
            </text>
          )}
          {yLabel && (
            <text
              x={12}
              y={PAD.top + plotH / 2}
              textAnchor="middle"
              fontSize={10}
              fill="var(--dt-scatter-axis-ink, #8b949e)"
              transform={`rotate(-90, 12, ${PAD.top + plotH / 2})`}
            >
              {yLabel} →
            </text>
          )}
          {/* Markers — biggest first so small ones stay clickable on top. */}
          {[...points]
            .sort((a, b) => b.bytes - a.bytes)
            .map(p => {
              const color = getColor?.(p.node, p.i) ?? DEFAULT_PALETTE[p.i % DEFAULT_PALETTE.length]
              const isActive = active === p.i
              return (
                <circle
                  key={p.i}
                  className="dt-scatter-marker"
                  cx={xToPx(p.x)}
                  cy={yToPx(p.y)}
                  r={radiusFor(getWeight?.(p.node), maxWeight)}
                  fill={color}
                  fillOpacity={active == null || isActive ? 0.75 : 0.25}
                  stroke={isActive ? 'var(--dt-scatter-marker-active, #fff)' : 'none'}
                  strokeWidth={isActive ? 1.5 : 0}
                  style={{ cursor: onNodeClick ? 'pointer' : 'default' }}
                  onMouseEnter={() => pin.hover(p.i)}
                  onMouseLeave={() => pin.hover(null)}
                  onClick={e => {
                    pin.togglePin(p.i)
                    onNodeClick?.(p.node, e)
                  }}
                >
                  <title>{getLabel(p.node)}</title>
                </circle>
              )
            })}
        </svg>
      )}
      {/* Tooltip */}
      {activePoint && (
        <div
          className="dt-scatter-tip"
          style={{
            position: 'absolute',
            left: Math.min(xToPx(activePoint.x) + 10, Math.max(0, w - 190)),
            top: Math.max(0, yToPx(activePoint.y) - 12),
            background: 'var(--dt-scatter-tip-bg, rgba(20,20,24,0.94))',
            color: 'var(--dt-scatter-tip-ink, #e6e6ea)',
            border: '1px solid var(--dt-scatter-tip-border, #333)',
            borderRadius: 4,
            padding: '4px 8px',
            fontSize: 11,
            pointerEvents: 'none',
            whiteSpace: 'nowrap',
            zIndex: 1,
          }}
        >
          {renderTooltip?.(activePoint.node) ?? (
            <>
              <div style={{ fontWeight: 500 }}>{getLabel(activePoint.node)}</div>
              <div style={{ opacity: 0.75 }}>
                {formatSize(activePoint.bytes)} · {formatAge(activePoint.ageSec)}
              </div>
              <div style={{ opacity: 0.6 }}>{formatScore(activePoint.x * activePoint.y)}</div>
            </>
          )}
        </div>
      )}
      </div>
      {hidden > 0 && (
        <div
          className="dt-scatter-note"
          style={{ flex: 'none', fontSize: 11, opacity: 0.6, marginTop: 2 }}
        >
          {hidden} not plotted (no age or zero size)
        </div>
      )}
    </div>
  )
}
