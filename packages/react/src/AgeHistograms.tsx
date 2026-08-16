import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, ReactNode } from 'react'
import { DEFAULT_PALETTE } from './colors'
import { bytesOlderThan, peakBin, timeTicks, totalBytes } from './histogram'
import { useHoverPin } from './useHoverPin'

/**
 * Byte-weighted age histograms, one column per item (spec: viz-widgets.md §4).
 *
 * x = items (children of a drill dir), y = mtime, and each item's bars are
 * **weighted by bytes**, sharing one bar-width scale — so a column's total
 * area is its byte total, and the area below a threshold line is exactly the
 * bytes reclaimable by deleting everything older than it. A mean can't show
 * you that a directory is half ancient and half fresh; this can.
 *
 * Drag anywhere in the plot to move the threshold; `onThresholdChange` reports
 * the time and the reclaimable total.
 */

export interface AgeHistogramsProps<T> {
  items: T[]
  /** Shared bin edges, epoch seconds ascending; length = bins + 1. */
  edges: number[]
  /** Bytes per bin for an item; length must be `edges.length - 1`. */
  getBins: (item: T) => number[]
  getLabel: (item: T) => string
  /** Bar color. Default: the shared 8-slot palette, by index. */
  getColor?: (item: T, i: number) => string | null | undefined
  formatSize?: (bytes: number) => string
  /** Format an epoch-seconds tick / readout. Default: locale date. */
  formatTime?: (epochSec: number) => string
  /** Threshold line, epoch seconds. Omit for none. */
  threshold?: number | null
  /** Fired on click/drag in the plot: the new threshold and Σ bytes older than it. */
  onThresholdChange?: (epochSec: number, reclaimableBytes: number) => void
  /**
   * Scale each column against its own largest bin instead of the shared peak.
   * Makes every column's *shape* legible when byte totals span orders of
   * magnitude — at the cost of the area-∝-bytes invariant, so widths are no
   * longer comparable between columns. Label it as such in the UI.
   */
  normalize?: boolean
  /** Tooltip body for a hovered column. Default: label + total + file-age span. */
  renderTooltip?: (item: T) => ReactNode
  onItemClick?: (item: T, event: React.MouseEvent) => void
  className?: string
  style?: CSSProperties
  height?: number | string
}

const PAD = { top: 14, right: 16, bottom: 46, left: 74 }
const FALLBACK_DIMS = { w: 640, h: 320 }

const SI_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

function defaultFormatSize(bytes: number): string {
  if (!(bytes > 0)) return '0 B'
  let v = bytes
  let e = 0
  while (v >= 1000 && e < SI_UNITS.length - 1) {
    v /= 1000
    e++
  }
  return `${Number(v.toPrecision(3))} ${SI_UNITS[e]}`
}

const defaultFormatTime = (s: number) => new Date(s * 1000).toLocaleDateString()

/** Truncate to what fits `px` at ~6px/char, with an ellipsis when cut. */
function fitLabel(label: string, px: number): string {
  const max = Math.max(1, Math.floor(px / 6))
  return label.length <= max ? label : label.slice(0, Math.max(1, max - 1)) + '…'
}

export function AgeHistograms<T>({
  items,
  edges,
  getBins,
  getLabel,
  getColor,
  formatSize = defaultFormatSize,
  formatTime = defaultFormatTime,
  threshold,
  onThresholdChange,
  normalize = false,
  renderTooltip,
  onItemClick,
  className,
  style,
  height,
}: AgeHistogramsProps<T>) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 0, h: 0 })
  const [dragging, setDragging] = useState(false)

  useLayoutEffect(() => {
    const el = plotRef.current
    if (!el) return
    // Measure synchronously — a ResizeObserver's initial delivery can be
    // arbitrarily late (or never, in a throttled tab).
    setDims({ w: el.clientWidth, h: el.clientHeight })
    const ro = new ResizeObserver(() => setDims({ w: el.clientWidth, h: el.clientHeight }))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const pin = useHoverPin<number>({ excludeRefs: [wrapRef] })

  const nBins = Math.max(0, edges.length - 1)
  const binsByItem = useMemo(() => items.map(getBins), [items, getBins])
  const peak = useMemo(() => peakBin(binsByItem), [binsByItem])

  const w = dims.w || FALLBACK_DIMS.w
  const h = dims.h || FALLBACK_DIMS.h
  const plotW = Math.max(0, w - PAD.left - PAD.right)
  const plotH = Math.max(0, h - PAD.top - PAD.bottom)

  const tLo = edges[0]
  const tHi = edges[edges.length - 1]
  /** Time → px, newest at the top (so "older" reads downward, like sediment). */
  const tToPx = (t: number) =>
    PAD.top + plotH - (tHi > tLo ? ((t - tLo) / (tHi - tLo)) * plotH : plotH / 2)
  const pxToT = (py: number) =>
    tHi > tLo ? tLo + ((PAD.top + plotH - py) / plotH) * (tHi - tLo) : tLo

  const slotW = items.length ? plotW / items.length : plotW
  const binH = nBins ? plotH / nBins : plotH

  const reclaimable = useMemo(
    () =>
      threshold == null
        ? 0
        : binsByItem.reduce((sum, bins) => sum + bytesOlderThan(edges, bins, threshold), 0),
    [binsByItem, edges, threshold],
  )

  const emitThreshold = (clientY: number, target: SVGSVGElement) => {
    if (!onThresholdChange) return
    const rect = target.getBoundingClientRect()
    const t = Math.max(tLo, Math.min(tHi, pxToT(clientY - rect.top)))
    onThresholdChange(t, binsByItem.reduce((sum, bins) => sum + bytesOlderThan(edges, bins, t), 0))
  }

  const active = pin.active
  const activeItem = active == null ? null : items[active] ?? null

  return (
    <div
      ref={wrapRef}
      className={'dt-histograms' + (className ? ` ${className}` : '')}
      style={{ display: 'flex', flexDirection: 'column', width: '100%', height: height ?? '100%', ...style }}
    >
      <div ref={plotRef} style={{ position: 'relative', flex: 1, minHeight: 0 }}>
        {items.length > 0 && nBins > 0 && (
          <svg
            width={w}
            height={h}
            style={{ display: 'block', cursor: onThresholdChange ? 'ns-resize' : 'default' }}
            role="img"
            aria-label="byte-weighted age histograms"
            onMouseDown={e => {
              setDragging(true)
              emitThreshold(e.clientY, e.currentTarget)
            }}
            onMouseMove={e => dragging && emitThreshold(e.clientY, e.currentTarget)}
            onMouseUp={() => setDragging(false)}
            onMouseLeave={() => setDragging(false)}
          >
            {/* Time ticks */}
            {timeTicks(tLo, tHi).map(t => (
              <g key={`t${t}`}>
                <line
                  x1={PAD.left}
                  x2={PAD.left + plotW}
                  y1={tToPx(t)}
                  y2={tToPx(t)}
                  stroke="var(--dt-hist-grid, rgba(255,255,255,0.08))"
                />
                <text
                  x={PAD.left - 6}
                  y={tToPx(t)}
                  textAnchor="end"
                  dominantBaseline="middle"
                  fontSize={10}
                  fill="var(--dt-hist-axis-ink, #8b949e)"
                >
                  {formatTime(t)}
                </text>
              </g>
            ))}
            {/* Columns */}
            {items.map((item, i) => {
              const bins = binsByItem[i]
              const color = getColor?.(item, i) ?? DEFAULT_PALETTE[i % DEFAULT_PALETTE.length]
              const cx = PAD.left + slotW * (i + 0.5)
              const maxHalf = (slotW * 0.9) / 2
              const isActive = active === i
              const scale = normalize ? Math.max(...bins, 0) : peak
              return (
                <g
                  key={i}
                  className="dt-hist-col"
                  onMouseEnter={() => pin.hover(i)}
                  onMouseLeave={() => pin.hover(null)}
                  onClick={e => {
                    pin.togglePin(i)
                    onItemClick?.(item, e)
                  }}
                  style={{ cursor: onItemClick ? 'pointer' : undefined }}
                >
                  {/* Full-slot hit area: thin bars are otherwise unhoverable. */}
                  <rect
                    x={PAD.left + slotW * i}
                    y={PAD.top}
                    width={slotW}
                    height={plotH}
                    fill="transparent"
                  />
                  {bins.map((bytes, b) => {
                    if (!(bytes > 0) || scale <= 0) return null
                    const half = (bytes / scale) * maxHalf
                    const yTop = tToPx(edges[b + 1])
                    return (
                      <rect
                        key={b}
                        className="dt-hist-bar"
                        x={cx - half}
                        y={yTop}
                        width={half * 2}
                        height={Math.max(1, binH)}
                        fill={color}
                        fillOpacity={active == null || isActive ? 0.8 : 0.28}
                      />
                    )
                  })}
                  <text
                    x={cx}
                    y={PAD.top + plotH + 14}
                    textAnchor="middle"
                    fontSize={10}
                    fill="var(--dt-hist-axis-ink, #8b949e)"
                  >
                    {fitLabel(getLabel(item), slotW)}
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
              stroke="var(--dt-hist-axis, rgba(255,255,255,0.2))"
            />
            <line
              x1={PAD.left}
              x2={PAD.left}
              y1={PAD.top}
              y2={PAD.top + plotH}
              stroke="var(--dt-hist-axis, rgba(255,255,255,0.2))"
            />
            {/* Threshold: everything below the line is older, i.e. reclaimable. */}
            {threshold != null && (
              <g className="dt-hist-threshold" pointerEvents="none">
                <rect
                  x={PAD.left}
                  y={tToPx(threshold)}
                  width={plotW}
                  height={Math.max(0, PAD.top + plotH - tToPx(threshold))}
                  fill="var(--dt-hist-reclaim, rgba(248,81,73,0.10))"
                />
                <line
                  x1={PAD.left}
                  x2={PAD.left + plotW}
                  y1={tToPx(threshold)}
                  y2={tToPx(threshold)}
                  stroke="var(--dt-hist-threshold, #f85149)"
                  strokeDasharray="4 3"
                />
                <text
                  x={PAD.left + plotW}
                  y={tToPx(threshold) - 4}
                  textAnchor="end"
                  fontSize={10}
                  fill="var(--dt-hist-threshold, #f85149)"
                >
                  {formatSize(reclaimable)} older than {formatTime(threshold)}
                </text>
              </g>
            )}
          </svg>
        )}
        {activeItem != null && (
          <div
            className="dt-hist-tip"
            style={{
              position: 'absolute',
              left: Math.min(PAD.left + slotW * ((active ?? 0) + 0.5) + 8, Math.max(0, w - 180)),
              top: PAD.top + 4,
              background: 'var(--dt-hist-tip-bg, rgba(20,20,24,0.94))',
              color: 'var(--dt-hist-tip-ink, #e6e6ea)',
              border: '1px solid var(--dt-hist-tip-border, #333)',
              borderRadius: 4,
              padding: '4px 8px',
              fontSize: 11,
              pointerEvents: 'none',
              whiteSpace: 'nowrap',
              zIndex: 1,
            }}
          >
            {renderTooltip?.(activeItem) ?? (
              <>
                <div style={{ fontWeight: 500 }}>{getLabel(activeItem)}</div>
                <div style={{ opacity: 0.75 }}>{formatSize(totalBytes(binsByItem[active ?? 0]))}</div>
                {threshold != null && (
                  <div style={{ opacity: 0.6 }}>
                    {formatSize(bytesOlderThan(edges, binsByItem[active ?? 0], threshold))} older
                  </div>
                )}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
