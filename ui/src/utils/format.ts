/**
 * Shared formatting utilities for the UI
 */
import { getUnits } from './units'

/**
 * Format a timestamp (Date string, number, or null) as relative time
 * Numbers are auto-detected as Unix seconds (< 1e11) or milliseconds (>= 1e11)
 */
export function timeAgo(value: string | number | null | undefined): string {
  if (value == null) return '-'

  let ms: number
  if (typeof value === 'string') {
    ms = new Date(value).getTime()
  } else {
    // Auto-detect seconds vs milliseconds (Unix timestamps are < 1e11 until year 5138)
    ms = value < 1e11 ? value * 1000 : value
  }

  const seconds = Math.floor((Date.now() - ms) / 1000)

  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  if (days < 30) return `${days}d ago`
  const months = Math.floor(days / 30)
  if (months < 12) return `${months}mo ago`
  const years = Math.floor(months / 12)
  return `${years}y ago`
}

/**
 * Format bytes compactly — `1.5 G` (SI, 10⁹) or `1.5 Gi` (IEC, 2³⁰) per the
 * units preference (`utils/units.ts`); bare bytes as `512 B`.
 */
export function formatSize(bytes: number | null | undefined): string {
  if (bytes == null) return '-'
  const iec = getUnits() === 'iec'
  const base = iec ? 1024 : 1000
  const suffix = iec ? 'i' : ''
  for (const [exp, letter] of [[4, 'T'], [3, 'G'], [2, 'M'], [1, 'K']] as const) {
    if (bytes >= base ** exp) return `${(bytes / base ** exp).toFixed(1)} ${letter}${suffix}`
  }
  return `${bytes} B`
}

/**
 * Format a number with K/M suffixes for large values
 */
export function formatCount(n: number | null | undefined): string {
  if (n == null) return '-'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

/**
 * Format a number with locale-aware separators (e.g., 1,234,567)
 */
export function formatNumber(n: number | null | undefined): string {
  if (n == null) return '-'
  return n.toLocaleString()
}

/**
 * Format a timestamp as elapsed duration (e.g., "2m 13s", "1h 5m")
 */
export function elapsed(value: string | number | null | undefined): string {
  if (value == null) return '-'
  let ms: number
  if (typeof value === 'string') {
    ms = new Date(value).getTime()
  } else {
    ms = value < 1e11 ? value * 1000 : value
  }
  const seconds = Math.max(0, Math.floor((Date.now() - ms) / 1000))
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  const secs = seconds % 60
  if (minutes < 60) return `${minutes}m ${secs}s`
  const hours = Math.floor(minutes / 60)
  const mins = minutes % 60
  return `${hours}h ${mins}m`
}
