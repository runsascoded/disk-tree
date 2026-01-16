/**
 * Shared formatting utilities for the UI
 */

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
 * Format bytes as human-readable size (e.g., "1.5 GB")
 */
export function formatSize(bytes: number | null | undefined): string {
  if (bytes == null) return '-'
  if (bytes >= 1024 ** 4) return `${(bytes / 1024 ** 4).toFixed(1)} TB`
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`
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
