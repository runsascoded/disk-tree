import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useSearchParams } from 'react-router-dom'
import {
  Box,
  Button,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Tooltip,
  Typography,
} from '@mui/material'
import { FaArrowRight, FaFolder, FaFile, FaSortUp, FaSortDown, FaSync, FaList } from 'react-icons/fa'
import { compareScans, fetchScanHistory, startScan } from '../api'
import type { CompareResult, CompareRow, ScanHistoryItem } from '../api'
import { useScanProgress } from '../hooks/useScanProgress'
import { useRecentPaths } from '../hooks/useRecentPaths'

type SortColumn = 'size_old' | 'size_new' | 'size_delta' | 'desc_old' | 'desc_new' | 'desc_delta'
type SortDirection = 'asc' | 'desc'

function formatSize(bytes: number | null | undefined): string {
  if (bytes == null) return '-'
  if (bytes >= 1024 ** 4) return `${(bytes / 1024 ** 4).toFixed(1)} TB`
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${bytes} B`
}

function formatDelta(bytes: number): string {
  const sign = bytes >= 0 ? '+' : ''
  return sign + formatSize(Math.abs(bytes)).replace(' ', '') + (bytes < 0 ? '' : '')
}

function formatDateTime(dateStr: string): string {
  const date = new Date(dateStr)
  return date.toLocaleString()
}

function timeAgo(dateStr: string): string {
  const date = new Date(dateStr)
  const now = new Date()
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000)

  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

const statusColors = {
  added: { bg: 'rgba(46, 160, 67, 0.15)' },
  removed: { bg: 'rgba(248, 81, 73, 0.15)' },
  changed: { bg: 'transparent' },
  unchanged: { bg: 'transparent' },
}

// Delta bar component - visual representation of size change
function DeltaBar({ delta, maxDelta }: { delta: number; maxDelta: number }) {
  if (maxDelta === 0) return <div style={{ width: '50px' }} />
  const pct = Math.min(Math.abs(delta) / maxDelta * 100, 100)
  const color = delta > 0 ? '#f85149' : delta < 0 ? '#3fb950' : 'transparent'
  return (
    <div style={{
      width: '50px',
      height: '8px',
      backgroundColor: 'rgba(255,255,255,0.1)',
      borderRadius: '4px',
      overflow: 'hidden',
      flexShrink: 0,
    }}>
      <div style={{
        width: `${pct}%`,
        height: '100%',
        backgroundColor: color,
        borderRadius: '4px',
      }} />
    </div>
  )
}

function formatNumber(n: number | null | undefined): string {
  if (n == null) return '-'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}

function formatDeltaNumber(n: number): string {
  const sign = n > 0 ? '+' : ''
  return sign + formatNumber(Math.abs(n))
}

// Check if a path is covered by a scan (path is at or below scan_path)
function isPathCoveredByScan(path: string, scanPath: string): boolean {
  if (!scanPath) return false
  // Normalize: ensure both have consistent trailing slash handling
  const normPath = path.endsWith('/') ? path.slice(0, -1) : path
  const normScan = scanPath.endsWith('/') ? scanPath.slice(0, -1) : scanPath
  return normPath === normScan || normPath.startsWith(normScan + '/')
}

// Breadcrumb component for compare view
function CompareBreadcrumbs({
  uri,
  routeType,
  scan1Path,
  scan2Path,
  scan1,
  scan2,
}: {
  uri: string
  routeType: RouteType
  scan1Path?: string
  scan2Path?: string
  scan1: number | ''
  scan2: number | ''
}) {
  // Split path into segments
  const isS3 = routeType === 's3'
  let segments: { name: string; path: string }[] = []

  if (isS3) {
    // s3://bucket/path/to/dir
    const withoutScheme = uri.slice(5) // remove 's3://'
    const parts = withoutScheme.split('/').filter(Boolean)
    let currentPath = 's3:/'
    for (const part of parts) {
      currentPath += '/' + part
      segments.push({ name: part, path: currentPath })
    }
  } else {
    // /Users/ryan/Library/...
    const parts = uri.split('/').filter(Boolean)
    let currentPath = ''
    for (const part of parts) {
      currentPath += '/' + part
      segments.push({ name: part, path: currentPath })
    }
  }

  return (
    <Typography
      variant="body2"
      sx={{ mb: 3, fontFamily: 'monospace', display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}
    >
      {!isS3 && <span style={{ color: '#8b949e' }}>/</span>}
      {isS3 && <span style={{ color: '#8b949e' }}>s3://</span>}
      {segments.map((seg, i) => {
        const basePath = isS3
          ? `/compare/s3/${seg.path.slice(5)}`
          : `/compare/file${seg.path}`
        const params = new URLSearchParams()
        if (scan1 !== '') params.set('scan1', String(scan1))
        if (scan2 !== '') params.set('scan2', String(scan2))
        const compareUrl = params.toString() ? `${basePath}?${params}` : basePath

        // Check if this segment is covered by both scans
        const coveredBy1 = scan1Path ? isPathCoveredByScan(seg.path, scan1Path) : true
        const coveredBy2 = scan2Path ? isPathCoveredByScan(seg.path, scan2Path) : true
        const fullyCovered = coveredBy1 && coveredBy2
        const partiallyCovered = coveredBy1 || coveredBy2

        // Style based on coverage - brighter colors for better visibility
        const color = fullyCovered ? '#e6edf3' : partiallyCovered ? '#b0b8c1' : '#8b949e'
        const opacity = 1

        return (
          <span key={seg.path} style={{ display: 'inline-flex', alignItems: 'center' }}>
            <Link
              to={compareUrl}
              style={{
                color,
                opacity,
                textDecoration: 'none',
              }}
              title={
                fullyCovered
                  ? 'Both scans cover this path'
                  : partiallyCovered
                    ? 'Only one scan covers this path'
                    : 'Neither scan covers this path'
              }
              onMouseEnter={(e) => (e.currentTarget.style.textDecoration = 'underline')}
              onMouseLeave={(e) => (e.currentTarget.style.textDecoration = 'none')}
            >
              {seg.name}
            </Link>
            {i < segments.length - 1 && (
              <span style={{ color: '#6e7681', margin: '0 2px' }}>/</span>
            )}
          </span>
        )
      })}
    </Typography>
  )
}

type RouteType = 'file' | 's3'

// Sortable column header component
function SortHeader({
  label,
  column,
  sortColumn,
  sortDirection,
  onSort,
  style,
}: {
  label: string
  column: SortColumn
  sortColumn: SortColumn | null
  sortDirection: SortDirection
  onSort: (col: SortColumn) => void
  style?: React.CSSProperties
}) {
  const isActive = sortColumn === column
  return (
    <th
      onClick={() => onSort(column)}
      style={{
        ...style,
        cursor: 'pointer',
        userSelect: 'none',
        fontWeight: 'normal',
        color: isActive ? '#e6edf3' : '#8b949e',
      }}
    >
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: '2px' }}>
        {label}
        {isActive && (sortDirection === 'desc' ? <FaSortDown size={10} /> : <FaSortUp size={10} />)}
      </span>
    </th>
  )
}

// Parent directory summary row - shows totals for the directory being compared
function ParentSummaryRow({
  result,
  maxSizeDelta,
  maxDescDelta,
  onScan,
  isScanning,
}: {
  result: CompareResult
  maxSizeDelta: number
  maxDescDelta: number
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
}) {
  const { scan1, scan2, uri } = result
  const sizeDelta = (scan2.size ?? 0) - (scan1.size ?? 0)
  const descDelta = (scan2.n_desc ?? 0) - (scan1.n_desc ?? 0)

  const sizeDeltaColor = sizeDelta > 0 ? '#f85149' : sizeDelta < 0 ? '#3fb950' : '#8b949e'
  const descDeltaColor = descDelta > 0 ? '#f85149' : descDelta < 0 ? '#3fb950' : '#8b949e'

  const td: React.CSSProperties = { padding: '8px 6px', textAlign: 'right', fontFamily: 'monospace', fontSize: '0.85em', whiteSpace: 'nowrap' }
  const dim: React.CSSProperties = { color: '#8b949e' }

  // Get the directory name for display
  const dirName = uri === '/' ? '/' : uri.split('/').pop() || uri

  return (
    <tr style={{ backgroundColor: 'rgba(88, 166, 255, 0.1)', borderBottom: '2px solid rgba(255,255,255,0.2)' }}>
      {/* Path */}
      <td style={{ padding: '8px', fontWeight: 'bold' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <FaFolder size={14} color="#54aeff" style={{ flexShrink: 0 }} />
          <span style={{ fontFamily: 'monospace', fontSize: '0.9em' }}>. ({dirName})</span>
        </div>
      </td>
      {/* Size: before */}
      <td style={{ ...td, ...dim, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{formatSize(scan1.size)}</td>
      {/* Size: after */}
      <td style={td}>{formatSize(scan2.size)}</td>
      {/* Size: delta */}
      <td style={{ ...td, color: sizeDeltaColor, fontWeight: sizeDelta !== 0 ? 'bold' : undefined }}>
        {formatDelta(sizeDelta)}
      </td>
      {/* Size: bar */}
      <td style={{ padding: '8px 12px 8px 4px' }}>
        <DeltaBar delta={sizeDelta} maxDelta={Math.max(maxSizeDelta, Math.abs(sizeDelta))} />
      </td>
      {/* Desc: before */}
      <td style={{ ...td, ...dim, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{formatNumber(scan1.n_desc)}</td>
      {/* Desc: after */}
      <td style={td}>{formatNumber(scan2.n_desc)}</td>
      {/* Desc: delta */}
      <td style={{ ...td, color: descDeltaColor, fontWeight: descDelta !== 0 ? 'bold' : undefined }}>
        {formatDeltaNumber(descDelta)}
      </td>
      {/* Desc: bar */}
      <td style={{ padding: '8px 4px' }}>
        <DeltaBar delta={descDelta} maxDelta={Math.max(maxDescDelta, Math.abs(descDelta))} />
      </td>
      {/* Scan button */}
      <td style={{ padding: '4px 8px', textAlign: 'center', borderLeft: '1px solid rgba(255,255,255,0.1)' }}>
        <Tooltip title={`Scan ${uri}`}>
          <span>
            <Button
              size="small"
              onClick={() => onScan(uri)}
              disabled={isScanning(uri)}
              sx={{ minWidth: 'auto', padding: '2px 6px' }}
            >
              {isScanning(uri) ? <CircularProgress size={12} /> : <FaSync size={10} />}
            </Button>
          </span>
        </Tooltip>
      </td>
    </tr>
  )
}

function CompareTable({
  result,
  routeType,
  onScan,
  isScanning,
  getProgress,
  scan1,
  scan2,
}: {
  result: CompareResult
  routeType: RouteType
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
  getProgress: (path: string) => { items_found?: number } | undefined
  scan1: number | ''
  scan2: number | ''
}) {
  const [sortColumn, setSortColumn] = useState<SortColumn | null>('size_delta')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')

  const handleSort = (col: SortColumn) => {
    if (sortColumn === col) {
      setSortDirection(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortColumn(col)
      setSortDirection('desc')
    }
  }

  // Filter out unchanged rows for cleaner view
  const changedRows = result.rows.filter(r => r.status !== 'unchanged')

  // Sort rows
  const sortedRows = useMemo(() => {
    if (!sortColumn) return changedRows
    return [...changedRows].sort((a, b) => {
      let aVal: number, bVal: number
      switch (sortColumn) {
        case 'size_old': aVal = a.size_old ?? a.size ?? 0; bVal = b.size_old ?? b.size ?? 0; break
        case 'size_new': aVal = a.size ?? 0; bVal = b.size ?? 0; break
        case 'size_delta': aVal = Math.abs(a.size_delta); bVal = Math.abs(b.size_delta); break
        case 'desc_old': aVal = a.n_desc_old ?? a.n_desc ?? 0; bVal = b.n_desc_old ?? b.n_desc ?? 0; break
        case 'desc_new': aVal = a.n_desc ?? 0; bVal = b.n_desc ?? 0; break
        case 'desc_delta': aVal = Math.abs(a.n_desc_delta ?? 0); bVal = Math.abs(b.n_desc_delta ?? 0); break
      }
      return sortDirection === 'desc' ? bVal - aVal : aVal - bVal
    })
  }, [changedRows, sortColumn, sortDirection])

  // Find max deltas for scaling bars
  const maxSizeDelta = Math.max(...changedRows.map(r => Math.abs(r.size_delta)), 1)
  const maxDescDelta = Math.max(...changedRows.map(r => Math.abs(r.n_desc_delta ?? 0)), 1)

  const subTh: React.CSSProperties = { padding: '4px 6px', textAlign: 'right', fontSize: '0.75em', whiteSpace: 'nowrap' }

  return (
    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
      <thead>
        <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <th style={{ textAlign: 'left', padding: '8px', width: '100%' }}>Path</th>
          <th colSpan={4} style={{ textAlign: 'center', padding: '8px 12px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}>Size</th>
          <th colSpan={4} style={{ textAlign: 'center', padding: '8px 12px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}>Descendants</th>
          <th style={{ padding: '8px', borderLeft: '1px solid rgba(255,255,255,0.1)', whiteSpace: 'nowrap' }}></th>
        </tr>
        <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <th style={{ width: '100%' }}></th>
          <SortHeader label="old" column="size_old" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={{ ...subTh, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }} />
          <SortHeader label="new" column="size_new" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <SortHeader label="Δ" column="size_delta" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <th style={subTh}></th>
          <SortHeader label="old" column="desc_old" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={{ ...subTh, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }} />
          <SortHeader label="new" column="desc_new" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <SortHeader label="Δ" column="desc_delta" sortColumn={sortColumn} sortDirection={sortDirection} onSort={handleSort} style={subTh} />
          <th style={subTh}></th>
          <th style={{ whiteSpace: 'nowrap' }}></th>
        </tr>
      </thead>
      <tbody>
        {/* Parent directory summary row */}
        <ParentSummaryRow
          result={result}
          maxSizeDelta={maxSizeDelta}
          maxDescDelta={maxDescDelta}
          onScan={onScan}
          isScanning={isScanning}
        />
        {sortedRows.map((row) => (
          <CompareRowComponent
            key={row.path}
            row={row}
            maxSizeDelta={maxSizeDelta}
            maxDescDelta={maxDescDelta}
            routeType={routeType}
            parentUri={result.uri}
            onScan={onScan}
            isScanning={isScanning}
            getProgress={getProgress}
            scan1={scan1}
            scan2={scan2}
          />
        ))}
        {sortedRows.length === 0 && (
          <tr>
            <td colSpan={10} style={{ padding: '24px', textAlign: 'center', color: '#8b949e' }}>
              No changes detected between scans
            </td>
          </tr>
        )}
      </tbody>
    </table>
  )
}

function CompareRowComponent({
  row,
  maxSizeDelta,
  maxDescDelta,
  routeType,
  onScan,
  isScanning,
  getProgress: _getProgress,
  scan1,
  scan2,
}: {
  row: CompareRow
  maxSizeDelta: number
  maxDescDelta: number
  routeType: RouteType
  parentUri: string
  onScan: (path: string) => void
  isScanning: (path: string) => boolean
  getProgress: (path: string) => { items_found?: number } | undefined
  scan1: number | ''
  scan2: number | ''
}) {
  const { bg } = statusColors[row.status]
  const Icon = row.kind === 'dir' ? FaFolder : FaFile
  const iconColor = row.kind === 'dir' ? '#54aeff' : '#8b949e'

  const sizeDeltaColor = row.size_delta > 0 ? '#f85149' : row.size_delta < 0 ? '#3fb950' : '#8b949e'
  const descDelta = row.n_desc_delta ?? 0
  const descDeltaColor = descDelta > 0 ? '#f85149' : descDelta < 0 ? '#3fb950' : '#8b949e'

  // Build link URL for drilling into subdirectory (preserving scan params)
  const childUri = row.uri
  const basePath = routeType === 's3'
    ? `/compare/s3/${childUri.slice(5)}`
    : `/compare/file${childUri}`
  const params = new URLSearchParams()
  if (scan1 !== '') params.set('scan1', String(scan1))
  if (scan2 !== '') params.set('scan2', String(scan2))
  const compareUrl = params.toString() ? `${basePath}?${params}` : basePath

  const td: React.CSSProperties = { padding: '8px 6px', textAlign: 'right', fontFamily: 'monospace', fontSize: '0.85em', whiteSpace: 'nowrap' }

  // Size values
  const sizeBefore = row.status === 'added' ? '-' : formatSize(row.size_old ?? row.size)
  const sizeAfter = row.status === 'removed' ? '-' : formatSize(row.size)
  const sizeBeforeColor = row.status === 'removed' ? '#f85149' : '#8b949e'
  const sizeAfterColor = row.status === 'added' ? '#3fb950' : undefined

  // Desc values
  const descBefore = row.status === 'added' ? '-' : formatNumber(row.n_desc_old ?? row.n_desc)
  const descAfter = row.status === 'removed' ? '-' : formatNumber(row.n_desc)
  const descBeforeColor = row.status === 'removed' ? '#f85149' : '#8b949e'
  const descAfterColor = row.status === 'added' ? '#3fb950' : undefined

  return (
    <tr style={{ backgroundColor: bg, borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
      {/* Path */}
      <td style={{ padding: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Icon size={14} color={iconColor} style={{ flexShrink: 0 }} />
          {row.kind === 'dir' ? (
            <Link to={compareUrl} style={{ fontFamily: 'monospace', fontSize: '0.9em', color: 'inherit', textDecoration: 'none' }}>
              {row.path}
            </Link>
          ) : (
            <span style={{ fontFamily: 'monospace', fontSize: '0.9em' }}>{row.path}</span>
          )}
        </div>
      </td>
      {/* Size: before */}
      <td style={{ ...td, color: sizeBeforeColor, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{sizeBefore}</td>
      {/* Size: after */}
      <td style={{ ...td, color: sizeAfterColor }}>{sizeAfter}</td>
      {/* Size: delta */}
      <td style={{ ...td, color: sizeDeltaColor, fontWeight: row.size_delta !== 0 ? 'bold' : undefined }}>
        {formatDelta(row.size_delta)}
      </td>
      {/* Size: bar */}
      <td style={{ padding: '8px 12px 8px 4px' }}>
        <DeltaBar delta={row.size_delta} maxDelta={maxSizeDelta} />
      </td>
      {/* Desc: before */}
      <td style={{ ...td, color: descBeforeColor, borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px' }}>{row.kind === 'dir' ? descBefore : '-'}</td>
      {/* Desc: after */}
      <td style={{ ...td, color: descAfterColor }}>{row.kind === 'dir' ? descAfter : '-'}</td>
      {/* Desc: delta */}
      <td style={{ ...td, color: descDeltaColor, fontWeight: descDelta !== 0 ? 'bold' : undefined }}>
        {row.kind === 'dir' ? formatDeltaNumber(descDelta) : '-'}
      </td>
      {/* Desc: bar */}
      <td style={{ padding: '8px 4px' }}>
        {row.kind === 'dir' ? <DeltaBar delta={descDelta} maxDelta={maxDescDelta} /> : null}
      </td>
      {/* Scan button */}
      <td style={{ padding: '4px 8px', textAlign: 'center', borderLeft: '1px solid rgba(255,255,255,0.1)' }}>
        {row.kind === 'dir' && (
          <Tooltip title={`Scan ${row.path}`}>
            <span>
              <Button
                size="small"
                onClick={() => onScan(row.uri)}
                disabled={isScanning(row.uri)}
                sx={{ minWidth: 'auto', padding: '2px 6px' }}
              >
                {isScanning(row.uri) ? <CircularProgress size={12} /> : <FaSync size={10} />}
              </Button>
            </span>
          </Tooltip>
        )}
      </td>
    </tr>
  )
}

function Summary({ result }: { result: CompareResult }) {
  const { summary, scan1, scan2 } = result

  return (
    <Paper sx={{ p: 2, mb: 3 }}>
      <Box sx={{ display: 'flex', gap: 4, flexWrap: 'wrap', alignItems: 'center' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Box>
            <Typography variant="caption" color="text.secondary">From</Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {formatDateTime(scan1.time)}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {formatSize(scan1.size)}
            </Typography>
          </Box>
          <FaArrowRight color="#8b949e" />
          <Box>
            <Typography variant="caption" color="text.secondary">To</Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {formatDateTime(scan2.time)}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {formatSize(scan2.size)}
            </Typography>
          </Box>
        </Box>
        <Box sx={{ borderLeft: '1px solid rgba(255,255,255,0.1)', pl: 3, display: 'flex', gap: 3 }}>
          <Box>
            <Typography variant="caption" color="text.secondary">Added</Typography>
            <Typography variant="body1" sx={{ color: '#3fb950', fontWeight: 'bold' }}>
              {summary.added}
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" color="text.secondary">Removed</Typography>
            <Typography variant="body1" sx={{ color: '#f85149', fontWeight: 'bold' }}>
              {summary.removed}
            </Typography>
          </Box>
          <Box>
            <Typography variant="caption" color="text.secondary">Changed</Typography>
            <Typography variant="body1" sx={{ color: '#d29922', fontWeight: 'bold' }}>
              {summary.changed}
            </Typography>
          </Box>
        </Box>
        <Box sx={{ borderLeft: '1px solid rgba(255,255,255,0.1)', pl: 3 }}>
          <Typography variant="caption" color="text.secondary">Total Delta</Typography>
          <Typography
            variant="body1"
            sx={{
              fontWeight: 'bold',
              fontFamily: 'monospace',
              color: summary.total_delta > 0 ? '#f85149' : summary.total_delta < 0 ? '#3fb950' : '#8b949e',
            }}
          >
            {formatDelta(summary.total_delta)}
          </Typography>
        </Box>
      </Box>
    </Paper>
  )
}

export function CompareView() {
  const location = useLocation()
  const [searchParams, setSearchParams] = useSearchParams()

  // Extract URI and routeType from path: /compare/file/Users/ryan/... or /compare/s3/bucket/...
  const pathAfterCompare = location.pathname.replace(/^\/compare/, '') || '/'
  let uri: string
  let routeType: RouteType = 'file'
  if (pathAfterCompare.startsWith('/file')) {
    uri = decodeURIComponent(pathAfterCompare.replace(/^\/file/, '') || '/')
    routeType = 'file'
  } else if (pathAfterCompare.startsWith('/s3/')) {
    uri = 's3:/' + decodeURIComponent(pathAfterCompare.slice(3))
    routeType = 's3'
  } else {
    uri = decodeURIComponent(pathAfterCompare || '/')
  }

  // Get scan selections from URL params
  const urlScan1 = searchParams.get('scan1')
  const urlScan2 = searchParams.get('scan2')
  const scan1: number | '' = urlScan1 ? parseInt(urlScan1, 10) : ''
  const scan2: number | '' = urlScan2 ? parseInt(urlScan2, 10) : ''

  // Update URL params when selections change
  const setScan1 = (id: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id === '') {
        next.delete('scan1')
      } else {
        next.set('scan1', String(id))
      }
      return next
    }, { replace: true })
  }
  const setScan2 = (id: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id === '') {
        next.delete('scan2')
      } else {
        next.set('scan2', String(id))
      }
      return next
    }, { replace: true })
  }
  const setScans = (id1: number | '', id2: number | '') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (id1 === '') {
        next.delete('scan1')
      } else {
        next.set('scan1', String(id1))
      }
      if (id2 === '') {
        next.delete('scan2')
      } else {
        next.set('scan2', String(id2))
      }
      return next
    }, { replace: true })
  }

  const [history, setHistory] = useState<ScanHistoryItem[]>([])
  const [historyLoading, setHistoryLoading] = useState(true)
  const [result, setResult] = useState<CompareResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Record visit to recent paths
  const { recordVisit } = useRecentPaths()
  useEffect(() => {
    if (uri && uri !== '/' && uri !== 's3://') {
      recordVisit(uri, 'compare')
    }
  }, [uri, recordVisit])

  // Track previous URI to detect navigation
  const prevUriRef = useRef(uri)

  // Load scan history
  useEffect(() => {
    const isNavigation = prevUriRef.current !== uri
    prevUriRef.current = uri

    setHistory([])
    setHistoryLoading(true)
    setResult(null)
    setError(null)

    fetchScanHistory(uri)
      .then(h => {
        setHistory(h)

        // If we have URL scan params, keep them - they may be valid even if not in
        // this path's history (e.g., for a newly added directory that didn't exist
        // in the older scan, the older scan won't be in history, but it's still
        // a valid comparison showing the directory was added)
        if (scan1 !== '' && scan2 !== '') {
          // URL params present - keep them, let compare API handle the details
          // Just ensure correct order if both are in history
          const scan1Item = h.find(item => item.id === scan1)
          const scan2Item = h.find(item => item.id === scan2)
          if (scan1Item && scan2Item) {
            const time1 = new Date(scan1Item.time).getTime()
            const time2 = new Date(scan2Item.time).getTime()
            if (time1 > time2) {
              // Swap to ensure scan1 is older
              setScans(scan2, scan1)
            }
          }
          // If not both in history, keep them as-is - the compare API will handle it
        } else if (!isNavigation && h.length >= 2) {
          // Initial page load without URL params - auto-select most recent two
          setScans(h[1].id, h[0].id) // h[1] is older, h[0] is newer (sorted DESC)
        }
        // If navigating with no URL params and < 2 scans in history, leave empty
      })
      .catch(err => setError(err.message))
      .finally(() => setHistoryLoading(false))
  }, [uri])

  // Fetch comparison when both scans selected
  useEffect(() => {
    if (scan1 === '' || scan2 === '' || scan1 === scan2) {
      setResult(null)
      return
    }

    setLoading(true)
    setError(null)
    compareScans(uri, scan1 as number, scan2 as number)
      .then(setResult)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [uri, scan1, scan2])

  // Get scan_path for selected scans (for breadcrumb coverage highlighting)
  const scan1Item = history.find(h => h.id === scan1)
  const scan2Item = history.find(h => h.id === scan2)
  const scan1Path = scan1Item?.scan_path ?? scan1Item?.path
  const scan2Path = scan2Item?.scan_path ?? scan2Item?.path

  // Scan progress tracking
  const scanProgress = useScanProgress()
  const [scanningPath, setScanningPath] = useState<string | null>(null)

  const handleStartScan = async (path: string) => {
    try {
      setScanningPath(path)
      await startScan(path)
      // Refresh history after scan completes (SSE will update progress)
    } catch (err) {
      console.error('Failed to start scan:', err)
      setScanningPath(null)
    }
  }

  // Check if there's an active scan for a path
  const isScanning = (path: string) => {
    return scanProgress.some(s => s.path === path && s.status === 'running')
  }

  // Get progress for a path
  const getProgress = (path: string) => {
    return scanProgress.find(s => s.path === path && s.status === 'running')
  }

  // Clear scanningPath when scan completes
  useEffect(() => {
    if (scanningPath && !isScanning(scanningPath)) {
      setScanningPath(null)
      // Refresh history and set new scan as "after"
      fetchScanHistory(uri).then(h => {
        setHistory(h)
        if (h.length >= 1) {
          // New scan becomes the "after" (scan2)
          // Keep current scan1 if it's valid, otherwise promote current scan2
          const currentScan1Valid = scan1 !== '' && h.some(item => item.id === scan1)
          if (currentScan1Valid) {
            setScans(scan1, h[0].id)
          } else if (scan2 !== '') {
            // Promote current scan2 to scan1 (shift the window)
            setScans(scan2, h[0].id)
          } else {
            // No prior selections, just set the new scan
            setScans('', h[0].id)
          }
        }
      })
    }
  }, [scanProgress, scanningPath, uri, scan1, scan2])

  return (
    <Box sx={{ p: 3, maxWidth: 1400, margin: '0 auto' }}>
      <Typography variant="h5" sx={{ mb: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <FaFolder color="#54aeff" />
        Compare Scans
      </Typography>
      <CompareBreadcrumbs
        uri={uri}
        routeType={routeType}
        scan1Path={scan1Path}
        scan2Path={scan2Path}
        scan1={scan1}
        scan2={scan2}
      />

      {/* Show comparison UI if we have URL params (even with 0-1 scans in local history,
          because the path may be newly added and the scans come from an ancestor) */}
      {historyLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      ) : history.length === 0 && (scan1 === '' || scan2 === '') ? (
        <Paper sx={{ p: 3, textAlign: 'center' }}>
          <Typography color="text.secondary" sx={{ mb: 2 }}>
            No scans found for this path.
          </Typography>
          <Button
            variant="outlined"
            onClick={() => handleStartScan(uri)}
            disabled={isScanning(uri)}
            startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
          >
            {isScanning(uri) ? 'Scanning...' : 'Scan Now'}
          </Button>
        </Paper>
      ) : history.length === 1 && (scan1 === '' || scan2 === '') ? (
        <Paper sx={{ p: 3 }}>
          <Typography color="text.secondary" sx={{ mb: 2 }}>
            Only one scan found for this path. This directory may have been added recently.
          </Typography>
          <Box sx={{ display: 'flex', gap: 3, alignItems: 'center', mb: 2 }}>
            <Box>
              <Typography variant="caption" color="text.secondary">Scanned</Typography>
              <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                {formatDateTime(history[0].time)} ({timeAgo(history[0].time)})
              </Typography>
            </Box>
            <Box>
              <Typography variant="caption" color="text.secondary">Size</Typography>
              <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                {formatSize(history[0].size)}
              </Typography>
            </Box>
            {history[0].n_desc != null && (
              <Box>
                <Typography variant="caption" color="text.secondary">Files</Typography>
                <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                  {formatNumber(history[0].n_desc)}
                </Typography>
              </Box>
            )}
          </Box>
          <Button
            variant="outlined"
            onClick={() => handleStartScan(uri)}
            disabled={isScanning(uri)}
            startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
          >
            {isScanning(uri) ? 'Scanning...' : 'Rescan to Compare'}
          </Button>
        </Paper>
      ) : (
        <>
          {/* Show dropdowns only if history has enough scans; otherwise show simpler header */}
          {history.length >= 2 ? (
            <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center', flexWrap: 'wrap' }}>
              <FormControl sx={{ minWidth: 300 }}>
                <InputLabel>From (older)</InputLabel>
                <Select
                  value={scan1}
                  label="From (older)"
                  onChange={(e) => setScan1(e.target.value as number)}
                >
                  {history.map(h => (
                    <MenuItem key={h.id} value={h.id}>
                      {timeAgo(h.time)} — {formatSize(h.size)} — {formatDateTime(h.time)}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FaArrowRight color="#8b949e" />
              <FormControl sx={{ minWidth: 300 }}>
                <InputLabel>To (newer)</InputLabel>
                <Select
                  value={scan2}
                  label="To (newer)"
                  onChange={(e) => setScan2(e.target.value as number)}
                >
                  {history.map(h => (
                    <MenuItem key={h.id} value={h.id}>
                      {timeAgo(h.time)} — {formatSize(h.size)} — {formatDateTime(h.time)}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
                <Tooltip title="View directory tree">
                  <Button
                    component={Link}
                    to={`${routeType === 's3' ? `/s3/${uri.slice(5)}` : `/file${uri}`}${scan2 !== '' ? `?scan_id=${scan2}` : ''}`}
                    variant="outlined"
                    size="small"
                    startIcon={<FaList />}
                  >
                    Tree
                  </Button>
                </Tooltip>
                <Tooltip title={`Rescan ${uri}`}>
                  <span>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => handleStartScan(uri)}
                      disabled={isScanning(uri)}
                      startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
                    >
                      {isScanning(uri) ? (
                        getProgress(uri)?.items_found
                          ? `${getProgress(uri)!.items_found.toLocaleString()} items`
                          : 'Scanning...'
                      ) : 'Rescan'}
                    </Button>
                  </span>
                </Tooltip>
              </Box>
            </Box>
          ) : (
            /* History has < 2 scans but we have URL params - show simpler header
               (for newly added/removed directories where path doesn't have full history) */
            <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center', flexWrap: 'wrap' }}>
              {result ? (
                <>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                    Comparing scans from {formatDateTime(result.scan1.time)} → {formatDateTime(result.scan2.time)}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                    (this path was {history.length === 0 ? 'added or removed' : 'added'} between scans)
                  </Typography>
                </>
              ) : loading ? null : (
                <Typography variant="body2" color="text.secondary">
                  Loading comparison...
                </Typography>
              )}
              <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
                <Tooltip title="View directory tree">
                  <Button
                    component={Link}
                    to={`${routeType === 's3' ? `/s3/${uri.slice(5)}` : `/file${uri}`}${scan2 !== '' ? `?scan_id=${scan2}` : ''}`}
                    variant="outlined"
                    size="small"
                    startIcon={<FaList />}
                  >
                    Tree
                  </Button>
                </Tooltip>
                <Tooltip title={`Rescan ${uri}`}>
                  <span>
                    <Button
                      variant="outlined"
                      size="small"
                      onClick={() => handleStartScan(uri)}
                      disabled={isScanning(uri)}
                      startIcon={isScanning(uri) ? <CircularProgress size={14} /> : <FaSync />}
                    >
                      {isScanning(uri) ? (
                        getProgress(uri)?.items_found
                          ? `${getProgress(uri)!.items_found.toLocaleString()} items`
                          : 'Scanning...'
                      ) : 'Rescan'}
                    </Button>
                  </span>
                </Tooltip>
              </Box>
            </Box>
          )}

          {error && (
            <Paper sx={{ p: 2, mb: 3, backgroundColor: 'rgba(248, 81, 73, 0.1)' }}>
              <Typography color="error">{error}</Typography>
            </Paper>
          )}

          {loading && (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
              <CircularProgress />
            </Box>
          )}

          {!loading && !result && history.length >= 2 && (scan1 === '' || scan2 === '') && (
            <Paper sx={{ p: 3, textAlign: 'center' }}>
              <Typography color="text.secondary">
                Select two scans to compare.
              </Typography>
            </Paper>
          )}

          {result && !loading && (
            <>
              <Summary result={result} />
              <Paper sx={{ overflow: 'auto' }}>
                <CompareTable
                  result={result}
                  routeType={routeType}
                  onScan={handleStartScan}
                  isScanning={isScanning}
                  getProgress={getProgress}
                  scan1={scan1}
                  scan2={scan2}
                />
              </Paper>
            </>
          )}
        </>
      )}
    </Box>
  )
}
