import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Alert, Box, Button, Checkbox, CircularProgress, Collapse, TextField, Tooltip } from '@mui/material'
import { FaChevronDown, FaChevronRight, FaExclamationTriangle, FaExchangeAlt, FaFileAlt, FaFolder, FaFolderOpen, FaSync, FaSortUp, FaSortDown, FaTrash, FaSearch } from 'react-icons/fa'
import { useAction } from 'use-kbd'
import { LazyPlot as Plot } from './LazyPlot'
import { useQuery } from '@tanstack/react-query'
import { fetchScanDetails, fetchScanHistory, startScan, fetchScanStatus, deletePath, revealPath, fetchFilePreview, DEFAULT_MAX_ROWS } from '../api'
import type { Row, ScanJob, ScanProgress, CollapsedRow } from '../api'
import { useScanProgress } from '../hooks/useScanProgress'
import { useRecentPaths } from '../hooks/useRecentPaths'
import { formatSize, formatCount, timeAgo, elapsed } from '../utils/format'

type SortKey = 'kind' | 'path' | 'size' | 'mtime' | 'n_children' | 'n_desc' | 'scanned'
type SortDir = 'asc' | 'desc'
type SortSpec = { key: SortKey; dir: SortDir }

function ScanProgressBanner({ progress, currentUri }: { progress: ScanProgress[]; currentUri: string }) {
  const [, setTick] = useState(0)

  // Find scans that are relevant to this path (exact match, ancestor, or descendant)
  const relevantScans = progress.filter(p => {
    const scanPath = p.path
    // Exact match
    if (scanPath === currentUri) return true
    // Scan is ancestor of current (current is inside scan)
    if (currentUri.startsWith(scanPath + '/')) return true
    // Scan is descendant of current (scan is inside current)
    if (scanPath.startsWith(currentUri + '/')) return true
    return false
  })

  // Tick every second so elapsed times update live
  useEffect(() => {
    if (relevantScans.length === 0) return
    const id = setInterval(() => setTick(t => t + 1), 1000)
    return () => clearInterval(id)
  }, [relevantScans.length])

  if (relevantScans.length === 0) return null

  return (
    <Alert severity="info" sx={{ mb: 2 }} icon={<CircularProgress size={20} />}>
      {relevantScans.map(scan => (
        <Box key={scan.id} sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <strong>Scanning:</strong>
          <code style={{ fontSize: '0.9em' }}>{scan.path}</code>
          <span>{formatCount(scan.items_found)} items</span>
          {scan.items_per_sec && <span>({formatCount(Math.round(scan.items_per_sec))}/sec)</span>}
          {scan.error_count > 0 && <span style={{ color: '#ed6c02' }}>{scan.error_count} errors</span>}
          <span>{elapsed(scan.started)}</span>
        </Box>
      ))}
    </Alert>
  )
}

type RouteType = 'file' | 's3' | 'ssh'

function Breadcrumbs({ uri, routeType }: { uri: string; routeType: RouteType }) {
  const prefix = routeType === 's3' ? '/s3' : routeType === 'ssh' ? '/ssh' : '/file'
  const displayUri = routeType === 's3'
    ? uri.replace('s3://', '')
    : routeType === 'ssh'
      ? uri.replace('ssh://', '')
      : uri

  const segments = displayUri.split('/').filter(Boolean)
  const paths = segments.reduce((acc, seg) => {
    const prev = acc[acc.length - 1]
    acc.push(prev ? `${prev}/${seg}` : seg)
    return acc
  }, [] as string[])

  return (
    <div className="breadcrumbs">
      {routeType === 's3' && <Link to="/s3">s3://</Link>}
      {routeType === 'ssh' && <span>ssh://</span>}
      {routeType === 'file' && <Link to="/file/" className="breadcrumb-sep">/</Link>}
      {paths.map((path, idx) => (
        <span key={idx}>
          {routeType === 'file' && idx > 0 && <span className="breadcrumb-sep">/</span>}
          {idx === paths.length - 1 ? (
            <span>{segments[idx]}</span>
          ) : (
            <Link to={`${prefix}/${path}`}>{segments[idx]}</Link>
          )}
          {(routeType === 's3' || routeType === 'ssh') && idx < paths.length - 1 && <span className="breadcrumb-sep">/</span>}
        </span>
      ))}
    </div>
  )
}

function RowIcon({ row }: { row: Row }) {
  if (row.kind === 'file') return <FaFileAlt />
  if (row.scanned === true) return <FaFolder style={{ color: '#4caf50' }} />
  if (row.scanned === 'partial') return <FaFolderOpen style={{ color: '#ff9800' }} />
  return <FaFolder style={{ opacity: 0.5 }} />
}

function scanTimeAgo(scanTime: string | undefined): string {
  if (!scanTime) return '-'
  const date = new Date(scanTime)
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

function SortableHeader({
  label,
  sortKey,
  sorts,
  onSort,
  tooltip,
  className,
}: {
  label: string
  sortKey: SortKey
  sorts: SortSpec[]
  onSort: (key: SortKey) => void
  tooltip?: string
  className?: string
}) {
  const primarySort = sorts[0]
  const isPrimary = primarySort?.key === sortKey
  const isSecondary = sorts.length > 1 && sorts[1]?.key === sortKey

  const header = (
    <th
      className={className}
      onClick={() => onSort(sortKey)}
      style={{ cursor: 'pointer', userSelect: 'none' }}
    >
      {label}
      {isPrimary && (
        primarySort.dir === 'asc'
          ? <FaSortUp style={{ marginLeft: 4, verticalAlign: 'middle' }} />
          : <FaSortDown style={{ marginLeft: 4, verticalAlign: 'middle' }} />
      )}
      {isSecondary && (
        <span style={{ opacity: 0.4, marginLeft: 2 }}>
          {sorts[1].dir === 'asc' ? <FaSortUp style={{ verticalAlign: 'middle', fontSize: '0.7em' }} /> : <FaSortDown style={{ verticalAlign: 'middle', fontSize: '0.7em' }} />}
        </span>
      )}
    </th>
  )

  return tooltip ? <Tooltip title={tooltip}>{header}</Tooltip> : header
}

function PermissionErrorWarning({ errorCount, errorPaths, scanPath }: { errorCount: number; errorPaths: string[] | null; scanPath: string | null }) {
  const [expanded, setExpanded] = useState(false)
  const displayPaths = errorPaths?.slice(0, 10) ?? []
  const hasMore = errorPaths && errorPaths.length > 10

  return (
    <Alert
      severity="warning"
      icon={<FaExclamationTriangle />}
      sx={{ mb: 2, '& .MuiAlert-message': { width: '100%' } }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, cursor: 'pointer' }} onClick={() => setExpanded(!expanded)}>
        {displayPaths.length > 0 && (expanded ? <FaChevronDown size={12} /> : <FaChevronRight size={12} />)}
        <span>
          <strong>{errorCount} permission error{errorCount !== 1 ? 's' : ''}</strong> during scan.
          Some directories were not indexed.
        </span>
      </Box>
      <Collapse in={expanded}>
        {displayPaths.length > 0 && (
          <Box sx={{ mt: 1, ml: 2 }}>
            <div style={{ fontFamily: 'monospace', fontSize: '0.8rem', opacity: 0.9 }}>
              {displayPaths.map((p, i) => <div key={i}>{p}</div>)}
              {hasMore && <div style={{ opacity: 0.6 }}>... and {errorPaths!.length - 10} more</div>}
            </div>
          </Box>
        )}
      </Collapse>
      <Box sx={{ mt: 1, fontSize: '0.85rem', opacity: 0.85 }}>
        Tip: Run <code style={{ background: 'rgba(0,0,0,0.1)', padding: '2px 4px', borderRadius: 3 }}>disk-tree index --sudo {scanPath ?? '<path>'}</code> for full access.
      </Box>
    </Alert>
  )
}

function ChildScanStatus({ row, scanStatus, parentScanTime }: { row: Row; scanStatus: 'full' | 'partial' | 'none'; parentScanTime: string | null }) {
  // If parent was fully scanned, children inherit that scan time (shown grayed)
  if (scanStatus === 'full' && !row.scan_time && parentScanTime) {
    return <span style={{ opacity: 0.5 }}>{scanTimeAgo(parentScanTime)}</span>
  }
  if (row.scanned === 'partial') {
    return <span style={{ color: '#ff9800' }}>partial</span>
  }
  if (row.scan_time) {
    return <span>{scanTimeAgo(row.scan_time)}</span>
  }
  return <span style={{ opacity: 0.4 }}>-</span>
}

function DetailsTable({ root, children, uri, routeType, onScanChild, scanningPaths, scanStatus, scanTime, onRescan, isScanning, sorts, onSort, onDelete, deletingPaths, selectedPaths, hoveredIndex, mouseHoverIndex, onRowClick, onRowHover, collapsedRows, tableRef }: {
  root: Row
  children: Row[]
  uri: string
  routeType: RouteType
  onScanChild: (path: string) => void
  scanningPaths: Set<string>
  scanStatus: 'full' | 'partial' | 'none'
  scanTime: string | null
  onRescan: () => void
  isScanning: boolean
  sorts: SortSpec[]
  onSort: (key: SortKey) => void
  onDelete: (path: string) => void
  deletingPaths: Set<string>
  selectedPaths: Set<string>
  hoveredIndex: number | null
  mouseHoverIndex: number | null
  onRowClick: (uri: string, index: number, event: React.MouseEvent | React.KeyboardEvent) => void
  onRowHover: (index: number | null) => void
  collapsedRows?: CollapsedRow[] | null
  tableRef?: React.RefObject<HTMLTableElement | null>
}) {
  // Track whether the collapsed (auto-expanded) rows are shown expanded
  const [collapsedExpanded, setCollapsedExpanded] = useState(true)

  // Build prefix for child links, avoiding double slashes
  // For root (/), prefix should be /file not /file/
  const prefix = routeType === 's3'
    ? `/s3/${uri.replace('s3://', '').replace(/\/$/, '')}`
    : routeType === 'ssh'
      ? `/ssh/${uri.replace('ssh://', '').replace(/\/$/, '')}`
      : uri === '/' ? '/file' : `/file${uri}`
  const allSelected = children.length > 0 && children.every(r => selectedPaths.has(r.uri))
  const someSelected = children.some(r => selectedPaths.has(r.uri))

  const handleSelectAll = () => {
    // Toggle all - if all selected, deselect all; otherwise select all
    const syntheticEvent = { shiftKey: false, metaKey: false, ctrlKey: false } as React.MouseEvent
    if (allSelected) {
      // Deselect all by clicking each selected one with meta key (toggle off)
      children.forEach((r, idx) => {
        if (selectedPaths.has(r.uri)) {
          onRowClick(r.uri, idx, { ...syntheticEvent, metaKey: true } as React.MouseEvent)
        }
      })
    } else {
      // Select all not yet selected
      children.forEach((r, idx) => {
        if (!selectedPaths.has(r.uri)) {
          onRowClick(r.uri, idx, { ...syntheticEvent, metaKey: true } as React.MouseEvent)
        }
      })
    }
  }

  return (
    <table className="scan-details-table" ref={tableRef}>
      <thead>
        <tr>
          <th className="col-checkbox">
            <Checkbox
              size="small"
              checked={allSelected}
              indeterminate={someSelected && !allSelected}
              onChange={handleSelectAll}
              sx={{ padding: 0 }}
            />
          </th>
          <SortableHeader className="col-icon" label="" sortKey="kind" sorts={sorts} onSort={onSort} tooltip="Sort by type (file/folder)" />
          <SortableHeader className="col-path" label="Path" sortKey="path" sorts={sorts} onSort={onSort} />
          <SortableHeader className="col-numeric" label="Size" sortKey="size" sorts={sorts} onSort={onSort} tooltip="Total size including all nested files and directories" />
          <SortableHeader className="col-numeric" label="Modified" sortKey="mtime" sorts={sorts} onSort={onSort} tooltip="Most recent modification time of any file in this directory tree" />
          <SortableHeader className="col-numeric" label="Children" sortKey="n_children" sorts={sorts} onSort={onSort} tooltip="Number of direct children (files and subdirectories)" />
          <SortableHeader className="col-numeric" label="Desc." sortKey="n_desc" sorts={sorts} onSort={onSort} tooltip="Total number of descendants (all nested files and directories)" />
          <SortableHeader className="col-numeric" label="Scanned" sortKey="scanned" sorts={sorts} onSort={onSort} tooltip="When this directory was last scanned" />
          <th className="col-action"></th>
          {routeType !== 's3' && <th className="col-action"></th>}
        </tr>
      </thead>
      <tbody>
        <tr className="root">
          <td className="col-checkbox"></td>
          <td className="col-icon">{root.kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
          <td className="col-path"><code>.</code></td>
          <td className="col-numeric">{formatSize(root.size)}</td>
          <td className="col-numeric">{timeAgo(root.mtime)}</td>
          <td className="col-numeric">{root.n_children?.toLocaleString()}</td>
          <td className="col-numeric">{root.n_desc && root.n_desc > 1 ? root.n_desc.toLocaleString() : null}</td>
          <td className="col-numeric">
            {scanStatus === 'full' && scanTime ? (
              <span>{scanTimeAgo(scanTime)}</span>
            ) : scanStatus === 'partial' ? (
              <Tooltip title="Some subdirectories have been scanned, but not this directory itself">
                <span style={{ color: '#ff9800' }}>partial</span>
              </Tooltip>
            ) : (
              <Tooltip title="This directory has not been scanned yet">
                <span style={{ opacity: 0.4 }}>-</span>
              </Tooltip>
            )}
          </td>
          <td className="col-action" style={{ display: 'flex', gap: '4px' }}>
            <Tooltip title={scanStatus === 'full' ? 'Rescan this directory' : 'Scan this directory'}>
              <span>
                <Button
                  size="small"
                  onClick={onRescan}
                  disabled={isScanning}
                  sx={{ minWidth: 0, padding: '2px 4px' }}
                >
                  {isScanning ? <CircularProgress size={14} /> : <FaSync size={12} />}
                </Button>
              </span>
            </Tooltip>
            <Tooltip title="Compare scans">
              <Link to={`/compare${routeType === 's3' ? '/s3/' + uri.slice(5) : '/file' + uri}`}>
                <Button size="small" sx={{ minWidth: 0, padding: '2px 4px' }}>
                  <FaExchangeAlt size={12} />
                </Button>
              </Link>
            </Tooltip>
          </td>
          {routeType !== 's3' && <td className="col-action"></td>}
        </tr>
        {/* Render collapsed/expanded parent rows (auto-expanded single-child dirs) */}
        {collapsedRows && collapsedRows.map((collapsedRow, depth) => {
          const indentPx = depth * 20
          const collapsedUri = collapsedRow.uri
          const isScanning = scanningPaths.has(collapsedUri)
          const isDeleting = deletingPaths.has(collapsedUri)
          const isFirst = depth === 0
          // Extract just the segment name from the path (last part)
          const segment = collapsedRow.original_path.split('/').pop() || collapsedRow.original_path
          // First row: clickable chevron to toggle; only show subsequent rows when expanded
          if (!isFirst && !collapsedExpanded) return null
          const ChevronIcon = collapsedExpanded ? FaChevronDown : FaChevronRight
          return (
            <tr key={`collapsed-${depth}`} className="collapsed-parent" style={{ background: 'rgba(100, 100, 100, 0.08)' }}>
              <td className="col-checkbox"></td>
              <td className="col-icon">
                <span style={{ paddingLeft: indentPx, display: 'inline-flex', alignItems: 'center' }}>
                  {isFirst ? (
                    <ChevronIcon
                      size={10}
                      style={{ marginRight: 4, opacity: 0.7, cursor: 'pointer' }}
                      onClick={() => setCollapsedExpanded(prev => !prev)}
                    />
                  ) : (
                    <FaChevronDown size={10} style={{ marginRight: 4, opacity: 0.5 }} />
                  )}
                  <FaFolderOpen style={{ color: '#ff9800' }} />
                </span>
              </td>
              <td className="col-path">
                <Link to={`${prefix}/${collapsedRow.original_path}`}>
                  <code>{segment}</code>
                </Link>
              </td>
              <td className="col-numeric">{formatSize(collapsedRow.size)}</td>
              <td className="col-numeric">{timeAgo(collapsedRow.mtime)}</td>
              <td className="col-numeric">{collapsedRow.n_children?.toLocaleString()}</td>
              <td className="col-numeric">{collapsedRow.n_desc && collapsedRow.n_desc > 1 ? collapsedRow.n_desc.toLocaleString() : null}</td>
              <td className="col-numeric">{scanStatus === 'full' && scanTime ? scanTimeAgo(scanTime) : null}</td>
              <td className="col-action">
                <Tooltip title="Rescan this directory">
                  <span>
                    <Button
                      size="small"
                      onClick={() => onScanChild(collapsedUri)}
                      disabled={isScanning}
                      sx={{ minWidth: 0, padding: '2px 4px' }}
                    >
                      {isScanning ? <CircularProgress size={14} /> : <FaSync size={12} />}
                    </Button>
                  </span>
                </Tooltip>
              </td>
              {routeType !== 's3' && (
                <td className="col-action">
                  <Tooltip title="Delete directory">
                    <span>
                      <Button
                        size="small"
                        onClick={() => onDelete(collapsedUri)}
                        disabled={isDeleting}
                        sx={{ minWidth: 0, padding: '2px 4px', color: '#d32f2f' }}
                      >
                        {isDeleting ? <CircularProgress size={14} /> : <FaTrash size={12} />}
                      </Button>
                    </span>
                  </Tooltip>
                </td>
              )}
            </tr>
          )
        })}
        {/* Only show children when collapsed rows are expanded (or there are no collapsed rows) */}
        {(!collapsedRows || collapsedExpanded) && children.map((row, idx) => {
          const childUri = row.uri
          const isChildScanning = scanningPaths.has(childUri)
          const isSelected = selectedPaths.has(childUri)
          const isCursor = hoveredIndex === idx  // Keyboard cursor position
          const isMouseHover = mouseHoverIndex === idx  // Mouse hover (for visual feedback)
          // Indent children under collapsed rows when expanded
          const indentPx = collapsedRows && collapsedExpanded ? collapsedRows.length * 20 : 0
          // Build the collapsed path prefix from the last collapsed row's original_path
          const collapsedPrefix = collapsedRows?.length
            ? collapsedRows[collapsedRows.length - 1].original_path
            : ''
          return (
            <tr
              key={row.path}
              style={{
                opacity: row.scanned || scanStatus === 'full' ? 1 : 0.6,
                background: isSelected
                  ? 'var(--selected-bg, rgba(25, 118, 210, 0.12))'
                  : isCursor
                    ? 'var(--cursor-bg, rgba(25, 118, 210, 0.08))'
                    : isMouseHover
                      ? 'var(--hover-bg, #f5f5f5)'
                      : undefined,
                // Show cursor indicator with left border
                boxShadow: isCursor ? 'inset 3px 0 0 var(--cursor-border, #1976d2)' : undefined,
              }}
              onClick={e => onRowClick(childUri, idx, e)}
              onMouseEnter={() => onRowHover(idx)}
              onMouseLeave={() => onRowHover(null)}
            >
              <td className="col-checkbox" onClick={e => e.stopPropagation()}>
                <Checkbox
                  size="small"
                  checked={isSelected}
                  onChange={() => {
                    // Checkbox always toggles (unlike row click which replaces selection)
                    onRowClick(childUri, idx, { metaKey: true, ctrlKey: true, shiftKey: false } as React.MouseEvent)
                  }}
                  sx={{ padding: 0 }}
                />
              </td>
              <td className={`col-icon${indentPx ? ' indented' : ''}`}>
                {routeType === 'file' ? (
                  <span
                    className="reveal-icon"
                    title="Reveal in Finder"
                    onClick={e => { e.stopPropagation(); revealPath(childUri) }}
                  >
                    <RowIcon row={row} />
                  </span>
                ) : (
                  <RowIcon row={row} />
                )}
              </td>
              <td className="col-path">
                <Link to={`${prefix}/${collapsedPrefix ? collapsedPrefix + '/' : ''}${row.path}`} onClick={e => e.stopPropagation()}>
                  <code>{row.path}</code>
                </Link>
                {row.expand_preview && row.expand_preview.split('/').map((segment, idx, arr) => {
                  const pathToSegment = `${row.path}/${arr.slice(0, idx + 1).join('/')}`
                  return (
                    <span key={idx}>
                      {' / '}
                      <Link to={`${prefix}/${collapsedPrefix ? collapsedPrefix + '/' : ''}${pathToSegment}`} onClick={e => e.stopPropagation()}>
                        <code>{segment}</code>
                      </Link>
                    </span>
                  )
                })}
              </td>
              <td className="col-numeric">{formatSize(row.size)}</td>
              <td className="col-numeric">{timeAgo(row.mtime)}</td>
              <td className="col-numeric">{row.n_children ? row.n_children.toLocaleString() : null}</td>
              <td className="col-numeric">{row.n_desc && row.n_desc > 1 ? row.n_desc.toLocaleString() : null}</td>
              <td className="col-numeric">
                <ChildScanStatus row={row} scanStatus={scanStatus} parentScanTime={scanTime} />
              </td>
              <td className="col-action" onClick={e => e.stopPropagation()}>
                {row.kind === 'dir' && (
                  <Tooltip title={row.scanned ? 'Rescan this directory' : 'Scan this directory'}>
                    <span>
                      <Button
                        size="small"
                        onClick={() => onScanChild(childUri)}
                        disabled={isChildScanning}
                        sx={{ minWidth: 0, padding: '2px 4px' }}
                      >
                        {isChildScanning ? <CircularProgress size={14} /> : <FaSync size={12} />}
                      </Button>
                    </span>
                  </Tooltip>
                )}
              </td>
              {routeType !== 's3' && (
                <td className="col-action" onClick={e => e.stopPropagation()}>
                  <Tooltip title={`Delete ${row.kind === 'dir' ? 'directory' : 'file'}`}>
                    <span>
                      <Button
                        size="small"
                        onClick={() => onDelete(childUri)}
                        disabled={deletingPaths.has(childUri)}
                        sx={{ minWidth: 0, padding: '2px 4px', color: '#d32f2f' }}
                      >
                        {deletingPaths.has(childUri) ? <CircularProgress size={14} /> : <FaTrash size={12} />}
                      </Button>
                    </span>
                  </Tooltip>
                </td>
              )}
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

type TreemapNode = {
  id: string
  label: string
  parent: string
  size: number
  isOther?: boolean
}

function Treemap({ root, rows, plotlySort = false, transpose = false, onToggleSort, onToggleTranspose }: { root: Row; rows: Row[]; plotlySort?: boolean; transpose?: boolean; onToggleSort?: () => void; onToggleTranspose?: () => void }) {
  // Build treemap data with "Other" placeholders for unaccounted space
  const data = useMemo(() => {
    const nodes: TreemapNode[] = []

    // Add root
    nodes.push({
      id: '.',
      label: root.path.split('/').pop() || '.',
      parent: '',
      size: root.size ?? 0,
    })

    // Add all rows
    for (const row of rows) {
      nodes.push({
        id: row.path,
        label: row.path.split('/').pop() || row.path,
        parent: row.parent || '.',
        size: row.size ?? 0,
      })
    }

    // Group rows by parent to calculate "other" sizes
    const childrenByParent = new Map<string, Row[]>()
    for (const row of rows) {
      const parent = row.parent || '.'
      if (!childrenByParent.has(parent)) {
        childrenByParent.set(parent, [])
      }
      childrenByParent.get(parent)!.push(row)
    }

    // Debug logging
    const depth1 = childrenByParent.get('.') || []
    const depth2ByParent = depth1.filter(r => r.kind === 'dir').map(r => {
      const children = childrenByParent.get(r.path) || []
      const childrenSize = children.reduce((s, c) => s + (c.size ?? 0), 0)
      const unaccounted = (r.size ?? 0) - childrenSize
      return {
        path: r.path,
        children: children.length,
        size: r.size,
        childrenSize,
        unaccounted,
        unaccountedMB: (unaccounted / 1e6).toFixed(1),
      }
    })
    console.log('[Treemap]', {
      totalRows: rows.length,
      depth1Count: depth1.length,
      depth2ByParent,
    })

    // Add placeholder nodes for directories with unaccounted size
    // (from truncated rows or depth limits)
    // Check root
    const rootChildren = childrenByParent.get('.') || []
    const rootChildrenSize = rootChildren.reduce((sum, c) => sum + (c.size ?? 0), 0)
    const rootUnaccounted = (root.size ?? 0) - rootChildrenSize
    if (rootUnaccounted > 1_000_000) {
      nodes.push({
        id: './__other__',
        label: '…',
        parent: '.',
        size: rootUnaccounted,
        isOther: true,
      })
    }

    // Check each depth-1 directory (direct children of root) for unaccounted size
    // Don't add placeholders for depth-2 items since we don't show their children
    for (const row of rows) {
      if (row.kind !== 'dir') continue
      if (row.parent !== '.') continue  // Only depth-1 directories
      const children = childrenByParent.get(row.path) || []
      const childrenSize = children.reduce((sum, c) => sum + (c.size ?? 0), 0)
      const unaccounted = (row.size ?? 0) - childrenSize
      // Show placeholder if unaccounted space is >1MB (significant enough to display)
      if (unaccounted > 1_000_000) {
        console.log(`[Treemap] Adding placeholder for ${row.path}: ${(unaccounted / 1e6).toFixed(1)}MB unaccounted`)
        nodes.push({
          id: `${row.path}/__other__`,
          label: '…',
          parent: row.path,
          size: unaccounted,
          isOther: true,
        })
      }
    }

    // Sort so real nodes are by size desc, "other" placeholders come last
    // (within each parent group). With sort=false on the treemap, this controls
    // the visual order: largest real items top-left, placeholders bottom-right.
    nodes.sort((a, b) => {
      // Root always first
      if (a.id === '.') return -1
      if (b.id === '.') return 1
      // Group by parent
      if (a.parent !== b.parent) return a.parent < b.parent ? -1 : 1
      // "Other" placeholders sort last within their parent
      if (a.isOther && !b.isOther) return 1
      if (!a.isOther && b.isOther) return -1
      // Real nodes: largest first
      return b.size - a.size
    })

    return nodes
  }, [root, rows])

  // Placeholder ("…") nodes get dark gray; regular nodes use Plotly auto-colors.
  const colors = data.map(n => n.isOther ? '#444444' : '')

  return (<>
    <Plot
      key={`treemap-${transpose}-${plotlySort}`}
      data={[{
        type: 'treemap',
        branchvalues: 'total',
        sort: plotlySort,
        ids: data.map(n => n.id),
        labels: data.map(n => n.label),
        parents: data.map(n => n.parent),
        values: data.map(n => n.size),
        text: data.map(n => n.isOther ? `<i>${formatSize(n.size)} not shown</i>` : formatSize(n.size)),
        texttemplate: '%{label}<br>%{text}',
        hovertemplate: '%{label}<br>%{text}<extra></extra>',
        tiling: { pad: 1, transpose },
        marker: {
          colors,
          line: { width: 1, color: 'rgba(255, 255, 255, 0.3)' },
        },
      } as Plotly.Data]}
      layout={{
        margin: { t: 10, r: 10, b: 10, l: 10 },
        paper_bgcolor: 'transparent',
      }}
      config={{
        displayModeBar: false,
        responsive: true,
      }}
      style={{ width: '100%', height: '400px' }}
    />
    {(onToggleSort || onToggleTranspose) && (
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginTop: 2, opacity: 0.6, fontSize: 12 }}>
        {onToggleTranspose && (
          <label style={{ cursor: 'pointer', userSelect: 'none' }}>
            <input type="checkbox" checked={transpose} onChange={onToggleTranspose} style={{ marginRight: 4 }} />
            Transpose
          </label>
        )}
        {onToggleSort && (
          <label style={{ cursor: 'pointer', userSelect: 'none' }}>
            <input type="checkbox" checked={plotlySort} onChange={onToggleSort} style={{ marginRight: 4 }} />
            Plotly sort
          </label>
        )}
      </div>
    )}
  </>
  )
}

function FilePreviewSection({ path }: { path: string }) {
  const { data: preview, isLoading, error } = useQuery({
    queryKey: ['file-preview', path],
    queryFn: () => fetchFilePreview(path),
    staleTime: 60 * 1000,
  })

  if (isLoading) {
    return (
      <Box sx={{ mt: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
        <CircularProgress size={16} />
        <span>Loading preview...</span>
      </Box>
    )
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        Failed to load preview: {error instanceof Error ? error.message : 'Unknown error'}
      </Alert>
    )
  }

  if (!preview) {
    return null
  }

  if (!preview.is_text) {
    return (
      <Box sx={{ mt: 2 }}>
        <Box sx={{ mb: 1, fontSize: '0.85rem', opacity: 0.7 }}>
          Binary file ({formatSize(preview.size)})
          {preview.hex_truncated && ` — showing first ${formatSize(preview.preview_bytes)}`}
        </Box>
        {preview.hex && (
          <Box
            component="pre"
            sx={{
              p: 2,
              bgcolor: 'rgba(0,0,0,0.3)',
              borderRadius: 1,
              overflow: 'auto',
              maxHeight: '500px',
              fontSize: '0.75rem',
              lineHeight: 1.3,
              fontFamily: 'monospace',
              m: 0,
              whiteSpace: 'pre',
            }}
          >
            {preview.hex}
          </Box>
        )}
      </Box>
    )
  }

  return (
    <Box sx={{ mt: 2 }}>
      <Box sx={{ mb: 1, fontSize: '0.85rem', opacity: 0.7 }}>
        Preview {preview.truncated && `(first ${formatSize(preview.preview_bytes)} of ${formatSize(preview.size)})`}
      </Box>
      <Box
        component="pre"
        sx={{
          p: 2,
          bgcolor: 'rgba(0,0,0,0.3)',
          borderRadius: 1,
          overflow: 'auto',
          maxHeight: '500px',
          fontSize: '0.8rem',
          lineHeight: 1.4,
          fontFamily: 'monospace',
          m: 0,
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        {preview.content}
      </Box>
    </Box>
  )
}

export function ScanDetails() {
  const params = useParams()
  const pathSegments = params['*'] || ''
  const pathname = window.location.pathname
  const isS3 = pathname.startsWith('/s3')
  const isSsh = pathname.startsWith('/ssh')
  const routeType: RouteType = isS3 ? 's3' : isSsh ? 'ssh' : 'file'

  const uri = isS3
    ? `s3://${pathSegments}`
    : isSsh
      ? `ssh://${pathSegments}`
      : `/${pathSegments}`

  // Selected scan ID for time-travel (undefined = latest)
  const [selectedScanId, setSelectedScanId] = useState<number | undefined>(undefined)

  // Fetch scan history for the dropdown
  const { data: scanHistory } = useQuery({
    queryKey: ['scan-history', uri],
    queryFn: () => fetchScanHistory(uri),
    staleTime: 60 * 1000,
  })

  const [treemapMaxRows, setTreemapMaxRows] = useState(DEFAULT_MAX_ROWS)
  const [plotlySort, setPlotlySort] = useState(false)
  const [transpose, setTranspose] = useState(false)
  const { data: details, isLoading, error: queryError, refetch } = useQuery({
    queryKey: ['scan-details', uri, selectedScanId, treemapMaxRows],
    queryFn: () => fetchScanDetails(uri, selectedScanId, 2, treemapMaxRows),
    staleTime: 60 * 1000, // 1 minute - scan details don't change frequently
  })
  const [mutationError, setMutationError] = useState<string | null>(null)
  const error = queryError?.message || mutationError
  const [scanning, setScanning] = useState(false)
  const [scanJob, setScanJob] = useState<ScanJob | null>(null)
  const [childJobs, setChildJobs] = useState<Map<string, ScanJob>>(new Map())
  const [sorts, setSorts] = useState<SortSpec[]>([{ key: 'size', dir: 'desc' }])
  const [deletingPaths, setDeletingPaths] = useState<Set<string>>(new Set())
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(50)
  const [filter, setFilter] = useState('')
  // Selection model (Superhuman-style):
  // - hoveredIndex: keyboard cursor position (moving end of range)
  // - rangeAnchor: fixed end of range selection
  // - pinnedUris: items selected via meta-click that persist across range changes
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const [rangeAnchor, setRangeAnchor] = useState<number | null>(null)
  const [pinnedUris, setPinnedUris] = useState<Set<string>>(new Set())
  const [mouseHoverIndex, setMouseHoverIndex] = useState<number | null>(null)
  const wrapperRef = useRef<HTMLDivElement>(null)
  const tableRef = useRef<HTMLTableElement>(null)

  // Live scan progress from SSE
  const scanProgress = useScanProgress()

  // Record visit to recent paths
  const { recordVisit } = useRecentPaths()
  useEffect(() => {
    if (uri && uri !== '/' && uri !== 's3://') {
      recordVisit(uri, 'tree')
    }
  }, [uri, recordVisit])

  const handleSort = (key: SortKey) => {
    setSorts(prev => {
      const existingIdx = prev.findIndex(s => s.key === key)
      if (existingIdx === 0) {
        // Toggle direction if clicking current primary sort
        return [{ key, dir: prev[0].dir === 'asc' ? 'desc' : 'asc' }, ...prev.slice(1)]
      }
      // Make this the new primary sort, keep previous as secondary
      const newSorts: SortSpec[] = [{ key, dir: 'desc' }]
      // Add previous primary as secondary (if different)
      if (prev.length > 0 && prev[0].key !== key) {
        newSorts.push(prev[0])
      }
      return newSorts
    })
  }

  const filteredChildren = useMemo(() => {
    if (!details) return []
    const { children } = details
    if (!filter.trim()) return children
    const lowerFilter = filter.toLowerCase()
    return children.filter(r => r.path.toLowerCase().includes(lowerFilter))
  }, [details, filter])

  const sortedChildren = useMemo(() => {
    return [...filteredChildren].sort((a, b) => {
      for (const { key, dir } of sorts) {
        let cmp = 0
        switch (key) {
          case 'kind':
            cmp = (a.kind === 'dir' ? 0 : 1) - (b.kind === 'dir' ? 0 : 1)
            break
          case 'path':
            cmp = a.path.localeCompare(b.path)
            break
          case 'size':
            cmp = (a.size ?? 0) - (b.size ?? 0)
            break
          case 'mtime':
            cmp = (a.mtime ?? 0) - (b.mtime ?? 0)
            break
          case 'n_children':
            cmp = (a.n_children ?? 0) - (b.n_children ?? 0)
            break
          case 'n_desc':
            cmp = (a.n_desc ?? 0) - (b.n_desc ?? 0)
            break
          case 'scanned':
            // Sort by: has own scan_time > scanned true > scanned partial > not scanned
            const scanOrder = (r: Row) => {
              if (r.scan_time) return 3
              if (r.scanned === true) return 2
              if (r.scanned === 'partial') return 1
              return 0
            }
            cmp = scanOrder(a) - scanOrder(b)
            break
        }
        if (cmp !== 0) {
          return dir === 'asc' ? cmp : -cmp
        }
      }
      return 0
    })
  }, [filteredChildren, sorts])

  const totalPages = Math.ceil(sortedChildren.length / pageSize)
  const paginatedChildren = useMemo(() => {
    const start = page * pageSize
    return sortedChildren.slice(start, start + pageSize)
  }, [sortedChildren, page, pageSize])

  // Compute selectedPaths from pinnedUris + range(rangeAnchor, hoveredIndex)
  const selectedPaths = useMemo(() => {
    const result = new Set(pinnedUris)
    if (hoveredIndex !== null && rangeAnchor !== null) {
      const start = Math.min(hoveredIndex, rangeAnchor)
      const end = Math.max(hoveredIndex, rangeAnchor)
      for (let i = start; i <= end; i++) {
        if (paginatedChildren[i]) {
          result.add(paginatedChildren[i].uri)
        }
      }
    }
    return result
  }, [pinnedUris, hoveredIndex, rangeAnchor, paginatedChildren])

  // Reset page when sort/filter changes or data reloads
  useEffect(() => {
    setPage(0)
  }, [sorts, filter, details])

  // Clear selection when data changes
  useEffect(() => {
    setHoveredIndex(null)
    setRangeAnchor(null)
    setPinnedUris(new Set())
  }, [details])

  // Handle row click with shift/meta modifiers
  const handleRowClick = useCallback((uri: string, index: number, event: React.MouseEvent | React.KeyboardEvent) => {
    const shiftKey = event.shiftKey
    const metaKey = 'metaKey' in event ? event.metaKey || event.ctrlKey : false

    if (metaKey) {
      // Meta-click: pin current selection, then toggle this item and set new anchor
      setPinnedUris(prev => {
        const next = new Set(prev)
        // Add current range to pinned
        if (hoveredIndex !== null && rangeAnchor !== null) {
          const start = Math.min(hoveredIndex, rangeAnchor)
          const end = Math.max(hoveredIndex, rangeAnchor)
          for (let i = start; i <= end; i++) {
            if (paginatedChildren[i]) {
              next.add(paginatedChildren[i].uri)
            }
          }
        }
        // Toggle clicked item
        if (next.has(uri)) {
          next.delete(uri)
        } else {
          next.add(uri)
        }
        return next
      })
      setHoveredIndex(index)
      setRangeAnchor(index)
    } else if (shiftKey && rangeAnchor !== null) {
      // Shift-click: extend range from anchor to clicked (pinnedUris stay)
      setHoveredIndex(index)
    } else {
      // Regular click: toggle if only this row selected, otherwise select just this row
      const isOnlySelected = selectedPaths.size === 1 && selectedPaths.has(uri)
      if (isOnlySelected) {
        // Clicking the only selected row deselects it
        setHoveredIndex(null)
        setRangeAnchor(null)
        setPinnedUris(new Set())
      } else {
        // Select just this row
        setHoveredIndex(index)
        setRangeAnchor(index)
        setPinnedUris(new Set())
      }
    }
  }, [hoveredIndex, rangeAnchor, paginatedChildren, selectedPaths])

  // Click outside table to deselect
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      // Clear selection when clicking outside the wrapper (table + toolbar)
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setHoveredIndex(null)
        setRangeAnchor(null)
        setPinnedUris(new Set())
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Get initial cursor position (from mouse hover or start/end)
  const getInitialIndex = useCallback((direction: 'up' | 'down') => {
    if (mouseHoverIndex !== null && mouseHoverIndex >= 0 && mouseHoverIndex < paginatedChildren.length) {
      return mouseHoverIndex
    }
    return direction === 'up' ? paginatedChildren.length - 1 : 0
  }, [mouseHoverIndex, paginatedChildren.length])

  // Move cursor up (clears selection, sets single-row selection)
  useAction('table:up', {
    label: 'Row up',
    group: 'Table: Navigation',
    defaultBindings: ['k', 'arrowup'],
    handler: useCallback(() => {
      if (paginatedChildren.length === 0) return
      const newIndex = hoveredIndex === null
        ? getInitialIndex('up')
        : Math.max(0, hoveredIndex - 1)
      setHoveredIndex(newIndex)
      setRangeAnchor(newIndex)
      setPinnedUris(new Set())
    }, [hoveredIndex, paginatedChildren.length, getInitialIndex]),
  })

  // Move cursor down
  useAction('table:down', {
    label: 'Row down',
    group: 'Table: Navigation',
    defaultBindings: ['j', 'arrowdown'],
    handler: useCallback(() => {
      if (paginatedChildren.length === 0) return
      const newIndex = hoveredIndex === null
        ? getInitialIndex('down')
        : Math.min(paginatedChildren.length - 1, hoveredIndex + 1)
      setHoveredIndex(newIndex)
      setRangeAnchor(newIndex)
      setPinnedUris(new Set())
    }, [hoveredIndex, paginatedChildren.length, getInitialIndex]),
  })

  // Extend selection up (keeps anchor fixed, moves cursor)
  useAction('table:extend-up', {
    label: 'Extend selection up',
    group: 'Table: Selection',
    defaultBindings: ['shift+k', 'shift+arrowup'],
    handler: useCallback(() => {
      if (paginatedChildren.length === 0) return
      if (rangeAnchor === null) {
        // First shift+arrow: set anchor and move cursor
        const startIndex = hoveredIndex ?? getInitialIndex('up')
        setRangeAnchor(startIndex)
        setHoveredIndex(Math.max(0, startIndex - 1))
      } else {
        // Move cursor up (anchor stays fixed)
        setHoveredIndex(prev => Math.max(0, (prev ?? rangeAnchor) - 1))
      }
    }, [hoveredIndex, rangeAnchor, paginatedChildren.length, getInitialIndex]),
  })

  // Extend selection down
  useAction('table:extend-down', {
    label: 'Extend selection down',
    group: 'Table: Selection',
    defaultBindings: ['shift+j', 'shift+arrowdown'],
    handler: useCallback(() => {
      if (paginatedChildren.length === 0) return
      if (rangeAnchor === null) {
        // First shift+arrow: set anchor and move cursor
        const startIndex = hoveredIndex ?? getInitialIndex('down')
        setRangeAnchor(startIndex)
        setHoveredIndex(Math.min(paginatedChildren.length - 1, startIndex + 1))
      } else {
        // Move cursor down (anchor stays fixed)
        setHoveredIndex(prev => Math.min(paginatedChildren.length - 1, (prev ?? rangeAnchor) + 1))
      }
    }, [hoveredIndex, rangeAnchor, paginatedChildren.length, getInitialIndex]),
  })

  // Clear selection
  useAction('table:clear', {
    label: 'Clear selection',
    group: 'Table: Selection',
    defaultBindings: ['escape'],
    handler: useCallback(() => {
      setHoveredIndex(null)
      setRangeAnchor(null)
      setPinnedUris(new Set())
    }, []),
  })

  // Select all
  useAction('table:select-all', {
    label: 'Select all',
    group: 'Table: Selection',
    defaultBindings: ['meta+a'],
    handler: useCallback(() => {
      if (paginatedChildren.length === 0) return
      setPinnedUris(new Set(paginatedChildren.map(r => r.uri)))
      setHoveredIndex(paginatedChildren.length - 1)
      setRangeAnchor(0)
    }, [paginatedChildren]),
  })

  // Compute selection summary
  const selectedRows = useMemo(() => {
    return paginatedChildren.filter(r => selectedPaths.has(r.uri))
  }, [paginatedChildren, selectedPaths])

  const selectedSize = useMemo(() => {
    return selectedRows.reduce((sum, r) => sum + (r.size ?? 0), 0)
  }, [selectedRows])

  const selectedDirs = useMemo(() => {
    return selectedRows.filter(r => r.kind === 'dir')
  }, [selectedRows])

  // Bulk actions
  const handleBulkScan = async () => {
    for (const row of selectedDirs) {
      try {
        const job = await startScan(row.uri)
        setChildJobs(prev => new Map(prev).set(row.uri, job))
      } catch (e) {
        setMutationError(e instanceof Error ? e.message : 'Failed to start scan')
      }
    }
  }

  const handleBulkDelete = async () => {
    if (selectedRows.length === 0) return

    const msg = selectedRows.length === 1
      ? `Delete "${selectedRows[0].path}"?`
      : `Delete ${selectedRows.length} items (${formatSize(selectedSize)})?`

    if (!confirm(`${msg} This cannot be undone.`)) {
      return
    }

    const pathsToDelete = selectedRows.map(r => r.uri)
    setDeletingPaths(prev => {
      const next = new Set(prev)
      pathsToDelete.forEach(p => next.add(p))
      return next
    })

    for (const path of pathsToDelete) {
      try {
        await deletePath(path)
      } catch (e) {
        setMutationError(e instanceof Error ? e.message : 'Failed to delete')
      }
    }

    setDeletingPaths(prev => {
      const next = new Set(prev)
      pathsToDelete.forEach(p => next.delete(p))
      return next
    })

    setHoveredIndex(null)
    setRangeAnchor(null)
    setPinnedUris(new Set())
    refetch()
  }

  // Poll scan job status (main scan)
  useEffect(() => {
    if (!scanJob || (scanJob.status !== 'pending' && scanJob.status !== 'running')) return

    const interval = setInterval(async () => {
      const status = await fetchScanStatus(scanJob.job_id)
      setScanJob(status)
      if (status.status === 'completed') {
        setScanning(false)
        refetch()
      } else if (status.status === 'failed') {
        setScanning(false)
        setMutationError(status.error || 'Scan failed')
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [scanJob])

  // Poll child scan jobs
  useEffect(() => {
    const activeJobs = Array.from(childJobs.entries()).filter(
      ([, job]) => job.status === 'pending' || job.status === 'running'
    )
    if (activeJobs.length === 0) return

    const interval = setInterval(async () => {
      const updates = new Map(childJobs)
      let anyCompleted = false

      for (const [path, job] of activeJobs) {
        const status = await fetchScanStatus(job.job_id)
        updates.set(path, status)
        if (status.status === 'completed') {
          anyCompleted = true
        }
      }

      setChildJobs(updates)
      if (anyCompleted) {
        refetch()
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [childJobs])

  const handleRescan = async () => {
    setScanning(true)
    try {
      const job = await startScan(uri)
      setScanJob(job)
    } catch (e) {
      setScanning(false)
      setMutationError(e instanceof Error ? e.message : 'Failed to start scan')
    }
  }

  const handleScanChild = async (childPath: string) => {
    try {
      const job = await startScan(childPath)
      setChildJobs(prev => new Map(prev).set(childPath, job))
    } catch (e) {
      setMutationError(e instanceof Error ? e.message : 'Failed to start scan')
    }
  }

  const handleDelete = async (path: string) => {
    const name = path.split('/').pop() || path
    if (!confirm(`Delete "${name}"? This cannot be undone.`)) {
      return
    }

    setDeletingPaths(prev => new Set(prev).add(path))
    try {
      await deletePath(path)
      refetch()
    } catch (e) {
      setMutationError(e instanceof Error ? e.message : 'Failed to delete')
    } finally {
      setDeletingPaths(prev => {
        const next = new Set(prev)
        next.delete(path)
        return next
      })
    }
  }

  const scanningPaths = new Set(
    Array.from(childJobs.entries())
      .filter(([, job]) => job.status === 'pending' || job.status === 'running')
      .map(([path]) => path)
  )

  if (isLoading) return <div>Loading...</div>
  if (error && !details) {
    return (
      <div>
        <p>Error: {error}</p>
        {error.includes('No scan found') && (
          <Button
            variant="contained"
            onClick={handleRescan}
            disabled={scanning}
            startIcon={scanning ? <CircularProgress size={16} /> : <FaSync />}
          >
            {scanning ? 'Scanning...' : 'Scan This Path'}
          </Button>
        )}
      </div>
    )
  }
  if (!details) return <div>No data</div>

  const { root, rows, scan_status, time, error_count, error_paths, collapsed_rows } = details

  // Parse error_paths JSON string to array (inline, no hook needed)
  const parsedErrorPaths = error_paths ? (() => {
    try {
      return JSON.parse(error_paths) as string[]
    } catch {
      return null
    }
  })() : null

  // Format scan time for dropdown display
  const formatScanTime = (timeStr: string) => {
    const date = new Date(timeStr)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))
    const timeFormatted = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    if (diffDays === 0) return `Today ${timeFormatted}`
    if (diffDays === 1) return `Yesterday ${timeFormatted}`
    if (diffDays < 7) return `${diffDays}d ago ${timeFormatted}`
    return date.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ` ${timeFormatted}`
  }

  return (
    <div ref={wrapperRef} tabIndex={0} style={{ outline: 'none' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1, flexWrap: 'wrap' }}>
        <h1 style={{ margin: 0 }}>
          <Breadcrumbs uri={uri} routeType={routeType} />
          {collapsed_rows && collapsed_rows.map((row, idx) => {
            const segment = row.original_path.split('/').pop() || row.original_path
            const fullPath = `${uri}/${row.original_path}`
            return (
              <span key={idx}>
                <span className="breadcrumb-sep">/</span>
                <Link to={`${routeType === 's3' ? '/s3/' : routeType === 'ssh' ? '/ssh/' : '/file'}${fullPath}`}>
                  {segment}
                </Link>
              </span>
            )
          })}
        </h1>
        {scanHistory && scanHistory.length > 1 && (
          <Tooltip title="View historical scans">
            <select
              value={selectedScanId ?? ''}
              onChange={e => setSelectedScanId(e.target.value ? Number(e.target.value) : undefined)}
              style={{ padding: '4px 8px', fontSize: '0.85rem', background: '#2d2d2d', color: '#e6edf3', border: '1px solid #444', borderRadius: 4 }}
            >
              <option value="">Latest scan</option>
              {scanHistory.map(scan => (
                <option key={scan.id} value={scan.id}>
                  {formatScanTime(scan.time)} - {formatSize(scan.size ?? 0)}
                  {scan.scan_path !== uri && ` (from ${scan.scan_path})`}
                </option>
              ))}
            </select>
          </Tooltip>
        )}
      </Box>
      <ScanProgressBanner progress={scanProgress} currentUri={uri} />
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {error_count && error_count > 0 && (
        <PermissionErrorWarning
          errorCount={error_count}
          errorPaths={parsedErrorPaths}
          scanPath={details.scan_path}
        />
      )}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
        <TextField
          size="small"
          placeholder="Filter by name..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
          slotProps={{ input: { startAdornment: <FaSearch style={{ marginRight: 8, opacity: 0.5 }} /> } }}
          sx={{ width: 250 }}
        />
        {selectedRows.length > 0 && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, fontSize: '0.85rem' }}>
            <span style={{ opacity: 0.8 }}>
              {selectedRows.length} selected ({formatSize(selectedSize)})
            </span>
            {selectedDirs.length > 0 && (
              <Tooltip title={`Scan ${selectedDirs.length} director${selectedDirs.length === 1 ? 'y' : 'ies'}`}>
                <Button
                  size="small"
                  onClick={handleBulkScan}
                  startIcon={<FaSync size={12} />}
                  sx={{ minWidth: 0 }}
                >
                  Scan
                </Button>
              </Tooltip>
            )}
            {routeType !== 's3' && (
              <Tooltip title={`Delete ${selectedRows.length} item${selectedRows.length === 1 ? '' : 's'}`}>
                <Button
                  size="small"
                  onClick={handleBulkDelete}
                  startIcon={<FaTrash size={12} />}
                  sx={{ minWidth: 0, color: '#d32f2f' }}
                >
                  Delete
                </Button>
              </Tooltip>
            )}
            <Button
              size="small"
              onClick={() => {
                setHoveredIndex(null)
                setRangeAnchor(null)
                setPinnedUris(new Set())
              }}
              sx={{ minWidth: 0, opacity: 0.7 }}
            >
              Clear
            </Button>
          </Box>
        )}
      </Box>
      <DetailsTable
        root={root}
        children={paginatedChildren}
        uri={uri}
        routeType={routeType}
        onScanChild={handleScanChild}
        scanningPaths={scanningPaths}
        scanStatus={scan_status}
        scanTime={time}
        onRescan={handleRescan}
        isScanning={scanning}
        sorts={sorts}
        onSort={handleSort}
        onDelete={handleDelete}
        deletingPaths={deletingPaths}
        selectedPaths={selectedPaths}
        hoveredIndex={hoveredIndex}
        mouseHoverIndex={mouseHoverIndex}
        onRowClick={handleRowClick}
        onRowHover={setMouseHoverIndex}
        collapsedRows={collapsed_rows}
        tableRef={tableRef}
      />
      {sortedChildren.length > pageSize && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginTop: '0.5rem', fontSize: '0.85rem' }}>
          <span style={{ opacity: 0.7 }}>
            {page * pageSize + 1}-{Math.min((page + 1) * pageSize, sortedChildren.length)} of {sortedChildren.length}
          </span>
          <div style={{ display: 'flex', gap: '0.25rem' }}>
            <Button size="small" disabled={page === 0} onClick={() => setPage(0)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              &laquo;
            </Button>
            <Button size="small" disabled={page === 0} onClick={() => setPage(p => p - 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              &lsaquo;
            </Button>
            <Button size="small" disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              &rsaquo;
            </Button>
            <Button size="small" disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              &raquo;
            </Button>
          </div>
          <select
            value={pageSize}
            onChange={e => { setPageSize(Number(e.target.value)); setPage(0) }}
            style={{ padding: '2px 4px', fontSize: '0.85rem' }}
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={250}>250</option>
          </select>
        </div>
      )}
      {rows.length > 0 && (
        <Box sx={{ mt: 2 }}>
          <Treemap root={root} rows={rows} plotlySort={plotlySort} transpose={transpose} onToggleSort={() => setPlotlySort(v => !v)} onToggleTranspose={() => setTranspose(v => !v)} />
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mt: 1, fontSize: '0.85rem', opacity: 0.7 }}>
            <span>{rows.length} items</span>
            <label>
              Max:
              <select
                value={treemapMaxRows}
                onChange={e => setTreemapMaxRows(Number(e.target.value))}
                style={{ marginLeft: 4 }}
              >
                <option value={500}>500</option>
                <option value={1000}>1,000</option>
                <option value={2000}>2,000</option>
                <option value={5000}>5,000</option>
                <option value={0}>All</option>
              </select>
            </label>
          </Box>
        </Box>
      )}
      {root.kind === 'file' && routeType === 'file' && <FilePreviewSection path={uri} />}
    </div>
  )
}
