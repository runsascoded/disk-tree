import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Button, CircularProgress, Tooltip } from '@mui/material'
import { FaFileAlt, FaFolder, FaFolderOpen, FaSync, FaSortUp, FaSortDown, FaTrash } from 'react-icons/fa'
import Plot from 'react-plotly.js'
import { fetchScanDetails, startScan, fetchScanStatus, deletePath } from '../api'
import type { Row, ScanDetails as ScanDetailsType, ScanJob } from '../api'

type SortKey = 'kind' | 'path' | 'size' | 'mtime' | 'n_children' | 'n_desc' | 'scanned'
type SortDir = 'asc' | 'desc'
type SortSpec = { key: SortKey; dir: SortDir }

function sizeStr(bytes: number | null): string {
  if (bytes === null) return '-'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GB`
}

function timeAgo(timestamp: number | null): string {
  if (timestamp === null) return '-'
  const date = new Date(timestamp * 1000)
  const now = new Date()
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000)

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

type RouteType = 'file' | 's3'

function Breadcrumbs({ uri, routeType }: { uri: string; routeType: RouteType }) {
  const prefix = routeType === 's3' ? '/s3' : '/file'
  const displayUri = routeType === 's3' ? uri.replace('s3://', '') : uri

  const segments = displayUri.split('/').filter(Boolean)
  const paths = segments.reduce((acc, seg) => {
    const prev = acc[acc.length - 1]
    acc.push(prev ? `${prev}/${seg}` : seg)
    return acc
  }, [] as string[])

  return (
    <div className="breadcrumbs">
      {routeType === 's3' && <span>s3://</span>}
      {paths.map((path, idx) => (
        <span key={idx}>
          {routeType === 'file' && <span>/</span>}
          {idx === paths.length - 1 ? (
            <span>{segments[idx]}</span>
          ) : (
            <Link to={`${prefix}/${path}`}>{segments[idx]}</Link>
          )}
          {routeType === 's3' && idx < paths.length - 1 && <span>/</span>}
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
}: {
  label: string
  sortKey: SortKey
  sorts: SortSpec[]
  onSort: (key: SortKey) => void
  tooltip?: string
}) {
  const primarySort = sorts[0]
  const isPrimary = primarySort?.key === sortKey
  const isSecondary = sorts.length > 1 && sorts[1]?.key === sortKey

  const header = (
    <th
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

function DetailsTable({ root, children, uri, routeType, onScanChild, scanningPaths, scanStatus, scanTime, onRescan, isScanning, sorts, onSort, onDelete, deletingPaths }: {
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
}) {
  const prefix = routeType === 's3' ? `/s3/${uri.replace('s3://', '')}` : `/file${uri}`
  return (
    <table>
      <thead>
        <tr>
          <SortableHeader label="" sortKey="kind" sorts={sorts} onSort={onSort} tooltip="Sort by type (file/folder)" />
          <SortableHeader label="Path" sortKey="path" sorts={sorts} onSort={onSort} />
          <SortableHeader label="Size" sortKey="size" sorts={sorts} onSort={onSort} tooltip="Total size including all nested files and directories" />
          <SortableHeader label="Modified" sortKey="mtime" sorts={sorts} onSort={onSort} tooltip="Most recent modification time of any file in this directory tree" />
          <SortableHeader label="Children" sortKey="n_children" sorts={sorts} onSort={onSort} tooltip="Number of direct children (files and subdirectories)" />
          <SortableHeader label="Desc." sortKey="n_desc" sorts={sorts} onSort={onSort} tooltip="Total number of descendants (all nested files and directories)" />
          <SortableHeader label="Scanned" sortKey="scanned" sorts={sorts} onSort={onSort} tooltip="When this directory was last scanned" />
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr className="root">
          <td>{root.kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
          <td><code>.</code></td>
          <td>{sizeStr(root.size)}</td>
          <td>{timeAgo(root.mtime)}</td>
          <td>{root.n_children?.toLocaleString()}</td>
          <td>{root.n_desc && root.n_desc > 1 ? root.n_desc.toLocaleString() : null}</td>
          <td>
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
          <td>
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
          </td>
          <td></td>
        </tr>
        {children.map(row => {
          const childUri = row.uri
          const isChildScanning = scanningPaths.has(childUri)
          return (
            <tr key={row.path} style={{ opacity: row.scanned || scanStatus === 'full' ? 1 : 0.6 }}>
              <td><RowIcon row={row} /></td>
              <td>
                <Link to={`${prefix}/${row.path}`}>
                  <code>{row.path}</code>
                </Link>
              </td>
              <td>{sizeStr(row.size)}</td>
              <td>{timeAgo(row.mtime)}</td>
              <td>{row.n_children ? row.n_children.toLocaleString() : null}</td>
              <td>{row.n_desc && row.n_desc > 1 ? row.n_desc.toLocaleString() : null}</td>
              <td>
                <ChildScanStatus row={row} scanStatus={scanStatus} parentScanTime={scanTime} />
              </td>
              <td>
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
              <td>
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
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function Treemap({ root, rows }: { root: Row; rows: Row[] }) {
  const data = [root, ...rows]

  return (
    <Plot
      data={[{
        type: 'treemap',
        branchvalues: 'total',
        ids: data.map(r => r.path),
        labels: data.map(r => r.path.split('/').pop() || r.path),
        // Root has no parent (''), all children should reference '.' as parent
        parents: data.map(r => r.path === '.' ? '' : (r.parent || '.')),
        values: data.map(r => r.size),
        text: data.map(r => sizeStr(r.size)),
        texttemplate: '%{label}<br>%{text}',
        hovertemplate: '%{label}<br>%{value} bytes<br>%{text}',
      }]}
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
  )
}

export function ScanDetails() {
  const params = useParams()
  const pathSegments = params['*'] || ''
  const isS3 = window.location.pathname.startsWith('/s3')
  const routeType: RouteType = isS3 ? 's3' : 'file'

  const uri = isS3
    ? `s3://${pathSegments}`
    : `/${pathSegments}`

  const [details, setDetails] = useState<ScanDetailsType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [scanning, setScanning] = useState(false)
  const [scanJob, setScanJob] = useState<ScanJob | null>(null)
  const [childJobs, setChildJobs] = useState<Map<string, ScanJob>>(new Map())
  const [sorts, setSorts] = useState<SortSpec[]>([{ key: 'size', dir: 'desc' }])
  const [deletingPaths, setDeletingPaths] = useState<Set<string>>(new Set())
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(50)

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

  const sortedChildren = useMemo(() => {
    if (!details) return []
    const { children } = details

    return [...children].sort((a, b) => {
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
  }, [details, sorts])

  const totalPages = Math.ceil(sortedChildren.length / pageSize)
  const paginatedChildren = useMemo(() => {
    const start = page * pageSize
    return sortedChildren.slice(start, start + pageSize)
  }, [sortedChildren, page, pageSize])

  // Reset page when sort changes or data reloads
  useEffect(() => {
    setPage(0)
  }, [sorts, details])

  const loadDetails = () => {
    setLoading(true)
    setError(null)
    setDetails(null) // Clear previous data when loading new URI
    fetchScanDetails(uri)
      .then(setDetails)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    loadDetails()
  }, [uri])

  // Poll scan job status (main scan)
  useEffect(() => {
    if (!scanJob || (scanJob.status !== 'pending' && scanJob.status !== 'running')) return

    const interval = setInterval(async () => {
      const status = await fetchScanStatus(scanJob.job_id)
      setScanJob(status)
      if (status.status === 'completed') {
        setScanning(false)
        loadDetails() // Refresh data
      } else if (status.status === 'failed') {
        setScanning(false)
        setError(status.error || 'Scan failed')
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
        loadDetails() // Refresh to show updated scan data
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
      setError(e instanceof Error ? e.message : 'Failed to start scan')
    }
  }

  const handleScanChild = async (childPath: string) => {
    try {
      const job = await startScan(childPath)
      setChildJobs(prev => new Map(prev).set(childPath, job))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start scan')
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
      loadDetails() // Refresh to show updated data
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete')
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

  if (loading) return <div>Loading...</div>
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

  const { root, rows, scan_status, time } = details

  return (
    <div>
      <h1 style={{ marginBottom: '1rem' }}><Breadcrumbs uri={uri} routeType={routeType} /></h1>
      {error && <p style={{ color: 'red' }}>{error}</p>}
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
      {rows.length > 0 && <Treemap root={root} rows={rows} />}
    </div>
  )
}
