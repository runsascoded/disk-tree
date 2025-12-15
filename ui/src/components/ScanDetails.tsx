import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Button, Chip, CircularProgress, Tooltip } from '@mui/material'
import { FaFileAlt, FaFolder, FaFolderOpen, FaSync } from 'react-icons/fa'
import Plot from 'react-plotly.js'
import { fetchScanDetails, startScan, fetchScanStatus } from '../api'
import type { Row, ScanDetails as ScanDetailsType, ScanJob } from '../api'

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

function ScanStatusChip({ status }: { status: 'full' | 'partial' | 'none' }) {
  if (status === 'full') {
    return (
      <Tooltip title="This directory has been fully scanned">
        <Chip label="Scanned" color="success" size="small" />
      </Tooltip>
    )
  }
  if (status === 'partial') {
    return (
      <Tooltip title="Some subdirectories have been scanned, but not this directory itself">
        <Chip label="Partial" color="warning" size="small" />
      </Tooltip>
    )
  }
  return (
    <Tooltip title="This directory has not been scanned yet">
      <Chip label="Not scanned" color="default" size="small" />
    </Tooltip>
  )
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

function ChildScanStatus({ row, scanStatus }: { row: Row; scanStatus: 'full' | 'partial' | 'none' }) {
  // If parent was fully scanned, children inherit that scan
  if (scanStatus === 'full' && !row.scan_time) {
    return <span style={{ opacity: 0.6 }}>via parent</span>
  }
  if (row.scanned === 'partial') {
    return <span style={{ color: '#ff9800' }}>partial</span>
  }
  if (row.scan_time) {
    return <span>{scanTimeAgo(row.scan_time)}</span>
  }
  return <span style={{ opacity: 0.4 }}>-</span>
}

function DetailsTable({ root, children, uri, routeType, onScanChild, scanningPaths, scanStatus, scanTime, onRescan, isScanning }: {
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
}) {
  const prefix = routeType === 's3' ? `/s3/${uri.replace('s3://', '')}` : `/file${uri}`
  return (
    <table>
      <thead>
        <tr>
          <th></th>
          <th>Path</th>
          <Tooltip title="Total size including all nested files and directories">
            <th>Size</th>
          </Tooltip>
          <Tooltip title="Most recent modification time of any file in this directory tree">
            <th>Modified</th>
          </Tooltip>
          <Tooltip title="Number of direct children (files and subdirectories)">
            <th>Children</th>
          </Tooltip>
          <Tooltip title="Total number of descendants (all nested files and directories)">
            <th>Desc.</th>
          </Tooltip>
          <Tooltip title="When this directory was last scanned">
            <th>Scanned</th>
          </Tooltip>
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
            <ScanStatusChip status={scanStatus} />
            {scanStatus === 'full' && scanTime && (
              <span style={{ marginLeft: 8, opacity: 0.7 }}>{scanTimeAgo(scanTime)}</span>
            )}
          </td>
          <td>
            <Tooltip title={scanStatus === 'full' ? 'Rescan this directory' : 'Scan this directory'}>
              <span>
                <Button
                  size="small"
                  onClick={onRescan}
                  disabled={isScanning}
                  sx={{ minWidth: 0, padding: '2px 8px' }}
                >
                  {isScanning ? <CircularProgress size={14} /> : <FaSync size={12} />}
                </Button>
              </span>
            </Tooltip>
          </td>
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
                <ChildScanStatus row={row} scanStatus={scanStatus} />
              </td>
              <td>
                {row.kind === 'dir' && (
                  <Tooltip title={row.scanned ? 'Rescan this directory' : 'Scan this directory'}>
                    <span>
                      <Button
                        size="small"
                        onClick={() => onScanChild(childUri)}
                        disabled={isChildScanning}
                        sx={{ minWidth: 0, padding: '2px 8px' }}
                      >
                        {isChildScanning ? <CircularProgress size={14} /> : <FaSync size={12} />}
                      </Button>
                    </span>
                  </Tooltip>
                )}
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
        parents: data.map(r => r.path === '.' ? '' : (r.parent ?? '.')),
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

  const { root, children, rows, scan_status, time } = details

  return (
    <div>
      <h1 style={{ marginBottom: '1rem' }}><Breadcrumbs uri={uri} routeType={routeType} /></h1>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <DetailsTable
        root={root}
        children={children}
        uri={uri}
        routeType={routeType}
        onScanChild={handleScanChild}
        scanningPaths={scanningPaths}
        scanStatus={scan_status}
        scanTime={time}
        onRescan={handleRescan}
        isScanning={scanning}
      />
      {rows.length > 0 && <Treemap root={root} rows={rows} />}
    </div>
  )
}
