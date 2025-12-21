import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Box,
  Button,
  CircularProgress,
  LinearProgress,
  MenuItem,
  Paper,
  Select,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import { FaPlay } from 'react-icons/fa'
import { fetchScans, startScan } from '../api'
import type { Scan, ScanJob, ScanProgress } from '../api'
import { useScanProgress } from '../hooks/useScanProgress'

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

function pathToRoute(path: string): string {
  if (path.startsWith('s3://')) {
    return `/s3/${path.slice(5)}`
  }
  return `/file${path}`
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}

function formatSize(bytes: number | null | undefined): string {
  if (bytes == null) return '-'
  if (bytes >= 1024 ** 4) return `${(bytes / 1024 ** 4).toFixed(1)} TB`
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${bytes} B`
}

function LiveScanProgress({ progress }: { progress: ScanProgress[] }) {
  const activeScans = progress.filter(p => p.status === 'running')
  if (activeScans.length === 0) return null

  return (
    <Paper sx={{ p: 2, mb: 2 }}>
      <Typography variant="subtitle2" sx={{ mb: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
        <CircularProgress size={16} />
        Scans in Progress (Live)
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        {activeScans.map(scan => (
          <Box key={scan.id} sx={{ borderLeft: '3px solid', borderColor: 'primary.main', pl: 2 }}>
            <Typography variant="body2" sx={{ fontFamily: 'monospace', mb: 0.5 }}>
              {scan.path}
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 0.5 }}>
              <LinearProgress
                variant="indeterminate"
                sx={{ flexGrow: 1, height: 6, borderRadius: 1 }}
              />
            </Box>
            <Box sx={{ display: 'flex', gap: 3, color: 'text.secondary', fontSize: '0.85rem' }}>
              <span><strong>{formatNumber(scan.items_found)}</strong> items</span>
              {scan.items_per_sec && (
                <span>{formatNumber(Math.round(scan.items_per_sec))} items/sec</span>
              )}
              {scan.error_count > 0 && (
                <span style={{ color: '#ed6c02' }}>{scan.error_count} errors</span>
              )}
              <span>{timeAgo(scan.started)}</span>
            </Box>
          </Box>
        ))}
      </Box>
    </Paper>
  )
}


function NewScanForm({ onStarted }: { onStarted: (job: ScanJob) => void }) {
  const [path, setPath] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!path.trim()) return

    setLoading(true)
    setError(null)
    try {
      const job = await startScan(path.trim())
      onStarted(job)
      setPath('')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start scan')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Paper sx={{ p: 2, mb: 2 }}>
      <Typography variant="subtitle2" sx={{ mb: 1 }}>Start New Scan</Typography>
      <form onSubmit={handleSubmit}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
          <Tooltip title="Enter a local path (e.g., /Users/ryan) or S3 URI (e.g., s3://bucket/prefix)">
            <TextField
              size="small"
              placeholder="/path/to/scan or s3://bucket"
              value={path}
              onChange={e => setPath(e.target.value)}
              error={!!error}
              helperText={error}
              sx={{ flexGrow: 1 }}
            />
          </Tooltip>
          <Button
            type="submit"
            variant="contained"
            disabled={loading || !path.trim()}
            startIcon={loading ? <CircularProgress size={16} /> : <FaPlay />}
          >
            Scan
          </Button>
        </Box>
      </form>
    </Paper>
  )
}

export function ScanList() {
  const [scans, setScans] = useState<Scan[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(50)

  // Live progress from SSE
  const scanProgress = useScanProgress()

  // Pagination
  const totalPages = Math.ceil(scans.length / pageSize)
  const paginatedScans = useMemo(() => {
    const start = page * pageSize
    return scans.slice(start, start + pageSize)
  }, [scans, page, pageSize])

  const loadData = async () => {
    try {
      const scansData = await fetchScans()
      setScans(scansData)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  // Refresh scans list when a live scan completes (SSE shows empty but we had scans)
  const [prevProgressCount, setPrevProgressCount] = useState(0)
  useEffect(() => {
    if (prevProgressCount > 0 && scanProgress.length === 0) {
      // A scan just finished - refresh the list
      fetchScans().then(setScans)
    }
    setPrevProgressCount(scanProgress.length)
  }, [scanProgress.length, prevProgressCount])

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleNewScan = (_job: ScanJob) => {
    // Scan progress is tracked via SSE, no need to track here
  }

  if (loading) return <div>Loading scans...</div>
  if (error) return <div>Error: {error}</div>

  return (
    <div>
      <h1>Scans</h1>
      <NewScanForm onStarted={handleNewScan} />
      <LiveScanProgress progress={scanProgress} />
      <Tooltip title="Previously completed scans. Click a path to browse its contents.">
        <Typography variant="subtitle2" sx={{ mb: 1 }}>Completed Scans</Typography>
      </Tooltip>
      <table>
        <thead>
          <tr>
            <th style={{ textAlign: 'left' }}>Path</th>
            <th style={{ textAlign: 'right' }}>Size</th>
            <th style={{ textAlign: 'right' }}>Items</th>
            <th style={{ textAlign: 'right' }}>Children</th>
            <th style={{ textAlign: 'left' }}>Scanned</th>
          </tr>
        </thead>
        <tbody>
          {paginatedScans.map(scan => (
            <tr key={scan.id}>
              <td style={{ textAlign: 'left' }}>
                <Link to={pathToRoute(scan.path)}>
                  <code>{scan.path}</code>
                </Link>
              </td>
              <td style={{ textAlign: 'right' }}>{formatSize(scan.size)}</td>
              <td style={{ textAlign: 'right' }}>{scan.n_desc != null ? formatNumber(scan.n_desc) : '-'}</td>
              <td style={{ textAlign: 'right' }}>{scan.n_children != null ? formatNumber(scan.n_children) : '-'}</td>
              <td style={{ textAlign: 'left' }}>{timeAgo(scan.time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {scans.length > pageSize && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1, fontSize: '0.85rem' }}>
          <span style={{ opacity: 0.7 }}>
            {page * pageSize + 1}-{Math.min((page + 1) * pageSize, scans.length)} of {scans.length}
          </span>
          <Box sx={{ display: 'flex', gap: 0.5 }}>
            <Button size="small" disabled={page === 0} onClick={() => setPage(0)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              ⏮
            </Button>
            <Button size="small" disabled={page === 0} onClick={() => setPage(p => p - 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              ◀
            </Button>
            <Button size="small" disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              ▶
            </Button>
            <Button size="small" disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)} sx={{ minWidth: 0, padding: '2px 6px' }}>
              ⏭
            </Button>
          </Box>
          <Select
            size="small"
            value={pageSize}
            onChange={e => { setPageSize(Number(e.target.value)); setPage(0) }}
            sx={{ fontSize: '0.85rem', height: '28px' }}
          >
            <MenuItem value={25}>25</MenuItem>
            <MenuItem value={50}>50</MenuItem>
            <MenuItem value={100}>100</MenuItem>
            <MenuItem value={250}>250</MenuItem>
          </Select>
        </Box>
      )}
    </div>
  )
}
