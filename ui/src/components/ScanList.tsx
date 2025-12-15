import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Box,
  Button,
  CircularProgress,
  Paper,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import { FaPlay, FaSync } from 'react-icons/fa'
import { fetchScans, fetchRunningScans, startScan } from '../api'
import type { Scan, ScanJob } from '../api'

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

function RunningScans({ jobs, onRefresh }: { jobs: ScanJob[]; onRefresh: () => void }) {
  const activeJobs = jobs.filter(j => j.status === 'running' || j.status === 'pending')
  if (activeJobs.length === 0) return null

  return (
    <Paper sx={{ p: 2, mb: 2 }}>
      <Typography variant="subtitle2" sx={{ mb: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <CircularProgress size={16} />
        Scans in Progress
      </Typography>
      <table style={{ width: '100%' }}>
        <tbody>
          {activeJobs.map(job => (
            <tr key={job.job_id}>
              <td><code>{job.path}</code></td>
              <td>{job.status}</td>
              <td>{timeAgo(job.started)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <Button size="small" startIcon={<FaSync />} onClick={onRefresh} sx={{ mt: 1 }}>
        Refresh
      </Button>
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
  const [runningJobs, setRunningJobs] = useState<ScanJob[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadData = async () => {
    try {
      const [scansData, jobsData] = await Promise.all([
        fetchScans(),
        fetchRunningScans(),
      ])
      setScans(scansData)
      setRunningJobs(jobsData)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [])

  // Poll for running scans
  useEffect(() => {
    const hasActive = runningJobs.some(j => j.status === 'running' || j.status === 'pending')
    if (!hasActive) return

    const interval = setInterval(async () => {
      const jobs = await fetchRunningScans()
      setRunningJobs(jobs)
      // If a job completed, refresh the scans list
      const wasActive = runningJobs.some(j => j.status === 'running' || j.status === 'pending')
      const nowActive = jobs.some(j => j.status === 'running' || j.status === 'pending')
      if (wasActive && !nowActive) {
        const newScans = await fetchScans()
        setScans(newScans)
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [runningJobs])

  const handleNewScan = (job: ScanJob) => {
    setRunningJobs(prev => [...prev, job])
  }

  if (loading) return <div>Loading scans...</div>
  if (error) return <div>Error: {error}</div>

  return (
    <div>
      <h1>Scans</h1>
      <NewScanForm onStarted={handleNewScan} />
      <RunningScans jobs={runningJobs} onRefresh={loadData} />
      <Tooltip title="Previously completed scans. Click a path to browse its contents.">
        <Typography variant="subtitle2" sx={{ mb: 1 }}>Completed Scans</Typography>
      </Tooltip>
      <table>
        <thead>
          <tr>
            <th>Path</th>
            <th>Scanned</th>
          </tr>
        </thead>
        <tbody>
          {scans.map(scan => (
            <tr key={scan.id}>
              <td>
                <Link to={pathToRoute(scan.path)}>
                  <code>{scan.path}</code>
                </Link>
              </td>
              <td>{timeAgo(scan.time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
