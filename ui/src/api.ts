export type Scan = {
  id: number
  path: string
  time: string
  blob: string
}

export type Row = {
  path: string
  size: number | null
  mtime: number | null
  kind: 'file' | 'dir'
  parent: string | null
  uri: string
  n_desc: number | null
  n_children: number | null
  scanned?: boolean | 'partial'
  scan_time?: string
}

export type ScanDetails = {
  root: Row
  children: Row[]
  rows: Row[]
  time: string | null
  scan_path: string | null
  scan_status: 'full' | 'partial' | 'none'
}

export async function fetchScans(): Promise<Scan[]> {
  const res = await fetch('/api/scans')
  if (!res.ok) throw new Error('Failed to fetch scans')
  return res.json()
}

export async function fetchScanDetails(uri: string): Promise<ScanDetails> {
  const res = await fetch(`/api/scan?uri=${encodeURIComponent(uri)}`)
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to fetch scan details')
  }
  return res.json()
}

export type ScanJob = {
  job_id: string
  path: string
  status: 'pending' | 'running' | 'completed' | 'failed'
  started: string
  finished?: string
  output?: string
  error?: string
}

export async function startScan(path: string): Promise<ScanJob> {
  const res = await fetch('/api/scan/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to start scan')
  }
  return res.json()
}

export async function fetchScanStatus(jobId: string): Promise<ScanJob> {
  const res = await fetch(`/api/scan/status/${jobId}`)
  if (!res.ok) throw new Error('Failed to fetch scan status')
  return res.json()
}

export async function fetchRunningScans(): Promise<ScanJob[]> {
  const res = await fetch('/api/scans/running')
  if (!res.ok) throw new Error('Failed to fetch running scans')
  return res.json()
}
