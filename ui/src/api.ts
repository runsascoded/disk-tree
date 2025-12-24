export type Scan = {
  id: number
  path: string
  time: string
  blob: string
  error_count?: number | null
  error_paths?: string | null  // JSON array
  size?: number | null
  n_children?: number | null
  n_desc?: number | null
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
  error_count?: number | null
  error_paths?: string | null  // JSON array
}

export async function fetchScans(): Promise<Scan[]> {
  const res = await fetch('/api/scans')
  if (!res.ok) throw new Error('Failed to fetch scans')
  return res.json()
}

export async function fetchScanDetails(uri: string, scanId?: number): Promise<ScanDetails> {
  const params = new URLSearchParams({ uri })
  if (scanId !== undefined) {
    params.set('scan_id', String(scanId))
  }
  const res = await fetch(`/api/scan?${params}`)
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

export type DeleteResult = {
  success: boolean
  path: string
  deleted_size: number
  deleted_n_desc: number
}

export async function deletePath(path: string): Promise<DeleteResult> {
  const res = await fetch('/api/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to delete')
  }
  return res.json()
}

export type ScanProgress = {
  id: number
  path: string
  pid: number
  started: string
  items_found: number
  items_per_sec: number | null
  error_count: number
  status: 'running' | 'completed' | 'failed'
}

export async function fetchScansProgress(): Promise<ScanProgress[]> {
  const res = await fetch('/api/scans/progress')
  if (!res.ok) throw new Error('Failed to fetch scans progress')
  return res.json()
}

export type S3Bucket = {
  name: string
  created: string
  last_scanned: string | null
  size?: number | null
  n_children?: number | null
  n_desc?: number | null
}

export async function fetchS3Buckets(): Promise<S3Bucket[]> {
  const res = await fetch('/api/s3/buckets')
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to fetch S3 buckets')
  }
  return res.json()
}

// Scan history for compare page
export type ScanHistoryItem = {
  id: number
  path: string
  time: string
  size?: number | null
  n_children?: number | null
  n_desc?: number | null
  scan_path?: string  // The actual scanned path (may be ancestor of requested path)
}

export async function fetchScanHistory(uri: string): Promise<ScanHistoryItem[]> {
  const res = await fetch(`/api/scans/history?uri=${encodeURIComponent(uri)}`)
  if (!res.ok) throw new Error('Failed to fetch scan history')
  return res.json()
}

// Compare scans
export type CompareRow = {
  path: string
  size: number | null
  mtime: number | null
  kind: 'file' | 'dir'
  parent: string | null
  uri: string
  n_desc: number | null
  n_children: number | null
  status: 'added' | 'removed' | 'changed' | 'unchanged'
  size_delta: number
  size_old?: number
  n_desc_delta?: number
  n_desc_old?: number
}

export type CompareResult = {
  uri: string
  scan1: { id: number; time: string; size: number | null; n_desc: number | null; scan_path?: string }
  scan2: { id: number; time: string; size: number | null; n_desc: number | null; scan_path?: string }
  rows: CompareRow[]
  summary: {
    added: number
    removed: number
    changed: number
    unchanged: number
    total_delta: number
  }
}

export async function compareScans(
  uri: string,
  scan1: number,
  scan2: number,
): Promise<CompareResult> {
  const params = new URLSearchParams({ uri, scan1: String(scan1), scan2: String(scan2) })
  const res = await fetch(`/api/compare?${params}`)
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to compare scans')
  }
  return res.json()
}
