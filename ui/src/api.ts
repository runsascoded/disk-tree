// Default max rows for treemap performance (keep in sync with server DEFAULT_MAX_ROWS)
export const DEFAULT_MAX_ROWS = 2000

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
  expand_preview?: string  // Preview of auto-expand path (e.g., "0" for vms that expands to vms/0)
}

// Row with extra fields for collapsed (auto-expanded) parents
export type CollapsedRow = Row & {
  original_path: string  // Path relative to the requested URI
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
  collapsed_rows?: CollapsedRow[] | null  // Auto-expanded single-child directories
}

export async function fetchScans(): Promise<Scan[]> {
  const res = await fetch('/api/scans')
  if (!res.ok) throw new Error('Failed to fetch scans')
  return res.json()
}

export async function fetchScanDetails(uri: string, scanId?: number, depth: number = 2, maxRows: number = DEFAULT_MAX_ROWS): Promise<ScanDetails> {
  const params = new URLSearchParams({ uri, depth: String(depth), max_rows: String(maxRows) })
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

export async function revealPath(path: string): Promise<void> {
  const res = await fetch('/api/reveal', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  })
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to reveal')
  }
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

// File preview
export type FilePreview = {
  path: string
  size: number
  truncated: boolean
  hex_truncated: boolean
  preview_bytes: number
  is_text: boolean
  content: string | null
  hex: string | null
  extension: string
}

export async function fetchFilePreview(path: string, maxSize?: number): Promise<FilePreview> {
  const params = new URLSearchParams({ path })
  if (maxSize !== undefined) {
    params.set('max_size', String(maxSize))
  }
  const res = await fetch(`/api/file/preview?${params}`)
  if (!res.ok) {
    const err = await res.json()
    throw new Error(err.error || 'Failed to fetch file preview')
  }
  return res.json()
}

// Storage backend info
export type BackendInfo = {
  name: string
  supports_updates: boolean
  stats?: {
    num_scans?: number
    total_rows?: number
    total_size_tracked?: number
    db_file_size?: number
  }
}

export type AvailableBackend = {
  name: string
  description: string
  supports_updates: boolean
  current: boolean
}

export type AvailableBackendsResponse = {
  backends: AvailableBackend[]
  current: string
  switch_instructions: string
}

export async function fetchBackendInfo(): Promise<BackendInfo> {
  const res = await fetch('/api/backend')
  if (!res.ok) throw new Error('Failed to fetch backend info')
  return res.json()
}

export async function fetchAvailableBackends(): Promise<AvailableBackendsResponse> {
  const res = await fetch('/api/backend/available')
  if (!res.ok) throw new Error('Failed to fetch available backends')
  return res.json()
}
