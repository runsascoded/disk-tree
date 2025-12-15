export type Scan = {
  id: number
  path: string
  time: string
  blob: string
}

export type Row = {
  path: string
  size: number
  mtime: number
  kind: 'file' | 'dir'
  parent: string | null
  uri: string
  n_desc: number
  n_children: number
}

export type ScanDetails = {
  root: Row
  children: Row[]
  rows: Row[]
  time: string
  scan_path: string
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
