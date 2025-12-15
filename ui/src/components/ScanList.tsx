import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchScans } from '../api'
import type { Scan } from '../api'

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

export function ScanList() {
  const [scans, setScans] = useState<Scan[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchScans()
      .then(setScans)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div>Loading scans...</div>
  if (error) return <div>Error: {error}</div>

  return (
    <div>
      <h1>Scans</h1>
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
