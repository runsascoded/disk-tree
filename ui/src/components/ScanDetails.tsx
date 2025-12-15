import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { FaFileAlt, FaFolder } from 'react-icons/fa'
import Plot from 'react-plotly.js'
import { fetchScanDetails } from '../api'
import type { Row, ScanDetails as ScanDetailsType } from '../api'

function sizeStr(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GB`
}

function timeAgo(timestamp: number): string {
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

function DetailsTable({ root, children, uri, routeType }: {
  root: Row
  children: Row[]
  uri: string
  routeType: RouteType
}) {
  const prefix = routeType === 's3' ? `/s3/${uri.replace('s3://', '')}` : `/file${uri}`
  return (
    <table>
      <thead>
        <tr>
          <th></th>
          <th>Path</th>
          <th>Size</th>
          <th>Modified</th>
          <th>Children</th>
          <th>Desc.</th>
        </tr>
      </thead>
      <tbody>
        <tr className="root">
          <td>{root.kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
          <td><code>.</code></td>
          <td>{sizeStr(root.size)}</td>
          <td>{timeAgo(root.mtime)}</td>
          <td>{root.n_children?.toLocaleString()}</td>
          <td>{root.n_desc > 1 ? root.n_desc.toLocaleString() : null}</td>
        </tr>
        {children.map(row => (
          <tr key={row.path}>
            <td>{row.kind === 'file' ? <FaFileAlt /> : <FaFolder />}</td>
            <td>
              <Link to={`${prefix}/${row.path}`}>
                <code>{row.path}</code>
              </Link>
            </td>
            <td>{sizeStr(row.size)}</td>
            <td>{timeAgo(row.mtime)}</td>
            <td>{row.n_children ? row.n_children.toLocaleString() : null}</td>
            <td>{row.n_desc > 1 ? row.n_desc.toLocaleString() : null}</td>
          </tr>
        ))}
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

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchScanDetails(uri)
      .then(setDetails)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [uri])

  if (loading) return <div>Loading...</div>
  if (error) return <div>Error: {error}</div>
  if (!details) return <div>No data</div>

  const { root, children, rows } = details

  return (
    <div>
      <h1><Breadcrumbs uri={uri} routeType={routeType} /></h1>
      <DetailsTable root={root} children={children} uri={uri} routeType={routeType} />
      <Treemap root={root} rows={rows} />
    </div>
  )
}
