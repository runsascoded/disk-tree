import { Link } from 'react-router-dom'
import { Box, Button, Typography } from '@mui/material'
import { FaCloud, FaFolder, FaHistory, FaTrash } from 'react-icons/fa'
import { useRecentPaths } from '../hooks/useRecentPaths'

function timeAgo(timestamp: number): string {
  const seconds = Math.floor((Date.now() - timestamp) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

function uriToRoute(uri: string): string {
  if (uri.startsWith('s3://')) {
    return `/s3/${uri.slice(5)}`
  }
  return `/file${uri}`
}

export function RecentList() {
  const { recent, clearRecent } = useRecentPaths()

  return (
    <div>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <h1 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', margin: 0 }}>
          <FaHistory /> Recent
        </h1>
        {recent.length > 0 && (
          <Button
            size="small"
            onClick={clearRecent}
            startIcon={<FaTrash size={12} />}
            sx={{ opacity: 0.7 }}
          >
            Clear
          </Button>
        )}
      </Box>
      {recent.length === 0 ? (
        <Typography color="text.secondary">
          No recently visited paths. Browse some directories to see them here.
        </Typography>
      ) : (
        <table>
          <thead>
            <tr>
              <th></th>
              <th style={{ textAlign: 'left' }}>Path</th>
              <th style={{ textAlign: 'left' }}>Visited</th>
            </tr>
          </thead>
          <tbody>
            {recent.map(item => {
              const isS3 = item.uri.startsWith('s3://')
              return (
                <tr key={item.uri}>
                  <td>
                    {isS3 ? (
                      <FaCloud style={{ color: '#ff9800' }} />
                    ) : (
                      <FaFolder style={{ color: '#4caf50' }} />
                    )}
                  </td>
                  <td>
                    <Link to={uriToRoute(item.uri)}>
                      <code>{item.uri}</code>
                    </Link>
                  </td>
                  <td style={{ opacity: 0.7 }}>{timeAgo(item.visitedAt)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}
