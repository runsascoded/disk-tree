import { Link } from 'react-router-dom'
import { Box, Button, Typography } from '@mui/material'
import { FaCloud, FaColumns, FaFolder, FaHistory, FaList, FaTrash } from 'react-icons/fa'
import { useRecentPaths } from '../hooks/useRecentPaths'
import type { RecentPath, ViewType } from '../hooks/useRecentPaths'
import { DataTable } from './DataTable'
import type { Column } from './DataTable'

function uriToRoute(uri: string, viewType: ViewType): string {
  const isS3 = uri.startsWith('s3://')
  if (viewType === 'compare') {
    return isS3 ? `/compare/s3/${uri.slice(5)}` : `/compare/file${uri}`
  }
  return isS3 ? `/s3/${uri.slice(5)}` : `/file${uri}`
}

const recentColumns: Column<RecentPath>[] = [
  {
    key: 'source',
    label: '',
    type: 'icon',
    render: item => item.uri.startsWith('s3://') ? (
      <FaCloud style={{ color: '#ff9800', verticalAlign: 'middle' }} title="S3" />
    ) : (
      <FaFolder style={{ color: '#4caf50', verticalAlign: 'middle' }} title="Local" />
    ),
  },
  {
    key: 'viewType',
    label: '',
    type: 'icon',
    render: item => item.viewType === 'compare' ? (
      <FaColumns style={{ color: '#58a6ff', verticalAlign: 'middle' }} title="Compare view" />
    ) : (
      <FaList style={{ color: '#8b949e', verticalAlign: 'middle' }} title="Tree view" />
    ),
  },
  {
    key: 'uri',
    label: 'Path',
    render: item => (
      <Link to={uriToRoute(item.uri, item.viewType)}>
        <code>{item.uri}</code>
      </Link>
    ),
  },
  {
    key: 'visitedAt',
    label: 'Visited',
    type: 'time',
  },
]

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
        <DataTable<RecentPath>
          columns={recentColumns}
          data={recent}
          rowKey={item => `${item.uri}:${item.viewType}`}
        />
      )}
    </div>
  )
}
