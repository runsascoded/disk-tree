import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { AppBar, Toolbar, Typography, Button, Box, Tooltip, Popover, List, ListItem, ListItemText, Chip, Alert } from '@mui/material'
import { FaCloud, FaDatabase, FaFolder, FaHistory, FaCog } from 'react-icons/fa'
import { useQuery } from '@tanstack/react-query'
import { fetchAvailableBackends } from '../api'

export function Header() {
  const location = useLocation()
  const path = location.pathname
  const isScansPage = path === '/'
  const isLocalPage = path.startsWith('/file')
  const isS3Page = path.startsWith('/s3')
  const isRecentPage = path === '/recent'

  // Backend popover state
  const [anchorEl, setAnchorEl] = useState<HTMLButtonElement | null>(null)
  const handleBackendClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget)
  }
  const handleBackendClose = () => {
    setAnchorEl(null)
  }
  const backendOpen = Boolean(anchorEl)

  // Fetch backend info
  const { data: backendData } = useQuery({
    queryKey: ['available-backends'],
    queryFn: fetchAvailableBackends,
    staleTime: Infinity, // Backend doesn't change during session
  })

  return (
    <AppBar position="static" color="transparent" elevation={0} sx={{ borderBottom: 1, borderColor: 'divider' }}>
      <Toolbar variant="dense">
        <Typography variant="h6" component="div" sx={{ flexGrow: 0, mr: 4 }}>
          disk-tree
        </Typography>
        <Box sx={{ flexGrow: 1, display: 'flex', gap: 1 }}>
          <Button
            component={Link}
            to="/"
            startIcon={<FaDatabase />}
            variant={isScansPage ? 'contained' : 'text'}
            size="small"
          >
            Scans
          </Button>
          <Button
            component={Link}
            to="/file/"
            startIcon={<FaFolder />}
            variant={isLocalPage ? 'contained' : 'text'}
            size="small"
          >
            Local
          </Button>
          <Button
            component={Link}
            to="/s3/"
            startIcon={<FaCloud />}
            variant={isS3Page ? 'contained' : 'text'}
            size="small"
          >
            S3
          </Button>
          <Button
            component={Link}
            to="/recent"
            startIcon={<FaHistory />}
            variant={isRecentPage ? 'contained' : 'text'}
            size="small"
          >
            Recent
          </Button>
        </Box>

        {/* Backend indicator */}
        {backendData && (
          <>
            <Tooltip title="Storage backend">
              <Button
                size="small"
                onClick={handleBackendClick}
                startIcon={<FaCog />}
                sx={{ textTransform: 'none', opacity: 0.8 }}
              >
                {backendData.current}
              </Button>
            </Tooltip>
            <Popover
              open={backendOpen}
              anchorEl={anchorEl}
              onClose={handleBackendClose}
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
              transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            >
              <Box sx={{ p: 2, minWidth: 300 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>Storage Backends</Typography>
                <List dense disablePadding>
                  {backendData.backends.map(b => (
                    <ListItem key={b.name} disablePadding sx={{ py: 0.5 }}>
                      <ListItemText
                        primary={
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <code style={{ fontWeight: b.current ? 600 : 400 }}>{b.name}</code>
                            {b.current && <Chip label="active" size="small" color="primary" sx={{ height: 18 }} />}
                            {b.supports_updates && <Chip label="updates" size="small" variant="outlined" sx={{ height: 18 }} />}
                          </Box>
                        }
                        secondary={b.description}
                        secondaryTypographyProps={{ variant: 'caption' }}
                      />
                    </ListItem>
                  ))}
                </List>
                <Alert severity="info" sx={{ mt: 2, py: 0 }}>
                  <Typography variant="caption">
                    To switch: <code>DISK_TREE_BACKEND=duckdb disk-tree-server</code>
                  </Typography>
                </Alert>
              </Box>
            </Popover>
          </>
        )}
      </Toolbar>
    </AppBar>
  )
}
