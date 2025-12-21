import { Link, useLocation } from 'react-router-dom'
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material'
import { FaCloud, FaDatabase, FaFolder, FaHistory } from 'react-icons/fa'

export function Header() {
  const location = useLocation()
  const path = location.pathname
  const isScansPage = path === '/'
  const isLocalPage = path.startsWith('/file')
  const isS3Page = path.startsWith('/s3')
  const isRecentPage = path === '/recent'

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
      </Toolbar>
    </AppBar>
  )
}
