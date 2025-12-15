import { Link, useLocation } from 'react-router-dom'
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material'
import { FaDatabase, FaFolder } from 'react-icons/fa'

export function Header() {
  const location = useLocation()
  const isScansPage = location.pathname === '/'

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
            variant={!isScansPage ? 'contained' : 'text'}
            size="small"
          >
            Browse
          </Button>
        </Box>
      </Toolbar>
    </AppBar>
  )
}
