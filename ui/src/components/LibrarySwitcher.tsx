import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button, Menu, MenuItem, Divider, ListItemText, TextField, Tooltip, Box, Typography } from '@mui/material'
import { FaBook, FaFolderOpen } from 'react-icons/fa'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchLibrary, openLibrary, pickLibraryDir, type LibraryInfo } from '../api'

const ellipsis = { maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis' as const }

/** Header control: shows the open scans library and switches to another root
 * (recents, or Open folder… → native picker in the app, path input in a browser). */
export function LibrarySwitcher() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [anchor, setAnchor] = useState<null | HTMLElement>(null)
  const [pathInput, setPathInput] = useState('')
  const [showInput, setShowInput] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const { data } = useQuery({ queryKey: ['library'], queryFn: fetchLibrary, staleTime: 30_000 })

  const close = () => { setAnchor(null); setShowInput(false); setPathInput(''); setError(null) }

  const open = useMutation({
    mutationFn: openLibrary,
    onSuccess: (info: LibraryInfo) => {
      qc.setQueryData(['library'], info)
      qc.invalidateQueries()  // a new root replaces the entire dataset
      close()
      navigate('/')
    },
    onError: (e: Error) => setError(e.message),
  })

  const openFolder = async () => {
    setError(null)
    try {
      const picked = await pickLibraryDir()
      if (picked === undefined) { setShowInput(true); return }  // browser: no native picker
      if (picked) open.mutate(picked)
    } catch (e) { setError((e as Error).message) }
  }

  const cur = data?.current
  const others = (data?.recents ?? []).filter(r => r.path !== cur?.path)

  return (
    <>
      <Tooltip title="Scans library — open another root">
        <Button
          size="small"
          startIcon={<FaBook />}
          onClick={e => setAnchor(e.currentTarget)}
          sx={{ textTransform: 'none', opacity: 0.85, mr: 1, maxWidth: 200, minWidth: 0 }}
        >
          <Box component="span" sx={{ ...ellipsis, whiteSpace: 'nowrap' }}>{cur?.label ?? 'library'}</Box>
        </Button>
      </Tooltip>
      <Menu anchorEl={anchor} open={!!anchor} onClose={close}>
        {cur && (
          <MenuItem disabled sx={{ opacity: '1 !important' }}>
            <ListItemText
              primary={cur.label}
              secondary={`${cur.scans} scan${cur.scans === 1 ? '' : 's'} · ${cur.path}`}
              secondaryTypographyProps={{ variant: 'caption', sx: ellipsis }}
            />
          </MenuItem>
        )}
        {others.length > 0 && <Divider />}
        {others.map(r => (
          <MenuItem key={r.path} onClick={() => open.mutate(r.path)}>
            <ListItemText primary={r.label} secondary={r.path} secondaryTypographyProps={{ variant: 'caption', sx: ellipsis }} />
          </MenuItem>
        ))}
        <Divider />
        <MenuItem onClick={openFolder}>
          <FaFolderOpen style={{ marginRight: 8 }} /> Open folder…
        </MenuItem>
        {showInput && (
          <Box sx={{ px: 2, py: 1, width: 340 }}>
            <TextField
              autoFocus fullWidth size="small" placeholder="/path/to/library"
              value={pathInput}
              onChange={e => setPathInput(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter' && pathInput.trim()) open.mutate(pathInput.trim()) }}
            />
          </Box>
        )}
        {error && (
          <Box sx={{ px: 2, py: 0.5, maxWidth: 340 }}>
            <Typography variant="caption" color="error">{error}</Typography>
          </Box>
        )}
      </Menu>
    </>
  )
}
