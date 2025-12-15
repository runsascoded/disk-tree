import { createTheme } from '@mui/material/styles'

export const theme = createTheme({
  palette: {
    mode: window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light',
  },
  typography: {
    fontFamily: 'system-ui, -apple-system, sans-serif',
  },
  components: {
    MuiAppBar: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
        },
      },
    },
  },
})
