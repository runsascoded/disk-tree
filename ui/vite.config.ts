import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Tailnet access: `VITE_ALLOWED_HOSTS=.rbw.sh` (set by dotfiles) lets e.g.
// http://m3.rbw.sh:7788 through Vite's host check; `host: true` binds all
// interfaces instead of loopback only. The `/api` proxy is server-side, so
// the Flask API can stay on localhost:5001.
const allowedHosts = process.env.VITE_ALLOWED_HOSTS?.split(',') ?? []

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],

  server: {
    // sha1('disk-tree') % 6000 + 4000
    port: 7788,
    strictPort: true,  // Exit with error if port is already in use
    host: true,
    allowedHosts,
    proxy: {
      '/api': {
        target: 'http://localhost:5001',
        changeOrigin: true,
      },
    },
  },

  optimizeDeps: {
    include: ['plotly.js/dist/plotly'],
  },
})
