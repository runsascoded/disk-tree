import { existsSync, readFileSync } from 'node:fs'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'


// dev only: serve a locally-generated `tmp/series.json` (from `dt-cloud series
// -r http://localhost:3254/data -o tmp/series.json`) at /data/series.json, so
// the scoped size chart can be previewed before the index is published to the
// bucket. Registered in the plugin body so it pre-empts the /data proxy; a no-op
// (falls through to the bucket) when the file is absent.
const devSeriesIndex = {
  name: 'dev-series-index',
  configureServer(server: { middlewares: { use: (path: string, fn: (req: unknown, res: { setHeader: (k: string, v: string) => void; end: (b: Buffer) => void }, next: () => void) => void) => void } }) {
    server.middlewares.use('/data/series.json', (_req, res, next) => {
      const p = 'tmp/series.json'
      if (existsSync(p)) { res.setHeader('content-type', 'application/json'); res.end(readFileSync(p)) }
      else next()
    })
  },
}

// Deployment default for the client's whoami source (src/auth.ts): this host
// sits behind a CF Access edge gate, so identity comes from the edge unless the
// environment says otherwise (`VITE_AUTH_MODE=app` for the app-session model).
const AUTH_MODE = process.env.VITE_AUTH_MODE ?? 'edge'

export default defineConfig({
  define: { 'import.meta.env.VITE_AUTH_MODE': JSON.stringify(AUTH_MODE) },
  plugins: [react(), devSeriesIndex],
  server: {
    port: 3253,
    host: true,
    allowedHosts: true,
    // dev only: forward the Pages Functions (snapshot data + scan-browser API)
    // to the local `wrangler pages dev` (run it on :3264 with GCS HMAC creds in
    // .dev.vars). Both /data and /v1/files now read live from the bucket.
    proxy: {
      '/data': 'http://localhost:3264',
      '/v1/files': 'http://localhost:3264',
      // Mark & sweep console: plans/marks/sweep/whoami Functions (D1 + Batch).
      '/api': 'http://localhost:3264',
    },
  },
  // The workspace-linked `@rdub/file-tree` calls `useLocation` etc. — force a
  // single instance of these so its hooks share the app's Router/React context
  // (else the rollup build bundles a 2nd copy → "useLocation outside <Router>").
  resolve: {
    dedupe: ['react', 'react-dom', 'react-router-dom'],
  },
  optimizeDeps: {
    exclude: ['@disk-tree/react'],
  },
})
