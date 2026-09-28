import { existsSync, readFileSync } from 'node:fs'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Ports: `devPort` in package.json (vite), `PORT` env overrides (a second dev
// stack, or when another worktree already holds 3263); wrangler pages dev (the
// Functions) is always the next port up — `./dev` derives it the same way.
const PORT = Number(process.env.PORT ?? JSON.parse(readFileSync('package.json', 'utf8')).devPort)
const WRANGLER = `http://localhost:${PORT + 1}`

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

// Deployment as configuration: the store this build serves (`src/stores.ts`
// registry key) and the client's whoami source (`src/auth.ts`: `edge` behind a
// CF Access gate, `app` for the app-session model, `public` for an open
// deploy) come from the same file wrangler reads — `STORE` / `AUTH_MODE` under
// `[vars]` in wrangler.toml — so a deployment branch declares itself in one
// place. `VITE_STORE` / `VITE_AUTH_MODE` in the environment still override (a
// CI build of another store, e.g. deploy-r2.yml). Neither set → the registry's
// first store, `edge`.
function wranglerVars(): Record<string, string> {
  if (!existsSync('wrangler.toml')) return {}
  const vars: Record<string, string> = {}
  let inVars = false
  for (const raw of readFileSync('wrangler.toml', 'utf8').split('\n')) {
    const line = raw.replace(/#.*$/, '').trim()
    if (line.startsWith('[')) { inVars = line === '[vars]'; continue }
    const m = inVars ? /^([A-Z_][A-Z0-9_]*)\s*=\s*"([^"]*)"$/.exec(line) : null
    if (m) vars[m[1]] = m[2]
  }
  return vars
}
const VARS = wranglerVars()
const STORE = process.env.VITE_STORE ?? VARS.STORE ?? ''
const AUTH_MODE = process.env.VITE_AUTH_MODE ?? VARS.AUTH_MODE ?? 'edge'

export default defineConfig({
  define: {
    'import.meta.env.VITE_STORE': JSON.stringify(STORE),
    'import.meta.env.VITE_AUTH_MODE': JSON.stringify(AUTH_MODE),
  },
  plugins: [react(), devSeriesIndex],
  server: {
    port: PORT,
    host: true,
    allowedHosts: true,
    // dev only: forward the Pages Functions (snapshot data + scan-browser API)
    // to the local `wrangler pages dev` (the next port up, with GCS HMAC creds
    // in .dev.vars). Both /data and /v1/files now read live from the bucket.
    proxy: {
      '/data': WRANGLER,
      '/v1/files': WRANGLER,
      // Mark & sweep console: plans/marks/sweep/whoami Functions (D1 + Batch).
      '/api': WRANGLER,
      // Sign-in Functions (`/auth/google*`, `/auth/email/*`). Keep the
      // browser's Host header (Vite's string-target default rewrites it to the
      // wrangler port): the OIDC callback + emailed links derive their origin
      // from it, so they resolve to `http://localhost:<PORT>/…` — the URI that
      // must be registered on the Google client for local sign-in to work.
      '/auth': { target: WRANGLER, changeOrigin: false },
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
