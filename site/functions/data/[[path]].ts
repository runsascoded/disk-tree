// CF Pages Function: serve the treemap's snapshot data live from the bucket.
//
// The app fetches /data/scans.json, /data/<date>/{age,meta}.json, and
// /data/rules.json. These used to be static assets baked into every deploy —
// so the dashboard went stale whenever the daily job's `wrangler pages deploy`
// step failed (it had been broken since ~07-30). Now they're read live from
// gs://<bucket>/snapshots/ (the canonical store the snapshot job already
// writes), so new snapshots surface with no site redeploy.
//
// Same model as the /v1/files browser proxy: a read-only GCS HMAC key over the
// S3-compatible XML API, same-origin behind CF Access (no second sign-in, no
// CORS). CF Pages serves static assets before Functions, so this only works
// because public/data/ is no longer shipped (see the build).
import { S3Store } from '@rdub/file-tree/stores/s3'
import { type Env, requireViewer } from '../_lib/auth.js'
import { storeCreds, storeReady, storeTarget } from '../_lib/index.js'

// Scan ids are `YYYY-MM-DD`, optionally sub-daily as `YYYY-MM-DDTHHMM` (no
// colon: it keeps the id safe as an object-key path segment). GCS publishes one
// scan a day so its ids stay date-only; CoreWeave runs ad hoc, several a day.
const DATE_RE = /^\d{4}-\d{2}-\d{2}(?:T\d{4})?$/
// `private`: these responses are now auth-gated — browser caching only.
const CACHE = 'private, max-age=300' // daily cadence — ≤5min staleness is fine

export const onRequest = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  if (!storeReady(ctx.env)) {
    return new Response('data proxy not configured (missing index store creds)', { status: 503 })
  }
  // Every payload is members-only: the edge (CF Access) session is the identity
  // (a public deploy grants the base viewer scope via PUBLIC_READ).
  const rel = new URL(ctx.request.url).pathname.replace(/^\/data\//, '')
  const gated = await requireViewer(ctx)
  if (gated instanceof Response) return gated
  // The store seam (`_lib/index.ts`): GCS by default, any S3-compatible
  // store (R2) once `STORE_*` is set — same data, same keys.
  const store = S3Store({
    ...storeTarget(ctx.env),
    prefixes: ['snapshots/'], // allow-list: only the published snapshots
    ...storeCreds(ctx.env),
  })

  try {
    // `<store>/scans.json` → the date dirs under snapshots/<store>/, newest-first.
    // The default (GCS) store is the bare `snapshots/`; additional stores live in
    // a named subdir (`snapshots/cw/`), which the DATE_RE filter keeps out of the
    // GCS listing. Per-store payload paths need no special case: the generic
    // `/data/<rel>` → `snapshots/<rel>` mapping below already resolves them.
    const scansM = /^(?:([a-z0-9-]+)\/)?scans\.json$/.exec(rel)
    if (scansM) {
      const prefix = scansM[1] ? `snapshots/${scansM[1]}/` : 'snapshots/'
      const dates: string[] = []
      let cursor: string | undefined
      do {
        const page = await store.list(prefix, { cursor })
        for (const e of page.entries) {
          const d = e.key.slice(prefix.length).replace(/\/$/, '')
          if (e.isDir && DATE_RE.test(d)) dates.push(d)
        }
        cursor = page.cursor
      } while (cursor)
      // Only scans the site can serve: every view reads the path index, so a
      // snapshot with no index (pre-2026-08-26, until re-aggregated) is left
      // out of the picker rather than offered and failing on every drill.
      if (ctx.env.DB) {
        const rows = await ctx.env.DB.prepare("SELECT DISTINCT date FROM index_schema WHERE variant = 'path'").all<{ date: string }>()
        const indexed = new Set(rows.results.map(r => r.date))
        for (let i = dates.length - 1; i >= 0; i--) if (!indexed.has(dates[i])) dates.splice(i, 1)
      }
      dates.sort().reverse()
      return new Response(JSON.stringify(dates), {
        headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': CACHE },
      })
    }
    // rules.json → snapshots/rules.json ; else /data/<date>/<file> → snapshots/<date>/<file>
    const key = rel === 'rules.json' ? 'snapshots/rules.json' : `snapshots/${rel}`
    const { bytes, contentType } = await store.get(key)
    // A date-only scan id can't say *when* in that day the scan ran, and the
    // daily GCS job publishes ids without a time — but the object itself knows.
    // Splice its lastModified into meta.json so the UI can show the real
    // publish time; sizes move enough over 24h that "which 8/17?" matters.
    if (key.endsWith('/meta.json')) {
      const dir = key.slice(0, key.lastIndexOf('/') + 1)
      const published = (await store.list(dir)).entries.find(e => e.key === key)?.lastModified
      if (published) {
        const meta = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>
        return new Response(JSON.stringify({ ...meta, published }), {
          headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': CACHE },
        })
      }
    }
    return new Response(bytes, {
      headers: { 'content-type': contentType ?? 'application/json; charset=utf-8', 'cache-control': CACHE },
    })
  } catch {
    return new Response('not found', { status: 404 })
  }
}
