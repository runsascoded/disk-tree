// CF Pages Function: read-only proxy for browsing the scan data bucket.
//
// Mounts file-tree's `createHandlers` over an `S3Store` pointed at GCS's
// S3-compatible XML API (verified: GCS speaks ListObjectsV2 + range GETs).
// The browser hits this same-origin, behind the site's CF Access gate — so
// no second sign-in and no CORS. Reads use a dedicated read-only HMAC key
// (SA `gcs-usage-browse@…`, `objectViewer` on this bucket ONLY); the
// `prefixes` allow-list caps exposure to the scan outputs.
//
// Auth model is intentionally coarse: anyone past CF Access can read the
// listing/snapshot data (metadata the gcs.oa.dev treemap already shows this
// audience). No per-user authz.
import { createHandlers } from '@rdub/file-tree/server'
import { S3Store } from '@rdub/file-tree/stores/s3'
import type { Env } from '../../_lib/auth.js'
import { storeCreds, storePrefixes, storeReady, storeTarget } from '../../_lib/index.js'

const BASE = '/v1/files'

export const onRequest = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  if (!storeReady(ctx.env)) {
    return new Response('scan-browser proxy not configured (missing store creds)', { status: 503 })
  }
  // The store seam (`_lib/index.ts`): GCS by default, R2 once `STORE_*` is set.
  const store = S3Store({
    ...storeTarget(ctx.env),
    prefixes: storePrefixes(ctx.env, ['listing/', 'snapshots/', 'sweep/']), // allow-list: scan outputs + sweep plans/logs + purge run records; `STORE_PREFIXES` overrides
    ...storeCreds(ctx.env),
  })
  // same-origin (behind CF Access) → no CORS needed
  const handlers = createHandlers(store, { basePath: BASE, corsOrigin: null })
  return (await handlers.handle(ctx.request)) ?? new Response('not found', { status: 404 })
}
