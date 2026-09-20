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

interface Env {
  GCS_HMAC_KEY_ID: string
  GCS_HMAC_SECRET: string
}

const BUCKET = 'oa-gcs-usage-dvx'
const BASE = '/v1/files'

export const onRequest = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  const { GCS_HMAC_KEY_ID, GCS_HMAC_SECRET } = ctx.env
  if (!GCS_HMAC_KEY_ID || !GCS_HMAC_SECRET) {
    return new Response('scan-browser proxy not configured (missing GCS HMAC creds)', { status: 503 })
  }
  const store = S3Store({
    endpoint: 'https://storage.googleapis.com', // GCS XML API is S3-compatible
    bucket: BUCKET,
    region: 'us-east1', // bucket location; GCS validates the SigV4 credential-scope region
    prefixes: ['listing/', 'snapshots/', 'sweep/', 'cw-sweep/'], // allow-list: scan outputs + sweep plans/logs + purge run records
    accessKeyId: GCS_HMAC_KEY_ID,
    secretAccessKey: GCS_HMAC_SECRET,
  })
  // same-origin (behind CF Access) → no CORS needed
  const handlers = createHandlers(store, { basePath: BASE, corsOrigin: null })
  return (await handlers.handle(ctx.request)) ?? new Response('not found', { status: 404 })
}
