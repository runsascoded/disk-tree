/**
 * `GET /api/store` — the configured scan store, as copy: where the `/files`
 * proxy reads from (`storeTarget`) and which prefixes it exposes
 * (`storePrefixes`, the `/v1/files` defaults). No credentials. The `/files`
 * page header names it instead of a baked-in bucket.
 *
 *   → { uri: "r2://idx", endpoint, bucket, region, prefixes: ["listing/", …] }
 */
import { type Ctx, json, requireViewer } from '../_lib/auth.js'
import { storePrefixes, storeScheme, storeTarget } from '../_lib/index.js'

export const FILES_PREFIXES = ['listing/', 'snapshots/', 'sweep/']

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const id = await requireViewer(ctx)
  if (id instanceof Response) return id
  const target = storeTarget(ctx.env)
  return json({ uri: `${storeScheme(target.endpoint)}://${target.bucket}`, ...target, prefixes: storePrefixes(ctx.env, FILES_PREFIXES) })
}
