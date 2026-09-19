/** `GET /og/<scheme>/<bucket>/<path…>` — the link-card treemap image for a scan
 *  uri (referenced by the og tags `_middleware.ts` injects). Resolution order:
 *
 *    1. R2 `og/<key>.jpg`   — refreshed daily from the live treemap by
 *                             `rescan-demo.yml` (freshest; tracks re-scans).
 *    2. static `/_og/<key>.jpg` — seed cards bundled with the deploy, so a
 *                             fresh project serves per-bucket cards on day one.
 *    3. static `/og.jpg`    — the site-default card, for any uri (sub-path,
 *                             uncaptured bucket) with neither of the above.
 *
 *  Tier B extends this by rendering the image at the edge on a miss. */
import type { Env } from '../../cfn/env'

export const onRequestGet: PagesFunction<Env> = async ({ params, request, env }) => {
  const segs = (Array.isArray(params.path) ? params.path : [params.path]).filter(Boolean) as string[]
  const key = segs.map(s => decodeURIComponent(s)).join('/')

  const obj = await env.SCANS.get(`og/${key}.jpg`)
  if (obj) {
    return new Response(obj.body, {
      headers: { 'content-type': 'image/jpeg', 'cache-control': 'public, max-age=3600' },
    })
  }

  // A missing static asset resolves to the SPA shell (200 text/html), not a
  // 404 — so only trust the seed when it actually came back as an image.
  const seeded = await fetch(new URL(`/_og/${key}.jpg`, request.url))
  if (seeded.ok && (seeded.headers.get('content-type') ?? '').startsWith('image/')) {
    return new Response(seeded.body, { headers: { 'content-type': 'image/jpeg', 'cache-control': 'public, max-age=3600' } })
  }

  return fetch(new URL('/og.jpg', request.url))
}
