/** The edge half of the cards (specs/dogi.md): the deployment's OG config,
 * page meta stamping, and `/og/<kind>.png` rendering with the colo cache. */
import RESVG from './vendor/resvg.wasm'
import type { Env } from '../auth.js'
import { ledgerHead } from '../ledger.js'
import { stampMeta } from '../unfurl.js'
import { cardSvg, type CardData } from './card.js'
import { mapCard, type Site } from './data.js'
import { assignmentsCard, stagedCard, userCard, usersCard } from './pages.js'
import { ensureWasm, FONT_FILES, svgToPng } from './render.js'
import { pageView, type OgKind } from './routes.js'
import { expDay, IMAGE_TTL_DAYS, imagePath, ogKey, verifyImage, type OgTier } from './sign.js'
import { fullTier } from './tokens.js'

export type OgEnv = Env & {
  ASSETS: { fetch: (req: Request) => Promise<Response> }
  /** Set (any value) to stamp dynamic cards; unset = the static `og.jpg`. */
  OG_CARDS?: string
}

/** The kinds this build draws; the rest keep their static card. */
export const DRAWN: ReadonlySet<OgKind> = new Set(['map', 'staged', 'users', 'user', 'assignments'])

/** Kinds whose page Function already stamps its own title and description
 * (`functions/staged.ts`, `users.ts`, `user/[id].ts`, `assignments.ts`): the
 * middleware only swaps their image. */
const OWN_TITLES: ReadonlySet<OgKind> = new Set(['staged', 'users', 'user', 'assignments'])

export const siteOf = (env: OgEnv): Site => ({ name: env.ROOT_LABEL ?? 'storage', scheme: env.STORE_SCHEME ?? 'gs://' })

let keyMemo: { secret: string; key: Promise<CryptoKey> } | null = null
/** The OG key for this deployment, or null when it has no `SESSION_SECRET`. */
export function deploymentKey(env: OgEnv): Promise<CryptoKey> | null {
  const s = env.SESSION_SECRET
  if (!s) return null
  if (keyMemo?.secret !== s) keyMemo = { secret: s, key: ogKey(s) }
  return keyMemo.key
}

const now = () => Math.floor(Date.now() / 1000)

/** Stamp a page's HTML with its card when it has one: the anonymous tier for
 * any fetch, the full tier for a URL carrying this view's live token. */
export async function stampPage(env: OgEnv, url: URL, html: Response): Promise<Response> {
  if (!env.OG_CARDS) return html
  const pv = pageView(url, siteOf(env).name)
  if (!pv || !DRAWN.has(pv.kind)) return html
  const keyP = deploymentKey(env)
  if (!keyP) return html
  const key = await keyP
  // Full only with an `og=` token minted for exactly this view (and live in
  // D1); its image URL never outlives the token.
  const full = await fullTier(env.DB, key, pv.kind, pv.params, url.searchParams.get('og'), now())
  const tier: OgTier = full ? 'full' : 'anon'
  const day = Math.min(expDay(now(), IMAGE_TTL_DAYS), full?.day ?? Infinity)
  const image = url.origin + await imagePath(key, pv.kind, pv.params, tier, day)
  if (OWN_TITLES.has(pv.kind)) return stampMeta(html, { image, imageType: 'image/png', page: url.href })
  return stampMeta(html, {
    title: pv.title,
    desc: `${siteOf(env).name}: an interactive storage map of this view: who owns what, by size.`,
    image,
    imageType: 'image/png',
    page: url.href,
  })
}

let fontsMemo: Promise<Uint8Array[]> | null = null
function fonts(env: OgEnv, origin: string): Promise<Uint8Array[]> {
  fontsMemo ??= Promise.all(FONT_FILES.map(async f => {
    const r = await env.ASSETS.fetch(new Request(origin + f))
    if (!r.ok) throw new Error(`font ${f}: ${r.status}`)
    return new Uint8Array(await r.arrayBuffer())
  })).catch(e => { fontsMemo = null; throw e })
  return fontsMemo
}

async function cardData(env: OgEnv, kind: string, params: Record<string, string>, tier: OgTier, url: URL): Promise<CardData | null> {
  const site = siteOf(env)
  // The page's own title, recomputed from the signed view.
  const page = kind === 'map' ? `/${params.path ?? ''}` : kind === 'user' ? `/user/${params.id ?? ''}` : `/${kind}`
  const pu = new URL(page, url.origin)
  for (const [k, v] of Object.entries(params)) if (k !== 'path' && k !== 'id') pu.searchParams.set(k, v)
  const title = pageView(pu, site.name)?.title ?? site.name
  switch (kind) {
    case 'map': return mapCard(env, site, title, params, tier)
    case 'staged': return stagedCard(env, site, title, params, tier)
    case 'users': return usersCard(env, site, title, params, tier)
    case 'user': return userCard(env, site, title, params, tier)
    case 'assignments': return assignmentsCard(env, site, title, params, tier)
    default: return null
  }
}

/** `GET /og/<kind>.png?<view>&sig=…`: verify, then render (or serve the
 * colo-cached PNG). The cache key adds the ledger head, so an assignment
 * recolours a card within the URL's lifetime. */
export async function serveCard(ctx: { request: Request; env: OgEnv; waitUntil?: (p: Promise<unknown>) => void }): Promise<Response> {
  const { request, env } = ctx
  const url = new URL(request.url)
  const key = deploymentKey(env)
  if (!key) return new Response('cards not configured', { status: 503 })
  const v = await verifyImage(await key, url, now())
  if ('error' in v) return new Response(v.error, { status: 403 })
  const head = env.DB ? await ledgerHead(env).catch(() => 0) : 0
  const cacheKey = new Request(`${url.origin}${url.pathname}${url.search}&h=${head}`)
  const cache = (caches as unknown as { default: Cache }).default
  const hit = await cache.match(cacheKey)
  if (hit) return hit
  const t0 = Date.now()
  const data = await cardData(env, v.kind, v.params, v.tier, url)
  if (!data) return new Response('no such card', { status: 404 })
  const t1 = Date.now()
  await ensureWasm(RESVG)
  const png = svgToPng(cardSvg(data), await fonts(env, url.origin))
  const t2 = Date.now()
  const res = new Response(png, {
    headers: {
      'content-type': 'image/png',
      // The URL is signed and expires within days; a day's caching anywhere is fine.
      'cache-control': 'public, max-age=86400',
      'server-timing': `data;dur=${t1 - t0}, render;dur=${t2 - t1}`,
    },
  })
  const put = cache.put(cacheKey, res.clone())
  if (ctx.waitUntil) ctx.waitUntil(put)
  else await put
  return res
}
