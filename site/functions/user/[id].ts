// `/user/:id` unfurl: the user's display name in the title (name + handle is
// the public ceiling — never sizes or $), sharing the /users og image.
import { IDENTITIES } from '../../src/identities.gen.js'
import { unfurlShell } from '../_lib/unfurl.js'

interface Env {
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}

export const onRequest = async (ctx: { request: Request; env: Env }): Promise<Response> => {
  const url = new URL(ctx.request.url)
  const id = decodeURIComponent(url.pathname.split('/')[2] ?? '')
  const name = IDENTITIES[id]?.name ?? (id ? id.split('-')[0].replace(/^./, c => c.toUpperCase()) : 'User')
  // Per-user card when `scripts/shoot-user-ogs.mjs` has generated one;
  // otherwise the shared owner-map card.
  const perUser = `${url.origin}/og-user/${encodeURIComponent(id)}.jpg`
  const probe = await ctx.env.ASSETS.fetch(new Request(perUser, { method: 'HEAD' })).catch(() => null)
  // A missing asset comes back as the SPA shell (200, text/html) — only a
  // real image counts as "this user has a card".
  const hasCard = !!probe?.ok && (probe.headers.get('content-type') ?? '').startsWith('image/')
  return unfurlShell(ctx, {
    title: `${name} — Marin GCS usage`,
    desc: 'Per-user storage breakdown: what’s keep-marked, sweep-marked, and still undecided.',
    image: hasCard ? perUser : `${url.origin}/og-users.jpg`,
    page: `${url.origin}${url.pathname}`,
  })
}
