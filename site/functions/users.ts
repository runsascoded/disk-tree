// `/users` unfurl: its own title/desc + the redacted owner-map og:image
// (names + state stripes; no sizes or $ — see src/UserPage.tsx UsersOgPage).
import { unfurlShell } from './_lib/unfurl.js'

interface Env {
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}

export const onRequest = (ctx: { request: Request; env: Env }): Promise<Response> => {
  const origin = new URL(ctx.request.url).origin
  return unfurlShell(ctx, {
    title: 'Marin GCS usage — users',
    desc: 'Who owns what across the marin-* buckets, and where every user’s bytes stand: keep / sweep / undecided.',
    image: `${origin}/og-users.jpg`,
    page: `${origin}/users`,
  })
}
