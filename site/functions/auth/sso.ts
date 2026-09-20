/**
 * SSO hand-off: CF Access authenticates (OA Google SSO or one-time PIN for
 * whitelisted externals), we trade its JWT for an app session cookie and
 * bounce back to `?next`. After the Access-app cutover this is the only
 * edge-gated path on gcs.oa.dev — the rest of the site is public shell +
 * app-gated data, which is what lets `?key=` share links work at all.
 */
import { type Ctx, gateFor, TEAM_DOMAIN } from '../_lib/auth.js'
import { verifyAccessJwt } from '@open-athena/auth/cf-access'

/** Same-origin paths only — anything else is an open redirect off a login path. */
const safeNext = (raw: string | null): string => (raw && raw.startsWith('/') && !raw.startsWith('//') ? raw : '/')

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const { request, env } = ctx
  const jwt = request.headers.get('Cf-Access-Jwt-Assertion')
  if (!jwt) return new Response('no Access JWT — is this path still gated?\n', { status: 401 })
  const teamDomain = env.ACCESS_TEAM_DOMAIN ?? TEAM_DOMAIN
  let email: string | null = null
  for (const aud of [env.ACCESS_AUD]) {
    email = await verifyAccessJwt(jwt, teamDomain, aud)
    if (email) break
  }
  if (!email) return new Response('Access JWT failed verification\n', { status: 401 })

  const gate = gateFor(env)
  if (!gate) return new Response('auth backend not configured (DB / SESSION_SECRET)\n', { status: 503 })
  const res = await gate.signIn(email, request)
  if (!res) {
    return new Response(
      `${email} is not on the allowlist for this dashboard.\nPing Ryan Williams (Discord) to be added, or open a share link if you were sent one.\n`,
      { status: 403 },
    )
  }

  const next = safeNext(new URL(request.url).searchParams.get('next'))
  return new Response(null, { status: 302, headers: { location: next, 'set-cookie': res.cookie } })
}
