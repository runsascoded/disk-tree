/** Local stand-in for `/auth/sso`: `pnpm cfn:dev` has no CF Access in front
 *  of it. Signs `?email=` in through the same gate (so the allowlist policy
 *  still decides), and only when the request is to localhost *and* no
 *  `ACCESS_TEAM_DOMAIN` is configured — a deployment never has this route. */
import { gateFor } from '../../cfn/auth'
import type { Env } from '../../cfn/env'

export const onRequest: PagesFunction<Env> = async ({ request, env }) => {
  const url = new URL(request.url)
  const local = url.hostname === 'localhost' || url.hostname === '127.0.0.1'
  if (!local || env.ACCESS_TEAM_DOMAIN) return new Response('not found\n', { status: 404 })
  const email = url.searchParams.get('email')
  if (!email) return new Response('?email= required\n', { status: 400 })
  const { gate } = gateFor(env, request)
  const signed = await gate.signIn(email, request)
  if (!signed) return new Response(`${email} is not on ALLOWED_EMAILS\n`, { status: 403 })
  const next = url.searchParams.get('next') ?? '/'
  return new Response(null, { status: 302, headers: { location: next.startsWith('/') && !next.startsWith('//') ? next : '/', 'set-cookie': signed.cookie } })
}
