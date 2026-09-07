/** The only Access-gated path. CF Access authenticates at the edge and
 *  attaches `Cf-Access-Jwt-Assertion`; the package verifies it against the
 *  team domain / AUD, signs the email in (allowlist policy → 403 otherwise)
 *  and bounces to `?next`. */
import { ssoHandler } from '@open-athena/auth/cf-access'
import { gateFor } from '../../cfn/auth'
import type { Env } from '../../cfn/env'

export const onRequest: PagesFunction<Env> = async ({ request, env }) => {
  if (!env.ACCESS_TEAM_DOMAIN) return new Response('ACCESS_TEAM_DOMAIN not configured\n', { status: 503 })
  const { gate } = gateFor(env, request)
  return ssoHandler({ gate, teamDomain: env.ACCESS_TEAM_DOMAIN, aud: env.ACCESS_AUD })({ request })
}
