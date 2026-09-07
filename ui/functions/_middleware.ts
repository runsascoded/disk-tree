/** Every `/api/*` request passes the gate (spec `specs/pages-auth.md`); the
 *  SPA shell and assets stay public so the wall has somewhere to render. */
import { gateApi, gateFor } from '../cfn/auth'
import type { Env } from '../cfn/env'

export const onRequest: PagesFunction<Env> = async ({ request, env, next }) => {
  if (!new URL(request.url).pathname.startsWith('/api/')) return next()
  const { gate, audit } = gateFor(env, request)
  return gateApi(gate, request, next, audit)
}
