/** Every `/api/*` request passes the gate (spec `specs/done/pages-auth.md`); the
 *  SPA shell and assets stay public so the wall has somewhere to render. */
import { gateApi, gateFor } from '../cfn/auth'
import { type Env, isOpen } from '../cfn/env'

export const onRequest: PagesFunction<Env> = async ({ request, env, next }) => {
  if (!new URL(request.url).pathname.startsWith('/api/')) return next()
  // Open demo: no gate, every `/api/*` route served without a session.
  if (isOpen(env)) return next()
  const { gate, audit } = gateFor(env, request)
  return gateApi(gate, request, next, audit)
}
