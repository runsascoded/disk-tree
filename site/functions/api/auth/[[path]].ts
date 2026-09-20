/**
 * The package's whole `/api/auth/*` surface: whoami, exchange (`?key=` →
 * session), logout, request-access, and the admin grant/request/log routes
 * (minting happens here, via POST /api/auth/grants).
 */
import { authRoutes } from '@open-athena/auth'
import { d1AuditQuery } from '@open-athena/auth/d1'
import { ADMIN_SCOPE, type Ctx, gateFor } from '../../_lib/auth.js'

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const gate = gateFor(ctx.env)
  if (!gate) return new Response('auth backend not configured (DB / SESSION_SECRET)\n', { status: 503 })
  const handle = authRoutes(gate, {
    adminScope: ADMIN_SCOPE,
    audit: ctx.env.DB ? d1AuditQuery(ctx.env.DB) : undefined,
  })
  return (await handle(ctx.request)) ?? new Response('not found\n', { status: 404 })
}
