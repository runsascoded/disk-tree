// GET /api/whoami — the viewer's identity for FE gating, in the package's
// `SsoWhoami` shape ({ kind, email, admin, scopes, subject }) so it can also
// serve as the client's whoami source on an edge-gated deployment (`AUTH_MODE
// === 'edge'`: `/cdn-cgi/access/get-identity` only returns {email,name}, and
// the app needs scopes + the admin flag, which are decided server-side — staff
// domain, viewer domains, the `admin_emails` / `allowed_emails` rows). Signed
// out = 401, which the package hook reads as "nobody"; the body still says so
// for the plain-fetch callers (`StagedPage.tsx`).
import { type Ctx, type Env as AuthEnv, identify, json } from "../_lib/auth.js"

export const onRequestGet = async (ctx: Ctx & { env: AuthEnv }): Promise<Response> => {
  const id = await identify(ctx)
  if (!id || !id.email) return json({ email: null, admin: false }, 401)
  return json({ kind: 'sso', email: id.email, admin: id.admin, scopes: id.scopes, subject: null })
}
