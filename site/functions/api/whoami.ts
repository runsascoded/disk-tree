// GET /api/whoami — the viewer's identity + admin flag, for FE gating.
// `/cdn-cgi/access/get-identity` only returns {email,name}; the app needs to
// know whether to show mark/plan/dispatch admin controls, which is decided
// server-side (staff domain OR the admin_emails allowlist). Returns
// { email, admin } (admin false when unauthenticated).
import { type Ctx, type Env as AuthEnv, identify, json } from "../_lib/auth.js"

export const onRequestGet = async (ctx: Ctx & { env: AuthEnv }): Promise<Response> => {
  const id = await identify(ctx)
  if (!id) return json({ email: null, admin: false })
  return json({ email: id.email, admin: id.admin })
}
