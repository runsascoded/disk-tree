/**
 * One gate for the whole site (`@open-athena/auth`, Tier 2), plus the
 * transition shim that keeps both hosts working while the CF Access topology
 * moves from "edge-gate the whole host" to "Access is an SSO IdP on
 * `/auth/sso`; the app gate authorizes everything else".
 *
 * Identity sources, in the order `requireScope` tries them:
 *
 *  1. `Cf-Access-Jwt-Assertion` header — present on any request that came
 *     through a CF Access edge gate — today only `/auth/sso` (the SSO hand-off;
 *     the CoreWeave dashboard is its own deployment with its own Access app).
 *  2. The app session cookie / `Authorization: Bearer` / `?key=` — the
 *     `@open-athena/auth` gate, backed by D1. This is what makes named share
 *     links ("anyone with the link can view") possible: minted links redeem
 *     for a session that re-joins its grant row every request, so revocation
 *     is instant.
 *
 * Scopes: staff (`@openathena.ai`) get everything; anyone else who made it
 * through an Access gate (the Stanford whitelist) gets `gcs` only — same for
 * email sessions minted at `/auth/sso`, whose scopes re-derive from
 * `scopesFor` on every request. Grant sessions carry the scopes they were
 * minted with (normally just `gcs`).
 */
import { type Auth, createGate, type Gate, hasScope } from '@open-athena/auth'
import { verifyAccessJwt } from '@open-athena/auth/cf-access'
import { d1AuditSink, d1GrantStore, d1RequestStore } from '@open-athena/auth/d1'
import type { D1Database } from '@cloudflare/workers-types'

export interface Env {
  DB?: D1Database
  SESSION_SECRET?: string
  ACCESS_TEAM_DOMAIN?: string
  /** AUD tags of the Access apps whose edge JWTs we accept (gcs + cw). */
  ACCESS_AUD?: string
  STAFF_DOMAIN?: string
  /** Local dev only (`.dev.vars`): email the localhost dev identity acts as.
   *  Matters when the D1 binding is remote (writes land in the real ledger). */
  DEV_EMAIL?: string
  /** GCS deploys: HMAC creds for the index/data store. Optional now that the
   *  store seam is env-generalized — an r2/s3 deploy sets `STORE_*` instead
   *  (`_lib/index.ts:storeCreds`). */
  GCS_HMAC_KEY_ID?: string
  GCS_HMAC_SECRET?: string
  /** Generalized index-store seam (specs/union-of-roots.md — the r2/public
   *  layer). Point the index reader at any S3-compatible store; unset falls
   *  back to the GCS defaults + `GCS_HMAC_*` so gcs/cw are unchanged. */
  STORE_ENDPOINT?: string
  STORE_BUCKET?: string
  STORE_REGION?: string
  /** Comma-separated allowed key prefixes; unset = the GCS default set. */
  STORE_PREFIXES?: string
  STORE_ACCESS_KEY_ID?: string
  STORE_SECRET_ACCESS_KEY?: string
  /** Global second cache tier behind the colo cache (`_lib/edgeCache.ts`). */
  CACHE_KV?: KVNamespace
  /** Deployment seam (specs/denovo-factor.md): set when the WHOLE host sits
   *  behind a CF Access edge gate (cw-s3.oa.dev). Every request then carries
   *  an edge JWT for an already-authorized viewer, so edge identities get the
   *  base scope without an `allowed_emails` row; staff and `admin_emails`
   *  rows get `admin`. Unset (gcs.oa.dev): only `/auth/sso` is edge-gated
   *  and the app gate authorizes everything else. */
  EDGE_TRUSTED?: string
  /** The scope every viewer of this deployment needs (`gcs` | `cw`). */
  BASE_SCOPE?: string
  /** Public/no-gate deploys (r2.rbw.sh, per-project embeds): grant the base
   *  viewer scope anonymously so read endpoints serve without auth. Admin and
   *  any non-base scope still require a real identity, so mutations stay closed
   *  (specs/federated-scans.md). */
  PUBLIC_READ?: string
  /** The store root's crumb label (`marin GCS`, `marin CoreWeave`). */
  ROOT_LABEL?: string
  /** Snapshot dir of this store inside the data bucket (`snapshots/<sub>/`); unset = the bare `snapshots/`. */
  SNAPSHOTS_SUBDIR?: string
  /** Dedicated SA key (Batch submit + actAs the job SA) for the sweep dispatch bridge. */
  GCP_SA_KEY?: string
}

export interface Ctx {
  request: Request
  env: Env
}

export const GCS_SCOPE = 'gcs'
export const CW_SCOPE = 'cw'
export const ADMIN_SCOPE = 'admin'
export const REQUESTS_SCOPE = 'requests'

export const TEAM_DOMAIN = 'https://openathena-ai-pages.cloudflareaccess.com'

/** The deployment's base scope: what `requireViewer` asks for. */
export const baseScope = (env: Env): string => env.BASE_SCOPE ?? GCS_SCOPE

const staffDomain = (env: Env) => env.STAFF_DOMAIN ?? 'openathena.ai'

/**
 * Email → scopes. Staff get everything; anyone else must be in the D1
 * `allowed_emails` table (the app-owned whitelist — see /admin) to get the
 * base `gcs` scope. Email sessions re-derive scopes here on every request,
 * so removing a row de-authorizes existing sessions on their next request.
 * If the DB isn't bound (local dev), non-staff fall back to allowed — the
 * CF Access edge gate is the enforcement in that configuration.
 */
export const scopesFor = (env: Env) => async (email: string): Promise<string[] | null> => {
  if (email.endsWith(`@${staffDomain(env)}`)) return [GCS_SCOPE, CW_SCOPE, ADMIN_SCOPE, REQUESTS_SCOPE]
  if (!env.DB) return [GCS_SCOPE]
  const row = await env.DB.prepare('SELECT email FROM allowed_emails WHERE email = ?')
    .bind(email.toLowerCase()).first()
  return row ? [GCS_SCOPE] : null
}

export function gateFor(env: Env): Gate | null {
  if (!env.DB || !env.SESSION_SECRET) return null
  return createGate({
    store: d1GrantStore(env.DB),
    requests: d1RequestStore(env.DB),
    audit: d1AuditSink(env.DB),
    secret: env.SESSION_SECRET,
    policy: scopesFor(env),
  })
}

/** Identity + scopes however the request proved them; null if it didn't. */
export interface Identity {
  email: string | null
  /** Grant display name, for grant sessions with no email. */
  name: string | null
  scopes: string[]
  /** `admin` scope, as a flag — what the plan-first sweep console keys on. */
  admin: boolean
  via: 'edge' | 'session' | 'grant' | 'public'
}
const withAdmin = (id: Omit<Identity, 'admin'>): Identity => ({ ...id, admin: id.scopes.includes(ADMIN_SCOPE) || id.scopes.includes('*') })

/** Staff (by domain) or an `admin_emails` row (the edge-trusted deployment's
 *  own admin list, `site/migrations/cw/0004_admin.sql`). */
export async function isAdmin(env: Env, email: string): Promise<boolean> {
  if (email.toLowerCase().endsWith(`@${staffDomain(env)}`)) return true
  if (!env.DB || !env.EDGE_TRUSTED) return false
  const row = await env.DB.prepare('SELECT email FROM admin_emails WHERE email = ?').bind(email.toLowerCase()).first()
  return !!row
}

async function edgeIdentity(req: Request, env: Env): Promise<Identity | null> {
  const jwt = req.headers.get('Cf-Access-Jwt-Assertion')
  if (!jwt) return null
  const teamDomain = env.ACCESS_TEAM_DOMAIN ?? TEAM_DOMAIN
  for (const aud of [env.ACCESS_AUD]) {
    const email = await verifyAccessJwt(jwt, teamDomain, aud)
    if (email) {
      if (env.EDGE_TRUSTED) {
        // The edge already authorized this viewer; the app only ranks them.
        const admin = await isAdmin(env, email)
        return withAdmin({ email, name: null, scopes: admin ? [baseScope(env), ADMIN_SCOPE] : [baseScope(env)], via: 'edge' })
      }
      const scopes = await scopesFor(env)(email)
      if (!scopes) return null
      return withAdmin({ email, name: null, scopes, via: 'edge' })
    }
  }
  return null
}

function authIdentity(auth: Auth): Identity {
  if (auth.kind === 'sso') return withAdmin({ email: auth.email, name: null, scopes: auth.scopes, via: 'session' })
  return withAdmin({ email: auth.grant.email ?? null, name: auth.grant.name ?? null, scopes: auth.scopes, via: 'grant' })
}

/** All app scopes — granted to the local-dev identity so `/data` etc. work
 *  without a CF Access edge or a minted session. */
const DEV_SCOPES = [GCS_SCOPE, CW_SCOPE, ADMIN_SCOPE, REQUESTS_SCOPE]

export async function identify(ctx: Ctx): Promise<Identity | null> {
  // Local dev has no CF Access edge and no minted session, so `wrangler pages
  // dev` requests would 401 — leaving the client `DEV_IDENTITY` stub (which
  // only fakes the *client's* whoami) disagreeing with the server. Treat any
  // request whose host is localhost as a full-scope dev user so the two agree.
  // This can't reach prod: Cloudflare routes by the real hostname, so a
  // gcs.oa.dev request's URL host is never `localhost`/`127.0.0.1`.
  const host = new URL(ctx.request.url).hostname
  if (host === 'localhost' || host === '127.0.0.1') {
    return withAdmin({ email: ctx.env.DEV_EMAIL ?? 'dev@example.test', name: null, scopes: DEV_SCOPES, via: 'session' })
  }
  const edge = await edgeIdentity(ctx.request, ctx.env)
  if (edge) return edge
  const gate = gateFor(ctx.env)
  if (!gate) return null
  const auth = await gate.authenticate(ctx.request)
  return auth ? authIdentity(auth) : null
}

/** Gate a handler on a scope; 401/403 as JSON. */
export async function requireScope(ctx: Ctx, scope: string): Promise<Identity | Response> {
  // Public deploy: the base viewer scope is granted anonymously so read
  // endpoints serve without auth. A real identity (if the deploy also has a
  // gate) still wins; any non-base scope (admin, other stores) falls through
  // to the normal gate, so mutations stay closed (specs/federated-scans.md).
  if (ctx.env.PUBLIC_READ && scope === baseScope(ctx.env)) {
    const id = await identify(ctx)
    return id ?? { email: null, name: null, scopes: [baseScope(ctx.env)], admin: false, via: 'public' }
  }
  const id = await identify(ctx)
  if (!id) return json({ error: 'unauthenticated' }, 401)
  if (!id.scopes.includes(scope) && !id.scopes.includes('*')) return json({ error: 'forbidden' }, 403)
  return id
}

export { hasScope }

/** Any authenticated viewer of this deployment (reads). */
export const requireViewer = (ctx: Ctx): Promise<Identity | Response> => requireScope(ctx, baseScope(ctx.env))
/** An admin (plan writes + sweep dispatch). */
export const requireAdmin = (ctx: Ctx): Promise<Identity | Response> => requireScope(ctx, ADMIN_SCOPE)

export const json = (data: unknown, status = 200, headers: Record<string, string> = {}): Response =>
  new Response(JSON.stringify(data) + '\n', {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...headers },
  })
