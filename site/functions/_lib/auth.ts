/**
 * One gate for the whole site (`@open-athena/auth`, Tier 2).
 *
 * Identity sources, in the order `requireScope` tries them:
 *
 *  1. `Cf-Access-Jwt-Assertion` header — only on a deployment that still sits
 *     behind a CF Access edge gate (`ACCESS_AUD` set). Neither gcs.oa.dev nor
 *     (once specs/oidc-cutover-cw.md lands) cw-s3.oa.dev has one: sessions are
 *     minted by our own Google OIDC client (`/auth/google`) or an emailed code
 *     (`/auth/email/*`).
 *  2. The app session cookie / `Authorization: Bearer` / `?key=` — the
 *     `@open-athena/auth` gate, backed by D1. This is what makes named share
 *     links ("anyone with the link can view") possible: minted links redeem
 *     for a session that re-joins its grant row every request, so revocation
 *     is instant.
 *
 * Scopes: staff (`STAFF_DOMAIN`) get everything; a viewer domain
 * (`VIEWER_DOMAINS`, e.g. coreweave.com on cw-s3) or a D1 `allowed_emails`
 * row gets the deployment's base scope — email sessions re-derive that from
 * `scopesFor` on every request, so removal bites instantly. Grant sessions
 * carry the scopes they were minted with (normally just the base scope).
 */
import { type Auth, createGate, type Gate, hasScope } from '@open-athena/auth'
import { verifyAccessJwt } from '@open-athena/auth/cf-access'
import { d1AuditSink, d1GrantStore, d1RequestStore } from '@open-athena/auth/d1'
import type { D1Database } from '@cloudflare/workers-types'

export interface Env {
  DB?: D1Database
  SESSION_SECRET?: string
  ACCESS_TEAM_DOMAIN?: string
  /** AUD tag of the Access app whose edge JWTs we accept; unset = no edge. */
  ACCESS_AUD?: string
  /** OIDC (our own Google client) — the ZT-free sign-in path. Set as Pages
   *  secrets; see specs/oidc-cutover-cw.md. Absent → `/auth/google` 503s. */
  GOOGLE_CLIENT_ID?: string
  GOOGLE_CLIENT_SECRET?: string
  /** Email-code fallback (ZT One-Time-PIN replacement) — Resend sender + `from`
   *  address (`Name <addr@verified-domain>`). Absent → `/auth/email/*` 503s. */
  RESEND_API_KEY?: string
  MAIL_FROM?: string
  STAFF_DOMAIN?: string
  /** Comma-separated email domains admitted to the base scope with no
   *  `allowed_emails` row — what the Access policy's email-domain include did
   *  (cw-s3: `coreweave.com`). Unset = allowlist only. */
  VIEWER_DOMAINS?: string
  /** Set where the `admin_emails` table exists (cw-s3, migrations/cw/0004):
   *  its rows get `admin` on top of the base scope. Unset (gcs) = staff only. */
  ADMIN_EMAILS?: string
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
   *  behind a CF Access edge gate. Every request then carries an edge JWT for
   *  an already-authorized viewer, so edge identities get the base scope
   *  without a policy check; staff and `admin_emails` rows get `admin`. Unset:
   *  no edge; the app gate authorizes everything. */
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

const viewerDomains = (env: Env): string[] =>
  (env.VIEWER_DOMAINS ?? '').split(',').map(d => d.trim().toLowerCase()).filter(Boolean)

/** Rows of the deployment's `admin_emails` table (where it exists — `ADMIN_EMAILS`)
 *  rank as admins on top of whatever the policy admits them to. */
async function adminRow(env: Env, email: string): Promise<boolean> {
  if (!env.DB || !env.ADMIN_EMAILS) return false
  const row = await env.DB.prepare('SELECT email FROM admin_emails WHERE email = ?').bind(email).first()
  return !!row
}

/**
 * Email → scopes: the in-app policy that the Access policy used to be. Staff
 * get everything; a viewer domain (`VIEWER_DOMAINS`) or a D1 `allowed_emails`
 * row (the app-owned allowlist — see /admin/db) gets the base scope, plus
 * `admin` for an `admin_emails` row. Email sessions re-derive scopes here on
 * every request, so removing a row de-authorizes existing sessions on their
 * next request. If the DB isn't bound (local dev), non-staff fall back to
 * allowed — the CF Access edge gate is the enforcement in that configuration.
 */
export const scopesFor = (env: Env) => async (raw: string): Promise<string[] | null> => {
  const email = raw.toLowerCase()
  if (email.endsWith(`@${staffDomain(env)}`)) return [GCS_SCOPE, CW_SCOPE, ADMIN_SCOPE, REQUESTS_SCOPE]
  const base = baseScope(env)
  if (!env.DB) return [base]
  const admitted = viewerDomains(env).some(d => email.endsWith(`@${d}`))
    || !!(await env.DB.prepare('SELECT email FROM allowed_emails WHERE email = ?').bind(email).first())
  if (!admitted) return null
  return (await adminRow(env, email)) ? [base, ADMIN_SCOPE] : [base]
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

/** Staff (by domain) or an `admin_emails` row (the deployment's own admin
 *  list, `site/migrations/cw/0004_admin.sql`, where `ADMIN_EMAILS` says it exists). */
export async function isAdmin(env: Env, email: string): Promise<boolean> {
  if (email.toLowerCase().endsWith(`@${staffDomain(env)}`)) return true
  return adminRow(env, email.toLowerCase())
}

async function edgeIdentity(req: Request, env: Env): Promise<Identity | null> {
  // Only a deployment behind an Access edge (ACCESS_AUD set) trusts the header.
  if (!env.ACCESS_AUD) return null
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
