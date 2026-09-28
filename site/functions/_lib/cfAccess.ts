/**
 * Cloudflare Access JWT verification for a deployment that still sits behind a
 * Zero Trust edge gate (`ACCESS_AUD` set). `@open-athena/auth` dropped its
 * `cf-access` adapter when the OA sites moved to their own Google OIDC client
 * (specs/done/oidc-cutover.md); the edge path lives on here, verbatim from the
 * package's last copy (`e254545`: `adapters/cf-access.ts` + `core/jwt.ts`), for
 * the deployments that haven't cut over yet. Delete with the last `ACCESS_AUD`.
 *
 * We verify the RS256 `Cf-Access-Jwt-Assertion` ourselves rather than trust
 * `Cf-Access-Authenticated-User-Email`: the friendly header is not forwarded
 * through Pages origin-to-origin proxying, and full verification keeps the
 * identity trustworthy even if the edge gating is later misconfigured.
 */
import { b64uDecodeBytes, b64uDecodeString } from '@open-athena/auth'

const enc = new TextEncoder()

interface Jwk { kid?: string; [k: string]: unknown }

/**
 * The verified claims, or null. Null covers every failure — malformed, wrong
 * algorithm, unknown key, bad signature, wrong issuer/audience, expired —
 * because a caller can do nothing useful with the distinction, and reporting
 * it back to whoever presented the token is an oracle.
 */
async function verifyRs256Jwt(
  jwt: string,
  jwksUrl: string,
  { issuer, audience, nowMs = Date.now(), cacheTtlS = 3600 }: { issuer: string; audience?: string; nowMs?: number; cacheTtlS?: number },
): Promise<Record<string, unknown> | null> {
  const parts = jwt.split('.')
  if (parts.length !== 3) return null
  const [h, p, s] = parts
  let header: { alg?: string; kid?: string }
  try { header = JSON.parse(b64uDecodeString(h)) as { alg?: string; kid?: string } } catch { return null }
  // Pinned, not read from the token: accepting the token's own `alg` is how
  // `alg: none` and HMAC-with-the-public-key forgeries get in.
  if (header.alg !== 'RS256') return null
  const certs = await fetch(jwksUrl, { cf: { cacheTtl: cacheTtlS } } as RequestInit)
    .then(r => (r.ok ? (r.json() as Promise<{ keys?: Jwk[] }>) : null))
    .catch(() => null)
  const jwk = certs?.keys?.find(k => k.kid === header.kid)
  if (!jwk) return null
  const key = await crypto.subtle.importKey('jwk', jwk as unknown as JsonWebKey, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['verify'])
  let ok: boolean
  try { ok = await crypto.subtle.verify('RSASSA-PKCS1-v1_5', key, b64uDecodeBytes(s), enc.encode(`${h}.${p}`)) } catch { return null }
  // Verify before parsing: an unauthenticated payload never reaches JSON.parse.
  if (!ok) return null
  let claims: Record<string, unknown>
  try { claims = JSON.parse(b64uDecodeString(p)) as Record<string, unknown> } catch { return null }
  if (claims.iss !== issuer) return null
  // `<=`: "exactly at exp" is expired, as everywhere else in the package.
  if (typeof claims.exp !== 'number' || claims.exp * 1000 <= nowMs) return null
  if (audience) {
    const aud = claims.aud
    if (!(Array.isArray(aud) ? aud : [aud]).includes(audience)) return null
  }
  return claims
}

/**
 * Verify an Access JWT against the Zero Trust team's public certs and return
 * the authenticated email, or null. `aud` is checked when `expectedAud` is
 * given — do give it: without it any app in the same team is accepted.
 */
export async function verifyAccessJwt(jwt: string, teamDomain: string, expectedAud?: string, nowMs = Date.now()): Promise<string | null> {
  const claims = await verifyRs256Jwt(jwt, `${teamDomain}/cdn-cgi/access/certs`, { issuer: teamDomain, audience: expectedAud, nowMs })
  return typeof claims?.email === 'string' ? claims.email : null
}
