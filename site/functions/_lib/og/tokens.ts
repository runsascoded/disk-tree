/** Per-view token mints in D1 (`og_tokens`, gcs `0034`; specs/dogi.md): the
 * audit trail and the revocation switch. A deployment without the table
 * mints nothing and honours no token (every card stays anonymous). */
import type { D1Database } from '@cloudflare/workers-types'
import { pageView } from './routes.js'
import { canonical, checkToken, expDay, mintToken } from './sign.js'

export const TOKEN_DAYS_DEFAULT = 30
export const TOKEN_DAYS_MAX = 90

export interface Mint {
  token: string
  /** The page URL with `og=<token>` added. */
  url: string
  expDay: number
}

/** Mint a token for `pageUrl`'s view and record it. Null when the URL has no
 * card of its own. */
export async function mint(db: D1Database, key: CryptoKey, site: string, pageUrl: URL, by: string, now: number, days = TOKEN_DAYS_DEFAULT): Promise<Mint | null> {
  const u = new URL(pageUrl.href)
  u.searchParams.delete('og')
  const pv = pageView(u, site)
  if (!pv) return null
  const day = expDay(now, Math.min(Math.max(1, Math.floor(days)), TOKEN_DAYS_MAX))
  const token = await mintToken(key, pv.kind, pv.params, day)
  await db.prepare('INSERT INTO og_tokens (token, kind, view, page, minted_by, minted_ts, exp_day) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .bind(token, pv.kind, canonical(pv.params), u.pathname + u.search, by, now, day).run()
  u.searchParams.set('og', token)
  return { token, url: u.href, expDay: day }
}

/** Minted (a row exists) and not revoked. A missing table = no. */
export async function tokenLive(db: D1Database, token: string): Promise<boolean> {
  try {
    const r = await db.prepare('SELECT COUNT(*) AS n, SUM(revoked_ts IS NOT NULL) AS rev FROM og_tokens WHERE token = ?').bind(token).first<{ n: number; rev: number | null }>()
    return !!r && r.n > 0 && !r.rev
  } catch {
    return false
  }
}

/** The full tier for this page fetch: an `og=` token minted for exactly this
 * view, unexpired, and live in D1. */
export async function fullTier(db: D1Database | undefined, key: CryptoKey, kind: string, params: Record<string, string>, token: string | null, now: number): Promise<{ day: number } | null> {
  if (!token || !db) return null
  const ok = await checkToken(key, token, kind, params, now)
  return ok && await tokenLive(db, token) ? ok : null
}

export interface TokenRow {
  id: number
  token: string
  kind: string
  page: string
  minted_by: string
  minted_ts: number
  exp_day: number
  revoked_by: string | null
  revoked_ts: number | null
}

export async function listTokens(db: D1Database, limit = 200): Promise<TokenRow[]> {
  return (await db.prepare('SELECT id, token, kind, page, minted_by, minted_ts, exp_day, revoked_by, revoked_ts FROM og_tokens ORDER BY minted_ts DESC, id DESC LIMIT ?').bind(limit).all<TokenRow>()).results
}

/** Revoke every mint of `token`; returns how many rows changed. */
export async function revoke(db: D1Database, token: string, by: string, now: number): Promise<number> {
  const r = await db.prepare('UPDATE og_tokens SET revoked_by = ?, revoked_ts = ? WHERE token = ? AND revoked_ts IS NULL').bind(by, now, token).run()
  return r.meta.changes ?? 0
}
