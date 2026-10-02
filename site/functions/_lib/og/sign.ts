/** Card URLs and view tokens (specs/dogi.md).
 *
 * - **Anonymous card URL**: `/og/<kind>.png?<view>`, unsigned. Signing it
 *   would protect nothing: any page fetch yields any view's anonymous card.
 * - **Full card URL**: `/og/<kind>.png?<view>&sig=<ee><tag>` (12 chars). The
 *   tag covers kind + canonical view + expiry. A missing, bad or expired `sig`
 *   serves the anonymous card (never an error), so it can't be forged,
 *   re-pointed or extended into labels.
 * - **View token**: `og=<ee><tag>` (12 chars) on a page URL. The tag covers
 *   kind + canonical view + expiry, so a token is good for exactly the view it
 *   was minted for (not a child, parent, sibling or other params). D1 records
 *   each mint by its token, which is how one is revoked.
 *
 * Tags are HMAC-SHA256 under one deployment key derived from `SESSION_SECRET`,
 * truncated to 60 bits: exactly 10 base64url chars. The key never leaves the
 * server, so the only attack is online guessing, and 2⁶⁰ guesses is out of
 * reach. Base64url because Slack and linkifiers mangle sub-delims. Expiries
 * are whole days since `EPOCH`, packed as 2 base64url chars (4,096 days ≈ 11
 * years). Image and token tags carry distinct purpose labels, so neither can
 * stand in for the other.
 *
 * Pure WebCrypto: runs in the Worker and in Node tests. */

export type OgTier = 'anon' | 'full'

/** Day 0 of packed expiries: 2026-01-01T00:00Z. */
export const EPOCH = 1_767_225_600
export const DAY_S = 86400
/** How long a stamped image URL stays valid (a week of Slack scrollback). */
export const IMAGE_TTL_DAYS = 7

const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

/** A day count as 2 base64url chars (0 ≤ days < 4096). */
export function packDays(days: number): string {
  if (!Number.isInteger(days) || days < 0 || days >= 4096) throw new Error(`expiry day out of range: ${days}`)
  return B64[days >> 6] + B64[days & 63]
}

export function unpackDays(s: string): number | null {
  if (s.length !== 2) return null
  const hi = B64.indexOf(s[0])
  const lo = B64.indexOf(s[1])
  return hi < 0 || lo < 0 ? null : (hi << 6) | lo
}

/** The day index (since `EPOCH`) whose end a credential made at `now` (unix s)
 * expires at, `ttlDays` whole days later. Valid through the end of that day. */
export const expDay = (now: number, ttlDays: number): number => Math.floor((now - EPOCH) / DAY_S) + ttlDays

/** Expired once `now` passes the end of `day`. */
const expired = (day: number, now: number): boolean => now >= EPOCH + (day + 1) * DAY_S

/** The view's params, canonical: keys sorted, empty values dropped,
 * URL-encoded as `URLSearchParams` would. Two URLs for the same view
 * canonicalize equal however their params were ordered. */
export function canonical(params: Record<string, string | null | undefined>): string {
  const sp = new URLSearchParams()
  for (const k of Object.keys(params).sort()) {
    const v = params[k]
    if (v != null && v !== '') sp.set(k, v)
  }
  return sp.toString()
}

const enc = new TextEncoder()

async function hmac(key: CryptoKey, msg: string): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(msg)))
}

/** The first 60 bits of a mac as 10 base64url chars. */
export const TAG_CHARS = 10
const tag60 = (mac: Uint8Array): string =>
  btoa(String.fromCharCode(...mac.slice(0, 8))).replace(/\+/g, '-').replace(/\//g, '_').slice(0, TAG_CHARS)

/** The OG key: HMAC(`SESSION_SECRET`, `og-card:v1`), imported for HMAC.
 * Rotating the session secret rotates it (and voids every card and token). */
export async function ogKey(secret: string): Promise<CryptoKey> {
  const base = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const derived = await hmac(base, 'og-card:v1')
  return crypto.subtle.importKey('raw', derived, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
}

/** Constant-time equality of two short ASCII strings. */
function same(a: string, b: string): boolean {
  if (a.length !== b.length) return false
  let d = 0
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i)
  return d === 0
}

async function imageTag(key: CryptoKey, kind: string, view: string, day: number): Promise<string> {
  return tag60(await hmac(key, `img\n${kind}\n${view}\nfull\n${day}`))
}

/** A card's path (the caller adds the origin): anonymous = the bare view;
 * full = the view plus a `sig` good through `day`. */
export async function imagePath(key: CryptoKey | null, kind: string, params: Record<string, string | null | undefined>, tier: OgTier, day?: number): Promise<string> {
  const view = canonical(params)
  const base = `/og/${kind}.png${view ? `?${view}` : ''}`
  if (tier === 'anon') return base
  if (!key || day == null) throw new Error('a full card URL needs the key and an expiry day')
  return `${base}${view ? '&' : '?'}sig=${packDays(day)}${await imageTag(key, kind, view, day)}`
}

export type Resolved = { kind: string; params: Record<string, string>; tier: OgTier; day?: number; why?: string }

/** Resolve a card request: its kind and view, and `full` only for a `sig`
 * that matches the kind + canonical view + expiry and hasn't expired; any
 * other `sig` (or none) is the anonymous card, with `why` for the log. Null
 * only for a non-card path. */
export async function resolveImage(key: CryptoKey | null, url: URL, now: number): Promise<Resolved | null> {
  const m = /^\/og\/([a-z]+)\.png$/.exec(url.pathname)
  if (!m) return null
  const sp = new URLSearchParams(url.search)
  const sig = sp.get('sig')
  sp.delete('sig')
  const params: Record<string, string> = {}
  for (const [k, v] of sp) params[k] = v
  const anon = (why?: string): Resolved => ({ kind: m[1], params, tier: 'anon', ...(why ? { why } : {}) })
  if (sig == null) return anon()
  const day = unpackDays(sig.slice(0, 2))
  if (!key || day == null || sig.length !== 2 + TAG_CHARS) return anon('bad sig')
  if (expired(day, now)) return anon('expired')
  if (!same(sig.slice(2), await imageTag(key, m[1], canonical(params), day))) return anon('bad signature')
  return { kind: m[1], params, tier: 'full', day }
}

async function tokenTag(key: CryptoKey, kind: string, view: string, day: number): Promise<string> {
  return tag60(await hmac(key, `tok\n${kind}\n${view}\n${day}`))
}

/** A view token: `<ee><tag>`, 12 chars. Deterministic per (view, expiry day):
 * two mints of one view on one day are one token (D1 logs both mints). */
export async function mintToken(key: CryptoKey, kind: string, params: Record<string, string | null | undefined>, day: number): Promise<string> {
  return packDays(day) + await tokenTag(key, kind, canonical(params), day)
}

/** The token's expiry day when it was minted for exactly this view and hasn't
 * expired; null otherwise. Revocation is the caller's (D1, by token). */
export async function checkToken(key: CryptoKey, token: string, kind: string, params: Record<string, string | null | undefined>, now: number): Promise<{ day: number } | null> {
  if (token.length !== 2 + TAG_CHARS || !/^[A-Za-z0-9_-]+$/.test(token)) return null
  const day = unpackDays(token.slice(0, 2))
  if (day == null || expired(day, now)) return null
  return same(token.slice(2), await tokenTag(key, kind, canonical(params), day)) ? { day } : null
}
