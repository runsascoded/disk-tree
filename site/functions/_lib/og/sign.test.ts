import { describe, expect, it } from 'vitest'
import { canonical, checkToken, EPOCH, expDay, imagePath, mintToken, ogKey, packDays, resolveImage, unpackDays } from './sign'

// 2026-10-02T12:00Z: day 274 since EPOCH (2026-01-01).
const NOW = EPOCH + 274 * 86400 + 12 * 3600
const at = (p: string) => new URL(`https://site.example.org${p}`)
// Vectors cross-checked against Python: base64url(HMAC-SHA256(HMAC(b's3cret', b'og-card:v1'), msg)[:8])[:10].
const VIEW = { path: 'marin-a/ckpt', d: '261002' }

describe('canonical, days', () => {
  it('sorted keys, empties dropped, encoded', () => {
    expect([
      canonical({ path: 'b/x y', d: '261002', f: '' }),
      canonical({ f: undefined, d: '261002', path: 'b/x y' }),
      canonical({ q: 'percy|chi-heem', s: null }),
      canonical({}),
    ]).toEqual(['d=261002&path=b%2Fx+y', 'd=261002&path=b%2Fx+y', 'q=percy%7Cchi-heem', ''])
  })
  it('expiry days pack to 2 base64url chars', () => {
    expect([expDay(NOW, 7), packDays(0), packDays(63), packDays(64), packDays(281), packDays(4095), unpackDays('EZ'), unpackDays('E'), unpackDays('E!')])
      .toEqual([281, 'AA', 'A_', 'BA', 'EZ', '__', 281, null, null])
    expect(() => packDays(4096)).toThrow('expiry day out of range: 4096')
  })
})

describe('card URLs: anonymous unsigned, full with a 12-char sig', () => {
  it('test vectors', async () => {
    const k = await ogKey('s3cret')
    expect([
      await imagePath(null, 'map', VIEW, 'anon'),
      await imagePath(k, 'map', VIEW, 'full', 281),
      await imagePath(null, 'staged', {}, 'anon'),
      await imagePath(k, 'staged', {}, 'full', 281),
    ]).toEqual([
      '/og/map.png?d=261002&path=marin-a%2Fckpt',
      '/og/map.png?d=261002&path=marin-a%2Fckpt&sig=EZuHk9X_33Kf',
      '/og/staged.png',
      '/og/staged.png?sig=EZeJyGwFNOrU',
    ])
  })
  it('full only for the exact signed view and an unexpired sig; anything else is the anonymous card', async () => {
    const k = await ogKey('s3cret')
    const p = await imagePath(k, 'map', VIEW, 'full', 281)
    const r = (q: string, now = NOW) => resolveImage(k, at(q), now)
    const anon = (params: Record<string, string>, why?: string) => ({ kind: 'map', params, tier: 'anon', ...(why ? { why } : {}) })
    expect([
      await r(p),
      await r(p.replace('path=marin-a%2Fckpt', 'path=marin-a')),
      await r(p.replace('d=261002', 'd=261001')),
      await r(p.replace('sig=EZ', 'sig=Ea')),
      await r(p.replace(/sig=.*/, 'sig=short')),
      await r(p, EPOCH + 282 * 86400),
      await r(p, EPOCH + 282 * 86400 - 1),
      await resolveImage(await ogKey('other'), at(p), NOW),
      await r('/og/map.png?path=marin-a%2Fckpt&d=261002'),
      await resolveImage(k, at('/api/subtree'), NOW),
    ]).toEqual([
      { kind: 'map', params: VIEW, tier: 'full', day: 281 },
      anon({ path: 'marin-a', d: '261002' }, 'bad signature'),
      anon({ path: 'marin-a/ckpt', d: '261001' }, 'bad signature'),
      anon(VIEW, 'bad signature'),
      anon(VIEW, 'bad sig'),
      anon(VIEW, 'expired'),
      { kind: 'map', params: VIEW, tier: 'full', day: 281 },
      anon(VIEW, 'bad signature'),
      anon({ path: 'marin-a/ckpt', d: '261002' }),
      null,
    ])
  })
})

describe('view tokens: 12 chars = expiry + 60-bit tag', () => {
  it('test vector', async () => {
    expect(await mintToken(await ogKey('s3cret'), 'map', VIEW, 281)).toBe('EZjUobkMrUuF')
  })
  it('good for exactly the minted view: not a child, parent, sibling, other params or after expiry', async () => {
    const k = await ogKey('s3cret')
    const tok = await mintToken(k, 'map', VIEW, 281)
    expect([
      await checkToken(k, tok, 'map', VIEW, NOW),
      await checkToken(k, tok, 'map', { ...VIEW, path: 'marin-a/ckpt/run-1' }, NOW),
      await checkToken(k, tok, 'map', { ...VIEW, path: 'marin-a' }, NOW),
      await checkToken(k, tok, 'map', { ...VIEW, path: 'marin-a/tmp' }, NOW),
      await checkToken(k, tok, 'map', { ...VIEW, f: 'x' }, NOW),
      await checkToken(k, tok, 'staged', VIEW, NOW),
      await checkToken(k, tok, 'map', VIEW, EPOCH + 282 * 86400),
      await checkToken(k, `Ea${tok.slice(2)}`, 'map', VIEW, NOW),
      await checkToken(k, `${tok}x`, 'map', VIEW, NOW),
      await checkToken(k, 'garbage', 'map', VIEW, NOW),
    ]).toEqual([{ day: 281 }, null, null, null, null, null, null, null, null, null])
  })
})
