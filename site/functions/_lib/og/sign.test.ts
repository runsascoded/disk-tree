import { describe, expect, it } from 'vitest'
import { canonical, checkToken, EPOCH, expDay, imagePath, mintToken, ogKey, packDays, unpackDays, verifyImage } from './sign'

// 2026-10-02T12:00Z: day 274 since EPOCH (2026-01-01).
const NOW = EPOCH + 274 * 86400 + 12 * 3600
const at = (p: string) => new URL(`https://site.example.org${p}`)

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

describe('image URLs: one 14-char sig = tier + expiry + 64-bit tag', () => {
  it('test vectors', async () => {
    const k = await ogKey('s3cret')
    expect([
      await imagePath(k, 'map', { path: 'marin-a/ckpt', d: '261002' }, 'anon', 281),
      await imagePath(k, 'map', { path: 'marin-a/ckpt', d: '261002' }, 'full', 281),
      await imagePath(k, 'staged', {}, 'anon', 281),
    ]).toEqual([
      '/og/map.png?d=261002&path=marin-a%2Fckpt&sig=aEZxezPs6HYrnE',
      '/og/map.png?d=261002&path=marin-a%2Fckpt&sig=fEZuHk9X_33KfM',
      '/og/staged.png?sig=aEZuToejm0t5F4',
    ])
  })
  it('verify only the exact signed view, tier and expiry', async () => {
    const k = await ogKey('s3cret')
    const p = await imagePath(k, 'map', { path: 'marin-a/ckpt', d: '261002' }, 'anon', 281)
    const tampered = (from: string, to: string) => verifyImage(k, at(p.replace(from, to)), NOW)
    expect([
      await verifyImage(k, at(p), NOW),
      await tampered('sig=a', 'sig=f'),
      await tampered('path=marin-a%2Fckpt', 'path=marin-a'),
      await tampered('d=261002', 'd=261001'),
      await tampered('/og/map.png', '/og/staged.png'),
      await tampered('sig=aEZ', 'sig=aEa'),
      await tampered('sig=', 'x='),
      await verifyImage(k, at(p), EPOCH + 282 * 86400),
      await verifyImage(k, at(p), EPOCH + 282 * 86400 - 1),
      await verifyImage(await ogKey('other'), at(p), NOW),
    ]).toEqual([
      { kind: 'map', params: { d: '261002', path: 'marin-a/ckpt' }, tier: 'anon', day: 281 },
      { error: 'bad signature' },
      { error: 'bad signature' },
      { error: 'bad signature' },
      { error: 'bad signature' },
      { error: 'bad signature' },
      { error: 'bad sig' },
      { error: 'expired' },
      { kind: 'map', params: { d: '261002', path: 'marin-a/ckpt' }, tier: 'anon', day: 281 },
      { error: 'bad signature' },
    ])
  })
  it('param order on the request does not matter', async () => {
    const k = await ogKey('s3cret')
    const sig = new URL(at(await imagePath(k, 'map', { path: 'a', f: 'tomat' }, 'full', 281))).searchParams.get('sig')
    expect(await verifyImage(k, at(`/og/map.png?sig=${sig}&path=a&f=tomat`), NOW))
      .toEqual({ kind: 'map', params: { path: 'a', f: 'tomat' }, tier: 'full', day: 281 })
  })
})

describe('view tokens: 13 chars = expiry + 64-bit tag', () => {
  it('test vector', async () => {
    expect(await mintToken(await ogKey('s3cret'), 'map', { path: 'marin-a/ckpt', d: '261002' }, 281)).toBe('EZjUobkMrUuF8')
  })
  it('good for exactly the minted view: not a child, parent, sibling, other params or after expiry', async () => {
    const k = await ogKey('s3cret')
    const view = { path: 'marin-a/ckpt', d: '261002' }
    const tok = await mintToken(k, 'map', view, 281)
    expect([
      await checkToken(k, tok, 'map', view, NOW),
      await checkToken(k, tok, 'map', { ...view, path: 'marin-a/ckpt/run-1' }, NOW),
      await checkToken(k, tok, 'map', { ...view, path: 'marin-a' }, NOW),
      await checkToken(k, tok, 'map', { ...view, path: 'marin-a/tmp' }, NOW),
      await checkToken(k, tok, 'map', { ...view, f: 'x' }, NOW),
      await checkToken(k, tok, 'staged', view, NOW),
      await checkToken(k, tok, 'map', view, EPOCH + 282 * 86400),
      await checkToken(k, `Ea${tok.slice(2)}`, 'map', view, NOW),
      await checkToken(k, 'garbage', 'map', view, NOW),
    ]).toEqual([{ day: 281 }, null, null, null, null, null, null, null, null])
  })
})
