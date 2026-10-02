import { describe, expect, it } from 'vitest'
import { warmUrls } from './warm'

describe('warmUrls: the first-paint reads a card view would make', () => {
  it('map: depth=1 then the full view; a filter paints full at once', () => {
    expect([
      warmUrls('map', {}, '2026-10-02'),
      warmUrls('map', { path: 'marin-a/ckpt', o: 'will', cl: '34' }, '2026-10-02', u => `${u}-held`),
      warmUrls('map', { path: 'marin-a', f: 'tomat', qs: 'simple', o: 'unowned' }, '2026-10-02'),
      warmUrls('user', { id: 'will-held' }, '2026-10-02'),
      warmUrls('staged', {}, '2026-10-02'),
    ]).toEqual([
      ['/api/subtree?date=2026-10-02&path=&w=1536&h=922&depth=1', '/api/subtree?date=2026-10-02&path=&w=1536&h=922'],
      ['/api/subtree?date=2026-10-02&path=marin-a%2Fckpt&w=1536&h=922&lens=user%3Awill-held&cl=34&depth=1', '/api/subtree?date=2026-10-02&path=marin-a%2Fckpt&w=1536&h=922&lens=user%3Awill-held&cl=34'],
      ['/api/subtree?date=2026-10-02&path=marin-a&w=1536&h=922&o=unowned&q=tomat&qs=simple&full=1'],
      ['/api/subtree?date=2026-10-02&path=&w=1536&h=922&lens=user%3Awill-held&depth=1', '/api/subtree?date=2026-10-02&path=&w=1536&h=922&lens=user%3Awill-held'],
      [],
    ])
  })
})
