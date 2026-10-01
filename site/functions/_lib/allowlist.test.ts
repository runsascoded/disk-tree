import { describe, expect, it } from 'vitest'
import { onRequest as authApi } from '../api/auth/[[path]]'
import { type Env, gateFor } from './auth'
import { siteAllowlist } from './allowlist'
import { sqliteD1 } from './testD1'

const ORIGIN = 'https://disk.example'
const STAFF = 'ops@runsascoded.com'

async function setup() {
  const { db } = await sqliteD1('cw')
  const env: Env = { DB: db, SESSION_SECRET: 'test-secret', BASE_SCOPE: 'laptop', STAFF_DOMAIN: 'runsascoded.com' }
  return { db, env, store: siteAllowlist(env) }
}

describe('siteAllowlist: the `allowed_emails` table as an `AllowlistStore`', () => {
  it('a row reads as the base scope (and its read-only half); no row is null', async () => {
    const { db, store } = await setup()
    await db.prepare('INSERT INTO allowed_emails (email, note, who, ts) VALUES (?, ?, ?, ?)').bind('a@x.org', 'hi', STAFF, 1).run()
    expect(await store.lookup('a@x.org')).toEqual(['laptop', 'laptop:read'])
    expect(await store.lookup('b@x.org')).toBe(null)
    expect(await store.list()).toEqual([
      { email: 'a@x.org', scopes: ['laptop', 'laptop:read'], source: 'manual', note: 'hi', addedBy: STAFF, updatedAt: 1 },
    ])
  })

  it('removes a row, and refuses a directory sync (the table has no `source`)', async () => {
    const { store } = await setup()
    await store.put({ email: 'c@x.org', scopes: ['laptop'], source: 'manual', note: null, addedBy: null, updatedAt: 5 })
    expect(await store.remove('c@x.org')).toBe(true)
    expect(await store.remove('c@x.org')).toBe(false)
    await expect(store.replaceSource('sync:board@x.org', [])).rejects.toThrow('no `source` column')
  })
})

describe('POST /api/auth/grants with `allowlist: true`', () => {
  it('mints the link and lets its recipient sign in — once; a re-mint reports them already allowed', async () => {
    const { db, env } = await setup()
    const cookie = (await gateFor(env)!.signIn(STAFF, new Request(`${ORIGIN}/signin`)))!.cookie.split(';')[0]
    const mint = async () => {
      const res = await authApi({
        env,
        request: new Request(`${ORIGIN}/api/auth/grants`, {
          method: 'POST',
          headers: { cookie, origin: ORIGIN, 'sec-fetch-site': 'same-origin', 'content-type': 'application/json' },
          body: JSON.stringify({ note: 'for Bo', email: 'Bo@X.org', allowlist: true, scopes: ['laptop:read'] }),
        }),
      } as Parameters<typeof authApi>[0])
      return { status: res.status, allowed: ((await res.json()) as { allowed?: unknown }).allowed }
    }
    expect(await mint()).toEqual({ status: 200, allowed: { email: 'bo@x.org', status: 'added' } })
    expect(await mint()).toEqual({ status: 200, allowed: { email: 'bo@x.org', status: 'already' } })
    expect((await db.prepare('SELECT email, note, who FROM allowed_emails').all()).results).toEqual([
      { email: 'bo@x.org', note: 'with a share link', who: STAFF },
    ])
  })
})
