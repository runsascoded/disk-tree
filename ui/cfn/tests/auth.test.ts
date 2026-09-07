/** The gate over `/api/*` (spec `specs/done/pages-auth.md`), end to end through
 *  `gateApi` over the package's in-memory stores — the same code the
 *  middleware runs against D1. */
import { beforeEach, describe, expect, it } from 'vitest'
import { memoryAudit, memoryGrantStore } from '@open-athena/auth/testing'
import type { Gate } from '@open-athena/auth'
import { ADMIN_SCOPE, COOKIE, VIEW_SCOPE, allowlistPolicy, buildGate, gateApi } from '../auth'

const ADMIN = 'ryan@example.test'
let gate: Gate

beforeEach(() => {
  gate = buildGate({
    store: memoryGrantStore(),
    audit: memoryAudit(),
    secret: 'test-secret',
    policy: allowlistPolicy(`${ADMIN}, other@example.test`),
  })
})

const DATA = 'the scans'
const next = async () => new Response(DATA, { status: 200, headers: { 'cache-control': 'public, max-age=60' } })

async function call(path: string, init: RequestInit & { cookie?: string } = {}) {
  const { cookie, ...rest } = init
  const headers = new Headers(rest.headers)
  if (cookie) headers.set('cookie', cookie)
  if (rest.body) headers.set('content-type', 'application/json')
  const res = await gateApi(gate, new Request(`https://disk-tree.test${path}`, { ...rest, headers }), next)
  const text = await res.text()
  let body: unknown = text
  try { body = JSON.parse(text) } catch { /* not JSON */ }
  return { status: res.status, body, cookie: res.headers.get('set-cookie')?.split(';')[0] ?? null, cache: res.headers.get('cache-control') }
}

const post = (path: string, body: unknown, cookie?: string) => call(path, { method: 'POST', body: JSON.stringify(body), cookie })

/** An allowlisted SSO session's cookie, as `/auth/sso` would set it. */
async function ssoCookie(email: string): Promise<string | null> {
  const signed = await gate.signIn(email, new Request('https://disk-tree.test/auth/sso'))
  return signed ? signed.cookie.split(';')[0] : null
}

describe('anonymous', () => {
  it('is refused on the data, passed on the public paths', async () => {
    expect(await call('/api/scans')).toEqual({ status: 401, body: { error: 'unauthenticated' }, cookie: null, cache: 'no-store' })
    expect(await call('/api/scan?uri=/Users/ryan')).toEqual({ status: 401, body: { error: 'unauthenticated' }, cookie: null, cache: 'no-store' })
    expect(await call('/api/capabilities')).toEqual({ status: 200, body: DATA, cookie: null, cache: 'public, max-age=60' })
    expect(await call('/')).toEqual({ status: 200, body: DATA, cookie: null, cache: 'public, max-age=60' })
    expect(await call('/api/auth/whoami')).toEqual({ status: 401, body: { error: 'unauthenticated' }, cookie: null, cache: 'no-store' })
  })
})

describe('share links', () => {
  it('a minted link exchanges into a view session; revoking it ends the session', async () => {
    const { grant, token } = await gate.mint({ name: 'bob', scopes: [VIEW_SCOPE], createdBy: ADMIN })

    const ex = await post('/api/auth/exchange', { token })
    expect(ex.status).toBe(200)
    expect(ex.cookie).toMatch(new RegExp(`^${COOKIE}=`))
    expect(ex.body).toEqual({
      kind: 'grant', id: grant.id, name: 'bob', subject: null, email: null, scopes: [VIEW_SCOPE], admin: false, expiresAt: null,
    })
    const cookie = ex.cookie!

    expect(await call('/api/scans', { cookie })).toEqual({ status: 200, body: DATA, cookie: null, cache: 'no-store' })
    // `view` can't mint or read the ledger.
    expect(await post('/api/auth/grants', { name: 'x', scopes: [VIEW_SCOPE] }, cookie)).toEqual({ status: 403, body: { error: 'forbidden' }, cookie: null, cache: 'no-store' })
    expect(await call('/api/auth/log', { cookie })).toEqual({ status: 403, body: { error: 'forbidden' }, cookie: null, cache: 'no-store' })

    expect(await gate.revoke(grant.id)).toBe(true)
    expect(await call('/api/scans', { cookie })).toEqual({ status: 401, body: { error: 'unauthenticated' }, cookie: null, cache: 'no-store' })
  })

  it('a bad token is refused', async () => {
    expect(await post('/api/auth/exchange', { token: 'nope' })).toEqual({ status: 401, body: { error: 'invalid link', reason: 'bad-token' }, cookie: null, cache: 'no-store' })
  })
})

describe('SSO', () => {
  it('admits allowlisted emails as admins, refuses the rest', async () => {
    expect(await ssoCookie('stranger@example.test')).toBeNull()
    const cookie = (await ssoCookie(ADMIN))!
    expect(cookie).toMatch(new RegExp(`^${COOKIE}=`))
    expect(await call('/api/auth/whoami', { cookie })).toEqual({
      status: 200, body: { kind: 'sso', email: ADMIN, admin: false, scopes: [VIEW_SCOPE, ADMIN_SCOPE] }, cookie: null, cache: 'no-store',
    })
    expect(await call('/api/scans', { cookie })).toEqual({ status: 200, body: DATA, cookie: null, cache: 'no-store' })
  })

  it('an admin mints, lists, and revokes links through the routes', async () => {
    const cookie = (await ssoCookie(ADMIN))!
    const minted = await post('/api/auth/grants', { name: 'carol', scopes: [VIEW_SCOPE], expiresInS: 86_400, maxRedeems: 2 }, cookie)
    expect(minted.status).toBe(200)
    const { grant, token } = minted.body as { grant: Record<string, unknown>; token: string }
    expect(typeof token).toBe('string')
    const norm = (g: Record<string, unknown>) => ({ ...g, id: '<id>', createdAt: '<ts>', expiresAt: '<ts>' })
    expect(norm(grant)).toEqual({
      id: '<id>', name: 'carol', note: null, subject: null, email: null, scopes: [VIEW_SCOPE], maxRedeems: 2, redeems: 0,
      expiresAt: '<ts>', sessionTtlS: null, createdAt: '<ts>', createdBy: ADMIN, disabledAt: null, revokedAt: null,
      expiryEndsSessions: true, firstUsedAt: null, lastUsedAt: null,
    })

    const listed = await call('/api/auth/grants', { cookie })
    expect((listed.body as { grants: Record<string, unknown>[] }).grants.map(norm)).toEqual([norm(grant)])

    // The recipient opens it; the ledger counts the redemption.
    const ex = await post('/api/auth/exchange', { token })
    expect(ex.status).toBe(200)
    const after = await call('/api/auth/grants', { cookie })
    expect((after.body as { grants: { redeems: number; firstUsedAt: number | null }[] }).grants.map(g => [g.redeems, g.firstUsedAt != null])).toEqual([[1, true]])

    expect(await post(`/api/auth/grants/${grant.id}/revoke`, {}, cookie)).toEqual({ status: 200, body: { ok: true }, cookie: null, cache: 'no-store' })
    expect(await call('/api/scans', { cookie: ex.cookie! })).toEqual({ status: 401, body: { error: 'unauthenticated' }, cookie: null, cache: 'no-store' })
  })
})
