import { describe, expect, it } from 'vitest'
import { ADMIN_SCOPE, baseScope, requireScope, type Ctx, type Env } from './auth'

// A non-localhost request, so identify() doesn't take the dev short-circuit
// (which would return a full-scope admin regardless of PUBLIC_READ).
const ctxOf = (env: Partial<Env>): Ctx => ({
  request: new Request('https://r2.rbw.sh/api/subtree'),
  env: env as Env,
})

describe('requireScope — PUBLIC_READ (public/no-gate deploys)', () => {
  it('grants the base viewer scope anonymously — reads open', async () => {
    const env: Partial<Env> = { PUBLIC_READ: '1', BASE_SCOPE: 'r2' }
    const id = await requireScope(ctxOf(env), baseScope(env as Env))
    expect(id).toEqual({ email: null, name: null, scopes: ['r2'], admin: false, via: 'public' })
  })

  it('still gates a non-base scope — mutations/admin stay closed', async () => {
    const res = await requireScope(ctxOf({ PUBLIC_READ: '1', BASE_SCOPE: 'r2' }), ADMIN_SCOPE)
    expect(res).toBeInstanceOf(Response)
    expect((res as Response).status).toBe(401)
  })

  it('without PUBLIC_READ, the base scope is closed by default', async () => {
    const env: Partial<Env> = { BASE_SCOPE: 'r2' }
    const res = await requireScope(ctxOf(env), baseScope(env as Env))
    expect(res).toBeInstanceOf(Response)
    expect((res as Response).status).toBe(401)
  })
})
