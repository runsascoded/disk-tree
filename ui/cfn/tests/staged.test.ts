/** The staged-delete edge layer (spec `specs/staged-delete.md`, CP2): the D1
 *  data layer (`cfn/staged.ts`) directly, and the routes (`cfn/stagedRoutes.ts`)
 *  through the *real* gate over an in-memory D1 — the same scope checks the
 *  middleware runs, so `view` stages and only `admin` dispatches. */
import { beforeEach, describe, expect, it } from 'vitest'
import { migratedD1 } from './fakeD1'
import type { Env } from '../env'
import { VIEW_SCOPE, gateFor } from '../auth'
import * as S from '../staged'
import { getStaged, postDispatch, postStage, postUnstage } from '../stagedRoutes'

const A = 'r2://bucket/a'
const B = 'r2://bucket/b'
const C = 'r2://bucket/c'
const ADMIN = 'ryan@example.test'

let db: D1Database
beforeEach(() => {
  db = migratedD1()
})

// ---- data layer -----------------------------------------------------------

describe('cfn/staged (D1 data layer)', () => {
  it('stages idempotently into one shared open plan', async () => {
    const first = await S.stage(db, [A, B], 'ryan')
    const second = await S.stage(db, [B, C], 'ryan')
    expect(first.plan.id).toBe(second.plan.id)
    expect([first.added, second.added]).toEqual([[A, B], [C]])
    expect((await S.planItems(db, first.plan.id)).map(i => i.uri)).toEqual([A, B, C])
  })

  it('canonicalizes a trailing slash but keeps the scheme', async () => {
    const { added } = await S.stage(db, ['r2://bucket/d/'], 'ryan')
    expect(added).toEqual(['r2://bucket/d'])
  })

  it('unstages from the open plan and counts removals', async () => {
    const { plan } = await S.stage(db, [A, B], 'ryan')
    expect(await S.unstage(db, [A])).toBe(1)
    expect(await S.unstage(db, [A])).toBe(0)
    expect((await S.planItems(db, plan.id)).map(i => i.uri)).toEqual([B])
  })

  it('resolves a plan by id, name, default, and miss', async () => {
    const { plan } = await S.stage(db, [A], 'ryan')
    expect((await S.planByRef(db, null))?.id).toBe(plan.id)
    expect((await S.planByRef(db, String(plan.id)))?.id).toBe(plan.id)
    expect((await S.planByRef(db, 'Staged'))?.id).toBe(plan.id)
    expect(await S.planByRef(db, 'nope')).toBeNull()
  })

  it('enqueues a dispatch: a pending real run, plan closed, no bands', async () => {
    const { plan } = await S.stage(db, [A, B], 'ryan')
    const run = await S.enqueueDispatch(db, plan, ADMIN)
    expect(run.run_id.startsWith(`${plan.id}-`)).toBe(true)
    expect([run.mode, run.finished_ts, run.deleted_bytes, run.actor]).toEqual(['real', null, 0, ADMIN])
    expect((await S.planByRef(db, String(plan.id)))?.state).toBe('closed')
    expect((await S.listOpenPlans(db)).length).toBe(0)
    const bands = await db.prepare('SELECT * FROM deletion_bands WHERE run_id = ?1').bind(run.run_id).all()
    expect(bands.results).toEqual([]) // the server-side executor lands bands, not the edge
    const runs = await S.listRuns(db)
    expect(runs.map(r => [r.run_id, r.mode, r.finished_ts])).toEqual([[run.run_id, 'real', null]])
  })
})

// ---- routes + auth --------------------------------------------------------

const req = () => new Request('https://dt.test/x')

function envOf(open = false): Env {
  const base = { SCANS: undefined, DB: db, SESSION_SECRET: 'test-secret', ALLOWED_EMAILS: ADMIN }
  return (open ? { ...base, PUBLIC_OPEN: '1' } : base) as unknown as Env
}

async function adminCookie(env: Env): Promise<string> {
  const signed = await gateFor(env, req()).gate.signIn(ADMIN, req())
  return signed!.cookie.split(';')[0]
}

async function viewerCookie(env: Env): Promise<string> {
  const gate = gateFor(env, req()).gate
  const { token } = await gate.mint({ name: 'viewer', scopes: [VIEW_SCOPE], createdBy: ADMIN })
  const r = await gate.redeem(token, req())
  if (!r.ok) throw new Error('redeem failed')
  return r.cookie.split(';')[0]
}

function request(cookie: string | null, body?: unknown): Request {
  const headers = new Headers()
  if (cookie) headers.set('cookie', cookie)
  const init: RequestInit = { method: body === undefined ? 'GET' : 'POST', headers }
  if (body !== undefined) {
    headers.set('content-type', 'application/json')
    init.body = JSON.stringify(body)
  }
  return new Request('https://dt.test/api/staged', init)
}

async function read(r: Response): Promise<{ status: number; body: unknown }> {
  const text = await r.text()
  let body: unknown = text
  try {
    body = JSON.parse(text)
  } catch {
    /* not JSON */
  }
  return { status: r.status, body }
}

describe('staged routes (real gate)', () => {
  it('refuses an anonymous stage/dispatch', async () => {
    const env = envOf()
    expect(await read(await postStage(env, request(null, { uris: [A] })))).toEqual({ status: 401, body: { error: 'unauthenticated' } })
    expect(await read(await postDispatch(env, request(null, {})))).toEqual({ status: 401, body: { error: 'unauthenticated' } })
  })

  it('lets a `view` session stage and read back the staged set', async () => {
    const env = envOf()
    const cookie = await viewerCookie(env)
    expect(await read(await postStage(env, request(cookie, { uris: [A, B] })))).toEqual({ status: 200, body: { plan_id: 1, added: [A, B] } })

    const listed = await read(await getStaged(env))
    expect(listed.status).toBe(200)
    const { plans, runs } = listed.body as { plans: { id: number; state: string; items: string[]; created_by: string }[]; runs: unknown[] }
    expect(plans.map(p => [p.id, p.state, p.items])).toEqual([[1, 'open', [A, B]]])
    expect(plans[0].created_by).toBe('viewer')
    expect(runs).toEqual([])
  })

  it('rejects a stage with no `uris`', async () => {
    const env = envOf()
    const cookie = await viewerCookie(env)
    expect(await read(await postStage(env, request(cookie, { note: 'x' })))).toEqual({ status: 400, body: { error: '`uris` must be a non-empty array of strings' } })
  })

  it('lets a `view` session unstage', async () => {
    const env = envOf()
    const cookie = await viewerCookie(env)
    await postStage(env, request(cookie, { uris: [A, B] }))
    expect(await read(await postUnstage(env, request(cookie, { uris: [A] })))).toEqual({ status: 200, body: { removed: 1 } })
    const listed = (await read(await getStaged(env))).body as { plans: { items: string[] }[] }
    expect(listed.plans.map(p => p.items)).toEqual([[B]])
  })

  it('forbids `view` from dispatching, admits `admin`', async () => {
    const env = envOf()
    const viewer = await viewerCookie(env)
    await postStage(env, request(viewer, { uris: [A, B] }))

    expect(await read(await postDispatch(env, request(viewer, {})))).toEqual({ status: 403, body: { error: 'forbidden' } })

    const admin = await adminCookie(env)
    const dispatched = await read(await postDispatch(env, request(admin, {})))
    expect(dispatched.status).toBe(200)
    const d = dispatched.body as { run_id: string; plan_id: number; mode: string; items: number; state: string }
    expect([d.plan_id, d.mode, d.items, d.state]).toEqual([1, 'real', 2, 'enqueued'])
    expect(d.run_id.startsWith('1-')).toBe(true)

    // plan closed, run recorded (pending), staged set now empty
    const after = await read(await getStaged(env))
    const ab = after.body as { plans: unknown[]; runs: { run_id: string; mode: string; finished_ts: number | null; actor: string }[] }
    expect(ab.plans).toEqual([])
    expect(ab.runs.map(r => [r.run_id, r.mode, r.finished_ts, r.actor])).toEqual([[d.run_id, 'real', null, ADMIN]])
  })

  it('refuses a dispatch of an empty / missing plan', async () => {
    const env = envOf()
    const admin = await adminCookie(env)
    expect(await read(await postDispatch(env, request(admin, {})))).toEqual({ status: 404, body: { error: 'no plan (open Staged)' } })
    expect(await read(await postDispatch(env, request(admin, { plan: '999' })))).toEqual({ status: 404, body: { error: 'no plan 999' } })
  })

  it('is unavailable on the open (unauthenticated) demo', async () => {
    const env = envOf(true)
    const msg = 'staged delete is unavailable on the open (unauthenticated) demo'
    expect(await read(await getStaged(env))).toEqual({ status: 501, body: { error: msg } })
    expect(await read(await postStage(env, request(null, { uris: [A] })))).toEqual({ status: 501, body: { error: msg } })
  })
})
