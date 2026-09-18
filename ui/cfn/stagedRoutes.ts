/**
 * The staged-delete write + read routes (spec `specs/staged-delete.md`, CP2),
 * as plain `(env, request) -> Response` handlers so the Functions stay thin and
 * the tests drive the real gate over an in-memory D1.
 *
 * The `/api/*` middleware already enforced a `view` session; these re-derive the
 * identity to record an actor, and `dispatch` additionally requires `admin`.
 * Unavailable on the open (unauthenticated) demo — there is no identity to
 * attribute a deletion to.
 */
import { hasScope } from '@open-athena/auth'
import { ADMIN_SCOPE, actorLabel, authFor } from './auth'
import { type Env, isOpen } from './env'
import { error, json } from './http'
import {
  type Plan,
  enqueueDispatch,
  listOpenPlans,
  listRuns,
  planByRef,
  planItems,
  stage,
  unstage,
} from './staged'

const OPEN_MSG = 'staged delete is unavailable on the open (unauthenticated) demo'

/** The `view` identity behind the (already-gated) request, or an error Response. */
async function actor(env: Env, request: Request): Promise<string | Response> {
  if (isOpen(env) || !env.DB) return error(OPEN_MSG, 501)
  const auth = await authFor(env, request)
  if (!auth) return error('unauthenticated', 401)
  return actorLabel(auth)
}

async function body(request: Request): Promise<Record<string, unknown> | Response> {
  try {
    const b = await request.json()
    if (b && typeof b === 'object' && !Array.isArray(b)) return b as Record<string, unknown>
  } catch {
    /* fall through */
  }
  return error('expected a JSON object body', 400)
}

/** The non-empty `uris` string array of a parsed body, or an error Response. */
function uris(b: Record<string, unknown>): string[] | Response {
  const list = b.uris
  if (!Array.isArray(list) || list.length === 0 || list.some(u => typeof u !== 'string')) {
    return error('`uris` must be a non-empty array of strings', 400)
  }
  return list as string[]
}

/** `GET /api/staged` — open plans (with their staged URIs) + the recent runs feed. */
export async function getStaged(env: Env): Promise<Response> {
  if (isOpen(env) || !env.DB) return error(OPEN_MSG, 501)
  const db = env.DB
  const plans = await listOpenPlans(db)
  const withItems = await Promise.all(
    plans.map(async p => ({ ...p, items: (await planItems(db, p.id)).map(i => i.uri) })),
  )
  return json({ plans: withItems, runs: await listRuns(db) }, { maxAge: 0 })
}

/** `POST /api/plans/stage` {uris, note?} — stage URIs into the shared open plan (view). */
export async function postStage(env: Env, request: Request): Promise<Response> {
  const who = await actor(env, request)
  if (who instanceof Response) return who
  const b = await body(request)
  if (b instanceof Response) return b
  const list = uris(b)
  if (list instanceof Response) return list
  const note = typeof b.note === 'string' ? b.note : null
  const { plan, added } = await stage(env.DB!, list, who, note)
  return json({ plan_id: plan.id, added }, { maxAge: 0 })
}

/** `POST /api/plans/unstage` {uris} — remove URIs from every open plan (view). */
export async function postUnstage(env: Env, request: Request): Promise<Response> {
  const who = await actor(env, request)
  if (who instanceof Response) return who
  const b = await body(request)
  if (b instanceof Response) return b
  const list = uris(b)
  if (list instanceof Response) return list
  return json({ removed: await unstage(env.DB!, list) }, { maxAge: 0 })
}

/** `POST /api/dispatch` {plan?} — enqueue a plan for the server-side executor
 *  and close it (admin). The edge records intent; it never deletes. */
export async function postDispatch(env: Env, request: Request): Promise<Response> {
  const who = await actor(env, request)
  if (who instanceof Response) return who
  const auth = await authFor(env, request)
  if (!auth || !hasScope(auth, ADMIN_SCOPE)) return error('forbidden', 403)
  // The plan ref is optional (default the open Staged plan), so a missing/empty
  // body is fine — only a present-but-malformed body is an error.
  let ref: string | null = null
  const raw = (await request.text()).trim()
  if (raw) {
    try {
      ref = ((JSON.parse(raw) as { plan?: string }).plan) ?? null
    } catch {
      return error('expected a JSON object body', 400)
    }
  }
  const plan: Plan | null = await planByRef(env.DB!, ref)
  if (!plan) return error(`no plan ${ref ?? '(open Staged)'}`, 404)
  if (plan.state !== 'open') return error(`plan ${plan.id} is already ${plan.state}`, 409)
  const its = await planItems(env.DB!, plan.id)
  if (its.length === 0) return error(`plan ${plan.id} has no staged items`, 400)
  const run = await enqueueDispatch(env.DB!, plan, who)
  return json(
    { run_id: run.run_id, plan_id: plan.id, mode: run.mode, items: its.length, state: 'enqueued' },
    { maxAge: 0 },
  )
}
