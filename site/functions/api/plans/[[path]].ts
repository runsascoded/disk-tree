// /api/plans[/:id[/items]] — CRUD for first-class deletion plans
// (specs/sweep-plan-union.md, seam 1).
//
//   GET    /api/plans              list plans (+ item / run counts)         viewer
//   POST   /api/plans              { name, note? } -> { id }                admin
//   GET    /api/plans/:id          plan + items + runs                      viewer
//   PATCH  /api/plans/:id          { state: 'closed' }                      admin
//   POST   /api/plans/:id/items    { prefixes: [...], note? }               admin
//   DELETE /api/plans/:id/items    { prefixes: [...] }                      admin
//   POST   /api/plans/stage        { prefixes: [...], note? } -> { plan_id } viewer
//
// Reads and staging (the opt-in trash proposal) are open to any authenticated
// viewer; curating a specific plan's items and closing plans require admin. Items are
// editable only while the plan is `open`. gcs adaptation of cw's handler:
// gs:// prefixes, gcs auth helpers, `admin_edits` audit; a plan may span buckets.
import type { D1Database } from '@cloudflare/workers-types'
import { type Ctx, type Env as AuthEnv, json, requireAdmin, requireStager, requireViewer } from '../../_lib/auth.js'
import { audit, canonicalPrefix, stageItems, type PlanRow } from '../../_lib/plans.js'

type Env = AuthEnv & { DB?: D1Database }

const now = (): number => Math.floor(Date.now() / 1000)

async function readBody(req: Request): Promise<Record<string, unknown>> {
  return ((await req.json().catch(() => null)) as Record<string, unknown> | null) ?? {}
}

async function listPlans(db: D1Database): Promise<Response> {
  const { results } = await db.prepare(`
    SELECT p.*,
      (SELECT count(*) FROM plan_items i WHERE i.plan_id = p.id) AS items,
      (SELECT count(*) FROM deletion_runs r WHERE r.plan_id = p.id) AS runs
    FROM plans p ORDER BY p.created_ts DESC
  `).all()
  return json({ plans: results })
}

async function getPlan(db: D1Database, id: number): Promise<Response> {
  const plan = await db.prepare('SELECT * FROM plans WHERE id = ?').bind(id).first<PlanRow>()
  if (!plan) return json({ error: 'no such plan' }, 404)
  // The memo lives on the item's stage batch (one gesture, one note) — join it
  // back so a staged item carries the reason it was trashed.
  const items = await db.prepare(
    `SELECT i.prefix, i.added_by, i.added_ts, i.batch_id, b.note
     FROM plan_items i LEFT JOIN stage_batches b ON b.id = i.batch_id
     WHERE i.plan_id = ? ORDER BY i.prefix`,
  ).bind(id).all()
  const runs = await db.prepare('SELECT * FROM deletion_runs WHERE plan_id = ? ORDER BY started_ts DESC').bind(id).all()
  return json({ plan, items: items.results, runs: runs.results })
}

async function createPlan(db: D1Database, who: string, body: Record<string, unknown>): Promise<Response> {
  const name = typeof body.name === 'string' ? body.name.trim() : ''
  if (!name) return json({ error: 'name required' }, 400)
  const note = typeof body.note === 'string' ? body.note : null
  const row = await db.prepare(
    "INSERT INTO plans (name, note, state, created_by, created_ts) VALUES (?, ?, 'open', ?, ?) RETURNING id",
  ).bind(name, note, who, now()).first<{ id: number }>()
  const id = row!.id
  await audit(db, 'plans', String(id), 'insert', who, null, { name, note })
  return json({ id }, 201)
}

async function closePlan(db: D1Database, who: string, id: number, body: Record<string, unknown>): Promise<Response> {
  if (body.state !== 'closed') return json({ error: "only { state: 'closed' } is supported" }, 400)
  const plan = await db.prepare('SELECT * FROM plans WHERE id = ?').bind(id).first<PlanRow>()
  if (!plan) return json({ error: 'no such plan' }, 404)
  await db.prepare("UPDATE plans SET state = 'closed', closed_ts = ? WHERE id = ?").bind(now(), id).run()
  await audit(db, 'plans', String(id), 'update', who, { state: plan.state }, { state: 'closed' })
  return json({ id, state: 'closed' })
}

async function editItems(
  db: D1Database, who: string, id: number, add: boolean, body: Record<string, unknown>,
): Promise<Response> {
  const plan = await db.prepare('SELECT * FROM plans WHERE id = ?').bind(id).first<PlanRow>()
  if (!plan) return json({ error: 'no such plan' }, 404)
  if (plan.state !== 'open') return json({ error: 'plan is closed; reopen or make a new plan' }, 409)
  const raw = Array.isArray(body.prefixes) ? body.prefixes : []
  if (!raw.length) return json({ error: 'prefixes required' }, 400)
  const prefixes: string[] = []
  for (const r of raw) {
    const c = typeof r === 'string' ? canonicalPrefix(r) : null
    if (!c) return json({ error: `bad prefix ${JSON.stringify(r)}` }, 400)
    prefixes.push(c)
  }
  const note = typeof body.note === 'string' ? body.note : null
  const ts = now()
  if (add) {
    for (const p of prefixes) {
      await db.prepare(
        'INSERT OR IGNORE INTO plan_items (plan_id, prefix, note, added_by, added_ts) VALUES (?, ?, ?, ?, ?)',
      ).bind(id, p, note, who, ts).run()
    }
    await audit(db, 'plan_items', String(id), 'insert', who, null, { prefixes })
  } else {
    for (const p of prefixes) {
      await db.prepare('DELETE FROM plan_items WHERE plan_id = ? AND prefix = ?').bind(id, p).run()
    }
    await audit(db, 'plan_items', String(id), 'delete', who, { prefixes }, null)
  }
  return json({ id, [add ? 'added' : 'removed']: prefixes })
}

const who = (id: { email?: string | null; name?: string | null }): string => id.email ?? id.name ?? 'guest'

export const onRequest = async (ctx: Ctx & { env: Env }): Promise<Response> => {
  if (!ctx.env.DB) return json({ error: 'plans store not configured (no D1 binding)' }, 503)
  const db = ctx.env.DB
  const segs = new URL(ctx.request.url).pathname.replace(/^\/api\/plans\/?/, '').split('/').filter(Boolean)
  const method = ctx.request.method

  // /api/plans
  if (segs.length === 0) {
    if (method === 'GET') {
      const gated = await requireViewer(ctx)
      return gated instanceof Response ? gated : listPlans(db)
    }
    if (method === 'POST') {
      const gated = await requireAdmin(ctx)
      return gated instanceof Response ? gated : createPlan(db, who(gated), await readBody(ctx.request))
    }
    return json({ error: 'method not allowed' }, 405)
  }

  // /api/plans/stage — the opt-in trash proposal; any viewer may stage.
  if (segs.length === 1 && segs[0] === 'stage' && method === 'POST') {
    // Staging needs the full base scope; read-only guest links can't propose. 
    const gated = await requireStager(ctx)
    if (gated instanceof Response) return gated
    const body = await readBody(ctx.request)
    const prefixes = Array.isArray(body.prefixes)
      ? (body.prefixes as unknown[]).filter((x): x is string => typeof x === 'string')
      : []
    const note = typeof body.note === 'string' ? body.note : null
    const res = await stageItems(db, prefixes, who(gated), note)
    return 'error' in res ? json(res, 400) : json(res, 201)
  }

  const id = Number(segs[0])
  if (!Number.isInteger(id) || id <= 0) return json({ error: 'bad plan id' }, 400)

  // /api/plans/:id
  if (segs.length === 1) {
    if (method === 'GET') {
      const gated = await requireViewer(ctx)
      return gated instanceof Response ? gated : getPlan(db, id)
    }
    if (method === 'PATCH') {
      const gated = await requireAdmin(ctx)
      return gated instanceof Response ? gated : closePlan(db, who(gated), id, await readBody(ctx.request))
    }
    return json({ error: 'method not allowed' }, 405)
  }

  // /api/plans/:id/items
  if (segs.length === 2 && segs[1] === 'items' && (method === 'POST' || method === 'DELETE')) {
    const gated = await requireAdmin(ctx)
    return gated instanceof Response ? gated : editItems(db, who(gated), id, method === 'POST', await readBody(ctx.request))
  }

  return json({ error: 'not found' }, 404)
}
