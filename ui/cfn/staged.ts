/**
 * The staged-delete data layer over D1 (spec `specs/staged-delete.md`, CP2) —
 * the edge mirror of the CP1 Python engine (`src/disk_tree/staged.py`).
 *
 * A shared open `plan` collects `plan_items` (trash gestures, `view` scope);
 * an admin dispatch (`admin` scope) *enqueues* a real `deletion_run` and closes
 * the plan. The edge can't reach arbitrary user buckets, so it never deletes:
 * an enqueued run (`finished_ts IS NULL`) is drained by the server-side
 * executor (CP4), which sizes each item and lands its `deletion_band`.
 */

export const STAGED = 'Staged'

export interface Plan {
  id: number
  name: string
  note: string | null
  state: 'open' | 'closed'
  created_by: string
  created_ts: number
  closed_ts: number | null
}

export interface PlanItem {
  plan_id: number
  uri: string
  note: string | null
  added_by: string
  added_ts: number
}

export interface DeletionRun {
  run_id: string
  plan_id: number
  mode: 'dry' | 'real'
  actor: string
  started_ts: number
  finished_ts: number | null
  deleted_bytes: number
  deleted_objects: number
  skipped_gone: number
  undo_state: string
  undo_deadline: number | null
}

const nowS = (): number => Math.floor(Date.now() / 1000)

/** URI as the CP1 engine canonicalizes it: no trailing slash (schemes keep
 *  their `//`, since only *trailing* slashes are stripped). */
export const canonicalUri = (raw: string): string => raw.replace(/\/+$/, '')

/** `YYYYMMDDTHHMMSS` (UTC) for a run id, mirroring the Python `strftime`. */
function stamp(ts: number): string {
  const d = new Date(ts * 1000)
  const p = (n: number, w = 2) => String(n).padStart(w, '0')
  return (
    `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}` +
    `T${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}`
  )
}

function hex4(): string {
  const b = new Uint8Array(4)
  crypto.getRandomValues(b)
  return Array.from(b, x => x.toString(16).padStart(2, '0')).join('')
}

/** The shared open plan named `name`, created (empty) if none is open. */
export async function openPlan(db: D1Database, who: string, name: string = STAGED): Promise<Plan> {
  const found = await db
    .prepare(`SELECT * FROM plans WHERE name = ?1 AND state = 'open' ORDER BY id DESC LIMIT 1`)
    .bind(name)
    .first<Plan>()
  if (found) return found
  const ts = nowS()
  const { meta } = await db
    .prepare(`INSERT INTO plans (name, created_by, created_ts) VALUES (?1, ?2, ?3)`)
    .bind(name, who, ts)
    .run()
  return { id: Number(meta.last_row_id), name, note: null, state: 'open', created_by: who, created_ts: ts, closed_ts: null }
}

/** Add `uris` to the shared open plan (idempotent per URI). Returns the plan
 *  and the URIs newly staged. */
export async function stage(
  db: D1Database,
  uris: string[],
  who: string,
  note: string | null = null,
): Promise<{ plan: Plan; added: string[] }> {
  const plan = await openPlan(db, who)
  const have = new Set(
    (await db.prepare(`SELECT uri FROM plan_items WHERE plan_id = ?1`).bind(plan.id).all<{ uri: string }>()).results.map(
      r => r.uri,
    ),
  )
  const ts = nowS()
  const added: string[] = []
  const inserts: D1PreparedStatement[] = []
  for (const raw of uris) {
    const uri = canonicalUri(raw)
    if (have.has(uri)) continue
    have.add(uri)
    added.push(uri)
    inserts.push(
      db
        .prepare(`INSERT INTO plan_items (plan_id, uri, note, added_by, added_ts) VALUES (?1, ?2, ?3, ?4, ?5)`)
        .bind(plan.id, uri, note, who, ts),
    )
  }
  if (inserts.length) await db.batch(inserts)
  return { plan, added }
}

/** Remove `uris` from every open plan. Returns the number removed. */
export async function unstage(db: D1Database, uris: string[]): Promise<number> {
  let removed = 0
  for (const raw of uris) {
    const { meta } = await db
      .prepare(
        `DELETE FROM plan_items WHERE uri = ?1 AND plan_id IN (SELECT id FROM plans WHERE state = 'open')`,
      )
      .bind(canonicalUri(raw))
      .run()
    removed += meta.changes ?? 0
  }
  return removed
}

/** Resolve a plan by id (numeric) or name; `null` ref -> the open `Staged` plan. */
export async function planByRef(db: D1Database, ref: string | null): Promise<Plan | null> {
  if (ref === null) {
    return db
      .prepare(`SELECT * FROM plans WHERE name = ?1 AND state = 'open' ORDER BY id DESC LIMIT 1`)
      .bind(STAGED)
      .first<Plan>()
  }
  if (/^\d+$/.test(ref)) return db.prepare(`SELECT * FROM plans WHERE id = ?1`).bind(Number(ref)).first<Plan>()
  return db.prepare(`SELECT * FROM plans WHERE name = ?1 ORDER BY id DESC LIMIT 1`).bind(ref).first<Plan>()
}

export async function planItems(db: D1Database, planId: number): Promise<PlanItem[]> {
  const { results } = await db
    .prepare(`SELECT * FROM plan_items WHERE plan_id = ?1 ORDER BY uri`)
    .bind(planId)
    .all<PlanItem>()
  return results
}

export async function listOpenPlans(db: D1Database): Promise<Plan[]> {
  const { results } = await db.prepare(`SELECT * FROM plans WHERE state = 'open' ORDER BY id`).all<Plan>()
  return results
}

export async function listRuns(db: D1Database, limit = 10): Promise<DeletionRun[]> {
  const { results } = await db
    .prepare(`SELECT * FROM deletion_runs ORDER BY started_ts DESC LIMIT ?1`)
    .bind(limit)
    .all<DeletionRun>()
  return results
}

/** Dispatch a plan from the edge: enqueue a real run (pending — the executor
 *  drains it) and close the plan so its staged set is frozen. No bands: the
 *  server-side executor produces them with real sizes. */
export async function enqueueDispatch(db: D1Database, plan: Plan, actor: string): Promise<DeletionRun> {
  const ts = nowS()
  const run_id = `${plan.id}-${stamp(ts)}-${hex4()}`
  await db.batch([
    db
      .prepare(
        `INSERT INTO deletion_runs (run_id, plan_id, mode, actor, started_ts) VALUES (?1, ?2, 'real', ?3, ?4)`,
      )
      .bind(run_id, plan.id, actor, ts),
    db.prepare(`UPDATE plans SET state = 'closed', closed_ts = ?2 WHERE id = ?1`).bind(plan.id, ts),
  ])
  return {
    run_id, plan_id: plan.id, mode: 'real', actor, started_ts: ts, finished_ts: null,
    deleted_bytes: 0, deleted_objects: 0, skipped_gone: 0, undo_state: 'none', undo_deadline: null,
  }
}
