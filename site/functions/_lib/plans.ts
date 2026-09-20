// Plans store (specs/cw-sweep.md): the first-class deletion plan. An admin
// curates `sweep`-marked prefixes into a named plan; a dispatch snapshots the
// plan's items into the plan.json the Batch executor consumes. D1 CRUD +
// snapshot + the admin-edit audit trail live here; the HTTP surface is
// api/plans/[[path]].ts.
import type { D1Database } from "@cloudflare/workers-types"
import { CW_BUCKET, CW_BUCKETS } from "./cwBatch.js"

// A plan prefix stored as `s3://<bucket>/<path>/` (matching the marks convention);
// normalized to a relative key prefix only at snapshot time.
const PREFIX_RE = /^(?!\/)(?![.]{1,2}\/)[^\\]+\/$/

/** The bucket a raw prefix names — `s3://<b>/…` or `<b>/…` for a scanned
 * bucket — else the primary. The treemap's paths start with the bucket, so
 * a mark or plan item under `hero-checkpoints/…` must not canonicalize under
 * the primary (specs/cw-multi-bucket.md §4). */
export function bucketOf(raw: string, buckets: readonly string[] = CW_BUCKETS): string {
  const s = raw.trim().replace(/^s3:\/\//, "").replace(/^\/+/, "")
  return buckets.find(b => s === b || s.startsWith(`${b}/`)) ?? buckets[0]
}

export interface PlanRow {
  id: number
  name: string
  note: string | null
  state: "open" | "closed"
  created_by: string
  created_ts: number
  closed_ts: number | null
}

/** `s3://bucket/a/b/` or `/a/b` or `a/b` -> `a/b/` (relative, trailing slash). */
export function relPrefix(raw: string, bucket: string = CW_BUCKET): string {
  let s = raw.trim().replace(/^s3:\/\//, "")
  if (s.startsWith(`${bucket}/`)) s = s.slice(bucket.length + 1)
  s = s.replace(/^\/+/, "")
  if (!s.endsWith("/")) s += "/"
  return s
}

/** Canonical stored form of a plan-item prefix: `s3://<bucket>/<path>/`, the
 * bucket resolved from the raw (`bucketOf`) unless given. */
export function canonicalPrefix(raw: string, bucket: string = bucketOf(raw)): string | null {
  const rel = relPrefix(raw, bucket)
  if (!PREFIX_RE.test(rel)) return null
  return `s3://${bucket}/${rel}`
}

/** A plan whose items name more than one bucket: the executor runs against one
 * bucket per run, so dispatch refuses it (400) rather than guessing. */
export class PlanSpansBuckets extends Error {
  constructor(public readonly buckets: string[]) {
    super(`plan spans buckets: ${buckets.join(", ")}`)
  }
}

/** The one bucket a plan's (canonical) item prefixes live in, and the items
 * relative to it; a plan with no items is the primary's. */
export function planBucket(prefixes: string[]): { bucket: string; sweep: string[] } {
  const buckets = [...new Set(prefixes.map(p => bucketOf(p)))]
  if (buckets.length > 1) throw new PlanSpansBuckets(buckets)
  const bucket = buckets[0] ?? CW_BUCKET
  return { bucket, sweep: prefixes.map(p => relPrefix(p, bucket)) }
}

export async function audit(
  db: D1Database,
  tbl: string,
  pk: string,
  action: "insert" | "update" | "delete",
  who: string,
  oldJson: unknown,
  newJson: unknown,
): Promise<void> {
  await db
    .prepare("INSERT INTO admin_edits (tbl, pk, action, who, ts, old_json, new_json) VALUES (?, ?, ?, ?, ?, ?, ?)")
    .bind(tbl, pk, action, who, Math.floor(Date.now() / 1000),
      oldJson == null ? null : JSON.stringify(oldJson),
      newJson == null ? null : JSON.stringify(newJson))
    .run()
}

/** Snapshot a plan into the executor's plan.json: the plan's one bucket
 * (`planBucket`; throws `PlanSpansBuckets`), relative sweep prefixes (the
 * plan's items) + relative keep prefixes (the current keep/keep_last_ckpt marks
 * in that bucket, which carve out at manifest time). Returns null if the plan
 * is missing. */
export async function snapshotPlan(db: D1Database, planId: number): Promise<
  { plan_id: number; name: string; bucket: string; sweep: string[]; keep: string[] } | null
> {
  const plan = await db.prepare("SELECT id, name FROM plans WHERE id = ?").bind(planId).first<{ id: number; name: string }>()
  if (!plan) return null
  const items = await db.prepare("SELECT prefix FROM plan_items WHERE plan_id = ? ORDER BY prefix").bind(planId).all<{ prefix: string }>()
  const keeps = await db.prepare("SELECT prefix FROM marks WHERE keep IN ('keep', 'keep_last_ckpt') ORDER BY prefix").all<{ prefix: string }>()
  const { bucket, sweep } = planBucket(items.results.map(r => r.prefix))
  return {
    plan_id: planId,
    name: plan.name,
    bucket,
    sweep,
    keep: keeps.results.map(r => r.prefix).filter(p => bucketOf(p) === bucket).map(p => relPrefix(p, bucket)),
  }
}
