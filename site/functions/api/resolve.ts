/**
 * Resolve one path's effective keep + owner, with provenance
 * (specs/actions-ledger.md § Resolution). The single source of truth the UI,
 * CLI and agents all consume — no client re-implements the recency fold.
 *
 *   GET /api/resolve?path=gs://marin-<bucket>/<dir>/
 *     → { path, keep: {action, prefix, own, who, ts, memo} | null,
 *              owner: {owner, prefix, own, who, ts, memo} | null }
 *
 * `null` on an axis = unmarked (keep defaults to swept at the deadline; owner
 * defaults to unattributed). `own` = the winning row sits exactly on `path`
 * rather than being inherited from an ancestor. A winning row whose value is a
 * clear (NULL) resolves to `null` — an explicit un-mark, newest, wins.
 */
import { type Ctx, json, requireScope, requireViewer } from '../_lib/auth.js'
import { ancestorPrefixes, PREFIX_RE } from '../_lib/resolve.js'

interface KeepHit { prefix: string; keep: string | null; ts: number; who: string; memo: string | null }
interface OwnerHit { prefix: string; owner: string | null; ts: number; who: string; memo: string | null }

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const { request, env } = ctx
  if (!env.DB) return json({ error: 'resolve backend not configured (DB)' }, 503)
  const id = await requireViewer(ctx)
  if (id instanceof Response) return id

  const path = new URL(request.url).searchParams.get('path') ?? ''
  if (!PREFIX_RE.test(path) || path.length > 1024) {
    return json({ error: 'path must be gs://marin-<bucket>/<dir>/ (trailing slash)' }, 400)
  }
  const anc = ancestorPrefixes(path)
  const ph = anc.map(() => '?').join(',')

  const [keep, owner] = await Promise.all([
    env.DB.prepare(
      `SELECT k.prefix, k.keep, k.ts, a.actor AS who, a.memo
         FROM keep_prefixes k JOIN actions a ON a.id = k.action_id
        WHERE k.prefix IN (${ph}) AND k.tombstoned IS NULL
        ORDER BY k.ts DESC, a.id DESC LIMIT 1`,
    ).bind(...anc).first<KeepHit>(),
    env.DB.prepare(
      `SELECT o.prefix, o.owner, o.ts, a.actor AS who, a.memo
         FROM owner_prefixes o JOIN actions a ON a.id = o.action_id
        WHERE o.prefix IN (${ph}) AND o.tombstoned IS NULL
        ORDER BY o.ts DESC, a.id DESC LIMIT 1`,
    ).bind(...anc).first<OwnerHit>(),
  ])

  return json({
    path,
    keep: keep && keep.keep != null
      ? { action: keep.keep, prefix: keep.prefix, own: keep.prefix === path, who: keep.who, ts: keep.ts, memo: keep.memo }
      : null,
    owner: owner && owner.owner != null
      ? { owner: owner.owner, prefix: owner.prefix, own: owner.prefix === path, who: owner.who, ts: owner.ts, memo: owner.memo }
      : null,
  })
}
