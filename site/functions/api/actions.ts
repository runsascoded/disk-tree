/**
 * Actions ledger (specs/actions-ledger.md): attribution + keep/sweep as one
 * append-only WAL.
 *
 *   GET  /api/actions          → { keeps: [...], owners: [...] } — the live
 *                                expanded rows joined to their raw action's
 *                                provenance; the client folds them
 *                                (most-recent-wins per axis) over the tree.
 *   POST /api/actions          → append one action (or an array of them):
 *                                { pattern, keep?, set_keep?, owner?,
 *                                  set_owner?, memo?, scan? }. Prefix
 *                                patterns only for now (regex expansion is
 *                                the next arc). Requires an email-bearing
 *                                identity — guest grants are read-only.
 *
 * Anyone with the `gcs` scope can read; the sweep's review gate is where
 * authority gets applied — the ledger keeps the full who-did-what trail.
 */
import { type Ctx, json, requireAdmin, requireScope, requireViewer } from '../_lib/auth.js'

/** gs://marin-<suffix>/<path>/ — the six marin buckets only, dir prefixes only. */
const PREFIX_RE = /^gs:\/\/marin-[a-z0-9-]+\/(?:[^\s]*\/)?$/

const KEEPS = new Set(['keep', 'keep_last_ckpt', 'sweep'])

interface ActionBody {
  pattern?: string
  set_keep?: boolean
  keep?: string | null
  set_owner?: boolean
  owner?: string | null
  memo?: string
  scan?: string
}

const bad = (error: string) => ({ error })

function validate(b: ActionBody): { error: string } | {
  pattern: string
  setKeep: boolean
  keep: string | null
  setOwner: boolean
  owner: string | null
  memo: string | null
  scan: string
} {
  const pattern = b.pattern ?? ''
  if (!PREFIX_RE.test(pattern) || pattern.length > 512) {
    return bad('pattern must be gs://marin-<bucket>/<path>/ (trailing slash; regex patterns not accepted yet)')
  }
  // Touching an axis = the key is present (null = clear); `set_*` may also be
  // passed explicitly. At least one axis must be touched.
  const setKeep = b.set_keep ?? 'keep' in b
  const setOwner = b.set_owner ?? 'owner' in b
  if (!setKeep && !setOwner) return bad('action must touch at least one axis (keep and/or owner)')
  const keep = setKeep ? (b.keep ?? null) : null
  if (keep !== null && !KEEPS.has(keep)) {
    return bad(`keep must be one of ${[...KEEPS].join(', ')}, or null to unmark`)
  }
  // '@me' = resolve the actor's canonical user id server-side (claims).
  const owner = setOwner ? (b.owner ?? null) : null
  if (owner !== null && (typeof owner !== 'string' || owner.length > 128)) return bad('owner must be a user id')
  const memo = b.memo?.slice(0, 1024) ?? null
  const scan = typeof b.scan === 'string' && /^[\d-]{8,16}(T\d{4})?$/.test(b.scan) ? b.scan : 'unknown'
  return { pattern, setKeep, keep, setOwner, owner, memo, scan }
}

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const { request, env } = ctx
  if (!env.DB) return json({ error: 'actions backend not configured (DB)' }, 503)

  if (request.method === 'GET') {
    const gated = await requireViewer(ctx)
    if (gated instanceof Response) return gated
    const [keeps, owners] = await Promise.all([
      env.DB.prepare(
        'SELECT k.prefix, k.keep, k.ts, a.actor AS who, a.memo, a.id AS action_id ' +
        'FROM keep_prefixes k JOIN actions a ON a.id = k.action_id ' +
        'WHERE k.tombstoned IS NULL ORDER BY k.prefix, k.ts',
      ).all(),
      env.DB.prepare(
        'SELECT o.prefix, o.owner, o.ts, a.actor AS who, a.memo, a.id AS action_id ' +
        'FROM owner_prefixes o JOIN actions a ON a.id = o.action_id ' +
        'WHERE o.tombstoned IS NULL ORDER BY o.prefix, o.ts',
      ).all(),
    ])
    return json({ keeps: keeps.results, owners: owners.results })
  }

  if (request.method === 'POST') {
    // Marking is admin-only: non-admins propose deletions by staging, not by
    // writing the keep/owner ledger directly (specs/share-link-hardening.md).
    const id = await requireAdmin(ctx)
    if (id instanceof Response) return id
    if (!id.email) return json({ error: 'admin identity has no email' }, 403)
    const raw = (await request.json()) as ActionBody | ActionBody[]
    const items = Array.isArray(raw) ? raw : [raw]
    if (!items.length || items.length > 500) return json({ error: 'expected 1–500 actions' }, 400)
    const parsed = items.map(validate)
    const firstErr = parsed.find(p => 'error' in p)
    if (firstErr && 'error' in firstErr) return json(firstErr, 400)
    const ts = Math.floor(Date.now() / 1000)
    const ok = parsed as Exclude<ReturnType<typeof validate>, { error: string }>[]
    if (ok.some(p => p.owner === '@me')) {
      const row = await env.DB.prepare('SELECT user FROM user_emails WHERE email = ?')
        .bind(id.email.toLowerCase()).first<{ user: string }>()
      const me = row?.user ?? id.email
      for (const p of ok) if (p.owner === '@me') p.owner = me
    }
    const stmts = []
    for (const p of ok) {
      stmts.push(
        env.DB.prepare(
          'INSERT INTO actions (actor, ts, scan, pattern, set_owner, owner, set_keep, keep, memo) ' +
          'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ).bind(id.email, ts, p.scan, p.pattern, p.setOwner ? 1 : 0, p.owner, p.setKeep ? 1 : 0, p.keep, p.memo),
      )
      // Prefix patterns expand 1:1. The batch runs sequentially inside one
      // transaction, so "newest actions row" is the INSERT just above.
      if (p.setKeep) {
        stmts.push(
          env.DB.prepare(
            'INSERT INTO keep_prefixes (action_id, prefix, keep, ts) SELECT id, ?, ?, ? FROM actions ORDER BY id DESC LIMIT 1',
          ).bind(p.pattern, p.keep, ts),
        )
      }
      if (p.setOwner) {
        stmts.push(
          env.DB.prepare(
            'INSERT INTO owner_prefixes (action_id, prefix, owner, ts) SELECT id, ?, ?, ? FROM actions ORDER BY id DESC LIMIT 1',
          ).bind(p.pattern, p.owner, ts),
        )
      }
    }
    await env.DB.batch(stmts)
    return json({ ok: true, count: parsed.length, ts })
  }

  return json({ error: 'method not allowed' }, 405)
}
