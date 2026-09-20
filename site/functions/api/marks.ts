/**
 * Marks store for the mark & sweep flow (specs/mark-sweep-ui.md).
 *
 *   GET  /api/marks             → { marks: [...], claims: [...] } (small; the
 *                                 UI overlays them on the tree client-side)
 *   PUT  /api/marks             → upsert { prefix, action, note? }; action
 *                                 null removes the mark. `who`/`ts` come from
 *                                 the session; every write appends to mark_log.
 *
 * Anyone who can see the dashboard can mark (`gcs` scope) — the sweep's
 * review gate is where authority gets applied, and `mark_log` keeps the full
 * who-did-what trail either way.
 */
import { type Ctx, json, requireViewer } from '../_lib/auth.js'

/** gs://marin-<suffix>/<path>/ — the six marin buckets only, dir prefixes only. */
const PREFIX_RE = /^gs:\/\/marin-[a-z0-9-]+\/(?:[^\s]*\/)?$/

const ACTIONS = new Set(['keep', 'keep_last_ckpt', 'delete'])

export const onRequest = async (ctx: Ctx): Promise<Response> => {
  const { request, env } = ctx
  if (!env.DB) return json({ error: 'marks backend not configured (DB)' }, 503)
  const id = await requireViewer(ctx)
  if (id instanceof Response) return id
  const who = id.email ?? id.name ?? 'unknown'

  if (request.method === 'GET') {
    const [marks, claims] = await Promise.all([
      env.DB.prepare('SELECT prefix, action, who, ts, note FROM marks ORDER BY prefix').all(),
      env.DB.prepare('SELECT prefix, who, ts FROM claims ORDER BY prefix').all(),
    ])
    return json({ marks: marks.results, claims: claims.results })
  }

  if (request.method === 'PUT') {
    if (!id.email) {
      return json({ error: 'marking requires a signed-in email — guest links are read-only; sign in via Google or ask for a personal link' }, 403)
    }
    const b = (await request.json()) as { prefix?: string; action?: string | null; note?: string }
    const prefix = b.prefix ?? ''
    if (!PREFIX_RE.test(prefix) || prefix.length > 1024) {
      return json({ error: 'prefix must be gs://marin-<bucket>/<path>/ (trailing slash)' }, 400)
    }
    const action = b.action ?? null
    if (action !== null && !ACTIONS.has(action)) {
      return json({ error: `action must be one of ${[...ACTIONS].join(', ')}, or null to unmark` }, 400)
    }
    const note = b.note?.slice(0, 1024) ?? null
    const ts = Math.floor(Date.now() / 1000)
    const stmts = [
      env.DB.prepare('INSERT INTO mark_log (prefix, action, who, ts, note) VALUES (?, ?, ?, ?, ?)')
        .bind(prefix, action, who, ts, note),
      action === null
        ? env.DB.prepare('DELETE FROM marks WHERE prefix = ?').bind(prefix)
        : env.DB.prepare(
            'INSERT INTO marks (prefix, action, who, ts, note) VALUES (?, ?, ?, ?, ?) ' +
            'ON CONFLICT (prefix) DO UPDATE SET action = ?2, who = ?3, ts = ?4, note = ?5',
          ).bind(prefix, action, who, ts, note),
    ]
    await env.DB.batch(stmts)
    return json({ ok: true, prefix, action, who, ts })
  }

  return json({ error: 'method not allowed' }, 405)
}
