/**
 * The deployment's `allowed_emails` table (`email, note, who, ts`; edited at
 * /admin/db) as `@open-athena/auth`'s `AllowlistStore`, so `authRoutes` can
 * put a share link's recipient on it (`POST /api/auth/grants` with
 * `allowlist: true`, specs/mint-link-allowlist.md).
 *
 * The table has no scopes column: a row admits its email at the base scope
 * (`scopesFor`), so a row reads as the base scope and its read-only half — a
 * read-only link's recipient is already covered by it, and one added for such
 * a link signs in as a full viewer. No `source` column either, so every row is
 * `manual` and a directory sync (`replaceSource`) is refused.
 */
import type { AllowEntry, AllowlistStore } from '@open-athena/auth'
import { baseScope, type Env } from './auth.js'

interface Row { email: string; note: string | null; who: string; ts: number }

export function siteAllowlist(env: Env): AllowlistStore {
  const db = env.DB!
  const base = baseScope(env)
  const scopes = [base, `${base}:read`]
  const entry = (r: Row): AllowEntry => ({ email: r.email, scopes, source: 'manual', note: r.note, addedBy: r.who, updatedAt: r.ts })
  return {
    async lookup(email) {
      const row = await db.prepare('SELECT email FROM allowed_emails WHERE email = ?').bind(email.toLowerCase()).first()
      return row ? scopes : null
    },
    async list() {
      const { results } = await db.prepare('SELECT email, note, who, ts FROM allowed_emails ORDER BY email').all<Row>()
      return results.map(entry)
    },
    async put(e) {
      await db.prepare(
        `INSERT INTO allowed_emails (email, note, who, ts) VALUES (?, ?, ?, ?)
         ON CONFLICT(email) DO UPDATE SET note = excluded.note, who = excluded.who, ts = excluded.ts`,
      ).bind(e.email.toLowerCase(), e.note, e.addedBy ?? 'share link', e.updatedAt).run()
    },
    async remove(email) {
      const res = await db.prepare('DELETE FROM allowed_emails WHERE email = ?').bind(email.toLowerCase()).run()
      return (res.meta?.changes ?? 0) > 0
    },
    async replaceSource() {
      throw new Error('allowed_emails has no `source` column: directory sync is not supported here')
    },
  }
}
