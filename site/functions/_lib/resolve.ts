/**
 * Path resolution shared by the mark endpoints (specs/actions-ledger.md
 * § Resolution). The effective owner / keep of a path is the most-recent live
 * row on an ancestor-or-equal prefix — recency beats specificity, so a newer
 * broad mark repaints older deeper ones. The winner selection is done in SQL
 * (`ORDER BY ts DESC, action_id DESC LIMIT 1`); the only logic here is turning
 * a path into the list of ancestor prefixes to match against, which is pure and
 * therefore the piece worth isolating.
 */

/** gs://marin-<bucket>/<path>/ — a directory prefix, trailing slash required. */
export const PREFIX_RE = /^gs:\/\/marin-[a-z0-9-]+\/(?:[^\s]*\/)?$/

/**
 * Every ancestor-or-equal prefix of a directory path, bucket root first.
 *
 *   gs://marin-b/x/y/  →  [gs://marin-b/, gs://marin-b/x/, gs://marin-b/x/y/]
 *   gs://marin-b/      →  [gs://marin-b/]
 *
 * These are exactly the prefixes a mark could sit on to cover `path`.
 */
export function ancestorPrefixes(path: string): string[] {
  const m = /^(gs:\/\/marin-[a-z0-9-]+\/)(.*)$/.exec(path)
  if (!m) return []
  const root = m[1]
  const out = [root]
  let acc = root
  for (const seg of m[2].split('/').filter(Boolean)) {
    acc += seg + '/'
    out.push(acc)
  }
  return out
}
