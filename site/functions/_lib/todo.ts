/**
 * "To-do" = the review backlog on the keep axis (specs/actions-ledger.md):
 * the largest prefixes with no keep/sweep decision yet. Since anything unmarked
 * defaults to swept at the deadline, this is the queue to burn down.
 *
 * A keep decision **inherits down the tree**, so a prefix isn't a to-do item
 * just because it has no mark of its own — it's a to-do item when its whole
 * subtree is untouched: no keep on it, on any ancestor, or on any descendant.
 * Marking part of a prefix keep/sweep removes that part and surfaces the still-
 * clean siblings, so the list drills down as review progresses. The owner axis
 * ("lost & found" / unowned) is a separate lens; this is keep only.
 *
 * The walk is pure and lives here so it can be tested without a bucket or D1.
 */

/** A minimal tree node — the site's TreeNode, only the fields the walk needs. */
export interface Node {
  n: string
  b: number
  o: number
  c?: Node[]
}

export interface TodoItem {
  prefix: string // gs://marin-<bucket>/<dir>/ — ready to hand to `dt-cloud mark`
  bytes: number
  objects: number
}

/**
 * From the live keep rows, the two prefix sets the walk needs:
 * - `decided`: prefixes carrying an affirmative keep (a clear — keep null —
 *   does not count as decided); recency already folded by the caller's query.
 * - `hasBelow`: every strict ancestor of a decided prefix, so the walk knows a
 *   subtree contains a decision somewhere beneath it and must recurse in.
 *
 * Prefixes are the stored gs:// dir form, trailing slash included.
 */
export function keepSets(decidedPrefixes: string[]): { decided: Set<string>; hasBelow: Set<string> } {
  const decided = new Set(decidedPrefixes)
  const hasBelow = new Set<string>()
  for (const p of decided) {
    // gs://marin-b/x/y/ → ancestors gs://marin-b/x/, gs://marin-b/  (strict)
    const m = /^(gs:\/\/[^/]+\/)(.*)$/.exec(p)
    if (!m) continue
    let acc = m[1]
    hasBelow.add(acc)
    const segs = m[2].replace(/\/$/, '').split('/').filter(Boolean)
    for (let i = 0; i < segs.length - 1; i++) {
      acc += segs[i] + '/'
      hasBelow.add(acc)
    }
  }
  return { decided, hasBelow }
}

/**
 * Largest fully-untouched prefixes ≥ `minBytes`, biggest first, at most `limit`.
 *
 * `roots` are the tree's bucket nodes (the synthetic "marin GCS" root is never
 * itself a to-do item — you review buckets, not "everything"). Each node's
 * gs:// prefix is built from the path of names down from its bucket.
 */
export function todoItems(
  roots: Node[],
  decided: Set<string>,
  hasBelow: Set<string>,
  minBytes: number,
  limit: number,
): TodoItem[] {
  const out: TodoItem[] = []
  const visit = (node: Node, prefix: string): void => {
    if (decided.has(prefix)) return // this prefix (and its subtree) is decided
    if (!hasBelow.has(prefix)) {
      // No decision on it or beneath it → a clean chunk. Take it whole; don't
      // descend (its children are clean too, and the parent is the bigger item).
      if (node.b >= minBytes) out.push({ prefix, bytes: node.b, objects: node.o })
      return
    }
    // A decision lives somewhere below — recurse to the clean pockets beside it.
    for (const c of node.c ?? []) {
      if (!c.n.startsWith('(')) visit(c, `${prefix}${c.n}/`)
    }
  }
  for (const r of roots) visit(r, `gs://${r.n}/`)
  out.sort((a, b) => b.bytes - a.bytes)
  return out.slice(0, limit)
}
