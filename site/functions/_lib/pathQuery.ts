/**
 * The path filter's query syntax (`q=`; specs/path-store-search.md §1) — the
 * one parser the server (`scope.ts`, the search planner) and the client
 * (`src/filterTree.ts`) share. Case-insensitive throughout; tested against a
 * node's full index path (`bucket/dir/sub`):
 *
 * - `tomat` — substring anywhere in the path;
 * - `a b` — AND: the path contains every term (in any segments);
 * - `a|b` — OR, binding looser than AND (`a b|c` = (a AND b) OR c);
 * - `-x` — NOT: nothing whose path contains `x` counts. Negatives apply to
 *   the whole query (every alternative); a query of only negatives means
 *   "everything except";
 * - `*` — any characters within one segment (`[^/]*`), also in a term with `/`;
 * - `"a b"` — a quoted term is literal (spaces, a leading `-`, `*`, `|`);
 * - `/…/` — a JS regex over the full path (flag `i`): an undocumented
 *   fallback, never served by the search index.
 *
 * Pure, DOM- and Workers-free.
 */

/** A term: literal pieces (lowercase) joined by `*` wildcards. */
export interface Term { pieces: string[] }

export interface PathQuery {
  /** The query string as given (trimmed). */
  q: string
  /** OR of alternatives, each an AND of positive terms; an empty alternative
   * (a query of only negatives) matches everything. Empty for a regex. */
  alts: Term[][]
  /** NOT terms — of the whole query. */
  neg: Term[]
  /** The `/…/` fallback. */
  regex: RegExp | null
}

/** A path predicate (`pos ∧ ¬neg` on the full path) carrying its parse: the
 * query string (`q`), its structure (`query`), and its two halves — the
 * positive part (monotone: once a prefix of a path holds it, every longer
 * path does) and the negative part (null without negatives). */
export type NamePred = ((path: string) => boolean) & {
  q?: string
  query?: PathQuery
  pos?: (path: string) => boolean
  neg?: ((path: string) => boolean) | null
}

const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/** A term's test on a lowercased path: substring, or with `*` a regex whose
 * wildcards stay inside one segment. */
export function termTest(t: Term): (lowerPath: string) => boolean {
  if (t.pieces.length === 1) {
    const s = t.pieces[0]
    return p => p.includes(s)
  }
  const re = new RegExp(t.pieces.map(esc).join('[^/]*'))
  return p => re.test(p)
}

/** Parse `q`; null for nothing to filter by (blank, only `|`s / empty
 * quotes, an invalid regex). */
export function parsePathQuery(q: string | null | undefined): PathQuery | null {
  const raw = (q ?? '').trim()
  if (!raw) return null
  if (raw.length > 2 && raw.startsWith('/') && raw.endsWith('/')) {
    try {
      return { q: raw, alts: [], neg: [], regex: new RegExp(raw.slice(1, -1), 'i') }
    } catch {
      return null
    }
  }
  const s = raw.toLowerCase()
  const alts: { terms: Term[]; any: boolean }[] = [{ terms: [], any: false }]
  const neg: Term[] = []
  const sep = (c: string) => c === '|' || /\s/.test(c)
  let i = 0
  while (i < s.length) {
    const c = s[i]
    if (/\s/.test(c)) { i++; continue }
    if (c === '|') { alts.push({ terms: [], any: false }); i++; continue }
    let isNeg = false
    if (c === '-' && i + 1 < s.length && !sep(s[i + 1])) { isNeg = true; i++ }
    const pieces = ['']
    let quoted = false
    while (i < s.length && (quoted || !sep(s[i]))) {
      const ch = s[i++]
      if (ch === '"') quoted = !quoted
      else if (ch === '*' && !quoted) pieces.push('')
      else pieces[pieces.length - 1] += ch
    }
    const alt = alts[alts.length - 1]
    alt.any = true
    if (pieces.length === 1 && !pieces[0]) continue // `""`
    if (isNeg) neg.push({ pieces })
    else alt.terms.push({ pieces })
  }
  // A blank alternative (`a|`, `||`) adds nothing; one that held only
  // negatives is "everything" (the negatives are the query's).
  const kept = alts.filter(a => a.any).map(a => a.terms)
  if (!kept.some(a => a.length) && !neg.length) return null
  return { q: raw, alts: kept.length ? kept : [[]], neg, regex: null }
}

/** `q` → its predicate, or null for no filter. */
export function parseQuery(q: string | null | undefined): NamePred | null {
  const query = parsePathQuery(q)
  if (!query) return null
  if (query.regex) {
    const re = query.regex
    const pred = (path: string) => re.test(path)
    return Object.assign(pred, { q: query.q, query, pos: pred, neg: null })
  }
  const alts = query.alts.map(a => a.map(termTest))
  const negs = query.neg.map(termTest)
  const pos = (path: string) => { const p = path.toLowerCase(); return alts.some(a => a.every(t => t(p))) }
  const neg = negs.length ? (path: string) => { const p = path.toLowerCase(); return negs.some(t => t(p)) } : null
  const pred = neg ? (path: string) => pos(path) && !neg(path) : pos
  return Object.assign(pred, { q: query.q, query, pos, neg })
}
