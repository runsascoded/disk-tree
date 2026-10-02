/**
 * The search index's query planner (specs/path-store-search.md §1, §4.1):
 * a `q=` string → the candidate condition on a match root's **last segment**,
 * as an OR of branches, each a trigram formula (what the postings can
 * narrow) plus the exact name test (what a candidate must pass). Pure; the
 * reads are `search.ts`'s.
 *
 * The planner never changes what matches: the view still tests full paths
 * with `parseQuery`'s predicate. It only answers "which last segments can a
 * match root have?" — and returns null where that set isn't a function of
 * one segment (a needle ending in `/`, a regex that can match `/`) or where
 * the regex uses syntax it doesn't model; the view then reads as before.
 */

/** A monotone boolean formula over trigram codes: `true` (no constraint), a
 * trigram (the name contains it), or an AND / OR of formulas. */
export type Formula = true | number | { and: Formula[] } | { or: Formula[] }

export interface Branch {
  /** What the postings narrow: every name passing `test` satisfies it. */
  formula: Formula
  /** The exact condition on a candidate name (the segment as stored). */
  test: (name: string) => boolean
}

/** An OR of branches: a name is a candidate when some branch's test passes. */
export interface SearchPlan { branches: Branch[] }

const isAscii = (c: number) => c < 128

/** The trigram code of 3 ASCII characters: `c0<<16 | c1<<8 | c2`. */
export const triCode = (s: string, i = 0): number => (s.charCodeAt(i) << 16) | (s.charCodeAt(i + 1) << 8) | s.charCodeAt(i + 2)

/** Distinct all-ASCII trigrams of the lowercased string, ascending. Non-ASCII
 * trigrams are never looked up (case mappings of non-ASCII text differ
 * between DuckDB and JS; an ASCII run is the same run under any of them). */
export function trigrams(s: string): number[] {
  const l = s.toLowerCase()
  const out = new Set<number>()
  for (let i = 0; i + 3 <= l.length; i++) {
    if (isAscii(l.charCodeAt(i)) && isAscii(l.charCodeAt(i + 1)) && isAscii(l.charCodeAt(i + 2))) out.add(triCode(l, i))
  }
  return [...out].sort((a, b) => a - b)
}

export function and(fs: Formula[]): Formula {
  const out: Formula[] = []
  for (const f of fs) {
    if (f === true) continue
    if (typeof f === 'object' && 'and' in f) out.push(...f.and)
    else out.push(f)
  }
  return out.length === 0 ? true : out.length === 1 ? out[0] : { and: out }
}

export function or(fs: Formula[]): Formula {
  const out: Formula[] = []
  for (const f of fs) {
    if (f === true) return true
    if (typeof f === 'object' && 'or' in f) out.push(...f.or)
    else out.push(f)
  }
  // Absorption: an alternative requiring a superset of another's trigrams
  // adds nothing (`ckpt | ckpts` = `ckpt`); equal sets collapse to one.
  const flat = (f: Formula): number[] | null => typeof f === 'number' ? [f] : typeof f === 'object' && 'and' in f && f.and.every(x => typeof x === 'number') ? f.and as number[] : null
  const sets = out.map(f => { const t = flat(f); return t && new Set(t) })
  const kept = out.filter((_, i) => {
    const si = sets[i]
    if (!si) return true
    return !sets.some((sj, j) => j !== i && sj && [...sj].every(t => si.has(t)) && (sj.size < si.size || j < i))
  })
  // An empty OR can't arise from a query (every alternative yields a formula).
  return kept.length === 1 ? kept[0] : { or: kept }
}

/** A string's formula: every name containing it holds all its trigrams. */
const stringFormula = (s: string): Formula => and(trigrams(s))

/** `q` → the plan, or null when the index can't serve it (the view's
 * existing read answers). Mirrors `parseQuery`'s parse exactly. */
export function planSearch(q: string | null | undefined): SearchPlan | null {
  const s = (q ?? '').trim()
  if (!s) return null
  if (s.length > 2 && s.startsWith('/') && s.endsWith('/')) return planRegex(s.slice(1, -1))
  const needles = s.toLowerCase().split('|').map(t => t.trim()).filter(Boolean)
  if (!needles.length) return null
  const branches: Branch[] = []
  for (const t of needles) {
    const cut = t.lastIndexOf('/')
    if (cut === -1) {
      branches.push({ formula: stringFormula(t), test: n => n.toLowerCase().includes(t) })
      continue
    }
    // `…/u`: the occurrence's last `/` is the separator before the root's
    // last segment, which therefore starts with `u`. `u = ''` constrains
    // nothing (every child of a `…t` dir is a root).
    const u = t.slice(cut + 1)
    if (!u) return null
    branches.push({ formula: stringFormula(u), test: n => n.toLowerCase().startsWith(u) })
  }
  return { branches }
}

// --- regex: a JS (non-unicode, flag `i`) syntax subset → AST → formula --------

/** One character position: the lowercase characters it can match when few
 * (`set`), else null; whether it can match `/`. */
interface CharNode { t: 'char'; set: string[] | null; slash: boolean }
type Node =
  | CharNode
  | { t: 'empty' } // an anchor or word-boundary assertion
  | { t: 'cat'; xs: Node[] }
  | { t: 'alt'; xs: Node[] }
  | { t: 'rep'; x: Node; min: number; max: number }

/** Thrown inside the parser for syntax the planner doesn't model. */
class Unsupported extends Error {}

/** Exact sets above this size become formulas. */
const MAX_SET = 16
const SLASH = 0x2f

const lit = (c: string): CharNode => ({ t: 'char', set: [c.toLowerCase()], slash: c === '/' })
const wide = (slash: boolean): CharNode => ({ t: 'char', set: null, slash })

/** A class escape's matcher (`\d \w \s` and their negations). */
function classEscape(c: string): { slash: boolean; set: string[] | null } | null {
  switch (c) {
    case 'd': return { slash: false, set: '0123456789'.split('') }
    case 'w': case 's': return { slash: false, set: null }
    case 'D': case 'W': case 'S': return { slash: true, set: null }
    default: return null
  }
}

/** A control/identity escape's character, or null for the class escapes. */
function escapeChar(src: string, i: number): { c: string; next: number } {
  const c = src[i]
  if (c === undefined) throw new Unsupported('trailing backslash')
  const ctl: Record<string, string> = { n: '\n', r: '\r', t: '\t', f: '\f', v: '\v', 0: '\0' }
  if (c in ctl && !(c === '0' && /[0-9]/.test(src[i + 1] ?? ''))) return { c: ctl[c], next: i + 1 }
  if (c === 'x' && /^[0-9a-fA-F]{2}$/.test(src.slice(i + 1, i + 3))) return { c: String.fromCharCode(parseInt(src.slice(i + 1, i + 3), 16)), next: i + 3 }
  if (c === 'u' && /^[0-9a-fA-F]{4}$/.test(src.slice(i + 1, i + 5))) return { c: String.fromCharCode(parseInt(src.slice(i + 1, i + 5), 16)), next: i + 5 }
  // Backreferences, `\c`, `\k`, `\p` (an identity escape outside unicode
  // mode, but easy to misread) and octal forms: not modelled.
  if (/[1-9ckpPux]/.test(c)) throw new Unsupported(`\\${c}`)
  return { c, next: i + 1 }
}

function parseClass(src: string, i: number): { node: CharNode; next: number } {
  // `src[i]` is just past '['.
  let neg = false
  if (src[i] === '^') { neg = true; i++ }
  const chars = new Set<string>()
  let slash = false
  let wideSet = false
  let first = true
  const atom = (): { c: string } | { esc: { slash: boolean; set: string[] | null } } => {
    if (src[i] === '\\') {
      const e = classEscape(src[i + 1] ?? '')
      if (e) { i += 2; return { esc: e } }
      if (src[i + 1] === 'b') { i += 2; return { c: '\b' } }
      if (src[i + 1] === '-') { i += 2; return { c: '-' } }
      const { c, next } = escapeChar(src, i + 1)
      i = next
      return { c }
    }
    return { c: src[i++] }
  }
  for (;;) {
    if (i >= src.length) throw new Unsupported('unterminated class')
    if (src[i] === ']' && !first) { i++; break }
    first = false
    const a = atom()
    if ('esc' in a) {
      slash ||= a.esc.slash
      if (a.esc.set) for (const c of a.esc.set) chars.add(c)
      else wideSet = true
      continue
    }
    if (src[i] === '-' && src[i + 1] !== ']' && i + 1 < src.length) {
      i++
      const b = atom()
      if ('esc' in b) throw new Unsupported('class range to an escape')
      const lo = a.c.charCodeAt(0)
      const hi = b.c.charCodeAt(0)
      if (hi < lo) throw new Unsupported('bad range')
      if (lo <= SLASH && SLASH <= hi) slash = true
      if (hi - lo + 1 > 2 * MAX_SET) wideSet = true
      else for (let k = lo; k <= hi; k++) chars.add(String.fromCharCode(k).toLowerCase())
      continue
    }
    if (a.c === '/') slash = true
    chars.add(a.c.toLowerCase())
  }
  if (neg) return { node: wide(!slash), next: i }
  const set = !wideSet && chars.size <= MAX_SET ? [...chars] : null
  return { node: { t: 'char', set, slash }, next: i }
}

/** Parse `src` (the pattern between the slashes). Throws `Unsupported`. */
function parseRegex(src: string): Node {
  let i = 0
  const quant = (): { min: number; max: number } | null => {
    const c = src[i]
    let q: { min: number; max: number } | null = null
    if (c === '*') { q = { min: 0, max: Infinity }; i++ }
    else if (c === '+') { q = { min: 1, max: Infinity }; i++ }
    else if (c === '?') { q = { min: 0, max: 1 }; i++ }
    else if (c === '{') {
      const m = /^\{(\d+)(,(\d*))?\}/.exec(src.slice(i))
      if (m) {
        q = { min: +m[1], max: m[2] ? (m[3] ? +m[3] : Infinity) : +m[1] }
        i += m[0].length
      }
    }
    if (q && src[i] === '?') i++ // lazy: same strings
    return q
  }
  const alt = (): Node => {
    const xs = [cat()]
    while (src[i] === '|') { i++; xs.push(cat()) }
    return xs.length === 1 ? xs[0] : { t: 'alt', xs }
  }
  const cat = (): Node => {
    const xs: Node[] = []
    while (i < src.length && src[i] !== '|' && src[i] !== ')') {
      let a = atom()
      for (let q = quant(); q; q = quant()) {
        if (a.t === 'empty') throw new Unsupported('quantified assertion')
        a = { t: 'rep', x: a, min: q.min, max: q.max }
      }
      xs.push(a)
    }
    return xs.length === 1 ? xs[0] : { t: 'cat', xs }
  }
  const atom = (): Node => {
    const c = src[i]
    if (c === '(') {
      i++
      if (src[i] === '?') {
        if (src[i + 1] === ':') i += 2
        else if (src[i + 1] === '<' && /^[A-Za-z_$]/.test(src[i + 2] ?? '')) {
          const end = src.indexOf('>', i)
          if (end === -1) throw new Unsupported('group name')
          i = end + 1
        } else throw new Unsupported('lookaround')
      }
      const x = alt()
      if (src[i] !== ')') throw new Unsupported('unbalanced group')
      i++
      return x
    }
    if (c === '[') {
      const { node, next } = parseClass(src, i + 1)
      i = next
      return node
    }
    if (c === '.') { i++; return wide(true) }
    if (c === '^' || c === '$') { i++; return { t: 'empty' } }
    if (c === '\\') {
      const e = src[i + 1]
      if (e === 'b' || e === 'B') { i += 2; return { t: 'empty' } }
      const ce = classEscape(e ?? '')
      if (ce) { i += 2; return { t: 'char', set: ce.set, slash: ce.slash } }
      const { c: ch, next } = escapeChar(src, i + 1)
      i = next
      return lit(ch)
    }
    if (c === '*' || c === '+' || c === '?') throw new Unsupported('nothing to repeat')
    i++
    return lit(c)
  }
  const out = alt()
  if (i !== src.length) throw new Unsupported(`unexpected '${src[i]}'`)
  return out
}

const canMatchSlash = (n: Node): boolean =>
  n.t === 'char' ? n.slash
  : n.t === 'empty' ? false
  : n.t === 'rep' ? canMatchSlash(n.x)
  : n.xs.some(canMatchSlash)

/** What a node's matches all satisfy: an exact (small) string set, else a formula. */
type Info = { exact: string[] } | { f: Formula }

const toFormula = (x: Info): Formula => ('exact' in x ? or(x.exact.map(stringFormula)) : x.f)

const product = (a: string[], b: string[]): string[] => [...new Set(a.flatMap(x => b.map(y => x + y)))]

function info(n: Node): Info {
  switch (n.t) {
    case 'char': return n.set ? { exact: n.set } : { f: true }
    case 'empty': return { exact: [''] }
    case 'cat': {
      // `run`: the exact strings of the current run of exact parts (their
      // product); `f`: what the parts before it require. A wide part, or a
      // product past the cap, closes the run into `f` — the match then
      // contains one of the run's strings — and the next run starts fresh.
      let f: Formula = true
      let closed = false
      let run = ['']
      for (const x of n.xs.map(info)) {
        if ('exact' in x && run.length * x.exact.length <= MAX_SET) {
          run = product(run, x.exact)
          continue
        }
        f = and([f, or(run.map(stringFormula))])
        closed = true
        if ('exact' in x) run = x.exact
        else {
          f = and([f, x.f])
          run = ['']
        }
      }
      return closed ? { f: and([f, or(run.map(stringFormula))]) } : { exact: run }
    }
    case 'alt': {
      const xs = n.xs.map(info)
      if (xs.every(x => 'exact' in x)) {
        const u = [...new Set(xs.flatMap(x => (x as { exact: string[] }).exact))]
        if (u.length <= MAX_SET) return { exact: u }
      }
      return { f: or(xs.map(toFormula)) }
    }
    case 'rep': {
      const x = info(n.x)
      if (n.min === 0) {
        if (n.max === 1 && 'exact' in x && x.exact.length < MAX_SET) return { exact: [...new Set(['', ...x.exact])] }
        return { f: true }
      }
      if ('exact' in x && n.max === n.min) {
        let acc = x.exact
        let k = 1
        for (; k < n.min && acc.length * x.exact.length <= MAX_SET; k++) acc = product(acc, x.exact)
        // Stopped short of `min`: the match contains one of `acc` (its first
        // k repetitions) but is not one of them.
        return k === n.min ? { exact: acc } : { f: or(acc.map(stringFormula)) }
      }
      return { f: toFormula(x) }
    }
  }
}

function planRegex(src: string): SearchPlan | null {
  let re: RegExp
  try {
    re = new RegExp(src, 'i')
  } catch {
    return null // parseQuery has no predicate either
  }
  let ast: Node
  try {
    ast = parseRegex(src)
  } catch (e) {
    if (e instanceof Unsupported) return null
    throw e
  }
  // A regex that can match `/` can match across segments: no single
  // segment of a match root is constrained (spec §1).
  if (canMatchSlash(ast)) return null
  return { branches: [{ formula: toFormula(info(ast)), test: n => re.test(n) }] }
}
