import { describe, expect, it } from 'vitest'
import { and, type Formula, or, planSearch, triCode, trigrams } from './searchQuery'

/** A string's formula, spelled out: AND of its trigrams. */
const all = (s: string): Formula => and(trigrams(s))
const formulas = (q: string): Formula[] | null => planSearch(q)?.branches.map(b => b.formula) ?? null
/** Which of `names` a plan's branches accept (the exact name test). */
const accepts = (q: string, names: string[]): string[] => {
  const p = planSearch(q)!
  return names.filter(n => p.branches.some(b => b.test(n)))
}

describe('trigrams', () => {
  it('distinct, lowercased, ASCII-only, ascending codes', () => {
    expect(trigrams('TTLttl')).toEqual([triCode('ltt'), triCode('tlt'), triCode('ttl')])
    expect(trigrams('ab')).toEqual([])
    // `é` is not ASCII: only `caf` survives of `caf`, `afé`, `fés`.
    expect(trigrams('Cafés')).toEqual([triCode('caf')])
    // The Kelvin sign lowercases to `k` in JS, as in DuckDB.
    expect(trigrams('\u212aey')).toEqual([triCode('key')])
  })
})

describe('substring needles (parseQuery’s non-regex form)', () => {
  it('a slash-free needle: the root’s last segment contains it', () => {
    expect(formulas('ttl')).toEqual([triCode('ttl')])
    expect(formulas(' TTL=14d ')).toEqual([all('ttl=14d')])
    expect(accepts('TTL', ['ttl=7d', 'iris-TTL', 'tl', 'x'])).toEqual(['ttl=7d', 'iris-TTL'])
  })
  it('alternatives are branches; blanks dropped', () => {
    expect(formulas('ttl | ckpt ||')).toEqual([triCode('ttl'), all('ckpt')])
  })
  it('a needle with `/`: the last segment starts with its last piece', () => {
    expect(formulas('run-a/ckpt')).toEqual([all('ckpt')])
    expect(accepts('run-a/CKPT', ['ckpt', 'ckpt-final.pt', 'x-ckpt'])).toEqual(['ckpt', 'ckpt-final.pt'])
    expect(formulas('/tmp')).toEqual([all('tmp')])
  })
  it('a needle ending in `/` constrains no segment: not served', () => {
    expect(planSearch('ckpt/')).toBeNull()
    expect(planSearch('ttl|ckpt/')).toBeNull()
  })
  it('a needle under 3 characters has no trigram: unconstrained (the names scan)', () => {
    expect(formulas('gr')).toEqual([true])
    expect(formulas('ttl|gr')).toEqual([triCode('ttl'), true])
  })
  it('nothing to search', () => {
    expect([planSearch(''), planSearch('  '), planSearch('|'), planSearch(null)]).toEqual([null, null, null, null])
  })
})

describe('regexes (`/…/`, flag i, full-path semantics)', () => {
  it('literals concatenate; classes and repeats bound what they can', () => {
    expect(formulas('/^model-\\d+-of-\\d+\\.safetensors$/')).toEqual([and([all('model-'), all('-of-'), all('.safetensors')])])
    expect(formulas('/ttl=\\d+d/')).toEqual([all('ttl=')])
    expect(formulas('/^run-(a|b)c$/')).toEqual([or([all('run-ac'), all('run-bc')])])
    expect(formulas('/(?:ckpt|checkpoint)s?/')).toEqual([or([all('ckpt'), all('checkpoint')])])
    expect(formulas('/[Tt][Tt][Ll]/')).toEqual([triCode('ttl')])
    // A wide class or `*` constrains nothing; a short alternative ends the AND.
    expect(formulas('/[a-z]+x*/')).toEqual([true])
    expect(formulas('/ab|cde/')).toEqual([true])
  })
  it('a repeat cut short of its minimum keeps a formula, not an exact set', () => {
    // [ab]{5}: 2^4 = 16 four-letter prefixes, then the 5th repetition would
    // exceed the set cap — the match contains one of them (an OR).
    const f = formulas('/[ab]{5}c/')![0]
    const p = planSearch('/[ab]{5}c/')!
    expect(typeof f === 'object' && 'or' in f).toBe(true)
    expect(p.branches[0].test('xababbc')).toBe(true)
    // Sound: the formula holds for a name the regex matches (every trigram it ANDs is present).
    const tris = new Set(trigrams('xababbc'))
    const holds = (g: Formula): boolean => g === true ? true : typeof g === 'number' ? tris.has(g) : 'and' in g ? g.and.every(holds) : g.or.some(holds)
    expect(holds(f)).toBe(true)
  })
  it('the name test is the regex itself, case-insensitive', () => {
    expect(accepts('/^model-\\d+/', ['model-00001-of-00002.safetensors', 'Model-7', 'tiny-model-1', 'model-x'])).toEqual(['model-00001-of-00002.safetensors', 'Model-7'])
  })
  it('a regex that can match `/` spans segments: not served', () => {
    for (const q of ['/ckpt.*final/', '/a\\/b/', '/a[/]b/', '/a[^x]b/', '/a\\Wb/', '/a\\Db/', '/a\\Sb/', '/a[!-0]b/']) expect([q, planSearch(q)]).toEqual([q, null])
    // A negated class that excludes `/` stays inside a segment.
    expect(formulas('/ckpt[^/]*final/')).toEqual([and([all('ckpt'), all('final')])])
  })
  it('syntax the planner doesn’t model is not served; an invalid regex has no predicate at all', () => {
    for (const q of ['/a(?=b)/', '/a(?!b)/', '/(?<=a)b/', '/(a)\\1/', '/\\p{L}/', '/\\cJ/']) expect([q, planSearch(q)]).toEqual([q, null])
    expect(planSearch('/a(/')).toBeNull()
  })
  it('named groups, lazy quantifiers, literal braces and escapes parse', () => {
    expect(formulas('/(?<run>run-\\d+?)-ckpt/')).toEqual([and([all('run-'), all('-ckpt')])])
    expect(formulas('/a{x}b/')).toEqual([all('a{x}b')])
    expect(formulas('/\\x41bc\\.d/')).toEqual([all('abc.d')])
  })
})
