import { describe, expect, it } from 'vitest'
import { parsePathQuery, parseQuery } from './pathQuery'

const PATHS = [
  'bk/tmp/ttl=14d/run-a/ckpt',
  'bk/tmp/ttl=7d',
  'bk/x/ckpt/run/final',
  'bk/x/ckpt-final',
  'bk/x/ckpt-run-b-final',
  'bk/x/ckpt/final.pt',
  'bk/models/Llama/model-1.safetensors',
  'bk/models/tiny.safetensors',
  'bk/notes/a b.txt',
  'bk/notes/-draft',
  'bk/notes/a|b',
]
/** The paths a query's predicate accepts, in `PATHS` order. */
const hits = (q: string): string[] => PATHS.filter(parseQuery(q)!)

describe('parsePathQuery: the syntax', () => {
  const t = (...pieces: string[]) => ({ pieces })
  it.each([
    ['tomat', [[t('tomat')]], []],
    ['TTL', [[t('ttl')]], []],
    ['a|b', [[t('a')], [t('b')]], []],
    ['a b|c', [[t('a'), t('b')], [t('c')]], []],
    ['ckpt -run', [[t('ckpt')]], [t('run')]],
    ['-x', [[]], [t('x')]],
    ['-x -y', [[]], [t('x'), t('y')]],
    ['ckpt*final', [[t('ckpt', 'final')]], []],
    ['*.pt', [[t('', '.pt')]], []],
    ['tmp/*/ckpt', [[t('tmp/', '/ckpt')]], []],
    ['"a b"', [[t('a b')]], []],
    ['"-draft"', [[t('-draft')]], []],
    ['-"a b"', [[]], [t('a b')]],
    ['"a|b"', [[t('a|b')]], []],
    ['"x*y"', [[t('x*y')]], []],
    ['a - b', [[t('a'), t('-'), t('b')]], []],
    ['ttl|', [[t('ttl')]], []],
    ['a | -x', [[t('a')], []], [t('x')]],
  ])('%s', (q, alts, neg) => {
    expect(parsePathQuery(q)).toEqual({ q, alts, neg, regex: null })
  })
  it('nothing to filter by', () => {
    expect(['', '  ', '|', '||', '""', null].map(parsePathQuery)).toEqual([null, null, null, null, null, null])
  })
  it('`/…/` is the (undocumented) regex fallback; an invalid one is no filter', () => {
    expect(parsePathQuery('/ckpt.*final/')).toEqual({ q: '/ckpt.*final/', alts: [], neg: [], regex: /ckpt.*final/i })
    expect(parsePathQuery('/a(/')).toBeNull()
  })
})

describe('parseQuery: the predicate on the full path', () => {
  it.each([
    // substring anywhere, case-insensitive
    ['ckpt', ['bk/tmp/ttl=14d/run-a/ckpt', 'bk/x/ckpt/run/final', 'bk/x/ckpt-final', 'bk/x/ckpt-run-b-final', 'bk/x/ckpt/final.pt']],
    ['LLAMA', ['bk/models/Llama/model-1.safetensors']],
    // OR
    ['ttl=7d|tiny', ['bk/tmp/ttl=7d', 'bk/models/tiny.safetensors']],
    // AND, in any segments
    ['ckpt final', ['bk/x/ckpt/run/final', 'bk/x/ckpt-final', 'bk/x/ckpt-run-b-final', 'bk/x/ckpt/final.pt']],
    // OR binds looser than AND
    ['ckpt run|tiny', ['bk/tmp/ttl=14d/run-a/ckpt', 'bk/x/ckpt/run/final', 'bk/x/ckpt-run-b-final', 'bk/models/tiny.safetensors']],
    // NOT
    ['ckpt -run', ['bk/x/ckpt-final', 'bk/x/ckpt/final.pt']],
    ['ckpt -run|tiny', ['bk/x/ckpt-final', 'bk/x/ckpt/final.pt', 'bk/models/tiny.safetensors']],
    // only negatives: everything except
    ['-x -tmp -notes', ['bk/models/Llama/model-1.safetensors', 'bk/models/tiny.safetensors']],
    // `*` stays inside one segment
    ['ckpt*final', ['bk/x/ckpt-final', 'bk/x/ckpt-run-b-final']],
    ['ckpt/*/final', ['bk/x/ckpt/run/final']],
    ['*.safetensors', ['bk/models/Llama/model-1.safetensors', 'bk/models/tiny.safetensors']],
    // a term with `/`: a substring of the full path
    ['x/ckpt', ['bk/x/ckpt/run/final', 'bk/x/ckpt-final', 'bk/x/ckpt-run-b-final', 'bk/x/ckpt/final.pt']],
    // quoted: literal spaces, a leading `-`, `|`
    ['"a b"', ['bk/notes/a b.txt']],
    ['"-draft"', ['bk/notes/-draft']],
    ['"a|b"', ['bk/notes/a|b']],
    ['notes -"a b"', ['bk/notes/-draft', 'bk/notes/a|b']],
    // the regex fallback: full-path semantics, spanning segments
    ['/ckpt.*final/', ['bk/x/ckpt/run/final', 'bk/x/ckpt-final', 'bk/x/ckpt-run-b-final', 'bk/x/ckpt/final.pt']],
  ])('%s', (q, want) => {
    expect(hits(q)).toEqual(want)
  })
  it('carries its parse and halves: `pos` is monotone along a path, `neg` null without negatives', () => {
    const p = parseQuery('ckpt -run')!
    expect([p.q, p.pos!('bk/x/ckpt/run'), p.neg!('bk/x/ckpt/run'), p('bk/x/ckpt/run'), p('bk/x/ckpt')]).toEqual(['ckpt -run', true, true, false, true])
    expect(parseQuery('ckpt')!.neg).toBeNull()
  })
})
