import { describe, expect, it } from 'vitest'
import { planMarkBody, planMarksToLedger } from './marks'

describe('planMarksToLedger', () => {
  it('maps cw rows to KeepRows (trailing slash, memo, row-order action ids) and no owners', () => {
    expect(planMarksToLedger([
      { prefix: 's3://b/a/', keep: 'keep', who: 'ryan', ts: 1700000000, note: 'why' },
      { prefix: 's3://b/c', keep: 'sweep', who: 'dev', ts: 1700000001 },
      { prefix: 's3://b/d/', keep: null, who: 'dev', ts: 1700000002, note: null },
    ])).toEqual({
      keeps: [
        { prefix: 's3://b/a/', keep: 'keep', ts: 1700000000, who: 'ryan', memo: 'why', action_id: 1 },
        { prefix: 's3://b/c/', keep: 'sweep', ts: 1700000001, who: 'dev', memo: null, action_id: 2 },
        { prefix: 's3://b/d/', keep: null, ts: 1700000002, who: 'dev', memo: null, action_id: 3 },
      ],
      owners: [],
    })
  })
})

describe('planMarkBody', () => {
  it("turns a keep post into cw's body, with the scan and memo when present", () => {
    expect(planMarkBody({ pattern: 's3://b/a/', keep: 'sweep' })).toEqual({ prefixes: ['s3://b/a/'], keep: 'sweep' })
    expect(planMarkBody({ pattern: 's3://b/a/', keep: null, memo: 'done' }, '2026-09-16T0001')).toEqual({ prefixes: ['s3://b/a/'], keep: null, scan: '2026-09-16T0001', note: 'done' })
  })
  it('refuses owner posts (no claims on a plan-first store)', () => {
    expect(() => planMarkBody({ pattern: 's3://b/a/', owner: '@me' })).toThrow('assignments are not available on this store')
  })
})
