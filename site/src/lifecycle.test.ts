import { describe, expect, it } from 'vitest'
import { describeRule, lifecycleDiff, lifecycleDiffByBucket, parseLifecycle, rulePrefix } from './lifecycle'
import type { BucketLifecycleRow, LifecycleRule, LifecycleRow, LifecycleSnapshot } from './lifecycle'

const ttl = (days: number): LifecycleRule => ({ ID: `marin-ttl-${days}d`, Filter: { Prefix: `tmp/ttl=${days}d/` }, Status: 'Enabled', Expiration: { Days: days } })
const gc: LifecycleRule = { ID: 'cw-noncurrent-gc', Filter: { Prefix: '' }, Status: 'Enabled', NoncurrentVersionExpiration: { NoncurrentDays: 1 }, Expiration: { ExpiredObjectDeleteMarker: true } }
const mpu: LifecycleRule = { ID: 'marin-abort-incomplete-mpu', Filter: { Prefix: '' }, Status: 'Enabled', AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 } }
const test: LifecycleRule = { ID: 'cw-sweep-lifecycle-test', Filter: { Prefix: 'tmp/cw-sweep-lifecycle-test/' }, Status: 'Enabled', NoncurrentVersionExpiration: { NoncurrentDays: 1 }, Expiration: { ExpiredObjectDeleteMarker: true } }

describe('describeRule', () => {
  it('words each action', () => {
    expect(describeRule(ttl(14))).toBe('expire objects after 14 d')
    expect(describeRule(gc)).toBe('expire noncurrent versions after 1 d + drop expired delete markers')
    expect(describeRule(mpu)).toBe('abort incomplete multipart uploads after 7 d')
    expect(describeRule({ ID: 'x', Status: 'Disabled' })).toBe('no action')
  })
  it('joins every action a rule carries, objects first', () => {
    expect(describeRule({ ...gc, Expiration: { Days: 3, ExpiredObjectDeleteMarker: true }, AbortIncompleteMultipartUpload: { DaysAfterInitiation: 2 } }))
      .toBe('expire objects after 3 d + expire noncurrent versions after 1 d + drop expired delete markers + abort incomplete multipart uploads after 2 d')
  })
  it('prefix: empty filter = whole bucket', () => {
    expect(rulePrefix(gc)).toBe('')
    expect(rulePrefix({ ID: 'x', Status: 'Enabled' })).toBe('')
    expect(rulePrefix(ttl(1))).toBe('tmp/ttl=1d/')
  })
})

describe('lifecycleDiff', () => {
  it('no previous snapshot: rows sorted by ID, no changes', () => {
    expect(lifecycleDiff(null, [ttl(7), gc, ttl(1)])).toEqual<LifecycleRow[]>([
      { rule: gc, change: null },
      { rule: ttl(1), change: null },
      { rule: ttl(7), change: null },
    ])
  })
  it('added, removed, changed against the previous snapshot', () => {
    const prev = [mpu, ttl(1), test, { ...ttl(7), Status: 'Disabled' }]
    const cur = [gc, mpu, ttl(1), ttl(7)]
    expect(lifecycleDiff(prev, cur)).toEqual<LifecycleRow[]>([
      { rule: gc, change: 'new' },
      { rule: mpu, change: null },
      { rule: ttl(1), change: null },
      { rule: ttl(7), change: 'changed', prev: { ...ttl(7), Status: 'Disabled' } },
      { rule: test, change: 'removed' },
    ])
  })
  it('identical snapshots: no chips', () => {
    const rules = [mpu, ttl(1), ttl(14)]
    expect(lifecycleDiff(rules, rules).map(r => r.change)).toEqual([null, null, null])
  })
  it('a changed action (not just status) is `changed`, with the old rule kept', () => {
    const was = { ...ttl(2), Expiration: { Days: 3 } }
    expect(lifecycleDiff([was], [ttl(2)])).toEqual<LifecycleRow[]>([{ rule: ttl(2), change: 'changed', prev: was }])
  })
})

describe('parseLifecycle', () => {
  it('a bare Rules[] (scans before the multi-bucket job) is the primary bucket’s', () => {
    expect(parseLifecycle([ttl(1), mpu], 'marin-us-east-02a')).toEqual<LifecycleSnapshot>({ 'marin-us-east-02a': [ttl(1), mpu] })
  })
  it('a {bucket: Rules[]} map is taken as is', () => {
    const snap: LifecycleSnapshot = { 'marin-us-east-02a': [gc, mpu], 'hero-checkpoints': [mpu] }
    expect(parseLifecycle(snap, 'marin-us-east-02a')).toEqual(snap)
  })
  it('anything else throws', () => {
    expect(() => parseLifecycle('x', 'b')).toThrow('lifecycle.json: expected Rules[] or {bucket: Rules[]}')
    expect(() => parseLifecycle(null, 'b')).toThrow('lifecycle.json: expected Rules[] or {bucket: Rules[]}')
  })
})

describe('lifecycleDiffByBucket', () => {
  const P = 'marin-us-east-02a'
  const H = 'hero-checkpoints'
  it('diffs each bucket by rule ID within the bucket, in the current snapshot’s order', () => {
    const prev: LifecycleSnapshot = { [P]: [mpu, ttl(1)], [H]: [ttl(1)] }
    const cur: LifecycleSnapshot = { [P]: [mpu, ttl(1), gc], [H]: [{ ...ttl(1), Expiration: { Days: 2 } }] }
    expect(lifecycleDiffByBucket(prev, cur)).toEqual<BucketLifecycleRow[]>([
      { bucket: P, rule: gc, change: 'new' },
      { bucket: P, rule: mpu, change: null },
      { bucket: P, rule: ttl(1), change: null },
      { bucket: H, rule: { ...ttl(1), Expiration: { Days: 2 } }, change: 'changed', prev: ttl(1) },
    ])
  })
  it('a bucket new to the snapshot is all `new`; one only the previous scan had is all `removed`', () => {
    expect(lifecycleDiffByBucket({ [P]: [mpu] }, { [P]: [mpu], [H]: [ttl(7), ttl(1)] })).toEqual<BucketLifecycleRow[]>([
      { bucket: P, rule: mpu, change: null },
      { bucket: H, rule: ttl(1), change: 'new' },
      { bucket: H, rule: ttl(7), change: 'new' },
    ])
    expect(lifecycleDiffByBucket({ [P]: [mpu], [H]: [ttl(7), ttl(1)] }, { [P]: [mpu] })).toEqual<BucketLifecycleRow[]>([
      { bucket: P, rule: mpu, change: null },
      { bucket: H, rule: ttl(1), change: 'removed' },
      { bucket: H, rule: ttl(7), change: 'removed' },
    ])
  })
  it('no previous snapshot: every row unchanged', () => {
    expect(lifecycleDiffByBucket(null, { [H]: [ttl(1)] })).toEqual<BucketLifecycleRow[]>([{ bucket: H, rule: ttl(1), change: null }])
  })
})
