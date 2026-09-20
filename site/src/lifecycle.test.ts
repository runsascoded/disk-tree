import { describe, expect, it } from 'vitest'
import { describeRule, displayId, fromGcs, groupRows, lifecycleDiff, normalizeSnapshot, rulePrefix } from './lifecycle'
import type { GcsRule, GroupedRow, LifecycleRule, LifecycleRow } from './lifecycle'

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
  it('no previous snapshot: rows sorted by ID (numeric-aware), no changes', () => {
    expect(lifecycleDiff(null, [ttl(7), gc, ttl(14), ttl(1)])).toEqual<LifecycleRow[]>([
      { rule: gc, change: null },
      { rule: ttl(1), change: null },
      { rule: ttl(7), change: null },
      { rule: ttl(14), change: null },
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

// GCS rules: anonymous `{action, condition}` → S3-shaped rows with a content ID.
const gTtl = (days: number): GcsRule => ({ action: { type: 'Delete' }, condition: { age: days, matchesPrefix: [`tmp/ttl=${days}d/`] } })
const gCustom: GcsRule = { action: { type: 'Delete' }, condition: { daysSinceCustomTime: 0, matchesPrefix: ['scratch/compilation_cache/'] } }
const gCold: GcsRule = { action: { type: 'SetStorageClass', storageClass: 'COLDLINE' }, condition: { age: 90, matchesStorageClass: ['STANDARD'] } }
const gNoncurrent: GcsRule = { action: { type: 'Delete' }, condition: { isLive: false, numNewerVersions: 3 } }
const gMpu: GcsRule = { action: { type: 'AbortIncompleteMultipartUpload' }, condition: { age: 7 } }

describe('fromGcs', () => {
  it('maps a TTL rule onto Expiration + Filter, with a content ID', () => {
    expect(fromGcs(gTtl(14))).toEqual<LifecycleRule>({
      ID: 'delete age=14 matchesPrefix=tmp/ttl=14d/',
      Status: 'Enabled',
      Filter: { Prefix: 'tmp/ttl=14d/' },
      Expiration: { Days: 14 },
    })
    expect(describeRule(fromGcs(gTtl(14)))).toBe('expire objects after 14 d')
    expect(displayId(fromGcs(gTtl(14)))).toBe('delete age=14')
    expect(displayId(fromGcs(gNoncurrent))).toBe('delete isLive=false numNewerVersions=3')
    expect(displayId(ttl(14))).toBe('marin-ttl-14d')
  })
  it('words what S3 has no field for', () => {
    expect(fromGcs(gCustom)).toEqual<LifecycleRule>({
      ID: 'delete daysSinceCustomTime=0 matchesPrefix=scratch/compilation_cache/',
      Status: 'Enabled',
      Filter: { Prefix: 'scratch/compilation_cache/' },
      Extra: "0 d after the object's custom time",
    })
    expect(fromGcs(gCold)).toEqual<LifecycleRule>({
      ID: 'setstorageclass age=90 matchesStorageClass=STANDARD',
      Status: 'Enabled',
      Extra: 'move to COLDLINE after 90 d, in STANDARD',
    })
    expect(fromGcs(gNoncurrent)).toEqual<LifecycleRule>({
      ID: 'delete isLive=false numNewerVersions=3',
      Status: 'Enabled',
      Extra: 'when 3 newer versions exist',
    })
    expect(fromGcs(gMpu)).toEqual<LifecycleRule>({
      ID: 'abortincompletemultipartupload age=7',
      Status: 'Enabled',
      AbortIncompleteMultipartUpload: { DaysAfterInitiation: 7 },
    })
    expect(describeRule(fromGcs(gCustom))).toBe("0 d after the object's custom time")
  })
})

describe('normalizeSnapshot', () => {
  it('a bare list is one unnamed bucket; either cloud', () => {
    expect(normalizeSnapshot([ttl(1)])).toEqual([{ bucket: null, rules: [ttl(1)] }])
    expect(normalizeSnapshot([gTtl(1)])).toEqual([{ bucket: null, rules: [fromGcs(gTtl(1))] }])
  })
  it('a map is keyed by bucket, buckets sorted', () => {
    expect(normalizeSnapshot({ 'marin-us-west4': [gTtl(1), gCustom], 'marin-eu-west4': [gTtl(1)] })).toEqual([
      { bucket: 'marin-eu-west4', rules: [fromGcs(gTtl(1))] },
      { bucket: 'marin-us-west4', rules: [fromGcs(gTtl(1)), fromGcs(gCustom)] },
    ])
  })
})

describe('groupRows', () => {
  const t1 = fromGcs(gTtl(1)); const t14 = fromGcs(gTtl(14)); const custom = fromGcs(gCustom)
  it('a fleet-wide rule shows once with every bucket; a one-bucket rule with its bucket', () => {
    const perBucket = [
      { bucket: 'marin-us-west4', rows: lifecycleDiff(null, [t1, t14, custom]) },
      { bucket: 'marin-eu-west4', rows: lifecycleDiff(null, [t14, t1]) },
    ]
    expect(groupRows(perBucket)).toEqual<GroupedRow[]>([
      { rule: t1, change: null, buckets: ['marin-eu-west4', 'marin-us-west4'] },
      { rule: t14, change: null, buckets: ['marin-eu-west4', 'marin-us-west4'] },
      { rule: custom, change: null, buckets: ['marin-us-west4'] },
    ])
  })
  it('the same rule with different changes stays two rows; removed rows sort last', () => {
    const perBucket = [
      { bucket: 'a', rows: lifecycleDiff([t14, t1], [t1]) },   // t14 removed on a
      { bucket: 'b', rows: lifecycleDiff([t1], [t1, t14]) },   // t14 new on b
    ]
    expect(groupRows(perBucket)).toEqual<GroupedRow[]>([
      { rule: t1, change: null, buckets: ['a', 'b'] },
      { rule: t14, change: 'new', buckets: ['b'] },
      { rule: t14, change: 'removed', buckets: ['a'] },
    ])
  })
})
