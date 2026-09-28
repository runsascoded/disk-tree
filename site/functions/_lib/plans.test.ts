import { describe, expect, it } from 'vitest'
import { bucketOf, canonicalPrefix, covers, planBucket, PlanSpansBuckets, planStaging, prefixShape, relPrefix, uncovered } from './plans'

const P = 'marin-us-east-02a'
const H = 'hero-checkpoints'

describe('bucketOf', () => {
  it('reads the bucket off `s3://<b>/…`, `<b>/…` or a bare `<b>`', () => {
    expect(bucketOf(`s3://${H}/tmp/`)).toBe(H)
    expect(bucketOf(`${H}/tmp/ttl=14d/`)).toBe(H)
    expect(bucketOf(`/${H}/tmp/`)).toBe(H)
    expect(bucketOf(H)).toBe(H)
    expect(bucketOf(`s3://${P}/marin/`)).toBe(P)
  })
  it('anything else is the primary — including a dir that merely starts with a bucket’s name', () => {
    expect(bucketOf('marin/checkpoints/')).toBe(P)
    expect(bucketOf(`${H}-old/x/`)).toBe(P)
    expect(bucketOf('s3://rhoarnet-us-east-08a/x/')).toBe(P)
  })
})

describe('canonicalPrefix', () => {
  it('stores `s3://<bucket>/<rel>/` with the bucket the raw names', () => {
    expect(canonicalPrefix('marin/checkpoints')).toBe(`s3://${P}/marin/checkpoints/`)
    expect(canonicalPrefix(`s3://${P}/marin/checkpoints/`)).toBe(`s3://${P}/marin/checkpoints/`)
    expect(canonicalPrefix(`${H}/tmp/ttl=14d`)).toBe(`s3://${H}/tmp/ttl=14d/`)
    expect(canonicalPrefix(`s3://${H}/marin/`)).toBe(`s3://${H}/marin/`)
  })
  it('rejects the bucket root, `.`/`..` segments and backslashes', () => {
    expect(canonicalPrefix(`s3://${H}/`)).toBeNull()
    expect(canonicalPrefix('../x/')).toBeNull()
    expect(canonicalPrefix('a\\b/')).toBeNull()
  })
  it('relPrefix strips only the given bucket', () => {
    expect(relPrefix(`s3://${H}/tmp/`, H)).toBe('tmp/')
    expect(relPrefix(`s3://${H}/tmp/`, P)).toBe(`${H}/tmp/`)
  })
})

describe('planBucket', () => {
  it('one bucket: that bucket and the items relative to it', () => {
    expect(planBucket([`s3://${H}/tmp/ttl=14d/`, `s3://${H}/marin/old/`])).toEqual({ bucket: H, sweep: ['tmp/ttl=14d/', 'marin/old/'] })
    expect(planBucket([`s3://${P}/tmp/x/`])).toEqual({ bucket: P, sweep: ['tmp/x/'] })
  })
  it('no items: the primary', () => {
    expect(planBucket([])).toEqual({ bucket: P, sweep: [] })
  })
  it('two buckets: refused, naming both', () => {
    let err: unknown
    try { planBucket([`s3://${P}/tmp/x/`, `s3://${H}/tmp/y/`]) } catch (e) { err = e }
    expect(err).toBeInstanceOf(PlanSpansBuckets)
    expect((err as PlanSpansBuckets).buckets).toEqual([P, H])
    expect((err as Error).message).toBe(`plan spans buckets: ${P}, ${H}`)
  })
})

describe('prefixShape — the deployment\'s scheme + bucket set from [vars]', () => {
  const GCS = prefixShape({ STORE_SCHEME: 'gs://', STORE_BUCKETS: 'marin-us-central2, marin-us-east5,marin-eu-west4' })
  it('unset = the CoreWeave shape', () => {
    expect(prefixShape({})).toEqual({ scheme: 's3://', buckets: [P, H] })
    expect(prefixShape({ STORE_BUCKETS: ' , ' })).toEqual({ scheme: 's3://', buckets: [P, H] })
  })
  it('gcs: `gs://marin-<bucket>/<path>/`, the bucket read off the raw over the store\'s set', () => {
    expect(GCS).toEqual({ scheme: 'gs://', buckets: ['marin-us-central2', 'marin-us-east5', 'marin-eu-west4'] })
    expect(canonicalPrefix('gs://marin-us-east5/checkpoints/run/', GCS)).toBe('gs://marin-us-east5/checkpoints/run/')
    expect(canonicalPrefix('gs://marin-us-east5/checkpoints/run', GCS)).toBe('gs://marin-us-east5/checkpoints/run/')
    expect(canonicalPrefix('marin-eu-west4/x/', GCS)).toBe('gs://marin-eu-west4/x/')
    expect(bucketOf('gs://marin-eu-west4/x/', GCS.buckets)).toBe('marin-eu-west4')
  })
  it('an unknown bucket canonicalizes under the primary, as on cw', () => {
    expect(canonicalPrefix('gs://other/x/', GCS)).toBe('gs://marin-us-central2/other/x/')
  })
})

describe('covers / uncovered — the no-nesting rule', () => {
  it('a prefix covers itself and its descendants, not a sibling that shares its name as a prefix', () => {
    expect(covers('s3://b/a/', 's3://b/a/')).toBe(true)
    expect(covers('s3://b/a/', 's3://b/a/x/y/')).toBe(true)
    expect(covers('s3://b/a/', 's3://b/ab/')).toBe(false)
    expect(covers('s3://b/a/x/', 's3://b/a/')).toBe(false)
  })
  it('uncovered keeps only the outermost prefixes', () => {
    expect(uncovered(['s3://b/a/', 's3://b/a/x/', 's3://b/c/', 's3://b/a/y/z/'])).toEqual(['s3://b/a/', 's3://b/c/'])
    expect(uncovered([])).toEqual([])
  })
})

describe('planStaging — one gesture against the plan', () => {
  const have = ['s3://b/a/', 's3://b/c/d/']
  it('a new prefix under a staged one is covered; one over staged ones absorbs them', () => {
    expect(planStaging(have, ['s3://b/a/x/', 's3://b/c/', 's3://b/e/'])).toEqual({
      staged: ['s3://b/c/', 's3://b/e/'],
      covered: ['s3://b/a/x/'],
      absorbed: ['s3://b/c/d/'],
    })
  })
  it('re-staging an existing prefix is a no-op stage (kept, nothing absorbed)', () => {
    expect(planStaging(have, ['s3://b/a/'])).toEqual({ staged: ['s3://b/a/'], covered: [], absorbed: [] })
  })
  it('an empty plan stages everything', () => {
    expect(planStaging([], ['s3://b/a/'])).toEqual({ staged: ['s3://b/a/'], covered: [], absorbed: [] })
  })
})
