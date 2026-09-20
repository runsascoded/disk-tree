import { describe, expect, it } from 'vitest'
import { bucketOf, canonicalPrefix, planBucket, PlanSpansBuckets, relPrefix } from './plans'

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
