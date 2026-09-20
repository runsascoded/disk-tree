import { describe, expect, it } from 'vitest'
import { bucketOf, canonicalPrefix, planBuckets } from './plans'

describe('bucketOf', () => {
  it('names the marin bucket of a stored prefix, else null', () => {
    expect(bucketOf('gs://marin-us-east5/checkpoints/run/')).toBe('marin-us-east5')
    expect(bucketOf('gs://marin-eu-west4/')).toBe('marin-eu-west4')
    expect(bucketOf('gs://other-bucket/x/')).toBe(null)
    expect(bucketOf('s3://marin-us-east5/x/')).toBe(null)
  })
})

describe('canonicalPrefix', () => {
  it('accepts a marin dir prefix and adds a missing trailing slash', () => {
    expect(canonicalPrefix('gs://marin-us-east5/checkpoints/run/')).toBe('gs://marin-us-east5/checkpoints/run/')
    expect(canonicalPrefix('  gs://marin-us-east5/checkpoints/run  ')).toBe('gs://marin-us-east5/checkpoints/run/')
    expect(canonicalPrefix('gs://marin-us-central2/')).toBe('gs://marin-us-central2/')
  })
  it('rejects non-marin, non-gs, and over-long prefixes', () => {
    expect(canonicalPrefix('gs://other/x/')).toBe(null)
    expect(canonicalPrefix('s3://marin-us-east5/x/')).toBe(null)
    expect(canonicalPrefix('marin-us-east5/x/')).toBe(null)
    expect(canonicalPrefix('gs://marin-us-east5/' + 'a'.repeat(1100) + '/')).toBe(null)
  })
})

describe('planBuckets', () => {
  it('the distinct buckets a plan spans, sorted; gcs does not refuse multi-bucket', () => {
    expect(planBuckets([
      'gs://marin-us-east5/a/',
      'gs://marin-us-east5/b/',
      'gs://marin-eu-west4/c/',
    ])).toEqual(['marin-eu-west4', 'marin-us-east5'])
    expect(planBuckets([])).toEqual([])
  })
})
