import { describe, expect, it } from 'vitest'
import { isFold, pathCopy, pathCrumbs, pathLead, pathText, pathUri } from './pathCrumbs'
import type { Crumb } from './pathCrumbs'

describe('pathCrumbs', () => {
  it('every ancestor drills; the deepest segment is inert', () => {
    expect(pathCrumbs(['checkpoints', 'run-1', 'step-100'])).toEqual<Crumb[]>([
      { name: 'checkpoints', segs: ['checkpoints'], drillable: true, last: false },
      { name: 'run-1', segs: ['checkpoints', 'run-1'], drillable: true, last: false },
      { name: 'step-100', segs: ['checkpoints', 'run-1', 'step-100'], drillable: false, last: true },
    ])
  })
  it('a folded `(other)` never drills, wherever it sits', () => {
    expect(pathCrumbs(['(other)', 'x'])).toEqual<Crumb[]>([
      { name: '(other)', segs: ['(other)'], drillable: false, last: false },
      { name: 'x', segs: ['(other)', 'x'], drillable: false, last: true },
    ])
    expect(isFold('(files)')).toBe(true)
    expect(isFold('files')).toBe(false)
  })
  it('the root has no crumbs', () => {
    expect(pathCrumbs([])).toEqual([])
  })
})

describe('pathUri', () => {
  it('joins the segments under the store’s scheme', () => {
    expect(pathUri('gs://', ['marin-us-central2', 'checkpoints'])).toBe('gs://marin-us-central2/checkpoints')
    expect(pathUri('s3://', ['hero-checkpoints'])).toBe('s3://hero-checkpoints')
    expect(pathUri('r2://', [])).toBe('r2://')
  })
  it('a directory prefix ends in `/`, except the root', () => {
    expect(pathUri('gs://', ['marin-us-central2', 'checkpoints'], true)).toBe('gs://marin-us-central2/checkpoints/')
    expect(pathUri('gs://', [], true)).toBe('gs://')
  })
})

describe('pathLead / pathText / pathCopy', () => {
  it('a file store reads as a plain absolute path', () => {
    expect([pathLead('file:///'), pathText('file:///', ['Applications', 'Slack.app']), pathText('file:///', ['Users', 'ryan', 'c'], true), pathText('file:///', [])])
      .toEqual(['/', '/Applications/Slack.app', '/Users/ryan/c/', '/'])
  })
  it('other schemes read as their URI', () => {
    expect([pathLead('gs://'), pathText('gs://', ['b', 'x'], true), pathText('r2://', [])]).toEqual(['gs://', 'gs://b/x/', 'r2://'])
  })
  it('copies a file store as an absolute path, other stores as the URI', () => {
    expect(pathCopy('file:///', ['Users', 'ryan', 'c'], true)).toBe('/Users/ryan/c/')
    expect(pathCopy('file:///', [])).toBe('/')
    expect(pathCopy('gs://', ['b', 'x'])).toBe('gs://b/x')
  })
})
