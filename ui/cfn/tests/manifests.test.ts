import { describe, expect, it } from 'vitest'
import { ancestors, findCovering, latestPerPath, loadScans, parentOf } from '../manifests'
import type { Scan, Store } from '../manifests'

const manifest = (path: string, time: string, blob: string, extra: Record<string, unknown> = {}) =>
  JSON.stringify({ format: 'disk-tree-scan', version: 1, path, time, blob, size: 10, n_children: 1, n_desc: 2, mtime: 5, error_count: null, error_paths: null, ...extra })

function memStore(files: Record<string, string>): Store {
  return {
    async keys(prefix) { return Object.keys(files).filter(k => k.startsWith(prefix)) },
    async text(key) { return files[key] },
  }
}

describe('loadScans', () => {
  it('reads every *.scan.json under the prefix, ids positional in (time, blob) order', async () => {
    const store = memStore({
      'p/b.parquet.scan.json': manifest('/x', '2026-01-02T00:00:00', 'b.parquet'),
      'p/a.parquet.scan.json': manifest('/x/y', '2026-01-01T00:00:00', 'a.parquet', { error_count: 2, error_paths: ['/x/y/z'] }),
      'p/a.parquet': 'not a manifest',
      'q/c.parquet.scan.json': manifest('/other', '2026-01-03T00:00:00', 'c.parquet'),
    })
    expect(await loadScans(store, 'p/')).toEqual([
      { id: 1, path: '/x/y', time: '2026-01-01T00:00:00', blob: 'a.parquet', error_count: 2, error_paths: '["/x/y/z"]', size: 10, n_children: 1, n_desc: 2, mtime: 5 },
      { id: 2, path: '/x', time: '2026-01-02T00:00:00', blob: 'b.parquet', error_count: null, error_paths: null, size: 10, n_children: 1, n_desc: 2, mtime: 5 },
    ])
  })
})

const scan = (id: number, path: string, time: string): Scan =>
  ({ id, path, time, blob: `${id}.parquet`, error_count: null, error_paths: null, size: null, n_children: null, n_desc: null, mtime: null })

describe('latestPerPath', () => {
  it('keeps the newest scan of each path, newest first', () => {
    const scans = [scan(1, '/a', '2026-01-01'), scan(2, '/a', '2026-01-03'), scan(3, '/b', '2026-01-02')]
    expect(latestPerPath(scans).map(s => s.id)).toEqual([2, 3])
  })
})

describe('parentOf / ancestors', () => {
  it('walks local paths to the root and stops', () => {
    expect(ancestors('/a/b/c')).toEqual(['/a/b/c', '/a/b', '/a', '/'])
    expect(parentOf('/')).toBeNull()
  })
  it('walks bucket URIs to the bucket root and stops', () => {
    expect(ancestors('r2://bkt/x/y')).toEqual(['r2://bkt/x/y', 'r2://bkt/x', 'r2://bkt'])
    expect(parentOf('r2://bkt')).toBeNull()
  })
})

describe('findCovering', () => {
  it('picks the freshest scan among the uri and its ancestors', () => {
    const scans = [scan(1, '/a/b', '2026-01-01'), scan(2, '/a', '2026-01-05'), scan(3, '/a/b', '2026-01-03'), scan(4, '/c', '2026-01-09')]
    expect(findCovering(scans, '/a/b/deep')?.id).toBe(2)  // the ancestor scan is newer than the exact one
    expect(findCovering(scans, '/a/b')?.id).toBe(2)
    expect(findCovering(scans, '/zzz')).toBeNull()
  })
})
