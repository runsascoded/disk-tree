import { describe, expect, it } from 'vitest'
import type { Env } from './auth'
import { requireViewer } from './auth'
import { cacheKeyFor } from './edgeCache'
import { indexDir, openIndex, storeTarget, storeCreds } from './index'
import { overTimeDataset } from './overTime'
import { snapshotsPrefix } from './shared'
import { LENS_PRIMARY_ONLY, primaryOnly, secondaryStores, storeEnv, storeKey, withStore } from './stores'
import { sqliteD1 } from './testD1'
import { onRequestGet as owners } from '../api/owners'
import { onRequestGet as subtree } from '../api/subtree'

// The multi-store seam (specs/multi-store.md phase 1): no `store=` is the
// primary exactly as before; `store=<key>` reads only that store's config,
// D1 rows and cache keys.

const META = {
  scope: 'admin',
  vars: { ROOT_LABEL: 'our storage', SNAPSHOTS_SUBDIR: 'meta', STORE_PREFIXES: 'meta-l2/,snapshots/meta/' },
  secrets: { STORE_ACCESS_KEY_ID: 'STORE_META_ACCESS_KEY_ID', STORE_SECRET_ACCESS_KEY: 'STORE_META_SECRET_ACCESS_KEY' },
}
const STORES_JSON = JSON.stringify({ meta: META })

// The primary's single-store vars, plus the meta store's secrets by name.
const PRIMARY: Env = {
  BASE_SCOPE: 'cw',
  ROOT_LABEL: 'marin CoreWeave',
  SNAPSHOTS_SUBDIR: 'cw',
  STORE_PREFIXES: 'cw-l2/,snapshots/',
  GCS_HMAC_KEY_ID: 'gcs-id',
  GCS_HMAC_SECRET: 'gcs-secret',
  STORES_JSON,
  STORE_META_ACCESS_KEY_ID: 'meta-id',
  STORE_META_SECRET_ACCESS_KEY: 'meta-secret',
} as Env

const req = (qs: string) => new Request(`https://cw-s3.example.test/api/subtree?${qs}`)

describe('secondaryStores', () => {
  it('parses STORES_JSON; unset = none', () => {
    expect(secondaryStores(PRIMARY)).toEqual({ meta: META })
    expect(secondaryStores({} as Env)).toEqual({})
  })
  it('refuses a bad key, an unknown field, an unknown var', () => {
    const err = (json: unknown) => {
      try { secondaryStores({ STORES_JSON: JSON.stringify(json) } as Env) } catch (e) { return (e as Error).message }
      return null
    }
    expect(err({ primary: {} })).toBe("STORES_JSON: bad store key 'primary'")
    expect(err({ Meta: {} })).toBe("STORES_JSON: bad store key 'Meta'")
    expect(err({ meta: { bucket: 'x' } })).toBe('STORES_JSON.meta: unknown field(s) bucket')
    expect(err({ meta: { vars: { BASE_SCOPE: 'admin' } } })).toBe("STORES_JSON.meta.vars: unknown var 'BASE_SCOPE'")
    expect(err([])).toBe('STORES_JSON: want an object of store configs')
  })
})

describe('storeEnv', () => {
  it('clears every per-store var (and the GCS fallback creds), then applies the config and resolves secrets by name', () => {
    expect(storeEnv(PRIMARY, 'meta', META)).toEqual({
      BASE_SCOPE: 'cw',
      STORES_JSON,
      STORE_META_ACCESS_KEY_ID: 'meta-id',
      STORE_META_SECRET_ACCESS_KEY: 'meta-secret',
      ROOT_LABEL: 'our storage',
      SNAPSHOTS_SUBDIR: 'meta',
      STORE_PREFIXES: 'meta-l2/,snapshots/meta/',
      STORE_ACCESS_KEY_ID: 'meta-id',
      STORE_SECRET_ACCESS_KEY: 'meta-secret',
      STORE_KEY: 'meta',
      STORE_SCOPE: 'admin',
    })
  })
  it('the overlay drives the existing seams: snapshots dir, store target, creds, store key', () => {
    const env = storeEnv(PRIMARY, 'meta', META)
    expect([snapshotsPrefix(env), storeTarget(env), storeCreds(env), storeKey(env)]).toEqual([
      'snapshots/meta/',
      { endpoint: 'https://storage.googleapis.com', bucket: 'oa-gcs-usage-dvx', region: 'us-east1' },
      { accessKeyId: 'meta-id', secretAccessKey: 'meta-secret' },
      'meta',
    ])
    expect([snapshotsPrefix(PRIMARY), storeCreds(PRIMARY), storeKey(PRIMARY)]).toEqual([
      'snapshots/cw/',
      { accessKeyId: 'gcs-id', secretAccessKey: 'gcs-secret' },
      'primary',
    ])
  })
})

describe('withStore', () => {
  it('no `store=` (or `store=primary`, or empty) is the very same context', () => {
    for (const qs of ['date=2026-09-01', 'store=primary', 'store=']) {
      const ctx = { request: req(qs), env: PRIMARY }
      expect(withStore(ctx)).toBe(ctx)
    }
  })
  it('`store=meta` swaps in the overlay and keeps the request', () => {
    const request = req('store=meta')
    const out = withStore({ request, env: PRIMARY })
    expect(out).toEqual({ request, env: storeEnv(PRIMARY, 'meta', META) })
  })
  it('unknown / malformed / misconfigured stores are JSON errors', async () => {
    const res = async (qs: string, env: Env = PRIMARY) => {
      const r = withStore({ request: req(qs), env }) as Response
      return [r.status, await r.text()]
    }
    expect(await res('store=nope')).toEqual([404, '{"error":"unknown store \'nope\'"}\n'])
    expect(await res('store=Me%2Fta')).toEqual([400, '{"error":"bad store"}\n'])
    expect(await res('store=meta', { STORES_JSON: '{' } as Env)).toEqual([500, `{"error":"STORES_JSON: ${(() => { try { JSON.parse('{') } catch (e) { return (e as Error).message } })()}"}\n`])
  })
  it('primaryOnly: null for the primary, a 404 for another store', async () => {
    expect(primaryOnly({ request: req('date=2026-09-01') })).toBe(null)
    const r = primaryOnly({ request: req('store=meta') })!
    expect([r.status, await r.text()]).toEqual([404, '{"error":"store \'meta\' has no ownership ledger (primary store only)"}\n'])
  })
})

describe('cache keys', () => {
  it('the primary’s are unchanged; a secondary store’s get an `@<store>/` segment', () => {
    expect(cacheKeyFor('series', 'a%2Fb?P=').url).toBe('https://series.cache/v2/a%2Fb?P=')
    expect(cacheKeyFor('series', 'a%2Fb?P=', 'primary').url).toBe('https://series.cache/v2/a%2Fb?P=')
    expect(cacheKeyFor('series', 'a%2Fb?P=', 'meta').url).toBe('https://series.cache/v2/@meta/a%2Fb?P=')
  })
  it('over-time manifest datasets: `over-time` for the primary, `<store>:over-time` else', () => {
    expect([overTimeDataset(PRIMARY), overTimeDataset(storeEnv(PRIMARY, 'meta', META))]).toEqual(['over-time', 'meta:over-time'])
  })
})

describe('D1 isolation', () => {
  // One scan id in both stores: each env reads only its own pointer and rows.
  const seeded = async () => {
    const { db, raw } = await sqliteD1('cw')
    raw.exec(`
      INSERT INTO index_schema (store, date, variant, version, schema_json, gen, dir) VALUES
        ('primary', '2026-09-01', 'path', 1, '[{"name":"schema"}]', 'g1', 'cw-l2/2026-09-01/index/g1'),
        ('meta', '2026-09-01', 'path', 2, '[{"name":"schema"}]', 'meta:g7', 'meta-l2/2026-09-01/index/g7'),
        ('meta', '2026-09-02', 'path', 2, '[{"name":"schema"}]', 'meta:g8', 'meta-l2/2026-09-02/index/g8');
      INSERT INTO index_row_groups (store, date, variant, gen, rg, d_min, d_max, p_min, p_max, b_max, row_start, row_end, rg_json) VALUES
        ('primary', '2026-09-01', 'path', 'g1', 0, 0, 1, 'a', 'b', 1, 0, 1, '[1,"ZSTD",[]]'),
        ('meta', '2026-09-01', 'path', 'meta:g7', 0, 0, 1, 'a', 'b', 1, 0, 1, '[1,"ZSTD",[]]');
    `)
    return { primary: { ...PRIMARY, DB: db } as Env, meta: storeEnv({ ...PRIMARY, DB: db } as Env, 'meta', META) }
  }

  it('indexDir reads the env’s own store', async () => {
    const { primary, meta } = await seeded()
    expect(await Promise.all([indexDir(primary, '2026-09-01'), indexDir(meta, '2026-09-01'), indexDir(primary, '2026-09-02'), indexDir(meta, '2026-09-02')])).toEqual([
      'cw-l2/2026-09-01/index/g1',
      'meta-l2/2026-09-01/index/g7',
      null,
      'meta-l2/2026-09-02/index/g8',
    ])
  })

  it('openIndex handles are per store (same scan id, separate pointers and memo entries)', async () => {
    const { primary, meta } = await seeded()
    const [a, b] = await Promise.all([openIndex(primary, '2026-09-01'), openIndex(meta, '2026-09-01')])
    expect([a.mode, a.gen, a.version, b.mode, b.gen, b.version]).toEqual(['d1', 'g1', 1, 'd1', 'meta:g7', 2])
  })
})

describe('handlers', () => {
  it('the ownership endpoints refuse a secondary store before any work', async () => {
    const r = await owners({ request: new Request('https://cw-s3.example.test/api/owners?date=2026-09-01&store=meta'), env: PRIMARY })
    expect([r.status, await r.text()]).toEqual([404, '{"error":"store \'meta\' has no ownership ledger (primary store only)"}\n'])
  })
  it('a data endpoint 404s an unknown store, and refuses the user lens on a secondary one', async () => {
    const get = async (qs: string) => {
      const r = await subtree({ request: req(qs), env: PRIMARY })
      return [r.status, await r.text()]
    }
    expect(await get('date=2026-09-01&store=nope')).toEqual([404, '{"error":"unknown store \'nope\'"}\n'])
    expect(await get('date=2026-09-01&store=meta&lens=user:ann')).toEqual([400, LENS_PRIMARY_ONLY])
  })
  it('a secondary store’s `scope` gates its viewers on top of the deployment’s', async () => {
    // A public deploy: anonymous viewers hold the base scope only.
    const pub = { ...PRIMARY, PUBLIC_READ: '1' } as Env
    const primaryId = await requireViewer({ request: req(''), env: pub })
    expect(primaryId).toEqual({ email: null, name: null, scopes: ['cw'], admin: false, via: 'public' })
    const r = await requireViewer({ request: req('store=meta'), env: storeEnv(pub, 'meta', META) }) as Response
    expect([r.status, await r.text()]).toEqual([401, '{"error":"unauthenticated"}\n'])
    // Localhost dev holds every scope, `admin` included.
    const dev = await requireViewer({ request: new Request('http://localhost/api/subtree?store=meta'), env: storeEnv(PRIMARY, 'meta', META) })
    expect(dev).toEqual({ email: 'dev@example.test', name: null, scopes: ['gcs', 'cw', 'admin', 'requests'], admin: true, via: 'session' })
  })
})
