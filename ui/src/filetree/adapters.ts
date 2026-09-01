/** Adapters mapping `@rdub/file-tree`'s `Store` + `TreeSource` onto disk-tree's
 *  live Flask API — the reciprocal-dogfood backing for Half A (spec
 *  `half-a-adopt-filetree.md`). No new backend beyond `/api/files/get`: the
 *  listing and recursive sizes reuse `/api/scan` (which already does depth
 *  pushdown + fresher-child patching), history reuses `/api/scans/history`,
 *  and scan dispatch reuses `/api/scan/start` + `/status`.
 *
 *  disk-tree wire paths: a `Row`'s `path` is scan-root-relative, its `uri` is
 *  absolute. We root each adapter at an absolute `rootUri` and key everything
 *  store-relative off that, deriving keys from `uri` (robust to the path/uri
 *  split). See `specs/file-tree-integration.md` for the snake→camel contract.
 */
import type { Entry, GetResult, ListOptions, ListResult, Range, Store } from '@rdub/file-tree'
import { NotFoundError } from '@rdub/file-tree'
import type {
  ChildrenRequest, ScanJob, ScanRequest, Snapshot, TreeLevel, TreeNode, TreeSource,
} from '@rdub/file-tree/renderers/treeSource'

/** Subset of disk-tree's `Row` / scan-response shape we read. */
interface DTRow {
  path: string
  uri: string
  kind: 'file' | 'dir'
  size: number | null
  mtime: number | null
  mtime_mean?: number | null
  n_children: number | null
  n_desc: number | null
}
interface DTScan {
  root: DTRow
  children: DTRow[]
}

// ---- URI ⇄ store-relative-key helpers (rootUri may be '/' or 'scheme://host…') ----

function joinUri(root: string, rel: string): string {
  if (!rel) return root
  const r = root === '/' ? '' : root.replace(/\/+$/, '')
  return `${r}/${rel.replace(/^\/+/, '')}`
}

/** Store-relative key for an absolute `uri` under `root`. */
function relOf(uri: string, root: string): string {
  const base = root === '/' ? '/' : root.replace(/\/+$/, '') + '/'
  return uri.startsWith(base) ? uri.slice(base.length) : uri.replace(/^\/+/, '')
}

function baseName(uri: string): string {
  const t = uri.replace(/\/+$/, '')
  const i = t.lastIndexOf('/')
  return (i < 0 ? t : t.slice(i + 1)) || t
}

const trimApi = (b: string) => b.replace(/\/+$/, '')

/** `Store` over the live API: `list` ← `/api/scan?depth=1`, `get` ← `/api/files/get`. */
export function diskTreeStore(rootUri: string, apiBase = ''): Store {
  const api = trimApi(apiBase)
  return {
    async list(prefix: string, _opts?: ListOptions): Promise<ListResult> {
      const uri = joinUri(rootUri, prefix.replace(/\/+$/, ''))
      const res = await fetch(`${api}/api/scan?uri=${encodeURIComponent(uri)}&depth=1`)
      if (res.status === 404) throw new NotFoundError(prefix)
      if (!res.ok) throw new Error(`list ${prefix}: ${res.status}`)
      const data = (await res.json()) as DTScan
      const entries: Entry[] = (data.children ?? []).map(c => {
        const isDir = c.kind === 'dir'
        const entry: Entry = { key: relOf(c.uri, rootUri) + (isDir ? '/' : ''), isDir }
        if (!isDir && c.size != null) entry.size = c.size
        if (c.mtime != null) entry.lastModified = new Date(c.mtime * 1000).toISOString()
        return entry
      })
      return { entries }
    },

    async get(path: string, range?: Range): Promise<GetResult> {
      const uri = joinUri(rootUri, path)
      const headers: Record<string, string> = {}
      if (range) headers['Range'] = `bytes=${range.offset}-${range.offset + range.length - 1}`
      const res = await fetch(`${api}/api/files/get?path=${encodeURIComponent(uri)}`, { headers })
      if (res.status === 404) throw new NotFoundError(path)
      if (!res.ok && res.status !== 206) throw new Error(`get ${path}: ${res.status}`)
      const cr = res.headers.get('Content-Range')
      const totalSize = cr ? parseInt(cr.split('/')[1]!, 10) : undefined
      const contentType = res.headers.get('Content-Type') ?? undefined
      const out: GetResult = { bytes: new Uint8Array(await res.arrayBuffer()) }
      if (totalSize != null && Number.isFinite(totalSize)) out.totalSize = totalSize
      if (contentType) out.contentType = contentType
      return out
    },

    capabilities: { range: true },
    describe() {
      return rootUri
    },
    getUrl(path: string) {
      return `${api}/api/files/get?path=${encodeURIComponent(joinUri(rootUri, path))}`
    },
  }
}

function toNode(r: DTRow, rootUri: string): TreeNode {
  return {
    path: relOf(r.uri, rootUri),
    name: baseName(r.uri),
    kind: r.kind,
    size: r.size,
    nChildren: r.n_children ?? undefined,
    nDesc: r.n_desc ?? undefined,
    mtime: r.mtime ?? null,
    mtimeMean: r.mtime_mean ?? null,
  }
}

/** `TreeSource` over the live API: recursive sizes/history/scan-dispatch.
 *  `diff` is intentionally off for Phase 1 (CompareView stays custom; mapping
 *  `/api/compare`'s frontier payload is a later phase). */
export function diskTreeTreeSource(rootUri: string, apiBase = ''): TreeSource {
  const api = trimApi(apiBase)
  return {
    capabilities: { history: true, diff: false, scan: true, lazy: true },

    async children(req?: ChildrenRequest): Promise<TreeLevel> {
      const uri = joinUri(rootUri, req?.path ?? '')
      const qs = new URLSearchParams({ uri, depth: String(req?.depth ?? 1) })
      if (req?.snapshot) qs.set('scan_id', req.snapshot)
      const res = await fetch(`${api}/api/scan?${qs}`)
      if (!res.ok) throw new Error(`children ${uri}: ${res.status}`)
      const data = (await res.json()) as DTScan
      return {
        node: toNode(data.root, rootUri),
        children: (data.children ?? []).map(c => toNode(c, rootUri)),
        snapshot: req?.snapshot,
      }
    },

    async snapshots(): Promise<readonly Snapshot[]> {
      const res = await fetch(`${api}/api/scans/history?uri=${encodeURIComponent(rootUri)}`)
      if (!res.ok) return []
      const rows = (await res.json()) as { id: number; time: string; size: number | null }[]
      return rows.map(r => ({ id: String(r.id), time: r.time, size: r.size }))
    },

    async scan(req?: ScanRequest): Promise<ScanJob> {
      const res = await fetch(`${api}/api/scan/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: joinUri(rootUri, req?.path ?? '') }),
      })
      if (!res.ok) throw new Error(`scan: ${res.status}`)
      const data = (await res.json()) as { job_id?: string; id?: string; status?: string }
      return { id: String(data.job_id ?? data.id ?? ''), status: (data.status as ScanJob['status']) ?? 'pending' }
    },

    async scanStatus(id: string): Promise<ScanJob> {
      const res = await fetch(`${api}/api/scan/status/${encodeURIComponent(id)}`)
      if (!res.ok) throw new Error(`scanStatus ${id}: ${res.status}`)
      const data = (await res.json()) as { status?: string; items_found?: number; error?: string | null }
      const job: ScanJob = { id, status: (data.status as ScanJob['status']) ?? 'pending' }
      if (data.items_found != null) job.itemsFound = data.items_found
      if (data.error != null) job.error = data.error
      return job
    },
  }
}
