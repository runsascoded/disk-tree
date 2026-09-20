// Bucket lifecycle rules as the scan job snapshots them (`<store>/<scan>/
// lifecycle.json`: the S3 `Rules[]`, normalized and sorted by `ID`) — the
// pure parts of the home page's "Bucket lifecycle" fold: one human sentence
// per rule, and the added / removed / changed diff against the previous
// scan's snapshot.

export interface LifecycleRule {
  ID: string
  Filter?: { Prefix?: string }
  Status: string
  Expiration?: { Days?: number; ExpiredObjectDeleteMarker?: boolean }
  NoncurrentVersionExpiration?: { NoncurrentDays?: number }
  AbortIncompleteMultipartUpload?: { DaysAfterInitiation?: number }
}

/** A rule's actions in words, joined with ` + ` (objects, noncurrent
 * versions, delete markers, multipart uploads — the order S3 evaluates them
 * in is irrelevant to a reader; this order reads from common to rare). */
export function describeRule(r: LifecycleRule): string {
  const parts: string[] = []
  if (r.Expiration?.Days != null) parts.push(`expire objects after ${r.Expiration.Days} d`)
  if (r.NoncurrentVersionExpiration?.NoncurrentDays != null) parts.push(`expire noncurrent versions after ${r.NoncurrentVersionExpiration.NoncurrentDays} d`)
  if (r.Expiration?.ExpiredObjectDeleteMarker) parts.push('drop expired delete markers')
  if (r.AbortIncompleteMultipartUpload?.DaysAfterInitiation != null) parts.push(`abort incomplete multipart uploads after ${r.AbortIncompleteMultipartUpload.DaysAfterInitiation} d`)
  return parts.length ? parts.join(' + ') : 'no action'
}

export const rulePrefix = (r: LifecycleRule): string => r.Filter?.Prefix ?? ''

/** Rules per bucket. The job writes `{<bucket>: Rules[]}` since the
 * multi-bucket scan (specs/cw-multi-bucket.md §2); a bare `Rules[]` (earlier
 * scans) is the primary bucket's. */
export type LifecycleSnapshot = Record<string, LifecycleRule[]>

export function parseLifecycle(json: unknown, primary: string): LifecycleSnapshot {
  if (Array.isArray(json)) return { [primary]: json as LifecycleRule[] }
  if (json && typeof json === 'object') return json as LifecycleSnapshot
  throw new Error('lifecycle.json: expected Rules[] or {bucket: Rules[]}')
}

export type RuleChange = 'new' | 'removed' | 'changed' | null

export interface LifecycleRow {
  rule: LifecycleRule
  change: RuleChange
  /** The previous scan's version of a `changed` rule. */
  prev?: LifecycleRule
}

export interface BucketLifecycleRow extends LifecycleRow {
  bucket: string
}

/** What a rule's row compares by: prefix, status and the described action —
 * two rules that read the same are the same. */
const key = (r: LifecycleRule): string => JSON.stringify([rulePrefix(r), r.Status, describeRule(r)])
const byId = (a: LifecycleRule, b: LifecycleRule): number => a.ID.localeCompare(b.ID)

/** Rows for the current snapshot (sorted by ID) — each marked `new` /
 * `changed` against `prev` — followed by `prev`'s rules that are gone, marked
 * `removed`. With no previous snapshot (`null`) every change is `null`. */
export function lifecycleDiff(prev: LifecycleRule[] | null, cur: LifecycleRule[]): LifecycleRow[] {
  const before = new Map((prev ?? []).map(r => [r.ID, r]))
  const rows: LifecycleRow[] = [...cur].sort(byId).map(rule => {
    if (!prev) return { rule, change: null }
    const p = before.get(rule.ID)
    if (!p) return { rule, change: 'new' }
    return key(p) === key(rule) ? { rule, change: null } : { rule, change: 'changed', prev: p }
  })
  const now = new Set(cur.map(r => r.ID))
  for (const rule of [...(prev ?? [])].sort(byId)) if (!now.has(rule.ID)) rows.push({ rule, change: 'removed' })
  return rows
}

/** `lifecycleDiff` per bucket, in `cur`'s bucket order, then the buckets only
 * `prev` has (every rule `removed`). Rule IDs are compared within a bucket —
 * Marin's `marin-ttl-<N>d` rules exist in every bucket. */
export function lifecycleDiffByBucket(prev: LifecycleSnapshot | null, cur: LifecycleSnapshot): BucketLifecycleRow[] {
  const rows: BucketLifecycleRow[] = []
  for (const [bucket, rules] of Object.entries(cur)) {
    for (const row of lifecycleDiff(prev ? prev[bucket] ?? [] : null, rules)) rows.push({ bucket, ...row })
  }
  for (const [bucket, rules] of Object.entries(prev ?? {})) {
    if (bucket in cur) continue
    for (const rule of [...rules].sort(byId)) rows.push({ bucket, rule, change: 'removed' })
  }
  return rows
}
