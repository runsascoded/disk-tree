import { useQuery } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { MdAutoDelete } from 'react-icons/md'
import { describeRule, displayId, groupRows, lifecycleDiff, normalizeSnapshot, rulePrefix } from './lifecycle'
import type { GroupedRow, LifecycleRule, RawSnapshot } from './lifecycle'
import type { Store } from './stores'
import { Tooltip } from './Tooltip'

// The bucket lifecycle rules as the scan job snapshotted them
// (`<store.base>/<scan>/lifecycle.json`), diffed against the previous scan's
// snapshot: a `<details>` fold on the home page. One bucket (cw-s3) or a
// fleet keyed by bucket (gcs: six `marin-*` buckets) — a rule the whole
// fleet shares shows once, with the buckets it holds for. Scans from before
// the job started recording have no file — the fold says so rather than
// hiding.

const RECORDED_FROM = '2026-09-17'
const PREFIX_CLIP = 36

/** One scan's snapshot, normalized per bucket; `null` = no snapshot for that scan. */
function useLifecycle(store: Store, scan: string | null | undefined) {
  return useQuery<{ bucket: string | null; rules: LifecycleRule[] }[] | null>({
    queryKey: ['lifecycle', store.key, scan],
    enabled: !!scan,
    staleTime: Infinity,
    queryFn: async () => {
      const r = await fetch(`${store.base}/${scan}/lifecycle.json`)
      return r.ok ? normalizeSnapshot((await r.json()) as RawSnapshot) : null
    },
  })
}

/** Per-bucket diffs folded into one table. A single-bucket snapshot is the
 *  same table without the buckets column. */
function fold(cur: { bucket: string | null; rules: LifecycleRule[] }[], prev: { bucket: string | null; rules: LifecycleRule[] }[] | null | undefined): { rows: GroupedRow[]; keyed: boolean; nBuckets: number } {
  const before = new Map((prev ?? []).map(b => [b.bucket ?? '', b.rules]))
  const perBucket = cur.map(({ bucket, rules }) => ({ bucket: bucket ?? '', rows: lifecycleDiff(prev ? before.get(bucket ?? '') ?? null : null, rules) }))
  return { rows: groupRows(perBucket), keyed: cur.some(b => b.bucket != null), nBuckets: cur.length }
}

export function LifecycleFold({ store, asof, prevScan, note }: {
  store: Store
  asof: string | null | undefined
  /** The scan before `asof` in the store's list (the diff baseline), if any. */
  prevScan: string | null | undefined
  /** One line under the table — where this deployment tracks the intended state. */
  note?: ReactNode
}) {
  const curQ = useLifecycle(store, asof)
  const prevQ = useLifecycle(store, prevScan)
  const cur = curQ.data
  const { rows, keyed, nBuckets } = cur ? fold(cur, prevQ.data) : { rows: [], keyed: false, nBuckets: 0 }
  const loading = !!asof && curQ.isPending
  const nRules = rows.filter(r => r.change !== 'removed').length
  const title = cur
    ? `${nRules} rule${nRules === 1 ? '' : 's'}${keyed ? ` across ${nBuckets} bucket${nBuckets === 1 ? '' : 's'}` : ''}`
    : loading ? 'loading…' : 'no snapshot'
  const bucketsCell = (b: string[]) => (b.length === nBuckets ? <i>all {nBuckets}</i> : b.join(', '))
  // A long prefix (a deep scratch path) clips with the whole value on hover, so the action column stays in view.
  const prefixCell = (p: string) => (p.length > PREFIX_CLIP ? <Tooltip content={<code className="elide-full">{p}</code>}><code className="clip">{p}</code></Tooltip> : <code>{p}</code>)
  return (
    <details className="prose fold lifecycle">
      <summary>
        <MdAutoDelete className="fold-icon" aria-hidden />
        <span><b>Bucket lifecycle</b> — {title}</span>
      </summary>
      {cur ? (
        <div className="tbl-scroll">
        <table className="worklist lifecycle-tbl">
          <thead>
            <tr><th>rule</th>{keyed && <th>buckets</th>}<th>prefix</th><th>action</th><th>status</th></tr>
          </thead>
          <tbody>
            {rows.map(({ rule, change, prev, buckets }) => (
              <tr key={`${rule.ID}:${change ?? ''}:${buckets.join(',')}`} className={change === 'removed' ? 'removed' : undefined}>
                <td className="id">
                  {displayId(rule)}
                  {change === 'new' && <span className="chip new">new</span>}
                  {change === 'removed' && <span className="chip removed">removed</span>}
                  {change === 'changed' && prev && (
                    <Tooltip content={<>
                      Previous scan: <b>{describeRule(prev)}</b>
                      {prev.Status !== rule.Status && <> · {prev.Status}</>}
                      {rulePrefix(prev) !== rulePrefix(rule) && <> · <code>{rulePrefix(prev) || 'whole bucket'}</code></>}
                    </>}>
                      <span className="chip changed">changed</span>
                    </Tooltip>
                  )}
                </td>
                {keyed && <td className="buckets">{bucketsCell(buckets)}</td>}
                <td>{rulePrefix(rule) ? prefixCell(rulePrefix(rule)) : <i>whole bucket</i>}</td>
                <td>{describeRule(rule)}</td>
                <td>{rule.Status}</td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      ) : (
        <p className="tab-note">{loading ? 'loading…' : <>no snapshot for this scan (recorded from {RECORDED_FROM} on)</>}</p>
      )}
      {cur && prevScan && prevQ.data === null && <p className="tab-note">Changes aren’t shown: the previous scan has no snapshot.</p>}
      {note && <p className="tab-note">{note}</p>}
    </details>
  )
}
