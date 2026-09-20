import { useQuery } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { describeRule, lifecycleDiffByBucket, parseLifecycle, rulePrefix } from './lifecycle'
import type { LifecycleSnapshot } from './lifecycle'
import type { Store } from './stores'
import { Tooltip } from './Tooltip'

// Each bucket's lifecycle rules as the scan job snapshotted them
// (`<store.base>/<scan>/lifecycle.json`), diffed per bucket against the
// previous scan's snapshot: a `<details>` fold under About. Scans from before
// the job started recording (the first with a file is 2026-09-17T0001) have
// no file — the fold says so rather than fetching a 404 for them.

const RECORDED_FROM = '2026-09-17'

/** One scan's rules; `null` = no snapshot for that scan (404 or any non-OK). */
function useLifecycle(store: Store, scan: string | null | undefined) {
  return useQuery<LifecycleSnapshot | null>({
    queryKey: ['lifecycle', store.key, scan],
    // Snapshots exist from the recorded-from date on; earlier scans would only 404.
    enabled: !!scan && scan >= RECORDED_FROM,
    staleTime: Infinity,
    queryFn: async () => {
      const r = await fetch(`${store.base}/${scan}/lifecycle.json`)
      return r.ok ? parseLifecycle(await r.json(), store.buckets[0]) : null
    },
  })
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
  const rules = curQ.data
  const rows = rules ? lifecycleDiffByBucket(prevQ.data ?? null, rules) : []
  const loading = !!asof && curQ.isPending
  const nRules = rules ? Object.values(rules).reduce((n, rs) => n + rs.length, 0) : 0
  const nBuckets = rules ? Object.keys(rules).length : 0
  const title = rules
    ? `${nRules} rule${nRules === 1 ? '' : 's'}${nBuckets > 1 ? ` · ${nBuckets} buckets` : ''}`
    : loading ? 'loading…' : 'no snapshot'
  return (
    <details className="prose fold lifecycle">
      <summary>
        <span><b>Bucket lifecycle</b> — {title}</span>
      </summary>
      {rules ? (
        <div className="tbl-scroll">
        <table className="worklist lifecycle-tbl">
          <thead>
            <tr>{nBuckets > 1 && <th>bucket</th>}<th>rule</th><th>prefix</th><th>action</th><th>status</th></tr>
          </thead>
          <tbody>
            {rows.map(({ bucket, rule, change, prev }) => (
              <tr key={`${bucket}/${rule.ID}:${change ?? ''}`} className={change === 'removed' ? 'removed' : undefined}>
                {nBuckets > 1 && <td className="id">{bucket}</td>}
                <td className="id">
                  {rule.ID}
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
                <td>{rulePrefix(rule) ? <code>{rulePrefix(rule)}</code> : <i>whole bucket</i>}</td>
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
      {rules && prevScan && prevQ.data === null && <p className="tab-note">Changes aren’t shown: the previous scan has no snapshot.</p>}
      {note && <p className="tab-note">{note}</p>}
    </details>
  )
}
