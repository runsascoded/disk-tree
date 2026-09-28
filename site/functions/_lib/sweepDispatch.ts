// The gcs sweep dispatch's pure parts (api/sweep/dispatch.ts): the run dir,
// the `-b` cut a plan allows, and the executor script — ledger-sourced
// (`sweep manifest -S`, the marks ledger + the console's approvals) or
// plan-sourced (`--plan`, the staged set: specs/staged-delete.md,
// sweep-plan-union checkpoint 3).
export const SWEEP_RUNS = 'gs://oa-gcs-usage-dvx/sweep/runs'
export const runDir = (jobId: string): string => `${SWEEP_RUNS}/${jobId}`
export const planJsonPath = (jobId: string): string => `${runDir(jobId)}/plan.json`
/** `plan.json`'s object name in the data bucket (the JSON upload API's `name`). */
export const planJsonObject = (jobId: string): string => `sweep/runs/${jobId}/plan.json`

/** The buckets a plan-sourced run touches: the plan's, cut to `requested`
 * when the body names any. Empty = the request names none of the plan's. */
export const bucketCut = (planBuckets: readonly string[], requested: readonly string[]): string[] =>
  requested.length ? planBuckets.filter(b => requested.includes(b)) : [...planBuckets]

export interface SweepScript {
  mode: 'dry' | 'real'
  jobId: string
  buckets: readonly string[]
  /** Set for a plan-sourced run: the manifest reads this plan.json instead of the ledger. */
  plan?: string
}

/** The Batch container's bash: manifest then execute, both against the run dir. */
export const sweepScript = ({ mode, jobId, buckets, plan }: SweepScript): string => {
  const run = runDir(jobId)
  const bflags = buckets.map(b => `-b ${b}`).join(' ')
  const source = plan ? `--plan "${plan}" ` : '-S '
  return [
    'set -euo pipefail',
    `dt-cloud sweep manifest -d "$SWEEP_DATE" ${source}${bflags} -o "${run}"`,
    `dt-cloud sweep execute ${bflags} ${mode === 'real' ? '--for-real ' : ''}"${run}"`,
  ].join('\n')
}
