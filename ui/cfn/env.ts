/** Bindings the Pages Functions see (`wrangler.toml`). */
export interface Env {
  /** R2 bucket holding reduced scans: `<prefix><uuid>.parquet` + `.scan.json`. */
  SCANS: R2Bucket
  /** Key prefix within the bucket (with trailing slash). */
  SCANS_PREFIX?: string
}

export const scansPrefix = (env: Env): string => env.SCANS_PREFIX ?? ''
