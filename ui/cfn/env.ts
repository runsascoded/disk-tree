/** Bindings the Pages Functions see (`wrangler.toml`). */
export interface Env {
  /** R2 bucket holding reduced scans: `<prefix><uuid>.parquet` + `.scan.json`. */
  SCANS: R2Bucket
  /** Key prefix within the bucket (with trailing slash). */
  SCANS_PREFIX?: string
  /** Grants + access log for the gate (spec `specs/done/pages-auth.md`). */
  DB?: D1Database
  /** HMAC key for session cookies (Pages secret). Localhost gets a fixed dev value. */
  SESSION_SECRET?: string
  /** Comma/space-separated emails that SSO admits — they get `view` + `admin` (Pages secret). */
  ALLOWED_EMAILS?: string
  /** `https://<team>.cloudflareaccess.com`: the Zero Trust org whose JWTs `/auth/sso` accepts. */
  ACCESS_TEAM_DOMAIN?: string
  /** The Access application's AUD tag. */
  ACCESS_AUD?: string
}

export const scansPrefix = (env: Env): string => env.SCANS_PREFIX ?? ''
