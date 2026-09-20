# Union-of-roots view as the shared upstream base

Status: **in progress** (2026-09-20) — Phases 1–2 landed (breadcrumb registry + per-scheme `/r2`,`/gcs` landings); Phases 3 (Header nav) + 4 (contract/mgu adoption) pending.

## Goal

Make disk-tree `main`'s multi-root / union scan view the **canonical shared implementation** that the `gcs` and `cw-s3` deployments (mgu branches) consume with *minimal* per-branch code — "as simple as possible, no simpler." Each deployment should differ only in **which schemes it registers** and **its backend implementation of the API contract**; every view component is shared verbatim.

This is **not a port into disk-tree**. The union view already lives here and is *ahead* of gcs/cw:
- `ui/src/components/UnionTreemap.tsx` — the generalized navigable synthetic-root treemap (`{items, rootName}`; sums scanned children; each cell links via `uriToPath`). **disk-tree-only** — gcs/cw never had it (they have only an inline `BucketsTreemap` on `/s3`, and a plain table on `/`).
- Used by `ScanList.tsx` (`rootName="all scans"`, the `/` landing) and `S3BucketList.tsx` (`rootName="S3"`, the `/s3` page).
- `schemes.ts` is a single-file scheme↔route model (one-line to add a scheme), near-identical across all branches.

The work is therefore **removing the per-scheme special-casing** that would force gcs/cw to customize, pushing it into `schemes.ts` config + a stable API contract. The mgu branches (`gcs` == `cw-s3` == `factored/…` for this feature) are an older *subset*; treat them only as a reference for the (identical) scheme/breadcrumb conventions. mgu@cw-s3's "second root" is **data/config, not code**.

## Divergence points to eliminate (what forces per-branch code today)

1. **Breadcrumb hardcodes `s3`** — `ui/src/components/ScanDetails.tsx:103–105`:
   ```tsx
   routeType === 's3'
     ? <Link to="/s3">s3://</Link>
     : <span>{routeType}://</span>
   ```
   Only s3's top crumb navigates (to its `/s3` landing); `r2`/`gcs`/`ssh` render dead text, so drilling into a bucket has **no breadcrumb back up** to the union root — and any new scheme needs a breadcrumb edit. This is the gap behind the "frame it as a top-root `r2://` with a breadcrumb" ask.
2. **Header scheme-nav hardcodes `s3`** — `ui/src/components/Header.tsx:18,76`: `isS3Page = path.startsWith('/s3')` + a literal `<Link to="/s3/">`. The "buckets" nav entry + active-highlight exist only for s3.
3. **The scheme-landing page is s3-only** — `ui/src/components/S3BucketList.tsx` hardcodes `s3://` throughout (scan input adornment, `startScan(\`s3://…\`)`, bucket-name parsing, `items=…path:\`s3://\${b.name}\``). gcs/cw cannot get an equivalent `/gcs` / `/cw` bucket-list landing without forking this component.

## Design — a scheme-descriptor registry in `schemes.ts`

Add one declarative table; `Header`, `Breadcrumbs`, and the generalized bucket-list page read from it. Adding a scheme stays a one-line edit; nothing scheme-specific survives in the view components.

```ts
export interface SchemeDesc {
  scheme: RouteType            // 'file' | 's3' | 'gcs' | 'r2' | 'ssh'
  label: string                // breadcrumb/nav label, e.g. 'r2://', '/'
  landing: string | null       // path the top crumb + nav link target: a dedicated
                               // bucket-list page ('/s3') if it has one, else '/'
                               // (the union root), else null (no up-link)
  liveScan: boolean            // startScan reachable (drives S3BucketList's scan UI)
  // delete stays supportsDelete(); could fold in here later
}
export const SCHEMES: Record<RouteType, SchemeDesc>
export function schemeLanding(rt: RouteType): string | null  // SCHEMES[rt].landing
```

- **Breadcrumb** becomes scheme-agnostic:
  ```tsx
  const up = schemeLanding(routeType)
  {!isFile && (up ? <Link to={up}>{label}</Link> : <span>{routeType}://</span>)}
  ```
  For this R2 deploy `schemeLanding('r2') === '/'`, so `r2://` links up to the union landing — the whole screenshot ask, and correct for gcs/ssh/any future scheme with zero edits.
- **Header** renders a nav link per scheme whose `landing` is a dedicated page (not `/`), highlighting by `path.startsWith(landing)` — no `isS3Page`.
- **Per-scheme landing page** (decided 2026-09-20): each cloud scheme this deployment supports gets a dedicated landing at `/<scheme>` (`/r2` = the R2 buckets, `/gcs` = GCS) so a *multi-cloud* deployment can disambiguate; a *single*-cloud deployment (mgu gcs/cw-s3) that needs no disambiguator sets its one scheme's `landing` to `/`. That page is **`ScanList` filtered to the scheme** (`<ScanList scheme="r2">`) — scans-derived, so it works on the **static demo** (which has no live cloud lister; `/api/scans` is all it needs) and reuses the existing union treemap verbatim. `s3` is the exception: it keeps the richer **live** `S3BucketList` (lists *all* account buckets incl. unscanned, with scan buttons) at `/s3` — a local-Flask capability the demo disables (`caps.s3 === false`). So the per-scheme *component* varies by live-lister capability, not by scheme; `S3BucketList` is not generalized (no live r2/gcs lister exists to generalize toward).

## Breadcrumb: super-root navigability

- Top scheme crumb links to the scheme's `landing` (`r2://` → `/r2`, `gcs://` → `/gcs`, `s3://` → `/s3`); a scheme with `landing: '/'` links to the union root instead.
- Fuller two-tier crumb (`all ▸ scheme ▸ bucket ▸ path`, where `all`→`/` and the scheme crumb→its landing) is deferred to **Multi-account** below; not needed while a deploy is effectively single-scheme.

## API contract (the backend seam)

The union landing is driven entirely by JSON; each deployment implements it in its own stack, and that is the *only* backend delta:
- `GET /api/scans` → `[{ path|uri, size, n_children, n_desc, time }]` — newest scan per root.
- `GET /api/s3/buckets` (for deployments with a dedicated bucket-list page) → `[{ name, size, last_scanned, … }]`.
- disk-tree `main`: Cloudflare Pages Functions (`ui/functions/api/scans.ts` → `latestPerPath(getScans(env))`, `ui/cfn/manifests.ts`). gcs/cw: Python Flask (`src/disk_tree/server.py` `get_scans` / `list_s3_buckets`).
- The **super-root has no server row**: `UnionTreemap` sums child sizes client-side, and there is **no per-super-root scan time**. Per-root scan time stays a per-row field (`Scan.time`).

## Non-goal — scan-time skew

Roots are scanned by separate jobs (even one nightly cron scans buckets sequentially; a 920K-object `ctbk` list takes minutes, `crashes` seconds), so they are never byte-synchronous. That is fine: summing sizes across roots is meaningful for a usage overview (unlike a *diff*, which needs a common baseline). With all roots on the same cron there is **no need to distinguish or surface per-root scan time** in the union view — the union root simply aggregates. No per-root scan-time UX.

## Per-branch delta (the target)

| deployment | schemes registered | backend | dedicated bucket-list page |
|---|---|---|---|
| disk-tree `main` (R2, multi-cloud) | `file, r2` (+ gcs/s3/ssh as added) | CF Pages Functions | `/r2`, `/gcs` (scans-filtered `ScanList`) |
| mgu `gcs` (single-cloud) | `file, gcs` | Flask | none — `gcs → /` |
| mgu `cw-s3` (single-cloud) | `file, s3` | Flask | `/s3` (live `S3BucketList`), or `s3 → /` |

All view components (`UnionTreemap`, `ScanList`, `SchemeBucketList`, `Breadcrumbs`, `Header`) shared verbatim; only the `SCHEMES` registry entries + backend differ.

## Deferred — multi-account tier

A true `all ▸ scheme ▸ account ▸ bucket ▸ path` hierarchy (mgu@cw-s3's "second root" generalized to *accounts within a scheme*) is net-new in **all** branches. It earns its keep only when one deployment serves >1 account/scheme. Build it on `UnionTreemap` + an account grouping in the descriptor when needed; scan-time skew stays a non-goal.

## Phased implementation

1. **Breadcrumb + `schemeLanding()`** — **DONE 2026-09-20** (`5502957`+): added `SchemeDesc`/`SCHEMES`/`schemeLanding` to `schemes.ts`; rewrote the `ScanDetails.tsx` top crumb to read the registry. Verified (CIC): `gcs://` (and `r2://`, same path) top crumb now `href="/"`, `s3://` still `/s3`; `tsc -b` clean. `ScanList` `rootName` kept `"all scans"` — the `/` union root is scheme-agnostic, so that label is correct (not `r2://`).
2. **Per-scheme landings** — **DONE 2026-09-20**: `SCHEMES` cloud landings → `/<scheme>` (`r2 → /r2`, `gcs → /gcs`, `s3 → /s3`, `ssh → /`); `ScanList` gained an optional `scheme` prop (filters `/api/scans` to `<scheme>://`, scheme-named root/title); `App.tsx` routes `/r2`, `/gcs` → `<ScanList scheme=…>`. Verified (CIC): `/r2` renders the R2 buckets as `r2://`; `/gcs/b1/b` breadcrumb top crumb `href="/gcs"`; `tsc -b` clean.
3. **Header nav from the registry**: drop `isS3Page`; surface a nav link per cloud scheme that has scans (or is configured), from `SCHEMES`. *(pending — needs the "which schemes to surface" call: data-driven by present scans vs configured list.)*
4. **Contract doc + mgu adoption**: freeze the `/api/scans` (+ live `/api/s3/buckets`) shapes; gcs/cw drop their table-only landing and adopt `UnionTreemap` + the shared components, differing only in `SCHEMES` + backend. *(pending)*
