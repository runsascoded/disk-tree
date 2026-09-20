# disk-tree as the shared base for the cloud-usage viewers

Status: **in progress** (2026-09-20). Union view + single-cloud collapse landed (`786b5bb`). **Direction (2026-09-20): disk-tree `main` is the shared base; the public Map shell (`site/`) + ingestion (`cloud/`) get hoisted *into* this repo from mgu, and all public deploys build from it.** mgu is a fork of disk-tree (merge-base `109ab11`, 2026-08-19), so this is a **code move (CP/`mv`), not a from-scratch build.**

## Goal

disk-tree `main` is the **canonical base** for every cloud-storage-usage viewer:
- **r2.rbw.sh** — public R2 bucket viewer (ctbk, crashes, jc-taxes).
- **gcs.oa.dev / cw-s3.oa.dev** (mgu `site/`) — cloud-usage maps (auth-gated, owner/marks on gcs).
- a future **multi-cloud/local superset** (disky/blobby.rbw.sh, rename TBD) — the full scan-manager.

Each deploy differs only in **config** (schemes/buckets, capabilities, auth on/off, store), never in forked view code. Where disk-tree and mgu each have a *better* implementation of a feature, the base takes it and the other deploy **adopts** it — merge, not replace. Instance that surfaced this: **mgu's static OG → disk-tree's dynamic edge-rendered OG.**

## The key fact — mgu is a fork; its Map is two additive subtrees

`git merge-base main m/gcs` = `109ab11` (2026-08-19). Divergence: 199 disk-tree-only vs 642 mgu-only commits. The entire mgu Map app lives in **two directories disk-tree does not have** (purely additive — zero conflict on checkout):
- **`site/`** (207 files) — the public Map SPA + all CF Functions (`subtree`/`diff`/`series`/`estate`/…), the **D1-tiers** reader/index (`site/functions/_lib/index.ts`, `view.ts`), `SiteNav.tsx`, `stores.ts`. Imports `@rdub/treemap` + `@disk-tree/react` as **`workspace:*`** — already built to resolve the shared libs from a monorepo `packages/*` (which disk-tree is).
- **`cloud/`** (57 files) — the `dt_cloud` ingestion CLI (listing → build-index → `index-tiers` → `index-sync` → D1), built on `src/disk_tree`.

So bringing the Map into disk-tree is `git checkout m/gcs -- site/ cloud/` + `pnpm-workspace` add `"site"` + reconcile against disk-tree's (ahead) `packages/*`/`src/disk_tree`. **Not** designing `/api/subtree`, D1-tiers, or Map chrome — they exist.

## Already shared (the floor)

Both apps build on the same libraries — `@rdub/treemap` (core `Treemap`/`squarify`/layout/colors/diff) and `@disk-tree/react` (re-export + `StalenessScatter`/`AgeHistograms`/`BytesOverTime`). mgu `site/` and disk-tree `ui/` are two *shells* over this floor. The hoist brings the `site/` shell + `cloud/` ingestion home too, so the whole viewer base — not just the libs — lives in this repo.

## The two shells (both end up here)

| | disk-tree `ui/` → **superset** (disky) | mgu `site/` → **public Map** (r2/gcs/cw) |
|---|---|---|
| Chrome | scan-manager: Scans/Recent/Local/S3, live scan, delete | Map-first: ☰ menu, "all buckets" root, scope controls |
| Landing treemap | `UnionTreemap` — flat bucket leaves | "the Map" — deeply nested pixel-budget subtree |
| Read backend | per-`<uuid>` parquet, direct hyparquet footer reads | tiered parquets + **D1** footer index (`d1-tiers`) |
| OG | **dynamic** edge-rendered | static pre-gen → **adopts disk-tree's dynamic** |
| owners/marks/sweep, storage-class/write-time | — | ledger (gcs; off for cw) + per-node columns |

`ui/` is the superset shell; `site/` is the public shell. Both live in disk-tree; a deploy picks one.

## Backend — standardize on `d1-tiers` (decided)

Not a real fork. `direct` (disk-tree's no-D1 footer reads) buys only ops simplicity, which is moot: the gated deploy already runs a D1 (`disk-tree-auth`), so the footer index is new *tables*, not a new binding — and `d1-tiers` is required for gcs/marin scale (34M dirs) regardless. Two backends would violate "one base." Every deploy uses `site/`'s `d1-tiers` reader; r2.rbw.sh gains a D1 + the tier/index-sync ingestion (trivial at ctbk/crashes scale).

## Naming

`dt_cloud`'s `webdata` (listing → `path-index.parquet` + layer-3 JSONs) is a bad name — rename during the hoist. Candidates: `web-index` (pairs with `index-tiers`/`index-sync`) or `publish`. Final pick TBD (user's call).

## Per-deploy config (thin deltas)

| deploy | shell | schemes/buckets | auth | owners/marks | OG |
|---|---|---|---|---|---|
| r2.rbw.sh | `site/` (Map) | r2: ctbk, crashes, jc-taxes | public | off | dynamic |
| gcs.oa.dev | `site/` (Map) | gs://… | CF Access | on | dynamic (adopt) |
| cw-s3.oa.dev | `site/` (Map) | cw s3://… | CF Access | off | dynamic (adopt) |
| disky.rbw.sh | `ui/` (superset) | file+r2+gcs+s3+ssh | none/local | off | dynamic |

All `site/` code shared; only this row + wrangler vars/store config differ.

## Phased plan (mv-shaped)

1. **Hoist** `site/` + `cloud/` from `m/gcs` into disk-tree; add `"site"` to `pnpm-workspace`, `cloud/` to the python project; `pnpm i`, build, reconcile any `packages/*`/`src/disk_tree` API drift (par-construct only where the fork diverged). Goal: `site/` builds + serves against disk-tree's base libs.
2. **Generalize + go public for r2**: env-drive the hardcoded GCS bucket seam (`makeStore` endpoint/bucket, `index.ts:34,113`); add a public/anonymous mode to `requireViewer`/`scopesFor`; add an `r2` `stores.ts` row + wrangler vars; stand up `dt_cloud` ingestion (renamed) over ctbk/crashes → tiers + D1. Deploy `site/` at r2.rbw.sh.
3. **CP disk-tree's edge-OG into `site/`** (replace static OG); pull in diff-index if it beats mgu's `/api/diff`. In-place now that code is co-located.
4. **gcs/cw-s3 track this repo's `site/`** — deltas only (owner/marks config, their D1). Handoff spec into the mgu repo (`/Users/ryan/c/oa/marin-gcs-usage/specs/`) for an mgu-session.
5. **Superset** (`ui/` → disky/blobby): unchanged multi-cloud/local scan-manager. Rename TBD.

## Landed so far

- **Breadcrumb registry + per-scheme landings + single-cloud collapse** (`dab2d2e`, `786b5bb`): `SCHEMES` descriptor drives the breadcrumb/landing; on r2.rbw.sh `/` is the R2 union (`r2 → /`), `/scans` the scheme-agnostic list, `/r2` redirects; scheme-scoped `ScanList` strips its `<scheme>://` prefix; `ScanDetails` root cell/row shows the basename. These live in `ui/` (the superset shell) — the Map shell (`site/`) supersedes them for the public deploys once hoisted.

## Reconciliation risks (Phase 1)

- **Package API drift** — `site/` was built against mgu's `packages/*` (base + mgu commits); disk-tree's are base + 199 commits (dynamic-OG/diff work). `site/` consumes public API, so mostly fine, but expect a few adapt/par-construct spots. If mgu *added* to `packages/*` and disk-tree lacks it, CP that package delta too (it's a shared-core improvement).
- **`cloud/` ↔ `src/disk_tree`** — `dt_cloud` uses the engine; reconcile against disk-tree's version.
- **`@rdub/file-tree` github-pin** in `site/package.json` — carry it as-is.

## Deferred — multi-account tier

A true `all ▸ scheme ▸ account ▸ bucket ▸ path` hierarchy earns its keep only when a deployment serves >1 account/scheme; build it on the shared shell + a store grouping when needed. Scan-time skew stays a non-goal (roots scanned by separate jobs, never byte-synchronous; the map aggregates child sizes, no per-super-root scan time).
