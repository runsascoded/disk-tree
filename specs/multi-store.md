# Multi-store deploys: secondary stores at their own URL paths (first user: a `/meta` self-scan)

Status: **proposed** (2026-09-29, from the cw-s3 session; Ryan: "(a) multi-store base sg … yes spec it, this is a good direction").

## Motivation

A deploy today serves exactly **one store**: the build picks `STORE` (`wrangler.toml` `[vars]`, read by `vite.config.ts`), and the Functions' per-scan tables and caches are keyed by scan id alone. Ryan wants cw-s3.oa.dev to also show a treemap of **our own storage**, meaning the scan and index data the deployment writes, at a separate URL tree (`/meta`) on its own schedule (daily).

Data points for the first secondary store, measured 2026-09-29:

- `gs://oa-gcs-usage-dvx` ≈ 5.4 TiB: `listing/` 2.45 TiB and `access/` 2.43 TiB (gcs), `cw-l2/` 308 GiB (cw; 100 scans ≈ 3.1 GiB each, of which ≈ 3.6 GiB raw per-object listing vs 0.56 GiB indexes on the latest), plus about 75 GiB of leftovers (`central2-listing/` 27, `scratch/` 21, `listing-archive/` 15, `sweep/` 12).
- R2 `oa-cw-s3-usage-index`: 308 GiB / 4,343 objects, a full mirror of `cw-l2/` plus `snapshots/`. `wrangler r2 bucket info` reports 0 B (stale metric); count via the S3 API.

Self-reference is fine: each meta scan sees the previous meta scan's output, which is honest and small. Nothing about scanning the deploy's own bucket needs special handling beyond keeping meta out of the primary store's views.

## Relationship to `federated-scans.md`

That spec's phase 2 is a **union** of N locations into one Map. This one is its sibling: N **separate** stores, each with its own root, scans, schedule and URL path, served by one deploy. Design both on the same `{store, prefix, creds}` location shape so a secondary store can itself later be a union.

## Design

1. **Store registry → per-deploy list.** `STORE` becomes the *primary* store (served at `/`, unchanged URLs). A new `STORES_EXTRA` var (e.g. `"meta"`) names secondary stores, each served under its `Store.path` (`/meta/...`). `src/stores.ts` already gives each store a `path` and `base`; the router mounts one app subtree per configured store. The primary's deep links must not change.
2. **Per-store data seam in the Functions.** Every data route resolves a store from the request: the path prefix for pages, and an explicit `store=<key>` param (default = primary) for `/api/*`, `/data/*`, `/v1/files/*`. Per-store config (today's single-store vars `SNAPSHOTS_SUBDIR`, `BASE_SCOPE`, `STORE_SCHEME`, `STORE_BUCKETS`, `STORE_PREFIXES`, `STORE_BUCKET`/creds, `ROOT_LABEL`) moves into a per-store config object. Suggested shape: a `STORES_JSON` var, with secrets referenced by name (`STORE_META_ACCESS_KEY_ID`, …). Today's single-store vars remain the primary's.
3. **D1: store-scoped index tables.** `index_schema` and `index_row_groups` get `store TEXT NOT NULL DEFAULT '<primary>'`, and the PK becomes `(store, date, variant)`. Otherwise `SELECT DISTINCT date FROM index_schema WHERE variant='path'` (`/api/series`, the scan list, …) would mix meta scans into the primary's history. Same for `pyramid_multiscans` (over-time groups) and any other scan-keyed table. `index-sync` / `sync_d1` take `--store`. Migration: add the column with the primary's key as default, rebuild the PK. **Mind `d1-enforces-foreign-keys`:** don't rebuild a table something references; test with `PRAGMA foreign_keys=ON` on a seeded copy.
4. **Caches.** `edgeCache` keys (`cacheKeyFor`) gain the store key. Otherwise `/api/series?path=` for a meta path could collide with a primary path of the same spelling. `CACHE_V`-style versioning is unchanged.
5. **Auth.** One gate for all stores in a deploy by default: same viewer policy, and each store declares its required scope. Meta is staff-only (`admin` or the staff domain), since it exposes both deployments' data layout. Staging, owners and executors are per-store flags already (`Store.staging`, `owners`, `executor`); meta has all three off.
6. **Ingestion.** The meta store is scanned by the same job image with different targets: `gs://oa-gcs-usage-dvx` via the GCS lister and the R2 bucket via `disk-tree bulk-list -s s3` against the R2 endpoint, as a two-root store (`cw-multi-bucket` style). Output goes to its own layer-2 prefix (`meta-l2/{scan}/`), snapshots to `snapshots/meta/`, and D1 rows with `store='meta'`. It runs on its **own daily Cloud Scheduler job**, independent of cw's 12-hourly one. It warms its own caches (`warm-cache` with the store param).
7. **Ownership.** The GCS bucket is shared (≈95% of its bytes are gcs's). Hosting meta on cw-s3.oa.dev is Ryan's call for now. Keep the store definition deploy-agnostic so gcs.oa.dev can mount it too, or instead.

## Phases

1. **Schema + seam (base):** the `store` column and PKs, `store=` resolution in every data Function (default = primary, so existing deploys are byte-identical), store in cache keys, and the per-store config object. Verify: existing gcs/cw/r2 behaviour unchanged (tests plus a dev-stack diff of `/api/series`, `/api/subtree`, `/api/diff` responses).
2. **Frontend mount:** secondary stores routed under `Store.path`, the store switcher, and the `STORES_EXTRA` build var.
3. **Meta store for cw:** the job targets, scheduler, first scan, `wrangler.toml` config, then CIC `cw-s3.oa.dev/meta`.
4. **(later)** Converge with `federated-scans.md` phase 2: a store may be a union of locations.

## Open questions

- Route shape for secondary-store APIs: a `store=` param (proposed; minimal churn) vs a path prefix (`/meta/api/...`).
- Whether the meta store should also cover the D1 database size and KV namespace sizes (not objects, but part of "our storage"), shown as synthetic roots.
