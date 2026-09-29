# Multi-store deploys: secondary stores at their own URL paths (first user: a `/meta` self-scan)

Status: **phase 1 implemented on the base** (2026-09-29; not yet deployed or migrated anywhere); phases 2–3 open. Proposed 2026-09-29 from the cw-s3 session (Ryan: "(a) multi-store base sg … yes spec it, this is a good direction").

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

## Phase 1 as built

**D1** (`site/migrations/cw/0006_store_scoped_index.sql`, `site/migrations/gcs/0030_store_scoped_index.sql`, identical DDL):

- The primary's rows are `store = 'primary'`, a fixed sentinel, not the deploy's `STORE` key: the cw lineage serves both cw-s3 and the r2 demo, so a per-deploy default can't live in a shared migration. `'primary'` is reserved (not a valid `STORES_JSON` key); `store=primary` is accepted as a synonym for no param.
- `index_schema` is rebuilt (create / copy / drop / rename) with PK `(store, date, variant)`. It's small, and nothing `REFERENCES` it, so the rebuild can't trip D1's FK enforcement.
- `index_row_groups` is **not** rebuilt (deviation from design item 3). It's the multi-GB table: gcs's DROP COLUMN on it ran past D1's statement limit (gcs `0019`). It gets `store TEXT NOT NULL DEFAULT 'primary'` in place, an O(1) change, and its PK stays `(date, variant, gen, rg)`. Uniqueness across stores comes from the writer: `index-sync --store S` records a secondary store's generation as `S:<gen>` (`dt_cloud.index_footer.d1_gen`), in both the row groups and the pointer. The reader reaches row groups only through the store-scoped pointer's `gen`, so the hot row-group queries are unchanged. The writer's gc / retention / compact passes filter on `store`.
- `pyramid_multiscans` gets **no** column (deviation). pyrmts owns its DDL (`CREATE TABLE IF NOT EXISTS` at first sync; the gcs lineage never created it, so an `ALTER` there would fail on a fresh DB) and its read query (`MultiScanD1Index.listMultiScans(dataset)`). Its `(dataset, key)` PK already namespaces it: a secondary store's groups are dataset `<store>:over-time`, and the primary's stay `over-time` (`overTimeDataset` in `_lib/overTime.ts`, `multiscan_dataset` in `dt_cloud.overtime`).
- `owner_totals` (gcs; per-scan ownership cache) is left alone: the ownership surfaces are primary-only (below).
- Verified: `storeMigration.test.ts` applies each lineage from scratch with `PRAGMA foreign_keys=ON` (`sqliteD1(lineage, {before})`), seeds `plans` / `plan_items` / index rows, applies the migration, and asserts the exact rows, `foreign_key_check = []`, `integrity_check = ok`, the new PK columns, and that a second store's scan fits under the same id while a duplicate is refused. Both files were also applied with `wrangler d1 migrations apply --local` (miniflare's D1) over a seeded pre-migration DB, which enforces FKs as D1 does. Rows came through as `primary`, `foreign_key_check` was empty, and `foreign_keys` was 1.

**Rollout order** (each D1: gcs, cw-s3, the r2 demo and its dev twin): apply the migration **before** deploying the new Functions or running a job image with the new `dt-cloud`. Both now name `store` in their SQL, and the pre-migration schema has no such column.

**Functions** (`site/functions/_lib/stores.ts`):

- `withStore(ctx)`: no `store=` (or `store=primary`, or empty) returns the context object itself, so the primary's behaviour and responses are unchanged. Its D1 reads now say `store = 'primary'`, which returns the same rows once migrated. `store=<key>` returns a copy with an `Env` overlay. A malformed key gets 400, an unknown one 404, and a bad `STORES_JSON` 500.
- `STORES_JSON = {"<key>": {"scope"?, "vars"?, "secrets"?}}`. `vars` may set only the per-store vars: `ROOT_LABEL`, `SNAPSHOTS_SUBDIR`, `STORE_SCHEME`, `STORE_BUCKETS`, `STORE_PREFIXES`, `STORE_BUCKET`, `STORE_ENDPOINT`, `STORE_REGION`, `STORE_ACCESS_KEY_ID`, `STORE_SECRET_ACCESS_KEY`. `secrets` maps one of those vars to the **name** of the Pages secret holding its value. The overlay first clears all of them, plus the `GCS_HMAC_*` fallback, so nothing of the primary's leaks into a secondary store. Unknown fields or vars are refused.
- `BASE_SCOPE` stays deploy-wide (deviation from design item 2's var list). It's the identity policy's grant (`scopesFor`), so overlaying it would hand every allow-listed viewer the secondary store's scope. A store's `scope` is instead an **extra** scope that `requireViewer` demands on top of the viewer scope: `STORE_SCOPE` on the overlay, 401 for anonymous and 403 for signed-in.
- Wired into `/api/{subtree,diff,series,age,age-pyramid,path-index,bench,store}`, `/data/*` and `/v1/files/*`. On a secondary store, `/data/*` reads only `snapshotsPrefix(env)` (its own `snapshots/<sub>/`, never `rules.json`). `/v1/files?store=` is gated by `requireViewer` like the store's data; the primary's proxy keeps no gate of its own, unchanged (see open questions).
- Primary-only: `/api/{owners,estate,assignments,claims,actions}` return 404 for any other store, and `lens=user:` on subtree/diff/series returns 400. The ownership ledger and identity registry are the primary's. `slack/actions`' "latest scan" reads the primary's rows.
- Caches: `cacheKeyFor(ns, parts, store)` inserts `@<store>/` only for a secondary store. The primary's keys are byte-identical to before, so there is no one-time miss. The index-blob colo cache and the per-isolate memos (`openIndex` handles, the row-group LRU, the extras index) are keyed by store too.
- Tests: `stores.test.ts` (15 cases). Config parsing and refusals; the exact overlay; `withStore` returns the same object for the primary; the error bodies; cache keys and over-time datasets; D1 isolation (one scan id in two stores: `indexDir` / `openIndex` each see only their own pointer); and handler-level 404 / 400 / scope-gate responses.

**`dt-cloud`**: `-s/--store` (default `primary`) on `index-sync`, `index-gc`, `index-dir`, `over-time-groups` and `over-time-write`. Every index-writer statement names its store.

**Not done in phase 1**: the dev-stack diff of `/api/series` / `/api/subtree` / `/api/diff` before vs after. It needs a migrated D1, and every dev stack here points at a prod D1. The byte-identity argument rests on `withStore` returning the untouched context, unchanged primary cache keys, and `store = 'primary'` selecting every pre-existing row. `warm-cache` has no `--store` yet (phase 3).

## Open questions

- Route shape for secondary-store APIs: a `store=` param (proposed; minimal churn) vs a path prefix (`/meta/api/...`).
- Whether the meta store should also cover the D1 database size and KV namespace sizes (not objects, but part of "our storage"), shown as synthetic roots.
- `/v1/files` (the primary's) calls no `requireViewer`: it dates from the CF Access era, and both deployments are now off Zero Trust. Is it meant to be open? Phase 1 leaves the primary as it was and gates only `?store=`.
- Should `index_row_groups` eventually carry `store` in its PK, e.g. via a new table filled a `(date, variant)` at a time the way `index_groups` was retired? The gen namespacing makes that unnecessary for correctness.
