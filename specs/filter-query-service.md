# Filter query service: exact path filters at any scale

Status: proposed (2026-10-02). Owner: the gcs session. Lands on `cloud` (shared); deployments opt in.

## 1. Why

The path filter (`q=`) is served by the Pages Function that serves everything else: a Worker with ~128 MB of memory, limited CPU, and 6 concurrent subrequests. [`path-store-search.md`] made it exact on small stores (r2: 1.24M rows, every query exact, ≤ ~2 s). At gcs scale it can't be, and it says so (`partial`) instead of missing matches silently. Measured on gcs's 2026-10-01 generation with search sidecars (v1 layout, 778M rows, 127M distinct names, 5.2B trigram postings):

| query | trigram candidates | result |
|---|---:|---|
| `tomat` | 52,586 | 4 matches, `partial` (128 name groups + 1.5 s) |
| `grug` | 8,308 | 13 matches, `partial` (same) |
| `safetensors` | 1,327 | 0 matches, `partial` (96 path groups) |
| `ckpt -eval` | 166 | 0 matches, `partial` (1.5 s) |

Two limits, neither fixed by a layout change:

- **Candidate volume.** With 127M names, even a specific term shares its trigrams with thousands of names; verifying them is many reads. Layout v2 (name-major rows) cuts reads per candidate but not the number of candidates.
- **Match volume.** A file-name term (`safetensors`, `.json`) matches a file in nearly every run directory: millions of rows. The answer is a rollup of millions of rows. That is a scan-and-aggregate job, not a request-sized read.

## 2. What

A small query service, **`dt-query`**: a container running DuckDB over the same path-store files in GCS. It answers the filter view's two questions exactly:

1. **Match roots:** the outermost paths satisfying the query AST (OR of AND-groups, plus whole-query NOT; [`path-store-search.md`] §1.1), with each root's net aggregates (`b`, `o`, class mix, owners), after NOT subtraction.
2. **The drawn tree:** for a view root `P`, canvas `w×h` and `minArea`, every node under the match roots at or above the size threshold (the same threshold and attenuation as `view.ts`), and `(other)` remainders. This is the same `TreeNode` shape `/api/subtree` returns, so the client doesn't change.

Plus the diff (`/api/diff?q=`): the same two reads on both scans, joined, with `buildDiff`'s row semantics.

The Worker stays the front door. The service is a backend it calls only when its own read can't answer exactly within budget.

## 3. Request flow

1. The client calls `/api/subtree?q=…` as today.
2. The Worker parses `q` (the shared parser, so syntax errors stay 400s) and tries its own path: the index with budgets.
3. If the Worker's answer is exact (no `partial`, no `approximate`), it returns it. Selective queries on indexed scans stay sub-second and never touch the service.
4. Otherwise the Worker forwards the parsed AST (not the raw string) plus the view parameters to `dt-query`, and returns its answer, cached like any other view response (immutable per generation and parameters).
5. If `dt-query` is down or over its own deadline, the Worker returns its own flagged `partial` answer. Never silently short.

The client may show the Worker's partial first paint while the exact answer loads (the existing `firstPaint` mechanism), flagged as such.

## 4. The service

- **Runtime:** Cloud Run (gen2), Python + DuckDB, one container image built from `cloud/` (a `dt-cloud serve-query` command), so the AST evaluator is shared with the job's code and tested alongside it.
- **Auth:** private (no unauthenticated invoke). The Worker calls it with a Google ID token for a dedicated service account (key in a Pages secret, like `GCP_SA_KEY` for sweep dispatch). The Worker has already authorized the viewer.
- **Data:** reads the generation the Worker names (`listing/<date>/index/<gen>/`), via DuckDB `httpfs`/GCS. The pointer and scan list stay in D1. The Worker passes the generation, and the service never reads D1.
- **Evaluation:**
  - Match finding is a scan of the `path` column (or, when present, the names vocabulary plus v2 `rows`), with the AST compiled to SQL (`contains`/`ILIKE` per segment term, `regexp_matches` for the regex syntax). DuckDB scans compressed `VARCHAR` at GB/s per core with predicate pushdown on `depth`/`path` ranges for a drilled view root.
  - Match roots are an anti-join of matched paths against matched ancestors.
  - The rollup is a `GROUP BY` over ancestors to `depth ≤ k`, cut at the threshold.
- **Caching:** an in-instance LRU of results keyed by (generation, AST, view root, canvas). The Worker's edge cache sits in front, so a repeated view never reaches the service.
- **Sizing:** start at 4 vCPU / 16 GiB and measure. Targets: `safetensors` at the gcs root under ~5 s cold, under ~1 s for selective terms once the instance is warm.

## 5. Costs and trade-offs

- **Cold starts:** `min-instances=0` keeps the idle cost near zero, but the first heavy query after idle waits for container start plus DuckDB's first reads (estimate 5–15 s). `min-instances=1` at 4 vCPU / 16 GiB is roughly $150–250/month always-on. Start at 0 and measure how often people hit it.
- **Egress:** none. Same region as the data bucket (`us-east1`), so the service should run in `us-east1`, not the job's `us-central1`.
- **Interaction with the search sidecars:** the service makes them optional at gcs scale. It can answer every query by scanning `path`. The sidecars keep selective queries off the service and fast. This changes the sidecar retention question from "must exist for correctness" to "how many recent scans deserve the fast path".
- **What it doesn't fix:** plain (unfiltered) views and diffs still run in the Worker. Their remaining cost is decode CPU (pure-JS zstd), tracked separately.

## 6. Phases

1. **Service + AST → SQL**, with tests: exact parity with `matchRoots` + NOT subtraction on the existing fixtures, run against the same parquet files the vitest fixtures use.
2. **Worker hand-off** behind a deployment var (`QUERY_SERVICE_URL`; absent = today's behaviour), with the flags rules in §3. `dt-cloud probe` gains broad-query scenarios (`safetensors`, a 3-character common term).
3. **Infra:** `infra/gcp` component for the Cloud Run service, its SA and the invoker binding (the gcs stack adds an instance), plus a `deploy/query-service/` build like `sheet-mirror`.
4. **Measure on gcs**, then decide `min-instances`, and the sidecar retention window (the root session's proposal: last ~7 scans plus pinned dates).

## 7. Open questions

- **Diff on two scans:** two scans in one query (both generations' `path` files) vs two calls joined in the Worker. One query in DuckDB is simpler and keeps the join exact; prefer it.
- **User-lens queries with claims:** the service needs the ledger's claims (D1). Simplest: the Worker passes the relevant claim regions in the request, as it already computes them.
- **Where the AST compiler lives:** TypeScript (Worker) and Python (service) both need it. Keep the AST JSON schema in one place (the TS types plus a JSON schema test fixture), with an evaluator per language, and parity tests over a shared case table.

[`path-store-search.md`]: path-store-search.md
