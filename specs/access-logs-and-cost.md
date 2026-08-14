# Two new planes: access-log ingest (read attribution) + cost import

Written 2026-08-14 from a marin-gcs-usage session (spec workflow). Context: the engine spec
(`gcs-backend-and-snapshot-diff.md`) is fully landed (items A–E). This spec adds the next two
capability planes, both generic-in-DT / overlays-in-consumer, mirroring the size-scan split.
Driving consumer: marin's GCP bill is **~half operations ("Class B" reads) + egress**,
which size scans can't see; GCS usage logging is now enabled on 6 buckets → hourly CSVs at
`gs://marin-usage-logs/usage/<bucket>/*` (delivery pending). Marin eng explicitly asked for an
ops auto-report. Urgency: plane 1 v1 is wanted within days of CSVs landing.

## Plane 1 — access logs (`dt access` namespace)

### Canonical access row (layer-1a)

One row per request, normalized across providers:

```
ts          TIMESTAMP   -- request time
store       VARCHAR     -- gcs | s3 | r2
bucket      VARCHAR
path        VARCHAR     -- object key ('' for bucket-level ops)
op          VARCHAR     -- normalized: GET|PUT|LIST|HEAD|DELETE|OTHER (keep raw in op_raw)
op_raw      VARCHAR     -- provider verb (cs_method+cs_operation for GCS)
status      SMALLINT
bytes_out   BIGINT      -- sc_bytes (egress-ish)
bytes_in    BIGINT      -- cs_bytes
requester   VARCHAR     -- best available identity: IP for GCS usage logs; canonical id for CloudTrail
user_agent  VARCHAR
```

### Sources / parsers

1. **GCS usage logs** (first; format is fixed & documented): hourly CSVs named
   `<prefix>_usage_<ts>_<id>_v0`, header row, fields incl. `time_micros, c_ip, cs_method,
   cs_uri, sc_status, cs_bytes, sc_bytes, cs_user_agent, cs_operation, cs_bucket, cs_object`.
   Parse with DuckDB `read_csv` over a glob (`gs://…/usage/<bucket>/*_usage_*`); dedupe on the
   `s_request_id` field (Google documents rare duplicate log lines). Also accept the sibling
   `_storage_` daily files (bucket byte-hours) as a trivial bonus table.
2. **S3 server access logs** (space-delimited text; well-known schema) and **CloudTrail data
   events** (JSON; has real `principalEmail`-equivalent identity) — parser stubs behind the
   same row shape; implement when an S3 consumer materializes.
3. **R2**: Logpush HTTP-request dataset when configured; document the "front with a Worker"
   caveat — no per-object server logs otherwise.

### Aggregation (layer-2a) — reuse the path-tree machinery

`dt access agg` (mirrors `import --engine duckdb`): out-of-core group-by from raw rows to
**per-path per-day per-op** rollups, then the same bottom-up parent synthesis used for size
scans, so results are a *path tree* (`path,parent,depth` + `n_ops,bytes_out,n_requesters` per
op-class per day). That makes the existing treemap/diff/series widgets work over *ops* and
*egress* the way they already work over bytes-at-rest: hot-prefix treemap = `<Treemap>` with a
`n_ops` accessor; ops-over-time = `<TimeSeries>`.

### Surfaces

- `dt access import <glob> [--store gcs] [--since/--until]` → canonical parquet
- `dt access agg` → layer-2a tree parquet (registered like scans, dated)
- `dt access top [-d depth] [-n N] [--op GET] [--by ops|bytes]` → hot-prefix table (the
  auto-report primitive; consumer formats it for Slack/Discord)
- server/UI: an ops-mode on the existing scan views (later; CLI + parquet first)

### Consumer-side (NOT in DT)

Requester→person joins (marin: IP/UA heuristics + path conventions + identities.yaml),
digest posting, cost-per-op pricing overlays.

## Plane 2 — cost import (`dt cost`, later)

Canonical cost rows (`period,account,project,service,sku,region,usage_amt,usage_unit,
list_cost,net_cost,credits`) from: GCP billing-console **CSV export** (available today —
no-API world), GCP **BigQuery billing export** (if enabled), AWS **CUR**, CF **GraphQL**.
Deliberately thin: parse + normalize + store; reconciliation/policy (gross-vs-net, edu
discounts, rebill markups) is consumer logic. Ship after plane 1; the marin need today is
served by manual CSV downloads.

## Non-goals

- Real-time streaming (hourly/daily batch is the regime)
- Identity resolution inside DT (requester stays a raw string; joins are consumer overlays)
- R2 completeness parity (document the Worker-fronting option instead)

## Status

- [x] GCS usage-log parser (+ `s_request_id` dedupe, `_storage_` bonus table)
      — `disk_tree/access/parsers/gcs.py`; 3 fixture tests
- [x] `dt access import` / `agg` / `top` — `disk_tree/cli/access.py`
- [x] layer-2a parent-synthesis reuse — `disk_tree/access/aggregate.py`
      copies the `_PARENT_EXPR` shape from `find/aggregate_duckdb.py`
      (deliberate copy vs. import to keep the access module decoupled from
      the find module's SQL internals; if the parent-of policy ever needs
      to change, both files change together)
- [ ] widgets: ops accessors documented for `<Treemap>`/`<TimeSeries>` (likely zero code)
- [x] S3/CloudTrail parser stubs; R2 Logpush note — `parsers/{s3,r2}.py` raise
      `NotImplementedError` with pinned interfaces + provider-format doc
- [ ] Real-data smoke against GCS-delivered CSVs (waiting on delivery — mgu owns)
- [ ] `dt cost` plane (deferred)

Post-landing (2026-08-14): all core scaffolding + GCS parser + fixture tests
in `6181b1c`+. Once marin's usage-log CSVs land, `disk-tree access import
gs://marin-usage-logs/usage/<bucket>/* -o /tmp/canonical.parquet` should
Just Work; anything that doesn't is a real-data-driven follow-up.
