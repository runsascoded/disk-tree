# Slimmer layer-2 listings: drop redundant columns, zstd, then cross-scan consolidation

Status: **proposed** (2026-09-29, from the cw-s3 session; Ryan: "yes spec it, this is a good direction").

## Motivation

The per-scan, per-bucket layer-2 listing (one row per object and dir) dominates our storage: on cw it is ≈ 3.6 of each scan's 4.2 GiB, about 250 GiB across 100 scans, mirrored to R2 as well. gcs's `listing/` (2.45 TiB) is very likely the same format. The indexes the site serves are the small part (≈ 0.56 GiB per cw scan).

Measured on `cw-l2/2026-09-29T1201/marin-us-east-02a.parquet` (3.58 GiB, 54.2 M rows, 207 row groups, **Snappy**):

| column | compressed | uncompressed | note |
|---|---|---|---|
| `uri` | 1447 MiB | 9529 MiB | **exactly** `s3://<bucket>/` + `path` (verified on 524k rows) |
| `path` | 1372 MiB | 8340 MiB | |
| `parent` | 252 MiB | 3100 MiB | derivable from `path` (`_PARENT_EXPR`) |
| `size` | 153 MiB | 224 MiB | |
| `sum_storage_class_id_1` | 153 MiB | 224 MiB | **equals `size`** (single storage class) |
| `mtime_mean`, `mtime` | 151 / 127 MiB | | |
| `n_desc`, `n_files`, `n_children`, `kind`, `depth` | < 3 MiB each | | |

Rewrite experiment (2 row groups, 524k rows, pyarrow):

| variant | size vs plain rewrite |
|---|---|
| snappy, all columns | 100 % |
| zstd-3, all columns | 36.7 % |
| snappy, − `uri` − `sum_storage_class_id_1` | 61.1 % |
| **zstd-3, − both** | **23.4 %** |
| zstd-9, − both | 21.3 % |

The plain rewrite is itself smaller than the original (≈ 24 vs ≈ 37 MiB for those two row groups), so against today's files the slim zstd variant is ≈ 15 %. Estimate: cw's ≈ 250 GiB of listings → ≈ 40–55 GiB, **lossless**.

## Phase 1: writer (base engine, `src/disk_tree/find/aggregate_duckdb.py`)

- Stop writing `uri` (lines ~188, 357–378). Readers that need it compute `scan_root || '/' || path`. First **audit every reader** of `uri`, `parent` and the `sum_storage_class_id_*` columns: engine (`server.py`, `blobfs.py`, `sqla/model.py`, `cli/{snapshots,index,capture,import_listing}.py`, `find/{index,agg_ext}.py`), the `cloud/` overlay (`cascade_a2a.py` uses the class columns), the site's `/files` parquet viewer, and the plan-sweep / sweep manifest builders. Where a reader needs `uri`, derive it at read time. Keep a schema version in the parquet KV metadata so readers can tell old from new.
- Keep `parent` for now: it is cheap after zstd, and many engine paths group on it. Revisit after measuring zstd-with vs zstd-without.
- `sum_storage_class_id_<k>`: write only classes that are present and non-trivial. A single-class store (cw) writes none, and readers treat "absent" as "all bytes in the default class".
- **zstd** (level 3 default; measure 6/9 for the write-time CPU trade) for every layer-2 and index parquet the engine and overlay write. The overlay already uses zstd for access and reactive outputs.
- Verify with byte-for-byte identical *indexes* (`path-index`, coarse tiers, age pyramid) built from an old-format vs a new-format listing of the same scan.

## Phase 2: recompress history (per deployment, lossless)

A one-off Batch job per deployment (not the laptop — `ec2-data-node` rule) rewrites each old listing in place as new-format (drop `uri` and the trivial class columns, zstd), with verify-then-swap:

1. Write the new file beside the old one.
2. Check row count plus an order-insensitive digest of `(path, size, mtime, kind)`.
3. Swap.

Also update the R2 mirror, or stop mirroring raw listings to R2 if nothing served reads them (check `/files`, `/v1/files`). cw first (≈ 200 GiB saved); gcs's `listing/` is gcs's call.

## Phase 3: cross-scan consolidation (the big one)

Same idea as the over-time groups (`obs-axis-indexing.md`), but at the **object** level and lossless for the listing's columns: SCD-2 intervals `(path, size, mtime, …, __scan_lo, __scan_hi)` over K-scan sealed groups via pyrmts' `consolidate_parquet_duckdb`. Object churn is higher than path-total churn: cw's 13-day diff shows +22.3 M objects over a 46 M base, roughly 3–4 % per 12 h scan. So 100 scans should cost about 5–10 single scans rather than 100. Rough estimate, to be measured on one group first: **10–20 GiB instead of ≈ 250 GiB**. Policy: the latest N scans stay raw (the hot reads: index builds, sweep manifests, `/files`), and older scans are served from the group (`extract_scan`) when anything needs them.

Readers to wire before dropping raw copies: reindex (`job/cw-reindex.sh`), `/files` / `/v1/files` for historical listings, and sweep/plan manifests for a past scan. That's the same "drop after digest-verify, latest exempt" rule as `pyrmts-adoption.md` §6.

## Out of scope

- Consolidating the *indexes* (path-index, tiers, age pyramid) across scans: ≈ 55 GiB total on cw, and needed by the treemap and diff readers (see `pyrmts-adoption.md`). A smaller, harder win than the listings.
- gcs `access/` logs (2.43 TiB): a separate retention question for the gcs session.
