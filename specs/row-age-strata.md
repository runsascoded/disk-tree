# Row age strata: replace the per-row "mean created" with a distribution

**Status:** proposed (2026-10-01), from `m3`. Needs a `[base]` decision (store schema).

## Why

The children table's `created` column (and the treemap's age colour) is one number per directory: the bytes-weighted mean write time. It is misleading wherever timestamps are fake or mixed. `~/.cargo` reads **Jun '13** because Cargo stamps packaged crate sources with a fixed 2006 time (30,853 files), mixed with ~3,000 real 2025–26 files. A mean of a bimodal distribution describes neither mode.

The age pyramid (`/api/age-pyramid`, `AgeChart`) already shows a distribution, but for one path at a time; a table page would need one request per row. On the laptop store it isn't built at all: that ingest is `path-index` from listings, and the pyramid comes from `index-write` over layer-2.

## Proposal: a fixed age-strata column on every store row

`dt-cloud path-index` already computes `age_days` (bytes and objects per `(day, dir)`, the dir's own objects) to write `age.json`. Roll it up descendant-inclusive, the same way `b`/`o` roll up, into a small fixed vector per row:

- `ag`: `LIST<BIGINT>` bytes by **age at scan time**, in log buckets: `<1d, <1w, <1mo, <3mo, <1y, <3y, ≥3y` (7 values; Σ = `b`).
- Age, not calendar year, because the question is "how stale is this", and it stays comparable across scans.

The subtree API already returns every child row, so the table gets the strata for free: no extra request, and none per row. It renders as a 7-cell stacked micro-bar (the existing viridis ramp) in place of the dot, month and year. Hover shows the bucket bytes. `.cargo` would read as mostly `≥3y` with a sliver of `<1mo`.

The `created` dot and the treemap's age colour can keep using `d` (the mean) until someone wants them changed; this adds a column, it removes nothing.

## Cost

- **Rows**: one 7-element list per row. On gcs (the largest store) that's about 56 bytes uncompressed per row, before parquet's list encoding and compression. Measure on one gcs scan before committing.
- **Compute**: the roll-up is a group-by over `age_days` joined to its ancestors, like the existing `b` roll-up. Measure on the laptop (`/`, 8.6M rows, 4.3 GiB peak today).
- **Wire**: `/api/subtree` rows grow by 7 numbers. Gate behind a `cv` bump.

## Open questions

- Bucket edges: the 7 above, or the age pyramid's own ladder (`1h…8d`, then months)?
- Whether `index-write` (cw's layer-2 path) emits the same column, so cw rows carry it too.
