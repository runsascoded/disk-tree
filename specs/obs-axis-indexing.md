# Observation-axis indexing: diff-index + over-time index

Good indices for **every plot on the mgu pages**, by filling the two gaps that
today are served the expensive way. Both gaps are the same axis — the
**observation axis** (scan timestamp), distinct from the **event-time axis**
(object created-date) the age pyramid already covers. This is the cw-s3 build
that consumes pyrmts' `multi-scan-consolidation` obs-axis work.

## Motivation — the plots and their indices

| plot | axis | index | status |
|---|---|---|---|
| treemap / subtree | path (1 scan) | `path-index` (floor-free) + coarse tiers | ✓ have |
| age chart | event-time (created) | age pyramid `1h–8d` | ✓ shipped |
| **"size over time"** | **scan × path, totals** | telescoping → **scan×path over-time index** | **gap** (today: root-only `series.json`) |
| **diff / dTM** | scan → scan **changeset** | non-invertible → **SCD-2 diff-index** | **gap** (today: on-the-fly 2× full-index join) |
| lifecycle / marks | D1 ledger | — | ✓ (D1) |

Two gaps, and they are the two halves of one obs-axis index, split by **invertibility**:

- **Aggregate bytes/objects are an additive group → they telescope.**
  `over_time(path)[k] = Σ` of that path's `b` at scan `k`; a range is a
  prefix-difference. So the "size over time" line for *any* drilled path (not
  just the root's `series.json`) is a **prefix-sum / scan×path totals** read,
  O(1) in span. Cheap.
- **The changeset is non-invertible** (a path removed then re-added nets to
  zero but is real churn) → it needs the actual per-scan deltas. This is the
  **SCD-2 change-interval** encoding pyrmts chose: a diff of scans `a→b` reads
  only the keys whose value-interval crosses `(a, b]` — **O(changes-in-span)**,
  not O(2 full indices).

## The measured operating point (why this is worth building)

Real cw churn, measured over 6 consecutive `path-index` scans at `(path)`-total
granularity (6.18 M paths, floor-free):

- **~0.020 % of paths change per 12 h scan** (~1.3 k of 6.18 M). Storage is
  nearly static.
- **interval vs baseline (keep every scan): 5.9× at N=6 → ~78× projected at
  N=81, ~335× at daily-for-a-year.** Grows with scan count.
- Byte-exact on a real 1/40 sample via pyrmts' encoder: interval **2.73 MB**
  vs densify **4.14 MB** vs 6-separate **16.04 MB** (5.87×) — matches the row
  crossover; interval beats densify.

So the diff/over-time substrate is ~two orders of magnitude compressible, and —
crucially — the *incremental* delta per scan is ~1.3 k rows, i.e. **building
the diff-index costs O(changes), not O(fleet)**. That is also the answer to the
age pyramid's "1h base explode" cost: compute the delta, not the whole rollup.

## North-star (confirmed with the user + pyrmts)

**The delta representation is primary.** Build the diff-index **by default** for
diffing any two scans; **consolidation is a storage policy on top** that reuses
the same index (fold a range into archive files, drop the redundant per-scan
copies). One encoding (SCD-2), two uses (query = diff/over-time; storage =
archival compaction). Refinements:

- **Keyframes.** Keep periodic full snapshots (e.g. the existing per-scan
  `path-index`, or every Nth) so a reconstruction/diff composes a bounded number
  of deltas — like video keyframes. Diffing far-apart scans composes
  O(changes-in-span) deltas, or **O(log d)** with a dyadic (Bentley–Saxe /
  segment-tree) delta-hierarchy over power-of-2 scan spans.
- **Shared encoding with pyrmts.** The diff-index rows *are* the SCD-2
  change-intervals pyrmts' `consolidate_tables`/`diff_scans` speak, so a
  cw-built default index and a pyrmts-consolidated archive are the same shape —
  consolidation just compacts + prunes.

## Division of labor

- **pyrmts** owns the pure algo + layout + read primitives (`multiscan.py`:
  `consolidate_tables`, `extract_table`, `diff_scans`, over-time reads) and the
  storage-driver CLI (their Phase 2). Dependency-free of our storage.
- **cw-s3** owns: the **DuckDB producer** (native, no pyrmts-Python dep on the
  hot path) that emits the diff-index incrementally per scan; the **CFW reader**
  wiring (`/api/diff`, a new `/api/over-time?path=`); the **FE** (per-path
  over-time line; dTM served from the diff-index instead of the 2-index join).
  The full-scale consolidation, if/when we archive, uses pyrmts' CLI (the Python
  encoder OOMs at 81 × 6 M keys — that's why production consolidation is DuckDB;
  see the churn measurement).

## Phasing (proposed)

1. **Over-time-per-path index** (aggregate half). ✅ **Implemented** (cw-s3;
   verified on dev with a 15-scan real build). Not a throwaway densify — the
   durable SCD-2 interval substrate (Phase 2 reuses it), since the churn is so
   low (measured 1.0016 intervals/path over 15 real scans) that intervals cost
   ≈ one tier for the whole history. Shipped shape:
   - **Producer** `dt_cloud.overtime.write_over_time_index` (DuckDB, no pyrmts
     hot-path dep): rolls each scan's `path-index` to `(depth, path)` totals
     (owner slices summed), synthesizes a depth-0 fleet root per scan, and
     SCD-2-encodes each path's `(b, o)` stream into pyrmts' `__scan_lo/__scan_hi`
     layout, sorted `(depth, path, __scan_lo)`. A run breaks on value change or
     a scan-index gap (absence ≠ `b=0`). CLI `over-time-write`.
   - **Storage**: a cross-scan **singleton** — D1 variant `over-time` under the
     *latest* scan it was built for (reader takes the max-date pointer); the
     ordered scan list rides an `over-time.scans.json` sidecar (the D1-footer
     read path carries row-group stats but not KV metadata). Reuses the whole
     per-scan footer-in-D1 machinery (`index-sync -v over-time`, `_group_rows`).
   - **Serving**: no new endpoint / no FE change — `/api/series` reads the index
     once for the plain unscoped case (`readOverTime` → `expandIntervals`) and
     the per-scan `point()` loop consults it (O(1) hit); tail scans newer than
     the index, and any scoped/split/lens/owner/class query, fall through to the
     existing per-scan reads. So `series.json` is already effectively retired
     (the endpoint computes it) and the win is pure read-cost collapse:
     N point-reads across N generations → one contiguous read + cached sidecar.
2. **Diff-index** (changeset half — SCD-2 adjacent-scan deltas, produced by the
   DuckDB producer). `/api/diff` reads the intervals crossing the window
   (O(changes)) instead of joining two full `path-index`es. dTM gets faster +
   sparse.
3. **Dyadic hierarchy / consolidation integration** — bound far-apart diffs at
   O(log d); wire pyrmts' consolidation CLI for archival (drop old per-scan
   copies once digest-verified).

## Open questions

- Keyframe cadence (every scan is a keyframe today; how sparse can they get
  before diff-reconstruction cost bites — informed by the O(log d) hierarchy).
- Key granularity: `(path)` totals (over-time) vs `(path, usr)` slices (owner
  attribution) — the diff-index likely needs the `usr` slice to keep the dTM's
  per-owner story; over-time can roll to `(path)`.
- Whether the over-time index and the age pyramid should share a producer pass
  (both are cross-scan/bitemporal rollups off the same L2).
