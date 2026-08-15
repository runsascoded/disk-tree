# Adaptive range-splitting for `bulk-list` (+ in-DC placement)

Status: proposed 2026-08-15 (from marin-gcs-usage session; endorsed "1+2 for 30-50x"). Motivating case: full listing of CoreWeave CAIOS `marin-us-east-02a` — 92,767,314 objects / 960 TB — took hours from a laptop, OoMs longer than the I/O justifies.

## Problem

Object-store LISTs return ≤1000 keys/page, and each page in a continuation chain is a serial round-trip: page N+1 needs page N's token. So a listing's wall clock has a hard floor of `(pages in longest chain) × RTT`, regardless of `-P`/`-w`:

- 92.7M objects = ~92,800 pages. From a laptop (~60ms RTT to the DC), one serial chain = ~1.5h *minimum*; real runs are worse (TLS, throttling, skew).
- Today's parallelism is only across **pre-planned** shards (`-W` weights from a prior run, or per-prefix). Hot prefixes dominate: `datakit/` holds 72.1M of the 92.7M objects (78%), so its chains are the long pole no matter how the rest is packed.
- First scan of a bucket has no weights at all → effectively serial per top-level prefix.

Two multiplicative fixes:

## 1. Adaptive range splitting (work stealing, no pre-planning)

Workers own key **ranges** `[start, end)`, not prefixes. When the work queue is empty and a worker's range is still deep in pagination, split its *remaining* range `[last_key_seen, end)` at an estimated midpoint and enqueue the upper half. Repeat until all workers are busy. This finds the hot keyspace automatically — no prior run, no `-W`, no reliance on directory-ish structure.

### Mechanics

- **Queue**: ranges. Initial state: one range `["", ∞)` (or the user's prefixes). Workers pull; a worker lists its range serially and emits rows.
- **Split trigger**: queue empty AND some worker has paginated ≥2 pages in its current range without reaching `end`. Steal from the worker with the most estimated remaining keys.
- **Midpoint estimate**: byte-wise lexicographic interpolation between `last_key_seen` and `end` (like bisecting UUIDs). Keys already seen in the range give a density hint: extrapolate from (keys/page × pages so far) vs the key-prefix distribution. A bad guess is cheap — the split still halves *something*, and the new shard re-splits if it's fat. Optional refinement: probe `LIST(StartAfter=guess, MaxKeys=1)` to snap the guess to a real key.
- **Bounded-range listing per backend**:
  - **GCS**: native `startOffset`/`endOffset` — exact, server-side.
  - **S3 / CAIOS / R2**: `StartAfter` gives the lower bound; no `endOffset` equivalent, so enforce the upper bound client-side — stop the chain as soon as a page's last key ≥ `end` (discard overshoot rows past `end`). Worst case wastes most of one page per shard.
- **Output contract unchanged**: same `shard-*.parquet` + `_SUCCESS.json` as today. Record final shard boundaries in `_SUCCESS.json` — they are exactly next run's `-W` warm start (making explicit weights optional rather than required).
- **Skew bound**: with W workers and ~N/1000 total pages, converged wall clock → `N/(1000·W) × RTT + O(split overhead)`. 92.7M objects, 64 workers, in-DC 7ms RTT → ~10s of pure pagination (plus row handling / parquet writes, realistically a few minutes).

### Prior art, in-house: marin fsutil

marin's `lib/rigging/src/rigging/fsutil/listing.py:330-391` (`_s3_listing_pages`, Russell Power, 2026-08) does adaptive **delimiter** splitting: PROBE a prefix flat; if it overflows one page, pay one `Delimiter="/"` call to discover children and recurse each (cap depth 3); past the cap, fall back to serial FLAT pagination. Good instinct, but structure-*dependent*: it only helps when fanout exists at shallow depth. The `datakit/` shape (72M keys, fanout buried deep / long shared prefixes) blows through the depth cap into exactly the serial chain we're trying to kill. Range splitting is structure-*independent* — it bisects the keyspace itself, so it degrades gracefully on pathological layouts. (fsutil's `s3_compat.py` is separately worth borrowing: CAIOS vhost-only addressing, per-backend signing region, request deadlines.)

## 2. In-DC execution

RTT is the other factor in the floor. Laptop→DC ≈ 60ms; VM-in-the-same-DC ≈ 5-10ms → 6-12× per-chain, on top of the splitting win, and LIST responses never cross the WAN (only the final parquet does — MBs, not GBs). `bulk-list` already runs fine on a remote node (mgu flow); the missing piece is just doctrine + maybe a `disk-tree bulk-list --via <host>` convenience later. GCS listings should run from a GCE VM in-region; CAIOS from a CW instance (or accept the WAN penalty with splitting doing the heavy lifting). Not blocking: fix 1 alone recovers most of the 30-50× when fanout is available to steal.

## Non-goals

- Not replacing `-W` weights — they become the warm-start cache (`_SUCCESS.json` boundaries), not the required input.
- No inventory/manifest integration (S3 Inventory, GCS Storage Insights) here — that's `external-listings-and-gcs.md` territory; adaptive listing is for stores without SII (CAIOS) or fresh-scan needs.
- Delimiter-based discovery stays useful for *shallow* estimation (e.g. seeding initial ranges from top-level prefixes); it just isn't the split mechanism.

## Acceptance

- Re-list `marin-us-east-02a` (92.7M objects) from a laptop in ≤15 min with 32 workers and **no** `-W` input (vs hours today); byte/count totals match the 2026-08-14 baseline listing modulo churn.
- GCS: re-list a large marin bucket with no weights; verify identical rows vs the weighted-shard path (order-insensitive).
- `_SUCCESS.json` boundaries from run 1 accepted as warm start by run 2; run 2 splits ≤10% as often.
