# `/api/scan` takes 17–23 s when the scan's blobs live only in R2

From the `~/.disk` cleanup session, 2026-09-26. User: "even that long on 1st load is unacceptable".

## Repro

Scan 133 (`/Users/ryan`, written with `index --to r2://disk-tree/scans`) has a 12 MiB root blob plus 4 chunk blobs: `c` 131 MiB / 3.6M rows, `Library` 116 MiB / 1.4M, `.cache` 30 MiB, `.pyenv` 6 MiB. All were in R2 only. Server env: `DISK_TREE_SCAN_DIRS=r2://disk-tree/scans:~/.config/disk-tree/scans`.

`GET /api/scan?uri=/Users/ryan&depth=2&max_rows=2000` took **16.8–23 s on every request**, including repeats. Nothing is cached across requests.

## Profile (cProfile via the Flask test client)

17.1 s total, of which 15.4 s is `blobfs.read_parquet` → s3fs `_fetch_range`.

The main cost is the loop at `server.py` ~815–835: for each direct child with a `child_scan_id`, it runs `blobfs.read_parquet(resolve_blob(child_scan))`. That reads the **whole chunk** (all rows, all columns), then filters `depth == 1` in pandas, just to get ~100 rows per chunk.

Timings per chunk on this laptop's link, which downloads from R2 at about 19 MiB/s:

| chunk | rows | full read, R2 | `depth==1` filter, R2 | full read, local | `depth==1` filter, local | `depth==1` + 3 columns, local |
|---|---|---|---|---|---|---|
| `c` | 3.6M | 6.5 s | 9.6 s | 0.71 s | 0.17 s | 0.08 s |
| `Library` | 1.4M | 5.7 s | 6.2 s | | | |

The pushdown barely helps remotely: blobs are written with **1,048,576-row row groups**, and the first group spans depth 0–9, so `depth == 1` still fetches about 35 MiB.

## Mitigation applied (by us, not a fix)

I copied the 5 blobs (296 MiB) into `~/.config/disk-tree/scans/`. The search path prefers the local dir, so the same request now takes **0.48–0.69 s**. `/Users/ryan/c` takes 0.15 s and `Library` 0.24 s.

Scans written `--to r2://` stay slow until someone does this by hand, and doing it defeats the "zero local disk" point of `--to r2`.

## Asks

1. **Child-chunk reads in `get_scan`:** pass `filters=[('depth', '==', 1)]` and only the needed `columns`. Also cache them: blobs are immutable (UUID-named), so a per-process LRU keyed by blob ref is always valid. This is a cheap, big win locally, and for the second and later loads remotely.
2. **Smaller row groups** in `write_parquet`/`write_table` for scan blobs (e.g. 64K rows), so a `depth <= 2` pushdown over R2 fetches kilobytes, not a 35 MiB group. The files are sorted by `(depth, path)` already, so this is just the row-group size.
3. **The first load has to be fast too.** Two options, your call:
   - Denormalize each scan's shallow levels (e.g. depth ≤ 2 rows, or at least each chunk's depth-1 rows) into a small sidecar or SQLite at index time, the way `.groups.json` is written for `--to`. Then a page load never touches the big blobs.
   - Keep a local read-through cache of remote blobs, bounded, in a configurable dir. This is weaker on a near-full disk; the sidecar is better.
4. `load_scan_data` (hybrid) was another ~1.5 s of the 17. Check it after 1–3.

Target: `/api/scan` for the top of an R2-only `~` scan in under 1 s on the first request.
