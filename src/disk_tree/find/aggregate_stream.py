"""O(depth) streaming aggregation over sorted listings (spec: streaming-aggregation.md).

Third engine alongside pandas (:func:`disk_tree.find.index.aggregate`) and
DuckDB (:mod:`disk_tree.find.aggregate_duckdb`). Object-store listings are
lexicographically sorted by key, and sorted-by-path is DFS order: every dir's
subtree is a contiguous key interval. So aggregation is the classic ``du``
algorithm — a stack of open ancestor dirs with running accumulators, popped
(and emitted) as soon as the stream leaves their subtree. Working state is
O(max depth); the DuckDB cascade's level re-materialization, long-VARCHAR hash
group-bys, and RSS overshoot all disappear.

Output contract is byte-identical to the other engines: canonical layer-2
rows sorted by ``(depth, path)``. Streaming emits files in path order and
dirs in postorder, so a final bounded external sort (DuckDB ``COPY … ORDER
BY``) restores the contract (spec option 2) — one sort over already-thin
aggregated rows, no hash aggregation.

The one wrinkle: `//` canonicalization (``a//b`` → ``a/b``, same policy as
the other engines) does not preserve lexicographic order — raw ``a//z``
sorts before ``a/b`` but canonical ``a/z`` sorts after, and a dirty key's
canonical position can fall either before or after its raw position. A
single-pass stack over raw order would pop dirs prematurely. Fix: a cheap
names-only pre-scan finds the shards containing dirty keys (rare — a handful
out of 92.7M in marin's production listings); those rows are collected,
canonicalized, sorted, and joined into the k-way merge as one more sorted
source, so the stack only ever sees globally canonical-sorted rows.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from glob import glob as _glob
from operator import itemgetter
from typing import Iterator

_SLASHES = re.compile(r'/+')


def _stage(msg: str) -> None:
    """Timestamped stderr progress line (same shape as aggregate_duckdb's) —
    long runs need a last-known-stage for post-mortems and phase timing."""
    print(f"[agg {datetime.now().isoformat(timespec='seconds')}] {msg}", file=sys.stderr, flush=True)

# Safety valve: dirty keys are collected in RAM. If a listing is mostly dirty
# keys something is pathological — the pre-scan diversion isn't the right tool.
_DIRTY_MAX = 10_000_000

# Rows buffered before flushing a parquet row group in the pre-output writer.
_FLUSH_ROWS = 1 << 18

_COLS = ['path', 'size', 'mtime', 'kind', 'parent', 'uri', 'n_desc', 'n_files', 'n_children', 'depth']


def _canonicalize(name: str) -> str:
    """`a//b` → `a/b`; strip trailing slashes. Same policy as import_listing."""
    return _SLASHES.sub('/', name).rstrip('/')


def _parent_of(p: str) -> str:
    """Everything before the last slash; '' if none. Matches the DuckDB
    engine's `_PARENT_EXPR` (incl. leading-slash keys: parent('/a') = '')."""
    i = p.rfind('/')
    return p[:i] if i > 0 else ('' if i < 0 else '')


def _expand_shards(listing_glob: str) -> list[str]:
    files = sorted(_glob(listing_glob))
    if not files:
        # A non-glob path that exists gets picked up by glob() too, so an
        # empty result means the input genuinely matches nothing.
        raise ValueError(f"no files match listing glob {listing_glob!r}")
    return files


def _check_schema(shard: str, required: frozenset[str]) -> bool:
    """True if the shard carries all `required` columns."""
    import pyarrow.parquet as pq
    names = set(pq.ParquetFile(shard).schema_arrow.names)
    return required <= names


def _epoch_seconds(arr):
    """Timestamp array → int64 epoch seconds, nulls → 0. Sub-second values
    TRUNCATE (floor) — the canonical epoch-seconds semantic; the duckdb and
    pandas engines use `floor(epoch(created))::BIGINT` to match (bare
    `::BIGINT` rounds, which skewed ~50% of real sub-second timestamps +1s
    — caught by the CW 92.7M cross-engine hashsum)."""
    import pyarrow as pa
    import pyarrow.compute as pc
    t = arr.type
    if pa.types.is_timestamp(t):
        per_s = {'s': 1, 'ms': 1_000, 'us': 1_000_000, 'ns': 1_000_000_000}[t.unit]
        ints = pc.fill_null(arr.cast(pa.int64()), 0)
        return pc.divide(ints, per_s) if per_s > 1 else ints
    return pc.fill_null(arr.cast(pa.int64()), 0)


def _dirty_mask(names):
    """Boolean mask of keys whose canonical form differs from the raw key."""
    import pyarrow.compute as pc
    return pc.or_(pc.match_substring(names, '//'), pc.ends_with(names, pattern='/'))


_MAX_RUNS = 100_000


def _merge_runs(run_srcs: list[tuple[str, Iterator[tuple]]], hw: dict):
    """K-way merge of sorted sources, opening each only when the merge horizon
    reaches its first key (known from pass-1) and dropping it at exhaustion.

    `run_srcs` is [(first_key, row_iter)] sorted by first_key. Sources are
    generators that open their parquet reader on first pull — so concurrently
    open files / read buffers stay O(max range overlap), not O(total runs).
    `heapq.merge` by contrast primes every source up front: a fleet-scale
    bin-packed listing (thousands of runs) blows EMFILE and holds a row-group
    buffer per run (observed: 6 parallel imports all dead at `Too many open
    files`, ~20GB RSS each within minutes).

    `hw['max_open']` records the concurrently-open high-water mark.
    """
    from heapq import heappush, heappop
    h: list[tuple[str, int, tuple, Iterator[tuple]]] = []
    seq = 0
    i = 0
    n = len(run_srcs)
    n_open = 0
    while i < n or h:
        # Open every pending source whose range may contain the next key.
        while i < n and (not h or run_srcs[i][0] <= h[0][0]):
            src = run_srcs[i][1]
            i += 1
            row = next(src, None)
            if row is not None:
                heappush(h, (row[0], seq, row, src))
                seq += 1
                n_open += 1
                if n_open > hw['max_open']:
                    hw['max_open'] = n_open
        _, _, row, src = heappop(h)
        yield row
        nxt = next(src, None)
        if nxt is None:
            n_open -= 1
        else:
            heappush(h, (nxt[0], seq, nxt, src))
            seq += 1


def _scan_names(
    shards: list[str],
    bucket: str,
    pivot_sums: tuple[str, ...] = (),
) -> tuple[int, set[str], list[list], dict[str, tuple[list[int], list[str], list[str]]]]:
    """Pass 1: names scan. Returns (total rows for bucket, shards with dirty
    keys, sorted distinct non-null values per pivot column, per-shard runs as
    parallel lists (start ordinals, first keys, last keys)).

    Runs: `bulk-list` bin-packs multiple non-contiguous key ranges into each
    shard (weight balancing), so a shard is *piecewise* sorted — a
    concatenation of internally-sorted runs. Each run becomes its own merge
    source downstream. Ordinals are *raw* (unfiltered) row indices — that's
    what lets `_shard_rows` map them onto row-group boundaries and skip
    non-intersecting row groups entirely. First/last keys let the merge
    detect globally disjoint runs (the bulk-list common case: ranges split
    from one sorted keyspace) and degrade the k-way heap to concatenation."""
    import numpy as np
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    n_rows = 0
    dirty_shards: set[str] = set()
    distinct: list[set] = [set() for _ in pivot_sums]
    runs: dict[str, tuple[list[int], list[str], list[str]]] = {}
    total_runs = 0
    for shard in shards:
        pf = pq.ParquetFile(shard)
        raw0 = 0
        prev_last: str | None = None
        starts: list[int] = []
        firsts: list[str] = []
        lasts: list[str] = []
        for batch in pf.iter_batches(columns=['bucket', 'name', *pivot_sums]):
            nb = batch.num_rows
            mask = pc.fill_null(pc.equal(batch.column('bucket'), bucket), False)
            names = batch.column('name').filter(mask)
            n = len(names)
            if n == 0:
                raw0 += nb
                continue
            n_rows += n
            # Raw (batch-relative) indices of this bucket's rows — run-start
            # ordinals must be raw so the reader can row-group-skip.
            raw_idx = np.nonzero(mask.to_numpy(zero_copy_only=False))[0]
            first_name = names[0].as_py()
            if not starts:
                starts.append(raw0 + int(raw_idx[0]))
                firsts.append(first_name)
            elif prev_last is not None and first_name < prev_last:
                lasts.append(prev_last)
                starts.append(raw0 + int(raw_idx[0]))
                firsts.append(first_name)
            if n > 1:
                ge = pc.greater_equal(names.slice(1), names.slice(0, n - 1))
                desc = np.nonzero(~ge.to_numpy(zero_copy_only=False))[0]
                if len(desc):
                    names_py = names.to_pylist()
                    for idx in desc:
                        lasts.append(names_py[int(idx)])
                        starts.append(raw0 + int(raw_idx[int(idx) + 1]))
                        firsts.append(names_py[int(idx) + 1])
            prev_last = names[n - 1].as_py()
            raw0 += nb
            if pc.any(_dirty_mask(names)).as_py():
                dirty_shards.add(shard)
            for i, col in enumerate(pivot_sums):
                vals = pc.unique(pc.drop_null(batch.column(col).filter(mask)))
                distinct[i].update(vals.to_pylist())
        if starts:
            lasts.append(prev_last)
            runs[shard] = (starts, firsts, lasts)
            total_runs += len(starts)
        if total_runs > _MAX_RUNS:
            raise ValueError(
                f"more than {_MAX_RUNS:,} sorted runs across listing shards — "
                f"input is essentially unsorted; use `-e duckdb`"
            )
    return n_rows, dirty_shards, [sorted(s) for s in distinct], runs


def _collect_dirty(
    dirty_shards: list[str],
    bucket: str,
    pivot_sums: tuple[str, ...] = (),
) -> list[tuple]:
    """Pass 1b: re-read only the shards flagged dirty, extracting the dirty
    rows as (canonical_name, size, mtime, *pivot_values), sorted by canonical name."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    out: list[tuple] = []
    for shard in dirty_shards:
        pf = pq.ParquetFile(shard)
        cols = ['bucket', 'name', 'size_bytes', *pivot_sums]
        has_created = 'created' in pf.schema_arrow.names
        if has_created:
            cols.append('created')
        for batch in pf.iter_batches(columns=cols):
            mask = pc.and_(pc.equal(batch.column('bucket'), bucket), _dirty_mask(batch.column('name')))
            if not pc.any(mask).as_py():
                continue
            names = batch.column('name').filter(mask).to_pylist()
            sizes = pc.fill_null(batch.column('size_bytes'), 0).filter(mask).to_pylist()
            if has_created:
                mtimes = _epoch_seconds(batch.column('created')).filter(mask).to_pylist()
            else:
                mtimes = [0] * len(names)
            pivots = [batch.column(c).filter(mask).to_pylist() for c in pivot_sums]
            out.extend(zip(map(_canonicalize, names), map(int, sizes), map(int, mtimes), *pivots))
            if len(out) > _DIRTY_MAX:
                raise ValueError(
                    f"more than {_DIRTY_MAX:,} `//`/trailing-slash keys in listing — "
                    f"pathological input for the stream engine; use `-e duckdb`"
                )
    out.sort(key=itemgetter(0))
    return out


def _shard_rows(
    shard: str,
    bucket: str,
    pivot_sums: tuple[str, ...] = (),
    start: int = 0,
    stop: int | None = None,
) -> Iterator[tuple]:
    """Clean rows of one sorted run of a shard as (name, size, mtime,
    *pivot_values). `[start, stop)` are *raw* row ordinals from
    `_scan_names`' run detection — raw so the read can skip every row group
    that doesn't intersect the run (bin-packed shards hold many runs; without
    the skip each run source re-reads the shard from row 0, ~R×/2 read
    amplification). The slice is verified sorted as it goes (a violation
    means the two passes disagreed — a bug, not bad input)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(shard)
    cols = ['bucket', 'name', 'size_bytes', *pivot_sums]
    has_created = 'created' in pf.schema_arrow.names
    if has_created:
        cols.append('created')
    # Row-group skip: read only groups intersecting [start, stop).
    md = pf.metadata
    rgs: list[int] = []
    off = 0
    ord0 = 0
    for i in range(md.num_row_groups):
        n_rg = md.row_group(i).num_rows
        if off + n_rg > start and (stop is None or off < stop):
            if not rgs:
                ord0 = off
            rgs.append(i)
        off += n_rg
    if not rgs:
        return
    prev: str | None = None
    for batch in pf.iter_batches(columns=cols, row_groups=rgs):
        nb = batch.num_rows
        lo = max(start - ord0, 0)
        hi = nb if stop is None else min(stop - ord0, nb)
        ord0 += nb
        if lo >= hi:
            if stop is not None and ord0 >= stop:
                break
            continue
        sl = batch.slice(lo, hi - lo)
        mask = pc.fill_null(pc.equal(sl.column('bucket'), bucket), False)
        names_sl = sl.column('name').filter(mask)
        m = len(names_sl)
        if m == 0:
            continue
        # Sortedness within the run: vectorized within the batch slice, plus
        # the batch boundary.
        first = names_sl[0].as_py()
        if (m > 1 and not pc.all(
            pc.greater_equal(names_sl.slice(1), names_sl.slice(0, m - 1))
        ).as_py()) or (prev is not None and first < prev):
            raise RuntimeError(
                f"run [{start}, {stop}) of shard {shard!r} not sorted — "
                f"run detection and row reader disagree (bug)"
            )
        prev = names_sl[m - 1].as_py()

        clean = pc.invert(_dirty_mask(names_sl))
        names = names_sl.filter(clean).to_pylist()
        if not names:
            continue
        sizes = pc.fill_null(sl.column('size_bytes'), 0).filter(mask).filter(clean).to_pylist()
        if has_created:
            mtimes = _epoch_seconds(sl.column('created')).filter(mask).filter(clean).to_pylist()
        else:
            mtimes = [0] * len(names)
        pivots = [sl.column(c).filter(mask).filter(clean).to_pylist() for c in pivot_sums]
        yield from zip(names, map(int, sizes), map(int, mtimes), *pivots)


class _Acc:
    """Running accumulators for one open dir on the stack."""
    __slots__ = ('path', 'pfx', 'size', 'mtime', 'n_desc', 'n_files', 'n_children', 'pivot', 'mt_wsum')

    def __init__(self, path: str, n_pivot: int):
        self.path = path
        self.pfx = f'{path}/'  # hoisted: subtree-membership startswith runs per input row
        self.size = 0
        self.mtime = 0
        self.n_desc = 1  # self — matches the other engines' dir-row seeding
        self.n_files = 0
        self.n_children = 0
        self.pivot = [0] * n_pivot
        self.mt_wsum = 0  # exact Σ mtime·size — Python bigint, no overflow


class _Writer:
    """Buffered parquet writer for pre-output rows (files + dirs, unsorted)."""

    def __init__(self, path: str, cols: list[str], mean_mtime: bool):
        import pyarrow as pa
        from .agg_ext import MTIME_MEAN
        fields = []
        for c in cols:
            if c in ('path', 'kind', 'parent', 'uri'):
                fields.append((c, pa.string()))
            elif c == MTIME_MEAN:
                fields.append((c, pa.float64()))
            else:
                fields.append((c, pa.int64()))
        self._cols = cols
        self._schema = pa.schema(fields)
        import pyarrow.parquet as pq
        self._writer = pq.ParquetWriter(path, self._schema)
        self._rows: list[tuple] = []
        self.n_rows = 0

    def write(self, row: tuple) -> None:
        self._rows.append(row)
        self.n_rows += 1
        if len(self._rows) >= _FLUSH_ROWS:
            self._flush()

    def _flush(self) -> None:
        if not self._rows:
            return
        import pyarrow as pa
        cols = list(zip(*self._rows))
        self._writer.write_table(pa.table(dict(zip(self._cols, cols)), schema=self._schema))
        self._rows.clear()

    def close(self) -> None:
        self._flush()
        self._writer.close()


class _PartWriters:
    """Per-(depth, kind) family of buffered parquet part writers.

    The du-stack's emission is already *almost* the output contract: file rows
    at a fixed depth are a subsequence of the globally-sorted key stream (so
    sorted by path), and dir rows at a fixed depth pop in subtree-interval
    order — i.e. sorted by ``path + '/'``, which differs from plain ``path``
    order exactly when a dir name is a proper prefix of a same-depth sibling
    whose next char is < ``'/'`` (``store`` vs ``store-backup``: the sibling's
    subtree sorts *before* ``store/``, so it pops first, inverted). Rather than
    trust that analysis, each part *measures* its own sortedness (one string
    compare per row); the finalize sorts only the parts that actually need it
    and streams the rest — no global external sort anywhere.
    """

    def __init__(self, parts_dir: str, cols: list[str], mean_mtime: bool):
        import os
        self._dir = parts_dir
        self._cols = cols
        self._mean_mtime = mean_mtime
        self._writers: dict[tuple[int, str], _Writer] = {}
        self._last: dict[tuple[int, str], str] = {}
        self._unsorted: set[tuple[int, str]] = set()
        self._join = os.path.join
        self.n_rows = 0

    def write(self, depth: int, kind: str, path: str, row: tuple) -> None:
        key = (depth, kind)
        w = self._writers.get(key)
        if w is None:
            w = self._writers[key] = _Writer(
                self._join(self._dir, f'{depth:04d}-{kind}.parquet'), self._cols, self._mean_mtime,
            )
        if key not in self._unsorted:
            last = self._last.get(key)
            if last is not None and path < last:
                self._unsorted.add(key)
            self._last[key] = path
        w.write(row)
        self.n_rows += 1

    def close(self) -> list[dict]:
        """Close all writers; return part descriptors for the manifest."""
        parts = []
        for (depth, kind), w in sorted(self._writers.items()):
            w.close()
            parts.append({
                'depth': depth,
                'kind': kind,
                'file': f'{depth:04d}-{kind}.parquet',
                'rows': w.n_rows,
                'sorted': (depth, kind) not in self._unsorted,
            })
        return parts


def _part_batches(path: str, part_sorted: bool, batch_rows: int):
    """Record batches of one part, in path order.

    A part that measured unsorted at write time (prefix-sibling dir inversions)
    is loaded and sorted here — bounded by that single (depth, kind) slice, not
    the whole output (the win over the retired global external sort).
    """
    import pyarrow.parquet as pq
    if part_sorted:
        yield from pq.ParquetFile(path).iter_batches(batch_size=batch_rows)
    else:
        yield from pq.read_table(path).sort_by('path').to_batches(max_chunksize=batch_rows)


def _merge_two_sorted(dirs, files, emit) -> None:
    """Ordered merge of two path-sorted record-batch streams; ties → dir first.

    Vectorized boundary merge: whole batches pass through when their key
    ranges don't interleave; otherwise the earlier batch is split at the
    other's boundary key (``searchsorted``) — O(#batches) Python, O(rows) C.
    """
    import numpy as np

    def _paths(rb):
        return rb.column('path')

    a = next(dirs, None)
    b = next(files, None)
    while a is not None and b is not None:
        if a.num_rows == 0:
            a = next(dirs, None)
            continue
        if b.num_rows == 0:
            b = next(files, None)
            continue
        pa_, pb = _paths(a), _paths(b)
        a_first, a_last = pa_[0].as_py(), pa_[a.num_rows - 1].as_py()
        b_first, b_last = pb[0].as_py(), pb[b.num_rows - 1].as_py()
        if a_last <= b_first:
            # All dir keys ≤ first file key; equal path → dir row first, so
            # the whole dirs batch goes before the files batch.
            emit(a)
            a = next(dirs, None)
        elif b_last < a_first:
            # Strictly before every dir key (a tie would owe the dir row first).
            emit(b)
            b = next(files, None)
        elif a_first <= b_first:
            # Overlap, dirs start first: peel dir keys ≤ b_first (ties are
            # dirs → included, keeping dir-before-file on equal path).
            idx = int(np.searchsorted(pa_.to_numpy(zero_copy_only=False), b_first, side='right'))
            emit(a.slice(0, idx))
            a = a.slice(idx)
        else:
            # Overlap, files start first: peel file keys strictly < a_first.
            idx = int(np.searchsorted(pb.to_numpy(zero_copy_only=False), a_first, side='left'))
            emit(b.slice(0, idx))
            b = b.slice(idx)
    while a is not None:
        emit(a)
        a = next(dirs, None)
    while b is not None:
        emit(b)
        b = next(files, None)


def _finalize_parts(
    parts_dir: str,
    manifest: dict,
    out_parquet: str,
    batch_rows: int = 1 << 16,
) -> None:
    """Depth-partitioned, sort-free finalize: parts → canonical layer-2 parquet.

    For each depth ascending, a 2-way ordered merge of that depth's (dirs,
    files) parts, written in canonical column order. O(1) memory and zero
    spill except for parts that measured unsorted (see :func:`_part_batches`).
    """
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .agg_ext import MTIME_MEAN

    pivot_names = manifest['pivot_names']
    mean_mtime = manifest['mean_mtime']
    # Canonical column order (matches the pandas concat / duckdb tail: extras
    # between `parent` and `uri`) — the published layer-2 must be column-order
    # identical across engines so file-level diffs and positional set ops
    # (EXCEPT) work.
    canonical_cols = [
        'path', 'size', 'mtime', 'n_desc', 'n_files', 'n_children', 'kind', 'parent',
        *pivot_names, *([MTIME_MEAN] if mean_mtime else []),
        'uri', 'depth',
    ]
    fields = []
    for c in canonical_cols:
        if c in ('path', 'kind', 'parent', 'uri'):
            fields.append((c, pa.string()))
        elif c == MTIME_MEAN:
            fields.append((c, pa.float64()))
        else:
            fields.append((c, pa.int64()))
    schema = pa.schema(fields)

    by_depth: dict[int, dict[str, dict]] = {}
    for part in manifest['parts']:
        by_depth.setdefault(part['depth'], {})[part['kind']] = part

    writer = pq.ParquetWriter(out_parquet, schema)
    buf: list = []
    buf_rows = 0

    def emit(rb) -> None:
        nonlocal buf_rows
        if rb.num_rows == 0:
            return
        buf.append(rb.select(canonical_cols))
        buf_rows += rb.num_rows
        if buf_rows >= _FLUSH_ROWS:
            flush()

    def flush() -> None:
        nonlocal buf_rows
        if buf:
            writer.write_table(pa.Table.from_batches(buf, schema=schema))
            buf.clear()
            buf_rows = 0

    try:
        for depth in sorted(by_depth):
            kinds = by_depth[depth]
            streams = {
                kind: _part_batches(os.path.join(parts_dir, p['file']), p['sorted'], batch_rows)
                for kind, p in kinds.items()
            }
            if len(streams) == 1:
                for rb in next(iter(streams.values())):
                    emit(rb)
            else:
                _merge_two_sorted(streams['dir'], streams['file'], emit)
        flush()
    finally:
        writer.close()


def _finalize_and_clean(parts_dir: str, manifest: dict, out_parquet: str) -> dict:
    """Run the finalize; preserve the parts dir on failure (resume token),
    remove it on success; return the engine stats dict."""
    import shutil
    try:
        _finalize_parts(parts_dir, manifest, out_parquet)
    except BaseException:
        # A finalize-only failure must not cost the (expensive) stream pass:
        # the parts + manifest stay put, and a rerun with the same output path
        # resumes at the merge.
        _stage(f"FAILED — streamed parts preserved at {parts_dir}; rerun resumes at finalize")
        raise
    _stage("finalize done")
    shutil.rmtree(parts_dir, ignore_errors=True)
    return {
        'rows': manifest['rows'],
        'files': manifest['files'],
        'max_open_sources': manifest['max_open_sources'],
        'root_size': manifest['root_size'],
        'root_n_desc': manifest['root_n_desc'],
        'root_n_files': manifest['root_n_files'],
        'root_n_children': manifest['root_n_children'],
        'root_mtime': manifest['root_mtime'],
    }


def aggregate_stream(
    listings: tuple[str, ...],
    bucket: str,
    scheme: str,
    out_parquet: str,
    con: "object | None" = None,
    memory_limit: str | None = None,
    temp_dir: str | None = None,
    max_temp_size: str | None = None,
    pivot_sums: tuple[str, ...] = (),
    mean_mtime: bool = False,
) -> dict:
    """Streaming rollup: sorted listing shards → canonical layer-2 parquet.

    Same signature-shape and stats dict as
    :func:`disk_tree.find.aggregate_duckdb.aggregate_listing_to_parquet`.
    `listings` are parquet globs in earlier-source-wins order (the first glob
    containing any rows for `bucket` is used). Requires the raw listing
    schema (``bucket, name, size_bytes[, created]``) with shards *piecewise*
    sorted by key (bulk-list bin-packs sorted ranges into shards; each run
    becomes a merge source) — for anything else, `-e duckdb`.
    `pivot_sums` / `mean_mtime` are the opt-in aggregation extensions — all
    monoid sums, so they stream identically (see :mod:`disk_tree.find.agg_ext`).

    `con` / `memory_limit` / `temp_dir` / `max_temp_size` are accepted for
    call-site compatibility but unused: the finalize is a depth-partitioned
    ordered merge (see :func:`_finalize_parts`), not a DuckDB external sort,
    so there is nothing to cap or spill.
    """
    import json
    import os
    import shutil
    from .agg_ext import MTIME_MEAN, check_pivot_values, mean_of, pivot_col

    # The parts dir sits next to the output; its manifest doubles as the
    # resume token — a finalize-only failure leaves it in place, and a rerun
    # with the same output path skips straight to the merge.
    parts_dir = f'{out_parquet}.parts'
    manifest_path = os.path.join(parts_dir, 'manifest.json')
    if os.path.exists(manifest_path):
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        _stage(f"resuming from streamed parts at {parts_dir} (stream pass skipped)")
        return _finalize_and_clean(parts_dir, manifest, out_parquet)

    required = frozenset({'bucket', 'name', 'size_bytes', *pivot_sums})

    # ---- resolve which listing source serves this bucket (earlier wins) ----
    chosen: list[str] | None = None
    dirty_shards: set[str] = set()
    distinct: list[list] = []
    runs: dict[str, list[int]] = {}
    for listing_glob in listings:
        shards = _expand_shards(listing_glob)
        bad = [s for s in shards if not _check_schema(s, required)]
        if bad:
            raise ValueError(
                f"shard {bad[0]!r} lacks required columns {sorted(required)} — "
                f"the stream engine only reads raw/bulk-list listings; use `-e duckdb`"
            )
        _stage(f"pass-1 names scan: {len(shards)} shard(s) in {listing_glob!r}")
        n_rows, dirty_shards, distinct, runs = _scan_names(shards, bucket, pivot_sums)
        if n_rows > 0:
            chosen = shards
            break
    if chosen is None:
        raise ValueError(f"no rows for bucket {bucket!r}")
    _stage(
        f"pass-1 done: {n_rows:,} rows, {sum(len(starts) for starts, _, _ in runs.values()):,} run(s) "
        f"across {len(runs)} shard(s), {len(dirty_shards)} dirty shard(s)"
    )

    dirty = _collect_dirty(sorted(dirty_shards), bucket, pivot_sums) if dirty_shards else []
    if dirty_shards:
        _stage(f"collected {len(dirty):,} dirty row(s)")

    # Pivot layout: value → index into each _Acc's flat `pivot` vector; column
    # names in (CLI col order) × (sorted value order), matching the other engines.
    pivot_names: list[str] = []
    pivot_maps: list[dict] = []
    for col, vals in zip(pivot_sums, distinct):
        check_pivot_values(col, vals)
        pivot_maps.append({v: len(pivot_names) + i for i, v in enumerate(vals)})
        pivot_names.extend(pivot_col(col, v) for v in vals)
    n_pivot = len(pivot_names)
    out_cols = [*_COLS, *pivot_names, *([MTIME_MEAN] if mean_mtime else [])]

    scan_root = f'{scheme}://{bucket}'
    # One merge source per sorted run (bin-packed shards are piecewise sorted).
    # When runs are globally disjoint, ordered by first key the merge degrades
    # to concatenation — O(1)/row instead of O(log n_runs) string compares in
    # the heap. Caveat: consecutive in-order ranges within a shard coalesce
    # into one detected run with key *gaps* inside, and other shards' ranges
    # land in those gaps — so multi-shard bulk-list listings usually take the
    # heap fallback; the chain engages for single-run inputs (one shard, or
    # pre-merged listings). Any overlap or a dirty side-stream falls back.
    run_infos: list[tuple[str, str, str, int, int | None]] = []
    for s in chosen:
        entry = runs.get(s)
        if not entry:
            continue
        starts, firsts, lasts = entry
        bounds: list[int | None] = [*starts[1:], None]
        for start, stop, first, last in zip(starts, bounds, firsts, lasts):
            run_infos.append((first, last, s, start, stop))
    run_infos.sort()
    sources = [
        _shard_rows(s, bucket, pivot_sums, start=a, stop=b)
        for _, _, s, a, b in run_infos
    ]
    disjoint = not dirty and all(
        run_infos[i][0] > run_infos[i - 1][1] for i in range(1, len(run_infos))
    )
    hw = {'max_open': 0}
    if disjoint:
        from itertools import chain
        merged = chain.from_iterable(sources)
    else:
        run_srcs = [(info[0], src) for info, src in zip(run_infos, sources)]
        if dirty:
            run_srcs.append((dirty[0][0], iter(dirty)))
            run_srcs.sort(key=itemgetter(0))
        merged = _merge_runs(run_srcs, hw)
    _stage(f"merge+stream: {len(sources)} source(s) ({'disjoint, concatenating' if disjoint else 'lazy-open merge'})")

    if os.path.isdir(parts_dir):
        # Manifest-less leftovers from a mid-stream crash — unusable.
        shutil.rmtree(parts_dir)
    os.makedirs(parts_dir)
    parts = _PartWriters(parts_dir, out_cols, mean_mtime)
    n_files_total = 0
    try:
        stack: list[_Acc] = [_Acc('', n_pivot)]

        def dir_extras(acc: _Acc) -> tuple:
            return (
                *acc.pivot,
                *([mean_of(acc.mt_wsum, acc.size)] if mean_mtime else []),
            )

        def pop_emit() -> None:
            acc = stack.pop()
            parent_acc = stack[-1]
            parent_acc.size += acc.size
            parent_acc.mtime = max(parent_acc.mtime, acc.mtime)
            parent_acc.n_desc += acc.n_desc
            parent_acc.n_files += acc.n_files
            for i, v in enumerate(acc.pivot):
                parent_acc.pivot[i] += v
            parent_acc.mt_wsum += acc.mt_wsum
            raw_parent = _parent_of(acc.path)
            depth = acc.path.count('/') + 1
            parts.write(depth, 'dir', acc.path, (
                acc.path, acc.size, acc.mtime, 'dir',
                raw_parent if raw_parent else '.',
                f'{scan_root}/{acc.path}',
                acc.n_desc, acc.n_files, acc.n_children,
                depth,
                *dir_extras(acc),
            ))

        # Single-pivot-column fast path (the common CLI shape, e.g. just
        # `-p storage_class_id`): skip the per-row zip over pivot_maps.
        pmap0 = pivot_maps[0] if len(pivot_maps) == 1 else None

        for name, size, mtime, *pvals in merged:
            parent = _parent_of(name)
            # Pop everything the new row's parent chain has left behind.
            top = stack[-1]
            while not (top.path == '' or parent == top.path or parent.startswith(top.pfx)):
                pop_emit()
                top = stack[-1]
            # Push the dirs between the surviving top and the row's parent.
            if top.path != parent:
                rel = parent if top.path == '' else parent[len(top.path) + 1:]
                base = top.path
                for comp in rel.split('/'):
                    base = comp if base == '' else f'{base}/{comp}'
                    top.n_children += 1  # new dir is a direct child of current top
                    top = _Acc(base, n_pivot)
                    stack.append(top)
            # Fold the file into its parent (subtree totals propagate on pop).
            top.size += size
            top.mtime = max(top.mtime, mtime)
            top.n_desc += 1
            top.n_files += 1
            top.n_children += 1
            if n_pivot:
                file_pivot = [0] * n_pivot
                if pmap0 is not None:
                    v = pvals[0]
                    if v is not None:
                        idx = pmap0[v]
                        top.pivot[idx] += size
                        file_pivot[idx] = size
                else:
                    for pmap, v in zip(pivot_maps, pvals):
                        if v is not None:
                            idx = pmap[v]
                            top.pivot[idx] += size
                            file_pivot[idx] = size
            else:
                file_pivot = ()
            top.mt_wsum += mtime * size
            n_files_total += 1
            if n_files_total % 10_000_000 == 0:
                _stage(f"  …{n_files_total:,} files streamed")
            depth = name.count('/') + 1
            parts.write(depth, 'file', name, (
                name, size, mtime, 'file', parent, f'{scan_root}/{name}',
                1, 1, 0, depth,
                *file_pivot,
                *([float(mtime)] if mean_mtime else []),
            ))

        # EOF: close out the stack; the root emits last with its path/parent
        # normalized ('.'/'') the way the other engines do.
        while len(stack) > 1:
            pop_emit()
        root = stack.pop()
        parts.write(0, 'dir', '.', (
            '.', root.size, root.mtime, 'dir', '', scan_root,
            root.n_desc, root.n_files, root.n_children, 0,
            *dir_extras(root),
        ))
        part_list = parts.close()
    except BaseException:
        # Mid-stream failure: partial parts (no manifest) are unusable.
        shutil.rmtree(parts_dir, ignore_errors=True)
        raise

    manifest = {
        'pivot_names': pivot_names,
        'mean_mtime': mean_mtime,
        'rows': parts.n_rows,
        'files': n_files_total,
        'max_open_sources': hw['max_open'],
        'root_size': root.size,
        'root_n_desc': root.n_desc,
        'root_n_files': root.n_files,
        'root_n_children': root.n_children,
        'root_mtime': root.mtime,
        'parts': part_list,
    }
    with open(manifest_path, 'w') as fh:
        json.dump(manifest, fh)
    n_unsorted = sum(1 for p in part_list if not p['sorted'])
    _stage(
        f"streamed {parts.n_rows:,} rows ({n_files_total:,} files, "
        f"max {hw['max_open']} source(s) open) into {len(part_list)} part(s)"
        f" ({n_unsorted} unsorted); finalize"
    )
    return _finalize_and_clean(parts_dir, manifest, out_parquet)
