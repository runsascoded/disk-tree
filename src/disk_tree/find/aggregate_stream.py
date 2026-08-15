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

import heapq
import re
from glob import glob as _glob
from operator import itemgetter
from typing import Iterator

_SLASHES = re.compile(r'/+')

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


def _scan_names(
    shards: list[str],
    bucket: str,
    pivot_sums: tuple[str, ...] = (),
) -> tuple[int, set[str], list[list], dict[str, list[int]]]:
    """Pass 1: names scan. Returns (total rows for bucket, shards with dirty
    keys, sorted distinct non-null values per pivot column, sorted-run start
    ordinals per shard).

    Runs: `bulk-list` bin-packs multiple non-contiguous key ranges into each
    shard (weight balancing), so a shard is *piecewise* sorted — a
    concatenation of internally-sorted runs. Each run becomes its own k-way
    merge source downstream. Ordinals are bucket-filtered row indices,
    matching `_shard_rows`' accounting."""
    import numpy as np
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    n_rows = 0
    dirty_shards: set[str] = set()
    distinct: list[set] = [set() for _ in pivot_sums]
    runs: dict[str, list[int]] = {}
    total_runs = 0
    for shard in shards:
        pf = pq.ParquetFile(shard)
        ord0 = 0
        prev_last: str | None = None
        starts: list[int] = []
        for batch in pf.iter_batches(columns=['bucket', 'name', *pivot_sums]):
            mask = pc.equal(batch.column('bucket'), bucket)
            names = batch.column('name').filter(mask)
            n = len(names)
            if n == 0:
                continue
            n_rows += n
            if not starts:
                starts.append(0)
            if prev_last is not None and names[0].as_py() < prev_last:
                starts.append(ord0)
            if n > 1:
                ge = pc.greater_equal(names.slice(1), names.slice(0, n - 1))
                for idx in np.nonzero(~ge.to_numpy(zero_copy_only=False))[0]:
                    starts.append(ord0 + int(idx) + 1)
            prev_last = names[n - 1].as_py()
            ord0 += n
            if pc.any(_dirty_mask(names)).as_py():
                dirty_shards.add(shard)
            for i, col in enumerate(pivot_sums):
                vals = pc.unique(pc.drop_null(batch.column(col).filter(mask)))
                distinct[i].update(vals.to_pylist())
        if starts:
            runs[shard] = starts
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
    *pivot_values). `[start, stop)` are bucket-filtered row ordinals from
    `_scan_names`' run detection; the slice is verified sorted as it goes
    (a violation means the two passes disagreed — a bug, not bad input)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(shard)
    cols = ['bucket', 'name', 'size_bytes', *pivot_sums]
    has_created = 'created' in pf.schema_arrow.names
    if has_created:
        cols.append('created')
    prev: str | None = None
    ord0 = 0
    for batch in pf.iter_batches(columns=cols):
        mask = pc.equal(batch.column('bucket'), bucket)
        names_arr = batch.column('name').filter(mask)
        n = len(names_arr)
        if n == 0:
            continue
        lo = max(start - ord0, 0)
        hi = n if stop is None else min(stop - ord0, n)
        ord0 += n
        if lo >= hi:
            if stop is not None and ord0 > stop:
                break
            continue
        names_sl = names_arr.slice(lo, hi - lo)
        m = len(names_sl)
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
        sizes = pc.fill_null(batch.column('size_bytes'), 0).filter(mask).slice(lo, hi - lo).filter(clean).to_pylist()
        if has_created:
            mtimes = _epoch_seconds(batch.column('created')).filter(mask).slice(lo, hi - lo).filter(clean).to_pylist()
        else:
            mtimes = [0] * len(names)
        pivots = [batch.column(c).filter(mask).slice(lo, hi - lo).filter(clean).to_pylist() for c in pivot_sums]
        yield from zip(names, map(int, sizes), map(int, mtimes), *pivots)


class _Acc:
    """Running accumulators for one open dir on the stack."""
    __slots__ = ('path', 'size', 'mtime', 'n_desc', 'n_files', 'n_children', 'pivot', 'mt_wsum')

    def __init__(self, path: str, n_pivot: int):
        self.path = path
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


def aggregate_stream(
    listings: tuple[str, ...],
    bucket: str,
    scheme: str,
    out_parquet: str,
    con: "object | None" = None,
    memory_limit: str | None = None,
    temp_dir: str | None = None,
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
    """
    import os
    import tempfile
    from .agg_ext import MTIME_MEAN, check_pivot_values, mean_of, pivot_col

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
        n_rows, dirty_shards, distinct, runs = _scan_names(shards, bucket, pivot_sums)
        if n_rows > 0:
            chosen = shards
            break
    if chosen is None:
        raise ValueError(f"no rows for bucket {bucket!r}")

    dirty = _collect_dirty(sorted(dirty_shards), bucket, pivot_sums) if dirty_shards else []

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
    sources = []
    for s in chosen:
        starts = runs.get(s)
        if not starts:
            continue
        bounds: list[int | None] = [*starts, None]
        for a, b in zip(bounds, bounds[1:]):
            sources.append(_shard_rows(s, bucket, pivot_sums, start=a, stop=b))
    merged = heapq.merge(*sources, dirty, key=itemgetter(0))

    fd, tmp_parquet = tempfile.mkstemp(suffix='.parquet', dir=os.path.dirname(out_parquet) or None)
    os.close(fd)
    writer = _Writer(tmp_parquet, out_cols, mean_mtime)
    n_files_total = 0
    root_stats: dict = {}
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
            writer.write((
                acc.path, acc.size, acc.mtime, 'dir',
                raw_parent if raw_parent else '.',
                f'{scan_root}/{acc.path}',
                acc.n_desc, acc.n_files, acc.n_children,
                acc.path.count('/') + 1,
                *dir_extras(acc),
            ))

        for name, size, mtime, *pvals in merged:
            parent = _parent_of(name)
            # Pop everything the new row's parent chain has left behind.
            top = stack[-1]
            while not (top.path == '' or parent == top.path or parent.startswith(top.path + '/')):
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
            file_pivot = [0] * n_pivot
            for pmap, v in zip(pivot_maps, pvals):
                if v is not None:
                    idx = pmap[v]
                    top.pivot[idx] += size
                    file_pivot[idx] = size
            top.mt_wsum += mtime * size
            n_files_total += 1
            writer.write((
                name, size, mtime, 'file', parent, f'{scan_root}/{name}',
                1, 1, 0, name.count('/') + 1,
                *file_pivot,
                *([float(mtime)] if mean_mtime else []),
            ))

        # EOF: close out the stack; the root emits last with its path/parent
        # normalized ('.'/'') the way the other engines do.
        while len(stack) > 1:
            pop_emit()
        root = stack.pop()
        writer.write((
            '.', root.size, root.mtime, 'dir', '', scan_root,
            root.n_desc, root.n_files, root.n_children, 0,
            *dir_extras(root),
        ))
        root_stats = {
            'root_size': root.size,
            'root_n_desc': root.n_desc,
            'root_n_files': root.n_files,
            'root_n_children': root.n_children,
            'root_mtime': root.mtime,
        }
        writer.close()

        # ---- final bounded sort: restore the (depth, path) contract ----
        import duckdb as _duckdb
        _con = con if con is not None else _duckdb.connect()
        if memory_limit:
            _con.execute(f"SET memory_limit = '{memory_limit}'")
        if temp_dir:
            _con.execute(f"SET temp_directory = '{temp_dir}'")
        # Canonical column order (matches the pandas concat / duckdb tail:
        # extras between `parent` and `uri`) — the pre-output's internal order
        # is a writer convenience; the published layer-2 must be column-order
        # identical across engines so file-level diffs and positional set ops
        # (EXCEPT) work.
        canonical_cols = [
            'path', 'size', 'mtime', 'n_desc', 'n_files', 'n_children', 'kind', 'parent',
            *pivot_names, *([MTIME_MEAN] if mean_mtime else []),
            'uri', 'depth',
        ]
        _con.execute(f"""
            COPY (
                SELECT {', '.join(canonical_cols)}
                FROM read_parquet('{tmp_parquet}')
                -- kind tiebreaker: when a key is both a file and a dir name,
                -- the other engines emit the dir row first (stable sort over
                -- dirs-then-files concat); 'dir' < 'file' reproduces that.
                ORDER BY depth, path, kind
            ) TO '{out_parquet}' (FORMAT PARQUET)
        """)
    finally:
        if os.path.exists(tmp_parquet):
            os.remove(tmp_parquet)

    return {
        'rows': writer.n_rows,
        'files': n_files_total,
        **root_stats,
    }
