"""The path store's search index (spec ``path-store-search.md``): what the
site's filter view (``q=``) reads to find match roots without scanning rows.

Built from a generation's ``path`` sort (``path-index.parquet``), three files
beside it:

- ``<stem>.names.parquet`` — the vocabulary: one row per distinct last path
  segment over every row (``id, name, n, b, n_rgs, rgs``), sorted ``b desc,
  name`` so ids are an impact order; ``rgs`` lists (comma-separated, ascending)
  the ``path``-sort row groups holding rows with that last segment, NULL past
  ``rg_cap`` groups;
- ``<stem>.trigrams.parquet`` — postings ``(tri, id)`` sorted ``(tri, id)``:
  every all-ASCII trigram of DuckDB ``lower(name)``, packed ``c0<<16 | c1<<8 |
  c2``;
- ``<stem>.search.parquet`` — the directory: one row per row group of both
  data files (``file`` 0 = names / 1 = trigrams, ``rg``, ``row_start``,
  ``row_end``, key range ``k_min``/``k_max`` = ``id`` / ``tri``, the compact
  ``rg_json`` a reader revives), statistics on the key columns, the data
  files' flat schemas and the build's counts in the key-value metadata.

A reader opens the directory's footer, prunes its groups by those stats,
then range-reads only the postings / names groups a query needs — never a
data file's own footer (a gcs-sized postings footer is ~10 MB of thrift).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

SEARCH_V = 1
NAMES_SUFFIX = '.names.parquet'
TRIGRAMS_SUFFIX = '.trigrams.parquet'
SEARCH_SUFFIX = '.search.parquet'
#: Rows per names row group — the reader's verification unit (~20–60 KB).
#: DuckDB flushes groups in 2048-row steps, so sizes are multiples of 2048.
NAMES_RG_ROWS = 2048
#: Rows per postings row group (~40 KB of sorted ids).
POSTINGS_RG_ROWS = 16384
#: A name in more `path` row groups than this gets a NULL `rgs`: wider than
#: any request may read (`n_rgs` still says how wide).
RG_CAP = 512
#: Rows per directory row group (the `.groups.parquet` default).
DIR_RG_ROWS = 512
ROW_GROUP_STEP = 2048
#: Directory columns with statistics (what a reader prunes by).
DIR_STAT_COLS = ('file', 'rg', 'k_min', 'k_max')
FILE_NAMES = 0
FILE_TRIGRAMS = 1


@dataclass
class SearchStats:
    names: int
    postings: int
    path_groups: int
    files: dict[str, str]


def search_paths(path_sort: str) -> dict[str, str]:
    """``…/path-index.parquet`` → its three sidecars, by role."""
    if not path_sort.endswith('.parquet'):
        raise ValueError(f"not a parquet path: {path_sort}")
    stem = path_sort[: -len('.parquet')]
    return {'names': stem + NAMES_SUFFIX, 'trigrams': stem + TRIGRAMS_SUFFIX, 'search': stem + SEARCH_SUFFIX}


def tri_code(t: str) -> int:
    """A 3-character ASCII trigram as the int the postings store."""
    if len(t) != 3 or any(ord(c) > 127 for c in t):
        raise ValueError(f"not an ASCII trigram: {t!r}")
    return (ord(t[0]) << 16) | (ord(t[1]) << 8) | ord(t[2])


def _q(s: str) -> str:
    return s.replace("'", "''")


def _rg_dir_rows(md: "pq.FileMetaData", file: int, key: str) -> list[dict]:
    """One directory row per row group of a data file: rows, key range, compact `rg_json`."""
    ki = md.schema.names.index(key)
    out = []
    start = 0
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        chunks = [rg.column(c) for c in range(rg.num_columns)]
        codecs = {cc.compression for cc in chunks}
        if len(codecs) != 1:
            raise ValueError(f"row group {g}: mixed codecs {sorted(codecs)}")
        st = chunks[ki].statistics
        if st is None or not st.has_min_max:
            raise ValueError(f"row group {g}: no `{key}` statistics")
        n = rg.num_rows
        cols = [[cc.data_page_offset, cc.total_compressed_size, cc.dictionary_page_offset or 0] for cc in chunks]
        out.append({
            'file': file, 'rg': g, 'row_start': start, 'row_end': start + n,
            'k_min': int(st.min), 'k_max': int(st.max),
            'rg_json': json.dumps([n, codecs.pop(), cols], separators=(',', ':')),
        })
        start += n
    return out


def _dir_schema() -> "pa.Schema":
    import pyarrow as pa
    return pa.schema([
        pa.field('file', pa.int8(), nullable=False),
        pa.field('rg', pa.int32(), nullable=False),
        pa.field('row_start', pa.int64(), nullable=False),
        pa.field('row_end', pa.int64(), nullable=False),
        pa.field('k_min', pa.int64(), nullable=False),
        pa.field('k_max', pa.int64(), nullable=False),
        pa.field('rg_json', pa.string(), nullable=False),
    ])


def write_search(
    path_sort: str,
    *,
    con: "duckdb.DuckDBPyConnection | None" = None,
    names_rg_rows: int = NAMES_RG_ROWS,
    postings_rg_rows: int = POSTINGS_RG_ROWS,
    rg_cap: int = RG_CAP,
    dir_rg_rows: int = DIR_RG_ROWS,
) -> SearchStats:
    """Write the search sidecars beside a local ``path`` sort (any store sort
    with ``path`` and ``size`` works; the row-group ordinals in ``rgs`` are
    this file's)."""
    import duckdb as _duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from disk_tree import listing_format as lf
    from disk_tree.find.groups import schema_json

    for name, v in (('names_rg_rows', names_rg_rows), ('postings_rg_rows', postings_rg_rows)):
        if v < ROW_GROUP_STEP or v % ROW_GROUP_STEP:
            raise ValueError(f"{name} must be a positive multiple of {ROW_GROUP_STEP}; got {v}")
    md = pq.read_metadata(path_sort)
    for need in ('path', 'size'):
        if need not in md.schema.names:
            raise ValueError(f"{path_sort}: no `{need}` column ({md.schema.names})")
    starts, s = [], 0
    for g in range(md.num_row_groups):
        starts.append(s)
        s += md.row_group(g).num_rows
    paths = search_paths(path_sort)
    own = con is None
    if own:
        con = _duckdb.connect()
    prev_order = con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    # The data files' row order IS the index (ids, (tri, id)): a parallel COPY
    # without insertion order may write sorted batches out of order.
    con.execute("SET preserve_insertion_order = true")
    codec = lf.duckdb_codec()
    try:
        con.register('_search_rgb', pa.table({'rg': pa.array(range(len(starts)), pa.int32()), 'row_start': pa.array(starts, pa.int64())}))
        names_tmp = paths['names'] + '.tmp'
        con.execute(f"""
            COPY (
              WITH r AS (
                SELECT regexp_extract(path, '[^/]*$') AS name, size, file_row_number AS i
                FROM read_parquet('{_q(path_sort)}', file_row_number = true)
              ),
              nr AS (
                SELECT r.name, g.rg, count(*) AS n, coalesce(sum(r.size), 0) AS b
                FROM r ASOF JOIN _search_rgb g ON r.i >= g.row_start
                GROUP BY r.name, g.rg
              ),
              nm AS (
                SELECT name, sum(n)::BIGINT AS n, sum(b)::BIGINT AS b, count(*)::INTEGER AS n_rgs,
                  CASE WHEN count(*) <= {int(rg_cap)} THEN string_agg(rg::VARCHAR, ',' ORDER BY rg) END AS rgs
                FROM nr GROUP BY name
              )
              SELECT (row_number() OVER (ORDER BY b DESC, name) - 1)::INTEGER AS id, name, n, b, n_rgs, rgs
              FROM nm ORDER BY b DESC, name
            ) TO '{_q(names_tmp)}' (FORMAT PARQUET, {codec}, ROW_GROUP_SIZE {names_rg_rows})
        """)
        os.replace(names_tmp, paths['names'])
        tri_tmp = paths['trigrams'] + '.tmp'
        con.execute(f"""
            COPY (
              WITH k AS (
                SELECT id, l, unnest(range(1, length(l) - 1)) AS k
                FROM (SELECT id, lower(name) AS l FROM read_parquet('{_q(paths['names'])}'))
              ),
              t AS (
                SELECT DISTINCT id,
                  (ascii(substr(l, k, 1)) * 65536 + ascii(substr(l, k + 1, 1)) * 256 + ascii(substr(l, k + 2, 1)))::INTEGER AS tri
                FROM k WHERE strlen(substr(l, k, 3)) = 3
              )
              SELECT tri, id FROM t ORDER BY tri, id
            ) TO '{_q(tri_tmp)}' (FORMAT PARQUET, {codec}, ROW_GROUP_SIZE {postings_rg_rows})
        """)
        os.replace(tri_tmp, paths['trigrams'])
    finally:
        con.unregister('_search_rgb')
        con.execute(f"SET preserve_insertion_order = {str(prev_order).lower()}")
        if own:
            con.close()
    nmd = pq.read_metadata(paths['names'])
    tmd = pq.read_metadata(paths['trigrams'])
    rows = _rg_dir_rows(nmd, FILE_NAMES, 'id') if nmd.num_rows else []
    rows += _rg_dir_rows(tmd, FILE_TRIGRAMS, 'tri') if tmd.num_rows else []
    t = pa.table({c: [r[c] for r in rows] for c in _dir_schema().names}, schema=_dir_schema())
    kv = {
        'search_v': str(SEARCH_V),
        'names_schema': json.dumps(schema_json(nmd)['schema'], separators=(',', ':')),
        'trigrams_schema': json.dumps(schema_json(tmd)['schema'], separators=(',', ':')),
        'names': str(nmd.num_rows),
        'postings': str(tmd.num_rows),
        'rg_cap': str(rg_cap),
        'path_groups': str(md.num_row_groups),
        'path_rows': str(md.num_rows),
    }
    tmp = paths['search'] + '.tmp'
    with pq.ParquetWriter(tmp, t.schema, compression='zstd', write_statistics=list(DIR_STAT_COLS), store_schema=False) as w:
        w.write_table(t, row_group_size=dir_rg_rows)
        w.add_key_value_metadata(kv)
    os.replace(tmp, paths['search'])
    return SearchStats(names=nmd.num_rows, postings=tmd.num_rows, path_groups=md.num_row_groups, files=paths)
