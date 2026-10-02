"""The path store's search sidecars (`disk_tree.find.search`, spec
`path-store-search.md` §2): exact names, postings and directory rows for a
hand-built `path` sort spanning three 2048-row groups."""

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from disk_tree.find.search import search_paths, tri_code, write_search

N_FILL = 4500
#: (path, size) in `(depth, path)` order — the `path` sort's order.
ROWS = [
    ('bk', 10000), ('zz', 300),
    ('bk/TTL', 2000), ('bk/a', 3000), ('bk/f', 4500), ('zz/ttl', 300),
    ('bk/TTL/ttl', 1500), ('bk/a/x.bin', 3000),
    *((f'bk/f/n{i:04d}', 1) for i in range(N_FILL)),
    ('zz/ttl/x.bin', 300),
]
RG = 2048


def _path_sort(path: Path) -> str:
    t = pa.table({
        'path': [p for p, _ in ROWS],
        'depth': pa.array([p.count('/') + 1 for p, _ in ROWS], pa.int32()),
        'size': pa.array([s for _, s in ROWS], pa.int64()),
    })
    pq.write_table(t, path, row_group_size=RG)
    return str(path)


def _rows(path: str, cols: str = '*') -> list[tuple]:
    return duckdb.connect().execute(f"SELECT {cols} FROM read_parquet('{path}')").fetchall()


def _expected_names(rg_cap: int = 512) -> list[tuple]:
    """(id, name, n, b, n_rgs, rgs), `b desc, name`: rows by their index → rg = index // 2048."""
    def rgs(*rg: int) -> str | None:
        return ','.join(map(str, rg)) if len(rg) <= rg_cap else None
    head = [
        ('bk', 1, 10000, 1, rgs(0)),
        ('f', 1, 4500, 1, rgs(0)),
        ('x.bin', 2, 3300, 2, rgs(0, 2)),  # rows 7 and 4508
        ('a', 1, 3000, 1, rgs(0)),
        ('TTL', 1, 2000, 1, rgs(0)),
        ('ttl', 2, 1800, 1, rgs(0)),  # zz/ttl (row 5), bk/TTL/ttl (row 6)
        ('zz', 1, 300, 1, rgs(0)),
    ]
    fill = [(f'n{i:04d}', 1, 1, 1, rgs((8 + i) // RG)) for i in range(N_FILL)]
    return [(i, *r) for i, r in enumerate(head + fill)]


def _expected_postings(names: list[tuple]) -> list[tuple[int, int]]:
    out = set()
    for id, name, *_ in names:
        low = name.lower()
        for k in range(len(low) - 2):
            t = low[k:k + 3]
            if all(ord(c) < 128 for c in t):
                out.add((tri_code(t), id))
    return sorted(out)


def test_write_search(tmp_path: Path):
    src = _path_sort(tmp_path / 'path-index.parquet')
    st = write_search(src, names_rg_rows=2048, postings_rg_rows=2048, dir_rg_rows=4)
    paths = search_paths(src)
    assert paths == {
        'names': str(tmp_path / 'path-index.names.parquet'),
        'trigrams': str(tmp_path / 'path-index.trigrams.parquet'),
        'search': str(tmp_path / 'path-index.search.parquet'),
    }
    names = _expected_names()
    postings = _expected_postings(names)
    assert (st.names, st.postings, st.path_groups, st.files) == (len(names), len(postings), 3, paths)

    # The vocabulary: one row per last segment, in impact order.
    assert _rows(paths['names']) == names
    assert [pq.read_metadata(paths['names']).row_group(g).num_rows for g in range(3)] == [2048, 2048, 411]
    # Postings: every ASCII trigram of the lowercased name, `(tri, id)` order.
    assert _rows(paths['trigrams']) == postings
    ttl = tri_code('ttl')
    assert [id for t, id in postings if t == ttl] == [4, 5]  # `TTL` and `ttl` alike

    # The directory: one row per data row group, key ranges = ids / trigrams.
    d = pq.read_table(paths['search']).to_pylist()
    for r in d:
        assert json.loads(r.pop('rg_json'))[0] == r['row_end'] - r['row_start']
    n_post = len(postings)
    post_groups = [(i * RG, min((i + 1) * RG, n_post)) for i in range((n_post + RG - 1) // RG)]
    assert d == [
        {'file': 0, 'rg': 0, 'row_start': 0, 'row_end': 2048, 'k_min': 0, 'k_max': 2047},
        {'file': 0, 'rg': 1, 'row_start': 2048, 'row_end': 4096, 'k_min': 2048, 'k_max': 4095},
        {'file': 0, 'rg': 2, 'row_start': 4096, 'row_end': 4507, 'k_min': 4096, 'k_max': 4506},
        *({'file': 1, 'rg': g, 'row_start': a, 'row_end': b, 'k_min': postings[a][0], 'k_max': postings[b - 1][0]} for g, (a, b) in enumerate(post_groups)),
    ]
    md = pq.read_metadata(paths['search'])
    assert md.num_row_groups == (len(d) + 3) // 4
    kv = {k.decode(): v.decode() for k, v in md.metadata.items()}
    schemas = {k: [e['name'] for e in json.loads(kv.pop(k))] for k in ('names_schema', 'trigrams_schema')}
    assert schemas == {'names_schema': ['schema', 'id', 'name', 'n', 'b', 'n_rgs', 'rgs'], 'trigrams_schema': ['schema', 'tri', 'id']}
    assert kv == {'search_v': '1', 'names': str(len(names)), 'postings': str(n_post), 'rg_cap': '512', 'path_groups': '3', 'path_rows': str(len(ROWS))}
    # Statistics only on what a reader prunes by.
    rg0 = md.row_group(0)
    assert {rg0.column(c).path_in_schema: rg0.column(c).statistics is not None for c in range(rg0.num_columns)} == {
        'file': True, 'rg': True, 'row_start': False, 'row_end': False, 'k_min': True, 'k_max': True, 'rg_json': False,
    }


def test_rg_cap(tmp_path: Path):
    """A name in more `path` groups than `rg_cap` keeps its count, not its list."""
    src = _path_sort(tmp_path / 'path-index.parquet')
    write_search(src, rg_cap=1)
    assert _rows(search_paths(src)['names']) == _expected_names(rg_cap=1)


def test_unicode_names(tmp_path: Path):
    """Trigrams come from DuckDB `lower()`, ASCII ones only: the Kelvin sign
    lowercases to `k` (as in JS), and a non-ASCII trigram is never stored."""
    p = tmp_path / 'path-index.parquet'
    pq.write_table(pa.table({'path': ['bk', 'bk/Key', 'bk/cafés'], 'size': pa.array([3, 2, 1], pa.int64())}), p)
    write_search(str(p))
    tri = {v: k for k, v in {t: tri_code(t) for t in ('key', 'caf')}.items()}
    assert [(tri.get(t, t), id) for t, id in _rows(search_paths(str(p))['trigrams'])] == [('caf', 2), ('key', 1)]


def test_rejects(tmp_path: Path):
    p = tmp_path / 'x.parquet'
    pq.write_table(pa.table({'path': ['a']}), p)
    with pytest.raises(ValueError, match='no `size` column'):
        write_search(str(p))
    with pytest.raises(ValueError, match='multiple of 2048'):
        write_search(str(p), names_rg_rows=1000)
