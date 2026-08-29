"""Tests for the persisted diff index (spec: diff-index.md)."""

import pyarrow as pa
import pyarrow.parquet as pq

from disk_tree.diff_index import DiffIndexStats, _normalize, build_diff_table, load_index_slice, serve_slice
from test_diff_recursive import _scan_a, _scan_b_deep_change


def _tbl(df):
    return _normalize(pa.Table.from_pandas(df, preserve_index=False))


def _rows(tbl):
    return [
        (r['path'], r['depth'], r['kind'], r['status'], int(r['size_a'] or 0), int(r['size_b'] or 0), r['context'])
        for r in tbl.to_pylist()
    ]


def test_deep_change_with_context():
    """The changed spine down to the file, plus `a` as unchanged context
    beside `b` (their parent has a change); `a`'s own children are not
    stored (nothing changed beside them)."""
    tbl, stats = build_diff_table(_tbl(_scan_a()), _tbl(_scan_b_deep_change()))
    assert _rows(tbl) == [
        ('.', 0, 'dir', 'changed', 1000, 2000, False),
        ('a', 1, 'dir', 'unchanged', 400, 400, True),
        ('b', 1, 'dir', 'changed', 600, 1600, False),
        ('b/sub', 2, 'dir', 'changed', 600, 1600, False),
        ('b/sub/big.bin', 3, 'file', 'changed', 600, 1600, False),
    ]
    assert stats == DiffIndexStats(n_rows=5, n_added=0, n_removed=0, n_changed=4, n_touched=0, n_context=1)


def test_rename_is_touched_dir_plus_added_removed():
    df_b = _scan_a()
    df_b.loc[df_b['path'] == 'a/f2.txt', 'path'] = 'a/f3.txt'
    df_b.loc[df_b['path'] == 'a', 'mtime'] = 95.0
    tbl, stats = build_diff_table(_tbl(_scan_a()), _tbl(df_b))
    assert _rows(tbl) == [
        ('a', 1, 'dir', 'touched', 400, 400, False),
        ('b', 1, 'dir', 'unchanged', 600, 600, True),
        ('a/f1.txt', 2, 'file', 'unchanged', 100, 100, True),
        ('a/f2.txt', 2, 'file', 'removed', 300, 0, False),
        ('a/f3.txt', 2, 'file', 'added', 0, 300, False),
    ]
    assert stats.n_touched == 1 and stats.n_added == 1 and stats.n_removed == 1


def test_removed_dir_is_one_row():
    """Nothing under a removed (or added) dir is stored — its row is the story."""
    df_b = _scan_a()
    df_b = df_b[~df_b['path'].str.startswith('b')]
    df_b.loc[df_b['path'] == '.', ['size', 'n_desc', 'n_children']] = [400, 3, 1]
    tbl, _ = build_diff_table(_tbl(_scan_a()), _tbl(df_b))
    assert [(r[0], r[3]) for r in _rows(tbl)] == [
        ('.', 'changed'),
        ('a', 'unchanged'),
        ('b', 'removed'),
    ]


def test_serve_slice_root_and_subtree(tmp_path):
    tbl, _ = build_diff_table(_tbl(_scan_a()), _tbl(_scan_b_deep_change()))
    path = str(tmp_path / 'idx.parquet')
    pq.write_table(tbl, path)

    root = serve_slice(load_index_slice(path, ''))
    assert [(r['path'], r['depth'], r['status'], r['expanded'], r['pruned']) for r in root['rows']] == [
        ('b', 1, 'changed', True, False),
        ('b/sub', 2, 'changed', True, False),
        ('b/sub/big.bin', 3, 'changed', False, False),
    ]
    assert [(r['path'], r['status']) for r in root['unchanged']['top']] == [('a', 'unchanged')]
    assert root['unchanged']['rest'] == {}
    assert root['truncated'] is False
    assert root['expansions'] == 2

    sub = serve_slice(load_index_slice(path, 'b'))
    assert [(r['path'], r['depth'], r['status']) for r in sub['rows']] == [
        ('sub', 1, 'changed'),
        ('sub/big.bin', 2, 'changed'),
    ]


def test_serve_slice_row_budget_keeps_spines_marks_pruned(tmp_path):
    tbl, _ = build_diff_table(_tbl(_scan_a()), _tbl(_scan_b_deep_change()))
    path = str(tmp_path / 'idx.parquet')
    pq.write_table(tbl, path)
    resp = serve_slice(load_index_slice(path, ''), max_rows=2)
    # top-2 by |Δ| are `b` and `b/sub` (ties broken by path); big.bin is
    # trimmed, so its parent is `pruned`
    assert [(r['path'], r['expanded'], r['pruned']) for r in resp['rows']] == [
        ('b', True, False),
        ('b/sub', False, True),
    ]
    assert resp['truncated'] is True


def test_serve_slice_min_frac_trims_and_marks_pruned(tmp_path):
    """Rows too small to draw are dropped (`min_frac` of the compared
    subtree), their ancestors kept, and the dirs that lost descendants are
    `pruned` — drilling into one gets a slice with finer thresholds."""
    df_b = _scan_b_deep_change()          # b/sub/big.bin 600 → 1600
    for path, delta in (('.', 10), ('a', 10), ('a/f1.txt', 10)):
        df_b.loc[df_b['path'] == path, 'size'] += delta   # a tiny change too
    tbl, _ = build_diff_table(_tbl(_scan_a()), _tbl(df_b))
    path = str(tmp_path / 'idx.parquet')
    pq.write_table(tbl, path)

    full = serve_slice(load_index_slice(path, ''), min_frac=0, total=2010)
    assert [r['path'] for r in full['rows']] == ['b', 'b/sub', 'b/sub/big.bin', 'a', 'a/f1.txt']
    assert full['truncated'] is False

    # 10% of the root = 201 bytes: `a/f1.txt` (110 bytes, Δ10) can't be drawn,
    # so it goes; `a` (410) stays and is marked `pruned` for the child it lost
    trimmed = serve_slice(load_index_slice(path, ''), min_frac=0.1, total=2010)
    assert [(r['path'], r['pruned']) for r in trimmed['rows']] == [
        ('b', False), ('b/sub', False), ('b/sub/big.bin', False), ('a', True),
    ]
    assert trimmed['truncated'] is True


def test_serve_slice_max_depth(tmp_path):
    tbl, _ = build_diff_table(_tbl(_scan_a()), _tbl(_scan_b_deep_change()))
    path = str(tmp_path / 'idx.parquet')
    pq.write_table(tbl, path)
    resp = serve_slice(load_index_slice(path, ''), max_depth=2)
    assert [(r['path'], r['depth'], r['pruned']) for r in resp['rows']] == [
        ('b', 1, False),
        ('b/sub', 2, True),
    ]
    assert resp['truncated'] is True
