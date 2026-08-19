"""Tests for the best-first pruned recursive diff (spec: diff-and-search.md §3a)."""

import tempfile

import pandas as pd
import pytest

from disk_tree.diff import DeltaRow, ScanSource, recursive_diff
from disk_tree.storage.parquet import ParquetBackend


def _row(path, size, kind, parent, n_desc, n_children, mtime, depth):
    return {
        'path': path, 'size': size, 'mtime': mtime, 'kind': kind, 'parent': parent,
        'uri': f'/r/{path}' if path != '.' else '/r', 'n_desc': n_desc,
        'n_children': n_children, 'depth': depth,
    }


def _scan_a() -> pd.DataFrame:
    return pd.DataFrame([
        _row('.', 1000, 'dir', '', 6, 2, 100.0, 0),
        _row('a', 400, 'dir', '.', 2, 2, 90.0, 1),
        _row('b', 600, 'dir', '.', 2, 1, 100.0, 1),
        _row('a/f1.txt', 100, 'file', 'a', 0, 0, 80.0, 2),
        _row('a/f2.txt', 300, 'file', 'a', 0, 0, 90.0, 2),
        _row('b/sub', 600, 'dir', 'b', 1, 1, 100.0, 2),
        _row('b/sub/big.bin', 600, 'file', 'b/sub', 0, 0, 100.0, 3),
    ])


def _scan_b_deep_change() -> pd.DataFrame:
    """`big.bin` grows 600→1600, three levels deep; `a` untouched."""
    df = _scan_a()
    for p in ('.', 'b', 'b/sub', 'b/sub/big.bin'):
        df.loc[df['path'] == p, 'size'] += 1000
        df.loc[df['path'] == p, 'mtime'] = 110.0
    return df


@pytest.fixture
def sources(tmp_path):
    """Build (make_sources) over a temp ParquetBackend."""
    backend = ParquetBackend(scans_dir=str(tmp_path))

    def make(df_a: pd.DataFrame, df_b: pd.DataFrame, uri='/r', scan_path='/r'):
        blob_a = backend.save(df_a, scan_path)
        blob_b = backend.save(df_b, scan_path)
        return (
            ScanSource(blob_a, scan_path, uri, backend.load),
            ScanSource(blob_b, scan_path, uri, backend.load),
        )

    return make


def test_localizes_a_deep_change(sources):
    """One request returns the changed spine down to the file — not one level."""
    src_a, src_b = sources(_scan_a(), _scan_b_deep_change())
    result = recursive_diff(src_a, src_b)
    assert result.rows == [
        DeltaRow('b', 1, 'dir', 'changed', 600, 1600, 2, 2, expanded=True),
        DeltaRow('b/sub', 2, 'dir', 'changed', 600, 1600, 1, 1, expanded=True),
        DeltaRow('b/sub/big.bin', 3, 'file', 'changed', 600, 1600, 0, 0),
    ]
    assert result.expansions == 3  # root, b, b/sub — `a` pruned (stats equal)
    assert result.truncated is False


def test_added_dir_is_reported_not_descended(sources):
    """An added dir's aggregate row is the whole story — no child rows."""
    df_b = pd.concat([
        _scan_a(),
        pd.DataFrame([
            _row('c', 300, 'dir', '.', 1, 1, 120.0, 1),
            _row('c/x.txt', 300, 'file', 'c', 0, 0, 120.0, 2),
        ]),
    ], ignore_index=True)
    df_b.loc[df_b['path'] == '.', ['size', 'n_desc', 'n_children']] = [1300, 8, 3]
    src_a, src_b = sources(_scan_a(), df_b)
    result = recursive_diff(src_a, src_b)
    assert result.rows == [
        DeltaRow('c', 1, 'dir', 'added', 0, 300, 0, 1),
    ]
    assert result.expansions == 1  # root only
    assert result.truncated is False


def test_budget_marks_pruned_frontier(sources):
    src_a, src_b = sources(_scan_a(), _scan_b_deep_change())
    result = recursive_diff(src_a, src_b, budget=1)
    assert result.rows == [
        DeltaRow('b', 1, 'dir', 'changed', 600, 1600, 2, 2, expanded=False, pruned=True),
    ]
    assert result.expansions == 1
    assert result.truncated is True


def test_max_depth_marks_pruned_frontier(sources):
    src_a, src_b = sources(_scan_a(), _scan_b_deep_change())
    result = recursive_diff(src_a, src_b, max_depth=1)
    assert result.rows == [
        DeltaRow('b', 1, 'dir', 'changed', 600, 1600, 2, 2, expanded=False, pruned=True),
    ]
    assert result.truncated is True


def test_net_zero_rename_is_found_via_mtime(sources):
    """f2→f3 rename: dir sizes/counts identical, only mtime differs — the walk
    still descends and surfaces the rename as added + removed rows."""
    df_b = _scan_a()
    df_b.loc[df_b['path'] == 'a/f2.txt', 'path'] = 'a/f3.txt'
    df_b.loc[df_b['path'] == 'a', 'mtime'] = 95.0
    src_a, src_b = sources(_scan_a(), df_b)
    result = recursive_diff(src_a, src_b)
    assert result.rows == [
        DeltaRow('a/f2.txt', 2, 'file', 'removed', 300, 0, 0, 0),
        DeltaRow('a/f3.txt', 2, 'file', 'added', 0, 300, 0, 0),
    ]
    assert result.expansions == 2  # root + a
    assert result.truncated is False


def test_ancestor_scan_rebases_into_uri(sources):
    """Comparing `/r/b` against scans of `/r` walks only b's spine, with paths
    relative to the compared uri."""
    src_a, src_b = sources(_scan_a(), _scan_b_deep_change(), uri='/r/b', scan_path='/r')
    result = recursive_diff(src_a, src_b)
    assert result.rows == [
        DeltaRow('sub', 1, 'dir', 'changed', 600, 1600, 1, 1, expanded=True),
        DeltaRow('sub/big.bin', 2, 'file', 'changed', 600, 1600, 0, 0),
    ]
    assert result.truncated is False


def test_include_unchanged(sources):
    src_a, src_b = sources(_scan_a(), _scan_b_deep_change())
    result = recursive_diff(src_a, src_b, include_unchanged=True)
    changed = [r for r in result.rows if r.status != 'unchanged']
    unchanged = sorted((r.path for r in result.rows if r.status == 'unchanged'))
    assert len(changed) == 3
    # `a` (stats-equal dir) and big.bin's siblings-free levels contribute none;
    # unchanged rows come only from expanded dirs' listings.
    assert unchanged == ['a']
