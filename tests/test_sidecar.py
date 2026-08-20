"""Tests for the vocab sidecar + block index (spec: diff-and-search.md, index tiers 1–2)."""

import os

import pandas as pd
import pyarrow.parquet as pq
import pytest

from disk_tree.filter import FilterNode, filter_scan, parse_query, query_mode
from disk_tree.sidecar import (
    build_vocab_sidecar,
    candidate_row_groups,
    indexed_filter_frame,
    sidecar_is_fresh,
    sidecar_path_for,
)


def _row(path, size, kind):
    return {'path': path, 'size': size, 'kind': kind, 'depth': path.count('/') + (0 if path == '.' else 1)}


def _frame() -> pd.DataFrame:
    """Same shape as test_filter's tree: an outermost `demo` dir whose
    contents must dedup, a deep match, and noise — written across several
    row groups so the block index has something to prune."""
    return pd.DataFrame([
        _row('.', 1450, 'dir'),
        _row('a', 700, 'dir'),
        _row('b', 730, 'dir'),
        _row('other.txt', 20, 'file'),
        _row('a/demo', 500, 'dir'),
        _row('a/noise.txt', 200, 'file'),
        _row('b/x', 730, 'dir'),
        _row('a/demo/demo.txt', 400, 'file'),
        _row('a/demo/other.bin', 100, 'file'),
        _row('b/x/y', 730, 'dir'),
        _row('b/x/y/z', 730, 'dir'),
        _row('b/x/y/z/deep-demo.dat', 730, 'file'),
    ])


@pytest.fixture
def blob(tmp_path):
    p = str(tmp_path / 'scan.parquet')
    # row_group_size=4 → 3 row groups over the 12 rows, in frame order.
    _frame().to_parquet(p, index=False, row_group_size=4)
    return p


def test_build_vocab_contents(blob):
    stats = build_vocab_sidecar(blob)
    assert stats.path == str(blob)[:-len('.parquet')] + '.vocab.parquet'
    assert (stats.n_names, stats.n_rows, stats.n_row_groups) == (11, 12, 3)  # 11 names; root '.' contributes none
    vocab = pq.read_table(stats.path).to_pandas()
    vocab['row_groups'] = vocab['row_groups'].apply(list)
    assert [tuple(r) for r in vocab.itertuples(index=False)] == [
        ('a', 1, 0, [0]),
        ('b', 1, 0, [0]),
        ('deep-demo.dat', 0, 1, [2]),
        ('demo', 1, 0, [1]),
        ('demo.txt', 0, 1, [1]),
        ('noise.txt', 0, 1, [1]),
        ('other.bin', 0, 1, [2]),
        ('other.txt', 0, 1, [0]),
        ('x', 1, 0, [1]),
        ('y', 1, 0, [2]),
        ('z', 1, 0, [2]),
    ]


def test_vocab_sorted_by_name(blob):
    stats = build_vocab_sidecar(blob)
    names = pq.read_table(stats.path, columns=['name'])['name'].to_pylist()
    assert names == sorted(names)


def test_candidate_row_groups(blob):
    sc = build_vocab_sidecar(blob).path
    assert candidate_row_groups(sc, parse_query('demo')) == ([1, 2], 3)  # demo, demo.txt, deep-demo.dat
    assert candidate_row_groups(sc, parse_query('other')) == ([0, 2], 2)
    assert candidate_row_groups(sc, parse_query('zzz-nothing')) == ([], 0)


def test_staleness(blob):
    sc = build_vocab_sidecar(blob).path
    assert sidecar_is_fresh(blob)
    # Blob rewritten after the sidecar (e.g. /api/delete) → sidecar is stale.
    future = os.path.getmtime(sc) + 10
    os.utime(blob, (future, future))
    assert not sidecar_is_fresh(blob)
    assert indexed_filter_frame(blob, 'demo') is None


def test_refuses_chunked_blobs(tmp_path):
    df = _frame()
    df['child_scan_id'] = [None] * (len(df) - 1) + ['some-scan-uuid']
    p = str(tmp_path / 'chunked.parquet')
    df.to_parquet(p, index=False)
    with pytest.raises(ValueError, match='chunk'):
        build_vocab_sidecar(p)


def test_indexed_equals_brute(blob):
    build_vocab_sidecar(blob)
    brute = filter_scan(_frame(), 'demo', display_depth=4)
    matched = indexed_filter_frame(blob, 'demo')
    indexed = filter_scan(matched, '', display_depth=4)
    assert indexed.nodes == brute.nodes
    assert (indexed.total_size, indexed.n_matches) == (brute.total_size, brute.n_matches)


def test_indexed_prefix_restriction(blob):
    build_vocab_sidecar(blob)
    matched = indexed_filter_frame(blob, 'demo', rel_path='a')
    result = filter_scan(matched, '', display_depth=4)
    assert result.nodes == [
        FilterNode(path='demo', depth=1, kind='dir', size=500, n_matches=1, matched=True),
    ]
    assert (result.total_size, result.n_matches) == (500, 1)


def test_indexed_declines_path_mode_and_empty(blob):
    build_vocab_sidecar(blob)
    assert indexed_filter_frame(blob, 'x/y') is None       # substring spans segments
    assert indexed_filter_frame(blob, '/b/x/') is None     # regex pattern contains '/'
    assert indexed_filter_frame(blob, '') is None          # match-all: index buys nothing
    assert indexed_filter_frame(blob, '/demo\\.(txt|dat)$/') is not None  # slash-free regex


def test_indexed_no_matches(blob):
    build_vocab_sidecar(blob)
    matched = indexed_filter_frame(blob, 'zzz-nothing')
    result = filter_scan(matched, '', display_depth=4)
    assert result.nodes == []
    assert (result.total_size, result.n_matches) == (0, 0)


def test_query_mode():
    assert query_mode('demo') == 'segment'
    assert query_mode('a/b') == 'path'
    assert query_mode('/demo\\.(txt|dat)$/') == 'segment'
    assert query_mode('/^foo//') == 'path'                 # greedy parse: pattern '^foo/' contains '/'
    assert query_mode('/demo(/') == 'path'                 # invalid regex → substring '/demo(/', slash included
    assert query_mode('') == 'segment'


def test_rebuild_short_circuits(blob):
    s1 = build_vocab_sidecar(blob)
    m1 = os.path.getmtime(s1.path)
    s2 = build_vocab_sidecar(blob)
    assert os.path.getmtime(s2.path) == m1
    assert (s2.n_names, s2.n_rows, s2.n_row_groups) == (s1.n_names, s1.n_rows, s1.n_row_groups)
    s3 = build_vocab_sidecar(blob, force=True)
    assert s3.n_names == s1.n_names
