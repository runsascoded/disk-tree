"""Tests for the recursive filter core (spec: diff-and-search.md §4, v1)."""

import pandas as pd

from disk_tree.filter import FilterNode, filter_scan, parse_query


def _row(path, size, kind, depth):
    return {'path': path, 'size': size, 'kind': kind, 'depth': depth}


def _tree() -> pd.DataFrame:
    """Two projects named `demo` at different depths, plus noise. `a/demo` is
    a matched *dir* whose contents must count once (via its aggregate) and
    never again (outermost-only)."""
    return pd.DataFrame([
        _row('.', 1450, 'dir', 0),
        _row('a', 700, 'dir', 1),
        _row('b', 730, 'dir', 1),
        _row('other.txt', 20, 'file', 1),
        _row('a/demo', 500, 'dir', 2),            # outermost match (dir)
        _row('a/noise.txt', 200, 'file', 2),
        _row('b/x', 730, 'dir', 2),
        _row('a/demo/demo.txt', 400, 'file', 3),  # inside a match: covered
        _row('a/demo/other.bin', 100, 'file', 3),
        _row('b/x/y', 730, 'dir', 3),
        _row('b/x/y/z', 730, 'dir', 4),
        _row('b/x/y/z/deep-demo.dat', 730, 'file', 5),  # match below display depth
    ])


def test_outermost_dedup_and_rollup():
    result = filter_scan(_tree(), 'demo', display_depth=4)
    # a/demo counts once (500, its aggregate); a/demo/demo.txt does NOT add.
    # deep-demo.dat (depth 5) rolls into its depth<=4 ancestors.
    assert result.nodes == [
        FilterNode(path='a', depth=1, kind='dir', size=500, n_matches=1, matched=False),
        FilterNode(path='b', depth=1, kind='dir', size=730, n_matches=1, matched=False),
        FilterNode(path='a/demo', depth=2, kind='dir', size=500, n_matches=1, matched=True),
        FilterNode(path='b/x', depth=2, kind='dir', size=730, n_matches=1, matched=False),
        FilterNode(path='b/x/y', depth=3, kind='dir', size=730, n_matches=1, matched=False),
        FilterNode(path='b/x/y/z', depth=4, kind='dir', size=730, n_matches=1, matched=False),
    ]
    assert result.total_size == 1230  # 500 + 730, NOT 400 more from demo.txt
    assert result.n_matches == 2
    assert result.max_depth_scanned == 5


def test_display_depth_bounds_nodes_not_matches():
    result = filter_scan(_tree(), 'demo', display_depth=1)
    assert result.nodes == [
        FilterNode(path='a', depth=1, kind='dir', size=500, n_matches=1, matched=False),
        FilterNode(path='b', depth=1, kind='dir', size=730, n_matches=1, matched=False),
    ]
    # Totals still cover every depth — display depth only shapes the slice.
    assert result.total_size == 1230
    assert result.n_matches == 2


def test_regex_query():
    result = filter_scan(_tree(), r'/demo\.(txt|dat)$/', display_depth=4)
    # a/demo/demo.txt matches on its own now (a/demo does not match this
    # pattern, so it isn't covered); deep-demo.dat matches below display.
    assert [(n.path, n.size, n.matched) for n in result.nodes] == [
        ('a', 400, False),
        ('b', 730, False),
        ('a/demo', 400, False),
        ('b/x', 730, False),
        ('a/demo/demo.txt', 400, True),
        ('b/x/y', 730, False),
        ('b/x/y/z', 730, False),
    ]
    assert result.total_size == 1130
    assert result.n_matches == 2


def test_case_insensitive_by_default():
    result = filter_scan(_tree(), 'DEMO', display_depth=4)
    assert result.n_matches == 2
    assert result.total_size == 1230


def test_invalid_regex_degrades_to_substring():
    # `/demo(/` is an unterminated group — must not raise, must match nothing
    # (no path contains the literal '/demo(/').
    result = filter_scan(_tree(), '/demo(/', display_depth=4)
    assert result.nodes == []
    assert result.n_matches == 0


def test_empty_query_matches_everything_outermost():
    result = filter_scan(_tree(), '', display_depth=4)
    # Every depth-1 node is an outermost match; nothing below survives dedup
    # (files at depth 1 match too).
    assert [(n.path, n.matched) for n in result.nodes] == [
        ('a', True), ('b', True), ('other.txt', True),
    ]
    assert result.total_size == 1450
    assert result.n_matches == 3


def test_on_depth_snapshots_are_cumulative():
    depths: list[tuple[int, int, int]] = []
    filter_scan(_tree(), 'demo', display_depth=4,
                on_depth=lambda d, snap: depths.append((d, snap.n_matches, snap.total_size)))
    assert depths == [
        (1, 0, 0),
        (2, 1, 500),     # a/demo found
        (3, 1, 500),     # demo.txt covered by a/demo — no change
        (4, 1, 500),
        (5, 2, 1230),    # deep-demo.dat lands
    ]


def test_parse_query_vectorized_shapes():
    s = pd.Series(['Foo/Bar', 'baz'])
    assert parse_query('bar')(s).tolist() == [True, False]
    assert parse_query('bar', case_sensitive=True)(s).tolist() == [False, False]
    assert parse_query('/^foo//')(s).tolist() == [True, False]
    assert parse_query('')(s).tolist() == [True, True]


def test_stale_depth_column_is_ignored():
    """`filter_scan` derives depth from `path` — a frame whose `depth` column
    lies (as chunk-expanded frames did before the `_unbase_paths` fix) must
    still treat a matched top-level dir as ONE outermost match."""
    df = _tree()
    df['depth'] = 7  # uniformly wrong
    result = filter_scan(df, 'demo', display_depth=4)
    assert result.n_matches == 2
    assert result.total_size == 1230
