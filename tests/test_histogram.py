"""Byte-weighted mtime histograms (spec: viz-widgets.md §4/V.4b)."""

from __future__ import annotations

import pandas as pd
import pytest

from disk_tree.histogram import age_histograms


def frame(rows: list[tuple[str, str, int, int | None]]) -> pd.DataFrame:
    """(path, kind, size, mtime) → layer-2-shaped frame."""
    return pd.DataFrame(rows, columns=['path', 'kind', 'size', 'mtime'])


# Two dirs and a loose file; mtimes chosen to land in known bins of [0, 100].
BASIC = frame([
    ('.', 'dir', 300, 100),
    ('a', 'dir', 210, 100),
    ('a/old.bin', 'file', 200, 0),
    ('a/new.bin', 'file', 10, 100),
    ('b', 'dir', 60, 50),
    ('b/mid.bin', 'file', 60, 50),
    ('loose.txt', 'file', 30, 75),
])


def test_bins_bytes_by_child_and_mtime():
    h = age_histograms(BASIC, bins=4)
    assert h.edges == [0, 25, 50, 75, 100]
    assert [(c.path, c.kind, c.bytes) for c in h.children] == [
        ('a', 'dir', [200, 0, 0, 10]),
        ('b', 'dir', [0, 0, 60, 0]),
        ('loose.txt', 'file', [0, 0, 0, 30]),
    ]


def test_children_sorted_by_total_bytes_descending():
    h = age_histograms(BASIC, bins=4)
    assert [(c.path, c.total_bytes, c.n_files) for c in h.children] == [
        ('a', 210, 2),
        ('b', 60, 1),
        ('loose.txt', 30, 1),
    ]


def test_each_childs_area_is_its_byte_total():
    """The invariant the whole widget rests on: Σ bins == the child's bytes."""
    h = age_histograms(BASIC, bins=7)
    assert [sum(c.bytes) for c in h.children] == [c.total_bytes for c in h.children]
    assert sum(sum(c.bytes) for c in h.children) == 300


def test_dir_rows_are_not_double_counted():
    """Dir rows carry rolled-up sizes; counting them too would double every byte."""
    only_dirs = frame([('.', 'dir', 300, 100), ('a', 'dir', 300, 100)])
    h = age_histograms(only_dirs, bins=4)
    assert h.children == []
    assert h.edges == [0, 0]


def test_drill_into_subdir():
    nested = frame([
        ('.', 'dir', 30, 40),
        ('top', 'dir', 30, 40),
        ('top/x', 'dir', 20, 40),
        ('top/x/f.bin', 'file', 20, 40),
        ('top/y.bin', 'file', 10, 20),
        ('other/z.bin', 'file', 999, 30),
    ])
    h = age_histograms(nested, rel_path='top', bins=2)
    assert h.edges == [20, 30, 40]
    assert [(c.path, c.kind, c.bytes) for c in h.children] == [
        ('x', 'dir', [0, 20]),
        ('y.bin', 'file', [10, 0]),
    ]


def test_single_distinct_mtime_gets_a_one_second_interval():
    flat = frame([('.', 'dir', 10, 500), ('f.bin', 'file', 10, 500)])
    h = age_histograms(flat, bins=2)
    assert h.edges == [500, 500, 501]
    assert [c.bytes for c in h.children] == [[10, 0]]


def test_caller_supplied_edges_are_used_verbatim():
    h = age_histograms(BASIC, edges=[0, 50, 100])
    assert h.edges == [0, 50, 100]
    assert [(c.path, c.bytes) for c in h.children] == [
        ('a', [200, 10]),
        ('b', [0, 60]),
        ('loose.txt', [0, 30]),
    ]


def test_data_outside_supplied_edges_lands_in_the_end_bins():
    """Clamping, not dropping — silently-lost bytes would understate totals."""
    h = age_histograms(BASIC, edges=[40, 60])
    assert [(c.path, c.bytes, c.total_bytes) for c in h.children] == [
        ('a', [210], 210),
        ('b', [60], 60),
        ('loose.txt', [30], 30),
    ]


def test_limit_reports_what_it_dropped():
    h = age_histograms(BASIC, bins=2, limit=1)
    assert [c.path for c in h.children] == ['a']
    assert (h.omitted, h.omitted_bytes) == (2, 90)


def test_no_limit_keeps_every_child():
    h = age_histograms(BASIC, bins=2, limit=None)
    assert [c.path for c in h.children] == ['a', 'b', 'loose.txt']
    assert (h.omitted, h.omitted_bytes) == (0, 0)


def test_rows_with_null_mtime_are_skipped():
    with_null = frame([
        ('.', 'dir', 30, 10),
        ('ok.bin', 'file', 20, 10),
        ('unknown.bin', 'file', 10, None),
    ])
    h = age_histograms(with_null, bins=2)
    assert [(c.path, c.total_bytes) for c in h.children] == [('ok.bin', 20)]


def test_empty_frame_returns_no_children():
    h = age_histograms(frame([]), bins=4)
    assert h.to_dict() == {'edges': [0, 0], 'children': [], 'omitted': 0, 'omitted_bytes': 0}


def test_bins_must_be_positive():
    with pytest.raises(ValueError, match='bins must be >= 1; got 0'):
        age_histograms(BASIC, bins=0)


def test_to_dict_shape_is_the_api_contract():
    h = age_histograms(frame([
        ('.', 'dir', 5, 7),
        ('f.bin', 'file', 5, 7),
    ]), bins=1)
    assert h.to_dict() == {
        'edges': [7, 8],
        'children': [{'path': 'f.bin', 'kind': 'file', 'bytes': [5], 'total_bytes': 5, 'n_files': 1}],
        'omitted': 0,
        'omitted_bytes': 0,
    }
