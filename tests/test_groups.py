"""Group manifests beside index tiers (`find/groups.py`): the precomputed footer
a serverless range reader plans from, in mgu's `index_footer.py` wire format.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pytest

from disk_tree.find.aggregate_duckdb import aggregate_listing_to_parquet
from disk_tree.find.groups import (
    GROUP_FIELDS, GROUPS_VERSION, extract, group_rows, groups_json, groups_path, schema_json, write_groups,
)
from disk_tree.find.tiers import write_tiers
from disk_tree.listing import prepare_listing
from test_tiers import _layer2

TS = dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)


def _wide_layer2(tmp_path: Path, n_files: int, labels: str | None = None) -> str:
    """`n_files` one-byte files `d<k>/f<i>` spread over 4 dirs — enough rows for
    the objects tier to span several 2048-row groups."""
    names = [f'd{i % 4}/f{i:05d}' for i in range(n_files)]
    listing = tmp_path / 'wide.parquet'
    pd.DataFrame({
        'bucket': ['b1'] * n_files,
        'name': names,
        'size_bytes': [1] * n_files,
        'created': [TS] * n_files,
        'storage_class_id': [1] * n_files,
    }).to_parquet(listing)
    out = str(tmp_path / 'wide-layer2.parquet')
    con = duckdb.connect()
    aggregate_listing_to_parquet(
        prepare_listing(con, (str(listing),)), bucket='b1', scheme='gcs', out_parquet=out, con=con, label=labels,
    )
    return out


def _chunk_triples(path: str) -> list[list[list[int]]]:
    """Per row group, `[data_page_offset, total_compressed_size, dictionary_page_offset|0]`
    per column — the reference for `rg_json`'s third element."""
    md = pq.read_metadata(path)
    return [
        [
            [cc.data_page_offset, cc.total_compressed_size, cc.dictionary_page_offset or 0]
            for cc in (md.row_group(g).column(c) for c in range(md.num_columns))
        ]
        for g in range(md.num_row_groups)
    ]


def test_groups_path():
    assert groups_path('/x/gcs-b1.dirs.parquet') == '/x/gcs-b1.dirs.groups.json'
    assert groups_path('r2://bk/p/gcs-b1.coarse-by-usr.parquet') == 'r2://bk/p/gcs-b1.coarse-by-usr.groups.json'
    with pytest.raises(ValueError, match='not a parquet path'):
        groups_path('/x/gcs-b1.dirs.groups.json')


def test_schema_json_is_hyparquet_shaped(tmp_path: Path):
    layer2 = _layer2(tmp_path)
    stem = str(tmp_path / 'gcs-b1')
    write_tiers(layer2, stem, tiers=('coarse',), coarse_exp=3)
    md = pq.read_metadata(f'{stem}.coarse.parquet')
    schema = schema_json(md)
    names = md.schema.names
    # One leaf per column, in file order, physical types as parquet-thrift names.
    assert [el['name'] for el in schema['schema'][1:]] == names
    assert schema['schema'][0] == {'repetition_type': 'REQUIRED', 'name': 'schema', 'num_children': len(names)}
    by_name = {el['name']: el for el in schema['schema'][1:]}
    assert by_name['path'] == {'type': 'BYTE_ARRAY', 'repetition_type': 'OPTIONAL', 'name': 'path', 'converted_type': 'UTF8'}
    assert by_name['depth'] == {'type': 'INT64', 'repetition_type': 'OPTIONAL', 'name': 'depth', 'converted_type': 'INT_64'}
    assert by_name['size'] == {'type': 'INT64', 'repetition_type': 'OPTIONAL', 'name': 'size', 'converted_type': 'INT_64'}
    # The coarse floor rides along from the parquet key-value metadata.
    assert schema['version'] == 1
    assert schema['floor_bytes'] == int(md.metadata[b'floor_bytes'])
    # A tier without a floor has none.
    write_tiers(layer2, stem, tiers=('dirs',))
    assert 'floor_bytes' not in schema_json(pq.read_metadata(f'{stem}.dirs.parquet'))


def test_group_rows_span_the_sorted_objects_tier(tmp_path: Path):
    n = 5000
    layer2 = _wide_layer2(tmp_path, n)
    stem = str(tmp_path / 'gcs-b1')
    write_tiers(layer2, stem, tiers=('objects',), row_group_rows=2048)
    tier = f'{stem}.objects.parquet'
    rows = group_rows(pq.read_metadata(tier))
    # The objects tier is sorted by path: group boundaries are the sorted
    # paths at 0 / 2047 / 2048 / … — exact, and every file is depth 2 (root
    # `.` = 0, `d<k>` = 1), size 1.
    paths = sorted(f'd{i % 4}/f{i:05d}' for i in range(n))
    spans = [(0, 2048), (2048, 4096), (4096, 5000)]
    expected = [
        {
            'rg': g, 'd_min': 2, 'd_max': 2,
            'p_min': paths[a], 'p_max': paths[b - 1],
            'b_max': 1, 'u_min': None, 'u_max': None,
            'row_start': a, 'row_end': b,
        }
        for g, (a, b) in enumerate(spans)
    ]
    assert [{k: r[k] for k in GROUP_FIELDS if k != 'rg_json'} for r in rows] == expected
    # `rg_json` = [num_rows, codec, per-column [data_page_offset, total_compressed_size, dictionary_page_offset|0]].
    triples = _chunk_triples(tier)
    assert [json.loads(r['rg_json']) for r in rows] == [
        [b - a, 'SNAPPY', triples[g]] for g, (a, b) in enumerate(spans)
    ]
    assert all(len(t) == pq.read_metadata(tier).num_columns for t in triples)


def test_user_slice_bounds_and_size_column_fallback(tmp_path: Path):
    # A `usr`-labeled layer-2: the dirs-by-usr tier sorts on usr first, so each
    # group's u_min/u_max are that slice's bounds; the size column is `size`.
    labels = tmp_path / 'labels.parquet'
    pd.DataFrame({'prefix': ['d0', 'd1', 'd2'], 'usr': ['alice', 'bob', 'carol']}).to_parquet(labels)
    layer2 = _layer2(tmp_path, labels=str(labels))
    stem = str(tmp_path / 'gcs-b1')
    write_tiers(layer2, stem, tiers=('dirs',), sort_variants=(('usr',),))
    rows = group_rows(pq.read_metadata(f'{stem}.dirs-by-usr.parquet'))
    # One group (7 dir rows × slices ≤ 8192): NULL-labeled rows sort first, so
    # the string stats span the labeled slices only.
    assert [(r['rg'], r['u_min'], r['u_max'], r['row_start']) for r in rows] == [(0, 'alice', 'carol', 0)]
    assert rows[0]['b_max'] == int(pd.read_parquet(f'{stem}.dirs-by-usr.parquet')['size'].max())

    # mgu's path index names its size column `b`: the same extraction applies.
    df = pd.DataFrame({'path': ['.', 'a'], 'depth': [0, 1], 'b': [7, 3]})
    df.to_parquet(tmp_path / 'mgu.parquet')
    assert [(r['b_max'], r['u_min']) for r in group_rows(pq.read_metadata(tmp_path / 'mgu.parquet'))] == [(7, None)]
    # Without any size column the manifest is undefined, loudly.
    pd.DataFrame({'path': ['.'], 'depth': [0]}).to_parquet(tmp_path / 'nosize.parquet')
    with pytest.raises(ValueError, match=r'no size column \(size/b\)'):
        group_rows(pq.read_metadata(tmp_path / 'nosize.parquet'))


def test_write_tiers_groups_writes_the_manifest_beside_each_tier(tmp_path: Path):
    layer2 = _layer2(tmp_path)
    stem = str(tmp_path / 'gcs-b1')
    written = write_tiers(layer2, stem, tiers=('dirs', 'coarse'), coarse_exp=3, groups=True)
    assert sorted(p.name for p in tmp_path.glob('gcs-b1.*')) == [
        'gcs-b1.coarse.groups.json', 'gcs-b1.coarse.parquet',
        'gcs-b1.dirs.groups.json', 'gcs-b1.dirs.parquet',
    ]
    for tier_path, n_rows in written.items():
        doc = json.loads(Path(groups_path(tier_path)).read_text())
        schema, rows = extract(tier_path)
        # The document is exactly `groups_json(extract(tier))`: envelope + arrays in GROUP_FIELDS order.
        assert doc == json.loads(groups_json(schema, rows))
        assert list(doc) == ['v', 'version', 'schema', 'floor_bytes', 'groups']
        assert doc['v'] == GROUPS_VERSION
        assert doc['groups'] == [[r[k] for k in GROUP_FIELDS] for r in rows]
        assert doc['groups'][-1][9] == n_rows  # row_end of the last group = the tier's rows
    assert json.loads(Path(f'{stem}.dirs.groups.json').read_text())['floor_bytes'] is None
    assert json.loads(Path(f'{stem}.coarse.groups.json').read_text())['floor_bytes'] == int(
        pq.read_metadata(f'{stem}.coarse.parquet').metadata[b'floor_bytes']
    )


def test_write_groups_over_a_url(tmp_path: Path):
    # The blob seam handles URLs; `file://` exercises the fsspec branch end to end.
    layer2 = _layer2(tmp_path)
    stem = str(tmp_path / 'gcs-b1')
    write_tiers(layer2, stem, tiers=('dirs',))
    st = write_groups(f'file://{stem}.dirs.parquet')
    assert (st.path, st.n_groups) == (f'file://{stem}.dirs.groups.json', 1)
    assert Path(f'{stem}.dirs.groups.json').read_text() == groups_json(*extract(f'{stem}.dirs.parquet'))
    assert st.n_bytes == len(Path(f'{stem}.dirs.groups.json').read_bytes())
