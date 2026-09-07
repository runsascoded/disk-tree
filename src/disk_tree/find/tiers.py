"""Layer-2 as index tiers (spec mgu-scale-unification.md, item C).

Consumers that read layer-2 over HTTP range requests (mgu's Pages Functions;
DT's own Functions do the same over the scan blob) want *sorted parquet with
small row groups*, in tiers a planner can pick from by the floor it needs:

======== ============================= ==================== ======================================
tier     rows                          sort                 floor
======== ============================= ==================== ======================================
dirs     every dir row (× label slice) (depth, path, …)     none
objects  every file row                (path, …)            none — pre-order: one range per subtree
coarse   dir rows with size ≥ F        (depth, path, …)     F = 2^(round(log2 total) − E), E=24
======== ============================= ==================== ======================================

The floor is a tier boundary, never a loss: every kept row's sums are exact
(it is the layer-2 row, filtered, not re-aggregated) — with label slices the
floor applies per *row*, so a path can keep one slice and drop another. Every
row keeps ``kind``; label columns (item B) sit right after ``path`` and
close each sort. Sort variants are extra sorted copies of the ``dirs`` and
``coarse`` tiers led by other columns — parquet has no secondary index; a
sorted copy is one.

Tiers are cut from the finished layer-2 parquet (one filtered, sorted COPY
each, spillable), so they are engine-agnostic: anything that leaves a layer-2
blob on disk can call :func:`write_tiers`.
"""

from __future__ import annotations

import os
from math import log2
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

TIERS = ('dirs', 'objects', 'coarse')
DEFAULT_COARSE_EXP = 24
DEFAULT_ROW_GROUP_ROWS = 8192
# DuckDB's vector size: the granularity at which its parquet writer cuts row groups.
ROW_GROUP_STEP = 2048

# Layer-2 columns before/after the label block; anything between `path` and
# `size` is a label column.
_HEAD = 'path'
_AFTER_LABELS = 'size'


def coarse_floor(total_size: int, exp: int = DEFAULT_COARSE_EXP) -> int:
    """`2^(round(log2 total_size) − exp)`, clamped at 1; 0 when there are no bytes."""
    if total_size <= 0:
        return 0
    return 1 << max(round(log2(total_size)) - exp, 0)


def parse_tiers(spec: str) -> tuple[str, ...]:
    tiers = tuple(t for t in spec.split(',') if t)
    bad = [t for t in tiers if t not in TIERS]
    if bad:
        raise ValueError(f"unknown tier(s) {bad}; choose from {list(TIERS)}")
    if len(set(tiers)) != len(tiers):
        raise ValueError(f"tier repeated in {spec!r}")
    return tiers


def tier_path(stem: str, tier: str, variant: tuple[str, ...] = ()) -> str:
    suffix = f"-by-{'-'.join(variant)}" if variant else ''
    return f'{stem}.{tier}{suffix}.parquet'


def write_tiers(
    layer2: str,
    stem: str,
    tiers: tuple[str, ...] = TIERS,
    coarse_exp: int = DEFAULT_COARSE_EXP,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    sort_variants: tuple[tuple[str, ...], ...] = (),
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> dict[str, int]:
    """Cut `tiers` (+ `sort_variants` of the dirs/coarse tiers) from the layer-2
    parquet at `layer2` into `<stem>.<tier>[-by-<cols>].parquet`.

    Returns `{output path: row count}` in write order. Each file carries
    `tier` / `sort` (and, for coarse, `floor_bytes` / `coarse_exp` /
    `total_size`) in its parquet key-value metadata so a planner can read the
    floor without a sidecar.
    """
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()
    # DuckDB's parquet writer flushes a row group once the buffered rows reach
    # the target, in 2048-row vector steps — so "≤ N rows per group" holds
    # exactly for multiples of 2048 and silently overshoots otherwise
    # (measured: ROW_GROUP_SIZE 3000 wrote a 5000-row group).
    if row_group_rows < ROW_GROUP_STEP or row_group_rows % ROW_GROUP_STEP:
        raise ValueError(f"row_group_rows must be a positive multiple of {ROW_GROUP_STEP}; got {row_group_rows}")
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{layer2}')").fetchall()]
    if cols[0] != _HEAD or _AFTER_LABELS not in cols:
        raise ValueError(f"{layer2}: not a layer-2 parquet (columns {cols})")
    labels = tuple(cols[1:cols.index(_AFTER_LABELS)])
    for variant in sort_variants:
        missing = [c for c in variant if c not in cols]
        if missing:
            raise ValueError(f"sort variant {variant}: column(s) {missing} not in {layer2} ({cols})")
        if not variant:
            raise ValueError("empty sort variant")

    total_size = con.execute(
        f"SELECT COALESCE(SUM(size), 0)::BIGINT FROM read_parquet('{layer2}') WHERE path = '.'"
    ).fetchone()[0]
    floor = coarse_floor(int(total_size), coarse_exp)

    def order(lead: tuple[str, ...], base: tuple[str, ...]) -> tuple[str, ...]:
        out: list[str] = []
        for c in (*lead, *base, *labels):
            if c not in out:
                out.append(c)
        return tuple(out)

    written: dict[str, int] = {}

    def copy(tier: str, where: str, sort: tuple[str, ...], variant: tuple[str, ...], extra_kv: dict) -> None:
        out = tier_path(stem, tier, variant)
        kv = {'tier': tier, 'sort': ','.join(sort), **extra_kv}
        kv_sql = ', '.join(f"'{k}': '{v}'" for k, v in kv.items())
        order_by = ', '.join(f'{c} NULLS FIRST' for c in sort)
        tmp = out + '.tmp'
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{layer2}')
                WHERE {where}
                ORDER BY {order_by}
            ) TO '{tmp}' (FORMAT PARQUET, ROW_GROUP_SIZE {row_group_rows}, KV_METADATA {{{kv_sql}}})
        """)
        os.replace(tmp, out)
        written[out] = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0])

    coarse_kv = {'floor_bytes': floor, 'coarse_exp': coarse_exp, 'total_size': int(total_size)}
    for tier in tiers:
        if tier == 'dirs':
            copy(tier, "kind = 'dir'", order((), ('depth', 'path')), (), {})
        elif tier == 'objects':
            copy(tier, "kind = 'file'", order((), ('path',)), (), {})
        elif tier == 'coarse':
            copy(tier, f"kind = 'dir' AND size >= {floor}", order((), ('depth', 'path')), (), coarse_kv)
        else:
            raise ValueError(f"unknown tier {tier!r}")
        if tier in ('dirs', 'coarse'):
            where = "kind = 'dir'" if tier == 'dirs' else f"kind = 'dir' AND size >= {floor}"
            for variant in sort_variants:
                copy(tier, where, order(variant, ('depth', 'path')), variant, coarse_kv if tier == 'coarse' else {})
    return written
