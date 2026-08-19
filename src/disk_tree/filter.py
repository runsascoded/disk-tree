"""Recursive filter / search over a scan (spec: diff-and-search.md §4, v1).

Selects the layer-2 rows whose (uri-relative) path matches a query, and rolls
their *aggregate* stats up to ancestors at the display depth — true
re-aggregation, unlike the UI's v0 display-only dimming. Two properties keep
it cheap (spec "Two properties"):

1. A matched directory needs no descendant scan — its `size`/`n_desc` are
   already aggregates. Only *outermost* matches count (a match inside a
   matched dir is already inside its ancestor's aggregate), which also
   guarantees no double-counting.
2. Rows are depth-major, so processing depth-by-depth visits every potential
   outer match before anything it covers — dedup is a per-depth ancestor
   check, and per-depth snapshots give iterative-deepening progress for free.

Query semantics mirror `@disk-tree/react`'s `parseQuery` (`filter.ts`):
`/…/flags` is a regex, anything else a substring, case-insensitive by
default; an invalid regex degrades to a substring match instead of raising.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd

DEFAULT_DISPLAY_DEPTH = 4

_REGEX_QUERY = re.compile(r'^/(.*)/([gimsuy]*)$')


def parse_query(query: str, case_sensitive: bool = False) -> Callable[[pd.Series], pd.Series]:
    """Compile a query into a vectorized path-Series predicate.

    Returns a function mapping a Series of paths to a boolean Series.
    An empty query matches everything.
    """
    q = query.strip()
    if not q:
        return lambda s: pd.Series(True, index=s.index)
    m = _REGEX_QUERY.match(q)
    if m:
        pattern, js_flags = m.groups()
        # Mirrors filter.ts: the default adds `i` unless case_sensitive; the
        # JS-only flags (g/u/y) have no `re` analogue and are ignored.
        flags = 0
        if 'i' in js_flags or not case_sensitive:
            flags |= re.IGNORECASE
        if 's' in js_flags:
            flags |= re.DOTALL
        if 'm' in js_flags:
            flags |= re.MULTILINE
        try:
            compiled = re.compile(pattern, flags)

            def pred(s: pd.Series) -> pd.Series:
                with warnings.catch_warnings():
                    # contains() warns when the pattern has capture groups
                    # ("use str.extract") — we want containment, not groups.
                    warnings.simplefilter('ignore', UserWarning)
                    return s.str.contains(compiled, na=False)
            return pred
        except re.error:
            pass  # degrade to substring of the raw query, slashes included
    if case_sensitive:
        return lambda s: s.str.contains(q, regex=False, na=False)
    return lambda s: s.str.lower().str.contains(q.lower(), regex=False, na=False)


def rebase_frame(df: pd.DataFrame, rel_path: str) -> pd.DataFrame:
    """Rebase a scan-relative frame so `rel_path` becomes the root — keeps only
    the subtree's rows (dropping the `rel_path` row itself, filter_scan's root)
    and strips the prefix from `path`/`depth`."""
    if not rel_path or rel_path == '.':
        return df
    pfx = rel_path + '/'
    sub = df[df['path'].str.startswith(pfx)].copy()
    sub['path'] = sub['path'].str[len(pfx):]
    if 'depth' in sub.columns:
        sub['depth'] = sub['depth'] - (rel_path.count('/') + 1)
    return sub


@dataclass
class FilterNode:
    """One node of the filtered slice — either an outermost match (at any
    depth ≤ display) or an ancestor dir carrying rolled-up matched bytes."""
    path: str
    depth: int
    kind: str            # ancestors are always 'dir'
    size: int            # matched bytes at-or-under this node
    n_matches: int       # outermost matches at-or-under this node
    matched: bool        # this node itself is an outermost match


@dataclass
class FilterResult:
    nodes: list[FilterNode]   # sorted (depth, path); excludes the root
    total_size: int           # Σ outermost matches' sizes — no double-counting
    n_matches: int            # count of outermost matches (any depth)
    max_depth_scanned: int


def filter_scan(
    df: pd.DataFrame,
    query: str,
    display_depth: int = DEFAULT_DISPLAY_DEPTH,
    case_sensitive: bool = False,
    on_depth: Callable[[int, 'FilterResult'], None] | None = None,
) -> FilterResult:
    """Filter a scan frame (paths relative to the viewed root, `.` = root).

    `on_depth(depth, snapshot)` fires after each depth completes — snapshots
    are cumulative. (Convenience wrapper over `iter_filter_scan`, the SSE seam.)
    """
    result = FilterResult(nodes=[], total_size=0, n_matches=0, max_depth_scanned=0)
    for d, result in iter_filter_scan(df, query, display_depth=display_depth, case_sensitive=case_sensitive):
        if on_depth is not None:
            on_depth(d, result)
    return result


def iter_filter_scan(
    df: pd.DataFrame,
    query: str,
    display_depth: int = DEFAULT_DISPLAY_DEPTH,
    case_sensitive: bool = False,
):
    """Yield `(depth, cumulative FilterResult)` after each depth completes —
    depth-major layout makes shallow-first iterative deepening plain iteration
    order, so partial results stream for free."""
    match = parse_query(query, case_sensitive=case_sensitive)

    # ALWAYS derive depth from `path` — stored depth columns can be stale
    # (chunk-expanded frames carried chunk-relative depths until the
    # `_unbase_paths` fix) and correctness must not hinge on their fidelity.
    depths = (df['path'].str.count('/') + 1).mask(df['path'] == '.', 0)

    covered: dict[int, set[str]] = {}          # depth -> outermost matched dir paths
    agg: dict[str, FilterNode] = {}            # ancestor path -> rollup node
    matched_nodes: list[FilterNode] = []
    total_size = 0
    n_matches = 0
    levels = {int(d): g for d, g in df.groupby(depths)}  # one pass, not one scan per depth
    max_d = max(levels) if levels else 0

    for d in range(1, max_d + 1):
        level = levels.get(d)
        if level is None or level.empty:
            yield d, _snapshot(agg, matched_nodes, total_size, n_matches, d)
            continue
        paths = level['path']
        # Drop rows inside an already-matched dir: their bytes are already in
        # that ancestor's aggregate (outermost-only ⇒ no double-counting).
        # Sorted paths make each covered prefix a *contiguous range*
        # ({p : p.startswith(pfx+'/')} == [pfx+'/', pfx+'0')), so exclusion is
        # two binary searches per prefix — not a vector pass per prefix, which
        # cost ~35s at 150 covered dirs over a 4M-row scan.
        if covered:
            if not paths.is_monotonic_increasing:
                level = level.sort_values('path')
                paths = level['path']
            arr = paths.to_numpy()
            keep = np.ones(len(arr), dtype=bool)
            for dc, cset in covered.items():
                if dc >= d:
                    continue
                for p in cset:
                    lo = np.searchsorted(arr, p + '/')
                    hi = np.searchsorted(arr, p + chr(ord('/') + 1))
                    keep[lo:hi] = False
            level = level[keep]
            paths = level['path']
            if level.empty:
                yield d, _snapshot(agg, matched_nodes, total_size, n_matches, d)
                continue

        hits = level[match(paths)]
        for row in hits.itertuples():
            size = 0 if pd.isna(row.size) else int(row.size)
            kind = row.kind
            total_size += size
            n_matches += 1
            if kind == 'dir':
                covered.setdefault(d, set()).add(row.path)
            node = FilterNode(path=row.path, depth=d, kind=kind, size=size, n_matches=1, matched=True)
            if d <= display_depth:
                matched_nodes.append(node)
            # Credit ancestors at depths 1..min(display, d-1).
            comps = row.path.split('/')
            for k in range(1, min(display_depth, d - 1) + 1):
                anc = '/'.join(comps[:k])
                a = agg.get(anc)
                if a is None:
                    a = agg[anc] = FilterNode(path=anc, depth=k, kind='dir', size=0, n_matches=0, matched=False)
                a.size += size
                a.n_matches += 1

        yield d, _snapshot(agg, matched_nodes, total_size, n_matches, d)


def _snapshot(
    agg: dict[str, FilterNode],
    matched_nodes: list[FilterNode],
    total_size: int,
    n_matches: int,
    depth: int,
) -> FilterResult:
    # A matched node at display depth is its own entry; if it ALSO accumulated
    # as an ancestor (it can't — ancestors only accrue from non-covered rows,
    # and everything under a matched dir is covered), agg and matched are
    # disjoint by construction.
    nodes = sorted(
        [FilterNode(**vars(n)) for n in agg.values()] + [FilterNode(**vars(n)) for n in matched_nodes],
        key=lambda n: (n.depth, n.path),
    )
    return FilterResult(
        nodes=nodes,
        total_size=total_size,
        n_matches=n_matches,
        max_depth_scanned=depth,
    )
