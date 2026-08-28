"""Best-first pruned recursive diff between two scans (spec: diff-and-search.md §3a).

Deltas propagate up the tree, so a change 9 levels deep shows at depth 1 — as an
undifferentiated Δ on one child. This module does the drill-down server-side, in
one pass, and only down *changed spines*:

- `added`/`removed` dirs are never descended — their aggregate row already tells
  the whole story (layer-2 dirs carry `size`/`n_desc`).
- Common dirs whose `(size, n_desc, n_children, mtime)` all match are pruned.
- Same size & count but a different mtime → `touched` (emitted; dirs are
  still descended, since a net-zero rename hides below).
  Heuristic: compensating changes can hide (the eventual `digest` column makes
  this exact — spec §3d). `mtime` is in the descend-trigger so a same-size
  rename (net-zero Δ) is still found and surfaces as `added` + `removed` rows.
- Expansion order is best-first on `|Δsize|` (not BFS), so a budget cuts the
  walk where the *least* signal is; unexpanded frontier dirs are marked
  `pruned` so callers know change may hide below them.

Also home to `resolve_blob` / `resolve_chunk_for_path` (moved from `server.py`
so the CLI can share them without importing Flask).
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from functools import lru_cache
from os.path import exists, getmtime, isabs, join
from typing import Callable, Protocol

import pandas as pd
import pyarrow.parquet as pq

from . import config as _config


def resolve_blob(blob_ref: str) -> str:
    """Resolve a parquet blob ref to its absolute path.

    Honors legacy absolute refs and ignores DuckDB/SQLite opaque refs.
    Reads SCANS_DIR via the config module so tests can monkeypatch it.
    """
    if not blob_ref or blob_ref.startswith(('ddb:', 'sqlite:')):
        return blob_ref
    return blob_ref if isabs(blob_ref) else join(_config.SCANS_DIR, blob_ref)


def _chunk_map(parquet_path: str) -> dict[str, str] | None:
    """path → child_scan_id for the chunk-pointer rows of a hybrid parquet
    (None when the blob has no `child_scan_id` column). Keyed on mtime because
    delete updates rewrite blobs in place."""
    return _chunk_map_cached(parquet_path, getmtime(parquet_path))


@lru_cache(maxsize=64)
def _chunk_map_cached(parquet_path: str, mtime: float) -> dict[str, str] | None:
    if 'child_scan_id' not in pq.read_schema(parquet_path).names:
        return None
    df = pd.read_parquet(parquet_path, columns=['path', 'child_scan_id'])
    df = df[df['child_scan_id'].notna()]
    return dict(zip(df['path'], df['child_scan_id']))


def resolve_chunk_for_path(blob_ref: str, rel_path: str) -> tuple[str, str]:
    """Resolve the actual blob_ref and rebased rel_path for a path that may be in a chunk.

    If rel_path maps to a chunked subtree, returns (chunk_blob_ref, rebased_path).
    Otherwise returns (blob_ref, rel_path) unchanged.

    Note: Only hybrid backend uses chunked parquets. DuckDB/SQLite blob refs
    (prefixed with 'ddb:' or 'sqlite:') don't have chunks.
    """
    if not rel_path or rel_path == '.':
        return blob_ref, rel_path

    # DuckDB and SQLite backends don't use chunking
    if blob_ref.startswith('ddb:') or blob_ref.startswith('sqlite:'):
        return blob_ref, rel_path

    chunks = _chunk_map(resolve_blob(blob_ref))
    if chunks is None:
        return blob_ref, rel_path

    # Check if any ancestor of rel_path has a child_scan_id
    parts = rel_path.split('/')
    for i in range(len(parts)):
        chunk_ref = chunks.get('/'.join(parts[:i+1]))
        if chunk_ref is not None and exists(resolve_blob(chunk_ref)):
            # Rebase the remaining path relative to chunk root
            remaining = '/'.join(parts[i+1:]) if i + 1 < len(parts) else '.'
            # Recursively resolve in case of nested chunks
            return resolve_chunk_for_path(chunk_ref, remaining)

    return blob_ref, rel_path


class LoadFn(Protocol):
    def __call__(
        self,
        blob_ref: str,
        max_depth: int | None = None,
        min_depth: int | None = None,
        follow_refs: bool = False,
        path_prefix: str | None = None,
    ) -> pd.DataFrame: ...


class ScanSource:
    """Per-scan children loader for the diff walk.

    Takes paths *relative to the compared uri* (`''` = the uri itself), rebases
    into the scan's own coordinates (the scan may be of an ancestor path),
    resolves hybrid chunk boundaries, and loads exactly one directory's
    children per call — cheap via depth + path-prefix pushdown.
    """

    def __init__(
        self,
        blob: str,
        scan_path: str,
        uri: str,
        load: LoadFn,
        resolve: Callable[[str, str], tuple[str, str]] | None = None,
    ):
        """`resolve` maps (blob, rel_path) across hybrid chunk boundaries —
        pass `resolve_chunk_for_path` for hybrid-backed scans (it resolves refs
        via the configured SCANS_DIR, so it's not the default: backends with
        their own scans_dir, and chunk-free backends, want identity)."""
        self.blob = blob
        self.load = load
        self.resolve = resolve or (lambda b, p: (b, p))
        if scan_path == uri:
            self.rel_prefix = ''
        else:
            self.rel_prefix = uri[len(scan_path):].lstrip('/')

    def children(self, rel: str) -> pd.DataFrame:
        """Direct children of `rel`, indexed by child name.

        Rows at exactly one depth under one prefix *are* the direct children —
        no parent-column mask needed.
        """
        if self.rel_prefix and rel:
            full = f"{self.rel_prefix}/{rel}"
        else:
            full = self.rel_prefix or rel
        blob, rebased = self.resolve(self.blob, full or '.')
        at_root = rebased in ('', '.')
        d = 0 if at_root else rebased.count('/') + 1
        df = self.load(
            blob,
            min_depth=d + 1,
            max_depth=d + 1,
            path_prefix=None if at_root else rebased,
        )
        if not df.empty:
            # Depth pushdown is best-effort (blobs without a `depth` column
            # load unfiltered) — enforce "exactly one level below `rebased`"
            # here so correctness never depends on the backend's fidelity.
            depths = df['path'].map(lambda p: 0 if p == '.' else p.count('/') + 1)
            mask = depths == d + 1
            if not at_root:
                mask &= df['path'].str.startswith(rebased + '/')
            df = df[mask]
        if df.empty:
            return df.set_index(df['path'] if 'path' in df.columns else pd.Index([]))
        names = df['path'] if at_root else df['path'].str.rsplit('/', n=1).str[-1]
        df = df.set_index(names)
        return df[~df.index.duplicated()]


def _neq(a, b) -> bool:
    """NaN-safe inequality (both-NaN counts as equal, unlike `!=`)."""
    a_na, b_na = pd.isna(a), pd.isna(b)
    if a_na and b_na:
        return False
    if a_na or b_na:
        return True
    return bool(a != b)


@dataclass
class DeltaRow:
    path: str      # relative to the compared uri
    depth: int     # levels below the compared uri (1 = direct child)
    kind: str
    status: str    # added | removed | changed | touched | unchanged
                   # touched: same size & n_desc, mtime differs (a rename /
                   # net-zero churn / plain `touch` — bytes didn't move)
    size_a: int
    size_b: int
    n_desc_a: int
    n_desc_b: int
    expanded: bool = False   # a dir we descended into
    pruned: bool = False     # a dir with differing stats we did NOT descend into (budget/depth)

    @property
    def size_delta(self) -> int:
        return self.size_b - self.size_a

    @property
    def n_desc_delta(self) -> int:
        return self.n_desc_b - self.n_desc_a


@dataclass
class UnchangedRest:
    """Unchanged children of one expanded dir that were *not* emitted (beyond
    the top-K): enough to label the grey remainder without shipping rows."""
    count: int      # direct children
    size: int       # Σ size (both sides equal — they're unchanged)
    n_desc: int     # Σ n_desc — descendants below those children


@dataclass
class RecursiveDiffResult:
    rows: list[DeltaRow]   # sorted by |Δsize| desc
    expansions: int        # directories whose children were loaded (root included)
    truncated: bool        # budget or max_depth cut the walk short
    # The biggest unchanged siblings seen while expanding each dir (labeled
    # grey context for the treemap), and the aggregate of the rest, keyed by
    # parent rel path ('' = the compared uri). Empty when `include_unchanged`
    # already put every unchanged row in `rows`.
    unchanged_top: list[DeltaRow] = field(default_factory=list)
    unchanged_rest: dict[str, UnchangedRest] = field(default_factory=dict)


def _ival(row, col: str) -> int:
    if row is None:
        return 0
    v = row.get(col, 0)
    return 0 if pd.isna(v) else int(v)


def recursive_diff(
    src_a: ScanSource,
    src_b: ScanSource,
    budget: int = 100,
    max_depth: int | None = None,
    include_unchanged: bool = False,
    unchanged_top: int = 8,
) -> RecursiveDiffResult:
    """Walk changed spines best-first; return the delta frontier.

    `unchanged_top`: per expanded dir, keep this many of the biggest unchanged
    children (by size) in `unchanged_top`, summarizing the rest in
    `unchanged_rest[parent]` — free, since every child was compared anyway.
    """
    rows: list[DeltaRow] = []
    top_rows: list[DeltaRow] = []
    rest: dict[str, UnchangedRest] = {}
    emitted: set[str] = set()
    by_path: dict[str, DeltaRow] = {}
    # heap entries: (-|Δsize|, depth, seq, rel) — seq breaks ties FIFO
    heap: list[tuple[int, int, int, str]] = [(0, 0, 0, '')]
    seq = 0
    expansions = 0
    truncated = False

    while heap:
        if expansions >= budget:
            truncated = True
            break
        _, d, _, rel = heapq.heappop(heap)
        ca = src_a.children(rel)
        cb = src_b.children(rel)
        expansions += 1
        if rel:
            by_path[rel].expanded = True

        unchanged_here: list[DeltaRow] = []
        for name in sorted(set(ca.index) | set(cb.index)):
            child_rel = f"{rel}/{name}" if rel else name
            ra = ca.loc[name] if name in ca.index else None
            rb = cb.loc[name] if name in cb.index else None
            kind_a = ra['kind'] if ra is not None else None
            kind_b = rb['kind'] if rb is not None else None
            row = DeltaRow(
                path=child_rel,
                depth=d + 1,
                kind=kind_b or kind_a or '',
                status='',
                size_a=_ival(ra, 'size'),
                size_b=_ival(rb, 'size'),
                n_desc_a=_ival(ra, 'n_desc'),
                n_desc_b=_ival(rb, 'n_desc'),
            )
            descend = False
            if ra is None:
                row.status = 'added'
            elif rb is None:
                row.status = 'removed'
            elif kind_a != kind_b:
                # dir↔file swap: incomparable subtrees, report don't descend
                row.status = 'changed'
            else:
                if row.size_delta or row.n_desc_delta:
                    row.status = 'changed'
                elif _neq(ra.get('mtime'), rb.get('mtime')):
                    row.status = 'touched'
                else:
                    row.status = 'unchanged'
                if kind_a == 'dir':
                    # mtime is in the trigger so net-zero renames are still found
                    descend = any(
                        _neq(ra.get(c), rb.get(c))
                        for c in ('size', 'n_desc', 'n_children', 'mtime')
                    )

            by_path[child_rel] = row
            if descend:
                if max_depth is not None and d + 1 >= max_depth:
                    row.pruned = True
                    truncated = True
                else:
                    seq += 1
                    heapq.heappush(heap, (-abs(row.size_delta), d + 1, seq, child_rel))
            if row.status != 'unchanged' or include_unchanged or row.pruned:
                rows.append(row)
                emitted.add(child_rel)
            elif not row.pruned:
                unchanged_here.append(row)
        if unchanged_here:
            unchanged_here.sort(key=lambda r: (-r.size_b, r.path))
            top_rows.extend(unchanged_here[:unchanged_top])
            tail = unchanged_here[unchanged_top:]
            if tail:
                rest[rel] = UnchangedRest(
                    count=len(tail),
                    size=sum(r.size_b for r in tail),
                    n_desc=sum(r.n_desc_b for r in tail),
                )

    # Anything still queued was never expanded: mark it (and surface it even if
    # its own Δ is zero — differing stats below mean unexplored change).
    for _, _, _, rel in heap:
        row = by_path.get(rel)
        if row is not None:
            row.pruned = True
            truncated = True
            if rel not in emitted:
                rows.append(row)
                emitted.add(rel)

    rows.sort(key=lambda r: (-abs(r.size_delta), r.path))
    return RecursiveDiffResult(
        rows=rows, expansions=expansions, truncated=truncated,
        unchanged_top=top_rows, unchanged_rest=rest,
    )
