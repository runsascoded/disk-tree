"""Per-directory attribution lookup over a scan's ``path-index.parquet``.

The band-level ``owner_match`` on the /sweep console is a *plurality* signal —
a sweeper can be a band's top user at 41% while the other 59% of bytes belong
to other people (the 2026-09-02 ``marin-eu-west4/checkpoints/`` case). The
manifest therefore pushes the sweeper-vs-attribution check down to directory
level: an approved-band dir is only eligible when the attribution top user of
its nearest indexed ancestor IS one of its sweepers, with a majority
(``MIN_SHARE``) of that subtree's bytes.

The path index has one row per rolled-up path × attribution slice
``(path, depth, usr, b, …)``, sorted ``(depth, path)`` — so a band's
rows are cheap to pull via row-group statistics, and attribution is resolved
at run-dir depth (rules + W&B mining assign there), letting deep ``step-N``
dirs inherit via the ancestor walk.
"""

from __future__ import annotations

from typing import Optional

#: A dir's top user must hold at least this fraction of its subtree's bytes
#: (denominator includes unattributed bytes) for the dir to count as "theirs".
MIN_SHARE = 0.5

#: Attribution ids that never count as a person for the sweeper check.
NON_USERS = frozenset({"root"})


class AttrIndex:
    """Lazy per-band view of a path-index parquet: ``lookup`` answers
    "who does this directory's data belong to, and how decisively?"."""

    def __init__(self, path_index_url: str, extra_depth: int = 3):
        import fsspec
        import pyarrow.parquet as pq

        fs, path = fsspec.core.url_to_fs(path_index_url)
        self.pf = pq.ParquetFile(path, filesystem=fs)
        self.extra_depth = extra_depth
        md = self.pf.metadata
        names = self.pf.schema_arrow.names
        di, pi = names.index("depth"), names.index("path")
        self.rg_stats = []
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            ds, ps = rg.column(di).statistics, rg.column(pi).statistics
            self.rg_stats.append((i, ds.min, ds.max, ps.min, ps.max))
        # band prefix (gs://…/) → {path: (top_user, share, total_bytes)}
        self.bands: dict[str, dict[str, tuple[Optional[str], float, int]]] = {}

    def _load_band(self, band: str) -> dict[str, tuple[Optional[str], float, int]]:
        p = band.removeprefix("gs://").rstrip("/")
        d0 = p.count("/") + 1
        lo, hi = p + "/", p + "/\x7f"
        rgs = [
            i for i, dmin, dmax, pmin, pmax in self.rg_stats
            if dmin <= d0 + self.extra_depth and dmax >= d0 and pmin <= hi and pmax >= p
        ]
        out: dict[str, tuple[Optional[str], float]] = {}
        if rgs:
            t = self.pf.read_row_groups(rgs, columns=["path", "depth", "usr", "b"]).to_pandas()
            t = t[(t.depth >= d0) & (t.depth <= d0 + self.extra_depth)
                  & ((t.path == p) | t.path.str.startswith(lo))]
            for path, g in t.groupby("path"):
                total = int(g.b.sum())
                if total <= 0:
                    continue
                attr = g.dropna(subset=["usr"])
                attr = attr[~attr.usr.isin(NON_USERS)]
                if not len(attr):
                    out[path] = (None, 0.0, total)
                    continue
                by_u = attr.groupby("usr").b.sum()
                top_u = by_u.idxmax()
                out[path] = (str(top_u), int(by_u.loc[top_u]) / total, total)
        self.bands[band] = out
        return out

    def lookup(self, band: str, bucket: str, dirname: str) -> Optional[tuple[Optional[str], float]]:
        """(top_user, share) at the deepest indexed ancestor of
        ``bucket/dirname`` at-or-below the band root; None when even the band
        root has no index row (nothing to attribute against)."""
        tbl = self.bands.get(band)
        if tbl is None:
            tbl = self._load_band(band)
        root = band.removeprefix("gs://").rstrip("/")
        path = f"{bucket}/{dirname}" if dirname else bucket
        while len(path) >= len(root):
            hit = tbl.get(path)
            if hit is not None:
                return hit[0], hit[1]
            if path == root or "/" not in path:
                break
            path = path.rsplit("/", 1)[0]
        return None

    def dirs(self, max_depth: int = 6) -> "pd.DataFrame":
        """``(bucket, name)`` rows for every indexed path at depth ≤ ``max_depth``
        (``name`` = the path below the bucket, ``''`` for the bucket row): a
        stand-in for a listing's directories where only shallow structure is
        needed — expanding path-glob attribution rules
        (:func:`~dt_cloud.prefixes.load_prefix_map`) without streaming the
        listing twice."""
        import pandas as pd

        rgs = [i for i, dmin, dmax, pmin, pmax in self.rg_stats if dmin <= max_depth]
        if not rgs:
            return pd.DataFrame({"bucket": pd.Series(dtype=str), "name": pd.Series(dtype=str)})
        t = self.pf.read_row_groups(rgs, columns=["path", "depth"]).to_pandas()
        t = t[t.depth <= max_depth].drop_duplicates("path").sort_values("path")
        parts = t.path.str.partition("/")
        return pd.DataFrame({"bucket": parts[0].to_numpy(), "name": parts[2].to_numpy()})

    def child_split(self, band: str, sweepers: set[str]) -> tuple[int, int, int]:
        """Gross byte split of the band's immediate children:
        (attributed to a sweeper with ≥ MIN_SHARE, attributed to others,
        unattributed/minority) — the console's "what would approval actually
        delete" estimate (gross: kept/conflicted data inside still counts)."""
        tbl = self.bands.get(band)
        if tbl is None:
            tbl = self._load_band(band)
        root = band.removeprefix("gs://").rstrip("/")
        d_child = root.count("/") + 2
        match_b = other_b = unattr_b = 0
        for path, (top, share, total) in tbl.items():
            if path.count("/") + 1 != d_child:
                continue
            if top is None or share < MIN_SHARE:
                unattr_b += total
            elif top in sweepers:
                match_b += total
            else:
                other_b += total
        if not (match_b or other_b or unattr_b):
            # leaf band (no children in the index): judge the band row itself
            hit = tbl.get(root)
            if hit is not None:
                top, share, total = hit
                if top is None or share < MIN_SHARE:
                    unattr_b = total
                elif top in sweepers:
                    match_b = total
                else:
                    other_b = total
        return match_b, other_b, unattr_b
