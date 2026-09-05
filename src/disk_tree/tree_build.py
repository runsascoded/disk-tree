"""One arbitrary-depth tree builder for the site's layer-3 JSON.

Both the GCS (`marin/gcs_usage/viz.py`) and CoreWeave (`job/cw-webdata.py`)
paths produce the same `{n, b, o, c?, …}` `TreeNode`; they differed only in that
CW linked rolled-up dir rows parent→child (any depth) while GCS flattened to
four fixed path components — a cardinality hack for a 595M-row object listing,
never a design choice (`specs/tree-builder-unification.md`). This is CW's
linker, generalized to carry GCS's per-node fields and to mark folds.

Input: **rolled-up dir rows**, one per directory, whose `b`/`o` and additive
fields (`add`) are already **descendant-inclusive**. Additive fields are kept
in that form — sums and weighted-sums, never pre-averaged — precisely so a
folded `(other)` node can be computed by *subtracting* the kept children from
their parent. That subtraction is what makes `(other)` correct in the presence
of direct files: `(other)` = parent − Σ(kept children) = direct files + folded
subdirs, and its attribution/class/date follow the same subtraction. Summing
the folded children instead would silently drop the parent's own direct files.
Display fields (`d`, `cb`, `tm`, `sh`, `us`) are derived from `add` last.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

#: Root sentinel — the path of the top dir row (CW uses "."; either works).
ROOT_KEYS = ("", ".")

#: Additive map fields on `add`: class bytes, team bytes, user bytes, shared
#: (team bytes with no per-user owner). Summed on rollup, subtracted for folds.
_MAPS = ("cb", "tm", "ub", "sh")
#: Additive scalars: byte-weighted mtime numerator/denominator (mean = wts/wb).
_SCALARS = ("wts", "wb")


@dataclass
class DirRow:
    """One rolled-up directory. ``add`` carries descendant-inclusive *additive*
    quantities — any of ``cb``/``tm``/``ub``/``sh`` (dict ``{key: bytes}``) and
    ``wts``/``wb`` (numbers) — from which display fields are derived."""

    path: str
    b: int
    o: int
    add: dict = field(default_factory=dict)


def _seg(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _parent(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else "."


def _sum_add(rows: list[dict]) -> dict:
    out: dict = {}
    for key in _MAPS:
        acc: dict[str, int] = defaultdict(int)
        for r in rows:
            for k, v in (r.get(key) or {}).items():
                acc[k] += v
        if acc:
            out[key] = dict(acc)
    for key in _SCALARS:
        s = sum(r.get(key) or 0 for r in rows)
        if s:
            out[key] = s
    return out


def _sub_add(parent: dict, kept: list[dict]) -> dict:
    """``parent`` additive minus the kept children's — the ``(other)`` residual
    (direct files + folded subdirs). Non-positive entries drop out."""
    ksum = _sum_add(kept)
    out: dict = {}
    for key in _MAPS:
        acc = {}
        sub = ksum.get(key, {})
        for k, v in (parent.get(key) or {}).items():
            r = v - sub.get(k, 0)
            if r > 0:
                acc[k] = r
        if acc:
            out[key] = acc
    for key in _SCALARS:
        r = (parent.get(key) or 0) - ksum.get(key, 0)
        if r > 0:
            out[key] = r
    return out


def _display(add: dict) -> dict:
    """Additive fields → the site's display fields, most-bytes-first."""
    out: dict = {}
    wb = add.get("wb") or 0
    if wb:
        out["d"] = int((add.get("wts") or 0) / wb / 86400)
    if add.get("cb"):
        out["cb"] = dict(sorted(add["cb"].items(), key=lambda kv: -kv[1]))
    if add.get("tm"):
        out["tm"] = dict(sorted(add["tm"].items(), key=lambda kv: -kv[1]))
    if add.get("sh"):
        out["sh"] = dict(sorted(add["sh"].items(), key=lambda kv: -kv[1]))
    if add.get("ub"):
        out["us"] = [[u, b] for u, b in sorted(add["ub"].items(), key=lambda kv: -kv[1])]
    return out


def build_tree(
    rows: list[DirRow],
    total_bytes: int,
    min_frac: float,
    abs_floor: int = 0,
    max_children: int | None = None,
) -> dict:
    """Link ``rows`` into a nested tree; return the root node (path in
    :data:`ROOT_KEYS`).

    ``min_frac`` is the fold floor **relative to each parent**: a child below
    ``min_frac`` × its parent's bytes folds into that parent's ``(other)`` node
    — marked ``f`` (folded count) and carrying the subtracted residual
    attribution — which the client renders as an expandable placeholder rather
    than a dead leaf. Parent-relative (not total-relative) so drilling stays
    useful at every depth: each level folds only what's invisible *in that
    view*, instead of a fleet-scale cut deleting every small child everywhere
    (the 2026-08-26 grug regression: 1,060 drill-worthy swarm dirs collapsed
    into one blob because each was <0.02% of the *fleet*). When callers
    pre-floor in SQL (so no sub-floor rows arrive), the ``(other)`` still forms
    from the parent's byte gap; ``f`` is then the count actually seen.
    ``abs_floor`` (bytes) and ``max_children`` (top-K per parent) bound the
    artifact when parent-relative alone would keep too much — the fleet has
    >100M dirs, and 0.02%-of-parent keeps every child of any evenly-split
    parent recursively. ``total_bytes`` sizes the root (signature stability).
    """
    add_by_path = {r.path: r.add for r in rows}
    nodes: dict[str, dict] = {
        r.path: {"n": r.path if r.path in ROOT_KEYS else _seg(r.path), "b": int(r.b), "o": int(r.o)}
        for r in rows
    }

    kids: dict[str, list[str]] = defaultdict(list)
    root_path = None
    for path in nodes:
        if path in ROOT_KEYS:
            root_path = path
        else:
            kids[_parent(path)].append(path)

    for parent_path, child_paths in kids.items():
        parent = nodes.get(parent_path)
        if not parent:
            continue
        floor = max(int(parent["b"] * min_frac), abs_floor)
        children = sorted((nodes[p] for p in child_paths), key=lambda n: -n["b"])
        kept = [n for n in children if n["b"] >= floor]
        if max_children is not None and len(kept) > max_children:
            kept = kept[:max_children]
        kept_ids = {id(n) for n in kept}
        folded = [n for n in children if id(n) not in kept_ids]
        parent["c"] = kept
        rest_b = parent["b"] - sum(n["b"] for n in kept)
        rest_o = parent["o"] - sum(n["o"] for n in kept)
        if rest_b > floor:
            other = {"n": "(other)", "b": int(rest_b), "o": int(max(rest_o, 0)), "f": len(folded)}
            kept_add = [add_by_path[p] for p in child_paths if id(nodes[p]) in kept_ids]
            other.update(_display(_sub_add(add_by_path.get(parent_path, {}), kept_add)))
            parent["c"].append(other)
        if not parent["c"]:
            del parent["c"]

    if root_path is None:
        raise ValueError("no root row (path in {'', '.'}) among dir rows")
    for path, node in nodes.items():
        node.update(_display(add_by_path[path]))
    return nodes[root_path]
