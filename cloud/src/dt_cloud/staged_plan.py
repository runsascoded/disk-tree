"""The staged set as the executor's delete set (specs/staged-delete.md;
sweep-plan-union checkpoint 3).

A dispatch snapshots a plan's items into ``plan.json`` in the run dir; ``sweep
manifest --plan`` reads it here instead of the marks ledger. Items are the
canonical ``gs://<bucket>/<path>/`` prefixes the plans store keeps, and a gcs
plan may span buckets — so the plan is read into per-bucket relative prefix
sets, and the manifest's bucket set is derived from it. Under the opt-in model
the plan is the whole intent: no ledger is consulted, and the only carve-outs
are the plan's own ``keep`` list (deepest prefix wins, as on cw).

plan.json::

    {
      "plan_id": 12,
      "name": "Staged",
      "sweep": ["gs://marin-us-east1/checkpoints/old/", "gs://marin-eu-west4/tmp/x/"],
      "keep": ["gs://marin-us-east1/checkpoints/old/best/"],   # optional
      "buckets": ["marin-eu-west4", "marin-us-east1"]           # optional, derived here anyway
    }
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

# `gs://<bucket>/<path>`: the bucket per GCS naming (lowercase, digits, `-`,
# `_`, `.`), then a non-empty path — the bucket root is not a plan item.
CANONICAL_RE = re.compile(r"^gs://([a-z0-9][a-z0-9._-]*)/(.+)$")
SEGMENT_RE = re.compile(r"^[^/\\]+$")


class PlanError(Exception):
    """A malformed plan.json — the dispatch's snapshot is wrong, not the data."""


def split_prefix(raw: str) -> tuple[str, str]:
    """``gs://b/a/c`` or ``gs://b/a/c/`` -> ``("b", "a/c/")``: the bucket and the
    relative prefix (trailing slash). Raises ``PlanError`` for anything that is
    not a canonical, non-root, ``.``/``..``-free ``gs://`` prefix."""
    m = CANONICAL_RE.match(raw.strip())
    if not m:
        raise PlanError(f"bad plan prefix {raw!r} (want gs://<bucket>/<path>/)")
    bucket, path = m.groups()
    rel = path.rstrip("/") + "/"
    segments = rel[:-1].split("/")
    if any(s in (".", "..") or not SEGMENT_RE.match(s) for s in segments):
        raise PlanError(f"bad plan prefix {raw!r} (empty, '.', '..' or backslash segment)")
    return bucket, rel


def _by_bucket(prefixes: list[str]) -> dict[str, tuple[str, ...]]:
    out: dict[str, set[str]] = {}
    for p in prefixes:
        bucket, rel = split_prefix(p)
        out.setdefault(bucket, set()).add(rel)
    return {b: tuple(sorted(rels)) for b, rels in sorted(out.items())}


@dataclass(frozen=True)
class StagedPlan:
    """A plan's items grouped by bucket: ``sweep[bucket]`` / ``keep[bucket]`` are
    sorted relative prefixes (trailing slash). ``keep`` only carries buckets
    that have a keep."""

    plan_id: int
    name: str
    sweep: dict[str, tuple[str, ...]]
    keep: dict[str, tuple[str, ...]]

    @property
    def buckets(self) -> tuple[str, ...]:
        """Every bucket a sweep item names, sorted — the manifest's bucket set."""
        return tuple(self.sweep)

    def bands(self, bucket: str) -> tuple[str, ...]:
        """The bucket's sweep items back in canonical form — what the executor
        takes as the run's bands (its listing roots + per-band accounting)."""
        return tuple(f"gs://{bucket}/{rel}" for rel in self.sweep.get(bucket, ()))

    def classify(self, bucket: str, dirname: str) -> str:
        """One directory (``''`` = bucket root, else ``a/b``) under the plan:
        ``eligible`` when its deepest covering sweep prefix is deeper than any
        covering keep prefix, ``keep`` when a keep carves it out, else
        ``outside_bands``. The same deepest-mark-wins rule as cw's manifest."""
        key = f"{dirname}/" if dirname else ""
        sw = _deepest(key, self.sweep.get(bucket, ()))
        kp = _deepest(key, self.keep.get(bucket, ()))
        if sw > kp:
            return "eligible"
        if kp >= 0:
            return "keep"
        return "outside_bands"


def _deepest(key: str, prefixes: tuple[str, ...]) -> int:
    return max((len(p) for p in prefixes if key.startswith(p)), default=-1)


def parse_plan(d: object) -> StagedPlan:
    """A plan.json object -> ``StagedPlan``; ``PlanError`` when malformed
    (no/invalid ``plan_id``, empty or non-list ``sweep``, a bad prefix)."""
    if not isinstance(d, dict):
        raise PlanError("plan.json must be a JSON object")
    plan_id = d.get("plan_id")
    if not isinstance(plan_id, int) or isinstance(plan_id, bool):
        raise PlanError(f"plan_id must be an integer, got {plan_id!r}")
    sweep = d.get("sweep")
    if not isinstance(sweep, list) or not sweep or not all(isinstance(p, str) for p in sweep):
        raise PlanError("sweep must be a non-empty list of gs:// prefixes")
    keep = d.get("keep", [])
    if not isinstance(keep, list) or not all(isinstance(p, str) for p in keep):
        raise PlanError("keep must be a list of gs:// prefixes")
    name = d.get("name", f"plan {plan_id}")
    if not isinstance(name, str):
        raise PlanError(f"name must be a string, got {name!r}")
    return StagedPlan(plan_id=plan_id, name=name, sweep=_by_bucket(sweep), keep=_by_bucket(keep))


def load_plan(path: str) -> StagedPlan:
    """Read a plan.json (local path or fsspec URL, e.g. the run dir's
    ``gs://…/plan.json``)."""
    import fsspec

    with fsspec.open(path, "r") as fh:
        return parse_plan(json.load(fh))
