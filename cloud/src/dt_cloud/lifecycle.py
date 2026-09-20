"""Bucket lifecycle rules as a tracked file (`dt-cloud lifecycle pull|diff|push`).

The bucket's lifecycle configuration is the one piece of storage state that
decides what gets deleted without anyone running a job — Marin's `tmp/ttl=<N>d/`
TTLs, the abort-incomplete-MPU rule, and (since the 2026-09-16 quota incident)
a bucket-wide noncurrent-version GC. Nobody should hand-edit it in place, and
its history should be reviewable, so:

- `pull` snapshots the live rules (normalized: sorted by ID) to a JSON file —
  `job/cw-lifecycle.json` in the repo is the intended state, and the scan job
  writes one per snapshot so every scan carries the rules that were in force;
- `diff` shows file vs live (added / removed / changed rules);
- `push` applies the file read-modify-write style: PUT replaces the whole
  configuration, so the whole intended set is sent and then read back and
  compared — a mismatch raises rather than leaving a half-applied config.

`gc_rule` builds the bucket-wide GC rule (`NoncurrentVersionExpiration` +
`ExpiredObjectDeleteMarker`): a no-op for null-version objects while versioning
is Suspended, it cleans stragglers and zero-byte delete markers, and it is the
safety net that was missing when versioning was Enabled without one.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

GC_RULE_ID = "cw-noncurrent-gc"


def normalize(rules: list[dict]) -> list[dict]:
    """Rules sorted by ID, so file and live compare and diff stably."""
    return sorted((json.loads(json.dumps(r, default=str)) for r in rules), key=lambda r: r["ID"])


def gc_rule(days: int = 1, prefix: str = "") -> dict:
    """Bucket-wide (or `prefix`-scoped) noncurrent-version GC after `days`
    (S3's minimum is 1; no sub-day granularity) + expired-delete-marker cleanup."""
    return {
        "ID": GC_RULE_ID,
        "Filter": {"Prefix": prefix},
        "Status": "Enabled",
        "NoncurrentVersionExpiration": {"NoncurrentDays": days},
        "Expiration": {"ExpiredObjectDeleteMarker": True},
    }


def pull(client: "S3Client", bucket: str) -> list[dict]:
    """The live rules, normalized. A bucket with no configuration → `[]`."""
    try:
        return normalize(client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"])
    except client.exceptions.ClientError as e:  # no config at all is a legitimate state
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            return []
        raise


def diff(intended: list[dict], live: list[dict]) -> dict[str, list]:
    """`{"added": [ids only in intended], "removed": [ids only in live],
    "changed": [ids in both whose bodies differ]}` — all sorted."""
    a = {r["ID"]: r for r in normalize(intended)}
    b = {r["ID"]: r for r in normalize(live)}
    return {
        "added": sorted(a.keys() - b.keys()),
        "removed": sorted(b.keys() - a.keys()),
        "changed": sorted(k for k in a.keys() & b.keys() if a[k] != b[k]),
    }


class LifecycleRaced(RuntimeError):
    """The live configuration changed between reading it and writing ours."""


def push(client: "S3Client", bucket: str, intended: list[dict], *, base: list[dict] | None = None) -> list[dict]:
    """Replace the bucket's configuration with `intended` and verify the
    read-back equals it (normalized). Returns the live rules after the PUT.

    S3 has no conditional PUT for bucket configuration (no ETag/If-Match), so
    `base` is the poor man's compare-and-swap: the live rules the caller
    diffed against. They are re-read immediately before the PUT and, if they
    moved, nothing is written (`LifecycleRaced`) — a millisecond window instead
    of the seconds a human spends reviewing a diff."""
    want = normalize(intended)
    if base is not None:
        live = pull(client, bucket)
        if live != normalize(base):
            raise LifecycleRaced(f"{bucket}: live rules changed since they were read: {diff(live, base)}")
    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": want})
    got = pull(client, bucket)
    if got != want:
        raise RuntimeError(
            f"lifecycle push did not round-trip on {bucket}: live differs from intended: {diff(want, got)}"
        )
    return got


def load(path: str) -> list[dict]:
    with open(path) as f:
        return normalize(json.load(f))


def dump(rules: list[dict]) -> str:
    return json.dumps(normalize(rules), indent=2) + "\n"


def dump_map(by_bucket: dict[str, list[dict]]) -> str:
    """The multi-bucket snapshot the scan job writes (specs/cw-multi-bucket.md
    §2): `{<bucket>: Rules[]}` in the given (deployment) order, each bucket's
    rules normalized. The site reads this and the bare `Rules[]` alike."""
    return json.dumps({b: normalize(r) for b, r in by_bucket.items()}, indent=2) + "\n"
