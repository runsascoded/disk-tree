"""The server-side drainer (spec ``specs/staged-delete.md``, CP4).

The edge (CP2) *enqueues* a deletion run (``finished_ts IS NULL``) — it can't
reach arbitrary user buckets. A process with ``buckets.yml`` creds (the laptop,
or a worker) drains those runs: for each staged URI it sizes from the local
scan DB and deletes through ``backend_for``, records a per-URI band, and writes
the totals + ``finished_ts`` back to D1.

The DB is duck-typed (``.query(sql, params) -> list[dict]``) so the loop runs
against the real :class:`~disk_tree.d1.D1Client` or an in-memory fake in tests;
``size_fn``/``delete_fn`` are injected exactly as the CP1 engine does.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

SizeFn = Callable[[str], "tuple[int, int]"]
DeleteFn = Callable[[str], None]
Announce = Callable[[dict], None]


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def pending_runs(db: Any) -> list[dict]:
    """Enqueued runs (awaiting execution), oldest first."""
    return db.query(
        "SELECT run_id, plan_id, mode, actor, started_ts FROM deletion_runs "
        "WHERE finished_ts IS NULL ORDER BY started_ts"
    )


def run_items(db: Any, plan_id: int) -> list[str]:
    return [r["uri"] for r in db.query("SELECT uri FROM plan_items WHERE plan_id = ? ORDER BY uri", [plan_id])]


def execute_run(
    db: Any,
    run: dict,
    *,
    size_fn: SizeFn,
    delete_fn: DeleteFn,
    undo_state: str = "none",
    now: Callable[[], int] = _now,
) -> dict:
    """Execute one enqueued run: delete each staged URI, record a band per URI,
    finish the run in D1. A single URI's failure is recorded (not deleted) and
    doesn't abort the run. Returns a summary."""
    uris = run_items(db, run["plan_id"])
    deleted_bytes = deleted_objects = 0
    errors: list[tuple[str, str]] = []
    for uri in uris:
        nbytes, nobjs = size_fn(uri)
        deleted = 0
        try:
            delete_fn(uri)
            deleted = 1
            deleted_bytes += nbytes
            deleted_objects += nobjs
        except Exception as e:  # one URI failing must not strand the rest of the run
            errors.append((uri, str(e)))
        # idempotent on a retry of a half-finished run
        db.query(
            "INSERT OR REPLACE INTO deletion_bands (run_id, uri, bytes, objects, deleted, gone) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            [run["run_id"], uri, nbytes, nobjs, deleted],
        )
    finished = now()
    db.query(
        "UPDATE deletion_runs SET finished_ts = ?, deleted_bytes = ?, deleted_objects = ?, undo_state = ? "
        "WHERE run_id = ?",
        [finished, deleted_bytes, deleted_objects, undo_state if deleted_objects else "none", run["run_id"]],
    )
    return {
        "run_id": run["run_id"], "plan_id": run["plan_id"], "actor": run.get("actor"),
        "items": len(uris), "deleted_bytes": deleted_bytes, "deleted_objects": deleted_objects,
        "errors": errors, "finished_ts": finished,
    }


def drain_once(
    db: Any,
    *,
    size_fn: SizeFn,
    delete_fn: DeleteFn,
    undo_state: str = "none",
    announce: Optional[Announce] = None,
    now: Callable[[], int] = _now,
) -> list[dict]:
    """Execute every enqueued run once. Returns a summary per run."""
    out = []
    for run in pending_runs(db):
        summary = execute_run(db, run, size_fn=size_fn, delete_fn=delete_fn, undo_state=undo_state, now=now)
        out.append(summary)
        if announce:
            announce(summary)
    return out
