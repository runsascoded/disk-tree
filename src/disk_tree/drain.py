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
SubmitFn = Callable[[str, list[str]], str]  # (run_id, uris) -> batch job id


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def pending_runs(db: Any) -> list[dict]:
    """Runs awaiting execution, oldest first: not finished and not handed to Batch
    (a `batch_job` run is running remotely — the Batch job finishes it)."""
    return db.query(
        "SELECT run_id, plan_id, mode, actor, started_ts FROM deletion_runs "
        "WHERE finished_ts IS NULL AND batch_job IS NULL ORDER BY started_ts"
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
    submit_fn: Optional[SubmitFn] = None,
    batch_threshold: Optional[int] = None,
) -> dict:
    """Execute one enqueued run. Small (or no `submit_fn`): delete each staged URI
    inline, record a band per URI, finish the run — a single URI's failure is
    recorded, not fatal. Large (scope over `batch_threshold`): hand the run to
    Batch (`submit_fn`) and record its `batch_job` instead, leaving it unfinished
    for the Batch job to complete. Returns a summary."""
    uris = run_items(db, run["plan_id"])
    sized = [(uri, *size_fn(uri)) for uri in uris]  # (uri, bytes, objects)

    if submit_fn is not None and batch_threshold is not None and sum(o for _, _, o in sized) > batch_threshold:
        job = submit_fn(run["run_id"], uris)
        db.query("UPDATE deletion_runs SET batch_job = ? WHERE run_id = ?", [job, run["run_id"]])
        return {
            "run_id": run["run_id"], "plan_id": run["plan_id"], "actor": run.get("actor"),
            "items": len(uris), "deleted_bytes": 0, "deleted_objects": 0, "errors": [],
            "submitted": True, "batch_job": job,
        }

    deleted_bytes = deleted_objects = 0
    errors: list[tuple[str, str]] = []
    for uri, nbytes, nobjs in sized:
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
        "errors": errors, "submitted": False, "finished_ts": finished,
    }


def drain_once(
    db: Any,
    *,
    size_fn: SizeFn,
    delete_fn: DeleteFn,
    undo_state: str = "none",
    announce: Optional[Announce] = None,
    now: Callable[[], int] = _now,
    submit_fn: Optional[SubmitFn] = None,
    batch_threshold: Optional[int] = None,
) -> list[dict]:
    """Execute every pending run once (inline, or submit oversized ones to Batch).
    Returns a summary per run."""
    out = []
    for run in pending_runs(db):
        summary = execute_run(
            db, run, size_fn=size_fn, delete_fn=delete_fn, undo_state=undo_state, now=now,
            submit_fn=submit_fn, batch_threshold=batch_threshold,
        )
        out.append(summary)
        if announce:
            announce(summary)
    return out
