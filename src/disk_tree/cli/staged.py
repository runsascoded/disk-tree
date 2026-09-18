"""`disk-tree stage` / `staged` / `unstage` / `dispatch` — the local staged-delete
path (spec ``specs/staged-delete.md``, checkpoint 1).

Stage URIs into a shared open plan, review them, then dispatch — dry by default
(report bytes/objects), ``--for-real`` to delete through the existing backend
(`aws s3 rm` for S3/R2, `rm` for local). A run + per-URI bands are recorded.
"""
from __future__ import annotations

import json
import os
from sys import stdout

from click import argument, option
from humanize import naturalsize
from utz import err

from disk_tree.cli.base import cli
from disk_tree.staged_backend import delete_fn as _delete_fn
from disk_tree.staged_backend import restore_fn as _restore_fn
from disk_tree.staged_backend import session as _session
from disk_tree.staged_backend import size_fn as _size_fn


def _who() -> str:
    return os.environ.get("USER") or "local"


@cli.command("stage")
@option("-m", "--note", default=None, help="Note recorded on each staged item")
@argument("uris", nargs=-1, required=True)
def stage_cmd(note: str | None, uris: tuple[str, ...]):
    """Stage URIS for deletion (add them to the shared open plan)."""
    from disk_tree.staged import stage

    session = _session()
    plan, added = stage(session, uris, _who(), note)
    session.commit()
    err(f"staged {len(added)} into plan {plan.id} ({plan.name}); {len(uris) - len(added)} already staged")
    for uri in added:
        print(uri)


@cli.command("unstage")
@argument("uris", nargs=-1, required=True)
def unstage_cmd(uris: tuple[str, ...]):
    """Remove URIS from every open plan."""
    from disk_tree.staged import unstage

    session = _session()
    n = unstage(session, uris)
    session.commit()
    err(f"unstaged {n}")


@cli.command("staged")
@option("-j", "--json", "as_json", is_flag=True, help="Emit JSON")
def staged_cmd(as_json: bool):
    """List open plans (staged sets) and recent runs."""
    from sqlalchemy import select

    from disk_tree.sqla import DeletionRun, Plan
    from disk_tree.staged import items

    session = _session()
    plans = list(session.scalars(select(Plan).where(Plan.state == "open").order_by(Plan.id)))
    runs = list(session.scalars(select(DeletionRun).order_by(DeletionRun.started_ts.desc()).limit(10)))
    if as_json:
        out = {
            "plans": [
                {"id": p.id, "name": p.name, "state": p.state, "items": [it.uri for it in items(session, p)]}
                for p in plans
            ],
            "runs": [
                {"run_id": r.run_id, "plan_id": r.plan_id, "mode": r.mode,
                 "deleted_bytes": r.deleted_bytes, "deleted_objects": r.deleted_objects}
                for r in runs
            ],
        }
        json.dump(out, stdout, indent=2)
        print()
        return
    for p in plans:
        its = items(session, p)
        print(f'Plan {p.id} "{p.name}" ({p.state}) — {len(its)} item(s)')
        for it in its:
            print(f"  {it.uri}")
    if runs:
        print("Runs:")
        for r in runs:
            print(f"  {r.run_id}  {r.mode:4}  {naturalsize(r.deleted_bytes)}  {r.deleted_objects} obj")


@cli.command("dispatch")
@option("-c", "--config", "config_path", default=None, help="--serve: buckets.yml path (for the `delete:` chat/undo/database_id block)")
@option("-f", "--for-real", is_flag=True, help="Actually delete (default: dry-run, report only)")
@option("-i", "--interval", default=30, type=int, help="--serve: base poll seconds (exp-backoff to 5x while idle)")
@option("-j", "--json", "as_json", is_flag=True, help="Emit JSON")
@option("-o", "--once", is_flag=True, help="--serve: drain the enqueued runs once and exit")
@option("-s", "--serve", is_flag=True, help="Drain edge-enqueued runs from D1 and execute them (the CP4 drainer)")
@argument("plan_ref", required=False)
def dispatch_cmd(config_path: str | None, for_real: bool, interval: int, as_json: bool, once: bool, serve: bool, plan_ref: str | None):
    """Dispatch a plan (PLAN_REF = id or name; default the open `Staged` plan):
    delete its staged URIs, or (default) report what would be deleted.

    With `--serve`, instead run the drainer: poll the edge's D1 for enqueued
    runs (the browser dispatched them; the edge can't reach user buckets) and
    execute each here, where `buckets.yml` creds live."""
    if serve:
        _serve(config_path, interval, once)
        return

    from disk_tree.staged import dispatch, items, plan_by_ref

    session = _session()
    plan = plan_by_ref(session, plan_ref)
    if plan is None:
        raise SystemExit(f"dispatch: no plan {plan_ref or '(open Staged)'!r}")
    n = len(items(session, plan))
    if n == 0:
        raise SystemExit(f"dispatch: plan {plan.id} has no staged items")
    err(f"dispatch: plan {plan.id} ({plan.name}) — {n} item(s), mode={'real' if for_real else 'dry'}")
    run = dispatch(session, plan, _who(), for_real=for_real, delete_fn=_delete_fn, size_fn=_size_fn)
    session.commit()
    # dry runs record sizes on the bands but delete nothing, so report the band totals
    from sqlalchemy import func, select

    from disk_tree.sqla import DeletionBand

    tot_bytes, tot_objs = session.execute(
        select(func.coalesce(func.sum(DeletionBand.bytes), 0), func.coalesce(func.sum(DeletionBand.objects), 0))
        .where(DeletionBand.run_id == run.run_id)
    ).one()
    if as_json:
        json.dump(
            {"run_id": run.run_id, "mode": run.mode, "bytes": tot_bytes, "objects": tot_objs,
             "deleted_bytes": run.deleted_bytes, "deleted_objects": run.deleted_objects, "plan_state": plan.state},
            stdout, indent=2,
        )
        print()
        return
    verb = "deleted" if for_real else "would delete"
    print(f"{run.run_id}: {verb} {naturalsize(tot_bytes)} across {tot_objs} object(s)")


@cli.command("undo")
@option("-f", "--for-real", is_flag=True, help="Actually restore (default: report what would be restored)")
@option("-j", "--json", "as_json", is_flag=True, help="Emit JSON")
@argument("run_id")
def undo_cmd(for_real: bool, as_json: bool, run_id: str):
    """Undo a deletion RUN_ID: restore the objects it deleted, where the store
    allows it (S3/R2 versioning — remove the delete-markers). Dry by default."""
    from disk_tree.sqla import DeletionRun
    from disk_tree.staged import restorable_bands, undo_run

    session = _session()
    run = session.get(DeletionRun, run_id)
    if run is None:
        raise SystemExit(f"undo: no run {run_id!r}")
    bands = restorable_bands(session, run)
    if not bands:
        raise SystemExit(f"undo: run {run_id} deleted nothing to restore")

    if not for_real:
        would = sum(b.objects for b in bands)
        if as_json:
            json.dump({"run_id": run_id, "paths": len(bands), "objects": would, "for_real": False}, stdout, indent=2)
            print()
            return
        print(f"{run_id}: would restore {would} object(s) across {len(bands)} path(s) (--for-real to do it)")
        return

    _, results = undo_run(session, run, _restore_fn)
    session.commit()
    total = sum(n for _, n in results)
    if as_json:
        json.dump(
            {"run_id": run_id, "restored": total, "undo_state": run.undo_state,
             "paths": [{"uri": u, "restored": n} for u, n in results]},
            stdout, indent=2,
        )
        print()
        return
    print(f"{run_id}: restored {total} object(s) across {len(results)} path(s); undo_state={run.undo_state}")


def _delete_cfg(config_path: str | None) -> dict:
    """The deployment-wide `delete:` block from buckets.yml, or `{}` if none."""
    from disk_tree.cli.sync import load_config

    try:
        return load_config(config_path).delete or {}
    except FileNotFoundError:
        return {}


def _batch_submitter(batch_cfg: dict | None):
    """Build a `(submit_fn, threshold)` for the drainer from a `delete.batch`
    block (`provider: aws`, `job_queue`, `job_definition`, `region?`,
    `threshold?`), or `(None, None)` when Batch isn't configured."""
    if not batch_cfg:
        return None, None
    import boto3

    from disk_tree.batch import submit_delete_job

    client = boto3.client("batch", region_name=batch_cfg.get("region"))
    jq, jd = batch_cfg["job_queue"], batch_cfg["job_definition"]

    def submit(run_id: str, uris: list[str]) -> str:
        return submit_delete_job(client, run_id=run_id, uris=uris, job_queue=jq, job_definition=jd)

    return submit, int(batch_cfg.get("threshold", 10000))


def _serve(config_path: str | None, interval: int, once: bool) -> None:
    """The CP4 drainer: execute edge-enqueued runs from D1, here where the
    backend creds live. Exp-backoff polling (base `interval`, up to 5x while
    idle); Ctrl-C stops cleanly."""
    import time

    from disk_tree.d1 import D1Client, D1Error
    from disk_tree.drain import drain_once
    from disk_tree.notify.announce import make_announcer

    delete_cfg = _delete_cfg(config_path)
    try:
        d1 = D1Client.from_env(delete_cfg.get("database_id"))
    except D1Error as e:
        raise SystemExit(f"dispatch --serve: {e}")
    undo_state = delete_cfg.get("undo", "none")
    announce = make_announcer(delete_cfg)
    submit_fn, batch_threshold = _batch_submitter(delete_cfg.get("batch"))
    err(f"dispatch --serve: draining D1 {d1.database_id} (undo={undo_state}, "
        f"chat={delete_cfg.get('chat', 'none')}, batch={'on' if submit_fn else 'off'})")

    idle = 0
    try:
        while True:
            summaries = drain_once(
                d1, size_fn=_size_fn, delete_fn=_delete_fn, undo_state=undo_state, announce=announce,
                submit_fn=submit_fn, batch_threshold=batch_threshold,
            )
            for s in summaries:
                if s.get("submitted"):
                    err(f"  submitted {s['run_id']} to Batch job {s['batch_job']} ({s['items']} path(s), over threshold)")
                    continue
                err(f"  ran {s['run_id']}: deleted {naturalsize(s['deleted_bytes'])}"
                    f" across {s['deleted_objects']}/{s['items']} object(s)"
                    + (f", {len(s['errors'])} failed" if s['errors'] else ""))
            if once:
                if not summaries:
                    err("dispatch --serve --once: no enqueued runs")
                return
            idle = 0 if summaries else min(idle + 1, 4)
            time.sleep(interval * (1 + idle))
    except KeyboardInterrupt:
        err("dispatch --serve: stopped")
