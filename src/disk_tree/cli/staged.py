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
@option("-f", "--for-real", is_flag=True, help="Actually delete (default: dry-run, report only)")
@option("-j", "--json", "as_json", is_flag=True, help="Emit JSON")
@argument("plan_ref", required=False)
def dispatch_cmd(for_real: bool, as_json: bool, plan_ref: str | None):
    """Dispatch a plan (PLAN_REF = id or name; default the open `Staged` plan):
    delete its staged URIs, or (default) report what would be deleted."""
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
