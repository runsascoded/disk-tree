"""The staged-delete engine (spec ``specs/staged-delete.md``, checkpoint 1).

Stage URIs into a shared open :class:`~disk_tree.sqla.Plan`, then dispatch the
plan — a :class:`~disk_tree.sqla.DeletionRun` that deletes each item (real) or
reports it (dry), recording a per-URI :class:`~disk_tree.sqla.DeletionBand`.

Deployment-agnostic: ``delete_fn(uri)`` (what actually removes the object — the
existing ``backend_for(uri).delete``) and ``size_fn(uri) -> (bytes, objects)``
are injected, so the engine has no backend or DB-init coupling and unit-tests
against a plain session with fakes.
"""
from __future__ import annotations

import secrets
from datetime import datetime
from typing import Callable, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .sqla import DeletionBand, DeletionRun, Plan, PlanItem

STAGED = "Staged"

SizeFn = Callable[[str], "tuple[int, int]"]
DeleteFn = Callable[[str], None]


def _now() -> datetime:
    return datetime.now().astimezone()


def _canonical(uri: str) -> str:
    from disk_tree.backends import canonical

    return canonical(uri)


def open_plan(session: Session, who: str, name: str = STAGED) -> Plan:
    """The shared open plan named ``name``, created (empty) if none is open."""
    plan = session.scalars(
        select(Plan).where(Plan.name == name, Plan.state == "open").order_by(Plan.id.desc())
    ).first()
    if plan is None:
        plan = Plan(name=name, created_by=who, created_ts=_now())
        session.add(plan)
        session.flush()
    return plan


def stage(session: Session, uris: Iterable[str], who: str, note: str | None = None) -> tuple[Plan, list[str]]:
    """Add ``uris`` to the shared open plan (idempotent per URI). Returns the
    plan and the URIs newly staged (already-staged ones are skipped)."""
    plan = open_plan(session, who)
    have = set(session.scalars(select(PlanItem.uri).where(PlanItem.plan_id == plan.id)))
    added: list[str] = []
    for raw in uris:
        uri = _canonical(raw)
        if uri in have:
            continue
        session.add(PlanItem(plan_id=plan.id, uri=uri, added_by=who, added_ts=_now(), note=note))
        have.add(uri)
        added.append(uri)
    session.flush()
    return plan, added


def unstage(session: Session, uris: Iterable[str]) -> int:
    """Remove ``uris`` from every open plan. Returns the number removed."""
    open_ids = set(session.scalars(select(Plan.id).where(Plan.state == "open")))
    removed = 0
    for raw in uris:
        uri = _canonical(raw)
        for item in session.scalars(select(PlanItem).where(PlanItem.uri == uri, PlanItem.plan_id.in_(open_ids))):
            session.delete(item)
            removed += 1
    session.flush()
    return removed


def plan_by_ref(session: Session, ref: str | None) -> Plan | None:
    """Resolve a plan by id (numeric) or name; ``None`` -> the open ``Staged`` plan."""
    if ref is None:
        return session.scalars(
            select(Plan).where(Plan.name == STAGED, Plan.state == "open").order_by(Plan.id.desc())
        ).first()
    if ref.isdigit():
        return session.get(Plan, int(ref))
    return session.scalars(select(Plan).where(Plan.name == ref).order_by(Plan.id.desc())).first()


def items(session: Session, plan: Plan) -> list[PlanItem]:
    return list(session.scalars(select(PlanItem).where(PlanItem.plan_id == plan.id).order_by(PlanItem.uri)))


def dispatch(
    session: Session,
    plan: Plan,
    actor: str,
    *,
    for_real: bool,
    delete_fn: DeleteFn,
    size_fn: SizeFn,
    undo_state: str = "none",
) -> DeletionRun:
    """Execute ``plan``: size each item (``size_fn``) and, when ``for_real``,
    delete it (``delete_fn``). Records a :class:`DeletionRun` + one
    :class:`DeletionBand` per item; a real dispatch closes the plan. ``dry`` (the
    default) deletes nothing and leaves the plan open."""
    started = _now()
    # a plan takes many dry runs + one real, so the second-resolution timestamp
    # alone collides; a random suffix keeps run_ids unique
    run_id = f"{plan.id}-{started.strftime('%Y%m%dT%H%M%S')}-{secrets.token_hex(4)}"
    run = DeletionRun(
        run_id=run_id, plan_id=plan.id, mode="real" if for_real else "dry",
        actor=actor, started_ts=started, undo_state=undo_state if for_real else "none",
    )
    session.add(run)
    for item in items(session, plan):
        nbytes, nobjs = size_fn(item.uri)
        deleted = 0
        if for_real:
            delete_fn(item.uri)
            deleted = 1
            run.deleted_bytes += nbytes
            run.deleted_objects += nobjs
        session.add(DeletionBand(run_id=run_id, uri=item.uri, bytes=nbytes, objects=nobjs, deleted=deleted))
    run.finished_ts = _now()
    if for_real:
        plan.state = "closed"
        plan.closed_ts = run.finished_ts
    session.flush()
    return run
