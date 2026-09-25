"""Wiring between the pure staged-delete engine (`disk_tree.staged`) and the
real backends/DB (spec ``specs/staged-delete.md``).

The engine takes ``size_fn``/``delete_fn`` injected so it stays backend- and
DB-free; this is the one place that binds them to the actual scan DB and
``backend_for``. Shared by the CLI (``cli/staged.py``) and the Flask server.

The session comes from a standalone SQLAlchemy engine over
``config.SQLITE_PATH`` — *not* flask-sqlalchemy's ``init()``, which pushes its
own app context and so corrupts the server's request context when called inside
a request handler.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session

_ENGINE: "Engine | None" = None


def _engine() -> "Engine":
    """A process-wide engine over the current ``config.SQLITE_PATH`` (rebuilt if
    the root changes, e.g. a library switch). The deletion tables auto-create,
    and columns the models gained since the DB was made are added."""
    global _ENGINE
    from sqlalchemy import create_engine

    from . import config
    from .sqla import Plan  # noqa: F401  (registers every model on `Base.metadata`)
    from .sqla.migrate import add_missing_columns

    url = f"sqlite:///{config.SQLITE_PATH}"
    if _ENGINE is None or str(_ENGINE.url) != url:
        _ENGINE = create_engine(url)
        add_missing_columns(_ENGINE)  # create_all + the schema catch-up pass
    return _ENGINE


def session() -> "Session":
    """A fresh session on the scan DB (caller commits/closes)."""
    from sqlalchemy.orm import Session

    return Session(_engine())


def size_fn(uri: str) -> tuple[int, int]:
    """``(bytes, objects)`` for ``uri`` from its freshest covering scan; ``(0, 0)``
    if none is known (the executor still deletes — sizing is for the report)."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from disk_tree.backends import canonical
    from disk_tree.sqla import Scan

    with Session(_engine()) as s:
        scan = s.scalars(
            select(Scan).where(Scan.path == canonical(uri)).order_by(Scan.time.desc())
        ).first()
        if scan is None or scan.size is None:
            return 0, 0
        return scan.size, (scan.n_desc or 0) + 1


def delete_fn(uri: str) -> None:
    from disk_tree.backends import backend_for

    backend_for(uri).delete(uri)


def restore_fn(uri: str) -> int:
    """Undo a delete of ``uri`` through its backend (spec CP5); objects restored."""
    from disk_tree.backends import backend_for

    return backend_for(uri).restore(uri)
