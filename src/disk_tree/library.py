"""Open a scans "library" (a ``DISK_TREE_ROOT`` directory) at runtime.

A library is a self-contained root: ``disk-tree.db`` + ``scans/`` + ``diffs/`` +
logs. :func:`open_library` switches the active root and rebinds everything that
hangs off it — via :func:`config.set_root`'s hooks: the SQLAlchemy engine, the
backend singleton, the server caches. Only one library is open at a time.

Recents live in a pointer file *outside* any library (libraries come and go), on
the boot-disk config dir by default; ``DISK_TREE_LIBRARIES_FILE`` overrides it.
"""
import json
import time
from os import environ as env, makedirs
from os.path import dirname, exists, expanduser, isdir, join

from . import config

LIBRARIES_FILE_VAR = 'DISK_TREE_LIBRARIES_FILE'
MAX_RECENTS = 20


def _libraries_file() -> str:
    return expanduser(env.get(LIBRARIES_FILE_VAR, join(config.DEFAULT_ROOT_DIR, 'libraries.json')))


def _load() -> dict:
    p = _libraries_file()
    if exists(p):
        with open(p) as f:
            return json.load(f)
    return {'current': None, 'recents': []}


def _save(state: dict) -> None:
    p = _libraries_file()
    makedirs(dirname(p), exist_ok=True)
    with open(p, 'w') as f:
        json.dump(state, f, indent=2)
        f.write('\n')


def list_libraries() -> dict:
    """``{current, recents: [{path, opened_at}, ...]}`` — active root reflected in ``current``."""
    state = _load()
    state['current'] = config.current_root()
    return state


def record_open(path: str) -> None:
    """Move ``path`` to the front of the recents list and mark it current."""
    path = expanduser(path)
    state = _load()
    recents = [r for r in state.get('recents', []) if r.get('path') != path]
    recents.insert(0, {'path': path, 'opened_at': time.time()})
    state['recents'] = recents[:MAX_RECENTS]
    state['current'] = path
    _save(state)


def summary() -> dict:
    """The active library at a glance: root path, DB path, whether the DB exists yet."""
    root = config.current_root()
    return {'path': root, 'db': config.SQLITE_PATH, 'exists': exists(config.SQLITE_PATH)}


def open_library(path: str) -> dict:
    """Switch the active library to ``path``, rebinding DB / backend / caches.

    Raises ``ValueError`` if ``path`` isn't an existing directory — opening never
    *creates* a library (a fresh root is created by ``disk-tree index`` against
    it). Returns :func:`summary` for the newly opened library.
    """
    path = expanduser(path)
    if not isdir(path):
        raise ValueError(f'not a directory: {path!r}')
    config.set_root(path)
    record_open(path)
    return summary()
