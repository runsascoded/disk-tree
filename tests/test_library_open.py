"""Runtime library switching (`disk_tree.library` + `config.set_root`).

Opening a library re-points the active root and everything hanging off it — the
SQLAlchemy engine, the backend singleton, the server caches — so scans written
under one root are invisible once another is open, and reappear on switch-back.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from disk_tree import config, library
from disk_tree.sqla import db as sqla_db
from disk_tree.storage import reset_backend


@pytest.fixture
def isolated_config(monkeypatch, tmp_path: Path):
    """Snapshot/restore the mutable `config` + DB singletons around a test.

    `set_root` mutates module globals and rebinds the shared engine; without this
    the switch would leak into the rest of the suite.
    """
    snap = {k: getattr(config, k) for k in ('ROOT_DIR', 'DEFAULT_SCANS_DIR', 'SQLITE_PATH', 'SCANS_DIR', '_explicit_root')}
    db_snap = (sqla_db.db, sqla_db.app, sqla_db.cache_url)
    monkeypatch.setenv(library.LIBRARIES_FILE_VAR, str(tmp_path / 'libraries.json'))
    sqla_db.db = sqla_db.app = sqla_db.cache_url = None
    reset_backend()
    yield
    for k, v in snap.items():
        setattr(config, k, v)
    sqla_db.db, sqla_db.app, sqla_db.cache_url = db_snap
    reset_backend()


def _add_scan(path: str) -> None:
    """Insert one Scan row into the *currently open* library's DB, via the ORM."""
    from disk_tree.sqla.model import Scan
    db = sqla_db.init()
    db.session.add(Scan(path=path, time=datetime.now().astimezone(), blob='x.parquet'))
    db.session.commit()


def _scan_paths() -> list[str]:
    from disk_tree.sqla.model import Scan
    db = sqla_db.init()
    return sorted(s.path for s in db.session.query(Scan).all())


def test_open_rebinds_root_and_derived_paths(isolated_config, tmp_path: Path):
    a = tmp_path / 'A'
    a.mkdir()
    out = library.open_library(str(a))
    assert config.current_root() == str(a)
    assert config.SQLITE_PATH == str(a / 'disk-tree.db')
    assert config.SCANS_DIR == str(a / 'scans')  # explicit root → no volume discovery
    assert out == {'path': str(a), 'db': str(a / 'disk-tree.db'), 'exists': False}


def test_scans_follow_the_open_library(isolated_config, tmp_path: Path):
    a, b = tmp_path / 'A', tmp_path / 'B'
    a.mkdir()
    b.mkdir()

    library.open_library(str(a))
    _add_scan('/from/a')
    assert _scan_paths() == ['/from/a']

    library.open_library(str(b))       # engine rebinds to B's fresh DB
    assert _scan_paths() == []

    library.open_library(str(a))       # switch back — A's row is still there
    assert _scan_paths() == ['/from/a']


def test_recents_records_most_recent_first(isolated_config, tmp_path: Path):
    a, b = tmp_path / 'A', tmp_path / 'B'
    a.mkdir()
    b.mkdir()
    library.open_library(str(a))
    library.open_library(str(b))
    library.open_library(str(a))       # re-open dedupes, moves A to front

    state = library.list_libraries()
    assert state['current'] == str(a)
    assert [r['path'] for r in state['recents']] == [str(a), str(b)]

    # Persisted to the override pointer file, not to any library dir.
    on_disk = json.loads((tmp_path / 'libraries.json').read_text())
    assert [r['path'] for r in on_disk['recents']] == [str(a), str(b)]


def test_open_nonexistent_dir_raises(isolated_config, tmp_path: Path):
    with pytest.raises(ValueError, match='not a directory'):
        library.open_library(str(tmp_path / 'nope'))


def test_open_unmounted_volume_refuses_boot_disk_makedirs(isolated_config, monkeypatch):
    """A root under an absent /Volumes mount must not be created on the boot disk."""
    monkeypatch.setattr(config, 'ismount', lambda p: False)
    monkeypatch.setattr(config, 'isdir', lambda p: True, raising=False)
    monkeypatch.setattr(library, 'isdir', lambda p: True)
    with pytest.raises(RuntimeError, match='unmounted volume'):
        library.open_library('/Volumes/gone/lib')
