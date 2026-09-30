"""`dt-cloud index-sync`: a deployment's index generation holds only some of
`INDEX_VARIANTS` (gcs's `path-index` writes no age pyramid or over-time tier);
the absent ones are skipped and reported, the present ones synced in order."""
from pathlib import Path

import pytest
from click.testing import CliRunner

from dt_cloud import cli as C
from dt_cloud import index_footer as F
from dt_cloud.cli import main


@pytest.fixture
def synced(monkeypatch) -> list[tuple[str, str, str, str, str, bool]]:
    calls: list[tuple[str, str, str, str, str, bool]] = []

    def fake_sync_d1(date, parquet_path, *, variant, gen, key, remote=True, **_):
        calls.append((date, parquet_path, variant, gen, key, remote))
        return {"path": 26516, "coarse16": 3}[variant]

    monkeypatch.setattr(F, "sync_d1", fake_sync_d1)
    return calls


@pytest.fixture
def logged(monkeypatch) -> list[str]:
    """`err` binds stderr at import time (neither CliRunner's swap nor
    capsys sees it), so record what the command logs instead."""
    lines: list[str] = []
    monkeypatch.setattr(C, "err", lambda *a: lines.append(" ".join(map(str, a))))
    return lines


def test_index_sync_skips_absent_variants(tmp_path: Path, synced, logged):
    for name in ("path-index.parquet", "path-index-coarse16.parquet"):
        (tmp_path / name).write_bytes(b"")
    r = CliRunner().invoke(main, ["index-sync", "-d", str(tmp_path), "-g", "G1", "-k", "listing/2026-09-30/index/G1", "2026-09-30"])
    assert r.exit_code == 0, r.output
    assert synced == [
        ("2026-09-30", f"{tmp_path}/path-index.parquet", "path", "G1", "listing/2026-09-30/index/G1", True),
        ("2026-09-30", f"{tmp_path}/path-index-coarse16.parquet", "coarse16", "G1", "listing/2026-09-30/index/G1", True),
    ]
    assert logged == [
        "index-sync: 2026-09-30 [path] gen G1 @ listing/2026-09-30/index/G1 — 26516 row groups (remote)",
        "index-sync: 2026-09-30 [coarse16] gen G1 @ listing/2026-09-30/index/G1 — 3 row groups (remote)",
        "index-sync: 2026-09-30 gen G1 @ listing/2026-09-30/index/G1 — skipped 15 absent variant(s): "
        "user, coarse16-user, coarse20, coarse20-user, coarse24, coarse24-user, "
        "age-pyramid-1h, age-pyramid-3h, age-pyramid-6h, age-pyramid-12h, age-pyramid-1d, age-pyramid-2d, age-pyramid-4d, age-pyramid-8d, "
        "over-time",
    ]


def test_index_sync_fails_when_nothing_is_there(tmp_path: Path, synced, logged):
    r = CliRunner().invoke(main, ["index-sync", "-d", str(tmp_path), "-g", "G1", "-v", "path", "-v", "user", "2026-09-30"])
    assert r.exit_code == 1
    assert synced == []
    assert logged == [
        "index-sync: 2026-09-30 gen G1 @ listing/2026-09-30/index/G1 — skipped 2 absent variant(s): path, user",
        f"index-sync: no variant file under {tmp_path}",
    ]
