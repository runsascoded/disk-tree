import json
from pathlib import Path

import duckdb
import pandas as pd

from dt_cloud.extras import ATTR_FILE, CK_FILE, ckpt_dirs, write_blocked, write_extras

DIRS = [
    "b", "b/grug",
    "b/grug/run1", "b/grug/run1/checkpoints", "b/grug/run1/checkpoints/step-100", "b/grug/run1/checkpoints/step-200", "b/grug/run1/hf",
    "b/grug/run2", "b/grug/run2/step-1", "b/grug/run2/step-2", "b/grug/run2/eval",
    "b/grug/run3", "b/grug/run3/step-1", "b/grug/run3/logs",  # one step child: not checkpoint-shaped
    "b/data", "b/data/x",
    "b/ckpts", "b/ckpts/a",
    "b/eval", "b/eval/gs__marin__checkpoints__run__step-600",  # named after a checkpoint path; not one
]


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE d(fp VARCHAR)")
    con.executemany("INSERT INTO d VALUES (?)", [(d,) for d in DIRS])
    return con


def test_ckpt_dirs_rule():
    # A `checkpoints`/`ckpts` child (`b`, `run1`), ≥ 2 step children (`run1/checkpoints`, `run2`).
    # `b/ckpts` (own name only) is the reader's call; `run3` has one step child; `b/eval` is a
    # substring hit that must NOT count.
    assert ckpt_dirs(_con(), "d") == ["b", "b/grug/run1", "b/grug/run1/checkpoints", "b/grug/run2"]


def test_write_extras(tmp_path: Path):
    pfx = pd.DataFrame([
        {"key": "b/grug", "user": "calvin", "source": "wandb-run"},
        {"key": "b/data", "user": "ryan", "source": "manual"},
    ])
    assert write_extras(_con(), "d", pfx, tmp_path) == {"ck": 4, "attr": 2}
    assert (tmp_path / CK_FILE).read_text() == "b\nb/grug/run1\nb/grug/run1/checkpoints\nb/grug/run2\n"
    assert (tmp_path / ATTR_FILE).read_text() == "b/data\tryan\tmanual\t\nb/grug\tcalvin\twandb-run\t\n"
    assert json.loads((tmp_path / (CK_FILE + ".idx.json")).read_text()) == {"v": 1, "n": 4, "size": 50, "keys": ["b"], "offsets": [0]}
    assert json.loads((tmp_path / (ATTR_FILE + ".idx.json")).read_text()) == {"v": 1, "n": 2, "size": 45, "keys": ["b/data"], "offsets": [0]}


def test_write_extras_without_attribution(tmp_path: Path):
    assert write_extras(_con(), "d", None, tmp_path) == {"ck": 4}
    assert sorted(p.name for p in tmp_path.iterdir()) == [CK_FILE, CK_FILE + ".idx.json"]


def test_write_blocked_cuts_at_line_boundaries(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("dt_cloud.extras.BLOCK", 10)
    lines = ["a/1", "a/2", "b/1", "b/2", "c/1"]  # 4 bytes each with the newline
    assert write_blocked(tmp_path / "x.txt", lines) == {"lines": 5, "bytes": 20, "blocks": 2}
    # block 0 spans a/1..b/1 (0–12 ≥ 10 → cut before b/2), block 1 the rest
    assert json.loads((tmp_path / "x.txt.idx.json").read_text()) == {"v": 1, "n": 5, "size": 20, "keys": ["a/1", "b/2"], "offsets": [0, 12]}
