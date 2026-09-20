"""Index extras (specs/index-extras.md): sidecars beside a scan's index
tiers, written by ``webdata`` for a fresh scan and by ``dt-cloud
index-extras`` as the backfill for archived generations.

- ``ck.txt``:   the checkpoint-shaped directories — computed over each dir's
                FULL child list, which the site's pixel-budgeted subtree never
                has — so "keep last ckpt" is offered exactly where it applies.
- ``attr.tsv``: every attributing prefix → ``user  source  evidence`` —
                the provenance of inferred ownership, which the path index
                drops on the way to its ``usr`` column.

Both are **sorted by key, one line per entry, with a block index**
(``<name>.idx.json``: the first key and byte offset of every ~64 KB block), so
the site fetches just the byte range covering a view's subtree — a bucket's
worth of checkpoint dirs is hundreds of thousands of lines, far past what a
worker isolate should parse whole.

Keys are index paths: ``<bucket>/<dir>/…`` (no ``gs://``, no trailing slash).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from utz import err

if TYPE_CHECKING:
    import duckdb
    import pandas as pd

# Mirrors the site's `looksCkpt` / `CKPT_SEG_RE` (site/src/sweep.ts). The
# sidecar carries only what a name can't tell: dirs with a direct child
# named exactly `checkpoints`/`ckpts` (a run dir) or ≥ 2 step-numbered
# children. A dir whose OWN name says checkpoint is left out — the reader
# applies that rule itself (`CKPT_NAME_RE`). The child match is the whole
# segment, not a substring: eval-output dirs are named after checkpoint
# paths (`gs__…__checkpoints__…__step-600`) and a substring match flagged
# 250k of them on the first run.
CKPT_NAME_RE = r"(^|[-_.])(ckpts?|checkpoints?)([-_.]|$)"
CKPT_DIR_RE = r"^(ckpts?|checkpoints?)$"
CKPT_SEG_RE = r"^(step|checkpoint|ckpt|iter|epoch|global_?step)[-_]?\d+"

CK_FILE = "ck.txt"
ATTR_FILE = "attr.tsv"
BLOCK = 64 * 1024


def write_blocked(path: Path, lines: list[str], key_of=lambda line: line) -> dict[str, int]:
    """Write ``lines`` (already sorted by key) as one text file plus a block
    index ``<path>.idx.json`` = ``{"v":1, "n": lines, "size": bytes,
    "keys": [first key per block], "offsets": [byte offset per block]}``.
    Blocks are cut at line boundaries at or past every ``BLOCK`` bytes."""
    keys: list[str] = []
    offsets: list[int] = []
    off = 0
    next_cut = 0
    with path.open("wb") as f:
        for line in lines:
            if off >= next_cut:
                keys.append(key_of(line))
                offsets.append(off)
                next_cut = off + BLOCK
            b = (line + "\n").encode()
            f.write(b)
            off += len(b)
    idx = {"v": 1, "n": len(lines), "size": off, "keys": keys, "offsets": offsets}
    path.with_name(path.name + ".idx.json").write_text(json.dumps(idx, separators=(",", ":")) + "\n")
    return {"lines": len(lines), "bytes": off, "blocks": len(keys)}


def ckpt_dirs(con: "duckdb.DuckDBPyConnection", dirs_sql: str) -> list[str]:
    """Dirs the child rules flag (see above), from a relation with an ``fp``
    column (one row per dir, ``bucket/a/b``; duplicates are fine). Sorted."""
    rows = con.execute(
        f"""
        WITH d AS (SELECT DISTINCT fp FROM {dirs_sql} WHERE fp IS NOT NULL AND position('/' IN fp) > 0),
        named AS (SELECT regexp_replace(fp, '/[^/]*$', '') AS parent, regexp_extract(fp, '[^/]*$') AS name FROM d)
        SELECT parent AS fp FROM named
        GROUP BY parent
        HAVING bool_or(regexp_matches(name, ?, 'i')) OR count_if(regexp_matches(name, ?, 'i')) >= 2
        ORDER BY parent
        """,
        [CKPT_DIR_RE, CKPT_SEG_RE],
    ).fetchall()
    return [fp for (fp,) in rows]


def attr_map(pfx_df: "pd.DataFrame") -> dict[str, list]:
    """``key → [user, source, evidence]`` from the prefix-label frame
    (``prefix_labels``; ``evidence`` when the frame carries it)."""
    has_ev = "evidence" in pfx_df.columns
    out: dict[str, list] = {}
    for row in pfx_df.itertuples(index=False):
        ev = getattr(row, "evidence", None) if has_ev else None
        out[row.key] = [row.user, getattr(row, "source", None), None if ev is None or ev != ev else ev]
    return out


def write_extras(
    con: "duckdb.DuckDBPyConnection",
    dirs_sql: str,
    pfx_df: "pd.DataFrame | None",
    out_dir: Path,
) -> dict[str, int]:
    """Write ``ck.txt`` (+ index) and, given a prefix frame, ``attr.tsv`` (+
    index) into ``out_dir``; returns entry counts."""
    out_dir.mkdir(parents=True, exist_ok=True)
    ck = ckpt_dirs(con, dirs_sql)  # ORDER BY fp — sorted
    write_blocked(out_dir / CK_FILE, ck)
    counts = {"ck": len(ck)}
    if pfx_df is not None:
        attr = attr_map(pfx_df)
        rows = [f"{k}\t{u}\t{src or ''}\t{ev or ''}" for k, (u, src, ev) in sorted(attr.items())]
        write_blocked(out_dir / ATTR_FILE, rows, key_of=lambda line: line.split("\t", 1)[0])
        counts["attr"] = len(attr)
    err(f"extras: {out_dir}: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
    return counts
