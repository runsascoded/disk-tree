"""Extract a parquet's footer as hyparquet-compatible JSON (schema + per-row-group
column metadata) and sync it to the site's D1, so the Cloudflare reader can build
a subset ``FileMetaData`` for a prefix query without parsing the whole footer on a
cold isolate (specs/path-agnostic-serving.md §2.1 — the footer-in-D1 seam).

Only the fields a *read* needs are kept, in the compact form the reader revives
(`reviveRowGroup` in `site/functions/_lib/index.ts`):

    rg_json = [num_rows, codec, [[data_page_offset, total_compressed_size, dictionary_page_offset|0], …]]

one triple per leaf column in schema order — hyparquet reads nothing else from a
column chunk (physical type and `path_in_schema` come from `index_schema`). Per
group, the (depth, path, bytes, usr) min/max sit in their own columns for the
row-group pruning SQL. ~250 B per group: D1 holds every scan's every tier (2M+
groups) under its 10 GB cap, where the verbose thrift-shaped JSON (~3 KB) hit
8.4 GB at 38 scans (2026-09-06).

Generations (specs/view-serving.md, "Index rewrite vs D1 footer"): a run never
overwrites a parquet D1 points at. Each run writes its tiers under a fresh
``listing/<date>/index/<gen>/`` and syncs them tagged with that ``gen``
(``index_row_groups`` PK is (date, variant, gen, rg), so two generations
coexist); the ``index_schema`` row — (gen, dir) per (date, variant) — is the
pointer, written last as one ``INSERT OR REPLACE`` so readers flip from the old
complete set to the new complete set with no window. Stale generations are
swept by ``gc_d1`` (end of the job) and at the start of the next sync.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from .secrets import env_secret

# Physical/logical strings pyarrow emits already match parquet-thrift (and thus
# hyparquet) — no remapping table needed; we just restructure.


def _site_dir() -> Path:
    """Nearest ancestor ``site/`` holding wrangler.toml (repo layout)."""
    for base in [Path.cwd(), *Path.cwd().parents, Path(__file__).resolve().parents[3]]:
        cand = base / "site"
        if (cand / "wrangler.toml").exists():
            return cand
    raise FileNotFoundError("site/wrangler.toml not found (run from the repo)")


def _schema_json(md: "pq.FileMetaData") -> dict:
    """hyparquet `FileMetaData.schema` (root element + one leaf per column)."""
    sch = md.schema
    root = {"repetition_type": "REQUIRED", "name": sch.to_arrow_schema().pandas_metadata and "schema" or "schema", "num_children": md.num_columns}
    # The Arrow name of the root isn't load-bearing (reads key off leaf
    # path_in_schema); use a stable placeholder.
    root["name"] = "schema"
    leaves = []
    for i in range(md.num_columns):
        col = sch.column(i)
        el: dict = {"type": col.physical_type, "repetition_type": "OPTIONAL" if col.max_definition_level else "REQUIRED", "name": col.name}
        ct = col.converted_type
        if ct and ct != "NONE":
            el["converted_type"] = ct
        leaves.append(el)
    return {"version": 1, "schema": [root, *leaves]}


def _group_rows(md: "pq.FileMetaData") -> list[dict]:
    """One row per row group: pruning stats + the stripped RowGroup JSON."""
    rows = []
    row_start = 0
    names = md.schema.names
    di = names.index("depth")
    pi = names.index("path")
    bi = names.index("b")
    # The age index (variant `age`) has no ownership column; its `u_min/u_max`
    # stay NULL, and the (depth, path) rectangle prunes it like any other tier.
    ui = names.index("usr") if "usr" in names else None

    def _srange(stats) -> tuple:
        # (min, max) of a string column's stats, or (None, None) when absent
        # (an all-NULL group has no min/max). Bytes → str for JSON.
        if stats is None or not stats.has_min_max:
            return None, None
        def s(v):
            return v.decode() if isinstance(v, bytes) else v
        return s(stats.min), s(stats.max)

    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        n = rg.num_rows
        codecs = {rg.column(c).compression for c in range(rg.num_columns)}
        if len(codecs) != 1:
            raise ValueError(f"row group {g}: mixed codecs {sorted(codecs)} (one codec per group assumed)")
        cols = [
            [cc.data_page_offset, cc.total_compressed_size, cc.dictionary_page_offset or 0]
            for cc in (rg.column(c) for c in range(rg.num_columns))
        ]
        ds, ps, bs = rg.column(di).statistics, rg.column(pi).statistics, rg.column(bi).statistics
        u_min, u_max = _srange(rg.column(ui).statistics) if ui is not None else (None, None)
        rows.append({
            "rg": g,
            "d_min": int(ds.min), "d_max": int(ds.max),
            "p_min": ps.min, "p_max": ps.max,
            "b_max": int(bs.max),
            "u_min": u_min, "u_max": u_max,
            "row_start": row_start, "row_end": row_start + n,
            "rg_json": json.dumps([n, codecs.pop(), cols], separators=(",", ":")),
        })
        row_start += n
    return rows


def extract(parquet_path: str) -> tuple[dict, list[dict]]:
    """Return (schema_meta, group_rows) from a local or fsspec-readable parquet."""
    import gcsfs

    opener = gcsfs.GCSFileSystem().open if parquet_path.startswith(("gs://", "oa-")) else open
    with opener(parquet_path, "rb") as f:
        md = pq.ParquetFile(f).metadata
    schema = _schema_json(md)
    # A coarse tier records its absolute floor F in the parquet key-value
    # metadata (viz.py COARSE_EXP); it lands in D1 beside the schema so the
    # reader can plan tiers without touching the file (view-serving.md §1).
    kv = md.metadata or {}
    if b"coarse_floor" in kv:
        schema["floor_bytes"] = int(kv[b"coarse_floor"])
    return schema, _group_rows(md)


GROUPS_BLOB_SUFFIX = ".groups.json"
GROUPS_BLOB_VERSION = 1


def groups_blob_path(parquet_path: str) -> str:
    """The group-manifest blob beside a tier: `…/path-index-by-user.parquet` →
    `…/path-index-by-user.groups.json`."""
    if not parquet_path.endswith(".parquet"):
        raise ValueError(f"not a parquet path: {parquet_path}")
    return parquet_path[: -len(".parquet")] + GROUPS_BLOB_SUFFIX


def groups_blob(schema: dict, rows: list[dict]) -> str:
    """The blob's JSON: what `sync_d1` puts in `index_schema` + `index_row_groups`
    for one tier, as one document — the site's fallback for a scan whose row
    groups retention retired from D1 (`_lib/index.ts` `openBlob`). Groups are
    compact arrays in `index_row_groups` column order; `rg_json` stays the
    string the reader revives."""
    groups = [
        [r["rg"], r["d_min"], r["d_max"], r["p_min"], r["p_max"], r["b_max"], r["u_min"], r["u_max"], r["row_start"], r["row_end"], r["rg_json"]]
        for r in rows
    ]
    body = {"v": GROUPS_BLOB_VERSION, "version": schema["version"], "schema": schema["schema"], "floor_bytes": schema.get("floor_bytes"), "groups": groups}
    return json.dumps(body, separators=(",", ":"))


def write_groups_blob(parquet_path: str, schema: dict, rows: list[dict]) -> tuple[str, int]:
    """Write the group-manifest blob beside ``parquet_path`` (local, mounted,
    or `gs://`); returns (path, bytes)."""
    out = groups_blob_path(parquet_path)
    text = groups_blob(schema, rows)
    if out.startswith(("gs://", "oa-")):
        import gcsfs

        with gcsfs.GCSFileSystem().open(out, "w") as f:
            f.write(text)
    else:
        with open(out, "w") as f:
            f.write(text)
    return out, len(text)


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


# The D1 database `/query` runs one SQL string; we send multi-row INSERTs.
# Deployment config (specs/denovo-factor.md): the site's D1, as `site/wrangler.toml`
# binds it — `D1_DB_ID` / `D1_DB_NAME` in the job's environment (`job/cw-run.sh`
# exports the CoreWeave pair); the defaults are the GCS deployment's.
D1_DB_ID = os.environ.get("D1_DB_ID", "e52398b7-5538-4bc4-83db-3355a1b5ef9a")  # oa-gcs-usage-auth
D1_DB_NAME = os.environ.get("D1_DB_NAME", "oa-gcs-usage-auth")


def _creds() -> tuple[str, str]:
    """(api_token, account_id) from the env, falling back to the repo .envrc —
    so it works in the Batch job (env) and from a laptop (direnv/.envrc)."""
    tok = env_secret("CLOUDFLARE_API_TOKEN", "")
    acct = os.environ.get("CLOUDFLARE_ACCOUNT_ID") or os.environ.get("OA_CF_ACCT", "")
    if not (tok and acct):
        try:
            envrc = _site_dir().parent / ".envrc"
            for line in envrc.read_text().splitlines():
                m = re.match(r"^\s*export\s+(CLOUDFLARE_API_TOKEN|OA_CF_ACCT)=[\"']?([^\"'\s]+)", line)
                if m:
                    if m.group(1) == "CLOUDFLARE_API_TOKEN" and not tok:
                        tok = m.group(2)
                    if m.group(1) == "OA_CF_ACCT" and not acct:
                        acct = m.group(2)
        except FileNotFoundError:
            pass
    if not (tok and acct):
        raise RuntimeError("need CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID (or OA_CF_ACCT)")
    return tok, acct


# Transient-error retries for the D1 HTTP API: the 2026-09-01 daily run's
# index-sync 401'd on the [team] variant after the same token had just synced
# [path] and [user] — CF's API throws occasional spurious 401/5xx under
# sustained load. Retried statuses, attempts, and base sleep (doubles per try).
D1_RETRY_STATUSES = (401, 429, 500, 502, 503, 504)
D1_RETRIES = 4
D1_RETRY_SLEEP = 5.0
# Per-request wall clock. A request that never answers (observed 2026-09-06:
# the `[team]` sync of one REPROC went silent for an hour while a concurrent
# job's syncs completed) must fail into the retry loop, not hang the job to
# its maxRunDuration. A ~64 KB INSERT answers in ~1 s; 60 s is generous.
D1_TIMEOUT = 60.0
# Bytes of SQL per multi-row INSERT (D1 caps a statement at 100 KB): ~100
# compact group rows per request, so a 27k-group tier syncs in ~270 requests.
INSERT_BYTES = 64_000


def _d1_query(sql: str, acct: str, tok: str, db_id: str = D1_DB_ID) -> list[dict]:
    """Run one SQL string against D1 over the HTTP API (no Node/wrangler).
    Returns the statement's result rows (`[]` for writes); `meta` per row batch
    is dropped."""
    import time
    import urllib.error
    import urllib.request

    url = f"https://api.cloudflare.com/client/v4/accounts/{acct}/d1/database/{db_id}/query"
    for attempt in range(D1_RETRIES + 1):
        req = urllib.request.Request(
            url,
            data=json.dumps({"sql": sql}).encode(),
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            resp = json.loads(urllib.request.urlopen(req, timeout=D1_TIMEOUT).read())
        except urllib.error.HTTPError as e:
            if e.code in D1_RETRY_STATUSES and attempt < D1_RETRIES:
                time.sleep(D1_RETRY_SLEEP * 2**attempt)
                continue
            # Surface D1's error body (scope/SQL) without echoing the token.
            raise RuntimeError(f"D1 query failed ({e.code}): {e.read().decode()[:300]}") from None
        except (urllib.error.URLError, TimeoutError) as e:  # no answer / no route
            if attempt < D1_RETRIES:
                time.sleep(D1_RETRY_SLEEP * 2**attempt)
                continue
            raise RuntimeError(f"D1 query gave no answer after {D1_RETRIES + 1} tries: {e}") from None
        if not resp.get("success"):
            raise RuntimeError(f"D1 query error: {resp.get('errors')}")
        return [row for r in resp.get("result", []) for row in r.get("results", [])]
    raise AssertionError("unreachable")


def _q(v) -> str:  # nullable string literal for SQL
    return "NULL" if v is None else f"'{_sql_escape(v)}'"


def sync_d1(
    date: str,
    parquet_path: str,
    *,
    variant: str = "path",
    gen: str,
    key: str,
    db_id: str = D1_DB_ID,
    remote: bool = True,
    insert_bytes: int = INSERT_BYTES,
    blob: bool = True,
) -> int:
    """Extract the footer of ``parquet_path`` (one tier/sort ``variant`` of
    scan ``date``, generation ``gen``, living under the bucket-relative dir
    ``key``) and publish it to D1 (index_row_groups + index_schema) over the
    Cloudflare **HTTP API** — pure Python, so it runs in the Node-less Batch
    image. Returns #row groups written. ``remote=False`` uses the local wrangler
    D1 (dev only, via `d1 execute`).

    Order matters for atomicity: every group row (tagged ``gen``) lands first;
    the schema row — the pointer readers key off — is written LAST, as one
    ``INSERT OR REPLACE``, so a reader sees either the previous complete
    generation or this one, never a half-written set. A mid-run failure leaves
    the pointer untouched (still serving the previous generation) and orphan
    rows the next sync/gc sweeps. Nothing is deleted before the flip.

    The same rows also land as the group-manifest blob beside the parquet
    (``write_groups_blob``) first — the durable copy the site opens once
    retention retires this tier's rows from D1."""
    schema, rows = extract(parquet_path)
    if blob:
        write_groups_blob(parquet_path, schema, rows)
    floor = schema.get("floor_bytes")
    # Leftovers from earlier flips (any gen that is neither the current pointer's
    # nor this one) go first — they are unreachable by construction.
    gc_sql = (
        f"DELETE FROM index_row_groups WHERE date='{date}' AND variant='{variant}' AND gen <> '{_sql_escape(gen)}' "
        f"AND gen <> COALESCE((SELECT gen FROM index_schema WHERE date='{date}' AND variant='{variant}'), '');"
    )
    schema_sql = (
        "INSERT OR REPLACE INTO index_schema (date, variant, version, schema_json, floor_bytes, gen, dir) VALUES "
        f"('{date}', '{variant}', {schema['version']}, '{_sql_escape(json.dumps(schema['schema'], separators=(',', ':')))}', "
        f"{'NULL' if floor is None else int(floor)}, '{_sql_escape(gen)}', '{_sql_escape(key)}');"
    )

    def group_values(r: dict) -> str:
        return (
            f"('{date}', '{variant}', '{_sql_escape(gen)}', {r['rg']}, {r['d_min']}, {r['d_max']}, "
            f"'{_sql_escape(r['p_min'])}', '{_sql_escape(r['p_max'])}', {r['b_max']}, "
            f"{_q(r['u_min'])}, {_q(r['u_max'])}, "
            f"{r['row_start']}, {r['row_end']}, '{_sql_escape(r['rg_json'])}')"
        )

    cols = "(date, variant, gen, rg, d_min, d_max, p_min, p_max, b_max, u_min, u_max, row_start, row_end, rg_json)"
    # OR REPLACE: a chunk whose request timed out may or may not have landed;
    # re-sending it must be a no-op, not a PK collision (date, variant, gen, rg).
    head = f"INSERT OR REPLACE INTO index_row_groups {cols} VALUES "
    if not remote:  # dev: local wrangler D1
        stmts = [gc_sql] + [f"{head}{group_values(r)};" for r in rows] + [schema_sql]
        site = _site_dir()
        for i in range(0, len(stmts), 300):
            with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as tf:
                tf.write("\n".join(stmts[i : i + 300]))
                sqlpath = tf.name
            subprocess.run(["npx", "wrangler", "d1", "execute", D1_DB_NAME, "--local", "--file", sqlpath], check=True, cwd=str(site))
        return len(rows)

    tok, acct = _creds()
    _d1_query(gc_sql, acct, tok, db_id)
    for chunk in _pack(head, [group_values(r) for r in rows], insert_bytes):
        _d1_query(chunk, acct, tok, db_id)
    _d1_query(schema_sql, acct, tok, db_id)  # the pointer flip: schema row last
    return len(rows)


def gc_d1(date: str, db_id: str = D1_DB_ID) -> int:
    """Delete every row group of ``date`` whose generation is not the one its
    variant's schema row points at (leftovers of a flip, or of a sync that
    failed before flipping). Returns rows deleted. Safe any time: readers only
    ever query the pointer's generation, and a handle outlives the pointer by
    at most its cache TTL (`_lib/index.ts`), which the end-of-job call clears."""
    tok, acct = _creds()
    rows = _d1_query(
        "DELETE FROM index_row_groups WHERE date = '{d}' AND gen <> COALESCE("
        "(SELECT s.gen FROM index_schema s WHERE s.date = index_row_groups.date AND s.variant = index_row_groups.variant), '') "
        "RETURNING 1 AS n;".format(d=_sql_escape(date)),
        acct, tok, db_id,
    )
    return len(rows)


# The floor-free (uncoarsened) tiers `index-gc -r` retires: the deployment's
# variant set minus the coarse ones (cw has no user-sorted variant).
FLOOR_FREE_VARIANTS = tuple(v for v in os.environ.get("INDEX_VARIANTS", "path,user").split(",") if v)

# Index variants the site reads (functions/_lib/index.ts `fileFor` mirrors this):
# each floor-free tier (`path`, and `user` where the deployment writes it) and
# every coarse tier (viz.py COARSE_EXPS) in the same sorts. D1 keys (date, variant).
COARSE_EXPS = (16, 20, 24)
INDEX_VARIANTS: dict[str, str] = {"path": "path-index.parquet"}
if "user" in FLOOR_FREE_VARIANTS:
    INDEX_VARIANTS["user"] = "path-index-by-user.parquet"
for _e in COARSE_EXPS:
    INDEX_VARIANTS[f"coarse{_e}"] = f"path-index-coarse{_e}.parquet"
    if "user" in FLOOR_FREE_VARIANTS:
        INDEX_VARIANTS[f"coarse{_e}-user"] = f"path-index-coarse{_e}-by-user.parquet"
# The age chart's backend: multi-scale path-major pyramid tiers, one per bin
# (specs/age-index.md, Phase B — supersedes the single-bin `age-index.parquet`).
# Standalone indexes, own base names; the footer's (depth, path, b) stats prune
# them as usual, `usr` absent (u_min/u_max NULL). Keep in sync with
# `AGE_PYRAMID_VARIANTS` in dt_cloud.index.
for _b in ("1h", "3h", "6h", "12h", "1d", "2d", "4d", "8d"):
    INDEX_VARIANTS[f"age-pyramid-{_b}"] = f"age-pyramid-{_b}.parquet"
# The cross-scan over-time index (specs/obs-axis-indexing.md Phase 1): a single
# SCD-2 interval table over the *observation* axis, not per-scan. Its (depth,
# path, b) stats prune like any tier; `usr` absent, the extra `__scan_lo/hi`
# columns carry no stats the reader needs. `dt_cloud.overtime.OVER_TIME_*`.
INDEX_VARIANTS["over-time"] = "over-time.parquet"


def retire_d1(retain: int, db_id: str = D1_DB_ID) -> list[tuple[str, str, int]]:
    """Retention (specs/view-serving.md follow-ups): drop the floor-free
    variants' row groups for every synced scan older than the newest
    ``retain`` — they are 95 % of D1's index bytes (~27k groups per variant per
    scan) and a deep drill into an old scan is rare. The pointer stays, so the
    reader serves those from the parquet footer (slow path); the coarse tiers,
    which answer everything above the floor, are kept for every scan. Returns
    (date, variant, rows deleted) per retired variant."""
    tok, acct = _creds()
    dates = sorted({d for d, _ in synced_variants(db_id)})
    out: list[tuple[str, str, int]] = []
    for d in dates[:-retain] if retain > 0 else dates:
        for v in FLOOR_FREE_VARIANTS:
            rows = _d1_query(
                f"DELETE FROM index_row_groups WHERE date = '{_sql_escape(d)}' AND variant = '{v}' RETURNING 1 AS n;",
                acct, tok, db_id,
            )
            if rows:
                out.append((d, v, len(rows)))
    return out


def index_dir(date: str, variant: str = "path", db_id: str = D1_DB_ID) -> str | None:
    """Bucket-relative dir holding ``date``'s ``variant`` parquet — the D1
    pointer (``index_schema.dir``); None when that (date, variant) was never
    synced. The parquet is ``<dir>/<INDEX_VARIANTS[variant]>``."""
    tok, acct = _creds()
    rows = _d1_query(
        f"SELECT dir FROM index_schema WHERE date = '{_sql_escape(date)}' AND variant = '{_sql_escape(variant)}';",
        acct, tok, db_id,
    )
    return rows[0]["dir"] if rows else None


def _pack(head: str, values: list[str], limit: int) -> list[str]:
    """Greedy multi-row INSERT statements: `head` + comma-joined `values` + `;`,
    each as long as fits in `limit` bytes (a lone oversized tuple still ships)."""
    out: list[str] = []
    cur: list[str] = []
    size = len(head) + 1
    for v in values:
        if cur and size + len(v) + 1 > limit:
            out.append(head + ",".join(cur) + ";")
            cur, size = [], len(head) + 1
        cur.append(v)
        size += len(v) + 1
    if cur:
        out.append(head + ",".join(cur) + ";")
    return out


# In-place rewrite of rows still holding the verbose (pre-2026-09-06) thrift-shaped
# `rg_json` object into the compact array form, with SQLite's JSON1 — no parquet
# read, one statement per (date, variant). Old rows start with `{`.
COMPACT_SQL = (
    "UPDATE index_row_groups SET rg_json = json_array("
    "CAST(json_extract(rg_json, '$.num_rows') AS INTEGER), "
    "json_extract(rg_json, '$.columns[0].meta_data.codec'), "
    "(SELECT json_group_array(json_array("
    "CAST(json_extract(value, '$.meta_data.data_page_offset') AS INTEGER), "
    "CAST(json_extract(value, '$.meta_data.total_compressed_size') AS INTEGER), "
    "CAST(coalesce(json_extract(value, '$.meta_data.dictionary_page_offset'), '0') AS INTEGER))) "
    "FROM json_each(index_row_groups.rg_json, '$.columns'))"
    ") WHERE date = '{date}' AND variant = '{variant}' AND rg_json LIKE '{{%';"
)


def synced_variants(db_id: str = D1_DB_ID) -> list[tuple[str, str]]:
    """Every (date, variant) with a schema row in D1 (= a complete sync)."""
    tok, acct = _creds()
    rows = _d1_query("SELECT date, variant FROM index_schema ORDER BY date, variant;", acct, tok, db_id)
    return [(r["date"], r["variant"]) for r in rows]


def compact_d1(date: str, variant: str, db_id: str = D1_DB_ID) -> int:
    """Compact one (date, variant)'s verbose `rg_json` rows in place; returns the
    number of rows left in the old form afterwards (0 = done)."""
    tok, acct = _creds()
    _d1_query(COMPACT_SQL.format(date=date, variant=variant), acct, tok, db_id)
    rows = _d1_query(
        f"SELECT count(*) AS n FROM index_row_groups WHERE date = '{date}' AND variant = '{variant}' AND rg_json LIKE '{{%';",
        acct, tok, db_id,
    )
    return int(rows[0]["n"])
