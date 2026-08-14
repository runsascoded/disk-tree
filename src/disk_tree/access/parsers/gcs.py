"""GCS usage-log parser (layer-1a raw → canonical).

Google delivers hourly CSVs to a bucket you designate. Filename convention:
``<prefix>_usage_YYYY_MM_DD_HH_MM_SS_<id>_v0`` (usage) or ``_storage_`` (daily
byte-hours). This module handles the former; the latter is a bonus stats
table (bucket-level, no path attribution) parsed by
:func:`parse_storage_daily`.

Header row is present. Fields we care about (per
`Google's docs
<https://cloud.google.com/storage/docs/access-logs#format>`_):

- ``time_micros`` — request time (usec since epoch)
- ``c_ip`` — client IP (best requester ID we get without extra work)
- ``cs_method`` — HTTP method (GET/PUT/DELETE/HEAD)
- ``cs_uri`` — request URI (includes query string)
- ``sc_status`` — response status
- ``cs_bytes`` / ``sc_bytes`` — request/response body sizes
- ``cs_user_agent``
- ``cs_operation`` — Google's own verb (``GET_Object``, ``LIST_Bucket``, …)
- ``cs_bucket`` / ``cs_object`` — bucket + key targeted
- ``s_request_id`` — Google's request id (used for dedupe: Google documents
  rare duplicate log lines; deduping on this field is idempotent).

The parser returns a DuckDB relation exposing the canonical
:data:`~disk_tree.access.schema.ACCESS_COLUMNS`. Callers can materialize
to parquet with ``rel.write_parquet(path)`` or feed it directly to
:mod:`disk_tree.access.aggregate`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def parse(input_glob: str, store: str = 'gcs', con: "duckdb.DuckDBPyConnection | None" = None) -> "duckdb.DuckDBPyRelation":
    """Parse GCS usage-log CSVs matching ``input_glob`` → canonical relation.

    Dedupes on ``s_request_id`` (Google documents rare duplicate log lines).
    The output relation is lazy — no I/O until you materialize or aggregate.
    """
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()

    # `read_csv` with a header-inferring load + explicit type hints so we can
    # ingest arbitrary GCS bucket paths (gs://…), local globs, or fsspec URLs.
    # `union_by_name=true` handles the (unlikely but possible) case where
    # Google's field set drifts across days.
    con.execute(f"""
        CREATE OR REPLACE VIEW _gcs_usage_raw AS
        SELECT *
        FROM read_csv(
            {_sql_literal(input_glob)},
            header=true,
            filename=true,
            union_by_name=true
        )
    """)

    # Canonical projection + normalization. Path derived from cs_object
    # (already the object key); bucket from cs_bucket. `cs_operation` beats
    # `cs_method` for the normalized op (matches schema.normalize_op).
    return con.sql(f"""
        WITH deduped AS (
            SELECT * FROM _gcs_usage_raw
            QUALIFY row_number() OVER (PARTITION BY s_request_id) = 1
        )
        SELECT
            to_timestamp(time_micros / 1e6) AS ts,
            '{store}'::VARCHAR AS store,
            cs_bucket::VARCHAR AS bucket,
            COALESCE(cs_object, '')::VARCHAR AS path,
            {_normalize_op_sql} AS op,
            (COALESCE(cs_operation, cs_method, ''))::VARCHAR AS op_raw,
            sc_status::INTEGER AS status,
            COALESCE(sc_bytes, 0)::BIGINT AS bytes_out,
            COALESCE(cs_bytes, 0)::BIGINT AS bytes_in,
            COALESCE(c_ip, '')::VARCHAR AS requester,
            COALESCE(cs_user_agent, '')::VARCHAR AS user_agent,
            s_request_id::VARCHAR AS request_id
        FROM deduped
        WHERE cs_bucket IS NOT NULL
    """)


def parse_storage_daily(input_glob: str, con: "duckdb.DuckDBPyConnection | None" = None) -> "duckdb.DuckDBPyRelation":
    """Parse the daily ``_storage_`` files (bucket byte-hours) → simple relation.

    Not a request log — one row per bucket per day with storage-class byte
    totals. Useful for cost reconciliation but distinct from the access-log
    plane; treat as a bonus.
    """
    import duckdb as _duckdb
    if con is None:
        con = _duckdb.connect()
    return con.sql(f"""
        SELECT * FROM read_csv({_sql_literal(input_glob)}, header=true, union_by_name=true)
    """)


# SQL fragment: fold GCS's cs_operation/cs_method into the canonical op
# vocabulary. Matches disk_tree.access.schema.normalize_op semantics exactly
# so parser output === Python-normalized output. Any drift here would violate
# the cross-parser identity contract.
_normalize_op_sql = """
CASE
    WHEN COALESCE(cs_operation, '') LIKE 'LIST%' OR COALESCE(cs_operation, '') LIKE '%_BUCKET' THEN 'LIST'
    WHEN COALESCE(cs_operation, '') LIKE 'GET%' OR cs_method = 'GET' THEN 'GET'
    WHEN COALESCE(cs_operation, '') LIKE 'PUT%' OR cs_method = 'PUT'
         OR COALESCE(cs_operation, '') IN ('POST_Object', 'POST_Uploads') THEN 'PUT'
    WHEN COALESCE(cs_operation, '') LIKE 'HEAD%' OR cs_method = 'HEAD' THEN 'HEAD'
    WHEN COALESCE(cs_operation, '') LIKE 'DELETE%' OR cs_method = 'DELETE' THEN 'DELETE'
    ELSE 'OTHER'
END::VARCHAR
"""


def _sql_literal(s: str) -> str:
    """Safely embed a string literal in SQL (single quotes; escape internal)."""
    return "'" + s.replace("'", "''") + "'"
