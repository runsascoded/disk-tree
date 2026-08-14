"""S3 server access log parser — space-delimited text.

Fields (per `AWS docs
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html>`_):

::

    <owner> <bucket> <time> <remote_ip> <requester> <request_id> <op> <key>
    <request_uri> <status> <error_code> <bytes_sent> <object_size>
    <total_time> <turn_around_time> <referer> <user_agent> <version_id>
    <host_id> <sig_version> <cipher_suite> <auth_type> <host_header>
    <tls_version> <access_point_arn> <acl_required>

Quoted fields (URI, user-agent, referer) can contain spaces and even escaped
quotes. Rather than hand-roll a tokenizer, we use DuckDB's ``read_csv`` with
``delim=' '`` + ``quote='"'``.

TODO: this is a stub — implement + test with fixture CSVs before real use.
The interface + row shape are pinned so a landed implementation swaps in
cleanly. mgu doesn't need S3 today; the ordering per D-planning is GCS
first, then S3 for the personal-deploy track.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def parse(input_glob: str, store: str = 's3', con: "duckdb.DuckDBPyConnection | None" = None) -> "duckdb.DuckDBPyRelation":
    """Parse S3 server access logs → canonical relation.

    NOT YET IMPLEMENTED. Interface + row shape (see
    :data:`~disk_tree.access.schema.ACCESS_COLUMNS`) are pinned so a landed
    implementation is drop-in.
    """
    raise NotImplementedError(
        "S3 access-log parser is a stub. "
        "See src/disk_tree/access/parsers/s3.py for the interface + field docs; "
        "landing this is queued for the personal-deploy branch of work."
    )
