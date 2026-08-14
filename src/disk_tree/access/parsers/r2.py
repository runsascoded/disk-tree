"""Cloudflare R2 access log parser — via Logpush's HTTP-requests dataset.

R2 buckets don't expose per-object server logs on their own. To get access
telemetry you either:

1. Front the bucket with a Worker and Logpush the Worker's request stream
   (recommended: gives you request URIs, IPs, statuses, bytes; the Worker
   costs 0.5μs per request but you get the visibility);
2. Use Logpush's ``r2 datasets`` (per-bucket, still requires enabling).

Both flavors deliver newline-delimited JSON to R2/S3/etc. Fields differ
slightly between "Worker requests" and native "R2 operations"; the parser
should union-schema both.

TODO: stub — implement when a personal / OA R2 consumer needs it. The
Worker-fronting caveat is documented in ``specs/access-logs-and-cost.md``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def parse(input_glob: str, store: str = 'r2', con: "duckdb.DuckDBPyConnection | None" = None) -> "duckdb.DuckDBPyRelation":
    """Parse R2 Logpush NDJSON → canonical relation.

    NOT YET IMPLEMENTED. Interface + row shape are pinned so a landed
    implementation is drop-in.
    """
    raise NotImplementedError(
        "R2 access-log parser is a stub. R2 needs Logpush configured "
        "(often Worker-fronted); see src/disk_tree/access/parsers/r2.py for the "
        "interface + field notes. Landing this is queued for the personal / OA R2 track."
    )
