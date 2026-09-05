"""Canonical access-log row shape (layer-1a).

One row per request, normalized across providers. Fields chosen to be:

- **Requester-neutral** — provider-specific identity (IP, principalArn,
  Cloudflare account) all folds into a single ``requester`` string.
- **Op-normalized** — a small vocabulary (``GET`` / ``PUT`` / ``LIST`` / ``HEAD``
  / ``DELETE`` / ``OTHER``) so cross-store rollups compose; ``op_raw`` preserves
  the provider's own verb for provenance.
- **Path-only** — ``bucket`` + ``path`` (no scheme, no host); the store is a
  separate column so a single scan can span multi-cloud footprints.
- **Byte-explicit** — ``bytes_out`` / ``bytes_in`` are directional (sc/cs in
  GCS parlance); egress is ``bytes_out``.

Empty ``path`` = bucket-level op (list, bucket metadata, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Vocabulary of normalized ops. Anything outside this set becomes 'OTHER'.
OPS = ('GET', 'PUT', 'LIST', 'HEAD', 'DELETE', 'OTHER')


@dataclass(frozen=True)
class AccessRow:
    """One request. Field order matches parquet write order."""

    ts: str            # ISO-8601 UTC; parquet column type is TIMESTAMP
    store: str         # 'gcs' | 's3' | 'r2'
    bucket: str
    path: str          # object key; '' for bucket-level ops
    op: str            # normalized (see OPS)
    op_raw: str        # provider verb — cs_method+cs_operation for GCS, s3-op for S3
    status: int        # HTTP status
    bytes_out: int     # sc_bytes-equivalent (egress)
    bytes_in: int      # cs_bytes-equivalent
    requester: str     # best available identity string
    user_agent: str = ''
    request_id: Optional[str] = None  # provider request-id; used for dedupe


# Canonical parquet column order (parsers all emit in this shape).
ACCESS_COLUMNS: tuple[str, ...] = (
    'ts', 'store', 'bucket', 'path',
    'op', 'op_raw', 'status',
    'bytes_out', 'bytes_in',
    'requester', 'user_agent', 'request_id',
)


def normalize_op(method: str, operation: str = '') -> str:
    """Fold a provider verb into the canonical vocabulary.

    ``operation`` is the finer-grained GCS ``cs_operation`` (e.g.
    ``GET_Object`` / ``LIST_Bucket``); when set, it wins over ``method``
    because the same HTTP method (GET) covers both GET_Object and LIST_Bucket.
    """
    op = operation.upper() if operation else method.upper()
    # GCS-specific compound verbs. Listing is spelled three ways depending on
    # API surface — XML: GET_Bucket (an HTTP GET on the bucket); JSON:
    # storage.objects.list / storage.buckets.list; docs' LIST_Bucket — so
    # match on substring, plus the GET_Bucket special case.
    if 'LIST' in op or op == 'GET_BUCKET':
        return 'LIST'
    if op.startswith('GET') or op.startswith('STORAGE.') and op.endswith('.GET'):
        return 'GET'
    if op.startswith('PUT') or op in ('POST_OBJECT', 'POST_UPLOADS') or (
        op.startswith('STORAGE.') and (op.endswith('.INSERT') or op.endswith('.PATCH') or op.endswith('.UPDATE'))
    ):
        return 'PUT'
    if op.startswith('HEAD'):
        return 'HEAD'
    if op.startswith('DELETE') or (op.startswith('STORAGE.') and op.endswith('.DELETE')):
        return 'DELETE'
    if op in OPS:
        return op
    return 'OTHER'
