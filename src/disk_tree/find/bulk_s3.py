"""S3 (+ R2, via S3-compatible endpoint) plug-in for :mod:`disk_tree.find.bulk`.

Requires the ``[s3]`` (or ``[r2]`` / ``[bulk]``) extra.

Semantic quirks handled here so the generic backbone can keep GCS's inclusive-
start / exclusive-end contract:

- **Boundary inclusivity**: S3's ``StartAfter`` is *exclusive* of the key,
  while ``bulk.split_hot_prefixes`` produces range starts that are inclusive
  (real object names from a reservoir quantile). We compensate by HEADing the
  boundary object first and prepending it to the stream if present — one
  extra request per range boundary, negligible.
- **Range end**: S3 has no native end-cursor. We iterate under ``StartAfter``
  and break out when a key reaches ``end`` (one extra page over-fetched;
  cheap vs. the alternative of client-side page-truncation).
- **Placeholder folders**: S3 has no GCS-style zero-byte ``<name>/`` blob
  showing up as a directory in listings, so ``placeholder_rows`` returns [].
"""

from __future__ import annotations

import sys
import threading
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Iterable, Optional

from disk_tree.find.bulk import BlobRow, generic_discover

if TYPE_CHECKING:
    import fsspec  # noqa: F401


err = partial(print, file=sys.stderr)


@dataclass(frozen=True)
class S3BulkLister:
    """S3 / R2 implementation of :class:`~disk_tree.find.bulk.BulkLister`.

    ``endpoint_url`` lets a caller point at R2 or any other S3-compatible
    service (e.g. MinIO). ``scheme`` is used only to tag the emitted rows;
    the streaming code is identical.

    The boto3 client is created lazily inside worker processes (thread-local
    below) so the instance itself stays pickle-safe.
    """

    scheme: str = "s3"
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None

    def _client(self):
        # Cache one client per thread — boto3 clients aren't thread-safe for
        # long-running iterators but reuse across independent list-loops is
        # fine, and pagination cost dominates the ~50ms client-init anyway.
        local = getattr(self, "_local", None)
        if local is None:
            object.__setattr__(self, "_local", threading.local())
            local = self._local
        client = getattr(local, "client", None)
        if client is None:
            import boto3

            kw = {}
            if self.endpoint_url:
                kw["endpoint_url"] = self.endpoint_url
            if self.region_name:
                kw["region_name"] = self.region_name
            client = local.client = boto3.client("s3", **kw)
        return client

    def stream_prefix(
        self,
        bucket: str,
        prefix: str,
        start: Optional[str],
        end: Optional[str],
    ) -> Iterable[BlobRow]:
        client = self._client()

        # Compensate for S3's exclusive StartAfter: HEAD `start` and yield it
        # first if present. Reservoir-quantile boundaries are real names, so
        # this typically hits.
        if start is not None:
            try:
                head = client.head_object(Bucket=bucket, Key=start)
            except client.exceptions.ClientError as e:
                if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                    head = None
                else:
                    raise
            if head is not None:
                lm = head.get("LastModified")
                created = lm.isoformat().replace("+00:00", "Z") if lm else None
                yield BlobRow(
                    name=start,
                    size=int(head.get("ContentLength", 0) or 0),
                    created=created,
                    storage_class=head.get("StorageClass"),
                )

        paginator = client.get_paginator("list_objects_v2")
        kw: dict = {"Bucket": bucket, "Prefix": prefix}
        if start is not None:
            kw["StartAfter"] = start
        for page in paginator.paginate(**kw):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if end is not None and key >= end:
                    return
                lm = obj.get("LastModified")
                created = lm.isoformat().replace("+00:00", "Z") if lm else None
                yield BlobRow(
                    name=key,
                    size=int(obj.get("Size", 0) or 0),
                    created=created,
                    storage_class=obj.get("StorageClass"),
                )

    def discover_prefixes(self, fs: "fsspec.AbstractFileSystem", root: str):
        return generic_discover(fs, root)

    def placeholder_rows(
        self,
        bucket: str,
        self_dirs: list[str],
    ) -> "list[tuple[str, int, Optional[str], Optional[str]]]":
        # No GCS-style placeholder objects in S3 land.
        return []


def list_s3_bucket_to_parquet(
    bucket: str,
    out_dir: str,
    procs: int = 6,
    threads: int = 8,
    prefix: Optional[str] = None,
    exists: str = "error",
    weights_from: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    region_name: Optional[str] = None,
    scheme: str = "s3",
) -> int:
    """Bulk-list an S3-compatible bucket to sharded canonical listing parquet.

    Pass ``endpoint_url`` (and typically ``scheme='r2'``) to target Cloudflare
    R2 or another S3-compatible service.
    """
    import s3fs

    from disk_tree.find.bulk import list_bucket_to_parquet

    lister = S3BulkLister(scheme=scheme, endpoint_url=endpoint_url, region_name=region_name)
    kw: dict = {}
    if endpoint_url:
        kw["client_kwargs"] = {"endpoint_url": endpoint_url}
    fs = s3fs.S3FileSystem(**kw)
    return list_bucket_to_parquet(
        lister=lister,
        bucket=bucket,
        out_dir=out_dir,
        fs=fs,
        procs=procs,
        threads=threads,
        prefix=prefix,
        exists=exists,
        weights_from=weights_from,
        discover=lister.discover_prefixes,
    )
