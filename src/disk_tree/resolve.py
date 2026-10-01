"""Scan-blob resolution: blob refs → paths on the search path (`index`)."""

from __future__ import annotations

from os.path import isabs

from . import config as _config


def resolve_blob(blob_ref: str) -> str:
    """Resolve a parquet blob ref to its absolute path.

    Honors legacy absolute refs. Searches every configured scans dir (blobs
    may sit in any of them, or behind a URL), reading config at call time so tests can
    monkeypatch it.
    """
    if not blob_ref:
        return blob_ref
    return blob_ref if isabs(blob_ref) else _config.resolve_scan_blob(blob_ref)
