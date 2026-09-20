"""Bulk mark & sweep from the command line — the agent-facing primitive.

An agent that knows which prefixes it owns pipes them in and claims them:

    my-list-of-dirs | dt-cloud mark --owner @me

This POSTs to the site's append-only actions ledger (`site/functions/api/
actions.ts`), the same endpoint the dashboard's mark UI drives. Auth is a
personal token (a grant minted with the caller's email) passed as a Bearer
header; guest links are read-only server-side, so a token that can't write
gets a clean 403 rather than a silent no-op.

The pure request-building logic lives here, free of I/O, so it can be pinned
by tests exactly the way the server validates.
"""

from __future__ import annotations

import json
import os
import re
import sys
from functools import partial
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .secrets import secret

err = partial(print, file=sys.stderr)

DEFAULT_URL = "https://gcs.oa.dev"
# a real UA — CF edge-blocks bot UAs (1010), same as healthcheck.UA
UA = "gcs-usage-cli/1.0"


def creds(token: str | None, url: str | None) -> tuple[str, str | None]:
    """Resolve (base_url, token) from args then env, so every verb shares one
    convention: ``$GCS_USAGE_TOKEN`` / ``$GCS_USAGE_URL``."""
    return (url or os.environ.get("GCS_USAGE_URL") or DEFAULT_URL), secret(token, "GCS_USAGE_TOKEN")

#: Mirror of `PREFIX_RE` in the three site endpoints: a directory prefix under
#: one of the six marin buckets, trailing slash required. Kept in lockstep so
#: the CLI rejects the same strings the server would, before any network round
#: trip — a wrong prefix should fail on the caller's machine, not at the edge.
PREFIX_RE = re.compile(r"^gs://marin-[a-z0-9-]+/(?:[^\s]*/)?$")

#: Server's `KEEPS` set. `None` = don't touch the keep axis at all.
KEEP_ACTIONS = ("keep", "keep_last_ckpt", "sweep")

#: The server accepts 1–500 actions per POST; batch to that ceiling.
MAX_BATCH = 500

#: Cap enforced server-side on each field; check here for a better message.
MAX_PATTERN = 512
MAX_OWNER = 128
MAX_MEMO = 1024


class MarkError(Exception):
    """A caller-fixable problem (bad prefix, no axis, no token)."""


def validate_prefixes(prefixes: list[str]) -> list[str]:
    """Return ``prefixes`` unchanged, or raise naming every offender.

    Fails the whole batch on the first bad prefix rather than silently
    dropping it: a marking run that quietly skipped half its input would leave
    an agent believing it claimed prefixes it never did.
    """
    bad = [p for p in prefixes if not PREFIX_RE.match(p) or len(p) > MAX_PATTERN]
    if bad:
        shown = "\n  ".join(bad[:10])
        more = f"\n  … and {len(bad) - 10} more" if len(bad) > 10 else ""
        raise MarkError(
            f"{len(bad)} prefix(es) are not gs://marin-<bucket>/<path>/ "
            f"(trailing slash, ≤{MAX_PATTERN} chars):\n  {shown}{more}"
        )
    return prefixes


def build_actions(
    prefixes: list[str],
    keep: str | None = "keep",
    owner: str | None = "@me",
    memo: str | None = None,
    scan: str | None = None,
) -> list[dict]:
    """Prefixes → action dicts in the exact shape ``/api/actions`` expects.

    ``keep``/``owner`` of ``None`` leave that axis untouched (the ``set_*``
    flag stays false); at least one axis must be set, matching the server.
    """
    if not prefixes:
        raise MarkError("no prefixes to mark")
    if keep is None and owner is None:
        raise MarkError("nothing to set — pass a --keep action, an --owner, or both")
    if keep is not None and keep not in KEEP_ACTIONS:
        raise MarkError(f"--keep must be one of {', '.join(KEEP_ACTIONS)}")
    if owner is not None and len(owner) > MAX_OWNER:
        raise MarkError(f"--owner must be ≤{MAX_OWNER} chars")
    if memo is not None and len(memo) > MAX_MEMO:
        memo = memo[:MAX_MEMO]
    validate_prefixes(prefixes)

    action: dict = {}
    if keep is not None:
        action["set_keep"], action["keep"] = True, keep
    if owner is not None:
        action["set_owner"], action["owner"] = True, owner
    if memo is not None:
        action["memo"] = memo
    if scan is not None:
        action["scan"] = scan
    return [{"pattern": p, **action} for p in prefixes]


def batches(actions: list[dict], size: int = MAX_BATCH) -> list[list[dict]]:
    """Split into POST-sized chunks; the server rejects >500 in one call."""
    if size < 1:
        raise ValueError("size must be >= 1")
    return [actions[i : i + size] for i in range(0, len(actions), size)]


def gather_prefixes(args: list[str], line_sources: list) -> list[str]:
    """Collect prefixes from CLI args and any file/stdin sources.

    Blank lines and ``#`` comments in file input are dropped, so an agent can
    hand over an annotated manifest. Order is preserved and duplicates are
    de-duped (last position kept stable), because re-marking the same prefix
    twice in one run is just wasted rows.
    """
    seen: dict[str, None] = {}
    for a in args:
        if a.strip():
            seen.setdefault(a.strip(), None)
    for src in line_sources:
        for line in src:
            s = line.strip()
            if s and not s.startswith("#"):
                seen.setdefault(s, None)
    return list(seen)


def post_actions(url: str, token: str, chunk: list[dict], timeout: int = 30) -> dict:
    """POST one chunk to ``<url>/api/actions``; return the parsed JSON body.

    Raises :class:`MarkError` with the server's own message on a 4xx/5xx, so a
    403 (guest token) or 400 (bad pattern that slipped the client check) reads
    as one clear line rather than a stack trace.
    """
    endpoint = f"{url.rstrip('/')}/api/actions"
    req = Request(
        endpoint,
        data=json.dumps(chunk).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}", "User-Agent": UA},
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            msg = json.loads(body).get("error", body)
        except ValueError:
            msg = body
        raise MarkError(f"{endpoint} → HTTP {e.code}: {msg}") from e
    except URLError as e:
        raise MarkError(f"{endpoint} unreachable: {e.reason}") from e


def get_json(url: str, token: str, endpoint_path: str, params: dict | None = None, timeout: int = 30) -> dict:
    """GET ``<url><endpoint_path>?<params>`` as an authenticated JSON call.

    The read counterpart to :func:`post_actions`; surfaces the server's own
    error string on a non-2xx rather than a raw ``HTTPError``.
    """
    q = f"?{urlencode(params)}" if params else ""
    endpoint = f"{url.rstrip('/')}{endpoint_path}{q}"
    req = Request(endpoint, headers={"Authorization": f"Bearer {token}", "User-Agent": UA})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            msg = json.loads(body).get("error", body)
        except ValueError:
            msg = body
        raise MarkError(f"{endpoint} → HTTP {e.code}: {msg}") from e
    except URLError as e:
        raise MarkError(f"{endpoint} unreachable: {e.reason}") from e


def resolve_path(url: str, token: str, path: str) -> dict:
    """One path's effective keep + owner (GET /api/resolve)."""
    return get_json(url, token, "/api/resolve", {"path": path})


def todo_list(url: str, token: str, limit: int | None = None, min_frac: float | None = None) -> dict:
    """The keep-axis review backlog — largest undecided prefixes (GET /api/todo)."""
    params: dict = {}
    if limit is not None:
        params["limit"] = limit
    if min_frac is not None:
        params["min_frac"] = min_frac
    return get_json(url, token, "/api/todo", params or None)
