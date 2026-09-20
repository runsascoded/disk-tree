"""`dt-cloud healthcheck` — the live-site serving invariant (`dt_cloud.healthcheck`).

Pins the pass/fail logic that would have caught the 2026-08-31 `/users` outage:
a scan whose `/api/marks/totals` served from `index != 'd1'` (footer/coarse
fallback, the 1102 risk) must fail. The HTTP layer is injected, so these run
offline against canned responses — no network, no prod.
"""

from __future__ import annotations

import datetime as dt
import json

from dt_cloud.healthcheck import (
    Check,
    check_freshness,
    check_status,
    check_totals,
    run_checks,
)

TODAY = dt.date(2026, 8, 31)


def test_freshness_fresh_stale_empty():
    assert check_freshness(["2026-08-31", "2026-08-30"], 2, TODAY) == Check(
        "freshness", True, "latest scan 2026-08-31 (0d old, limit 2d)")
    assert check_freshness(["2026-08-28"], 2, TODAY) == Check(
        "freshness", False, "latest scan 2026-08-28 (3d old, limit 2d)")
    assert check_freshness([], 2, TODAY) == Check("freshness", False, "scans.json empty or unreadable")


def test_totals_healthy():
    body = {"computed": {"index": "d1", "ms": 13319}, "users": {"a": {}, "b": {}}}
    assert check_totals(200, body, 25000) == Check("marks/totals", True, "200 · index=d1 · users=2 · 13319ms")


def test_totals_footer_fallback_flagged():
    # The exact 2026-08-31 shape: served, but from the footer-parse path.
    body = {"computed": {"index": "footer", "ms": 40000}, "users": {"a": {}}}
    assert check_totals(200, body, 25000) == Check(
        "marks/totals", False, "index='footer' (want 'd1' — footer/coarse fallback risks 1102); users=1")


def test_totals_503_and_slow_and_empty():
    assert check_totals(503, None, 25000) == Check("marks/totals", False, "HTTP 503 (want 200 JSON)")
    slow = {"computed": {"index": "d1", "ms": 30000}, "users": {"a": {}}}
    assert check_totals(200, slow, 25000) == Check(
        "marks/totals", False, "index=d1 users=1 but 30000ms ≥ 25000ms budget")
    empty = {"computed": {"index": "d1", "ms": 100}, "users": {}}
    assert check_totals(200, empty, 25000) == Check("marks/totals", False, "users empty")


def test_status_ok_codes():
    assert check_status("subtree", 200, (200,)) == Check("subtree", True, "HTTP 200 (want 200)")
    assert check_status("subtree", 500, (200,)) == Check("subtree", False, "HTTP 500 (want 200)")


def _fake_site(*, totals_body: dict, subtree=200, meta=200, scans=("2026-08-31", "2026-08-30")):
    """A getter over a canned site: maps request URL → (status, body_bytes)."""
    routes = {
        "/data/scans.json": (200, json.dumps(list(scans)).encode()),
        "/api/marks/totals?date=2026-08-31": (200, json.dumps(totals_body).encode()),
        "/api/subtree?date=2026-08-31&w=128&h=128": (subtree, b"{}"),
        "/data/2026-08-31/meta.json": (meta, b"{}"),
    }

    def get(url: str, rng: str | None) -> tuple[int, bytes]:
        path = url.replace("https://gcs.oa.dev", "")
        return routes.get(path, (404, b""))

    return get


def test_run_checks_all_green_resolves_latest_scan():
    get = _fake_site(totals_body={"computed": {"index": "d1", "ms": 13319}, "users": {"a": {}}})
    date, checks = run_checks("https://gcs.oa.dev", "tok", None, today=TODAY, get=get)
    assert date == "2026-08-31"
    assert checks == [
        Check("freshness", True, "latest scan 2026-08-31 (0d old, limit 2d)"),
        Check("marks/totals", True, "200 · index=d1 · users=1 · 13319ms"),
        Check("subtree", True, "HTTP 200 (want 200)"),
        Check("data/meta.json", True, "HTTP 200 (want 200)"),
    ]


def test_run_checks_flags_footer_fallback_and_missing_data():
    # marks/totals on the footer path + a missing meta.json → two failed checks.
    get = _fake_site(totals_body={"computed": {"index": "footer", "ms": 40000}, "users": {"a": {}}}, meta=404)
    date, checks = run_checks("https://gcs.oa.dev", "tok", None, today=TODAY, get=get)
    assert date == "2026-08-31"
    assert [(c.name, c.ok) for c in checks] == [
        ("freshness", True),
        ("marks/totals", False),
        ("subtree", True),
        ("data/meta.json", False),
    ]


def test_run_checks_retries_transport_blip_once(monkeypatch):
    # First subtree probe dies at the transport level (status 0), the retry
    # succeeds — the check passes and the probe was fetched exactly twice.
    import dt_cloud.healthcheck as hc

    monkeypatch.setattr(hc, "RETRY_SLEEP", 0)
    inner = _fake_site(totals_body={"computed": {"index": "d1", "ms": 13319}, "users": {"a": {}}})
    subtree_calls = []

    def get(url: str, rng: str | None) -> tuple[int, bytes]:
        if "/api/subtree" in url:
            subtree_calls.append(url)
            if len(subtree_calls) == 1:
                return 0, b""
        return inner(url, rng)

    date, checks = run_checks("https://gcs.oa.dev", "tok", None, today=TODAY, get=get)
    assert date == "2026-08-31"
    assert len(subtree_calls) == 2
    assert checks == [
        Check("freshness", True, "latest scan 2026-08-31 (0d old, limit 2d)"),
        Check("marks/totals", True, "200 · index=d1 · users=1 · 13319ms"),
        Check("subtree", True, "HTTP 200 (want 200)"),
        Check("data/meta.json", True, "HTTP 200 (want 200)"),
    ]


def test_run_checks_transport_failure_persists_after_retry():
    # Both attempts fail at the transport level → the check fails as HTTP 0.
    import dt_cloud.healthcheck as hc

    assert hc.RETRY_SLEEP == 5.0  # prod pause between attempts

    inner = _fake_site(totals_body={"computed": {"index": "d1", "ms": 13319}, "users": {"a": {}}})
    hc.RETRY_SLEEP = 0
    try:
        def get(url: str, rng: str | None) -> tuple[int, bytes]:
            if "/api/subtree" in url:
                return 0, b""
            return inner(url, rng)

        date, checks = run_checks("https://gcs.oa.dev", "tok", None, today=TODAY, get=get)
    finally:
        hc.RETRY_SLEEP = 5.0
    assert date == "2026-08-31"
    assert [(c.name, c.ok, c.detail) for c in checks if c.name == "subtree"] == [
        ("subtree", False, "HTTP 0 (want 200)"),
    ]
