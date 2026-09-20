"""`dt-cloud mark` request-building (`dt_cloud.mark`).

These pin the exact JSON the CLI sends to `/api/actions`, and the client-side
validation that mirrors the server's `PREFIX_RE` / axis rules — so a bad prefix
fails on the caller's machine, and the payload shape can't drift from what the
endpoint accepts without a test breaking.
"""

from __future__ import annotations

import io

import pytest

from dt_cloud.mark import (
    MAX_BATCH,
    MarkError,
    batches,
    build_actions,
    gather_prefixes,
    validate_prefixes,
)

P1 = "gs://marin-us-central1/checkpoints/run-a/"
P2 = "gs://marin-us-east1/data/foo/"


# ---------- validation ----------

@pytest.mark.parametrize("prefix", [
    "gs://marin-us-central1/checkpoints/run-a/",
    "gs://marin-eu-west4/",                         # bucket root
    "gs://marin-us-west4/a/b/c/d/",                 # deep
])
def test_validate_accepts_marin_dir_prefixes(prefix: str):
    assert validate_prefixes([prefix]) == [prefix]


@pytest.mark.parametrize("prefix", [
    "gs://not-marin/foo/",                          # wrong bucket family
    "gs://marin-us-central1/nomissingslash",        # no trailing slash
    "marin-us-central1/foo/",                        # no scheme
    "gs://marin-us-central1/has space/",            # whitespace
])
def test_validate_rejects_non_conforming_prefixes(prefix: str):
    with pytest.raises(MarkError, match="not gs://marin"):
        validate_prefixes([prefix])


def test_validate_names_every_offender_and_omits_the_valid_one():
    with pytest.raises(MarkError) as e:
        validate_prefixes([P1, "gs://bad/", "also-bad"])
    assert str(e.value) == (
        "2 prefix(es) are not gs://marin-<bucket>/<path>/ "
        "(trailing slash, ≤512 chars):\n  gs://bad/\n  also-bad"
    )


# ---------- build_actions ----------

def test_build_actions_default_sets_both_axes():
    assert build_actions([P1, P2]) == [
        {"pattern": P1, "set_keep": True, "keep": "keep", "set_owner": True, "owner": "@me"},
        {"pattern": P2, "set_keep": True, "keep": "keep", "set_owner": True, "owner": "@me"},
    ]


def test_build_actions_keep_only():
    assert build_actions([P1], keep="keep_last_ckpt", owner=None) == [
        {"pattern": P1, "set_keep": True, "keep": "keep_last_ckpt"},
    ]


def test_build_actions_owner_only():
    assert build_actions([P1], keep=None, owner="ryan") == [
        {"pattern": P1, "set_owner": True, "owner": "ryan"},
    ]


def test_build_actions_carries_memo_and_scan():
    assert build_actions([P1], memo="nightly", scan="2026-08-24") == [
        {
            "pattern": P1, "set_keep": True, "keep": "keep",
            "set_owner": True, "owner": "@me", "memo": "nightly", "scan": "2026-08-24",
        },
    ]


def test_build_actions_requires_at_least_one_axis():
    with pytest.raises(MarkError, match="nothing to set"):
        build_actions([P1], keep=None, owner=None)


def test_build_actions_rejects_unknown_keep():
    with pytest.raises(MarkError, match="--keep must be one of"):
        build_actions([P1], keep="hoard")


def test_build_actions_rejects_empty_input():
    with pytest.raises(MarkError, match="no prefixes"):
        build_actions([])


def test_build_actions_truncates_an_overlong_memo():
    action = build_actions([P1], memo="x" * 2000)[0]
    assert len(action["memo"]) == 1024


# ---------- gather_prefixes ----------

def test_gather_merges_args_and_line_sources_dropping_blanks_and_comments():
    src = io.StringIO("gs://marin-us-west4/y/\n# a note\n\n  gs://marin-eu-west4/z/  \n")
    assert gather_prefixes([P1], [src]) == [
        P1, "gs://marin-us-west4/y/", "gs://marin-eu-west4/z/",
    ]


def test_gather_dedups_preserving_first_position():
    src = io.StringIO(f"{P2}\n{P1}\n")
    assert gather_prefixes([P1, P2], [src]) == [P1, P2]


# ---------- batches ----------

def test_batches_below_the_ceiling_is_one_request():
    acts = build_actions([P1] * 10)
    assert [len(b) for b in batches(acts)] == [10]


def test_batches_splits_at_the_server_ceiling():
    acts = [{"pattern": P1, "set_keep": True, "keep": "keep"}] * (MAX_BATCH + 1)
    sizes = [len(b) for b in batches(acts)]
    assert sizes == [MAX_BATCH, 1]


def test_batches_rejects_a_nonpositive_size():
    with pytest.raises(ValueError, match="size must be"):
        batches([{"pattern": P1}], size=0)
