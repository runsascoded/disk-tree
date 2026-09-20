"""MarkState-resolution spec for the sweep planner (specs/sweep-executor.md).

Each test states the ledger and the exact expected states — these are the
semantics `/api/resolve` implements in SQL and the site implements in TS;
divergence here means the planner must not run.
"""

from __future__ import annotations

from dt_cloud.sweep_plan import (
    StateResolver,
    KeepRow,
    KlcSplit,
    key_to_prefixes,
    klc_key_state,
    klc_split,
    load_keeps,
)

B = "gs://marin-us-east5/"


def _r(prefix: str, keep, ts: int, aid: int) -> KeepRow:
    return KeepRow(prefix=prefix, keep=keep, ts=ts, action_id=aid, who="t@oa")


def test_key_to_prefixes():
    assert key_to_prefixes("marin-b", "x/y/f.bin") == [
        "gs://marin-b/",
        "gs://marin-b/x/",
        "gs://marin-b/x/y/",
    ]
    assert key_to_prefixes("marin-b", "f.bin") == ["gs://marin-b/"]


def test_recency_beats_specificity():
    """A newer broad mark repaints an older deeper one."""
    fr = StateResolver([
        _r(f"{B}run/ckpt/", "keep", ts=100, aid=1),
        _r(f"{B}run/", "sweep", ts=200, aid=2),
    ])
    assert fr.state(f"{B}run/ckpt/") == "sweep"
    assert fr.state(f"{B}run/") == "sweep"
    assert fr.state(f"{B}other/") is None


def test_child_carve_out_after_parent():
    """Marking the child *after* the parent carves the exception."""
    fr = StateResolver([
        _r(f"{B}run/", "sweep", ts=100, aid=1),
        _r(f"{B}run/gold/", "keep", ts=200, aid=2),
    ])
    assert fr.state(f"{B}run/gold/") == "keep"
    assert fr.state(f"{B}run/") == "sweep"
    assert fr.state(f"{B}run/junk/") == "sweep"  # covered by the parent band


def test_explicit_unmark_wins():
    fr = StateResolver([
        _r(f"{B}d/", "sweep", ts=100, aid=1),
        _r(f"{B}d/", None, ts=200, aid=2),
    ])
    assert fr.state(f"{B}d/") is None


def test_ts_tie_breaks_on_action_id():
    fr = StateResolver([
        _r(f"{B}d/", "keep", ts=100, aid=1),
        _r(f"{B}d/", "sweep", ts=100, aid=2),
    ])
    assert fr.state(f"{B}d/") == "sweep"


def test_key_state_deepest_marked_ancestor_with_provenance():
    """The deepest marked ancestor decides; the winning row (provenance) may
    sit on a shallower prefix when recency repainted it."""
    deep = _r(f"{B}run/ckpt/", "keep", ts=100, aid=1)
    broad = _r(f"{B}run/", "sweep", ts=200, aid=2)
    fr = StateResolver([deep, broad])
    state, row = fr.key_state("marin-us-east5", "run/ckpt/model.bin")
    assert (state, row) == ("sweep", broad)
    state, row = fr.key_state("marin-us-east5", "unmarked/f.bin")
    assert (state, row) == (None, None)


def test_load_keeps_shapes_api_rows():
    payload = {"keeps": [
        {"prefix": f"{B}a/", "keep": "sweep", "ts": 5, "action_id": 9, "who": "x@oa", "memo": None},
        {"prefix": f"{B}b/", "keep": None, "ts": 6, "action_id": 10},
    ]}
    assert load_keeps(payload) == [
        KeepRow(prefix=f"{B}a/", keep="sweep", ts=5, action_id=9, who="x@oa"),
        KeepRow(prefix=f"{B}b/", keep=None, ts=6, action_id=10),
    ]


# ---- KLC ------------------------------------------------------------------

def test_klc_split_hf_checkpoints_numeric_max():
    """`checkpoint-N` layout: numeric max (checkpoint-10000 > checkpoint-9000),
    non-step siblings at that level sweep — matching the site's klcSplits."""
    split = klc_split([
        "checkpoint-9000/model.safetensors",
        "checkpoint-10000/model.safetensors",
        "checkpoint-10000/optimizer.pt",
        "config.json",
        "logs/train.log",
    ])
    assert split == KlcSplit(prefix="", kept=("checkpoint-10000/",), resolved=True)
    assert klc_key_state("checkpoint-10000/model.safetensors", split) == "keep"
    assert klc_key_state("checkpoint-9000/model.safetensors", split) == "sweep"
    assert klc_key_state("config.json", split) == "sweep"
    assert klc_key_state("logs/train.log", split) == "sweep"


def test_klc_split_nested_runs_recurse():
    """Steps one level down: each run dir gets its own max-step keep."""
    split = klc_split([
        "run-a/step_100/w.bin",
        "run-a/step_200/w.bin",
        "run-b/global_step50/w.bin",
        "run-b/global_step150/w.bin",
    ])
    assert sorted(split.kept) == ["run-a/step_200/", "run-b/global_step150/"]
    assert split.resolved is True
    assert klc_key_state("run-a/step_200/w.bin", split) == "keep"
    assert klc_key_state("run-a/step_100/w.bin", split) == "sweep"
    assert klc_key_state("run-b/global_step150/w.bin", split) == "keep"


def test_klc_split_no_steps_is_unresolved_and_keeps():
    split = klc_split(["data/part-0.parquet", "data/part-1.parquet"])
    assert split == KlcSplit(prefix="", kept=(), resolved=False)
    assert klc_key_state("data/part-0.parquet", split) == "keep"


def test_klc_split_stops_at_first_step_level():
    """A node with step children does NOT recurse — deeper step dirs inside the
    kept subtree stay kept wholesale (exactly the tree walk's `return`)."""
    split = klc_split([
        "step-1/inner/step-99/w.bin",
        "step-2/inner/step-1/w.bin",
    ])
    assert split.kept == ("step-2/",)
    assert klc_key_state("step-2/inner/step-1/w.bin", split) == "keep"
    assert klc_key_state("step-1/inner/step-99/w.bin", split) == "sweep"

# ---- clobbered keeps + ever-kept guard ------------------------------------

def test_clobbered_keeps_broad_sweep_incident():
    """The 2026-09-01 shape: a whole-checkpoints/ sweep repaints two earlier
    keeps; a later carve-out keep is NOT clobbered."""
    from dt_cloud.sweep_plan import Clobber, clobbered_keeps
    rows = [
        _r(f"{B}checkpoints/calvin/", "keep", ts=100, aid=1),
        _r(f"{B}checkpoints/pinlin_calvin_xu/", "keep", ts=110, aid=2),
        KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=200, action_id=3, who="p@x"),
        _r(f"{B}checkpoints/gold/", "keep", ts=300, aid=4),  # re-kept after — safe
        _r(f"{B}elsewhere/", "keep", ts=50, aid=5),  # never covered by a sweep
    ]
    assert clobbered_keeps(rows) == [
        Clobber(prefix=f"{B}checkpoints/calvin/", keep="keep", keeper="t@oa", keep_ts=100,
                by_prefix=f"{B}checkpoints/", by_who="p@x", by_ts=200),
        Clobber(prefix=f"{B}checkpoints/pinlin_calvin_xu/", keep="keep", keeper="t@oa", keep_ts=110,
                by_prefix=f"{B}checkpoints/", by_who="p@x", by_ts=200),
    ]


def test_ever_kept_prefixes_ignores_repaints():
    from dt_cloud.sweep_plan import ever_kept_prefixes
    rows = [
        _r(f"{B}a/", "keep", ts=100, aid=1),
        KeepRow(prefix=f"{B}a/", keep="sweep", ts=200, action_id=2, who="p@x"),
        _r(f"{B}b/", "keep_last_ckpt", ts=100, aid=3),
        KeepRow(prefix=f"{B}c/", keep="sweep", ts=100, action_id=4, who="p@x"),
        _r(f"{B}d/", None, ts=100, aid=5),  # unmark is not a keep
    ]
    assert ever_kept_prefixes(rows) == frozenset({f"{B}a/", f"{B}b/"})

# ---- vote model (specs/vote-model.md) -------------------------------------

def test_votes_broad_sweep_does_not_clobber():
    """The 9/1 incident under the vote model: Pranshu's broad sweep + Calvin's
    earlier keep = conflict at Calvin's path, sweep-only elsewhere."""
    from dt_cloud.sweep_plan import VoteResolver
    vr = VoteResolver([
        KeepRow(prefix=f"{B}checkpoints/calvin/", keep="keep", ts=100, action_id=1, who="calvin"),
        KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=200, action_id=2, who="pranshu"),
    ])
    assert vr.votes(f"{B}checkpoints/calvin/") == {"calvin": "keep", "pranshu": "sweep"}
    assert vr.state(f"{B}checkpoints/calvin/") == "conflict"
    assert vr.votes(f"{B}checkpoints/junk/") == {"pranshu": "sweep"}
    assert vr.state(f"{B}checkpoints/junk/") == "sweep"
    assert vr.state(f"{B}elsewhere/") == "unmarked"


def test_votes_unmark_retracts_only_own_vote():
    """Pranshu's final east5 unmark drops HIS vote; the keeps stand alone."""
    from dt_cloud.sweep_plan import VoteResolver
    vr = VoteResolver([
        KeepRow(prefix=f"{B}checkpoints/g/", keep="keep", ts=100, action_id=1, who="gonzalo"),
        KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=200, action_id=2, who="pranshu"),
        KeepRow(prefix=f"{B}checkpoints/", keep=None, ts=300, action_id=3, who="pranshu"),
    ])
    assert vr.votes(f"{B}checkpoints/g/") == {"gonzalo": "keep"}
    assert vr.state(f"{B}checkpoints/g/") == "keep"
    assert vr.state(f"{B}checkpoints/junk/") == "unmarked"


def test_votes_self_repaint_still_works():
    """A user repainting their OWN keep to sweep yields a deletable path —
    more precise than the ever-kept guard."""
    from dt_cloud.sweep_plan import VoteResolver
    vr = VoteResolver([
        KeepRow(prefix=f"{B}mine/", keep="keep", ts=100, action_id=1, who="a"),
        KeepRow(prefix=f"{B}mine/", keep="sweep", ts=200, action_id=2, who="a"),
    ])
    assert vr.votes(f"{B}mine/") == {"a": "sweep"}
    assert vr.state(f"{B}mine/") == "sweep"


def test_key_state_per_user_granularity():
    """Per-user recency across granularity: A sweeps broad then keeps a child;
    B sweeps the child later — child is conflict, sibling sweep-only."""
    from dt_cloud.sweep_plan import VoteResolver
    vr = VoteResolver([
        KeepRow(prefix=f"{B}run/", keep="sweep", ts=100, action_id=1, who="a"),
        KeepRow(prefix=f"{B}run/gold/", keep="keep", ts=200, action_id=2, who="a"),
        KeepRow(prefix=f"{B}run/gold/", keep="sweep", ts=300, action_id=3, who="b"),
    ])
    assert vr.key_votes("marin-us-east5", "run/gold/model.bin") == {"a": "keep", "b": "sweep"}
    assert vr.key_state("marin-us-east5", "run/gold/model.bin") == "conflict"
    assert vr.key_state("marin-us-east5", "run/junk/f.bin") == "sweep"

# ---- manifest classification (policy (b)) ---------------------------------

def test_classify_dir_policy_b():
    from dt_cloud.identity import IdentityMap
    from dt_cloud.sweep_plan import VoteResolver, classify_dir, ever_kept_prefixes, owners_resolver
    idmap = IdentityMap(users=frozenset(), alias_to_user={"kaiyuewen3": "kaiyue"}, prefix_owners=())
    rows = [
        KeepRow(prefix=f"{B}grug/", keep="sweep", ts=100, action_id=1, who="kaiyuewen3@gmail.com"),
        KeepRow(prefix=f"{B}other/", keep="sweep", ts=100, action_id=2, who="kaiyuewen3@gmail.com"),
        KeepRow(prefix=f"{B}unowned/", keep="sweep", ts=100, action_id=3, who="kaiyuewen3@gmail.com"),
        KeepRow(prefix=f"{B}was-kept/", keep="keep", ts=50, action_id=4, who="kaiyuewen3@gmail.com"),
        KeepRow(prefix=f"{B}was-kept/", keep="sweep", ts=100, action_id=5, who="kaiyuewen3@gmail.com"),
        KeepRow(prefix=f"{B}klc/", keep="keep_last_ckpt", ts=100, action_id=6, who="kaiyuewen3@gmail.com"),
    ]
    owners = {"owners": [
        {"prefix": f"{B}grug/", "owner": "kaiyue", "ts": 90, "action_id": 10, "who": "kaiyuewen3@gmail.com"},
        {"prefix": f"{B}other/", "owner": "gonzalo", "ts": 90, "action_id": 11, "who": "g@oa"},
        {"prefix": f"{B}was-kept/", "owner": "kaiyue", "ts": 90, "action_id": 12, "who": "kaiyuewen3@gmail.com"},
    ]}
    vr, own = VoteResolver(rows), owners_resolver(owners)
    ever = ever_kept_prefixes(rows)
    bkt = "marin-us-east5"
    assert classify_dir(bkt, "grug/run1", vr, own, idmap, ever) == ("eligible", "kaiyue", ("kaiyue",))
    assert classify_dir(bkt, "other/x", vr, own, idmap, ever) == ("deferred_owner", "gonzalo", ("kaiyue",))
    assert classify_dir(bkt, "unowned/x", vr, own, idmap, ever) == ("deferred_unowned", None, ("kaiyue",))
    assert classify_dir(bkt, "was-kept/x", vr, own, idmap, ever) == ("ever_kept", None, ("kaiyue",))
    assert classify_dir(bkt, "klc/run", vr, own, idmap, ever) == ("klc_pending", None, ())
    assert classify_dir(bkt, "nothing/here", vr, own, idmap, ever) == ("unmarked", None, ())


def test_classify_dir_approved_bands_replace_owner_check():
    from dt_cloud.identity import IdentityMap
    from dt_cloud.sweep_plan import VoteResolver, classify_dir, ever_kept_prefixes, owners_resolver
    idmap = IdentityMap(users=frozenset(), alias_to_user={}, prefix_owners=())
    rows = [
        KeepRow(prefix=f"{B}rl_testing/", keep="sweep", ts=100, action_id=1, who="ahmed@x"),
        KeepRow(prefix=f"{B}grug/", keep="sweep", ts=100, action_id=2, who="k@x"),
    ]
    vr = VoteResolver(rows)
    own = owners_resolver({"owners": []})
    ever = ever_kept_prefixes(rows)
    approved = (f"{B}rl_testing/",)
    assert classify_dir("marin-us-east5", "rl_testing/run1", vr, own, idmap, ever, approved) == ("eligible", None, ("ahmed",))
    assert classify_dir("marin-us-east5", "grug/x", vr, own, idmap, ever, approved) == ("deferred_owner", None, ("k",))
    # without an approved list the unclaimed dirs defer entirely
    assert classify_dir("marin-us-east5", "rl_testing/run1", vr, own, idmap, ever) == ("deferred_unowned", None, ("ahmed",))


def test_classify_dir_attr_gate():
    """With an attr lookup, approval means "the sweeper's own slice": dirs
    attributed to someone else, unattributed, or minority-share defer."""
    from dt_cloud.identity import IdentityMap
    from dt_cloud.sweep_plan import VoteResolver, classify_dir, ever_kept_prefixes, owners_resolver
    idmap = IdentityMap(users=frozenset(), alias_to_user={"k": "kaiyue"}, prefix_owners=())
    rows = [KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=100, action_id=1, who="k@x")]
    vr = VoteResolver(rows)
    own = owners_resolver({"owners": []})
    ever = ever_kept_prefixes(rows)
    approved = (f"{B}checkpoints/",)
    attrs = {
        "marin-us-east5/checkpoints/isoflop": ("kaiyue", 0.97),
        "marin-us-east5/checkpoints/llama-8b": ("percy", 0.99),
        "marin-us-east5/checkpoints/vlm": (None, 0.0),
        "marin-us-east5/checkpoints/mixed": ("kaiyue", 0.41),
    }

    def attr(band, bucket, dirname):
        assert band == f"{B}checkpoints/"
        path = f"{bucket}/{dirname}"
        while path:
            if path in attrs:
                return attrs[path]
            path = path.rsplit("/", 1)[0] if "/" in path else ""
        return None

    def cat(dn):
        return classify_dir("marin-us-east5", dn, vr, own, idmap, ever, approved, attr)[0]

    assert cat("checkpoints/isoflop/run1/step-100") == "eligible"  # sweeper's own run
    assert cat("checkpoints/llama-8b/ckpt") == "deferred_attr"     # someone else's
    assert cat("checkpoints/vlm/x") == "deferred_attr"             # unattributed
    assert cat("checkpoints/mixed/y") == "deferred_attr"           # minority share
    assert cat("checkpoints/unknown/z") == "deferred_attr"         # no index row at all
    # attr=None keeps the pre-gate behavior (whole band eligible)
    assert classify_dir("marin-us-east5", "checkpoints/llama-8b/ckpt", vr, own, idmap, ever, approved)[0] == "eligible"


def test_attr_index_lookup_ancestor_walk(tmp_path):
    import pandas as pd
    from dt_cloud.attr_index import AttrIndex
    df = pd.DataFrame([
        {"path": "marin-us-east5/checkpoints", "depth": 2, "usr": "kaiyue", "b": 41},
        {"path": "marin-us-east5/checkpoints", "depth": 2, "usr": "percy", "b": 30},
        {"path": "marin-us-east5/checkpoints", "depth": 2, "usr": None, "b": 29},
        {"path": "marin-us-east5/checkpoints/isoflop", "depth": 3, "usr": "kaiyue", "b": 90},
        {"path": "marin-us-east5/checkpoints/isoflop", "depth": 3, "usr": None, "b": 10},
        {"path": "marin-us-east5/checkpoints/llama-8b", "depth": 3, "usr": "percy", "b": 100},
        {"path": "marin-us-east5/checkpoints/rooty", "depth": 3, "usr": "root", "b": 100},
        {"path": "marin-us-east5/other", "depth": 2, "usr": "kaiyue", "b": 5},
    ])
    p = tmp_path / "path-index.parquet"
    df.to_parquet(p, index=False)
    ai = AttrIndex(str(p))
    band = "gs://marin-us-east5/checkpoints/"
    # deep dirs inherit the run-level attribution via the ancestor walk
    assert ai.lookup(band, "marin-us-east5", "checkpoints/isoflop/run/step-5") == ("kaiyue", 0.9)
    assert ai.lookup(band, "marin-us-east5", "checkpoints/llama-8b/x") == ("percy", 1.0)
    # infra ids don't count as people; nothing else attributed → (None, 0.0)
    assert ai.lookup(band, "marin-us-east5", "checkpoints/rooty/x") == (None, 0.0)
    # falls back to the band root's own (plurality) attribution
    assert ai.lookup(band, "marin-us-east5", "checkpoints/never-indexed") == ("kaiyue", 0.41)
    # outside the band → None
    assert ai.lookup(band, "marin-us-east5", "other/x") is None


def test_attr_index_child_split(tmp_path):
    import pandas as pd
    from dt_cloud.attr_index import AttrIndex
    df = pd.DataFrame([
        {"path": "b/ck", "depth": 2, "usr": "kaiyue", "b": 41},
        {"path": "b/ck/iso", "depth": 3, "usr": "kaiyue", "b": 90},
        {"path": "b/ck/iso", "depth": 3, "usr": None, "b": 10},
        {"path": "b/ck/llama", "depth": 3, "usr": "percy", "b": 100},
        {"path": "b/ck/vlm", "depth": 3, "usr": None, "b": 60},
        {"path": "b/ck/mixed", "depth": 3, "usr": "kaiyue", "b": 40},
        {"path": "b/ck/mixed", "depth": 3, "usr": "percy", "b": 30},
        {"path": "b/ck/mixed", "depth": 3, "usr": None, "b": 30},
        {"path": "b/ck/iso/run", "depth": 4, "usr": "kaiyue", "b": 90},
        {"path": "b/leaf", "depth": 2, "usr": "ahmed", "b": 70},
        {"path": "b/leaf", "depth": 2, "usr": None, "b": 30},
    ])
    p = tmp_path / "path-index.parquet"
    df.to_parquet(p, index=False)
    ai = AttrIndex(str(p))
    # iso → kaiyue 90% (match); llama → percy (other); vlm unattributed and
    # mixed minority-share (40%) → unattr bucket. Depth-4 rows don't count.
    assert ai.child_split("gs://b/ck/", {"kaiyue"}) == (100, 100, 160)
    # leaf band with no children: judged on its own row (ahmed 70%)
    assert ai.child_split("gs://b/leaf/", {"ahmed"}) == (100, 0, 0)
    assert ai.child_split("gs://b/leaf/", {"kaiyue"}) == (0, 100, 0)


def test_classify_dir_attr_exempt_full_mode():
    """A band approved in 'full' mode skips the attr gate — the whole band is
    eligible even where dirs are attributed to others."""
    from dt_cloud.identity import IdentityMap
    from dt_cloud.sweep_plan import VoteResolver, classify_dir, ever_kept_prefixes, owners_resolver
    idmap = IdentityMap(users=frozenset(), alias_to_user={"k": "kaiyue"}, prefix_owners=())
    rows = [KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=100, action_id=1, who="k@x")]
    vr = VoteResolver(rows)
    own = owners_resolver({"owners": []})
    ever = ever_kept_prefixes(rows)
    band = f"{B}checkpoints/"

    def attr(b, bucket, dn):
        return ("percy", 1.0)  # everything belongs to someone else

    gated = classify_dir("marin-us-east5", "checkpoints/llama/x", vr, own, idmap, ever, (band,), attr)
    exempt = classify_dir("marin-us-east5", "checkpoints/llama/x", vr, own, idmap, ever, (band,), attr, frozenset({band}))
    assert (gated[0], exempt[0]) == ("deferred_attr", "eligible")


def test_bands_for_bucket() -> None:
    from dt_cloud.sweep_plan import bands_for_bucket

    approved = (
        "gs://marin-us-east5/grug/",
        "gs://marin-us-central2/scratch/kaiyue/checkpoints/",
        "gs://marin-us-east5/checkpoints/dpo/",
        "gs://marin-eu-west4/",
    )
    assert bands_for_bucket("marin-us-east5", approved) == ("checkpoints/dpo/", "grug/")
    assert bands_for_bucket("marin-us-central2", approved) == ("scratch/kaiyue/checkpoints/",)
    assert bands_for_bucket("marin-eu-west4", approved) == ("",)  # the whole bucket
    assert bands_for_bucket("marin-us-east1", approved) == ()


def test_classify_dir_residue_rule():
    """Passing the majority gate isn't deletion authority: the attribution
    *rule* covering a dir must name the sweeper. Another user's rule →
    `deferred_residue`; no rule, or an explicit nobody → `deferred_unattr`;
    a full-mode band skips both (specs/sweep-coowned-residue.md)."""
    from dt_cloud.identity import IdentityMap
    from dt_cloud.sweep_plan import VoteResolver, classify_dir, ever_kept_prefixes, owners_resolver
    idmap = IdentityMap(users=frozenset(), alias_to_user={"k": "kaiyue"}, prefix_owners=())
    rows = [KeepRow(prefix=f"{B}checkpoints/", keep="sweep", ts=100, action_id=1, who="k@x")]
    vr = VoteResolver(rows)
    own = owners_resolver({"owners": []})
    ever = ever_kept_prefixes(rows)
    band = f"{B}checkpoints/"
    # the majority gate says the whole band is kaiyue's (0.9) …
    attr = lambda b, bucket, dirname: ("kaiyue", 0.9)
    # … but the rules know better inside it
    rules = {
        "marin-us-east5/checkpoints/isoflop": ("kaiyue", "wandb"),
        "marin-us-east5/checkpoints/isoflop/percy-eval": ("percy", "manual"),
        "marin-us-east5/checkpoints/isoflop/nobody": (None, "manual"),
    }

    def rule(bucket, dirname):
        path = f"{bucket}/{dirname}"
        while path:
            if path in rules:
                return rules[path]
            path = path.rsplit("/", 1)[0] if "/" in path else ""
        return None

    def cat(dn, exempt=frozenset()):
        return classify_dir("marin-us-east5", dn, vr, own, idmap, ever, (band,), attr, exempt, rule)[0]

    assert cat("checkpoints/isoflop/run1/step-100") == "eligible"          # ruled to the sweeper (inherited)
    assert cat("checkpoints/isoflop/percy-eval/x") == "deferred_residue"   # a deeper rule names someone else
    assert cat("checkpoints/isoflop/nobody/y") == "deferred_unattr"        # explicit nobody
    assert cat("checkpoints/orphan/z") == "deferred_unattr"                # no rule at all, majority notwithstanding
    assert cat("checkpoints/isoflop/percy-eval/x", frozenset({band})) == "eligible"  # full mode: verified out of band
    # without a rule lookup the majority gate alone decides (pre-residue behavior)
    assert classify_dir("marin-us-east5", "checkpoints/orphan/z", vr, own, idmap, ever, (band,), attr)[0] == "eligible"


def test_attr_index_dirs(tmp_path):
    """`dirs()` = the indexed paths at or above a depth as (bucket, name)
    rows — the listing stand-in for expanding path-glob rules."""
    import pandas as pd
    from dt_cloud.attr_index import AttrIndex
    df = pd.DataFrame([
        {"path": "marin-us-east5", "depth": 1, "usr": None, "b": 1},
        {"path": "marin-us-east5/grug", "depth": 2, "usr": "kaiyue", "b": 1},
        {"path": "marin-us-east5/grug", "depth": 2, "usr": None, "b": 1},
        {"path": "marin-us-east5/grug/swarm_a", "depth": 3, "usr": "kaiyue", "b": 1},
        {"path": "marin-us-east5/grug/swarm_a/run/step-9", "depth": 5, "usr": "kaiyue", "b": 1},
    ])
    p = tmp_path / "path-index.parquet"
    df.to_parquet(p, index=False)
    got = AttrIndex(str(p)).dirs(max_depth=3)
    assert got.to_dict("records") == [
        {"bucket": "marin-us-east5", "name": ""},
        {"bucket": "marin-us-east5", "name": "grug"},
        {"bucket": "marin-us-east5", "name": "grug/swarm_a"},
    ]


def test_rule_attr_expands_path_globs(tmp_path):
    """`_rule_attr`: parquet rules + identities' prefix owners, path-glob rules
    expanded against the index's dirs, deepest prefix wins."""
    import pandas as pd
    from dt_cloud.attr_index import AttrIndex
    from dt_cloud.cli import _rule_attr
    from dt_cloud.identity import IdentityMap, PrefixOwner
    pd.DataFrame([
        {"path": "marin-us-east5", "depth": 1, "usr": None, "b": 1},
        {"path": "marin-us-east5/grug", "depth": 2, "usr": None, "b": 1},
        {"path": "marin-us-east5/grug/swarm_a", "depth": 3, "usr": None, "b": 1},
        {"path": "marin-us-east5/grug/other", "depth": 3, "usr": None, "b": 1},
    ]).to_parquet(tmp_path / "path-index.parquet", index=False)
    pd.DataFrame([
        {"prefix": "gs://marin-us-east5/grug/", "user": "k", "source": "wandb"},
        {"prefix": "gs://marin-us-east5/grug/other/percy-eval/", "user": "percy", "source": "wandb"},
    ]).to_parquet(tmp_path / "attribution.parquet", index=False)
    idmap = IdentityMap(users=frozenset(), alias_to_user={"k": "kaiyue"},
                        prefix_owners=(PrefixOwner("gs://marin-*/grug/swarm_*/", "pranshu"),))
    rule = _rule_attr(AttrIndex(str(tmp_path / "path-index.parquet")), (str(tmp_path / "attribution.parquet"),), idmap, str(tmp_path))
    assert rule("marin-us-east5", "grug/run/step-1") == ("kaiyue", "wandb")                 # resolved alias
    assert rule("marin-us-east5", "grug/swarm_a/x") == ("pranshu", "manual")                # path-glob rule, expanded
    assert rule("marin-us-east5", "grug/other/percy-eval/x") == ("percy", "wandb")          # deepest wins
    assert rule("marin-us-east5", "scratch/y") is None                                      # nothing covers it
