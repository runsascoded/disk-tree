"""`disk_tree.tree_build.build_tree` — the shared arbitrary-depth tree linker.

Pins parent→child linking, the relative-floor fold (with the `f` marker), the
derivation of display fields from additive descendant-inclusive quantities, and
— the subtle one — that a folded `(other)` node's attribution is the parent
*minus its kept children*, so the parent's own direct files aren't dropped.
"""

from __future__ import annotations

from disk_tree.tree_build import DirRow, build_tree


def test_links_arbitrary_depth_parent_to_child():
    # Bytes flow straight down (no direct files → no remainder): isolates linking.
    tree = build_tree([
        DirRow(".", 70, 4), DirRow("a", 70, 4), DirRow("a/b", 70, 4), DirRow("a/b/c", 70, 4),
    ], total_bytes=70, min_frac=0.0)
    assert tree == {
        "n": ".", "b": 70, "o": 4,
        "c": [{"n": "a", "b": 70, "o": 4,
               "c": [{"n": "b", "b": 70, "o": 4,
                      "c": [{"n": "c", "b": 70, "o": 4}]}]}],
    }


def test_children_sorted_by_bytes_desc():
    tree = build_tree([
        DirRow(".", 100, 3), DirRow("small", 10, 1), DirRow("big", 80, 1), DirRow("mid", 10, 1),
    ], total_bytes=100, min_frac=0.0)
    assert [c["n"] for c in tree["c"]] == ["big", "small", "mid"]  # stable on ties


def test_subfloor_children_fold_into_marked_other():
    # floor = 20. keep(50) survives; two 5-byte dirs fold; (other) also absorbs
    # the 20 direct-file bytes the parent holds beyond its kids.
    tree = build_tree([
        DirRow(".", 80, 10), DirRow("keep", 50, 4), DirRow("x", 5, 2), DirRow("y", 5, 2),
    ], total_bytes=80, min_frac=0.25)
    assert tree["c"] == [
        {"n": "keep", "b": 50, "o": 4},
        {"n": "(other)", "b": 30, "o": 6, "f": 2},
    ]


def test_no_other_when_remainder_is_dust():
    tree = build_tree([
        DirRow(".", 55, 5), DirRow("keep", 50, 4), DirRow("tiny", 5, 1),
    ], total_bytes=55, min_frac=0.36)  # floor 19; remainder 5 < floor → dropped
    assert tree["c"] == [{"n": "keep", "b": 50, "o": 4}]


def test_leaf_parent_gets_no_c_key():
    tree = build_tree([DirRow(".", 40, 4), DirRow("solo", 40, 4)], total_bytes=40, min_frac=0.0)
    assert tree["c"] == [{"n": "solo", "b": 40, "o": 4}]
    assert "c" not in tree["c"][0]


def test_display_fields_derived_from_additive():
    # wts/wb → mean day 25 (216000000/100/86400); maps sorted most-bytes-first.
    tree = build_tree([
        DirRow(".", 100, 2, {"wts": 216_000_000, "wb": 100,
                             "cb": {"2": 40, "3": 60}, "tm": {"Stanford": 60, "OA": 40},
                             "ub": {"ryan": 40}, "sh": {"Stanford": 60}}),
        DirRow("a", 100, 2, {"wts": 216_000_000, "wb": 100, "tm": {"OA": 100}}),
    ], total_bytes=100, min_frac=0.0)
    assert tree["c"][0] == {"n": "a", "b": 100, "o": 2, "d": 25, "tm": {"OA": 100}}
    assert tree["d"] == 25
    assert tree["cb"] == {"3": 60, "2": 40}
    assert tree["tm"] == {"Stanford": 60, "OA": 40}
    assert tree["sh"] == {"Stanford": 60}
    assert tree["us"] == [["ryan", 40]]


def test_other_attribution_is_parent_minus_kept_not_sum_of_folded():
    # The correctness crux. Parent totals OA=400, Stanford=600 (desc-inclusive).
    # `keep` accounts for OA=400. Everything else — folded dir + the parent's
    # own direct files — is Stanford, and must all land on (other): summing the
    # folded child alone (say it were empty of attribution) would drop it.
    tree = build_tree([
        DirRow(".", 1000, 20, {"tm": {"OA": 400, "Stanford": 600}, "cb": {"3": 300},
                               "wts": 600 * 86400 * 30 + 400 * 86400 * 10, "wb": 1000}),
        DirRow("keep", 400, 4, {"tm": {"OA": 400}, "wts": 400 * 86400 * 10, "wb": 400}),
        DirRow("f1", 300, 6, {"tm": {"Stanford": 300}, "cb": {"3": 300}, "wts": 300 * 86400 * 30, "wb": 300}),
    ], total_bytes=1000, min_frac=0.35)  # floor 350: keep(400) kept, f1(300) folds
    assert tree["c"][0]["n"] == "keep"
    other = tree["c"][-1]
    # rest = 1000 − 400 = 600 (f1's 300 + 300 direct files), all Stanford.
    assert (other["n"], other["b"], other["o"], other["f"]) == ("(other)", 600, 16, 1)
    assert other["tm"] == {"Stanford": 600}       # parent 600 − kept 0, NOT f1's 300
    assert other["cb"] == {"3": 300}
    assert other["d"] == 30                         # residual wts/wb → day 30
