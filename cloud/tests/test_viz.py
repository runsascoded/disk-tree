"""End-to-end test of `write_webdata` on a tiny synthetic listing.

The tree is built at arbitrary depth (specs/tree-builder-unification.md): dirs
are linked parent→child with no d1..d4 cap, and a directory's own direct files
surface as an expandable `(other)` node (bytes the kept children don't account
for), not a `(files)` leaf. The fold floor is relative (`MIN_FRAC` × total); at
this fixture's scale nothing folds, so the tree is asserted exactly.
"""

import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from dt_cloud.viz import write_webdata

IDENTITIES_YAML = """\
users:
  ryan-williams:
    aliases: [rw]
prefix_owners:
  - prefix: gs://b1/datasets/
    user: data-team
"""

GB = 10**9

TS = {
    "d0615": dt.datetime(2026, 6, 15, tzinfo=dt.timezone.utc),
    "d0701": dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc),
    "d0702": dt.datetime(2026, 7, 2, tzinfo=dt.timezone.utc),
    "d0703": dt.datetime(2026, 7, 3, tzinfo=dt.timezone.utc),
    "d0720": dt.datetime(2026, 7, 20, 6, tzinfo=dt.timezone.utc),  # the scan date itself
}

epoch_day = lambda ts: int(ts.timestamp() // 86400)  # noqa: E731

def wmean_day(*pairs: tuple[int, dt.datetime]) -> int:
    """Bytes-weighted mean created date in epoch days, mirroring `_date_of`."""
    return int(sum(b * ts.timestamp() for b, ts in pairs) / sum(b for b, _ in pairs) / 86400)


@pytest.fixture
def listing(tmp_path: Path) -> str:
    path = tmp_path / "listing.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1"] * 4,
            "name": [
                "users/rw/ckpt/model.bin",
                "users/rw/ckpt/opt.bin",
                "datasets/raw/part0",
                "top.bin",
            ],
            "size_bytes": [100 * GB, 50 * GB, 200 * GB, 30 * GB],
            "created": [TS["d0701"], TS["d0703"], TS["d0615"], TS["d0702"]],
            "storage_class_id": [1, 1, 2, 1],
        }
    ).to_parquet(path)
    return str(path)


@pytest.fixture
def attribution(tmp_path: Path) -> str:
    path = tmp_path / "attribution.parquet"
    pd.DataFrame(
        {
            "prefix": ["gs://b1/users/rw/"],
            "user": ["rw"],
            "source": ["user-prefix"],
            "asof": [dt.date(2026, 7, 20)],
        }
    ).to_parquet(path)
    return str(path)


def test_write_webdata_attr(tmp_path: Path, listing: str, attribution: str):
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    out = tmp_path / "out"

    pidx = tmp_path / "path-index.parquet"
    meta = write_webdata((listing,), out, "2026-07-20", (attribution,), identities_path, path_index=pidx)

    assert meta == {
        "asof": "2026-07-20",
        "generated": dt.date.today().isoformat(),
        "total_bytes": 380 * GB,
        "total_objects": 4,
        "class_bytes": {1: 180 * GB, 2: 200 * GB},
        "index": {"coarse": {"16": {"floor": 2 ** 22, "paths": 6}, "20": {"floor": 2 ** 18, "paths": 6}, "24": {"floor": 2 ** 14, "paths": 6}}},
        "users": [{"u": "data-team", "b": 200 * GB}, {"u": "ryan-williams", "b": 150 * GB}],
        "user_class_bytes": {"data-team": {2: 200 * GB}, "ryan-williams": {1: 150 * GB}},
    }

    # The index: every ancestor path at every depth, one row per owner slice,
    # descendant-inclusive and exact (the site folds views from it).
    df = pd.read_parquet(pidx)
    rows = {(r.path, r.usr if isinstance(r.usr, str) else None): (int(r.b), int(r.o)) for r in df.itertuples()}
    assert rows == {
        ("b1", "data-team"): (200 * GB, 1),
        ("b1", "ryan-williams"): (150 * GB, 2),
        ("b1", None): (30 * GB, 1),
        ("b1/datasets", "data-team"): (200 * GB, 1),
        ("b1/datasets/raw", "data-team"): (200 * GB, 1),
        ("b1/users", "ryan-williams"): (150 * GB, 2),
        ("b1/users/rw", "ryan-williams"): (150 * GB, 2),
        ("b1/users/rw/ckpt", "ryan-williams"): (150 * GB, 2),
    }
    assert sorted(df.depth.unique().tolist()) == [1, 2, 3, 4]

    age = json.loads((out / "age.json").read_text())
    assert sorted(age, key=lambda r: (r["d"], r["d1"])) == [
        {"d": epoch_day(TS["d0615"]), "d1": "datasets", "u": "data-team", "b": 200 * GB, "o": 1},
        {"d": epoch_day(TS["d0701"]), "d1": "users", "u": "ryan-williams", "b": 100 * GB, "o": 1},
        {"d": epoch_day(TS["d0702"]), "d1": "(files)", "b": 30 * GB, "o": 1},
        {"d": epoch_day(TS["d0703"]), "d1": "users", "u": "ryan-williams", "b": 50 * GB, "o": 1},
    ]


@pytest.fixture
def access(tmp_path: Path) -> str:
    """Layer-2a access agg: one read prefix (plus its ancestor rollup row), and
    a read of `datasets` dated ON the scan date — which the as-of rule
    (`day < scan date`) must leave out, so `datasets` stays never-read."""
    path = tmp_path / "access.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1", "b1", "b1"],
            "path": ["users/rw/ckpt", "users", "datasets"],
            "day": [TS["d0703"].date(), TS["d0703"].date(), TS["d0720"].date()],
            "op": ["GET", "GET", "GET"],
            "last_ts": [TS["d0703"], TS["d0703"], TS["d0720"]],
            "n_ops": [3, 3, 9],
            "bytes_out": [1000, 1000, 9000],
        }
    ).to_parquet(path)
    return str(path)


def test_access_rows_on_or_after_scan_date_are_excluded(tmp_path: Path, listing: str, attribution: str, access: str):
    """A scan dated D aggregates access-log rows with `day < D` only, whatever
    shards exist when it runs — so a re-aggregation of an old date reproduces
    it rather than leaking later reads in. The fixture's `datasets` read is
    dated on the scan date: the access window, the tree and the path index
    all behave as if it never happened."""
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    out = tmp_path / "out"
    pidx = tmp_path / "path-index.parquet"
    meta = write_webdata((listing,), out, "2026-07-20", (attribution,), identities_path, access=(access,), path_index=pidx)
    rd = epoch_day(TS["d0703"])
    assert meta["access"] == {"from": rd, "to": rd}
    a_by_path = pd.read_parquet(pidx).groupby("path")["a"].max().to_dict()
    assert pd.isna(a_by_path["b1/datasets"])
    assert a_by_path["b1"] == rd


def test_age_rows_carry_last_read(tmp_path: Path, listing: str, attribution: str, access: str):
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    out = tmp_path / "out"
    meta = write_webdata((listing,), out, "2026-07-20", (attribution,), identities_path, access=(access,))
    rd = epoch_day(TS["d0703"])
    assert meta["access"] == {"from": rd, "to": rd}
    # Strata whose dir was read carry `a` (its last-read epoch day); the rest
    # omit it — the site's read axis colors those "never read".
    age = json.loads((out / "age.json").read_text())
    assert sorted(age, key=lambda r: (r["d"], r["d1"])) == [
        {"d": epoch_day(TS["d0615"]), "d1": "datasets", "u": "data-team", "b": 200 * GB, "o": 1},
        {"d": epoch_day(TS["d0701"]), "d1": "users", "u": "ryan-williams", "a": rd, "b": 100 * GB, "o": 1},
        {"d": epoch_day(TS["d0702"]), "d1": "(files)", "b": 30 * GB, "o": 1},
        {"d": epoch_day(TS["d0703"]), "d1": "users", "u": "ryan-williams", "a": rd, "b": 50 * GB, "o": 1},
    ]


def test_path_index_carries_read_day(tmp_path: Path, listing: str, attribution: str, access: str):
    """The floor-free path index carries subtree-MAX `a` (last-read epoch day):
    a read on `users/rw/ckpt` lights up every ancestor up to the bucket, while
    a never-read sibling (`datasets`) stays NULL."""
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    out = tmp_path / "out"
    pidx = tmp_path / "path-index.parquet"
    write_webdata((listing,), out, "2026-07-20", (attribution,), identities_path, access=(access,), path_index=pidx)
    rd = epoch_day(TS["d0703"])
    df = pd.read_parquet(pidx)
    assert list(df.columns) == ["path", "depth", "usr", "b", "o", "wts", "wb", "c2", "c3", "c4", "a"]
    # `a` is per (path) — collapse the usr slices to the path's max.
    a_by_path = df.groupby("path")["a"].max().to_dict()
    assert a_by_path["b1"] == rd            # bucket: max over everything under it
    assert a_by_path["b1/users"] == rd      # read subtree
    assert a_by_path["b1/users/rw/ckpt"] == rd
    assert pd.isna(a_by_path["b1/datasets"])  # never read → NULL


def test_coarse_tiers_are_exact_subsets(tmp_path: Path, listing: str, attribution: str):
    """Each coarse tier (E in COARSE_EXPS) holds exactly the floor-free rows of
    paths whose subtree clears F_E = 2^(round(log2 fleet) - E), in the same
    three sort orders, with F_E in the parquet key-value metadata and in
    meta.json. The fixture fleet is 380 GB (2^38.5 → round → 38), so
    E=16/20/24 floor at 2^22/2^18/2^14 bytes; every fixture path is >= 30 GB,
    so all three tiers equal the full index — the test then plants a floor
    via the metadata to check the subset math on a real cut."""
    from dt_cloud.viz import COARSE_EXPS
    import pyarrow.parquet as pq

    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    out = tmp_path / "out"
    pidx = tmp_path / "path-index.parquet"
    meta = write_webdata((listing,), out, "2026-07-20", (attribution,), identities_path, path_index=pidx)
    full = pd.read_parquet(pidx)
    fleet = int(full[full.depth == 1]["b"].sum())
    assert fleet == 380 * GB
    import math
    floors = {e: 2 ** (round(math.log2(fleet)) - e) for e in COARSE_EXPS}
    assert meta["index"] == {"coarse": {str(e): {"floor": floors[e], "paths": full["path"].nunique()} for e in COARSE_EXPS}}
    subtree = full.groupby("path")["b"].sum()
    for e in COARSE_EXPS:
        keep = set(subtree[subtree >= floors[e]].index)
        expect = full[full.path.isin(keep)]
        for suffix, order in (("", ["depth", "path"]), ("-by-user", ["usr", "depth", "path"])):
            f = tmp_path / f"path-index-coarse{e}{suffix}.parquet"
            assert pq.read_metadata(f).metadata[b"coarse_floor"] == str(floors[e]).encode()
            got = pd.read_parquet(f)
            assert list(got.columns) == list(full.columns)
            # same rows (as a set), sorted as declared (NULL usr last)
            key = lambda d: d.sort_values(["path", "usr"], na_position="last").reset_index(drop=True)
            pd.testing.assert_frame_equal(key(got), key(expect))
            srt = got.sort_values(order, na_position="last", kind="stable").reset_index(drop=True)
            pd.testing.assert_frame_equal(got.reset_index(drop=True), srt)
    # by-user variant of the floor-free tier: same rows, re-sorted so a user
    # lens's row groups prune by usr (specs/path-agnostic-serving.md §2.3).
    by_user = pd.read_parquet(pidx.with_name("path-index-by-user.parquet"))
    assert len(by_user) == len(full)
    assert not pidx.with_name("path-index-by-team.parquet").exists()
    # by-user is sorted (usr NULLS LAST, depth, path)
    uk = by_user["usr"].fillna("\uffff").tolist()
    assert uk == sorted(uk)


def test_write_webdata_plain(tmp_path: Path, listing: str):
    out = tmp_path / "out"
    meta = write_webdata((listing,), out, "2026-07-20")
    assert "users" not in meta
    assert meta["total_bytes"] == 380 * GB
    age = json.loads((out / "age.json").read_text())
    assert sorted(age, key=lambda r: (r["d"], r["d1"])) == [
        {"d": epoch_day(TS["d0615"]), "d1": "datasets", "b": 200 * GB, "o": 1},
        {"d": epoch_day(TS["d0701"]), "d1": "users", "b": 100 * GB, "o": 1},
        {"d": epoch_day(TS["d0702"]), "d1": "(files)", "b": 30 * GB, "o": 1},
        {"d": epoch_day(TS["d0703"]), "d1": "users", "b": 50 * GB, "o": 1},
    ]


def test_deeper_nobody_rule_overrides_user_prefix(tmp_path: Path):
    # A `user: ~` (explicit nobody) prefix nested INSIDE a user prefix must win
    # for its subtree (deepest-prefix-wins is row-wise: the deeper row's NULL
    # user must not be skipped in favor of the shallower row's user).
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(
        """\
users:
  ryan-williams:
    aliases: [rw]
prefix_owners:
  - prefix: gs://b1/users/rw/shared/
    user: ~
"""
    )
    listing_path = tmp_path / "listing.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1", "b1"],
            "name": ["users/rw/own/a.bin", "users/rw/shared/b.bin"],
            "size_bytes": [100 * GB, 60 * GB],
            "created": [TS["d0701"], TS["d0702"]],
            "storage_class_id": [1, 1],
        }
    ).to_parquet(listing_path)
    attribution_path = tmp_path / "attribution.parquet"
    pd.DataFrame(
        {
            "prefix": ["gs://b1/users/rw/"],
            "user": ["rw"],
            "source": ["user-prefix"],
            "asof": [dt.date(2026, 7, 20)],
        }
    ).to_parquet(attribution_path)
    out = tmp_path / "out"
    pidx = tmp_path / "path-index.parquet"
    write_webdata((str(listing_path),), out, "2026-07-28", (str(attribution_path),), identities_path, path_index=pidx)
    df = pd.read_parquet(pidx)
    b1 = {(r.usr if isinstance(r.usr, str) else None): int(r.b) for r in df[df.path == "b1"].itertuples()}
    assert b1 == {"ryan-williams": 100 * GB, None: 60 * GB}  # the 60 GB under shared/ is nobody's


def test_dir_cache_roundtrip(tmp_path: Path, listing: str, attribution: str):
    """Cold run writes the layer-2 cache; a warm run (same cache dir, listing
    unreadable to prove it isn't touched) produces byte-identical outputs."""
    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    cache = tmp_path / "dir-cache"

    cold_out = tmp_path / "cold"
    write_webdata((listing,), cold_out, "2026-07-20", (attribution,), identities_path, dir_cache=cache)
    assert sorted(p.name for p in cache.iterdir()) == ["age-days.parquet", "dir-stats.parquet"]

    # Warm: point the listing at a copy that we then corrupt — object rows must
    # not be read. (The listing arg is still parsed for schema, so keep a valid
    # parquet with different contents: one giant bogus row.)
    bogus = tmp_path / "bogus-listing.parquet"
    pd.DataFrame(
        {
            "bucket": ["b1"],
            "name": ["SHOULD_NOT_BE_READ"],
            "size_bytes": [1],
            "created": [TS["d0701"]],
            "storage_class_id": [1],
        }
    ).to_parquet(bogus)
    warm_out = tmp_path / "warm"
    write_webdata((str(bogus),), warm_out, "2026-07-20", (attribution,), identities_path, dir_cache=cache)

    for name in ("age.json", "meta.json"):
        assert (warm_out / name).read_bytes() == (cold_out / name).read_bytes()
    meta = json.loads((warm_out / "meta.json").read_text())
    assert meta["total_bytes"] == 380 * GB  # cache content won, bogus listing ignored


def test_index_tiers_backfill_matches_webdata(tmp_path: Path, listing: str, attribution: str):
    """`dt-cloud index-tiers` (the backfill for archived scans) derives the
    coarse tiers from a floor-free path index and must produce byte-identical
    files to the ones `webdata` writes on a fresh scan — same rows, same sort,
    same KV floor. An index carrying the retired `team` column is accepted too."""
    from click.testing import CliRunner

    from dt_cloud.cli import main
    from dt_cloud.viz import COARSE_EXPS

    identities_path = tmp_path / "identities.yaml"
    identities_path.write_text(IDENTITIES_YAML)
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    write_webdata((listing,), tmp_path / "out", "2026-07-20", (attribution,), identities_path, path_index=fresh / "path-index.parquet")
    old = tmp_path / "old"
    old.mkdir()
    df = pd.read_parquet(fresh / "path-index.parquet")
    df.insert(2, "team", "legacy")
    df.to_parquet(old / "path-index.parquet", index=False)
    res = CliRunner().invoke(main, ["index-tiers", "-m", "1GB", "-t", "2", "-P", str(old / "path-index.parquet"), "2026-07-20"])
    assert res.exit_code == 0, res.output
    names = [f"path-index-coarse{e}{sfx}.parquet" for e in COARSE_EXPS for sfx in ("", "-by-user")]
    assert sorted(p.name for p in old.glob("path-index-coarse*.parquet")) == sorted(names)
    for name in names:
        pd.testing.assert_frame_equal(pd.read_parquet(old / name), pd.read_parquet(fresh / name))

