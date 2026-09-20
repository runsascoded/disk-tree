"""`dt_cloud.lifecycle`: pull / diff / push against a fake S3 client."""
import json

import pytest

from dt_cloud import lifecycle as L

TTL = {"ID": "marin-ttl-1d", "Filter": {"Prefix": "tmp/ttl=1d/"}, "Status": "Enabled", "Expiration": {"Days": 1}}
MPU = {"ID": "marin-abort-incomplete-mpu", "Filter": {"Prefix": ""}, "Status": "Enabled",
       "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}


class _ClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Exceptions:
    ClientError = _ClientError


class _FakeS3:
    """Holds one bucket's lifecycle config; `puts` records every PUT body."""

    exceptions = _Exceptions

    def __init__(self, rules: list[dict] | None):
        self.rules = rules
        self.puts: list[list[dict]] = []

    def get_bucket_lifecycle_configuration(self, Bucket: str) -> dict:
        if self.rules is None:
            raise _ClientError("NoSuchLifecycleConfiguration")
        return {"Rules": list(self.rules)}

    def put_bucket_lifecycle_configuration(self, Bucket: str, LifecycleConfiguration: dict) -> None:
        self.puts.append(LifecycleConfiguration["Rules"])
        self.rules = list(LifecycleConfiguration["Rules"])


def test_normalize_sorts_by_id():
    assert L.normalize([TTL, MPU]) == [MPU, TTL]


def test_gc_rule():
    assert L.gc_rule() == {
        "ID": "cw-noncurrent-gc",
        "Filter": {"Prefix": ""},
        "Status": "Enabled",
        "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
        "Expiration": {"ExpiredObjectDeleteMarker": True},
    }
    assert L.gc_rule(days=7, prefix="tmp/")["NoncurrentVersionExpiration"] == {"NoncurrentDays": 7}
    assert L.gc_rule(days=7, prefix="tmp/")["Filter"] == {"Prefix": "tmp/"}


def test_pull_normalizes_and_handles_no_config():
    assert L.pull(_FakeS3([TTL, MPU]), "b") == [MPU, TTL]
    assert L.pull(_FakeS3(None), "b") == []


def test_diff():
    changed_ttl = {**TTL, "Expiration": {"Days": 2}}
    assert L.diff([MPU, changed_ttl, L.gc_rule()], [TTL, MPU]) == {
        "added": ["cw-noncurrent-gc"],
        "removed": [],
        "changed": ["marin-ttl-1d"],
    }
    assert L.diff([MPU], [TTL, MPU]) == {"added": [], "removed": ["marin-ttl-1d"], "changed": []}
    assert L.diff([TTL, MPU], [MPU, TTL]) == {"added": [], "removed": [], "changed": []}


def test_push_replaces_whole_config_and_round_trips():
    s3 = _FakeS3([TTL, MPU])
    live = L.push(s3, "b", [MPU, TTL, L.gc_rule()])
    assert s3.puts == [[L.gc_rule(), MPU, TTL]]  # one PUT, the full normalized set
    assert live == [L.gc_rule(), MPU, TTL]


def test_push_raises_when_read_back_differs():
    class _Lossy(_FakeS3):
        def put_bucket_lifecycle_configuration(self, Bucket, LifecycleConfiguration):
            super().put_bucket_lifecycle_configuration(Bucket, LifecycleConfiguration)
            self.rules = [r for r in self.rules if r["ID"] != L.GC_RULE_ID]  # the store "dropped" a rule

    with pytest.raises(RuntimeError, match=r"did not round-trip on b: .*'added': \['cw-noncurrent-gc'\]"):
        L.push(_Lossy([TTL, MPU]), "b", [MPU, TTL, L.gc_rule()])


def test_dump_load_round_trip(tmp_path):
    p = tmp_path / "lifecycle.json"
    p.write_text(L.dump([TTL, MPU]))
    assert p.read_text() == L.dump([MPU, TTL])
    assert L.load(str(p)) == [MPU, TTL]


def test_push_refuses_when_live_moved_since_base():
    s3 = _FakeS3([TTL, MPU])
    base = L.pull(s3, "b")
    s3.rules = [MPU]  # someone removed the TTL rule after we read `base`
    with pytest.raises(L.LifecycleRaced, match=r"live rules changed since they were read: .*'removed': \['marin-ttl-1d'\]"):
        L.push(s3, "b", [MPU, TTL, L.gc_rule()], base=base)
    assert s3.puts == []  # nothing written
    # with an up-to-date base the same push goes through
    assert L.push(s3, "b", [MPU, L.gc_rule()], base=L.pull(s3, "b")) == [L.gc_rule(), MPU]


def test_dump_map_keeps_bucket_order_and_normalizes():
    assert L.dump_map({"marin-us-east-02a": [TTL, MPU], "hero-checkpoints": [TTL]}) == (
        json.dumps({"marin-us-east-02a": [MPU, TTL], "hero-checkpoints": [TTL]}, indent=2) + "\n"
    )
