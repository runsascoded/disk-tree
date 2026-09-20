"""`dt_cloud.lifecycle`: pull / diff / push against fake S3 and GCS clients."""
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
        def put_bucket_lifecycle_configuration(self, Bucket: str, LifecycleConfiguration: dict) -> None:
            self.rules = [MPU]  # the "service" drops a rule

    with pytest.raises(RuntimeError, match="did not round-trip"):
        L.push(_Lossy([TTL]), "b", [TTL, MPU])


def test_push_refuses_when_live_moved_since_the_diff():
    s3 = _FakeS3([TTL])
    with pytest.raises(L.LifecycleRaced):
        L.push(s3, "b", [TTL, MPU], base=[TTL, MPU])  # caller diffed against a state that isn't live
    assert s3.puts == []


# --- GCS ---------------------------------------------------------------------

G_TTL1 = {"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": ["tmp/ttl=1d/"]}}
G_TTL14 = {"action": {"type": "Delete"}, "condition": {"age": 14, "matchesPrefix": ["tmp/ttl=14d/"]}}
G_COLD = {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"}, "condition": {"age": 90}}


class _RuleDict(dict):
    """The client yields dict *subclasses* (`LifecycleRuleDelete`, …)."""


class _FakeGcsBucket:
    def __init__(self, store: "_FakeGcs", name: str):
        self._store, self.name = store, name
        self._rules: list[dict] | None = None

    def reload(self) -> None:
        self._rules = list(self._store.rules.get(self.name, []))

    @property
    def lifecycle_rules(self):
        assert self._rules is not None, "reload() first"
        return (_RuleDict(r) for r in self._rules)

    @lifecycle_rules.setter
    def lifecycle_rules(self, rules: list[dict]) -> None:
        self._pending = list(rules)

    def patch(self) -> None:
        self._store.patches.append((self.name, self._pending))
        self._store.rules[self.name] = list(self._pending)


class _FakeGcs:
    def __init__(self, rules: dict[str, list[dict]]):
        self.rules = rules
        self.patches: list[tuple[str, list[dict]]] = []

    def bucket(self, name: str) -> _FakeGcsBucket:
        return _FakeGcsBucket(self, name)


def test_gcs_key_and_normalize_are_content_based():
    assert L.gcs_key(G_TTL1) == '{"action":{"type":"Delete"},"condition":{"age":1,"matchesPrefix":["tmp/ttl=1d/"]}}'
    assert L.normalize_gcs([_RuleDict(G_TTL14), _RuleDict(G_TTL1), G_COLD]) == [G_COLD, G_TTL1, G_TTL14]
    assert all(type(r) is dict for r in L.normalize_gcs([_RuleDict(G_TTL1)]))


def test_pull_gcs_reloads_and_normalizes():
    gcs = _FakeGcs({"b": [G_TTL14, G_TTL1], "empty": []})
    assert L.pull_gcs(gcs, "b") == [G_TTL1, G_TTL14]
    assert L.pull_gcs(gcs, "empty") == []


def test_diff_gcs_has_no_changed_only_added_and_removed():
    assert L.diff_gcs([G_TTL1, G_COLD], [G_TTL1, G_TTL14]) == {
        "added": [L.gcs_key(G_COLD)],
        "removed": [L.gcs_key(G_TTL14)],
        "changed": [],
    }
    assert L.diff_gcs([G_TTL14, G_TTL1], [G_TTL1, G_TTL14]) == {"added": [], "removed": [], "changed": []}


def test_push_gcs_patches_whole_set_and_round_trips():
    gcs = _FakeGcs({"b": [G_TTL1]})
    live = L.push_gcs(gcs, "b", [G_TTL14, G_TTL1], base=[G_TTL1])
    assert gcs.patches == [("b", [G_TTL1, G_TTL14])]
    assert live == [G_TTL1, G_TTL14]


def test_push_gcs_refuses_when_live_moved():
    gcs = _FakeGcs({"b": [G_TTL1, G_COLD]})
    with pytest.raises(L.LifecycleRaced):
        L.push_gcs(gcs, "b", [G_TTL14], base=[G_TTL1])
    assert gcs.patches == []


def test_scheme_dispatch_and_pull_many():
    assert L.is_gcs("gs://x") and not L.is_gcs("x")
    assert L.bucket_name("gs://marin-us-east5") == "marin-us-east5"
    assert L.bucket_name("marin-us-east-02a") == "marin-us-east-02a"
    gcs = _FakeGcs({"g1": [G_TTL14, G_TTL1], "g2": [G_COLD]})
    s3 = _FakeS3([TTL, MPU])
    assert L.pull_many(["gs://g2", "s3b", "gs://g1"], s3=s3, gcs=gcs) == {
        "g1": [G_TTL1, G_TTL14],
        "g2": [G_COLD],
        "s3b": [MPU, TTL],
    }
    with pytest.raises(ValueError, match="GCS client is required"):
        L.pull_any("gs://g1", s3=s3)
    assert L.diff_any("gs://g1", [G_TTL1], [G_TTL1]) == {"added": [], "removed": [], "changed": []}
    assert L.diff_any("b", [TTL], [MPU]) == {"added": ["marin-ttl-1d"], "removed": ["marin-abort-incomplete-mpu"], "changed": []}


def test_dump_and_load(tmp_path):
    p = tmp_path / "b.json"
    p.write_text(L.dump([G_TTL14, G_TTL1], bucket="gs://b"))
    assert L.load(str(p)) == [G_TTL1, G_TTL14]
    snap = L.dump({"g2": [G_COLD], "g1": [G_TTL14, G_TTL1]}, bucket="gs://")
    assert snap.startswith('{\n  "g1": [\n')  # keyed, bucket-sorted, normalized
    (tmp_path / "snap.json").write_text(snap)
    with pytest.raises(ValueError, match="expected a JSON list"):
        L.load(str(tmp_path / "snap.json"))
