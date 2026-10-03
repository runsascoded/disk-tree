import pytest

from dt_cloud.deploy import data_bucket, site_url


def test_set(monkeypatch):
    monkeypatch.setenv("DATA_BUCKET", " my-data ")
    assert (data_bucket(), site_url(), site_url("https://other.example/")) == ("my-data", "https://site.example.org", "https://other.example")


def test_unset_is_an_error_naming_the_variable(monkeypatch):
    for v in ("DATA_BUCKET", "SITE_URL", "GCS_USAGE_URL"):
        monkeypatch.delenv(v, raising=False)
    with pytest.raises(SystemExit) as e:
        data_bucket()
    assert str(e.value) == "DATA_BUCKET is unset: export the deployment's data bucket (snapshots, index tiers, sweep state)"
    with pytest.raises(SystemExit) as e:
        site_url()
    assert str(e.value) == "no site URL: pass -u or export SITE_URL"


def test_site_url_falls_back_to_the_token_tools_var(monkeypatch):
    monkeypatch.delenv("SITE_URL")
    monkeypatch.setenv("GCS_USAGE_URL", "https://legacy.example.org")
    assert site_url() == "https://legacy.example.org"
