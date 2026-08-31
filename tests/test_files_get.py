"""Tests for `GET /api/files/get` — raw file bytes with HTTP Range support,
backing `<FileTree>`'s `Store.get` (Half A, spec `half-a-adopt-filetree.md`)."""

from __future__ import annotations

from pathlib import Path

import pytest

from disk_tree.server import app


@pytest.fixture
def client():
    return app.test_client()


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    f = tmp_path / 'data.bin'
    f.write_bytes(bytes(range(256)))  # 256 bytes, value == index
    return f


def test_full_get_returns_all_bytes(client, sample: Path):
    res = client.get(f'/api/files/get?path={sample}')
    assert res.status_code == 200
    assert res.data == bytes(range(256))
    assert res.headers['Accept-Ranges'] == 'bytes'
    assert res.headers['Content-Length'] == '256'


def test_range_get_returns_partial_206(client, sample: Path):
    res = client.get(f'/api/files/get?path={sample}', headers={'Range': 'bytes=10-19'})
    assert res.status_code == 206
    assert res.data == bytes(range(10, 20))
    assert res.headers['Content-Range'] == 'bytes 10-19/256'
    assert res.headers['Content-Length'] == '10'


def test_open_ended_range_runs_to_eof(client, sample: Path):
    res = client.get(f'/api/files/get?path={sample}', headers={'Range': 'bytes=250-'})
    assert res.status_code == 206
    assert res.data == bytes(range(250, 256))
    assert res.headers['Content-Range'] == 'bytes 250-255/256'


def test_suffix_range_returns_last_n_bytes(client, sample: Path):
    res = client.get(f'/api/files/get?path={sample}', headers={'Range': 'bytes=-4'})
    assert res.status_code == 206
    assert res.data == bytes(range(252, 256))
    assert res.headers['Content-Range'] == 'bytes 252-255/256'


def test_unsatisfiable_range_416(client, sample: Path):
    res = client.get(f'/api/files/get?path={sample}', headers={'Range': 'bytes=999-1005'})
    assert res.status_code == 416
    assert res.headers['Content-Range'] == 'bytes */256'


def test_missing_file_404(client, tmp_path: Path):
    res = client.get(f'/api/files/get?path={tmp_path / "nope.bin"}')
    assert res.status_code == 404


def test_cloud_path_501(client):
    res = client.get('/api/files/get?path=s3://bucket/key')
    assert res.status_code == 501


def test_relative_path_400(client):
    res = client.get('/api/files/get?path=relative/x')
    assert res.status_code == 400
