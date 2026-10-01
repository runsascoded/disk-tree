"""`backend_for`: scheme → backend, including `r2://` through the S3 backend
with the bucket's endpoint, and a loud refusal for `gcs://` (which used to
fall through to a filesystem walk and "succeed" with an empty scan) and for
local paths (this engine scans object stores only).
"""

import re
from io import StringIO
from os.path import dirname, join
from unittest.mock import MagicMock, patch

import pytest

from disk_tree import find
from disk_tree.backends import S3Backend, UnsupportedBackend, backend_for
from disk_tree.blobfs import R2_ENDPOINT_VAR

TESTDATA = join(dirname(__file__), 'data')
EP = 'https://acct.r2.cloudflarestorage.com'


def test_dispatch_by_scheme(monkeypatch):
    monkeypatch.setenv(R2_ENDPOINT_VAR, EP)
    local = backend_for('/Users/x')
    assert (type(local), local.scheme) == (UnsupportedBackend, 'file')
    s3 = backend_for('s3://bk/p')
    assert (type(s3), s3.scheme, s3.endpoint_url) == (S3Backend, 's3', None)
    r2 = backend_for('r2://bk/p')
    assert (type(r2), r2.scheme, r2.endpoint_url) == (S3Backend, 'r2', EP)
    gcs = backend_for('gcs://bk')
    assert (type(gcs), gcs.scheme) == (UnsupportedBackend, 'gcs')


def test_r2_lists_through_the_endpoint_with_r2_uris(monkeypatch):
    monkeypatch.setenv(R2_ENDPOINT_VAR, EP)
    with open(join(TESTDATA, 's3.txt')) as f:
        listing = f.read()
    with patch('subprocess.Popen') as popen:
        proc = MagicMock()
        proc.stdout = StringIO(listing)
        popen.return_value = proc
        result = find.index('r2://runsascoded/gopro')
    # The aws CLI is invoked against the endpoint with an `s3://` URL…
    assert popen.call_args.args[0] == ['aws', '--endpoint-url', EP, 's3', 'ls', '--recursive', 's3://runsascoded/gopro']
    # …and the scan is the same shape as the s3 fixture's, with `r2://` uris.
    with patch('subprocess.Popen') as popen2:
        proc2 = MagicMock()
        proc2.stdout = StringIO(listing)
        popen2.return_value = proc2
        expected = find.index('s3://runsascoded/gopro').df
    expected['uri'] = expected['uri'].str.replace('^s3://', 'r2://', regex=True)
    assert result.df.equals(expected)
    assert result.df['uri'].str.startswith('r2://').all()


def test_r2_without_an_endpoint_refuses_at_list_time(monkeypatch, tmp_path):
    monkeypatch.delenv(R2_ENDPOINT_VAR, raising=False)
    # No buckets.yml entry either: point the config root at a fresh dir. (The
    # `DISK_TREE_ROOT` env var alone isn't enough: `config.ROOT_DIR` is read at
    # import, so a real `~/.config/disk-tree/buckets.yml` with a `defaults`
    # endpoint leaked in.)
    from disk_tree import config
    monkeypatch.setenv('DISK_TREE_ROOT', str(tmp_path / 'root'))
    monkeypatch.setattr(config, 'ROOT_DIR', str(tmp_path / 'root'))
    r2 = backend_for('r2://bk')
    assert r2.endpoint_url is None
    with pytest.raises(RuntimeError, match=f'r2:// needs an S3 endpoint — set {R2_ENDPOINT_VAR}'):
        list(r2.list('r2://bk'))


def test_backend_for_threads_bucket_profile(monkeypatch, tmp_path):
    """A per-bucket `profile` in buckets.yml reaches the lister backend, so a
    cross-account source authenticates with its own key (`s3://` and `r2://`);
    an unconfigured bucket gets no profile (ambient credentials)."""
    from disk_tree import config
    monkeypatch.setenv(R2_ENDPOINT_VAR, EP)
    monkeypatch.setattr(config, 'ROOT_DIR', str(tmp_path))
    (tmp_path / 'buckets.yml').write_text(
        'buckets:\n  - uri: r2://ctbk\n    profile: hccs\n  - uri: s3://raw\n    profile: aws-src\n'
    )
    assert backend_for('r2://ctbk/p').profile == 'hccs'
    assert backend_for('s3://raw/p').profile == 'aws-src'
    assert backend_for('s3://unconfigured/p').profile is None


def test_gcs_refuses_live_operations():
    gcs = backend_for('gcs://bk/p')
    msg = "live scanning of gcs:// isn't implemented; import a listing instead (`disk-tree bulk-list` + `disk-tree import -l <listing>`)"
    with pytest.raises(NotImplementedError, match=re.escape(msg)):
        list(gcs.list('gcs://bk/p'))
    with pytest.raises(NotImplementedError, match=r'existence check of gcs://'):
        gcs.exists('gcs://bk/p')


@pytest.mark.parametrize('url', ['/Users/x', 'file:///Users/x', 'rel/dir'])
def test_local_paths_refuse_live_operations(url):
    local = backend_for(url)
    msg = (
        "live scanning of local paths isn't supported; index an `s3://` or `r2://` URL, "
        "or import a listing (`disk-tree bulk-list` + `disk-tree import -l <listing>`)"
    )
    with pytest.raises(NotImplementedError, match=re.escape(msg)):
        list(local.list(url))
    with pytest.raises(NotImplementedError, match=r'existence check of local paths'):
        local.exists(url)
