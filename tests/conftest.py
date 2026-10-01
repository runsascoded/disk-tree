"""Shared fixtures: a fake `aws` CLI, so `disk-tree index s3://…` runs end to
end (subprocess included) against a canned `aws s3 ls --recursive` listing."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Callable

import pytest


@pytest.fixture
def fake_aws(tmp_path: Path) -> Callable[[str], dict[str, str]]:
    """`fake_aws(listing)` writes an `aws` executable that prints `listing` (an
    `aws s3 ls --recursive` body) whatever its args, and returns the env
    (`PATH`) that puts it first."""
    bin_dir = tmp_path / 'fake-bin'

    def install(listing: str) -> dict[str, str]:
        bin_dir.mkdir(exist_ok=True)
        (bin_dir / 'listing.txt').write_text(listing)
        aws = bin_dir / 'aws'
        aws.write_text(f'#!/bin/sh\ncat "{bin_dir / "listing.txt"}"\n')
        aws.chmod(aws.stat().st_mode | stat.S_IXUSR)
        return {'PATH': f'{bin_dir}{os.pathsep}{os.environ["PATH"]}'}

    return install
