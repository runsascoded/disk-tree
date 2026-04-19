import re
import subprocess
from datetime import timezone
from dateutil.parser import parse
from os.path import dirname
from subprocess import PIPE
from typing import Iterator
from urllib.parse import urlparse

from tqdm import tqdm
from utz import o

from disk_tree import time

from .base import Backend, ErrorCollector


WS = re.compile(r'\s+')


class S3Backend(Backend):
    """S3 (or S3-compatible) bucket, scanned via `aws s3 ls --recursive`."""

    scheme = 's3'

    def __init__(self, endpoint_url: str | None = None, profile: str | None = None):
        # Support for S3-compatible endpoints (R2, MinIO, ...) — wired later via config
        self.endpoint_url = endpoint_url
        self.profile = profile

    def _aws_cmd(self, subcmd: list[str]) -> list[str]:
        cmd = ['aws']
        if self.profile:
            cmd.extend(['--profile', self.profile])
        if self.endpoint_url:
            cmd.extend(['--endpoint-url', self.endpoint_url])
        cmd.extend(subcmd)
        return cmd

    def list(
        self,
        url: str,
        *,
        errors: ErrorCollector | None = None,
        excludes: list[str] | None = None,
        sudo: bool = False,
    ) -> Iterator[dict]:
        cmd = self._aws_cmd(['s3', 'ls', '--recursive', url])
        proc = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)
        parsed = urlparse(url)
        bkt = parsed.netloc
        key0 = parsed.path.lstrip('/')
        dirs = set()
        with time("s3_files_iter lines"):
            for line in tqdm(proc.stdout):
                strs = WS.split(line.rstrip('\n'), 3)
                mtime_str = f'{strs[0]} {strs[1]}'
                mtime = int(parse(mtime_str).replace(tzinfo=timezone.utc).timestamp())
                size = int(strs[2])
                key = strs[3]
                if key0:
                    if not key.startswith(f'{key0}/') and key != key0:
                        raise ValueError(f"{url}: unexpected {key=}")
                    relpath = key[len(key0) + 1:] if key != key0 else ''
                else:
                    relpath = key
                cur = relpath
                if not cur and not relpath:
                    continue
                new_dirs = []
                while True:
                    try:
                        idx = cur.rindex('/')
                    except ValueError:
                        idx = 0
                    cur = cur[:idx]
                    seen = cur in dirs
                    if not seen:
                        dirs.add(cur)
                        new_dirs.append(cur)
                    if not cur or seen:
                        break

                for d in reversed(new_dirs):
                    yield o(
                        path=d,
                        size=0,
                        mtime=0,
                        kind='dir',
                        parent=dirname(d) if d else None,
                        uri=f's3://{bkt}/{key0}/{d}' if key0 else f's3://{bkt}/{d}',
                    )
                yield o(
                    path=relpath,
                    size=size,
                    mtime=mtime,
                    kind='file',
                    parent=dirname(relpath),
                    uri=f's3://{bkt}/{key}',
                )

    def delete(self, url: str) -> None:
        cmd = self._aws_cmd(['s3', 'rm', '--recursive', url])
        subprocess.run(cmd, check=True)

    def exists(self, url: str) -> bool:
        cmd = self._aws_cmd(['s3', 'ls', url])
        return subprocess.run(cmd, capture_output=True).returncode == 0
