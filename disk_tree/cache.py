from os import walk
from os.path import abspath, dirname, exists, isdir, isfile, islink, join, basename

import os
import pandas as pd
import shlex
from datetime import datetime as dt
from pandas import to_datetime as to_dt
from subprocess import check_call, CalledProcessError
from utz import err

from . import s3
from .config import ROOT_DIR
from .db import db, cache_url
from .model import File, S3


def is_descendant(path, ancestor):
    path = path.rstrip('/').split('/')
    ancestor = ancestor.rstrip('/').split('/')
    if len(path) < len(ancestor):
        return False
    for (l, r) in zip(path, ancestor):
        if l != r:
            return False
    return True


def strip_prefix(key, prefix):
    if not prefix:
        return key
    if key.startswith(prefix):
        return key[len(prefix):]
    else:
        raise ValueError(f"Key {key} doesn't start with expected prefix {prefix}")


class Cache:
    def __init__(self, ttl=None):
        err(f'Using cache: {cache_url}')
        self.ttl = ttl

    def compute_s3(self, url, bucket, root_key):
        now = to_dt(dt.now())
        e = S3.query.get((bucket, root_key))
        if e:
            elem_age = now - e.checked_at
            if elem_age <= self.ttl:
                return e

        S3_LINE_CACHE = join(ROOT_DIR, '.s3/ls')
        s3_cache_path = join(*(
                [S3_LINE_CACHE, bucket] +
                ([root_key] if root_key else [])
        )) + '.txt'
        cached = False
        if exists(s3_cache_path):
            stat = os.stat(s3_cache_path)
            mtime = to_dt(stat.st_mtime, unit='s')
            cache_age = now - mtime
            if cache_age <= self.ttl:
                err(f'Found {s3_cache_path} ({cache_age} old)')
                cached = True

        if not cached:
            parent = dirname(s3_cache_path)
            os.makedirs(parent, exist_ok=True)
            try:
                with open(s3_cache_path, 'w') as f:
                    cmd = ['aws', 's3', 'ls', '--recursive', url]
                    err(f'Running: {shlex.join(cmd)} > {s3_cache_path}')
                    check_call(cmd, stdout=f)
            except CalledProcessError as e:
                os.remove(s3_cache_path)
                raise

        with open(s3_cache_path, 'r') as f:
            lines = [ line.rstrip('\n') for line in f.readlines() ]

        files = pd.DataFrame([ s3.parse_line(line) for line in lines ])
        files = files[~files.key.str.endswith('/')]
        files['relpath'] = files['key'].apply(strip_prefix, prefix=f'{root_key}/' if root_key else None)
        files['root_key'] = root_key
        aggd = s3.agg_dirs(files).sort_values('key')
        # aggd['url'] = f's3://{bucket}/' + aggd['key']
        aggd['bucket'] = bucket
        # aggd['root_key'] = root_key
        #aggd['key'] = aggd['root_key'] + '/' + aggd['relpath']
        aggd['parent'] = aggd['key'].apply(dirname)
        aggd['checked_at'] = now
        keys = [ 'bucket', 'key', 'mtime', 'size', 'parent', 'kind', 'num_descendants', 'checked_at', ]
        for idx, r in aggd.iterrows():
            e = S3(**{
                k: r[k]
                for k in keys
            })
            self.insert(e)
        #aggd = aggd.drop(columns=['root_key'])
        #return aggd
        return S3.query.get((bucket, root_key))

    def compute_file(self, path, now=None, fsck=False, excludes=None):
        path = abspath(path)
        record = self.get(path)
        if record:
            return record
        else:
            return self.compute(path, now=now, fsck=fsck, excludes=excludes)

    def compute(self, path, now=None, fsck=False, excludes=None):
        if excludes and any(is_descendant(path, exclude) for exclude in excludes):
            err(f'skipping excluded: {path}')
            return None
        if islink(path):
            err(f'Skipping symlink: {path}')
            return None
        elif isfile(path):
            if not now:
                now = to_dt(dt.now())
            stat = os.stat(path)
            mtime = to_dt(stat.st_mtime, unit='s')
            size = stat.st_size
            parent = dirname(path)
            file = File(
                path=path,
                mtime=mtime,
                size=size,
                parent=parent,
                kind='file',
                num_descendants=1,
                checked_at=now,
            )
            self.insert(file)
            return file
        elif isdir(path):
            try:
                _, dirs, files = next(walk(path))
            except StopIteration:
                err(f'Error traversing {path}')
                return None
            files_map = { file: self.compute(join(path, file), excludes=excludes) for file in files }
            dirs_map = { dir: self.compute(join(path, dir), excludes=excludes) for dir in dirs }
            all_keys = set(files_map.keys()) | set(dirs_map.keys())
            files = list(filter(None, files_map.values()))
            dirs = list(filter(None, dirs_map.values()))
            children = files + dirs
            db_children = File.query.filter((File.parent == path) & File.path.not_in(excludes)).all()
            expired_children = [ c for c in db_children if basename(c.path) not in all_keys ]
            if expired_children:
                err(f'Cache: expiring {len(expired_children)} stale children of {path}:')
                for child in expired_children:
                    err(f'\t{child.path}')
                    self.expire(child)
            num_descendants = sum( c.num_descendants for c in children )
            size = sum( c.size for c in children )
            parent = dirname(path)
            stat = os.stat(path)
            mtime = to_dt(stat.st_mtime, unit='s')
            if children:
                mtime = max(mtime, max(c.mtime for c in children))
            if not now:
                now = to_dt(dt.now())
            d = File(
                path=path,
                mtime=mtime,
                size=size,
                parent=parent,
                kind='dir',
                num_descendants=num_descendants,
                checked_at=now,
            )
            self.insert(d)
            if fsck:
                descendants = File.query.filter(File.path.startswith(path)).all()
                for descendant in descendants:
                    path = descendant.path
                    if not exists(path):
                        self.expire(descendant)
            return d
        else:
            raise RuntimeError(f'Unrecognized path type: {path}')

    def expire(self, file, exist_ok=False, top_level=True, commit=True):
        path = file.path
        if not exist_ok and exists(path):
            raise RuntimeError(f"Refusing to expire extant path {path}")
        db_children = File.query.filter(File.parent == path).all()
        if top_level:
            err(f'Expiring {path}…')
        num_expired = 0
        for child in db_children:
            num_expired += self.expire(child, exist_ok=exist_ok, top_level=False, commit=False)
        db.session.delete(file)
        if top_level:
            err(f'Expired {path} and {num_expired} descendants')
        if commit:
            db.session.commit()
        return num_expired

    def insert(self, file, commit=True):
        existing = File.query.get(file.path)
        if existing:
            for k in ['mtime', 'size', 'parent', 'kind', 'num_descendants', 'checked_at']:
                setattr(existing, k, getattr(file, k))
        else:
            db.session.add(file)
        if commit:
            db.session.commit()

    def get(self, path):
        existing = File.query.get(path)
        if existing:
            now = dt.now()
            if now - existing.checked_at <= self.ttl:
                return existing
        return None

    def missing_parents(self):
        files = pd.read_sql_table('file', con=cache_url)
        merged = (
            files
            [['path', 'parent', 'num_descendants']]
            .merge(
                files.path.rename('parent2'),
                left_on='parent',
                right_on='parent2',
                how='left',
            )
        )
        missing_parents = merged[merged.parent2.isna()]['path']
        return missing_parents

    def fsck(self):
        files = pd.read_sql_table('file', con=cache_url)
        gone_paths = ~(files['path'].apply(exists))
        gone_parents = ~(files['parent'].apply(exists))
        invalid = files[gone_paths | gone_parents]
        err(f'Found {gone_paths.sum()} nonexistent paths and {gone_parents.sum()} nonexistent parents ({len(invalid)} total)')
