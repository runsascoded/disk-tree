import os
import shlex
from datetime import datetime as dt
from os import walk
from os.path import abspath, dirname, exists, isdir, isfile, islink, join
from subprocess import check_call, CalledProcessError

import pandas as pd
from pandas import to_datetime as to_dt
from utz import err

from . import s3
from .config import SQLITE_PATH, ROOT_DIR
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
    if key.startswith(prefix):
        return key[len(prefix):]
    else:
        raise ValueError(f"Key {key} doesn't start with expected prefix {prefix}")


class Cache:
    def __init__(self, ttl=None):
        print(f'cache: {cache_url}')
        self.ttl = ttl

    def compute_s3(self, url, bucket, root_key):
        now = to_dt(dt.now())
        e = S3.query.get((bucket, root_key))
        if e:
            elem_age = now - e.checked_at
            if elem_age <= self.ttl:
                return e

        S3_LINE_CACHE = join(ROOT_DIR, '.s3/ls')
        s3_cache_path = join(S3_LINE_CACHE, bucket, root_key) + '.txt'
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
        files['relpath'] = files['key'].apply(strip_prefix, prefix=f'{root_key}/')
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

    def compute(self, path, now=None, fsck=False, excludes=None):
        path = abspath(path)
        if excludes and any(is_descendant(path, exclude) for exclude in excludes):
            print(f'skipping excluded: {path}')
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
            files = list(filter(None, [ self.compute(join(path, file), excludes=excludes) for file in files ]))
            dirs = list(filter(None, [ self.compute(join(path, dir), excludes=excludes) for dir in dirs ]))
            children = files + dirs
            child_paths = set([ c.path for c in children ])
            db_children = File.query.filter(File.parent == path).all()
            expired_children = [ c for c in db_children if c.path not in child_paths ]
            if expired_children:
                print(f'Cache: expiring {len(expired_children)} stale children of {path}:')
                for child in expired_children:
                    print(f'\t{child.path}')
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
            err(f'Unrecognized path type: {path}')
            return None

    def expire(self, file, exist_ok=False, top_level=True, commit=True):
        path = file.path
        if not exist_ok and exists(path):
            raise RuntimeError(f"Refusing to expire extant path {path}")
        db_children = File.query.filter(File.parent == path).all()
        if top_level:
            print(f'Expiring {path}…')
        num_expired = 0
        for child in db_children:
            num_expired += self.expire(child, exist_ok=exist_ok, top_level=False, commit=False)
        db.session.delete(file)
        if top_level:
            print(f'Expired {path} and {num_expired} descendants')
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
        existing = File.query.filter_by(path=path).first()
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
        print(f'Found {gone_paths.sum()} nonexistent paths and {gone_parents.sum()} nonexistent parents ({len(invalid)} total)')
