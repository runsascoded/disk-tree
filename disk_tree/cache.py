import os
from datetime import datetime as dt
from os import walk
from os.path import abspath, dirname, exists, isdir, isfile, islink, join
import pandas as pd
from pandas import to_datetime as to_dt
import sys
from sys import stderr

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from .config import SQLITE_PATH

app = Flask(__name__)
cache_url = f'sqlite:///{SQLITE_PATH}'
app.config['SQLALCHEMY_DATABASE_URI'] = cache_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

print(f'DB: {SQLITE_PATH}')

class File(db.Model):
    path = db.Column(db.String, primary_key=True)
    mtime = db.Column(db.DateTime, nullable=False)
    size = db.Column(db.Integer, nullable=False)
    parent = db.Column(db.String, nullable=True)
    kind = db.Column(db.String, nullable=False)
    num_descendants = db.Column(db.Integer, nullable=False)
    checked_at = db.Column(db.DateTime, nullable=False)

    @property
    def descendants(self):
        # TODO: this is incorrect given spaces in paths with shared prefixes, e.g. will count "a/b c/d" as a descendant
        #  of "a/b"
        return File.query.filter(File.path.startswith(self.path)).all()

    def __repr__(self):
        return f'File({self.path})'


db.create_all()


def is_descendant(path, ancestor):
    path = path.rstrip('/').split('/')
    ancestor = ancestor.rstrip('/').split('/')
    if len(path) < len(ancestor):
        return False
    for (l, r) in zip(path, ancestor):
        if l != r:
            return False
    return True


class Cache:
    def __init__(self, ttl=None):
        print(f'cache: {SQLITE_PATH}')
        self.ttl = ttl

    def compute(self, path, now=None, fsck=False, excludes=None):
        path = abspath(path)
        if excludes and any(is_descendant(path, exclude) for exclude in excludes):
            print(f'skipping excluded: {path}')
            return None
        if islink(path):
            stderr.write(f'Skipping symlink: {path}\n')
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
                stderr.write(f'Error traversing {path}\n')
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
            stderr.write(f'Unrecognized path type: {path}\n')
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
            for k in ['mtime', 'size', 'parent', 'kind', 'num_descendants', 'checked_at',]:
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
        #files['path']
        merged = (
            files
                [['path', 'parent','num_descendants']]
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
