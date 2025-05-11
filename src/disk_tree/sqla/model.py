import os
from datetime import datetime
from os import makedirs
from os.path import join, exists
from typing import Optional

import pandas as pd
from utz import err

from disk_tree import find
from .db import db
from ..config import SCANS_DIR

if not db:
    from .db import init
    db = init()


class Scan(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    path = db.Column(db.String, nullable=False)
    time = db.Column(db.DateTime, nullable=False)

    @classmethod
    def create(
        cls,
        path: str,
        scans_dir: str | None = None,
    ) -> pd.DataFrame:
        abspath = os.path.abspath(path).rstrip('/')
        now = datetime.now()
        scan = Scan(path=abspath, time=now)
        df = find.index(abspath)
        if not scans_dir:
            scans_dir = SCANS_DIR
        makedirs(scans_dir, exist_ok=True)
        ms = int(now.timestamp() * 1000)
        out_path = join(scans_dir, f'{ms}.parquet')
        if exists(out_path):
            raise RuntimeError(f"{out_path} exists")
        df.to_parquet(out_path)
        # Save scan
        db.session.add(scan)
        db.session.commit()
        err(f"{path}: saved {len(df)} rows to {out_path}")
        return df

    @classmethod
    def load(
        cls,
        path: str,
        scans_dir: str | None = None,
    ) -> pd.DataFrame | None:
        abspath = os.path.abspath(path).rstrip('/')
        scan_entry = cls.query.filter_by(path=abspath).order_by(cls.time.desc()).first()
        if scan_entry:
            ms = int(scan_entry.time.timestamp() * 1000)
            if not scans_dir:
                scans_dir = SCANS_DIR

            pqt_path = join(scans_dir, f'{ms}.parquet')
            if exists(pqt_path):
                df = pd.read_parquet(pqt_path)
                err(f"{path}: loaded {len(df)} rows from {pqt_path}")
                return df
            else:
                raise FileNotFoundError(f"Parquet file not found for scan: {pqt_path}")
        else:
            return None
            # raise FileNotFoundError(f"No scan found for path: {path}")

    @classmethod
    def load_or_create(
        cls,
        path: str,
        scans_dir: str | None = None,
    ) -> pd.DataFrame:
        return cls.load(path, scans_dir) or cls.create(path, scans_dir)


class File(db.Model):
    path = db.Column(db.String, primary_key=True)
    mtime = db.Column(db.DateTime, nullable=False)
    size = db.Column(db.Integer, nullable=False)
    parent = db.Column(db.String, nullable=True)
    kind = db.Column(db.String, nullable=False)
    num_descendants = db.Column(db.Integer, nullable=False)
    checked_at = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return f'File({self.path})'

    def descendants(self, excludes: Optional[list[str]] = None):
        filter = (File.parent == self.path) | File.parent.startswith(f'{self.path}/')
        if excludes:
            filter = filter & File.path.not_in(excludes) & File.parent.not_in(excludes)
            for exclude in excludes:
                filter = filter & ~File.parent.startswith(f'{exclude}/')
        return [self] + self.query.filter(filter).all()


class S3(db.Model):
    bucket = db.Column(db.String, primary_key=True)
    key = db.Column(db.String, primary_key=True)
    mtime = db.Column(db.DateTime, nullable=False)
    size = db.Column(db.Integer, nullable=False)
    parent = db.Column(db.String, nullable=True)
    kind = db.Column(db.String, nullable=False)
    num_descendants = db.Column(db.Integer, nullable=False)
    checked_at = db.Column(db.DateTime, nullable=False)

    # Used by `self.descendants`
    @property
    def path(self):
        return self.key

    def __repr__(self):
        return f'S3({self.bucket}/{self.path})'

    def descendants(self, excludes: Optional[list[str]] = None):
        filter = S3.bucket == self.bucket
        if self.key:
            filter = filter & (
                (S3.key == self.key) | (S3.parent == self.key) | S3.parent.startswith(f'{self.key}/')
            )
        if excludes:
            filter = filter & S3.key.not_in(excludes) & S3.parent.not_in(excludes)
            for exclude in excludes:
                filter = filter & ~S3.parent.startswith(f'{exclude}/')
        return self.query.filter(filter).all()
