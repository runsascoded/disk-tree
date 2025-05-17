import os
from datetime import datetime
from os import makedirs, remove
from os.path import join, exists
from typing import Optional
from uuid import uuid4

import pandas as pd
from utz import err, iec

from disk_tree import find
from .db import db
from ..config import SCANS_DIR

if not db:
    from .db import init
    db = init()


Column = db.Column


class Scan(db.Model):
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    path = Column(db.String, nullable=False)
    time = Column(db.DateTime, nullable=False)
    blob = Column(db.String, nullable=False)

    @classmethod
    def create(
        cls,
        path: str,
        scans_dir: str | None = None,
        gc: bool = False,
        sudo: bool = False,
    ) -> pd.DataFrame:
        path = os.path.abspath(path).rstrip('/')
        now = datetime.now()
        df = find.index(path, sudo=sudo)

        uuid = uuid4()
        if not scans_dir:
            scans_dir = SCANS_DIR
        makedirs(scans_dir, exist_ok=True)
        out_path = join(scans_dir, f'{uuid}.parquet')
        if exists(out_path):
            raise RuntimeError(f"{out_path} exists")
        df.to_parquet(out_path)

        # Save "scan" record
        scan = Scan(path=path, time=now, blob=out_path)
        db.session.add(scan)
        db.session.commit()
        err(f"{path}: saved {len(df)} rows to {out_path}")
        if gc:
            cls.gc(path, now)

        return df

    @classmethod
    def gc(
        cls,
        path: str,
        cutoff: datetime,
    ):
        scans = Scan.query.filter_by(path=path).filter(Scan.time < cutoff).all()
        err(f"{path}: deleting {len(scans)} old scans:")
        for scan in scans:
            blob = scan.blob
            size = os.stat(blob).st_size
            err(f"\t{scan.time}: {blob} ({iec(size)})")
            db.session.delete(scan)
            db.session.commit()
            remove(blob)

    @classmethod
    def load(
        cls,
        path: str,
    ) -> 'Scan | None':
        abspath = os.path.abspath(path).rstrip('/')
        return cls.query.filter_by(path=abspath).order_by(cls.time.desc()).first()

    def df(self) -> pd.DataFrame:
        pqt_path = self.blob
        if not exists(pqt_path):
            raise FileNotFoundError(f"Parquet file not found for scan: {pqt_path}")
        df = pd.read_parquet(pqt_path)
        err(f"{self.path}: loaded {len(df)} rows from {pqt_path}")
        return df

    @classmethod
    def load_or_create(
        cls,
        path: str,
        scans_dir: str | None = None,
        gc: bool = False,
        sudo: bool = False,
    ) -> pd.DataFrame:
        scan = cls.load(path)
        if not scan:
            return cls.create(path, scans_dir, gc=gc, sudo=sudo)
        else:
            df = scan.df()
            cls.gc(path=path, cutoff=scan.time)
            return df


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
