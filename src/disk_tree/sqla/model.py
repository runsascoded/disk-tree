import os
from datetime import datetime
from os import makedirs, remove
from os.path import join, exists
from uuid import uuid4

import pandas as pd
from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from utz import err, iec

from disk_tree import find
from .base import Base
from ..config import SCANS_DIR


class Scan(Base):
    __tablename__ = "scan"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    blob: Mapped[str] = mapped_column(String, nullable=False)

    @classmethod
    def create(
        cls,
        path: str,
        scans_dir: str | None = None,
        gc: bool = False,
        sudo: bool = False,
    ) -> pd.DataFrame:
        from .db import db

        path = os.path.abspath(path).rstrip('/')
        now = datetime.now().astimezone()
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
        from .db import db

        scans = db.session.query(Scan).filter_by(path=path).filter(Scan.time < cutoff).all()
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
        from .db import db

        abspath = os.path.abspath(path).rstrip('/')
        return db.session.query(cls).filter_by(path=abspath).order_by(cls.time.desc()).first()

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
