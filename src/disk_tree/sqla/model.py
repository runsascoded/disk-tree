import json
import os
from datetime import datetime
from os import makedirs, remove
from os.path import join, exists
from typing import Optional
from uuid import uuid4

import pandas as pd
from sqlalchemy import Integer, String, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from utz import err, iec

from disk_tree import find
from .base import Base
from ..config import SCANS_DIR


class ScanProgress(Base):
    """Track progress of active scans. One row per active scan, deleted when complete."""
    __tablename__ = "scan_progress"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    path: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    started: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    items_found: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_per_sec: Mapped[Optional[float]] = mapped_column(Integer, nullable=True, default=None)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String, nullable=False, default='running')  # running, completed, failed

    @classmethod
    def start(cls, path: str) -> 'ScanProgress':
        """Start tracking a new scan."""
        from .db import db
        import os

        path = os.path.abspath(path).rstrip('/') if not path.startswith('s3://') else path.rstrip('/')
        # Remove any existing progress for this path
        db.session.query(cls).filter_by(path=path).delete()
        progress = cls(
            path=path,
            pid=os.getpid(),
            started=datetime.now().astimezone(),
            items_found=0,
            error_count=0,
            status='running',
        )
        db.session.add(progress)
        db.session.commit()
        return progress

    @classmethod
    def update(cls, path: str, items_found: int, items_per_sec: float | None = None, error_count: int = 0):
        """Update progress for a scan."""
        from .db import db

        progress = db.session.query(cls).filter_by(path=path).first()
        if progress:
            progress.items_found = items_found
            if items_per_sec is not None:
                progress.items_per_sec = items_per_sec
            progress.error_count = error_count
            db.session.commit()

    @classmethod
    def finish(cls, path: str, status: str = 'completed'):
        """Mark a scan as complete and remove progress tracking."""
        from .db import db

        db.session.query(cls).filter_by(path=path).delete()
        db.session.commit()

    @classmethod
    def get_all(cls) -> list['ScanProgress']:
        """Get all active scans."""
        from .db import db
        return db.session.query(cls).all()


class Scan(Base):
    __tablename__ = "scan"
    __table_args__ = (
        # Index for finding fresher child scans: path LIKE 'prefix%' AND time > parent_time
        Index('ix_scan_path_time', 'path', 'time'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    blob: Mapped[str] = mapped_column(String, nullable=False)
    error_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    error_paths: Mapped[Optional[str]] = mapped_column(String, nullable=True, default=None)  # JSON array
    # Denormalized root stats (to avoid parquet reads on scan list)
    size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    n_children: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    n_desc: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)

    @classmethod
    def create(
        cls,
        path: str,
        scans_dir: str | None = None,
        gc: bool = False,
        sudo: bool = False,
        track_progress: bool = True,
    ) -> tuple['Scan', pd.DataFrame]:
        from .db import db

        if not path.startswith('s3://'):
            path = os.path.abspath(path)
        path = path.rstrip('/')
        now = datetime.now().astimezone()

        # Set up progress tracking
        progress_callback = None
        if track_progress:
            ScanProgress.start(path)

            def progress_callback(items_found: int, items_per_sec: float | None, error_count: int):
                ScanProgress.update(path, items_found, items_per_sec, error_count)

        try:
            result = find.index(path, sudo=sudo, progress_callback=progress_callback)
        except Exception as e:
            if track_progress:
                ScanProgress.finish(path, status='failed')
            raise
        finally:
            if track_progress:
                ScanProgress.finish(path, status='completed')

        df = result.df

        uuid = uuid4()
        if not scans_dir:
            scans_dir = SCANS_DIR
        makedirs(scans_dir, exist_ok=True)
        out_path = join(scans_dir, f'{uuid}.parquet')
        if exists(out_path):
            raise RuntimeError(f"{out_path} exists")
        df.to_parquet(out_path)

        # Extract root stats for denormalization
        root_rows = df[df['parent'] == '']
        root_size = None
        root_n_children = None
        root_n_desc = None
        if not root_rows.empty:
            root = root_rows.iloc[0]
            root_size = int(root['size']) if pd.notna(root['size']) else None
            root_n_children = int(root['n_children']) if pd.notna(root.get('n_children')) else None
            root_n_desc = int(root['n_desc']) if pd.notna(root.get('n_desc')) else None

        # Save "scan" record with error info and denormalized stats
        error_paths_json = json.dumps(result.error_paths) if result.error_paths else None
        scan = Scan(
            path=path,
            time=now,
            blob=out_path,
            error_count=result.error_count if result.error_count > 0 else None,
            error_paths=error_paths_json,
            size=root_size,
            n_children=root_n_children,
            n_desc=root_n_desc,
        )
        db.session.add(scan)
        db.session.commit()
        err(f"{path}: saved {len(df)} rows to {out_path}")
        if result.error_count > 0:
            err(f"{path}: {result.error_count} permission errors")
        if gc:
            cls.gc(path, now)

        return scan, df

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
        track_progress: bool = True,
    ) -> tuple['Scan', pd.DataFrame]:
        scan = cls.load(path)
        if not scan:
            return cls.create(path, scans_dir, gc=gc, sudo=sudo, track_progress=track_progress)
        else:
            df = scan.df()
            cls.gc(path=path, cutoff=scan.time)
            return scan, df
