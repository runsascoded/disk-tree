from dataclasses import dataclass
from typing import List
import pandas as pd


# def parse_s3_line(line):
#     m = re.fullmatch(LINE_RGX, line)
#     if not m:
#         raise ValueError(f'Unrecognized line: {line}')
#
#     mtime = to_dt(m['mtime'])
#     size = int(m['size'])
#     key = m['key']
#     parent = dirname(key)
#     if key.endswith('/'):
#         parent = dirname(parent)
#     return o(mtime=mtime, size=size, key=key, parent=parent)


# def path_metadata(path):
#     stat = os.stat(path)
#     mtime = to_dt(stat.st_mtime, unit='s')
#     size = stat.st_size
#     path = abspath(path)
#     parent = dirname(path)
#     return o(mtime=mtime, size=size, path=path, parent=parent)


DISK_TREE_ROOT_VAR = 'DISK_TREE_ROOT'
HOME = env['HOME']
CONFIG_DIR = join(HOME, '.config')
if exists(CONFIG_DIR):
    DEFAULT_ROOT = join(CONFIG_DIR, 'disk-tree')
else:
    DEFAULT_ROOT = join(HOME, '.disk-tree')


from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DEFAULT_ROOT}'
db = SQLAlchemy(app)


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


db.create_all()

@dataclass
class Entry:
    path: str
    mtime: pd.Timestamp
    size: int
    parent: 'Entry'
    kind: str
    num_descendants: int
    checked_at: pd.Timestamp
    children: List['Entry']


class Cache:
    def __init__(self, path, ttl=None):
        self.path = path
        if path.endswith('.db'):
            self.url = f'sqlite:///{path}'
            if exists(cache_path):
                self.df = pd.read_sql_table('entries', self.url)
            else:
                self.df = None
        # elif path.endswith('.pqt'):
        #     if exists(cache_path):
        #         self.df = pd.read_parquet(path)
        #     else:
        #         self.df = None
        else:
            raise RuntimeError(f'Unrecognized cache path extension: {path}')
        self.map = {}
        self.ttl = ttl
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = sqlite3.connect(db_file)
        return self._conn

    def compute(self, path, now=None):
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
            parent = self.get(parent)
            entry = Entry(
                path,
                mtime=mtime,
                size=size,
                parent=parent,
                kind='file',
                num_descendants=1,
                checked_at=now,
                children=[],
            )
            return entry
        elif isdir(path):
            _, dirs, files = next(walk(path))
            files = list(filter(None, [ self[file] for file in files ]))
            dirs = [ self[dir] for dir in dirs ]
            children = files + dirs
            num_descendants = len(files) + sum( d.num_descendants for d in dirs )
            size = sum(c.size for c in children)
            parent = dirname(path)
            parent = self.get(parent)
            mtime = max(c.mtime for c in children)
            if not now:
                now = to_dt(dt.now())
            entry = Entry(
                path,
                mtime=mtime,
                size=size,
                parent=parent,
                kind='dir',
                num_descendants=num_descendants,
                checked_at=now,
                children = children,
            )
            for c in children:
                c.parent = entry
            return entry
        else:
            stderr.write(f'Unrecognized path type: {path}\n')
            return None

    def insert(self, k, v):
        sql = ''' INSERT INTO entries(name,begin_date,end_date) VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, (v.path, v.mtime, v.size, v.parent.path, v.kind, v.num_descendants, v.checked_at))
        conn.commit()
        return cur.lastrowid

    def get(self, path):
        existing = self.map.get(path)
        if existing:
            now = dt.now()
            if now - existing.checked_at <= self.ttl:
                return existing
        return None

    def __getitem__(self, path):
        existing = self.get(path)
        if existing:
            return existing
        entry = self.compute(path)
        if entry:
            self.map[path] = entry
            return entry
        else:
            return None
