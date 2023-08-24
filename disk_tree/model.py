from typing import Optional

from .db import db


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
