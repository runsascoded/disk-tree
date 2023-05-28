from .db import db


class Model:
    @property
    def descendants(self):
        return [self] + self.query.filter((self.__class__.parent == self.path) | self.__class__.parent.startswith(f'{self.path}/')).all()


class File(db.Model, Model):
    path = db.Column(db.String, primary_key=True)
    mtime = db.Column(db.DateTime, nullable=False)
    size = db.Column(db.Integer, nullable=False)
    parent = db.Column(db.String, nullable=True)
    kind = db.Column(db.String, nullable=False)
    num_descendants = db.Column(db.Integer, nullable=False)
    checked_at = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return f'File({self.path})'


class S3(db.Model, Model):
    # url = db.Column(db.String, primary_key=True)
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
