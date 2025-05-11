from os.path import abspath
from typing import Optional
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from utz import err

from ..config import SQLITE_PATH

app = None
db: Optional[SQLAlchemy] = None
cache_url = None


def init(sqlite_path: str = None):
    global app
    global cache_url
    global db
    app = Flask(__name__)
    if not sqlite_path:
        sqlite_path = SQLITE_PATH
        err("Initializing DB at default location")
    cache_path = abspath(sqlite_path)
    cache_url = f'sqlite:///{cache_path}'
    app.config['SQLALCHEMY_DATABASE_URI'] = cache_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)
    app.app_context().push()
    return db
