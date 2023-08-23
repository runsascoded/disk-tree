from os.path import abspath

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from .config import SQLITE_PATH

app = None
db = None
cache_url = None


def init(sqlite_path=None):
    global app
    global cache_url
    global db
    app = Flask(__name__)
    cache_path = abspath(sqlite_path or SQLITE_PATH)
    cache_url = f'sqlite:///{cache_path}'
    app.config['SQLALCHEMY_DATABASE_URI'] = cache_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db = SQLAlchemy(app)
    app.app_context().push()
    return db
