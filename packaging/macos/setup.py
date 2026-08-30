"""py2app build for disk-tree.app. Invoke via `packaging/macos/build.sh`, which
stages the built UI into `disk_tree/static/` first and signs afterward.

Manual: `python packaging/macos/setup.py py2app` (from the repo root, in the
venv with all extras + py2app installed).
"""
from pathlib import Path

from setuptools import setup

HERE = Path(__file__).resolve().parent

APP = [str(HERE / 'launcher.py')]

OPTIONS = {
    'argv_emulation': False,
    # Keep these packages UNZIPPED in the bundle: they rely on `__file__`
    # (disk_tree finds its bundled `static/` beside the module) and ship native
    # dylibs (pyarrow/duckdb/numpy) that a zipimport can't dlopen.
    'packages': [
        'disk_tree', 'flask', 'werkzeug', 'jinja2', 'click',
        'pandas', 'numpy', 'pyarrow', 'duckdb',
        'webview', 'humanize', 'dateutil', 'yaml', 'utz',
    ],
    'plist': {
        'CFBundleName': 'disk-tree',
        'CFBundleDisplayName': 'disk-tree',
        'CFBundleIdentifier': 'com.runsascoded.disk-tree',
        'CFBundleShortVersionString': '0.1.0',
        'CFBundleVersion': '0.1.0',
        'LSMinimumSystemVersion': '13.0',
        'NSHighResolutionCapable': True,
    },
}

setup(
    name='disk-tree',
    app=APP,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
