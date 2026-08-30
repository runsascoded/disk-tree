# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for disk-tree.app. Built via packaging/macos/build.sh.

Chosen over py2app because py2app requires a *framework* Python; the project's
uv-managed CPython 3.13 is a standalone build py2app can't package. PyInstaller
handles it, and produces a `disk-tree`-named binary → stable TCC identity.
See specs/macos-app.md.
"""
from pathlib import Path

from PyInstaller.utils.hooks import collect_all

HERE = Path(SPECPATH)               # packaging/macos
ROOT = HERE.parent.parent

# Bundle the built UI so STATIC_DIR resolves it at sys._MEIPASS/disk_tree/static
datas = [(str(ROOT / 'src' / 'disk_tree' / 'static'), 'disk_tree/static')]
binaries = []
hiddenimports = ['disk_tree.server', 'disk_tree.desktop', 'waitress']

# Native/plugin-heavy packages PyInstaller's static analysis under-collects.
for pkg in ('pyarrow', 'duckdb', 'pandas', 'numpy', 'webview'):
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h

a = Analysis(
    [str(HERE / 'launcher.py')],
    pathex=[str(ROOT / 'src')],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    excludes=['tkinter', 'matplotlib', 'PyQt5', 'PyQt6', 'PySide2', 'PySide6',
              'IPython', 'notebook', 'pytest', 'mypy'],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz, a.scripts, [],
    exclude_binaries=True,
    name='disk-tree',
    console=False,          # windowed (GUI) app
    argv_emulation=False,
    target_arch=None,       # host arch (arm64)
)
coll = COLLECT(exe, a.binaries, a.datas, name='disk-tree')

app = BUNDLE(
    coll,
    name='disk-tree.app',
    icon=None,
    bundle_identifier='com.runsascoded.disk-tree',
    info_plist={
        'CFBundleName': 'disk-tree',
        'CFBundleDisplayName': 'disk-tree',
        'CFBundleShortVersionString': '0.1.0',
        'CFBundleVersion': '0.1.0',
        'LSMinimumSystemVersion': '13.0',
        'NSHighResolutionCapable': True,
    },
)
