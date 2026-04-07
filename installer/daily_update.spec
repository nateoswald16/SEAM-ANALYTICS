# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec — Seam Analytics Daily Updater (console)

Build with:
    pyinstaller installer/daily_update.spec --noconfirm
"""

import os, sys

ROOT = os.path.abspath(os.path.join(SPECPATH, '..', 'app'))

import importlib.util as _ilu
_pyb_root = os.path.dirname(_ilu.find_spec('pybaseball').origin)

a = Analysis(
    [os.path.join(ROOT, 'daily_update.py')],
    pathex=[ROOT],
    binaries=[],
    datas=[
        (os.path.join(ROOT, 'database_schema.py'),               '.'),
        (os.path.join(ROOT, 'pybaseball_to_schema_mapping.json'), '.'),
        (os.path.join(ROOT, 'assets', 'team_abbreviations.csv'), 'assets'),
        # pybaseball internal data files
        (os.path.join(_pyb_root, 'data'), os.path.join('pybaseball', 'data')),
    ],
    hiddenimports=[
        '_app_paths',
        'build_raw_db',
        'build_calculated_db',
        # pybaseball (lazy imports)
        'pybaseball',
        'pybaseball.statcast',
        'pybaseball.statcast_sprint_speed',
        'pybaseball.playerid_lookup',
        # pandas / numpy
        'pandas.io.html',
        'pandas.io.sql',
        'numpy._core',
        # transitive
        'lxml', 'lxml.etree', 'lxml.html',
        'bs4',
        'sqlalchemy', 'sqlalchemy.dialects.sqlite',
        'tqdm',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'scipy', 'IPython', 'notebook',
        'pytest', 'setuptools', 'wheel', 'pip',
        # Updater doesn't need PyQt6
        'PyQt6',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='SeamUpdater',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,                 # console app for scheduled tasks
    icon=os.path.join(ROOT, 'assets', 'Logo.ico'),
    uac_admin=False,
    uac_uiaccess=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='SeamUpdater',
)
