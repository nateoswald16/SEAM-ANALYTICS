# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec — Seam Analytics (main GUI app)

Build with:
    pyinstaller installer/seam_app.spec --noconfirm
"""

import os, sys

ROOT = os.path.abspath(os.path.join(SPECPATH, '..', 'app'))

import importlib.util as _ilu
_pyb_root = os.path.dirname(_ilu.find_spec('pybaseball').origin)

a = Analysis(
    [os.path.join(ROOT, 'seam_app.py')],
    pathex=[ROOT],
    binaries=[],
    datas=[
        # Read-only assets bundled into the exe directory
        (os.path.join(ROOT, 'assets', 'team_abbreviations.csv'), 'assets'),
        (os.path.join(ROOT, 'assets', 'Logo.png'),               'assets'),
        (os.path.join(ROOT, 'assets', 'Logo.ico'),               'assets'),
        (os.path.join(ROOT, 'assets', 'logos'),                   os.path.join('assets', 'logos')),
        # Schema / mapping files (read-only, used by build pipeline)
        (os.path.join(ROOT, 'database_schema.py'),               '.'),
        (os.path.join(ROOT, 'pybaseball_to_schema_mapping.json'), '.'),
        # pybaseball internal data files
        (os.path.join(_pyb_root, 'data'), os.path.join('pybaseball', 'data')),
    ],
    hiddenimports=[
        # ── Local modules ────────────────────────────────────
        '_app_paths',
        'mlb_data_engine',
        'MLB_AVG',
        'park_factors',
        # ── PyQt6 ────────────────────────────────────────────
        'PyQt6.sip',
        'PyQt6.QtSvg',
        'PyQt6.QtSvgWidgets',
        # ── pybaseball (lazy imports inside functions) ────────
        'pybaseball',
        'pybaseball.statcast',
        'pybaseball.statcast_sprint_speed',
        'pybaseball.playerid_lookup',
        # ── pandas / numpy internals ─────────────────────────
        'pandas.io.html',
        'pandas.io.sql',
        'numpy._core',
        # ── transitive deps pybaseball needs at runtime ──────
        'lxml', 'lxml.etree', 'lxml.html',
        'bs4',
        'sqlalchemy', 'sqlalchemy.dialects.sqlite',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Heavy packages the GUI app does NOT need
        'tkinter', 'matplotlib', 'scipy', 'IPython', 'notebook',
        'pytest', 'setuptools', 'wheel', 'pip',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],                           # onedir: leave binaries/datas out of exe
    exclude_binaries=True,
    name='SeamAnalytics',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,                # windowed: no terminal
    icon=os.path.join(ROOT, 'assets', 'Logo.ico'),
    version=None,
    # Windows DPI awareness — crisp on HiDPI monitors
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
    name='SeamAnalytics',
)
