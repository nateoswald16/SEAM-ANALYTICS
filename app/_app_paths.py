"""
Centralised path resolution for Seam Analytics.

Dev mode  – everything lives in the source directory (app/).
Frozen    – read-only assets come from the PyInstaller bundle;
            writable data goes to an OS-appropriate user-data folder.
"""

import os
import sys

APP_VERSION = "1.2.0"

# Database schema versions — bump these when tables/columns change.
# The app checks these on startup and runs migrations or forces a recalc.
RAW_DB_SCHEMA_VERSION = 2       # v1 = initial, v2 = added outs_recorded/earned_runs/statcast_at_bat_number/base + SB/CS parsing fixes
CALC_DB_SCHEMA_VERSION = 9      # v1 = initial, v2 = aligned with raw v2 parsing fixes, v3 = added h_per_9 to pitching stats, v4 = added bat speed/squared-up/hard hit/chase rate for batters; SwStr%/GB%/F-Strike% for pitchers, v5 = pitcher windows changed to pitch-count (last50p/100p/150p/200p), v6 = added avg_bat_speed/squared_up_rate/hard_hit_pct/chase_rate columns, v7 = pitcher windows 100/200/300/500 pitches, v8 = blast_rate added; squared-up uses plate speed (release*0.92) + competitive-swings denominator, v9 = formula corrections: barrel=LSA6, soft=popup%, avg_bat_speed=top90% swings, slg/babip denominators fixed

_frozen = getattr(sys, "frozen", False)

# ── Root directories ─────────────────────────────────────────────────
if _frozen:
    # PyInstaller onedir: _MEIPASS == directory that contains the exe
    APP_DIR = sys._MEIPASS
    if sys.platform == "win32":
        _base = os.environ.get("LOCALAPPDATA", os.path.expanduser("~"))
    elif sys.platform == "darwin":
        _base = os.path.join(os.path.expanduser("~"), "Library",
                             "Application Support")
    else:
        _base = os.environ.get(
            "XDG_DATA_HOME",
            os.path.join(os.path.expanduser("~"), ".local", "share"),
        )
    DATA_DIR = os.path.join(_base, "SeamAnalytics")
    os.makedirs(DATA_DIR, exist_ok=True)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = APP_DIR                       # dev: read/write in-place

# ── Read-only assets (bundled) ───────────────────────────────────────
ASSETS_DIR      = os.path.join(APP_DIR, "assets")
TEAM_ABBREV_CSV = os.path.join(ASSETS_DIR, "team_abbreviations.csv")
SCHEMA_FILE     = os.path.join(APP_DIR, "database_schema.py")
MAPPING_FILE    = os.path.join(APP_DIR, "pybaseball_to_schema_mapping.json")
LOGO_DIR        = os.path.join(ASSETS_DIR, "logos")
LOGO_PNG        = os.path.join(ASSETS_DIR, "Logo.png")
PLAYERS_CSV     = os.path.join(ASSETS_DIR, "players.csv")

# ── Writable databases ──────────────────────────────────────────────
RAW_DB    = os.path.join(DATA_DIR, "mlb_raw.db")
CALC_DB   = os.path.join(DATA_DIR, "mlb_calculated.db")
STEALS_DB = os.path.join(DATA_DIR, "mlb_steals.db")

# ── Writable caches ─────────────────────────────────────────────────
LINEUP_CACHE_DIR      = os.path.join(DATA_DIR, "assets", "lineup_cache")
WEATHER_CACHE_DIR     = os.path.join(DATA_DIR, "assets", "weather_cache")
STATCAST_CACHE_DIR    = os.path.join(DATA_DIR, "statcast_cache")
PLAYER_IDS_CACHE      = os.path.join(DATA_DIR, "player_ids_cache.pkl")
TEMP_LINEUP_CACHE     = os.path.join(DATA_DIR, "temp_lineups_cache.json")
PROCESSED_DATES_CACHE = os.path.join(DATA_DIR, "processed_dates.pkl")
WEATHERAPI_KEY_FILE   = os.path.join(DATA_DIR, "weatherapi_key.txt")
HEADSHOT_CACHE_DIR    = os.path.join(DATA_DIR, "assets", "headshots")
