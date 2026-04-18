# -*- coding: utf-8 -*-
"""
Weather Detail Widget — Seam Analytics

Expanded weather card with stadium outline, wind arrows, HR carry ring,
hourly switcher, HR rating, wind insight, and conditions table.

Designed as a plug-in replacement for ParkWeatherCard: accepts the same
``data: dict`` from the weather cache.

Run standalone:  python test_weather_widget.py
"""

import math
import os
import sys

import pybaseball
import pandas as pd

from PyQt6.QtWidgets import (
    QWidget, QFrame, QVBoxLayout, QHBoxLayout, QLabel,
    QSizePolicy, QApplication, QGridLayout,
)
from PyQt6.QtCore import Qt, QRectF, QPointF, pyqtSignal
from PyQt6.QtGui import (
    QColor, QFont, QFontMetricsF, QPainter, QPainterPath, QPen, QBrush, QPixmap, QImage,
)
from PyQt6.QtSvg import QSvgRenderer

import _app_paths
from _app_theme import C
from _ui_utils import mk_label as _mk

# ═══════════════════════════════════════════════════════════════════════════════
# SVG logo cache — avoids re-parsing / re-rasterizing on every widget build
# ═══════════════════════════════════════════════════════════════════════════════
from functools import lru_cache as _lru_cache

@_lru_cache(maxsize=128)
def _cached_logo_pixmap(abbr: str, size: int):
    """Render an SVG team logo to QPixmap, cached by (abbr, size)."""
    svg_path = os.path.join(_app_paths.LOGO_DIR, f"{abbr}.svg")
    if not os.path.exists(svg_path):
        return None
    try:
        renderer = QSvgRenderer(svg_path)
        if not renderer.isValid():
            return None
        img = QImage(size, size, QImage.Format.Format_ARGB32_Premultiplied)
        img.fill(0)
        p = QPainter(img)
        renderer.render(p)
        p.end()
        return QPixmap.fromImage(img)
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# Stadium outline data (shared with player_card.py)
# ═══════════════════════════════════════════════════════════════════════════════

_STADIUMS_CSV = os.path.join(
    os.path.dirname(pybaseball.__file__), "data", "mlbstadiums.csv")
_stadium_df = None


def _load_stadiums():
    global _stadium_df
    if _stadium_df is None and os.path.exists(_STADIUMS_CSV):
        _stadium_df = pd.read_csv(_STADIUMS_CSV)
    return _stadium_df


_TEAM_TO_STADIUM = {
    "ARI": "diamondbacks", "ATL": "braves",   "BAL": "orioles",
    "BOS": "red_sox",      "CHC": "cubs",      "CWS": "white_sox",
    "CIN": "reds",         "CLE": "indians",   "COL": "rockies",
    "DET": "tigers",       "HOU": "astros",    "KC":  "royals",
    "LAA": "angels",       "LAD": "dodgers",   "MIA": "marlins",
    "MIL": "brewers",      "MIN": "twins",     "NYM": "mets",
    "NYY": "yankees",      "OAK": "athletics", "PHI": "phillies",
    "PIT": "pirates",      "SD":  "padres",    "SF":  "giants",
    "SEA": "mariners",     "STL": "cardinals", "TB":  "rays",
    "TEX": "rangers",      "TOR": "blue_jays", "WSH": "nationals",
    "ATH": "athletics",
    "AZ":  "diamondbacks",
}

# MLB API → app abbreviation normalisation (logo filenames, CSV, etc.)
_ABBR_NORM: dict[str, str] = {"AZ": "ARI"}


def _get_stadium_segments(team_abbrev):
    df = _load_stadiums()
    if df is None:
        return None, None
    key = _TEAM_TO_STADIUM.get(team_abbrev, "generic")
    tdf = df[df["team"] == key]
    if tdf.empty:
        tdf = df[df["team"] == "generic"]
    segs = {}
    for seg, grp in tdf.groupby("segment"):
        segs[seg] = list(zip(grp["x"].values, grp["y"].values))
    venue = None
    if "name" in tdf.columns and not tdf.empty:
        v = tdf["name"].iloc[0]
        if pd.notna(v):
            venue = str(v)
    return segs, venue


# ═══════════════════════════════════════════════════════════════════════════════
# Wind / carry helpers (shared logic from park_factors.py)
# ═══════════════════════════════════════════════════════════════════════════════

WIND_ANGLES = {
    "Out To CF":   0,   "Out To RF":  45,  "L To R":     90,
    "In From LF": 135,  "In From CF": 180, "In From RF": 225,
    "R To L":     270,  "Out To LF":  315,
}
HITTER_FRIENDLY = {"Out To CF", "Out To RF", "Out To LF"}
PITCHER_FRIENDLY = {"In From CF", "In From RF", "In From LF"}

# Stadium size classification — displayed below venue name on widget card.
# Based on foul-line distances, outfield gaps, and total field area.
VENUE_SIZE_DESC: dict[int, str] = {
    # Extra Small (under 325 ft lines)
    2529: "Extra Small",   # Sutter Health Park — ~13k cap bandbox
    3:    "Extra Small",   # Fenway Park — 310 ft LF, shallow RF
    3313: "Extra Small",   # Yankee Stadium — 314 ft LF, short porch RF
    2392: "Extra Small",   # Daikin Park — 315 ft LF line
    # Small (325–335 ft lines)
    4169: "Small",         # loanDepot Park — reduced dimensions
    22:   "Small",         # Dodger Stadium — 330 ft LF/RF
    2681: "Small",         # Citizens Bank Park — 330 ft LF/RF
    2680: "Small",         # Petco Park — 334 ft LF, 322 ft RF
    17:   "Small",         # Wrigley Field — small footprint, wind-influenced
    # Medium (335–340 ft lines)
    2395: "Medium",        # Oracle Park — 339 ft LF, 309 ft RF
    3309: "Medium",        # Nationals Park — 335 ft LF/RF
    31:   "Medium",        # PNC Park — 325/320 ft, deep CF balances
    32:   "Medium",        # American Family Field — 344 ft LF, 326 ft RF
    14:   "Medium",        # Rogers Centre — 328 ft LF/RF, uniform shape
    12:   "Medium",        # Tropicana Field — 315 ft LF, 322 ft RF, turf
    4705: "Medium",        # Truist Park — 335 ft lines
    2:    "Medium",        # Camden Yards — 333 ft LF, 318 ft RF
    3289: "Medium",        # Citi Field — 335 ft LF, 330 ft RF
    4:    "Medium",        # Guaranteed Rate Field — 330 ft LF, 335 ft RF
    5:    "Medium",        # Progressive Field — 325 ft LF, 325 ft RF
    3312: "Medium",        # Target Field — 339 ft LF, 328 ft RF
    1:    "Medium",        # Angel Stadium — 330 ft LF/RF
    5325: "Medium",        # Globe Life Field — 329 ft LF, 326 ft RF
    # Large (340–345 ft lines)
    2889: "Large",         # Busch Stadium — 336 ft LF, 335 ft RF
    2602: "Large",         # Great American Ball Park — 328/325 ft, hitter-park feel
    680:  "Large",         # T-Mobile Park — 331/326 ft, deep alleys
    2394: "Large",         # Comerica Park — 345 ft LF, deep LC gap
    # Extra Large (345 ft+ lines or massive area)
    19:   "Extra Large",   # Coors Field — 347 ft LF, 350 ft RF
    15:   "Extra Large",   # Chase Field — vast outfield territory
    7:    "Extra Large",   # Kauffman Stadium — 330 ft lines, huge alleys (387/390)
}

# Park factors (venue_id → HR index, 100 = neutral)
# Calibrated against BallparkPal stadium-only reference data
VENUE_PARK_FACTORS = {
    1: 99, 2: 100, 3: 92, 4: 99, 5: 98, 7: 101, 12: 100, 14: 100,
    15: 92, 17: 106, 19: 121, 22: 101, 31: 95, 32: 97, 680: 101,
    2392: 106, 2394: 100, 2395: 97, 2529: 99, 2602: 103, 2680: 97,
    2681: 106, 2889: 100, 3289: 98, 3309: 88, 3312: 93, 3313: 102,
    4169: 90, 4705: 101, 5325: 97,
}

# Per-venue hit-type park factors (venue_id → {1B, 2B, 3B})
# Expressed as 1.00 = league average.  HR factor already in VENUE_PARK_FACTORS.
# Sources: Fangraphs park factors, ESPN park factors, historical data.
VENUE_HIT_FACTORS: dict[int, dict[str, float]] = {
    #                              1B    2B    3B          Venue
    1:    {"1B": 1.01, "2B": 0.90, "3B": 0.96},  # Angel Stadium
    2:    {"1B": 1.00, "2B": 1.01, "3B": 0.85},  # Camden Yards
    3:    {"1B": 1.04, "2B": 1.10, "3B": 0.70},  # Fenway Park
    4:    {"1B": 0.99, "2B": 0.97, "3B": 1.00},  # Rate Field (Guaranteed Rate)
    5:    {"1B": 0.98, "2B": 1.02, "3B": 0.92},  # Progressive Field
    7:    {"1B": 1.01, "2B": 1.04, "3B": 1.15},  # Kauffman Stadium
    12:   {"1B": 1.00, "2B": 0.98, "3B": 0.75},  # Tropicana Field
    14:   {"1B": 1.00, "2B": 0.97, "3B": 0.80},  # Rogers Centre
    15:   {"1B": 1.00, "2B": 1.14, "3B": 1.05},  # Chase Field
    17:   {"1B": 0.93, "2B": 0.87, "3B": 0.90},  # Wrigley Field
    19:   {"1B": 1.16, "2B": 1.35, "3B": 2.50},  # Coors Field
    22:   {"1B": 1.01, "2B": 0.98, "3B": 0.80},  # Dodger Stadium
    31:   {"1B": 1.00, "2B": 1.10, "3B": 1.10},  # PNC Park
    32:   {"1B": 0.98, "2B": 0.96, "3B": 0.85},  # American Family Field
    680:  {"1B": 0.93, "2B": 0.86, "3B": 0.77},  # T-Mobile Park
    2392: {"1B": 0.96, "2B": 0.88, "3B": 0.90},  # Daikin Park (Minute Maid)
    2394: {"1B": 1.00, "2B": 1.02, "3B": 1.45},  # Comerica Park
    2395: {"1B": 0.98, "2B": 0.95, "3B": 0.88},  # Oracle Park
    2529: {"1B": 1.05, "2B": 0.98, "3B": 1.00},  # Sutter Health Park
    2602: {"1B": 1.02, "2B": 1.08, "3B": 0.95},  # Great American Ball Park
    2680: {"1B": 0.98, "2B": 0.96, "3B": 0.85},  # Petco Park
    2681: {"1B": 1.01, "2B": 1.05, "3B": 0.90},  # Citizens Bank Park
    2889: {"1B": 1.00, "2B": 1.00, "3B": 1.05},  # Busch Stadium
    3289: {"1B": 0.99, "2B": 0.97, "3B": 0.80},  # Citi Field
    3309: {"1B": 1.03, "2B": 1.10, "3B": 0.95},  # Nationals Park
    3312: {"1B": 1.01, "2B": 1.01, "3B": 1.10},  # Target Field
    3313: {"1B": 0.94, "2B": 0.87, "3B": 0.85},  # Yankee Stadium
    4169: {"1B": 1.04, "2B": 1.00, "3B": 0.80},  # loanDepot park
    4705: {"1B": 1.01, "2B": 1.02, "3B": 1.00},  # Truist Park
    5325: {"1B": 0.98, "2B": 0.95, "3B": 0.80},  # Globe Life Field
}

# Per-venue weather sensitivity profile (multipliers for each component).
# Keys: w = wind, t = temperature, h = humidity, p = pressure/altitude.
# 1.0 = league-average sensitivity.  Values >1 amplify, <1 dampen.
# Sources: MLB Statcast wind study, BallparkPal, FanGraphs, CLEATZ, Oddstrader.
VENUE_WX_PROFILE: dict[int, dict[str, float]] = {
    # ── Tier 1: Extreme weather volatility ──
    # Wind-dominated
    17:   {"w": 3.50, "t": 1.20, "h": 0.80, "p": 1.00},  # Wrigley Field — most wind-volatile park in baseball
    4:    {"w": 1.60, "t": 1.20, "h": 0.80, "p": 1.00},  # Guaranteed Rate — sheltered but still wind-sensitive
    2681: {"w": 1.80, "t": 0.80, "h": 0.90, "p": 1.00},  # Citizens Bank — wind-dominated, temp dampened
    7:    {"w": 1.80, "t": 1.00, "h": 0.80, "p": 1.00},  # Kauffman Stadium — plains, minimal obstruction
    3:    {"w": 1.60, "t": 1.10, "h": 1.00, "p": 1.00},  # Fenway Park — wind-sensitive, moderate temp
    3289: {"w": 1.50, "t": 1.00, "h": 0.90, "p": 1.00},  # Citi Field — most HR created by wind (28)
    # Altitude-dominated
    19:   {"w": 1.00, "t": 1.00, "h": 0.60, "p": 1.80},  # Coors Field — 5280 ft, thin air already reduces drag; wind/temp less impactful
    15:   {"w": 0.80, "t": 1.50, "h": 0.90, "p": 1.40},  # Chase Field — highest avg temp, 1100 ft elevation

    # ── Tier 2: Moderate-high sensitivity (northern / temp-dominant) ──
    2394: {"w": 1.20, "t": 1.10, "h": 0.90, "p": 1.00},  # Comerica Park — early-season cold exploitable
    5:    {"w": 1.20, "t": 1.10, "h": 0.90, "p": 1.00},  # Progressive Field — Lake Erie cold, harsh April
    3312: {"w": 1.20, "t": 0.60, "h": 0.90, "p": 1.00},  # Target Field — cold over-modeled at full sensitivity — cold early season over-modeled at full sensitivity
    3313: {"w": 1.10, "t": 0.90, "h": 0.90, "p": 1.00},  # Yankee Stadium — short porch dampens wind + temp effect
    2602: {"w": 1.40, "t": 1.20, "h": 0.90, "p": 1.00},  # Great American — bidirectional wind = temp
    2889: {"w": 1.40, "t": 1.10, "h": 0.80, "p": 1.00},  # Busch Stadium — Midwest wind belt

    # ── Tier 3: Moderate (marine air / cool parks) ──
    2395: {"w": 0.40, "t": 1.50, "h": 1.20, "p": 1.00},  # Oracle Park — wind blocked by architecture, marine layer
    2680: {"w": 0.70, "t": 1.30, "h": 1.20, "p": 1.00},  # Petco Park — marine air, temp > humidity > wind
    4169: {"w": 0.70, "t": 0.80, "h": 1.50, "p": 1.00},  # loanDepot Park — humidity-dominant when open
    680:  {"w": 1.20, "t": 0.80, "h": 1.00, "p": 1.00},  # T-Mobile Park — carport roof partially shields temp swings; wind unblocked

    # ── Tier 4: Low-moderate (Mid-Atlantic belt / balanced) ──
    3309: {"w": 1.40, "t": 1.10, "h": 0.90, "p": 1.00},  # Nationals Park — wind-exposed along Anacostia
    2:    {"w": 1.00, "t": 1.10, "h": 0.90, "p": 1.00},  # Camden Yards — Mid-Atlantic, balanced
    31:   {"w": 0.70, "t": 0.90, "h": 0.90, "p": 1.00},  # PNC Park — low sensitivity, deep fences dominate
    4705: {"w": 0.80, "t": 1.20, "h": 0.90, "p": 1.10},  # Truist Park — 3rd highest altitude, temp-dominant
    2529: {"w": 0.80, "t": 1.00, "h": 0.30, "p": 0.60},  # Sutter Health Park — extra-small bandbox, short fences dominate

    # ── Tier 5: Low sensitivity (mild climate / retractable roof) ──
    5325: {"w": 0.70, "t": 1.00, "h": 0.80, "p": 1.00},  # Globe Life Field — sheltered when open
    2392: {"w": 0.60, "t": 0.80, "h": 0.80, "p": 1.00},  # Daikin Park — rarely open, sheltered
    32:   {"w": 0.70, "t": 1.00, "h": 0.80, "p": 1.00},  # American Family — roof when cold
    14:   {"w": 0.70, "t": 1.00, "h": 0.80, "p": 1.00},  # Rogers Centre — roof open ~2/3 of games
    22:   {"w": 0.50, "t": 0.70, "h": 0.60, "p": 1.00},  # Dodger Stadium — least weather-volatile, mild year-round
    1:    {"w": 0.60, "t": 0.70, "h": 0.60, "p": 1.00},  # Angel Stadium — stable mild climate
    # Tier 6 (Tropicana, venue 12) — fixed dome, handled by _FIXED_DOMES
}

# Hit-type weather sensitivity relative to HR adjustments
_SINGLES_WX_WEIGHT = 0.15     # singles: 15% of HR weather sensitivity
_XBH_TEMP_WEIGHT = 0.50       # doubles/triples: 50% of HR temp effect
_XBH_WIND_WEIGHT = 0.35       # doubles/triples: 35% of HR wind effect
_XBH_HUMID_WEIGHT = 0.40      # doubles/triples: 40% of HR humidity effect
_XBH_PRESS_WEIGHT = 0.30      # doubles/triples: 30% of HR pressure effect
_HITS_WX_CAP = 25.0           # max ±25% weather modifier for hits

# League-average hit-type frequency weights
_W_1B = 0.70   # singles = 70% of all hits
_W_2B = 0.22   # doubles = 22% of all hits
_W_3B = 0.02   # triples = 2% of all hits
_W_HR = 0.06   # home runs = 6% of all hits

# ── HR rating framework constants ──
_REF_TEMP_F = 70.0           # league-average baseline temperature
_REF_HUMIDITY = 50.0         # baseline humidity %
_REF_PRESSURE_HPA = 1013.25  # sea-level standard pressure
_TEMP_PER_F = 0.65           # +0.65 pct-pts per °F above baseline
_HUMID_PER_PCT = +0.025      # +0.025 pct-pts per 1% humidity above baseline
                             # Humidor era (2022+): all 30 parks store balls at
                             # 70°F / 50-57% RH for 14+ days, neutralising the
                             # hygrothermal "dead ball" effect.  Ambient humidity
                             # now only reduces air density (lighter H₂O displaces
                             # heavier N₂/O₂), giving a small positive carry boost.
_WIND_TAIL_PER_MPH = 0.80    # +0.80 pct-pts per mph tailwind (drag reduction only)
_WIND_HEAD_PER_MPH = 1.20    # -1.20 pct-pts per mph headwind (direct opposition)
                             # Asymmetry: headwind fights ball flight directly;
                             # tailwind only reduces aerodynamic drag — weaker effect.
_WIND_PER_HAND_CAP = 14.0    # max ±14% wind contribution per handedness
_PRESS_PER_HPA = 0.20        # +0.20 pct-pts per hPa below expected
_WX_CAP = 40.0               # max ±40% weather modifier
_RHB_PULL_DEG = 315.0        # RHB pull-side direction (toward LF)
_LHB_PULL_DEG = 45.0         # LHB pull-side direction (toward RF)

# ── Sky condition (cloud cover) adjustments ──
# Overcast/cloudy conditions reduce air density slightly (warmer humid air aloft),
# reduce K-rates, and turn some flyouts into hits.  Clear day sky aids pitchers
# (visibility, higher K-rates).
# Values are additive pct-pt modifiers applied to HR and Hits weather layers.
_SKY_HR_MODIFIER = {
    "Overcast":       +1.8,   # thick cloud cover → +1.8% HR boost
    "Cloudy":         +1.2,   # moderate cloud → +1.2%
    "Partly Cloudy":  +0.4,   # partial cloud → small boost
    "Drizzle":        -0.8,   # wet ball (heavier, less elastic) partly offsets humid air
    "Rain":           -2.5,   # wet ball dominates — heavier, dead on contact
    "Clear":          -0.8,   # clear sky → slight pitcher advantage
}
_SKY_HITS_XBH_MOD = {
    "Overcast":       +1.5,
    "Cloudy":         +1.0,
    "Partly Cloudy":  +0.3,
    "Drizzle":        -0.5,   # wet ball reduces exit velo on line drives
    "Rain":           -2.0,   # heavy wet ball suppresses extra-base power
    "Clear":          -0.6,
}
_SKY_HITS_1B_MOD = {
    "Overcast":       +0.8,   # reduced K-rate → more balls in play → more singles
    "Cloudy":         +0.5,
    "Partly Cloudy":  +0.2,
    "Drizzle":        +0.2,   # wet ball slight offset, but reduced K-rate helps
    "Rain":           -1.0,   # wet ball + slick bat = fewer clean contacts
    "Clear":          -0.4,
}

# ── Day vs Night game adjustments ──
# Night games: cooler/denser air generally favours pitchers, BUT some parks
# flip (e.g. Truist plays as hitter-friendly at night).  Day games: hotter
# air + humidity cycles + glare = complex.  Clear-sky day games see the
# highest K-rates.
# Format: venue_id → (day_modifier, night_modifier) in pct-pts for HR.
# Positive = offence boost, negative = pitcher boost.
_VENUE_DAY_NIGHT: dict[int, tuple[float, float]] = {
    # park:           (day,   night)
    4705:             (-1.5,  +1.5),   # Truist Park — strong day→pitcher / night→hitter flip
    17:               (+0.5,  -0.5),   # Wrigley — day game tradition, heat + wind = hitter-friendly days
    2889:             (-0.5,  +0.3),   # Busch Stadium — moderate day/night split
    2602:             (+0.5,  -0.3),   # Great American — river heat in day games
    3312:             (-0.8,  +0.3),   # Target Field — cold night air suppresses; day less extreme
    3:                (-0.8,  +0.3),   # Fenway — dense cold night air, day heat helps
    5:                (-0.5,  +0.3),   # Progressive — Lake Erie cold night effect
}
# Default: day games are slightly pitcher-friendly (glare + high K-rate in sun),
# night games are neutral-to-slightly pitcher-friendly (dense air).
_DEFAULT_DAY_MOD  = -0.3     # day default: slight pitcher edge
_DEFAULT_NIGHT_MOD = +0.0    # night default: neutral

# ── Closed-roof humidity passthrough ──
# Most retractable / dome parks with AC control indoor humidity to ~50-55%
# (near _REF_HUMIDITY), so humidity effect ≈ 0 when closed.  These venues
# do NOT fully control indoor humidity:
_CLOSED_ROOF_HUMID_MULT: dict[int, float] = {
    32:  1.2,   # American Family — heating only, no AC; traps humid air on hot days
    680: 1.0,   # T-Mobile — carport-style roof, doesn't seal; outdoor humidity passes through
}

# ── Stadium roof classification ──
_FIXED_DOMES: set[int] = {12}  # Tropicana Field

_RETRACTABLE_VENUES: dict[int, dict] = {
    # Globe Life Field (Texas Rangers)
    # Open when: Temp 65–80 °F, Humidity < 50 %
    5325: {"temp_low": 65, "temp_high": 80, "humid_thr": 50,
           "wind_thr": 25, "default": "open"},
    # Chase Field (Arizona Diamondbacks)
    # Open when: Temp < 100 °F
    15:   {"temp_high": 100, "wind_thr": 25, "precip_thr": 20,
           "default": "open"},
    # Minute Maid Park / Daikin Park (Houston Astros)
    # Open when: Temp 65–77 °F, Humidity < 50 %, Dew Point < 55 °F
    2392: {"temp_low": 65, "temp_high": 77, "humid_thr": 50,
           "dew_thr": 55, "wind_thr": 25, "precip_thr": 10,
           "default": "open"},
    # loanDepot park (Miami Marlins)
    # Day/sun: Heat Index < 83, Humidity < 70 %, Rain < 15 %
    # Night/overcast: relaxed to HI < 90, Humidity < 82 %
    # (park_factors.py handles the night/overcast logic; fallback uses day thresholds)
    4169: {"heat_index_thr": 83, "humid_thr": 70, "precip_thr": 15,
           "wind_thr": 25, "default": "open"},
    # American Family Field (Milwaukee Brewers)
    # Open when: Temp ≥ 60–63 °F (use 60 as threshold)
    32:   {"temp_low": 60, "wind_thr": 25, "precip_thr": 15,
           "default": "open"},
    # Rogers Centre (Toronto Blue Jays)
    # Fan comfort + field safety; NO field drainage → low precip threshold
    14:   {"temp_low": 60, "wind_thr": 25, "precip_thr": 15,
           "default": "open"},
    # T-Mobile Park (Seattle Mariners)
    # "Open as much as possible"; stays open ~78 % of games
    680:  {"precip_thr": 50, "wind_thr": 30, "default": "open"},
}


def _predict_roof_status(data: dict) -> tuple[str, bool]:
    """Determine roof status from MLB data, with official-vs-predicted flag.

    Priority
    --------
    1. Fixed dome → ``("dome", True)``
    2. Official MLB status (``Retractable (CLOSED)`` / ``(OPEN)`` without
       trailing ``*``) → trust it, ``confirmed=True``.
    3. Predicted status from ``park_factors.py`` (trailing ``*``) → use it,
       ``confirmed=False``.
    4. Bare ``"Retractable"`` with no suffix (edge case: data hasn't been
       processed by ``park_factors.py``) → run local weather prediction,
       ``confirmed=False``.
    5. Open-air venue → ``("open", True)``

    Returns ``(status, confirmed)`` where *status* is one of
    ``'open'``, ``'closed'``, ``'dome'`` and *confirmed* indicates
    whether MLB has officially reported the roof state.
    """
    venue_id = data.get("venue_id")
    roof = data.get("roof_type", "Open")

    # Fixed dome — always enclosed
    if venue_id in _FIXED_DOMES or roof == "Dome":
        return ("dome", True)

    # Official MLB status (no trailing asterisk)
    if "CLOSED" in roof and not roof.endswith("*"):
        return ("closed", True)
    if "OPEN" in roof and not roof.endswith("*"):
        return ("open", True)

    # Predicted status from park_factors.py (trailing asterisk)
    if "CLOSED" in roof and roof.endswith("*"):
        return ("closed", False)
    if "OPEN" in roof and roof.endswith("*"):
        return ("open", False)

    # Not a retractable venue — open air
    if venue_id not in _RETRACTABLE_VENUES:
        return ("open", True)

    # ── Fallback: predict retractable roof status from weather ──
    cfg = _RETRACTABLE_VENUES[venue_id]
    temp = float(data.get("temp", 72) or 72)
    humid = float(data.get("humidity_pct", 50) or 50)
    wind = float(data.get("wind_speed", 0) or 0)
    precip = float(data.get("precip_pct", 0) or 0)

    # Heat index threshold (Miami)
    hi_thr = cfg.get("heat_index_thr")
    if hi_thr is not None and _heat_index(temp, humid) >= hi_thr:
        return ("closed", False)

    # Dew point threshold (Houston)
    dp_thr = cfg.get("dew_thr")
    if dp_thr is not None and _dew_point(temp, humid) >= dp_thr:
        return ("closed", False)

    # Temperature outside comfort window → closed
    if temp < cfg.get("temp_low", 0) or temp > cfg.get("temp_high", 999):
        return ("closed", False)

    # Humidity above threshold → closed
    if humid > cfg.get("humid_thr", 999):
        return ("closed", False)

    # Wind above threshold → closed
    if wind > cfg.get("wind_thr", 999):
        return ("closed", False)

    # Precipitation chance above threshold → closed
    if precip > cfg.get("precip_thr", 999):
        return ("closed", False)

    return (cfg.get("default", "open"), False)


def _heat_index(temp_f: float, rh: float) -> float:
    """Rothfusz regression heat index (°F).  Valid for T ≥ 80 °F, RH ≥ 40 %."""
    if temp_f < 80:
        return temp_f
    hi = (-42.379 + 2.04901523 * temp_f + 10.14333127 * rh
          - 0.22475541 * temp_f * rh - 6.83783e-3 * temp_f ** 2
          - 5.481717e-2 * rh ** 2 + 1.22874e-3 * temp_f ** 2 * rh
          + 8.5282e-4 * temp_f * rh ** 2 - 1.99e-6 * temp_f ** 2 * rh ** 2)
    return hi


def _dew_point(temp_f: float, rh: float) -> float:
    """Approximate dew point (°F) via Magnus formula."""
    if rh <= 0:
        return temp_f - 40
    tc = (temp_f - 32) * 5.0 / 9.0
    a, b = 17.27, 237.7
    import math as _m
    gamma = (_m.log(rh / 100.0) + a * tc / (b + tc))
    dp_c = b * gamma / (a - gamma)
    return dp_c * 9.0 / 5.0 + 32


def _expected_pressure(elevation_ft: float) -> float:
    """Standard atmospheric pressure at *elevation_ft* (barometric formula)."""
    h_m = elevation_ft * 0.3048
    return _REF_PRESSURE_HPA * (1.0 - 2.25577e-5 * h_m) ** 5.25588


def _wind_carry_pct(component_mph: float) -> float:
    """Convert a wind speed component (positive=tailwind) to carry %."""
    if component_mph >= 0:
        return component_mph * _WIND_TAIL_PER_MPH
    return component_mph * _WIND_HEAD_PER_MPH


def _sky_modifier(condition: str, table: dict) -> float:
    """Return additive pct-pt modifier for current sky/cloud condition."""
    if not condition:
        return 0.0
    cl = condition.strip().lower()
    # Map to canonical key
    if "overcast" in cl:
        return table.get("Overcast", 0.0)
    if "partly" in cl or "mostly clear" in cl:
        return table.get("Partly Cloudy", 0.0)
    if "cloud" in cl or "mist" in cl or "fog" in cl or "haze" in cl:
        return table.get("Cloudy", 0.0)
    if "drizzle" in cl:
        return table.get("Drizzle", 0.0)
    if "rain" in cl or "shower" in cl:
        return table.get("Rain", 0.0)
    if cl in ("clear", "sunny", ""):
        return table.get("Clear", 0.0)
    # Snow/other → no sky modifier
    return 0.0


def _day_night_modifier(data: dict) -> float:
    """Return additive pct-pt modifier for day vs night game."""
    # Determine if night: prefer explicit flag, then hourly slot 0, fall back to game time
    if "night" in data:
        is_night = bool(data["night"])
    else:
        hourly = data.get("hourly_conditions", [])
        if hourly:
            is_night = hourly[0].get("night", False)
        else:
            time_str = str(data.get("time", ""))
            is_night = False
            if "PM" in time_str.upper():
                try:
                    hour = int(time_str.split(":")[0])
                    is_night = hour >= 6 or hour == 12
                except (ValueError, IndexError):
                    pass

    venue_id = data.get("venue_id")
    mods = _VENUE_DAY_NIGHT.get(venue_id)
    if mods:
        return mods[1] if is_night else mods[0]
    return _DEFAULT_NIGHT_MOD if is_night else _DEFAULT_DAY_MOD


def _compute_hr_rating(data: dict, *, _roof_override=None) -> dict:
    """Tiered HR carry rating: static park factor × weather modifier.

    Three stadium categories
    ------------------------
    1. **Fixed dome** — zero weather adjustments; return base park factor.
    2. **Retractable roof** — predict roof status first; if closed, zero
       weather adjustments; if open, apply standard formula.
    3. **Open-air** — full weather adjustments.

    Weather layers (when applicable)
    --------------------------------
    1. Temperature — +0.85 % per °F above 70 °F baseline.
    2. Humidity — +0.08 % per 1 % above 50 % baseline.
    3. Wind — directional & handedness-specific via cosine projection.
    4. Pressure — deviation from expected at venue elevation.
    5. Cap at ±40 %, combined **additively** with park factor.

    Returns dict with total_pct, lhb_pct, rhb_pct, color, roof_status,
    and a ``components`` breakdown.
    """
    # ── Base park factor ──
    venue_id = data.get("venue_id")
    pf_index = VENUE_PARK_FACTORS.get(venue_id, 100)
    park_pct = pf_index - 100                # e.g. 107 → +7%

    # ── Determine roof status (gatekeeper for weather adjustments) ──
    if _roof_override:
        roof_status, roof_confirmed = _roof_override
    else:
        roof_status, roof_confirmed = _predict_roof_status(data)

    # Fixed dome or roof closed → mostly no weather adjustments
    if roof_status in ("dome", "closed"):
        # Some venues don't fully control indoor humidity (no AC / carport roof)
        _closed_hm = _CLOSED_ROOF_HUMID_MULT.get(venue_id, 0.0)
        if _closed_hm > 0:
            _vp_c = VENUE_WX_PROFILE.get(venue_id, {})
            humid_c = float(data.get("humidity_pct", _REF_HUMIDITY) or _REF_HUMIDITY)
            humid_residual = (humid_c - _REF_HUMIDITY) * _HUMID_PER_PCT * _vp_c.get("h", 1.0) * _closed_hm
        else:
            humid_residual = 0.0
        total = park_pct + humid_residual
        color = C["grn"] if total > 8 else C["red"] if total < -8 else C["t1"]
        return {
            "total_pct": float(total),
            "lhb_pct": float(total),
            "rhb_pct": float(total),
            "color": color,
            "roof_status": roof_status,
            "roof_confirmed": roof_confirmed,
            "components": {
                "park_factor": pf_index,
                "temp": 0.0, "humidity": float(humid_residual), "wind": 0.0,
                "wind_rhb": 0.0, "wind_lhb": 0.0,
                "pressure": 0.0, "weather_total": float(humid_residual),
            },
        }

    # ── Roof open or open-air: apply weather adjustments ──

    # Per-venue weather sensitivity profile
    _vp = VENUE_WX_PROFILE.get(venue_id, {})
    _wm = _vp.get("w", 1.0)   # wind multiplier
    _tm = _vp.get("t", 1.0)   # temperature multiplier
    _hm = _vp.get("h", 1.0)   # humidity multiplier
    _pm = _vp.get("p", 1.0)   # pressure/altitude multiplier

    # Step 1: Temperature
    temp_f = float(data.get("temp", _REF_TEMP_F) or _REF_TEMP_F)
    temp_pct = (temp_f - _REF_TEMP_F) * _TEMP_PER_F * _tm

    # Step 2: Humidity
    humid = float(data.get("humidity_pct", _REF_HUMIDITY) or _REF_HUMIDITY)
    humid_pct = (humid - _REF_HUMIDITY) * _HUMID_PER_PCT * _hm

    # Step 3: Wind — trig decomposition per handedness
    wind_speed = float(data.get("wind_speed", 0) or 0)
    wind_dir = data.get("wind_dir", "")
    wind_angle = WIND_ANGLES.get(wind_dir)

    if wind_angle is not None and wind_speed > 0:
        wrad = math.radians(wind_angle)
        rhb_comp = wind_speed * math.cos(wrad - math.radians(_RHB_PULL_DEG))
        lhb_comp = wind_speed * math.cos(wrad - math.radians(_LHB_PULL_DEG))

        wind_pct_rhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _wind_carry_pct(rhb_comp) * _wm))
        wind_pct_lhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _wind_carry_pct(lhb_comp) * _wm))
    else:
        wind_pct_rhb = wind_pct_lhb = 0.0

    # Step 4: Pressure — deviation from expected at elevation
    pressure = data.get("pressure_hpa")
    elev = float(data.get("elevation", 0) or 0)
    if pressure:
        expected_p = _expected_pressure(elev)
        press_pct = (expected_p - float(pressure)) * _PRESS_PER_HPA * _pm
    else:
        press_pct = 0.0

    # Step 5: Sky condition (cloud cover) modifier
    condition = data.get("condition", "")
    sky_pct = _sky_modifier(condition, _SKY_HR_MODIFIER)

    # Step 6: Day vs night modifier
    dn_pct = _day_night_modifier(data)

    # Step 7: Combine weather modifiers per handedness
    wx_rhb = temp_pct + humid_pct + wind_pct_rhb + press_pct + sky_pct + dn_pct
    wx_lhb = temp_pct + humid_pct + wind_pct_lhb + press_pct + sky_pct + dn_pct

    # Step 8: Additive combination — park factor % + weather %
    # Cap the *average* wx (not per-handedness) so asymmetric wind
    # games (e.g. Wrigley blowing out to LF) aren't clipped prematurely.
    rhb_pct = park_pct + wx_rhb
    lhb_pct = park_pct + wx_lhb
    avg_wx = (wx_rhb + wx_lhb) / 2.0
    avg_wx = max(-_WX_CAP, min(_WX_CAP, avg_wx))
    total_pct = park_pct + avg_wx

    # Color coding
    if total_pct > 8:
        color = C["grn"]
    elif total_pct < -8:
        color = C["red"]
    else:
        color = C["t1"]

    return {
        "total_pct": total_pct,
        "lhb_pct": lhb_pct,
        "rhb_pct": rhb_pct,
        "color": color,
        "roof_status": roof_status,
        "roof_confirmed": roof_confirmed,
        "components": {
            "park_factor": pf_index,
            "temp": round(temp_pct, 1),
            "humidity": round(humid_pct, 1),
            "wind": round((wind_pct_rhb + wind_pct_lhb) / 2.0, 1),
            "wind_rhb": round(wind_pct_rhb, 1),
            "wind_lhb": round(wind_pct_lhb, 1),
            "pressure": round(press_pct, 1),
            "weather_total": round((wx_rhb + wx_lhb) / 2.0, 1),
        },
    }


def _compute_wind_insight(data: dict, *, _roof_override=None) -> dict:
    """Compute wind insight with out/cross components.

    Returns dict with keys: description, out_component, cross_component,
    out_label, cross_label.
    """
    wind_speed = data.get("wind_speed", 0) or 0
    wind_dir = data.get("wind_dir", "")
    angle = WIND_ANGLES.get(wind_dir)

    # Enclosed venue — wind is irrelevant
    if _roof_override:
        roof_status, roof_confirmed = _roof_override
    else:
        roof_status, roof_confirmed = _predict_roof_status(data)
    if roof_status in ("dome", "closed"):
        _label = "Roof closed" if roof_confirmed else "Roof likely closed"
        return {
            "description": f"{_label} — no wind effect on ball flight.",
            "out_component": 0.0,
            "cross_component": 0.0,
            "out_label": "None",
            "cross_label": "None",
        }

    if angle is None or wind_speed == 0:
        return {
            "description": "Calm conditions — minimal wind effect on ball flight.",
            "out_component": 0.0,
            "cross_component": 0.0,
            "out_label": "None",
            "cross_label": "None",
        }

    # Decompose into outward (toward CF, 0°) and cross (perpendicular)
    rad = math.radians(angle)
    out = wind_speed * math.cos(rad)   # positive = toward outfield
    cross = wind_speed * math.sin(rad)  # positive = toward RF

    # Venue wind receptiveness tiers based on VENUE_WX_PROFILE "w" multiplier
    venue_id = data.get("venue_id")
    _wm = VENUE_WX_PROFILE.get(venue_id, {}).get("w", 1.0)

    if _wm >= 2.0:
        _out_effect = "ball will carry significantly further"
        _in_effect = "significantly reduced fly ball carry, strongly favours pitchers"
        _cross_verb = "will drift"
    elif _wm >= 1.4:
        _out_effect = "increased HR carry and fly ball distance"
        _in_effect = "reduced fly ball carry, favours pitchers"
        _cross_verb = "will drift"
    elif _wm >= 1.0:
        _out_effect = "ball may carry further"
        _in_effect = "may reduce fly ball carry, slight pitcher advantage"
        _cross_verb = "may drift"
    else:
        _out_effect = "ball could carry slightly further, though park is sheltered"
        _in_effect = "minimal impact — park is sheltered from wind"
        _cross_verb = "may drift slightly"

    # Description
    if wind_dir in HITTER_FRIENDLY:
        desc = f"Wind blowing out at {wind_speed} mph — {_out_effect}."
    elif wind_dir in PITCHER_FRIENDLY:
        desc = f"Wind blowing in at {wind_speed} mph — {_in_effect}."
    elif "L To R" in wind_dir or "R To L" in wind_dir:
        side = "right" if "L To R" in wind_dir else "left"
        desc = (f"Crosswind at {wind_speed} mph pushing toward {side} field — "
                f"balls {_cross_verb} {side}.")
    else:
        desc = f"Wind at {wind_speed} mph {wind_dir}."

    cross_dir = "→ RF" if cross > 0 else "← LF" if cross < 0 else "None"

    return {
        "description": desc,
        "out_component": out,
        "cross_component": abs(cross),
        "out_label": f"{'Out' if out >= 0 else 'In'} {abs(out):.1f} mph",
        "cross_label": f"{abs(cross):.1f} mph {cross_dir}" if cross != 0 else "None",
    }


def _compute_hits_rating(data: dict, *, _roof_override=None) -> dict:
    """Predict hit-type adjustments: singles, doubles/triples, and overall.

    Uses per-venue hit-type park factors with weather adjustments scaled
    by sensitivity:
      - Singles:  30 % of HR weather sensitivity
      - 2B/3B:   60–100 % of HR weather sensitivity (varies by component)
      - HR:      100 % (computed separately)

    For enclosed venues (dome / closed roof), weather adjustments are zero
    and only base park factors apply.

    Returns dict with xbh_pct, xbh_lhb, xbh_rhb, singles_pct, singles_lhb,
    singles_rhb, overall_pct, color, and roof_status.
    """
    venue_id = data.get("venue_id")
    hf = VENUE_HIT_FACTORS.get(venue_id, {"1B": 1.00, "2B": 1.00, "3B": 1.00})
    # Park-factor percentages (additive base)
    pf_1b_pct = (hf["1B"] - 1.0) * 100.0
    pf_2b_pct = (hf["2B"] - 1.0) * 100.0
    pf_3b_pct = (hf["3B"] - 1.0) * 100.0
    # Combined 2B/3B park-factor % (weighted: 2B≈92%, 3B≈8%)
    pf_xbh_pct = pf_2b_pct * 0.92 + pf_3b_pct * 0.08

    # HR park factor % for overall weighted calc
    hr_park_pct = float(VENUE_PARK_FACTORS.get(venue_id, 100) - 100)

    if _roof_override:
        roof_status, _ = _roof_override
    else:
        roof_status, _ = _predict_roof_status(data)

    # ── Enclosed: mostly no weather adjustments ──
    if roof_status in ("dome", "closed"):
        # Residual humidity for non-AC / carport venues
        _closed_hm = _CLOSED_ROOF_HUMID_MULT.get(venue_id, 0.0)
        if _closed_hm > 0:
            _vp_c = VENUE_WX_PROFILE.get(venue_id, {})
            humid_c = float(data.get("humidity_pct", _REF_HUMIDITY) or _REF_HUMIDITY)
            h_res = (humid_c - _REF_HUMIDITY) * _HUMID_PER_PCT * _vp_c.get("h", 1.0) * _closed_hm
        else:
            h_res = 0.0
        s_res = h_res * _SINGLES_WX_WEIGHT
        x_res = h_res * _XBH_HUMID_WEIGHT
        overall = ((pf_1b_pct + s_res) * _W_1B + (pf_2b_pct + x_res) * _W_2B
                   + (pf_3b_pct + x_res) * _W_3B + (hr_park_pct + h_res) * _W_HR)
        color = C["grn"] if overall > 8 else C["red"] if overall < -8 else C["t1"]
        return {
            "xbh_pct": pf_xbh_pct + x_res, "xbh_lhb": pf_xbh_pct + x_res,
            "xbh_rhb": pf_xbh_pct + x_res,
            "singles_pct": pf_1b_pct + s_res, "singles_lhb": pf_1b_pct + s_res,
            "singles_rhb": pf_1b_pct + s_res,
            "overall_pct": overall, "color": color,
            "roof_status": roof_status,
        }

    # ── Open / outdoor: calculate weather adjustments ──

    # Per-venue weather sensitivity profile
    _vp = VENUE_WX_PROFILE.get(venue_id, {})
    _wm = _vp.get("w", 1.0)   # wind multiplier
    _tm = _vp.get("t", 1.0)   # temperature multiplier
    _hm = _vp.get("h", 1.0)   # humidity multiplier
    _pm = _vp.get("p", 1.0)   # pressure/altitude multiplier

    # Temperature
    temp_f = float(data.get("temp", _REF_TEMP_F) or _REF_TEMP_F)
    temp_pct = (temp_f - _REF_TEMP_F) * _TEMP_PER_F * _tm

    # Humidity
    humid = float(data.get("humidity_pct", _REF_HUMIDITY) or _REF_HUMIDITY)
    humid_pct = (humid - _REF_HUMIDITY) * _HUMID_PER_PCT * _hm

    # Wind — directional per handedness
    wind_speed = float(data.get("wind_speed", 0) or 0)
    wind_dir = data.get("wind_dir", "")
    wind_angle = WIND_ANGLES.get(wind_dir)

    if wind_angle is not None and wind_speed > 0:
        wrad = math.radians(wind_angle)
        out_comp = wind_speed * math.cos(wrad)
        rhb_comp = wind_speed * math.cos(wrad - math.radians(_RHB_PULL_DEG))
        lhb_comp = wind_speed * math.cos(wrad - math.radians(_LHB_PULL_DEG))
        wind_pct = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _wind_carry_pct(out_comp) * _wm))
        wind_pct_rhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _wind_carry_pct(rhb_comp) * _wm))
        wind_pct_lhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _wind_carry_pct(lhb_comp) * _wm))
    else:
        wind_pct = wind_pct_rhb = wind_pct_lhb = 0.0

    # Pressure
    pressure = data.get("pressure_hpa")
    elev = float(data.get("elevation", 0) or 0)
    if pressure:
        expected_p = _expected_pressure(elev)
        press_pct = (expected_p - float(pressure)) * _PRESS_PER_HPA * _pm
    else:
        press_pct = 0.0

    # Sky condition (cloud cover) modifier
    condition = data.get("condition", "")
    sky_xbh = _sky_modifier(condition, _SKY_HITS_XBH_MOD)
    sky_1b  = _sky_modifier(condition, _SKY_HITS_1B_MOD)
    sky_hr  = _sky_modifier(condition, _SKY_HR_MODIFIER)

    # Day vs night modifier
    dn_pct = _day_night_modifier(data)
    # Scale day/night for hits: XBH gets 50%, singles gets 25%
    dn_xbh = dn_pct * 0.50
    dn_1b  = dn_pct * 0.25

    # ── Singles weather (15% of HR sensitivity + sky + day/night) ──
    s_wx = (temp_pct + humid_pct + wind_pct + press_pct) * _SINGLES_WX_WEIGHT + sky_1b + dn_1b
    s_wx = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx))
    s_wx_rhb = (temp_pct + humid_pct + wind_pct_rhb + press_pct) * _SINGLES_WX_WEIGHT + sky_1b + dn_1b
    s_wx_rhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx_rhb))
    s_wx_lhb = (temp_pct + humid_pct + wind_pct_lhb + press_pct) * _SINGLES_WX_WEIGHT + sky_1b + dn_1b
    s_wx_lhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx_lhb))

    # ── 2B/3B weather (component-weighted fraction of HR + sky + day/night) ──
    x_wx = (temp_pct * _XBH_TEMP_WEIGHT + humid_pct * _XBH_HUMID_WEIGHT
            + wind_pct * _XBH_WIND_WEIGHT + press_pct * _XBH_PRESS_WEIGHT) + sky_xbh + dn_xbh
    x_wx = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx))
    x_wx_rhb = (temp_pct * _XBH_TEMP_WEIGHT + humid_pct * _XBH_HUMID_WEIGHT
                + wind_pct_rhb * _XBH_WIND_WEIGHT + press_pct * _XBH_PRESS_WEIGHT) + sky_xbh + dn_xbh
    x_wx_rhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx_rhb))
    x_wx_lhb = (temp_pct * _XBH_TEMP_WEIGHT + humid_pct * _XBH_HUMID_WEIGHT
                + wind_pct_lhb * _XBH_WIND_WEIGHT + press_pct * _XBH_PRESS_WEIGHT) + sky_xbh + dn_xbh
    x_wx_lhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx_lhb))

    # ── Additive: park factor % + weather % ──
    singles_pct = pf_1b_pct + s_wx
    singles_rhb = pf_1b_pct + s_wx_rhb
    singles_lhb = pf_1b_pct + s_wx_lhb

    xbh_pct = pf_xbh_pct + x_wx
    xbh_rhb = pf_xbh_pct + x_wx_rhb
    xbh_lhb = pf_xbh_pct + x_wx_lhb

    # HR weather (100% sensitivity + sky + day/night) for overall calc
    hr_wx = (temp_pct + humid_pct
             + (wind_pct_rhb + wind_pct_lhb) / 2.0 + press_pct + sky_hr + dn_pct)
    hr_wx = max(-_WX_CAP, min(_WX_CAP, hr_wx))

    # ── Overall weighted hit adjustment ──
    overall = ((pf_1b_pct + s_wx) * _W_1B + (pf_2b_pct + x_wx) * _W_2B
               + (pf_3b_pct + x_wx) * _W_3B
               + (hr_park_pct + hr_wx) * _W_HR)

    color = C["grn"] if overall > 8 else C["red"] if overall < -8 else C["t1"]

    return {
        "xbh_pct": xbh_pct, "xbh_lhb": xbh_lhb, "xbh_rhb": xbh_rhb,
        "singles_pct": singles_pct, "singles_lhb": singles_lhb, "singles_rhb": singles_rhb,
        "overall_pct": overall, "color": color,
        "roof_status": roof_status,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Stadium outline widget with wind arrows and warning track
# ═══════════════════════════════════════════════════════════════════════════════

_HP_X, _HP_Y = 125.42, 198.27   # home plate in statcast coords


class StadiumWidget(QWidget):
    """Painted stadium outline with orange walls, warning track, wind arrows,
    and optional team logo centred on the field."""

    W, H = 320, 280

    def __init__(self, data: dict, parent=None):
        super().__init__(parent)
        self.d = data
        self.setFixedSize(self.W, self.H)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        home = data.get("home", "")
        self._segments, _ = _get_stadium_segments(home)
        self._logo_pm = self._load_logo(home, 40)

    @staticmethod
    def _load_logo(abbr, size=40):
        abbr = _ABBR_NORM.get(abbr, abbr)
        return _cached_logo_pixmap(abbr, size)

    def _to_canvas(self, sx, sy):
        """Convert statcast coords → widget pixel coords."""
        w, h = self.W, self.H
        pad = 12
        usable_w = w - 2 * pad
        usable_h = h - 2 * pad
        scale = min(usable_w / 210.0, usable_h / 215.0)
        cx = w / 2
        cy = pad + 177.0 * scale
        px = cx + (sx - _HP_X) * scale
        py = cy - (_HP_Y - sy) * scale
        return px, py

    def set_wind(self, wind_dir: str, wind_speed: int):
        """Update wind and repaint."""
        self.d["wind_dir"] = wind_dir
        self.d["wind_speed"] = wind_speed
        self.d["wind_angle"] = WIND_ANGLES.get(wind_dir)
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        hpx, hpy = self._to_canvas(_HP_X, _HP_Y)

        # ── 1a) Outfield grass fill (inside outfield_inner) ──
        if self._segments:
            inner_pts = self._segments.get("outfield_inner")
            if inner_pts:
                grass_path = QPainterPath()
                first = True
                for sx, sy in inner_pts:
                    px, py = self._to_canvas(sx, sy)
                    if first:
                        grass_path.moveTo(px, py)
                        first = False
                    else:
                        grass_path.lineTo(px, py)
                grass_path.lineTo(hpx, hpy)
                grass_path.closeSubpath()

                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(QBrush(QColor(C["bg2"])))
                p.drawPath(grass_path)

        # ── 1b) Infield dirt (semi-circle arc → bg1) ──
        if self._segments:
            inf_outer = self._segments.get("infield_outer")
            if inf_outer:
                dirt_path = QPainterPath()
                first = True
                for sx, sy in inf_outer:
                    px, py = self._to_canvas(sx, sy)
                    if first:
                        dirt_path.moveTo(px, py)
                        first = False
                    else:
                        dirt_path.lineTo(px, py)
                dirt_path.lineTo(hpx, hpy)
                dirt_path.closeSubpath()

                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(QBrush(QColor(C["bg1"])))
                p.drawPath(dirt_path)

        # ── 1c) Infield diamond grass (inside basepaths) ──
        if self._segments:
            inf_inner = self._segments.get("infield_inner")
            if inf_inner:
                diamond_path = QPainterPath()
                first = True
                for sx, sy in inf_inner:
                    px, py = self._to_canvas(sx, sy)
                    if first:
                        diamond_path.moveTo(px, py)
                        first = False
                    else:
                        diamond_path.lineTo(px, py)
                diamond_path.closeSubpath()

                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(QBrush(QColor(C["bg2"])))
                p.drawPath(diamond_path)

        # ── 2) Stadium outlines ──
        if self._segments:
            # outfield_outer = walls → orange, rest → t2
            for seg_name, color, width in [
                ("outfield_outer", C["ora"],  2.2),
                ("outfield_inner", C["t2"],   1.2),
                ("infield_outer",  C["t2"],   1.0),
                ("infield_inner",  C["t2"],   0.8),
            ]:
                pts = self._segments.get(seg_name)
                if not pts:
                    continue
                p.setPen(QPen(QColor(color), width))
                p.setBrush(Qt.BrushStyle.NoBrush)
                path = QPainterPath()
                first = True
                for sx, sy in pts:
                    px, py = self._to_canvas(sx, sy)
                    if first:
                        path.moveTo(px, py)
                        first = False
                    else:
                        path.lineTo(px, py)
                p.drawPath(path)

            # Foul lines
            fl = self._segments.get("foul_lines")
            if fl:
                p.setPen(QPen(QColor(C["t2"]), 0.8))
                path = QPainterPath()
                first = True
                for sx, sy in fl:
                    px, py = self._to_canvas(sx, sy)
                    if first:
                        path.moveTo(px, py)
                        first = False
                    else:
                        path.lineTo(px, py)
                p.drawPath(path)
        else:
            # Fallback: generic arc
            p.setPen(QPen(QColor(C["ora"]), 2.0))
            p.setBrush(Qt.BrushStyle.NoBrush)
            arc = QPainterPath()
            radius = min(self.W, self.H) * 0.55
            for s in range(91):
                a = math.radians(-45 + s)
                px = hpx + radius * math.sin(a)
                py = hpy - radius * math.cos(a)
                if s == 0:
                    arc.moveTo(px, py)
                else:
                    arc.lineTo(px, py)
            p.drawPath(arc)

        # ── 3) Infield diamond outline + bases ──
        s = 22
        scale = min((self.W - 24) / 210.0, (self.H - 24) / 215.0)
        s_scaled = s * scale / 1.0
        home = QPointF(hpx, hpy)
        first_b = QPointF(hpx + s_scaled, hpy - s_scaled)
        second_b = QPointF(hpx, hpy - 2 * s_scaled)
        third_b = QPointF(hpx - s_scaled, hpy - s_scaled)

        # Base squares
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(QColor("#cccccc")))
        bsz = 3
        for bp in (first_b, second_b, third_b):
            p.drawRect(QRectF(bp.x() - bsz, bp.y() - bsz, bsz * 2, bsz * 2))
        p.drawRect(QRectF(hpx - bsz, hpy - bsz, bsz * 2, bsz * 2))

        # ── 4) Wind arrows ──
        roof = self.d.get("roof_type", "Open")
        is_dome = roof == "Dome" or "CLOSED" in roof
        angle = self.d.get("wind_angle")
        if angle is not None and not is_dome:
            wd = self.d.get("wind_dir", "")
            if wd in HITTER_FRIENDLY:
                arrow_col = QColor(C["grn"])
            elif wd in PITCHER_FRIENDLY:
                arrow_col = QColor(C["red"])
            else:
                arrow_col = QColor(C["amb"])
            arrow_col.setAlpha(210)

            # Arrow positions — centred in outfield grass
            arrow_zones = [
                (0.72, -30),  # LF
                (0.75,   0),  # CF
                (0.72,  30),  # RF
            ]
            field_r = min(self.W, self.H) * 0.50
            for frac, zone_deg in arrow_zones:
                zrad = math.radians(zone_deg)
                dist = field_r * frac
                ax = hpx + dist * math.sin(zrad)
                ay = hpy - dist * math.cos(zrad)
                self._draw_arrow(p, ax, ay, angle, arrow_col, length=32)



        # ── 6) LF / RF labels ──
        p.setFont(QFont("Segoe UI", 9, QFont.Weight.Medium))
        p.setPen(QColor(C["t3"]))
        p.drawText(QRectF(8, hpy - 40, 28, 14),
                   Qt.AlignmentFlag.AlignCenter, "LF")
        p.drawText(QRectF(self.W - 36, hpy - 40, 28, 14),
                   Qt.AlignmentFlag.AlignCenter, "RF")

        p.end()

    @staticmethod
    def _draw_arrow(p: QPainter, cx: float, cy: float, angle_deg: float,
                    color: QColor, length: float = 36):
        p.save()
        p.translate(cx, cy)
        p.rotate(angle_deg)

        half = length / 2
        head = 9

        p.setPen(QPen(color, 2.5, Qt.PenStyle.SolidLine,
                       Qt.PenCapStyle.RoundCap))
        p.drawLine(QPointF(0, half), QPointF(0, -half + head))

        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(color))
        tri = QPainterPath()
        tri.moveTo(0, -half - 2)
        tri.lineTo(-head * 0.7, -half + head)
        tri.lineTo(head * 0.7, -half + head)
        tri.closeSubpath()
        p.drawPath(tri)

        p.restore()


# ═══════════════════════════════════════════════════════════════════════════════
# Hourly time-slot switcher
# ═══════════════════════════════════════════════════════════════════════════════

class HourlyToggle(QWidget):
    """4-position toggle slider for hourly time slots."""

    indexChanged = pyqtSignal(int)

    _SLOT_W = 44
    _H = 24
    _RADIUS = 4

    def __init__(self, hourly: list[dict], parent=None):
        super().__init__(parent)
        self._labels = [s.get("hour", f"H{i+1}") for i, s in enumerate(hourly[:4])]
        self._count = len(self._labels)
        self._idx = 0
        self.setFixedSize(self._SLOT_W * self._count, self._H)
        self.setCursor(Qt.CursorShape.PointingHandCursor)

    def _select(self, idx):
        if idx == self._idx:
            return
        self._idx = idx
        self.update()
        self.indexChanged.emit(idx)

    def current_index(self) -> int:
        return self._idx

    def mousePressEvent(self, ev):
        idx = int(ev.position().x()) // self._SLOT_W
        if 0 <= idx < self._count:
            self._select(idx)

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h = self.width(), self.height()
        r = self._RADIUS

        # Track background
        track = QPainterPath()
        track.addRoundedRect(QRectF(0, 0, w, h), r, r)
        p.setPen(QPen(QColor(C["bdr"]), 1))
        p.setBrush(QBrush(QColor(C["bg3"])))
        p.drawPath(track)

        # Active thumb
        tx = self._idx * self._SLOT_W
        thumb = QPainterPath()
        thumb.addRoundedRect(QRectF(tx + 1, 1, self._SLOT_W - 2, h - 2), r, r)
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(QColor(C["ora"])))
        p.drawPath(thumb)

        # Labels
        font = QFont("Segoe UI", 9)
        for i, lbl in enumerate(self._labels):
            lx = i * self._SLOT_W
            is_active = i == self._idx
            font.setWeight(QFont.Weight.Bold if is_active else QFont.Weight.Medium)
            p.setFont(font)
            p.setPen(QColor(C["bg0"] if is_active else C["t2"]))
            p.drawText(QRectF(lx, 0, self._SLOT_W, h),
                       Qt.AlignmentFlag.AlignCenter, lbl)

        p.end()


# ═══════════════════════════════════════════════════════════════════════════════
# Weather condition painters (ported from park_factors.py)
# ═══════════════════════════════════════════════════════════════════════════════

def _paint_sun(p: QPainter, cx: float, cy: float, sz: float = 22):
    """Yellow sun with rays."""
    p.setPen(Qt.PenStyle.NoPen)
    p.setBrush(QBrush(QColor("#fbbf24")))
    core = sz * 0.35
    p.drawEllipse(QPointF(cx, cy), core, core)
    p.setPen(QPen(QColor("#fbbf24"), 1.8))
    for i in range(8):
        rad = math.radians(i * 45)
        x1 = cx + sz * 0.45 * math.cos(rad)
        y1 = cy + sz * 0.45 * math.sin(rad)
        x2 = cx + sz * 0.65 * math.cos(rad)
        y2 = cy + sz * 0.65 * math.sin(rad)
        p.drawLine(QPointF(x1, y1), QPointF(x2, y2))


def _paint_moon(p: QPainter, cx: float, cy: float, sz: float = 22):
    """Crescent moon for nighttime clear conditions."""
    r = sz * 0.38
    p.setPen(Qt.PenStyle.NoPen)
    p.setBrush(QBrush(QColor("#e8d44d")))
    p.drawEllipse(QPointF(cx, cy), r, r)
    p.setBrush(QBrush(QColor(0, 0, 0, 0)))
    p.setCompositionMode(QPainter.CompositionMode.CompositionMode_Clear)
    p.drawEllipse(QPointF(cx + r * 0.6, cy - r * 0.25), r * 0.85, r * 0.85)
    p.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)


def _paint_cloud(p: QPainter, cx: float, cy: float, sz: float = 24,
                 color: str = "#78909c"):
    """Simple cloud blob."""
    p.setPen(Qt.PenStyle.NoPen)
    p.setBrush(QBrush(QColor(color)))
    r = sz * 0.22
    p.drawEllipse(QPointF(cx - r * 1.1, cy + r * 0.15), r * 1.0, r * 0.85)
    p.drawEllipse(QPointF(cx, cy - r * 0.3), r * 1.3, r * 1.1)
    p.drawEllipse(QPointF(cx + r * 1.1, cy + r * 0.15), r * 1.0, r * 0.85)
    p.drawRoundedRect(QRectF(cx - r * 2.0, cy, r * 4.0, r * 1.0), 2, 2)


def _paint_rain_drops(p: QPainter, cx: float, cy: float, sz: float = 24):
    """Three small rain streaks beneath a cloud."""
    pen = QPen(QColor("#60a5fa"), 1.5)
    p.setPen(pen)
    for dx in (-sz * 0.2, 0, sz * 0.2):
        x = cx + dx
        p.drawLine(QPointF(x, cy + sz * 0.35),
                    QPointF(x - 2, cy + sz * 0.55))


def _paint_condition(p: QPainter, cx: float, cy: float, cond: str,
                     sz: float = 26, *, night: bool = False):
    """Dispatch to the appropriate icon painter based on MLB condition text."""
    _clear_icon = _paint_moon if night else _paint_sun
    cl = cond.strip().lower()
    if "rain" in cl or "drizzle" in cl or "shower" in cl or "thunder" in cl:
        _paint_cloud(p, cx, cy, sz, "#607d8b")
        _paint_rain_drops(p, cx, cy, sz)
    elif cl in ("sunny", "clear") or "mostly clear" in cl or cl == "":
        _clear_icon(p, cx, cy, sz)
    elif "partly" in cl or "patchy" in cl:
        _clear_icon(p, cx - sz * 0.15, cy - sz * 0.1, sz * 0.75)
        _paint_cloud(p, cx + sz * 0.15, cy + sz * 0.15, sz * 0.8)
    elif "cloud" in cl or "overcast" in cl or "mist" in cl or "fog" in cl or "haze" in cl:
        _paint_cloud(p, cx, cy, sz)
    elif "snow" in cl or "sleet" in cl or "ice" in cl:
        _paint_cloud(p, cx, cy, sz, "#b0bec5")
    elif cl == "dome":
        p.setPen(QPen(QColor(C["t3"]), 1))
        p.drawText(QRectF(cx - 12, cy - 6, 24, 12),
                    Qt.AlignmentFlag.AlignCenter, "—")
    else:
        _clear_icon(p, cx - sz * 0.15, cy - sz * 0.1, sz * 0.75)
        _paint_cloud(p, cx + sz * 0.15, cy + sz * 0.15, sz * 0.8)


class WeatherOverlay(QWidget):
    """Compact weather icon + temp + condition for hourly slots."""

    _W, _H = 320, 36
    _ICON_SZ = 22
    _GAP1 = 4   # icon -> temp
    _GAP2 = 6   # temp -> condition

    def __init__(self, hourly: list[dict], parent=None):
        super().__init__(parent)
        self.setFixedSize(self._W, self._H)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._slots = hourly if hourly else []
        self._idx = 0
        self._frames: list[QPixmap] = []

        temp_font = QFont("Segoe UI", 10, QFont.Weight.Bold)
        cond_font = QFont("Segoe UI", 9)

        for slot in self._slots:
            pm = QPixmap(self._W, self._H)
            pm.fill(QColor(0, 0, 0, 0))
            p = QPainter(pm)
            p.setRenderHint(QPainter.RenderHint.Antialiasing)

            temp = slot.get("temp", "")
            temp_str = f"{temp}°F" if temp != "" else ""
            cond_str = slot.get("condition", "")

            # Measure text widths for centering
            temp_w = QFontMetricsF(temp_font).horizontalAdvance(temp_str)
            cond_w = QFontMetricsF(cond_font).horizontalAdvance(cond_str)
            total_w = self._ICON_SZ + self._GAP1 + temp_w + self._GAP2 + cond_w
            x0 = (self._W - total_w) / 2.0

            # Weather icon
            _paint_condition(p, x0 + self._ICON_SZ / 2, self._H / 2,
                             cond_str, self._ICON_SZ,
                             night=slot.get("night", False))

            # Temp
            tx = x0 + self._ICON_SZ + self._GAP1
            p.setFont(temp_font)
            p.setPen(QColor(C["t1"]))
            p.drawText(QRectF(tx, 0, temp_w + 2, self._H),
                       Qt.AlignmentFlag.AlignVCenter, temp_str)

            # Condition
            cx = tx + temp_w + self._GAP2
            p.setFont(cond_font)
            p.setPen(QColor(C["t2"]))
            p.drawText(QRectF(cx, 0, cond_w + 2, self._H),
                       Qt.AlignmentFlag.AlignVCenter, cond_str)

            p.end()
            self._frames.append(pm)

    def set_index(self, idx: int):
        """Jump to a specific hourly slot (used by HourlyToggle)."""
        if idx == self._idx or idx < 0 or idx >= len(self._frames):
            return
        self._idx = idx
        self.update()

    def paintEvent(self, _):
        if not self._frames:
            return
        p = QPainter(self)
        p.drawPixmap(0, 0, self._frames[self._idx])
        p.end()


# ═══════════════════════════════════════════════════════════════════════════════
# Conditions table row helper
# ═══════════════════════════════════════════════════════════════════════════════

def _cond_row(grid: QGridLayout, row: int, label_text: str, value_text: str):
    """Add a single row to the conditions grid."""
    lbl = _mk(label_text, color=C["t2"], size=11)
    val = _mk(value_text, color=C["t1"], size=11, bold=True)
    val.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    grid.addWidget(lbl, row, 0)
    grid.addWidget(val, row, 1)
    return val  # return so caller can update later


# ═══════════════════════════════════════════════════════════════════════════════
# Main weather detail widget
# ═══════════════════════════════════════════════════════════════════════════════

class WeatherDetailWidget(QFrame):
    """Expanded weather detail card.

    Accepts the same ``data: dict`` as ``ParkWeatherCard`` from the weather
    cache.  Provides:
      - Team header with logos and matchup
      - Stadium outline with wind arrows and orange wall outline
      - HR rating with LHB / RHB splits
      - Wind insight panel
      - Conditions table
      - 4-hour time-slot switcher
    """

    def __init__(self, data: dict, parent=None):
        super().__init__(parent)
        self.d = data
        self._hourly = data.get("hourly_conditions", [])
        self._cond_vals: dict[str, QLabel] = {}

        # Sync base data with the first hourly slot so the conditions table,
        # HR rating, and weather overlay all agree on initial load.
        if self._hourly:
            h0 = self._hourly[0]
            self.d["temp"] = h0.get("temp", self.d.get("temp"))
            self.d["condition"] = h0.get("condition", self.d.get("condition"))
            self.d["wind_speed"] = h0.get("wind_speed", self.d.get("wind_speed", 0))
            self.d["wind_dir"] = h0.get("wind_dir", self.d.get("wind_dir", ""))
            self.d["wind_angle"] = WIND_ANGLES.get(self.d["wind_dir"])
            self.d["night"] = h0.get("night", False)

        self.setStyleSheet(
            f"WeatherDetailWidget {{ background:{C['bg1']}; "
            f"border:1px solid {C['bdr']}; border-radius:8px; }}")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setMinimumWidth(520)
        self._build()

    # ── logo helper ──
    @staticmethod
    def _logo(abbr, size=22):
        abbr = _ABBR_NORM.get(abbr, abbr)
        return _cached_logo_pixmap(abbr, size)

    @staticmethod
    def _split_color(pct: float) -> str:
        if pct > 8:
            return C["grn"]
        elif pct < -8:
            return C["red"]
        return C["t1"]

    @staticmethod
    def _hits_color(pct: float) -> str:
        if pct > 8:
            return C["grn"]
        elif pct < -8:
            return C["red"]
        return C["t1"]  # white for neutral

    def _update_hits_labels(self, hits: dict):
        """Refresh all hits-predict labels from a computed result dict."""
        xc = self._hits_color(hits["xbh_pct"])
        self._hits_xbh_total.setText(f"{hits['xbh_pct']:+.1f}%")
        self._hits_xbh_total.setStyleSheet(
            f"color:{xc}; background:transparent; "
            f"font-family:'Segoe UI'; font-size:24px; font-weight:700;")
        xl = self._hits_color(hits["xbh_lhb"])
        xr = self._hits_color(hits["xbh_rhb"])
        self._hits_xbh_lhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>LHB:</span> "
            f"<span style='color:{xl}; font-weight:700'>{hits['xbh_lhb']:+.1f}%</span>")
        self._hits_xbh_rhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>RHB:</span> "
            f"<span style='color:{xr}; font-weight:700'>{hits['xbh_rhb']:+.1f}%</span>")

        sc = self._hits_color(hits["singles_pct"])
        self._hits_s_total.setText(f"{hits['singles_pct']:+.1f}%")
        self._hits_s_total.setStyleSheet(
            f"color:{sc}; background:transparent; "
            f"font-family:'Segoe UI'; font-size:24px; font-weight:700;")
        sl = self._hits_color(hits["singles_lhb"])
        sr = self._hits_color(hits["singles_rhb"])
        self._hits_s_lhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>LHB:</span> "
            f"<span style='color:{sl}; font-weight:700'>{hits['singles_lhb']:+.1f}%</span>")
        self._hits_s_rhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>RHB:</span> "
            f"<span style='color:{sr}; font-weight:700'>{hits['singles_rhb']:+.1f}%</span>")

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 10, 14, 14)
        root.setSpacing(8)

        # ── Header: logos + matchup ──
        hdr = QHBoxLayout()
        hdr.setSpacing(6)
        hdr.setContentsMargins(0, 0, 0, 0)

        away, home = self.d.get("away", ""), self.d.get("home", "")
        for abbr in (away, home):
            logo_lbl = QLabel()
            logo_lbl.setFixedSize(22, 22)
            logo_lbl.setStyleSheet("border:none; padding:0; background:transparent;")
            pm = self._logo(abbr)
            if pm:
                logo_lbl.setPixmap(pm)
            hdr.addWidget(logo_lbl)
            hdr.addWidget(_mk(abbr, color=C["t1"], size=13, bold=True))
            if abbr == away:
                hdr.addWidget(_mk("@", color=C["t3"], size=12))

        hdr.addStretch()

        time_lbl = _mk(self.d.get("time", "TBD"), color=C["t3"], size=11)
        hdr.addWidget(time_lbl)
        root.addLayout(hdr)

        # ── Separator ──
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color:{C['bdr']};")
        sep.setFixedHeight(1)
        root.addWidget(sep)

        # ── Body: left (stadium + switcher) | divider | right (ratings + conditions) ──
        body = QHBoxLayout()
        body.setSpacing(0)

        # ── LEFT PANEL ──
        left = QVBoxLayout()
        left.setSpacing(6)
        left.setContentsMargins(0, 0, 10, 0)

        self._stadium = StadiumWidget(self.d)
        left.addWidget(self._stadium, alignment=Qt.AlignmentFlag.AlignCenter)

        # Weather condition overlay (cycles through hourly slots)
        if self._hourly and len(self._hourly) > 0:
            self._wx_overlay = WeatherOverlay(self._hourly)
            left.addWidget(self._wx_overlay, alignment=Qt.AlignmentFlag.AlignCenter)
        else:
            cond = self.d.get("condition", "")
            self._wx_overlay = WeatherOverlay([{"hour": "", "condition": cond}])
            left.addWidget(self._wx_overlay, alignment=Qt.AlignmentFlag.AlignCenter)

        # Hourly toggle slider
        if self._hourly:
            self._switcher = HourlyToggle(self._hourly)
            self._switcher.indexChanged.connect(self._on_hour_change)
            left.addWidget(self._switcher, alignment=Qt.AlignmentFlag.AlignCenter)
        else:
            self._switcher = None

        # Venue name + roof type (use _predict_roof_status for proper label)
        venue = self.d.get("venue_name", "")
        venue_id = self.d.get("venue_id")
        roof_status, roof_confirmed = _predict_roof_status(self.d)
        if roof_status == "dome":
            venue = f"{venue}  ·  Dome"
        elif roof_status in ("open", "closed"):
            roof = self.d.get("roof_type", "Open")
            if "Retractable" in roof or venue_id in _RETRACTABLE_VENUES:
                star = "" if roof_confirmed else "*"
                venue = f"{venue}  ·  Retractable ({roof_status.upper()}){star}"
        left.addWidget(_mk(venue, color=C["t3"], size=10))

        # Stadium size descriptor
        size_desc = VENUE_SIZE_DESC.get(venue_id, "")
        if size_desc:
            left.addWidget(_mk(f"Stadium Size: {size_desc}", color=C["t3"], size=10))

        # Park bias badge (same style as park_factors.py)
        pf_val = VENUE_PARK_FACTORS.get(venue_id)
        if pf_val is not None:
            if pf_val >= 102:
                pf_tag = f"Hitter ({pf_val})"
                pf_color = C["grn"]
            elif pf_val <= 98:
                pf_tag = f"Pitcher ({pf_val})"
                pf_color = C["red"]
            else:
                pf_tag = f"Neutral ({pf_val})"
                pf_color = C["t2"]
            t3 = C["t3"]
            self._pf_lbl = QLabel(
                f"<span style='color:{t3}'>Park Bias:</span> "
                f"<span style='color:{pf_color}; font-weight:600'>{pf_tag}</span>")
            self._pf_lbl.setStyleSheet(
                f"background:transparent; border:none; "
                f"font-family:'Segoe UI'; font-size:10px;")
            left.addWidget(self._pf_lbl)

        left.addStretch()
        body.addLayout(left)

        # ── VERTICAL DIVIDER ──
        divider = QFrame()
        divider.setFrameShape(QFrame.Shape.VLine)
        divider.setStyleSheet(f"color:{C['bdr']};")
        divider.setFixedWidth(1)
        body.addWidget(divider)

        # ── RIGHT PANEL ──
        right = QVBoxLayout()
        right.setSpacing(10)
        right.setContentsMargins(14, 0, 0, 0)

        # HR Rating section
        hr = _compute_hr_rating(self.d)
        hr_frame = self._build_section("HR RATING", right)
        hr_row = QHBoxLayout()
        hr_row.setContentsMargins(0, 0, 0, 0)
        hr_row.setSpacing(12)

        self._hr_total = _mk(
            f"{hr['total_pct']:+.1f}%", color=hr["color"], size=24, bold=True)
        hr_row.addWidget(self._hr_total)

        splits_vl = QVBoxLayout()
        splits_vl.setSpacing(2)
        splits_vl.setContentsMargins(0, 4, 0, 0)
        lhb_col = self._hits_color(hr['lhb_pct'])
        rhb_col = self._hits_color(hr['rhb_pct'])
        self._hr_lhb = QLabel()
        self._hr_lhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hr_lhb.setText(f"<span style='color:{C["t2"]}; font-weight:700'>LHB:</span> <span style='color:{lhb_col}; font-weight:700'>{hr['lhb_pct']:+.1f}%</span>")
        self._hr_rhb = QLabel()
        self._hr_rhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hr_rhb.setText(f"<span style='color:{C["t2"]}; font-weight:700'>RHB:</span> <span style='color:{rhb_col}; font-weight:700'>{hr['rhb_pct']:+.1f}%</span>")
        splits_vl.addWidget(self._hr_lhb)
        splits_vl.addWidget(self._hr_rhb)
        hr_row.addLayout(splits_vl)
        hr_row.addStretch()
        hr_frame.addLayout(hr_row)

        # Hits Predict section (2B/3B left | 1B right, HR-style layout)
        hits = _compute_hits_rating(self.d)
        hits_frame = self._build_section("HITS PREDICT", right)

        hits_body = QHBoxLayout()
        hits_body.setContentsMargins(0, 0, 0, 0)
        hits_body.setSpacing(0)

        # — Left half: 2B / 3B (HR-style: big % + splits) —
        xbh_outer = QVBoxLayout()
        xbh_outer.setSpacing(2)
        xbh_outer.setContentsMargins(0, 0, 8, 0)
        xbh_hdr = _mk("2B / 3B", color=C["t2"], size=9, bold=True)
        xbh_outer.addWidget(xbh_hdr)

        xbh_row = QHBoxLayout()
        xbh_row.setContentsMargins(0, 0, 0, 0)
        xbh_row.setSpacing(12)
        xbh_col = self._hits_color(hits["xbh_pct"])
        self._hits_xbh_total = _mk(
            f"{hits['xbh_pct']:+.1f}%", color=xbh_col, size=24, bold=True)
        xbh_row.addWidget(self._hits_xbh_total)

        xbh_splits = QVBoxLayout()
        xbh_splits.setSpacing(2)
        xbh_splits.setContentsMargins(0, 4, 0, 0)
        xbh_lhb_col = self._hits_color(hits["xbh_lhb"])
        xbh_rhb_col = self._hits_color(hits["xbh_rhb"])
        self._hits_xbh_lhb = QLabel()
        self._hits_xbh_lhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hits_xbh_lhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>LHB:</span> "
            f"<span style='color:{xbh_lhb_col}; font-weight:700'>{hits['xbh_lhb']:+.1f}%</span>")
        self._hits_xbh_rhb = QLabel()
        self._hits_xbh_rhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hits_xbh_rhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>RHB:</span> "
            f"<span style='color:{xbh_rhb_col}; font-weight:700'>{hits['xbh_rhb']:+.1f}%</span>")
        xbh_splits.addWidget(self._hits_xbh_lhb)
        xbh_splits.addWidget(self._hits_xbh_rhb)
        xbh_row.addLayout(xbh_splits)
        xbh_row.addStretch()
        xbh_outer.addLayout(xbh_row)
        hits_body.addLayout(xbh_outer)

        # Thin divider between halves
        h_div = QFrame()
        h_div.setFrameShape(QFrame.Shape.VLine)
        h_div.setStyleSheet(f"color:{C['bdr']};")
        h_div.setFixedWidth(1)
        hits_body.addWidget(h_div)

        # — Right half: 1B (HR-style: big % + splits) —
        s_outer = QVBoxLayout()
        s_outer.setSpacing(2)
        s_outer.setContentsMargins(8, 0, 0, 0)
        s_hdr = _mk("1B", color=C["t2"], size=9, bold=True)
        s_outer.addWidget(s_hdr)

        s_row = QHBoxLayout()
        s_row.setContentsMargins(0, 0, 0, 0)
        s_row.setSpacing(12)
        s_col = self._hits_color(hits["singles_pct"])
        self._hits_s_total = _mk(
            f"{hits['singles_pct']:+.1f}%", color=s_col, size=24, bold=True)
        s_row.addWidget(self._hits_s_total)

        s_splits = QVBoxLayout()
        s_splits.setSpacing(2)
        s_splits.setContentsMargins(0, 4, 0, 0)
        s_lhb_col = self._hits_color(hits["singles_lhb"])
        s_rhb_col = self._hits_color(hits["singles_rhb"])
        self._hits_s_lhb = QLabel()
        self._hits_s_lhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hits_s_lhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>LHB:</span> "
            f"<span style='color:{s_lhb_col}; font-weight:700'>{hits['singles_lhb']:+.1f}%</span>")
        self._hits_s_rhb = QLabel()
        self._hits_s_rhb.setStyleSheet("background:transparent; border:none; font-family:'Segoe UI'; font-size:11px;")
        self._hits_s_rhb.setText(
            f"<span style='color:{C['t2']}; font-weight:700'>RHB:</span> "
            f"<span style='color:{s_rhb_col}; font-weight:700'>{hits['singles_rhb']:+.1f}%</span>")
        s_splits.addWidget(self._hits_s_lhb)
        s_splits.addWidget(self._hits_s_rhb)
        s_row.addLayout(s_splits)
        s_row.addStretch()
        s_outer.addLayout(s_row)
        hits_body.addLayout(s_outer)

        hits_frame.addLayout(hits_body)

        # Wind Insight section
        wi = _compute_wind_insight(self.d)
        wi_frame = self._build_section("WIND INSIGHT", right)
        wi_inner = QVBoxLayout()
        wi_inner.setContentsMargins(0, 0, 0, 0)
        wi_inner.setSpacing(4)

        self._wi_desc = _mk(wi["description"], color=C["t2"], size=10)
        self._wi_desc.setWordWrap(True)
        # Fixed 2-line height so the card doesn't resize when toggling hours
        _fm = self._wi_desc.fontMetrics()
        self._wi_desc.setFixedHeight(_fm.lineSpacing() * 2 + 4)
        wi_inner.addWidget(self._wi_desc)
        wi_frame.addLayout(wi_inner)

        # Conditions table
        cond_frame = self._build_section("CONDITIONS", right)
        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(4)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)

        temp = self.d.get("temp", "--")
        ws = self.d.get("wind_speed", 0)
        wd = self.d.get("wind_dir", "Calm")
        precip = self.d.get("precip_pct")
        humid = self.d.get("humidity_pct")
        pressure = self.d.get("pressure_hpa")

        self._cond_vals["temp"] = _cond_row(grid, 0, "Temperature",
                                            f"{temp}°F")
        self._cond_vals["wind"] = _cond_row(grid, 1, "Wind Speed",
                                            f"{ws} mph {wd}" if ws else "Calm")
        self._cond_vals["precip"] = _cond_row(grid, 2, "Precipitation",
                                              f"{precip:.0f}%" if precip is not None else "--%")
        self._cond_vals["humid"] = _cond_row(grid, 3, "Humidity",
                                             f"{humid:.0f}%" if humid is not None else "--%")
        self._cond_vals["pressure"] = _cond_row(grid, 4, "Air Pressure",
                                                f"{pressure:.1f} hPa" if pressure else "-- hPa")

        elev = self.d.get("elevation")
        self._cond_vals["altitude"] = _cond_row(grid, 5, "Altitude",
                                                f"{elev:,.0f} ft" if elev is not None else "-- ft")

        cond_frame.addLayout(grid)

        right.addStretch()
        body.addLayout(right, stretch=1)
        root.addLayout(body)

    def _build_section(self, title: str, parent_layout: QVBoxLayout) -> QVBoxLayout:
        """Create a bordered titled section and add it to *parent_layout*. Returns inner VBox."""
        frame = QFrame()
        frame.setStyleSheet(
            f"QFrame {{ background:{C['bg2']}; "
            f"border:1px solid {C['bdr']}; border-radius:6px; }}")
        fl = QVBoxLayout(frame)
        fl.setContentsMargins(10, 8, 10, 10)
        fl.setSpacing(4)
        lbl = _mk(title, color=C["t2"], size=9, bold=True)
        lbl.setStyleSheet(
            lbl.styleSheet() + f" letter-spacing:1px;")
        fl.addWidget(lbl)
        inner = QVBoxLayout()
        inner.setContentsMargins(0, 2, 0, 0)
        fl.addLayout(inner)
        parent_layout.addWidget(frame)
        return inner

    # ── Hourly switching ──
    def _on_hour_change(self, idx: int):
        if not self._hourly or idx < 0 or idx >= len(self._hourly):
            return
        slot = self._hourly[idx]

        # Update data dict with new hourly values
        self.d["wind_dir"] = slot.get("wind_dir", "")
        self.d["wind_speed"] = slot.get("wind_speed", 0)
        self.d["wind_angle"] = WIND_ANGLES.get(self.d["wind_dir"])
        self.d["temp"] = slot.get("temp", self.d.get("temp"))
        self.d["condition"] = slot.get("condition", self.d.get("condition"))
        self.d["night"] = slot.get("night", False)
        if slot.get("precip") is not None:
            self.d["precip_pct"] = slot["precip"]
        if slot.get("humidity") is not None:
            self.d["humidity_pct"] = slot["humidity"]

        # Update stadium wind arrows
        self._stadium.set_wind(self.d["wind_dir"], self.d["wind_speed"])

        # Sync weather overlay
        if self._wx_overlay:
            self._wx_overlay.set_index(idx)

        # Short-circuit: dome/closed-roof venues don't need weather recomputation
        roof_status, roof_confirmed = _predict_roof_status(self.d)
        if roof_status in ("dome", "closed"):
            # Update conditions display only
            temp = self.d.get("temp", "--")
            ws = self.d.get("wind_speed", 0)
            wd = self.d.get("wind_dir", "Calm")
            precip = self.d.get("precip_pct")
            humid = self.d.get("humidity_pct")
            self._cond_vals["temp"].setText(f"{temp}°F")
            self._cond_vals["wind"].setText(
                f"{ws} mph {wd}" if ws else "Calm")
            self._cond_vals["precip"].setText(
                f"{precip:.0f}%" if precip is not None else "--%")
            self._cond_vals["humid"].setText(
                f"{humid:.0f}%" if humid is not None else "--%")
            return

        # Recompute ratings (pass pre-computed roof status to avoid triple calls)
        hr = _compute_hr_rating(self.d, _roof_override=(roof_status, roof_confirmed))
        self._hr_total.setText(f"{hr['total_pct']:+.1f}%")
        self._hr_total.setStyleSheet(
            f"color:{hr['color']}; background:transparent; "
            f"font-family:'Segoe UI'; font-size:24px; font-weight:700;")
        lhb_col = self._hits_color(hr['lhb_pct'])
        rhb_col = self._hits_color(hr['rhb_pct'])
        self._hr_lhb.setText(f"<span style='color:{C['t2']}; font-weight:700'>LHB:</span> <span style='color:{lhb_col}; font-weight:700'>{hr['lhb_pct']:+.1f}%</span>")
        self._hr_rhb.setText(f"<span style='color:{C['t2']}; font-weight:700'>RHB:</span> <span style='color:{rhb_col}; font-weight:700'>{hr['rhb_pct']:+.1f}%</span>")

        # Recompute hits rating
        hits = _compute_hits_rating(self.d, _roof_override=(roof_status, roof_confirmed))
        self._update_hits_labels(hits)

        # Recompute wind insight
        wi = _compute_wind_insight(self.d, _roof_override=(roof_status, roof_confirmed))
        self._wi_desc.setText(wi["description"])

        # Update conditions
        temp = self.d.get("temp", "--")
        ws = self.d.get("wind_speed", 0)
        wd = self.d.get("wind_dir", "Calm")
        precip = self.d.get("precip_pct")
        humid = self.d.get("humidity_pct")
        self._cond_vals["temp"].setText(f"{temp}°F")
        self._cond_vals["wind"].setText(
            f"{ws} mph {wd}" if ws else "Calm")
        self._cond_vals["precip"].setText(
            f"{precip:.0f}%" if precip is not None else "--%")
        self._cond_vals["humid"].setText(
            f"{humid:.0f}%" if humid is not None else "--%")


# ═══════════════════════════════════════════════════════════════════════════════
# Test harness
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    import json

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # Load a sample game from weather cache
    cache_dir = os.path.join(os.path.dirname(__file__), "assets", "weather_cache")
    data = None
    for fname in sorted(os.listdir(cache_dir), reverse=True):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(cache_dir, fname), "r", encoding="utf-8") as f:
            games = json.load(f)
        # Pick first outdoor game with wind
        for g in games:
            roof = g.get("roof_type", "Open")
            if roof == "Open" and g.get("wind_speed", 0) > 0:
                data = g
                break
        if data:
            break

    if not data:
        print("No outdoor game with wind found in cache.")
        sys.exit(1)

    print(f"Showing: {data['away']} @ {data['home']} — {data['venue_name']}")

    # Dark background window
    win = QWidget()
    win.setWindowTitle("Weather Detail Widget — Test")
    win.setStyleSheet(f"background:{C['bg0']};")
    win.setMinimumSize(660, 500)

    layout = QVBoxLayout(win)
    layout.setContentsMargins(20, 20, 20, 20)

    widget = WeatherDetailWidget(data)
    # widget is a card — no close signal
    layout.addWidget(widget, alignment=Qt.AlignmentFlag.AlignCenter)

    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
