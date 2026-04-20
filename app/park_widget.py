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
# Calibrated against BallparkPal stadium-only reference data (Apr 19 2026)
VENUE_PARK_FACTORS = {
    # BPP Combined Effect calibrated — all 30 parks (Apr 20 2026, R2)
    1: 108, 2: 111, 3: 93, 4: 110, 5: 89, 7: 94, 12: 96, 14: 114,
    15: 92, 17: 95, 19: 126, 22: 116, 31: 84, 32: 108, 680: 101,
    2392: 106, 2394: 96, 2395: 84, 2529: 121, 2602: 123, 2680: 90,
    2681: 114, 2889: 86, 3289: 95, 3309: 92, 3312: 93, 3313: 106,
    4169: 87, 4705: 105, 5325: 85,
}

# Per-venue hit-type park factors (venue_id → {1B, 2B, 3B})
# Expressed as 1.00 = league average.  HR factor already in VENUE_PARK_FACTORS.
# Sources: BPP Stadium Only — XBH% applied to both 2B and 3B (code recombines 92/8).
VENUE_HIT_FACTORS: dict[int, dict[str, float]] = {
    #                              1B    2B    3B          Venue
    1:    {"1B": 1.04, "2B": 0.94, "3B": 0.94},  # Angel Stadium        XBH -6%, 1B +4%
    2:    {"1B": 0.98, "2B": 0.96, "3B": 0.96},  # Camden Yards         XBH -4%, 1B -2%
    3:    {"1B": 1.13, "2B": 1.29, "3B": 1.29},  # Fenway Park          XBH +29%, 1B +13%
    4:    {"1B": 0.96, "2B": 0.89, "3B": 0.89},  # Rate Field           XBH -11%, 1B -4%
    5:    {"1B": 1.00, "2B": 0.99, "3B": 0.99},  # Progressive Field    XBH -1%, 1B +0%
    7:    {"1B": 0.98, "2B": 1.01, "3B": 1.01},  # Kauffman Stadium     XBH +1%, 1B -2%
    12:   {"1B": 0.92, "2B": 0.93, "3B": 0.93},  # Tropicana Field      XBH -7%, 1B -8%
    14:   {"1B": 0.98, "2B": 0.99, "3B": 0.99},  # Rogers Centre        XBH -1%, 1B -2%
    15:   {"1B": 1.01, "2B": 1.10, "3B": 1.10},  # Chase Field          XBH +10%, 1B +1%
    17:   {"1B": 1.04, "2B": 0.93, "3B": 0.93},  # Wrigley Field        XBH -7%, 1B +4%
    19:   {"1B": 1.17, "2B": 1.16, "3B": 1.16},  # Coors Field          XBH +16%, 1B +17%
    22:   {"1B": 0.96, "2B": 0.90, "3B": 0.90},  # Dodger Stadium       XBH -10%, 1B -4%
    31:   {"1B": 1.04, "2B": 1.00, "3B": 1.00},  # PNC Park             XBH +0%, 1B +4%
    32:   {"1B": 0.97, "2B": 0.93, "3B": 0.93},  # American Family Fld  XBH -7%, 1B -3%
    680:  {"1B": 0.91, "2B": 0.82, "3B": 0.82},  # T-Mobile Park        XBH -18%, 1B -9%
    2392: {"1B": 0.96, "2B": 0.88, "3B": 0.88},  # Daikin Park          XBH -12%, 1B -4%
    2394: {"1B": 1.03, "2B": 1.12, "3B": 1.12},  # Comerica Park        XBH +12%, 1B +3%
    2395: {"1B": 1.02, "2B": 1.07, "3B": 1.07},  # Oracle Park          XBH +7%, 1B +2%
    2529: {"1B": 1.03, "2B": 1.08, "3B": 1.08},  # Sutter Health Park   XBH +8%, 1B +3%
    2602: {"1B": 0.99, "2B": 0.98, "3B": 0.98},  # Great American BP    XBH -2%, 1B -1%
    2680: {"1B": 0.97, "2B": 0.92, "3B": 0.92},  # Petco Park           XBH -8%, 1B -3%
    2681: {"1B": 1.04, "2B": 1.02, "3B": 1.02},  # Citizens Bank Park   XBH +2%, 1B +4%
    2889: {"1B": 0.99, "2B": 0.98, "3B": 0.98},  # Busch Stadium        XBH -2%, 1B -1%
    3289: {"1B": 0.97, "2B": 0.96, "3B": 0.96},  # Citi Field           XBH -4%, 1B -3%
    3309: {"1B": 1.01, "2B": 1.05, "3B": 1.05},  # Nationals Park       XBH +5%, 1B +1%
    3312: {"1B": 1.02, "2B": 1.11, "3B": 1.11},  # Target Field         XBH +11%, 1B +2%
    3313: {"1B": 1.03, "2B": 1.01, "3B": 1.01},  # Yankee Stadium       XBH +1%, 1B +3%
    4169: {"1B": 1.07, "2B": 0.97, "3B": 0.97},  # loanDepot park       XBH -3%, 1B +7%
    4705: {"1B": 0.98, "2B": 0.96, "3B": 0.96},  # Truist Park          XBH -4%, 1B -2%
    5325: {"1B": 0.99, "2B": 1.02, "3B": 1.02},  # Globe Life Field     XBH +2%, 1B -1%
}

# Per-venue weather sensitivity profile — asymmetric (pos, neg) multipliers.
# Each key maps to a tuple: (pos_mult, neg_mult)
#   pos_mult: applied when base effect > 0  (weather BOOSTS offense / carry)
#   neg_mult: applied when base effect < 0  (weather SUPPRESSES offense / carry)
# 1.0 = league-average sensitivity.  >1 amplifies, <1 dampens that direction.
# Example: Coors w=(0.80, 0.02) → tailwind amplified 0.80× (thin air helps),
#          headwind dampened to 0.02× (altitude carry compensates).
# Base constants already include _WX_SCALE (0.45).
# Sources: MLB Statcast wind study, BallparkPal, FanGraphs, CLEATZ, Oddstrader.
# Primary/secondary factors per BPP + physics analysis (Apr 2026).
VENUE_WX_PROFILE: dict[int, dict[str, tuple[float, float]]] = {
    # ── Wind-dominated parks ──
    17:   {"w": (1.00, 1.30), "t": (0.80, 1.05), "h": (0.40, 1.00), "p": (0.60, 0.60)},  # Wrigley — wind primary; cold Lake Michigan air secondary; overcast sky suppressive
    2529: {"w": (1.00, 1.00), "t": (0.50, 0.70), "h": (0.30, 0.50), "p": (0.15, 0.15)},  # Sutter — wind-receptive (no upper decks); SW push to RF; temp secondary
    7:    {"w": (1.78, 1.78), "t": (0.90, 0.98), "h": (0.70, 1.30), "p": (1.00, 1.00)},  # Kauffman — wind stops HRs more than almost any park; plains humidity/rain secondary
    680:  {"w": (1.20, 0.20), "t": (0.20, 0.02), "h": (0.05, 0.05), "p": (0.80, 0.80)},  # T-Mobile — wind keeps balls in park; heavy coastal air density secondary; carport roof

    # ── Altitude-driven parks ──
    19:   {"w": (0.30, 0.02), "t": (0.15, 0.05), "h": (1.80, 0.10), "p": (2.50, 0.02)},  # Coors — altitude dominant (5190ft); heat further thins already light air; dry ball carry
    15:   {"w": (0.60, 0.60), "t": (0.12, 0.20), "h": (0.20, 0.20), "p": (1.50, 1.50)},  # Chase — 2nd highest altitude; roof open/closed dictates temp exposure; pressure amplified

    # ── Temperature-volatile (northern / coastal cold) ──
    3:    {"w": (1.58, 1.58), "t": (1.20, 1.30), "h": (0.80, 1.40), "p": (1.00, 1.00)},  # Fenway — cold starts suppress carry; wind secondary (structures vary impact); waterfront humidity
    3313: {"w": (0.60, 0.60), "t": (1.50, 2.04), "h": (0.60, 1.10), "p": (1.00, 1.00)},  # Yankee — early cold hurts; wind critical to short porch in RF; East River moisture
    2681: {"w": (1.10, 1.10), "t": (1.20, 2.10), "h": (0.60, 1.20), "p": (1.00, 1.00)},  # Citizens Bank — heat drives high-run environment; wind knocks down would-be HRs
    3312: {"w": (0.56, 0.56), "t": (1.05, 1.22), "h": (0.50, 0.80), "p": (1.10, 1.10)},  # Target — coldest early season; 5th highest altitude (subtle p boost); wind secondary
    5:    {"w": (1.05, 1.05), "t": (1.00, 0.80), "h": (0.60, 1.10), "p": (0.20, 0.20)},  # Progressive — Great Lakes cold; temp primary; wind secondary; altitude pressure dampened
    2394: {"w": (0.90, 0.90), "t": (1.10, 1.30), "h": (0.60, 1.10), "p": (1.00, 1.00)},  # Comerica — large OF + early cold = difficult; temp primary; wind secondary
    4:    {"w": (0.80, 0.80), "t": (1.30, 1.50), "h": (0.80, 1.30), "p": (1.00, 1.00)},  # Rate — cold starts favor pitchers; humid Midwest summers massive uptick; temp primary
    3289: {"w": (1.00, 1.00), "t": (1.10, 1.20), "h": (0.60, 1.20), "p": (1.00, 1.00)},  # Citi — cold coastal air suppresses early-season HRs; temp primary; wind secondary
    31:   {"w": (0.69, 0.69), "t": (0.75, 0.90), "h": (0.60, 1.10), "p": (1.00, 1.00)},  # PNC — offensive drop when temp drops + wind shifts Sept; river confluence moisture
    3309: {"w": (0.80, 0.80), "t": (1.20, 0.95), "h": (0.70, 1.25), "p": (1.00, 1.00)},  # Nationals — moderate temp swing; Anacostia humidity secondary

    # ── Temperature + humidity driven (hot / humid) ──
    2602: {"w": (0.80, 0.80), "t": (1.30, 1.50), "h": (1.00, 1.60), "p": (1.00, 1.00)},  # Great American — temp primary; one of most humid parks; heat+humidity = extreme carry
    2889: {"w": (0.70, 0.70), "t": (1.20, 1.40), "h": (0.80, 1.30), "p": (1.00, 1.00)},  # Busch — consistently high summer heat; Mississippi valley humidity secondary
    1:    {"w": (0.10, 0.10), "t": (0.70, 1.40), "h": (0.70, 1.00), "p": (1.00, 1.00)},  # Angel — marine layer + evening cool-down suppressive; humidity secondary; mild winds
    22:   {"w": (0.30, 0.30), "t": (1.00, 1.20), "h": (0.40, 0.60), "p": (1.20, 1.20)},  # Dodger — high heat + coastal air density volatile; dry climate = low humidity variance

    # ── Humidity / air-density driven (marine / coastal) ──
    2:    {"w": (0.60, 0.60), "t": (0.90, 1.08), "h": (1.00, 1.50), "p": (1.00, 1.00)},  # Camden — summer humidity primary boosts runs; Inner Harbor; temp secondary
    4705: {"w": (0.50, 0.50), "t": (1.00, 1.19), "h": (0.80, 1.60), "p": (1.25, 1.25)},  # Truist — hot muggy humidity primary; 3rd highest altitude boosts p secondary
    2680: {"w": (0.50, 0.50), "t": (1.00, 1.10), "h": (0.60, 1.40), "p": (1.40, 1.40)},  # Petco — heavy marine layer/air density primary; expansive layout; temp secondary
    2395: {"w": (0.30, 0.50), "t": (1.20, 1.47), "h": (0.40, 1.50), "p": (1.00, 1.00)},  # Oracle — coldest avg temp in MLB; wind neutralized by barrier (swirl knocks down LDs)

    # ── Roof-controlled / low sensitivity ──
    5325: {"w": (0.40, 0.40), "t": (0.90, 1.10), "h": (0.05, 0.05), "p": (1.00, 1.00)},  # Globe Life — roof dictates performance; Texas heat secondary when open
    2392: {"w": (0.02, 0.02), "t": (0.20, 0.05), "h": (0.05, 0.05), "p": (0.05, 0.05)},  # Daikin — controlled environment; heat impacts when roof open
    4169: {"w": (0.50, 0.69), "t": (0.78, 0.90), "h": (0.30, 2.00), "p": (1.00, 1.00)},  # loanDepot — humid coastal; humidor + retractable roof manage conditions
    14:   {"w": (0.30, 0.30), "t": (0.80, 0.98), "h": (0.05, 0.05), "p": (1.00, 1.00)},  # Rogers — indoor neutralizes; temp matters when roof opens in summer
    32:   {"w": (0.69, 0.69), "t": (0.80, 0.98), "h": (0.05, 0.05), "p": (1.00, 1.00)},  # American Family — retractable; Midwest conditions when open
    # Tropicana (venue 12) — fixed dome, handled by _FIXED_DOMES
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
_REF_TEMP_F = 72.5           # neutral baseline: midpoint of 70–75 °F carry range
_REF_HUMID_CARRY = 50.0      # air density neutral: 50 % RH.  Below = denser air
                             # (less carry), above = lighter air (more carry).
_REF_HUMID_ABSORB = 57.0     # humidor standard: ALL 30 parks store balls at
                             # 70 °F / 57 % RH.  Ball moisture neutral point.
_REF_PRESSURE_HPA = 1013.25  # sea-level standard pressure

# Raw physics-to-pct conversion constants.
# These are multiplied by _WX_SCALE to produce MLB-average base values.
# Venue profiles in VENUE_WX_PROFILE then enhance (>1) or dampen (<1).
_WX_SCALE = 0.45             # global scaler: raw physics → MLB-average output
                             # Absorbs park geometry, ball-bat physics, etc.
_TEMP_PER_F = 1.47 * _WX_SCALE   # +0.66 pct-pts/°F at league-avg venue
                             # Research: ~8 % aero + bat/ball/grip cold effects.
# Humidity has TWO independent effects:
_HUMID_CARRY_PER_PCT = 0.02 * _WX_SCALE  # +0.009 pct-pts per 1 % above 50 %
                             # Air density only: humid air is lighter (H₂O
                             # displaces heavier N₂/O₂) → more fly-ball carry.
                             # ~1 ft per 50 % RH swing.  Universal physics —
                             # same at every park, NOT venue-adjusted.
_HUMID_ABSORB_PER_PCT = -0.08 * _WX_SCALE  # -0.036 pct-pts per 1 % above 57 %
                             # Ball absorption: ambient humidity above humidor →
                             # ball gains moisture → reduced COR / exit velo.
                             # Below humidor → ball dries → MORE pop.
                             # This is the dominant humidity effect and IS
                             # venue-adjusted (domes ≈ 0, open-air amplified).
_WIND_TAIL_PER_MPH = 1.80 * _WX_SCALE  # +0.81 pct-pts/mph tailwind
                             # Research: ~3 ft carry per mph tailwind.
_WIND_HEAD_PER_MPH = 2.50 * _WX_SCALE  # -1.125 pct-pts/mph headwind
                             # Asymmetry: headwind directly opposes flight.
_WIND_PER_HAND_CAP = 14.0    # max ±14 % wind contribution per handedness
_PRESS_PER_HPA = 0.45 * _WX_SCALE  # +0.20 pct-pts/hPa below expected
                             # Research: 2 ft per 0.3 inHg ≈ 10.16 hPa.
_WX_CAP = 40.0               # max ±40 % weather modifier
_RHB_PULL_DEG = 315.0        # RHB pull-side direction (toward LF)
_LHB_PULL_DEG = 45.0         # LHB pull-side direction (toward RF)

# ── Sky condition (cloud cover) adjustments ──
# Overcast/cloudy conditions reduce air density slightly (warmer humid air aloft),
# reduce K-rates, and turn some flyouts into hits.  Clear day sky aids pitchers
# (visibility, higher K-rates).
# Values are additive pct-pt modifiers applied to HR and Hits weather layers.
# Rain: wet ball suppresses HR distance, but +9.6% walks / -10.1% K / wet turf
#   errors produce +3.6% total run scoring.  HR is slightly negative (fly ball
#   knock-down), but singles/XBH benefit from reduced control + wet fielding.
_SKY_HR_MODIFIER = {
    "Overcast":       +1.8,   # thick cloud cover → +1.8% HR boost
    "Cloudy":         +1.2,   # moderate cloud → +1.2%
    "Partly Cloudy":  +0.4,   # partial cloud → small boost
    "Drizzle":        -0.5,   # wet ball (heavier, less elastic) partly offsets humid air
    "Rain":           -2.0,   # wet ball dominates — heavier, dead on contact; knocks down fly balls
    "Clear":          -0.8,   # clear sky → slight pitcher advantage
}
_SKY_HITS_XBH_MOD = {
    "Overcast":       +1.5,
    "Cloudy":         +1.0,
    "Partly Cloudy":  +0.3,
    "Drizzle":        +0.5,   # wet turf → unpredictable bounces benefit gap hits
    "Rain":           +1.5,   # slick grass + errors → balls get through for XBH
    "Clear":          -0.6,
}
_SKY_HITS_1B_MOD = {
    "Overcast":       +0.8,   # reduced K-rate → more balls in play → more singles
    "Cloudy":         +0.5,
    "Partly Cloudy":  +0.2,
    "Drizzle":        +1.0,   # reduced K-rate + wet grip → more walks/singles
    "Rain":           +2.0,   # +9.6% walks, -10.1% K, slick turf = lots of singles
    "Clear":          -0.4,
}

# ── Day vs Night game adjustments ──
# Night games: cooler/denser air generally favours pitchers, BUT some parks
# flip (e.g. Truist plays as hitter-friendly at night).  Day games: hotter
# air + humidity cycles + glare = complex.  Clear-sky day games see the
# highest K-rates.
# Format: venue_id → (day_modifier, night_modifier) in pct-pts for HR.
# Positive = offence boost, negative = pitcher boost.
# Based on BPP/Statcast day-vs-night splits for each park's unique physics:
#   - "Extreme" night suppression: marine layer parks (Oracle, Petco)
#   - "High" day/night flip: shadow parks (Truist, Fenway), wind parks (Wrigley)
#   - "Moderate": most open-air parks (temp/density shift)
#   - "Roof-Dependent"/"Neutral": domes & retractable (near-zero)
_VENUE_DAY_NIGHT: dict[int, tuple[float, float]] = {
    # park:           (day,   night)    # category from BPP analysis
    # ── Extreme night suppression (marine layer) ──
    2395:             (+2.0,  -2.5),    # Oracle — HR rates ~70% lower at night (marine layer)
    2680:             (+1.5,  -2.0),    # Petco — heavy night air; one of toughest HR parks after dark
    # ── High day/night volatility ──
    17:               (+1.5,  -1.0),    # Wrigley — solar glare severe; night = consistent lighting
    3:                (-1.5,  +1.0),    # Fenway — night neutralizes Green Monster shadows; cold AM
    4705:             (-1.8,  +1.8),    # Truist — SSE shadows favor pitchers PM; offense jumps at night
    19:               (+1.5,  -1.5),    # Coors — daytime dry heat = extreme carry; night rapid cooling
    2529:             (+1.5,  -0.5),    # Sutter — day temps boost already strong baseline; moderate night
    680:              (+1.0,  -1.2),    # T-Mobile — night air significantly denser than daytime coastal
    # ── Moderate day/night split ──
    1:                (+1.0,  -0.5),    # Angel — 79°F avg day temps reliable power boost
    22:               (+1.0,  -0.5),    # Dodger — warm afternoon valley air boosts HRs
    3313:             (-0.8,  +0.3),    # Yankee — late afternoon shadows tricky for RHB
    2681:             (+0.5,  -0.3),    # Citizens Bank — warm night humidity keeps park "live"
    3312:             (+0.3,  -1.0),    # Target — drastic night cooling in early/late season
    2602:             (+0.3,  +0.8),    # Great American — muggy night humidity amplifies +23% HR base
    2:                (-0.3,  +0.5),    # Camden — high night humidity thins air, aids carry
    2889:             (+0.3,  +0.5),    # Busch — high night humidity aids carry (Midwest)
    7:                (-0.3,  +0.5),    # Kauffman — night humidity helps HR carry (structurally suppressed)
    5:                (+0.3,  -0.5),    # Progressive — night Lake Erie air denser/cooler
    2394:             (+0.3,  -0.8),    # Comerica — early season night games coldest/densest in MLB
    31:               (+0.5,  -0.5),    # PNC — night river air denser; daytime heat best for offense
    3309:             (+0.5,  -0.3),    # Nationals — one of hottest avg temps; day games power-favorable
    3289:             (+0.3,  -0.5),    # Citi — coastal night air denser, favors pitchers
    4:                (+0.3,  -0.5),    # Rate — similar Midwest cooling pattern
    # ── Roof-dependent (near-zero when closed) ──
    15:               (+0.5,  -0.3),    # Chase — roof open day = extreme dry carry; closed = neutral
    5325:             (+0.2,  -0.1),    # Globe Life — roof negates most; Texas heat secondary when open
    32:               (+0.3,  -0.2),    # American Family — daytime heat when roof open
    14:               (+0.1,  -0.1),    # Rogers — usually closed; night roof open = more carry-friendly
    2392:             (+0.1,  -0.1),    # Daikin — usually closed; night humidity helps when open
    4169:             ( 0.0,   0.0),    # loanDepot — humidor + AC = static environment
    # ── Fully domed (zero) ──
    12:               ( 0.0,   0.0),    # Tropicana — fixed dome; zero volatility
}
# Default: day games slightly pitcher-friendly (glare + high K-rate; 6.40 K/gm
# clear day vs 5.95 cloudy), night games neutral (dense air offset by humidity).
_DEFAULT_DAY_MOD  = -0.3     # day default: slight pitcher edge
_DEFAULT_NIGHT_MOD = +0.0    # night default: neutral

# ── Per-venue sky (overcast) sensitivity multiplier ──
# Applied to base sky modifiers in _venue_adjust_weather.
# Overcast = fewer errors (0.73 vs 0.80 visitor), BA rises ~.259→.266.
# Major = high benefit to hitters; Low = negligible; 0.0 = dome/N/A.
_VENUE_SKY_MULT: dict[int, float] = {
    # ── Major: overcast dramatically helps hitters ──
    17:   1.80,   # Wrigley — eliminates severe high-glare shadows
    4705: 1.60,   # Truist — removes difficult SSE shadow lines, aids recognition
    # ── High: significant overcast benefit ──
    2395: 1.50,   # Oracle — fog/heavy clouds simulate "night" penalty during day
    2680: 1.40,   # Petco — overcast coastal conditions increase air density, favor pitchers
    # ── Moderate: standard overcast benefit ──
    3:    1.10,   # Fenway — cloudy helps track spin against low batter's eye
    2529: 1.00,   # Sutter — consistent cloud cover helps fielders on high flies
    680:  1.00,   # T-Mobile — frequent clouds; consistent hitting backdrop
    2:    1.00,   # Camden — overcast = cleaner batter's eye than sunny
    2602: 1.00,   # Great American — helps vision in bright Ohio Valley sun
    3312: 1.00,   # Target — helps track balls in bright Northern sky
    5:    1.00,   # Progressive — mitigates tricky wind-visual for hitters
    3289: 1.00,   # Citi — reduces outfield errors that spike on clear days
    # ── Low: minimal overcast impact ──
    19:   0.40,   # Coors — altitude dominates regardless of light conditions
    1:    0.40,   # Angel — typically clear; overcast rare but helps visibility
    22:   0.50,   # Dodger — shadows consistent; clouds mainly affect OF glare
    3313: 0.50,   # Yankee — clouds reduce sun-glare errors for visitors
    2681: 0.50,   # Citizens Bank — generally neutral for carry
    7:    0.50,   # Kauffman — visuals are primary overcast benefit
    2889: 0.50,   # Busch — neutral distance impact
    2394: 0.50,   # Comerica — neutral distance impact
    3309: 0.50,   # Nationals — standard overcast benefit for vision
    31:   0.50,   # PNC — tricky sun angles helped by clouds
    4:    0.50,   # Rate — neutral distance impact
    4169: 0.40,   # loanDepot — usually closed; minimal when open
    15:   0.40,   # Chase — overcast often = roof opened for natural cooling
    # ── Dome / retractable (near-zero) ──
    5325: 0.0,    # Globe Life — weather non-factor indoors
    2392: 0.0,    # Daikin — static environment when closed
    14:   0.0,    # Rogers — static environment when closed
    32:   0.0,    # American Family — static when closed
    12:   0.0,    # Tropicana — fully climate-controlled
}

# ── Per-venue rain vulnerability multiplier ──
# Applied to rain/drizzle sky modifiers in _venue_adjust_weather.
# Rain: +3.6% total runs (wet grip → +9.6% BB, -10.1% K, slick turf → errors).
# HR slightly suppressed (fly ball knock-down), but 1B/XBH boosted.
# Vulnerability varies by drainage quality, grass type, local rain frequency.
# Scale: Extreme=1.8, High=1.4, Moderate=1.0, Low=0.6, Minimal=0.3, None=0.0.
_VENUE_RAIN_MULT: dict[int, float] = {
    # ── Extreme: worst drainage / highest rain frequency ──
    3:    1.80,   # Fenway — league leader in rain postponements (31+/decade); old drainage
    5:    1.80,   # Progressive — one of most rain-hindered; frequently cited for needing a roof
    2394: 1.80,   # Comerica — high rainfall frequency; limited fan cover makes delays difficult
    3289: 1.80,   # Citi — very high rainout count (29+/decade); coastal humidity when wet helps HR
    3313: 1.80,   # Yankee — among rainiest venues; disruptions force early bullpen usage
    # ── High: frequent rain impact / poor drainage ──
    2:    1.40,   # Camden — historically high rain delay frequency (26+/decade)
    17:   1.40,   # Wrigley — open grass prone to ponding and slick infield
    7:    1.40,   # Kauffman — high regional rainfall; fewer delays than STL
    31:   1.40,   # PNC — fickle muggy riverfront weather → long delays
    2889: 1.40,   # Busch — frequent Midwestern storms disrupt summer schedules
    3309: 1.40,   # Nationals — frequent summer thunderstorms in DC
    # ── Moderate: standard rain risk ──
    4:    1.00,   # Rate — mid-range; standard Midwest patterns
    2602: 1.00,   # Great American — surprisingly resilient for its region
    3312: 1.00,   # Target — modern drainage allows quick recovery
    2529: 1.00,   # Sutter — minor-league grade drainage; more susceptible than top MLB parks
    2681: 1.00,   # Citizens Bank — modern drainage more efficient than NY/BAL
    2395: 1.00,   # Oracle — fog/marine layer behaves like light mist; makes ball heavy
    # ── Low: dry climate or good drainage ──
    19:   0.60,   # Coors — rain often accompanies high humidity that aids HR carry
    4705: 0.60,   # Truist — 2.5-acre underground drainage liner; rapid resume
    # ── Minimal: very rare rain ──
    1:    0.30,   # Angel — historically driest in MLB (1 rainout in a decade)
    22:   0.30,   # Dodger — rare rain; ancient drainage can cause issues when it pours
    2680: 0.30,   # Petco — Southern CA dry advantage; almost zero rainouts
    # ── None: roof eliminates rain ──
    15:   0.0,    # Chase — roof closed during rain
    2392: 0.0,    # Daikin — fully climate-controlled
    4169: 0.0,    # loanDepot — retractable roof eliminates Florida afternoon rain
    32:   0.0,    # American Family — roof closed during inclement weather
    680:  0.0,    # T-Mobile — retractable roof handles Pacific NW drizzle
    5325: 0.0,    # Globe Life — roof eliminates Texas downpours
    12:   0.0,    # Tropicana — permanent dome
    14:   0.0,    # Rogers — climate-controlled indoor
}

# ── Closed-roof humidity passthrough ──
# Most retractable / dome parks with AC control indoor humidity to ~50-55%
# (near humidor 57 %), so humidity effect ≈ 0 when closed.  These venues
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


# ═══════════════════════════════════════════════════════════════════════════════
# Wall-height-based LHB/RHB dimension splits
# ═══════════════════════════════════════════════════════════════════════════════

# Effective pull-zone wall heights (feet) per venue.
# lf_wall: height a RHB-pulled ball faces (LF line → LC area).
# rf_wall: height an LHB-pulled ball faces (RC → RF line area).
# Sourced from BallparkPal stadium diagrams (Jun 2025).
# Multi-section walls averaged to a single effective pull-zone height.
VENUE_WALL_HEIGHTS: dict[int, tuple[float, float]] = {
    #  venue_id: (lf_wall, rf_wall)
    1:    ( 5.0,   5.0),   # Angel Stadium — uniform 5' LF/RF, 8' CF
    2:    (13.0,  21.0),   # Camden Yards — LF 13', RF 21'
    3:    (37.0,   5.0),   # Fenway Park — Green Monster 37' LF, 5' RF
    4:    ( 8.0,   8.0),   # Rate Field — uniform 8'
    5:    (19.0,  14.0),   # Progressive Field — LF 19', RF 14'
    7:    ( 8.5,   8.5),   # Kauffman Stadium — uniform 8.5' (lowered from 10')
    12:   ( 8.0,  11.0),   # Tropicana Field — LF line 5'/LC 11' → eff 8', RF 11'
    14:   (14.0,  12.5),   # Rogers Centre — LF 14', RF 14'/11' → eff 12.5'
    15:   ( 8.0,   8.0),   # Chase Field — LF/RF 8', CF 25'
    17:   (16.0,  16.0),   # Wrigley Field — symmetric 16' ivy walls
    19:   (13.0,  17.0),   # Coors Field — LF 13', RF 17'
    22:   ( 4.0,   4.0),   # Dodger Stadium — uniform 4' LF/RF, 8' CF
    31:   ( 6.0,  21.0),   # PNC Park — LF 6', RF 21' Clemente wall
    32:   ( 8.0,   8.0),   # American Family Field — uniform 8'
    680:  ( 8.0,   8.0),   # T-Mobile Park — uniform 8'
    2392: (23.0,   7.0),   # Daikin Park — LF 21'/25' → eff 23', RF 7'
    2394: ( 7.0,  12.0),   # Comerica Park — LF 7', RF 9'/15' → eff 12'
    2395: ( 8.0,  22.0),   # Oracle Park — LF 8', RF arcade 20'/24' → eff 22'
    2529: ( 8.0,   5.0),   # Sutter Health Park — LF 8', RF 5'
    2602: (12.0,   8.0),   # Great American BP — LF 12', RF 8'
    2680: ( 7.0,   8.5),   # Petco Park — LF 7', RF 7'/10' → eff 8.5'
    2681: (11.0,  13.0),   # Citizens Bank Park — LF 11', RF 13'
    2889: ( 8.0,   8.0),   # Busch Stadium — uniform 8'
    3289: ( 8.0,   8.0),   # Citi Field — uniform 8'
    3309: (10.0,  12.5),   # Nationals Park — LF 10'/9' → 10', RF 16'/9' → 12.5'
    3312: ( 8.0,  23.0),   # Target Field — LF 8', RF 23'
    3313: ( 8.0,   8.0),   # Yankee Stadium — uniform 8'
    4169: (10.0,  10.0),   # LoanDepot Park — LF 12'/7' → 10', RF 12'/9'/7' → 10'
    4705: ( 6.0,  16.0),   # Truist Park — LF 6', RF 16'
    5325: ( 7.5,   9.0),   # Globe Life Field — LF 8'/7' → 7.5', RF 7'/10'/8' → 9'
}

_REF_WALL_FT = 8.0           # League-average baseline wall height
_HR_WALL_SCALE  = 0.12       # pct-pts per foot deviation for HR
_XBH_WALL_SCALE = 0.08       # pct-pts per foot deviation for XBH (doubles off wall)
_S_WALL_SCALE   = 0.02       # pct-pts per foot deviation for singles (carom)


def _dimension_splits(venue_id: int) -> dict:
    """Zero-sum LHB/RHB offsets derived from outfield wall asymmetry.

    Taller wall on pull side → fewer HR but more doubles for that hand.
    Returns dict with hr_dim_lhb, hr_dim_rhb, xbh_dim_lhb, xbh_dim_rhb,
    s_dim_lhb, s_dim_rhb — all zero-sum pairs.
    """
    lf_wall, rf_wall = VENUE_WALL_HEIGHTS.get(venue_id, (_REF_WALL_FT, _REF_WALL_FT))

    # Raw offsets: short wall on pull side → more HR for that hand
    # RHB pulls to LF, LHB pulls to RF
    raw_hr_rhb = (_REF_WALL_FT - lf_wall) * _HR_WALL_SCALE
    raw_hr_lhb = (_REF_WALL_FT - rf_wall) * _HR_WALL_SCALE
    hr_mean = (raw_hr_rhb + raw_hr_lhb) / 2.0

    # Tall wall → more doubles off it (inverse of HR)
    raw_xbh_rhb = (lf_wall - _REF_WALL_FT) * _XBH_WALL_SCALE
    raw_xbh_lhb = (rf_wall - _REF_WALL_FT) * _XBH_WALL_SCALE
    xbh_mean = (raw_xbh_rhb + raw_xbh_lhb) / 2.0

    # Caroms off tall wall → singles
    raw_s_rhb = (lf_wall - _REF_WALL_FT) * _S_WALL_SCALE
    raw_s_lhb = (rf_wall - _REF_WALL_FT) * _S_WALL_SCALE
    s_mean = (raw_s_rhb + raw_s_lhb) / 2.0

    return {
        "hr_dim_rhb":  raw_hr_rhb  - hr_mean,
        "hr_dim_lhb":  raw_hr_lhb  - hr_mean,
        "xbh_dim_rhb": raw_xbh_rhb - xbh_mean,
        "xbh_dim_lhb": raw_xbh_lhb - xbh_mean,
        "s_dim_rhb":   raw_s_rhb   - s_mean,
        "s_dim_lhb":   raw_s_lhb   - s_mean,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Three-stage weather pipeline
#   Stage 0: Neutral park baseline (no weather)
#   Stage 1: Base weather (universal constants, no venue multipliers)
#   Stage 2: Venue-adjusted weather (venue-specific multipliers applied)
# ═══════════════════════════════════════════════════════════════════════════════

def _neutral_park(venue_id: int) -> dict:
    """Stage 0: Static park baseline with all weather factors neutral.

    Returns what each stadium produces under perfectly neutral conditions
    (70°F, 50% humidity, no wind, standard pressure, clear skies).
    This is the stadium's inherent offensive environment.

    Returns dict with keys: hr_pct, xbh_pct, singles_pct, overall_pct,
    plus the raw factors for diagnostics.
    """
    # HR park factor
    pf_index = VENUE_PARK_FACTORS.get(venue_id, 100)
    hr_pct = float(pf_index - 100)

    # Hit-type park factors
    hf = VENUE_HIT_FACTORS.get(venue_id, {"1B": 1.00, "2B": 1.00, "3B": 1.00})
    singles_pct = (hf["1B"] - 1.0) * 100.0
    pf_2b_pct = (hf["2B"] - 1.0) * 100.0
    pf_3b_pct = (hf["3B"] - 1.0) * 100.0
    xbh_pct = pf_2b_pct * 0.92 + pf_3b_pct * 0.08

    # Overall (frequency-weighted composite)
    overall_pct = (singles_pct * _W_1B + pf_2b_pct * _W_2B
                   + pf_3b_pct * _W_3B + hr_pct * _W_HR)

    # Dimension-based LHB/RHB splits (wall asymmetry)
    dim = _dimension_splits(venue_id)

    return {
        "hr_pct": hr_pct,
        "xbh_pct": xbh_pct,
        "singles_pct": singles_pct,
        "overall_pct": overall_pct,
        "pf_index": pf_index,
        "pf_2b_pct": pf_2b_pct,
        "pf_3b_pct": pf_3b_pct,
        **dim,
    }


def _base_weather(data: dict) -> dict:
    """Stage 1: Compute raw weather pct-pt effects using universal constants.

    No venue multipliers are applied here — every park gets the same base
    values for a given set of weather conditions.  This is the consistent
    baseline that Stage 2 then adjusts per venue.

    Returns dict with keys:
        temp, humid_carry, humid_absorb, wind_rhb, wind_lhb, wind_avg,
        pressure, sky_hr, sky_xbh, sky_1b, day_night
    """
    # Temperature
    temp_f = float(data.get("temp", _REF_TEMP_F) or _REF_TEMP_F)
    temp = (temp_f - _REF_TEMP_F) * _TEMP_PER_F

    # Humidity — two independent effects
    humid = float(data.get("humidity_pct", _REF_HUMID_CARRY) or _REF_HUMID_CARRY)
    # 1) Air density / carry: ref=50%.  Higher humidity = lighter air = more carry.
    #    Universal physics — NOT venue-adjusted.
    humid_carry = (humid - _REF_HUMID_CARRY) * _HUMID_CARRY_PER_PCT
    # 2) Ball absorption / pop: ref=57% (humidor).  Higher humidity = ball absorbs
    #    moisture = less COR/exit velo.  Venue-adjusted (domes ≈ 0).
    humid_absorb = (humid - _REF_HUMID_ABSORB) * _HUMID_ABSORB_PER_PCT

    # Wind — trig decomposition per handedness
    wind_speed = float(data.get("wind_speed", 0) or 0)
    wind_dir = data.get("wind_dir", "")
    wind_angle = WIND_ANGLES.get(wind_dir)

    if wind_angle is not None and wind_speed > 0:
        wrad = math.radians(wind_angle)
        rhb_comp = wind_speed * math.cos(wrad - math.radians(_RHB_PULL_DEG))
        lhb_comp = wind_speed * math.cos(wrad - math.radians(_LHB_PULL_DEG))
        out_comp = wind_speed * math.cos(wrad)
        wind_rhb = _wind_carry_pct(rhb_comp)
        wind_lhb = _wind_carry_pct(lhb_comp)
        wind_avg = _wind_carry_pct(out_comp)
    else:
        wind_rhb = wind_lhb = wind_avg = 0.0

    # Pressure — deviation from expected at elevation
    pressure_val = data.get("pressure_hpa")
    elev = float(data.get("elevation", 0) or 0)
    if pressure_val:
        expected_p = _expected_pressure(elev)
        pressure = (expected_p - float(pressure_val)) * _PRESS_PER_HPA
    else:
        pressure = 0.0

    # Sky condition modifiers
    condition = data.get("condition", "")
    sky_hr = _sky_modifier(condition, _SKY_HR_MODIFIER)
    sky_xbh = _sky_modifier(condition, _SKY_HITS_XBH_MOD)
    sky_1b = _sky_modifier(condition, _SKY_HITS_1B_MOD)

    # Day vs night modifier
    day_night = _day_night_modifier(data)

    return {
        "temp": temp,
        "humid_carry": humid_carry,
        "humid_absorb": humid_absorb,
        "wind_rhb": wind_rhb,
        "wind_lhb": wind_lhb,
        "wind_avg": wind_avg,
        "pressure": pressure,
        "sky_hr": sky_hr,
        "sky_xbh": sky_xbh,
        "sky_1b": sky_1b,
        "day_night": day_night,
        "condition": condition,
    }


def _venue_adjust_weather(base: dict, venue_id: int) -> dict:
    """Stage 2: Apply asymmetric venue multipliers to base weather values.

    Each profile entry is (pos_mult, neg_mult).  When a base value is
    positive (weather boosts offense) pos_mult is used; when negative
    (weather suppresses) neg_mult is used.

    Humidity is split into two independent effects:
      humid_carry:  Air density (ref 50%).  Universal physics — passes
                    through WITHOUT venue adjustment.
      humid_absorb: Ball moisture absorption (ref 57% humidor).  Venue-
                    adjusted via "h" profile (domes ≈ 0, open-air varies).

    Sky modifiers are scaled by per-venue _VENUE_SKY_MULT (default 1.0).
    Day/night modifiers are venue-specific via _VENUE_DAY_NIGHT.
    """
    profile = VENUE_WX_PROFILE.get(venue_id, {})

    def _asym(val: float, key: str) -> float:
        tup = profile.get(key, (1.0, 1.0))
        return val * tup[0] if val >= 0 else val * tup[1]

    temp = _asym(base["temp"], "t")
    humid_carry = base["humid_carry"]          # universal — no venue adjustment
    humid_absorb = _asym(base["humid_absorb"], "h")  # venue-specific
    pressure = _asym(base["pressure"], "p")

    wind_rhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _asym(base["wind_rhb"], "w")))
    wind_lhb = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _asym(base["wind_lhb"], "w")))
    wind_avg = max(-_WIND_PER_HAND_CAP, min(_WIND_PER_HAND_CAP, _asym(base["wind_avg"], "w")))

    # Scale sky modifiers by venue sensitivity — rain uses separate multiplier
    cond = base.get("condition", "")
    _is_rain = any(k in (cond or "").lower() for k in ("rain", "drizzle", "shower"))
    sky_m = _VENUE_RAIN_MULT.get(venue_id, 1.0) if _is_rain else _VENUE_SKY_MULT.get(venue_id, 1.0)

    return {
        "temp": temp,
        "humid_carry": humid_carry,
        "humid_absorb": humid_absorb,
        "wind_rhb": wind_rhb,
        "wind_lhb": wind_lhb,
        "wind_avg": wind_avg,
        "pressure": pressure,
        "sky_hr": base["sky_hr"] * sky_m,
        "sky_xbh": base["sky_xbh"] * sky_m,
        "sky_1b": base["sky_1b"] * sky_m,
        "day_night": base["day_night"],
    }


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
    """Three-stage HR rating: neutral park → base weather → venue adjust.

    Pipeline
    --------
    Stage 0: ``_neutral_park()`` — static park factor at neutral weather.
    Stage 1: ``_base_weather()``  — universal weather effects.
    Stage 2: ``_venue_adjust_weather()`` — venue-specific multipliers.

    Returns dict with total_pct, lhb_pct, rhb_pct, color, roof_status,
    and a ``components`` breakdown.
    """
    # ── Stage 0: Neutral park baseline ──
    venue_id = data.get("venue_id")
    park = _neutral_park(venue_id)
    park_pct = park["hr_pct"]

    # ── Determine roof status (gatekeeper for weather adjustments) ──
    if _roof_override:
        roof_status, roof_confirmed = _roof_override
    else:
        roof_status, roof_confirmed = _predict_roof_status(data)

    # Fixed dome or roof closed → mostly no weather adjustments
    if roof_status in ("dome", "closed"):
        _closed_hm = _CLOSED_ROOF_HUMID_MULT.get(venue_id, 0.0)
        if _closed_hm > 0:
            base = _base_weather(data)
            profile = VENUE_WX_PROFILE.get(venue_id, {})
            def _asym_h(val):
                tup = profile.get("h", (1.0, 1.0))
                return val * tup[0] if val >= 0 else val * tup[1]
            # Carry passes through (universal); absorption gets venue + dome scaling
            humid_residual = base["humid_carry"] + _asym_h(base["humid_absorb"]) * _closed_hm
        else:
            humid_residual = 0.0
        total = park_pct + humid_residual
        color = C["grn"] if total > 8 else C["red"] if total < -8 else C["t1"]
        return {
            "total_pct": float(total),
            "lhb_pct": float(total + park["hr_dim_lhb"]),
            "rhb_pct": float(total + park["hr_dim_rhb"]),
            "color": color,
            "roof_status": roof_status,
            "roof_confirmed": roof_confirmed,
            "components": {
                "park_factor": park["pf_index"],
                "temp": 0.0, "humidity": float(humid_residual), "wind": 0.0,
                "wind_rhb": 0.0, "wind_lhb": 0.0,
                "pressure": 0.0, "weather_total": float(humid_residual),
            },
        }

    # ── Stage 1 → Stage 2: base weather → venue-adjusted weather ──
    base = _base_weather(data)
    wx = _venue_adjust_weather(base, venue_id)

    # Combine weather modifiers per handedness
    _h = wx["humid_carry"] + wx["humid_absorb"]
    wx_rhb = wx["temp"] + _h + wx["wind_rhb"] + wx["pressure"] + wx["sky_hr"] + wx["day_night"]
    wx_lhb = wx["temp"] + _h + wx["wind_lhb"] + wx["pressure"] + wx["sky_hr"] + wx["day_night"]

    # Cap the *average* wx so asymmetric wind games aren't clipped prematurely
    rhb_pct = park_pct + park["hr_dim_rhb"] + wx_rhb
    lhb_pct = park_pct + park["hr_dim_lhb"] + wx_lhb
    avg_wx = (wx_rhb + wx_lhb) / 2.0
    avg_wx = max(-_WX_CAP, min(_WX_CAP, avg_wx))
    total_pct = park_pct + avg_wx

    color = C["grn"] if total_pct > 8 else C["red"] if total_pct < -8 else C["t1"]

    return {
        "total_pct": total_pct,
        "lhb_pct": lhb_pct,
        "rhb_pct": rhb_pct,
        "color": color,
        "roof_status": roof_status,
        "roof_confirmed": roof_confirmed,
        "components": {
            "park_factor": park["pf_index"],
            "temp": round(wx["temp"], 1),
            "humidity": round(_h, 1),
            "wind": round((wx["wind_rhb"] + wx["wind_lhb"]) / 2.0, 1),
            "wind_rhb": round(wx["wind_rhb"], 1),
            "wind_lhb": round(wx["wind_lhb"], 1),
            "pressure": round(wx["pressure"], 1),
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
    _wm_raw = VENUE_WX_PROFILE.get(venue_id, {}).get("w", (1.0, 1.0))
    _wm = _wm_raw[0] if isinstance(_wm_raw, tuple) else _wm_raw

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
    """Three-stage hits rating: neutral park → base weather → venue adjust.

    Pipeline
    --------
    Stage 0: ``_neutral_park()`` — static hit-type park factors.
    Stage 1: ``_base_weather()``  — universal weather effects.
    Stage 2: ``_venue_adjust_weather()`` — venue-specific multipliers.

    Returns dict with xbh_pct, xbh_lhb, xbh_rhb, singles_pct, singles_lhb,
    singles_rhb, overall_pct, color, and roof_status.
    """
    # ── Stage 0: Neutral park baseline ──
    venue_id = data.get("venue_id")
    park = _neutral_park(venue_id)
    pf_1b_pct = park["singles_pct"]
    pf_2b_pct = park["pf_2b_pct"]
    pf_3b_pct = park["pf_3b_pct"]
    pf_xbh_pct = park["xbh_pct"]
    hr_park_pct = park["hr_pct"]

    if _roof_override:
        roof_status, _ = _roof_override
    else:
        roof_status, _ = _predict_roof_status(data)

    # ── Enclosed: mostly no weather adjustments ──
    if roof_status in ("dome", "closed", "Closed"):
        _closed_hm = _CLOSED_ROOF_HUMID_MULT.get(venue_id, 0.0)
        if _closed_hm > 0:
            base = _base_weather(data)
            profile = VENUE_WX_PROFILE.get(venue_id, {})
            def _asym_h2(val):
                tup = profile.get("h", (1.0, 1.0))
                return val * tup[0] if val >= 0 else val * tup[1]
            h_res = base["humid_carry"] + _asym_h2(base["humid_absorb"]) * _closed_hm
        else:
            h_res = 0.0
        s_res = h_res * _SINGLES_WX_WEIGHT
        x_res = h_res * _XBH_HUMID_WEIGHT
        overall = ((pf_1b_pct + s_res) * _W_1B + (pf_2b_pct + x_res) * _W_2B
                   + (pf_3b_pct + x_res) * _W_3B + (hr_park_pct + h_res) * _W_HR)
        color = C["grn"] if overall > 8 else C["red"] if overall < -8 else C["t1"]
        return {
            "xbh_pct": pf_xbh_pct + x_res,
            "xbh_lhb": pf_xbh_pct + x_res + park["xbh_dim_lhb"],
            "xbh_rhb": pf_xbh_pct + x_res + park["xbh_dim_rhb"],
            "singles_pct": pf_1b_pct + s_res,
            "singles_lhb": pf_1b_pct + s_res + park["s_dim_lhb"],
            "singles_rhb": pf_1b_pct + s_res + park["s_dim_rhb"],
            "overall_pct": overall, "color": color,
            "roof_status": roof_status,
        }

    # ── Stage 1 → Stage 2: base weather → venue-adjusted weather ──
    base = _base_weather(data)
    wx = _venue_adjust_weather(base, venue_id)

    dn_1b  = wx["day_night"] * 0.25
    dn_xbh = wx["day_night"] * 0.50

    # ── Singles weather (15% of HR sensitivity + sky + day/night) ──
    _hh = wx["humid_carry"] + wx["humid_absorb"]
    s_wx = (wx["temp"] + _hh + wx["wind_avg"] + wx["pressure"]) * _SINGLES_WX_WEIGHT + wx["sky_1b"] + dn_1b
    s_wx = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx))
    s_wx_rhb = (wx["temp"] + _hh + wx["wind_rhb"] + wx["pressure"]) * _SINGLES_WX_WEIGHT + wx["sky_1b"] + dn_1b
    s_wx_rhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx_rhb))
    s_wx_lhb = (wx["temp"] + _hh + wx["wind_lhb"] + wx["pressure"]) * _SINGLES_WX_WEIGHT + wx["sky_1b"] + dn_1b
    s_wx_lhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, s_wx_lhb))

    # ── 2B/3B weather (component-weighted fraction of HR + sky + day/night) ──
    x_wx = (wx["temp"] * _XBH_TEMP_WEIGHT + _hh * _XBH_HUMID_WEIGHT
            + wx["wind_avg"] * _XBH_WIND_WEIGHT + wx["pressure"] * _XBH_PRESS_WEIGHT) + wx["sky_xbh"] + dn_xbh
    x_wx = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx))
    x_wx_rhb = (wx["temp"] * _XBH_TEMP_WEIGHT + _hh * _XBH_HUMID_WEIGHT
                + wx["wind_rhb"] * _XBH_WIND_WEIGHT + wx["pressure"] * _XBH_PRESS_WEIGHT) + wx["sky_xbh"] + dn_xbh
    x_wx_rhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx_rhb))
    x_wx_lhb = (wx["temp"] * _XBH_TEMP_WEIGHT + _hh * _XBH_HUMID_WEIGHT
                + wx["wind_lhb"] * _XBH_WIND_WEIGHT + wx["pressure"] * _XBH_PRESS_WEIGHT) + wx["sky_xbh"] + dn_xbh
    x_wx_lhb = max(-_HITS_WX_CAP, min(_HITS_WX_CAP, x_wx_lhb))

    # ── Additive: park factor % + dimension splits + weather % ──
    singles_pct = pf_1b_pct + s_wx
    singles_rhb = pf_1b_pct + park["s_dim_rhb"] + s_wx_rhb
    singles_lhb = pf_1b_pct + park["s_dim_lhb"] + s_wx_lhb

    xbh_pct = pf_xbh_pct + x_wx
    xbh_rhb = pf_xbh_pct + park["xbh_dim_rhb"] + x_wx_rhb
    xbh_lhb = pf_xbh_pct + park["xbh_dim_lhb"] + x_wx_lhb

    # HR weather (100% sensitivity + sky + day/night) for overall calc
    hr_wx = (wx["temp"] + _hh
             + (wx["wind_rhb"] + wx["wind_lhb"]) / 2.0 + wx["pressure"] + wx["sky_hr"] + wx["day_night"])
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
            temp_str = f"{round(float(temp))}°F" if temp != "" else ""
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
                                            f"{round(float(temp))}°F" if temp != "--" else "--°F")
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
            self._cond_vals["temp"].setText(f"{round(float(temp))}°F" if temp != "--" else "--°F")
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
        self._cond_vals["temp"].setText(f"{round(float(temp))}°F" if temp != "--" else "--°F")
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
