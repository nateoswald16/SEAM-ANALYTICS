# -*- coding: utf-8 -*-
"""
Park Factors / Weather page — Seam Analytics

Fetches weather and venue data from the MLB Stats API + Open-Meteo,
renders an interactive grid of park-weather cards with mini-ballpark
wind-arrow visualisations.

Imported by seam_app.py — no reverse imports at module level.
"""

import json
import math
import os
import threading
import requests
import requests.adapters
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib3.util.retry import Retry

# Shared session with automatic retries on transient failures
_retry = Retry(total=2, backoff_factor=0.3, status_forcelist=[502, 503, 504, 429])
_http = requests.Session()
_http.mount("https://", requests.adapters.HTTPAdapter(max_retries=_retry))
_http.mount("http://", requests.adapters.HTTPAdapter(max_retries=_retry))

# Separate fast session for Open-Meteo (no retries — fail fast if down)
_http_om = requests.Session()
_http_om.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
_http_om.mount("http://", requests.adapters.HTTPAdapter(max_retries=0))

from PyQt6.QtWidgets import (
    QWidget, QFrame, QVBoxLayout, QHBoxLayout, QLabel,
    QScrollArea, QGridLayout, QPushButton, QSizePolicy,
)
from PyQt6.QtCore import Qt, QRectF, QPointF, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import (
    QColor, QFont, QPainter, QPainterPath, QPen, QBrush, QPixmap, QImage,
)

import _app_paths

# ═══════════════════════════════════════════════════════════════════════════════
# Design tokens  (mirrored from seam_app – kept local to avoid circular import)
# ═══════════════════════════════════════════════════════════════════════════════
C = {
    "bg0": "#0a0a0a", "bg1": "#111111", "bg2": "#1a1a1a", "bg3": "#242424",
    "bdr": "#2a2a2a", "bdrl": "#333333",
    "t1": "#f0f0ee", "t2": "#888885", "t3": "#555550",
    "ora": "#f07020", "red": "#e85d3a", "grn": "#4ade80", "amb": "#f59e0b",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Savant park factors  (venue_id → wOBA-based overall index, 100 = neutral)
# Source: baseballsavant.mlb.com/leaderboard/statcast-park-factors (3-yr rolling)
# ═══════════════════════════════════════════════════════════════════════════════
VENUE_PARK_FACTORS: dict[int, int] = {
    1:    101,  # Angel Stadium
    2:    100,  # Oriole Park at Camden Yards
    3:    104,  # Fenway Park
    4:     99,  # Rate Field (Guaranteed Rate / White Sox)
    5:     97,  # Progressive Field
    7:    101,  # Kauffman Stadium
    12:   100,  # Tropicana Field
    14:   100,  # Rogers Centre
    15:   103,  # Chase Field
    17:    97,  # Wrigley Field
    19:   113,  # Coors Field
    22:   101,  # UNIQLO Field at Dodger Stadium
    31:    99,  # PNC Park
    32:    97,  # American Family Field
    680:   91,  # T-Mobile Park
    2392: 100,  # Daikin Park
    2394: 100,  # Comerica Park
    2395:  97,  # Oracle Park
    2529: 100,  # Sutter Health Park (Athletics temp)
    2602: 103,  # Great American Ball Park
    2680:  97,  # Petco Park
    2681: 101,  # Citizens Bank Park
    2889: 100,  # Busch Stadium
    3289:  98,  # Citi Field
    3309: 101,  # Nationals Park
    3312: 102,  # Target Field
    3313: 100,  # Yankee Stadium
    4169: 101,  # loanDepot park
    4705: 101,  # Truist Park
    5325:  97,  # Globe Life Field
}


def _mk(text, color=None, size=10, bold=False, align=None):
    """Minimal label factory (avoids importing seam_app.mk_label)."""
    lbl = QLabel(str(text))
    lbl.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
    w = "700" if bold else "400"
    lbl.setStyleSheet(
        f"color:{color or C['t1']}; background:transparent;"
        f"font-family:'Segoe UI'; font-size:{size}px; font-weight:{w};")
    if align:
        lbl.setAlignment(align)
    return lbl


# ═══════════════════════════════════════════════════════════════════════════════
# Carry-effect legend  (altitude + air pressure scale bars)
# ═══════════════════════════════════════════════════════════════════════════════
# Rough carry-effect data (relative to sea-level / 1013 hPa baseline)
_ALT_TICKS = [          # (elevation_ft, label, carry_pct)
    (0,    "0 ft",       0),
    (600,  "600",       +1),
    (1000, "1 000",     +2),
    (2000, "2 000",     +4),
    (3500, "3 500",     +6),
    (5200, "5 200",     +9),   # Coors
]
_PRESS_TICKS = [        # (hPa, label, carry_pct)  lower pressure → more carry
    (1030, "1030",      -2),
    (1020, "1020",      -1),
    (1013, "1013",       0),
    (1000, "1000",      +1),
    (990,  "990",       +2),
    (975,  "975",       +4),
]


class _CarryLegend(QWidget):
    """Full-width legend with two side-by-side scale bars."""

    _H = 64

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(self._H)
        self.setSizePolicy(QSizePolicy.Policy.Expanding,
                           QSizePolicy.Policy.Fixed)

    # ── painting ──
    def paintEvent(self, _):
        w = self.width()
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        pad = 30                   # internal padding so edge labels aren't clipped
        gap = 40                   # space between the two bars
        usable = w - 2 * pad
        half = (usable - gap) / 2
        bar_h = 6
        y_bar = 32                 # vertical position of gradient bar

        def draw_bar(x0, bw, title, ticks, val_range):
            vmin, vmax = val_range

            # title
            p.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
            p.setPen(QColor(C["t2"]))
            p.drawText(QRectF(x0, 0, bw, 16),
                       Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                       title)

            # gradient bar
            steps = 80
            for i in range(steps):
                frac = i / steps
                r = int(60 + (74 - 60) * (1 - frac))
                g_c = int(90 + (222 - 90) * frac)
                b = int(60 + (128 - 60) * frac * 0.5)
                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(QColor(r, g_c, b, 160))
                sx = x0 + frac * bw
                sw = bw / steps + 1
                p.drawRect(QRectF(sx, y_bar, sw, bar_h))

            # ticks + labels
            for val, lbl, pct in ticks:
                frac = (val - vmin) / (vmax - vmin) if vmax != vmin else 0
                frac = max(0.0, min(1.0, frac))
                tx = x0 + frac * bw
                # tick mark
                p.setPen(QPen(QColor(C["t2"]), 1))
                p.drawLine(QPointF(tx, y_bar - 1), QPointF(tx, y_bar + bar_h + 1))
                # carry % — above bar
                p.setFont(QFont("Segoe UI", 8))
                col = C["grn"] if pct > 0 else (C["red"] if pct < 0 else C["t2"])
                p.setPen(QColor(col))
                sign = "+" if pct > 0 else ""
                p.drawText(QRectF(tx - 24, y_bar - 15, 48, 13),
                           Qt.AlignmentFlag.AlignCenter,
                           f"{sign}{pct}%")
                # value label — below bar
                p.setFont(QFont("Segoe UI", 8))
                p.setPen(QColor(C["t3"]))
                p.drawText(QRectF(tx - 28, y_bar + bar_h + 2, 56, 13),
                           Qt.AlignmentFlag.AlignCenter, lbl)

        draw_bar(pad, half, "ALTITUDE  →  BALL CARRY",
                 _ALT_TICKS, (0, 5200))
        draw_bar(pad + half + gap, half, "AIR PRESSURE  →  BALL CARRY",
                 _PRESS_TICKS, (1030, 975))

        p.end()


def _sa_style():
    return f"""
        QScrollArea {{ background:transparent; border:none; }}
        QScrollBar:vertical {{ background:transparent; width:4px; }}
        QScrollBar::handle:vertical {{ background:{C['bdrl']}; border-radius:2px; min-height:20px; }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}
        QScrollBar:horizontal {{ height:0; }}
    """


# ═══════════════════════════════════════════════════════════════════════════════
# Wind helpers
# ═══════════════════════════════════════════════════════════════════════════════
# Angle: 0° = toward CF (up on screen), clockwise
WIND_ANGLES = {
    "Out To CF":   0,
    "Out To RF":  45,
    "L To R":     90,
    "In From LF": 135,
    "In From CF": 180,
    "In From RF": 225,
    "R To L":     270,
    "Out To LF":  315,
}
HITTER_FRIENDLY = {"Out To CF", "Out To RF", "Out To LF"}
PITCHER_FRIENDLY = {"In From CF", "In From RF", "In From LF"}


def _parse_wind(wind_str):
    """Return (speed_mph: int, direction_label: str, angle_deg or None)."""
    if not wind_str:
        return 0, "Calm", None
    parts = wind_str.split(", ", 1)
    speed = 0
    try:
        speed = int(parts[0].split()[0])
    except Exception:
        pass
    direction = parts[1] if len(parts) > 1 else "Calm"
    return speed, direction, WIND_ANGLES.get(direction)


# ═══════════════════════════════════════════════════════════════════════════════
# Data fetching
# ═══════════════════════════════════════════════════════════════════════════════
_WMO_CONDITIONS = {
    0: "Clear", 1: "Partly Cloudy", 2: "Partly Cloudy", 3: "Overcast",
    45: "Cloudy", 48: "Cloudy",
    51: "Drizzle", 53: "Drizzle", 55: "Drizzle",
    61: "Rain", 63: "Rain", 65: "Rain",
    66: "Rain", 67: "Rain",
    71: "Snow", 73: "Snow", 75: "Snow", 77: "Snow",
    80: "Rain", 81: "Rain", 82: "Rain",
    85: "Snow", 86: "Snow",
    95: "Rain", 96: "Rain", 99: "Rain",
}

# Map compass degrees → nearest MLB-style wind direction label
_COMPASS_DIRS = [
    (  0, "Out To CF"),
    ( 45, "Out To RF"),
    ( 90, "L To R"),
    (135, "In From LF"),
    (180, "In From CF"),
    (225, "In From RF"),
    (270, "R To L"),
    (315, "Out To LF"),
]


def _compass_to_mlb_wind(deg: float) -> str:
    """Convert meteorological wind direction (degrees) to MLB-style label."""
    best = min(_COMPASS_DIRS, key=lambda d: min(abs(d[0] - deg),
                                                  360 - abs(d[0] - deg)))
    return best[1]


# ── NWS (National Weather Service) — primary provider ───────────────────────
_nws_circuit_open = True   # False → NWS known unreachable, skip calls
_nws_grid_cache: dict[str, dict] = {}   # "(lat,lon)" → {"hourly_url": ..., "stations_url": ...}
_nws_grid_lock = threading.Lock()

_NWS_HEADERS = {"User-Agent": "SeamAnalytics/1.0 (mlb-park-weather)", "Accept": "application/geo+json"}

_NWS_COMPASS = {
    "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5, "E": 90, "ESE": 112.5,
    "SE": 135, "SSE": 157.5, "S": 180, "SSW": 202.5, "SW": 225,
    "WSW": 247.5, "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
}

_NWS_COND_SCORE = {
    "rain": 90, "drizzle": 80, "shower": 85, "thunder": 95, "storm": 95,
    "snow": 70, "overcast": 40, "cloudy": 30, "mostly cloudy": 35,
    "partly": 20, "mist": 25, "fog": 25, "haze": 20,
    "clear": 10, "sunny": 10, "fair": 10,
}


def _nws_cond_score(txt: str) -> int:
    t = txt.lower()
    for kw, sc in _NWS_COND_SCORE.items():
        if kw in t:
            return sc
    return 15


def _nws_resolve_grid(lat, lon) -> dict:
    """Resolve lat/lon to NWS grid point URLs.  Cached per coordinate pair."""
    key = f"{round(lat, 3)},{round(lon, 3)}"
    with _nws_grid_lock:
        if key in _nws_grid_cache:
            return _nws_grid_cache[key]
    try:
        r = _http_om.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=_NWS_HEADERS, timeout=5)
        r.raise_for_status()
        props = r.json().get("properties", {})
        info = {
            "hourly_url": props.get("forecastHourly", ""),
        }
        with _nws_grid_lock:
            _nws_grid_cache[key] = info
        return info
    except Exception:
        return {}


def _fetch_nws(lat, lon, game_utc_iso: str | None = None):
    """Fetch forecast data from the National Weather Service (api.weather.gov).

    Returns the same dict shape as _fetch_open_meteo / _fetch_weatherapi.
    NWS hourly gives temp, precip %, wind, and condition.
    Pressure is NOT included — use _fetch_pressure_weatherapi() separately.
    """
    global _nws_circuit_open
    if not _nws_circuit_open:
        return {}
    try:
        grid = _nws_resolve_grid(lat, lon)
        hourly_url = grid.get("hourly_url")
        if not hourly_url:
            return {}

        # ── Hourly forecast ──────────────────────────────────────────────
        hr = _http_om.get(hourly_url, headers=_NWS_HEADERS, timeout=5)
        hr.raise_for_status()
        periods = hr.json().get("properties", {}).get("periods", [])
        if not periods:
            return {}

        result: dict = {"precip_pct": None, "pressure_hpa": None}

        # Current-hour precip: use the first period (closest to now)
        first = periods[0]
        result["precip_pct"] = (
            first.get("probabilityOfPrecipitation", {}).get("value") or 0)

        # ── Game-window hourly forecast ──────────────────────────────────
        if game_utc_iso:
            try:
                game_dt = datetime.fromisoformat(
                    game_utc_iso.replace("Z", "+00:00"))
                game_local = game_dt.astimezone()

                # Build periods keyed by their start hour
                def _period_dt(p):
                    return datetime.fromisoformat(p["startTime"])

                # Find the 4 periods covering the game window
                window = []
                for p in periods:
                    pdt = _period_dt(p)
                    diff = (pdt - game_local).total_seconds()
                    if -1800 <= diff < 4 * 3600:  # 30 min before → 4h after
                        window.append(p)
                    if len(window) >= 4:
                        break

                if not window:
                    # Fallback: pick closest period
                    closest = min(periods,
                                  key=lambda p: abs((_period_dt(p) - game_local).total_seconds()))
                    window = [closest]

                first_pitch = window[0]

                # Temperature
                result["forecast_temp"] = first_pitch.get("temperature")

                # Wind — NWS gives "5 mph" or "5 to 10 mph" string + compass dir
                ws_str = first_pitch.get("windSpeed", "0")
                try:
                    ws_parts = ws_str.replace(" mph", "").split(" to ")
                    result["forecast_wind_speed"] = int(ws_parts[-1])
                except Exception:
                    result["forecast_wind_speed"] = 0
                wd_str = first_pitch.get("windDirection", "")
                result["forecast_wind_deg"] = _NWS_COMPASS.get(wd_str)

                # Pressure from observation (already set above)
                result["forecast_pressure"] = result.get("pressure_hpa")

                # Precip: max across game window
                window_precip = [
                    p.get("probabilityOfPrecipitation", {}).get("value") or 0
                    for p in window
                ]
                result["forecast_precip"] = max(window_precip) if window_precip else None

                # Condition: worst weather in the window
                worst_cond = max(
                    (p.get("shortForecast", "") for p in window),
                    key=_nws_cond_score,
                )
                result["forecast_condition"] = worst_cond

                # Per-hour conditions for animated icon cycling
                hourly_conds = []
                for p in window:
                    pdt = _period_dt(p)
                    h_lbl = pdt.strftime("%I %p").lstrip("0")
                    cond_txt = p.get("shortForecast", "")
                    pr = p.get("probabilityOfPrecipitation", {}).get("value") or 0
                    # Parse wind speed from NWS string like "5 mph" or "5 to 10 mph"
                    h_ws_str = p.get("windSpeed", "0")
                    try:
                        h_ws = int(h_ws_str.replace(" mph", "").split(" to ")[-1])
                    except Exception:
                        h_ws = 0
                    h_wd_compass = p.get("windDirection", "")
                    h_wd_deg = _NWS_COMPASS.get(h_wd_compass)
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg) if h_wd_deg is not None else ""
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "night": not p.get("isDaytime", True),
                        "temp": p.get("temperature"),
                        "wind_speed": h_ws,
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        return result
    except Exception:
        _nws_circuit_open = False
        return {}


_om_circuit_open = True   # False → Open-Meteo known unreachable, skip calls


def _fetch_open_meteo(lat, lon, game_utc_iso: str | None = None):
    """Fetch forecast data from Open-Meteo.

    Always returns precip_pct and pressure_hpa for the current moment.
    When *game_utc_iso* is given, also returns hourly forecast fields
    (forecast_temp, forecast_wind_speed, forecast_wind_deg,
    forecast_condition) for the hour closest to game time.
    """
    global _om_circuit_open
    if not _om_circuit_open:
        return {}
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&current=precipitation_probability,surface_pressure"
            f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,"
            f"precipitation_probability,surface_pressure,weather_code"
            f"&temperature_unit=fahrenheit&wind_speed_unit=mph"
            f"&forecast_days=2&timezone=auto"
        )
        r = _http_om.get(url, timeout=3)
        r.raise_for_status()
        data = r.json()

        cur = data.get("current", {})
        result = {
            "precip_pct": cur.get("precipitation_probability", 0) or 0,
            "pressure_hpa": round(cur.get("surface_pressure", 0) or 0, 1),
        }

        # Pick hourly slots covering the game window (start → +3 h)
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        if game_utc_iso and times:
            try:
                game_dt = datetime.fromisoformat(
                    game_utc_iso.replace("Z", "+00:00"))
                game_local = game_dt.astimezone()
                game_hour = game_local.strftime("%Y-%m-%dT%H:00")
                # Find start index
                if game_hour in times:
                    idx = times.index(game_hour)
                else:
                    idx = min(range(len(times)),
                              key=lambda i: abs(
                                  datetime.fromisoformat(times[i]).hour
                                  - game_local.hour
                                  + (0 if times[i][:10] == game_hour[:10]
                                     else 24)))
                # Collect indices for the ~4-hour game window
                game_idxs = [i for i in range(idx, min(idx + 4, len(times)))]
                if not game_idxs:
                    game_idxs = [idx]

                # First-pitch hour for temp / wind / pressure / condition
                result["forecast_temp"] = hourly.get(
                    "temperature_2m", [None]*(idx+1))[idx]
                result["forecast_wind_speed"] = hourly.get(
                    "wind_speed_10m", [None]*(idx+1))[idx]
                result["forecast_wind_deg"] = hourly.get(
                    "wind_direction_10m", [None]*(idx+1))[idx]
                result["forecast_pressure"] = hourly.get(
                    "surface_pressure", [None]*(idx+1))[idx]

                # Precip: max chance across the game window
                precip_vals = hourly.get("precipitation_probability", [])
                window_precip = [precip_vals[i] for i in game_idxs
                                 if i < len(precip_vals)
                                 and precip_vals[i] is not None]
                result["forecast_precip"] = (
                    max(window_precip) if window_precip else None)

                # Condition: worst (rainiest) weather code in the window
                wcodes = hourly.get("weather_code", [])
                window_codes = [wcodes[i] for i in game_idxs
                                if i < len(wcodes)
                                and wcodes[i] is not None]
                if window_codes:
                    worst = max(window_codes)  # higher WMO = worse weather
                    result["forecast_condition"] = _WMO_CONDITIONS.get(
                        worst, "Clear")
                else:
                    wcode = wcodes[idx] if idx < len(wcodes) else None
                    result["forecast_condition"] = _WMO_CONDITIONS.get(
                        wcode, "Clear") if wcode is not None else ""

                # Per-hour conditions for animated icon cycling
                hourly_conds = []
                temps = hourly.get("temperature_2m", [])
                wspeeds = hourly.get("wind_speed_10m", [])
                wdirs = hourly.get("wind_direction_10m", [])
                for gi in game_idxs:
                    t_str = times[gi] if gi < len(times) else ""
                    try:
                        h_dt = datetime.fromisoformat(t_str)
                        h_lbl = h_dt.strftime("%I %p").lstrip("0")
                        is_night = h_dt.hour >= 19 or h_dt.hour < 6
                    except Exception:
                        h_lbl = ""
                        is_night = False
                    wc = wcodes[gi] if gi < len(wcodes) else None
                    cond_txt = _WMO_CONDITIONS.get(wc, "Clear") if wc is not None else ""
                    pr = precip_vals[gi] if gi < len(precip_vals) else None
                    h_temp = temps[gi] if gi < len(temps) else None
                    h_ws = wspeeds[gi] if gi < len(wspeeds) else None
                    h_wd_deg = wdirs[gi] if gi < len(wdirs) else None
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg) if h_wd_deg is not None else ""
                    if h_ws is not None:
                        h_ws = round(h_ws * 0.621371)  # km/h → mph
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "night": is_night,
                        "temp": round(h_temp * 9/5 + 32) if h_temp is not None else None,
                        "wind_speed": h_ws,
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        return result
    except Exception:
        _om_circuit_open = False   # trip circuit — skip remaining calls
        return {}


# ── WeatherAPI.com fallback ──────────────────────────────────────────────────
_weatherapi_key: str | None = None
_weatherapi_key_loaded = False
_wa_circuit_open = True  # False → WeatherAPI known unreachable, skip calls


def _load_weatherapi_key() -> str | None:
    """Load WeatherAPI.com key from disk file or environment variable (cached)."""
    global _weatherapi_key, _weatherapi_key_loaded
    if _weatherapi_key_loaded:
        return _weatherapi_key
    _weatherapi_key_loaded = True
    # Try the key file first
    try:
        kf = _app_paths.WEATHERAPI_KEY_FILE
        if os.path.isfile(kf):
            with open(kf, "r", encoding="utf-8") as f:
                key = f.read().strip()
            if key:
                _weatherapi_key = key
                return _weatherapi_key
    except Exception:
        pass
    # Fall back to environment variable
    key = os.environ.get("WEATHERAPI_KEY", "").strip()
    if key:
        _weatherapi_key = key
    return _weatherapi_key


def _fetch_weatherapi(lat, lon, game_utc_iso: str | None = None):
    """Fallback: fetch forecast data from WeatherAPI.com.

    Returns the same dict shape as _fetch_open_meteo so the caller
    doesn't need to know which provider answered.
    """
    global _wa_circuit_open
    if not _wa_circuit_open:
        return {}
    key = _load_weatherapi_key()
    if not key:
        return {}
    try:
        url = (
            f"https://api.weatherapi.com/v1/forecast.json"
            f"?key={key}&q={lat},{lon}&days=2&aqi=no&alerts=no"
        )
        r = _http_om.get(url, timeout=5)
        r.raise_for_status()
        data = r.json()

        cur = data.get("current", {})
        result = {
            "precip_pct": None,
            "pressure_hpa": round(cur.get("pressure_mb", 0) or 0, 1),
        }

        # Build a flat list of all hourly slots across forecast days
        all_hours = []
        for day in data.get("forecast", {}).get("forecastday", []):
            all_hours.extend(day.get("hour", []))

        if not all_hours:
            return result

        # Current-hour precipitation: find the slot closest to now
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:00")
        cur_hour = None
        for h in all_hours:
            if h.get("time", "")[:13] == now_str[:13]:
                cur_hour = h
                break
        if cur_hour:
            result["precip_pct"] = cur_hour.get("chance_of_rain", 0) or 0

        # Hourly forecast across the game window (start → +3 h)
        if game_utc_iso and all_hours:
            try:
                game_dt = datetime.fromisoformat(
                    game_utc_iso.replace("Z", "+00:00"))
                game_local = game_dt.astimezone()
                game_hour_str = game_local.strftime("%Y-%m-%d %H:00")

                # Find starting hour index
                start_idx = None
                for i, h in enumerate(all_hours):
                    if h.get("time", "")[:13] == game_hour_str[:13]:
                        start_idx = i
                        break
                if start_idx is None:
                    start_idx = min(
                        range(len(all_hours)),
                        key=lambda i: abs(
                            datetime.fromisoformat(
                                all_hours[i]["time"]).hour - game_local.hour
                            + (0 if all_hours[i]["time"][:10] == game_hour_str[:10]
                               else 24)),
                    )

                # Collect hours for the ~4-hour game window
                window = [all_hours[i] for i in range(
                    start_idx, min(start_idx + 4, len(all_hours)))]
                if not window:
                    window = [all_hours[start_idx]]

                match = window[0]  # first-pitch hour
                result["forecast_temp"] = match.get("temp_f")
                result["forecast_wind_speed"] = match.get("wind_mph")
                result["forecast_wind_deg"] = match.get("wind_degree")
                result["forecast_pressure"] = match.get("pressure_mb")

                # Precip: max chance across the game window
                window_precip = [h.get("chance_of_rain") for h in window
                                 if h.get("chance_of_rain") is not None]
                result["forecast_precip"] = (
                    max(window_precip) if window_precip else None)

                # Condition: worst weather in the window
                cond_priority = {
                    "rain": 90, "drizzle": 80, "shower": 85,
                    "thunder": 95, "snow": 70, "overcast": 40,
                    "cloudy": 30, "partly": 20, "mist": 25,
                    "fog": 25, "clear": 10, "sunny": 10,
                }
                def _cond_score(txt):
                    t = txt.lower()
                    for kw, sc in cond_priority.items():
                        if kw in t:
                            return sc
                    return 15

                worst_cond = max(
                    (h.get("condition", {}).get("text", "") for h in window),
                    key=_cond_score,
                )
                result["forecast_condition"] = worst_cond

                # Per-hour conditions for animated icon cycling
                hourly_conds = []
                for wh in window:
                    t_str = wh.get("time", "")
                    try:
                        h_dt = datetime.fromisoformat(t_str)
                        h_lbl = h_dt.strftime("%I %p").lstrip("0")
                    except Exception:
                        h_lbl = ""
                    cond_txt = wh.get("condition", {}).get("text", "")
                    pr = wh.get("chance_of_rain")
                    h_wd_deg = wh.get("wind_degree")
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg) if h_wd_deg is not None else ""
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "night": not wh.get("is_day", 1),
                        "temp": wh.get("temp_f"),
                        "wind_speed": wh.get("wind_mph"),
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        return result
    except Exception:
        _wa_circuit_open = False  # trip circuit — skip remaining calls
        return {}


def fetch_park_weather(date_str: str) -> list[dict]:
    """Fetch weather + venue info for every game on *date_str* (YYYY-MM-DD).

    Returns a list of dicts (one per game) sorted by game time.
    Each dict contains: game_id, away, home, time, status, venue_name,
    venue_id, lat, lon, elevation, roof_type, temp, condition,
    wind_speed, wind_dir, wind_angle, precip_pct, pressure_hpa.
    """
    global _nws_circuit_open, _wa_circuit_open, _om_circuit_open
    _nws_circuit_open = True   # reset circuit breakers for each fetch cycle
    _wa_circuit_open = True
    _om_circuit_open = True
    try:
        url = (
            "https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&date={date_str}"
            "&hydrate=probablePitcher,venue(location,fieldInfo),weather,team"
        )
        r = _http.get(url, timeout=10)
        r.raise_for_status()
        raw_games = []
        for d in r.json().get("dates", []):
            for g in d.get("games", []):
                if g.get("gameType", "R") in ("R", "W", "D", "L", "C"):
                    raw_games.append(g)
    except Exception:
        return []

    def _process(g):
        gid = g.get("gamePk", 0)
        t = g.get("teams", {})
        away_abbr = t.get("away", {}).get("team", {}).get("abbreviation", "?")
        home_abbr = t.get("home", {}).get("team", {}).get("abbreviation", "?")

        status = g.get("status", {}).get("detailedState", "")
        time_str = "TBD"
        gd = g.get("gameDate", "")
        if gd:
            try:
                dt_utc = datetime.fromisoformat(gd.replace("Z", "+00:00"))
                time_str = dt_utc.astimezone().strftime("%I:%M %p").lstrip("0")
            except Exception:
                pass

        weather = g.get("weather", {})
        venue = g.get("venue", {})
        loc = venue.get("location", {})
        coords = loc.get("defaultCoordinates", {})
        fi = venue.get("fieldInfo", {})
        ws, wd, wa = _parse_wind(weather.get("wind"))

        result = {
            "game_id": gid, "away": away_abbr, "home": home_abbr,
            "time": time_str, "status": status,
            "venue_name": venue.get("name", ""),
            "venue_id": venue.get("id"),
            "lat": coords.get("latitude"),
            "lon": coords.get("longitude"),
            "elevation": loc.get("elevation"),
            "roof_type": fi.get("roofType", "Open"),
            "temp": weather.get("temp"),
            "condition": weather.get("condition", ""),
            "wind_speed": ws, "wind_dir": wd, "wind_angle": wa,
            "precip_pct": None, "pressure_hpa": None,
        }

        # Venue coords may be missing from schedule; grab from game feed
        if not result["lat"]:
            try:
                feed = _http.get(
                    f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live",
                    timeout=10,
                ).json()
                gv = feed.get("gameData", {}).get("venue", {})
                gl = gv.get("location", {})
                gc = gl.get("defaultCoordinates", {})
                gf = gv.get("fieldInfo", {})
                result["lat"] = gc.get("latitude")
                result["lon"] = gc.get("longitude")
                result["elevation"] = gl.get("elevation", result["elevation"])
                result["roof_type"] = gf.get("roofType", result["roof_type"])
                if not result["temp"]:
                    gw = feed.get("gameData", {}).get("weather", {})
                    if gw:
                        result["temp"] = gw.get("temp")
                        result["condition"] = gw.get("condition", "")
                        ws2, wd2, wa2 = _parse_wind(gw.get("wind"))
                        result["wind_speed"] = ws2
                        result["wind_dir"] = wd2
                        result["wind_angle"] = wa2
            except Exception:
                pass

        # Retractable roofs: MLB often reports condition as "Dome" even when
        # the roof may be open.  Clear it so Open-Meteo outdoor weather fills in.
        if result["roof_type"] == "Retractable" and result["condition"].lower() in ("dome", ""):
            result["condition"] = ""

        # NWS primary → WeatherAPI fallback → Open-Meteo fallback
        if result["lat"] and result["lon"]:
            om = _fetch_nws(result["lat"], result["lon"],
                            game_utc_iso=gd or None)
            if not om:
                om = _fetch_weatherapi(result["lat"], result["lon"],
                                       game_utc_iso=gd or None)
            if not om:
                om = _fetch_open_meteo(result["lat"], result["lon"],
                                       game_utc_iso=gd or None)
            result["precip_pct"] = om.get("precip_pct")
            result["pressure_hpa"] = om.get("pressure_hpa")

            # Fill missing MLB weather from hourly forecast
            if not result["temp"] and om.get("forecast_temp") is not None:
                result["temp"] = str(round(om["forecast_temp"]))
            if not result["condition"] and om.get("forecast_condition"):
                result["condition"] = om["forecast_condition"]
            if result["wind_speed"] == 0 and om.get("forecast_wind_speed") is not None:
                result["wind_speed"] = round(om["forecast_wind_speed"])
                deg = om.get("forecast_wind_deg")
                if deg is not None:
                    mlb_dir = _compass_to_mlb_wind(deg)
                    result["wind_dir"] = mlb_dir
                    result["wind_angle"] = WIND_ANGLES.get(mlb_dir)
            if om.get("forecast_precip") is not None:
                result["precip_pct"] = om["forecast_precip"]
            if om.get("forecast_pressure") is not None:
                result["pressure_hpa"] = round(om["forecast_pressure"], 1)
            if om.get("hourly_conditions"):
                result["hourly_conditions"] = om["hourly_conditions"]

            # Pressure: grab from WeatherAPI current conditions if still missing
            if result["pressure_hpa"] is None:
                try:
                    wa_key = _load_weatherapi_key()
                    if wa_key:
                        pr = _http_om.get(
                            f"https://api.weatherapi.com/v1/current.json"
                            f"?key={wa_key}&q={result['lat']},{result['lon']}&aqi=no",
                            timeout=3,
                        )
                        pr.raise_for_status()
                        mb = pr.json().get("current", {}).get("pressure_mb")
                        if mb:
                            result["pressure_hpa"] = round(mb, 1)
                except Exception:
                    pass

        return result

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_process, g): g for g in raw_games}
        try:
            for f in as_completed(futs, timeout=20):
                try:
                    results.append(f.result())
                except Exception:
                    pass
        except TimeoutError:
            # Return whatever games finished in time
            for f in futs:
                if f.done() and not f.exception():
                    try:
                        r = f.result()
                        if r not in results:
                            results.append(r)
                    except Exception:
                        pass

    results.sort(key=lambda x: x.get("time", "TBD"))
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# QPainter helpers — weather icons
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
    # Full circle in pale yellow
    p.setBrush(QBrush(QColor("#e8d44d")))
    p.drawEllipse(QPointF(cx, cy), r, r)
    # Cutout circle shifted right to create crescent
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
    p.drawRoundedRect(
        QRectF(cx - r * 2.0, cy, r * 4.0, r * 1.0), 2, 2)


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
        # Dome — faint dash
        p.setPen(QPen(QColor(C["t3"]), 1))
        p.drawText(QRectF(cx - 12, cy - 6, 24, 12),
                    Qt.AlignmentFlag.AlignCenter, "—")
    else:
        # Unknown condition — try to show something reasonable
        _clear_icon(p, cx - sz * 0.15, cy - sz * 0.1, sz * 0.75)
        _paint_cloud(p, cx + sz * 0.15, cy + sz * 0.15, sz * 0.8)


# ═══════════════════════════════════════════════════════════════════════════════
# Animated weather-condition overlay (cycles through game-window hours)
# ═══════════════════════════════════════════════════════════════════════════════
class _WeatherCycleOverlay(QWidget):
    """Paints a weather icon + hour label that cross-fades between slots."""

    indexChanged = pyqtSignal(int)    # emitted when visible slot changes

    FADE_MS   = 600          # fade-transition duration
    HOLD_MS   = 3000         # how long each hour is displayed
    TICK_MS   = 30           # repaint interval during fades (~33 fps)

    def __init__(self, conditions: list[dict], w: int, h: int, parent=None):
        super().__init__(parent)
        self.setFixedSize(w, h)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._conds = conditions if conditions else []
        self._idx = 0
        self._opacity = 1.0        # current frame opacity
        self._fading_out = False
        self._frames: list[QPixmap] = []

        # Pre-render a pixmap for each hourly condition
        for slot in self._conds:
            pm = QPixmap(w, h)
            pm.fill(QColor(0, 0, 0, 0))
            p = QPainter(pm)
            p.setRenderHint(QPainter.RenderHint.Antialiasing)
            # icon centred in top portion
            _paint_condition(p, w / 2, h * 0.28, slot.get("condition", ""), 28,
                             night=slot.get("night", False))
            # hour label below icon
            p.setFont(QFont("Segoe UI", 8))
            p.setPen(QColor(C["t2"]))
            lbl = slot.get("hour", "")
            p.drawText(QRectF(0, h * 0.54, w, 14),
                       Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop,
                       lbl)
            # precip line
            pr = slot.get("precip")
            if pr is not None and pr > 0:
                p.drawText(QRectF(0, h * 0.72, w, 14),
                           Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop,
                           f"Precip {pr}%")
            p.end()
            self._frames.append(pm)

        if len(self._frames) > 1:
            self._timer = QTimer(self)
            self._timer.timeout.connect(self._tick)
            # Start with a hold period, then begin cycling
            self._hold_timer = QTimer(self)
            self._hold_timer.setSingleShot(True)
            self._hold_timer.timeout.connect(self._start_fade_out)
            self._hold_timer.start(self.HOLD_MS)

    # ── animation machinery ──────────────────────────────────────────────
    def _start_fade_out(self):
        self._fading_out = True
        self._opacity = 1.0
        self._timer.start(self.TICK_MS)

    def _tick(self):
        step = self.TICK_MS / self.FADE_MS
        if self._fading_out:
            self._opacity = max(0.0, self._opacity - step)
            if self._opacity <= 0.0:
                # Switch to next frame and fade in
                self._idx = (self._idx + 1) % len(self._frames)
                self.indexChanged.emit(self._idx)
                self._fading_out = False
                self._opacity = 0.0
        else:
            self._opacity = min(1.0, self._opacity + step)
            if self._opacity >= 1.0:
                # Hold on this frame
                self._timer.stop()
                self._hold_timer.start(self.HOLD_MS)
        self.update()

    # ── painting ─────────────────────────────────────────────────────────
    def paintEvent(self, _):
        if not self._frames:
            return
        p = QPainter(self)
        p.setOpacity(self._opacity)
        p.drawPixmap(0, 0, self._frames[self._idx])
        p.end()


# ═══════════════════════════════════════════════════════════════════════════════
# Mini-ballpark widget  (all QPainter)
# ═══════════════════════════════════════════════════════════════════════════════
class MiniParkWidget(QWidget):
    """Painted outfield with wind-direction arrows + weather overlay."""

    W, H = 260, 210

    def __init__(self, data: dict, parent=None):
        super().__init__(parent)
        self.d = data
        self.setFixedSize(self.W, self.H)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._cached_pm: QPixmap | None = None
        self._has_cycle = False

        # Animated weather icon overlay (cycles hourly conditions)
        hourly = data.get("hourly_conditions", [])
        if len(hourly) > 1:
            # Place overlay centred above the outfield
            ow, oh = 90, 68
            ox = self.CX - ow // 2
            oy = 2
            self._weather_overlay = _WeatherCycleOverlay(hourly, ow, oh, self)
            self._weather_overlay.move(ox, oy)
            self._has_cycle = True

    # ── geometry constants (computed once) ──
    CX, CY_HOME = 130, 195         # home-plate position
    FIELD_R = 120                   # outfield arc radius
    # Three arrow positions (% of radius from home, angle from vertical)
    _ARROW_ZONES = [
        (0.55, -32),   # LF zone
        (0.58,   0),   # CF zone
        (0.55,  32),   # RF zone
    ]

    def paintEvent(self, _):
        if self._cached_pm is not None:
            QPainter(self).drawPixmap(0, 0, self._cached_pm)
            return
        pm = QPixmap(self.W, self.H)
        pm.fill(QColor(0, 0, 0, 0))
        p = QPainter(pm)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        cx, cy = self.CX, self.CY_HOME
        R = self.FIELD_R

        # ── 1) outfield grass (pie slice) ──
        grass = QColor("#20462a")
        grass_light = QColor("#275e33")
        rect = QRectF(cx - R, cy - R, 2 * R, 2 * R)

        path = QPainterPath()
        path.moveTo(cx, cy)
        path.arcTo(rect, 45, 90)       # 45° start, 90° sweep (CCW in Qt)
        path.closeSubpath()

        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(grass))
        p.drawPath(path)

        # subtle outfield-wall arc
        p.setPen(QPen(grass_light, 1.5))
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawArc(rect, 45 * 16, 90 * 16)

        # ── 2) infield diamond ──
        s = 22  # half-diagonal length
        home = QPointF(cx, cy)
        first = QPointF(cx + s, cy - s)
        second = QPointF(cx, cy - 2 * s)
        third = QPointF(cx - s, cy - s)

        infield = QPainterPath()
        infield.moveTo(home)
        infield.lineTo(first)
        infield.lineTo(second)
        infield.lineTo(third)
        infield.closeSubpath()

        p.setPen(QPen(QColor("#3a3a3a"), 1))
        p.setBrush(QBrush(QColor("#2a2a1e")))
        p.drawPath(infield)

        # base squares
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(QColor("#cccccc")))
        bsz = 3
        for bp in (first, second, third):
            p.drawRect(QRectF(bp.x() - bsz, bp.y() - bsz, bsz * 2, bsz * 2))
        # home plate (pentagon → small triangle for simplicity)
        p.drawRect(QRectF(cx - bsz, cy - bsz, bsz * 2, bsz * 2))

        # ── 3) wind arrows (skip for domes) ──
        is_dome = self.d.get("roof_type", "Open") == "Dome"
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

            for frac, zone_deg in self._ARROW_ZONES:
                zrad = math.radians(zone_deg)
                dist = R * frac
                ax = cx + dist * math.sin(zrad)
                ay = cy - dist * math.cos(zrad)
                self._draw_arrow(p, ax, ay, angle, arrow_col)

        # ── 4) LF / RF labels ──
        lbl_font = QFont("Segoe UI", 9, QFont.Weight.Medium)
        p.setFont(lbl_font)
        p.setPen(QColor(C["t3"]))
        p.drawText(QRectF(8, cy - 40, 28, 14),
                    Qt.AlignmentFlag.AlignCenter, "LF")
        p.drawText(QRectF(self.W - 36, cy - 40, 28, 14),
                    Qt.AlignmentFlag.AlignCenter, "RF")

        # ── 5) weather icon (centred above outfield) ──
        # If we have the animated hourly overlay, skip the static icon
        if not self._has_cycle:
            _paint_condition(p, cx, cy - R - 2, self.d.get("condition", ""), 28)

        # ── 6) top-left: wind speed ──
        p.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        p.setPen(QColor(C["t1"]))
        if is_dome:
            p.drawText(QRectF(8, 4, 80, 16), Qt.AlignmentFlag.AlignLeft, "Dome")
        else:
            ws = self.d.get("wind_speed", 0)
            p.drawText(QRectF(8, 4, 80, 16), Qt.AlignmentFlag.AlignLeft,
                        f"{ws} mph" if ws else "Calm")

        # ── 7) top-right: temp / pressure / altitude / precip ──
        p.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        temp = self.d.get("temp")
        temp_txt = f"{temp}°F" if temp else "-- °F"
        p.setPen(QColor(C["t1"]))
        p.drawText(QRectF(self.W - 90, 4, 82, 16),
                    Qt.AlignmentFlag.AlignRight, temp_txt)

        p.setFont(QFont("Segoe UI", 8))
        p.setPen(QColor(C["t2"]))
        pressure = self.d.get("pressure_hpa")
        p_txt = f"{pressure:.0f} hPa" if pressure else "-- hPa"
        p.drawText(QRectF(self.W - 90, 20, 82, 14),
                    Qt.AlignmentFlag.AlignRight, p_txt)

        elev = self.d.get("elevation")
        e_txt = f"{elev:,} ft" if elev else "-- ft"
        p.drawText(QRectF(self.W - 90, 34, 82, 14),
                    Qt.AlignmentFlag.AlignRight, e_txt)

        precip = self.d.get("precip_pct")
        pr_txt = f"{precip:.0f}% precip" if precip is not None else "-- % precip"
        p.drawText(QRectF(self.W - 90, 48, 82, 14),
                    Qt.AlignmentFlag.AlignRight, pr_txt)

        p.end()
        self._cached_pm = pm
        QPainter(self).drawPixmap(0, 0, pm)

    # ── arrow helper ──
    @staticmethod
    def _draw_arrow(p: QPainter, cx: float, cy: float, angle_deg: float,
                    color: QColor, length: float = 32):
        """Draw an arrow at (cx, cy) pointing in *angle_deg* direction."""
        p.save()
        p.translate(cx, cy)
        p.rotate(angle_deg)

        half = length / 2
        head = 8

        # Shaft
        p.setPen(QPen(color, 2.5, Qt.PenStyle.SolidLine,
                       Qt.PenCapStyle.RoundCap))
        p.drawLine(QPointF(0, half), QPointF(0, -half + head))

        # Arrowhead
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
# Park-weather card
# ═══════════════════════════════════════════════════════════════════════════════
class ParkWeatherCard(QFrame):
    """Single game card with team header + mini-park widget + detail row."""

    def __init__(self, data: dict, parent=None):
        super().__init__(parent)
        self.d = data
        self.setStyleSheet(
            f"QFrame {{ background:{C['bg1']}; "
            f"border:1px solid {C['bdr']}; border-radius:6px; }}")
        self.setSizePolicy(QSizePolicy.Policy.Expanding,
                           QSizePolicy.Policy.Fixed)
        self._build()

    def _build(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(10, 8, 10, 10)
        vl.setSpacing(4)

        # ── header: logos + teams + time ──
        hdr = QHBoxLayout()
        hdr.setSpacing(5)
        hdr.setContentsMargins(0, 0, 0, 0)

        for abbr in (self.d["away"], self.d["home"]):
            logo_lbl = QLabel()
            logo_lbl.setFixedSize(18, 18)
            logo_lbl.setStyleSheet("border:none; padding:0; margin:0;")
            pm = self._logo(abbr)
            if pm:
                logo_lbl.setPixmap(pm)
            hdr.addWidget(logo_lbl)
            hdr.addWidget(_mk(abbr, color=C["t1"], size=12, bold=True))
            if abbr == self.d["away"]:
                hdr.addWidget(_mk("@", color=C["t3"], size=11))

        hdr.addStretch()

        # roof badge
        roof = self.d.get("roof_type", "Open")
        if roof and roof != "Open":
            badge = _mk(roof, color=C["t3"], size=9)
            badge.setStyleSheet(
                f"color:{C['t3']}; background:{C['bg3']}; "
                f"border:1px solid {C['bdr']}; border-radius:3px; "
                f"padding:1px 5px; font-size:9px; font-family:'Segoe UI';")
            hdr.addWidget(badge)

        time_lbl = _mk(self.d.get("time", "TBD"), color=C["t3"], size=11)
        hdr.addWidget(time_lbl)
        vl.addLayout(hdr)

        # ── mini park widget ──
        park = MiniParkWidget(self.d)
        pw = QHBoxLayout()
        pw.setContentsMargins(0, 0, 0, 0)
        pw.addStretch()
        pw.addWidget(park)
        pw.addStretch()
        vl.addLayout(pw)

        # ── detail row: temp + wind summary (cycles with weather overlay) ──
        hourly = self.d.get("hourly_conditions", [])
        self._detail_lbl = _mk("", color=C["t2"], size=11,
                                align=Qt.AlignmentFlag.AlignCenter)
        self._hourly = hourly
        self._detail_lbl.setText(self._detail_text(0))
        vl.addWidget(self._detail_lbl)

        # Connect to MiniParkWidget's overlay for synced cycling
        if hasattr(park, '_weather_overlay') and park._has_cycle:
            park._weather_overlay.indexChanged.connect(self._on_hour_change)

        # ── park factor badge ──
        venue_id = self.d.get("venue_id")
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
            pf_lbl = QLabel(f"<span style='color:{t3}'>Park Bias:</span> "
                            f"<span style='color:{pf_color}; font-weight:600'>{pf_tag}</span>")
            pf_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            pf_lbl.setStyleSheet(
                f"background:transparent; border:none; "
                f"font-family:'Segoe UI'; font-size:10px;")
            vl.addWidget(pf_lbl)

        # venue name
        vl.addWidget(_mk(self.d.get("venue_name", ""),
                          color=C["t3"], size=10,
                          align=Qt.AlignmentFlag.AlignCenter))

    # ── logo helper (lazy import to avoid circular dep) ──
    @staticmethod
    def _logo(abbr, size=18):
        try:
            from seam_app import get_team_pixmap
            return get_team_pixmap(abbr, size)
        except Exception:
            return None

    def _detail_text(self, idx: int) -> str:
        """Build the temp · wind summary string for hour *idx*."""
        roof = self.d.get("roof_type", "Open")
        if self._hourly and 0 <= idx < len(self._hourly):
            slot = self._hourly[idx]
            t = slot.get("temp")
            ws = slot.get("wind_speed", 0) or 0
            wd = slot.get("wind_dir", "")
        else:
            t = self.d.get("temp")
            ws = self.d.get("wind_speed", 0) or 0
            wd = self.d.get("wind_dir", "Calm")
        parts = []
        if t is not None:
            parts.append(f"{int(t)}°")
        if roof == "Dome":
            parts.append("Dome")
        else:
            parts.append(f"{ws}mph {wd}" if ws else "Calm")
        return "  ·  ".join(parts)

    def _on_hour_change(self, idx: int):
        self._detail_lbl.setText(self._detail_text(idx))


# ═══════════════════════════════════════════════════════════════════════════════
# Module-level weather cache  (prefetched at app startup)
# ═══════════════════════════════════════════════════════════════════════════════
_weather_cache: dict[str, list[dict]] = {}   # date_str → game list
_weather_lock = threading.Lock()

_WEATHER_CACHE_DIR = _app_paths.WEATHER_CACHE_DIR
os.makedirs(_WEATHER_CACHE_DIR, exist_ok=True)


def _weather_cache_path(date_str: str) -> str:
    """Return the disk-cache file path for a given date."""
    return os.path.join(_WEATHER_CACHE_DIR, f"{date_str}.json")


def _load_weather_from_disk(date_str: str) -> list[dict] | None:
    """Load weather data from the disk cache, or None if absent/corrupt."""
    path = _weather_cache_path(date_str)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return None


def _save_weather_to_disk(date_str: str, data: list[dict]):
    """Persist weather data to disk cache."""
    try:
        path = _weather_cache_path(date_str)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
    except Exception:
        pass


def _cleanup_old_weather_cache(keep_days: int = 3):
    """Remove disk cache files older than *keep_days*."""
    import datetime as _dt
    cutoff = _dt.date.today() - _dt.timedelta(days=keep_days)
    try:
        for fname in os.listdir(_WEATHER_CACHE_DIR):
            if not fname.endswith(".json"):
                continue
            date_part = fname.removesuffix(".json")
            try:
                fdate = _dt.date.fromisoformat(date_part)
            except ValueError:
                continue
            if fdate < cutoff:
                os.remove(os.path.join(_WEATHER_CACHE_DIR, fname))
    except Exception:
        pass


def prefetch_weather(date_str: str | None = None):
    """Seed the in-memory cache from disk, then always re-fetch fresh data.

    Safe to call from any thread.  Designed to be kicked off at app startup
    exactly like the lineup prefetch (daemon thread, fire-and-forget).
    """
    import datetime as _dt
    date_str = date_str or _dt.date.today().isoformat()

    # Delete today's stale disk cache so we always get fresh weather on launch
    try:
        stale = _weather_cache_path(date_str)
        if os.path.isfile(stale):
            os.remove(stale)
    except Exception:
        pass
    with _weather_lock:
        _weather_cache.pop(date_str, None)

    # Fetch fresh data and update cache + disk
    data = fetch_park_weather(date_str)
    with _weather_lock:
        _weather_cache[date_str] = data
    _save_weather_to_disk(date_str, data)

    # Housekeeping — remove stale cache files
    _cleanup_old_weather_cache()


def get_cached_weather(date_str: str) -> list[dict] | None:
    """Return cached data for *date_str*, or None if not yet available."""
    with _weather_lock:
        hit = _weather_cache.get(date_str)
    if hit is not None:
        return hit
    # Check disk as fallback
    disk = _load_weather_from_disk(date_str)
    if disk is not None:
        with _weather_lock:
            _weather_cache[date_str] = disk
        return disk
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Background data loader  (QThread fallback when cache miss)
# ═══════════════════════════════════════════════════════════════════════════════
class _WeatherWorker(QThread):
    finished = pyqtSignal(list)

    def __init__(self, date_str: str):
        super().__init__()
        self._date = date_str

    def run(self):
        try:
            data = fetch_park_weather(self._date)
        except Exception:
            data = []
        with _weather_lock:
            _weather_cache[self._date] = data
        _save_weather_to_disk(self._date, data)
        self.finished.emit(data)


# ═══════════════════════════════════════════════════════════════════════════════
# Filter bar (local copy to stay self-contained)
# ═══════════════════════════════════════════════════════════════════════════════
class _FilterBar(QWidget):
    changed = pyqtSignal(str)

    def __init__(self, options, parent=None):
        super().__init__(parent)
        self._btns: list[QPushButton] = []
        hl = QHBoxLayout(self)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.setSpacing(6)
        for i, opt in enumerate(options):
            btn = QPushButton(opt)
            btn.setCheckable(True)
            btn.setChecked(i == 0)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda _, b=btn: self._activate(b))
            self._style(btn, i == 0)
            hl.addWidget(btn)
            self._btns.append(btn)
        hl.addStretch()

    def _style(self, btn, active):
        bg = C["bg3"] if active else "transparent"
        col = C["t1"] if active else C["t3"]
        btn.setStyleSheet(
            f"QPushButton {{ background:{bg}; color:{col}; "
            f"border:1px solid {C['bdrl']}; border-radius:3px; "
            f"padding:4px 10px; font-family:'Segoe UI'; "
            f"font-size:11px; letter-spacing:1px; }}"
            f"QPushButton:hover {{ background:{C['bg3']}; color:{C['t1']}; }}")

    def _activate(self, ab):
        for b in self._btns:
            b.setChecked(b is ab)
            self._style(b, b is ab)
        self.changed.emit(ab.text())


# ═══════════════════════════════════════════════════════════════════════════════
# Smooth scroll area (local copy)
# ═══════════════════════════════════════════════════════════════════════════════
class _SmoothScroll(QScrollArea):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._target = 0
        self._anim = None

    def wheelEvent(self, ev):
        bar = self.verticalScrollBar()
        if not self._anim:
            from PyQt6.QtCore import QPropertyAnimation, QEasingCurve
            self._anim = QPropertyAnimation(bar, b"value", self)
            self._anim.setEasingCurve(QEasingCurve.Type.OutCubic)
            self._anim.setDuration(260)
            self._target = bar.value()
        delta = -ev.angleDelta().y()
        self._target = max(bar.minimum(),
                           min(bar.maximum(), self._target + delta))
        self._anim.stop()
        self._anim.setStartValue(bar.value())
        self._anim.setEndValue(self._target)
        self._anim.start()
        ev.accept()


# ═══════════════════════════════════════════════════════════════════════════════
# Page widget  (public entry-point)
# ═══════════════════════════════════════════════════════════════════════════════
class ParkFactorsPage(QWidget):
    """Full page widget — fetches data in background, builds card grid."""

    def __init__(self, date_str: str | None = None, parent=None):
        super().__init__(parent)
        import datetime as _dt
        self._date = date_str or _dt.date.today().isoformat()
        self._all_data: list[dict] = []
        self._cards: list[ParkWeatherCard] = []
        self._worker = None
        self._ncols = 4
        self._grid = None
        self.setStyleSheet(f"background:{C['bg0']};")

        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(0, 0, 0, 0)
        self._root.setSpacing(0)

        self._sa = None  # scroll area ref for background refresh
        self._build_header()

        # Show cached data instantly; always re-fetch in background
        cached = get_cached_weather(self._date)
        if cached is not None:
            self._on_data(cached)
        else:
            self._build_loading()

        # Background refresh — silently updates UI if data changed
        self._worker = _WeatherWorker(self._date)
        self._worker.finished.connect(self._on_refresh)
        self._worker.start()

    # ── title + filter section ──
    def _build_header(self):
        wrap = QWidget()
        wrap.setStyleSheet(f"background:{C['bg0']};")
        vl = QVBoxLayout(wrap)
        vl.setContentsMargins(24, 20, 24, 0)
        vl.setSpacing(0)

        title_row = QHBoxLayout()
        title_row.setSpacing(10)
        title_row.addWidget(_mk("Park Factors", color=C["t1"], size=22, bold=True))
        self._count_lbl = _mk("", color=C["t2"], size=13)
        title_row.addWidget(self._count_lbl)
        title_row.addStretch()
        vl.addLayout(title_row)

        try:
            friendly = datetime.strptime(self._date, "%Y-%m-%d").strftime("%a, %b %d %Y")
        except Exception:
            friendly = self._date
        sub = _mk(friendly, color=C["t2"], size=13)
        sub.setContentsMargins(0, 3, 0, 12)
        vl.addWidget(sub)

        self._filter = _FilterBar(["ALL", "OPEN AIR", "DOME", "RETRACTABLE"])
        self._filter.changed.connect(self._on_roof_filter)
        vl.addWidget(self._filter)
        vl.addSpacing(6)

        self._bias_filter = _FilterBar(["ALL", "HITTER FRIENDLY", "NEUTRAL", "PITCHER FRIENDLY"])
        self._bias_filter.changed.connect(self._on_bias_filter)
        vl.addWidget(self._bias_filter)
        vl.addSpacing(12)

        self._roof_label = "ALL"
        self._bias_label = "ALL"

        self._root.addWidget(wrap)

        # ── carry-effect legend row ──
        legend_wrap = QWidget()
        legend_wrap.setStyleSheet(
            f"background:{C['bg1']}; border-bottom:1px solid {C['bdr']};")
        lw_lay = QHBoxLayout(legend_wrap)
        lw_lay.setContentsMargins(24, 8, 24, 12)
        self._carry_legend = _CarryLegend()
        lw_lay.addWidget(self._carry_legend)
        self._root.addWidget(legend_wrap)

    # ── loading placeholder ──
    def _build_loading(self):
        self._loading = _mk("Loading weather data …", color=C["t3"], size=14,
                             align=Qt.AlignmentFlag.AlignCenter)
        self._loading.setMinimumHeight(200)
        self._root.addWidget(self._loading, 1)

    # ── data arrived callback ──
    def _on_data(self, data: list[dict]):
        self._all_data = data
        self._count_lbl.setText(f"{len(data)} games")

        # Remove loading label if it exists
        if hasattr(self, '_loading') and self._loading:
            self._loading.setParent(None)
            self._loading.deleteLater()
            self._loading = None

        # Remove previous scroll area (background refresh path)
        if self._sa is not None:
            self._sa.setParent(None)
            self._sa.deleteLater()
            self._sa = None

        # Scroll area with card grid
        sa = _SmoothScroll()
        sa.setWidgetResizable(True)
        sa.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        sa.setStyleSheet(_sa_style())

        cw = QWidget()
        cw.setStyleSheet(f"background:{C['bg0']};")
        self._grid = QGridLayout(cw)
        self._grid.setContentsMargins(24, 14, 24, 24)
        self._grid.setSpacing(14)
        self._ncols = 4
        for col in range(self._ncols):
            self._grid.setColumnStretch(col, 1)

        self._populate(data)

        sa.setWidget(cw)
        self._sa = sa
        self._root.addWidget(sa, 1)

    def _on_refresh(self, data: list[dict]):
        """Background fetch finished — update UI only if data changed."""
        if not data:
            return
        if data == self._all_data:
            return  # identical — nothing to update
        self._on_data(data)

    # ── populate grid with cards ──
    def _populate(self, games: list[dict]):
        # Clear existing
        for c in self._cards:
            c.setParent(None)
            c.deleteLater()
        self._cards.clear()

        for i, g in enumerate(games):
            card = ParkWeatherCard(g)
            self._cards.append(card)
            row, col = divmod(i, self._ncols)
            self._grid.addWidget(card, row, col)

    def _reflow_grid(self):
        """Reposition existing cards into the current column count."""
        for i, card in enumerate(self._cards):
            row, col = divmod(i, self._ncols)
            self._grid.addWidget(card, row, col)
        # update column stretches
        for c in range(self._grid.columnCount()):
            self._grid.setColumnStretch(c, 1 if c < self._ncols else 0)

    def resizeEvent(self, ev):
        super().resizeEvent(ev)
        w = self.width()
        if w < 900:
            new_cols = 2
        elif w < 1200:
            new_cols = 3
        else:
            new_cols = 4
        if new_cols != self._ncols and self._cards:
            self._ncols = new_cols
            self._reflow_grid()

    # ── filter handlers ──
    def _on_roof_filter(self, label: str):
        self._roof_label = label
        self._apply_filters()

    def _on_bias_filter(self, label: str):
        self._bias_label = label
        self._apply_filters()

    def _apply_filters(self):
        if not self._all_data:
            return
        filtered = self._all_data

        # Roof type filter
        if self._roof_label == "OPEN AIR":
            filtered = [g for g in filtered
                        if g.get("roof_type", "Open") == "Open"]
        elif self._roof_label == "DOME":
            filtered = [g for g in filtered
                        if g.get("roof_type", "Open") == "Dome"]
        elif self._roof_label == "RETRACTABLE":
            filtered = [g for g in filtered
                        if g.get("roof_type", "Open") == "Retractable"]

        # Park bias filter
        if self._bias_label == "HITTER FRIENDLY":
            filtered = [g for g in filtered
                        if VENUE_PARK_FACTORS.get(g.get("venue_id"), 100) >= 102]
        elif self._bias_label == "NEUTRAL":
            filtered = [g for g in filtered
                        if 99 <= VENUE_PARK_FACTORS.get(g.get("venue_id"), 100) <= 101]
        elif self._bias_label == "PITCHER FRIENDLY":
            filtered = [g for g in filtered
                        if VENUE_PARK_FACTORS.get(g.get("venue_id"), 100) <= 98]

        self._populate(filtered)
