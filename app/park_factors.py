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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from _http_utils import create_http_session

_http = create_http_session(total_retries=2, backoff_factor=0.3)

# Separate fast session for Open-Meteo (no retries — fail fast if down)
_http_om = create_http_session(total_retries=0, backoff_factor=0)

from PyQt6.QtWidgets import (
    QWidget, QFrame, QVBoxLayout, QHBoxLayout, QLabel,
    QScrollArea, QGridLayout, QPushButton, QSizePolicy, QToolTip,
)
from PyQt6.QtCore import Qt, QRectF, QPointF, QThread, pyqtSignal, QTimer, QByteArray, QSize
from PyQt6.QtGui import (
    QColor, QFont, QPainter, QPainterPath, QPen, QBrush, QPixmap, QImage, QIcon,
)

import _app_paths

# ═══════════════════════════════════════════════════════════════════════════════
# Design tokens  (shared palette — single source of truth)
# ═══════════════════════════════════════════════════════════════════════════════
from _app_theme import C
from _ui_utils import mk_label as _mk
from park_widget import WeatherDetailWidget, VENUE_PARK_FACTORS


# Abbreviation normalization (MLB API inconsistencies)
_ABBR_NORM = {"AZ": "ARI"}


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


def _compass_to_mlb_wind(deg: float, azimuth: float = 0) -> str:
    """Convert meteorological wind direction (degrees) to MLB-style label.

    Meteorological convention: degrees = direction wind blows FROM.
    MLB convention: labels describe where the wind GOES (e.g. "Out To CF").
    So we flip by 180° to get the "toward" direction, then subtract the
    stadium's *azimuth* (compass bearing from home plate to centre field)
    to convert from compass-relative to field-relative.
    """
    toward = (deg + 180) % 360
    field_rel = (toward - azimuth) % 360
    best = min(_COMPASS_DIRS, key=lambda d: min(abs(d[0] - field_rel),
                                                  360 - abs(d[0] - field_rel)))
    return best[1]


# ── Circuit breaker config ───────────────────────────────────────────────────
_CB_THRESHOLD = 3  # consecutive failures before tripping

# ── Weather cache TTL ────────────────────────────────────────────────────────
# Disk cache older than this will be re-fetched even for non-today dates.
# Keeps tomorrow's weather forecast from going stale overnight.
_WEATHER_CACHE_TTL_SECS = 60 * 60   # 1 hour

# ── Static venue coordinate overrides ───────────────────────────────────────
# Used when the MLB Stats API returns no lat/lon for a venue.
# Format: venue_id → (lat, lon, elev_ft, azimuth_deg)
#   azimuth = compass bearing from home plate toward CF (0=N, 90=E, etc.)
_VENUE_COORD_OVERRIDE: dict[int, tuple[float, float, float, float]] = {
    # Estadio Alfredo Harp Helú — Mexico City (MLB API returns None for all coords)
    # Coords: 19.3618 N, 99.1567 W  |  elev 7349 ft (2240 m)  |  azimuth ~45° (CF faces NE)
    5340: (19.3618, -99.1567, 7349.0, 45.0),
}

# ── NWS (National Weather Service) — primary provider ───────────────────────
_nws_fail_count = 0   # consecutive failures; >= _CB_THRESHOLD → skip calls
_nws_grid_cache: dict[str, dict] = {}   # "(lat,lon)" → {"hourly_url": ..., "stations_url": ...}
_nws_grid_lock = threading.Lock()
_nws_grid_pending: dict[str, threading.Event] = {}  # per-key fetch-in-progress events
_nws_grid_seed_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="seam-nws")

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
        # If another thread is already fetching this key, wait for it
        if key in _nws_grid_pending:
            evt = _nws_grid_pending[key]
            wait = True
        else:
            evt = threading.Event()
            _nws_grid_pending[key] = evt
            wait = False  # we are the fetcher
    if wait:
        evt.wait(timeout=10)
        with _nws_grid_lock:
            return _nws_grid_cache.get(key, {})
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
            evt.set()
            _nws_grid_pending.pop(key, None)
        return info
    except Exception:
        with _nws_grid_lock:
            evt.set()
            _nws_grid_pending.pop(key, None)
        return {}


def _approx_sunset_hour(lat: float, lon: float, date_obj) -> float:
    """Approximate sunset as UTC decimal hour using Spencer (1971) formula.
    Accurate to ±5 min for MLB venue latitudes (25–48 °N), April–September.
    Callers convert to local time by adding the venue UTC offset in hours.
    """
    doy = date_obj.timetuple().tm_yday
    B = 2 * math.pi * (doy - 81) / 364
    decl = math.radians(23.45 * math.sin(B))
    lat_r = math.radians(lat)
    cos_ha = (
        -math.sin(math.radians(-0.83))   # atmospheric refraction + solar disk
        - math.sin(lat_r) * math.sin(decl)
    ) / (math.cos(lat_r) * math.cos(decl))
    cos_ha = max(-1.0, min(1.0, cos_ha))
    ha_deg = math.degrees(math.acos(cos_ha))
    eot = 9.87 * math.sin(2 * B) - 7.53 * math.cos(B) - 1.5 * math.sin(B)
    sunset_utc_min = 720 - 4 * lon - eot + 4 * ha_deg
    return (sunset_utc_min / 60.0) % 24


def _fetch_nws(lat, lon, game_utc_iso: str | None = None, azimuth: float = 0):
    """Fetch forecast data from the National Weather Service (api.weather.gov).

    Returns the same dict shape as _fetch_open_meteo / _fetch_weatherapi.
    NWS hourly gives temp, precip %, wind, and condition.
    Pressure is NOT included — use _fetch_pressure_weatherapi() separately.
    """
    global _nws_fail_count
    if _nws_fail_count >= _CB_THRESHOLD:
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

        result: dict = {"precip_pct": None, "pressure_hpa": None, "humidity_pct": None}

        # Current-hour precip: use the first period (closest to now)
        first = periods[0]
        result["precip_pct"] = (
            first.get("probabilityOfPrecipitation", {}).get("value") or 0)
        result["humidity_pct"] = (
            first.get("relativeHumidity", {}).get("value"))

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
                for period in periods:
                    pdt = _period_dt(period)
                    diff = (pdt - game_local).total_seconds()
                    if -1800 <= diff < 4 * 3600:  # 30 min before → 4h after
                        window.append(period)
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

                # Humidity from first-pitch hour
                result["forecast_humidity"] = (
                    first_pitch.get("relativeHumidity", {}).get("value"))

                # Precip: max across game window
                window_precip = [
                    period.get("probabilityOfPrecipitation", {}).get("value") or 0
                    for period in window
                ]
                result["forecast_precip"] = max(window_precip) if window_precip else None

                # Condition: worst weather in the window
                worst_cond = max(
                    (period.get("shortForecast", "") for period in window),
                    key=_nws_cond_score,
                )
                result["forecast_condition"] = worst_cond

                # Compute actual sunset for accurate day/night determination
                _w0 = _period_dt(window[0])
                _nws_tz_ofs = (
                    _w0.utcoffset().total_seconds() / 3600
                    if _w0.utcoffset() else -5)
                _nws_sunset_utc_h = _approx_sunset_hour(lat, lon, _w0.date())
                _nws_sunset_local_hr = (_nws_sunset_utc_h + _nws_tz_ofs) % 24
                result["sunset_hour"] = round(_nws_sunset_local_hr, 2)

                # Per-hour conditions for animated icon cycling
                hourly_conds = []
                for period in window:
                    pdt = _period_dt(period)
                    h_local = pdt.astimezone()
                    h_lbl = h_local.strftime("%I %p").lstrip("0")
                    cond_txt = period.get("shortForecast", "")
                    pr = period.get("probabilityOfPrecipitation", {}).get("value") or 0
                    # Parse wind speed from NWS string like "5 mph" or "5 to 10 mph"
                    h_ws_str = period.get("windSpeed", "0")
                    try:
                        h_ws = int(h_ws_str.replace(" mph", "").split(" to ")[-1])
                    except Exception:
                        h_ws = 0
                    h_wd_compass = period.get("windDirection", "")
                    h_wd_deg = _NWS_COMPASS.get(h_wd_compass)
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg, azimuth) if h_wd_deg is not None else ""
                    h_rh = period.get("relativeHumidity", {}).get("value")
                    _h_dec = pdt.hour + pdt.minute / 60.0
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "humidity": h_rh,
                        "night": _h_dec > _nws_sunset_local_hr + 0.75 or pdt.hour < 6,
                        "temp": period.get("temperature"),
                        "wind_speed": h_ws,
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        _nws_fail_count = 0  # success → reset consecutive failure count
        return result
    except Exception:
        _nws_fail_count += 1
        return {}


_om_fail_count = 0   # consecutive failures; >= _CB_THRESHOLD → skip calls


def _pressure_from_elevation(elevation_ft) -> float:
    """Estimate surface pressure from venue elevation using the barometric formula.

    Uses the international barometric formula:
        P = 1013.25 × (1 − 2.25577×10⁻⁵ × h)^5.25588
    where h is elevation in metres.  Returns hPa rounded to 1 decimal.
    """
    if elevation_ft is None:
        return 1013.3                       # sea-level default
    h_m = float(elevation_ft) * 0.3048
    return round(1013.25 * (1 - 2.25577e-5 * h_m) ** 5.25588, 1)


def _fetch_pressure_open_meteo(lat, lon) -> float | None:
    """Lightweight Open-Meteo query for current surface pressure only."""
    try:
        r = _http_om.get(
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&current=surface_pressure",
            timeout=6,
        )
        r.raise_for_status()
        sp = r.json().get("current", {}).get("surface_pressure")
        if sp:
            return round(sp, 1)
    except Exception:
        pass
    return None


def _fetch_pressure_batch(games: list[dict]) -> None:
    """Fetch surface pressure for multiple games in a single Open-Meteo call.

    Open-Meteo supports comma-separated lat/lon for multi-location queries,
    returning all results in one request (~0.5–1 s total).
    Falls back to elevation-based estimation when API is unavailable.
    """
    need = [g for g in games
            if g.get("pressure_hpa") is None and g.get("lat") and g.get("lon")]
    if not need:
        # Still fill any games missing pressure via elevation
        for g in games:
            if g.get("pressure_hpa") is None:
                g["pressure_hpa"] = _pressure_from_elevation(g.get("elevation"))
        return
    lats = ",".join(str(g["lat"]) for g in need)
    lons = ",".join(str(g["lon"]) for g in need)

    # Try batch call with one retry on 429 (rate-limit)
    for attempt in range(2):
        try:
            r = _http_om.get(
                f"https://api.open-meteo.com/v1/forecast?"
                f"latitude={lats}&longitude={lons}&current=surface_pressure",
                timeout=8,
            )
            if r.status_code == 429 and attempt == 0:
                import time; time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                data = [data]
            for g, d in zip(need, data):
                sp = d.get("current", {}).get("surface_pressure")
                if sp:
                    g["pressure_hpa"] = round(sp, 1)
            break
        except Exception:
            if attempt == 0:
                import time; time.sleep(1)
                continue
            break

    # Elevation-based fallback for any games still missing pressure
    for g in games:
        if g.get("pressure_hpa") is None:
            g["pressure_hpa"] = _pressure_from_elevation(g.get("elevation"))


def _fetch_open_meteo(lat, lon, game_utc_iso: str | None = None, azimuth: float = 0):
    """Fetch forecast data from Open-Meteo.

    Always returns precip_pct and pressure_hpa for the current moment.
    When *game_utc_iso* is given, also returns hourly forecast fields
    (forecast_temp, forecast_wind_speed, forecast_wind_deg,
    forecast_condition) for the hour closest to game time.
    """
    global _om_fail_count
    if _om_fail_count >= _CB_THRESHOLD:
        return {}
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&current=precipitation_probability,surface_pressure,relative_humidity_2m"
            f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,"
            f"precipitation_probability,surface_pressure,weather_code,relative_humidity_2m"
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
            "humidity_pct": cur.get("relative_humidity_2m"),
        }

        # Pick hourly slots covering the game window (start → +3 h)
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        if game_utc_iso and times:
            try:
                game_dt = datetime.fromisoformat(
                    game_utc_iso.replace("Z", "+00:00"))
                game_local = game_dt.astimezone()
                # Venue timezone from Open-Meteo response
                _ofs = data.get("utc_offset_seconds", 0) or 0
                _venue_tz = timezone(timedelta(seconds=_ofs))
                game_venue = game_dt.astimezone(_venue_tz)
                # Compute actual sunset for accurate day/night determination
                _om_sunset_utc_h = _approx_sunset_hour(lat, lon, game_dt.date())
                _om_sunset_utc_dt = (
                    datetime(game_dt.year, game_dt.month, game_dt.day,
                             0, 0, tzinfo=timezone.utc)
                    + timedelta(hours=_om_sunset_utc_h))
                _om_sl = _om_sunset_utc_dt.astimezone(_venue_tz)
                _om_sunset_local_hr = _om_sl.hour + _om_sl.minute / 60.0
                result["sunset_hour"] = round(_om_sunset_local_hr, 2)
                game_hour = game_venue.strftime("%Y-%m-%dT%H:00")
                # Find start index
                if game_hour in times:
                    idx = times.index(game_hour)
                else:
                    idx = min(range(len(times)),
                              key=lambda i: abs(
                                  datetime.fromisoformat(times[i]).hour
                                  - game_venue.hour
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
                _rh_vals = hourly.get("relative_humidity_2m", [])
                result["forecast_humidity"] = (
                    _rh_vals[idx] if idx < len(_rh_vals) else None)

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
                        # Display in user's local timezone
                        h_dt_aware = h_dt.replace(tzinfo=_venue_tz)
                        h_local = h_dt_aware.astimezone()
                        h_lbl = h_local.strftime("%I %p").lstrip("0")
                        _h_dec = h_dt_aware.hour + h_dt_aware.minute / 60.0
                        is_night = _h_dec > _om_sunset_local_hr + 0.75 or h_dt_aware.hour < 6
                    except Exception:
                        h_lbl = ""
                        is_night = False
                    wc = wcodes[gi] if gi < len(wcodes) else None
                    cond_txt = _WMO_CONDITIONS.get(wc, "Clear") if wc is not None else ""
                    pr = precip_vals[gi] if gi < len(precip_vals) else None
                    h_temp = temps[gi] if gi < len(temps) else None
                    h_ws = wspeeds[gi] if gi < len(wspeeds) else None
                    h_wd_deg = wdirs[gi] if gi < len(wdirs) else None
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg, azimuth) if h_wd_deg is not None else ""
                    _rh_arr = hourly.get("relative_humidity_2m", [])
                    h_rh = _rh_arr[gi] if gi < len(_rh_arr) else None
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "humidity": h_rh,
                        "night": is_night,
                        "temp": round(h_temp) if h_temp is not None else None,
                        "wind_speed": round(h_ws) if h_ws is not None else None,
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        _om_fail_count = 0  # success → reset consecutive failure count
        return result
    except Exception:
        _om_fail_count += 1
        return {}


# ── WeatherAPI.com fallback ──────────────────────────────────────────────────
_weatherapi_key: str | None = None
_weatherapi_key_loaded = False
_wa_fail_count = 0  # consecutive failures; >= _CB_THRESHOLD → skip calls


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


def _fetch_weatherapi(lat, lon, game_utc_iso: str | None = None, azimuth: float = 0,
                      elev_ft: float = 0.0):
    """Fallback: fetch forecast data from WeatherAPI.com.

    Returns the same dict shape as _fetch_open_meteo so the caller
    doesn't need to know which provider answered.

    WeatherAPI always reports pressure in mb as mean-sea-level (MSL) pressure.
    When *elev_ft* is provided the value is converted to actual surface
    pressure using the standard atmosphere formula so it matches Open-Meteo's
    ``surface_pressure`` field and is physically correct for elevated parks.
    """
    global _wa_fail_count
    if _wa_fail_count >= _CB_THRESHOLD:
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
        # WeatherAPI pressure_mb is MSL-adjusted; convert to actual surface
        # pressure when elevation is known (standard atmosphere formula).
        _h_m = float(elev_ft) * 0.3048
        _surf_factor = (1.0 - 2.25577e-5 * _h_m) ** 5.25588 if _h_m > 0 else 1.0
        _msl_now = cur.get("pressure_mb", 0) or 0
        result = {
            "precip_pct": None,
            "pressure_hpa": round(_msl_now * _surf_factor, 1),
            "humidity_pct": cur.get("humidity"),
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
        for hour in all_hours:
            if hour.get("time", "")[:13] == now_str[:13]:
                cur_hour = hour
                break
        if cur_hour:
            result["precip_pct"] = cur_hour.get("chance_of_rain", 0) or 0

        # Hourly forecast across the game window (start → +3 h)
        if game_utc_iso and all_hours:
            try:
                game_dt = datetime.fromisoformat(
                    game_utc_iso.replace("Z", "+00:00"))
                game_local = game_dt.astimezone()
                # Compute venue UTC offset from WeatherAPI location data
                _wa_loc = data.get("location", {})
                _wa_epoch = _wa_loc.get("localtime_epoch")
                _wa_lstr = _wa_loc.get("localtime", "")
                if _wa_epoch and _wa_lstr:
                    _wa_utc_naive = datetime.utcfromtimestamp(_wa_epoch)
                    _wa_loc_naive = datetime.fromisoformat(_wa_lstr)
                    _wa_ofs = round((_wa_loc_naive - _wa_utc_naive).total_seconds())
                    _wa_venue_tz = timezone(timedelta(seconds=_wa_ofs))
                else:
                    _wa_venue_tz = game_local.tzinfo  # fallback: assume same tz
                game_venue = game_dt.astimezone(_wa_venue_tz)
                game_hour_str = game_venue.strftime("%Y-%m-%d %H:00")
                # Compute actual sunset for accurate day/night determination
                _wa_sunset_utc_h = _approx_sunset_hour(lat, lon, game_dt.date())
                _wa_sunset_utc_dt = (
                    datetime(game_dt.year, game_dt.month, game_dt.day,
                             0, 0, tzinfo=timezone.utc)
                    + timedelta(hours=_wa_sunset_utc_h))
                _wa_sl = _wa_sunset_utc_dt.astimezone(_wa_venue_tz)
                _wa_sunset_local_hr = _wa_sl.hour + _wa_sl.minute / 60.0
                result["sunset_hour"] = round(_wa_sunset_local_hr, 2)

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
                                all_hours[i]["time"]).hour - game_venue.hour
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
                _msl_fc = match.get("pressure_mb")
                result["forecast_pressure"] = (
                    round(_msl_fc * _surf_factor, 1) if _msl_fc is not None else None)
                result["forecast_humidity"] = match.get("humidity")

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
                    h_local_hour = 12  # fallback: treat as daytime
                    h_dt_aware = None
                    try:
                        h_dt = datetime.fromisoformat(t_str)
                        # Display in user's local timezone
                        h_dt_aware = h_dt.replace(tzinfo=_wa_venue_tz)
                        h_local = h_dt_aware.astimezone()
                        h_lbl = h_local.strftime("%I %p").lstrip("0")
                        h_local_hour = h_local.hour
                    except Exception:
                        h_lbl = ""
                    cond_txt = wh.get("condition", {}).get("text", "")
                    pr = wh.get("chance_of_rain")
                    h_wd_deg = wh.get("wind_degree")
                    h_wd_mlb = _compass_to_mlb_wind(h_wd_deg, azimuth) if h_wd_deg is not None else ""
                    _wa_h_dec = (h_dt_aware.hour + h_dt_aware.minute / 60.0
                                 if h_dt_aware is not None else float(h_local_hour))
                    hourly_conds.append({
                        "hour": h_lbl, "condition": cond_txt,
                        "precip": pr,
                        "humidity": wh.get("humidity"),
                        "night": _wa_h_dec > _wa_sunset_local_hr + 0.75 or h_local_hour < 6,
                        "temp": wh.get("temp_f"),
                        "wind_speed": wh.get("wind_mph"),
                        "wind_dir": h_wd_mlb,
                    })
                result["hourly_conditions"] = hourly_conds
            except Exception:
                pass

        _wa_fail_count = 0  # success → reset consecutive failure count
        return result
    except Exception:
        _wa_fail_count += 1
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# Retractable roof prediction (pregame, before MLB provides official status)
# ═══════════════════════════════════════════════════════════════════════════════
# Per-venue thresholds for "open roof" conditions (from MLB operational data):
#   MIL (32):  Temp ≥ 60–63 °F
#   ARI (15):  Temp < 100 °F
#   TEX (5325): Temp 65–80 °F, Humidity < 50 %
#   MIA (4169): Heat Index < 83, Humidity < 70 %, Rain < 15 %
#   HOU (2392): Temp 65–77 °F, Humidity < 50 %, Dew Point < 55 °F
#   TOR (14):  Fan comfort & field safety (no field drainage)
#   SEA (680): "Open as much as possible" (~78 % of games)
# General: high winds ≥ 25 mph close the roof at all venues.
# Home team decides pregame; roof can only move once mid-game.

def _heat_index(temp_f: float, rh: float) -> float:
    """Rothfusz regression heat index (°F)."""
    if temp_f < 80:
        return temp_f
    return (-42.379 + 2.04901523 * temp_f + 10.14333127 * rh
            - 0.22475541 * temp_f * rh - 6.83783e-3 * temp_f ** 2
            - 5.481717e-2 * rh ** 2 + 1.22874e-3 * temp_f ** 2 * rh
            + 8.5282e-4 * temp_f * rh ** 2 - 1.99e-6 * temp_f ** 2 * rh ** 2)

def _dew_point(temp_f: float, rh: float) -> float:
    """Approximate dew point (°F) via Magnus formula."""
    if rh <= 0:
        return temp_f - 40
    tc = (temp_f - 32) * 5.0 / 9.0
    a, b = 17.27, 237.7
    gamma = (math.log(rh / 100.0) + a * tc / (b + tc))
    dp_c = b * gamma / (a - gamma)
    return dp_c * 9.0 / 5.0 + 32

def _predict_retractable_roof(game: dict) -> str:
    """Return predicted roof_type string for a retractable venue.

    Uses forecast temperature, precipitation, humidity, and wind to
    estimate whether the roof will be open or closed.  Returns a label
    with a trailing asterisk to indicate the status is predicted.
    """
    vid = game.get("venue_id")
    try:
        temp = float(game.get("temp") or 0)
    except (ValueError, TypeError):
        temp = 0
    precip = game.get("precip_pct") or 0
    humid = game.get("humidity_pct") or 0
    wind = game.get("wind_speed") or 0

    high_wind = wind >= 25

    closed = False
    if vid == 32:       # MIL — American Family Field
        # Open when: Temp ≥ 60 °F
        closed = temp < 60 or precip >= 15 or high_wind
    elif vid == 2392:   # HOU — Minute Maid / Daikin Park
        # Open when: Temp 65–77 °F, Humidity < 50 %, Dew Point < 55 °F
        closed = (temp < 65 or temp > 77 or humid >= 50
                  or _dew_point(temp, humid) >= 55
                  or precip >= 10 or high_wind)
    elif vid == 680:    # SEA — T-Mobile Park
        # "Open as much as possible" — ~78 % of games open
        closed = precip >= 50 or wind >= 30
    elif vid == 15:     # ARI — Chase Field
        # Open when: Temp < 100 °F
        closed = temp >= 100 or precip >= 20 or high_wind
    elif vid == 5325:   # TEX — Globe Life Field
        # Open when: Temp 65–80 °F, Humidity < 50 %
        closed = (temp < 65 or temp > 80 or humid >= 50
                  or precip >= 10 or high_wind)
    elif vid == 14:     # TOR — Rogers Centre
        # Fan comfort + field safety; no field drainage → low rain threshold
        closed = temp < 60 or precip >= 15 or high_wind
    elif vid == 4169:   # MIA — loanDepot park
        # Night games and overcast conditions are more comfortable —
        # Marlins tolerate higher humidity/heat index without the roof.
        _cond = (game.get("condition") or "").lower()
        _hourly = game.get("hourly_conditions") or []
        _night = any(h.get("night") for h in _hourly[:2]) if _hourly else False
        _overcast = any(w in _cond for w in ("cloud", "overcast", "partly"))
        if _night or _overcast:
            # Relaxed: Heat Index < 90, Humidity < 82 %
            closed = (_heat_index(temp, humid) >= 90 or humid >= 82
                      or precip >= 15 or high_wind)
        else:
            # Daytime sun: Heat Index < 83, Humidity < 70 %
            closed = (_heat_index(temp, humid) >= 83 or humid >= 70
                      or precip >= 15 or high_wind)

    tag = "CLOSED" if closed else "OPEN"
    return f"Retractable ({tag})*"


def fetch_park_weather(date_str: str) -> list[dict]:
    """Fetch weather + venue info for every game on *date_str* (YYYY-MM-DD).

    Returns a list of dicts (one per game) sorted by game time.
    Each dict contains: game_id, away, home, time, status, venue_name,
    venue_id, lat, lon, elevation, roof_type, temp, condition,
    wind_speed, wind_dir, wind_angle, precip_pct, pressure_hpa.
    """
    global _nws_fail_count, _wa_fail_count, _om_fail_count
    _nws_fail_count = 0   # reset circuit breakers for each fetch cycle
    _wa_fail_count = 0
    _om_fail_count = 0
    try:
        url = (
            "https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&date={date_str}"
            "&hydrate=probablePitcher,venue(location,fieldInfo),weather,team"
        )
        resp = _http.get(url, timeout=10)
        resp.raise_for_status()
        raw_games = []
        for date_entry in resp.json().get("dates", []):
            for game_raw in date_entry.get("games", []):
                if game_raw.get("gameType", "R") in ("R", "W", "D", "L", "C"):
                    raw_games.append(game_raw)
    except Exception:
        return []

    def _process(game_data):
        gid = game_data.get("gamePk", 0)
        t = game_data.get("teams", {})
        away_abbr = t.get("away", {}).get("team", {}).get("abbreviation", "?")
        home_abbr = t.get("home", {}).get("team", {}).get("abbreviation", "?")
        away_abbr = _ABBR_NORM.get(away_abbr, away_abbr)
        home_abbr = _ABBR_NORM.get(home_abbr, home_abbr)

        status = game_data.get("status", {}).get("detailedState", "")
        time_str = "TBD"
        gd = game_data.get("gameDate", "")
        if gd:
            try:
                dt_utc = datetime.fromisoformat(gd.replace("Z", "+00:00"))
                time_str = dt_utc.astimezone().strftime("%I:%M %p").lstrip("0")
            except Exception:
                pass

        # ── Delay / suspension: re-anchor to actual first pitch ──
        # When a game is delayed the scheduled gameDate still reflects the
        # original start.  The live feed's firstPitch (or resumeDateTime for
        # suspended games) gives the real start so hourly weather windows
        # align with the actual playing time.
        #
        # Mid-game delays (rain delay after play has started): firstPitch ≈
        # gameDate so the threshold won't trigger.  Instead, detect that the
        # game is mid-delay (status "Delayed" + currentInning > 0) and
        # re-anchor to the current hour so the hourly slots show conditions
        # for when play resumes.
        _delay_statuses = ("Delayed", "Delayed Start", "Suspended",
                           "In Progress", "Warmup", "Game Over", "Final")
        _feed_json = None
        if status in _delay_statuses and gd:
            try:
                _feed_json = _http.get(
                    f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live",
                    timeout=10,
                ).json()
                _feed_gd = _feed_json.get("gameData", {})
                _feed_dt = _feed_gd.get("datetime", {})
                _fp = (_feed_dt.get("firstPitch")
                       or _feed_dt.get("resumeDateTime")
                       or "")
                if _fp:
                    _fp_utc = datetime.fromisoformat(
                        _fp.replace("Z", "+00:00"))
                    _orig_utc = datetime.fromisoformat(
                        gd.replace("Z", "+00:00"))
                    # Only re-anchor if actual start is ≥15 min later
                    if (_fp_utc - _orig_utc).total_seconds() >= 900:
                        gd = _fp
                        time_str = (_fp_utc.astimezone()
                                    .strftime("%I:%M %p").lstrip("0"))

                # Mid-game delay: game started on time but is currently
                # stopped.  Re-anchor to the current hour so weather slots
                # cover the expected resume window.
                if status in ("Delayed",):
                    _ls = _feed_json.get("liveData", {}).get("linescore", {})
                    _inning = _ls.get("currentInning", 0) or 0
                    if _inning > 0:
                        _now_utc = datetime.now(timezone.utc)
                        # Snap to the top of the current hour
                        _now_hour = _now_utc.replace(
                            minute=0, second=0, microsecond=0)
                        gd = _now_hour.isoformat()
                        time_str = (_now_hour.astimezone()
                                    .strftime("%I:%M %p").lstrip("0"))
            except Exception:
                pass

        weather = game_data.get("weather", {})
        venue = game_data.get("venue", {})
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
            "azimuth": loc.get("azimuthAngle", 0) or 0,
            "temp": weather.get("temp"),
            "condition": weather.get("condition", ""),
            "wind_speed": ws, "wind_dir": wd, "wind_angle": wa,
            "precip_pct": None, "pressure_hpa": None, "humidity_pct": None,
        }

        # Venue coords may be missing from schedule; grab from game feed
        if not result["lat"]:
            try:
                feed = (_feed_json if _feed_json is not None
                        else _http.get(
                            f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live",
                            timeout=10,
                        ).json())
                gv = feed.get("gameData", {}).get("venue", {})
                gl = gv.get("location", {})
                gc = gl.get("defaultCoordinates", {})
                gf = gv.get("fieldInfo", {})
                result["lat"] = gc.get("latitude")
                result["lon"] = gc.get("longitude")
                result["elevation"] = gl.get("elevation", result["elevation"])
                result["roof_type"] = gf.get("roofType", result["roof_type"])
                if not result["azimuth"]:
                    result["azimuth"] = gl.get("azimuthAngle", 0) or 0
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

        # Static coord override for venues where MLB API provides no coordinates
        # (e.g. international venues like Estadio Alfredo Harp Helú).
        if not result["lat"] and result["venue_id"] in _VENUE_COORD_OVERRIDE:
            _ov = _VENUE_COORD_OVERRIDE[result["venue_id"]]
            result["lat"]       = _ov[0]
            result["lon"]       = _ov[1]
            result["elevation"] = result["elevation"] or _ov[2]
            if not result["azimuth"]:
                result["azimuth"] = _ov[3]

        # Retractable roofs: determine open/closed from MLB condition field.
        # Only trust the API condition as an official roof indicator within
        # 1 hour of game time (or once the game is live).  Earlier than that,
        # the API frequently echoes stale or placeholder values that don't
        # reflect the actual roof decision.
        _is_retractable = result["roof_type"] == "Retractable"
        _roof_confirmed = False
        _game_live = status in ("In Progress", "Warmup", "Final",
                                "Game Over", "Delayed")
        # Check if we're within 1 hour of first pitch
        _within_1hr = False
        if gd and not _game_live:
            try:
                _gt = datetime.fromisoformat(gd.replace("Z", "+00:00"))
                _now = datetime.now(_gt.tzinfo)
                _within_1hr = (_gt - _now).total_seconds() <= 3600
            except Exception:
                pass
        _trust_api = _game_live or _within_1hr
        if _is_retractable:
            cond_lower = result["condition"].lower()
            if cond_lower in ("roof closed", "dome") and _trust_api:
                result["roof_type"] = "Retractable (CLOSED)"
                _roof_confirmed = True
            elif cond_lower and _game_live:
                # Real weather reported while game is live → roof is open
                result["roof_type"] = "Retractable (OPEN)"
                _roof_confirmed = True
            # Clear dome/roof-closed condition so real weather can fill in
            if cond_lower in ("dome", "roof closed", ""):
                result["condition"] = ""

        # NWS primary → WeatherAPI fallback → Open-Meteo fallback
        _om_was_primary = False
        if result["lat"] and result["lon"]:
            _az = result["azimuth"]
            om = _fetch_nws(result["lat"], result["lon"],
                            game_utc_iso=gd or None, azimuth=_az)
            if not om:
                om = _fetch_weatherapi(result["lat"], result["lon"],
                                       game_utc_iso=gd or None, azimuth=_az,
                                       elev_ft=result.get("elevation") or 0)
            if not om:
                om = _fetch_open_meteo(result["lat"], result["lon"],
                                       game_utc_iso=gd or None, azimuth=_az)
                _om_was_primary = True

            result["precip_pct"] = om.get("precip_pct")
            result["pressure_hpa"] = om.get("pressure_hpa")
            result["humidity_pct"] = om.get("humidity_pct")

            # Fill missing MLB weather from hourly forecast.
            # forecast_condition is the WORST condition across the game window
            # (computed by the weather provider from all game-hour slots) —
            # always prefer it over the MLB Stats API observation which may
            # reflect current conditions far from game time.
            if not result["temp"] and om.get("forecast_temp") is not None:
                result["temp"] = str(round(om["forecast_temp"]))
            if om.get("forecast_condition"):
                result["condition"] = om["forecast_condition"]
            if result["wind_speed"] == 0 and om.get("forecast_wind_speed") is not None:
                result["wind_speed"] = round(om["forecast_wind_speed"])
                deg = om.get("forecast_wind_deg")
                if deg is not None:
                    mlb_dir = _compass_to_mlb_wind(deg, _az)
                    result["wind_dir"] = mlb_dir
                    result["wind_angle"] = WIND_ANGLES.get(mlb_dir)
            if om.get("forecast_precip") is not None:
                result["precip_pct"] = om["forecast_precip"]
            if om.get("forecast_pressure") is not None:
                result["pressure_hpa"] = round(om["forecast_pressure"], 1)
            if om.get("forecast_humidity") is not None:
                result["humidity_pct"] = om["forecast_humidity"]

            # Hourly conditions: NWS only provides future periods so past
            # games may have < 4 slots.  Fall back to Open-Meteo (which has
            # full historical hourly) when the primary source is incomplete.
            # Skip the fallback call if Open-Meteo was already the primary
            # source (its hourly data is already in ``om``).
            _hourly = om.get("hourly_conditions") or []
            if len(_hourly) < 4 and gd and not _om_was_primary:
                _om_hourly = _fetch_open_meteo(
                    result["lat"], result["lon"],
                    game_utc_iso=gd, azimuth=_az)
                _fallback_h = _om_hourly.get("hourly_conditions") or []
                if len(_fallback_h) >= 4:
                    _hourly = _fallback_h
            if _hourly:
                result["hourly_conditions"] = _hourly
            # Store sunset hour from the weather provider for day/night logic
            if om.get("sunset_hour") is not None:
                result["sunset_hour"] = om["sunset_hour"]

        # Predictive roof status for retractable venues (pregame only)
        if _is_retractable and not _roof_confirmed:
            result["roof_type"] = _predict_retractable_roof(result)

        return result

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_process, game_raw): game_raw for game_raw in raw_games}
        try:
            for future in as_completed(futs, timeout=20):
                try:
                    results.append(future.result())
                except Exception:
                    pass
        except TimeoutError:
            # Return whatever games finished in time
            for future in futs:
                if future.done() and not future.exception():
                    try:
                        result_item = future.result()
                        if result_item not in results:
                            results.append(result_item)
                    except Exception:
                        pass

    def _park_time_key(x):
        t = (x.get("time") or "").strip()
        try:
            from datetime import datetime as _dt
            return _dt.strptime(t.upper(), "%I:%M %p").time()
        except Exception:
            from datetime import time as _t
            return _t(23, 59, 59)
    results.sort(key=_park_time_key)

    # ── Pressure fill — single batched Open-Meteo call for all games ──
    _fetch_pressure_batch(results)

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

        self._hourly = data.get("hourly_conditions", [])

        # Animated weather icon overlay (cycles hourly conditions)
        hourly = self._hourly
        if len(hourly) > 1:
            # Place overlay centred above the outfield
            ow, oh = 90, 68
            ox = self.CX - ow // 2
            oy = 2
            self._weather_overlay = _WeatherCycleOverlay(hourly, ow, oh, self)
            self._weather_overlay.move(ox, oy)
            self._has_cycle = True

    def set_wind(self, idx: int):
        """Update wind data from hourly slot *idx* and repaint arrows."""
        if not self._hourly or idx < 0 or idx >= len(self._hourly):
            return
        slot = self._hourly[idx]
        wd = slot.get("wind_dir", "")
        self.d["wind_dir"] = wd
        self.d["wind_angle"] = WIND_ANGLES.get(wd)
        self.d["wind_speed"] = slot.get("wind_speed", 0)
        self._cached_pm = None
        self.update()

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
        _roof = self.d.get("roof_type", "Open")
        is_dome = _roof == "Dome" or "CLOSED" in _roof
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
        temp_txt = f"{round(float(temp))}°F" if temp else "-- °F"
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
        e_txt = f"{elev:,} ft" if elev is not None else "-- ft"
        p.drawText(QRectF(self.W - 90, 34, 82, 14),
                    Qt.AlignmentFlag.AlignRight, e_txt)

        precip = self.d.get("precip_pct")
        pr_txt = f"{precip:.0f}% precip" if precip is not None else "-- % precip"
        p.drawText(QRectF(self.W - 90, 48, 82, 14),
                    Qt.AlignmentFlag.AlignRight, pr_txt)

        humid = self.d.get("humidity_pct")
        hm_txt = f"{humid:.0f}% humid" if humid is not None else "-- % humid"
        p.drawText(QRectF(self.W - 90, 62, 82, 14),
                    Qt.AlignmentFlag.AlignRight, hm_txt)

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
        self._game_id = data.get("game_id")
        self.setStyleSheet(
            f"QFrame {{ background:{C['bg1']}; "
            f"border:1px solid {C['bdr']}; border-radius:6px; }}")
        self.setSizePolicy(QSizePolicy.Policy.Expanding,
                           QSizePolicy.Policy.Fixed)
        self._build()

    def update_data(self, data: dict):
        """Update card with new weather data — update labels in-place."""
        self.d = data
        self._game_id = data.get("game_id")
        # Update detail label
        self._hourly = data.get("hourly_conditions", [])
        self._detail_lbl.setText(self._detail_text(0))
        # Update mini park widget
        if hasattr(self._park, 'update_data'):
            self._park.update_data(data)

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
        self._park = MiniParkWidget(self.d)
        pw = QHBoxLayout()
        pw.setContentsMargins(0, 0, 0, 0)
        pw.addStretch()
        pw.addWidget(self._park)
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
        if hasattr(self._park, '_weather_overlay') and self._park._has_cycle:
            self._park._weather_overlay.indexChanged.connect(self._on_hour_change)

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
        _is_dome = roof == "Dome" or "CLOSED" in roof
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
        if _is_dome:
            parts.append("Dome")
        else:
            parts.append(f"{ws}mph {wd}" if ws else "Calm")
        return "  ·  ".join(parts)

    def _on_hour_change(self, idx: int):
        self._detail_lbl.setText(self._detail_text(idx))
        self._park.set_wind(idx)


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


def _weather_cache_is_stale(date_str: str) -> bool:
    """Return True if the disk cache for *date_str* is older than _WEATHER_CACHE_TTL_SECS."""
    import time as _time
    path = _weather_cache_path(date_str)
    if not os.path.isfile(path):
        return True
    try:
        age = _time.time() - os.path.getmtime(path)
        return age > _WEATHER_CACHE_TTL_SECS
    except Exception:
        return True


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


def _cleanup_old_weather_cache(keep_days: int = 4):
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


def _seed_nws_grid_cache(games: list[dict]) -> None:
    """Submit NWS grid resolution for each game venue in the background.

    Uses the existing dedup logic inside _nws_resolve_grid (pending events),
    so concurrent or duplicate calls are safe.
    """
    seen: set[str] = set()
    for g in games:
        lat, lon = g.get("lat"), g.get("lon")
        if lat is None or lon is None:
            continue
        key = f"{round(lat, 3)},{round(lon, 3)}"
        if key in seen:
            continue
        seen.add(key)
        # Skip if already cached
        with _nws_grid_lock:
            if key in _nws_grid_cache:
                continue
        try:
            _nws_grid_seed_pool.submit(_nws_resolve_grid, lat, lon)
        except Exception:
            pass


def prefetch_weather(date_str: str | None = None, force_refresh: bool = False):
    """Seed the in-memory cache from disk, then re-fetch fresh data.

    *force_refresh* (default False): When True, delete existing disk cache
    and always re-fetch.  When False, only fetch if no cache exists yet.
    Today's date always forces a refresh regardless of this flag.

    Safe to call from any thread.  Designed to be kicked off at app startup
    exactly like the lineup prefetch (daemon thread, fire-and-forget).
    """
    import datetime as _dt
    date_str = date_str or _dt.date.today().isoformat()
    is_today = (date_str == _dt.date.today().isoformat())

    # Only preserve hourly windows for games actively in progress —
    # preserving for Scheduled games locks in any bad data from the first fetch.
    _LIVE_PRESERVE_STATUSES = frozenset({
        "In Progress", "Warmup", "Delayed", "Delayed Start", "Suspended",
    })

    if is_today or force_refresh:
        # Preserve original hourly_conditions from existing cache before
        # clearing — weather APIs drop past hours on re-fetch, which would
        # cause the hourly toggle to shift mid-game.
        _prev_hourly: dict[int, list[dict]] = {}
        prev = get_cached_weather(date_str)
        if prev:
            for g in prev:
                hc = g.get("hourly_conditions")
                if hc and len(hc) >= 4 and g.get("status", "") in _LIVE_PRESERVE_STATUSES:
                    gid = g.get("game_id")
                    if gid:
                        _prev_hourly[gid] = hc

        # Delete stale disk cache so we get fresh weather
        try:
            stale = _weather_cache_path(date_str)
            if os.path.isfile(stale):
                os.remove(stale)
        except Exception:
            pass
        with _weather_lock:
            _weather_cache.pop(date_str, None)
    else:
        _prev_hourly = {}
        # For non-today dates, use existing cache if available
        existing = get_cached_weather(date_str)
        if existing is not None:
            # Re-fetch if any game is missing coords that can be filled via
            # a static override (e.g. international venues added after the
            # cache was built).
            needs_refresh = any(
                not g.get("lat") and g.get("venue_id") in _VENUE_COORD_OVERRIDE
                for g in existing
            )
            if not needs_refresh:
                return
            # Invalidate stale cache so we fall through to a fresh fetch
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

    # Pre-populate NWS grid cache for all venues — eliminates first-request
    # latency when weather widgets open later.
    _seed_nws_grid_cache(data)

    # Restore original hourly_conditions so the 4-hour window stays
    # anchored to game start rather than shifting to "now".
    if _prev_hourly:
        for g in data:
            gid = g.get("game_id")
            if gid and gid in _prev_hourly:
                g["hourly_conditions"] = _prev_hourly[gid]

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
        import datetime as _dt
        is_today = (self._date == _dt.date.today().isoformat())

        # For non-today dates, use existing cache only if it is fresh (< TTL).
        # Stale cache means the forecast has aged out and needs a live re-fetch.
        if not is_today:
            if not _weather_cache_is_stale(self._date):
                cached = get_cached_weather(self._date)
                if cached and all(g.get("pressure_hpa") for g in cached):
                    self.finished.emit(cached)
                    return
            else:
                # Stale — clear from memory so prefetch_weather will re-fetch
                with _weather_lock:
                    _weather_cache.pop(self._date, None)

        # Preserve original hourly_conditions before re-fetch —
        # only for live games to avoid locking in bad data from early fetches.
        _LIVE_PRESERVE_STATUSES = frozenset({
            "In Progress", "Warmup", "Delayed", "Delayed Start", "Suspended",
        })
        _prev_hourly: dict[int, list[dict]] = {}
        prev = get_cached_weather(self._date)
        if prev:
            for g in prev:
                hc = g.get("hourly_conditions")
                if hc and len(hc) >= 4 and g.get("status", "") in _LIVE_PRESERVE_STATUSES:
                    gid = g.get("game_id")
                    if gid:
                        _prev_hourly[gid] = hc

        try:
            data = fetch_park_weather(self._date)
        except Exception:
            data = []

        # Restore original hourly_conditions so the 4-hour window stays
        # anchored to game start.
        if _prev_hourly:
            for g in data:
                gid = g.get("game_id")
                if gid and gid in _prev_hourly:
                    g["hourly_conditions"] = _prev_hourly[gid]

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

        # ── 30-minute periodic refresh for long sessions ──
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(30 * 60 * 1000)  # 30 minutes
        self._refresh_timer.timeout.connect(self.refresh_weather)
        self._refresh_timer.start()

        # Track last refresh time so nav-back can skip if recent
        import time as _t
        self._last_refresh_mono = _t.monotonic()

    # ── public refresh entry point ──
    def refresh_weather(self):
        """Kick off a background weather re-fetch (debounced to 60s)."""
        import time as _t
        now = _t.monotonic()
        # Don't re-fetch if last refresh was < 60s ago
        if now - self._last_refresh_mono < 60:
            return
        self._last_refresh_mono = now
        if self._worker is not None and self._worker.isRunning():
            return
        self._worker = _WeatherWorker(self._date)
        self._worker.finished.connect(self._on_refresh)
        self._worker.start()

    def showEvent(self, event):
        """Re-fetch weather whenever the page becomes visible (nav-back)."""
        super().showEvent(event)
        self.refresh_weather()

    # ── title + filter section ──
    def _build_header(self):
        wrap = QWidget()
        wrap.setStyleSheet(f"background:{C['bg0']}; border-bottom:1px solid {C['bdr']};")
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
        vl.addSpacing(6)

        # ── Info button: aligned bottom-right of filter container ──
        info_row = QHBoxLayout()
        info_row.setContentsMargins(0, 0, 0, 0)
        info_row.addStretch()
        _info_svg_path = os.path.join(_app_paths.APP_DIR, "assets", "info-svgrepo-com.svg")
        _info_btn = QPushButton("Predict Calculations")
        _ss_normal = f"QPushButton {{ background:transparent; border:none; color:{C['t3']}; font-size:11px; padding-right:4px; }}"
        _ss_hover  = f"QPushButton {{ background:transparent; border:none; color:{C['ora']}; font-size:11px; padding-right:4px; }}"
        try:
            from PyQt6.QtSvg import QSvgRenderer
            with open(_info_svg_path, "r", encoding="utf-8") as _f:
                _svg_data = _f.read()
            def _make_icon(color):
                _svg = _svg_data.replace('fill="#000000"', f'fill="{color}"')
                _pm = QPixmap(18, 18)
                _pm.fill(QColor(0, 0, 0, 0))
                _r = QSvgRenderer(QByteArray(_svg.encode()))
                _p = QPainter(_pm)
                _r.render(_p)
                _p.end()
                return QIcon(_pm)
            _icon_n = _make_icon(C["t3"])
            _icon_h = _make_icon(C["ora"])
            _info_btn.setIcon(_icon_n)
            _info_btn._icon_normal = _icon_n
            _info_btn._icon_hover  = _icon_h
            def _info_enter(e, b=_info_btn, ss=_ss_hover):
                b.setIcon(b._icon_hover)
                b.setStyleSheet(ss)
                QPushButton.enterEvent(b, e)
            def _info_leave(e, b=_info_btn, ss=_ss_normal):
                b.setIcon(b._icon_normal)
                b.setStyleSheet(ss)
                QPushButton.leaveEvent(b, e)
            _info_btn.enterEvent = _info_enter
            _info_btn.leaveEvent = _info_leave
        except Exception:
            _info_btn.setIcon(QIcon(_info_svg_path))
        _info_btn.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        _info_btn.setIconSize(QSize(18, 18))
        _info_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        _info_btn.setStyleSheet(_ss_normal)
        _tip = (
            "<b>How Predict % is calculated</b><br><br>"
            "Each stat shows deviation from the MLB average rate (2021–2026 Statcast, 840K+ ABs).<br>"
            "<b>0%</b> = exactly league average &nbsp;&nbsp; <b>+10%</b> = 10% more than average per game<br><br>"
            "<table cellspacing='4'>"
            "<tr><td><b>HR</b></td><td style='padding-right:12px'>2.3 per game</td><td style='color:#888'>(both teams combined)</td></tr>"
            "<tr><td><b>1B</b></td><td style='padding-right:12px'>10.6 per game</td><td style='color:#888'>(both teams combined)</td></tr>"
            "<tr><td><b>2B/3B</b></td><td style='padding-right:12px'>3.6 per game</td><td style='color:#888'>(2B + 3B combined, both teams)</td></tr>"
            "</table><br>"
            "<i>Model: Stage 0 park geometry + Stage 1A endemic climate + Stage 2 day-to-day weather.</i>"
        )
        def _show_tip(checked, b=_info_btn, tip=_tip):
            QToolTip.showText(b.mapToGlobal(b.rect().bottomLeft()), tip, b)
        _info_btn.clicked.connect(_show_tip)
        info_row.addWidget(_info_btn)
        vl.addLayout(info_row)
        vl.addSpacing(6)

        self._roof_label = "ALL"
        self._bias_label = "ALL"

        self._root.addWidget(wrap)

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
        self._ncols = 2
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
        # Diff per-game and update only changed cards in-place
        old_map = {g.get("game_id"): g for g in (self._all_data or [])}
        new_map = {g.get("game_id"): g for g in data}
        if set(old_map.keys()) == set(new_map.keys()):
            # Same games — update cards in-place instead of full rebuild
            card_map = {getattr(c, '_game_id', None): c for c in self._cards
                        if hasattr(c, '_game_id')}
            changed = False
            for gid, new_g in new_map.items():
                if old_map.get(gid) != new_g and gid in card_map:
                    card_map[gid].update_data(new_g)
                    changed = True
            if changed:
                self._all_data = data
                self._count_lbl.setText(f"{len(data)} games")
            return
        # Game set changed — full rebuild required
        self._on_data(data)

    # ── populate grid with cards ──
    def _populate(self, games: list[dict]):
        # Clear existing
        for c in self._cards:
            c.setParent(None)
            c.deleteLater()
        self._cards.clear()
        self._pending_games = list(games)
        self._populate_idx = 0
        self._populate_batch()

    def _populate_batch(self):
        """Create cards in small batches to avoid UI freeze."""
        BATCH = 4
        end = min(self._populate_idx + BATCH, len(self._pending_games))
        for i in range(self._populate_idx, end):
            card = WeatherDetailWidget(self._pending_games[i])
            self._cards.append(card)
            row, col = divmod(i, self._ncols)
            self._grid.addWidget(card, row, col)
        self._populate_idx = end
        if self._populate_idx < len(self._pending_games):
            QTimer.singleShot(0, self._populate_batch)

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
        if w < 1200:
            new_cols = 1
        else:
            new_cols = 2
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
                        if g.get("roof_type", "Open").startswith("Retractable")]

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
