"""
Player Profile Card widget for Seam Analytics.

Provides a popup dialog with player headshot, bio info, and key stats.
Headshots are fetched from the MLB CDN and cached to disk.
"""

import math
import os
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pybaseball
import _app_paths
from _http_utils import create_http_session, TIMEOUT_DEFAULT

from PyQt6.QtWidgets import (
    QDialog, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QFrame, QPushButton, QApplication, QTableWidget,
    QTableWidgetItem, QHeaderView, QAbstractItemView, QStyledItemDelegate, QStyle,
)
from PyQt6.QtCore import Qt, QRectF, QPointF, pyqtSignal, QObject
from PyQt6.QtGui import (
    QPixmap, QImage, QColor, QPainter, QPainterPath, QPen, QPolygonF, QFont,
)

log = logging.getLogger("seam.player_card")

# ── Theme (shared palette — single source of truth) ─────────────────
from _app_theme import C

# ── HTTP session ────────────────────────────────────────────────────
_http = create_http_session(total_retries=2, backoff_factor=0.3)

_pool = ThreadPoolExecutor(max_workers=2)

HEADSHOT_URL = (
    "https://img.mlbstatic.com/mlb-photos/image/upload/"
    "d_people:generic:headshot:67:current.png/"
    "w_213,q_auto:best/v1/people/{pid}/headshot/67/current"
)

HEADSHOT_DIR = _app_paths.HEADSHOT_CACHE_DIR
os.makedirs(HEADSHOT_DIR, exist_ok=True)

# ── Player roster CSV ───────────────────────────────────────────────
PLAYERS_CSV = _app_paths.PLAYERS_CSV
_roster_cache: dict | None = None
_enrichment_inflight: set[int] = set()


def _supplement_roster_from_db(cache: dict) -> None:
    """Add stub entries for players in the raw DB but missing from players.csv."""
    if not os.path.exists(_app_paths.RAW_DB):
        return
    try:
        conn = sqlite3.connect(_app_paths.RAW_DB, timeout=10)

        # ── Batters ──────────────────────────────────────────────────
        batter_rows = conn.execute("""
            WITH latest AS (
                SELECT batter_id, MAX(game_date) AS max_date
                FROM plate_appearances
                GROUP BY batter_id
            )
            SELECT pa.batter_id, pa.batter_name,
                   CASE WHEN pa.batter_is_home = 1
                        THEN g.home_team ELSE g.away_team END AS team,
                   pa.stand AS bats
            FROM plate_appearances pa
            JOIN latest l ON pa.batter_id = l.batter_id
                         AND pa.game_date = l.max_date
            JOIN games g ON pa.game_id = g.game_id
            GROUP BY pa.batter_id
        """).fetchall()

        for batter_id, batter_name, team, bats in batter_rows:
            if not batter_id or batter_id in cache:
                continue
            name = batter_name or ""
            parts = name.split()
            cache[batter_id] = {
                "player_id": str(batter_id),
                "name_full": name,
                "name_last": parts[-1] if parts else "",
                "name_first": parts[0] if parts else "",
                "team": team or "",
                "team_id": "",
                "jersey_number": "",
                "position": "",
                "position_type": "",
                "bats": bats or "",
                "throws": "",
                "age": "",
                "height": "",
                "weight": "",
                "mlb_debut": "",
                "birth_country": "",
                "headshot_url": HEADSHOT_URL.format(pid=batter_id),
                "_db_stub": True,
            }

        # ── Pitchers ─────────────────────────────────────────────────
        pitcher_rows = conn.execute("""
            WITH latest AS (
                SELECT pitcher_id, MAX(game_date) AS max_date
                FROM plate_appearances
                GROUP BY pitcher_id
            )
            SELECT pa.pitcher_id, pa.pitcher_name,
                   CASE WHEN pa.batter_is_home = 1
                        THEN g.away_team ELSE g.home_team END AS team,
                   pa.p_throws AS throws
            FROM plate_appearances pa
            JOIN latest l ON pa.pitcher_id = l.pitcher_id
                         AND pa.game_date = l.max_date
            JOIN games g ON pa.game_id = g.game_id
            GROUP BY pa.pitcher_id
        """).fetchall()

        for pitcher_id, pitcher_name, team, throws in pitcher_rows:
            if not pitcher_id or pitcher_id in cache:
                continue
            name = pitcher_name or ""
            parts = name.split()
            cache[pitcher_id] = {
                "player_id": str(pitcher_id),
                "name_full": name,
                "name_last": parts[-1] if parts else "",
                "name_first": parts[0] if parts else "",
                "team": team or "",
                "team_id": "",
                "jersey_number": "",
                "position": "P",
                "position_type": "Pitcher",
                "bats": "",
                "throws": throws or "",
                "age": "",
                "height": "",
                "weight": "",
                "mlb_debut": "",
                "birth_country": "",
                "headshot_url": HEADSHOT_URL.format(pid=pitcher_id),
                "_db_stub": True,
            }

        conn.close()
        csv_count = sum(1 for v in cache.values() if not v.get("_db_stub"))
        stub_count = len(cache) - csv_count
        if stub_count:
            log.debug("roster: %d from CSV, %d supplemented from DB", csv_count, stub_count)
    except Exception:
        log.exception("_supplement_roster_from_db")


# ── Today's lineup-derived team map (ground truth, overrides CSV/API) ─
_today_player_teams: dict[int, str] = {}


def set_today_player_teams(pt: dict[int, str]) -> None:
    """Store today's {player_id: team} map and immediately fix roster cache + CSV.

    Called once from the leaderboard fetch thread after lineups are loaded.
    The lineup cache is the ground truth for current team — it overrides any
    stale values in players.csv or the MLB Stats API.
    """
    global _today_player_teams
    _today_player_teams = pt
    cache = _load_roster()
    changed: list[int] = []
    for pid, team in pt.items():
        entry = cache.get(pid)
        if entry and entry.get("team") != team:
            log.debug("team correction: player %s %s → %s", pid,
                      entry.get("team"), team)
            entry["team"] = team
            changed.append(pid)
    if changed:
        log.debug("persisting %d team corrections from lineup cache", len(changed))
        for pid in changed:
            _persist_roster_entry(pid, cache[pid])


_CSV_FIELDS = [
    "player_id", "name_full", "name_last", "name_first",
    "team", "team_id", "jersey_number",
    "position", "position_type",
    "bats", "throws",
    "age", "height", "weight",
    "mlb_debut", "birth_country",
    "headshot_url",
]

_csv_write_lock = __import__("threading").Lock()


def _persist_roster_entry(player_id: int, entry: dict) -> None:
    """Rewrite players.csv with the updated entry for player_id.

    Thread-safe; silently skips if the CSV doesn't exist yet.
    """
    if not os.path.exists(PLAYERS_CSV):
        return
    try:
        import csv as _csv2
        with _csv_write_lock:
            with open(PLAYERS_CSV, encoding="utf-8", newline="") as f:
                rows = list(_csv2.DictReader(f))
            updated = False
            for row in rows:
                try:
                    if int(row.get("player_id", -1)) == player_id:
                        for field in _CSV_FIELDS:
                            if field in entry and entry[field] not in (None, ""):
                                row[field] = entry[field]
                        updated = True
                        break
                except ValueError:
                    continue
            if not updated:
                # New player (DB stub) — append
                new_row = {f: entry.get(f, "") for f in _CSV_FIELDS}
                rows.append(new_row)
            with open(PLAYERS_CSV, "w", encoding="utf-8", newline="") as f:
                writer = _csv2.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            log.debug("persisted player %s to %s", player_id, PLAYERS_CSV)
    except Exception:
        log.debug("_persist_roster_entry failed for %s", player_id)


def _enrich_player(player_id: int) -> None:
    """Async: fetch bio from MLB API and update the roster cache entry.

    Called for DB stubs (missing from CSV) and for any CSV player whose
    team may be stale (e.g. traded players).
    """
    if player_id in _enrichment_inflight:
        return
    _enrichment_inflight.add(player_id)

    def _do_fetch():
        try:
            resp = _http.get(
                f"https://statsapi.mlb.com/api/v1/people/{player_id}?hydrate=currentTeam",
                timeout=8,
            )
            if resp.status_code != 200:
                return
            people = resp.json().get("people", [])
            if not people:
                return
            data = people[0]
            cache = _load_roster()
            entry = cache.get(player_id)
            if entry is None:
                return
            entry["name_full"] = data.get("fullName", entry["name_full"])
            entry["name_first"] = data.get("firstName", entry["name_first"])
            entry["name_last"] = data.get("lastName", entry["name_last"])
            entry["bats"] = data.get("batSide", {}).get("code", entry["bats"])
            entry["throws"] = data.get("pitchHand", {}).get("code", entry["throws"])
            entry["age"] = str(data.get("currentAge", ""))
            entry["height"] = data.get("height", "")
            entry["weight"] = str(data.get("weight", ""))
            entry["mlb_debut"] = data.get("mlbDebutDate", "")
            entry["birth_country"] = data.get("birthCountry", "")
            ct = data.get("currentTeam", {})
            if ct.get("abbreviation"):
                entry["team"] = ct["abbreviation"]
                entry["team_id"] = str(ct.get("id", ""))
            pos = data.get("primaryPosition", {})
            if pos.get("abbreviation") and not entry.get("position"):
                entry["position"] = pos["abbreviation"]
                entry["position_type"] = pos.get("type", "")
            entry["_db_stub"] = False
            log.debug("enriched player %s (%s) from MLB API", player_id, entry["name_full"])
            _persist_roster_entry(player_id, entry)
        except Exception:
            log.debug("MLB API enrichment failed for %s", player_id)
        finally:
            _enrichment_inflight.discard(player_id)

    _pool.submit(_do_fetch)


def _load_roster() -> dict:
    """Load players.csv into a dict keyed by player_id (int).

    Also supplements with any batters/pitchers found in the raw DB that are
    not present in the CSV (rookies, returning players, call-ups missed by
    the last build_player_roster.py run).
    """
    global _roster_cache
    if _roster_cache is not None:
        return _roster_cache
    _roster_cache = {}
    if os.path.exists(PLAYERS_CSV):
        import csv as _csv
        with open(PLAYERS_CSV, encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                try:
                    pid = int(row["player_id"])
                except (ValueError, KeyError):
                    continue
                _roster_cache[pid] = row
    _supplement_roster_from_db(_roster_cache)
    return _roster_cache


def get_player_roster_info(player_id: int) -> dict | None:
    """Return CSV row dict for a player, or None.

    Always fires an async MLB API fetch to keep team/bio current.
    For DB stubs the fetch also fills in bio fields; for CSV players
    it updates stale team info (e.g. traded players).
    """
    entry = _load_roster().get(player_id)
    if entry is not None:
        _enrich_player(player_id)
    return entry


def resolve_venue_team(player_team: str, games: list | None = None) -> str:
    """Determine the stadium team for a player based on their next game.

    1. Check schedule games (today + tomorrow) for the player's team → home team.
    2. Fallback: query raw DB for the most recent game the team played in → home team.
    3. Final fallback: player's own team (home stadium).
    """
    if not player_team:
        return 'generic'

    # ── Schedule lookup ──
    if games:
        for g in games:
            away, home = g.get('away', ''), g.get('home', '')
            if player_team in (away, home):
                return home  # venue is always the home team's park

    # ── DB fallback: most recent game this team played in ──
    try:
        conn = sqlite3.connect(_app_paths.RAW_DB)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT home_team FROM plate_appearances "
            "WHERE (home_team = ? OR away_team = ?) "
            "ORDER BY game_date DESC LIMIT 1",
            (player_team, player_team)
        ).fetchone()
        conn.close()
        if row and row["home_team"]:
            return row["home_team"]
    except Exception:
        pass

    return player_team
# ═════════════════════════════════════════════════════════════════════
# Headshot loader
# ═════════════════════════════════════════════════════════════════════

class _HeadshotSignal(QObject):
    ready = pyqtSignal(int, QPixmap)          # player_id, pixmap


_headshot_signal = _HeadshotSignal()


HS_W, HS_H, HS_R = 90, 136, 10   # headshot width, height, corner radius


def _round_pixmap(pm, w=HS_W, h=HS_H, r=HS_R):
    """Clip a QPixmap into a rounded rectangle."""
    scaled = pm.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                       Qt.TransformationMode.SmoothTransformation)
    out = QPixmap(w, h)
    out.fill(QColor(0, 0, 0, 0))
    painter = QPainter(out)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    path = QPainterPath()
    path.addRoundedRect(0, 0, w, h, r, r)
    painter.setClipPath(path)
    x = (scaled.width() - w) // 2
    y = (scaled.height() - h) // 2
    painter.drawPixmap(-x, -y, scaled)
    painter.end()
    return out


def _placeholder_pixmap(w=HS_W, h=HS_H, r=HS_R):
    """Dark rounded-rect placeholder shown while headshot loads."""
    pm = QPixmap(w, h)
    pm.fill(QColor(0, 0, 0, 0))
    painter = QPainter(pm)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setBrush(QColor(C["bg3"]))
    painter.setPen(QColor(C["bdr"]))
    painter.drawRoundedRect(1, 1, w - 2, h - 2, r, r)
    # Silhouette icon
    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(QColor(C["t3"]))
    cx, cy = w // 2, h // 2
    painter.drawEllipse(cx - 16, cy - 30, 32, 32)        # head
    painter.drawEllipse(cx - 28, cy + 8, 56, 36)         # body
    painter.end()
    return pm


def fetch_headshot(player_id, callback_label=None):
    """Fetch headshot async — shows placeholder immediately, swaps on load."""
    cache_path = os.path.join(HEADSHOT_DIR, f"{player_id}.png")

    # Fast path: already cached
    if os.path.exists(cache_path):
        pm = QPixmap(cache_path)
        if not pm.isNull():
            return _round_pixmap(pm)

    # Async fetch
    def _download():
        try:
            url = HEADSHOT_URL.format(pid=player_id)
            resp = _http.get(url, timeout=TIMEOUT_DEFAULT)
            if resp.status_code == 200 and len(resp.content) > 500:
                with open(cache_path, "wb") as f:
                    f.write(resp.content)
                img = QImage()
                img.loadFromData(resp.content)
                if not img.isNull():
                    pm = QPixmap.fromImage(img)
                    _headshot_signal.ready.emit(player_id, _round_pixmap(pm))
        except Exception:
            log.debug("headshot fetch failed for %s", player_id)

    _pool.submit(_download)
    return _placeholder_pixmap()


# ═════════════════════════════════════════════════════════════════════
# Stat fetcher
# ═════════════════════════════════════════════════════════════════════

def get_player_stats(player_id, is_pitcher=False, calc_db=None):
    """Fetch stats for a player from the calculated DB.

    Returns dict with 'current' (latest season) and 'career' (all seasons).
    """
    calc_db = calc_db or _app_paths.CALC_DB
    if not os.path.exists(calc_db):
        return None
    try:
        conn = sqlite3.connect(calc_db, timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if is_pitcher:
            cur.execute("""
                SELECT season, plate_appearances, outs_recorded,
                       strikeouts, walks, hits_allowed, home_runs_allowed,
                       earned_runs, k_pct, bb_pct, era, whip,
                       whiff_pct, barrel_pct, xoba_against, babip_against,
                       slg_against, hard_pct, soft_pct, ld_pct,
                       avg_velo, top_velo, innings_pitched, contact_pct, zone_pct
                FROM calculated_pitching_stats
                WHERE player_id = ? AND matchup = 'all' AND window = 'season'
                ORDER BY season DESC
            """, (player_id,))
        else:
            cur.execute("""
                SELECT season, plate_appearances, at_bats, hits,
                       singles, doubles, triples, home_runs,
                       runs, rbis, total_bases, walks, strikeouts,
                       avg, obp, slg, k_pct, bb_pct, iso,
                       barrel_pct, pulled_air_pct, fb_pct, ev50, max_ev
                FROM calculated_batting_stats
                WHERE player_id = ? AND matchup = 'all' AND window = 'season'
                ORDER BY season DESC
            """, (player_id,))

        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        if not rows:
            return None
        return {'current': rows[0], 'seasons': rows}
    except Exception:
        log.exception("get_player_stats")
        return None


# ═════════════════════════════════════════════════════════════════════
# Stat card helpers
# ═════════════════════════════════════════════════════════════════════

def _mk_label(text, color=None, size=10, bold=False, align=None):
    lbl = QLabel(str(text))
    lbl.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
    c = color or C["t1"]
    w = "700" if bold else "400"
    lbl.setStyleSheet(
        f"color:{c}; background:transparent;"
        f"font-family:'Segoe UI'; font-size:{size}px; font-weight:{w};")
    if align:
        lbl.setAlignment(align)
    return lbl


def _stat_tile(label, value, highlight=False):
    """Small stat box: value on top, label below."""
    tile = QFrame()
    tile.setFixedSize(88, 58)
    tile.setStyleSheet(
        f"background:{C['bg2']}; border:1px solid {C['bdr']}; border-radius:6px;")
    vl = QVBoxLayout(tile)
    vl.setContentsMargins(4, 4, 4, 4)
    vl.setSpacing(1)
    vc = C["ora"] if highlight else C["t1"]
    val_lbl = _mk_label(value, color=vc, size=15, bold=True,
                         align=Qt.AlignmentFlag.AlignCenter)
    vl.addWidget(val_lbl)
    lbl = _mk_label(label, color=C["t3"], size=9,
                     align=Qt.AlignmentFlag.AlignCenter)
    vl.addWidget(lbl)
    return tile


def _fmt_pct(v):
    if v is None:
        return ""
    return f"{v * 100:.1f}%" if abs(v) < 1 else f"{v:.1f}%"


def _fmt3(v):
    return f"{v:.3f}" if v is not None else ""


def _fmt_era(v):
    return f"{v:.2f}" if v is not None else ""


def _fmt1(v):
    return f"{v:.1f}" if v is not None else ""


# ═════════════════════════════════════════════════════════════════════
# Hit direction data
# ═════════════════════════════════════════════════════════════════════

def _compute_hit_zones(rows):
    """Vectorized 5-zone spray computation from list of (hc_x, hc_y) tuples."""
    if not rows:
        return [0.0] * 5, 0
    arr = np.array(rows, dtype=float)
    hc_x = arr[:, 0]
    hc_y = arr[:, 1]
    spray = np.degrees(np.arctan2(hc_x - 125.42, 198.27 - hc_y))
    mask = (spray >= -45) & (spray <= 45)
    spray = spray[mask]
    zones = np.zeros(5, dtype=int)
    zones[0] = int(np.sum(spray < -34))
    zones[1] = int(np.sum((spray >= -34) & (spray < -12)))
    zones[2] = int(np.sum((spray >= -12) & (spray <= 12)))
    zones[3] = int(np.sum((spray > 12) & (spray <= 34)))
    zones[4] = int(np.sum(spray > 34))
    total = int(zones.sum())
    pcts = [z / total for z in zones] if total else [0.0] * 5
    return pcts, total


def get_hit_zones(player_id, p_throws_filter=None, vs_pitcher_id=None,
                  since_season=None, db_path=None, conn=None):
    """Compute 5-zone hit direction percentages from raw plate appearances.

    Zones map to field positions left-to-right as seen from home plate:
      [far_left, left_center, center, right_center, far_right].
    Returns dict: {'zones': [5 floats 0-1], 'total': int}.

    *conn* — optional existing sqlite3 connection to reuse (not closed by this fn).
    """
    db_path = db_path or _app_paths.RAW_DB
    _own_conn = conn is None
    if _own_conn:
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path, timeout=10)
        except Exception:
            return None
    try:
        cur = conn.cursor()
        where = ["batter_id = ?", "hc_x IS NOT NULL", "hc_y IS NOT NULL",
                 "bb_type IN ('ground_ball','fly_ball','line_drive')"]
        params = [player_id]
        if since_season:
            where.append("game_date >= ?")
            params.append(f"{since_season}-01-01")
        if p_throws_filter in ('R', 'L'):
            where.append("p_throws = ?")
            params.append(p_throws_filter)
        if vs_pitcher_id:
            where.append("pitcher_id = ?")
            params.append(vs_pitcher_id)
        cur.execute(
            f"SELECT hc_x, hc_y FROM plate_appearances "
            f"WHERE {' AND '.join(where)}", params)
        rows = cur.fetchall()
        if not rows:
            return {'zones': [0.0] * 5, 'total': 0}
        pcts, total = _compute_hit_zones(rows)
        return {'zones': pcts, 'total': total}
    except Exception:
        log.exception("get_hit_zones")
        return None
    finally:
        if _own_conn and conn:
            conn.close()


def get_pitcher_hit_zones(pitcher_id, stand_filter=None,
                          since_season=None, db_path=None, conn=None):
    """Compute 5-zone hit direction percentages for hits *allowed* by a pitcher.

    Same zone logic as get_hit_zones but queries by pitcher_id.
    stand_filter: 'L' or 'R' to filter by batter handedness.
    *conn* — optional existing sqlite3 connection to reuse (not closed by this fn).
    """
    db_path = db_path or _app_paths.RAW_DB
    _own_conn = conn is None
    if _own_conn:
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path, timeout=10)
        except Exception:
            return None
    try:
        cur = conn.cursor()
        where = ["pitcher_id = ?", "hc_x IS NOT NULL", "hc_y IS NOT NULL",
                 "bb_type IN ('ground_ball','fly_ball','line_drive')"]
        params = [pitcher_id]
        if since_season:
            where.append("game_date >= ?")
            params.append(f"{since_season}-01-01")
        if stand_filter in ('R', 'L'):
            where.append("stand = ?")
            params.append(stand_filter)
        cur.execute(
            f"SELECT hc_x, hc_y FROM plate_appearances "
            f"WHERE {' AND '.join(where)}", params)
        rows = cur.fetchall()
        if not rows:
            return {'zones': [0.0] * 5, 'total': 0}
        pcts, total = _compute_hit_zones(rows)
        return {'zones': pcts, 'total': total}
    except Exception:
        log.exception("get_pitcher_hit_zones")
        return None
    finally:
        if _own_conn and conn:
            conn.close()


# ═════════════════════════════════════════════════════════════════════
# Stadium outline data
# ═════════════════════════════════════════════════════════════════════

_STADIUMS_CSV = os.path.join(
    os.path.dirname(pybaseball.__file__), "data", "mlbstadiums.csv")
_stadium_df = None


def _load_stadiums():
    global _stadium_df
    if _stadium_df is None and os.path.exists(_STADIUMS_CSV):
        _stadium_df = pd.read_csv(_STADIUMS_CSV)
    return _stadium_df


# MLB team abbreviation → pybaseball stadium key
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
}


def _get_stadium_segments(team_abbrev):
    """Return (segments_dict, venue_name) for a team."""
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


# ═════════════════════════════════════════════════════════════════════
# Spray chart widget
# ═════════════════════════════════════════════════════════════════════

def _zone_color(pct):
    """Return QColor on a gradient based on hit-zone percentage (0.0–1.0).

    Green gradient: 10%–40%+  (dark→light green)
    Red gradient:   0%–9%     (light→dark red)
    """
    p = pct * 100
    if p >= 10:
        # Green gradient: 10% = darkest, 40%+ = lightest
        t = min((p - 10) / 30.0, 1.0)  # 0.0 at 10%, 1.0 at 40%
        r = int(20 + t * 50)            # 20 → 70
        g = int(100 + t * 130)          # 100 → 230
        b = int(40 + t * 80)            # 40 → 120
        return QColor(r, g, b, 150)
    else:
        # Red gradient: 0% = lightest, 9% = darkest
        t = min(p / 9.0, 1.0)          # 0.0 at 0%, 1.0 at 9%
        r = int(210 - t * 90)          # 210 → 120
        g = int(70 - t * 45)           # 70 → 25
        b = int(60 - t * 35)           # 60 → 25
        return QColor(r, g, b, 150)


# Statcast coordinate anchor
_HP_X, _HP_Y = 125.42, 198.27


class _SprayFanWidget(QWidget):
    """Custom-painted 5-zone spray fan overlaid on stadium outline."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(260, 240)
        self._zones = [0.0] * 5
        self._total = 0
        self._segments = None   # stadium outline data
        self._venue_name = None

    def set_stadium(self, team_abbrev):
        self._segments, self._venue_name = _get_stadium_segments(team_abbrev)
        self.update()

    def set_data(self, zones, total):
        self._zones = zones
        self._total = total
        self.update()

    def _to_canvas(self, sx, sy):
        """Convert Statcast coords → widget pixel coords."""
        w, h = self.width(), self.height()
        # Worst-case stadium bounds (all 30 parks):
        #   X: 22.6–230.8  Y: 21.3–234.0  HP: (125.42, 198.27)
        # Need to fit: 105.4 right of HP, 102.8 left, 177.0 above, 35.7 below
        pad = 8
        usable_w = w - 2 * pad
        usable_h = h - 2 * pad
        # Scale to fit the full statcast range (0–250) in both axes
        scale = min(usable_w / 210.0, usable_h / 215.0)
        # Center horizontally, anchor home plate near bottom
        cx = w / 2
        cy = pad + 177.0 * scale  # top pad + distance from top of field to HP
        px = cx + (sx - _HP_X) * scale
        py = cy - (_HP_Y - sy) * scale
        return px, py

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        w, h = self.width(), self.height()
        hpx, hpy = self._to_canvas(_HP_X, _HP_Y)

        # ── Draw stadium outline ──
        if self._segments:
            for seg_name in ("outfield_outer", "outfield_inner",
                             "infield_outer", "infield_inner"):
                pts = self._segments.get(seg_name)
                if not pts:
                    continue
                is_outer = seg_name == "outfield_outer"
                pen_c = QColor(C["t2"]) if is_outer else QColor(C["t3"])
                pen_w = 1.8 if is_outer else 1.0
                p.setPen(QPen(pen_c, pen_w))
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
                p.setPen(QPen(QColor(C["t3"]), 1.0))
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
            p.setPen(QPen(QColor(C["t3"]), 1.2))
            p.setBrush(Qt.BrushStyle.NoBrush)
            arc = QPainterPath()
            radius = min(w, h) * 0.72
            for s in range(91):
                a = math.radians(-45 + s)
                px = hpx + radius * math.sin(a)
                py = hpy - radius * math.cos(a)
                if s == 0:
                    arc.moveTo(px, py)
                else:
                    arc.lineTo(px, py)
            p.drawPath(arc)

        # ── Wedges extend beyond wall ──
        # Radius must fit: horizontally (±45°) and vertically (0°)
        max_r_horiz = (hpx - 4) / math.sin(math.radians(45))  # left/right
        max_r_vert = hpy - 4                                    # top
        base_radius = min(max_r_horiz, max_r_vert)

        # Zone angles: evenly split 5 × 18° across the 90° fan
        angles = [(-45, -27), (-27, -9), (-9, 9), (9, 27), (27, 45)]

        # ── Draw transparent wedges ──
        for i, (a0_deg, a1_deg) in enumerate(angles):
            path = QPainterPath()
            path.moveTo(hpx, hpy)
            steps = 40
            for s in range(steps + 1):
                a = math.radians(a0_deg + (a1_deg - a0_deg) * s / steps)
                px = hpx + base_radius * math.sin(a)
                py = hpy - base_radius * math.cos(a)
                path.lineTo(px, py)
            path.closeSubpath()

            p.setBrush(_zone_color(self._zones[i]))
            p.setPen(Qt.PenStyle.NoPen)
            p.drawPath(path)

            # Percentage label — positioned near outer edge
            pct = self._zones[i]
            if pct > 0.005:
                mid = math.radians((a0_deg + a1_deg) / 2)
                lr = base_radius * 0.75
                lx = hpx + lr * math.sin(mid)
                ly = hpy - lr * math.cos(mid)
                font = p.font()
                font.setFamily("Segoe UI")
                font.setPixelSize(13)
                font.setBold(True)
                p.setFont(font)
                p.setPen(QColor("white"))
                p.drawText(QRectF(lx - 22, ly - 11, 44, 22),
                           Qt.AlignmentFlag.AlignCenter,
                           f"{pct * 100:.0f}%")

        # ── Stadium name bottom-left ──
        if self._venue_name:
            font = p.font()
            font.setFamily("Segoe UI")
            font.setPixelSize(11)
            font.setBold(False)
            p.setFont(font)
            p.setPen(QColor(C["t3"]))
            p.drawText(6, h - 6, self._venue_name)

        p.end()


_FILTER_BTN_STYLE = f"""
    QPushButton {{
        background: transparent;
        color: {C['t3']};
        border: 1px solid transparent;
        border-radius: 10px;
        padding: 2px 8px;
        font-family: 'Segoe UI';
        font-size: 10px;
    }}
    QPushButton:checked {{
        color: {C['t1']};
        border: 1px solid {C['ora']};
    }}
    QPushButton:hover:!checked {{
        color: {C['t2']};
    }}
"""


# ─────────────────────────────────────────────────────────────────────
# Game Log Data
# ─────────────────────────────────────────────────────────────────────

def get_game_log(player_id, num_games=None, season=None, db_path=None, team=None):
    """Return per-game batting stats, most recent first."""
    try:
        db = db_path or _app_paths.RAW_DB
        conn = sqlite3.connect(db)
        where = ["pa.batter_id = ?"]
        params = [player_id]
        if season:
            where.append("pa.season = ?")
            params.append(season)
        rows = conn.execute(f"""
            SELECT pa.game_date, pa.game_id, g.home_team, g.away_team,
                   COUNT(*) as pa_count,
                   SUM(pa.is_hit) as hits,
                   SUM(pa.total_bases) as tb,
                   SUM(pa.is_home_run) as hr,
                   SUM(pa.is_single) as singles,
                   SUM(pa.is_double) as doubles,
                   SUM(pa.is_triple) as triples,
                   SUM(pa.rbi) as rbi,
                   SUM(pa.runs) as runs,
                   SUM(pa.is_strikeout) as k,
                   SUM(pa.is_walk) as bb,
                   SUM(CASE WHEN pa.launch_speed >= 95 THEN 1 ELSE 0 END) as hard_hit,
                   SUM(CASE WHEN pa.launch_speed IS NOT NULL THEN 1 ELSE 0 END) as bbe
            FROM plate_appearances pa
            JOIN games g ON pa.game_id = g.game_id
            WHERE {' AND '.join(where)}
            GROUP BY pa.game_id
            ORDER BY pa.game_date DESC
        """, params).fetchall()

        # Stolen bases per game from separate table
        sb_where = ["runner_id = ?", "event_type = 'stolen_base'", "is_successful = 1"]
        sb_params = [player_id]
        if season:
            sb_where.append("season = ?")
            sb_params.append(season)
        sb_rows = conn.execute(f"""
            SELECT game_id, COUNT(*) as sb
            FROM stolen_bases
            WHERE {' AND '.join(sb_where)}
            GROUP BY game_id
        """, sb_params).fetchall()
        sb_map = {r[0]: r[1] for r in sb_rows}

        conn.close()
        games = []
        for r in rows:
            date, gid, home, away, pa_count, hits, tb, hr, s1, d, t, rbi, runs, k, bb, hh, bbe = r
            if team:
                opp = away if team == home else f"@{home}"
            else:
                opp = away  # fallback
            month_day = date[5:]  # "MM-DD" -> "M/D"
            parts = month_day.split('-')
            short_date = f"{int(parts[0])}/{int(parts[1])}"
            games.append({
                'date': date, 'short_date': short_date, 'opp': opp,
                'game_id': str(gid), 'pa': pa_count or 0,
                'hits': hits or 0, 'tb': tb or 0, 'hr': hr or 0,
                '1b': s1 or 0, '2b': d or 0, '3b': t or 0,
                'rbi': rbi or 0, 'runs': runs or 0, 'k': k or 0,
                'bb': bb or 0, 'sb': sb_map.get(str(gid), 0),
                'hrr': (hits or 0) + (runs or 0) + (rbi or 0),
            })
        if num_games and not season:
            games = games[:num_games]
        return games
    except Exception:
        log.exception("get_game_log")
        return []


def get_pitcher_game_log(player_id, num_games=None, season=None,
                         db_path=None, team=None):
    """Return per-game pitching stats, most recent first."""
    try:
        db = db_path or _app_paths.RAW_DB
        conn = sqlite3.connect(db)
        where = ["pa.pitcher_id = ?"]
        params = [player_id]
        if season:
            where.append("pa.season = ?")
            params.append(season)
        rows = conn.execute(f"""
            SELECT pa.game_date, pa.game_id, g.home_team, g.away_team,
                   COUNT(*) as bf,
                   SUM(pa.is_strikeout) as k,
                   SUM(pa.is_walk) as bb,
                   SUM(pa.is_hit) as h,
                   SUM(pa.is_single) as singles,
                   SUM(pa.is_double) as doubles,
                   SUM(pa.is_triple) as triples,
                   SUM(pa.is_home_run) as hr,
                   SUM(pa.earned_runs) as er,
                   SUM(pa.outs_recorded) as outs
            FROM plate_appearances pa
            JOIN games g ON pa.game_id = g.game_id
            WHERE {' AND '.join(where)}
            GROUP BY pa.game_id
            ORDER BY pa.game_date DESC
        """, params).fetchall()

        # SB given up per game
        sb_where = ["pitcher_id = ?", "event_type = 'stolen_base'",
                     "is_successful = 1"]
        sb_params = [player_id]
        if season:
            sb_where.append("season = ?")
            sb_params.append(season)
        sb_rows = conn.execute(f"""
            SELECT game_id, COUNT(*) as sb
            FROM stolen_bases
            WHERE {' AND '.join(sb_where)}
            GROUP BY game_id
        """, sb_params).fetchall()
        sb_map = {r[0]: r[1] for r in sb_rows}

        conn.close()
        games = []
        for r in rows:
            (date, gid, home, away, bf, k, bb, h,
             s1, d, t, hr, er, outs) = r
            if team:
                opp = away if team == home else f"@{home}"
            else:
                opp = away
            parts = date[5:].split('-')
            short_date = f"{int(parts[0])}/{int(parts[1])}"
            games.append({
                'date': date, 'short_date': short_date, 'opp': opp,
                'game_id': str(gid), 'bf': bf or 0,
                'k': k or 0, 'bb': bb or 0,
                'h': h or 0, '1b': s1 or 0, '2b': d or 0,
                '3b': t or 0, 'hr': hr or 0,
                'er': er or 0, 'outs': outs or 0,
                'sb': sb_map.get(str(gid), 0),
            })
        if num_games and not season:
            games = games[:num_games]
        return games
    except Exception:
        log.exception("get_pitcher_game_log")
        return []


def get_recent_pa(player_id, limit=10, team=None, db_path=None):
    """Return the most recent individual plate appearances."""
    _NON_PA_EVENTS = {
        'pickoff_1b', 'pickoff_2b', 'pickoff_3b',
        'pickoff caught stealing 2b', 'pickoff caught stealing 3b',
        'caught stealing 2b', 'caught stealing 3b', 'caught stealing home',
        'caught_stealing_2b', 'caught_stealing_3b', 'caught_stealing_home',
        'stolen_base_2b', 'stolen_base_3b', 'stolen_base_home',
        'balk', 'wild_pitch', 'passed_ball', 'truncated_pa',
    }
    try:
        db = db_path or _app_paths.RAW_DB
        conn = sqlite3.connect(db)
        rows = conn.execute("""
            SELECT pa.game_date, g.home_team, g.away_team,
                   pa.pitch_name, pa.release_speed,
                   pa.events, pa.description, pa.des,
                   pa.launch_speed, pa.pitcher_name
            FROM plate_appearances pa
            JOIN games g ON pa.game_id = g.game_id
            WHERE pa.batter_id = ?
              AND pa.events IS NOT NULL
            ORDER BY pa.game_date DESC, pa.at_bat_number DESC
            LIMIT ?
        """, (player_id, limit + 20)).fetchall()
        conn.close()
        results = []
        for r in rows:
            if len(results) >= limit:
                break
            (date, home, away, pitch_name, speed,
             events, description, des, ev, pitcher) = r
            # Skip non-PA events
            if events and events.lower() in _NON_PA_EVENTS:
                continue
            parts = date[5:].split('-')
            short_date = f"{int(parts[0])}/{int(parts[1])}"
            matchup = f"{away} @ {home}"
            # Pitch info: name + speed
            pitch_str = ""
            if pitch_name and speed:
                pitch_str = f"{pitch_name} {speed:.0f} mph"
            elif pitch_name:
                pitch_str = pitch_name
            elif speed:
                pitch_str = f"{speed:.0f} mph"
            # Result: prefer narrative description, fall back to event name
            # Narrative is in 'description' (from game feed) or 'des' (from statcast)
            narrative = None
            if description and not description.replace('_', '').isalpha() and len(description) > 20:
                narrative = description
            elif des and len(des) > 10 and not des.startswith(('Called', 'Ball', 'Foul', 'Swinging')):
                narrative = des
            if narrative:
                event_str = narrative
            else:
                event_str = (events or "").replace("_", " ").title()
            # EV
            ev_str = f"{ev:.1f}" if ev else "--"
            results.append({
                'date': short_date, 'matchup': matchup,
                'pitch': pitch_str, 'event': event_str,
                'desc': des or description or '', 'ev': ev_str,
                'pitcher': pitcher or '',
            })
        return results
    except Exception:
        log.exception("get_recent_pa")
        return []


def get_recent_pa_against(pitcher_id, limit=10, team=None, db_path=None):
    """Return the most recent individual plate appearances faced by a pitcher."""
    _NON_PA_EVENTS = {
        'pickoff_1b', 'pickoff_2b', 'pickoff_3b',
        'pickoff caught stealing 2b', 'pickoff caught stealing 3b',
        'caught stealing 2b', 'caught stealing 3b', 'caught stealing home',
        'caught_stealing_2b', 'caught_stealing_3b', 'caught_stealing_home',
        'stolen_base_2b', 'stolen_base_3b', 'stolen_base_home',
        'balk', 'wild_pitch', 'passed_ball', 'truncated_pa',
    }
    try:
        db = db_path or _app_paths.RAW_DB
        conn = sqlite3.connect(db)
        rows = conn.execute("""
            SELECT pa.game_date, g.home_team, g.away_team,
                   pa.pitch_name, pa.release_speed,
                   pa.events, pa.description, pa.des,
                   pa.launch_speed, pa.batter_name, pa.batter_id
            FROM plate_appearances pa
            JOIN games g ON pa.game_id = g.game_id
            WHERE pa.pitcher_id = ?
              AND pa.events IS NOT NULL
            ORDER BY pa.game_date DESC, pa.at_bat_number DESC
            LIMIT ?
        """, (pitcher_id, limit + 20)).fetchall()
        conn.close()
        results = []
        for r in rows:
            if len(results) >= limit:
                break
            (date, home, away, pitch_name, speed,
             events, description, des, ev, batter_name, batter_id) = r
            if events and events.lower() in _NON_PA_EVENTS:
                continue
            parts = date[5:].split('-')
            short_date = f"{int(parts[0])}/{int(parts[1])}"
            matchup = f"{away} @ {home}"
            pitch_str = ""
            if pitch_name and speed:
                pitch_str = f"{pitch_name} {speed:.0f} mph"
            elif pitch_name:
                pitch_str = pitch_name
            elif speed:
                pitch_str = f"{speed:.0f} mph"
            narrative = None
            if description and not description.replace('_', '').isalpha() and len(description) > 20:
                narrative = description
            elif des and len(des) > 10 and not des.startswith(('Called', 'Ball', 'Foul', 'Swinging')):
                narrative = des
            if narrative:
                event_str = narrative
            else:
                event_str = (events or "").replace("_", " ").title()
            ev_str = f"{ev:.1f}" if ev else "--"
            # Shorten batter name to "F. Last"
            bname = batter_name or ""
            if bname and " " in bname:
                first, last = bname.split(" ", 1)
                bname = f"{first[0]}. {last}"
            results.append({
                'date': short_date, 'matchup': matchup,
                'batter': bname,
                'pitch': pitch_str, 'event': event_str,
                'desc': des or description or '', 'ev': ev_str,
            })
        return results
    except Exception:
        log.exception("get_recent_pa_against")
        return []


# ─────────────────────────────────────────────────────────────────────
# Game Log Bar Chart Widget
# ─────────────────────────────────────────────────────────────────────

class _BarChartCanvas(QWidget):
    """Custom-painted bar chart for game log stats."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(180)
        self.setMouseTracking(True)
        self._games = []      # list of game dicts (oldest first for display)
        self._stat_key = 'hits'
        self._avg = 0.0
        self._series_labels = False  # only label first game of each series
        # Cached layout for hit-testing
        self._bar_x_offset = 0
        self._bar_spacing = 0
        self._bar_n = 0

    def set_data(self, games, stat_key):
        self._games = list(reversed(games))  # oldest first for L-to-R
        self._stat_key = stat_key
        vals = [g[stat_key] for g in self._games]
        self._avg = sum(vals) / len(vals) if vals else 0
        self.update()

    def paintEvent(self, event):
        if not self._games:
            return
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        w, h = self.width(), self.height()
        left_margin = 24
        bottom_margin = 36
        top_margin = 14
        right_margin = 8

        chart_w = w - left_margin - right_margin
        chart_h = h - top_margin - bottom_margin

        n = len(self._games)
        vals = [g[self._stat_key] for g in self._games]
        max_val = max(max(vals), 1)

        # Nice y-axis ceiling
        if max_val <= 5:
            y_max = max_val
        elif max_val <= 10:
            y_max = max_val + (1 if max_val % 2 else 0)
        else:
            # Round up to nearest 10 for percentages/large values
            y_max = ((max_val // 5) + 1) * 5

        # Bar sizing: max width for few games, shrinks for many, L30 = min
        MAX_BAR_W = 32
        bar_spacing = min(chart_w / n, MAX_BAR_W / 0.85)
        bar_w = bar_spacing * 0.85
        # Center bars when they don't fill the full width
        total_bars_w = bar_spacing * n
        x_offset = left_margin + (chart_w - total_bars_w) / 2

        # Cache layout for hit-testing
        self._bar_x_offset = x_offset
        self._bar_spacing = bar_spacing
        self._bar_n = n

        # Y-axis labels and grid lines
        font = p.font()
        font.setFamily("Segoe UI")
        font.setPixelSize(11)
        p.setFont(font)

        # Decide how many y-axis ticks
        if y_max <= 5:
            y_ticks = list(range(y_max + 1))
        elif y_max <= 10:
            y_ticks = list(range(0, y_max + 1, 2))
            if y_max not in y_ticks:
                y_ticks.append(y_max)
        else:
            step = max(y_max // 4, 5)
            step = (step // 5) * 5 or 5
            y_ticks = list(range(0, y_max + 1, step))
            if y_max not in y_ticks:
                y_ticks.append(y_max)

        for v in y_ticks:
            y = top_margin + chart_h - (v / y_max * chart_h) if y_max else top_margin + chart_h
            p.setPen(QColor(C["t3"]))
            p.drawText(QRectF(0, y - 8, left_margin - 4, 16),
                       Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                       str(v))
            if v > 0:
                p.setPen(QPen(QColor(C["bg3"]), 1))
                p.drawLine(int(left_margin), int(y), int(w - right_margin), int(y))

        # Betting line: over/under at ceil(avg) - 0.5
        if self._avg > 0:
            import math
            line_val = max(0.5, math.ceil(self._avg) - 0.5)
            line_y = top_margin + chart_h - (line_val / y_max * chart_h)
            pen = QPen(QColor(C["t2"]), 1.5, Qt.PenStyle.DashLine)
            p.setPen(pen)
            p.drawLine(int(left_margin), int(line_y),
                       int(w - right_margin), int(line_y))

        # Bars
        bar_color = QColor(C["grn"])
        bar_color.setAlpha(153)  # ~60% opacity

        # Pre-compute series label positions to detect overlaps
        label_positions = []
        if self._series_labels:
            for i, g in enumerate(self._games):
                if i == 0 or g['opp'] != self._games[i - 1]['opp']:
                    cx = x_offset + bar_spacing * (i + 0.5)
                    label_positions.append((i, cx))

        for i, g in enumerate(self._games):
            val = g[self._stat_key]
            cx = x_offset + bar_spacing * (i + 0.5)
            bx = cx - bar_w / 2
            bar_h = (val / y_max * chart_h) if y_max else 0
            by = top_margin + chart_h - bar_h

            if bar_h > 0:
                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(bar_color)
                p.drawRoundedRect(QRectF(bx, by, bar_w, bar_h), 2, 2)
            else:
                # Grey stub so zero-value games are still visible
                stub_h = 4
                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(QColor(C["t3"]))
                p.drawRoundedRect(QRectF(bx, top_margin + chart_h - stub_h,
                                         bar_w, stub_h), 1, 1)

            # Value label inside bar near top
            if val > 0:
                fsize = 11 if bar_spacing >= 18 else 9
                font.setPixelSize(fsize)
                font.setBold(True)
                p.setFont(font)
                p.setPen(QColor("white"))
                label_y = by + 2
                p.drawText(QRectF(cx - 16, label_y, 32, fsize + 2),
                           Qt.AlignmentFlag.AlignCenter, str(val))

            # X-axis label
            if self._series_labels:
                # Only label first game of each series, skip if too close to previous label
                is_series_start = (i == 0 or g['opp'] != self._games[i - 1]['opp'])
                if is_series_start:
                    # Check distance to previous label
                    idx_in_labels = [j for j, (gi, _) in enumerate(label_positions) if gi == i]
                    if idx_in_labels:
                        li = idx_in_labels[0]
                        prev_cx = label_positions[li - 1][1] if li > 0 else -999
                        min_gap = max(bar_spacing * 1.5, 28)
                        show_label = (cx - prev_cx) >= min_gap
                    else:
                        show_label = False
                else:
                    show_label = False
            else:
                show_label = True
            if show_label:
                font.setPixelSize(9)
                font.setBold(False)
                p.setFont(font)
                p.setPen(QColor(C["t2"]))
                p.drawText(QRectF(cx - 24, top_margin + chart_h + 2, 48, 14),
                           Qt.AlignmentFlag.AlignCenter, g['opp'])
                p.drawText(QRectF(cx - 24, top_margin + chart_h + 14, 48, 14),
                           Qt.AlignmentFlag.AlignCenter, g['short_date'])

        p.end()

    def mouseMoveEvent(self, event):
        if not self._games or self._bar_spacing <= 0:
            self.setToolTip('')
            return
        pos = event.position() if hasattr(event, 'position') else event.pos()
        x = pos.x() - self._bar_x_offset
        idx = int(x / self._bar_spacing)
        if 0 <= idx < self._bar_n:
            g = self._games[idx]
            self.setToolTip(f"{g['opp']}  \u00b7  {g['short_date']}")
        else:
            self.setToolTip('')


_STAT_FILTER_STYLE = f"""
    QPushButton {{
        background: transparent;
        color: {C['t3']};
        border: 1px solid transparent;
        border-radius: 10px;
        padding: 2px 8px;
        font-family: 'Segoe UI';
        font-size: 10px;
    }}
    QPushButton:checked {{
        color: {C['t1']};
        border: 1px solid {C['ora']};
    }}
    QPushButton:hover:!checked {{
        color: {C['t2']};
    }}
"""

_RANGE_FILTER_STYLE = f"""
    QPushButton {{
        background: transparent;
        color: {C['t3']};
        border: 1px solid transparent;
        border-radius: 10px;
        padding: 2px 8px;
        font-family: 'Segoe UI';
        font-size: 10px;
    }}
    QPushButton:checked {{
        color: {C['t1']};
        border: 1px solid {C['ora']};
    }}
    QPushButton:hover:!checked {{
        color: {C['t2']};
    }}
"""


_BATTER_STAT_FILTERS = [
    ('hits', 'HITS'), ('hrr', 'HRR'), ('tb', 'TB'),
    ('hr', 'HR'), ('1b', '1B'), ('2b', '2B'),
    ('3b', '3B'), ('rbi', 'RBI'), ('runs', 'RUNS'),
    ('bb', 'BB'), ('sb', 'SB'), ('k', 'K'),
]

_PITCHER_STAT_FILTERS = [
    ('k', 'K'), ('bb', 'BB'), ('h', 'H'), ('1b', '1B'),
    ('2b', '2B'), ('3b', '3B'), ('hr', 'HR'),
    ('er', 'ER'), ('outs', 'OUTS'), ('sb', 'SB'),
]


class GameLogBarChart(QWidget):
    """Bar chart of per-game stats with stat + range filters."""

    def __init__(self, player_id, team=None, is_pitcher=False, parent=None):
        super().__init__(parent)
        self._player_id = player_id
        self._team = team
        self._is_pitcher = is_pitcher
        stat_filters = _PITCHER_STAT_FILTERS if is_pitcher else _BATTER_STAT_FILTERS
        default_stat = stat_filters[0][0]
        self._stat_key = default_stat
        self._range_key = '2026'
        self._all_games = {}   # cache: range_key -> games list

        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(4)

        # Stat filter row
        stat_row = QHBoxLayout()
        stat_row.setSpacing(3)
        self._stat_btns = {}
        for key, label in stat_filters:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setStyleSheet(_STAT_FILTER_STYLE)
            btn.clicked.connect(lambda _, k=key: self._on_stat(k))
            stat_row.addWidget(btn)
            self._stat_btns[key] = btn
        stat_row.addStretch()
        vl.addLayout(stat_row)

        # Bar chart canvas
        self._canvas = _BarChartCanvas()
        vl.addWidget(self._canvas, stretch=1)

        # Range filter row
        range_row = QHBoxLayout()
        range_row.setSpacing(3)
        self._range_btns = {}
        for key, label in [('2026', '2026'), ('5', 'Last 5'),
                           ('10', 'Last 10'), ('20', 'Last 20'),
                           ('30', 'Last 30')]:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setStyleSheet(_RANGE_FILTER_STYLE)
            btn.clicked.connect(lambda _, k=key: self._on_range(k))
            range_row.addWidget(btn)
            self._range_btns[key] = btn
        range_row.addStretch()
        vl.addLayout(range_row)

        # Initial load
        self._on_stat(default_stat)
        self._on_range('2026')

    def _fetch_games(self, range_key):
        if range_key in self._all_games:
            return self._all_games[range_key]
        fetch = get_pitcher_game_log if self._is_pitcher else get_game_log
        if range_key == '2026':
            games = fetch(self._player_id, season=2026, team=self._team)
        else:
            games = fetch(self._player_id, num_games=int(range_key), team=self._team)
        self._all_games[range_key] = games
        return games

    def _refresh(self):
        games = self._fetch_games(self._range_key)
        self._canvas.set_data(games, self._stat_key)

    def _on_stat(self, key):
        self._stat_key = key
        for k, btn in self._stat_btns.items():
            btn.setChecked(k == key)
        self._refresh()

    def _on_range(self, key):
        self._range_key = key
        for k, btn in self._range_btns.items():
            btn.setChecked(k == key)
        self._canvas._series_labels = key in ('20', '30')
        self._refresh()


class SprayChartWidget(QWidget):
    """Title + 5-zone spray fan on stadium outline + filter tabs."""

    def __init__(self, player_id, stand='R', team='generic',
                 vs_pitcher=None, since_season=2023, parent=None):
        super().__init__(parent)
        self._player_id = player_id
        self._stand = stand
        self._since_season = since_season
        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(6)

        # Title
        title_row = QHBoxLayout()
        title_row.addWidget(
            _mk_label("Hit Locations", color=C["ora"], size=11, bold=True))
        title_row.addStretch()
        since_label = _mk_label(f"Since {since_season}", color=C["t2"], size=10)
        title_row.addWidget(since_label)
        vl.addLayout(title_row)

        # Fan with stadium overlay
        self._fan = _SprayFanWidget()
        self._fan.set_stadium(team)
        vl.addWidget(self._fan, stretch=1)

        # Filter tabs
        tab_row = QHBoxLayout()
        tab_row.setSpacing(4)
        self._btns = {}
        filters = [('all', 'All Pitchers'), ('R', 'RHP'), ('L', 'LHP')]
        if vs_pitcher:
            filters.append(
                ('sp', f"vs {vs_pitcher.get('name', 'SP')}"))
        for key, label in filters:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setStyleSheet(_FILTER_BTN_STYLE)
            btn.clicked.connect(lambda _, k=key: self._on_filter(k))
            tab_row.addWidget(btn)
            self._btns[key] = btn
        tab_row.addStretch()
        vl.addLayout(tab_row)

        self._vs_pitcher_id = (
            vs_pitcher.get('id') if vs_pitcher else None)
        self._on_filter('all')

    def _on_filter(self, key):
        for k, btn in self._btns.items():
            btn.setChecked(k == key)
        p_throws = key if key in ('R', 'L') else None
        vs_pid = self._vs_pitcher_id if key == 'sp' else None
        data = get_hit_zones(self._player_id,
                             p_throws_filter=p_throws,
                             vs_pitcher_id=vs_pid,
                             since_season=self._since_season)
        if data:
            self._fan.set_data(data['zones'], data['total'])


class PitcherSprayChartWidget(QWidget):
    """Spray chart showing where a pitcher gives up hits, filtered by batter hand."""

    def __init__(self, pitcher_id, team='generic', since_season=2023, parent=None):
        super().__init__(parent)
        self._pitcher_id = pitcher_id
        self._since_season = since_season
        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(6)

        # Title
        title_row = QHBoxLayout()
        title_row.addWidget(
            _mk_label("Hits Allowed", color=C["ora"], size=11, bold=True))
        title_row.addStretch()
        title_row.addWidget(
            _mk_label(f"Since {since_season}", color=C["t2"], size=10))
        vl.addLayout(title_row)

        # Fan with stadium overlay
        self._fan = _SprayFanWidget()
        self._fan.set_stadium(team)
        vl.addWidget(self._fan, stretch=1)

        # Filter tabs — by batter handedness
        tab_row = QHBoxLayout()
        tab_row.setSpacing(4)
        self._btns = {}
        for key, label in [('all', 'All Batters'), ('R', 'RHB'), ('L', 'LHB')]:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setStyleSheet(_FILTER_BTN_STYLE)
            btn.clicked.connect(lambda _, k=key: self._on_filter(k))
            tab_row.addWidget(btn)
            self._btns[key] = btn
        tab_row.addStretch()
        vl.addLayout(tab_row)
        self._on_filter('all')

    def _on_filter(self, key):
        for k, btn in self._btns.items():
            btn.setChecked(k == key)
        stand = key if key in ('R', 'L') else None
        data = get_pitcher_hit_zones(self._pitcher_id,
                                     stand_filter=stand,
                                     since_season=self._since_season)
        if data:
            self._fan.set_data(data['zones'], data['total'])


# ═════════════════════════════════════════════════════════════════════
# Player Profile Dialog
# ═════════════════════════════════════════════════════════════════════

class PlayerProfileDialog(QDialog):
    """Popup showing player headshot, bio, and stats."""

    def __init__(self, player_info, parent=None):
        """
        player_info: dict with keys {id, name, team, position, hand, is_pitcher}
        position 'P' → pitcher, 'TWP' → two-way toggle, else → batter.
        """
        super().__init__(parent)
        self.setWindowTitle(f"{player_info['name']} — Player Profile")
        self.setMinimumSize(1100, 880)
        self.setStyleSheet(f"""
            QDialog {{
                background: {C['bg0']};
                border: 1px solid {C['bdr']};
            }}
        """)
        self._player_id = player_info['id']
        self._player_info = player_info

        # Determine mode from position
        pos = player_info.get('position', '')
        self._is_twp = pos == 'TWP'
        if self._is_twp:
            self._pitcher_mode = False       # default to batting for TWP
        else:
            self._pitcher_mode = player_info.get('is_pitcher', False)

        main_vl = QVBoxLayout(self)
        main_vl.setContentsMargins(20, 20, 20, 20)
        main_vl.setSpacing(16)
        self._main_vl = main_vl

        # ── Header: headshot + bio ──
        header = QHBoxLayout()
        header.setSpacing(18)

        # Left: headshot + bio
        left_vl = QVBoxLayout()
        left_vl.setSpacing(8)
        head_row = QHBoxLayout()
        head_row.setSpacing(14)

        # Headshot
        self._headshot_label = QLabel()
        self._headshot_label.setFixedSize(HS_W, HS_H)
        self._headshot_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        pm = fetch_headshot(player_info['id'])
        self._headshot_label.setPixmap(pm)
        _headshot_signal.ready.connect(self._on_headshot_ready)
        head_row.addWidget(self._headshot_label)

        # Bio info — enrich from roster CSV
        roster = get_player_roster_info(player_info['id'])
        bio_vl = QVBoxLayout()
        bio_vl.setSpacing(4)

        # Name + jersey number
        name = player_info['name']
        jersey = (roster or {}).get('jersey_number', '')
        name_lbl = name if not jersey else f"{name}  #{jersey}"
        bio_vl.addWidget(_mk_label(name_lbl, size=22, bold=True))

        # Primary line: team · position_type · bats/throws
        sub_parts = []
        team = (roster or {}).get('team') or player_info.get('team', '')
        pos_type = (roster or {}).get('position_type', '')
        pos_abbr = (roster or {}).get('position', '') or player_info.get('position', '')
        bats = (roster or {}).get('bats', '')
        throws = (roster or {}).get('throws', '')
        if team:
            sub_parts.append(team)
        if pos_type and pos_abbr:
            sub_parts.append(f"{pos_type} ({pos_abbr})")
        elif pos_abbr:
            sub_parts.append(pos_abbr)
        if bats or throws:
            bt = []
            if bats:
                bt.append(f"Bats {bats}")
            if throws:
                bt.append(f"Throws {throws}")
            sub_parts.append(" / ".join(bt))
        if sub_parts:
            bio_vl.addWidget(_mk_label("  ·  ".join(sub_parts), color=C["t2"], size=13))

        # Detail line: age · height · weight · debut · country
        detail_parts = []
        age = (roster or {}).get('age', '')
        height = (roster or {}).get('height', '')
        weight = (roster or {}).get('weight', '')
        debut = (roster or {}).get('mlb_debut', '')
        country = (roster or {}).get('birth_country', '')
        if age:
            detail_parts.append(f"Age {age}")
        if height:
            detail_parts.append(height)
        if weight:
            detail_parts.append(f"{weight} lbs")
        if debut:
            try:
                from datetime import datetime as _dt
                debut = _dt.strptime(debut, "%Y-%m-%d").strftime("%m/%d/%Y")
            except Exception:
                pass
            detail_parts.append(f"Debut {debut}")
        if country and country != "USA":
            detail_parts.append(country)
        if detail_parts:
            bio_vl.addWidget(_mk_label("  ·  ".join(detail_parts), color=C["t3"], size=11))

        bio_vl.addStretch()
        head_row.addLayout(bio_vl)
        left_vl.addLayout(head_row)
        left_vl.addStretch()
        header.addLayout(left_vl)

        # Right: stat tiles placeholder (rebuilt on mode switch)
        self._stats_container = QVBoxLayout()
        self._stats_container.setSpacing(8)
        header.addLayout(self._stats_container)

        main_vl.addLayout(header)

        # ── TWP toggle bar ──
        if self._is_twp:
            toggle_row = QHBoxLayout()
            toggle_row.setSpacing(4)
            self._twp_btns = {}
            for key, label in [('batting', 'Batting'), ('pitching', 'Pitching')]:
                btn = QPushButton(label)
                btn.setCheckable(True)
                btn.setCursor(Qt.CursorShape.PointingHandCursor)
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background: {C['bg2']};
                        color: {C['t3']};
                        border: 1px solid {C['bdr']};
                        border-radius: 12px;
                        padding: 4px 18px;
                        font-family: 'Segoe UI','Inter',sans-serif;
                        font-size: 11px;
                        font-weight: 600;
                    }}
                    QPushButton:checked {{
                        color: {C['t1']};
                        border: 1px solid {C['ora']};
                        background: {C['bg3']};
                    }}
                    QPushButton:hover:!checked {{
                        color: {C['t2']};
                    }}
                """)
                btn.clicked.connect(lambda _, k=key: self._on_twp_toggle(k))
                toggle_row.addWidget(btn)
                self._twp_btns[key] = btn
            toggle_row.addStretch()
            main_vl.addLayout(toggle_row)

        # ── Dynamic content area ──
        self._content_widget = None
        self._build_content(self._pitcher_mode)

        # Activate default TWP button
        if self._is_twp:
            self._twp_btns['batting'].setChecked(True)

    def _on_twp_toggle(self, key):
        pitcher = key == 'pitching'
        if pitcher == self._pitcher_mode:
            return
        self._pitcher_mode = pitcher
        for k, btn in self._twp_btns.items():
            btn.setChecked(k == key)
        self._build_content(pitcher)

    def _build_content(self, is_pitcher):
        """Build or rebuild the stats tiles + charts + PA table section."""
        # Remove old content widget
        if self._content_widget is not None:
            self._content_widget.setParent(None)
            self._content_widget.deleteLater()
            self._content_widget = None

        # Clear stat tiles
        while self._stats_container.count():
            item = self._stats_container.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

        # Rebuild stat tiles
        stats_data = get_player_stats(self._player_id, is_pitcher)
        if stats_data:
            current = stats_data['current']
            season = current.get('season', '?')
            self._stats_container.addWidget(
                _mk_label(f"{season} Season Stats", color=C["ora"], size=14, bold=True,
                          align=Qt.AlignmentFlag.AlignRight))
            if is_pitcher:
                self._build_pitching_stats(current)
            else:
                self._build_batting_stats(current)
        self._stats_container.addStretch()

        # Content container
        content = QWidget()
        content_vl = QVBoxLayout(content)
        content_vl.setContentsMargins(0, 0, 0, 0)
        content_vl.setSpacing(16)

        pi = self._player_info

        # ── Charts row: bar chart | spray chart ──
        charts_row = QHBoxLayout()
        charts_row.setSpacing(12)

        # Bar chart panel
        bar_panel = QFrame()
        bar_panel.setStyleSheet(f"""
            QFrame {{
                background: {C['bg1']};
                border: 1px solid {C['bdr']};
                border-radius: 8px;
            }}
        """)
        bar_layout = QVBoxLayout(bar_panel)
        bar_layout.setContentsMargins(10, 8, 10, 8)
        self._bar_chart = GameLogBarChart(
            pi['id'], team=pi.get('team'), is_pitcher=is_pitcher)
        bar_layout.addWidget(self._bar_chart)
        charts_row.addWidget(bar_panel, stretch=1)

        # Spray chart panel
        spray_panel = QFrame()
        spray_panel.setFixedWidth(320)
        spray_panel.setStyleSheet(f"""
            QFrame {{
                background: {C['bg1']};
                border: 1px solid {C['bdr']};
                border-radius: 8px;
            }}
        """)
        spray_layout = QVBoxLayout(spray_panel)
        spray_layout.setContentsMargins(10, 8, 10, 8)
        venue_team = resolve_venue_team(pi.get('team', ''), pi.get('games'))
        if is_pitcher:
            self._spray = PitcherSprayChartWidget(pi['id'], team=venue_team)
        else:
            stand = pi.get('stand', 'R')
            self._spray = SprayChartWidget(
                pi['id'], stand, team=venue_team,
                vs_pitcher=pi.get('vs_pitcher'))
        spray_layout.addWidget(self._spray)
        charts_row.addWidget(spray_panel)

        content_vl.addLayout(charts_row)

        # ── Divider ──
        sep = QFrame()
        sep.setFixedHeight(1)
        sep.setStyleSheet(f"background:{C['bdr']};")
        content_vl.addWidget(sep)

        # ── Recent Plate Appearances table ──
        self._build_pa_table(pi, content_vl, is_pitcher=is_pitcher)

        content_vl.addStretch()

        self._content_widget = content
        self._main_vl.addWidget(content)

    def _on_headshot_ready(self, pid, pixmap):
        if pid == self._player_id and not pixmap.isNull():
            self._headshot_label.setPixmap(pixmap)

    def _build_pa_table(self, player_info, parent_layout, is_pitcher=False):
        """Build a styled table showing recent plate appearances."""
        if is_pitcher:
            parent_layout.addWidget(
                _mk_label("Recent Plate Appearances Against", color=C["t2"], size=12, bold=True))
            pa_data = get_recent_pa_against(player_info['id'], limit=10,
                                            team=player_info.get('team'))
            cols = ["DATE", "MATCHUP", "BATTER", "PITCH", "RESULT", "EV"]
            col_keys = ['date', 'matchup', 'batter', 'pitch', 'event', 'ev']
            col_widths = [52, 82, 90, 120, 0, 42]
        else:
            parent_layout.addWidget(
                _mk_label("Recent Plate Appearances", color=C["t2"], size=12, bold=True))
            pa_data = get_recent_pa(player_info['id'], limit=10,
                                    team=player_info.get('team'))
            cols = ["DATE", "MATCHUP", "PITCH", "RESULT", "EV"]
            col_keys = ['date', 'matchup', 'pitch', 'event', 'ev']
            col_widths = [52, 82, 130, 0, 42]

        if not pa_data:
            parent_layout.addWidget(
                _mk_label("No plate appearance data", color=C["t3"], size=11))
            return

        ROW_H = 32
        table = QTableWidget(len(pa_data), len(cols))
        table.setHorizontalHeaderLabels(cols)
        table.verticalHeader().setVisible(False)
        table.setShowGrid(False)
        table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        table.setMouseTracking(True)
        table.viewport().setMouseTracking(True)
        table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.verticalScrollBar().setSingleStep(8)

        table.setStyleSheet(f"""
            QTableWidget {{
                background:{C['bg1']}; border:none;
                color:{C['t1']}; font-family:'Segoe UI','Inter','Roboto',sans-serif;
                font-size:11px; font-weight:500; outline:none;
            }}
            QHeaderView::section {{
                background:{C['bg0']}; color:{C['t2']};
                font-family:'Segoe UI','Inter','Roboto',sans-serif;
                font-size:10px; text-transform:uppercase; letter-spacing:1px; font-weight:600;
                border:none; border-bottom:1px solid {C['bdr']}; padding:6px 6px;
            }}
            QHeaderView::section:hover {{
                color:{C['t1']};
            }}
            QScrollBar:vertical {{ background:transparent; width:4px; }}
            QScrollBar::handle:vertical {{ background:{C['bdrl']}; border-radius:2px; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}
            QScrollBar:horizontal {{ background:transparent; height:4px; }}
            QScrollBar::handle:horizontal {{ background:transparent; border-radius:2px; }}
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width:0; }}
            QTableWidget::item:focus {{ outline:none; border:none; }}
        """)

        # Suppress per-cell hover highlight — row hover is handled manually
        class _NoHoverDelegate(QStyledItemDelegate):
            def initStyleOption(self, option, index):
                super().initStyleOption(option, index)
                option.state &= ~QStyle.StateFlag.State_MouseOver
        table.setItemDelegate(_NoHoverDelegate(table))

        header = table.horizontalHeader()
        header.setHighlightSections(False)
        header.setFixedHeight(40)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        for c, w in enumerate(col_widths):
            if w > 0:
                header.resizeSection(c, w)
            else:
                header.setSectionResizeMode(c, QHeaderView.ResizeMode.Stretch)
        header.setStretchLastSection(False)

        header_h = header.height()
        visible_rows = min(len(pa_data), 5)
        table.setFixedHeight(header_h + (visible_rows * ROW_H) + 2)

        # -- full-row hover highlight --
        table._hovered_row = -1
        _qc_bg3 = QColor(C['bg3'])

        def _row_bg(r):
            return QColor(C['bg1'] if r % 2 == 0 else C['bg2'])

        def _set_row_bg(tbl, row, color):
            for col in range(tbl.columnCount()):
                it = tbl.item(row, col)
                if it:
                    it.setBackground(color)

        _orig_mouse = table.mouseMoveEvent
        _orig_leave = table.leaveEvent

        def _hover_move(e, _tbl=table):
            row = _tbl.rowAt(e.pos().y())
            if row != _tbl._hovered_row:
                if _tbl._hovered_row >= 0:
                    _set_row_bg(_tbl, _tbl._hovered_row, _row_bg(_tbl._hovered_row))
                if row >= 0:
                    _set_row_bg(_tbl, row, _qc_bg3)
                _tbl._hovered_row = row
            _orig_mouse(e)

        def _hover_leave(e, _tbl=table):
            if _tbl._hovered_row >= 0:
                _set_row_bg(_tbl, _tbl._hovered_row, _row_bg(_tbl._hovered_row))
                _tbl._hovered_row = -1
            _orig_leave(e)

        table.mouseMoveEvent = _hover_move
        table.leaveEvent = _hover_leave

        _qc_bg1 = QColor(C['bg1'])
        _qc_bg2 = QColor(C['bg2'])
        _qc_t1 = QColor(C['t1'])
        _qc_t3 = QColor(C['t3'])
        _qc_grn = QColor(C['grn'])
        _qc_red = QColor(C['red'])
        _cell_font = QFont('Segoe UI', 11)
        _cell_font.setPixelSize(11)
        _cell_font.setWeight(QFont.Weight.Bold)

        for r, pa in enumerate(pa_data):
            table.setRowHeight(r, ROW_H)
            bg_color = _qc_bg1 if r % 2 == 0 else _qc_bg2
            for c, key in enumerate(col_keys):
                val = str(pa.get(key, ''))
                display = '--' if not val.strip() else val
                item = QTableWidgetItem(display)
                item.setBackground(bg_color)
                item.setFont(_cell_font)

                if key in ('date', 'matchup', 'batter'):
                    item.setTextAlignment(
                        Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                else:
                    item.setTextAlignment(
                        Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)

                if display == '--':
                    item.setForeground(_qc_t3)
                elif key == 'ev' and display != '--':
                    try:
                        ev_val = float(display)
                        if ev_val >= 95:
                            item.setForeground(_qc_grn)
                        elif ev_val < 80:
                            item.setForeground(_qc_red)
                        else:
                            item.setForeground(_qc_t1)
                    except ValueError:
                        item.setForeground(_qc_t3)
                else:
                    item.setForeground(_qc_t1)

                table.setItem(r, c, item)

        self._pa_table = table
        parent_layout.addWidget(table)

    def _build_stats(self, data, is_pitcher):
        current = data['current']
        season = current.get('season', '?')

        self._stats_container.addWidget(
            _mk_label(f"{season} Season Stats", color=C["ora"], size=14, bold=True))

        if is_pitcher:
            self._build_pitching_stats(current)
        else:
            self._build_batting_stats(current)

        # Season history
        seasons = data.get('seasons', [])
        if len(seasons) > 1:
            sep2 = QFrame()
            sep2.setFixedHeight(1)
            sep2.setStyleSheet(f"background:{C['bdr']};")
            self._stats_container.addWidget(sep2)
            self._stats_container.addWidget(
                _mk_label("Season History", color=C["t2"], size=12, bold=True))
            self._build_season_history(seasons, is_pitcher)

    def _build_batting_stats(self, s):
        # Row 1: core slash line
        row1 = QHBoxLayout()
        row1.setSpacing(8)
        row1.addStretch()
        row1.addWidget(_stat_tile("AVG", _fmt3(s.get('avg'))))
        row1.addWidget(_stat_tile("OBP", _fmt3(s.get('obp'))))
        row1.addWidget(_stat_tile("SLG", _fmt3(s.get('slg'))))
        row1.addWidget(_stat_tile("ISO", _fmt3(s.get('iso'))))
        row1.addWidget(_stat_tile("PA", str(s.get('plate_appearances', 0))))
        self._stats_container.addLayout(row1)

        # Row 2: power & discipline
        row2 = QHBoxLayout()
        row2.setSpacing(8)
        row2.addStretch()
        row2.addWidget(_stat_tile("HR", str(s.get('home_runs', 0))))
        row2.addWidget(_stat_tile("K%", _fmt_pct(s.get('k_pct'))))
        row2.addWidget(_stat_tile("BB%", _fmt_pct(s.get('bb_pct'))))
        row2.addWidget(_stat_tile("Brl%", _fmt_pct(s.get('barrel_pct'))))
        row2.addWidget(_stat_tile("EV50", _fmt1(s.get('ev50'))))
        self._stats_container.addLayout(row2)

    def _build_pitching_stats(self, s):
        outs = s.get('outs_recorded', 0) or 0
        ip = f"{outs // 3}.{outs % 3}"

        # Row 1: core
        row1 = QHBoxLayout()
        row1.setSpacing(8)
        row1.addStretch()
        row1.addWidget(_stat_tile("ERA", _fmt_era(s.get('era')), highlight=True))
        row1.addWidget(_stat_tile("WHIP", _fmt3(s.get('whip'))))
        row1.addWidget(_stat_tile("IP", ip))
        row1.addWidget(_stat_tile("K%", _fmt_pct(s.get('k_pct'))))
        row1.addWidget(_stat_tile("BB%", _fmt_pct(s.get('bb_pct'))))
        self._stats_container.addLayout(row1)

        # Row 2: advanced
        row2 = QHBoxLayout()
        row2.setSpacing(8)
        row2.addStretch()
        row2.addWidget(_stat_tile("Whiff%", _fmt_pct(s.get('whiff_pct'))))
        row2.addWidget(_stat_tile("Brl%", _fmt_pct(s.get('barrel_pct'))))
        row2.addWidget(_stat_tile("xOBA", _fmt3(s.get('xoba_against'))))
        row2.addWidget(_stat_tile("Velo", _fmt1(s.get('avg_velo'))))
        row2.addWidget(_stat_tile("Top", _fmt1(s.get('top_velo'))))
        self._stats_container.addLayout(row2)

    def _build_season_history(self, seasons, is_pitcher):
        """Compact grid of key stats per season."""
        grid = QGridLayout()
        grid.setSpacing(4)

        if is_pitcher:
            headers = ["Season", "IP", "ERA", "WHIP", "K%", "BB%", "Whiff%"]
        else:
            headers = ["Season", "PA", "AVG", "OBP", "SLG", "HR", "K%", "BB%"]

        for col, h in enumerate(headers):
            lbl = _mk_label(h, color=C["t3"], size=9, bold=True,
                             align=Qt.AlignmentFlag.AlignCenter)
            grid.addWidget(lbl, 0, col)

        for row_idx, s in enumerate(seasons):
            r = row_idx + 1
            color = C["t1"] if row_idx == 0 else C["t2"]
            if is_pitcher:
                outs = s.get('outs_recorded', 0) or 0
                ip = f"{outs // 3}.{outs % 3}"
                vals = [
                    str(s.get('season', '')), ip,
                    _fmt_era(s.get('era')), _fmt3(s.get('whip')),
                    _fmt_pct(s.get('k_pct')), _fmt_pct(s.get('bb_pct')),
                    _fmt_pct(s.get('whiff_pct')),
                ]
            else:
                vals = [
                    str(s.get('season', '')), str(s.get('plate_appearances', 0)),
                    _fmt3(s.get('avg')), _fmt3(s.get('obp')),
                    _fmt3(s.get('slg')), str(s.get('home_runs', 0)),
                    _fmt_pct(s.get('k_pct')), _fmt_pct(s.get('bb_pct')),
                ]
            for col, v in enumerate(vals):
                lbl = _mk_label(v, color=color, size=10,
                                 align=Qt.AlignmentFlag.AlignCenter)
                grid.addWidget(lbl, r, col)

        self._history_container.addLayout(grid)


# ═════════════════════════════════════════════════════════════════════
# Convenience: open profile dialog for a player
# ═════════════════════════════════════════════════════════════════════

def show_player_profile(player_info, parent=None):
    """Open a modal player profile dialog."""
    dlg = PlayerProfileDialog(player_info, parent)
    dlg.exec()


# ═════════════════════════════════════════════════════════════════════
# Standalone test — Ohtani (batter) + Skenes (pitcher)
# ═════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)

    # Test two-way player: Shohei Ohtani
    show_player_profile({
        'id': 660271, 'name': 'Shohei Ohtani',
        'team': 'LAD', 'position': 'TWP',
        'hand': 'Bats L', 'stand': 'L', 'is_pitcher': False,
    })

    sys.exit(0)
