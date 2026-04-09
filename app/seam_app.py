# -*- coding: utf-8 -*-
"""
Seam Analytics — MLB Stats Desktop App  (PyQt6 / Win32)
 
Architecture
────────────
  Titlebar
  Main Navbar  →  Home | Hitting | Pitching | Base Running | Matchup
  ┌─ Sidebar ──────────────┬─ Main Content ──────────────────────────────┐
  │  Scrollable game cards  │  QStackedWidget                             │
  │  click → game detail    │    0: Home                                  │
  │                         │    1: Top Hitting                           │
  │                         │    2: Top Pitching                          │
  │                         │    3: Top Base Running                      │
  │                         │    4: Matchup                               │
  │                         │    5: Game Detail ← loads on card click     │
  │                         │         Sub-Navbar: Batting | Pitching |    │
  │                         │                    Base Running | BvP       │
  │                         │         Two-team lineup tables              │
  └─────────────────────────┴────────────────────────────────────────────┘
"""
 
import sys
import os
import sqlite3
import json
import csv
import logging
import requests
import requests.adapters
from pathlib import Path
import datetime as dt
import threading
import threading as _threading_mod
_thread_local = _threading_mod.local()
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry

from mlb_data_engine import MLBDataEngine
from MLB_AVG import grade_stat
from park_factors import ParkFactorsPage, prefetch_weather
import _app_paths
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QFrame,
    QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QScrollArea, QAbstractScrollArea, QStackedWidget, QTableWidget,
    QTableWidgetItem, QHeaderView, QAbstractItemView, QComboBox,
    QGraphicsDropShadowEffect, QGraphicsOpacityEffect, QSizePolicy,
)
from PyQt6.QtCore import Qt, QTimer, QRectF, QPointF, pyqtSignal, QObject, QPropertyAnimation, QEasingCurve, QEvent, QThread, QSize, QSettings, QByteArray
from PyQt6 import sip
from PyQt6.QtGui import (
    QColor, QFont, QPainter, QPainterPath,
    QPen, QBrush, QWheelEvent, QPalette, QPixmap, QImage, QIcon,
)

log = logging.getLogger("seam")
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# ── Safe sip.isdeleted wrapper ──────────────────────────────────────
def _is_deleted(obj):
    """Return True if the underlying C++ object has been deleted."""
    try:
        return sip.isdeleted(obj)
    except (RuntimeError, SystemError):
        return True

# ── Fade transition helper ──────────────────────────────────────────

def _cleanup_anim(stack):
    anim = getattr(stack, '_fade_anim', None)
    if anim is not None:
        anim.deleteLater()
        stack._fade_anim = None

def _fade_switch(stack, new_index, duration=150):
    """Fade-out current widget, switch index, fade-in new widget."""
    if _is_deleted(stack):
        return
    if stack.currentIndex() == new_index:
        return

    # Cancel any in-progress fade on this stack
    prev = getattr(stack, '_fade_anim', None)
    if prev is not None:
        prev.stop()
        prev.deleteLater()
        # Jump to final state: show the target widget cleanly
        cur = stack.currentWidget()
        if cur:
            cur.setGraphicsEffect(None)
        stack._fade_anim = None

    old_w = stack.currentWidget()
    if old_w is None:
        stack.setCurrentIndex(new_index)
        return

    # Tag this transition so stale callbacks are ignored
    tag = object()
    stack._fade_tag = tag

    old_eff = QGraphicsOpacityEffect(old_w)
    old_eff.setOpacity(1.0)
    old_w.setGraphicsEffect(old_eff)
    fade_out = QPropertyAnimation(old_eff, b"opacity")
    fade_out.setDuration(duration)
    fade_out.setStartValue(1.0)
    fade_out.setEndValue(0.0)
    fade_out.setEasingCurve(QEasingCurve.Type.InQuad)

    def _on_faded():
        # Ignore if a newer fade has started or stack has been deleted
        if _is_deleted(stack) or getattr(stack, '_fade_tag', None) is not tag:
            return
        # Clean up the fade-out animation
        fade_out.deleteLater()
        old_w.setGraphicsEffect(None)
        stack.setCurrentIndex(new_index)
        new_w = stack.currentWidget()
        if new_w:
            new_eff = QGraphicsOpacityEffect(new_w)
            new_eff.setOpacity(0.0)
            new_w.setGraphicsEffect(new_eff)
            fade_in = QPropertyAnimation(new_eff, b"opacity")
            fade_in.setDuration(duration)
            fade_in.setStartValue(0.0)
            fade_in.setEndValue(1.0)
            fade_in.setEasingCurve(QEasingCurve.Type.OutQuad)
            fade_in.finished.connect(lambda: (
                (new_w.setGraphicsEffect(None), _cleanup_anim(stack))
                if getattr(stack, '_fade_tag', None) is tag else None
            ))
            stack._fade_anim = fade_in
            fade_in.start()
        else:
            stack._fade_anim = None

    fade_out.finished.connect(_on_faded)
    stack._fade_anim = fade_out
    fade_out.start()
from PyQt6.QtSvg import QSvgRenderer

# Module-level thread pool for background tasks (reuses threads)
_bg_pool = ThreadPoolExecutor(max_workers=4)

# Shared HTTP session with automatic retries on transient failures
_retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[502, 503, 504, 429])
_http = requests.Session()
_http.mount("https://", requests.adapters.HTTPAdapter(max_retries=_retry))
_http.mount("http://", requests.adapters.HTTPAdapter(max_retries=_retry))

# ═══════════════════════════════════════════════════════════════════════════════
# Design tokens
# ═══════════════════════════════════════════════════════════════════════════════
C = {
    "bg0":  "#0a0a0a",
    "bg1":  "#111111",
    "bg2":  "#1a1a1a",
    "bg3":  "#242424",
    "bdr":  "#2a2a2a",
    "bdrl": "#333333",
    "t1":   "#f0f0ee",
    "t2":   "#888885",
    "t3":   "#555550",
    "ora":  "#f07020",
    "red":  "#e85d3a",
    "grn":  "#4ade80",
    "amb":  "#f59e0b",
}
ROW_H = 32
PIT_SECTION_H = 42 + 40 + ROW_H + 2  # section header + table header + 1 row + border

# ── Reusable format helpers (avoid lambda re-creation per cell) ──────────────
_fmt3   = lambda v: f"{v:.3f}" if v is not None else ""
_fmt2   = lambda v: f"{v:.2f}" if v is not None else ""
_fmt1   = lambda v: f"{v:.1f}" if v is not None else ""
_fmt_pct = lambda v: f"{v * 100:.1f}%" if v is not None else ""
_fmt_deg = lambda v: f"{v:.1f}°" if v is not None else ""
_fmt_era = lambda v: f"{v:.2f}" if v is not None else ""

# ── Pre-compiled regex for handedness suffix on player names ─────────────────
import re as _re
_HAND_RE = _re.compile(r'^(.*\S)\s+([RLBS])$')

# ── Pitcher info cache (pitcher_id → {name, throws}) — avoids repeated DB lookups
_PITCHER_CACHE: dict[int, dict] = {}   # {pid: {'name': str, 'throws': str}}
_PITCHER_NAME_CACHE: dict[str, int] = {}  # {pitcher_name: pid}

def _cache_pitcher(pid, pname=None, throws=None):
    """Add or update pitcher info in the module-level cache."""
    if pid:
        entry = _PITCHER_CACHE.get(pid, {})
        if pname:
            entry['name'] = pname
            _PITCHER_NAME_CACHE[pname] = pid
        if throws:
            entry['throws'] = throws
        _PITCHER_CACHE[pid] = entry

# ── Module-level combobox stylesheet (avoid re-parsing on each filter bar build)
_CB_STYLE = f"""
    QComboBox {{
        background:{C['bg2']}; color:{C['t1']};
        padding:5px 22px 5px 8px;
        border:1px solid {C['bdr']}; border-radius:4px;
        font-family:'Segoe UI','Inter',sans-serif; font-size:11px; font-weight:500;
    }}
    QComboBox:focus {{ border:1px solid {C['bdrl']}; outline:none; }}
    QComboBox:on {{ border:1px solid {C['bdrl']}; outline:none; }}
    QComboBox::drop-down {{
        subcontrol-origin: padding; subcontrol-position: center right;
        width:18px; border:none; background:transparent;
    }}
    QComboBox::down-arrow {{ image:none; width:0; height:0; }}
    QComboBox QAbstractItemView {{
        background:{C['bg2']}; color:{C['t1']};
        border:1px solid {C['bdrl']};
        selection-background-color:{C['bg3']}; selection-color:{C['t1']};
        outline:none; padding:4px 0;
        font-family:'Segoe UI','Inter',sans-serif; font-size:11px;
    }}
    QComboBox QAbstractItemView::item {{ padding:5px 10px; min-height:24px; }}
    QComboBox QAbstractItemView::item:hover {{ background:{C['bg3']}; }}
"""
_CB_VIEW_STYLE = f"""
    background:{C['bg2']}; color:{C['t1']};
    border:1px solid {C['bdrl']};
    selection-background-color:{C['bg3']}; selection-color:{C['t1']};
    outline:none; padding:4px 0;
    font-family:'Segoe UI','Inter',sans-serif; font-size:11px;
"""

# ═══════════════════════════════════════════════════════════════════════════════
# Team logos
# ═══════════════════════════════════════════════════════════════════════════════
_LOGO_DIR = _app_paths.LOGO_DIR
_LOGO_MAP: dict[str, str] = {}  # abbr → local svg path
_LOGO_PIXMAPS: dict[tuple[str, int], QPixmap] = {}  # (abbr, size) → QPixmap

def _init_logos():
    """Read CSV, download any missing SVGs to assets/logos/."""
    csv_path = _app_paths.TEAM_ABBREV_CSV
    if not os.path.exists(csv_path):
        return
    os.makedirs(_LOGO_DIR, exist_ok=True)
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            abbr = row.get("abbreviation", "").strip()
            url = row.get("logo_url", "").strip()
            if not abbr or not url:
                continue
            local = os.path.join(_LOGO_DIR, f"{abbr}.svg")
            _LOGO_MAP[abbr] = local
            if not os.path.exists(local):
                try:
                    r = _http.get(url, timeout=10)
                    if r.status_code == 200:
                        with open(local, "wb") as fp:
                            fp.write(r.content)
                except Exception as e:
                    log.warning("Logo download failed for %s: %s", abbr, e)

def get_team_pixmap(abbr: str, size: int = 20) -> QPixmap | None:
    """Return a cached QPixmap for the team abbreviation, or None."""
    key = (abbr, size)
    if key in _LOGO_PIXMAPS:
        return _LOGO_PIXMAPS[key]
    svg_path = _LOGO_MAP.get(abbr)
    if not svg_path or not os.path.exists(svg_path):
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
        pm = QPixmap.fromImage(img)
        _LOGO_PIXMAPS[key] = pm
        return pm
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# Sample data
# ═══════════════════════════════════════════════════════════════════════════════
 
STAT_CARDS = []  # Populated at runtime by get_dashboard_stats()
BA_LEADERS = []
HR_LEADERS = []

# --- Data manager: DB access and lineup cache ---------------------------------
class LineupNotifier(QObject):
    lineup_cached = pyqtSignal(str)


class _ScoreNotifier(QObject):
    scores_ready = pyqtSignal(list)


class _OddsNotifier(QObject):
    odds_ready = pyqtSignal(dict)


class _PlaysNotifier(QObject):
    plays_ready = pyqtSignal(dict)  # {game_id_str: [play_dicts]}


class _GameDataNotifier(QObject):
    """Emitted from bg thread when game data (batting/pitching/br/bvp) is ready."""
    data_ready = pyqtSignal(dict)


class DataManager:
    def __init__(self, db_path=None, cache_dir=None):
        self.root = _app_paths.APP_DIR
        self.db_path = db_path or _app_paths.RAW_DB
        self.calc_db_path = _app_paths.CALC_DB
        self.cache_dir = cache_dir or _app_paths.LINEUP_CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
        self._conn = None
        self._calc_conn = None
        self._thread_conns = []  # track all thread-local connections for cleanup
        self._shutting_down = False
        # MLB API engine for live schedules and lineups
        try:
            self.api = MLBDataEngine()
            # suppress verbose prints from the engine during normal app usage
            self.api.suppress_output = True
        except Exception:
            self.api = None
        # notifier for cached lineup availability (emitted from worker threads)
        try:
            self.notifier = LineupNotifier()
        except Exception:
            self.notifier = None

    def connect(self):
        """Return a thread-local SQLite connection to mlb_raw.db."""
        conn = getattr(_thread_local, 'raw_conn', None)
        if conn is None:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.row_factory = sqlite3.Row
                _thread_local.raw_conn = conn
                self._thread_conns.append(conn)
            except Exception:
                conn = None
        return conn

    def close(self):
        """Close all tracked connections (main + thread-local)."""
        for c in self._thread_conns:
            try:
                c.close()
            except sqlite3.ProgrammingError:
                pass  # connection created in another thread — expected at shutdown
            except Exception:
                log.exception("close")
        self._thread_conns.clear()
        _thread_local.raw_conn = None
        _thread_local.calc_conn = None
        self._conn = None
        self._calc_conn = None

    def calc_connect(self):
        """Return a thread-local connection to mlb_calculated.db (or None)."""
        conn = getattr(_thread_local, 'calc_conn', None)
        if conn is None:
            if not os.path.exists(self.calc_db_path):
                return None
            try:
                conn = sqlite3.connect(self.calc_db_path, timeout=30)
                _thread_local.calc_conn = conn
                self._thread_conns.append(conn)
            except Exception:
                conn = None
        return conn

    _season_cache: dict = {}  # {game_id: season_int} — class-level cache

    def _detect_season(self, cur, game_id):
        """Return the season year for a game_id, checking cache then DB."""
        cached = self._season_cache.get(game_id)
        if cached is not None:
            return cached
        for tbl in ("plate_appearances", "games"):
            try:
                cur.execute(f"SELECT season FROM {tbl} WHERE game_id = ? LIMIT 1", (game_id,))
                r = cur.fetchone()
                if r and r[0]:
                    val = int(r[0])
                    self._season_cache[game_id] = val
                    return val
            except Exception:
                pass
        try:
            return int(dt.date.today().year)
        except Exception:
            return None

    # ── Leaderboard helpers ──────────────────────────────────────────────
    def get_todays_player_info(self, games):
        """Return {player_id: team_abbrev} for every player in today's lineup caches."""
        player_teams = {}
        for g in games:
            gid = g.get('id') or g.get('game_id')
            if not gid:
                continue
            cache_file = os.path.join(self.cache_dir, f"{gid}.json")
            if not os.path.exists(cache_file):
                continue
            away_team = g.get('away', '')
            home_team = g.get('home', '')
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                for side, team in [('away', away_team), ('home', home_team)]:
                    for p in data.get('players', {}).get(side, []):
                        pid = p.get('player_id')
                        if pid:
                            player_teams[pid] = team
            except Exception:
                continue
        return player_teams

    def get_leaderboard(self, table, column, player_teams, season=2026,
                        top_n=10, ascending=False, min_col=None, min_val=None):
        """Top-N players (filtered to *player_teams* keys) for a single stat column.

        Returns [(player_name, team_abbrev, formatted_value), …]
        """
        player_ids = list(player_teams.keys())
        if not player_ids:
            return []
        conn = self.calc_connect()
        if not conn:
            return []
        placeholders = ','.join('?' * len(player_ids))
        order = 'ASC' if ascending else 'DESC'
        min_clause = f"AND {min_col} >= ? " if min_col and min_val is not None else ""
        query = (
            f"SELECT player_name, player_id, {column} "
            f"FROM {table} "
            f"WHERE season = ? AND matchup = 'all' AND window = 'season' "
            f"  AND player_id IN ({placeholders}) "
            f"  AND {column} IS NOT NULL "
            f"{min_clause}"
            f"ORDER BY {column} {order} "
            f"LIMIT ?"
        )
        params = [season] + player_ids
        if min_col and min_val is not None:
            params.append(min_val)
        params.append(top_n)
        try:
            rows = conn.execute(query, params).fetchall()
        except Exception:
            return []
        result = []
        for name, pid, val in rows:
            team = player_teams.get(pid, '')
            result.append((name, team, val))
        return result

    def get_leaderboards_batch(self, table, columns, player_teams, season=2026,
                               top_n=10, ascending_cols=None, min_col=None, min_val=None):
        """Fetch top-N for multiple columns from the same table in a single DB round-trip.

        columns: list of (column_name, ascending_flag) tuples or just column names.
        ascending_cols: set of column names that should sort ASC (default DESC).
        Returns {column_name: [(player_name, team_abbrev, value), …], …}
        """
        player_ids = list(player_teams.keys())
        if not player_ids:
            return {c: [] for c in columns}
        conn = self.calc_connect()
        if not conn:
            return {c: [] for c in columns}
        ascending_cols = ascending_cols or set()
        placeholders = ','.join('?' * len(player_ids))
        min_clause = f"AND {min_col} >= ? " if min_col and min_val is not None else ""
        base_params = [season] + player_ids
        if min_col and min_val is not None:
            base_params.append(min_val)

        # Build one UNION ALL query for all columns
        parts = []
        all_params = []
        for col in columns:
            order = 'ASC' if col in ascending_cols else 'DESC'
            part = (
                f"SELECT '{col}' AS stat_key, player_name, player_id, {col} AS val "
                f"FROM {table} "
                f"WHERE season = ? AND matchup = 'all' AND window = 'season' "
                f"  AND player_id IN ({placeholders}) "
                f"  AND {col} IS NOT NULL "
                f"{min_clause}"
                f"ORDER BY {col} {order} LIMIT ?"
            )
            parts.append(f"SELECT * FROM ({part})")
            all_params.extend(base_params + [top_n])

        query = " UNION ALL ".join(parts)
        results = {c: [] for c in columns}
        try:
            rows = conn.execute(query, all_params).fetchall()
            for key, name, pid, val in rows:
                team = player_teams.get(pid, '')
                results[key].append((name, team, val))
        except Exception:
            log.exception("get_leaderboards_batch")
        return results

    def get_streak_leaderboard(self, player_teams, hit_col='is_hit', season=2026, top_n=10):
        """Active streak leaderboard: consecutive most-recent games with ≥1 of *hit_col*.

        hit_col: 'is_hit' for hitting streak, 'is_home_run' for HR streak.
        Returns [(player_name, team_abbrev, streak_length), …] sorted desc.
        """
        player_ids = list(player_teams.keys())
        if not player_ids:
            return []
        conn = self.connect()
        if not conn:
            return []
        placeholders = ','.join('?' * len(player_ids))
        # per-player, per-game: did they get ≥1 hit (or HR)?
        query = (
            f"SELECT batter_id, game_date, "
            f"  MAX({hit_col}) AS had_event "
            f"FROM plate_appearances "
            f"WHERE season = ? AND batter_id IN ({placeholders}) "
            f"GROUP BY batter_id, game_date "
            f"ORDER BY batter_id, game_date DESC"
        )
        try:
            rows = conn.execute(query, [season] + player_ids).fetchall()
        except Exception:
            return []

        # compute active streak per player (consecutive from most recent game)
        streaks = {}   # batter_id → streak count
        names = {}     # batter_id → player_name (filled from calc DB)
        cur_pid = None
        for r in rows:
            pid = r['batter_id']
            if pid != cur_pid:
                cur_pid = pid
                streaks[pid] = 0
            if streaks[pid] == -1:
                continue          # already broken
            if r['had_event']:
                streaks[pid] += 1
            else:
                streaks[pid] = streaks.get(pid, 0) or streaks[pid]
                streaks[pid] = -1 if streaks[pid] > 0 else -1
                # mark done — keep the count we have

        # resolve -1 → actual count, 0 → 0
        for pid in streaks:
            if streaks[pid] == -1:
                pass  # was set to -1 after counting — need to fix logic
        # Simpler approach: re-walk
        streaks = {}
        cur_pid = None
        done = set()
        for r in rows:
            pid = r['batter_id']
            if pid in done:
                continue
            if pid != cur_pid:
                cur_pid = pid
                streaks[pid] = 0
            if r['had_event']:
                streaks[pid] += 1
            else:
                done.add(pid)

        # get player names from calc DB
        calc = self.calc_connect()
        if calc:
            ph2 = ','.join('?' * len(player_ids))
            try:
                name_rows = calc.execute(
                    f"SELECT player_id, player_name FROM calculated_batting_stats "
                    f"WHERE season = ? AND matchup = 'all' AND window = 'season' "
                    f"AND player_id IN ({ph2})",
                    [season] + player_ids).fetchall()
                names = {r[0]: r[1] for r in name_rows}
            except Exception:
                log.exception("get_streak_leaderboard")

        result = []
        for pid, streak in streaks.items():
            if streak < 1:
                continue
            name = names.get(pid, str(pid))
            team = player_teams.get(pid, '')
            result.append((name, team, streak))
        result.sort(key=lambda x: x[2], reverse=True)
        return result[:top_n]

    def get_hr_sb_game_leaderboard(self, player_teams, season=2026, top_n=10):
        """Top players with most games containing both a HR and a SB.

        Joins plate_appearances (HR) and stolen_bases (successful SB)
        per player per game, then counts games where both occurred.
        Returns [(player_name, team_abbrev, game_count), …] sorted desc.
        """
        player_ids = list(player_teams.keys())
        if not player_ids:
            return []
        conn = self.connect()
        if not conn:
            return []
        placeholders = ','.join('?' * len(player_ids))
        query = (
            f"SELECT hr.batter_id AS pid, COUNT(*) AS games "
            f"FROM ("
            f"  SELECT batter_id, game_date"
            f"  FROM plate_appearances"
            f"  WHERE season = ? AND batter_id IN ({placeholders})"
            f"  GROUP BY batter_id, game_date"
            f"  HAVING MAX(is_home_run) = 1"
            f") hr "
            f"INNER JOIN ("
            f"  SELECT runner_id, game_date"
            f"  FROM stolen_bases"
            f"  WHERE season = ? AND runner_id IN ({placeholders})"
            f"    AND event_type = 'stolen_base' AND is_successful = 1"
            f"  GROUP BY runner_id, game_date"
            f") sb ON hr.batter_id = sb.runner_id AND hr.game_date = sb.game_date "
            f"GROUP BY hr.batter_id "
            f"ORDER BY games DESC "
            f"LIMIT ?"
        )
        params = [season] + player_ids + [season] + player_ids + [top_n]
        try:
            rows = conn.execute(query, params).fetchall()
        except Exception:
            log.exception("get_hr_sb_game_leaderboard")
            return []

        # resolve player names
        calc = self.calc_connect()
        names = {}
        if calc:
            ph2 = ','.join('?' * len(player_ids))
            try:
                name_rows = calc.execute(
                    f"SELECT player_id, player_name FROM calculated_batting_stats "
                    f"WHERE season = ? AND matchup = 'all' AND window = 'season' "
                    f"AND player_id IN ({ph2})",
                    [season] + player_ids).fetchall()
                names = {r[0]: r[1] for r in name_rows}
            except Exception:
                pass

        result = []
        for r in rows:
            pid, cnt = r[0], r[1]
            if cnt < 1:
                continue
            name = names.get(pid, str(pid))
            team = player_teams.get(pid, '')
            result.append((name, team, cnt))
        return result

    def get_most_recent_game_date(self):
        conn = self.connect()
        if not conn:
            return None
        cur = conn.cursor()
        try:
            cur.execute("SELECT DISTINCT game_date FROM plate_appearances ORDER BY game_date DESC LIMIT 1")
            r = cur.fetchone()
            return r[0] if r else None
        except Exception:
            return None

    def fetch_live_games(self, date_str):
        """Fetch live schedule from MLB API for a given date (returns app-friendly game dicts)."""
        if not self.api:
            return []
        try:
            sched = self.api.get_schedule(date_str)
            out = []
            for g in sched:
                gid = g.get('id')
                away = g.get('away') or ''
                home = g.get('home') or ''
                time_str = g.get('time') or ''
                status = g.get('status') or ''
                pp = self.api.probable_pitchers.get(str(gid), {}) if hasattr(self.api, 'probable_pitchers') else {}
                away_p = pp.get('away', {}).get('name') if pp.get('away') else 'TBD'
                home_p = pp.get('home', {}).get('name') if pp.get('home') else 'TBD'
                away_p_id = pp.get('away', {}).get('id') if pp.get('away') else None
                home_p_id = pp.get('home', {}).get('id') if pp.get('home') else None
                away_score = g.get('away_score', 0)
                home_score = g.get('home_score', 0)
                # Determine live flag conservatively
                today = dt.date.today().isoformat()
                _ast = (g.get('abstract_state') or '').lower()
                _st_lower = status.lower() if status else ''
                live = (date_str == today and (
                    _ast == 'live'
                    or _st_lower.startswith('in progress')
                    or _st_lower.startswith('manager challenge')
                    or _st_lower.startswith('umpire review')
                ))
                # Override time display for postponed games
                postponed = status.lower().startswith('postponed') if status else False
                if postponed:
                    time_str = 'PPD'
                    live = False
                out.append({
                    'game_id': str(gid), 'away': away, 'home': home,
                    'away_p': away_p, 'home_p': home_p,
                    'away_p_id': away_p_id, 'home_p_id': home_p_id,
                    'away_p_throws': '', 'home_p_throws': '',
                    'time': time_str, 'live': live,
                    'away_score': int(away_score or 0),
                    'home_score': int(home_score or 0),
                    'id': gid,
                    'status': status,
                    'abstract_state': g.get('abstract_state', ''),
                    'inning': g.get('inning'),
                    'inning_half': g.get('inning_half'),
                    'inning_state': g.get('inning_state'),
                    'on_first': g.get('on_first', False),
                    'on_second': g.get('on_second', False),
                    'on_third': g.get('on_third', False),
                    'outs': g.get('outs', 0),
                    'innings_detail': g.get('innings_detail', []),
                    'away_hits': g.get('away_hits', 0),
                    'home_hits': g.get('home_hits', 0),
                    'away_errors': g.get('away_errors', 0),
                    'home_errors': g.get('home_errors', 0),
                    'current_batter_name': g.get('current_batter_name', ''),
                    'current_batter_hand': g.get('current_batter_hand', ''),
                    'current_pitcher_name': g.get('current_pitcher_name', ''),
                    'current_pitcher_hand': g.get('current_pitcher_hand', ''),
                })
            return out
        except Exception:
            return []

    def _format_and_cache_lineup(self, game_id, lineup):
        """Convert API lineup dict into table rows and cache to disk."""
        cache_file = os.path.join(self.cache_dir, f"{game_id}.json")
        BAT_COLS = ["#","POS","PLAYER","PA","AVG","ISO","K%","BB%","H","1B","2B","3B","HR","R","RBI","TB","Brl%","Pull%","EV","MaxEV","LA"]
        BAT_HI = {4, 5}
        # Build structured player lists (preserve batting order). Include player_id when available.
        def build_players_from_api(lst):
            out = []
            for p in lst:
                try:
                    pos = p[0] if len(p) > 0 else ''
                    name = p[1] if len(p) > 1 else ''
                    hand = p[2] if len(p) > 2 else ''
                    pid = p[3] if len(p) > 3 else None
                except Exception:
                    pos = name = hand = ''
                    pid = None
                try:
                    if isinstance(pos, str) and pos.strip().upper() == 'P':
                        # Skip pitchers for batting lists
                        continue
                except Exception:
                    log.exception("build_players_from_api")
                out.append({'pos': pos, 'name': name, 'hand': hand, 'player_id': pid})
            return out

        away_players = build_players_from_api(lineup.get('away', []))
        home_players = build_players_from_api(lineup.get('home', []))

        cache_data: dict = {
            'cols': BAT_COLS,
            'hi': list(BAT_HI),
            'players': {'away': away_players, 'home': home_players},
        }

        # Write cache
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f)
        except Exception:
            log.exception("build_players_from_api")

        # Return minimal rows (empty placeholders) for compatibility
        away_rows = [[str(i+1), p['pos'], p['name'], p.get('hand',''), "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""] for i, p in enumerate(away_players)]
        home_rows = [[str(i+1), p['pos'], p['name'], p.get('hand',''), "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""] for i, p in enumerate(home_players)]
        return BAT_COLS, BAT_HI, away_rows, home_rows

    def prefetch_lineups(self, games):
        """Background fetch lineup for a list of games (each item is a dict with 'id' or 'game_id').
        Always fetches fresh from the API (bypasses engine cache) so updated lineups are picked up."""
        if not self.api or self._shutting_down:
            return
        def _fetch_one(gid):
            if self._shutting_down:
                return
            try:
                lineups = self.api.get_lineup(gid, force_fresh=True)
                if self._shutting_down:
                    return
                if lineups:
                    self._format_and_cache_lineup(gid, lineups)
                    try:
                        if hasattr(self, 'notifier') and self.notifier:
                            self.notifier.lineup_cached.emit(str(gid))
                    except Exception:
                        log.exception("_fetch_one")
            except Exception:
                log.exception("_fetch_one")

        try:
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = []
                for g in games:
                    if self._shutting_down:
                        break
                    gid = g.get('id') or g.get('game_id')
                    if gid:
                        futures.append(ex.submit(_fetch_one, int(gid)))
                for f in as_completed(futures):
                    if self._shutting_down:
                        ex.shutdown(wait=False, cancel_futures=True)
                        break
                    try:
                        f.result()
                    except Exception:
                        log.exception("_fetch_one")
        except Exception:
            log.exception("_fetch_one")

    def refresh_lineup(self, game_id):
        """Fetch a single game's lineup fresh from the API and update the cache."""
        if not self.api or self._shutting_down:
            return
        try:
            lineups = self.api.get_lineup(int(game_id), force_fresh=True)
            if self._shutting_down:
                return
            if lineups:
                self._format_and_cache_lineup(int(game_id), lineups)
                try:
                    if hasattr(self, 'notifier') and self.notifier:
                        self.notifier.lineup_cached.emit(str(game_id))
                except Exception:
                    log.exception("refresh_lineup")
        except Exception:
            log.exception("refresh_lineup")

    def get_games_for_date(self, date_str):
        """Return list of game dicts for sidebar (uses plate_appearances table)."""
        conn = self.connect()
        if not conn:
            return []
        cur = conn.cursor()
        try:
            cur.execute("SELECT DISTINCT game_id, home_team, away_team FROM plate_appearances WHERE game_date = ? ORDER BY game_id", (date_str,))
            rows = cur.fetchall()
            out = []
            for r in rows:
                game_id = r[0]
                home = r[1] or ''
                away = r[2] or ''
                # scores
                cur.execute("SELECT SUM(COALESCE(runs,0)) FROM plate_appearances WHERE game_id=? AND batter_is_home=1", (game_id,))
                home_score = cur.fetchone()[0] or 0
                cur.execute("SELECT SUM(COALESCE(runs,0)) FROM plate_appearances WHERE game_id=? AND batter_is_home=0", (game_id,))
                away_score = cur.fetchone()[0] or 0
                # starting pitchers (first PA for each side) — home pitcher faces away batters
                cur.execute("SELECT pitcher_name, p_throws FROM plate_appearances WHERE game_id=? AND batter_is_home=0 ORDER BY at_bat_number LIMIT 1", (game_id,))
                rowp = cur.fetchone()
                away_p = rowp[0] if rowp and rowp[0] else 'TBD'
                home_p_throws = rowp[1] if rowp and rowp[1] else ''
                cur.execute("SELECT pitcher_name, p_throws FROM plate_appearances WHERE game_id=? AND batter_is_home=1 ORDER BY at_bat_number LIMIT 1", (game_id,))
                rowp = cur.fetchone()
                home_p = rowp[0] if rowp and rowp[0] else 'TBD'
                away_p_throws = rowp[1] if rowp and rowp[1] else ''
                # time / live flag: conservative defaults
                today = dt.date.today().isoformat()
                live = (date_str == today)
                time_str = 'LIVE' if live else ('FINAL' if (home_score or away_score) else 'TBD')
                out.append({
                    'game_id': str(game_id), 'away': away, 'home': home,
                    'away_p': away_p, 'home_p': home_p,
                    'away_p_throws': away_p_throws, 'home_p_throws': home_p_throws,
                    'time': time_str, 'live': live,
                    'away_score': int(away_score or 0), 'home_score': int(home_score or 0),
                })
            return out
        except Exception:
            return []

    def fetch_draftkings_odds(self, date_str=None):
        """Fetch MLB DraftKings odds via ESPN APIs.
        Uses scoreboard for teams + core API for per-event odds.
        Returns dict keyed by (away_abbr, home_abbr) → odds dict."""
        import concurrent.futures

        ds = (date_str or dt.date.today().isoformat()).replace("-", "")
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={ds}"
        try:
            r = _http.get(url, timeout=10)
            if r.status_code != 200:
                return {}
            data = r.json()
        except Exception:
            return {}

        # ESPN → MLB Stats API abbreviation mapping for mismatches
        _ESPN_TO_MLB = {"CHW": "CWS", "OAK": "ATH"}

        # Collect event IDs and team abbreviations from scoreboard
        event_info = []  # list of (event_id, away_abbr, home_abbr)
        for ev in data.get("events", []):
            eid = ev.get("id")
            if not eid:
                continue
            for comp in ev.get("competitions", []):
                away = home = ""
                for c in comp.get("competitors", []):
                    abbr = c.get("team", {}).get("abbreviation", "")
                    abbr = _ESPN_TO_MLB.get(abbr, abbr)
                    if c.get("homeAway") == "away":
                        away = abbr
                    elif c.get("homeAway") == "home":
                        home = abbr
                if away and home:
                    event_info.append((eid, away, home))

        if not event_info:
            return {}

        def _fetch_event_odds(eid):
            odds_url = (f"http://sports.core.api.espn.com/v2/sports/baseball/"
                        f"leagues/mlb/events/{eid}/competitions/{eid}/odds")
            try:
                resp = _http.get(odds_url, timeout=8)
                if resp.status_code != 200:
                    return None
                od = resp.json()
                items = od.get("items", [])
                if not items:
                    return None
                item = items[0]  # DraftKings (priority 1)
                d = {}
                ou = item.get("overUnder")
                if ou is not None:
                    d["over_under"] = ou
                away_ml = item.get("awayTeamOdds", {}).get("moneyLine")
                home_ml = item.get("homeTeamOdds", {}).get("moneyLine")
                if away_ml is not None:
                    d["away_ml"] = away_ml
                if home_ml is not None:
                    d["home_ml"] = home_ml
                return d
            except Exception:
                return None

        result = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch_event_odds, eid): (away, home)
                       for eid, away, home in event_info}
            for fut in concurrent.futures.as_completed(futures):
                away, home = futures[fut]
                try:
                    d = fut.result()
                    if d:
                        result[(away, home)] = d
                except Exception:
                    log.exception("_fetch_event_odds")
        return result

    def get_dashboard_stats(self, season=2026):
        """Return real league-wide stats and leaderboards for the home dashboard."""
        prev = season - 1
        conn = self.connect()
        if not conn:
            return None
        try:
            # ── League aggregates for current season ──
            r = conn.execute(
                "SELECT SUM(is_hit) h, SUM(is_ab) ab, SUM(is_home_run) hr,"
                " SUM(runs) runs, COUNT(DISTINCT game_id) games"
                " FROM plate_appearances WHERE season=?", (season,)).fetchone()
            games = r["games"] or 1
            lg_avg = r["h"] / max(r["ab"], 1)
            hr_per_g = r["hr"] / games
            runs_per_g = r["runs"] / games

            # Stolen bases
            sb_row = conn.execute(
                "SELECT SUM(is_successful) sb FROM stolen_bases WHERE season=? AND event_type='stolen_base'",
                (season,)).fetchone()
            sb_per_g = (sb_row["sb"] or 0) / games

            # ── Previous season for deltas ──
            rp = conn.execute(
                "SELECT SUM(is_hit) h, SUM(is_ab) ab, SUM(is_home_run) hr,"
                " SUM(runs) runs, COUNT(DISTINCT game_id) games"
                " FROM plate_appearances WHERE season=?", (prev,)).fetchone()
            prev_games = rp["games"] or 1
            prev_avg = rp["h"] / max(rp["ab"], 1)
            prev_hr = rp["hr"] / prev_games
            prev_sb_row = conn.execute(
                "SELECT SUM(is_successful) sb FROM stolen_bases WHERE season=? AND event_type='stolen_base'",
                (prev,)).fetchone()
            prev_sb = (prev_sb_row["sb"] or 0) / prev_games

            stat_cards = [
                ("AVG HR / GAME", f"{hr_per_g:.2f}",
                 f"{hr_per_g - prev_hr:+.2f} vs '{str(prev)[-2:]}", hr_per_g >= prev_hr),
                ("LEAGUE AVG", f".{round(lg_avg, 3) * 1000:03.0f}",
                 f"{(lg_avg - prev_avg) * 1000:+.1f} pts vs '{str(prev)[-2:]}", lg_avg >= prev_avg),
                ("RUNS / GAME", f"{runs_per_g:.1f}",
                 f"{runs_per_g - (rp['runs'] / prev_games):+.1f} vs '{str(prev)[-2:]}",
                 runs_per_g >= rp["runs"] / prev_games),
                ("SB / GAME", f"{sb_per_g:.1f}",
                 f"{sb_per_g - prev_sb:+.1f} vs '{str(prev)[-2:]}", sb_per_g >= prev_sb),
            ]

            # ── BA leaderboard (min 3.1 PA per team game ≈ ~20 early season) ──
            min_ab = max(20, int(games * 3.1 / 30))  # rough scaling
            ba_rows = conn.execute(
                "SELECT batter_name,"
                " CASE WHEN batter_is_home=1 THEN home_team ELSE away_team END AS team,"
                " ROUND(CAST(SUM(is_hit) AS FLOAT)/SUM(is_ab),3) avg"
                " FROM plate_appearances WHERE season=? AND is_ab=1"
                " GROUP BY batter_id HAVING SUM(is_ab)>=?"
                " ORDER BY avg DESC LIMIT 5", (season, min_ab)).fetchall()
            ba_leaders = [(str(i), r["batter_name"], r["team"],
                           f"{r['avg']:.3f}".lstrip('0'))
                          for i, r in enumerate(ba_rows, 1)]

            # ── HR leaderboard ──
            hr_rows = conn.execute(
                "SELECT batter_name,"
                " CASE WHEN batter_is_home=1 THEN home_team ELSE away_team END AS team,"
                " SUM(is_home_run) hr"
                " FROM plate_appearances WHERE season=?"
                " GROUP BY batter_id ORDER BY hr DESC LIMIT 5", (season,)).fetchall()
            hr_leaders = [(str(i), r["batter_name"], r["team"], str(r["hr"]))
                          for i, r in enumerate(hr_rows, 1)]

            return {"stat_cards": stat_cards, "ba_leaders": ba_leaders,
                    "hr_leaders": hr_leaders, "games_today": len(GAMES)}
        except Exception:
            return None

    def get_game_lineup(self, game_id, season=None, matchup=None, window=None):
        """Return batting table cols and rows for away/home. Supports optional
        `season`, `matchup` (all|vs_lefty|vs_righty) and `window` (season|last5|last10|last15|last30)
        which will be used to query `mlb_calculated.db` when available. Falls back
        to raw plate_appearances queries when calc DB entries are missing.
        """
        cache_file = os.path.join(self.cache_dir, f"{game_id}.json")

        def _build_row_for_player_info(pid, pos, name, hand, cur, eff_season, eff_matchup, eff_window):
            pa = 0
            avg = iso = k_pct = bb_pct = None
            hits = singles = doubles = triples = hrs = runs = rbi = total_bases = 0
            barrel_pct = pull_pct = avg_ev = max_ev = avg_la = None
            display_name = f"{name} {hand}".strip() if hand else name
            empty = [pos, display_name, "0", "", "", "", "", "0", "0", "0", "0", "0", "0", "0", "0", "", "", "", "", ""]
            if not pid:
                return empty

            # Prefer calculated DB for every combo of season/matchup/window when available.
            used_calc = False
            conn_calc = self.calc_connect() if eff_season is not None else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute(
                        """
                        SELECT plate_appearances, at_bats, hits, singles, doubles, triples,
                               home_runs, runs, rbis, total_bases, walks, strikeouts,
                               avg, slg, obp, k_pct, bb_pct,
                               barrel_pct, pull_pct, iso, avg_launch_angle, avg_ev, max_ev
                        FROM calculated_batting_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                        """,
                        (eff_season, pid, eff_matchup, eff_window)
                    )
                    crow = cur_calc.fetchone()
                    if crow is not None:
                        pa = int(crow[0] or 0)
                        ab = int(crow[1] or 0)
                        hits = int(crow[2] or 0)
                        singles = int(crow[3] or 0)
                        doubles = int(crow[4] or 0)
                        triples = int(crow[5] or 0)
                        hrs = int(crow[6] or 0)
                        runs = int(crow[7] or 0)
                        rbi = int(crow[8] or 0)
                        total_bases = int(crow[9] or 0)
                        walks = int(crow[10] or 0)
                        so = int(crow[11] or 0)
                        avg = round(float(crow[12]), 3) if crow[12] is not None else None
                        slg = round(float(crow[13]), 3) if crow[13] is not None else None
                        obp = round(float(crow[14]), 3) if crow[14] is not None else None
                        k_pct = round(float(crow[15]), 2) if crow[15] is not None else None
                        bb_pct = round(float(crow[16]), 2) if crow[16] is not None else None
                        barrel_pct = round(float(crow[17]), 3) if crow[17] is not None else None
                        pull_pct = round(float(crow[18]), 3) if crow[18] is not None else None
                        iso = round(float(crow[19]), 3) if crow[19] is not None else None
                        avg_la = round(float(crow[20]), 1) if crow[20] is not None else None
                        avg_ev = round(float(crow[21]), 1) if crow[21] is not None else None
                        max_ev = round(float(crow[22]), 1) if crow[22] is not None else None
                    # only treat calc DB as used when we actually found non-zero calculated batting
                    # (fall back to raw aggregation for batting when calc PAs are zero)
                    try:
                        if crow is not None and int(crow[0] or 0) > 0:
                            used_calc = True
                    except Exception:
                        used_calc = False
                except Exception:
                    used_calc = False

            if not used_calc:
                # fallback to raw plate_appearances aggregation if calc DB missing
                try:
                    date_sql = ''
                    date_params = []
                    if eff_window and eff_window != 'season':
                        try:
                            n = int(eff_window.replace('last', ''))
                        except Exception:
                            n = None
                        dates = []
                        if n and eff_season is not None:
                            try:
                                cur.execute("SELECT DISTINCT game_date FROM plate_appearances WHERE season = ? AND batter_id = ? ORDER BY game_date DESC LIMIT ?", (eff_season, pid, n))
                                dates = [r[0] for r in cur.fetchall()]
                            except Exception:
                                dates = []
                        if dates:
                            date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
                            date_params = dates
                        else:
                            return empty

                    matchup_sql = ''
                    if eff_matchup == 'vs_lefty':
                        # filter by pitcher throwing hand using `p_throws`
                        matchup_sql = " AND p_throws = 'L'"
                    elif eff_matchup == 'vs_righty':
                        matchup_sql = " AND p_throws = 'R'"

                    sql = f"""
                    SELECT
                      COUNT(*) as pa,
                      SUM(COALESCE(is_ab,0)) as ab,
                      SUM(COALESCE(is_hit,0)) as hits,
                      SUM(COALESCE(is_single,0)) as singles,
                      SUM(COALESCE(is_double,0)) as doubles,
                      SUM(COALESCE(is_triple,0)) as triples,
                      SUM(COALESCE(is_home_run,0)) as hrs,
                      SUM(COALESCE(runs,0)) as runs,
                      SUM(COALESCE(rbi,0)) as rbi,
                      SUM(COALESCE(total_bases,0)) as total_bases,
                      SUM(COALESCE(is_walk,0)) as walks,
                      SUM(COALESCE(is_strikeout,0)) as so,
                      SUM(CASE WHEN bb_type IS NOT NULL AND launch_speed_angle = 6 THEN 1 ELSE 0 END) as barrels,
                      SUM(CASE WHEN bb_type IS NOT NULL THEN 1 ELSE 0 END) as barrel_denom,
                      SUM(CASE WHEN hc_x IS NOT NULL AND ((stand='R' AND hc_x<125.42) OR (stand='L' AND hc_x>125.42)) THEN 1 ELSE 0 END) as pulls,
                      SUM(CASE WHEN hc_x IS NOT NULL THEN 1 ELSE 0 END) as pull_denom,
                      AVG(CASE WHEN bb_type IS NOT NULL AND launch_angle IS NOT NULL THEN launch_angle END) as avg_la_raw,
                      AVG(CASE WHEN bb_type IS NOT NULL AND launch_speed IS NOT NULL THEN launch_speed END) as avg_ev_raw,
                      MAX(CASE WHEN bb_type IS NOT NULL THEN launch_speed END) as max_ev_raw
                    FROM plate_appearances
                    WHERE season = ? AND batter_id = ? {matchup_sql} {date_sql}
                    """

                    params = [eff_season, pid] + date_params
                    cur.execute(sql, params)
                    r = cur.fetchone()
                    if r:
                        pa = int(r[0] or 0)
                        ab = int(r[1] or 0)
                        hits = int(r[2] or 0)
                        singles = int(r[3] or 0)
                        doubles = int(r[4] or 0)
                        triples = int(r[5] or 0)
                        hrs = int(r[6] or 0)
                        runs = int(r[7] or 0)
                        rbi = int(r[8] or 0)
                        total_bases = int(r[9] or 0)
                        walks = int(r[10] or 0)
                        so = int(r[11] or 0)
                        barrels = int(r[12] or 0)
                        barrel_denom = int(r[13] or 0)
                        pulls = int(r[14] or 0)
                        pull_denom = int(r[15] or 0)

                        avg = round(float(hits) / float(ab), 3) if ab and ab > 0 else None
                        slg = round(float(total_bases) / float(ab), 3) if ab and ab > 0 else None
                        iso = round((slg or 0) - (avg or 0), 3) if avg is not None else None
                        k_pct = round(float(so) / float(pa), 2) if pa and pa > 0 else None
                        bb_pct = round(float(walks) / float(pa), 2) if pa and pa > 0 else None
                        barrel_pct = round(float(barrels) / float(barrel_denom), 3) if barrel_denom and barrel_denom > 0 else None
                        pull_pct = round(float(pulls) / float(pull_denom), 3) if pull_denom and pull_denom > 0 else None
                        avg_la = round(float(r[16]), 1) if r[16] is not None else None
                        avg_ev = round(float(r[17]), 1) if r[17] is not None else None
                        max_ev = round(float(r[18]), 1) if r[18] is not None else None

                except Exception:
                    pa = 0
                    avg = iso = k_pct = bb_pct = None
                    hits = singles = doubles = triples = hrs = runs = rbi = total_bases = 0
                    barrel_pct = pull_pct = avg_ev = max_ev = avg_la = None

            display_name = f"{name} {hand}".strip() if hand else name
            return [pos, display_name, str(pa), _fmt3(avg), _fmt3(iso), _fmt_pct(k_pct), _fmt_pct(bb_pct),
                    str(hits), str(singles), str(doubles), str(triples), str(hrs), str(runs), str(rbi), str(total_bases),
                    _fmt_pct(barrel_pct), _fmt_pct(pull_pct), _fmt1(avg_ev), _fmt1(max_ev), _fmt_deg(avg_la)]

        # try cache
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    players_block = data.get('players')
                    if players_block:
                        conn = self.connect()
                        if not conn:
                            return data.get('cols'), data.get('hi'), data.get('away_rows'), data.get('home_rows')
                        cur = conn.cursor()

                        detected_season = self._detect_season(cur, game_id)
                        eff_season = season if season is not None else detected_season
                        eff_matchup = matchup or 'all'
                        eff_window = window or 'season'

                        away_rows = [[str(i+1)] + _build_row_for_player_info(p.get('player_id'), p.get('pos'), p.get('name'), p.get('hand'), cur, eff_season, eff_matchup, eff_window) for i, p in enumerate(players_block.get('away', []))]
                        home_rows = [[str(i+1)] + _build_row_for_player_info(p.get('player_id'), p.get('pos'), p.get('name'), p.get('hand'), cur, eff_season, eff_matchup, eff_window) for i, p in enumerate(players_block.get('home', []))]

                        # cache computed rows only when no filters were provided
                        if season is None and matchup is None and window is None:
                            try:
                                data['away_rows'] = away_rows
                                data['home_rows'] = home_rows
                                with open(cache_file, 'w', encoding='utf-8') as f2:
                                    json.dump(data, f2)
                            except Exception:
                                log.exception("module_level")

                        return data.get('cols'), data.get('hi'), away_rows, home_rows
                    # Only return cached flat rows if they are non-empty;
                    # empty rows are likely stale/corrupted — fall through to DB.
                    away_cached = data.get('away_rows')
                    home_cached = data.get('home_rows')
                    if away_cached or home_cached:
                        return data.get('cols'), data.get('hi'), away_cached, home_cached
        except Exception:
            log.exception("module_level")

        conn = self.connect()
        if not conn:
            return None
        cur = conn.cursor()

        detected_season = self._detect_season(cur, game_id)

        BAT_COLS = ["#","POS","PLAYER","PA","AVG","ISO","K%","BB%","H","1B","2B","3B","HR","R","RBI","TB","Brl%","Pull%","EV","MaxEV","AVG LA"]
        BAT_HI = {4, 5}

        eff_season = season if season is not None else detected_season
        eff_matchup = matchup or 'all'
        eff_window = window or 'season'

        def _build_side(home_flag):
            cur.execute("""
                SELECT batter_id, batter_name, position, stand, COUNT(*) as pa,
                       SUM(COALESCE(is_hit,0)) as hits, SUM(COALESCE(is_home_run,0)) as hrs,
                       SUM(COALESCE(rbi,0)) as rbi, MIN(at_bat_number) as first_pa,
                       SUM(COALESCE(total_bases,0)) as total_bases,
                       SUM(COALESCE(is_walk,0)) as bb, SUM(COALESCE(is_strikeout,0)) as so
                FROM plate_appearances
                WHERE game_id = ? AND batter_is_home = ? AND batter_id IS NOT NULL
                  AND UPPER(COALESCE(position, '')) != 'P'
                GROUP BY batter_id
                ORDER BY first_pa ASC
            """, (game_id, home_flag))
            rows = []
            for rr in cur.fetchall():
                bid = rr[0]
                name = rr[1] or ''
                pos = rr[2] or ''
                stand = rr[3] or ''
                # build the row using the same helper so filters apply here too
                row = _build_row_for_player_info(bid, pos, name, stand, cur, eff_season, eff_matchup, eff_window)
                rows.append(row)
            return rows

        away_rows = [[str(i+1)] + row for i, row in enumerate(_build_side(0))]
        home_rows = [[str(i+1)] + row for i, row in enumerate(_build_side(1))]

        # cache default (no filters) — only write if we actually got rows
        try:
            if away_rows and season is None and matchup is None and window is None:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump({'cols': BAT_COLS, 'hi': list(BAT_HI), 'away_rows': away_rows, 'home_rows': home_rows}, f)
        except Exception:
            log.exception("_build_side")

        return BAT_COLS, BAT_HI, away_rows, home_rows

    def get_game_pitching(self, game_id, season=None, matchup=None, window=None,
                          away_starter=None, home_starter=None):
        """Return pitching table cols and rows for away/home starting pitchers.

        Looks up starters from plate_appearances for the game. When no PAs
        exist (unplayed/future game), uses ``away_starter``/``home_starter``
        dicts (keys: id, name, throws) to look up their season stats.
        """
        PIT_COLS = ["PITCHER", "IP", "K", "K%", "BB", "BB%",
                    "H", "1B", "2B", "3B", "HR", "ERA", "WHIP",
                    "xOBA", "BABIP", "SLG", "Zone%",
                    "Barrel%", "Soft%", "LD%", "Hard%", "Contact%", "Velo", "Top", "Whiff%"]
        PIT_HI = {3, 24}  # K%, Whiff%

        conn = self.connect()
        if not conn:
            return None
        cur = conn.cursor()

        detected_season = self._detect_season(cur, game_id)

        eff_season = season if season is not None else detected_season
        if eff_season is None:
            eff_season = dt.date.today().year
        eff_matchup = matchup or 'all'
        eff_window = window or 'season'

        def _fmt_ip(outs):
            """Format outs recorded as conventional IP (e.g. 18 outs → 6.0)."""
            if outs is None or outs == 0:
                return "0.0"
            full = outs // 3
            remainder = outs % 3
            return f"{full}.{remainder}"

        fmt2 = _fmt2; fmt3 = _fmt3; fmt_pct = _fmt_pct; fmt_era = _fmt_era

        def _build_pitcher_row(pid, pname, p_throws):
            # Resolve pid from name if missing — check cache first
            if not pid and pname and pname != 'TBD':
                cached_pid = _PITCHER_NAME_CACHE.get(pname)
                if cached_pid:
                    pid = cached_pid
                else:
                    try:
                        cur.execute("SELECT pitcher_id FROM plate_appearances WHERE pitcher_name=? LIMIT 1", (pname,))
                        r = cur.fetchone()
                        if r and r[0]:
                            pid = int(r[0])
                    except Exception:
                        log.exception("_build_pitcher_row")
            # Resolve handedness from cache, then DB
            if not p_throws and pid:
                cached = _PITCHER_CACHE.get(pid)
                if cached and cached.get('throws'):
                    p_throws = cached['throws']
                else:
                    try:
                        cur.execute("SELECT p_throws FROM plate_appearances WHERE pitcher_id=? AND p_throws IS NOT NULL LIMIT 1", (pid,))
                        r = cur.fetchone()
                        if r and r[0]:
                            p_throws = r[0]
                    except Exception:
                        log.exception("_build_pitcher_row")
            # Resolve full name from cache, then DB
            if pid:
                cached = _PITCHER_CACHE.get(pid)
                if cached and cached.get('name'):
                    pname = cached['name']
                else:
                    try:
                        cur.execute("SELECT pitcher_name FROM plate_appearances WHERE pitcher_id=? AND pitcher_name IS NOT NULL LIMIT 1", (pid,))
                        r = cur.fetchone()
                        if r and r[0]:
                            pname = r[0]
                    except Exception:
                        pass
            # Update cache with resolved values
            _cache_pitcher(pid, pname, p_throws)

            display_name = f"{pname} {p_throws}".strip() if p_throws else pname
            empty = [display_name, "0.0", "0", "", "0", "",
                     "0", "0", "0", "0", "0", "", "",
                     "", "", "", "", "", "", "", "", "", "", "", ""]

            if not pid:
                return empty

            # Try calculated DB
            conn_calc = self.calc_connect() if eff_season is not None else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute("""
                        SELECT plate_appearances, outs_recorded, innings_pitched,
                               strikeouts, walks, hits_allowed,
                               singles_allowed, doubles_allowed, triples_allowed,
                               home_runs_allowed, earned_runs,
                               k_pct, bb_pct, era, whiff_pct,
                               slg_against, hard_pct, xoba_against, babip_against,
                               whip, barrel_pct, ld_pct, soft_pct, contact_pct, zone_pct,
                               avg_velo, top_velo
                        FROM calculated_pitching_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                    """, (eff_season, pid, eff_matchup, eff_window))
                    crow = cur_calc.fetchone()
                    if crow and int(crow[0] or 0) > 0:
                        outs = int(crow[1] or 0)
                        k = int(crow[3] or 0)
                        bb = int(crow[4] or 0)
                        hits = int(crow[5] or 0)
                        singles = int(crow[6] or 0)
                        doubles = int(crow[7] or 0)
                        triples = int(crow[8] or 0)
                        hrs = int(crow[9] or 0)
                        k_pct = float(crow[11]) if crow[11] is not None else None
                        bb_pct = float(crow[12]) if crow[12] is not None else None
                        era = float(crow[13]) if crow[13] is not None else None
                        whiff_pct = float(crow[14]) if crow[14] is not None else None
                        slg_ag = float(crow[15]) if crow[15] is not None else None
                        hard_p = float(crow[16]) if crow[16] is not None else None
                        xoba_ag = float(crow[17]) if crow[17] is not None else None
                        babip_ag = float(crow[18]) if crow[18] is not None else None
                        whip_v = float(crow[19]) if crow[19] is not None else None
                        barrel_p = float(crow[20]) if crow[20] is not None else None
                        ld_p = float(crow[21]) if crow[21] is not None else None
                        soft_p = float(crow[22]) if crow[22] is not None else None
                        contact_p = float(crow[23]) if crow[23] is not None else None
                        zone_p = float(crow[24]) if crow[24] is not None else None
                        avg_velo_v = float(crow[25]) if crow[25] is not None else None
                        top_velo_v = float(crow[26]) if crow[26] is not None else None
                        fmt_velo = lambda v: f"{v:.1f}" if v is not None else ""
                        return [display_name, _fmt_ip(outs), str(k),
                                fmt_pct(k_pct), str(bb), fmt_pct(bb_pct),
                                str(hits), str(singles), str(doubles), str(triples), str(hrs),
                                fmt_era(era), fmt3(whip_v),
                                fmt3(xoba_ag), fmt3(babip_ag), fmt3(slg_ag), fmt_pct(zone_p),
                                fmt_pct(barrel_p), fmt_pct(soft_p), fmt_pct(ld_p), fmt_pct(hard_p), fmt_pct(contact_p),
                                fmt_velo(avg_velo_v), fmt_velo(top_velo_v), fmt_pct(whiff_pct)]
                except Exception:
                    log.exception("_build_pitcher_row")

            # Fallback: raw plate_appearances aggregation
            try:
                matchup_sql = ''
                if eff_matchup == 'vs_lefty':
                    matchup_sql = " AND stand = 'L'"
                elif eff_matchup == 'vs_righty':
                    matchup_sql = " AND stand = 'R'"

                date_sql = ''
                date_params = []
                if eff_window and eff_window != 'season':
                    try:
                        n = int(eff_window.replace('last', ''))
                    except Exception:
                        n = None
                    if n and eff_season is not None:
                        cur.execute(
                            "SELECT DISTINCT game_date FROM plate_appearances WHERE season = ? AND pitcher_id = ? ORDER BY game_date DESC LIMIT ?",
                            (eff_season, pid, n))
                        dates = [r[0] for r in cur.fetchall()]
                        if dates:
                            date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
                            date_params = dates
                        else:
                            return empty

                sql = f"""
                SELECT
                  COUNT(*) as pa,
                  SUM(CASE WHEN (COALESCE(is_ab,0)=1 AND COALESCE(is_hit,0)=0)
                             OR COALESCE(is_sac_fly,0)=1
                             OR COALESCE(is_sac_bunt,0)=1 THEN 1 ELSE 0 END) as outs,
                  SUM(COALESCE(is_strikeout,0)) as k,
                  SUM(COALESCE(is_walk,0)) as bb,
                  SUM(COALESCE(is_hit,0)) as hits,
                  SUM(COALESCE(is_single,0)) as singles,
                  SUM(COALESCE(is_double,0)) as doubles,
                  SUM(COALESCE(is_triple,0)) as triples,
                  SUM(COALESCE(is_home_run,0)) as hrs,
                  SUM(COALESCE(runs,0)) as runs_allowed
                FROM plate_appearances
                WHERE season = ? AND pitcher_id = ? {matchup_sql} {date_sql}
                """
                params = [eff_season, pid] + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r and int(r[0] or 0) > 0:
                    pa = int(r[0])
                    outs = int(r[1] or 0)
                    k = int(r[2] or 0)
                    bb = int(r[3] or 0)
                    hits = int(r[4] or 0)
                    singles = int(r[5] or 0)
                    doubles = int(r[6] or 0)
                    triples = int(r[7] or 0)
                    hrs = int(r[8] or 0)
                    runs = int(r[9] or 0)
                    innings = outs / 3.0
                    k_pct = round(k / pa, 2) if pa > 0 else None
                    bb_pct = round(bb / pa, 2) if pa > 0 else None
                    era = round(runs / innings * 9.0, 2) if innings > 0 else None
                    # Compute SLG/BABIP from raw counts (Hard%/xOBA unavailable without Statcast)
                    _ab = pa - bb
                    _tb = singles + 2 * doubles + 3 * triples + 4 * hrs
                    _slg = round(_tb / _ab, 3) if _ab > 0 else None
                    _babip_d = _ab - k - hrs
                    _babip = round((hits - hrs) / _babip_d, 3) if _babip_d > 0 else None
                    _whip = round((bb + hits) / innings, 3) if innings > 0 else None
                    return [display_name, _fmt_ip(outs), str(k),
                            fmt_pct(k_pct), str(bb), fmt_pct(bb_pct),
                            str(hits), str(singles), str(doubles), str(triples), str(hrs),
                            fmt_era(era), fmt3(_whip),
                            "", fmt3(_babip), fmt3(_slg), "",
                            "", "", "", "", "", "", "", ""]
            except Exception:
                log.exception("module_level")

            return empty

        def _get_pitcher_teams(game_id):
            """Return (home_pids, away_pids, pitcher_info) by checking the
            inning field which stores 'Top'/'Bot' half-inning values.

            Top-of-inning → away bats → pitcher is on home staff.
            Bottom-of-inning → home bats → pitcher is on away staff.
            """
            cur.execute("""
                SELECT pitcher_id, pitcher_name, p_throws, inning, MIN(at_bat_number) as first_ab
                FROM plate_appearances
                WHERE game_id = ? AND pitcher_id IS NOT NULL
                GROUP BY pitcher_id, pitcher_name, p_throws, inning
            """, (game_id,))
            rows = cur.fetchall()
            if not rows:
                return set(), set(), {}

            home_pids = set()
            away_pids = set()
            pitcher_info = {}  # pid → (pname, p_throws, first_ab)

            for pid, pname, pt, inning_str, first_ab in rows:
                if pid not in pitcher_info or first_ab < pitcher_info[pid][2]:
                    pitcher_info[pid] = (pname or '', pt or '', first_ab)

                inn_lower = str(inning_str).lower() if inning_str else ''
                if inn_lower.startswith('top') or inn_lower == 't':
                    # Top of inning: away bats, home team pitches
                    home_pids.add(pid)
                elif inn_lower.startswith('bot') or inn_lower == 'b':
                    # Bottom of inning: home bats, away team pitches
                    away_pids.add(pid)
                else:
                    # Inning is a number — can't determine; try first PA approach
                    # (first PA of game is always top of 1st = home pitcher)
                    home_pids.add(pid)

            return home_pids, away_pids, pitcher_info

        home_pids, away_pids, pitcher_info = _get_pitcher_teams(game_id)

        def _build_side(pids, info):
            """Return a single-row list for the starting pitcher (earliest appearance)."""
            sorted_pids = sorted(pids, key=lambda p: info.get(p, ('', '', 999))[2])
            if not sorted_pids:
                return []
            pid = sorted_pids[0]
            pname, p_throws, _ = info.get(pid, ('', '', 0))
            return [_build_pitcher_row(pid, pname, p_throws)]

        away_rows = _build_side(away_pids, pitcher_info)
        home_rows = _build_side(home_pids, pitcher_info)

        # Fallback for unplayed games: use starter info from schedule
        if not away_rows and away_starter:
            pid = away_starter.get('id')
            pname = away_starter.get('name', 'TBD')
            pt = away_starter.get('throws', '')
            away_rows = [_build_pitcher_row(pid, pname, pt)]
        if not home_rows and home_starter:
            pid = home_starter.get('id')
            pname = home_starter.get('name', 'TBD')
            pt = home_starter.get('throws', '')
            home_rows = [_build_pitcher_row(pid, pname, pt)]

        if not away_rows and not home_rows:
            return None

        return PIT_COLS, PIT_HI, away_rows, home_rows

    def get_game_baserunning(self, game_id, season=None, matchup=None, window=None,
                              away_starter=None, home_starter=None):
        """Return baserunning data for a game: pitcher table, catcher table, lineup table.

        Returns a dict with keys: pit_cols, pit_hi, pit_away, pit_home,
        cat_cols, cat_hi, cat_away, cat_home, br_cols, br_hi, br_away, br_home.
        """
        conn = self.connect()
        if not conn:
            return None
        cur = conn.cursor()

        detected_season = self._detect_season(cur, game_id)

        eff_season = season if season is not None else detected_season
        if eff_season is None:
            eff_season = dt.date.today().year
        eff_matchup = matchup or 'all'
        eff_window = window or 'season'

        fmt3 = _fmt3
        fmt_pct = _fmt_pct

        calc_ok = eff_season is not None and os.path.exists(self.calc_db_path)

        # ── Pitcher baserunning ──────────────────────────────────────────
        PIT_BR_COLS = ["PITCHER", "SB Att", "Pickoffs", "SB Allowed", "SB%"]
        PIT_BR_HI = set()

        def _build_pitcher_br_row(pid, pname, p_throws):
            # Resolve full name from DB (API may pass abbreviated "F. Last")
            if pid:
                try:
                    cur.execute("SELECT pitcher_name FROM plate_appearances WHERE pitcher_id=? AND pitcher_name IS NOT NULL LIMIT 1", (pid,))
                    r = cur.fetchone()
                    if r and r[0]:
                        pname = r[0]
                except Exception:
                    pass
            display_name = f"{pname} {p_throws}".strip() if p_throws else pname
            empty = [display_name, "0", "0", "0", ""]
            if not pid:
                return empty
            conn_calc = self.calc_connect() if calc_ok else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute("""
                        SELECT sb_attempts_against, pickoffs, sb_allowed, sb_allowed_avg
                        FROM calculated_pitcher_baserunning_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                    """, (eff_season, pid, eff_matchup, eff_window))
                    crow = cur_calc.fetchone()
                    if crow:
                        att = int(crow[0] or 0)
                        pk = int(crow[1] or 0)
                        sb = int(crow[2] or 0)
                        sb_avg = float(crow[3]) if crow[3] is not None else None
                        return [display_name, str(att), str(pk), str(sb), fmt_pct(sb_avg)]
                except Exception:
                    log.exception("_build_pitcher_br_row")
            return empty

        # ── Catcher baserunning ──────────────────────────────────────────
        CAT_BR_COLS = ["CATCHER", "SB Att", "CS", "SB Allowed", "SB%"]
        CAT_BR_HI = set()

        def _build_catcher_br_row(pid, pname):
            empty = [pname or '', "0", "0", "0", ""]
            if not pid:
                return empty
            conn_calc = self.calc_connect() if calc_ok else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute("""
                        SELECT sb_attempts_against, caught_stealing, sb_allowed, sb_allowed_avg
                        FROM calculated_catcher_baserunning_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                    """, (eff_season, pid, eff_matchup, eff_window))
                    crow = cur_calc.fetchone()
                    if crow:
                        att = int(crow[0] or 0)
                        cs = int(crow[1] or 0)
                        sb = int(crow[2] or 0)
                        sb_avg = float(crow[3]) if crow[3] is not None else None
                        return [pname or '', str(att), str(cs), str(sb), fmt_pct(sb_avg)]
                except Exception:
                    log.exception("_build_catcher_br_row")
            return empty

        # ── Runner baserunning (lineup) ──────────────────────────────────
        BR_COLS = ["POS", "PLAYER", "OBP", "SB Att", "SB", "Stole 2nd",
                   "Stole 3rd", "Sprint", "Bolts", "Comp Runs", "Bolt%"]
        BR_HI = {2}  # OBP

        def _build_runner_br_row(pid, pos, name, hand):
            display_name = f"{name} {hand}".strip() if hand else name
            empty = [pos, display_name, "", "0", "0", "0", "0", "", "0", "0", ""]
            if not pid:
                return empty
            conn_calc = self.calc_connect() if calc_ok else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute("""
                        SELECT steal_attempts, stolen_bases, caught_stealing, pickoffs,
                               stole_2b, stole_3b, obp,
                               sprint_speed, bolts, competitive_runs, bolt_pct
                        FROM calculated_baserunning_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                    """, (eff_season, pid, eff_matchup, eff_window))
                    crow = cur_calc.fetchone()
                    if crow:
                        att = int(crow[0] or 0)
                        sb = int(crow[1] or 0)
                        s2b = int(crow[4] or 0)
                        s3b = int(crow[5] or 0)
                        obp = float(crow[6]) if crow[6] is not None else None
                        sprint = crow[7]
                        bolts = int(crow[8] or 0)
                        comp_runs = int(crow[9] or 0)
                        bolt_pct = float(crow[10]) if crow[10] is not None else None
                        sprint_str = f"{sprint:.1f}" if sprint is not None else ""
                        return [pos, display_name, fmt3(obp), str(att), str(sb),
                                str(s2b), str(s3b), sprint_str, str(bolts),
                                str(comp_runs), fmt_pct(bolt_pct)]
                except Exception:
                    log.exception("_build_runner_br_row")

            # Fallback: try to get OBP from batting stats calc DB
            obp_str = ""
            conn_calc = self.calc_connect() if calc_ok else None
            if conn_calc:
                try:
                    cur_calc = conn_calc.cursor()
                    cur_calc.execute("""
                        SELECT obp FROM calculated_batting_stats
                        WHERE season = ? AND player_id = ? AND matchup = ? AND window = ?
                    """, (eff_season, pid, eff_matchup, eff_window))
                    brow = cur_calc.fetchone()
                    if brow and brow[0] is not None:
                        obp_str = fmt3(float(brow[0]))
                except Exception:
                    log.exception("_build_runner_br_row")
            return [pos, display_name, obp_str, "0", "0", "0", "0", "", "0", "0", ""]

        # ── Identify starters and lineup for each side ───────────────────
        # Load lineup from cache
        cache_file = os.path.join(self.cache_dir, f"{game_id}.json")
        lineup_data = None
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    lineup_data = json.load(f)
        except Exception:
            log.exception("_build_runner_br_row")

        players = {}
        if lineup_data:
            if 'players' in lineup_data:
                players = lineup_data['players']

        # Build side data
        def _build_side(side, starter_info):
            """Return (pit_rows, cat_rows, lineup_rows) for one side."""
            lineup_list = players.get(side, [])

            # Pitcher row
            pit_pid = starter_info.get('id') if starter_info else None
            pit_name = starter_info.get('name', 'TBD') if starter_info else 'TBD'
            pit_throws = starter_info.get('throws', '') if starter_info else ''
            if not pit_throws and pit_pid:
                try:
                    cur.execute("SELECT p_throws FROM plate_appearances WHERE pitcher_id=? AND p_throws IS NOT NULL LIMIT 1", (pit_pid,))
                    r = cur.fetchone()
                    if r and r[0]:
                        pit_throws = r[0]
                except Exception:
                    log.exception("_build_side")
            pit_row = [_build_pitcher_br_row(pit_pid, pit_name, pit_throws)]

            # Collect ALL catchers from the roster (shows multiple when
            # lineup isn't confirmed yet; narrows to one once it is).
            cat_rows = []
            for p in lineup_list:
                if p.get('pos', '').upper() == 'C':
                    cat_rows.append(_build_catcher_br_row(
                        p.get('player_id'), p.get('name', '')))
            # Fallback: look up from DB
            if not cat_rows:
                try:
                    fielding_flag = 0 if side == 'away' else 1
                    cur.execute("""
                        SELECT batter_id, batter_name FROM plate_appearances
                        WHERE game_id = ? AND batter_is_home = ? AND UPPER(position) = 'C'
                        LIMIT 1
                    """, (game_id, fielding_flag))
                    r = cur.fetchone()
                    if r:
                        cat_rows.append(_build_catcher_br_row(r[0], r[1] or ''))
                except Exception:
                    log.exception("_build_side")

            # Lineup rows
            if lineup_list:
                br_rows = [_build_runner_br_row(
                    p.get('player_id'), p.get('pos', ''), p.get('name', ''), p.get('hand', '')
                ) for p in lineup_list]
            else:
                # Fallback: build from PAs in the game
                flag = 0 if side == 'away' else 1
                try:
                    cur.execute("""
                        SELECT batter_id, batter_name, position, stand, MIN(at_bat_number)
                        FROM plate_appearances
                        WHERE game_id = ? AND batter_is_home = ? AND batter_id IS NOT NULL
                          AND UPPER(COALESCE(position, '')) != 'P'
                        GROUP BY batter_id ORDER BY MIN(at_bat_number) ASC
                    """, (game_id, flag))
                    br_rows = [_build_runner_br_row(r[0], r[2] or '', r[1] or '', r[3] or '')
                               for r in cur.fetchall()]
                except Exception:
                    br_rows = []

            return pit_row, cat_rows, br_rows

        # Away team faces home pitcher; home team faces away pitcher
        away_pit, away_cat, away_br = _build_side('away', away_starter)
        home_pit, home_cat, home_br = _build_side('home', home_starter)

        return {
            'pit_cols': PIT_BR_COLS, 'pit_hi': PIT_BR_HI,
            'pit_away': away_pit, 'pit_home': home_pit,
            'cat_cols': CAT_BR_COLS, 'cat_hi': CAT_BR_HI,
            'cat_away': away_cat, 'cat_home': home_cat,
            'br_cols': BR_COLS, 'br_hi': BR_HI,
            # Base runners are the OPPOSING batting lineup
            'br_away': home_br, 'br_home': away_br,
        }

    # ── BvP (Batter vs Pitcher) ──────────────────────────────────────────
    def get_bvp_data(self, game_id, window=None,
                     away_starter=None, home_starter=None):
        """Return head-to-head BvP data: pitcher stats vs opposing lineup,
        batting stats vs that pitcher, and baserunning matchup stats.

        All data spans every season (2021+), no year or handedness filter.
        ``window`` supports 'all'|'last5'|'last10'|'last15'|'last30'
        (last N *games the pitcher started*).

        Returns a dict with keys for each sub-table per side (away/home).
        """
        conn = self.connect()
        if not conn:
            return None
        cur = conn.cursor()

        fmt3 = _fmt3; fmt1 = _fmt1; fmt_pct = _fmt_pct; fmt_deg = _fmt_deg; fmt_era = _fmt_era

        def _fmt_ip(outs):
            if outs is None or outs == 0:
                return "0.0"
            full = outs // 3
            remainder = outs % 3
            return f"{full}.{remainder}"

        # Load lineup to get player IDs
        cache_file = os.path.join(self.cache_dir, f"{game_id}.json")
        lineup_data = None
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    lineup_data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            log.warning("Corrupt lineup cache %s — deleting", cache_file)
            try:
                os.remove(cache_file)
            except OSError:
                pass
        except Exception:
            log.exception("get_bvp_data: lineup cache")

        players = {}
        if lineup_data:
            if 'players' in lineup_data:
                players = lineup_data['players']

        def _get_pitcher_date_filter(pid, window):
            """Return a date SQL clause + params limiting to the pitcher's last N games."""
            if not window or window in ('all', 'season'):
                return '', []
            try:
                n = int(window.replace('last', ''))
            except Exception:
                return '', []
            cur.execute(
                "SELECT DISTINCT game_date FROM plate_appearances "
                "WHERE pitcher_id = ? ORDER BY game_date DESC LIMIT ?",
                (pid, n))
            dates = [r[0] for r in cur.fetchall()]
            if dates:
                return (' AND pa.game_date IN ({})'.format(','.join('?' for _ in dates)),
                        dates)
            return '', []

        # ── BvP Pitching: aggregate pitcher stats from PAs vs lineup batters ──
        BVP_PIT_COLS = ["PITCHER", "IP", "K", "K%", "BB", "BB%",
                        "H", "1B", "2B", "3B", "HR", "ERA", "WHIP",
                        "xOBA", "BABIP", "SLG", "Zone%",
                        "Barrel%", "Soft%", "LD%", "Hard%", "Contact%", "Velo", "Top", "Whiff%"]
        BVP_PIT_HI = {3, 24}

        def _build_bvp_pitcher_row(pid, pname, p_throws, batter_ids, window):
            # Resolve full name from DB (API may pass abbreviated "F. Last")
            if pid:
                try:
                    cur.execute("SELECT pitcher_name FROM plate_appearances WHERE pitcher_id=? AND pitcher_name IS NOT NULL LIMIT 1", (pid,))
                    r = cur.fetchone()
                    if r and r[0]:
                        pname = r[0]
                except Exception:
                    pass
            display_name = f"{pname} {p_throws}".strip() if p_throws else pname
            empty = [display_name, "0.0", "0", "", "0", "",
                     "0", "0", "0", "0", "0", "",
                     "", "", "", "", "",
                     "", "", "", "", "", "", "", ""]
            if not pid or not batter_ids:
                return empty
            placeholders = ','.join('?' for _ in batter_ids)
            date_sql, date_params = _get_pitcher_date_filter(pid, window)
            try:
                sql = f"""
                SELECT
                  COUNT(*) as pa,
                  SUM(CASE WHEN (COALESCE(is_ab,0)=1 AND COALESCE(is_hit,0)=0)
                             OR COALESCE(is_sac_fly,0)=1
                             OR COALESCE(is_sac_bunt,0)=1 THEN 1 ELSE 0 END) as outs,
                  SUM(COALESCE(is_strikeout,0)) as k,
                  SUM(COALESCE(is_walk,0)) as bb,
                  SUM(COALESCE(is_hit,0)) as hits,
                  SUM(COALESCE(is_single,0)) as singles,
                  SUM(COALESCE(is_double,0)) as doubles,
                  SUM(COALESCE(is_triple,0)) as triples,
                  SUM(COALESCE(is_home_run,0)) as hrs,
                  SUM(COALESCE(earned_runs,0)) as er,
                  SUM(COALESCE(swing, 0)) - SUM(COALESCE(contact, 0)) as whiffs,
                  SUM(COALESCE(swing, 0)) as swings,
                  SUM(COALESCE(is_ab,0)) as ab,
                  SUM(COALESCE(total_bases,0)) as tb,
                  SUM(COALESCE(is_sac_fly,0)) as sf,
                  AVG(CASE WHEN estimated_woba_using_speedangle IS NOT NULL THEN estimated_woba_using_speedangle END) as xwoba,
                  SUM(CASE WHEN bb_type IS NOT NULL THEN 1 ELSE 0 END) as bip,
                  SUM(CASE WHEN bb_type IS NOT NULL AND launch_speed >= 95 THEN 1 ELSE 0 END) as hard_ct,
                  SUM(CASE WHEN bb_type IS NOT NULL AND launch_speed < 88 THEN 1 ELSE 0 END) as soft_ct,
                  SUM(CASE WHEN bb_type = 'line_drive' THEN 1 ELSE 0 END) as ld_ct,
                  SUM(CASE WHEN bb_type IS NOT NULL AND launch_speed_angle = 6 THEN 1 ELSE 0 END) as barrel_ct,
                  SUM(CASE WHEN zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END) as in_zone,
                  COUNT(zone) as total_pitches_z,
                  AVG(CASE WHEN release_speed IS NOT NULL THEN release_speed END) as avg_velo,
                  MAX(release_speed) as top_velo
                FROM plate_appearances pa
                WHERE pa.pitcher_id = ? AND pa.batter_id IN ({placeholders}) {date_sql}
                """
                params = [pid] + list(batter_ids) + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r and int(r[0] or 0) > 0:
                    pa = int(r[0])
                    outs = int(r[1] or 0)
                    k = int(r[2] or 0)
                    bb = int(r[3] or 0)
                    hits = int(r[4] or 0)
                    singles = int(r[5] or 0)
                    doubles = int(r[6] or 0)
                    triples = int(r[7] or 0)
                    hrs = int(r[8] or 0)
                    er = int(r[9] or 0)
                    whiffs = int(r[10] or 0)
                    swings = int(r[11] or 0)
                    ab = int(r[12] or 0)
                    tb = int(r[13] or 0)
                    sf = int(r[14] or 0)
                    xwoba = float(r[15]) if r[15] is not None else None
                    bip = int(r[16] or 0)
                    hard_ct = int(r[17] or 0)
                    soft_ct = int(r[18] or 0)
                    ld_ct = int(r[19] or 0)
                    barrel_ct = int(r[20] or 0)
                    in_zone = int(r[21] or 0)
                    total_pitches_z = int(r[22] or 0)
                    avg_velo = float(r[23]) if r[23] is not None else None
                    top_velo = float(r[24]) if r[24] is not None else None
                    innings = outs / 3.0
                    k_pct = round(k / pa, 2) if pa > 0 else None
                    bb_pct = round(bb / pa, 2) if pa > 0 else None
                    era = round(er / innings * 9.0, 2) if innings > 0 else None
                    whiff_pct = round(whiffs / swings, 2) if swings > 0 else None
                    whip = round((bb + hits) / innings, 3) if innings > 0 else None
                    slg = round(tb / ab, 3) if ab > 0 else None
                    babip_d = ab - k - hrs + sf
                    babip = round((hits - hrs) / babip_d, 3) if babip_d > 0 else None
                    hard_pct = round(hard_ct / bip, 3) if bip > 0 else None
                    soft_pct = round(soft_ct / bip, 3) if bip > 0 else None
                    ld_pct = round(ld_ct / bip, 3) if bip > 0 else None
                    barrel_pct = round(barrel_ct / bip, 3) if bip > 0 else None
                    zone_pct = round(in_zone / total_pitches_z, 3) if total_pitches_z > 0 else None
                    contact_pct = round((swings - whiffs) / swings, 3) if swings > 0 else None
                    fmt_velo = lambda v: f"{v:.1f}" if v is not None else ""
                    return [display_name, _fmt_ip(outs), str(k),
                            fmt_pct(k_pct), str(bb), fmt_pct(bb_pct),
                            str(hits), str(singles), str(doubles), str(triples), str(hrs),
                            fmt_era(era), fmt3(whip),
                            fmt3(xwoba), fmt3(babip), fmt3(slg), fmt_pct(zone_pct),
                            fmt_pct(barrel_pct), fmt_pct(soft_pct), fmt_pct(ld_pct), fmt_pct(hard_pct), fmt_pct(contact_pct),
                            fmt_velo(avg_velo), fmt_velo(top_velo), fmt_pct(whiff_pct)]
            except Exception:
                log.exception("_build_bvp_pitcher_row")
            return empty

        # ── BvP Batting: each batter's stats vs the pitcher ──
        BVP_BAT_COLS = ["POS", "PLAYER", "PA", "AVG", "ISO", "K%", "BB%",
                        "H", "1B", "2B", "3B", "HR", "R", "RBI", "TB",
                        "Brl%", "Pull%", "EV", "MaxEV", "AVG LA"]
        BVP_BAT_HI = {3, 4}

        def _build_bvp_batter_row(pid, pos, name, hand, pitcher_id, window):
            display_name = f"{name} {hand}".strip() if hand else name
            empty = [pos, display_name, "0", "", "", "", "", "0", "0", "0", "0", "0",
                     "0", "0", "0", "", "", "", "", ""]
            if not pid or not pitcher_id:
                return empty
            date_sql, date_params = _get_pitcher_date_filter(pitcher_id, window)
            try:
                sql = f"""
                SELECT
                  COUNT(*) as pa,
                  SUM(COALESCE(is_ab,0)) as ab,
                  SUM(COALESCE(is_hit,0)) as hits,
                  SUM(COALESCE(is_single,0)) as singles,
                  SUM(COALESCE(is_double,0)) as doubles,
                  SUM(COALESCE(is_triple,0)) as triples,
                  SUM(COALESCE(is_home_run,0)) as hrs,
                  SUM(COALESCE(runs,0)) as runs,
                  SUM(COALESCE(rbi,0)) as rbi,
                  SUM(COALESCE(total_bases,0)) as total_bases,
                  SUM(COALESCE(is_walk,0)) as walks,
                  SUM(COALESCE(is_strikeout,0)) as so,
                  SUM(CASE WHEN bb_type IS NOT NULL AND launch_speed_angle = 6 THEN 1 ELSE 0 END) as barrels,
                  SUM(CASE WHEN bb_type IS NOT NULL THEN 1 ELSE 0 END) as barrel_denom,
                  SUM(CASE WHEN hc_x IS NOT NULL AND ((stand='R' AND hc_x<125.42) OR (stand='L' AND hc_x>125.42)) THEN 1 ELSE 0 END) as pulls,
                  SUM(CASE WHEN hc_x IS NOT NULL THEN 1 ELSE 0 END) as pull_denom,
                  AVG(CASE WHEN bb_type IS NOT NULL AND launch_angle IS NOT NULL THEN launch_angle END) as avg_la_raw,
                  AVG(CASE WHEN bb_type IS NOT NULL AND launch_speed IS NOT NULL THEN launch_speed END) as avg_ev_raw,
                  MAX(CASE WHEN bb_type IS NOT NULL THEN launch_speed END) as max_ev_raw
                FROM plate_appearances pa
                WHERE pa.batter_id = ? AND pa.pitcher_id = ? {date_sql}
                """
                params = [pid, pitcher_id] + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r and int(r[0] or 0) > 0:
                    pa = int(r[0])
                    ab = int(r[1] or 0)
                    hits = int(r[2] or 0)
                    singles = int(r[3] or 0)
                    doubles = int(r[4] or 0)
                    triples = int(r[5] or 0)
                    hrs = int(r[6] or 0)
                    runs = int(r[7] or 0)
                    rbi = int(r[8] or 0)
                    total_bases = int(r[9] or 0)
                    walks = int(r[10] or 0)
                    so = int(r[11] or 0)
                    barrels = int(r[12] or 0)
                    barrel_denom = int(r[13] or 0)
                    pulls = int(r[14] or 0)
                    pull_denom = int(r[15] or 0)

                    avg = round(float(hits) / float(ab), 3) if ab > 0 else None
                    slg = round(float(total_bases) / float(ab), 3) if ab > 0 else None
                    iso = round((slg or 0) - (avg or 0), 3) if avg is not None else None
                    k_pct = round(float(so) / float(pa), 2) if pa > 0 else None
                    bb_pct = round(float(walks) / float(pa), 2) if pa > 0 else None
                    barrel_pct = round(float(barrels) / float(barrel_denom), 3) if barrel_denom > 0 else None
                    pull_pct = round(float(pulls) / float(pull_denom), 3) if pull_denom > 0 else None
                    avg_la = round(float(r[16]), 1) if r[16] is not None else None
                    avg_ev = round(float(r[17]), 1) if r[17] is not None else None
                    max_ev = round(float(r[18]), 1) if r[18] is not None else None

                    return [pos, display_name, str(pa), fmt3(avg), fmt3(iso),
                            fmt_pct(k_pct), fmt_pct(bb_pct),
                            str(hits), str(singles), str(doubles), str(triples), str(hrs),
                            str(runs), str(rbi), str(total_bases),
                            fmt_pct(barrel_pct), fmt_pct(pull_pct),
                            fmt1(avg_ev), fmt1(max_ev), fmt_deg(avg_la)]
            except Exception:
                log.exception("_build_bvp_batter_row")
            return empty

        # ── BvP Baserunning: pitcher, catcher, lineup ──
        BVP_PIT_BR_COLS = ["PITCHER", "SB Att", "Pickoffs", "SB Allowed", "SB%"]
        BVP_PIT_BR_HI = set()
        BVP_CAT_BR_COLS = ["CATCHER", "SB Att", "CS", "SB Allowed", "SB%"]
        BVP_CAT_BR_HI = set()
        BVP_BR_COLS = ["POS", "PLAYER", "SB Att", "SB", "CS",
                       "Stole 2nd", "Stole 3rd", "Sprint", "Bolts"]
        BVP_BR_HI = set()

        def _build_bvp_pitcher_br(pid, pname, p_throws, batter_ids, window):
            # Resolve full name from DB (API may pass abbreviated "F. Last")
            if pid:
                try:
                    cur.execute("SELECT pitcher_name FROM plate_appearances WHERE pitcher_id=? AND pitcher_name IS NOT NULL LIMIT 1", (pid,))
                    r = cur.fetchone()
                    if r and r[0]:
                        pname = r[0]
                except Exception:
                    pass
            display_name = f"{pname} {p_throws}".strip() if p_throws else pname
            empty = [display_name, "0", "0", "0", ""]
            if not pid or not batter_ids:
                return empty
            placeholders = ','.join('?' for _ in batter_ids)
            date_sql, date_params = _get_pitcher_date_filter(pid, window)
            # Adapt date filter for stolen_bases table (no alias)
            sb_date_sql = date_sql.replace('pa.game_date', 'game_date') if date_sql else ''
            try:
                sql = f"""
                SELECT
                  COUNT(*) as total_events,
                  SUM(CASE WHEN event_type = 'pickoff' THEN 1 ELSE 0 END) as pickoffs,
                  SUM(CASE WHEN event_type IN ('stolen_base','caught_stealing') AND is_successful = 1 THEN 1 ELSE 0 END) as sb,
                  SUM(CASE WHEN event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END) as sb_att
                FROM stolen_bases
                WHERE pitcher_id = ? AND runner_id IN ({placeholders}) {sb_date_sql}
                """
                params = [pid] + list(batter_ids) + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r:
                    att = int(r[3] or 0)
                    pk = int(r[1] or 0)
                    sb = int(r[2] or 0)
                    sb_pct = round(sb / att, 2) if att > 0 else None
                    return [display_name, str(att), str(pk), str(sb), fmt_pct(sb_pct)]
            except Exception:
                log.exception("_build_bvp_pitcher_br")
            return empty

        def _build_bvp_catcher_br(cid, cname, batter_ids, window, pitcher_id=None):
            empty = [cname or '', "0", "0", "0", ""]
            if not cid or not batter_ids:
                return empty
            placeholders = ','.join('?' for _ in batter_ids)
            date_sql, date_params = '', []
            if pitcher_id:
                date_sql, date_params = _get_pitcher_date_filter(pitcher_id, window)
                date_sql = date_sql.replace('pa.game_date', 'game_date') if date_sql else ''
            try:
                sql = f"""
                SELECT
                  SUM(CASE WHEN event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END) as att,
                  SUM(CASE WHEN event_type = 'caught_stealing' AND is_successful = 1 THEN 1 ELSE 0 END) as cs,
                  SUM(CASE WHEN event_type = 'stolen_base' AND is_successful = 1 THEN 1 ELSE 0 END) as sb
                FROM stolen_bases
                WHERE catcher_id = ? AND runner_id IN ({placeholders}) {date_sql}
                """
                params = [cid] + list(batter_ids) + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r:
                    att = int(r[0] or 0)
                    cs = int(r[1] or 0)
                    sb = int(r[2] or 0)
                    sb_pct = round(sb / att, 2) if att > 0 else None
                    return [cname or '', str(att), str(cs), str(sb), fmt_pct(sb_pct)]
            except Exception:
                log.exception("_build_bvp_catcher_br")
            return empty

        def _build_bvp_runner_br(pid, pos, name, hand, pitcher_id, catcher_id, window):
            display_name = f"{name} {hand}".strip() if hand else name
            empty = [pos, display_name, "0", "0", "0", "0", "0", "", "0"]
            if not pid:
                return empty
            date_sql, date_params = _get_pitcher_date_filter(pitcher_id, window) if pitcher_id else ('', [])
            sb_date_sql = date_sql.replace('pa.game_date', 'game_date') if date_sql else ''
            try:
                # Combine pitcher + catcher filter: events where runner faced this battery
                battery_sql = ''
                battery_params = []
                if pitcher_id and catcher_id:
                    battery_sql = ' AND (pitcher_id = ? OR catcher_id = ?)'
                    battery_params = [pitcher_id, catcher_id]
                elif pitcher_id:
                    battery_sql = ' AND pitcher_id = ?'
                    battery_params = [pitcher_id]

                sql = f"""
                SELECT
                  SUM(CASE WHEN event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END) as att,
                  SUM(CASE WHEN event_type = 'stolen_base' AND is_successful = 1 THEN 1 ELSE 0 END) as sb,
                  SUM(CASE WHEN event_type = 'caught_stealing' AND is_successful = 1 THEN 1 ELSE 0 END) as cs,
                  SUM(CASE WHEN event_type = 'stolen_base' AND is_successful = 1 AND base = '2B' THEN 1 ELSE 0 END) as s2b,
                  SUM(CASE WHEN event_type = 'stolen_base' AND is_successful = 1 AND base = '3B' THEN 1 ELSE 0 END) as s3b,
                  AVG(sprint_speed) as sprint,
                  SUM(COALESCE(bolts,0)) as bolts
                FROM stolen_bases
                WHERE runner_id = ? {battery_sql} {sb_date_sql}
                """
                params = [pid] + battery_params + date_params
                cur.execute(sql, params)
                r = cur.fetchone()
                if r:
                    att = int(r[0] or 0)
                    sb = int(r[1] or 0)
                    cs = int(r[2] or 0)
                    s2b = int(r[3] or 0)
                    s3b = int(r[4] or 0)
                    sprint = round(float(r[5]), 1) if r[5] is not None else None
                    bolts = int(r[6] or 0)
                    sprint_str = f"{sprint:.1f}" if sprint is not None else ""
                    return [pos, display_name, str(att), str(sb), str(cs),
                            str(s2b), str(s3b), sprint_str, str(bolts)]
            except Exception:
                log.exception("_build_bvp_runner_br")
            return empty

        eff_window = window or 'all'

        def _build_bvp_side(pitcher_info, lineup, own_lineup=None):
            """Build all BvP tables for one side.
            pitcher_info: dict with id, name, throws
            lineup: list of player dicts (opposing batting lineup)
            own_lineup: list of player dicts (pitcher's own team lineup, for catcher lookup)
            """
            pid = pitcher_info.get('id')
            pname = pitcher_info.get('name', 'TBD')
            p_throws = pitcher_info.get('throws', '')

            # Resolve handedness if missing
            if not p_throws and pid:
                try:
                    cur.execute("SELECT p_throws FROM plate_appearances WHERE pitcher_id=? AND p_throws IS NOT NULL LIMIT 1", (pid,))
                    r = cur.fetchone()
                    if r and r[0]:
                        p_throws = r[0]
                except Exception:
                    log.exception("_build_bvp_side")

            batter_ids = [p.get('player_id') for p in lineup if p.get('player_id')]

            # Pitching row
            pit_row = [_build_bvp_pitcher_row(pid, pname, p_throws, batter_ids, eff_window)]

            # Batting rows
            bat_rows = [_build_bvp_batter_row(
                p.get('player_id'), p.get('pos', ''), p.get('name', ''),
                p.get('hand', ''), pid, eff_window
            ) for p in lineup]

            # Find catcher from pitcher's own team
            cat_id = None
            cat_name = ''
            # Check pitcher's own team lineup for position 'C'
            if own_lineup:
                for p in own_lineup:
                    if p.get('pos', '').upper() == 'C':
                        cat_id = p.get('player_id')
                        cat_name = p.get('name', '')
                        break
            # Fallback: look up catcher from pitcher's team in the game's PA data
            if not cat_id:
                try:
                    # Catcher is position='C' on the same side as pitcher
                    # We look in the game's PA data for a player with position C
                    # who batted on the pitcher's side
                    pitcher_side_home = None
                    if pid:
                        cur.execute("""
                            SELECT DISTINCT batter_is_home FROM plate_appearances
                            WHERE game_id = ? AND pitcher_id = ? LIMIT 1
                        """, (game_id, pid))
                        r = cur.fetchone()
                        if r is not None:
                            # pitcher faces batters where batter_is_home = X,
                            # so pitcher's own team is the opposite
                            pitcher_side_home = 1 - int(r[0])
                    if pitcher_side_home is not None:
                        cur.execute("""
                            SELECT batter_id, batter_name FROM plate_appearances
                            WHERE game_id = ? AND batter_is_home = ? AND UPPER(position) = 'C'
                            LIMIT 1
                        """, (game_id, pitcher_side_home))
                        r = cur.fetchone()
                        if r:
                            cat_id = r[0]
                            cat_name = r[1] or ''
                except Exception:
                    log.exception("_build_bvp_side")

            # Pitcher BR
            pit_br_row = [_build_bvp_pitcher_br(pid, pname, p_throws, batter_ids, eff_window)]
            # Catcher BR
            cat_br_row = [_build_bvp_catcher_br(cat_id, cat_name, batter_ids, eff_window, pitcher_id=pid)]
            # Runner BR (opposing lineup)
            runner_br_rows = [_build_bvp_runner_br(
                p.get('player_id'), p.get('pos', ''), p.get('name', ''),
                p.get('hand', ''), pid, cat_id, eff_window
            ) for p in lineup]

            return {
                'pit_rows': pit_row,
                'bat_rows': bat_rows,
                'pit_br_rows': pit_br_row,
                'cat_br_rows': cat_br_row,
                'runner_br_rows': runner_br_rows,
            }

        # Determine starters
        away_info = away_starter or {'id': None, 'name': 'TBD', 'throws': ''}
        home_info = home_starter or {'id': None, 'name': 'TBD', 'throws': ''}

        away_lineup = players.get('away', [])
        home_lineup = players.get('home', [])

        # Away pitcher faces home lineup; away pitcher's catcher is in away lineup
        away_side = _build_bvp_side(away_info, home_lineup, own_lineup=away_lineup)
        # Home pitcher faces away lineup; home pitcher's catcher is in home lineup
        home_side = _build_bvp_side(home_info, away_lineup, own_lineup=home_lineup)

        return {
            'pit_cols': BVP_PIT_COLS, 'pit_hi': BVP_PIT_HI,
            'bat_cols': BVP_BAT_COLS, 'bat_hi': BVP_BAT_HI,
            'pit_br_cols': BVP_PIT_BR_COLS, 'pit_br_hi': BVP_PIT_BR_HI,
            'cat_br_cols': BVP_CAT_BR_COLS, 'cat_br_hi': BVP_CAT_BR_HI,
            'br_cols': BVP_BR_COLS, 'br_hi': BVP_BR_HI,
            # "away" view = away pitcher vs home lineup
            'away': away_side,
            # "home" view = home pitcher vs away lineup
            'home': home_side,
        }

# instantiate a global data manager
_DM = DataManager()
# prefer system date (today) for games/lineups; if DB has entries for today use them,
# otherwise try live schedule for today; fallback to most recent DB games or placeholders
try:
    today = dt.date.today().isoformat()
    live_today = _DM.fetch_live_games(today)
    if live_today:
        GAMES = live_today
    else:
        db_games_today = _DM.get_games_for_date(today)
        if db_games_today:
            GAMES = db_games_today
        else:
            recent = _DM.get_most_recent_game_date()
            if recent:
                db_games = _DM.get_games_for_date(recent)
                if db_games:
                    GAMES = db_games
except Exception:
    log.exception("module_level_init")


def _game_batting(away, home):
    BAT_COLS = ["#","POS","PLAYER","PA","AVG","ISO","K%","BB%","H","1B","2B","3B","HR","R","RBI","TB","Brl%","Pull%","EV","MaxEV","LA"]
    BAT_HI   = {4, 5}
    away_rows = [
        ["1","CF","A. Judge R","110",".318",".296","26.4%","14.5%","35","15","8","0","12","18","28","69","19.8%","41.2%","95.2","118.4","12.3°"],
        ["2","SS","A. Volpe R","104",".261",".160","22.1%","7.7%","27","16","6","0","5","14","17","42","10.5%","38.0%","88.1","109.2","10.8°"],
        ["3","DH","G. Stanton R","98",".241",".278","31.6%","10.2%","23","10","4","0","9","15","24","49","22.0%","45.0%","93.7","116.8","14.1°"],
        ["4","1B","A. Rizzo L","88",".255",".185","18.2%","12.5%","22","14","4","0","4","10","16","38","9.8%","36.5%","87.4","107.5","8.7°"],
        ["5","RF","J. Soto L","112",".284",".209","19.6%","17.9%","32","18","6","0","8","20","22","55","16.5%","39.5%","91.3","114.2","11.5°"],
        ["6","3B","D. LeMahieu R","79",".271",".110","14.1%","8.9%","21","16","3","0","2","8","11","30","7.5%","34.0%","85.6","104.8","7.2°"],
        ["7","LF","O. Peraza R","72",".247",".123","23.6%","7.8%","18","12","3","0","3","9","9","27","8.8%","37.0%","86.9","106.1","9.4°"],
        ["8","2B","G. Torres R","85",".258",".157","20.0%","8.2%","22","15","3","0","4","11","13","35","11.0%","38.5%","89.2","110.3","10.1°"],
        ["9","C","J. Trevino R","61",".234",".105","17.8%","7.9%","14","10","2","0","2","5","7","20","6.5%","32.0%","84.3","102.7","6.8°"],
    ]
    home_rows = [
        ["1","CF","R. Duran L","107",".312",".181","18.7%","8.4%","33","21","6","0","6","22","24","52","14.5%","41.0%","90.8","112.5","11.2°"],
        ["2","1B","T. Casas L","103",".269",".214","25.2%","14.6%","28","15","6","0","7","18","22","50","17.5%","40.0%","92.1","115.3","13.4°"],
        ["3","DH","R. Devers L","109",".305",".306","22.0%","10.1%","33","15","9","0","9","24","28","66","21.0%","43.5%","94.5","117.6","12.8°"],
        ["4","RF","A. Verdugo L","98",".278",".149","17.3%","8.2%","27","19","4","0","4","14","15","41","9.5%","36.5%","87.8","108.4","9.1°"],
        ["5","LF","M. Yoshida L","95",".291",".164","14.7%","10.5%","28","19","4","0","5","16","19","43","12.0%","35.0%","88.4","109.7","10.3°"],
        ["6","SS","K. Hernández R","86",".252",".156","21.9%","8.7%","22","14","4","0","4","13","14","35","11.0%","39.0%","89.5","111.2","10.5°"],
        ["7","3B","J. Urias R","79",".241",".139","23.0%","7.9%","19","13","3","0","3","9","11","30","8.5%","35.5%","86.2","105.8","8.9°"],
        ["8","2B","D. Hamilton B","68",".235",".094","19.1%","9.6%","16","13","2","0","1","10","8","22","5.5%","33.0%","83.7","103.1","7.5°"],
        ["9","C","D. McGuire R","55",".218",".120","20.0%","9.1%","12","8","2","0","2","5","6","19","6.0%","31.0%","84.9","104.2","7.1°"],
    ]
    return BAT_COLS, BAT_HI, away_rows, home_rows
 
 
def _game_pitching(away, home, away_p, home_p):
    PIT_COLS = ["PITCHER", "IP", "K", "K%", "BB", "BB%",
                "H", "1B", "2B", "3B", "HR", "ERA", "WHIP",
                "xOBA", "BABIP", "SLG", "Zone%",
                "Barrel%", "Soft%", "LD%", "Hard%", "Contact%", "Velo", "Top", "Whiff%"]
    PIT_HI   = {3, 24}
    away_rows = [
        [f"{away_p} R", "38.1", "48", "0.28", "9", "0.05",
         "24", "16", "4", "0", "4", "1.64", "",
         "", "", "", "",
         "", "", "", "", "", "", "", ""],
    ]
    home_rows = [
        [f"{home_p} R", "29.1", "34", "0.26", "7", "0.05",
         "27", "18", "5", "1", "3", "3.07", "",
         "", "", "", "",
         "", "", "", "", "", "", "", ""],
    ]
    return PIT_COLS, PIT_HI, away_rows, home_rows
 
 
def _game_baserunning(away, home):
    BR_COLS = ["POS","PLAYER","SB","CS","SB%","SPRINT","XBT%","BOLTS","OAA"]
    BR_HI   = {2, 4, 5}
    away_rows = [
        ["CF","A. Judge","2","0","100%","28.2","61%","3","+2"],
        ["SS","A. Volpe","8","2","80%","27.9","67%","5","+3"],
        ["DH","G. Stanton","0","0","—","26.1","43%","0","0"],
        ["1B","A. Rizzo","0","0","—","25.8","38%","0","+1"],
        ["RF","J. Soto","3","1","75%","27.4","55%","2","+1"],
        ["3B","D. LeMahieu","0","0","—","26.3","47%","1","0"],
        ["LF","O. Peraza","4","1","80%","27.7","62%","3","+2"],
        ["2B","G. Torres","1","0","100%","27.0","52%","1","0"],
        ["C","J. Trevino","0","0","—","25.1","31%","0","-1"],
    ]
    home_rows = [
        ["CF","R. Duran","10","2","83%","29.1","72%","8","+4"],
        ["1B","T. Casas","1","0","100%","26.9","49%","1","0"],
        ["DH","R. Devers","0","0","—","26.4","41%","0","+1"],
        ["RF","A. Verdugo","3","1","75%","27.8","59%","2","+1"],
        ["LF","M. Yoshida","2","1","67%","27.2","54%","1","0"],
        ["SS","K. Hernández","3","0","100%","27.6","64%","3","+2"],
        ["3B","J. Urias","1","1","50%","26.8","45%","0","0"],
        ["2B","D. Hamilton","7","1","88%","28.7","69%","6","+3"],
        ["C","D. McGuire","0","0","—","24.8","28%","0","-1"],
    ]
    return BR_COLS, BR_HI, away_rows, home_rows
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Widget helpers
# ═══════════════════════════════════════════════════════════════════════════════
_label_style_cache = {}

def mk_label(text, color=None, size=10, bold=False, mono=False, align=None):
    lbl = QLabel(text)
    lbl.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
    key = (color or C['t1'], size, bold)
    ss = _label_style_cache.get(key)
    if ss is None:
        fam = "Segoe UI"
        w = "700" if bold else "400"
        ss = (f"color:{key[0]}; background:transparent;"
              f"font-family:'{fam}'; font-size:{size}px; font-weight:{w};")
        _label_style_cache[key] = ss
    lbl.setStyleSheet(ss)
    if align:
        lbl.setAlignment(align)
    return lbl
 
 
def mk_hline():
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    f.setFixedHeight(1)
    f.setStyleSheet(f"background:{C['bdr']}; border:none;")
    return f


class _GradientLine(QWidget):
    """1px horizontal line that fades from border color to background."""
    def __init__(self, bg=None, parent=None):
        super().__init__(parent)
        self._bg = bg or C["bg1"]
        self.setFixedHeight(1)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

    def set_bg(self, color):
        self._bg = color
        self.update()

    def paintEvent(self, event):
        from PyQt6.QtGui import QLinearGradient
        p = QPainter(self)
        g = QLinearGradient(0, 0, self.width(), 0)
        g.setColorAt(0.0, QColor(C["bdr"]))
        g.setColorAt(1.0, QColor(self._bg))
        p.fillRect(self.rect(), g)
        p.end()
 
 
_SA_STYLE = None
def _sa_style():
    global _SA_STYLE
    if _SA_STYLE is None:
        _SA_STYLE = f"""
        QScrollArea {{ background:transparent; border:none; }}
        QScrollBar:vertical {{ background:transparent; width: 4px; }}
        QScrollBar::handle:vertical {{ background:{C['bdrl']}; border-radius:2px; min-height:20px; }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}
        QScrollBar:horizontal {{ background:transparent; height: 4px; }}
        QScrollBar::handle:horizontal {{ background:{C['bdrl']}; border-radius:2px; }}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width:0; }}
    """
    return _SA_STYLE


class SmoothScrollArea(QScrollArea):
    """QScrollArea with pixel-smooth animated scrolling."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self._anim = QPropertyAnimation(self.verticalScrollBar(), b"value", self)
        self._anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._anim.setDuration(300)
        self._target = 0
        # Set single-step to 1 pixel so Qt never snaps to row-height increments
        self.verticalScrollBar().setSingleStep(1)

    def setWidget(self, w):
        super().setWidget(w)
        # Force pixel-mode on all child scroll areas and intercept their wheel events
        for child in w.findChildren(QAbstractScrollArea):
            if hasattr(child, 'setVerticalScrollMode'):
                child.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
            child.verticalScrollBar().setSingleStep(1)
            child.viewport().installEventFilter(self)
            child.installEventFilter(self)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.Wheel:
            self._smooth_scroll(event)
            return True
        return super().eventFilter(obj, event)

    def wheelEvent(self, e: QWheelEvent):
        self._smooth_scroll(e)
        e.accept()

    def _smooth_scroll(self, e):
        if self._anim.state() == QPropertyAnimation.State.Running:
            start = self._target
        else:
            start = self.verticalScrollBar().value()
        delta = -e.angleDelta().y()
        sb = self.verticalScrollBar()
        self._target = max(0, min(start + delta, sb.maximum()))
        self._anim.stop()
        self._anim.setStartValue(sb.value())
        self._anim.setEndValue(self._target)
        self._anim.start()
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# StatsTable
# ═══════════════════════════════════════════════════════════════════════════════
class StatsTable(QTableWidget):
    def __init__(self, cols, data, hi_cols=None, name_col_wide=180, parent=None):
        super().__init__(parent)
        self._hi = hi_cols or set()
        self._hovered = -1
        self._cols = cols
        self._data = [list(row) for row in data]  # keep original data for sorting
        self._sort_col = -1
        self._sort_asc = True
        self._name_col_wide = name_col_wide
        self._build(cols, data, name_col_wide)
        self.horizontalHeader().sectionClicked.connect(self._on_header_click)
 
    def _build(self, cols, data, name_col_wide):
        self.setColumnCount(len(cols))
        self.setRowCount(len(data))
        self.setHorizontalHeaderLabels(cols)
        self.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.setShowGrid(False)
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setHighlightSections(False)
        self.horizontalHeader().setSectionsClickable(True)
        self.horizontalHeader().setCursor(Qt.CursorShape.PointingHandCursor)
        self.horizontalHeader().setFixedHeight(40)
        self.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)
        self.setStyleSheet(f"""
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
            QScrollBar::handle:horizontal {{ background:{C['bdrl']}; border-radius:2px; }}
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width:0; }}
            QTableWidget::item:focus {{ outline:none; border:none; }}
        """)
 
        self._populate_rows(data, cols)

        # Column widths: # col narrow, POS col, name col wide, rest default
        _name_col = -1
        for _nc in ("PLAYER", "PITCHER", "CATCHER"):
            if _nc in cols:
                _name_col = cols.index(_nc)
                break
        _order_col = cols.index("#") if "#" in cols else -1
        _pos_col = cols.index("POS") if "POS" in cols else -1
        _wide_cols = {"SB Allowed", "Stole 2nd", "Stole 3rd", "Comp Runs"}
        _medium_cols = {"Contact%", "Barrel%"}
        _narrow_cols = {"IP", "K", "BB", "H", "1B", "2B", "3B", "HR",
                        "R", "RBI", "PA", "SB", "CS"}
        for c in range(self.columnCount()):
            if c == _order_col:
                self.setColumnWidth(c, 30)
            elif c == _pos_col:
                self.setColumnWidth(c, 40)
            elif c == _name_col:
                self.setColumnWidth(c, name_col_wide)
            elif c < len(cols) and cols[c] in _wide_cols:
                self.setColumnWidth(c, 90)
            elif c < len(cols) and cols[c] in _medium_cols:
                self.setColumnWidth(c, 72)
            elif c < len(cols) and cols[c] in _narrow_cols:
                self.setColumnWidth(c, 42)
            else:
                self.setColumnWidth(c, 62)

        # Let last column stretch to fill remaining space
        self.horizontalHeader().setStretchLastSection(True)

        # Expand table to show all rows (no internal vertical scrollbar)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        header_h = self.horizontalHeader().height()
        total_h = header_h + (len(data) * ROW_H) + 2  # +2 for border
        self.setFixedHeight(total_h)

    def _populate_rows(self, data, cols):
        _name_col = -1
        for _nc in ("PLAYER", "PITCHER", "CATCHER"):
            if _nc in cols:
                _name_col = cols.index(_nc)
                break
        _is_pitching = "PITCHER" in cols or "CATCHER" in cols
        # Pre-build reusable QColor / QFont objects for the render loop
        _qc_bg1 = QColor(C["bg1"])
        _qc_bg2 = QColor(C["bg2"])
        _qc_t1  = QColor(C["t1"])
        _qc_t3  = QColor(C["t3"])
        _qc_grn = QColor(C["grn"])
        _qc_red = QColor(C["red"])
        _cell_font = QFont("Segoe UI", 11)
        _cell_font.setPixelSize(11)
        _cell_font.setWeight(QFont.Weight.Bold)
        self.setRowCount(len(data))
        for r, row in enumerate(data):
            self.setRowHeight(r, ROW_H)
            bg_color = _qc_bg1 if r % 2 == 0 else _qc_bg2
            for c, val in enumerate(row):
                # Player name column: rich text label with muted handedness
                if c == _name_col:
                    m = _HAND_RE.match(str(val))
                    if m:
                        html = (f'<span style="color:{C["t1"]}; font-weight:600;">{m.group(1)}</span>'
                                f' <span style="color:{C["t3"]}; font-weight:400;">{m.group(2)}</span>')
                    else:
                        html = f'<span style="color:{C["t1"]}; font-weight:600;">{val}</span>'
                    lbl = QLabel(html)
                    lbl.setStyleSheet(f"background:transparent; border:none; margin:0; padding-left:4px; font-family:'Segoe UI','Inter',sans-serif; font-size:11px;")
                    lbl.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
                    self.setCellWidget(r, c, lbl)
                    item = QTableWidgetItem()
                    item.setBackground(bg_color)
                    self.setItem(r, c, item)
                    continue
                # Replace null / missing stat cells with muted "--"
                _sval = str(val).strip()
                _col_name = cols[c] if c < len(cols) else ""
                _is_info = _col_name in ("#", "POS") or c == _name_col
                _is_empty = _sval == "" and not _is_info
                display = "--" if _is_empty else _sval

                item = QTableWidgetItem(display)
                item.setTextAlignment(
                    (Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                    if c <= 1 else
                    (Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                if _is_empty:
                    item.setForeground(_qc_t3)
                else:
                    # Grade stat against MLB average → green / red / default
                    grade = grade_stat(_col_name, _sval, pitching=_is_pitching) if _col_name else None
                    if grade == "above":
                        item.setForeground(_qc_grn)
                    elif grade == "below":
                        item.setForeground(_qc_red)
                    else:
                        item.setForeground(_qc_t1)
                item.setBackground(bg_color)
                item.setFont(_cell_font)
                self.setItem(r, c, item)

    def _sort_key(self, row, col):
        """Return a sortable value for the given column."""
        val = str(row[col]).strip().rstrip('%')
        try:
            return (0, float(val))
        except ValueError:
            return (1, val.lower())

    def _on_header_click(self, col):
        if col == self._sort_col:
            self._sort_asc = not self._sort_asc
        else:
            self._sort_col = col
            self._sort_asc = True
        sorted_data = sorted(self._data, key=lambda r: self._sort_key(r, col), reverse=not self._sort_asc)
        self._populate_rows(sorted_data, self._cols)
        self._update_header_labels()
        self._hovered = -1

    def _update_header_labels(self):
        for c in range(self.columnCount()):
            base = self._cols[c]
            if c == self._sort_col:
                arrow = " \u25B2" if self._sort_asc else " \u25BC"
                self.horizontalHeaderItem(c).setText(base + arrow)
            else:
                self.horizontalHeaderItem(c).setText(base)
 
    def _row_bg(self, r):
        return QColor(C["bg1"] if r % 2 == 0 else C["bg2"])
 
    def _set_row_bg(self, row, color):
        for c in range(self.columnCount()):
            it = self.item(row, c)
            if it:
                it.setBackground(color)

    def mouseMoveEvent(self, e):
        row = self.rowAt(e.pos().y())
        if row != self._hovered:
            if self._hovered >= 0:
                self._set_row_bg(self._hovered, self._row_bg(self._hovered))
            if row >= 0:
                self._set_row_bg(row, QColor(C["bg3"]))
            self._hovered = row
        super().mouseMoveEvent(e)
 
    def leaveEvent(self, e):
        if self._hovered >= 0:
            self._set_row_bg(self._hovered, self._row_bg(self._hovered))
            self._hovered = -1
        super().leaveEvent(e)

    def set_data(self, data):
        """Replace table data in-place without recreating the widget."""
        self._data = [list(row) for row in data]
        self._sort_col = -1
        self._sort_asc = True
        self._hovered = -1
        old_count = self.rowCount()
        self.setRowCount(len(data))
        self._populate_rows(data, self._cols)
        self._update_header_labels()
        if len(data) != old_count:
            header_h = self.horizontalHeader().height()
            total_h = header_h + (len(data) * ROW_H) + 2
            self.setFixedHeight(total_h)
 

# ═══════════════════════════════════════════════════════════════════════════════
# Reusable section wrapper  (title bar + StatsTable)
# ═══════════════════════════════════════════════════════════════════════════════
def table_section(title, badge, cols, data, hi=None, name_col_wide=180):
    wrapper = QFrame()
    wrapper.setStyleSheet(f"""
        QFrame {{ background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px; }}
    """)
    vl = QVBoxLayout(wrapper)
    vl.setContentsMargins(0, 0, 0, 0)
    vl.setSpacing(0)
 
    hdr = QWidget()
    hdr.setFixedHeight(42)
    hdr.setStyleSheet(f"background:{C['bg1']}; border-bottom:1px solid {C['bdr']};")
    hl = QHBoxLayout(hdr)
    hl.setContentsMargins(14, 0, 14, 0)

    # Split title into bold team/pitcher parts with muted "vs" and handedness
    import re
    m = re.match(r'^(\S+)\s+(vs)\s+(.*\S)\s+([RLB])$', title)
    if m:
        team, vs, pitcher, hand = m.group(1), m.group(2), m.group(3), m.group(4)
        rich = (f'<span style="color:{C["t1"]}; font-weight:700;">{team} &nbsp;</span>'
                f'<span style="color:{C["t3"]}; font-weight:400;">{vs}</span>'
                f'<span style="color:{C["t1"]}; font-weight:700;">&nbsp; {pitcher} </span>'
                f'<span style="color:{C["t3"]}; font-weight:400;">{hand}</span>')
    else:
        # Fallback: still mute "vs" if present
        parts = re.split(r'(\bvs\b)', title, maxsplit=1)
        if len(parts) == 3:
            rich = (f'<span style="color:{C["t1"]}; font-weight:700;">{parts[0]}</span>'
                    f'<span style="color:{C["t3"]}; font-weight:400;">{parts[1]}</span>'
                    f'<span style="color:{C["t1"]}; font-weight:700;">{parts[2]}</span>')
        else:
            rich = f'<span style="color:{C["t1"]}; font-weight:700;">{title}</span>'
    title_lbl = QLabel(rich)
    title_lbl.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
    title_lbl.setStyleSheet(f"background:transparent; font-family:'Segoe UI'; font-size:12px;")
    hl.addWidget(title_lbl)
    hl.addStretch()
    hl.addWidget(mk_label(badge, color=C["t3"], size=11, mono=True))
    vl.addWidget(hdr)
    tbl = StatsTable(cols, data, hi, name_col_wide)
    tbl.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
    vl.addWidget(tbl)
    wrapper._table = tbl
    return wrapper
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Two-team lineup side-by-side
# ═══════════════════════════════════════════════════════════════════════════════
def lineup_pair(away_team, home_team, cols, hi, away_rows, home_rows, name_col_wide=180):
    root = QWidget()
    root.setStyleSheet(f"background:{C['bg0']};")
    hl = QHBoxLayout(root)
    hl.setContentsMargins(0, 0, 0, 0)
    hl.setSpacing(10)
    hl.addWidget(table_section(
        f"{away_team}  ·  AWAY", "[ ©SA ]", cols, away_rows, hi, name_col_wide))
    hl.addWidget(table_section(
        f"{home_team}  ·  HOME", "[ ©SA ]", cols, home_rows, hi, name_col_wide))
    return root
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Filter bar
# ═══════════════════════════════════════════════════════════════════════════════
class FilterBar(QWidget):
    def __init__(self, options, parent=None):
        super().__init__(parent)
        self._btns = []
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
        btn.setStyleSheet(f"""
            QPushButton {{
                background:{bg}; color:{col};
                border:1px solid {C['bdrl']}; border-radius:3px;
                padding:4px 10px; font-family:'Segoe UI';
                font-size:11px; letter-spacing:1px;
            }}
            QPushButton:hover {{ background:{C['bg3']}; color:{C['t1']}; }}
        """)
 
    def _activate(self, ab):
        for b in self._btns:
            b.setChecked(b is ab)
            self._style(b, b is ab)
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Diamond
# ═══════════════════════════════════════════════════════════════════════════════
class DiamondWidget(QWidget):
    def __init__(self, r1b=False, r2b=False, r3b=False, parent=None):
        super().__init__(parent)
        self.r1b, self.r2b, self.r3b = r1b, r2b, r3b
        self.setFixedSize(160, 160)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
 
    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        home, first, second, third = (
            QPointF(80,127), QPointF(127,80), QPointF(80,33), QPointF(33,80))
        pen = QPen(QColor(C["bdr"]), 1, Qt.PenStyle.DashLine)
        pen.setDashPattern([4, 3])
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        for a, b in [(home,first),(home,third),(first,second),(third,second)]:
            p.drawLine(a, b)
 
        def base(cx, cy, active=False, sz=9):
            path = QPainterPath()
            path.moveTo(cx, cy-sz); path.lineTo(cx+sz, cy)
            path.lineTo(cx, cy+sz); path.lineTo(cx-sz, cy); path.closeSubpath()
            p.setPen(QPen(QColor("#444444"), 1.5))
            p.setBrush(QBrush(QColor(C["ora"] if active else C["bdr"])))
            p.drawPath(path)
 
        base(80,127); base(127,80,self.r1b); base(80,33,self.r2b); base(33,80,self.r3b)
        for cx, cy, active in [(127,80,self.r1b),(80,33,self.r2b),(33,80,self.r3b)]:
            if active:
                p.setPen(QPen(QColor(C["t1"]), 1.5))
                p.setBrush(QBrush(QColor(C["ora"])))
                p.drawEllipse(QPointF(cx,cy), 5, 5)
        p.setFont(QFont("Segoe UI", 8))
        p.setPen(QColor(C["t3"]))
        for txt, rx, ry in [("1B",136,84),("2B",67,18),("3B",2,84),("HOME",60,152)]:
            p.drawText(QRectF(rx,ry,28,14), Qt.AlignmentFlag.AlignCenter, txt)
        p.end()


class MiniDiamondWidget(QWidget):
    """Compact diamond + outs indicator for sidebar game cards."""
    def __init__(self, r1b=False, r2b=False, r3b=False, outs=0, balls=0, strikes=0, parent=None):
        super().__init__(parent)
        self.r1b, self.r2b, self.r3b = r1b, r2b, r3b
        self.outs = max(0, min(3, outs))
        self.balls = max(0, min(3, balls))
        self.strikes = max(0, min(2, strikes))
        self.setFixedSize(42, 76)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

    def set_state(self, r1b, r2b, r3b, outs=0, balls=0, strikes=0):
        self.r1b, self.r2b, self.r3b = r1b, r2b, r3b
        self.outs = max(0, min(3, outs))
        self.balls = max(0, min(3, balls))
        self.strikes = max(0, min(2, strikes))
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        cx, cy = 21, 20
        home  = QPointF(cx, cy + 14)
        first = QPointF(cx + 14, cy)
        second = QPointF(cx, cy - 14)
        third = QPointF(cx - 14, cy)
        # Basepaths
        pen = QPen(QColor(C["bdr"]), 0.8)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        for a, b in [(home, first), (first, second), (second, third), (third, home)]:
            p.drawLine(a, b)
        # Bases
        def _base(pt, active, sz=5):
            path = QPainterPath()
            path.moveTo(pt.x(), pt.y() - sz)
            path.lineTo(pt.x() + sz, pt.y())
            path.lineTo(pt.x(), pt.y() + sz)
            path.lineTo(pt.x() - sz, pt.y())
            path.closeSubpath()
            p.setPen(QPen(QColor(C["bdr"]), 1))
            p.setBrush(QBrush(QColor(C["ora"] if active else "#1a1a1a")))
            p.drawPath(path)
        _base(home, False, 3)
        _base(first, self.r1b)
        _base(second, self.r2b)
        _base(third, self.r3b)
        # Row 1: Outs (3 dots, orange when filled)
        row1_y = 46
        for i in range(3):
            ox = cx - 9 + i * 9
            filled = i < self.outs
            p.setPen(QPen(QColor(C["bdr"]), 1))
            p.setBrush(QBrush(QColor(C["ora"] if filled else "#1a1a1a")))
            p.drawEllipse(QPointF(ox, row1_y), 3, 3)
        # Row 2: Balls (3 dots, blue when filled)
        row2_y = 55
        for i in range(3):
            bx = cx - 9 + i * 9
            filled = i < self.balls
            p.setPen(QPen(QColor(C["bdr"]), 1))
            p.setBrush(QBrush(QColor("#2196f3") if filled else QColor("#1a1a1a")))
            p.drawEllipse(QPointF(bx, row2_y), 3, 3)
        # Row 3: Strikes (2 dots, left-aligned, red when filled)
        row3_y = 64
        for i in range(2):
            sx = cx - 9 + i * 9
            filled = i < self.strikes
            p.setPen(QPen(QColor(C["bdr"]), 1))
            p.setBrush(QBrush(QColor("#c20303") if filled else QColor("#1a1a1a")))
            p.drawEllipse(QPointF(sx, row3_y), 3, 3)
        p.end()


# ═══════════════════════════════════════════════════════════════════════════════
# Shared blink timer — one 900ms timer drives all live GameCard dot animations
# ═══════════════════════════════════════════════════════════════════════════════
_BLINK_CARDS: set = set()  # weakref-like set of GameCard instances currently blinking
_BLINK_STATE: list = [True]  # mutable container so inner lambda can toggle

def _blink_tick():
    _BLINK_STATE[0] = not _BLINK_STATE[0]
    on = _BLINK_STATE[0]
    dead = []
    for card in _BLINK_CARDS:
        try:
            card._blink = on
            if hasattr(card, "_dot"):
                card._dot.setStyleSheet(
                    f"color:{'#e85d3a' if on else '#6b1a0c'};"
                    "background:transparent; font-family:'Segoe UI'; font-size:10px;")
        except RuntimeError:
            dead.append(card)
    if dead:
        _BLINK_CARDS.difference_update(dead)
    # Stop the global timer if no cards need blinking
    if not _BLINK_CARDS and _BLINK_TIMER is not None:
        _BLINK_TIMER.stop()

_BLINK_TIMER: QTimer | None = None  # created lazily after QApplication exists

def _ensure_blink_timer():
    global _BLINK_TIMER
    if _BLINK_TIMER is None:
        _BLINK_TIMER = QTimer()
        _BLINK_TIMER.setInterval(900)
        _BLINK_TIMER.timeout.connect(_blink_tick)
    if not _BLINK_TIMER.isActive() and _BLINK_CARDS:
        _BLINK_TIMER.start()


# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar game card
# ═══════════════════════════════════════════════════════════════════════════════
class GameCard(QFrame):
    _max_height = 0  # class-level: uniform height across all sidebar cards

    def __init__(self, game: dict, idx: int, on_click, parent=None):
        super().__init__(parent)
        self.game = game
        self._idx  = idx
        self._cb   = on_click
        self._sel  = False
        self._blink = True
        self.setObjectName("GC")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._restyle()
        self._build()
        if self._is_live():
            _BLINK_CARDS.add(self)
            _ensure_blink_timer()

    def _is_live(self):
        return _is_game_live(self.game)
 
    def _restyle(self):
        bdr  = C["ora"] if self._sel else C["bdr"]
        bdrw = "2px"    if self._sel else "1px"
        bg   = C["bg2"] if self._sel else C["bg1"]
        self.setStyleSheet(f"""
            #GC {{ background:{bg}; border:none;
                   border-left:{bdrw} solid {bdr};
                   border-bottom:1px solid {C['bdr']}; }}
        """)
 
    def set_selected(self, v):
        self._sel = v
        self._restyle()
 
    def _build(self):
        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(12, 9, 12, 9)
        self._root.setSpacing(4)
        self._populate()
        QTimer.singleShot(0, self._sync_height)

    def _clear_layout(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            w = item.widget()
            if w:
                w.setGraphicsEffect(None)
                w.hide()
                w.deleteLater()
            elif item.layout():
                self._clear_layout(item.layout())

    def update_game(self, new_game):
        was_live = self._is_live()
        self.game = new_game
        self._clear_layout(self._root)
        self._populate()
        self._sync_height()
        now_live = self._is_live()
        if now_live and not was_live:
            _BLINK_CARDS.add(self)
            _ensure_blink_timer()
        elif was_live and not now_live:
            _BLINK_CARDS.discard(self)

    def _sync_height(self):
        """Ensure all sidebar cards share the same height (tallest wins)."""
        h = self.sizeHint().height()
        if h > GameCard._max_height:
            GameCard._max_height = h
        if GameCard._max_height:
            self.setMinimumHeight(GameCard._max_height)

    def _populate(self):
        g = self.game
        ppd = (g.get("status") or "").lower().startswith("postponed")
        st = (g.get("status") or "").lower()
        is_final = (g.get("time", "").upper() == "FINAL"
                    or st.startswith("final") or st.startswith("game over")
                    or st.startswith("completed"))
        is_live = self._is_live()

        time_hl = QHBoxLayout()
        time_hl.setSpacing(4)
        if is_live:
            self._dot = mk_label("●", color=C["red"], size=10, mono=True)
            time_hl.addWidget(self._dot)
            lbl = mk_label("Live", color=C["red"], size=10, mono=True, bold=True)
            glow = QGraphicsDropShadowEffect(lbl)
            glow.setColor(QColor(C["red"]))
            glow.setBlurRadius(12)
            glow.setOffset(0, 0)
            lbl.setGraphicsEffect(glow)
            time_hl.addWidget(lbl)
            _gstatus = (g.get('status') or '').lower()
            if _gstatus.startswith('warmup'):
                time_hl.addWidget(mk_label(
                    "WARMUPS", color=C["t3"], size=10, mono=True))
            else:
                inn = g.get("inning")
                state = (g.get("inning_state") or g.get("inning_half") or "").lower()
                if inn is not None:
                    if state.startswith("top"):
                        hlbl = "TOP"
                    elif state.startswith("mid"):
                        hlbl = "MID"
                    elif state.startswith("end"):
                        hlbl = "END"
                    else:
                        hlbl = "BOT"
                    time_hl.addWidget(mk_label(
                        f"{hlbl} {inn}", color=C["t3"], size=10, mono=True))
        elif is_final:
            time_hl.addWidget(mk_label("Final", color=C["t3"], size=10, mono=True))
        elif ppd:
            time_hl.addWidget(mk_label("PPD", color=C["red"], size=10, mono=True))
        else:
            time_hl.addWidget(mk_label(
                g["time"], color=C["t3"], size=10, mono=True))
        time_hl.addStretch()
        self._root.addLayout(time_hl)

        def team_row(abbr, pitcher, score=None):
            hl = QHBoxLayout()
            hl.setSpacing(6)
            pm = get_team_pixmap(abbr, 18)
            if pm:
                logo = QLabel()
                logo.setPixmap(pm)
                logo.setFixedSize(18, 18)
                logo.setStyleSheet("background:transparent; border:none; padding:0; margin:0;")
                hl.addWidget(logo)
            hl.addWidget(mk_label(abbr, color=C["t1"], size=13, bold=True, mono=True))
            hl.addWidget(mk_label(pitcher, color=C["t2"], size=11, mono=True), 1)
            if score is not None:
                hl.addWidget(mk_label(str(score), color=C["t1"], size=15, bold=True, mono=True))
            return hl

        def _card_divider(has_score=False):
            """Divider: @ aligned with team abbr, gradient line fading out."""
            div = QHBoxLayout()
            div.setSpacing(4)
            div.setContentsMargins(24, 0, 0, 0)
            div.addWidget(mk_label("@", color=C["t3"], size=10, mono=True))
            div.addWidget(_GradientLine(bg=C["bg1"]), 1)
            if has_score:
                right_line = mk_hline()
                right_line.setFixedWidth(16)
                div.addWidget(right_line)
            return div

        if is_live or is_final:
            # Determine current batter/pitcher labels for live games
            _batter_name = g.get('current_batter_name', '')
            _pitcher_name = g.get('current_pitcher_name', '')
            _batter_hand = g.get('current_batter_hand', '')
            _pitcher_hand = g.get('current_pitcher_hand', '')
            # Suppress during inning transitions (Mid/End) — the API
            # updates batter/pitcher for the next half before inning_half
            # changes, which would show them on the wrong team rows.
            _inn_state = (g.get('inning_state') or '').lower()
            _in_transition = _inn_state.startswith('mid') or _inn_state.startswith('end')
            _show_pab = is_live and _batter_name and _pitcher_name and not _in_transition
            # Format short names: "F. LastName"
            def _short(n):
                p = n.split()
                return f"{p[0][0]}. {' '.join(p[1:])}" if len(p) >= 2 else n
            # Top → away batting, home pitching; Bottom → reversed
            _half = (g.get('inning_half') or '').lower()
            _away_is_batting = _half.startswith('top')
            away_pab = (f"AB: {_short(_batter_name)} ({_batter_hand})" if _away_is_batting
                        else f"P: {_short(_pitcher_name)} ({_pitcher_hand})") if _show_pab else ''
            home_pab = (f"P: {_short(_pitcher_name)} ({_pitcher_hand})" if _away_is_batting
                        else f"AB: {_short(_batter_name)} ({_batter_hand})") if _show_pab else ''

            # Grid layout: teams col 0, diamond col 1, scores col 2
            grid = QGridLayout()
            grid.setHorizontalSpacing(4)
            grid.setVerticalSpacing(4)
            grid.setContentsMargins(0, 0, 0, 0)
            row = 0
            # Row 0: away team + score
            grid.addLayout(team_row(g["away"], g.get("away_p", "")), row, 0)
            grid.addWidget(mk_label(str(g.get("away_score", 0)),
                                    color=C["t1"], size=15, bold=True, mono=True,
                                    align=Qt.AlignmentFlag.AlignRight), row, 2)
            row += 1
            # Row 1: away P/AB label (or empty placeholder)
            if _show_pab:
                lbl = mk_label(away_pab, color=C["t3"], size=9, mono=True, bold=True)
            else:
                lbl = mk_label("", size=9, mono=True)
            lbl.setContentsMargins(24, 0, 0, 0)
            grid.addWidget(lbl, row, 0)
            row += 1
            # Row 2: dividers
            grid.addLayout(_card_divider(has_score=False), row, 0)
            div_line = mk_hline()
            div_line.setFixedWidth(16)
            grid.addWidget(div_line, row, 2, Qt.AlignmentFlag.AlignRight)
            row += 1
            # Row 3: home team + score
            grid.addLayout(team_row(g["home"], g.get("home_p", "")), row, 0)
            grid.addWidget(mk_label(str(g.get("home_score", 0)),
                                    color=C["t1"], size=15, bold=True, mono=True,
                                    align=Qt.AlignmentFlag.AlignRight), row, 2)
            row += 1
            # Row 4: home P/AB label (or empty placeholder)
            if _show_pab:
                lbl = mk_label(home_pab, color=C["t3"], size=9, mono=True, bold=True)
            else:
                lbl = mk_label("", size=9, mono=True)
            lbl.setContentsMargins(24, 0, 0, 0)
            grid.addWidget(lbl, row, 0)
            row += 1
            # Diamond spanning all rows in column 1 (live games only)
            if is_live:
                diamond = MiniDiamondWidget(
                    r1b=g.get('on_first', False),
                    r2b=g.get('on_second', False),
                    r3b=g.get('on_third', False),
                    outs=g.get('outs', 0))
                grid.addWidget(diamond, 0, 1, row, 1, Qt.AlignmentFlag.AlignCenter)
                grid.setColumnMinimumWidth(1, 60)
            grid.setColumnStretch(0, 1)
            self._root.addLayout(grid)
        else:
            show_score = is_final
            self._root.addLayout(team_row(g["away"], g.get("away_p",""),
                                    g.get("away_score") if show_score else None))
            ph = mk_label("", size=9, mono=True)
            ph.setContentsMargins(24, 0, 0, 0)
            self._root.addWidget(ph)
            self._root.addLayout(_card_divider(has_score=show_score))
            self._root.addLayout(team_row(g["home"], g.get("home_p",""),
                                    g.get("home_score") if show_score else None))
            ph2 = mk_label("", size=9, mono=True)
            ph2.setContentsMargins(24, 0, 0, 0)
            self._root.addWidget(ph2)

    def mousePressEvent(self, e):
        self._cb(self._idx)
        super().mousePressEvent(e)
 
    def enterEvent(self, e):
        if not self._sel:
            self.setStyleSheet(self.styleSheet().replace(
                f"background:{C['bg1']}", f"background:{C['bg2']}"))
            for gl in self.findChildren(_GradientLine):
                gl.set_bg(C["bg2"])
        super().enterEvent(e)
 
    def leaveEvent(self, e):
        if not self._sel:
            self._restyle()
            for gl in self.findChildren(_GradientLine):
                gl.set_bg(C["bg1"])
        super().leaveEvent(e)


class ScheduleGameCard(QFrame):
    """Game card for the schedule page with DraftKings betting lines."""
    _collapsed_height = 0  # class-level: max collapsed height across all cards

    def __init__(self, game: dict, idx: int, odds: dict | None, on_click=None, parent=None):
        super().__init__(parent)
        self.game = game
        self._idx = idx
        self._odds = odds or {}
        self._cb = on_click
        self._plays = []          # accumulated play-by-play data
        self._expanded = False    # play log expanded?
        self._play_log = None     # QWidget holding the play log
        self._toggle_btn = None   # expand/collapse button
        self._last_event_lbl = None
        self._game_content = None # refreshable game-data area
        self._play_section = None # persistent play-log area
        self._live_preview = ""   # pitch-by-pitch preview string
        self._diamond = None      # MiniDiamondWidget ref for live count updates
        self.setObjectName("SGC")
        if on_click:
            self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFixedHeight(142)
        self.setStyleSheet(f"""
            #SGC {{ background:{C['bg1']}; border:1px solid {C['bdr']};
                   border-radius:6px; }}
            #SGC:hover {{ background:{C['bg2']}; }}
        """)
        self._build()

    def _build(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(14, 8, 14, 6)
        outer.setSpacing(2)
        # Game content area (rebuilt on update_game)
        self._game_content = QWidget()
        self._game_content.setStyleSheet("background:transparent;")
        gc_layout = QVBoxLayout(self._game_content)
        gc_layout.setContentsMargins(0, 0, 0, 0)
        gc_layout.setSpacing(2)
        self._build_content(gc_layout)
        outer.addWidget(self._game_content)
        # Play section (persists across game refreshes)
        self._build_play_section(outer)
        # Defer initial resize so glow effects don't trigger painter errors
        QTimer.singleShot(0, self._resize_for_expansion)

    def _build_content(self, root):
        g = self.game
        odds = self._odds

        ppd = (g.get("status") or "").lower().startswith("postponed")
        st = (g.get("status") or "").lower()
        is_final = (g.get("time", "").upper() == "FINAL"
                    or st.startswith("final") or st.startswith("game over")
                    or st.startswith("completed"))
        is_live = _is_game_live(g)
        show_score = is_live or is_final
        innings_detail = g.get('innings_detail', [])
        has_boxscore = show_score and len(innings_detail) > 0

        # ── Time / status row ──
        time_hl = QHBoxLayout()
        time_hl.setSpacing(4)
        time_hl.setContentsMargins(0, 0, 0, 4)
        if is_live:
            time_hl.addWidget(mk_label("●", color=C["red"], size=10, mono=True))
            lbl = mk_label("Live", color=C["red"], size=10, mono=True, bold=True)
            glow = QGraphicsDropShadowEffect(lbl)
            glow.setColor(QColor(C["red"]))
            glow.setBlurRadius(12)
            glow.setOffset(0, 0)
            lbl.setGraphicsEffect(glow)
            time_hl.addWidget(lbl)
            _gstatus = (g.get('status') or '').lower()
            if _gstatus.startswith('warmup'):
                time_hl.addWidget(mk_label(
                    "WARMUPS", color=C["t3"], size=10, mono=True))
            else:
                inn = g.get("inning")
                state = (g.get("inning_state") or g.get("inning_half") or "").lower()
                if inn is not None:
                    if state.startswith("top"):
                        hlbl = "TOP"
                    elif state.startswith("mid"):
                        hlbl = "MID"
                    elif state.startswith("end"):
                        hlbl = "END"
                    else:
                        hlbl = "BOT"
                    time_hl.addWidget(mk_label(f"{hlbl} {inn}", color=C["t3"], size=10, mono=True))
        elif is_final:
            time_hl.addWidget(mk_label("Final", color=C["t3"], size=10, mono=True))
        elif ppd:
            time_hl.addWidget(mk_label("PPD", color=C["red"], size=10, mono=True))
        else:
            time_hl.addWidget(mk_label(g.get("time", "TBD"), color=C["t3"], size=10, mono=True))
        time_hl.addStretch()
        root.addLayout(time_hl)

        # ── Team row helper ──
        def team_row(abbr, pitcher, ml=""):
            hl = QHBoxLayout()
            hl.setSpacing(6)
            pm = get_team_pixmap(abbr, 20)
            if pm:
                logo = QLabel()
                logo.setPixmap(pm)
                logo.setFixedSize(20, 20)
                logo.setStyleSheet("background:transparent; border:none;")
                hl.addWidget(logo)
            hl.addWidget(mk_label(abbr, color=C["t1"], size=13, bold=True, mono=True))
            hl.addWidget(mk_label(pitcher or "TBD", color=C["t2"], size=11, mono=True), 1)
            if ml:
                hl.addWidget(mk_label("ML:", color=C["t3"], size=9, mono=True))
                hl.addWidget(mk_label(ml, color=C["ora"], size=10, bold=True, mono=True))
            return hl

        _aml = odds.get("away_ml")
        _hml = odds.get("home_ml")
        away_ml = f"{_aml:+d}" if isinstance(_aml, (int, float)) and _aml else (str(_aml) if _aml else "")
        home_ml = f"{_hml:+d}" if isinstance(_hml, (int, float)) and _hml else (str(_hml) if _hml else "")
        ou = odds.get("over_under")
        ou_str = str(ou) if ou is not None else ""

        # Current batter / pitcher labels (live games only)
        _batter_name = g.get('current_batter_name', '')
        _pitcher_name = g.get('current_pitcher_name', '')
        _batter_hand = g.get('current_batter_hand', '')
        _pitcher_hand = g.get('current_pitcher_hand', '')
        # Suppress during inning transitions (Mid/End) — the API
        # updates batter/pitcher for the next half before inning_half
        # changes, which would show them on the wrong team rows.
        _inn_state = (g.get('inning_state') or '').lower()
        _in_transition = _inn_state.startswith('mid') or _inn_state.startswith('end')
        _show_pab = is_live and _batter_name and _pitcher_name and not _in_transition
        def _short(n):
            p = n.split()
            return f"{p[0][0]}. {' '.join(p[1:])}" if len(p) >= 2 else n
        _half = (g.get('inning_half') or '').lower()
        _away_is_batting = _half.startswith('top')
        away_pab = (f"AB: {_short(_batter_name)} ({_batter_hand})" if _away_is_batting
                    else f"P: {_short(_pitcher_name)} ({_pitcher_hand})") if _show_pab else ''
        home_pab = (f"P: {_short(_pitcher_name)} ({_pitcher_hand})" if _away_is_batting
                    else f"AB: {_short(_batter_name)} ({_batter_hand})") if _show_pab else ''

        if has_boxscore:
            # ── Live/final with box score ──

            # Grid: teams col 0, diamond col 1, scores col 2
            grid = QGridLayout()
            grid.setHorizontalSpacing(4)
            grid.setVerticalSpacing(4)
            grid.setContentsMargins(0, 0, 0, 0)
            row = 0
            # Row 0: away team + score
            grid.addLayout(team_row(g["away"], g.get("away_p", ""), away_ml), row, 0)
            grid.addWidget(mk_label(str(g.get("away_score", 0)),
                                    color=C["t1"], size=15, bold=True, mono=True,
                                    align=Qt.AlignmentFlag.AlignRight), row, 2)
            row += 1
            # Away P/AB label (or empty placeholder)
            if _show_pab:
                lbl = mk_label(away_pab, color=C["t3"], size=9, mono=True, bold=True)
            else:
                lbl = mk_label("", size=9, mono=True)
            lbl.setContentsMargins(26, 0, 0, 0)
            grid.addWidget(lbl, row, 0)
            row += 1
            # Divider with O/U
            div = QHBoxLayout()
            div.setSpacing(4)
            div.setContentsMargins(26, 0, 0, 0)
            div.addWidget(mk_label("@", color=C["t3"], size=10, mono=True))
            div.addWidget(_GradientLine(), 1)
            if ou_str:
                div.addWidget(mk_label("O/U", color=C["t3"], size=9, mono=True))
                div.addWidget(mk_label(ou_str, color=C["t1"], size=9, bold=True, mono=True))
            grid.addLayout(div, row, 0)
            row += 1
            # Home team + score
            grid.addLayout(team_row(g["home"], g.get("home_p", ""), home_ml), row, 0)
            grid.addWidget(mk_label(str(g.get("home_score", 0)),
                                    color=C["t1"], size=15, bold=True, mono=True,
                                    align=Qt.AlignmentFlag.AlignRight), row, 2)
            row += 1
            # Home P/AB label (or empty placeholder)
            if _show_pab:
                lbl = mk_label(home_pab, color=C["t3"], size=9, mono=True, bold=True)
            else:
                lbl = mk_label("", size=9, mono=True)
            lbl.setContentsMargins(26, 0, 0, 0)
            grid.addWidget(lbl, row, 0)
            row += 1
            # Diamond spanning all rows in column 1 (live games only)
            if is_live:
                diamond = MiniDiamondWidget(
                    r1b=g.get('on_first', False),
                    r2b=g.get('on_second', False),
                    r3b=g.get('on_third', False),
                    outs=g.get('outs', 0),
                    balls=g.get('balls', 0),
                    strikes=g.get('strikes', 0))
                self._diamond = diamond
                grid.addWidget(diamond, 0, 1, row, 1, Qt.AlignmentFlag.AlignCenter)
                grid.setColumnMinimumWidth(1, 60)
            else:
                grid.setColumnMinimumWidth(1, 16)
            grid.setColumnStretch(0, 1)
            root.addLayout(grid)

            root.addSpacing(14)

            # ── Box score ──
            # Pad innings_detail to at least 9 entries
            padded = list(innings_detail)
            while len(padded) < 9:
                padded.append({'num': len(padded) + 1, 'away': None, 'home': None})
            num_inn = len(padded)
            extra = num_inn > 9

            # Build the innings-only grid (columns: team abbr + inning cols)
            inn_w = QWidget()
            inn_w.setStyleSheet("background:transparent;")
            inn_grid = QGridLayout(inn_w)
            inn_grid.setContentsMargins(0, 0, 0, 0)
            inn_grid.setHorizontalSpacing(0)
            inn_grid.setVerticalSpacing(1)

            # Team abbreviation column
            away_name = mk_label(g["away"], color=C["t2"], size=9, mono=True)
            away_name.setFixedWidth(32)
            inn_grid.addWidget(away_name, 1, 0)
            home_name = mk_label(g["home"], color=C["t2"], size=9, mono=True)
            home_name.setFixedWidth(32)
            inn_grid.addWidget(home_name, 2, 0)

            # Header row: inning numbers
            for i, inn in enumerate(padded):
                lbl = mk_label(str(inn['num']), color=C["t3"], size=9, mono=True,
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(20)
                inn_grid.addWidget(lbl, 0, i + 1)

            # Away inning scores
            for i, inn in enumerate(padded):
                val = inn.get('away')
                txt = str(val) if val is not None else "-"
                lbl = mk_label(txt, color=C["t1" if val is not None else "t3"], size=9, mono=True,
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(20)
                inn_grid.addWidget(lbl, 1, i + 1)

            # Home inning scores
            for i, inn in enumerate(padded):
                val = inn.get('home')
                txt = str(val) if val is not None else "-"
                lbl = mk_label(txt, color=C["t1" if val is not None else "t3"], size=9, mono=True,
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(20)
                inn_grid.addWidget(lbl, 2, i + 1)

            # Build R, H, E fixed column widget
            rhe_w = QWidget()
            rhe_w.setStyleSheet("background:transparent;")
            rhe_grid = QGridLayout(rhe_w)
            rhe_grid.setContentsMargins(0, 0, 0, 0)
            rhe_grid.setHorizontalSpacing(0)
            rhe_grid.setVerticalSpacing(1)
            for j, hdr in enumerate(["R", "H", "E"]):
                lbl = mk_label(hdr, color=C["t3"], size=9, mono=True, bold=True,
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(24)
                rhe_grid.addWidget(lbl, 0, j)
            for j, val in enumerate([g.get('away_score', 0), g.get('away_hits', 0), g.get('away_errors', 0)]):
                lbl = mk_label(str(val), color=C["t1"], size=9, mono=True, bold=(j == 0),
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(24)
                rhe_grid.addWidget(lbl, 1, j)
            for j, val in enumerate([g.get('home_score', 0), g.get('home_hits', 0), g.get('home_errors', 0)]):
                lbl = mk_label(str(val), color=C["t1"], size=9, mono=True, bold=(j == 0),
                               align=Qt.AlignmentFlag.AlignCenter)
                lbl.setFixedWidth(24)
                rhe_grid.addWidget(lbl, 2, j)

            # Assemble: innings (scrollable if extra) | R H E (fixed)
            box_hl = QHBoxLayout()
            box_hl.setContentsMargins(0, 0, 0, 0)
            box_hl.setSpacing(0)
            if extra:
                scroll = QScrollArea()
                scroll.setWidgetResizable(True)
                scroll.setWidget(inn_w)
                scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
                scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
                scroll.setFixedHeight(inn_w.sizeHint().height() + 14)
                scroll.setStyleSheet(
                    f"QScrollArea {{ background:transparent; border:none; }}"
                    f"QScrollBar:horizontal {{ height:4px; background:transparent; }}"
                    f"QScrollBar::handle:horizontal {{ background:{C['t3']}; border-radius:2px; }}"
                    f"QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width:0; }}"
                )
                box_hl.addWidget(scroll, 1)
            else:
                box_hl.addWidget(inn_w, 1)
            box_hl.addWidget(rhe_w)
            root.addLayout(box_hl)

        else:
            if show_score:
                # ── Live/final but no inning detail yet ──
                grid = QGridLayout()
                grid.setHorizontalSpacing(4)
                grid.setVerticalSpacing(4)
                grid.setContentsMargins(0, 0, 0, 0)
                row = 0
                grid.addLayout(team_row(g["away"], g.get("away_p", ""), away_ml), row, 0)
                grid.addWidget(mk_label(str(g.get("away_score", 0)),
                                        color=C["t1"], size=15, bold=True, mono=True,
                                        align=Qt.AlignmentFlag.AlignRight), row, 2)
                row += 1
                # Away P/AB label (or empty placeholder)
                if _show_pab:
                    lbl = mk_label(away_pab, color=C["t3"], size=9, mono=True, bold=True)
                else:
                    lbl = mk_label("", size=9, mono=True)
                lbl.setContentsMargins(26, 0, 0, 0)
                grid.addWidget(lbl, row, 0)
                row += 1
                div = QHBoxLayout()
                div.setSpacing(4)
                div.setContentsMargins(26, 0, 0, 0)
                div.addWidget(mk_label("@", color=C["t3"], size=10, mono=True))
                div.addWidget(_GradientLine(), 1)
                if ou_str:
                    div.addWidget(mk_label("O/U", color=C["t3"], size=9, mono=True))
                    div.addWidget(mk_label(ou_str, color=C["t1"], size=9, bold=True, mono=True))
                grid.addLayout(div, row, 0)
                row += 1
                grid.addLayout(team_row(g["home"], g.get("home_p", ""), home_ml), row, 0)
                grid.addWidget(mk_label(str(g.get("home_score", 0)),
                                        color=C["t1"], size=15, bold=True, mono=True,
                                        align=Qt.AlignmentFlag.AlignRight), row, 2)
                row += 1
                # Home P/AB label (or empty placeholder)
                if _show_pab:
                    lbl = mk_label(home_pab, color=C["t3"], size=9, mono=True, bold=True)
                else:
                    lbl = mk_label("", size=9, mono=True)
                lbl.setContentsMargins(26, 0, 0, 0)
                grid.addWidget(lbl, row, 0)
                row += 1
                # Diamond spanning all rows in column 1 (live games only)
                if is_live:
                    diamond = MiniDiamondWidget(
                        r1b=g.get('on_first', False),
                        r2b=g.get('on_second', False),
                        r3b=g.get('on_third', False),
                        outs=g.get('outs', 0),
                        balls=g.get('balls', 0),
                        strikes=g.get('strikes', 0))
                    self._diamond = diamond
                    grid.addWidget(diamond, 0, 1, row, 1, Qt.AlignmentFlag.AlignCenter)
                    grid.setColumnMinimumWidth(1, 60)
                else:
                    grid.setColumnMinimumWidth(1, 16)
                grid.setColumnStretch(0, 1)
                root.addLayout(grid)
                root.addStretch()
            else:
                # ── Scheduled games ──
                root.addLayout(team_row(g["away"], g.get("away_p", ""), away_ml))
                ph = mk_label("", size=9, mono=True)
                ph.setContentsMargins(26, 0, 0, 0)
                root.addWidget(ph)
                div = QHBoxLayout()
                div.setSpacing(4)
                div.setContentsMargins(26, 0, 0, 0)
                div.addWidget(mk_label("@", color=C["t3"], size=10, mono=True))
                div.addWidget(_GradientLine(), 1)
                if ou_str:
                    div.addWidget(mk_label("O/U", color=C["t3"], size=9, mono=True))
                    div.addWidget(mk_label(ou_str, color=C["t1"], size=9, bold=True, mono=True))
                root.addLayout(div)
                root.addLayout(team_row(g["home"], g.get("home_p", ""), home_ml))
                ph2 = mk_label("", size=9, mono=True)
                ph2.setContentsMargins(26, 0, 0, 0)
                root.addWidget(ph2)

                root.addSpacing(14)

                # ── Empty box score with "-" placeholders ──
                inn_w = QWidget()
                inn_w.setStyleSheet("background:transparent;")
                inn_grid = QGridLayout(inn_w)
                inn_grid.setContentsMargins(0, 0, 0, 0)
                inn_grid.setHorizontalSpacing(0)
                inn_grid.setVerticalSpacing(1)
                away_name = mk_label(g["away"], color=C["t2"], size=9, mono=True)
                away_name.setFixedWidth(32)
                inn_grid.addWidget(away_name, 1, 0)
                home_name = mk_label(g["home"], color=C["t2"], size=9, mono=True)
                home_name.setFixedWidth(32)
                inn_grid.addWidget(home_name, 2, 0)
                for i in range(9):
                    lbl = mk_label(str(i + 1), color=C["t3"], size=9, mono=True,
                                   align=Qt.AlignmentFlag.AlignCenter)
                    lbl.setFixedWidth(20)
                    inn_grid.addWidget(lbl, 0, i + 1)
                    for row in (1, 2):
                        d = mk_label("-", color=C["t3"], size=9, mono=True,
                                     align=Qt.AlignmentFlag.AlignCenter)
                        d.setFixedWidth(20)
                        inn_grid.addWidget(d, row, i + 1)
                rhe_w = QWidget()
                rhe_w.setStyleSheet("background:transparent;")
                rhe_grid = QGridLayout(rhe_w)
                rhe_grid.setContentsMargins(0, 0, 0, 0)
                rhe_grid.setHorizontalSpacing(0)
                rhe_grid.setVerticalSpacing(1)
                for j, hdr in enumerate(["R", "H", "E"]):
                    lbl = mk_label(hdr, color=C["t3"], size=9, mono=True, bold=True,
                                   align=Qt.AlignmentFlag.AlignCenter)
                    lbl.setFixedWidth(24)
                    rhe_grid.addWidget(lbl, 0, j)
                    for row in (1, 2):
                        d = mk_label("-", color=C["t3"], size=9, mono=True,
                                     align=Qt.AlignmentFlag.AlignCenter)
                        d.setFixedWidth(24)
                        rhe_grid.addWidget(d, row, j)
                box_hl = QHBoxLayout()
                box_hl.setContentsMargins(0, 0, 0, 0)
                box_hl.setSpacing(0)
                box_hl.addWidget(inn_w, 1)
                box_hl.addWidget(rhe_w)
                root.addLayout(box_hl)

    def _build_play_section(self, outer):
        """Build the persistent play-log section (not rebuilt on score refresh)."""
        self._play_section = QWidget()
        self._play_section.setStyleSheet("background:transparent;")
        ps_layout = QVBoxLayout(self._play_section)
        ps_layout.setContentsMargins(0, 0, 0, 0)
        ps_layout.setSpacing(0)

        # Last-event preview — fixed 2-line height for uniform card sizing
        self._last_event_lbl = mk_label("", color=C["t2"], size=11)
        self._last_event_lbl.setWordWrap(True)
        self._last_event_lbl.setFixedHeight(30)
        self._last_event_lbl.setStyleSheet(
            f"color:{C['t2']}; background:transparent; font-family:'Segoe UI','Inter',sans-serif; font-size:11px; padding:0;")
        if self._plays:
            self._update_last_event()
        ps_layout.addWidget(self._last_event_lbl)

        self._toggle_btn = QPushButton("▸ Play Log")
        self._toggle_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._toggle_btn.setStyleSheet(f"""
            QPushButton {{ background:transparent; color:{C['t3']}; border:none;
                font-family:'Segoe UI','Inter',sans-serif; font-size:10px; padding:2px 0; }}
            QPushButton:hover {{ color:{C['t1']}; }}
        """)
        self._toggle_btn.setFixedHeight(18)
        self._toggle_btn.clicked.connect(self._toggle_play_log)
        ps_layout.addWidget(self._toggle_btn, 0, Qt.AlignmentFlag.AlignLeft)

        # Play log container (hidden by default)
        self._play_log = QWidget()
        self._play_log.setStyleSheet("background:transparent;")
        self._play_log.setVisible(False)
        pl_vl = QVBoxLayout(self._play_log)
        pl_vl.setContentsMargins(0, 4, 0, 0)
        pl_vl.setSpacing(0)
        self._play_scroll = SmoothScrollArea()
        self._play_scroll.setWidgetResizable(True)
        self._play_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._play_scroll.setFixedHeight(180)
        self._play_scroll.setStyleSheet(
            f"QScrollArea {{ background:transparent; border:none; }}"
            f"QScrollBar:vertical {{ width:4px; background:transparent; }}"
            f"QScrollBar::handle:vertical {{ background:{C['t3']}; border-radius:2px; }}"
            f"QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}"
        )
        self._play_content = QWidget()
        self._play_content.setStyleSheet("background:transparent;")
        self._play_layout = QVBoxLayout(self._play_content)
        self._play_layout.setContentsMargins(0, 0, 0, 0)
        self._play_layout.setSpacing(1)
        self._play_layout.addStretch()
        self._play_scroll.setWidget(self._play_content)
        pl_vl.addWidget(self._play_scroll)
        ps_layout.addWidget(self._play_log)

        # Populate if we already have plays
        if self._plays:
            self._render_plays()
        if self._expanded:
            self._play_log.setVisible(True)
            self._toggle_btn.setText("▾ Play Log")
            self._resize_for_expansion()

        outer.addWidget(self._play_section)

    def _toggle_play_log(self):
        """Expand or collapse the play-by-play log."""
        self._expanded = not self._expanded
        if self._play_log:
            self._play_log.setVisible(self._expanded)
        if self._toggle_btn:
            self._toggle_btn.setText("▾ Play Log" if self._expanded else "▸ Play Log")
        if self._expanded and self._plays:
            self._render_plays()
        elif self._expanded and not self._plays and self._play_layout:
            # Show placeholder message
            while self._play_layout.count():
                item = self._play_layout.takeAt(0)
                w = item.widget()
                if w:
                    w.deleteLater()
            st = (self.game.get('status') or '').lower()
            is_live = _is_game_live(self.game)
            is_final = ('final' in st or 'game over' in st or 'completed' in st
                        or self.game.get('time', '').upper() == 'FINAL')
            msg = "  Loading plays\u2026" if (is_live or is_final) else "  No play-by-play info yet"
            self._play_layout.addWidget(
                mk_label(msg, color=C["t3"], size=11))
            self._play_layout.addStretch()
        self._resize_for_expansion()

    def _resize_for_expansion(self):
        """Adjust card size for expand/collapse state."""
        self.setMinimumHeight(0)
        self.setMaximumHeight(16777215)
        # Temporarily strip glow effects so adjustSize() doesn't trigger painter errors
        effects = []
        for child in self.findChildren(QWidget):
            eff = child.graphicsEffect()
            if isinstance(eff, QGraphicsDropShadowEffect):
                effects.append((child, eff))
                child.setGraphicsEffect(None)
        self.adjustSize()
        h = self.sizeHint().height()
        # Re-apply glow effects
        for child, eff in effects:
            try:
                child.setGraphicsEffect(eff)
            except RuntimeError:
                pass  # widget deleted
        if self._expanded:
            self.setFixedHeight(h)
        else:
            if h > ScheduleGameCard._collapsed_height:
                ScheduleGameCard._collapsed_height = h
            self.setFixedHeight(ScheduleGameCard._collapsed_height)

    def _update_last_event(self):
        """Set the always-visible preview label.

        When a live_preview string is available (from the latest pitch or
        action in currentPlay), display it directly.  Otherwise fall back
        to the last completed play in the log.
        """
        if not self._last_event_lbl:
            return
        st = (self.game.get('status') or '').lower()
        is_final = ('final' in st or 'game over' in st or 'completed' in st
                    or self.game.get('time', '').upper() == 'FINAL')
        if is_final:
            self._last_event_lbl.setText("")
            return
        if not self._plays and not self._live_preview:
            return

        if self._live_preview:
            txt = self._live_preview
            self._last_event_lbl.setStyleSheet(
                f"color:{C['t2']}; background:transparent; font-family:'Segoe UI','Inter',sans-serif; font-size:11px; padding:0;")
        else:
            last = self._plays[-1] if self._plays else None
            if not last:
                return
            evt = last.get("event", "")
            desc = last.get("description", "")
            txt = f"{evt}: {desc}" if evt else desc
            if last.get("is_scoring"):
                self._last_event_lbl.setStyleSheet(
                    f"color:#4CAF50; background:transparent; font-family:'Segoe UI','Inter',sans-serif; font-size:11px; padding:0;")
            else:
                self._last_event_lbl.setStyleSheet(
                    f"color:{C['t2']}; background:transparent; font-family:'Segoe UI','Inter',sans-serif; font-size:11px; padding:0;")
        self._last_event_lbl.setText(txt)

    def update_plays(self, plays, live_preview="", live_count=None):
        """Update the play-by-play data and refresh the log widget."""
        old_count = len(self._plays)
        self._plays = plays
        self._live_preview = live_preview
        self._update_last_event()
        # Update diamond balls/strikes from live count
        if live_count and self._diamond and not _is_deleted(self._diamond):
            self._diamond.balls = max(0, min(3, live_count.get('balls', 0) or 0))
            self._diamond.strikes = max(0, min(2, live_count.get('strikes', 0) or 0))
            self._diamond.update()
        if self._play_log and self._expanded:
            if len(plays) != old_count:
                self._render_plays()

    def _render_plays(self):
        """Rebuild the play log labels from self._plays."""
        if not self._play_layout:
            return
        # Clear existing items
        while self._play_layout.count():
            item = self._play_layout.takeAt(0)
            w = item.widget()
            if w:
                w.hide()
                w.deleteLater()
        # Build play entries grouped by inning
        last_inning_hdr = None
        for p in self._plays:
            inn = p.get('inning', 0)
            half = (p.get('half') or '').capitalize()
            hdr_key = f"{half} {inn}"
            if hdr_key != last_inning_hdr:
                last_inning_hdr = hdr_key
                sep = QWidget()
                sep.setFixedHeight(1)
                sep.setStyleSheet(f"background:{C['bdr']};")
                self._play_layout.addWidget(sep)
                self._play_layout.addWidget(
                    mk_label(f"── {hdr_key} ──", color=C["t3"], size=11))
            ev = p.get('event', '')
            desc = p.get('description', '')
            scoring = p.get('is_scoring', False)
            is_action = p.get('is_action', False)
            if is_action:
                event_color = C["grn"] if scoring else C["amb"]
            else:
                event_color = C["grn"] if scoring else C["ora"]
            if ev:
                el = mk_label(ev, color=event_color, size=11, bold=not is_action)
                el.setContentsMargins(12, 0, 0, 0)
                self._play_layout.addWidget(el)
            if desc:
                dl = mk_label(desc, color=C["t2"], size=11)
                dl.setWordWrap(True)
                dl.setContentsMargins(24, 0, 0, 0)
                self._play_layout.addWidget(dl)
        self._play_layout.addStretch()
        # Auto-scroll to bottom
        QTimer.singleShot(50, lambda: (
            self._play_scroll.verticalScrollBar().setValue(
                self._play_scroll.verticalScrollBar().maximum())
            if self._play_scroll and not _is_deleted(self._play_scroll) else None
        ))

    def mousePressEvent(self, e):
        # Don't navigate when clicking toggle button or play section area
        child = self.childAt(e.pos().toPoint() if hasattr(e.pos(), 'toPoint') else e.pos())
        if child and (child is self._toggle_btn or
                      (self._play_section and self._play_section.isAncestorOf(child))):
            return super().mousePressEvent(e)
        if self._cb:
            self._cb(self._idx)
        super().mousePressEvent(e)

    @staticmethod
    def _clear_layout(layout):
        while layout.count():
            item = layout.takeAt(0)
            w = item.widget()
            if w:
                w.setGraphicsEffect(None)
                w.hide()
                w.deleteLater()
            elif item.layout():
                ScheduleGameCard._clear_layout(item.layout())
                item.layout().deleteLater()

    def update_game(self, new_game):
        """Rebuild the card with fresh game data (preserves play log section)."""
        self.game = new_game
        outer = self.layout()
        # Replace _game_content entirely to avoid QPainter conflicts
        if self._game_content:
            self._game_content.hide()
            for child in self._game_content.findChildren(QWidget):
                if child.graphicsEffect():
                    child.setGraphicsEffect(None)
            self._game_content.setParent(None)
            self._game_content.deleteLater()
        self._game_content = QWidget()
        self._game_content.setStyleSheet("background:transparent;")
        gc_layout = QVBoxLayout(self._game_content)
        gc_layout.setContentsMargins(0, 0, 0, 0)
        gc_layout.setSpacing(2)
        self._build_content(gc_layout)
        outer.insertWidget(0, self._game_content)
        # Re-check if play section needs toggle (e.g. game went live)
        st = (new_game.get('status') or '').lower()
        is_final = (new_game.get('time', '').upper() == 'FINAL'
                    or st.startswith('final') or st.startswith('game over')
                    or st.startswith('completed'))
        is_live = _is_game_live(new_game)
        if (is_live or is_final) and not self._toggle_btn and self._play_section:
            # Game just went live — rebuild entire play section
            old_ps = self._play_section
            outer = self.layout()
            if outer:
                outer.removeWidget(old_ps)
                old_ps.setGraphicsEffect(None)
                old_ps.hide()
                old_ps.deleteLater()
            self._play_log = None
            self._toggle_btn = None
            self._play_scroll = None
            self._play_content = None
            self._play_layout = None
            self._last_event_lbl = None
            self._play_section = None
            if outer:
                self._build_play_section(outer)
        self._resize_for_expansion()

    def enterEvent(self, event):
        for gl in self.findChildren(_GradientLine):
            gl.set_bg(C["bg2"])
        super().enterEvent(event)

    def leaveEvent(self, event):
        for gl in self.findChildren(_GradientLine):
            gl.set_bg(C["bg1"])
        super().leaveEvent(event)


# ═══════════════════════════════════════════════════════════════════════════════
# Navbar helpers
# ═══════════════════════════════════════════════════════════════════════════════
def _nav_btn_style(active):
    return f"""
        QPushButton {{
            background:transparent;
            color:{C['t1'] if active else C['t3']};
            border:none;
            border-bottom:2px solid {C['ora'] if active else 'transparent'};
            border-radius:0px; padding:0 10px;
            font-family:'Segoe UI'; font-size:12px;
            font-weight:{'700' if active else '400'};
            letter-spacing:1px;
        }}
        QPushButton:hover {{ color:{C['t1']}; }}
    """
 
 
def build_navbar(height, items, on_switch, bg=None):
    bar = QFrame()
    bar.setFixedHeight(height)
    _bg = bg or C["bg0"]
    bar.setStyleSheet(f"QFrame {{ background:{_bg}; border-bottom:1px solid {C['bdr']}; }}")
    btns = []
    hl = QHBoxLayout(bar)
    hl.setContentsMargins(8, 0, 8, 0)
    hl.setSpacing(0)
 
    def restyle():
        for b in btns:
            b.setStyleSheet(_nav_btn_style(b.isChecked()))
 
    for i, (num, name) in enumerate(items):
        btn = QPushButton(f"{num}  {name}")
        btn.setCheckable(True)
        btn.setChecked(i == 0)
        btn.setFixedHeight(height)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
 
        def _click(_, idx=i, _btns=btns, _rs=restyle, _sw=on_switch):
            for j, b in enumerate(_btns):
                b.setChecked(j == idx)
            _rs()
            _sw(idx)
 
        btn.clicked.connect(_click)
        btns.append(btn)
        hl.addWidget(btn)
 
    hl.addStretch()
    restyle()
    bar._layout = hl          # expose so callers can append right-side widgets
    return bar, btns
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Game banner
# ═══════════════════════════════════════════════════════════════════════════════
class GameBanner(QFrame):
    def __init__(self, game: dict, parent=None):
        super().__init__(parent)
        self.setFixedHeight(54)
        self.setStyleSheet(f"QFrame {{ background:{C['bg0']}; border-bottom:1px solid {C['bdr']}; }}")
        hl = QHBoxLayout(self)
        hl.setContentsMargins(16, 0, 16, 0)
        hl.setSpacing(8)
        hl.addWidget(mk_label(game["away"], color=C["t1"], size=15, bold=True, mono=True))
        hl.addWidget(mk_label(game.get("away_p","TBD"), color=C["t2"], size=12, mono=True))
        hl.addWidget(mk_label("@", color=C["t3"], size=13, mono=True))
        hl.addWidget(mk_label(game.get("home_p","TBD"), color=C["t2"], size=12, mono=True))
        hl.addWidget(mk_label(game["home"], color=C["t1"], size=15, bold=True, mono=True))
        hl.addStretch()
        hl.addWidget(mk_label(
            game["time"],
            color=C["red"] if game.get("live") else C["t3"],
            size=11, mono=True))
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Combo box with hamburger (☰) indicator
# ═══════════════════════════════════════════════════════════════════════════════
class _MenuComboBox(QComboBox):
    """QComboBox that draws a 3-line hamburger icon in the drop-down area."""
    def paintEvent(self, event):
        super().paintEvent(event)
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.setPen(QPen(QColor(C["t3"]), 1.2))
        x = self.width() - 16
        cy = self.height() / 2
        w = 8
        for dy in (-3, 0, 3):
            p.drawLine(int(x), int(cy + dy), int(x + w), int(cy + dy))
        p.end()


# ═══════════════════════════════════════════════════════════════════════════════
# Game Detail Panel
# ═══════════════════════════════════════════════════════════════════════════════
class GameDetailPanel(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"background:{C['bg0']};")
        self._vl = QVBoxLayout(self)
        self._vl.setContentsMargins(0, 0, 0, 0)
        self._vl.setSpacing(0)
        self._game_data_notifier = _GameDataNotifier()
        self._game_data_notifier.data_ready.connect(self._on_game_data_ready)
        self._pending_data = None  # holds fetched data dict for lazy tab builds
        self._built_tabs = set()   # indices of tabs whose widgets have been built
        self._load_gen = 0          # incremented every load_game; stale signals ignored
 
    def load_game(self, game: dict):
        self._load_gen += 1
        # remember current game for async updates
        try:
            self._current_game = game
        except Exception:
            self._current_game = None
        # Nullify references to old stacks before deleting widgets so stale
        # signal handlers never touch already-deleted C++ objects.
        self._inner_stack = None
        self._pending_data = None
        self._built_tabs = set()
        for attr in ('_team_stack', '_pit_stack', '_br_stack', '_bvp_stack',
                      '_br_tables', '_bvp_tables'):
            try:
                setattr(self, attr, None)
            except Exception:
                pass
        while self._vl.count():
            item = self._vl.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
 
        away = game["away"]
        home = game["home"]
        away_p = game.get("away_p", "TBD")
        home_p = game.get("home_p", "TBD")
        away_p_throws = game.get("away_p_throws", "")
        home_p_throws = game.get("home_p_throws", "")

        # Banner (shown immediately)
        self._vl.addWidget(GameBanner(game))

        # Sub-navbar + inner stack
        inner = QStackedWidget()
        inner.setStyleSheet(f"background:{C['bg0']};")

        sub_items = [("[ 01 ]","BATTING"),("[ 02 ]","PITCHING"),
                     ("[ 03 ]","BASE RUNNING"),("[ 04 ]","BvP")]
        subnav, _ = build_navbar(40, sub_items,
                                 self._on_subnav_changed,
                                 bg=C["bg1"])
        self._inner_stack = inner
        self._vl.addWidget(subnav)

        # ── Filter bar (shown immediately, seasons updated async) ──
        self._build_filter_bar(away, home)

        # Loading placeholder while data fetches in background
        loading_w = QWidget()
        loading_w.setStyleSheet(f"background:{C['bg0']};")
        ll = QVBoxLayout(loading_w)
        ll.setContentsMargins(20, 40, 20, 20)
        ll.addWidget(mk_label("Loading game data…", color=C["t3"], size=14, mono=True,
                               align=Qt.AlignmentFlag.AlignCenter))
        ll.addStretch()
        # Add loading page for all 4 tabs
        for _ in range(4):
            placeholder = QWidget()
            placeholder.setStyleSheet(f"background:{C['bg0']};")
            pl = QVBoxLayout(placeholder)
            pl.setContentsMargins(20, 40, 20, 20)
            pl.addWidget(mk_label("Loading…", color=C["t3"], size=14, mono=True,
                                   align=Qt.AlignmentFlag.AlignCenter))
            pl.addStretch()
            inner.addWidget(placeholder)

        self._vl.addWidget(inner, 1)
        self._current_subnav_idx = 0

        # Fetch data in background thread
        notifier = self._game_data_notifier
        gen = self._load_gen
        def _fetch():
            data = {'game': game, 'away': away, 'home': home,
                    'away_p': away_p, 'home_p': home_p,
                    'away_p_throws': away_p_throws, 'home_p_throws': home_p_throws}
            # Lookup pitcher handedness if missing — check cache first
            def _lookup_throws(pid, pname):
                # Check module-level pitcher cache first
                if pid:
                    cached = _PITCHER_CACHE.get(pid)
                    if cached and cached.get('throws'):
                        return cached['throws']
                if pname:
                    cached_pid = _PITCHER_NAME_CACHE.get(pname)
                    if cached_pid:
                        cached = _PITCHER_CACHE.get(cached_pid)
                        if cached and cached.get('throws'):
                            return cached['throws']
                try:
                    conn = _DM.connect()
                    if not conn:
                        return ''
                    cur = conn.cursor()
                    if pid:
                        cur.execute("SELECT p_throws FROM plate_appearances WHERE pitcher_id=? AND p_throws IS NOT NULL LIMIT 1", (pid,))
                        r = cur.fetchone()
                        if r and r[0]:
                            _cache_pitcher(pid, pname, r[0])
                            return r[0]
                    if pname and pname != 'TBD':
                        cur.execute("SELECT p_throws FROM plate_appearances WHERE pitcher_name=? AND p_throws IS NOT NULL LIMIT 1", (pname,))
                        r = cur.fetchone()
                        if r and r[0]:
                            _cache_pitcher(pid, pname, r[0])
                            return r[0]
                except Exception:
                    log.exception("_lookup_throws")
                return ''
            if not data['away_p_throws']:
                data['away_p_throws'] = _lookup_throws(game.get('away_p_id'), away_p)
            if not data['home_p_throws']:
                data['home_p_throws'] = _lookup_throws(game.get('home_p_id'), home_p)

            gid = game.get('game_id')
            away_starter = {'id': game.get('away_p_id'), 'name': away_p,
                            'throws': data['away_p_throws']}
            home_starter = {'id': game.get('home_p_id'), 'name': home_p,
                            'throws': data['home_p_throws']}

            # Run all 4 data queries in parallel
            def _q_bat():
                try:
                    res = _DM.get_game_lineup(gid) if gid else None
                    return res if res else _game_batting(away, home)
                except Exception:
                    return _game_batting(away, home)

            def _q_pit():
                try:
                    res = _DM.get_game_pitching(gid, away_starter=away_starter,
                                                 home_starter=home_starter) if gid else None
                    return res if res else _game_pitching(away, home, away_p, home_p)
                except Exception:
                    return _game_pitching(away, home, away_p, home_p)

            def _q_br():
                try:
                    return _DM.get_game_baserunning(gid, away_starter=away_starter,
                                                     home_starter=home_starter) if gid else None
                except Exception:
                    return None

            def _q_bvp():
                try:
                    return _DM.get_bvp_data(gid, away_starter=away_starter,
                                             home_starter=home_starter) if gid else None
                except Exception:
                    return None

            with ThreadPoolExecutor(max_workers=4) as detail_pool:
                f_bat = detail_pool.submit(_q_bat)
                f_pit = detail_pool.submit(_q_pit)
                f_br  = detail_pool.submit(_q_br)
                f_bvp = detail_pool.submit(_q_bvp)
                data['bat'] = f_bat.result()
                data['pit'] = f_pit.result()
                data['br']  = f_br.result()
                data['bvp'] = f_bvp.result()

            # Seasons for filter
            try:
                conn_tmp = _DM.connect()
                if conn_tmp:
                    cur_tmp = conn_tmp.cursor()
                    cur_tmp.execute("SELECT DISTINCT season FROM plate_appearances ORDER BY season DESC")
                    data['seasons'] = [str(r[0]) for r in cur_tmp.fetchall() if r and r[0] is not None][:2]
                else:
                    data['seasons'] = []
            except Exception:
                data['seasons'] = []

            notifier.data_ready.emit({'_gen': gen, **data})

        _bg_pool.submit(_fetch)

    def _build_filter_bar(self, away, home):
        """Create the filter bar and insert it above the inner stack."""
        self.season_cb = _MenuComboBox()
        self.season_cb.addItem(str(dt.date.today().year))
        self.matchup_cb = _MenuComboBox()
        self.matchup_cb.addItems(["Both", "RHP", "LHP"])
        self.window_cb = _MenuComboBox()
        self.window_cb.addItems(["All Games", "Last 5 Games", "Last 10 Games", "Last 15 Games", "Last 30 Games"])

        for cb in (self.season_cb, self.matchup_cb, self.window_cb):
            cb.setFixedHeight(28)
            cb.setStyleSheet(_CB_STYLE)
            cb.view().setMinimumWidth(150)
            container = cb.view().window()
            container.setWindowFlags(
                Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint | Qt.WindowType.NoDropShadowWindowHint
            )
            container.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
            container.setStyleSheet("background:transparent; border:none; margin:0; padding:0;")
            cb.view().setAutoFillBackground(True)
            cb.view().setStyleSheet(_CB_VIEW_STYLE)
        self.season_cb.setMinimumWidth(46)
        self.matchup_cb.setMinimumWidth(40)
        self.window_cb.setMinimumWidth(80)
        self.season_cb.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.matchup_cb.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.window_cb.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)

        filt_hdr = QFrame()
        filt_hdr.setFixedHeight(44)
        filt_hdr.setStyleSheet(f"background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px;")
        fhl = QHBoxLayout(filt_hdr)
        fhl.setContentsMargins(10, 0, 10, 0)
        fhl.setSpacing(8)

        self._away_btn = QPushButton(away)
        self._home_btn = QPushButton(home)
        for btn in (self._away_btn, self._home_btn):
            btn.setCheckable(True)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setFixedHeight(28)
        self._away_btn.setChecked(True)
        self._update_team_toggle_style()
        self._away_btn.clicked.connect(lambda: self._set_team_toggle('away'))
        self._home_btn.clicked.connect(lambda: self._set_team_toggle('home'))

        fhl.addWidget(self._away_btn)
        fhl.addWidget(self._home_btn)
        fhl.addStretch()
        self._season_label = mk_label("Season", color=C['t3'], size=10)
        fhl.addWidget(self._season_label)
        fhl.addWidget(self.season_cb)
        self._matchup_label = mk_label("Pitcher", color=C['t3'], size=10)
        fhl.addWidget(self._matchup_label)
        fhl.addWidget(self.matchup_cb)
        fhl.addWidget(mk_label("Time", color=C['t3'], size=10))
        fhl.addWidget(self.window_cb)

        self._vl.addWidget(filt_hdr)

        self.season_cb.currentIndexChanged.connect(self._on_filters_changed)
        self.matchup_cb.currentIndexChanged.connect(self._on_filters_changed)
        self.window_cb.currentIndexChanged.connect(self._on_filters_changed)

    def _on_game_data_ready(self, data):
        """Slot: called on main thread when background data fetch completes."""
        try:
            if _is_deleted(self):
                return
            # Reject stale results from a previous load_game call
            if data.get('_gen') != self._load_gen:
                return
            inner = getattr(self, '_inner_stack', None)
            if not inner or _is_deleted(inner):
                return

            self._pending_data = data
            away = data['away']
            home = data['home']
            away_p = data['away_p']
            home_p = data['home_p']
            away_p_throws = data['away_p_throws']
            home_p_throws = data['home_p_throws']

            def _sp_tag(name, hand):
                h = f" {hand}" if hand else ""
                return f"{name}{h}"
            self._away_title = f"{away}  vs  {_sp_tag(home_p, home_p_throws)}"
            self._home_title = f"{home}  vs  {_sp_tag(away_p, away_p_throws)}"
            self._bat_away = away
            self._bat_home = home
            self._pit_away_name = away
            self._pit_home_name = home
            self._br_away_name = away
            self._br_home_name = home
            self._bvp_away_name = away
            self._bvp_home_name = home
            self._bvp_away_p = away_p
            self._bvp_home_p = home_p

            # Update season combobox if DB returned additional seasons
            seasons = data.get('seasons', [])
            if seasons and hasattr(self, 'season_cb'):
                cur_items = [self.season_cb.itemText(i) for i in range(self.season_cb.count())]
                if cur_items != seasons:
                    self.season_cb.blockSignals(True)
                    self.season_cb.clear()
                    for s in seasons:
                        self.season_cb.addItem(s)
                    self.season_cb.blockSignals(False)

            # Build the currently visible tab (0 = batting by default)
            self._build_tab(self._current_subnav_idx)
        except (RuntimeError, SystemError):
            pass  # C++ object deleted during signal dispatch

    def _scroll_page(self, widget):
        sa = SmoothScrollArea()
        sa.setWidgetResizable(True)
        sa.setStyleSheet(_sa_style())
        sa.setWidget(widget)
        return sa

    def _build_tab(self, idx):
        """Build the widgets for tab `idx` using self._pending_data. Only builds once per tab."""
        try:
            if _is_deleted(self):
                return
            if idx in self._built_tabs or not self._pending_data:
                return
            inner = getattr(self, '_inner_stack', None)
            if not inner or _is_deleted(inner):
                return
        except (RuntimeError, SystemError):
            return
        self._built_tabs.add(idx)
        data = self._pending_data
        inner = self._inner_stack
        away = data['away']
        home = data['home']

        # Remove the loading placeholder at this index and insert the real widget
        old = inner.widget(idx)

        if idx == 0:
            # ── Batting ──
            bat_cols, bat_hi, bat_away, bat_home = data['bat']
            bat_w = QWidget()
            bat_w.setStyleSheet(f"background:{C['bg0']};")
            bv = QVBoxLayout(bat_w)
            bv.setContentsMargins(14, 12, 14, 12)
            bv.setSpacing(8)
            self._team_stack = QStackedWidget()
            self._team_stack.setStyleSheet(f"background:{C['bg0']};")
            self._team_stack.addWidget(table_section(self._away_title, "[ ©SA ]", bat_cols, bat_away, bat_hi))
            self._team_stack.addWidget(table_section(self._home_title, "[ ©SA ]", bat_cols, bat_home, bat_hi))
            self._team_stack.setCurrentIndex(0)
            bv.addWidget(self._team_stack)
            bv.addStretch()
            self._bat_bv = bv
            inner.insertWidget(idx, self._scroll_page(bat_w))

        elif idx == 1:
            # ── Pitching ──
            pit_cols, pit_hi, pit_away, pit_home = data['pit']
            pit_w = QWidget()
            pit_w.setStyleSheet(f"background:{C['bg0']};")
            pv = QVBoxLayout(pit_w)
            pv.setContentsMargins(14, 12, 14, 12)
            pv.setSpacing(10)
            self._pit_stack = QStackedWidget()
            self._pit_stack.setStyleSheet(f"background:{C['bg0']};")
            self._pit_stack.addWidget(table_section(
                f"{away}  PITCHING", "[ ©SA ]", pit_cols, pit_away, pit_hi, 170))
            self._pit_stack.addWidget(table_section(
                f"{home}  PITCHING", "[ ©SA ]", pit_cols, pit_home, pit_hi, 170))
            self._pit_stack.setCurrentIndex(0)
            self._pit_stack.setFixedHeight(PIT_SECTION_H)
            self._pit_cols = pit_cols
            self._pit_hi = pit_hi
            pv.addWidget(self._pit_stack)
            pv.addStretch()
            inner.insertWidget(idx, self._scroll_page(pit_w))

        elif idx == 2:
            # ── Base Running ──
            br_data = data['br']
            br_w = QWidget()
            br_w.setStyleSheet(f"background:{C['bg0']};")
            brl = QVBoxLayout(br_w)
            brl.setContentsMargins(14, 12, 14, 12)
            brl.setSpacing(10)
            self._br_stack = QStackedWidget()
            self._br_stack.setStyleSheet(f"background:{C['bg0']};")
            self._br_tables = []

            _opp = {away: home, home: away}
            for side_idx, (team_name, side_key) in enumerate([(away, 'away'), (home, 'home')]):
                opp_name = _opp[team_name]
                side_w = QWidget()
                side_w.setStyleSheet(f"background:{C['bg0']};")
                svl = QVBoxLayout(side_w)
                svl.setContentsMargins(0, 0, 0, 0)
                svl.setSpacing(10)

                side_tables = []
                if br_data:
                    sec = table_section(
                        f"{team_name}  PITCHER SB", "[ ©SA ]",
                        br_data['pit_cols'], br_data[f'pit_{side_key}'],
                        br_data['pit_hi'], 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{team_name}  CATCHER SB", "[ ©SA ]",
                        br_data['cat_cols'], br_data[f'cat_{side_key}'],
                        br_data['cat_hi'], 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{opp_name}  BASE RUNNING", "[ ©SA ]",
                        br_data['br_cols'], br_data[f'br_{side_key}'],
                        br_data['br_hi'])
                    svl.addWidget(sec); side_tables.append(sec._table)
                else:
                    sec = table_section(
                        f"{team_name}  PITCHER SB", "[ ©SA ]",
                        ["PITCHER", "SB Att", "Pickoffs", "SB Allowed", "SB%"], [], set(), 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{team_name}  CATCHER SB", "[ ©SA ]",
                        ["CATCHER", "SB Att", "CS", "SB Allowed", "SB%"], [], set(), 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{opp_name}  BASE RUNNING", "[ ©SA ]",
                        ["POS", "PLAYER", "OBP", "SB Att", "SB", "Stole 2nd",
                         "Stole 3rd", "Sprint", "Bolts", "Comp Runs", "Bolt%"],
                        [], {2})
                    svl.addWidget(sec); side_tables.append(sec._table)
                svl.addStretch()
                self._br_stack.addWidget(side_w)
                self._br_tables.append(side_tables)

            self._br_stack.setCurrentIndex(0)
            brl.addWidget(self._br_stack)
            brl.addStretch()
            inner.insertWidget(idx, self._scroll_page(br_w))

        elif idx == 3:
            # ── BvP ──
            bvp_data = data['bvp']
            bvp_w = QWidget()
            bvp_w.setStyleSheet(f"background:{C['bg0']};")
            bvl = QVBoxLayout(bvp_w)
            bvl.setContentsMargins(14, 12, 14, 12)
            bvl.setSpacing(10)
            self._bvp_stack = QStackedWidget()
            self._bvp_stack.setStyleSheet(f"background:{C['bg0']};")
            self._bvp_tables = []

            for side_key, (pitcher_team, pitcher_name, opp_team) in enumerate([
                (away, data['away_p'], home), (home, data['home_p'], away)
            ]):
                side_w = QWidget()
                side_w.setStyleSheet(f"background:{C['bg0']};")
                svl = QVBoxLayout(side_w)
                svl.setContentsMargins(0, 0, 0, 0)
                svl.setSpacing(10)
                sk = 'away' if side_key == 0 else 'home'

                side_tables = []
                if bvp_data and bvp_data.get(sk):
                    sd = bvp_data[sk]
                    sec = table_section(
                        f"{pitcher_team} SP  vs  {opp_team} Lineup",
                        "[ SINCE 2021 ]", bvp_data['pit_cols'], sd['pit_rows'],
                        bvp_data['pit_hi'], 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{opp_team} BATTERS  vs  {pitcher_name}  ({pitcher_team} SP)",
                        "[ SINCE 2021 ]", bvp_data['bat_cols'], sd['bat_rows'],
                        bvp_data['bat_hi'])
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sep = QFrame()
                    sep.setFixedHeight(2)
                    sep.setStyleSheet(f"background:{C['bdr']}; margin:4px 0;")
                    svl.addWidget(sep)
                    sec = table_section(
                        f"{pitcher_team} SP  BASERUNNING", "[ SINCE 2021 ]",
                        bvp_data['pit_br_cols'], sd['pit_br_rows'],
                        bvp_data['pit_br_hi'], 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{pitcher_team} C  BASERUNNING", "[ SINCE 2021 ]",
                        bvp_data['cat_br_cols'], sd['cat_br_rows'],
                        bvp_data['cat_br_hi'], 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{opp_team} LINEUP  BASERUNNING  vs  {pitcher_name}",
                        "[ SINCE 2021 ]", bvp_data['br_cols'], sd['runner_br_rows'],
                        bvp_data['br_hi'])
                    svl.addWidget(sec); side_tables.append(sec._table)
                else:
                    sec = table_section(
                        f"{pitcher_team} SP  vs  {opp_team} Lineup",
                        "[ SINCE 2021 ]",
                        ["PITCHER", "IP", "K", "K%", "Velo", "Top", "Whiff%", "BB", "BB%",
                         "H", "1B", "2B", "3B", "HR", "ERA"], [], {3, 6}, 170)
                    svl.addWidget(sec); side_tables.append(sec._table)
                    sec = table_section(
                        f"{opp_team} BATTERS  vs  {pitcher_name}  ({pitcher_team} SP)",
                        "[ SINCE 2021 ]",
                        ["POS", "PLAYER", "PA", "AVG", "ISO", "K%", "BB%",
                         "H", "1B", "2B", "3B", "HR", "R", "RBI", "TB",
                         "Brl%", "Pull%", "EV", "MaxEV", "AVG LA"], [], {3, 4})
                    svl.addWidget(sec); side_tables.append(sec._table)
                svl.addStretch()
                self._bvp_stack.addWidget(side_w)
                self._bvp_tables.append(side_tables)

            self._bvp_stack.setCurrentIndex(0)
            bvl.addWidget(self._bvp_stack)
            bvl.addStretch()
            inner.insertWidget(idx, self._scroll_page(bvp_w))

        # Remove the old placeholder and show the new tab
        if old:
            inner.removeWidget(old)
            old.deleteLater()
        if not _is_deleted(inner):
            inner.setCurrentIndex(self._current_subnav_idx)
        # Sync newly-built stack with current team toggle state
        toggle_idx = 1 if getattr(self, '_home_btn', None) and self._home_btn.isChecked() else 0
        for stack_name in ('_team_stack', '_pit_stack', '_br_stack', '_bvp_stack'):
            stack = getattr(self, stack_name, None)
            if stack and not _is_deleted(stack) and stack.currentIndex() != toggle_idx:
                stack.setCurrentIndex(toggle_idx)

    def _on_filters_changed(self, *args):
        """Unified handler for filter change events — refresh only the active tab."""
        try:
            if _is_deleted(self):
                return
            idx = getattr(self, '_current_subnav_idx', 0)
            if idx == 0:
                self._refresh_lineups()
            elif idx == 1:
                self._refresh_pitching()
            elif idx == 2:
                self._refresh_baserunning()
            elif idx == 3:
                self._refresh_bvp()
        except RuntimeError:
            pass  # C++ object deleted
        except Exception:
            log.exception("_on_filters_changed")

    def _on_subnav_changed(self, idx):
        """Handle sub-navbar page switch — update inner stack and matchup filter."""
        try:
            if _is_deleted(self) or _is_deleted(self._inner_stack):
                return
            prev = self._current_subnav_idx
            self._current_subnav_idx = idx
            # Lazy-build the tab if it hasn't been built yet
            self._build_tab(idx)
            _fade_switch(self._inner_stack, idx)
            # Refresh this tab's data if it was built with stale filter values
            if idx in self._built_tabs and idx != prev and self._pending_data:
                try:
                    if idx == 0:
                        self._refresh_lineups()
                    elif idx == 1:
                        self._refresh_pitching()
                    elif idx == 2:
                        self._refresh_baserunning()
                    elif idx == 3:
                        self._refresh_bvp()
                except Exception:
                    log.exception("_on_subnav_changed")
            # Update matchup label and items per page type
            if idx == 0:  # Batting
                self._season_label.show()
                self.season_cb.show()
                self._matchup_label.setText("Pitcher")
                self._matchup_label.show()
                self.matchup_cb.show()
                self.matchup_cb.blockSignals(True)
                self.matchup_cb.clear()
                self.matchup_cb.addItems(["Both", "RHP", "LHP"])
                self.matchup_cb.blockSignals(False)
            elif idx == 1:  # Pitching
                self._season_label.show()
                self.season_cb.show()
                self._matchup_label.setText("Batter")
                self._matchup_label.show()
                self.matchup_cb.show()
                self.matchup_cb.blockSignals(True)
                self.matchup_cb.clear()
                self.matchup_cb.addItems(["Both", "RHB", "LHB"])
                self.matchup_cb.blockSignals(False)
            elif idx == 2:  # Base Running
                self._season_label.show()
                self.season_cb.show()
                self._matchup_label.setText("Pitcher")
                self._matchup_label.show()
                self.matchup_cb.show()
                self.matchup_cb.blockSignals(True)
                self.matchup_cb.clear()
                self.matchup_cb.addItems(["Both", "RHP", "LHP"])
                self.matchup_cb.blockSignals(False)
            elif idx == 3:  # BvP
                self._matchup_label.hide()
                self.matchup_cb.hide()
                self._season_label.hide()
                self.season_cb.hide()
                # Show time filter for BvP (pitcher's last N games)
                self.window_cb.show()
        except RuntimeError:
            pass  # C++ object deleted

    def _set_team_toggle(self, which):
        """Switch between away and home team tables on all pages."""
        try:
            if _is_deleted(self):
                return
            idx = 0 if which == 'away' else 1
            self._away_btn.setChecked(which == 'away')
            self._home_btn.setChecked(which == 'home')
            subnav = getattr(self, '_current_subnav_idx', 0)
            _stack_map = {0: '_team_stack', 1: '_pit_stack', 2: '_br_stack', 3: '_bvp_stack'}
            visible_name = _stack_map.get(subnav)
            for stack_name in ('_team_stack', '_pit_stack', '_br_stack', '_bvp_stack'):
                stack = getattr(self, stack_name, None)
                if stack and not _is_deleted(stack):
                    if stack_name == visible_name:
                        _fade_switch(stack, idx)
                        QTimer.singleShot(320, lambda s=stack: (
                            s.setFixedHeight(s.currentWidget().sizeHint().height())
                            if not _is_deleted(s) and s.currentWidget() else None,
                            s.updateGeometry() if not _is_deleted(s) else None))
                    else:
                        stack.setCurrentIndex(idx)
            self._update_team_toggle_style()
        except (RuntimeError, SystemError):
            pass  # C++ object deleted

    def _update_team_toggle_style(self):
        """Apply active/inactive styling to the team toggle buttons."""
        for btn in (self._away_btn, self._home_btn):
            active = btn.isChecked()
            bg = C["bg3"] if active else "transparent"
            col = C["t1"] if active else C["t3"]
            btn.setStyleSheet(
                f"QPushButton {{ background:{bg}; color:{col}; "
                f"border:1px solid {C['bdrl']}; border-radius:3px; "
                f"padding:4px 10px; font-family:'Segoe UI'; "
                f"font-size:11px; letter-spacing:1px; }}"
                f"QPushButton:hover {{ background:{C['bg3']}; color:{C['t1']}; }}")

    def _refresh_lineups(self):
        """Refresh the batting lineup tables using current filter selections."""
        try:
            if _is_deleted(self):
                return
            game = getattr(self, '_current_game', None)
            if not game:
                return
            gid = game.get('game_id')
            try:
                sel_season = int(self.season_cb.currentText())
            except Exception:
                sel_season = None
            mmap = {'Both': 'all', 'RHP': 'vs_righty', 'LHP': 'vs_lefty'}
            wmap = {'All Games': 'season', 'Last 5 Games': 'last5', 'Last 10 Games': 'last10', 'Last 15 Games': 'last15', 'Last 30 Games': 'last30'}
            sel_matchup = mmap.get(self.matchup_cb.currentText(), 'all')
            sel_window = wmap.get(self.window_cb.currentText(), 'season')

            try:
                res = _DM.get_game_lineup(gid, season=sel_season, matchup=sel_matchup, window=sel_window)
                if not res:
                    return
                new_cols, new_hi, new_away, new_home = res
            except Exception:
                return

            stack = getattr(self, '_team_stack', None)
            if not stack or _is_deleted(stack):
                return
            cur_idx = stack.currentIndex()
            # Recycle existing table widgets if columns match
            away_w = stack.widget(0) if stack.count() > 0 else None
            home_w = stack.widget(1) if stack.count() > 1 else None
            if (away_w and home_w and
                    hasattr(away_w, '_table') and hasattr(home_w, '_table') and
                    away_w._table._cols == new_cols):
                away_w._table.set_data(new_away)
                home_w._table.set_data(new_home)
            else:
                while stack.count():
                    w = stack.widget(0)
                    stack.removeWidget(w)
                    w.deleteLater()
                away_sec = table_section(self._away_title, "[ ©SA ]", new_cols, new_away, new_hi)
                home_sec = table_section(self._home_title, "[ ©SA ]", new_cols, new_home, new_hi)
                stack.addWidget(away_sec)
                stack.addWidget(home_sec)
            stack.setCurrentIndex(cur_idx)
            cur_w = stack.currentWidget()
            if cur_w:
                stack.setFixedHeight(cur_w.sizeHint().height())
            stack.updateGeometry()
        except Exception:
            log.exception("_refresh_lineups")

    def _refresh_pitching(self):
        """Refresh the pitching tables using current filter selections."""
        try:
            if _is_deleted(self):
                return
            game = getattr(self, '_current_game', None)
            if not game:
                return
            gid = game.get('game_id')
            try:
                sel_season = int(self.season_cb.currentText())
            except Exception:
                sel_season = None
            # Pitching tab uses batter handedness for matchup (RHB/LHB)
            mmap = {'Both': 'all', 'RHB': 'vs_righty', 'LHB': 'vs_lefty',
                    'RHP': 'vs_righty', 'LHP': 'vs_lefty'}
            wmap = {'All Games': 'season', 'Last 5 Games': 'last5', 'Last 10 Games': 'last10',
                    'Last 15 Games': 'last15', 'Last 30 Games': 'last30'}
            sel_matchup = mmap.get(self.matchup_cb.currentText(), 'all')
            sel_window = wmap.get(self.window_cb.currentText(), 'season')

            try:
                away_starter = {'id': game.get('away_p_id'), 'name': game.get('away_p', 'TBD'),
                                'throws': game.get('away_p_throws', '')}
                home_starter = {'id': game.get('home_p_id'), 'name': game.get('home_p', 'TBD'),
                                'throws': game.get('home_p_throws', '')}
                res = _DM.get_game_pitching(gid, season=sel_season, matchup=sel_matchup, window=sel_window,
                                            away_starter=away_starter, home_starter=home_starter)
                if not res:
                    return
                new_cols, new_hi, new_away, new_home = res
            except Exception:
                return

            stack = getattr(self, '_pit_stack', None)
            if not stack or _is_deleted(stack):
                return
            cur_idx = stack.currentIndex()
            away_w = stack.widget(0) if stack.count() > 0 else None
            home_w = stack.widget(1) if stack.count() > 1 else None
            if (away_w and home_w and
                    hasattr(away_w, '_table') and hasattr(home_w, '_table') and
                    away_w._table._cols == new_cols):
                away_w._table.set_data(new_away)
                home_w._table.set_data(new_home)
            else:
                while stack.count():
                    w = stack.widget(0)
                    stack.removeWidget(w)
                    w.deleteLater()
                away_name = getattr(self, '_pit_away_name', 'AWAY')
                home_name = getattr(self, '_pit_home_name', 'HOME')
                stack.addWidget(table_section(
                    f"{away_name}  PITCHING", "[ ©SA ]", new_cols, new_away, new_hi, 170))
                stack.addWidget(table_section(
                    f"{home_name}  PITCHING", "[ ©SA ]", new_cols, new_home, new_hi, 170))
            stack.setCurrentIndex(cur_idx)
            stack.setFixedHeight(PIT_SECTION_H)
            stack.updateGeometry()
        except Exception:
            log.exception("_refresh_pitching")

    def _refresh_baserunning(self):
        """Refresh the base running tables using current filter selections."""
        try:
            if _is_deleted(self):
                return
            game = getattr(self, '_current_game', None)
            if not game:
                return
            gid = game.get('game_id')
            try:
                sel_season = int(self.season_cb.currentText())
            except Exception:
                sel_season = None
            mmap = {'Both': 'all', 'RHP': 'vs_righty', 'LHP': 'vs_lefty',
                    'RHB': 'vs_righty', 'LHB': 'vs_lefty'}
            wmap = {'All Games': 'season', 'Last 5 Games': 'last5', 'Last 10 Games': 'last10',
                    'Last 15 Games': 'last15', 'Last 30 Games': 'last30'}
            sel_matchup = mmap.get(self.matchup_cb.currentText(), 'all')
            sel_window = wmap.get(self.window_cb.currentText(), 'season')

            away_starter = {'id': game.get('away_p_id'), 'name': game.get('away_p', 'TBD'),
                            'throws': game.get('away_p_throws', '')}
            home_starter = {'id': game.get('home_p_id'), 'name': game.get('home_p', 'TBD'),
                            'throws': game.get('home_p_throws', '')}
            br_data = _DM.get_game_baserunning(gid, season=sel_season, matchup=sel_matchup,
                                                window=sel_window, away_starter=away_starter,
                                                home_starter=home_starter)
            if not br_data:
                return

            stack = getattr(self, '_br_stack', None)
            if not stack or _is_deleted(stack):
                return
            cur_idx = stack.currentIndex()

            away_name = getattr(self, '_br_away_name', 'AWAY')
            home_name = getattr(self, '_br_home_name', 'HOME')

            # Try to recycle existing table widgets
            existing_tables = getattr(self, '_br_tables', None)
            if existing_tables and len(existing_tables) == 2:
                for side_idx, side_key in enumerate(['away', 'home']):
                    tables = existing_tables[side_idx]
                    if len(tables) >= 3:
                        tables[0].set_data(br_data.get(f'pit_{side_key}', []))
                        tables[1].set_data(br_data.get(f'cat_{side_key}', []))
                        tables[2].set_data(br_data.get(f'br_{side_key}', []))
                cur_w = stack.currentWidget()
                if cur_w:
                    stack.setFixedHeight(cur_w.sizeHint().height())
                stack.updateGeometry()
                return

            # Full rebuild if no existing tables to recycle
            while stack.count():
                w = stack.widget(0)
                stack.removeWidget(w)
                w.deleteLater()
            self._br_tables = []

            _opp = {away_name: home_name, home_name: away_name}
            for side_idx, (team_name, side_key) in enumerate([(away_name, 'away'), (home_name, 'home')]):
                opp_name = _opp[team_name]
                side_w = QWidget()
                side_w.setStyleSheet(f"background:{C['bg0']};")
                svl = QVBoxLayout(side_w)
                svl.setContentsMargins(0, 0, 0, 0)
                svl.setSpacing(10)

                side_tables = []
                sec = table_section(
                    f"{team_name}  PITCHER SB", "[ ©SA ]",
                    br_data['pit_cols'], br_data[f'pit_{side_key}'],
                    br_data['pit_hi'], 170)
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sec = table_section(
                    f"{team_name}  CATCHER SB", "[ ©SA ]",
                    br_data['cat_cols'], br_data[f'cat_{side_key}'],
                    br_data['cat_hi'], 170)
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sec = table_section(
                    f"{opp_name}  BASE RUNNING", "[ ©SA ]",
                    br_data['br_cols'], br_data[f'br_{side_key}'],
                    br_data['br_hi'])
                svl.addWidget(sec)
                side_tables.append(sec._table)

                svl.addStretch()
                stack.addWidget(side_w)
                self._br_tables.append(side_tables)

            stack.setCurrentIndex(cur_idx)
            cur_w = stack.currentWidget()
            if cur_w:
                stack.setFixedHeight(cur_w.sizeHint().height())
            stack.updateGeometry()
        except Exception:
            log.exception("_refresh_baserunning")

    def _refresh_bvp(self):
        """Refresh the BvP tables using current time window selection."""
        if _is_deleted(self):
            return
        try:
            game = getattr(self, '_current_game', None)
            if not game:
                return
            gid = game.get('game_id')
            wmap = {'All Games': 'all', 'Last 5 Games': 'last5', 'Last 10 Games': 'last10',
                    'Last 15 Games': 'last15', 'Last 30 Games': 'last30'}
            sel_window = wmap.get(self.window_cb.currentText(), 'all')

            away_p = game.get('away_p', 'TBD')
            home_p = game.get('home_p', 'TBD')
            away_p_throws = game.get('away_p_throws', '')
            home_p_throws = game.get('home_p_throws', '')
            away = getattr(self, '_bvp_away_name', 'AWAY')
            home = getattr(self, '_bvp_home_name', 'HOME')

            bvp_data = _DM.get_bvp_data(gid, window=sel_window,
                away_starter={'id': game.get('away_p_id'), 'name': away_p, 'throws': away_p_throws},
                home_starter={'id': game.get('home_p_id'), 'name': home_p, 'throws': home_p_throws})
            if not bvp_data:
                return

            stack = getattr(self, '_bvp_stack', None)
            if not stack or _is_deleted(stack):
                return
            cur_idx = stack.currentIndex()

            # Try to recycle existing table widgets
            existing_tables = getattr(self, '_bvp_tables', None)
            data_keys = [
                ('pit_rows', 'pit_cols'), ('bat_rows', 'bat_cols'),
                ('pit_br_rows', 'pit_br_cols'), ('cat_br_rows', 'cat_br_cols'),
                ('runner_br_rows', 'br_cols')]

            if existing_tables and len(existing_tables) == 2:
                for side_idx, sk in enumerate(['away', 'home']):
                    sd = bvp_data.get(sk, {})
                    tables = existing_tables[side_idx]
                    for t_idx, (row_key, col_key) in enumerate(data_keys):
                        if t_idx < len(tables) and tables[t_idx] is not None:
                            tables[t_idx].set_data(sd.get(row_key, []))
                cur_w = stack.currentWidget()
                if cur_w:
                    stack.setFixedHeight(cur_w.sizeHint().height())
                stack.updateGeometry()
                return

            # Full rebuild if no existing tables to recycle
            while stack.count():
                w = stack.widget(0)
                stack.removeWidget(w)
                w.deleteLater()
            self._bvp_tables = []

            for side_key, (pitcher_team, pitcher_name, opp_team) in enumerate([
                (away, away_p, home), (home, home_p, away)
            ]):
                side_w = QWidget()
                side_w.setStyleSheet(f"background:{C['bg0']};")
                svl = QVBoxLayout(side_w)
                svl.setContentsMargins(0, 0, 0, 0)
                svl.setSpacing(10)

                sk = 'away' if side_key == 0 else 'home'
                sd = bvp_data.get(sk, {})

                side_tables = []
                sec = table_section(
                    f"{pitcher_team} SP  vs  {opp_team} Lineup",
                    "[ SINCE 2021 ]", bvp_data['pit_cols'], sd.get('pit_rows', []),
                    bvp_data['pit_hi'], 170)
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sec = table_section(
                    f"{opp_team} BATTERS  vs  {pitcher_name}  ({pitcher_team} SP)",
                    "[ SINCE 2021 ]", bvp_data['bat_cols'], sd.get('bat_rows', []),
                    bvp_data['bat_hi'])
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sep = QFrame()
                sep.setFixedHeight(2)
                sep.setStyleSheet(f"background:{C['bdr']}; margin:4px 0;")
                svl.addWidget(sep)

                sec = table_section(
                    f"{pitcher_team} SP  BASERUNNING", "[ SINCE 2021 ]",
                    bvp_data['pit_br_cols'], sd.get('pit_br_rows', []),
                    bvp_data['pit_br_hi'], 170)
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sec = table_section(
                    f"{pitcher_team} C  BASERUNNING", "[ SINCE 2021 ]",
                    bvp_data['cat_br_cols'], sd.get('cat_br_rows', []),
                    bvp_data['cat_br_hi'], 170)
                svl.addWidget(sec)
                side_tables.append(sec._table)

                sec = table_section(
                    f"{opp_team} LINEUP  BASERUNNING  vs  {pitcher_name}",
                    "[ SINCE 2021 ]", bvp_data['br_cols'], sd.get('runner_br_rows', []),
                    bvp_data['br_hi'])
                svl.addWidget(sec)
                side_tables.append(sec._table)

                svl.addStretch()
                stack.addWidget(side_w)
                self._bvp_tables.append(side_tables)

            stack.setCurrentIndex(cur_idx)
            cur_w = stack.currentWidget()
            if cur_w:
                stack.setFixedHeight(cur_w.sizeHint().height())
            stack.updateGeometry()
        except Exception:
            log.exception("module_level")

    def on_lineup_cached(self, game_id: str):
        """Slot: called when a lineup cache for `game_id` is available."""
        try:
            if _is_deleted(self):
                return
            cur = getattr(self, '_current_game', None)
            if not cur:
                return
            # Guard against deleted Qt widgets (deleteLater timing)
            stack = getattr(self, '_team_stack', None)
            if stack is None or _is_deleted(stack):
                return
            cur_gid = str(cur.get('game_id') or cur.get('id') or '')
            if not cur_gid:
                return
            if str(game_id) == cur_gid:
                # Refresh just the batting tables in-place (avoids full
                # load_game rebuild which can cause sizing glitches due to
                # deleteLater timing on the old widgets).
                self._refresh_lineups()
        except RuntimeError:
            pass  # C++ object deleted
        except Exception:
            log.exception("on_lineup_cached")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Main pages
# ═══════════════════════════════════════════════════════════════════════════════
def build_home_page():
    page = QWidget()
    page.setStyleSheet(f"background:{C['bg0']};")
    sa = SmoothScrollArea()
    sa.setWidgetResizable(True)
    sa.setStyleSheet(_sa_style())
 
    c = QWidget()
    c.setStyleSheet(f"background:{C['bg0']};")
    vl = QVBoxLayout(c)
    vl.setContentsMargins(20, 20, 20, 20)
    vl.setSpacing(0)
 
    vl.addWidget(mk_label("Dashboard", color=C["t1"], size=22, bold=True))
    s = mk_label("League activity · 2026", color=C["t2"], size=13)
    s.setContentsMargins(0, 3, 0, 18)
    vl.addWidget(s)
 
    # Live score bar
    top = QHBoxLayout()
    top.setSpacing(10)
    cnt = QFrame()
    cnt.setStyleSheet(f"background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px;")
    cl = QVBoxLayout(cnt)
    cl.setContentsMargins(12,10,12,10)
    cl.setSpacing(2)
    cl.addWidget(mk_label("TODAY'S GAMES", color=C["t3"], size=10, mono=True,
                           align=Qt.AlignmentFlag.AlignCenter))
    _games_count_lbl = mk_label(str(len(GAMES)), color=C["t1"], size=22, bold=True, mono=True,
                                align=Qt.AlignmentFlag.AlignCenter)
    cl.addWidget(_games_count_lbl)
    page._games_count_lbl = _games_count_lbl
    top.addWidget(cnt)
    vl.addLayout(top)
    vl.addSpacing(16)
 
    # Fetch real dashboard data
    dash = _DM.get_dashboard_stats()
    stat_cards = dash["stat_cards"] if dash else STAT_CARDS
    ba_leaders = dash["ba_leaders"] if dash else BA_LEADERS
    hr_leaders = dash["hr_leaders"] if dash else HR_LEADERS

    # Stat cards
    sc_row = QHBoxLayout()
    sc_row.setSpacing(10)
    for lbl_t, val, delta, up in stat_cards:
        card = QFrame()
        card.setStyleSheet(f"background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px;")
        cv = QVBoxLayout(card)
        cv.setContentsMargins(12,12,12,12)
        cv.setSpacing(3)
        cv.addWidget(mk_label(lbl_t, color=C["t3"], size=10, mono=True))
        cv.addWidget(mk_label(str(val), color=C["t1"], size=22, bold=True, mono=True))
        cv.addWidget(mk_label(str(delta), color=C["grn"] if up else C["red"], size=11, mono=True))
        sc_row.addWidget(card)
    vl.addLayout(sc_row)
    vl.addSpacing(16)
 
    # Leaders
    lr = QHBoxLayout()
    lr.setSpacing(10)
 
    def leader_card(title, leaders):
        card = QFrame()
        card.setStyleSheet(f"background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px;")
        cv = QVBoxLayout(card)
        cv.setContentsMargins(0,0,0,0)
        cv.setSpacing(0)
        hdr = QWidget()
        hdr.setStyleSheet(f"background:{C['bg1']}; border-bottom:1px solid {C['bdr']};")
        hl = QHBoxLayout(hdr)
        hl.setContentsMargins(12,8,12,8)
        hl.addWidget(mk_label(title, color=C["t3"], size=11, mono=True))
        hl.addStretch()
        hl.addWidget(mk_label("TOP 5", color=C["t3"], size=11, mono=True))
        cv.addWidget(hdr)
        for rank, name, team, val in leaders:
            row = QWidget()
            row.setStyleSheet(f"border-bottom:1px solid {C['bdr']};")
            rl = QHBoxLayout(row)
            rl.setContentsMargins(12,6,12,6)
            rl.setSpacing(8)
            rl.addWidget(mk_label(str(rank), color=C["t3"], size=11, mono=True))
            rl.addWidget(mk_label(str(name), color=C["t1"], size=13, bold=True), 1)
            rl.addWidget(mk_label(str(team), color=C["t3"], size=11, mono=True))
            rl.addWidget(mk_label(str(val),  color=C["t1"], size=13, bold=True, mono=True))
            cv.addWidget(row)
        if not leaders:
            cv.addWidget(mk_label("Loading…", color=C["t3"], size=11, mono=True,
                                  align=Qt.AlignmentFlag.AlignCenter))
        return card
 
    lr.addWidget(leader_card("BATTING AVG", ba_leaders))
    lr.addWidget(leader_card("HOME RUNS", hr_leaders))
    vl.addLayout(lr)
    vl.addStretch()
 
    sa.setWidget(c)
    ol = QVBoxLayout(page)
    ol.setContentsMargins(0,0,0,0)
    ol.addWidget(sa)
    return page


# ═══════════════════════════════════════════════════════════════════════════════
# Leaderboard Card Widget
# ═══════════════════════════════════════════════════════════════════════════════

class LeaderboardCard(QFrame):
    """Compact top-10 leaderboard widget (~260×210) styled like other cards."""

    _ROW_H = 26

    def __init__(self, title, rows, fmt=None, unit=None, parent=None):
        """
        Parameters
        ----------
        title : str   – e.g. "HITS (H)"
        rows  : list  – [(player_name, team_abbrev, value), …]  up to 10
        fmt   : callable or None – format value for display (default: str)
        unit  : str or None – muted text shown in header right side (e.g. "GAMES")
        """
        super().__init__(parent)
        self.setStyleSheet(
            f"QFrame {{ background:{C['bg1']}; border:1px solid {C['bdr']}; border-radius:6px; }}")
        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(0)

        # ── header ──
        hdr = QWidget()
        hdr.setFixedHeight(34)
        hdr.setStyleSheet(
            f"background:{C['bg1']}; border-bottom:1px solid {C['bdr']};")
        hl = QHBoxLayout(hdr)
        hl.setContentsMargins(10, 0, 10, 0)
        hl.addWidget(mk_label(title, color=C["t1"], size=11, bold=True))
        if unit:
            hl.addWidget(mk_label(unit, color=C["t3"], size=9))
        hl.addStretch()
        hl.addWidget(mk_label("TODAY", color=C["t3"], size=9))
        vl.addWidget(hdr)

        # ── scrollable list ──
        sa = SmoothScrollArea()
        sa.setWidgetResizable(True)
        sa.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        sa.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        sa.setStyleSheet(
            f"QScrollArea {{ background:{C['bg1']}; border:none; }}"
            f"QScrollBar:vertical {{ width:4px; background:{C['bg1']}; }}"
            f"QScrollBar::handle:vertical {{ background:{C['bdrl']}; border-radius:2px; }}"
            f"QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}")

        inner = QWidget()
        inner.setStyleSheet(f"background:{C['bg1']};")
        il = QVBoxLayout(inner)
        il.setContentsMargins(0, 0, 0, 0)
        il.setSpacing(0)

        format_val = fmt or (lambda v: f"{v:.3f}" if isinstance(v, float) and v < 1.5 else str(round(v, 1)) if isinstance(v, float) else str(v))

        for rank, (name, team, val) in enumerate(rows[:10], 1):
            row_w = QWidget()
            row_w.setFixedHeight(self._ROW_H)
            row_w.setStyleSheet(f"border-bottom:1px solid {C['bdr']};")
            rl = QHBoxLayout(row_w)
            rl.setContentsMargins(10, 0, 10, 0)
            rl.setSpacing(6)
            rl.addWidget(mk_label(str(rank), color=C["t3"], size=10, mono=True))
            name_lbl = mk_label(name, color=C["t1"], size=11, bold=(rank <= 3))
            name_lbl.setMinimumWidth(0)
            rl.addWidget(name_lbl, 1)
            rl.addWidget(mk_label(team, color=C["t3"], size=10, mono=True))
            rl.addWidget(mk_label(format_val(val), color=C["t1"], size=11, bold=True, mono=True))
            il.addWidget(row_w)

        if not rows:
            il.addWidget(mk_label("No data", color=C["t3"], size=11, mono=True,
                                  align=Qt.AlignmentFlag.AlignCenter))
        il.addStretch()
        sa.setWidget(inner)
        # show up to 5 rows; shrink to fit when fewer
        n = len(rows) if rows else 1
        visible = min(n, 5)
        sa.setFixedHeight(self._ROW_H * visible + 2)
        vl.addWidget(sa)
        self._visible_rows = visible

    def sizeHint(self):
        from PyQt6.QtCore import QSize
        return QSize(260, 34 + self._ROW_H * self._visible_rows + 4)


def build_top_stats_page(title, subtitle, leaderboards=None):
    """Build a top-stats page with leaderboard cards.

    leaderboards : list of (title, rows, fmt[, unit]) tuples for LeaderboardCard widgets
    """
    page = QWidget()
    page.setStyleSheet(f"background:{C['bg0']};")
    sa = SmoothScrollArea()
    sa.setWidgetResizable(True)
    sa.setStyleSheet(_sa_style())

    content = QWidget()
    content.setStyleSheet(f"background:{C['bg0']};")
    vl = QVBoxLayout(content)
    vl.setContentsMargins(20, 20, 20, 20)
    vl.setSpacing(0)
    vl.addWidget(mk_label(title, color=C["t1"], size=22, bold=True))
    sub = mk_label(subtitle, color=C["t2"], size=13)
    sub.setContentsMargins(0, 3, 0, 14)
    vl.addWidget(sub)

    # ── leaderboard cards (flow grid) ──
    if leaderboards:
        lb_label = mk_label("TODAY'S LEADERS", color=C["t3"], size=10, mono=True)
        lb_label.setContentsMargins(0, 0, 0, 6)
        vl.addWidget(lb_label)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(12)
        cols_per_row = 3 if len(leaderboards) <= 3 else 3
        for i, lb_entry in enumerate(leaderboards):
            lb_title, lb_rows, lb_fmt = lb_entry[0], lb_entry[1], lb_entry[2]
            lb_unit = lb_entry[3] if len(lb_entry) > 3 else None
            card = LeaderboardCard(lb_title, lb_rows, lb_fmt, unit=lb_unit)
            grid.addWidget(card, i // cols_per_row, i % cols_per_row,
                           Qt.AlignmentFlag.AlignTop)
        # stretch trailing columns so cards don't over-expand
        for c in range(cols_per_row):
            grid.setColumnStretch(c, 1)
        vl.addLayout(grid)
    else:
        vl.addStretch()
        vl.addWidget(mk_label("No leaderboard data available", color=C["t3"], size=13, mono=True,
                              align=Qt.AlignmentFlag.AlignCenter))

    vl.addStretch()
    sa.setWidget(content)
    ol = QVBoxLayout(page)
    ol.setContentsMargins(0, 0, 0, 0)
    ol.addWidget(sa)
    return page
 
 
def build_matchup_page(games=None, odds=None, on_click=None, date=None):
    page = QWidget()
    page.setStyleSheet(f"background:{C['bg0']};")
    outer = QVBoxLayout(page)
    outer.setContentsMargins(0, 0, 0, 0)
    outer.setSpacing(0)

    # Header area (non-scrolling)
    hdr = QWidget()
    hdr.setStyleSheet(f"background:{C['bg0']};")
    hvl = QVBoxLayout(hdr)
    hvl.setContentsMargins(24, 20, 24, 12)
    hvl.setSpacing(0)
    hvl.addWidget(mk_label("Game Tracker", color=C["t1"], size=22, bold=True))
    try:
        if date:
            friendly = date.strftime("%A, %B %d")
        else:
            friendly = dt.datetime.now().strftime("%A, %B %d")
    except Exception:
        friendly = ""
    today = dt.date.today()
    if date and date == today:
        prefix = "Today's matchups"
    elif date and date == today - dt.timedelta(days=1):
        prefix = "Yesterday's matchups"
    elif date and date == today + dt.timedelta(days=1):
        prefix = "Tomorrow's matchups"
    else:
        prefix = "Matchups"
    sub_text = f"{prefix} · {friendly}" if friendly else prefix
    sub = mk_label(sub_text, color=C["t2"], size=13)
    sub.setContentsMargins(0, 3, 0, 0)
    hvl.addWidget(sub)

    # DraftKings attribution
    dk_lbl = mk_label("Lines via DraftKings · ESPN", color=C["t3"], size=10, mono=True)
    dk_lbl.setContentsMargins(0, 4, 0, 0)
    hvl.addWidget(dk_lbl)
    outer.addWidget(hdr)

    # Scrollable card grid
    sa = SmoothScrollArea()
    sa.setWidgetResizable(True)
    sa.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    sa.setStyleSheet(_sa_style())
    content = QWidget()
    content.setStyleSheet(f"background:{C['bg0']};")

    if not games:
        vl = QVBoxLayout(content)
        vl.setContentsMargins(24, 40, 24, 20)
        vl.addWidget(mk_label("No games scheduled", color=C["t3"], size=15, mono=True,
                               align=Qt.AlignmentFlag.AlignCenter))
        vl.addStretch()
        page._schedule_cards = []
    else:
        grid = QGridLayout(content)
        grid.setContentsMargins(24, 8, 24, 20)
        grid.setSpacing(12)
        cols = 3
        odds_map = odds or {}
        schedule_cards = []
        games = sorted(games, key=_game_sort_key)
        for i, game in enumerate(games):
            key = (game.get("away", ""), game.get("home", ""))
            game_odds = odds_map.get(key)
            card = ScheduleGameCard(game, i, game_odds, on_click=on_click)
            schedule_cards.append(card)
            grid.addWidget(card, i // cols, i % cols, Qt.AlignmentFlag.AlignTop)
        # fill remaining cells with stretch
        rem = len(games) % cols
        if rem:
            for c in range(rem, cols):
                grid.addWidget(QWidget(), len(games) // cols, c)
        grid.setRowStretch(len(games) // cols + 1, 1)
        # Equalize all collapsed cards to the tallest one
        if ScheduleGameCard._collapsed_height:
            for card in schedule_cards:
                if not card._expanded:
                    card.setFixedHeight(ScheduleGameCard._collapsed_height)
        page._schedule_cards = schedule_cards

    sa.setWidget(content)
    outer.addWidget(sa, 1)

    # Inject any already-cached plays into newly created cards
    from types import SimpleNamespace as _NS
    _app = QApplication.instance()
    if _app:
        for w in _app.topLevelWidgets():
            cache = getattr(w, '_plays_cache', None)
            if cache:
                for card in getattr(page, '_schedule_cards', []):
                    gid = str(card.game.get('game_id') or card.game.get('id'))
                    if gid in cache:
                        card.update_plays(cache[gid])
                break

    return page
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# Shared live-game detection
# ═══════════════════════════════════════════════════════════════════════════════
def _is_game_live(g):
    """Return True if game dict represents a live/in-progress game."""
    if g.get('live'):
        return True
    ast = (g.get('abstract_state') or '').lower()
    if ast == 'live':
        return True
    st = (g.get('status') or '').lower()
    return (st.startswith('in progress')
            or st.startswith('manager challenge')
            or st.startswith('umpire review'))


# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar sort helper
# ═══════════════════════════════════════════════════════════════════════════════
def _game_sort_key(g):
    """Sort: live first (0), scheduled next (1), final/postponed last (2)."""
    t = (g.get('time') or '').upper()
    st = (g.get('status') or '').lower()
    is_final = (t == 'FINAL' or st.startswith('final')
                or st.startswith('game over')
                or st.startswith('completed'))
    is_ppd = t == 'PPD' or st.startswith('postponed')
    is_live = _is_game_live(g)
    if is_live:
        return (0, t)
    if not is_final and not is_ppd:
        return (1, t)
    return (2, t)


# ═══════════════════════════════════════════════════════════════════════════════
# Main Window
# ═══════════════════════════════════════════════════════════════════════════════
class SeamStatsApp(QMainWindow):
    IDX_HOME    = 0
    IDX_HIT     = 1
    IDX_PITCH   = 2
    IDX_BR      = 3
    IDX_MATCHUP = 4
    IDX_LINEUP  = 5
    IDX_PARK    = 6
    IDX_GAME    = 7
 
    _qsettings = QSettings("SeamAnalytics", "SeamStats")

    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Seam Analytics — MLB Stats  v{_app_paths.APP_VERSION}")
        self.resize(1440, 860)
        self.setMinimumSize(960, 600)
        self._restore_window_geometry()
        if os.path.exists(_app_paths.LOGO_PNG):
            self.setWindowIcon(QIcon(_app_paths.LOGO_PNG))
        self.setStyleSheet(f"QMainWindow {{ background:{C['bg0']}; }}")
        self._cards = []
        self._sel_card = None
        self._main_btns = []
 
        central = QWidget()
        central.setStyleSheet(f"background:{C['bg0']};")
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0,0,0,0)
        root.setSpacing(0)
 
        root.addWidget(self._titlebar())
 
        main_nav, self._main_btns = build_navbar(
            50,
            [("[ 01 ]","HOME"),("[ 02 ]","HITTING"),
             ("[ 03 ]","PITCHING"),("[ 04 ]","BASE RUNNING"),("[ 05 ]","GAME TRACKER"),
             ("[ 06 ]","LINEUPS"),("[ 07 ]","PARK FACTORS")],
            self._on_main_nav)

        # ── Date selector (right side of navbar) ──
        self._selected_date = dt.date.today()
        self._date_lbl = mk_label(
            self._selected_date.strftime("%a, %b %d"),
            color=C["t1"], size=12, bold=True, mono=True)
        self._date_lbl.setContentsMargins(6, 0, 6, 0)

        def _svg_icon(svg_path, color):
            from PyQt6.QtCore import QByteArray
            try:
                with open(svg_path, "r", encoding="utf-8") as f:
                    svg = f.read()
                svg = svg.replace('stroke="#000000"', f'stroke="{color}"')
                pm = QPixmap(20, 20)
                pm.fill(QColor(0, 0, 0, 0))
                rn = QSvgRenderer(QByteArray(svg.encode()))
                p = QPainter(pm)
                rn.render(p)
                p.end()
                return QIcon(pm)
            except Exception:
                return QIcon()

        _arrow_l_svg = os.path.join(_app_paths.APP_DIR, "assets", "arrow-circle-left.svg")
        _arrow_r_svg = os.path.join(_app_paths.APP_DIR, "assets", "arrow-circle-right.svg")
        self._arrow_icons = {
            'left':  _svg_icon(_arrow_l_svg, C["t2"]),
            'left_dim':  _svg_icon(_arrow_l_svg, C["t3"]),
            'right': _svg_icon(_arrow_r_svg, C["t2"]),
            'right_dim': _svg_icon(_arrow_r_svg, C["t3"]),
        }

        _arrow_ss = f"""QPushButton {{
            background:transparent; border:none; padding:2px;
        }}
        QPushButton:hover {{ background:{C['bg2']}; border-radius:4px; }}"""

        self._date_left = QPushButton()
        self._date_left.setFixedSize(28, 28)
        self._date_left.setIconSize(QSize(20, 20))
        self._date_left.setIcon(self._arrow_icons['left'])
        self._date_left.setCursor(Qt.CursorShape.PointingHandCursor)
        self._date_left.setStyleSheet(_arrow_ss)
        self._date_left.clicked.connect(lambda: self._shift_date(-1))

        self._date_right = QPushButton()
        self._date_right.setFixedSize(28, 28)
        self._date_right.setIconSize(QSize(20, 20))
        self._date_right.setIcon(self._arrow_icons['right'])
        self._date_right.setCursor(Qt.CursorShape.PointingHandCursor)
        self._date_right.setStyleSheet(_arrow_ss)
        self._date_right.clicked.connect(lambda: self._shift_date(+1))

        main_nav._layout.addWidget(self._date_left)
        main_nav._layout.addWidget(self._date_lbl)
        main_nav._layout.addWidget(self._date_right)
        main_nav._layout.addSpacing(8)
        self._update_date_arrows()

        root.addWidget(main_nav)
 
        body = QHBoxLayout()
        body.setContentsMargins(0,0,0,0)
        body.setSpacing(0)

        # ── Status bar (create early so _main_stack / _sidebar can call set_status) ──
        self._status_bar = QFrame()
        self._status_bar.setFixedHeight(22)
        self._status_bar.setStyleSheet(
            f"QFrame {{ background:{C['bg1']}; border-top:1px solid {C['bdr']}; }}")
        sb_hl = QHBoxLayout(self._status_bar)
        sb_hl.setContentsMargins(12, 0, 12, 0)
        sb_hl.setSpacing(8)
        self._status_lbl = mk_label("Ready", color=C["t3"], size=10, mono=True)
        sb_hl.addWidget(self._status_lbl)
        sb_hl.addStretch()
        self._status_right = mk_label("", color=C["t3"], size=10, mono=True)
        sb_hl.addWidget(self._status_right)
        self._status_timer = QTimer(self)
        self._status_timer.setSingleShot(True)
        self._status_timer.timeout.connect(self._reset_status)

        body.addWidget(self._sidebar())
        body.addWidget(self._main_stack(), 1)
        root.addLayout(body, 1)

        root.addWidget(self._status_bar)

        # --- 30-second live-score polling ---
        self._score_notifier = _ScoreNotifier()
        self._score_notifier.scores_ready.connect(self._on_scores_fetched)
        self._score_fetching = False
        self._score_timer = QTimer(self)
        self._score_timer.setInterval(10_000)
        self._score_timer.timeout.connect(self._poll_scores)
        self._score_timer.start()

        # Play-by-play notifier
        self._plays_notifier = _PlaysNotifier()
        self._plays_notifier.plays_ready.connect(self._on_plays_fetched)
        self._plays_cache = {}  # {game_id_str: [play_dicts]} — persists across polls

        # Immediately prefetch plays for all live/final games so data is ready
        # before the user opens the schedule page
        self._prefetch_all_plays()

        # Pre-fetch tomorrow's schedule so date switching is instant
        def _prefetch_tomorrow():
            try:
                import time
                time.sleep(3)                  # let today's weather finish first
                tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat()
                _DM.fetch_live_games(tomorrow)
                prefetch_weather(tomorrow)      # soft: skips if cache exists
            except Exception:
                log.exception("prefetch_tomorrow")
        _bg_pool.submit(_prefetch_tomorrow)

        # Auto-check for app updates on launch (silent — only shows status bar hint)
        QTimer.singleShot(3000, lambda: self._check_for_app_update(silent=True))
 
    def _titlebar(self):
        bar = QFrame()
        bar.setFixedHeight(44)
        bar.setStyleSheet(f"QFrame {{ background:{C['bg0']}; border-bottom:1px solid {C['bdr']}; }}")
        hl = QHBoxLayout(bar)
        hl.setContentsMargins(16,0,16,0)
        hl.setSpacing(8)
        hl.addSpacing(8)
        hl.addWidget(mk_label("SEAM \u2009ANALYTICS", color=C["t1"], size=12, bold=True, mono=True))
        hl.addWidget(mk_label("/", color=C["t3"], size=12, mono=True))
        hl.addWidget(mk_label("MLB · 2026 · ©SA", color=C["t3"], size=12, mono=True))
        hl.addStretch()

        # ── Update progress bar (hidden until update runs) ──
        from PyQt6.QtWidgets import QProgressBar
        self._update_progress = QProgressBar()
        self._update_progress.setFixedSize(100, 6)
        self._update_progress.setRange(0, 0)  # indeterminate
        self._update_progress.setTextVisible(False)
        self._update_progress.setStyleSheet(f"""
            QProgressBar {{ background:{C['bg3']}; border:none; border-radius:3px; }}
            QProgressBar::chunk {{ background:{C['ora']}; border-radius:3px; }}
        """)
        self._update_progress.hide()
        hl.addWidget(self._update_progress)
        self._update_pct_lbl = mk_label("0%", color=C["ora"], size=10, mono=True)
        self._update_pct_lbl.hide()
        hl.addWidget(self._update_pct_lbl)
        hl.addSpacing(4)

        # ── Version label ──
        ver_lbl = mk_label(f"v{_app_paths.APP_VERSION}", color=C["t3"], size=10, mono=True)
        hl.addWidget(ver_lbl)
        hl.addSpacing(6)

        # ── Check for Updates button ──
        self._check_update_btn = QPushButton("Check for Updates")
        self._check_update_btn.setToolTip("Check for new software versions")
        self._check_update_btn.setFixedHeight(28)
        self._check_update_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._check_update_btn.setStyleSheet(f"""
            QPushButton {{ background:transparent; color:{C['t3']}; border:1px solid {C['bdr']};
                           border-radius:4px; font-size:11px; padding:0 10px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ color:{C['grn']}; border-color:{C['grn']}; }}
            QPushButton:disabled {{ color:{C['t3']}; border-color:{C['bdr']}; opacity:0.5; }}
        """)
        self._check_update_btn.clicked.connect(self._on_check_for_updates)
        hl.addWidget(self._check_update_btn)
        hl.addSpacing(6)

        # ── Update Data button ──
        self._update_btn = QPushButton("Update Data")
        self._update_btn.setToolTip("Manually check for new database data")
        self._update_btn.setFixedHeight(28)
        self._update_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._update_btn.setStyleSheet(f"""
            QPushButton {{ background:transparent; color:{C['t3']}; border:1px solid {C['bdr']};
                           border-radius:4px; font-size:11px; padding:0 10px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ color:{C['ora']}; border-color:{C['ora']}; }}
            QPushButton:disabled {{ color:{C['t3']}; border-color:{C['bdr']}; opacity:0.5; }}
        """)
        self._update_btn.clicked.connect(self._run_manual_update)
        hl.addWidget(self._update_btn)
        hl.addSpacing(6)

        # ── Settings button ──
        settings_btn = QPushButton("Settings")
        settings_btn.setToolTip("Application settings")
        settings_btn.setFixedHeight(28)
        settings_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        settings_btn.setStyleSheet(f"""
            QPushButton {{ background:transparent; color:{C['t3']}; border:1px solid {C['bdr']};
                           border-radius:4px; font-size:11px; padding:0 10px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ color:{C['ora']}; border-color:{C['ora']}; }}
        """)
        settings_btn.clicked.connect(self._show_settings)
        hl.addWidget(settings_btn)
        hl.addSpacing(6)

        _info_svg = os.path.join(_app_paths.APP_DIR, "assets", "info-svgrepo-com.svg")
        about_btn = QPushButton()
        try:
            from PyQt6.QtSvg import QSvgRenderer
            from PyQt6.QtCore import QByteArray
            with open(_info_svg, "r", encoding="utf-8") as f:
                svg_data = f.read()
            # Normal state icon (theme subdued color)
            svg_normal = svg_data.replace('fill="#000000"', f'fill="{C["t3"]}"')
            pm_normal = QPixmap(18, 18)
            pm_normal.fill(QColor(0, 0, 0, 0))
            rn = QSvgRenderer(QByteArray(svg_normal.encode()))
            pn = QPainter(pm_normal)
            rn.render(pn)
            pn.end()
            # Hover state icon (accent color)
            svg_hover = svg_data.replace('fill="#000000"', f'fill="{C["ora"]}"')
            pm_hover = QPixmap(18, 18)
            pm_hover.fill(QColor(0, 0, 0, 0))
            rh = QSvgRenderer(QByteArray(svg_hover.encode()))
            ph = QPainter(pm_hover)
            rh.render(ph)
            ph.end()
            _icon_normal = QIcon(pm_normal)
            _icon_hover = QIcon(pm_hover)
            about_btn.setIcon(_icon_normal)
            about_btn._icon_normal = _icon_normal
            about_btn._icon_hover = _icon_hover
            _orig_enter = about_btn.enterEvent
            _orig_leave = about_btn.leaveEvent
            def _enter(e, b=about_btn, oe=_orig_enter):
                b.setIcon(b._icon_hover); oe(e)
            def _leave(e, b=about_btn, ol=_orig_leave):
                b.setIcon(b._icon_normal); ol(e)
            about_btn.enterEvent = _enter
            about_btn.leaveEvent = _leave
        except Exception:
            about_btn.setIcon(QIcon(_info_svg))
        about_btn.setIconSize(QSize(18, 18))
        about_btn.setFixedSize(28, 28)
        about_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        about_btn.setStyleSheet("QPushButton { background:transparent; border:none; }")
        about_btn.clicked.connect(self._show_about)
        hl.addWidget(about_btn)
        return bar

    # ── Manual data update ──
    def _run_manual_update(self):
        from PyQt6.QtWidgets import QMessageBox
        reply = QMessageBox.question(
            self, "Update Data",
            "Download and process the latest game data?\n\n"
            "This may take a few minutes depending on how many\n"
            "days need to be fetched.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes)
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._update_btn.setEnabled(False)
        self._update_btn.setText("Updating…")
        self._update_progress.setRange(0, 100)
        self._update_progress.setValue(0)
        self._update_progress.show()
        self._update_pct_lbl.setText("0%")
        self._update_pct_lbl.show()
        self.set_status("Data update in progress…", timeout=0)

        class _UpdateWorker(QThread):
            finished = pyqtSignal(str, dict)   # error_msg, summary_dict
            progress = pyqtSignal(int, int, str)  # current, total, phase

            def run(self_w):
                try:
                    import daily_update
                    summary = daily_update.main([], gui_cb=self_w.progress.emit) or {}
                    self_w.finished.emit("", summary)
                except Exception as exc:
                    self_w.finished.emit(str(exc), {})

        self._update_worker = _UpdateWorker()
        self._update_worker.progress.connect(self._on_update_progress)
        self._update_worker.finished.connect(self._on_update_done)
        self._update_worker.start()

    def _on_update_progress(self, current: int, total: int, phase: str):
        if total > 0:
            pct = min(int(current * 100 / total), 100)
            self._update_progress.setValue(pct)
            self._update_pct_lbl.setText(f"{pct}%")
            self._update_btn.setText(f"Updating… {pct}%")

    def _on_update_done(self, error: str, summary: dict):
        self._update_progress.hide()
        self._update_progress.setRange(0, 0)  # reset to indeterminate for next time
        self._update_pct_lbl.hide()
        self._update_btn.setEnabled(True)
        self._update_btn.setText("Update Data")
        from PyQt6.QtWidgets import QMessageBox
        if error:
            self.set_status(f"Update failed: {error}", timeout=10000, error=True)
            log.warning("Manual update failed: %s", error)
            msg = QMessageBox(self)
            msg.setWindowTitle("Update Failed")
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setText(f"Update failed:\n{error}")
            msg.setStyleSheet(f"QMessageBox {{ background:{C['bg1']}; color:{C['t1']}; }}"
                              f" QPushButton {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};"
                              f" padding:6px 20px; border-radius:4px; }}")
            msg.exec()
        else:
            log.info("Manual data update completed successfully")
            if summary.get("up_to_date"):
                body = "Your database is already up to date.\nNo new games to ingest."
            else:
                rng = summary.get("range", "")
                lines = [f"<b>Range:</b>  {rng}"]
                lines.append(f"<b>Games:</b>  {summary.get('games', 0)}")
                lines.append(f"<b>Plate Appearances:</b>  {summary.get('plate_appearances', 0)}")
                lines.append(f"<b>Pitching Appearances:</b>  {summary.get('pitching_appearances', 0)}")
                lines.append(f"<b>Stolen-Base Events:</b>  {summary.get('stolen_bases', 0)}")
                bf = summary.get("statcast_backfill", 0)
                if bf:
                    lines.append(f"<b>Statcast Backfill:</b>  {bf} date(s)")
                body = "<br>".join(lines)
            msg = QMessageBox(self)
            msg.setWindowTitle("Update Complete")
            msg.setIcon(QMessageBox.Icon.Information)
            msg.setText(body)
            msg.setStyleSheet(f"QMessageBox {{ background:{C['bg1']}; color:{C['t1']}; }}"
                              f" QPushButton {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};"
                              f" padding:6px 20px; border-radius:4px; }}")
            msg.exec()
            self.set_status("Data update complete!", timeout=8000)

    # ── App-version update system ────────────────────────────────────
    _GITHUB_RELEASES_URL = "https://api.github.com/repos/nateoswald16/SEAM-ANALYTICS/releases"
    _MSG_STYLE = (f"QMessageBox {{ background:{C['bg1']}; color:{C['t1']}; }}"
                  f" QPushButton {{ background:{C['bg3']}; color:{C['t1']};"
                  f" border:1px solid {C['bdr']}; padding:6px 20px; border-radius:4px; }}")

    def _check_for_app_update(self, silent: bool = True):
        """Query GitHub releases API for a newer version.

        *silent* — if True (launch check) only show status bar hint;
                   if False (button click) show progress on button.
        """
        class _CheckWorker(QThread):
            result = pyqtSignal(str, str, str)  # new_ver, asset_url, body (or "" on no update)

            def run(self_w):
                try:
                    r = requests.get(
                        SeamStatsApp._GITHUB_RELEASES_URL,
                        headers={"Accept": "application/vnd.github+json"},
                        timeout=10,
                    )
                    if r.status_code == 404:
                        self_w.result.emit("", "", "")
                        return
                    r.raise_for_status()
                    releases = r.json()
                    # /releases returns a list sorted newest-first;
                    # pick the first non-draft entry (includes pre-releases)
                    data = None
                    for rel in (releases if isinstance(releases, list) else []):
                        if not rel.get("draft", False):
                            data = rel
                            break
                    if data is None:
                        self_w.result.emit("", "", "")
                        return
                    tag = data.get("tag_name", "").lstrip("vV")
                    body = data.get("body", "")
                    # Find the setup .exe asset
                    asset_url = ""
                    for a in data.get("assets", []):
                        name = a.get("name", "")
                        if name.lower().endswith(".exe") and "setup" in name.lower():
                            asset_url = a.get("browser_download_url", "")
                            break
                    # Only notify if the remote tag is strictly newer
                    def _parse_ver(v):
                        """Parse '1.0.3-beta' → (1, 0, 3, 'beta')."""
                        import re as _re
                        m = _re.match(r"(\d+)\.(\d+)\.(\d+)(?:-(.+))?", v)
                        if not m:
                            return (0, 0, 0, "")
                        return (int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4) or "")
                    if tag and _parse_ver(tag) > _parse_ver(_app_paths.APP_VERSION):
                        self_w.result.emit(tag, asset_url, body)
                    else:
                        self_w.result.emit("", "", "")
                except Exception:
                    self_w.result.emit("", "", "")

        self._update_check_silent = silent
        if not silent:
            self._check_update_btn.setEnabled(False)
            self._check_update_btn.setText("Checking…")
        self._check_worker = _CheckWorker()
        self._check_worker.result.connect(self._on_update_check_result)
        self._check_worker.start()

    def _on_update_check_result(self, new_ver: str, asset_url: str, body: str):
        """Handle the result of a version check."""
        self._check_update_btn.setEnabled(True)
        self._check_update_btn.setText("Check for Updates")
        if not new_ver:
            if not self._update_check_silent:
                from PyQt6.QtWidgets import QMessageBox
                msg = QMessageBox(self)
                msg.setWindowTitle("Check for Updates")
                msg.setIcon(QMessageBox.Icon.Information)
                msg.setText(f"v{_app_paths.APP_VERSION} — You're on the latest version!")
                msg.setStyleSheet(self._MSG_STYLE)
                msg.exec()
            return
        # If this is a silent (auto) check, honour the user's skip preference
        skipped = self._qsettings.value("skippedUpdateVersion", "")
        if self._update_check_silent and new_ver == skipped:
            return
        # Store for download
        self._pending_update_ver = new_ver
        self._pending_update_url = asset_url
        self._pending_update_body = body
        # Highlight button
        self._check_update_btn.setStyleSheet(f"""
            QPushButton {{ background:transparent; color:{C['grn']}; border:1px solid {C['grn']};
                           border-radius:4px; font-size:11px; padding:0 10px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ color:{C['t1']}; border-color:{C['grn']}; background:{C['bg3']}; }}
        """)
        self._check_update_btn.setText(f"Update → v{new_ver}")
        if self._update_check_silent:
            self.set_status(
                f"Update v{new_ver} available — click \"Update → v{new_ver}\" to install",
                timeout=0)

    def _on_check_for_updates(self):
        """Button-click handler: if an update is pending, start download; otherwise check."""
        if hasattr(self, '_pending_update_ver') and self._pending_update_ver:
            self._start_update_download()
        else:
            self._check_for_app_update(silent=False)

    def _start_update_download(self):
        """Download the installer and show confirmation dialog."""
        from PyQt6.QtWidgets import QMessageBox
        url = getattr(self, '_pending_update_url', '')
        new_ver = getattr(self, '_pending_update_ver', '')
        body = getattr(self, '_pending_update_body', '')
        if not url:
            msg = QMessageBox(self)
            msg.setWindowTitle("Update Unavailable")
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setText("No installer found for this release.\n"
                        "Please download manually from the GitHub releases page.")
            msg.setStyleSheet(self._MSG_STYLE)
            msg.exec()
            return

        # Confirmation dialog
        from PyQt6.QtWidgets import QCheckBox
        msg = QMessageBox(self)
        msg.setWindowTitle("App Update Available")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setText(f"<b>Current version:</b>  v{_app_paths.APP_VERSION}<br>"
                    f"<b>New version:</b>  v{new_ver}")
        msg.setInformativeText("Download and install this update?")
        if body:
            msg.setDetailedText(body)
        _cb_style = (f"QCheckBox {{ color:{C['t2']}; font-size:11px; spacing:6px; }} "
                     f"QCheckBox::indicator {{ width:14px; height:14px; border:1px solid {C['bdrl']}; border-radius:3px; background:{C['bg2']}; }}"
                     f"QCheckBox::indicator:checked {{ background:{C['ora']}; border-color:{C['ora']}; }}"
                     f"QCheckBox::indicator:hover {{ border-color:{C['t2']}; }}")
        db_cb = QCheckBox("Replace local databases with the latest bundled data")
        db_cb.setStyleSheet(_cb_style)
        _grid = msg.layout()
        _grid.addWidget(db_cb, _grid.rowCount(), 0, 1, _grid.columnCount())
        # Add task repair checkbox
        task_cb = None
        if sys.platform == "win32":
            task_cb = QCheckBox("Ensure scheduled task exists for daily auto-updates")
            task_cb.setChecked(True)
            task_cb.setStyleSheet(_cb_style)
            _grid.addWidget(task_cb, _grid.rowCount(), 0, 1, _grid.columnCount())
        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        skip_btn = msg.addButton("Skip This Version", QMessageBox.ButtonRole.RejectRole)
        msg.setDefaultButton(QMessageBox.StandardButton.Yes)
        msg.setStyleSheet(self._MSG_STYLE)
        msg.exec()
        if msg.clickedButton() == skip_btn:
            self._qsettings.setValue("skippedUpdateVersion", new_ver)
            self._pending_update_ver = ""
            self._pending_update_url = ""
            self._pending_update_body = ""
            self._check_update_btn.setText("Check for Updates")
            self._check_update_btn.setStyleSheet(f"""
                QPushButton {{ background:transparent; color:{C['t2']}; border:1px solid {C['bdr']};
                               border-radius:4px; font-size:11px; padding:0 10px;
                               font-family:'Cascadia Mono','Consolas',monospace; }}
                QPushButton:hover {{ color:{C['t1']}; border-color:{C['bdrl']}; background:{C['bg3']}; }}
            """)
            self.set_status(f"v{new_ver} skipped — you'll be notified when a newer release is available", timeout=8000)
            return
        if msg.clickedButton() != msg.button(QMessageBox.StandardButton.Yes):
            return

        self._update_refresh_db = db_cb.isChecked()
        self._update_repair_task = task_cb.isChecked() if task_cb else False

        # Start download
        self._check_update_btn.setEnabled(False)
        self._check_update_btn.setText("Downloading…")
        self._update_progress.setRange(0, 100)
        self._update_progress.setValue(0)
        self._update_progress.show()
        self._update_pct_lbl.setText("0%")
        self._update_pct_lbl.show()
        self.set_status("Downloading update…", timeout=0)

        import tempfile
        self._update_tmp_dir = tempfile.mkdtemp(prefix="seam_update_")
        self._update_tmp_file = os.path.join(self._update_tmp_dir,
                                             f"SeamAnalytics-Setup-{new_ver}.exe")

        class _DownloadWorker(QThread):
            progress = pyqtSignal(int)      # percent
            finished = pyqtSignal(str)      # error_msg ("" on success)

            def __init__(self_w, url, dest):
                super().__init__()
                self_w._url = url
                self_w._dest = dest

            def run(self_w):
                try:
                    r = requests.get(self_w._url, stream=True, timeout=60,
                                     headers={"Accept": "application/octet-stream"})
                    r.raise_for_status()
                    total = int(r.headers.get("content-length", 0))
                    downloaded = 0
                    with open(self_w._dest, "wb") as f:
                        for chunk in r.iter_content(chunk_size=256 * 1024):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total > 0:
                                self_w.progress.emit(int(downloaded * 100 / total))
                    self_w.finished.emit("")
                except Exception as exc:
                    self_w.finished.emit(str(exc))

        self._dl_worker = _DownloadWorker(url, self._update_tmp_file)
        self._dl_worker.progress.connect(self._on_dl_progress)
        self._dl_worker.finished.connect(self._on_dl_done)
        self._dl_worker.start()

    def _on_dl_progress(self, pct: int):
        self._update_progress.setValue(pct)
        self._update_pct_lbl.setText(f"{pct}%")
        self._check_update_btn.setText(f"Downloading… {pct}%")

    def _on_dl_done(self, error: str):
        self._update_progress.hide()
        self._update_pct_lbl.hide()
        self._check_update_btn.setEnabled(True)
        from PyQt6.QtWidgets import QMessageBox
        if error:
            self._check_update_btn.setText("Check for Updates")
            self.set_status(f"Download failed: {error}", timeout=10000, error=True)
            # Clean up temp file
            try:
                os.remove(self._update_tmp_file)
                os.rmdir(self._update_tmp_dir)
            except Exception:
                pass
            return

        self.set_status("Download complete — installing…", timeout=0)
        self._check_update_btn.setText("Installing…")
        self._check_update_btn.setEnabled(False)

        # Run the Inno Setup installer silently
        import subprocess
        try:
            cmd = [self._update_tmp_file, "/VERYSILENT",
                   "/SUPPRESSMSGBOXES", "/CLOSEAPPLICATIONS"]
            tasks = []
            if getattr(self, '_update_repair_task', True):
                tasks.append("scheduledupdate")
            if getattr(self, '_update_refresh_db', False):
                tasks.append("refreshdb")
            cmd.append(f"/TASKS={','.join(tasks)}")
            subprocess.Popen(
                cmd,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
        except Exception as exc:
            self._check_update_btn.setEnabled(True)
            self._check_update_btn.setText("Check for Updates")
            self.set_status(f"Install failed: {exc}", timeout=10000, error=True)
            return

        # The installer will close this app via /CLOSEAPPLICATIONS,
        # then relaunch it via RestartApplications=yes in the ISS.
        # Clean up the temp installer in the background.
        tmp = getattr(self, '_update_tmp_file', '')
        tmp_dir = getattr(self, '_update_tmp_dir', '')
        if sys.platform == "win32":
            cleanup_cmd = (
                f'cmd /c ping -n 3 127.0.0.1 >nul '
                f'& del /q "{tmp}" '
                f'& rmdir /q "{tmp_dir}"'
            )
            subprocess.Popen(cleanup_cmd, shell=True,
                             creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            try:
                if tmp and os.path.isfile(tmp):
                    os.remove(tmp)
                if tmp_dir and os.path.isdir(tmp_dir):
                    os.rmdir(tmp_dir)
            except Exception:
                pass

        self.set_status("Installing update — app will restart automatically…", timeout=0)

    def _check_task_exists(self):
        """Check if the SeamAnalytics\\DailyUpdate scheduled task exists."""
        if sys.platform != "win32":
            return False
        import subprocess
        try:
            r = subprocess.run(
                ['schtasks', '/Query', '/TN', r'SeamAnalytics\DailyUpdate'],
                capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
            return r.returncode == 0
        except Exception:
            return False

    def _create_scheduled_task(self, time_str="03:00"):
        """Create or recreate the SeamAnalytics\\DailyUpdate scheduled task."""
        if sys.platform != "win32":
            return False
        import subprocess
        import tempfile
        from datetime import date as _date
        if _app_paths._frozen:
            install_dir = os.path.dirname(os.path.dirname(sys.executable))
            updater = os.path.join(install_dir, "SeamUpdater", "SeamUpdater.exe")
        else:
            updater = os.path.join(_app_paths.APP_DIR, "daily_update.py")
        xml = (
            '<?xml version="1.0" encoding="UTF-16"?>\n'
            '<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">\n'
            '  <Triggers><CalendarTrigger>\n'
            f'    <StartBoundary>{_date.today().isoformat()}T{time_str}:00</StartBoundary>\n'
            '    <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>\n'
            '  </CalendarTrigger></Triggers>\n'
            '  <Settings>\n'
            '    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n'
            '    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n'
            '    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n'
            '    <StartWhenAvailable>true</StartWhenAvailable>\n'
            '    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n'
            '  </Settings>\n'
            '  <Actions Context="Author"><Exec>\n'
            f'    <Command>{updater}</Command>\n'
            '  </Exec></Actions>\n'
            '</Task>\n'
        )
        xml_path = None
        try:
            fd, xml_path = tempfile.mkstemp(suffix='.xml', prefix='seam_task_')
            with os.fdopen(fd, 'w', encoding='utf-16') as f:
                f.write(xml)
            r = subprocess.run(
                ['schtasks', '/Create', '/F',
                 '/TN', r'SeamAnalytics\DailyUpdate',
                 '/XML', xml_path],
                capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
            return r.returncode == 0
        except Exception:
            return False
        finally:
            if xml_path:
                try:
                    os.unlink(xml_path)
                except Exception:
                    pass

    def _show_settings(self):
        """Show the Settings dialog."""
        from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                                     QPushButton, QComboBox, QGroupBox)

        dlg = QDialog(self)
        dlg.setWindowTitle("Settings")
        dlg.setFixedWidth(420)
        dlg.setStyleSheet(f"""
            QDialog {{ background:{C['bg1']}; color:{C['t1']}; }}
            QLabel {{ color:{C['t2']}; font-family:'Cascadia Mono','Consolas',monospace; font-size:11px; }}
            QGroupBox {{ color:{C['t2']}; border:1px solid {C['bdr']}; border-radius:6px;
                         margin-top:12px; padding:14px 10px 10px 10px;
                         font-family:'Cascadia Mono','Consolas',monospace; font-size:11px; }}
            QGroupBox::title {{ subcontrol-origin:margin; left:10px; padding:0 4px; }}
        """)

        vl = QVBoxLayout(dlg)
        vl.setContentsMargins(16, 16, 16, 16)
        vl.setSpacing(12)

        # ── Scheduled Task group ──
        grp = QGroupBox("Scheduled Task")
        gl = QVBoxLayout(grp)
        gl.setSpacing(8)

        # Status
        task_exists = self._check_task_exists()
        st_text = "Active" if task_exists else "Not found"
        st_color = C["grn"] if task_exists else C["red"]
        status_lbl = QLabel(f'Status: <span style="color:{st_color}">{st_text}</span>')
        gl.addWidget(status_lbl)

        # Time picker row
        time_row = QHBoxLayout()
        time_row.addWidget(QLabel("Time:"))

        combo_css = f"""
            QComboBox {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};
                         border-radius:4px; padding:4px 8px;
                         font-family:'Cascadia Mono','Consolas',monospace; font-size:11px; }}
            QComboBox:hover {{ border-color:{C['ora']}; }}
            QComboBox::drop-down {{ border:none; }}
            QComboBox QAbstractItemView {{ background:{C['bg2']}; color:{C['t1']};
                                           selection-background-color:{C['bg3']}; }}
        """
        hour_cb = QComboBox()
        for h in range(1, 13):
            hour_cb.addItem(str(h))
        hour_cb.setCurrentIndex(2)  # default 3
        hour_cb.setStyleSheet(combo_css)
        time_row.addWidget(hour_cb)

        colon_lbl = QLabel(":")
        colon_lbl.setFixedWidth(8)
        colon_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        time_row.addWidget(colon_lbl)

        min_cb = QComboBox()
        for m in ["00", "15", "30", "45"]:
            min_cb.addItem(m)
        min_cb.setStyleSheet(combo_css)
        time_row.addWidget(min_cb)

        ampm_cb = QComboBox()
        ampm_cb.addItems(["AM", "PM"])
        ampm_cb.setStyleSheet(combo_css)
        time_row.addWidget(ampm_cb)

        time_row.addStretch()
        gl.addLayout(time_row)

        # Description
        desc = QLabel("Creates a Windows scheduled task that runs the daily\n"
                      "data updater automatically. Use this to repair a\n"
                      "missing task or change the update time.")
        desc.setStyleSheet(f"color:{C['t3']}; font-size:10px;")
        desc.setWordWrap(True)
        gl.addWidget(desc)

        # Repair / Create button
        btn_text = "Repair Scheduled Task" if task_exists else "Create Scheduled Task"
        repair_btn = QPushButton(btn_text)
        repair_btn.setFixedHeight(30)
        repair_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        repair_btn.setStyleSheet(f"""
            QPushButton {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};
                           border-radius:4px; font-size:11px; padding:0 14px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ border-color:{C['ora']}; color:{C['ora']}; }}
        """)

        def _on_repair():
            h12 = int(hour_cb.currentText())
            is_pm = ampm_cb.currentText() == "PM"
            if is_pm:
                h24 = h12 if h12 == 12 else h12 + 12
            else:
                h24 = 0 if h12 == 12 else h12
            ts = f"{h24:02d}:{min_cb.currentText()}"
            ok = self._create_scheduled_task(ts)
            if ok:
                status_lbl.setText(f'Status: <span style="color:{C["grn"]}">Active</span>')
                repair_btn.setText("Repair Scheduled Task")
                self.set_status(
                    f"Scheduled task created \u2014 daily update at "
                    f"{hour_cb.currentText()}:{min_cb.currentText()} {ampm_cb.currentText()}",
                    timeout=8000)
            else:
                status_lbl.setText(f'Status: <span style="color:{C["red"]}">Failed</span>')
                self.set_status("Failed to create scheduled task", timeout=8000, error=True)

        repair_btn.clicked.connect(_on_repair)
        gl.addWidget(repair_btn)

        vl.addWidget(grp)

        # ── Close button ──
        close_row = QHBoxLayout()
        close_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.setFixedHeight(30)
        close_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        close_btn.setStyleSheet(f"""
            QPushButton {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};
                           border-radius:4px; font-size:11px; padding:0 20px;
                           font-family:'Cascadia Mono','Consolas',monospace; }}
            QPushButton:hover {{ border-color:{C['ora']}; }}
        """)
        close_btn.clicked.connect(dlg.accept)
        close_row.addWidget(close_btn)
        vl.addLayout(close_row)

        dlg.exec()

    def _show_about(self):
        from PyQt6.QtWidgets import QMessageBox
        msg = QMessageBox(self)
        msg.setWindowTitle("About Seam Analytics")
        msg.setIconPixmap(QPixmap(_app_paths.LOGO_PNG).scaled(64, 64, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
        msg.setText(
            f"<h3>Seam Analytics</h3>"
            f"<p>Version {_app_paths.APP_VERSION}</p>"
        )
        from PyQt6.QtCore import QLibraryInfo
        qt_ver = QLibraryInfo.version().toString()
        msg.setInformativeText(
            f"MLB Stats Desktop App\n\n"
            f"Python {sys.version.split()[0]}\n"
            f"PyQt6 {qt_ver}\n\n"
            f"Data: MLB Stats API · Baseball Savant\n"
            f"© 2026 Seam Analytics"
        )
        msg.setStyleSheet(f"QMessageBox {{ background:{C['bg1']}; color:{C['t1']}; }}"
                          f" QPushButton {{ background:{C['bg3']}; color:{C['t1']}; border:1px solid {C['bdr']};"
                          f" padding:6px 20px; border-radius:4px; }}"
                          f" QPushButton:hover {{ border-color:{C['ora']}; }}")
        msg.exec()

    def _reset_status(self):
        self._status_lbl.setText("Ready")
        self._status_lbl.setStyleSheet(
            f"color:{C['t3']}; background:transparent; font-family:'Cascadia Mono','Consolas',monospace; font-size:10px;")
        self._status_right.setText("")

    def set_status(self, msg, timeout=5000, right="", error=False):
        """Show a message in the status bar. Auto-reverts to 'Ready' after timeout ms (0=sticky)."""
        color = C["red"] if error else C["t3"]
        self._status_lbl.setStyleSheet(
            f"color:{color}; background:transparent; font-family:'Cascadia Mono','Consolas',monospace; font-size:10px;")
        self._status_lbl.setText(msg)
        if right:
            self._status_right.setText(right)
        if timeout > 0:
            self._status_timer.start(timeout)
        else:
            self._status_timer.stop()
 
    def _sidebar(self):
        sb = QFrame()
        sb.setMinimumWidth(290)
        sb.setMaximumWidth(320)
        sb.setStyleSheet(f"QFrame {{ background:{C['bg1']}; border-right:1px solid {C['bdr']}; }}")
        vl = QVBoxLayout(sb)
        vl.setContentsMargins(0,0,0,0)
        vl.setSpacing(0)
 
        hdr = QWidget()
        hdr.setFixedHeight(56)
        hdr.setStyleSheet(f"background:{C['bg1']}; border-bottom:1px solid {C['bdr']};")
        hl = QVBoxLayout(hdr)
        hl.setContentsMargins(14,8,14,8)
        hl.setSpacing(2)
        hl.addWidget(mk_label("[ SCHEDULE ]", color=C["t3"], size=10, mono=True))
        # use selected date (defaults to today, changed by navbar date selector)
        sel_date = self._selected_date.isoformat()
        try:
            friendly = dt.datetime.strptime(sel_date, "%Y-%m-%d").strftime("%a, %b %d")
        except Exception:
            friendly = sel_date
        self._sidebar_date_lbl = mk_label(friendly, color=C["t1"], size=14, bold=True)
        hl.addWidget(self._sidebar_date_lbl)
        vl.addWidget(hdr)
 
        sa = SmoothScrollArea()
        sa.setWidgetResizable(True)
        sa.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        sa.setStyleSheet(f"""
            QScrollArea {{ background:{C['bg1']}; border:none; }}
            QScrollBar:vertical {{ background:transparent; width:3px; }}
            QScrollBar::handle:vertical {{ background:{C['bdrl']}; border-radius:2px; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}
        """)
        cw = QWidget()
        cw.setStyleSheet(f"background:{C['bg1']};")
        self._card_layout = QVBoxLayout(cw)
        self._card_layout.setContentsMargins(0,0,0,0)
        self._card_layout.setSpacing(0)
        # Reuse module-level GAMES (already fetched at startup) instead of re-querying
        self._games = list(GAMES)

        self._games.sort(key=_game_sort_key)

        # start background prefetch of lineups (best-effort)
        # (weather is fetched by ParkFactorsPage's _WeatherWorker)
        try:
            self._lineup_prefetch_future = _bg_pool.submit(_DM.prefetch_lineups, self._games)
        except Exception:
            self._lineup_prefetch_future = None
            log.exception("_sidebar")
        GameCard._max_height = 0
        for i, game in enumerate(self._games):
            card = GameCard(game, i, self._on_game_clicked)
            self._cards.append(card)
            self._card_layout.addWidget(card)
        # Uniform height pass
        if GameCard._max_height:
            for c in self._cards:
                c.setMinimumHeight(GameCard._max_height)
        self._card_layout.addStretch()
        sa.setWidget(cw)
        vl.addWidget(sa, 1)
        return sb
 
    def _main_stack(self):
        self._stack = QStackedWidget()
        self._stack.setStyleSheet(f"background:{C['bg0']};")
        self._stack.addWidget(build_home_page())

        # Add placeholder pages for leaderboards (will be replaced when data arrives)
        for title in ("Top Stats - Hitting", "Top Stats - Pitching", "Top Stats - Base Running"):
            self._stack.addWidget(build_top_stats_page(title, "Loading…"))

        # Build schedule page immediately (odds loaded async)
        self._stack.addWidget(build_matchup_page(
            games=self._games if hasattr(self, '_games') else GAMES,
            odds=None,
            on_click=None,
            date=self._selected_date))
        self._stack.addWidget(QWidget())  # Lineups placeholder (actual view is IDX_GAME)
        self._park_page = ParkFactorsPage(date_str=self._selected_date.isoformat())
        self._stack.addWidget(self._park_page)

        # Fetch leaderboard data in background thread
        class _LBNotifier(QObject):
            ready = pyqtSignal(tuple)
        self._lb_notifier = _LBNotifier()
        self._lb_notifier.ready.connect(self._on_leaderboards_ready)

        def _fetch_lb():
            hit_lbs, pit_lbs, br_lbs = [], [], []
            try:
                # Wait for lineup prefetch (short timeout — proceed without if slow)
                if getattr(self, '_lineup_prefetch_future', None):
                    try:
                        self._lineup_prefetch_future.result(timeout=10)
                    except Exception:
                        pass
                pt = _DM.get_todays_player_info(self._games)
                if pt:
                    _fmt_avg = lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)
                    _fmt_pct = lambda v: f"{v * 100:.1f}%" if isinstance(v, float) else str(v)
                    _fmt_era = lambda v: f"{v:.2f}" if isinstance(v, float) else str(v)
                    _fmt_int = lambda v: str(int(v)) if isinstance(v, (int, float)) else str(v)
                    _fmt_spd = lambda v: f"{v:.1f}" if isinstance(v, float) else str(v)
                    _fmt_streak = lambda v: str(int(v))

                    _bat = "calculated_batting_stats"
                    _min_pa = ("plate_appearances", 10)
                    # Batch: 3 cols without min filter, 3 cols with min PA filter
                    bat_no_min = _DM.get_leaderboards_batch(
                        _bat, ["hits", "home_runs", "rbis", "singles", "doubles", "triples"], pt)
                    bat_with_min = _DM.get_leaderboards_batch(
                        _bat, ["avg", "barrel_pct", "avg_ev"], pt,
                        min_col=_min_pa[0], min_val=_min_pa[1])
                    hit_lbs = [
                        ("HITS (H)",       bat_no_min["hits"],       _fmt_int),
                        ("HOME RUNS (HR)", bat_no_min["home_runs"],  _fmt_int),
                        ("BATTING AVG",    bat_with_min["avg"],      _fmt_avg),
                        ("RBI",            bat_no_min["rbis"],       _fmt_int),
                        ("SINGLES (1B)",   bat_no_min["singles"],    _fmt_int),
                        ("DOUBLES (2B)",   bat_no_min["doubles"],    _fmt_int),
                        ("TRIPLES (3B)",   bat_no_min["triples"],    _fmt_int),
                        ("BARREL %",       bat_with_min["barrel_pct"], _fmt_pct),
                        ("EXIT VELO",      bat_with_min["avg_ev"],   _fmt_spd),
                        ("HIT STREAK",     _DM.get_streak_leaderboard(pt, 'is_hit'),     _fmt_streak, "GAMES"),
                        ("HR STREAK",      _DM.get_streak_leaderboard(pt, 'is_home_run'), _fmt_streak, "GAMES"),
                        ("HR + SB GAMES",  _DM.get_hr_sb_game_leaderboard(pt),            _fmt_streak, "GAMES"),
                    ]

                    _pit = "calculated_pitching_stats"
                    _min_ip = ("innings_pitched", 5.0)
                    # Batch: all 3 pitching cols (ERA ascending)
                    pit_batch = _DM.get_leaderboards_batch(
                        _pit, ["strikeouts", "era", "whiff_pct"], pt,
                        ascending_cols={"era"},
                        min_col=_min_ip[0], min_val=_min_ip[1])
                    pit_lbs = [
                        ("STRIKEOUTS (K)", pit_batch["strikeouts"], _fmt_int),
                        ("ERA",            pit_batch["era"],        _fmt_era),
                        ("WHIFF %",        pit_batch["whiff_pct"],  _fmt_pct),
                    ]

                    _br = "calculated_baserunning_stats"
                    # Batch: all 3 baserunning cols
                    br_batch = _DM.get_leaderboards_batch(
                        _br, ["stolen_bases", "sprint_speed", "bolts"], pt)
                    br_lbs = [
                        ("STOLEN BASES",  br_batch["stolen_bases"],  _fmt_int),
                        ("SPRINT SPEED",  br_batch["sprint_speed"],  _fmt_spd),
                        ("BOLTS",         br_batch["bolts"],         _fmt_int),
                    ]
            except Exception:
                log.exception("_fetch_lb")
            self._lb_notifier.ready.emit((hit_lbs, pit_lbs, br_lbs))

        self._fetch_lb = _fetch_lb
        self._lb_fetched = False  # defer until user navigates to a leaderboard page

        # Fetch DraftKings odds in background thread
        self._odds_notifier = _OddsNotifier()
        self._odds_notifier.odds_ready.connect(self._on_odds_fetched)
        self.set_status("Loading odds\u2026", timeout=0)
        _bg_pool.submit(self._fetch_odds_bg)

        self._game_panel = GameDetailPanel()
        self._stack.addWidget(self._game_panel)
        # connect lineup-cached signal to game panel for async updates
        try:
            if hasattr(_DM, 'notifier') and _DM.notifier:
                _DM.notifier.lineup_cached.connect(self._game_panel.on_lineup_cached)
        except Exception:
            log.exception("_fetch_lb")
        return self._stack
 
    def _on_main_nav(self, idx):
        try:
            if _is_deleted(self._stack):
                return
            # Lazy-load leaderboard data on first visit to HIT/PITCH/BR pages
            if not self._lb_fetched and idx in (self.IDX_HIT, self.IDX_PITCH, self.IDX_BR):
                self._lb_fetched = True
                _bg_pool.submit(self._fetch_lb)
            # Lineups tab → load first game and show game detail
            if idx == self.IDX_LINEUP:
                if (hasattr(self, '_games') and self._games) or GAMES:
                    self._on_game_clicked(0)
                    for i, b in enumerate(self._main_btns):
                        b.setChecked(i == self.IDX_LINEUP)
                        b.setStyleSheet(_nav_btn_style(i == self.IDX_LINEUP))
                return
            _fade_switch(self._stack, idx)
            if idx != self.IDX_GAME:
                self._deselect_cards()
        except RuntimeError:
            pass  # C++ object deleted
 
    def _on_game_clicked(self, idx: int):
        try:
            self._deselect_cards()
            self._cards[idx].set_selected(True)
            self._sel_card = idx
            self.set_status("Loading game\u2026", timeout=0)
            # prefer DB-backed games list when available
            try:
                game = (self._games[idx] if hasattr(self, '_games') and idx < len(self._games) else GAMES[idx])
            except Exception:
                game = GAMES[idx]
            # Show current cached data immediately …
            self._game_panel.load_game(game)
            away = game.get('away_team', '')
            home = game.get('home_team', '')
            self.set_status(f"{away} @ {home}", right="Game loaded")
            if self._stack.currentIndex() == self.IDX_GAME:
                # Already on game page — fade only the inner content, not the subnav
                inner = getattr(self._game_panel, '_inner_stack', None)
                if inner and not _is_deleted(inner):
                    prev = getattr(inner, '_fade_anim', None)
                    if prev is not None:
                        prev.stop()
                        prev.deleteLater()
                        inner.setGraphicsEffect(None)
                    eff = QGraphicsOpacityEffect(inner)
                    eff.setOpacity(0.0)
                    inner.setGraphicsEffect(eff)
                    anim = QPropertyAnimation(eff, b"opacity")
                    anim.setDuration(150)
                    anim.setStartValue(0.0)
                    anim.setEndValue(1.0)
                    anim.setEasingCurve(QEasingCurve.Type.OutQuad)
                    anim.finished.connect(lambda: (
                        inner.setGraphicsEffect(None) if not _is_deleted(inner) else None
                    ))
                    inner._fade_anim = anim
                    anim.start()
            else:
                if not _is_deleted(self._stack):
                    _fade_switch(self._stack, self.IDX_GAME)
            # … then refresh this game's lineup in the background (picks up new postings)
            gid = game.get('game_id') or game.get('id')
            if gid:
                _bg_pool.submit(_DM.refresh_lineup, gid)
            # Highlight LINEUPS in main nav — game detail is part of lineups
            for i, b in enumerate(self._main_btns):
                b.setChecked(i == self.IDX_LINEUP)
                b.setStyleSheet(_nav_btn_style(i == self.IDX_LINEUP))
        except RuntimeError:
            pass  # C++ object deleted
 
    # ── window geometry persistence ────────────────────────────
    def _restore_window_geometry(self):
        geo = self._qsettings.value("windowGeometry")
        state = self._qsettings.value("windowState")
        if geo is not None and isinstance(geo, QByteArray):
            self.restoreGeometry(geo)
            # Verify the restored position is on a visible screen
            from PyQt6.QtWidgets import QApplication
            screen = QApplication.screenAt(self.geometry().center())
            if screen is None:
                # Saved position is off-screen; reset to primary
                self.resize(1440, 860)
                primary = QApplication.primaryScreen()
                if primary:
                    ag = primary.availableGeometry()
                    self.move(ag.x() + (ag.width() - 1440) // 2,
                              ag.y() + (ag.height() - 860) // 2)
        if state is not None and isinstance(state, QByteArray):
            self.restoreState(state)
        self._geometry_restored = True

    def _save_window_geometry(self):
        self._qsettings.setValue("windowGeometry", self.saveGeometry())
        self._qsettings.setValue("windowState", self.saveState())

    def closeEvent(self, event):
        """Stop background work promptly when the window is closed."""
        self._save_window_geometry()
        _DM._shutting_down = True
        self._score_timer.stop()
        _bg_pool.shutdown(wait=False, cancel_futures=True)
        _DM.close()
        super().closeEvent(event)
        # Force-exit to avoid waiting on blocked network threads
        os._exit(0)

    def _poll_scores(self):
        """Spawn a background thread to fetch fresh schedule data."""
        if self._score_fetching:
            return
        # Only poll live scores when viewing today
        if self._selected_date != dt.date.today():
            return
        self._score_fetching = True
        self.set_status("Updating scores\u2026", timeout=0)
        notifier = self._score_notifier
        def _fetch():
            try:
                games = _DM.fetch_live_games(dt.date.today().isoformat())
            except Exception:
                games = []
            notifier.scores_ready.emit(games)
        _bg_pool.submit(_fetch)
        # Also fetch plays for expanded/live schedule cards (skip if nothing to do)
        self._poll_plays()

    @staticmethod
    def _game_changed(old, new, include_boxscore=False):
        """Return True if any visible game field differs between old and new."""
        _FIELDS = ('away_score', 'home_score', 'status', 'abstract_state', 'inning',
                   'inning_half', 'inning_state', 'live',
                   'on_first', 'on_second', 'on_third', 'outs')
        _BOX_FIELDS = ('innings_detail', 'away_hits', 'home_hits',
                       'away_errors', 'home_errors')
        for k in _FIELDS:
            if old.get(k) != new.get(k):
                return True
        if include_boxscore:
            for k in _BOX_FIELDS:
                if old.get(k) != new.get(k):
                    return True
        return False

    @staticmethod
    def _status_changed(old, new):
        return old.get('status') != new.get('status') or old.get('live') != new.get('live')

    @staticmethod
    def _merge_game(old, new):
        """Merge new game data over old, preserving live fields during API gaps."""
        merged = dict(old)
        merged.update(new)
        was_live = _is_game_live(old)
        if was_live:
            # Preserve status when new response is empty/missing
            if not (new.get('status') or '').strip() and old.get('status'):
                merged['status'] = old['status']
            # Preserve innings_detail when new is empty but old had data
            if not new.get('innings_detail') and old.get('innings_detail'):
                merged['innings_detail'] = old['innings_detail']
        return merged

    def _on_scores_fetched(self, games):
        """Update sidebar cards whose data changed; re-sort & stop timer if all done."""
        self._score_fetching = False
        if not games:
            self.set_status("Scores updated", right=f"{len(self._cards)} games")
            return
        fresh = {str(g.get('game_id') or g.get('id')): g for g in games}
        # Always keep self._games up-to-date with latest API data
        games_by_id = {}
        for i, gm in enumerate(self._games):
            gid = str(gm.get('game_id') or gm.get('id'))
            games_by_id[gid] = i
        for gid, new in fresh.items():
            if gid in games_by_id:
                idx = games_by_id[gid]
                self._games[idx] = self._merge_game(self._games[idx], new)
        any_active = False
        need_resort = False
        for i, card in enumerate(self._cards):
            gid = str(card.game.get('game_id') or card.game.get('id'))
            if gid not in fresh:
                continue
            new = fresh[gid]
            old = card.game
            if self._game_changed(old, new):
                if self._status_changed(old, new):
                    need_resort = True
                card.update_game(self._merge_game(old, new))
            st = (new.get('status') or '').lower()
            is_done = (st.startswith('final') or st.startswith('game over')
                       or st.startswith('completed') or st.startswith('postponed'))
            if not is_done:
                any_active = True
        # Update schedule page cards too
        need_resort_sched = False
        sched_page = self._stack.widget(self.IDX_MATCHUP) if self._stack.count() > self.IDX_MATCHUP else None
        sched_cards = getattr(sched_page, '_schedule_cards', []) if sched_page else []
        for card in sched_cards:
            try:
                gid = str(card.game.get('game_id') or card.game.get('id'))
                if gid not in fresh:
                    continue
                new = fresh[gid]
                old = card.game
                if self._game_changed(old, new, include_boxscore=True):
                    if self._status_changed(old, new):
                        need_resort_sched = True
                    card.update_game(self._merge_game(old, new))
            except RuntimeError:
                pass  # C++ object deleted
        if need_resort_sched:
            self._resort_schedule()
        if need_resort:
            self._resort_sidebar()
        active_count = sum(1 for c in self._cards
                           if not (c.game.get('status') or '').lower().startswith(('final', 'game over', 'completed', 'postponed')))
        any_live = any(_is_game_live(c.game) for c in self._cards)
        self.set_status("Scores updated", right=f"{active_count} active" if active_count else "All final")
        if not any_active:
            self._score_timer.stop()
        elif any_live:
            # Games in progress — poll fast
            if self._score_timer.interval() != 10_000:
                self._score_timer.setInterval(10_000)
        else:
            # Games upcoming but none live yet — slow poll
            if self._score_timer.interval() != 60_000:
                self._score_timer.setInterval(60_000)

    def _resort_sidebar(self):
        """Re-order sidebar cards in-place to reflect status changes."""
        paired = list(zip(self._games, self._cards))
        paired.sort(key=lambda p: _game_sort_key(p[0]))
        self._games = [g for g, _ in paired]
        self._cards = [c for _, c in paired]
        # Preserve which card is selected
        sel_gid = None
        if self._sel_card is not None and 0 <= self._sel_card < len(self._cards):
            sel_gid = str(self._cards[self._sel_card].game.get('game_id') or
                          self._cards[self._sel_card].game.get('id'))
        # Remove all widgets from layout (without deleting them)
        while self._card_layout.count():
            self._card_layout.takeAt(0)
        # Re-add in new order
        for i, card in enumerate(self._cards):
            card._idx = i
            self._card_layout.addWidget(card)
        self._card_layout.addStretch()
        # Restore selection index
        if sel_gid is not None:
            for i, card in enumerate(self._cards):
                gid = str(card.game.get('game_id') or card.game.get('id'))
                if gid == sel_gid:
                    self._sel_card = i
                    break

    def _resort_schedule(self):
        """Re-order schedule page cards in-place to reflect status changes."""
        sched_page = self._stack.widget(self.IDX_MATCHUP) if self._stack.count() > self.IDX_MATCHUP else None
        sched_cards = getattr(sched_page, '_schedule_cards', []) if not sched_page else getattr(sched_page, '_schedule_cards', [])
        if not sched_cards:
            return
        old_order = [id(c) for c in sched_cards]
        sched_cards.sort(key=lambda c: _game_sort_key(c.game))
        if [id(c) for c in sched_cards] == old_order:
            return  # Order unchanged, skip layout churn
        content = sched_cards[0].parentWidget()
        if content is None:
            return
        grid = content.layout()
        if grid is None:
            return
        cols = 3
        # Remove all widgets from grid without deleting
        while grid.count():
            grid.takeAt(0)
        # Re-add in sorted order
        for i, card in enumerate(sched_cards):
            card._idx = i
            grid.addWidget(card, i // cols, i % cols, Qt.AlignmentFlag.AlignTop)
        # Fill remaining cells
        rem = len(sched_cards) % cols
        if rem:
            for c in range(rem, cols):
                grid.addWidget(QWidget(), len(sched_cards) // cols, c)
        grid.setRowStretch(len(sched_cards) // cols + 1, 1)

    def _prefetch_all_plays(self):
        """Background-fetch plays for every live/final game using self._games.

        Called once at startup so data is warm before the schedule page opens,
        and can also be reused by _poll_plays when no schedule cards exist yet.
        """
        games = getattr(self, '_games', None) or GAMES
        gids = set()
        for g in games:
            gid = str(g.get('game_id') or g.get('id', ''))
            if not gid:
                continue
            st = (g.get('status') or '').lower()
            is_live = _is_game_live(g)
            is_final = ('final' in st or 'game over' in st or 'completed' in st
                        or g.get('time', '').upper() == 'FINAL')
            if is_live:
                gids.add(gid)
            elif is_final and gid not in self._plays_cache:
                gids.add(gid)
        if not gids:
            return
        notifier = self._plays_notifier
        def _fetch():
            result = {}
            for gid in gids:
                try:
                    if hasattr(_DM, 'api') and _DM.api:
                        plays, preview, count = _DM.api.fetch_game_plays(gid)
                        if plays or preview:
                            result[gid] = (plays, preview, count)
                except Exception:
                    pass
            notifier.plays_ready.emit(result)
        _bg_pool.submit(_fetch)

    def _poll_plays(self):
        """Fetch play-by-play for games that have any schedule card expanded or live."""
        sched_page = self._stack.widget(self.IDX_MATCHUP) if self._stack.count() > self.IDX_MATCHUP else None
        sched_cards = getattr(sched_page, '_schedule_cards', []) if sched_page else []
        if not sched_cards:
            # No schedule cards built yet — fall back to game-list prefetch
            self._prefetch_all_plays()
            return
        # Collect game IDs to fetch: any expanded card, or any live game
        gids = set()
        for card in sched_cards:
            gid = str(card.game.get('game_id') or card.game.get('id'))
            st = (card.game.get('status') or '').lower()
            is_live = _is_game_live(card.game)
            is_final = ('final' in st or 'game over' in st or 'completed' in st
                        or card.game.get('time', '').upper() == 'FINAL')
            if card._expanded or is_live:
                gids.add(gid)
            elif is_final and gid not in self._plays_cache:
                gids.add(gid)
        if not gids:
            return
        notifier = self._plays_notifier
        def _fetch():
            result = {}
            for gid in gids:
                try:
                    if hasattr(_DM, 'api') and _DM.api:
                        plays, preview, count = _DM.api.fetch_game_plays(gid)
                        if plays or preview:
                            result[gid] = (plays, preview, count)
                except Exception:
                    pass
            notifier.plays_ready.emit(result)
        _bg_pool.submit(_fetch)

    def _on_plays_fetched(self, plays_map):
        """Update schedule cards with fresh play-by-play data."""
        if not plays_map:
            return
        # Merge into cache (cap at 20 entries — evict oldest)
        self._plays_cache.update(plays_map)
        if len(self._plays_cache) > 20:
            excess = len(self._plays_cache) - 20
            for key in list(self._plays_cache)[:excess]:
                del self._plays_cache[key]
        sched_page = self._stack.widget(self.IDX_MATCHUP) if self._stack.count() > self.IDX_MATCHUP else None
        sched_cards = getattr(sched_page, '_schedule_cards', []) if sched_page else []
        for card in sched_cards:
            try:
                gid = str(card.game.get('game_id') or card.game.get('id'))
                if gid in plays_map:
                    plays, preview, count = plays_map[gid]
                    card.update_plays(plays, preview, count)
            except RuntimeError:
                pass

    def _fetch_odds_bg(self):
        """Background thread: fetch DraftKings odds then emit signal."""
        try:
            odds = _DM.fetch_draftkings_odds(dt.date.today().isoformat())
            self._odds_notifier.odds_ready.emit(odds)
        except Exception:
            log.exception("_fetch_odds_bg")
            self._odds_notifier.odds_ready.emit({})

    def _on_odds_fetched(self, odds):
        """Replace the schedule page widget with one that includes odds."""
        if not odds:
            self.set_status("Odds unavailable", error=True)
            return
        try:
            games = self._games if hasattr(self, '_games') else GAMES
            new_page = build_matchup_page(games=games, odds=odds,
                                          on_click=None, date=self._selected_date)
            cur = self._stack.currentIndex()
            old = self._stack.widget(self.IDX_MATCHUP)
            self._stack.removeWidget(old)
            old.deleteLater()
            self._stack.insertWidget(self.IDX_MATCHUP, new_page)
            self._stack.setCurrentIndex(cur)
            # Inject cached plays into new cards
            for card in getattr(new_page, '_schedule_cards', []):
                gid = str(card.game.get('game_id') or card.game.get('id'))
                if gid in self._plays_cache:
                    card._plays = self._plays_cache[gid]
            self.set_status("Odds loaded", right=f"{len(odds)} games")
        except Exception:
            log.exception("_on_odds_fetched")
            self.set_status("Odds failed to load", error=True)

    def _on_leaderboards_ready(self, result):
        """Replace placeholder leaderboard pages with real data."""
        try:
            hit_lbs, pit_lbs, br_lbs = result
            # If all empty, allow retry on next tab visit
            if not hit_lbs and not pit_lbs and not br_lbs:
                self._lb_fetched = False
                return
            replacements = [
                (self.IDX_HIT,   "Top Stats - Hitting",
                 "2026 season · all qualified hitters", hit_lbs),
                (self.IDX_PITCH, "Top Stats - Pitching",
                 "2026 season · starters & relievers", pit_lbs),
                (self.IDX_BR,    "Top Stats - Base Running",
                 "2026 season · speed & baserunning", br_lbs),
            ]
            cur = self._stack.currentIndex()
            for idx, title, subtitle, lbs in replacements:
                new_page = build_top_stats_page(title, subtitle,
                                                leaderboards=lbs or None)
                old = self._stack.widget(idx)
                self._stack.removeWidget(old)
                old.deleteLater()
                self._stack.insertWidget(idx, new_page)
            self._stack.setCurrentIndex(cur)
            self.set_status("Leaderboards loaded")
        except Exception:
            log.exception("_on_leaderboards_ready")
            self.set_status("Leaderboards failed", error=True)

    def _deselect_cards(self):
        for c in self._cards:
            c.set_selected(False)
        self._sel_card = None

    # ── Date selector logic ───────────────────────────────────────────
    def _update_date_arrows(self):
        """Enable/disable arrow buttons based on selected date vs today ±1."""
        today = dt.date.today()
        can_left = self._selected_date > today - dt.timedelta(days=1)
        can_right = self._selected_date < today + dt.timedelta(days=1)
        self._date_left.setEnabled(can_left)
        self._date_right.setEnabled(can_right)
        self._date_left.setIcon(self._arrow_icons['left' if can_left else 'left_dim'])
        self._date_right.setIcon(self._arrow_icons['right' if can_right else 'right_dim'])

    def _shift_date(self, delta: int):
        """Move selected date by *delta* days (-1 or +1), then reload."""
        today = dt.date.today()
        new_date = self._selected_date + dt.timedelta(days=delta)
        if new_date < today - dt.timedelta(days=1) or new_date > today + dt.timedelta(days=1):
            return
        self._selected_date = new_date
        self._date_lbl.setText(new_date.strftime("%a, %b %d"))
        self._update_date_arrows()
        self._load_date(new_date)

    def _load_date(self, date: dt.date):
        """Fetch games for *date* and refresh sidebar + schedule + leaderboards."""
        date_str = date.isoformat()
        today = dt.date.today()
        self.set_status(f"Loading {date.strftime('%b %d')}\u2026", timeout=0)

        # Fetch games: API first (includes innings_detail for box scores),
        # fall back to DB (plate_appearances) when API is unavailable.
        games = _DM.fetch_live_games(date_str)
        if not games:
            games = _DM.get_games_for_date(date_str)
        if not games:
            games = []

        self._games = sorted(games, key=_game_sort_key)

        # Clear stale play-by-play cache from previous date
        self._plays_cache.clear()

        # ── Rebuild sidebar cards ──
        while self._card_layout.count():
            item = self._card_layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()
        self._cards = []
        self._sel_card = None
        GameCard._max_height = 0
        for i, game in enumerate(self._games):
            card = GameCard(game, i, self._on_game_clicked)
            self._cards.append(card)
            self._card_layout.addWidget(card)
        # Uniform height pass
        if GameCard._max_height:
            for c in self._cards:
                c.setMinimumHeight(GameCard._max_height)
        self._card_layout.addStretch()

        # Update sidebar header date label
        try:
            self._sidebar_date_lbl.setText(date.strftime("%a, %b %d"))
        except Exception:
            pass

        # Update home page games count
        try:
            home = self._stack.widget(self.IDX_HOME)
            if hasattr(home, '_games_count_lbl'):
                home._games_count_lbl.setText(str(len(self._games)))
        except Exception:
            pass

        # ── Rebuild schedule (matchup) page — deferred to avoid UI freeze ──
        def _rebuild_matchup():
            try:
                new_page = build_matchup_page(games=self._games, odds=None, on_click=None, date=date)
                cur = self._stack.currentIndex()
                old = self._stack.widget(self.IDX_MATCHUP)
                self._stack.removeWidget(old)
                old.deleteLater()
                self._stack.insertWidget(self.IDX_MATCHUP, new_page)
                self._stack.setCurrentIndex(cur)
            except Exception:
                log.exception("_load_date matchup page")
        QTimer.singleShot(0, _rebuild_matchup)

        # ── Prefetch play-by-play for final games (schedule cards built above) ──
        QTimer.singleShot(200, self._prefetch_all_plays)

        # ── Rebuild park factors page for new date — deferred ──
        def _rebuild_park():
            try:
                new_park = ParkFactorsPage(date_str=date_str)
                cur = self._stack.currentIndex()
                old_park = self._stack.widget(self.IDX_PARK)
                self._stack.removeWidget(old_park)
                old_park.deleteLater()
                self._stack.insertWidget(self.IDX_PARK, new_park)
                self._park_page = new_park
                self._stack.setCurrentIndex(cur)
            except Exception:
                log.exception("_load_date park page")
        QTimer.singleShot(50, _rebuild_park)

        # ── Prefetch lineups for new date in background ──
        # (weather is fetched by ParkFactorsPage's _WeatherWorker above)
        try:
            self._lineup_prefetch_future = _bg_pool.submit(_DM.prefetch_lineups, self._games)
        except Exception:
            log.exception("_load_date prefetch lineups")

        # ── Reset leaderboards so they re-fetch with new date's lineups ──
        self._lb_fetched = False
        # If user is currently on a leaderboard page, re-fetch immediately
        if self._stack.currentIndex() in (self.IDX_HIT, self.IDX_PITCH, self.IDX_BR):
            self._lb_fetched = True
            _bg_pool.submit(self._fetch_lb)

        # ── Score polling: only for today ──
        if date == today:
            if not self._score_timer.isActive():
                self._score_timer.start()
        else:
            self._score_timer.stop()

        # ── Re-fetch odds for today only ──
        if date == today:
            _bg_pool.submit(self._fetch_odds_bg)

        # ── If currently viewing the lineup/game page, auto-select first game ──
        if self._games and self._stack.currentIndex() == self.IDX_GAME:
            self._on_game_clicked(0)

        count = len(self._games)
        self.set_status(f"Loaded {date.strftime('%b %d')}", right=f"{count} game{'s' if count != 1 else ''}")


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # ── File-based logging (visible even in windowed/frozen builds) ───
    import logging.handlers as _lh
    _log_path = os.path.join(_app_paths.DATA_DIR, "seam.log")
    _fh = _lh.RotatingFileHandler(_log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.getLogger().addHandler(_fh)
    logging.getLogger().setLevel(logging.INFO)
    log.info("Seam Analytics v%s starting  (frozen=%s)", _app_paths.APP_VERSION, _app_paths._frozen)

    if sys.platform.startswith("win"):
        try:
            import ctypes
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
                "SeamAnalytics.MLBStats.2026")
        except Exception:
            log.exception("SetAppUserModelID")
 
    app = QApplication(sys.argv)

    # ── Global crash handler (shows dialog instead of silent exit) ────
    def _excepthook(etype, value, tb):
        import traceback
        log.critical("Unhandled exception", exc_info=(etype, value, tb))
        detail = "".join(traceback.format_exception(etype, value, tb))
        from PyQt6.QtWidgets import QMessageBox
        dlg = QMessageBox()
        dlg.setWindowTitle("Seam Analytics — Error")
        dlg.setIcon(QMessageBox.Icon.Critical)
        dlg.setText("An unexpected error occurred.")
        dlg.setInformativeText(f"Details have been logged to:\n{_log_path}")
        dlg.setDetailedText(detail)
        dlg.exec()
    sys.excepthook = _excepthook

    app.setStyle("Fusion")
    app.setFont(QFont("Segoe UI", 11))
 
    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window,          QColor(C["bg0"]))
    pal.setColor(QPalette.ColorRole.WindowText,      QColor(C["t1"]))
    pal.setColor(QPalette.ColorRole.Base,            QColor(C["bg1"]))
    pal.setColor(QPalette.ColorRole.AlternateBase,   QColor(C["bg2"]))
    pal.setColor(QPalette.ColorRole.Text,            QColor(C["t1"]))
    pal.setColor(QPalette.ColorRole.ButtonText,      QColor(C["t1"]))
    pal.setColor(QPalette.ColorRole.Highlight,       QColor(C["ora"]))
    pal.setColor(QPalette.ColorRole.HighlightedText, QColor("#000000"))
    app.setPalette(pal)

    # ── First-run: warn if databases are missing ─────────────────────
    if not os.path.exists(_app_paths.RAW_DB) or os.path.getsize(_app_paths.RAW_DB) == 0:
        from PyQt6.QtWidgets import QMessageBox
        box = QMessageBox()
        box.setWindowTitle("Seam Analytics — First Run")
        box.setIcon(QMessageBox.Icon.Information)
        box.setText("No game database found.")
        box.setInformativeText(
            "The app will still launch and show live schedules, "
            "but historical stats and leaderboards require the database.\n\n"
            "Run the Daily Updater (SeamUpdater) to download game data, "
            "or re-run the installer to restore the pre-loaded databases."
        )
        box.exec()

    # Download missing logos in the background so the window appears instantly
    _bg_pool.submit(_init_logos)

    win = SeamStatsApp()
    # Restore saved window state (maximized, normal, etc.)
    if win._qsettings.value("windowGeometry") is not None:
        win.show()  # geometry already restored in __init__
        if win.isMaximized():  # restoreGeometry sets the maximized flag
            pass               # already correct
    else:
        win.showMaximized()    # first launch: default to maximized
    sys.exit(app.exec())