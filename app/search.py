"""
Player Search Widget for Seam Analytics.

Provides a search bar with a dropdown of matching players from the
roster CSV.  Selecting a result opens the PlayerProfileDialog.
"""

import bisect
import os
import csv as _csv
import logging
import threading
import unicodedata
from datetime import datetime

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QLabel,
    QScrollArea, QFrame, QPushButton, QApplication, QSizePolicy,
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QSize, QObject
from PyQt6.QtGui import QPixmap, QFont, QColor, QPainter, QPainterPath, QIcon, QImage

import _app_paths
import player_card as _pc
from player_card import (
    C, HEADSHOT_DIR, HEADSHOT_URL, PLAYERS_CSV, _load_roster,
    _http, _pool, _placeholder_pixmap,
    show_player_profile,
)

log = logging.getLogger("seam.search")

_FONT = "'Segoe UI','Inter',sans-serif"
_THUMB_W, _THUMB_H = 32, 48    # thumbnail vertical rectangle (2:3)
_THUMB_R = 6                    # thumbnail corner radius
_MAX_RESULTS = 25               # cap visible matches
_DEBOUNCE_MS = 200              # typing debounce


class _ThumbSignal(QObject):
    ready = pyqtSignal(int, QPixmap)       # player_id, clipped thumb


_thumb_signal = _ThumbSignal()

# Track in-flight headshot download player IDs to avoid duplicate fetches
_thumb_inflight: set[int] = set()

# In-memory thumbnail cache: player_id → already-clipped QPixmap
_thumb_cache: dict[int, QPixmap] = {}
_placeholder_thumb_pm: QPixmap | None = None  # singleton

# Search index — built once from the roster, reused every keystroke
_roster_list: list[dict] | None = None
_search_index: list[tuple] | None = None   # (name_norm, team_norm, row)
_name_keys: list[str] | None = None        # parallel to _search_index, for bisect
_index_lock = threading.Lock()


def _get_roster_list() -> list[dict]:
    """Flat list of player dicts sorted by name, loaded once."""
    global _roster_list
    if _roster_list is not None:
        return _roster_list
    roster = _load_roster()
    _roster_list = sorted(roster.values(), key=lambda r: r.get("name_full", ""))
    return _roster_list


def _thumb_pixmap(player_id: int) -> QPixmap:
    """Return a small thumbnail; uses in-memory cache, async fetch if missing."""
    if player_id in _thumb_cache:
        return _thumb_cache[player_id]

    cache_path = os.path.join(HEADSHOT_DIR, f"{player_id}.png")
    if os.path.exists(cache_path):
        src = QPixmap(cache_path)
        if not src.isNull():
            clipped = _clip_thumb(src)
            _thumb_cache[player_id] = clipped
            return clipped

    # Async fetch — return placeholder now, emit signal when ready
    if player_id in _thumb_inflight:
        return _placeholder_thumb()

    def _download():
        try:
            url = HEADSHOT_URL.format(pid=player_id)
            resp = _http.get(url, timeout=8)
            if resp.status_code == 200 and len(resp.content) > 500:
                with open(cache_path, "wb") as f:
                    f.write(resp.content)
                img = QImage()
                img.loadFromData(resp.content)
                if not img.isNull():
                    pm = QPixmap.fromImage(img)
                    clipped = _clip_thumb(pm)
                    _thumb_cache[player_id] = clipped
                    _thumb_signal.ready.emit(player_id, clipped)
        except Exception:
            log.debug("thumb fetch failed for %s", player_id)
        finally:
            _thumb_inflight.discard(player_id)

    _thumb_inflight.add(player_id)
    _pool.submit(_download)
    return _placeholder_thumb()


def _clip_thumb(pm: QPixmap, w: int = _THUMB_W, h: int = _THUMB_H, r: int = _THUMB_R) -> QPixmap:
    """Clip a pixmap into a small vertical rounded-rect thumbnail."""
    scaled = pm.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                       Qt.TransformationMode.SmoothTransformation)
    out = QPixmap(w, h)
    out.fill(QColor(0, 0, 0, 0))
    p = QPainter(out)
    p.setRenderHint(QPainter.RenderHint.Antialiasing)
    path = QPainterPath()
    path.addRoundedRect(0, 0, w, h, r, r)
    p.setClipPath(path)
    x = (scaled.width() - w) // 2
    y = (scaled.height() - h) // 2
    p.drawPixmap(-x, -y, scaled)
    p.end()
    return out


def _placeholder_thumb(w: int = _THUMB_W, h: int = _THUMB_H, r: int = _THUMB_R) -> QPixmap:
    """Tiny dark rounded-rect placeholder — singleton, painted only once."""
    global _placeholder_thumb_pm
    if _placeholder_thumb_pm is not None:
        return _placeholder_thumb_pm
    pm = QPixmap(w, h)
    pm.fill(QColor(0, 0, 0, 0))
    p = QPainter(pm)
    p.setRenderHint(QPainter.RenderHint.Antialiasing)
    p.setBrush(QColor(C["bg3"]))
    p.setPen(QColor(C["bdr"]))
    p.drawRoundedRect(1, 1, w - 2, h - 2, r, r)
    p.end()
    _placeholder_thumb_pm = pm
    return pm


def _strip_accents(text: str) -> str:
    """Remove diacritical marks so 'José' matches a search for 'Jose'."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(ch for ch in nfkd if unicodedata.category(ch) != 'Mn')


def _build_search_index() -> None:
    """Build normalized (name, team, row) tuples + bisect key list. Thread-safe."""
    global _search_index, _name_keys
    with _index_lock:
        if _search_index is not None:
            return
        roster = _get_roster_list()
        idx, keys = [], []
        for r in roster:
            name_n = _strip_accents(r.get("name_full", "")).lower()
            team_n = r.get("team", "").lower()
            idx.append((name_n, team_n, r))
            keys.append(name_n)
        _search_index = idx
        _name_keys = keys


# Pre-warm on a background thread so first keystroke is instant
_pool.submit(_build_search_index)


def _search_roster(keyword: str) -> list[dict]:
    """Return roster rows matching *keyword* — accent-insensitive, bisect + linear."""
    if _search_index is None:
        _build_search_index()
    kw = _strip_accents(keyword).lower().strip()
    if not kw:
        return []
    results: list[dict] = []
    seen: set = set()

    # Fast path: bisect finds entries whose full name starts with kw (e.g. "mike t" → Mike Trout)
    lo = bisect.bisect_left(_name_keys, kw)
    for i in range(lo, len(_name_keys)):
        if not _name_keys[i].startswith(kw):
            break
        row = _search_index[i][2]
        pid = row.get("player_id")
        if pid not in seen:
            seen.add(pid)
            results.append(row)
            if len(results) >= _MAX_RESULTS:
                return results

    # Linear: mid-word name matches + team name matches
    for name_n, team_n, row in _search_index:
        pid = row.get("player_id")
        if pid in seen:
            continue
        if kw in name_n or kw in team_n:
            seen.add(pid)
            results.append(row)
            if len(results) >= _MAX_RESULTS:
                break

    return results


# ═════════════════════════════════════════════════════════════════════
# Result row widget
# ═════════════════════════════════════════════════════════════════════

_SEASON_STR = str(datetime.now().year)


class _ResultRow(QFrame):
    """Reusable row in the search dropdown. Call set_data() to populate in-place."""
    clicked = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._row: dict = {}
        self._pid: int = 0
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.setFixedHeight(58)
        self.setStyleSheet(f"""
            _ResultRow {{
                background: {C['bg1']};
                border: none;
                border-bottom: 1px solid {C['bdr']};
                padding: 0;
            }}
            _ResultRow:hover {{
                background: {C['bg3']};
            }}
        """)

        hl = QHBoxLayout(self)
        hl.setContentsMargins(10, 6, 10, 6)
        hl.setSpacing(10)

        self._thumb_lbl = QLabel()
        self._thumb_lbl.setFixedSize(_THUMB_W, _THUMB_H)
        hl.addWidget(self._thumb_lbl)

        text_vl = QVBoxLayout()
        text_vl.setSpacing(2)
        text_vl.setContentsMargins(0, 0, 0, 0)

        self._name_lbl = QLabel()
        self._name_lbl.setStyleSheet(
            f"color:{C['t1']}; background:transparent; border:none;"
            f"font-family:{_FONT}; font-size:13px; font-weight:600;")
        text_vl.addWidget(self._name_lbl)

        self._sub_lbl = QLabel()
        self._sub_lbl.setStyleSheet(
            f"color:{C['t2']}; background:transparent; border:none;"
            f"font-family:{_FONT}; font-size:11px;")
        text_vl.addWidget(self._sub_lbl)

        hl.addLayout(text_vl)
        hl.addStretch()

        _thumb_signal.ready.connect(self._on_thumb_ready)

    def set_data(self, row: dict) -> None:
        """Update this row in-place with new player data (no widget re-creation)."""
        self._row = row
        self._pid = int(row.get("player_id", 0))
        self._name_lbl.setText(row.get("name_full", "Unknown"))
        pos = row.get("position", "")
        team = row.get("team", "")
        throws = row.get("throws", "")
        pos_type = row.get("position_type", "")
        hand = (f"{throws}HP" if throws else "") if pos_type == "Pitcher" else pos
        parts = ["MLB", _SEASON_STR]
        if hand:
            parts.append(hand)
        if team:
            parts.append(team)
        self._sub_lbl.setText("  |  ".join(parts))
        self._thumb_lbl.setPixmap(_thumb_pixmap(self._pid))

    def _on_thumb_ready(self, pid: int, pm: QPixmap) -> None:
        if pid == self._pid:
            self._thumb_lbl.setPixmap(pm)

    # ── Mouse ─────────────────────────────────────────────────────
    def mousePressEvent(self, ev):
        if ev.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self._row)
        super().mousePressEvent(ev)


# ═════════════════════════════════════════════════════════════════════
# Search bar + dropdown
# ═════════════════════════════════════════════════════════════════════

class PlayerSearchWidget(QWidget):
    """Search input with a floating dropdown of matching players."""

    def __init__(self, parent=None, compact: bool = False):
        super().__init__(parent)
        self.setFixedWidth(320 if compact else 380)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet("background:transparent;")
        self._compact = compact

        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(0)

        # ── Label (hidden in compact mode) ────────────────────────
        if not compact:
            label = QLabel("Search Player Name")
            label.setStyleSheet(
                f"color:{C['t2']}; background:transparent;"
                f"font-family:{_FONT}; font-size:10px; padding:0 0 4px 2px;")
            vl.addWidget(label)

        # ── Input row ─────────────────────────────────────────────
        input_frame = QFrame()
        if compact:
            input_frame.setFixedHeight(28)
        input_frame.setStyleSheet(f"""
            QFrame {{
                background: {C['bg2']};
                border: 1px solid {C['bdr']};
                border-radius: 6px;
            }}
            QFrame:focus-within {{
                border: 1px solid {C['ora']};
            }}
        """)
        input_hl = QHBoxLayout(input_frame)
        input_hl.setContentsMargins(10, 0, 6, 0)
        input_hl.setSpacing(4)

        _input_pad = "4px 0" if compact else "8px 0"
        _input_size = "12px" if compact else "13px"
        self._input = QLineEdit()
        self._input.setPlaceholderText("Search Player Name")
        self._input.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self._input.setStyleSheet(f"""
            QLineEdit {{
                background: transparent;
                border: none;
                color: {C['t1']};
                font-family: {_FONT};
                font-size: {_input_size};
                padding: {_input_pad};
                selection-background-color: {C['bg3']};
            }}
            QLineEdit::placeholder {{
                color: {C['t3']};
            }}
        """)
        input_hl.addWidget(self._input)

        # Clear button
        self._clear_btn = QPushButton("✕")
        self._clear_btn.setFixedSize(24, 24)
        self._clear_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._clear_btn.setStyleSheet(f"""
            QPushButton {{
                background: transparent;
                border: none;
                color: {C['t2']};
                font-size: 14px;
                font-family: {_FONT};
            }}
            QPushButton:hover {{
                color: {C['t1']};
            }}
        """)
        self._clear_btn.setVisible(False)
        self._clear_btn.clicked.connect(self._on_clear)
        input_hl.addWidget(self._clear_btn)

        vl.addWidget(input_frame)

        # ── Dropdown (floating, no focus steal) ─────────────────
        self._dropdown = QFrame(
            self,
            Qt.WindowType.Tool
            | Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.WindowDoesNotAcceptFocus,
        )
        self._dropdown.setAttribute(
            Qt.WidgetAttribute.WA_ShowWithoutActivating, True)
        self._dropdown.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._dropdown.setStyleSheet(f"""
            QFrame {{
                background: {C['bg1']};
                border: 1px solid {C['bdrl']};
                border-radius: 6px;
            }}
        """)
        self._dropdown.setVisible(False)

        drop_vl = QVBoxLayout(self._dropdown)
        drop_vl.setContentsMargins(0, 4, 0, 4)
        drop_vl.setSpacing(0)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._scroll.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self._scroll.setStyleSheet(f"""
            QScrollArea {{
                background: transparent;
                border: none;
            }}
            QScrollBar:vertical {{
                background: transparent;
                width: 6px;
                margin: 4px 2px;
            }}
            QScrollBar::handle:vertical {{
                background: {C['bg3']};
                border-radius: 3px;
                min-height: 20px;
            }}
            QScrollBar::add-line:vertical,
            QScrollBar::sub-line:vertical {{
                height: 0px;
            }}
        """)
        drop_vl.addWidget(self._scroll)

        self._results_container = QWidget()
        self._results_layout = QVBoxLayout(self._results_container)
        self._results_layout.setContentsMargins(0, 0, 0, 0)
        self._results_layout.setSpacing(0)
        self._scroll.setWidget(self._results_container)

        # Pre-build row pool — reused every keystroke, no widget churn
        self._row_pool: list[_ResultRow] = []
        for _ in range(_MAX_RESULTS):
            rw = _ResultRow()
            rw.clicked.connect(self._on_result_clicked)
            rw.setVisible(False)
            self._results_layout.addWidget(rw)
            self._row_pool.append(rw)
        self._results_layout.addStretch()

        # ── Debounce timer ──────────────────────────────────────────────
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.setInterval(_DEBOUNCE_MS)
        self._timer.timeout.connect(self._do_search)

        # ── State + Connections ─────────────────────────────────────────
        self._last_query: str = ""
        self._input.textChanged.connect(self._on_text_changed)

    # ── Slots ─────────────────────────────────────────────────────

    def _on_text_changed(self, text: str):
        self._clear_btn.setVisible(bool(text))
        kw = text.strip()
        if kw == self._last_query:
            return          # same query, skip redundant fire
        if len(kw) < 2:
            self._last_query = kw
            self._hide_dropdown()
            return
        self._timer.start()

    def _on_clear(self):
        self._input.clear()
        self._hide_dropdown()

    def _do_search(self):
        kw = self._input.text().strip()
        self._last_query = kw
        if len(kw) < 2:
            self._hide_dropdown()
            return
        results = _search_roster(kw)
        self._populate(results)

    def _populate(self, results: list[dict]):
        n = len(results)
        for i, rw in enumerate(self._row_pool):
            if i < n:
                rw.set_data(results[i])
                rw.setVisible(True)
            else:
                rw.setVisible(False)
        if n == 0:
            self._hide_dropdown()
        else:
            self._show_dropdown(n)

    def _show_dropdown(self, n: int):
        # Position below the input, matching width
        global_pos = self.mapToGlobal(self.rect().bottomLeft())
        w = 380 if self._compact else self.width()
        # Center dropdown under compact input
        if self._compact:
            global_pos.setX(global_pos.x() - (w - self.width()) // 2)
        row_h = 58
        h = min(n * row_h + 8, 6 * row_h + 8)     # max ~6 visible rows
        self._dropdown.setFixedSize(w, h)
        self._dropdown.move(global_pos)
        self._dropdown.setVisible(True)
        self._dropdown.raise_()

    def _hide_dropdown(self):
        self._dropdown.setVisible(False)

    def _on_result_clicked(self, row: dict):
        self._hide_dropdown()
        self._input.clear()

        pid = int(row.get("player_id", 0))
        name = row.get("name_full", "Unknown")
        team = row.get("team", "")
        pos = row.get("position", "")
        bats = row.get("bats", "")
        throws = row.get("throws", "")
        is_pitcher = pos == "P"
        hand = f"Throws {throws}" if is_pitcher else f"Bats {bats}"
        stand = bats

        # Get schedule games from main window for venue resolution
        games = getattr(self.window(), '_games', None)

        # Override stale CSV team with today's lineup team (ground truth)
        if pid in _pc._today_player_teams:
            team = _pc._today_player_teams[pid]

        show_player_profile({
            "id": pid,
            "name": name,
            "team": team,
            "position": pos,
            "hand": hand,
            "stand": stand,
            "is_pitcher": is_pitcher,
            "games": games,
        }, parent=self.window())


# ═════════════════════════════════════════════════════════════════════
# Standalone test
# ═════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)

    win = QWidget()
    win.setWindowTitle("Seam Analytics — Player Search")
    win.setFixedSize(440, 100)
    win.setStyleSheet(f"background:{C['bg0']};")

    vl = QVBoxLayout(win)
    vl.setContentsMargins(20, 20, 20, 20)
    search = PlayerSearchWidget()
    vl.addWidget(search)

    win.show()
    sys.exit(app.exec())
