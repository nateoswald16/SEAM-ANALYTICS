"""
Player Search Widget for Seam Analytics.

Provides a search bar with a dropdown of matching players from the
roster CSV.  Selecting a result opens the PlayerProfileDialog.
"""

import os
import csv as _csv
import logging
import unicodedata
from datetime import datetime

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QLabel,
    QScrollArea, QFrame, QPushButton, QApplication, QSizePolicy,
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QSize, QObject
from PyQt6.QtGui import QPixmap, QFont, QColor, QPainter, QPainterPath, QIcon, QImage

import _app_paths
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
_DEBOUNCE_MS = 150              # typing debounce


class _ThumbSignal(QObject):
    ready = pyqtSignal(int, QPixmap)       # player_id, clipped thumb


_thumb_signal = _ThumbSignal()

# Pre-load the roster once for fast searching
_roster_list: list[dict] | None = None


def _get_roster_list() -> list[dict]:
    """Flat list of player dicts sorted by name, loaded once."""
    global _roster_list
    if _roster_list is not None:
        return _roster_list
    roster = _load_roster()                   # dict keyed by pid
    _roster_list = sorted(roster.values(), key=lambda r: r.get("name_full", ""))
    return _roster_list


def _thumb_pixmap(player_id: int) -> QPixmap:
    """Return a small rounded-rect thumbnail, kicking off async download if missing."""
    cache_path = os.path.join(HEADSHOT_DIR, f"{player_id}.png")
    if os.path.exists(cache_path):
        src = QPixmap(cache_path)
        if not src.isNull():
            return _clip_thumb(src)

    # Async fetch — return placeholder now, emit signal when ready
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
                    _thumb_signal.ready.emit(player_id, _clip_thumb(pm))
        except Exception:
            log.debug("thumb fetch failed for %s", player_id)

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
    """Tiny dark rounded-rect placeholder."""
    pm = QPixmap(w, h)
    pm.fill(QColor(0, 0, 0, 0))
    p = QPainter(pm)
    p.setRenderHint(QPainter.RenderHint.Antialiasing)
    p.setBrush(QColor(C["bg3"]))
    p.setPen(QColor(C["bdr"]))
    p.drawRoundedRect(1, 1, w - 2, h - 2, r, r)
    p.end()
    return pm


def _strip_accents(text: str) -> str:
    """Remove diacritical marks so 'José' matches a search for 'Jose'."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(ch for ch in nfkd if unicodedata.category(ch) != 'Mn')


def _search_roster(keyword: str) -> list[dict]:
    """Return roster rows whose name or team matches *keyword* (accent-insensitive)."""
    roster = _get_roster_list()
    kw = _strip_accents(keyword).lower().strip()
    if not kw:
        return []
    results = []
    for row in roster:
        name = _strip_accents(row.get("name_full", "")).lower()
        team = row.get("team", "").lower()
        if kw in name or kw in team:
            results.append(row)
            if len(results) >= _MAX_RESULTS:
                break
    return results


# ═════════════════════════════════════════════════════════════════════
# Result row widget
# ═════════════════════════════════════════════════════════════════════

class _ResultRow(QFrame):
    """Single clickable row in the search dropdown."""
    clicked = pyqtSignal(dict)            # emits the CSV row dict

    def __init__(self, row: dict, parent=None):
        super().__init__(parent)
        self._row = row
        self._pid = int(row.get("player_id", 0))
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

        # Thumbnail
        self._thumb_lbl = QLabel()
        self._thumb_lbl.setFixedSize(_THUMB_W, _THUMB_H)
        self._thumb_lbl.setPixmap(_thumb_pixmap(self._pid))
        _thumb_signal.ready.connect(self._on_thumb_ready)
        hl.addWidget(self._thumb_lbl)

        # Text block
        text_vl = QVBoxLayout()
        text_vl.setSpacing(2)
        text_vl.setContentsMargins(0, 0, 0, 0)

        # Player name
        name_lbl = QLabel(row.get("name_full", "Unknown"))
        name_lbl.setStyleSheet(
            f"color:{C['t1']}; background:transparent; border:none;"
            f"font-family:{_FONT}; font-size:13px; font-weight:600;")
        text_vl.addWidget(name_lbl)

        # Subtitle: MLB | season | position | team
        season = str(datetime.now().year)
        pos = row.get("position", "")
        team = row.get("team", "")
        throws = row.get("throws", "")
        hand = ""
        pos_type = row.get("position_type", "")
        if pos_type == "Pitcher":
            hand = f"{throws}HP" if throws else ""
        else:
            hand = pos
        parts = ["MLB", season]
        if hand:
            parts.append(hand)
        if team:
            parts.append(team)
        sub_lbl = QLabel("  |  ".join(parts))
        sub_lbl.setStyleSheet(
            f"color:{C['t2']}; background:transparent; border:none;"
            f"font-family:{_FONT}; font-size:11px;")
        text_vl.addWidget(sub_lbl)

        hl.addLayout(text_vl)
        hl.addStretch()

    def _on_thumb_ready(self, pid: int, pm: QPixmap):
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
        self._results_layout.addStretch()
        self._scroll.setWidget(self._results_container)

        # ── Debounce timer ────────────────────────────────────────
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.setInterval(_DEBOUNCE_MS)
        self._timer.timeout.connect(self._do_search)

        # ── Connections ───────────────────────────────────────────
        self._input.textChanged.connect(self._on_text_changed)

    # ── Slots ─────────────────────────────────────────────────────

    def _on_text_changed(self, text: str):
        self._clear_btn.setVisible(bool(text))
        self._timer.start()

    def _on_clear(self):
        self._input.clear()
        self._hide_dropdown()

    def _do_search(self):
        kw = self._input.text().strip()
        if len(kw) < 2:
            self._hide_dropdown()
            return
        results = _search_roster(kw)
        self._populate(results)

    def _populate(self, results: list[dict]):
        # Clear old results
        layout = self._results_layout
        while layout.count() > 1:            # keep trailing stretch
            item = layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

        if not results:
            self._hide_dropdown()
            return

        for row in results:
            rw = _ResultRow(row)
            rw.clicked.connect(self._on_result_clicked)
            layout.insertWidget(layout.count() - 1, rw)  # before stretch

        self._show_dropdown(len(results))

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
