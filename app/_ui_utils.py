# -*- coding: utf-8 -*-
"""
Shared UI utilities for Seam Analytics.

Houses widget factories used by multiple modules (seam_app, park_factors)
so the logic — and its style cache — lives in one place.
"""

from PyQt6.QtWidgets import QLabel
from PyQt6.QtCore import Qt

from _app_theme import C

# ── Label style cache (bounded FIFO) ────────────────────────────────
_label_style_cache: dict = {}
_LABEL_STYLE_MAX = 128


def mk_label(text, color=None, size=10, bold=False, mono=False, align=None):
    """Create a styled ``QLabel`` with transparent background.

    Results are style-cached by (color, size, bold) to avoid redundant
    stylesheet string construction.
    """
    lbl = QLabel(str(text))
    lbl.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
    key = (color or C['t1'], size, bold)
    ss = _label_style_cache.get(key)
    if ss is None:
        fam = "Segoe UI"
        w = "700" if bold else "400"
        ss = (f"color:{key[0]}; background:transparent;"
              f"font-family:'{fam}'; font-size:{size}px; font-weight:{w};")
        if len(_label_style_cache) >= _LABEL_STYLE_MAX:
            _label_style_cache.pop(next(iter(_label_style_cache)))
        _label_style_cache[key] = ss
    lbl.setStyleSheet(ss)
    if align:
        lbl.setAlignment(align)
    return lbl
