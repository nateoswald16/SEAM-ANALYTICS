"""
MLB league-average benchmarks for stat grading.

Provides ``grade_stat(col_name, cell_str)`` which returns ``"above"``,
``"below"``, or ``None`` (average / ungraded).  Designed to be imported
by seam_app.py to colour table cells.

Cell values are expected in the display format produced by
``_build_row_for_player_info`` in seam_app.py:
    AVG/ISO  → "0.243"        (fmt3, raw fraction)
    K%/BB%   → "23.0%"        (fmt_pct, ×100 with % suffix)
    Brl%/Pull% → "6.5%"       (fmt_pct, ×100 with % suffix)
    EV/MaxEV → "88.5"         (fmt1, mph)
    AVG LA/LA → "12.5°"       (fmt_deg, degrees with ° suffix)
"""

# ---------------------------------------------------------------------------
# League-average benchmarks (approximate 2024-2025 MLB regular season)
#
#   avg       – league mean
#   threshold – half-width of the "average" band (±threshold around avg)
#   higher_is_better – True/False/None
#       True  → above band = good, below = bad
#       False → above band = bad, below = good   (e.g. K%)
#       None  → not graded
# ---------------------------------------------------------------------------
MLB_BENCHMARKS = {
    # ── Batting ──
    "AVG":    {"avg": 0.243, "threshold": 0.015, "higher_is_better": True},
    "ISO":    {"avg": 0.145, "threshold": 0.020, "higher_is_better": True},
    "K%":     {"avg": 23.0,  "threshold": 3.0,   "higher_is_better": False},
    "BB%":    {"avg": 8.5,   "threshold": 1.5,   "higher_is_better": True},
    "Brl%":   {"avg": 6.5,   "threshold": 2.0,   "higher_is_better": True},
    "EV50":   {"avg": 99.5,  "threshold": 1.4,   "higher_is_better": True},
    "MaxEV":  {"avg": 108.0, "threshold": 3.0,   "higher_is_better": True},
    "FB%":    {"avg": 29.5,  "threshold": 2.5,   "higher_is_better": True},
    "PullAir%": {"avg": 17.0,  "threshold": 2.9,   "higher_is_better": True},
    "LA":     {"avg": 12.0,  "threshold": 4.0,   "higher_is_better": None},  # generic alias
    "Hard%":  {"avg": 38.5,  "threshold": 4.0,   "higher_is_better": True},   # batter default; pitcher overridden below
    "BatSpd": {"avg": 70.6,  "threshold": 2.0,   "higher_is_better": True},
    "SqUp%":  {"avg": 26.0,  "threshold": 2.0,   "higher_is_better": True},   # avg band 24–28%; above avg ≥28%; below avg ≤24%
    "Blast%": {"avg": 10.0,  "threshold": 3.0,   "higher_is_better": True},   # avg band 7–13%; above avg ≥13%; below avg ≤7%
    "Chase%": {"avg": 30.0,  "threshold": 4.0,   "higher_is_better": False},
    # ── Pitching ──
    "ERA":      {"avg": 4.15,  "threshold": 0.60,  "higher_is_better": False},
    "Whiff%":   {"avg": 25.0,  "threshold": 4.0,   "higher_is_better": True},
    "WHIP":     {"avg": 1.29,  "threshold": 0.12,  "higher_is_better": False},
    "xOBA":     {"avg": 0.315, "threshold": 0.020, "higher_is_better": False},
    "BABIP":    {"avg": 0.300, "threshold": 0.020, "higher_is_better": False},
    "SLG":      {"avg": 0.400, "threshold": 0.030, "higher_is_better": False},
    "Zone%":    {"avg": 46.5,  "threshold": 3.0,   "higher_is_better": True},
    "Barrel%":  {"avg": 8.5,   "threshold": 2.0,   "higher_is_better": False},
    "Soft%":    {"avg": 39.0,  "threshold": 4.0,   "higher_is_better": True},
    "LD%":      {"avg": 22.0,  "threshold": 3.0,   "higher_is_better": False},
    "Contact%": {"avg": 77.0,  "threshold": 3.0,   "higher_is_better": False},
    "SwStr%":   {"avg": 10.5,  "threshold": 1.5,   "higher_is_better": True},
    "GB%":      {"avg": 44.0,  "threshold": 5.0,   "higher_is_better": True},
    "F-Strike%":{"avg": 60.0,  "threshold": 5.0,   "higher_is_better": True},
    # ── New pitcher stats ──
    "Z-Con%":   {"avg": 85.0,  "threshold": 2.0,   "higher_is_better": False},  # avg 83-87%; lower = better
    # ── Tempo / pace (seconds — lower = faster = better) ──
    "Pace/Runners On":{"avg": 19.5, "threshold": 1.5,   "higher_is_better": False},  # avg 18-21s
    # ── Secondary lead allowed by pitcher (ft gained: secondary - primary; lower = better for pitcher) ──
    "2\u00b0 Lead Allowed":{"avg": 3.0, "threshold": 0.5, "higher_is_better": False},  # avg 2.5-3.5 ft; above avg (good) ≤2.5 ft; below avg (bad) ≥3.5 ft
    # ── Catcher pop times (seconds — lower = better) ──
    "Pop 2B":   {"avg": 2.00,  "threshold": 0.05,  "higher_is_better": False},  # avg 1.95-2.05s
    "Pop 3B":   {"avg": 1.86,  "threshold": 0.04,  "higher_is_better": False},  # avg 1.82-1.90s
    "Exchange": {"avg": 0.73,  "threshold": 0.03,  "higher_is_better": False},  # avg 0.70-0.76s
    "CSAA/Throw":{"avg": 0.0,   "threshold": 0.06,  "higher_is_better": True},   # avg band -0.06 to +0.06; above avg ≥+0.07; below avg ≤-0.07
    # ── HR-Allowed table (pitching) ──
    "HR:BF%":     {"avg": 2.75,  "threshold": 0.75,  "higher_is_better": False},
    "Pull Air%":  {"avg": 17.0,  "threshold": 3.0,   "higher_is_better": False},
    "HR:P%":      {"avg": 0.75,  "threshold": 0.25,  "higher_is_better": False},
    "H:P%":       {"avg": 4.75,  "threshold": 0.75,  "higher_is_better": False},
    "H:BF%":      {"avg": 20.5,   "threshold": 2.5,   "higher_is_better": False},
    # ── Baserunning ──
    "OBP":    {"avg": 0.315, "threshold": 0.020, "higher_is_better": True},
    "Sprint": {"avg": 27.0,  "threshold": 1.0,   "higher_is_better": True},
    "Bolt%":  {"avg": 10.0,  "threshold": 4.0,   "higher_is_better": True},
    "SB%":    {"avg": 75.0,  "threshold": 8.0,   "higher_is_better": True},
    # ── Runner leads (ft) ──
    "1\u00b0 Lead": {"avg": 11.0, "threshold": 1.0, "higher_is_better": True},   # avg 10-12 ft; aggressive ≥12 ft; passive ≤10 ft
    "2\u00b0 Lead": {"avg": 2.75, "threshold": 0.75, "higher_is_better": True},  # gain 2.0-3.5 ft = avg; ≥3.5 = aggressive; ≤2.0 = passive
}

# Pitching-specific overrides (polarity flipped vs batting)
_PITCHING_BENCHMARKS = {
    "K%":     {"avg": 23.0,  "threshold": 3.0,   "higher_is_better": True},
    "BB%":    {"avg": 8.5,   "threshold": 1.5,   "higher_is_better": False},
    "Hard%":  {"avg": 40.0,  "threshold": 4.0,   "higher_is_better": False},  # pitcher Hard% allowed
    "SB%":    {"avg": 75.0,  "threshold": 8.0,   "higher_is_better": False},
    "FB%":    {"avg": 26.5,  "threshold": 2.5,   "higher_is_better": False},  # pitcher FB% allowed; avg 24-29%
    "OBP":    {"avg": 0.3175,"threshold": 0.0125,"higher_is_better": False},  # OBP against; avg .305-.330
}

# Columns that are never graded (info / counting stats)
_SKIP = {"#", "POS", "PLAYER", "PITCHER", "CATCHER", "PA", "H", "1B", "2B", "3B", "HR", "R",
         "RBI", "TB", "IP", "K", "BB", "HAND", "TEAM", "OPP",
         "SB Att", "SB", "SB Allowed", "Stole 2nd", "Stole 3rd", "Bolts",
         "Comp Runs", "CS", "Pickoffs", "BF", "Pitches"}


def _parse_cell(col: str, cell: str) -> float | None:
    """Extract a numeric value from a formatted table-cell string."""
    s = cell.strip()
    if not s:
        return None
    try:
        if s.endswith("%"):
            return float(s[:-1])
        if s.endswith("°"):
            return float(s[:-1])
        return float(s)
    except (ValueError, TypeError):
        return None


def grade_stat(col: str, cell: str, pitching: bool = False) -> str | None:
    """Return ``"above"``, ``"below"``, or ``None`` (average / ungraded).

    Parameters
    ----------
    col : str
        Column header exactly as it appears in the table columns
        (e.g. ``"AVG"``, ``"K%"``, ``"ERA"``).
    cell : str
        The formatted cell text (e.g. ``"0.267"``, ``"6.5%"``, ``"4.10"``).
    pitching : bool
        When True, use pitching-specific polarity for K% and BB%.
    """
    if col in _SKIP:
        return None
    # Use pitching overrides when applicable
    bench = None
    if pitching:
        bench = _PITCHING_BENCHMARKS.get(col)
    if bench is None:
        bench = MLB_BENCHMARKS.get(col)
    if bench is None or bench["higher_is_better"] is None:
        return None

    val = _parse_cell(col, cell)
    if val is None:
        return None

    avg = bench["avg"]
    thr = bench["threshold"]
    hib = bench["higher_is_better"]

    if val > avg + thr:
        return "above" if hib else "below"
    if val < avg - thr:
        return "below" if hib else "above"
    return None  # within average band
