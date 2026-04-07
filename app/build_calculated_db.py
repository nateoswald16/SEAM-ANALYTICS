#!/usr/bin/env python3
"""
build_calculated_db.py

Compute calculated/filtered statistics for batters, pitchers and baserunners.

Creates `mlb_calculated.db` with pre-aggregated tables for combinations of:
 - season (e.g. 2026, 2025)
 - matchup (all / vs_lefty / vs_righty)
 - time window (season, last5, last10, last15, last30)

This is intentionally conservative and readable rather than hyper-optimized.
"""
import os
import glob
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import _app_paths

ROOT = _app_paths.APP_DIR
RAW_DB = _app_paths.RAW_DB
CALC_DB = _app_paths.CALC_DB
STATCAST_CACHE = _app_paths.STATCAST_CACHE_DIR

WINDOWS = {
    'season': None,
    'last5': 5,
    'last10': 10,
    'last15': 15,
    'last30': 30,
}

# ---------------------------------------------------------------------------
# Opening Day detection
# ---------------------------------------------------------------------------
import requests as _requests

_opening_day_cache: Dict[int, Optional[str]] = {}

SCHEDULE_URL = 'https://statsapi.mlb.com/api/v1/schedule'


def _get_opening_day(season: int, conn_raw: Optional[sqlite3.Connection] = None) -> Optional[str]:
    """Return the Opening Day date (YYYY-MM-DD) for *season*.

    Resolution order:
      1. In-memory cache
      2. ``games`` table (``MIN(game_date) WHERE game_type='R'``)
      3. ``plate_appearances`` table (``MIN(game_date)`` — already RS-only)
      4. MLB Schedule API fallback
    """
    if season in _opening_day_cache:
        return _opening_day_cache[season]

    # --- Try games table first ---
    if conn_raw is not None:
        cur = conn_raw.cursor()
        try:
            cur.execute(
                "SELECT MIN(game_date) FROM games WHERE season = ? AND game_type = 'R'",
                (season,),
            )
            row = cur.fetchone()
            if row and row[0]:
                od = str(row[0])
                _opening_day_cache[season] = od
                print(f'  Opening Day {season}: {od} (from games table)')
                return od
        except Exception:
            pass  # games table may not exist yet

        # --- Fallback: plate_appearances (already RS-only) ---
        cur.execute(
            'SELECT MIN(game_date) FROM plate_appearances WHERE season = ?',
            (season,),
        )
        row = cur.fetchone()
        if row and row[0]:
            od = str(row[0])
            _opening_day_cache[season] = od
            print(f'  Opening Day {season}: {od} (from plate_appearances)')
            return od

    # --- Fallback: MLB Schedule API ---
    try:
        resp = _requests.get(
            SCHEDULE_URL,
            params={'startDate': f'{season}-01-01', 'endDate': f'{season}-12-31',
                    'sportId': 1, 'gameTypes': 'R'},
            timeout=15,
        )
        resp.raise_for_status()
        dates = resp.json().get('dates', [])
        for dd in dates:
            for g in dd.get('games', []):
                if g.get('gameType') == 'R':
                    od = g.get('officialDate')
                    if od:
                        _opening_day_cache[season] = od
                        print(f'  Opening Day {season}: {od} (from MLB API)')
                        return od
    except Exception as e:
        print(f'  Warning: could not fetch Opening Day from API: {e}')

    return None


# ---------------------------------------------------------------------------
# Statcast pickle loader (cached per season)
# ---------------------------------------------------------------------------
_statcast_cache: Dict[int, Optional[pd.DataFrame]] = {}


def _load_statcast_season(season: int, conn_raw: Optional[sqlite3.Connection] = None) -> Optional[pd.DataFrame]:
    """Load and merge ALL Statcast pickle files for *season*, returning only
    completed-PA rows (where ``events`` is set) within the regular-season
    date range.  Opening Day is detected via ``_get_opening_day()``; the
    end date comes from the latest game in the raw DB.  Multiple overlapping
    pickles are deduplicated.  Returns ``None`` when no pickles exist.
    Result is cached in ``_statcast_cache``."""
    if season in _statcast_cache:
        return _statcast_cache[season]

    # Find ALL pickle files for this season
    pattern = os.path.join(STATCAST_CACHE, f'{season}-*_{season}-*.pkl')
    candidates = sorted(glob.glob(pattern), key=os.path.getsize, reverse=True)

    if not candidates:
        _statcast_cache[season] = None
        return None

    # Determine regular-season date bounds
    min_date = _get_opening_day(season, conn_raw)
    max_date = None
    if conn_raw is not None:
        cur = conn_raw.cursor()
        cur.execute('SELECT MAX(game_date) FROM plate_appearances WHERE season = ?', (season,))
        row = cur.fetchone()
        if row and row[0]:
            max_date = str(row[0])

    # Load and merge all pickles
    frames: List[pd.DataFrame] = []
    for pkl_path in candidates:
        try:
            df = pd.read_pickle(pkl_path)
            df = df[df['events'].notna()]
            if min_date and max_date:
                gd = df['game_date'].astype(str)
                df = df[(gd >= min_date) & (gd <= max_date)]
            if len(df) > 0:
                frames.append(df)
        except Exception as e:
            print(f'  Warning: could not load {os.path.basename(pkl_path)}: {e}')

    if not frames:
        _statcast_cache[season] = None
        return None

    # Merge and deduplicate
    merged = pd.concat(frames, ignore_index=True)
    # Deduplicate on game_pk + batter + at_bat_number (unique per PA)
    dedup_cols = ['game_pk', 'batter', 'at_bat_number']
    if all(c in merged.columns for c in dedup_cols):
        before = len(merged)
        merged = merged.drop_duplicates(subset=dedup_cols, keep='first').copy()
        dupes = before - len(merged)
        if dupes:
            print(f'  Deduplicated: removed {dupes} duplicate rows')

    date_range = ''
    if min_date and max_date:
        date_range = f' ({min_date} – {max_date})'

    _statcast_cache[season] = merged
    print(f'  Loaded {len(candidates)} Statcast pickle(s): {len(merged)} regular-season PAs{date_range}')
    return merged


# ---------------------------------------------------------------------------
# Statcast ALL-pitches loader (for whiff calculations)
# ---------------------------------------------------------------------------
_statcast_pitches_cache: Dict[int, Optional[pd.DataFrame]] = {}


def _load_statcast_all_pitches(season: int, conn_raw: Optional[sqlite3.Connection] = None) -> Optional[pd.DataFrame]:
    """Load ALL pitches (not just completed PAs) for the season.
    Used for whiff% and pitches_thrown calculations.
    Deduplicates on game_pk + at_bat_number + pitch_number."""
    if season in _statcast_pitches_cache:
        return _statcast_pitches_cache[season]

    pattern = os.path.join(STATCAST_CACHE, f'{season}-*_{season}-*.pkl')
    candidates = sorted(glob.glob(pattern), key=os.path.getsize, reverse=True)
    if not candidates:
        _statcast_pitches_cache[season] = None
        return None

    min_date = _get_opening_day(season, conn_raw)
    max_date = None
    if conn_raw is not None:
        cur = conn_raw.cursor()
        cur.execute('SELECT MAX(game_date) FROM plate_appearances WHERE season = ?', (season,))
        row = cur.fetchone()
        if row and row[0]:
            max_date = str(row[0])

    frames: List[pd.DataFrame] = []
    for pkl_path in candidates:
        try:
            df = pd.read_pickle(pkl_path)
            if min_date and max_date:
                gd = df['game_date'].astype(str)
                df = df[(gd >= min_date) & (gd <= max_date)]
            # filter to RS only
            if 'game_type' in df.columns:
                df = df[df['game_type'] == 'R']
            if len(df) > 0:
                frames.append(df)
        except Exception:
            pass

    if not frames:
        _statcast_pitches_cache[season] = None
        return None

    merged = pd.concat(frames, ignore_index=True)
    dedup_cols = ['game_pk', 'at_bat_number', 'pitch_number']
    if all(c in merged.columns for c in dedup_cols):
        merged = merged.drop_duplicates(subset=dedup_cols, keep='first').copy()

    _statcast_pitches_cache[season] = merged
    print(f'  Loaded {len(candidates)} Statcast pitch file(s): {len(merged)} regular-season pitches')
    return merged


# ---------------------------------------------------------------------------
# Pitcher per-game stats: inherited runners + baserunning outs
# ---------------------------------------------------------------------------
_pitcher_game_stats_cache: Dict[int, Dict[Tuple[int, int], Dict[str, int]]] = {}


def _precompute_pitcher_game_stats(
    sc_df: pd.DataFrame,
) -> Dict[Tuple[int, int], Dict[str, int]]:
    """Pre-compute accurate outs and runs per pitcher per game.

    Uses ``outs_when_up`` transitions for outs (captures baserunning outs
    that are invisible in event types) and base-state tracking for runs
    (properly attributes inherited runners to the pitcher who placed them).

    Returns ``{(pitcher_id, game_pk): {'outs': int, 'runs': int}}``.
    """
    result: Dict[Tuple[int, int], Dict[str, int]] = {}

    required = {'game_pk', 'inning', 'inning_topbot', 'at_bat_number',
                'pitcher', 'batter', 'outs_when_up', 'events',
                'on_1b', 'on_2b', 'on_3b', 'bat_score', 'post_bat_score'}
    if not required.issubset(sc_df.columns):
        return result

    for game_pk, game_df in sc_df.groupby('game_pk'):
        game_pk_int = int(game_pk)
        for (_inn, _topbot), hi_df in game_df.groupby(['inning', 'inning_topbot']):
            rows = list(hi_df.sort_values('at_bat_number').itertuples())
            base_owner: Dict[int, int] = {}  # runner_id → responsible pitcher

            for i, r in enumerate(rows):
                pid = int(r.pitcher)
                batter_id = int(r.batter)
                outs_start = int(r.outs_when_up) if pd.notna(r.outs_when_up) else 0
                score_before = int(r.bat_score) if pd.notna(r.bat_score) else 0
                score_after = int(r.post_bat_score) if pd.notna(r.post_bat_score) else 0
                runs_scored = max(0, score_after - score_before)
                ev = r.events

                # --- outs made on this PA ---
                if i + 1 < len(rows):
                    next_outs = int(rows[i + 1].outs_when_up) if pd.notna(rows[i + 1].outs_when_up) else 0
                    outs_made = next_outs - outs_start
                    if outs_made < 0:          # shouldn't happen in same half-inning
                        outs_made = 3 - outs_start
                else:
                    outs_made = 3 - outs_start  # last PA → inning ends

                key = (pid, game_pk_int)
                if key not in result:
                    result[key] = {'outs': 0, 'runs': 0}
                result[key]['outs'] += outs_made

                # --- base state after this PA (from next PA) ---
                on_base_after: set = set()
                if i + 1 < len(rows):
                    nr = rows[i + 1]
                    if pd.notna(nr.on_1b):
                        on_base_after.add(int(nr.on_1b))
                    if pd.notna(nr.on_2b):
                        on_base_after.add(int(nr.on_2b))
                    if pd.notna(nr.on_3b):
                        on_base_after.add(int(nr.on_3b))

                # --- attribute runs to responsible pitchers ---
                if runs_scored > 0:
                    # Prioritise runners closest to home
                    potential_scorers: List[int] = []
                    if pd.notna(r.on_3b) and int(r.on_3b) not in on_base_after:
                        potential_scorers.append(int(r.on_3b))
                    if pd.notna(r.on_2b) and int(r.on_2b) not in on_base_after:
                        potential_scorers.append(int(r.on_2b))
                    if pd.notna(r.on_1b) and int(r.on_1b) not in on_base_after:
                        potential_scorers.append(int(r.on_1b))
                    if ev == 'home_run':
                        potential_scorers.append(batter_id)

                    for rid in potential_scorers[:runs_scored]:
                        resp_pid = base_owner.get(rid, pid)
                        rkey = (resp_pid, game_pk_int)
                        if rkey not in result:
                            result[rkey] = {'outs': 0, 'runs': 0}
                        result[rkey]['runs'] += 1

                    remaining = runs_scored - min(len(potential_scorers), runs_scored)
                    if remaining > 0:
                        result[key]['runs'] += remaining

                # --- update base ownership ---
                new_owner: Dict[int, int] = {}
                for rid in on_base_after:
                    if rid in base_owner:
                        new_owner[rid] = base_owner[rid]  # keep original pitcher
                    else:
                        new_owner[rid] = pid  # new runner → current pitcher
                base_owner = new_owner

    return result


def ensure_calc_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS calculated_batting_stats (
        season INTEGER,
        player_id INTEGER,
        player_name TEXT,
        matchup TEXT,
        window TEXT,
        plate_appearances INTEGER,
        at_bats INTEGER,
        hits INTEGER,
        singles INTEGER,
        doubles INTEGER,
        triples INTEGER,
        home_runs INTEGER,
        runs INTEGER,
        rbis INTEGER,
        total_bases INTEGER,
        walks INTEGER,
        strikeouts INTEGER,
        avg REAL,
        slg REAL,
        obp REAL,
        k_pct REAL,
        bb_pct REAL,
        barrel_pct REAL,
        pull_pct REAL,
        iso REAL,
        avg_launch_angle REAL,
        avg_ev REAL,
        max_ev REAL,
        PRIMARY KEY(season, player_id, matchup, window)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS calculated_pitching_stats (
        season INTEGER,
        player_id INTEGER,
        player_name TEXT,
        matchup TEXT,
        window TEXT,
        plate_appearances INTEGER,
        outs_recorded INTEGER,
        innings_pitched REAL,
        strikeouts INTEGER,
        walks INTEGER,
        hits_allowed INTEGER,
        singles_allowed INTEGER,
        doubles_allowed INTEGER,
        triples_allowed INTEGER,
        home_runs_allowed INTEGER,
        runs_allowed INTEGER,
        earned_runs INTEGER,
        k_pct REAL,
        bb_pct REAL,
        k_per_9 REAL,
        bb_per_9 REAL,
        era REAL,
        whiff_pct REAL,
        pitches_thrown INTEGER,
        slg_against REAL,
        hard_pct REAL,
        xoba_against REAL,
        babip_against REAL,
        whip REAL,
        barrel_pct REAL,
        ld_pct REAL,
        soft_pct REAL,
        contact_pct REAL,
        zone_pct REAL,
        avg_velo REAL,
        top_velo REAL,
        PRIMARY KEY(season, player_id, matchup, window)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS calculated_baserunning_stats (
        season INTEGER,
        player_id INTEGER,
        player_name TEXT,
        matchup TEXT,
        window TEXT,
        steal_attempts INTEGER,
        stolen_bases INTEGER,
        caught_stealing INTEGER,
        pickoffs INTEGER,
        stole_2b INTEGER,
        stole_3b INTEGER,
        obp REAL,
        sprint_speed REAL,
        bolts INTEGER,
        competitive_runs INTEGER,
        bolt_pct REAL,
        PRIMARY KEY(season, player_id, matchup, window)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS calculated_pitcher_baserunning_stats (
        season INTEGER,
        player_id INTEGER,
        player_name TEXT,
        matchup TEXT,
        window TEXT,
        sb_attempts_against INTEGER,
        pickoffs INTEGER,
        sb_allowed INTEGER,
        sb_allowed_avg REAL,
        PRIMARY KEY(season, player_id, matchup, window)
    )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS calculated_catcher_baserunning_stats (
        season INTEGER,
        player_id INTEGER,
        player_name TEXT,
        matchup TEXT,
        window TEXT,
        sb_attempts_against INTEGER,
        caught_stealing INTEGER,
        sb_allowed INTEGER,
        sb_allowed_avg REAL,
        PRIMARY KEY(season, player_id, matchup, window)
    )
    ''')

    conn.commit()


def _last_n_game_dates_for_player(conn_raw: sqlite3.Connection, season: int, player_id: int,
                                 role: str, n: int) -> List[str]:
    # role: 'batter' or 'pitcher' or 'runner'
    cur = conn_raw.cursor()
    if role == 'batter':
        q = "SELECT DISTINCT game_date FROM plate_appearances WHERE season = ? AND batter_id = ? ORDER BY game_date DESC LIMIT ?"
    elif role == 'pitcher':
        q = "SELECT DISTINCT game_date FROM plate_appearances WHERE season = ? AND pitcher_id = ? ORDER BY game_date DESC LIMIT ?"
    else:
        # runner: try plate_appearances then stolen_bases
        q = "SELECT DISTINCT game_date FROM plate_appearances WHERE season = ? AND batter_id = ? ORDER BY game_date DESC LIMIT ?"
    cur.execute(q, (season, player_id, n))
    rows = [r[0] for r in cur.fetchall()]
    if rows:
        return rows
    if role == 'runner':
        cur.execute("SELECT DISTINCT game_date FROM stolen_bases WHERE season = ? AND runner_id = ? ORDER BY game_date DESC LIMIT ?", (season, player_id, n))
        return [r[0] for r in cur.fetchall()]
    return []


def _insert_batting_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                        season: int, player_id: int, player_name: Optional[str], matchup: str, window: str,
                        sc_df: Optional[pd.DataFrame] = None):
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    # date filter
    date_params: List = []
    date_sql = ''
    dates: Optional[List[str]] = None
    if window != 'season':
        n = int(window.replace('last', ''))
        dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'batter', n)
        if not dates:
            # nothing to insert (no games)
            pa = 0
            # still insert a zero-row
            cur_calc.execute('INSERT OR REPLACE INTO calculated_batting_stats(season, player_id, player_name, matchup, window, plate_appearances) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, pa))
            return
        date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
        date_params = dates

    # matchup filter: use authoritative pitcher handedness from pitchers table
    matchup_sql = ''
    if matchup == 'vs_lefty':
        matchup_sql = " AND (SELECT p_throws FROM pitchers WHERE pitcher_id = plate_appearances.pitcher_id LIMIT 1) = 'L'"
    elif matchup == 'vs_righty':
        matchup_sql = " AND (SELECT p_throws FROM pitchers WHERE pitcher_id = plate_appearances.pitcher_id LIMIT 1) = 'R'"

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
      SUM(COALESCE(rbi,0)) as rbis,
      SUM(COALESCE(total_bases,0)) as total_bases,
      SUM(COALESCE(is_walk,0)) as walks,
      SUM(COALESCE(is_strikeout,0)) as so,
      SUM(COALESCE(is_sac_fly,0)) as sf,
      SUM(COALESCE(is_hbp,0)) as hbp
    FROM plate_appearances
    WHERE season = ? AND batter_id = ? {matchup_sql} {date_sql}
    """

    params = [season, player_id] + date_params
    cur_raw.execute(sql, params)
    row = cur_raw.fetchone()
    if not row:
        return
    (pa, ab, hits, singles, doubles, triples, hrs, runs, rbis, total_bases, walks, so, sf, hbp) = row

    # -- Statcast-only metrics (barrel, pull, EV, LA) from pickle --
    # All counting stats (PA, AB, H, R, RBI, etc.) come from the MLB API
    # via the raw DB.  Statcast only enriches with detailed tracking data.
    barrel_pct = pull_pct = avg_launch_angle = avg_ev = max_ev = None

    if sc_df is not None:
        pf = sc_df[sc_df['batter'] == player_id]
        # matchup filter
        if matchup == 'vs_lefty':
            pf = pf[pf['p_throws'] == 'L']
        elif matchup == 'vs_righty':
            pf = pf[pf['p_throws'] == 'R']
        # window filter
        if dates is not None:
            pf = pf[pf['game_date'].astype(str).isin(dates)]

        # BBE subset for barrel/pull
        bbe = pf[pf['bb_type'].notna()]
        if len(bbe) > 0:
            barrel_count = int((bbe['launch_speed_angle'] == 6).sum())
            barrel_pct = round(barrel_count / len(bbe), 3)
            hcxy = bbe[bbe['hc_x'].notna() & bbe['hc_y'].notna()]
            if len(hcxy) > 0:
                # Spray angle: 0° = centre-field, negative = 3B side, positive = 1B side
                spray = np.degrees(np.arctan2(
                    hcxy['hc_x'].values - 125.42,
                    198.27 - hcxy['hc_y'].values))
                stand = hcxy['stand'].values
                # Pull zone: spray > 16° for LHH, spray < -16° for RHH
                is_pull = (((stand == 'R') & (spray < -16))
                           | (((stand == 'L') | (stand == 'S')) & (spray > 16)))
                pull_pct = round(int(is_pull.sum()) / len(hcxy), 3)
        la_vals = pf['launch_angle'].dropna()
        if len(la_vals) > 0:
            avg_launch_angle = round(float(la_vals.mean()), 1)
        ev_vals = pf['launch_speed'].dropna()
        if len(ev_vals) > 0:
            avg_ev = round(float(ev_vals.mean()), 1)
            max_ev = round(float(ev_vals.max()), 1)

    def safe_div(a, b):
        try:
            return round(float(a) / float(b), 3) if b and b != 0 else None
        except Exception:
            return None

    avg = safe_div(hits, ab)
    slg = safe_div(total_bases, ab)
    obp = None
    denom = (ab or 0) + (walks or 0) + (hbp or 0) + (sf or 0)
    if denom and denom != 0:
        obp = round(float((hits or 0) + (walks or 0) + (hbp or 0)) / denom, 3)

    k_pct = safe_div(so, pa)
    bb_pct = safe_div(walks, pa)
    iso = round((slg or 0) - (avg or 0), 3) if (slg is not None and avg is not None) else None

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_batting_stats(
            season, player_id, player_name, matchup, window,
            plate_appearances, at_bats, hits, singles, doubles, triples, home_runs,
            runs, rbis, total_bases, walks, strikeouts, avg, slg, obp, k_pct, bb_pct,
            barrel_pct, pull_pct, iso, avg_launch_angle, avg_ev, max_ev
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        season, player_id, player_name or '', matchup, window,
        pa or 0, ab or 0, hits or 0, singles or 0, doubles or 0, triples or 0, hrs or 0,
        runs or 0, rbis or 0, total_bases or 0, walks or 0, so or 0, avg, slg, obp, k_pct, bb_pct,
        barrel_pct, pull_pct, iso, avg_launch_angle, avg_ev, max_ev
    ))


def _insert_pitching_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                         season: int, player_id: int, player_name: Optional[str], matchup: str, window: str,
                         sc_df: Optional[pd.DataFrame] = None, sc_pitches: Optional[pd.DataFrame] = None,
                         sc_game_stats: Optional[Dict[Tuple[int, int], Dict[str, int]]] = None):
    """Compute pitching stats. Prefers Statcast data for accurate outs, runs,
    and whiff%.  Falls back to raw DB plate_appearances when Statcast is
    unavailable."""
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    # Resolve window → game dates
    dates: Optional[List[str]] = None
    if window != 'season':
        n = int(window.replace('last', ''))
        dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'pitcher', n)
        if not dates:
            cur_calc.execute('INSERT OR REPLACE INTO calculated_pitching_stats(season, player_id, player_name, matchup, window, plate_appearances) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, 0))
            return

    # ── Event-type → outs mapping ────────────────────────────────────────
    _SINGLE_OUT = frozenset(['strikeout', 'field_out', 'force_out',
                             'fielders_choice_out', 'sac_fly', 'sac_bunt',
                             'fielders_choice', 'other_out'])
    _DOUBLE_OUT = frozenset(['grounded_into_double_play', 'double_play',
                             'strikeout_double_play', 'sac_fly_double_play'])
    _TRIPLE_OUT = frozenset(['triple_play'])

    # ── Try Statcast events for accurate counting stats ──────────────────
    pa = k = bb = hits_allowed = singles_allowed = doubles_allowed = 0
    triples_allowed = hrs_allowed = runs_allowed = earned_runs = outs_recorded = 0
    used_statcast = False

    if sc_df is not None:
        pf = sc_df[sc_df['pitcher'] == player_id].copy()
        if matchup == 'vs_lefty':
            pf = pf[pf['stand'] == 'L']
        elif matchup == 'vs_righty':
            pf = pf[pf['stand'] == 'R']
        if dates is not None:
            pf = pf[pf['game_date'].astype(str).isin(dates)]

        if len(pf) > 0:
            used_statcast = True
            pa = len(pf)
            evs = pf['events']
            k = int(evs.isin(['strikeout', 'strikeout_double_play']).sum())
            bb = int(evs.isin(['walk', 'intent_walk']).sum())
            singles_allowed = int((evs == 'single').sum())
            doubles_allowed = int((evs == 'double').sum())
            triples_allowed = int((evs == 'triple').sum())
            hrs_allowed = int((evs == 'home_run').sum())
            hits_allowed = singles_allowed + doubles_allowed + triples_allowed + hrs_allowed

            # Accurate outs: count DPs as 2, TPs as 3
            for ev in evs:
                if ev in _SINGLE_OUT:
                    outs_recorded += 1
                elif ev in _DOUBLE_OUT:
                    outs_recorded += 2
                elif ev in _TRIPLE_OUT:
                    outs_recorded += 3

            # Runs from score changes (more accurate than RBI)
            if 'post_bat_score' in pf.columns and 'bat_score' in pf.columns:
                score_diff = pf['post_bat_score'].fillna(0).astype(int) - pf['bat_score'].fillna(0).astype(int)
                runs_allowed = int(score_diff.clip(lower=0).sum())

            # Override outs and runs with pre-computed values that properly
            # handle inherited runners and baserunning outs (all matchup only;
            # splits still use per-PA counting above as fallback).
            if matchup == 'all' and sc_game_stats:
                game_pks = set(int(g) for g in pf['game_pk'].unique())
                outs_recorded = 0
                runs_allowed = 0
                for gpk in game_pks:
                    stats = sc_game_stats.get((player_id, gpk))
                    if stats:
                        outs_recorded += stats['outs']
                        runs_allowed += stats['runs']

            # Earned runs: query raw DB since Statcast doesn't separate earned/unearned
            er_date_params: List = []
            er_date_sql = ''
            if dates is not None:
                er_date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
                er_date_params = list(dates)
            er_matchup_sql = ''
            if matchup == 'vs_lefty':
                er_matchup_sql = " AND stand = 'L'"
            elif matchup == 'vs_righty':
                er_matchup_sql = " AND stand = 'R'"
            cur_raw.execute(
                f"SELECT SUM(COALESCE(earned_runs,0)) FROM plate_appearances WHERE season = ? AND pitcher_id = ? {er_matchup_sql} {er_date_sql}",
                [season, player_id] + er_date_params)
            er_row = cur_raw.fetchone()
            if er_row and er_row[0]:
                earned_runs = int(er_row[0])
            else:
                earned_runs = runs_allowed  # fallback: treat all runs as earned

    # ── Fallback: raw DB plate_appearances ───────────────────────────────
    if not used_statcast:
        date_params: List = []
        date_sql = ''
        if dates is not None:
            date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
            date_params = list(dates)
        matchup_sql = ''
        if matchup == 'vs_lefty':
            matchup_sql = " AND stand = 'L'"
        elif matchup == 'vs_righty':
            matchup_sql = " AND stand = 'R'"

        pa_sql = f"""
        SELECT
          COUNT(*) as pa,
          SUM(COALESCE(is_strikeout,0)),
          SUM(COALESCE(is_walk,0)),
          SUM(COALESCE(is_hit,0)),
          SUM(COALESCE(is_single,0)),
          SUM(COALESCE(is_double,0)),
          SUM(COALESCE(is_triple,0)),
          SUM(COALESCE(is_home_run,0)),
          SUM(COALESCE(runs,0)),
          SUM(CASE WHEN (COALESCE(is_ab,0)=1 AND COALESCE(is_hit,0)=0)
                     OR COALESCE(is_sac_fly,0)=1
                     OR COALESCE(is_sac_bunt,0)=1 THEN 1 ELSE 0 END),
          SUM(COALESCE(earned_runs,0))
        FROM plate_appearances
        WHERE season = ? AND pitcher_id = ? {matchup_sql} {date_sql}
        """
        params = [season, player_id] + date_params
        cur_raw.execute(pa_sql, params)
        row = cur_raw.fetchone()
        if row:
            (pa, k, bb, hits_allowed, singles_allowed, doubles_allowed,
             triples_allowed, hrs_allowed, runs_allowed, outs_recorded,
             earned_runs) = (
                int(v or 0) for v in row)

    if not pa:
        return

    innings = round(outs_recorded / 3.0, 2) if outs_recorded else 0.0

    # ── Whiff% and pitches from Statcast all-pitches data ────────────────
    pitches_thrown = 0
    whiff_pct = None
    if sc_pitches is not None:
        pp = sc_pitches[sc_pitches['pitcher'] == player_id].copy()
        if matchup == 'vs_lefty':
            pp = pp[pp['stand'] == 'L']
        elif matchup == 'vs_righty':
            pp = pp[pp['stand'] == 'R']
        if dates is not None:
            pp = pp[pp['game_date'].astype(str).isin(dates)]

        pitches_thrown = len(pp)
        if pitches_thrown > 0 and 'description' in pp.columns:
            swing_events = frozenset(['swinging_strike', 'swinging_strike_blocked',
                                      'foul_tip', 'foul', 'foul_bunt',
                                      'hit_into_play', 'hit_into_play_score',
                                      'hit_into_play_no_out', 'missed_bunt'])
            whiff_events = frozenset(['swinging_strike', 'swinging_strike_blocked'])
            total_swings = int(pp['description'].isin(swing_events).sum())
            total_whiffs = int(pp['description'].isin(whiff_events).sum())
            if total_swings > 0:
                whiff_pct = round(total_whiffs / total_swings, 3)

    def safe_rate(num, denom, mult=1.0):
        try:
            return round(float(num) / float(denom) * mult, 3) if denom and denom > 0 else None
        except Exception:
            return None

    k_pct = safe_rate(k, pa)
    bb_pct = safe_rate(bb, pa)
    k_per_9 = safe_rate(k, innings, 9.0) if innings > 0 else None
    bb_per_9 = safe_rate(bb, innings, 9.0) if innings > 0 else None
    era = safe_rate(earned_runs, innings, 9.0) if innings > 0 else None

    # ── New advanced stats: SLG-against, Hard%, xOBA, BABIP ──────────────
    ab = pa - bb  # approximate AB (PA minus walks; HBP/SF not tracked separately here)
    total_bases = singles_allowed + 2 * doubles_allowed + 3 * triples_allowed + 4 * hrs_allowed
    slg_against = round(total_bases / ab, 3) if ab and ab > 0 else None

    babip_denom = ab - k - hrs_allowed  # AB - K - HR  (approximation without SF)
    babip_against = round((hits_allowed - hrs_allowed) / babip_denom, 3) if babip_denom and babip_denom > 0 else None

    # ── WHIP ────────────────────────────────────────────────────────────
    whip = round((bb + hits_allowed) / innings, 3) if innings and innings > 0 else None

    # ── Statcast PA-level: Hard%, xOBA, Barrel%, LD%, Soft% ─────────────
    hard_pct = None
    xoba_against = None
    barrel_pct = None
    ld_pct = None
    soft_pct = None
    if sc_df is not None:
        pf = sc_df[sc_df['pitcher'] == player_id].copy()
        if matchup == 'vs_lefty':
            pf = pf[pf['stand'] == 'L']
        elif matchup == 'vs_righty':
            pf = pf[pf['stand'] == 'R']
        if dates is not None:
            pf = pf[pf['game_date'].astype(str).isin(dates)]
        # Balls in play = PAs with a batted-ball type (excludes fouls)
        if 'bb_type' in pf.columns and 'launch_speed' in pf.columns:
            bip_mask = pf['bb_type'].notna()
            n_bip = int(bip_mask.sum())
            if n_bip > 0:
                ev = pf.loc[bip_mask, 'launch_speed']
                hard_pct = round(float((ev >= 95).sum()) / n_bip, 3)
                soft_pct = round(float((ev < 88).sum()) / n_bip, 3)
                ld_pct = round(float((pf.loc[bip_mask, 'bb_type'] == 'line_drive').sum()) / n_bip, 3)
                # Barrel%: launch_speed_angle == 6 is barrel classification
                if 'launch_speed_angle' in pf.columns:
                    barrel_pct = round(float((pf.loc[bip_mask, 'launch_speed_angle'] == 6).sum()) / n_bip, 3)
        # xOBA (expected wOBA using Statcast speed+angle)
        if 'estimated_woba_using_speedangle' in pf.columns:
            xwoba = pf['estimated_woba_using_speedangle'].dropna()
            if len(xwoba) > 0:
                xoba_against = round(float(xwoba.mean()), 3)

    # ── Statcast pitch-level: Contact%, Zone% ───────────────────────────
    contact_pct = None
    zone_pct = None
    if sc_pitches is not None and pitches_thrown > 0:
        # pp was already filtered above for whiff%; reuse it
        pp = sc_pitches[sc_pitches['pitcher'] == player_id].copy()
        if matchup == 'vs_lefty':
            pp = pp[pp['stand'] == 'L']
        elif matchup == 'vs_righty':
            pp = pp[pp['stand'] == 'R']
        if dates is not None:
            pp = pp[pp['game_date'].astype(str).isin(dates)]
        if len(pp) > 0:
            # Contact% = contact swings / total swings
            if 'description' in pp.columns:
                swing_ev = frozenset(['swinging_strike', 'swinging_strike_blocked',
                                      'foul_tip', 'foul', 'foul_bunt',
                                      'hit_into_play', 'hit_into_play_score',
                                      'hit_into_play_no_out', 'missed_bunt'])
                contact_ev = frozenset(['foul', 'foul_bunt', 'foul_tip',
                                        'hit_into_play', 'hit_into_play_score',
                                        'hit_into_play_no_out'])
                t_swings = int(pp['description'].isin(swing_ev).sum())
                t_contact = int(pp['description'].isin(contact_ev).sum())
                if t_swings > 0:
                    contact_pct = round(t_contact / t_swings, 3)
            # Zone% = pitches in strike zone (zone 1-9) / total pitches
            if 'zone' in pp.columns:
                zone_vals = pp['zone'].dropna()
                if len(zone_vals) > 0:
                    in_zone = int(((zone_vals >= 1) & (zone_vals <= 9)).sum())
                    zone_pct = round(in_zone / len(zone_vals), 3)

    # ── Velocity: avg and max from pitch-level release_speed ─────────────
    avg_velo = None
    top_velo = None
    if sc_pitches is not None:
        vp = sc_pitches[sc_pitches['pitcher'] == player_id].copy()
        if matchup == 'vs_lefty':
            vp = vp[vp['stand'] == 'L']
        elif matchup == 'vs_righty':
            vp = vp[vp['stand'] == 'R']
        if dates is not None:
            vp = vp[vp['game_date'].astype(str).isin(dates)]
        if 'release_speed' in vp.columns:
            velo = vp['release_speed'].dropna()
            if len(velo) > 0:
                avg_velo = round(float(velo.mean()), 1)
                top_velo = round(float(velo.max()), 1)

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_pitching_stats(
            season, player_id, player_name, matchup, window,
            plate_appearances, outs_recorded, innings_pitched, strikeouts, walks,
            hits_allowed, singles_allowed, doubles_allowed, triples_allowed,
            home_runs_allowed, runs_allowed, earned_runs,
            k_pct, bb_pct, k_per_9, bb_per_9, era, whiff_pct, pitches_thrown,
            slg_against, hard_pct, xoba_against, babip_against,
            whip, barrel_pct, ld_pct, soft_pct, contact_pct, zone_pct,
            avg_velo, top_velo
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        season, player_id, player_name or '', matchup, window,
        pa, outs_recorded, innings, k, bb,
        hits_allowed, singles_allowed, doubles_allowed, triples_allowed,
        hrs_allowed, runs_allowed, earned_runs,
        k_pct, bb_pct, k_per_9, bb_per_9, era, whiff_pct, pitches_thrown,
        slg_against, hard_pct, xoba_against, babip_against,
        whip, barrel_pct, ld_pct, soft_pct, contact_pct, zone_pct,
        avg_velo, top_velo
    ))


def _insert_baserunning_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                           season: int, player_id: int, player_name: Optional[str], matchup: str, window: str):
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    date_params: List = []
    date_sql = ''
    if window != 'season':
        n = int(window.replace('last', ''))
        # Use runner dates from stolen_bases if available, otherwise fall back to batter game dates
        dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'runner', n)
        if not dates:
            dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'batter', n)
        if not dates:
            cur_calc.execute('INSERT OR REPLACE INTO calculated_baserunning_stats(season, player_id, player_name, matchup, window, steal_attempts) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, 0))
            return
        date_sql = ' AND sb.game_date IN ({})'.format(','.join('?' for _ in dates))
        date_params = dates

    # matchup: check pitcher's handedness from pitchers table (authoritative)
    matchup_sql = ''
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_sql = f" AND (SELECT p_throws FROM pitchers WHERE pitcher_id = sb.pitcher_id LIMIT 1) = '{want}'"

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END) as attempts,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END) as stolen,
        SUM(CASE WHEN sb.event_type='caught_stealing' THEN 1 ELSE 0 END) as caught,
        SUM(CASE WHEN sb.event_type='pickoff' THEN 1 ELSE 0 END) as pickoffs,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 AND sb.base='2B' THEN 1 ELSE 0 END) as stole_2b,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 AND sb.base='3B' THEN 1 ELSE 0 END) as stole_3b
    FROM stolen_bases sb WHERE sb.season = ? AND sb.runner_id = ? {matchup_sql} {date_sql}"""

    params = [season, player_id] + date_params
    cur_raw.execute(sql, params)
    row = cur_raw.fetchone()
    if not row:
        return
    attempts, stolen, caught, pickoffs, stole_2b, stole_3b = (
        row[0] or 0, row[1] or 0, row[2] or 0, row[3] or 0, row[4] or 0, row[5] or 0)

    # OBP from plate_appearances (same matchup/window filters)
    obp = None
    pa_date_params: List = []
    pa_date_sql = ''
    if window != 'season':
        n = int(window.replace('last', ''))
        pa_dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'batter', n)
        if pa_dates:
            pa_date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in pa_dates))
            pa_date_params = pa_dates
    pa_matchup_sql = ''
    if matchup == 'vs_lefty':
        pa_matchup_sql = " AND (SELECT p_throws FROM pitchers WHERE pitcher_id = plate_appearances.pitcher_id LIMIT 1) = 'L'"
    elif matchup == 'vs_righty':
        pa_matchup_sql = " AND (SELECT p_throws FROM pitchers WHERE pitcher_id = plate_appearances.pitcher_id LIMIT 1) = 'R'"

    cur_raw.execute(f"""SELECT
        SUM(COALESCE(is_ab,0)), SUM(COALESCE(is_hit,0)),
        SUM(COALESCE(is_walk,0)), SUM(COALESCE(is_hbp,0)),
        SUM(COALESCE(is_sac_fly,0))
    FROM plate_appearances WHERE season = ? AND batter_id = ? {pa_matchup_sql} {pa_date_sql}""",
        [season, player_id] + pa_date_params)
    pa_row = cur_raw.fetchone()
    if pa_row:
        ab, h, bb, hbp, sf = (int(v or 0) for v in pa_row)
        denom = ab + bb + hbp + sf
        if denom > 0:
            obp = round(float(h + bb + hbp) / denom, 3)

    # sprint metrics from sprint_speeds table
    cur_raw.execute('SELECT sprint_speed, bolts, competitive_runs FROM sprint_speeds WHERE player_id = ? AND season = ? LIMIT 1', (player_id, season))
    srow = cur_raw.fetchone()
    sprint_speed, bolts, competitive_runs = (srow[0] if srow else None, srow[1] if srow else None, srow[2] if srow else None)
    bolt_pct = None
    try:
        if competitive_runs and competitive_runs != 0:
            bolt_pct = round(float(bolts) / float(competitive_runs), 3)
    except Exception:
        bolt_pct = None

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_baserunning_stats(
            season, player_id, player_name, matchup, window,
            steal_attempts, stolen_bases, caught_stealing, pickoffs,
            stole_2b, stole_3b, obp,
            sprint_speed, bolts, competitive_runs, bolt_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        season, player_id, player_name or '', matchup, window,
        attempts, stolen, caught, pickoffs, stole_2b, stole_3b, obp,
        sprint_speed, bolts, competitive_runs, bolt_pct
    ))


def _insert_pitcher_baserunning_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                                     season: int, player_id: int, player_name: Optional[str], matchup: str, window: str):
    """Aggregate SB stats allowed by a pitcher."""
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    date_params: List = []
    date_sql = ''
    if window != 'season':
        n = int(window.replace('last', ''))
        dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'pitcher', n)
        if not dates:
            cur_calc.execute('INSERT OR REPLACE INTO calculated_pitcher_baserunning_stats(season, player_id, player_name, matchup, window, sb_attempts_against) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, 0))
            conn_calc.commit()
            return
        date_sql = ' AND sb.game_date IN ({})'.format(','.join('?' for _ in dates))
        date_params = dates

    matchup_sql = ''
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_sql = f" AND (SELECT p_throws FROM pitchers WHERE pitcher_id = sb.pitcher_id LIMIT 1) = '{want}'"

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='pickoff' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END)
    FROM stolen_bases sb WHERE sb.season = ? AND sb.pitcher_id = ? {matchup_sql} {date_sql}"""

    cur_raw.execute(sql, [season, player_id] + date_params)
    row = cur_raw.fetchone()
    if not row:
        return
    sb_attempts = row[0] or 0
    pickoffs = row[1] or 0
    sb_allowed = row[2] or 0
    sb_allowed_avg = round(float(sb_allowed) / sb_attempts, 3) if sb_attempts > 0 else None

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_pitcher_baserunning_stats(
            season, player_id, player_name, matchup, window,
            sb_attempts_against, pickoffs, sb_allowed, sb_allowed_avg
        ) VALUES (?,?,?,?,?,?,?,?,?)
    ''', (season, player_id, player_name or '', matchup, window,
          sb_attempts, pickoffs, sb_allowed, sb_allowed_avg))


def _insert_catcher_baserunning_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                                     season: int, player_id: int, player_name: Optional[str], matchup: str, window: str):
    """Aggregate SB stats against a catcher."""
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    date_params: List = []
    date_sql = ''
    if window != 'season':
        n = int(window.replace('last', ''))
        # Catchers don't have their own game dates in PA as catchers, use stolen_bases dates
        cur_raw.execute("SELECT DISTINCT game_date FROM stolen_bases WHERE season = ? AND catcher_id = ? ORDER BY game_date DESC LIMIT ?",
                        (season, player_id, n))
        dates = [r[0] for r in cur_raw.fetchall()]
        if not dates:
            cur_calc.execute('INSERT OR REPLACE INTO calculated_catcher_baserunning_stats(season, player_id, player_name, matchup, window, sb_attempts_against) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, 0))
            conn_calc.commit()
            return
        date_sql = ' AND sb.game_date IN ({})'.format(','.join('?' for _ in dates))
        date_params = dates

    matchup_sql = ''
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_sql = f" AND (SELECT p_throws FROM pitchers WHERE pitcher_id = sb.pitcher_id LIMIT 1) = '{want}'"

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='caught_stealing' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END)
    FROM stolen_bases sb WHERE sb.season = ? AND sb.catcher_id = ? {matchup_sql} {date_sql}"""

    cur_raw.execute(sql, [season, player_id] + date_params)
    row = cur_raw.fetchone()
    if not row:
        return
    sb_attempts = row[0] or 0
    caught = row[1] or 0
    sb_allowed = row[2] or 0
    sb_allowed_avg = round(float(sb_allowed) / sb_attempts, 3) if sb_attempts > 0 else None

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_catcher_baserunning_stats(
            season, player_id, player_name, matchup, window,
            sb_attempts_against, caught_stealing, sb_allowed, sb_allowed_avg
        ) VALUES (?,?,?,?,?,?,?,?,?)
    ''', (season, player_id, player_name or '', matchup, window,
          sb_attempts, caught, sb_allowed, sb_allowed_avg))


def build_calculated_db_incremental(season: int, start_date: str, end_date: str, progress_cb=None):
    """Rebuild calculated stats only for players who appeared in the given date range.

    Much faster than a full rebuild — only touches players from newly ingested games.
    """
    conn_raw = sqlite3.connect(RAW_DB)
    cur_raw = conn_raw.cursor()
    conn_calc = sqlite3.connect(CALC_DB)
    ensure_calc_schema(conn_calc)

    # Identify affected player IDs from the date range
    cur_raw.execute(
        'SELECT DISTINCT batter_id FROM plate_appearances WHERE season = ? AND game_date BETWEEN ? AND ?',
        (season, start_date, end_date))
    batter_ids = [r[0] for r in cur_raw.fetchall()]

    cur_raw.execute(
        'SELECT DISTINCT pitcher_id FROM plate_appearances WHERE season = ? AND game_date BETWEEN ? AND ?',
        (season, start_date, end_date))
    pitcher_ids = [r[0] for r in cur_raw.fetchall()]

    cur_raw.execute(
        'SELECT DISTINCT runner_id FROM stolen_bases WHERE season = ? AND game_date BETWEEN ? AND ?',
        (season, start_date, end_date))
    runner_ids_from_sb = set(r[0] for r in cur_raw.fetchall() if r[0] is not None)
    # Include all batters so every lineup player gets OBP + sprint speed
    runner_ids = list(set(batter_ids) | runner_ids_from_sb)

    cur_raw.execute(
        'SELECT DISTINCT pitcher_id FROM stolen_bases WHERE season = ? AND pitcher_id IS NOT NULL AND game_date BETWEEN ? AND ?',
        (season, start_date, end_date))
    br_pitcher_ids = [r[0] for r in cur_raw.fetchall()]

    cur_raw.execute(
        'SELECT DISTINCT catcher_id FROM stolen_bases WHERE season = ? AND catcher_id IS NOT NULL AND game_date BETWEEN ? AND ?',
        (season, start_date, end_date))
    br_catcher_ids = [r[0] for r in cur_raw.fetchall()]

    print(f'  Incremental rebuild for {season}: {len(batter_ids)} batters, {len(pitcher_ids)} pitchers, '
          f'{len(runner_ids)} runners, {len(br_pitcher_ids)} BR pitchers, {len(br_catcher_ids)} BR catchers')

    # Load Statcast data (needed for barrel/pull/ev/la)
    sc_df = _load_statcast_season(season, conn_raw=conn_raw)
    if sc_df is None:
        print('  No Statcast pickle found; barrel/pull/ev/la will be NULL')
    sc_pitches = _load_statcast_all_pitches(season, conn_raw=conn_raw)
    sc_game_stats = None
    if sc_df is not None:
        sc_game_stats = _precompute_pitcher_game_stats(sc_df)

    # Total player count for progress tracking
    _calc_total = len(batter_ids) + len(pitcher_ids) + len(runner_ids) + len(br_pitcher_ids) + len(br_catcher_ids)
    _calc_done = 0

    # Batters
    for b in batter_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'batters')
        cur_raw.execute('SELECT batter_name FROM plate_appearances WHERE season = ? AND batter_id = ? AND batter_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, b))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_batting_agg(conn_raw, conn_calc, season, b, name, matchup, window, sc_df=sc_df)
        _calc_done += 1
    conn_calc.commit()

    # Pitchers
    for p in pitcher_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'pitchers')
        cur_raw.execute('SELECT pitcher_name FROM plate_appearances WHERE season = ? AND pitcher_id = ? AND pitcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, p))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_pitching_agg(conn_raw, conn_calc, season, p, name, matchup, window,
                                     sc_df=sc_df, sc_pitches=sc_pitches, sc_game_stats=sc_game_stats)
        _calc_done += 1
    conn_calc.commit()

    # Baserunners (all batters + any runners from stolen_bases)
    for r_id in runner_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'runners')
        cur_raw.execute('SELECT batter_name FROM plate_appearances WHERE season = ? AND batter_id = ? AND batter_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, r_id))
        pr = cur_raw.fetchone()
        if not pr:
            cur_raw.execute('SELECT runner_name FROM stolen_bases WHERE season = ? AND runner_id = ? AND runner_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, r_id))
            pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_baserunning_agg(conn_raw, conn_calc, season, r_id, name, matchup, window)
        _calc_done += 1
    conn_calc.commit()

    # Pitcher baserunning
    for pid in br_pitcher_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'BR pitchers')
        cur_raw.execute('SELECT pitcher_name FROM stolen_bases WHERE season = ? AND pitcher_id = ? AND pitcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, pid))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_pitcher_baserunning_agg(conn_raw, conn_calc, season, pid, name, matchup, window)
        _calc_done += 1
    conn_calc.commit()

    # Catcher baserunning
    for cid in br_catcher_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'BR catchers')
        cur_raw.execute('SELECT catcher_name FROM stolen_bases WHERE season = ? AND catcher_id = ? AND catcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, cid))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_catcher_baserunning_agg(conn_raw, conn_calc, season, cid, name, matchup, window)
        _calc_done += 1
    conn_calc.commit()

    if progress_cb:
        progress_cb('calc_player', _calc_total, _calc_total, 'done')
    conn_raw.close()
    conn_calc.close()
    print(f'  Incremental calculated DB update complete for season {season}')


def build_calculated_db(seasons: Optional[List[int]] = None):
    conn_raw = sqlite3.connect(RAW_DB)
    cur_raw = conn_raw.cursor()

    # decide seasons
    if not seasons:
        cur_raw.execute('SELECT DISTINCT season FROM plate_appearances ORDER BY season DESC')
        rows = [r[0] for r in cur_raw.fetchall()]
        if not rows:
            print('No seasons found in raw DB; nothing to do')
            conn_raw.close()
            return
        # prefer 2026,2025 if present otherwise last two
        want = [y for y in [2026, 2025] if y in rows]
        if not want:
            want = rows[:2]
        seasons = want

    print('Seasons to process:', seasons)

    conn_calc = sqlite3.connect(CALC_DB)
    ensure_calc_schema(conn_calc)

    for season in seasons:
        print('Processing season', season)

        # Load Statcast pickle for this season (detailed tracking data)
        sc_df = _load_statcast_season(season, conn_raw=conn_raw)
        if sc_df is None:
            print('  No Statcast pickle found; barrel/pull/ev/la will be NULL')

        # Load ALL pitches for whiff% calculation
        sc_pitches = _load_statcast_all_pitches(season, conn_raw=conn_raw)

        # Pre-compute per-pitcher-per-game outs/runs (inherited runners + base outs)
        sc_game_stats = None
        if sc_df is not None:
            sc_game_stats = _precompute_pitcher_game_stats(sc_df)
            print(f'  Pre-computed pitcher game stats: {len(sc_game_stats)} pitcher-game entries')

        # Batters
        cur_raw.execute('SELECT DISTINCT batter_id FROM plate_appearances WHERE season = ?', (season,))
        batters = [r[0] for r in cur_raw.fetchall()]
        for b in batters:
            # fetch latest name
            cur_raw.execute('SELECT batter_name FROM plate_appearances WHERE season = ? AND batter_id = ? AND batter_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, b))
            pr = cur_raw.fetchone()
            name = pr[0] if pr else ''
            for matchup in ('all', 'vs_lefty', 'vs_righty'):
                for window in WINDOWS.keys():
                    _insert_batting_agg(conn_raw, conn_calc, season, b, name, matchup, window, sc_df=sc_df)
        conn_calc.commit()
        print(f'  Batters: {len(batters)} done')

        # Pitchers
        cur_raw.execute('SELECT DISTINCT pitcher_id FROM plate_appearances WHERE season = ?', (season,))
        pitchers = [r[0] for r in cur_raw.fetchall()]
        for p in pitchers:
            cur_raw.execute('SELECT pitcher_name FROM plate_appearances WHERE season = ? AND pitcher_id = ? AND pitcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, p))
            pr = cur_raw.fetchone()
            name = pr[0] if pr else ''
            for matchup in ('all', 'vs_lefty', 'vs_righty'):
                for window in WINDOWS.keys():
                    _insert_pitching_agg(conn_raw, conn_calc, season, p, name, matchup, window,
                                         sc_df=sc_df, sc_pitches=sc_pitches, sc_game_stats=sc_game_stats)
        conn_calc.commit()
        print(f'  Pitchers: {len(pitchers)} done')

        # Baserunners — include ALL batters (not just those with steal events)
        # so that OBP and sprint speed are available for every lineup player
        runner_ids_from_sb = set()
        cur_raw.execute('SELECT DISTINCT runner_id FROM stolen_bases WHERE season = ?', (season,))
        for r in cur_raw.fetchall():
            if r[0] is not None:
                runner_ids_from_sb.add(r[0])
        # Merge with all batter IDs to cover players with 0 steal events
        all_runner_ids = set(b for b in batters) | runner_ids_from_sb
        runners = list(all_runner_ids)
        for r_id in runners:
            # Try batter name first (more reliable), then stolen_bases
            cur_raw.execute('SELECT batter_name FROM plate_appearances WHERE season = ? AND batter_id = ? AND batter_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, r_id))
            pr = cur_raw.fetchone()
            if not pr:
                cur_raw.execute('SELECT runner_name FROM stolen_bases WHERE season = ? AND runner_id = ? AND runner_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, r_id))
                pr = cur_raw.fetchone()
            name = pr[0] if pr else ''
            for matchup in ('all', 'vs_lefty', 'vs_righty'):
                for window in WINDOWS.keys():
                    _insert_baserunning_agg(conn_raw, conn_calc, season, r_id, name, matchup, window)
        conn_calc.commit()
        print(f'  Baserunners: {len(runners)} done')

        # Pitcher baserunning (SB allowed)
        cur_raw.execute('SELECT DISTINCT pitcher_id FROM stolen_bases WHERE season = ? AND pitcher_id IS NOT NULL', (season,))
        br_pitchers = [r[0] for r in cur_raw.fetchall()]
        for pid in br_pitchers:
            cur_raw.execute('SELECT pitcher_name FROM stolen_bases WHERE season = ? AND pitcher_id = ? AND pitcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, pid))
            pr = cur_raw.fetchone()
            name = pr[0] if pr else ''
            for matchup in ('all', 'vs_lefty', 'vs_righty'):
                for window in WINDOWS.keys():
                    _insert_pitcher_baserunning_agg(conn_raw, conn_calc, season, pid, name, matchup, window)
        conn_calc.commit()

        # Catcher baserunning (SB allowed / CS)
        cur_raw.execute('SELECT DISTINCT catcher_id FROM stolen_bases WHERE season = ? AND catcher_id IS NOT NULL', (season,))
        br_catchers = [r[0] for r in cur_raw.fetchall()]
        for cid in br_catchers:
            cur_raw.execute('SELECT catcher_name FROM stolen_bases WHERE season = ? AND catcher_id = ? AND catcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, cid))
            pr = cur_raw.fetchone()
            name = pr[0] if pr else ''
            for matchup in ('all', 'vs_lefty', 'vs_righty'):
                for window in WINDOWS.keys():
                    _insert_catcher_baserunning_agg(conn_raw, conn_calc, season, cid, name, matchup, window)
        conn_calc.commit()

    conn_raw.close()
    conn_calc.close()
    print('Calculated DB build complete:', CALC_DB)


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--seasons', help='Comma-separated seasons to build (e.g. 2026,2025)')
    args = p.parse_args()
    seasons = None
    if args.seasons:
        seasons = [int(s.strip()) for s in args.seasons.split(',') if s.strip()]
    build_calculated_db(seasons)


if __name__ == '__main__':
    main()
