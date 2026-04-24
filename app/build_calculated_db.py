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
from typing import Dict, List, Optional

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

# Pitcher-specific windows: based on pitch count rather than game count.
# Keys ending in 'p' (e.g. 'last100p') are detected in _insert_pitching_agg.
PITCHER_WINDOWS = {
    'season': None,
    'last100p': 100,
    'last200p': 200,
    'last300p': 300,
    'last500p': 500,
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
        merged = merged.drop_duplicates(subset=dedup_cols, keep='first')
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
        merged = merged.drop_duplicates(subset=dedup_cols, keep='first')

    _statcast_pitches_cache[season] = merged
    print(f'  Loaded {len(candidates)} Statcast pitch file(s): {len(merged)} regular-season pitches')
    return merged


_CALC_TABLES = ('calculated_batting_stats', 'calculated_pitching_stats',
                'calculated_baserunning_stats', 'calculated_pitcher_baserunning_stats',
                'calculated_catcher_baserunning_stats')


def _create_calc_tables(cur):
    """Create the five calculated-stats tables (idempotent)."""
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
        pulled_air_pct REAL,
        iso REAL,
        fb_pct REAL,
        ev50 REAL,
        max_ev REAL,
        avg_bat_speed REAL,
        squared_up_rate REAL,
        blast_rate REAL,
        hard_hit_pct REAL,
        chase_rate REAL,
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
        h_per_9 REAL,
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
        swstr_pct REAL,
        gb_pct REAL,
        fp_strike_pct REAL,
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


def ensure_calc_schema(conn: sqlite3.Connection) -> bool:
    """Create/migrate calc DB schema. Returns True if schema structure changed."""

    # Create tables if they don't exist yet
    _create_calc_tables(conn.cursor())
    conn.commit()

    # ── Schema version tracking ──────────────────────────────────────
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (key TEXT PRIMARY KEY, value INTEGER)")
    row = conn.execute("SELECT value FROM schema_version WHERE key = 'version'").fetchone()
    old_version = row[0] if row else 0
    schema_changed = False

    if old_version < _app_paths.CALC_DB_SCHEMA_VERSION:
        # Schema structure changed (or no version recorded) — DROP and recreate
        # tables so new columns are picked up.  Data is rebuilt per-season when
        # build_calculated_db() processes each requested season.
        print(f'  Calc DB schema version {old_version} -> {_app_paths.CALC_DB_SCHEMA_VERSION}: recreating table structures...')
        for tbl in _CALC_TABLES:
            conn.execute(f'DROP TABLE IF EXISTS {tbl}')
        conn.commit()
        _create_calc_tables(conn.cursor())
        conn.commit()
        schema_changed = True
        conn.execute("INSERT OR REPLACE INTO schema_version (key, value) VALUES ('version', ?)",
                    (_app_paths.CALC_DB_SCHEMA_VERSION,))

    # Enable WAL mode for concurrent read/write access
    conn.execute('PRAGMA journal_mode=WAL')
    conn.commit()
    return schema_changed


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


def _last_n_pitches_game_dates_for_pitcher(conn_raw: sqlite3.Connection, season: int,
                                           player_id: int, n: int,
                                           sc_pitches: Optional[pd.DataFrame] = None) -> List[str]:
    """Return the game dates covering the pitcher's most recent N pitches thrown.
    Uses sc_pitches (pitch-level Statcast) as the authoritative source.
    Falls back to pitching_appearances COUNT(*) only if sc_pitches is unavailable."""
    if sc_pitches is not None and 'pitcher' in sc_pitches.columns and 'game_date' in sc_pitches.columns:
        pp = sc_pitches[sc_pitches['pitcher'] == player_id]
        if len(pp) > 0:
            per_game = (
                pp.groupby('game_date').size()
                .reset_index(name='pitch_count')
                .sort_values('game_date', ascending=False)
            )
            dates: List[str] = []
            total = 0
            for _, row_g in per_game.iterrows():
                dates.append(str(row_g['game_date']))
                total += int(row_g['pitch_count'])
                if total >= n:
                    break
            return dates
        return []
    # Fallback: pitching_appearances (one row per PA; COUNT gives PA count, not pitches — rough)
    cur = conn_raw.cursor()
    cur.execute("""
        SELECT game_date, COUNT(*) AS pa_count
        FROM pitching_appearances
        WHERE season = ? AND pitcher_id = ?
        GROUP BY game_date
        ORDER BY game_date DESC
    """, (season, player_id))
    rows = cur.fetchall()
    dates = []
    total = 0
    for game_date, count in rows:
        dates.append(str(game_date))
        total += count
        if total >= n:
            break
    return dates


def _insert_batting_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                        season: int, player_id: int, player_name: Optional[str], matchup: str, window: str,
                        sc_df: Optional[pd.DataFrame] = None,
                        sc_pitches: Optional[pd.DataFrame] = None):
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

    # matchup filter: JOIN with pitchers table for handedness (avoids correlated subquery)
    matchup_join = ''
    matchup_sql = ''
    if matchup == 'vs_lefty':
        matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = plate_appearances.pitcher_id'
        matchup_sql = " AND pit.p_throws = 'L'"
    elif matchup == 'vs_righty':
        matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = plate_appearances.pitcher_id'
        matchup_sql = " AND pit.p_throws = 'R'"

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
    FROM plate_appearances{matchup_join}
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
    barrel_pct = pulled_air_pct = fb_pct = ev50 = max_ev = None
    avg_bat_speed = squared_up_rate = blast_rate = hard_hit_pct = chase_rate = None

    # --- Raw DB batted-ball stats (always available, covers all games) ---
    raw_bb_sql = f"""
        SELECT
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' THEN 1 ELSE 0 END) AS bbe,
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed_angle = 6 THEN 1 ELSE 0 END) AS barrels,
            SUM(CASE WHEN bb_type = 'fly_ball' THEN 1 ELSE 0 END) AS fly_balls,
            MAX(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed IS NOT NULL THEN launch_speed END) AS max_ev,
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed IS NOT NULL AND launch_speed >= 95 THEN 1 ELSE 0 END) AS hard
        FROM plate_appearances{matchup_join}
        WHERE season = ? AND batter_id = ? {matchup_sql} {date_sql}
    """
    raw_bb = cur_raw.execute(raw_bb_sql, params).fetchone()
    if raw_bb:
        raw_bbe = raw_bb[0] or 0
        if raw_bbe > 0:
            barrel_pct = round((raw_bb[1] or 0) / raw_bbe, 3)
            fb_pct = round((raw_bb[2] or 0) / raw_bbe, 3)
            hard_hit_pct = round((raw_bb[4] or 0) / raw_bbe, 3)
        if raw_bb[3] is not None:
            max_ev = round(raw_bb[3], 1)

    # --- Raw DB EV50 and Pulled Air% — require per-row fetch ---
    raw_detail_sql = f"""
        SELECT launch_speed, hc_x, hc_y, stand, bb_type
        FROM plate_appearances{matchup_join}
        WHERE season = ? AND batter_id = ? {matchup_sql} {date_sql}
          AND bb_type IS NOT NULL AND bb_type != ''
    """
    raw_detail_rows = cur_raw.execute(raw_detail_sql, params).fetchall()
    if raw_detail_rows:
        import math as _math
        ev_vals_raw = sorted(
            [r[0] for r in raw_detail_rows if r[0] is not None],
            reverse=True
        )
        if ev_vals_raw:
            top_n = max(1, len(ev_vals_raw) // 2)
            ev50 = round(sum(ev_vals_raw[:top_n]) / top_n, 1)
        total_bbe_raw = len(raw_detail_rows)
        pulled_air_count = 0
        for r in raw_detail_rows:
            _ls, hc_x, hc_y, stand, bb_type = r
            if bb_type == 'ground_ball':
                continue
            if hc_x is None or hc_y is None or stand is None:
                continue
            denom = 198.27 - hc_y
            if denom == 0:
                continue
            raw_angle = _math.atan((hc_x - 125.42) / denom) * (180 / _math.pi) * 0.75
            adj_angle = raw_angle if stand == 'R' else -raw_angle
            if adj_angle < -17:
                pulled_air_count += 1
        if total_bbe_raw > 0:
            pulled_air_pct = round(pulled_air_count / total_bbe_raw, 3)

    # --- Raw DB bat speed, squared-up rate, blast rate, and chase rate ---
    # Blast: approximate theoretical_max_EV using avg plate_speed (91 mph fastball * 0.92 decay = 83.7 mph)
    # when sc_pitches pitch-level data is unavailable (e.g. recent call-ups, pkl lag).
    # Chase: out-of-zone (zone > 9) swings from raw DB when sc_pitches unavailable.
    _AVG_PLATE_SPEED = 83.7  # mph — used only for raw DB blast approximation
    raw_bat_sql = f"""
        SELECT
            AVG(CASE WHEN bat_speed IS NOT NULL THEN bat_speed END) AS avg_bs,
            SUM(CASE WHEN bat_speed IS NOT NULL AND launch_speed IS NOT NULL AND bb_type IS NOT NULL
                      AND launch_speed >= bat_speed * 1.2 THEN 1 ELSE 0 END) AS sq_up,
            SUM(CASE WHEN bat_speed IS NOT NULL AND launch_speed IS NOT NULL AND bb_type IS NOT NULL
                      THEN 1 ELSE 0 END) AS sq_denom,
            -- Blast fallback: (EV / (1.23*bs + 0.23*avg_plate_spd))*100 + bs >= 164
            SUM(CASE WHEN bat_speed IS NOT NULL AND launch_speed IS NOT NULL AND bb_type IS NOT NULL
                      AND (launch_speed / (1.23 * bat_speed + {_AVG_PLATE_SPEED} * 0.23)) * 100 + bat_speed >= 164
                      THEN 1 ELSE 0 END) AS blasts,
            SUM(CASE WHEN bat_speed IS NOT NULL AND launch_speed IS NOT NULL
                      AND bb_type IS NOT NULL THEN 1 ELSE 0 END) AS blast_denom,
            -- Chase fallback: swings at pitches out of zone (zone > 9)
            SUM(CASE WHEN zone IS NOT NULL AND zone > 9
                      AND description IN ('swinging_strike','swinging_strike_blocked','foul','foul_tip',
                                          'foul_bunt','hit_into_play','hit_into_play_score',
                                          'hit_into_play_no_out','missed_bunt')
                      THEN 1 ELSE 0 END) AS ooz_swings,
            SUM(CASE WHEN zone IS NOT NULL AND zone > 9 THEN 1 ELSE 0 END) AS ooz_pitches
        FROM plate_appearances{matchup_join}
        WHERE season = ? AND batter_id = ? {matchup_sql} {date_sql}
    """
    raw_bat = cur_raw.execute(raw_bat_sql, params).fetchone()
    if raw_bat:
        if raw_bat[0] is not None:
            avg_bat_speed = round(raw_bat[0], 1)
        if raw_bat[2] and raw_bat[2] > 0:
            squared_up_rate = round((raw_bat[1] or 0) / raw_bat[2], 3)
        if raw_bat[4] and raw_bat[4] > 0:
            blast_rate = round((raw_bat[3] or 0) / raw_bat[4], 3)
        if raw_bat[6] and raw_bat[6] > 0:
            chase_rate = round((raw_bat[5] or 0) / raw_bat[6], 3)

    # --- Statcast enrichment: overrides raw DB when it has MORE data ---
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
        raw_bbe_count = (raw_bb[0] or 0) if raw_bb else 0
        if len(bbe) > 0 and len(bbe) >= raw_bbe_count:
            # Statcast covers all BBE — use its richer data
            barrel_count = int((bbe['launch_speed_angle'] == 6).sum())
            barrel_pct = round(barrel_count / len(bbe), 3)
            # fb_pct: fly balls (bb_type='fly_ball') / total BBE
            fb_pct = round(int((bbe['bb_type'] == 'fly_ball').sum()) / len(bbe), 3)
            # ev50: average of top 50% exit velocities by BBE
            ev_vals = bbe['launch_speed'].dropna().sort_values(ascending=False)
            if len(ev_vals) > 0:
                top_n = max(1, len(ev_vals) // 2)
                ev50 = round(float(ev_vals.iloc[:top_n].mean()), 1)
                max_ev = round(float(ev_vals.iloc[0]), 1)
            # pulled_air_pct: airballs (not ground_ball) with spray angle < -17° / all BBE
            # Uses per-row stand field to handle switch hitters correctly
            if all(c in bbe.columns for c in ['hc_x', 'hc_y', 'stand']):
                import math as _math
                bbe_air = bbe[bbe['bb_type'] != 'ground_ball'].copy()
                bbe_air = bbe_air.dropna(subset=['hc_x', 'hc_y', 'stand'])
                if len(bbe_air) > 0:
                    dy = 198.27 - bbe_air['hc_y']
                    valid_dy = dy != 0
                    raw_angle = bbe_air.loc[valid_dy, 'hc_x'].copy()
                    raw_angle = (bbe_air.loc[valid_dy, 'hc_x'] - 125.42) / dy[valid_dy]
                    raw_angle = raw_angle.apply(_math.atan) * (180 / _math.pi) * 0.75
                    adj_angle = raw_angle.copy()
                    lhb_mask = bbe_air.loc[valid_dy, 'stand'] == 'L'
                    adj_angle[lhb_mask] = -raw_angle[lhb_mask]
                    pulled_air_pct = round(int((adj_angle < -17).sum()) / len(bbe), 3)
            # Hard Hit% from pkl
            ev_all = bbe['launch_speed'].dropna()
            if len(ev_all) > 0:
                hard_hit_pct = round(float((ev_all >= 95).sum()) / len(bbe), 3)
            # NOTE: avg_bat_speed computed below from sc_pitches (all swings, top 90%)
            # NOTE: squared_up_rate computed below from sc_pitches (pitch-level)
            # using Statcast formula: EV >= 0.8*(1.23*bat_speed + 0.23*pitch_speed)
            # with denominator = all contacts (BBE + fouls with tracked EV)

    # Chase rate + squared-up rate — both require pitch-level Statcast (sc_pitches)
    # Statcast zones: 1-9 in zone, 11-14 out of zone; chase = swing at out-of-zone pitch
    # Squared-up: EV >= 0.8*(1.23*bat_speed + 0.23*pitch_speed)
    #   denom = all contacts with tracked EV (BBE + fouls), bat_speed >= 40
    if sc_pitches is not None:
        pb = sc_pitches[sc_pitches['batter'] == player_id]
        # apply same matchup filter as sc_df
        if matchup == 'vs_lefty':
            pb = pb[pb['p_throws'] == 'L']
        elif matchup == 'vs_righty':
            pb = pb[pb['p_throws'] == 'R']
        # apply same date filter
        if dates is not None:
            pb = pb[pb['game_date'].astype(str).isin(dates)]

        # ── Avg bat speed: top 90% of tracked swings (Statcast definition) ──
        if 'bat_speed' in pb.columns:
            _bs_all = pb['bat_speed'].dropna()
            if len(_bs_all) > 0:
                _bs_10th_cut = _bs_all.quantile(0.10)
                avg_bat_speed = round(float(_bs_all[_bs_all >= _bs_10th_cut].mean()), 1)

        # ── Squared-up contact rate ──────────────────────────────────────
        # Statcast methodology:
        #   Pitch speed in formula = plate speed ≈ release_speed * 0.92 (velocity decays ~8% in flight)
        #   Theoretical max EV = 1.23 * bat_speed + 0.23 * plate_speed
        #   Denominator = competitive swings: fastest 90% of player's swings (by bat_speed)
        #                 + any swing ≥60 mph bat_speed that produced EV ≥90 mph
        #   Numerator = BBE (hit_into_play*) in that denominator set where EV ≥ 80% of theoretical max
        #   Fouls penalise the rate (count in denom) but never contribute to numerator.
        if 'bat_speed' in pb.columns and 'launch_speed' in pb.columns and 'release_speed' in pb.columns:
            _bbe_desc = frozenset(['hit_into_play', 'hit_into_play_score', 'hit_into_play_no_out'])
            # All swings with tracked bat_speed and pitch speed (fouls may lack launch_speed — that's OK)
            _tracked = pb[
                pb['bat_speed'].notna() &
                pb['release_speed'].notna()
            ]
            if len(_tracked) > 0:
                # Competitive swings: top 90% by bat_speed within this player-season
                _bs_10th = _tracked['bat_speed'].quantile(0.10)
                _fast_90 = _tracked[_tracked['bat_speed'] >= _bs_10th]
                # Include any ≥60 mph swing that produced ≥90 mph EV regardless of threshold
                if 'description' in pb.columns:
                    _bonus = _tracked[
                        (_tracked['bat_speed'] >= 60) &
                        (_tracked['launch_speed'].notna()) &
                        (_tracked['launch_speed'] >= 90) &
                        ~_tracked.index.isin(_fast_90.index)
                    ]
                    all_contacts = (
                        pd.concat([_fast_90, _bonus])
                        if len(_bonus) > 0 else _fast_90
                    )
                else:
                    all_contacts = _fast_90
                if len(all_contacts) > 0:
                    # Numerator: BBE rows in competitive set that have EV ≥ 80% of theoretical max
                    if 'description' in all_contacts.columns:
                        bbe_c = all_contacts[
                            all_contacts['description'].isin(_bbe_desc) &
                            all_contacts['launch_speed'].notna()
                        ]
                    else:
                        bbe_c = all_contacts[all_contacts['launch_speed'].notna()]
                    plate_speed = bbe_c['release_speed'] * 0.92
                    theoretical_max_ev = 1.23 * bbe_c['bat_speed'] + 0.23 * plate_speed
                    sq_count = int((bbe_c['launch_speed'] >= 0.8 * theoretical_max_ev).sum())
                    squared_up_rate = round(sq_count / len(all_contacts), 3)
                    # ── Blast rate ───────────────────────────────────────────
                    # Per-swing: sq_pct * 100 + bat_speed >= 164
                    # Same competitive-swings denominator as squared_up_rate
                    _per_swing_sq = bbe_c['launch_speed'] / theoretical_max_ev
                    _blast_scores = _per_swing_sq * 100 + bbe_c['bat_speed']
                    blast_rate = round(int((_blast_scores >= 164).sum()) / len(all_contacts), 3)

        # ── Chase rate ───────────────────────────────────────────────────
        if 'zone' in pb.columns and 'description' in pb.columns:
            _chase_swing_desc = frozenset(['swinging_strike', 'swinging_strike_blocked',
                                           'foul', 'foul_tip', 'foul_bunt',
                                           'hit_into_play', 'hit_into_play_score',
                                           'hit_into_play_no_out', 'missed_bunt'])
            ooz = pb[pb['zone'].notna() & (pb['zone'] > 9)]
            if len(ooz) > 0:
                chases = int(ooz['description'].isin(_chase_swing_desc).sum())
                chase_rate = round(chases / len(ooz), 3)

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
            barrel_pct, pulled_air_pct, iso, fb_pct, ev50, max_ev,
            avg_bat_speed, squared_up_rate, blast_rate, hard_hit_pct, chase_rate
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        season, player_id, player_name or '', matchup, window,
        pa or 0, ab or 0, hits or 0, singles or 0, doubles or 0, triples or 0, hrs or 0,
        runs or 0, rbis or 0, total_bases or 0, walks or 0, so or 0, avg, slg, obp, k_pct, bb_pct,
        barrel_pct, pulled_air_pct, iso, fb_pct, ev50, max_ev,
        avg_bat_speed, squared_up_rate, blast_rate, hard_hit_pct, chase_rate
    ))


def _insert_pitching_agg(conn_raw: sqlite3.Connection, conn_calc: sqlite3.Connection,
                         season: int, player_id: int, player_name: Optional[str], matchup: str, window: str,
                         sc_df: Optional[pd.DataFrame] = None, sc_pitches: Optional[pd.DataFrame] = None):
    """Compute pitching stats.  Counting stats (PA, K, BB, outs, etc.) always
    come from the raw DB which covers all games.  Statcast pkl data is used
    only for advanced metrics (barrel%, hard%, velo, whiff%, xOBA, etc.)."""
    cur_raw = conn_raw.cursor()
    cur_calc = conn_calc.cursor()

    # Resolve window → game dates
    dates: Optional[List[str]] = None
    if window != 'season':
        if window.endswith('p'):
            # Pitch-count window: use sc_pitches (accurate pitch-level Statcast)
            # if available, otherwise fall back to pitching_appearances.
            n = int(window[4:-1])  # strip 'last' prefix and 'p' suffix
            # Use the unified helper that combines sc_pitches counts with
            # plate_appearances-based estimation for games not in sc_pitches.
            dates = _last_n_pitches_game_dates_for_pitcher(conn_raw, season, player_id, n, sc_pitches)
        else:
            # Game-count window (e.g. 'last5' = last 5 games)
            n = int(window.replace('last', ''))
            dates = _last_n_game_dates_for_player(conn_raw, season, player_id, 'pitcher', n)
        if not dates:
            cur_calc.execute('INSERT OR REPLACE INTO calculated_pitching_stats(season, player_id, player_name, matchup, window, plate_appearances) VALUES (?, ?, ?, ?, ?, ?)',
                             (season, player_id, player_name or '', matchup, window, 0))
            return

    # ── Counting stats: always from raw DB (covers all games) ───────────
    pa = k = bb = hits_allowed = singles_allowed = doubles_allowed = 0
    triples_allowed = hrs_allowed = runs_allowed = earned_runs = outs_recorded = 0

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
      SUM(COALESCE(outs_recorded,0)),
      SUM(COALESCE(earned_runs,0)),
      SUM(COALESCE(is_sac_fly,0)),
      SUM(COALESCE(is_hbp,0)),
      SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' THEN 1 ELSE 0 END)
    FROM plate_appearances
    WHERE season = ? AND pitcher_id = ? {matchup_sql} {date_sql}
    """
    params = [season, player_id] + date_params
    cur_raw.execute(pa_sql, params)
    row = cur_raw.fetchone()
    if row:
        (pa, k, bb, hits_allowed, singles_allowed, doubles_allowed,
         triples_allowed, hrs_allowed, runs_allowed, outs_recorded,
         earned_runs, sf_allowed, hbp_allowed, bbe_allowed) = (
            int(v or 0) for v in row)

    if not pa:
        return

    innings = round(outs_recorded / 3.0, 2) if outs_recorded else 0.0

    # ── Whiff% and pitches — initialized here, filled by raw DB or pkl ──
    pitches_thrown = 0
    whiff_pct = None

    def safe_rate(num, denom, mult=1.0):
        try:
            return round(float(num) / float(denom) * mult, 3) if denom and denom > 0 else None
        except Exception:
            return None

    k_pct = safe_rate(k, pa)
    bb_pct = safe_rate(bb, pa)
    k_per_9 = safe_rate(k, innings, 9.0) if innings > 0 else None
    bb_per_9 = safe_rate(bb, innings, 9.0) if innings > 0 else None
    h_per_9 = safe_rate(hits_allowed, innings, 9.0) if innings > 0 else None
    era = safe_rate(earned_runs, innings, 9.0) if innings > 0 else None

    # ── New advanced stats: SLG-against, Hard%, xOBA, BABIP ──────────────
    # AB = PA - BB - SF - HBP  (correct MLB definition)
    ab = pa - bb - sf_allowed - hbp_allowed
    total_bases = singles_allowed + 2 * doubles_allowed + 3 * triples_allowed + 4 * hrs_allowed
    slg_against = round(total_bases / ab, 3) if ab and ab > 0 else None

    # BABIP = (H - HR) / (BBE - HR)  — BBE = batted balls in play (no Ks, walks, etc.)
    babip_denom = bbe_allowed - hrs_allowed
    babip_against = round((hits_allowed - hrs_allowed) / babip_denom, 3) if babip_denom and babip_denom > 0 else None

    # ── WHIP ────────────────────────────────────────────────────────────
    whip = round((bb + hits_allowed) / innings, 3) if innings and innings > 0 else None

    # ── Raw DB batted-ball stats (always available, covers all games) ────
    hard_pct = None
    xoba_against = None
    barrel_pct = None
    ld_pct = None
    soft_pct = None
    gb_pct = None
    swstr_pct = None
    fp_strike_pct = None

    raw_date_params: List = []
    raw_date_sql = ''
    if dates is not None:
        raw_date_sql = ' AND game_date IN ({})'.format(','.join('?' for _ in dates))
        raw_date_params = list(dates)
    raw_matchup_sql = ''
    if matchup == 'vs_lefty':
        raw_matchup_sql = " AND stand = 'L'"
    elif matchup == 'vs_righty':
        raw_matchup_sql = " AND stand = 'R'"
    raw_params = [season, player_id] + raw_date_params

    raw_bb_sql = f"""
        SELECT
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' THEN 1 ELSE 0 END) AS bbe,
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed_angle = 6 THEN 1 ELSE 0 END) AS barrels,
            AVG(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed IS NOT NULL THEN launch_speed END) AS avg_ev,
            MAX(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed IS NOT NULL THEN launch_speed END) AS max_ev,
            SUM(CASE WHEN bb_type IS NOT NULL AND bb_type != '' AND launch_speed IS NOT NULL AND launch_speed >= 95 THEN 1 ELSE 0 END) AS hard,
            SUM(CASE WHEN bb_type = 'popup' THEN 1 ELSE 0 END) AS soft,
            SUM(CASE WHEN bb_type = 'line_drive' THEN 1 ELSE 0 END) AS ld,
            SUM(CASE WHEN bb_type = 'ground_ball' THEN 1 ELSE 0 END) AS gb
        FROM plate_appearances
        WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
    """
    raw_bb = cur_raw.execute(raw_bb_sql, raw_params).fetchone()
    if raw_bb:
        raw_bbe = raw_bb[0] or 0
        if raw_bbe > 0:
            barrel_pct = round((raw_bb[1] or 0) / raw_bbe, 3)
            hard_pct = round((raw_bb[4] or 0) / raw_bbe, 3)
            soft_pct = round((raw_bb[5] or 0) / raw_bbe, 3)
            ld_pct = round((raw_bb[6] or 0) / raw_bbe, 3)
            gb_pct = round((raw_bb[7] or 0) / raw_bbe, 3)

    # ── Raw DB xOBA (estimated_woba_using_speedangle) ────────────────────
    raw_xoba_sql = f"""
        SELECT AVG(estimated_woba_using_speedangle)
        FROM plate_appearances
        WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
          AND estimated_woba_using_speedangle IS NOT NULL
    """
    raw_xoba = cur_raw.execute(raw_xoba_sql, raw_params).fetchone()
    if raw_xoba and raw_xoba[0] is not None:
        xoba_against = round(raw_xoba[0], 3)

    # ── Raw DB pitch-level: Whiff%, Contact%, Zone%, Velocity ───────────
    # swing/contact/zone/velo are enriched into plate_appearances from
    # Statcast during daily_update.  Query them as fallback when pkl files
    # don't cover recent dates.
    raw_pitch_sql = f"""
        SELECT
            SUM(COALESCE(swing, 0)) AS swings,
            SUM(COALESCE(contact, 0)) AS contacts,
            SUM(CASE WHEN zone IS NOT NULL AND zone >= 1 AND zone <= 9 THEN 1 ELSE 0 END) AS in_zone,
            SUM(CASE WHEN zone IS NOT NULL THEN 1 ELSE 0 END) AS total_zone,
            AVG(CASE WHEN release_speed IS NOT NULL THEN release_speed END) AS avg_velo,
            MAX(CASE WHEN release_speed IS NOT NULL THEN release_speed END) AS top_velo
        FROM plate_appearances
        WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
    """
    raw_pitch = cur_raw.execute(raw_pitch_sql, raw_params).fetchone()
    contact_pct = None
    zone_pct = None
    avg_velo = None
    top_velo = None
    raw_swings = 0
    if raw_pitch:
        raw_swings = raw_pitch[0] or 0
        raw_contacts = raw_pitch[1] or 0
        if raw_swings > 0:
            contact_pct = round(raw_contacts / raw_swings, 3)
        raw_total_zone = raw_pitch[3] or 0
        if raw_total_zone > 0:
            zone_pct = round((raw_pitch[2] or 0) / raw_total_zone, 3)
        if raw_pitch[4] is not None:
            avg_velo = round(raw_pitch[4], 1)
        if raw_pitch[5] is not None:
            top_velo = round(raw_pitch[5], 1)

    # ── Statcast PA-level overrides (when pkl covers more data) ─────────
    if sc_df is not None:
        pf = sc_df[sc_df['pitcher'] == player_id]
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
            raw_bbe_count = (raw_bb[0] or 0) if raw_bb else 0
            if n_bip > 0 and n_bip >= raw_bbe_count:
                ev = pf.loc[bip_mask, 'launch_speed']
                hard_pct = round(float((ev >= 95).sum()) / n_bip, 3)
                soft_pct = round(float((pf.loc[bip_mask, 'bb_type'] == 'popup').sum()) / n_bip, 3)
                ld_pct = round(float((pf.loc[bip_mask, 'bb_type'] == 'line_drive').sum()) / n_bip, 3)
                # Barrel%: launch_speed_angle == 6 is barrel classification
                if 'launch_speed_angle' in pf.columns:
                    barrel_pct = round(float((pf.loc[bip_mask, 'launch_speed_angle'] == 6).sum()) / n_bip, 3)
                if 'bb_type' in pf.columns:
                    gb_pct = round(float((pf.loc[bip_mask, 'bb_type'] == 'ground_ball').sum()) / n_bip, 3)
        # xOBA (expected wOBA using Statcast speed+angle)
        if 'estimated_woba_using_speedangle' in pf.columns:
            xwoba = pf['estimated_woba_using_speedangle'].dropna()
            if len(xwoba) > 0:
                xoba_against = round(float(xwoba.mean()), 3)

    # ── Statcast pitch-level overrides: Whiff%, Contact%, Zone% ─────────
    if sc_pitches is not None:
        pp = sc_pitches[sc_pitches['pitcher'] == player_id]
        if matchup == 'vs_lefty':
            pp = pp[pp['stand'] == 'L']
        elif matchup == 'vs_righty':
            pp = pp[pp['stand'] == 'R']
        if dates is not None:
            pp = pp[pp['game_date'].astype(str).isin(dates)]
        sc_pitches_count = len(pp)
        if sc_pitches_count > 0:
            pitches_thrown = sc_pitches_count
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
                    # Whiff from pkl (more accurate than raw DB which only has PA-level counts)
                    whiff_events = frozenset(['swinging_strike', 'swinging_strike_blocked'])
                    t_whiffs = int(pp['description'].isin(whiff_events).sum())
                    whiff_pct = round(t_whiffs / t_swings, 3)
                    # SwStr% = swinging strikes / total pitches
                    swstr_events = frozenset(['swinging_strike', 'swinging_strike_blocked'])
                    t_swstr = int(pp['description'].isin(swstr_events).sum())
                    if sc_pitches_count > 0:
                        swstr_pct = round(t_swstr / sc_pitches_count, 3)
            if 'zone' in pp.columns:
                zone_vals = pp['zone'].dropna()
                if len(zone_vals) > 0:
                    in_zone = int(((zone_vals >= 1) & (zone_vals <= 9)).sum())
                    zone_pct = round(in_zone / len(zone_vals), 3)
            if 'release_speed' in pp.columns:
                velo = pp['release_speed'].dropna()
                if len(velo) > 0:
                    avg_velo = round(float(velo.mean()), 1)
                    top_velo = round(float(velo.max()), 1)

    # ── SwStr% fallback from pitching_appearances when pkl unavailable ────
    if swstr_pct is None:
        swstr_sql = f"""
            SELECT
                SUM(CASE WHEN swing = 1 AND contact = 0 THEN 1 ELSE 0 END),
                COUNT(*)
            FROM pitching_appearances
            WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
        """
        swstr_row = cur_raw.execute(swstr_sql, raw_params).fetchone()
        if swstr_row and swstr_row[1] and swstr_row[1] > 0:
            swstr_pct = round((swstr_row[0] or 0) / swstr_row[1], 3)

    # ── First Pitch Strike% from sc_pitches (pitch-level Statcast) ────────
    # pitching_appearances.swing/zone are always NULL, so we use sc_pitches.
    # type 'S' = strike (called/swinging/foul), 'X' = in play — both count as
    # first-pitch strikes. 'B' = ball.
    if sc_pitches is not None and 'pitcher' in sc_pitches.columns and 'pitch_number' in sc_pitches.columns and 'type' in sc_pitches.columns:
        pp = sc_pitches[sc_pitches['pitcher'] == player_id]
        if matchup == 'vs_lefty':
            pp = pp[pp['stand'] == 'L'] if 'stand' in pp.columns else pp
        elif matchup == 'vs_righty':
            pp = pp[pp['stand'] == 'R'] if 'stand' in pp.columns else pp
        if dates is not None:
            pp = pp[pp['game_date'].astype(str).isin(dates)] if 'game_date' in pp.columns else pp
        fp = pp[pp['pitch_number'] == 1]
        if len(fp) > 0:
            fp_strikes = fp[fp['type'].isin(['S', 'X'])]
            fp_strike_pct = round(len(fp_strikes) / len(fp), 3)

    # ── Raw DB whiff fallback (when pkl is unavailable) ──────────────────
    if whiff_pct is None and raw_swings > 0:
        raw_whiff_sql = f"""
            SELECT SUM(COALESCE(swing, 0)) - SUM(COALESCE(contact, 0))
            FROM plate_appearances
            WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
        """
        raw_whiff = cur_raw.execute(raw_whiff_sql, raw_params).fetchone()
        if raw_whiff and raw_whiff[0] is not None:
            whiff_pct = round(raw_whiff[0] / raw_swings, 3)

    # ── Raw DB pitches_thrown fallback (when pkl is unavailable) ──────────
    if pitches_thrown == 0:
        raw_pitch_count_sql = f"""
            SELECT COUNT(*)
            FROM pitching_appearances
            WHERE season = ? AND pitcher_id = ? {raw_matchup_sql} {raw_date_sql}
        """
        raw_pc = cur_raw.execute(raw_pitch_count_sql, raw_params).fetchone()
        if raw_pc and raw_pc[0]:
            pitches_thrown = raw_pc[0]

    cur_calc.execute('''
        INSERT OR REPLACE INTO calculated_pitching_stats(
            season, player_id, player_name, matchup, window,
            plate_appearances, outs_recorded, innings_pitched, strikeouts, walks,
            hits_allowed, singles_allowed, doubles_allowed, triples_allowed,
            home_runs_allowed, runs_allowed, earned_runs,
            k_pct, bb_pct, k_per_9, bb_per_9, h_per_9, era, whiff_pct, pitches_thrown,
            slg_against, hard_pct, xoba_against, babip_against,
            whip, barrel_pct, ld_pct, soft_pct, contact_pct, zone_pct,
            avg_velo, top_velo, swstr_pct, gb_pct, fp_strike_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        season, player_id, player_name or '', matchup, window,
        pa, outs_recorded, innings, k, bb,
        hits_allowed, singles_allowed, doubles_allowed, triples_allowed,
        hrs_allowed, runs_allowed, earned_runs,
        k_pct, bb_pct, k_per_9, bb_per_9, h_per_9, era, whiff_pct, pitches_thrown,
        slg_against, hard_pct, xoba_against, babip_against,
        whip, barrel_pct, ld_pct, soft_pct, contact_pct, zone_pct,
        avg_velo, top_velo, swstr_pct, gb_pct, fp_strike_pct
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

    # matchup: JOIN with pitchers table for handedness (avoids correlated subquery)
    matchup_join = ''
    matchup_sql = ''
    matchup_params = []
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = sb.pitcher_id'
        matchup_sql = ' AND pit.p_throws = ?'
        matchup_params = [want]

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END) as attempts,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END) as stolen,
        SUM(CASE WHEN sb.event_type='caught_stealing' THEN 1 ELSE 0 END) as caught,
        SUM(CASE WHEN sb.event_type='pickoff' THEN 1 ELSE 0 END) as pickoffs,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 AND sb.base='2B' THEN 1 ELSE 0 END) as stole_2b,
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 AND sb.base='3B' THEN 1 ELSE 0 END) as stole_3b
    FROM stolen_bases sb{matchup_join} WHERE sb.season = ? AND sb.runner_id = ? {matchup_sql} {date_sql}"""

    params = [season, player_id] + matchup_params + date_params
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
    pa_matchup_join = ''
    pa_matchup_sql = ''
    if matchup == 'vs_lefty':
        pa_matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = plate_appearances.pitcher_id'
        pa_matchup_sql = " AND pit.p_throws = 'L'"
    elif matchup == 'vs_righty':
        pa_matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = plate_appearances.pitcher_id'
        pa_matchup_sql = " AND pit.p_throws = 'R'"

    cur_raw.execute(f"""SELECT
        SUM(COALESCE(is_ab,0)), SUM(COALESCE(is_hit,0)),
        SUM(COALESCE(is_walk,0)), SUM(COALESCE(is_hbp,0)),
        SUM(COALESCE(is_sac_fly,0))
    FROM plate_appearances{pa_matchup_join} WHERE season = ? AND batter_id = ? {pa_matchup_sql} {pa_date_sql}""",
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

    matchup_join = ''
    matchup_sql = ''
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = sb.pitcher_id'
        matchup_sql = f" AND pit.p_throws = '{want}'"

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='pickoff' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END)
    FROM stolen_bases sb{matchup_join} WHERE sb.season = ? AND sb.pitcher_id = ? {matchup_sql} {date_sql}"""

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

    matchup_join = ''
    matchup_sql = ''
    if matchup in ('vs_lefty', 'vs_righty'):
        want = 'L' if matchup == 'vs_lefty' else 'R'
        matchup_join = ' JOIN pitchers pit ON pit.pitcher_id = sb.pitcher_id'
        matchup_sql = f" AND pit.p_throws = '{want}'"

    sql = f"""SELECT
        SUM(CASE WHEN sb.event_type IN ('stolen_base','caught_stealing') THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='caught_stealing' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sb.event_type='stolen_base' AND sb.is_successful=1 THEN 1 ELSE 0 END)
    FROM stolen_bases sb{matchup_join} WHERE sb.season = ? AND sb.catcher_id = ? {matchup_sql} {date_sql}"""

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
    conn_raw = sqlite3.connect(RAW_DB, timeout=30)
    cur_raw = conn_raw.cursor()
    conn_calc = sqlite3.connect(CALC_DB, timeout=30)
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
    # Total player count for progress tracking
    _calc_total = len(batter_ids) + len(pitcher_ids) + len(runner_ids) + len(br_pitcher_ids) + len(br_catcher_ids)
    _calc_done = 0

    # Batters
    for batter_id in batter_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'batters')
        cur_raw.execute('SELECT batter_name FROM plate_appearances WHERE season = ? AND batter_id = ? AND batter_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, batter_id))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in WINDOWS.keys():
                _insert_batting_agg(conn_raw, conn_calc, season, batter_id, name, matchup, window, sc_df=sc_df, sc_pitches=sc_pitches)
        _calc_done += 1
    conn_calc.commit()

    # Pitchers
    for pitcher_id in pitcher_ids:
        if progress_cb:
            progress_cb('calc_player', _calc_done, _calc_total, 'pitchers')
        cur_raw.execute('SELECT pitcher_name FROM plate_appearances WHERE season = ? AND pitcher_id = ? AND pitcher_name IS NOT NULL ORDER BY game_date DESC LIMIT 1', (season, pitcher_id))
        pr = cur_raw.fetchone()
        name = pr[0] if pr else ''
        for matchup in ('all', 'vs_lefty', 'vs_righty'):
            for window in PITCHER_WINDOWS.keys():
                _insert_pitching_agg(conn_raw, conn_calc, season, pitcher_id, name, matchup, window,
                                     sc_df=sc_df, sc_pitches=sc_pitches)
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

    # Write version marker file for installer version checks
    version_file = CALC_DB + '.schema_version'
    with open(version_file, 'w') as f:
        f.write(str(_app_paths.CALC_DB_SCHEMA_VERSION))

    print(f'  Incremental calculated DB update complete for season {season}')


def build_calculated_db(seasons: Optional[List[int]] = None):
    conn_raw = sqlite3.connect(RAW_DB, timeout=30)
    cur_raw = conn_raw.cursor()

    # Get all available seasons from raw DB
    cur_raw.execute('SELECT DISTINCT season FROM plate_appearances ORDER BY season DESC')
    all_seasons = [r[0] for r in cur_raw.fetchall()]
    if not all_seasons:
        print('No seasons found in raw DB; nothing to do')
        conn_raw.close()
        return

    # decide which seasons to process
    if not seasons:
        seasons = all_seasons

    print('Seasons to process:', seasons)

    conn_calc = sqlite3.connect(CALC_DB, timeout=30)
    schema_changed = ensure_calc_schema(conn_calc)

    # If schema structure changed, rebuild ALL seasons so no season is left
    # with stale/missing table structures.
    if schema_changed and set(seasons) != set(all_seasons):
        print(f'  Schema changed — expanding rebuild to all seasons: {all_seasons}')
        seasons = all_seasons

    for season in seasons:
        print('Processing season', season)

        # Clean stale rows for this season before rebuilding
        cur_calc = conn_calc.cursor()
        _CALC_TABLES = ('calculated_batting_stats', 'calculated_pitching_stats',
                     'calculated_baserunning_stats', 'calculated_pitcher_baserunning_stats',
                     'calculated_catcher_baserunning_stats')
        for tbl in _CALC_TABLES:
            cur_calc.execute(f'DELETE FROM {tbl} WHERE season = ?', (season,))
        conn_calc.commit()

        # Load Statcast pickle for this season (detailed tracking data)
        sc_df = _load_statcast_season(season, conn_raw=conn_raw)
        if sc_df is None:
            print('  No Statcast pickle found; barrel/pull/ev/la will be NULL')

        # Load ALL pitches for whiff% calculation
        sc_pitches = _load_statcast_all_pitches(season, conn_raw=conn_raw)

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
                    _insert_batting_agg(conn_raw, conn_calc, season, b, name, matchup, window, sc_df=sc_df, sc_pitches=sc_pitches)
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
                for window in PITCHER_WINDOWS.keys():
                    _insert_pitching_agg(conn_raw, conn_calc, season, p, name, matchup, window,
                                         sc_df=sc_df, sc_pitches=sc_pitches)
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

    # Write version marker file for installer version checks
    version_file = CALC_DB + '.schema_version'
    with open(version_file, 'w') as f:
        f.write(str(_app_paths.CALC_DB_SCHEMA_VERSION))

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
