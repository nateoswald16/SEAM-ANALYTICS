#!/usr/bin/env python3
"""
build_raw_db.py

Creates/updates the raw SQLite DB and ingests play-by-play + statcast.

Features:
- Creates tables from `database_schema_.py` (and adds missing columns via ALTER TABLE).
- Fetches completed regular-season games (by date range or --year mode).
- Inserts plate_appearances rows parsed from MLB game feed.
- Enriches PA rows with statcast fields (using mapping JSON) and inserts pitching_appearances rows.

Usage:
  python build_raw_db.py --start 2026-03-25 --end 2026-03-27 --season 2026
  python build_raw_db.py --year 2022
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import json
import os
import re
import sqlite3
import time
from typing import List, Dict, Any

import pandas as pd
import requests
from pybaseball import statcast

import _app_paths

ROOT = _app_paths.APP_DIR
DB_PATH = _app_paths.RAW_DB
SCHEMA_FILE = _app_paths.SCHEMA_FILE
MAPPING_FILE = _app_paths.MAPPING_FILE

SCHEDULE_URL = 'https://statsapi.mlb.com/api/v1/schedule'
# use v1.1 feed path (schedule 'link' values reference /api/v1.1/)
GAME_FEED_URL = 'https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live'


def extract_sql_statements(schema_text: str) -> List[str]:
    tables = re.findall(r"(CREATE TABLE IF NOT EXISTS\s+[a-zA-Z0-9_]+\s*\(.*?\);)", schema_text, flags=re.S | re.I)
    indexes = re.findall(r"(CREATE INDEX IF NOT EXISTS\s+.*?;)", schema_text, flags=re.S | re.I)
    views = re.findall(r"(CREATE VIEW IF NOT EXISTS\s+[a-zA-Z0-9_]+\s+AS\s+.*?;)", schema_text, flags=re.S | re.I)
    return tables + indexes + views


def create_db(db_path: str = DB_PATH):
    """Create DB and add any missing columns defined in schema file.

    This will not drop existing data. Missing columns are added with a
    conservative type token parsed from the schema definition.
    """
    print('Creating database and migrating missing columns (if needed)...')
    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        txt = f.read()

    stmts = extract_sql_statements(txt)
    # Pre-drop any existing views so CREATE VIEW will replace them
    view_names = re.findall(r"CREATE VIEW IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s+AS", txt, flags=re.S | re.I)
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    # WAL mode allows concurrent readers during writes
    cur.execute('PRAGMA journal_mode=WAL')
    for v in view_names:
        try:
            cur.execute(f"DROP VIEW IF EXISTS {v}")
        except Exception as e:
            print('Warning dropping view (continuing):', e)

    for s in stmts:
        try:
            cur.execute(s)
        except Exception as e:
            print('Warning executing statement (continuing):', e)
    conn.commit()

    # Migration: add newly-introduced columns if table exists but column missing
    try:
        cur.execute("PRAGMA table_info(plate_appearances)")
        existing = [r[1] for r in cur.fetchall()]
        if 'outs_recorded' not in existing:
            try:
                cur.execute("ALTER TABLE plate_appearances ADD COLUMN outs_recorded INTEGER DEFAULT 0")
                print('Added column outs_recorded to plate_appearances')
            except Exception as e:
                print('Warning adding outs_recorded column:', e)
        if 'earned_runs' not in existing:
            try:
                cur.execute("ALTER TABLE plate_appearances ADD COLUMN earned_runs INTEGER DEFAULT 0")
                print('Added column earned_runs to plate_appearances')
            except Exception as e:
                print('Warning adding earned_runs column:', e)
        if 'statcast_at_bat_number' not in existing:
            try:
                cur.execute("ALTER TABLE plate_appearances ADD COLUMN statcast_at_bat_number INTEGER")
                print('Added column statcast_at_bat_number to plate_appearances')
            except Exception as e:
                print('Warning adding statcast_at_bat_number column:', e)
        conn.commit()
    except Exception as e:
        print('Warning during migration checks:', e)

    # Migration: add 'base' column to stolen_bases if missing
    try:
        cur.execute("PRAGMA table_info(stolen_bases)")
        existing_sb = [r[1] for r in cur.fetchall()]
        if 'base' not in existing_sb:
            try:
                cur.execute("ALTER TABLE stolen_bases ADD COLUMN base TEXT DEFAULT ''")
                print('Added column base to stolen_bases')
            except Exception as e:
                print('Warning adding base column:', e)
        conn.commit()
    except Exception as e:
        print('Warning during stolen_bases migration:', e)

    conn.close()
    print('Database created/updated.')


def fetch_schedule(start_date: str, end_date: str, only_completed: bool = False) -> List[Dict[str, Any]]:
    """Fetch schedule from the MLB Stats API and return regular-season games.

    If `only_completed` is True, only final games will be returned.
    """
    # include sportId and gameTypes by default to avoid ambiguous/400 responses
    # hydrate=team provides team abbreviations for the games table
    params = {'startDate': start_date, 'endDate': end_date, 'sportId': 1, 'gameTypes': 'R', 'hydrate': 'team'}
    try:
        r = requests.get(SCHEDULE_URL, params=params, timeout=30)
        r.raise_for_status()
        d = r.json()
    except requests.RequestException as e:
        # attempt to surface response details for easier debugging
        resp = getattr(e, 'response', None)
        if resp is not None:
            try:
                print(f"Warning fetching schedule: {resp.status_code} {resp.reason} for url: {resp.url}")
                print('Response body (truncated):', resp.text[:1000])
            except Exception:
                print('Warning fetching schedule (response present but unreadable)')
        else:
            print('Warning fetching schedule:', e)
        return []
    games: List[Dict[str, Any]] = []
    for dd in d.get('dates', []):
        for g in dd.get('games', []):
            if g.get('gameType') != 'R':
                continue
            if only_completed:
                status = g.get('status', {}) or {}
                abstract = (status.get('abstractGameState') or '').lower()
                detailed = (status.get('detailedState') or '').lower()
                coded = status.get('codedGameState') or status.get('statusCode') or ''
                if not (abstract == 'final' or detailed == 'final' or str(coded).upper() == 'F'):
                    continue
            games.append(g)
    print(f'Found {len(games)} regular-season games in range (only_completed={only_completed})')
    return games


def fetch_game_feed(game_pk: int) -> Dict[str, Any]:
    url = GAME_FEED_URL.format(gamePk=game_pk)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def insert_rows(conn: sqlite3.Connection, table: str, rows: List[Dict[str, Any]], commit: bool = True):
    if not rows:
        return
    cols = get_table_columns(conn, table)
    col_clause = ','.join(cols)
    placeholders = ','.join('?' for _ in cols)
    sql = f'INSERT OR REPLACE INTO {table}({col_clause}) VALUES ({placeholders})'
    vals = []
    for r in rows:
        vals.append([r.get(c) for c in cols])
    cur = conn.cursor()
    cur.executemany(sql, vals)
    if commit:
        conn.commit()


def update_row_by_pa_id(conn: sqlite3.Connection, pa_id: str, updates: Dict[str, Any], commit: bool = True):
    if not updates:
        return
    cols = list(updates.keys())
    assignments = ','.join(f"{c} = ?" for c in cols)
    sql = f"UPDATE plate_appearances SET {assignments} WHERE pa_id = ?"
    cur = conn.cursor()
    cur.execute(sql, [updates[c] for c in cols] + [pa_id])
    if commit:
        conn.commit()


def parse_plays_to_pas(feed: Dict[str, Any], season: int) -> List[Dict[str, Any]]:
    out = []
    game_pk = feed.get('gamePk') or feed.get('gamePk')
    gd = feed.get('gameData', {})
    game_date = gd.get('datetime', {}).get('officialDate')
    game_type = gd.get('game', {}).get('type') or 'R'
    teams = gd.get('teams', {})
    home = teams.get('home', {}).get('abbreviation')
    away = teams.get('away', {}).get('abbreviation')
    plays = feed.get('liveData', {}).get('plays', {}).get('allPlays', [])

    # Pre-scan: count game-level runs scored per player (runners crossing home)
    # A player scores a run as a baserunner during ANY at-bat, not just their own.
    from collections import defaultdict
    _game_runs = defaultdict(int)
    for play in plays:
        for rn in play.get('runners') or []:
            if (rn.get('movement') or {}).get('end') == 'score':
                rn_id = (rn.get('details') or {}).get('runner', {}).get('id')
                if rn_id:
                    _game_runs[rn_id] += 1
    # Track which player's runs we've already assigned to a PA
    _runs_assigned = {}

    for idx, play in enumerate(plays, start=1):
        about = play.get('about', {})
        matchup = play.get('matchup', {})
        result = play.get('result', {})
        at_bat_number = about.get('atBatIndex', idx)
        batter = matchup.get('batter', {})
        pitcher = matchup.get('pitcher', {})
        # attempt to derive pitcher's throwing hand from feed fields
        p_throws = None
        try:
            # MLB API v1.1: pitchHand is on the matchup object, not on pitcher
            ph = matchup.get('pitchHand')
            if isinstance(ph, dict):
                p_throws = ph.get('code')
            # fallback: check pitcher dict itself (older feeds)
            if not p_throws and isinstance(pitcher, dict):
                if 'pitchHand' in pitcher and isinstance(pitcher.get('pitchHand'), dict):
                    p_throws = pitcher.get('pitchHand', {}).get('code')
                elif 'throwHand' in pitcher:
                    p_throws = pitcher.get('throwHand')
                elif 'throws' in pitcher:
                    p_throws = pitcher.get('throws')
            if isinstance(p_throws, str):
                p_throws = p_throws.strip().upper()[:1]
        except Exception:
            p_throws = None

        # derive is_vs_lefty from pitcher handedness
        is_vs_lefty = None
        if p_throws == 'L':
            is_vs_lefty = 1
        elif p_throws == 'R':
            is_vs_lefty = 0

        # extract batter position from feed
        position = None
        try:
            bat_pos = batter.get('primaryPosition')
            if isinstance(bat_pos, dict):
                position = bat_pos.get('abbreviation') or bat_pos.get('code')
        except Exception:
            pass
        batter_id = batter.get('id')
        pitcher_id = pitcher.get('id')
        pa_id = f"{game_pk}_{batter_id}_{at_bat_number}"

        # derive simple event flags
        event = (result.get('event') or result.get('eventType') or '')
        ev_low = (event or '').lower()
        is_walk = 1 if ('walk' in ev_low or 'base on balls' in ev_low or 'intent_walk' in ev_low) else 0
        is_strikeout = 1 if ('strikeout' in ev_low or 'struck out' in ev_low) else 0
        is_hr = 1 if ('home_run' in ev_low or 'home run' in ev_low) else 0
        is_single = 1 if ('single' in ev_low and 'double' not in ev_low) else 0
        is_double = 1 if 'double' in ev_low else 0
        is_triple = 1 if 'triple' in ev_low else 0
        is_hbp = 1 if ('hit by pitch' in ev_low or 'hbp' in ev_low) else 0
        is_sac_fly = 1 if ('sac fly' in ev_low or 'sacrifice fly' in ev_low) else 0
        is_sac_bunt = 1 if ('sac bunt' in ev_low or 'sacrifice bunt' in ev_low) else 0
        non_ab_keywords = ['walk', 'hit by pitch', 'hbp', 'sacrifice', 'sac fly', 'sac bunt', 'intent_walk', 'catcher interference']
        is_ab = 0 if any(k in ev_low for k in non_ab_keywords) else 1
        total_bases = 0
        if is_single:
            total_bases = 1
        elif is_double:
            total_bases = 2
        elif is_triple:
            total_bases = 3
        elif is_hr:
            total_bases = 4

        # Assign game-level runs to this PA if not yet assigned for this batter
        # (all runs go on one PA so SUM(runs) is correct per player per game)
        if batter_id in _game_runs and batter_id not in _runs_assigned:
            batter_runs = _game_runs[batter_id]
            _runs_assigned[batter_id] = True
        else:
            batter_runs = 0

        # compute outs recorded and earned runs for this plate appearance
        outs_on_play = 0
        earned_runs = 0
        play_events = play.get('playEvents') or []
        if play_events:
            # sum outs; detect scoring events and whether they are earned
            for ev in play_events:
                # outs
                if ev.get('isOut'):
                    try:
                        outs_on_play += 1
                    except Exception:
                        outs_on_play += 0

            # detect scoring + earned status per event
            for ev in play_events:
                # determine rbi count
                rbi_count = 0
                if ev.get('rbi') is not None:
                    try:
                        rbi_count = int(ev.get('rbi'))
                    except Exception:
                        rbi_count = 1
                elif ev.get('isScoringPlay'):
                    rbi_count = 1

                if rbi_count > 0:
                    # check for explicit earned flag
                    is_earned = None
                    if 'isEarned' in ev:
                        is_earned = ev.get('isEarned')
                    elif isinstance(ev.get('details'), dict) and 'isEarned' in ev.get('details'):
                        is_earned = ev.get('details').get('isEarned')

                    # fallback: if any play event has an error flag, treat runs as unearned
                    if is_earned is None:
                        has_error = any((e.get('isError') or (isinstance(e.get('details'), dict) and e.get('details').get('error'))) for e in play_events)
                        is_earned = not has_error

                    if is_earned:
                        earned_runs += rbi_count
        else:
            # fallback heuristics when playEvents not available
            if 'double play' in ev_low or 'double_play' in ev_low:
                outs_on_play = 2
            elif 'triple play' in ev_low or 'triple_play' in ev_low:
                outs_on_play = 3
            elif is_strikeout:
                outs_on_play = 1
            elif 'out' in ev_low or 'ground out' in ev_low or 'fly out' in ev_low:
                outs_on_play = 1

            # earned runs fallback: assume rbi is earned unless description contains 'error'
            rbi_val = result.get('rbi') if result.get('rbi') is not None else 0
            try:
                rbi_val = int(rbi_val)
            except Exception:
                rbi_val = 0
            desc = (result.get('description') or '') or ''
            if rbi_val > 0:
                if 'error' in desc.lower() or 'fielding error' in desc.lower():
                    earned_runs = 0
                else:
                    earned_runs = rbi_val

        row = {
            'pa_id': pa_id,
            'game_id': str(game_pk),
            'batter_id': batter_id,
            'pitcher_id': pitcher_id,
            'game_date': game_date,
            'season': season,
            'game_year': season,
            'game_type': game_type,
            'inning': about.get('inning'),
            'outs_when_up': about.get('outs', None),
            'batter_name': batter.get('fullName'),
            'stand': matchup.get('batSide', {}).get('code'),
            'position': position,
            'pitcher_name': pitcher.get('fullName'),
            'p_throws': p_throws,
            'is_vs_lefty': is_vs_lefty,
            'events': (result.get('event') or result.get('eventType')),
            'description': result.get('description'),
            'result': result.get('event'),
            'is_hit': 1 if (is_single or is_double or is_triple or is_hr) else 0,
            'is_home_run': is_hr,
            'is_ab': is_ab,
            'is_walk': is_walk,
            'is_strikeout': is_strikeout,
            'is_hbp': is_hbp,
            'runs': batter_runs,
            'rbi': result.get('rbi') if result.get('rbi') is not None else None,
            'is_single': is_single,
            'is_double': is_double,
            'is_triple': is_triple,
            'total_bases': total_bases,
            'is_sac_fly': is_sac_fly,
            'is_sac_bunt': is_sac_bunt,
            'launch_speed': None,
            'launch_angle': None,
            'spray_angle': None,
            'on_1b': None,
            'on_2b': None,
            'on_3b': None,
            'bb_type': None,
            'hit_location': None,
            'hit_distance_sc': None,
            'at_bat_number': at_bat_number,
            'balls': None,
            'strikes': None,
            'release_speed': None,
            'release_spin_rate': None,
            'spin_axis': None,
            'effective_speed': None,
            'release_extension': None,
            'pitch_call': None,
            'swing': None,
            'contact': None,
            'zone': None,
            'batter_is_home': 0 if about.get('isTopInning', True) else 1,
            'home_team': home,
            'away_team': away,
            'outs_recorded': outs_on_play,
            'earned_runs': earned_runs,
        }
        out.append(row)
    return out


def parse_stolen_events(feed: Dict[str, Any], season: int) -> List[Dict[str, Any]]:
    """Parse stolen-base, caught-stealing, and pickoff events from a game feed.

    Pickoffs come from playEvents[].  Stolen bases and caught stealing come
    from the runners[] array on each play (the authoritative source).
    Catcher is identified from the boxscore based on which team is fielding.
    """
    out = []
    seen_ids: set = set()
    game_pk = feed.get('gamePk') or feed.get('game_pk')
    gd = feed.get('gameData', {})
    game_date = gd.get('datetime', {}).get('officialDate')
    plays = feed.get('liveData', {}).get('plays', {}).get('allPlays', [])

    # ── Extract starting catchers from boxscore ──────────────────────────
    # Top inning → home team fields → home catcher
    # Bottom inning → away team fields → away catcher
    boxscore = feed.get('liveData', {}).get('boxscore', {}).get('teams', {})
    catchers = {}  # key: 'away' or 'home' → (id, name)
    for side in ('away', 'home'):
        team_players = boxscore.get(side, {}).get('players', {})
        for _pid, pdata in team_players.items():
            for ap in pdata.get('allPositions', []):
                if ap.get('abbreviation') == 'C' or ap.get('code') == '2':
                    if not pdata.get('gameStatus', {}).get('isSubstitute', False):
                        person = pdata.get('person', {})
                        catchers[side] = (person.get('id'), person.get('fullName'))
                        break
            if side in catchers:
                break
        # Fallback: any catcher if no non-substitute found
        if side not in catchers:
            for _pid, pdata in team_players.items():
                for ap in pdata.get('allPositions', []):
                    if ap.get('abbreviation') == 'C' or ap.get('code') == '2':
                        person = pdata.get('person', {})
                        catchers[side] = (person.get('id'), person.get('fullName'))
                        break
                if side in catchers:
                    break

    def _fielding_catcher(half_inning: str):
        """Return (catcher_id, catcher_name) for the fielding team."""
        if half_inning == 'top':
            return catchers.get('home', (None, None))
        else:
            return catchers.get('away', (None, None))

    def _parse_base(event_type_str: str) -> str:
        """Extract base from eventType like 'stolen_base_2b' → '2B'."""
        if not event_type_str:
            return ''
        low = event_type_str.lower()
        if '2b' in low:
            return '2B'
        if '3b' in low:
            return '3B'
        if 'home' in low:
            return 'Home'
        return ''

    for p_idx, play in enumerate(plays, start=1):
        matchup = play.get('matchup') or {}
        pitcher_info = matchup.get('pitcher') or {}
        pitcher_id = pitcher_info.get('id')
        pitcher_name = pitcher_info.get('fullName')
        half_inning = (play.get('about') or {}).get('halfInning', '')
        catcher_id, catcher_name = _fielding_catcher(half_inning)

        # ── Phase 1: playEvents — pickoffs only ─────────────────────────
        play_events = play.get('playEvents') or []
        for e_idx, ev in enumerate(play_events, start=1):
            details = ev.get('details') or {}
            desc = (details.get('description') or ev.get('result') or ev.get('text') or '')
            ev_low = (desc or '').lower()

            is_pickoff = False
            if ev.get('isPickoff'):
                is_pickoff = True
            elif not is_pickoff:
                if 'picked off' in ev_low or 'pickoff' in ev_low or 'pick off' in ev_low:
                    is_pickoff = True

            if not is_pickoff:
                continue

            # Identify runner from playEvent players or matchup
            runner_id = None
            runner_name = None
            for p in (ev.get('players') or []):
                ptype = (p.get('playerType') or '').lower()
                player = p.get('player') or {}
                if 'runner' in ptype:
                    runner_id = player.get('id')
                    runner_name = player.get('fullName') or player.get('lastName')

            if runner_id is None:
                # Try to infer from description base reference
                base_num = None
                m = re.search(r'([1-3])B', desc or '', flags=re.I)
                if m:
                    try:
                        base_num = int(m.group(1))
                    except Exception:
                        pass
                if base_num:
                    key = f"postOn{['', 'First', 'Second', 'Third'][base_num]}"
                    cand = matchup.get(key) or {}
                    if isinstance(cand, dict) and cand.get('id'):
                        runner_id = cand.get('id')
                        runner_name = cand.get('fullName') or cand.get('lastName')

            event_id = f"{game_pk}_p{p_idx}_e{e_idx}"
            if event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            out.append({
                'event_id': event_id,
                'game_id': str(game_pk),
                'season': season,
                'game_date': game_date,
                'runner_id': runner_id,
                'runner_name': runner_name,
                'event_type': 'pickoff',
                'is_successful': 1,
                'pitcher_id': pitcher_id,
                'pitcher_name': pitcher_name,
                'catcher_id': catcher_id,
                'catcher_name': catcher_name,
                'base': '',
                'top_speed': None,
                'sprint_speed': None,
                'bolts': None,
                'competitive_runs': None,
                'description': desc,
            })

        # ── Phase 2: runners[] — stolen bases and caught stealing ────────
        runners = play.get('runners') or []
        for r_idx, runner in enumerate(runners, start=1):
            det = runner.get('details') or {}
            event_type_raw = det.get('eventType') or ''
            low_et = event_type_raw.lower()

            event_type = None
            is_successful = None
            if 'stolen_base' in low_et and 'caught' not in low_et:
                event_type = 'stolen_base'
                is_successful = 1
            elif 'caught_stealing' in low_et:
                event_type = 'caught_stealing'
                is_successful = 0
            else:
                continue

            runner_person = det.get('runner') or {}
            runner_id = runner_person.get('id')
            runner_name = runner_person.get('fullName')
            base = _parse_base(event_type_raw)

            movement = runner.get('movement') or {}
            desc = (play.get('result') or {}).get('description', '')

            event_id = f"{game_pk}_p{p_idx}_r{r_idx}"
            if event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            out.append({
                'event_id': event_id,
                'game_id': str(game_pk),
                'season': season,
                'game_date': game_date,
                'runner_id': runner_id,
                'runner_name': runner_name,
                'event_type': event_type,
                'is_successful': 1 if is_successful else 0,
                'pitcher_id': pitcher_id,
                'pitcher_name': pitcher_name,
                'catcher_id': catcher_id,
                'catcher_name': catcher_name,
                'base': base,
                'top_speed': None,
                'sprint_speed': None,
                'bolts': None,
                'competitive_runs': None,
                'description': desc,
            })
    return out


def enrich_with_statcast(conn: sqlite3.Connection, start_date: str, end_date: str):
    print('Fetching statcast for enrichment', start_date, end_date)
    df = statcast(start_date, end_date)
    if df is None or df.empty:
        print('No statcast rows; skipping enrichment')
        return
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
    except Exception:
        mapping = {}

    pitch_inserts = []
    _uncommitted = 0
    _BATCH_SIZE = 500

    def _maybe_commit():
        nonlocal _uncommitted
        _uncommitted += 1
        if _uncommitted >= _BATCH_SIZE:
            conn.commit()
            _uncommitted = 0

    def resolve_player_name(game_id, player_id, role='pitcher'):
        if player_id is None:
            return None
        col = 'pitcher_name' if role == 'pitcher' else 'batter_name'
        idcol = 'pitcher_id' if role == 'pitcher' else 'batter_id'
        cur = conn.execute(f"SELECT {col} FROM plate_appearances WHERE game_id = ? AND {idcol} = ? LIMIT 1", (str(game_id), int(player_id)))
        r = cur.fetchone()
        if r and r[0]:
            return r[0]
        return f"id_{int(player_id)}"

    for _, srow in df.iterrows():
        game_pk = srow.get('game_pk') or srow.get('game_pk')
        batter = srow.get('batter')
        at_bat_number = srow.get('at_bat_number')
        pa_id = None
        if pd.notna(game_pk) and pd.notna(batter) and pd.notna(at_bat_number):
            # Statcast `at_bat_number` is 1-based while the MLB feed `about.atBatIndex`
            # (used when parsing plate_appearances) is 0-based. Normalize here so
            # pa_id constructed from Statcast rows matches the plate_appearances pa_id.
            try:
                ab = int(at_bat_number)
                pa_index = ab - 1 if ab > 0 else ab
                pa_id = f"{int(game_pk)}_{int(batter)}_{pa_index}"
            except Exception:
                pa_id = f"{int(game_pk)}_{int(batter)}_{int(at_bat_number)}"

        pa_update = {}
        pitch_row = {}
        for statcol, info in mapping.items():
            mapped = info.get('mapped_to') if isinstance(info, dict) else None
            if not mapped:
                continue
            table, col = mapped.split('.')
            val = srow.get(statcol)
            if pd.isna(val):
                val = None
            if table == 'plate_appearances':
                pa_update[col] = val
            elif table == 'pitching_appearances':
                pitch_row[col] = val

        # Keep the feed's `at_bat_number` canonical; Statcast's original
        # 1-based value is stored in `statcast_at_bat_number` (mapping updated).

        # compute total_bases if possible
        if pa_update.get('is_single'):
            pa_update.setdefault('total_bases', 1)
        elif pa_update.get('is_double'):
            pa_update.setdefault('total_bases', 2)
        elif pa_update.get('is_triple'):
            pa_update.setdefault('total_bases', 3)
        elif pa_update.get('is_home_run'):
            pa_update.setdefault('total_bases', 4)

        # Derive `barrel` (approx) and `pull` flags when Statcast does not provide them.
        # Barrel: fallback heuristic based on exit velocity and launch angle
        ls = srow.get('launch_speed')
        la = srow.get('launch_angle')
        hcx = srow.get('hc_x')
        try:
            if 'barrel' not in pa_update:
                if pd.notna(ls) and pd.notna(la):
                    try:
                        pa_update['barrel'] = 1 if (float(ls) >= 98 and 26 <= float(la) <= 30) else 0
                    except Exception:
                        pass
        except Exception:
            pass

        # Pull: handedness-aware heuristic using hit coordinate (`hc_x`) and batter stand
        try:
            if 'pull' not in pa_update and pd.notna(hcx):
                stand = srow.get('stand')
                try:
                    hx = float(hcx)
                    if pd.notna(stand):
                        if stand == 'R':
                            pa_update['pull'] = 1 if hx < 125.42 else 0
                        else:  # L or S
                            pa_update['pull'] = 1 if hx > 125.42 else 0
                except Exception:
                    pass
        except Exception:
            pass

        if pa_id and pa_update:
            # Try exact pa_id first
            cur = conn.execute("SELECT 1 FROM plate_appearances WHERE pa_id = ? LIMIT 1", (pa_id,))
            if cur.fetchone():
                update_row_by_pa_id(conn, pa_id, pa_update, commit=False)
                _maybe_commit()
            else:
                matched = False
                # Statcast `at_bat_number` is 1-based; convert to 0-based index for DB matching
                pa_index = None
                try:
                    sab = int(at_bat_number)
                    pa_index = sab - 1 if sab > 0 else sab
                except Exception:
                    pa_index = None

                if pa_index is not None:
                    for candidate in (pa_index, pa_index - 1, pa_index + 1):
                        try:
                            cur = conn.execute(
                                "SELECT pa_id FROM plate_appearances WHERE game_id = ? AND batter_id = ? AND at_bat_number = ? LIMIT 1",
                                (str(int(game_pk)), int(batter), int(candidate)),
                            )
                            r = cur.fetchone()
                            if r:
                                update_row_by_pa_id(conn, r[0], pa_update, commit=False)
                                _maybe_commit()
                                matched = True
                                break
                        except Exception:
                            continue

                # Fallback: try matching by inning
                if not matched:
                    inning_val = srow.get('inning') if 'inning' in srow else None
                    try:
                        if inning_val is not None and pd.notna(inning_val):
                            cur = conn.execute(
                                "SELECT pa_id FROM plate_appearances WHERE game_id = ? AND batter_id = ? AND inning = ? LIMIT 1",
                                (str(int(game_pk)), int(batter), int(inning_val)),
                            )
                            r = cur.fetchone()
                            if r:
                                update_row_by_pa_id(conn, r[0], pa_update, commit=False)
                                _maybe_commit()
                                matched = True
                    except Exception:
                        pass

                # Fallback: match by short description snippet
                if not matched:
                    desc = srow.get('description') or srow.get('events') or ''
                    if isinstance(desc, str) and desc.strip():
                        short = desc.strip()[:60]
                        try:
                            cur = conn.execute(
                                "SELECT pa_id FROM plate_appearances WHERE game_id = ? AND batter_id = ? AND description LIKE ? LIMIT 1",
                                (str(int(game_pk)), int(batter), f"%{short}%"),
                            )
                            r = cur.fetchone()
                            if r:
                                update_row_by_pa_id(conn, r[0], pa_update, commit=False)
                                _maybe_commit()
                                matched = True
                        except Exception:
                            pass
                # If still not matched, skip to avoid incorrect mappings

        if pitch_row:
            pitch_number = srow.get('pitch_number')
            pitcher = srow.get('pitcher')
            if pd.notna(game_pk) and pd.notna(pitcher) and pd.notna(pitch_number):
                pitch_id = f"{int(game_pk)}_{int(pitcher)}_{int(pitch_number)}"
                pitch_row['pitch_id'] = pitch_id
                pitch_row['game_id'] = str(int(game_pk))
                pitch_row['pitcher_id'] = int(pitcher)
                pitch_row['batter_id'] = int(srow.get('batter')) if pd.notna(srow.get('batter')) else None
                pitch_row['game_date'] = srow.get('game_date')
                pitch_row['season'] = srow.get('game_year')
                if 'pitcher_name' not in pitch_row or not pitch_row.get('pitcher_name'):
                    pitch_row['pitcher_name'] = resolve_player_name(game_pk, pitcher, role='pitcher')
                if 'batter_name' not in pitch_row or not pitch_row.get('batter_name'):
                    pitch_row['batter_name'] = resolve_player_name(game_pk, pitch_row.get('batter_id'), role='batter')
                pitch_inserts.append(pitch_row)

    # Final commit for any remaining uncommitted updates
    conn.commit()

    if pitch_inserts:
        insert_rows(conn, 'pitching_appearances', pitch_inserts)
    print('Enrichment complete')


def _insert_games(conn: sqlite3.Connection, games: List[Dict[str, Any]], season: int):
    """Insert game-level rows into the ``games`` table from schedule API data."""
    if not games:
        return
    cur = conn.cursor()
    for g in games:
        game_id = str(g.get('gamePk', ''))
        teams = g.get('teams', {})
        away = teams.get('away', {})
        home = teams.get('home', {})
        cur.execute(
            '''INSERT OR REPLACE INTO games
               (game_id, game_date, season, game_type, game_year, away_team, home_team, away_score, home_score)
               VALUES (?,?,?,?,?,?,?,?,?)''',
            (
                game_id,
                g.get('officialDate'),
                season,
                g.get('gameType', 'R'),
                season,
                away.get('team', {}).get('abbreviation', ''),
                home.get('team', {}).get('abbreviation', ''),
                away.get('score'),
                home.get('score'),
            ),
        )
    conn.commit()
    print(f'Inserted {len(games)} game(s) into games table')


def fetch_sprint_speeds(season: int, conn: sqlite3.Connection):
    """Fetch sprint speed leaderboard from Baseball Savant via pybaseball and upsert into sprint_speeds table."""
    try:
        from pybaseball import statcast_sprint_speed
        df = statcast_sprint_speed(season, min_opp=1)
        if df is None or df.empty:
            print(f'  No sprint speed data for {season}')
            return 0
        cur = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            pid = row.get('player_id')
            if pid is None or pd.isna(pid):
                continue
            sprint = row.get('sprint_speed')
            bolts = row.get('bolts')
            comp_runs = row.get('competitive_runs')
            sprint = float(sprint) if sprint is not None and not pd.isna(sprint) else None
            bolts = int(bolts) if bolts is not None and not pd.isna(bolts) else None
            comp_runs = int(comp_runs) if comp_runs is not None and not pd.isna(comp_runs) else None
            games_sampled = comp_runs  # competitive_runs ≈ games sampled
            cur.execute('''INSERT OR REPLACE INTO sprint_speeds
                           (player_id, season, sprint_speed, bolts, competitive_runs, games_sampled)
                           VALUES (?, ?, ?, ?, ?, ?)''',
                        (int(pid), season, sprint, bolts, comp_runs, games_sampled))
            count += 1
        conn.commit()
        print(f'  Sprint speeds: {count} players for {season}')
        return count
    except Exception as e:
        print(f'  Warning: failed to fetch sprint speeds for {season}: {e}')
        return 0


def run_pipeline(start_date: str, end_date: str, season: int, only_completed: bool = False, progress_cb=None, games=None):
    """Ingest game feeds and enrich with statcast.

    Parameters
    ----------
    games : list, optional
        Pre-fetched schedule games list. When provided the internal
        ``fetch_schedule`` call is skipped, avoiding a duplicate API hit.
    """
    create_db()
    conn = sqlite3.connect(DB_PATH, timeout=30)

    if games is None:
        games = fetch_schedule(start_date, end_date, only_completed=only_completed)

    # Populate the games table with schedule-level metadata
    _insert_games(conn, games, season)

    # ── Determine which games actually need ingestion ──
    existing_game_ids: set = set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_id FROM plate_appearances")
        existing_game_ids = {r[0] for r in cur.fetchall()}
    except Exception:
        pass

    games_to_fetch = []
    for g in games:
        game_pk = g.get('gamePk')
        if str(game_pk) not in existing_game_ids:
            games_to_fetch.append(g)
        else:
            print(f'  Skipping game {game_pk} (already ingested)')

    total = len(games_to_fetch)
    if total == 0:
        print('All games already ingested — skipping feed fetches.')
    else:
        # ── Fetch game feeds in parallel ──
        feeds: dict = {}  # game_pk → feed json
        with ThreadPoolExecutor(max_workers=6) as pool:
            future_to_pk = {
                pool.submit(fetch_game_feed, g.get('gamePk')): g.get('gamePk')
                for g in games_to_fetch
            }
            for fut in as_completed(future_to_pk):
                gpk = future_to_pk[fut]
                try:
                    feeds[gpk] = fut.result()
                except Exception as e:
                    print(f'Warning: failed to fetch feed for {gpk}: {e}')

        # ── Process feeds sequentially (DB writes) ──
        for i, g in enumerate(games_to_fetch):
            game_pk = g.get('gamePk')
            if progress_cb:
                progress_cb('raw_game', i, total, game_pk)

            feed = feeds.get(game_pk)
            if feed is None:
                continue

            pas = parse_plays_to_pas(feed, season)
            if pas:
                insert_rows(conn, 'plate_appearances', pas, commit=False)
                # Update pitchers reference table with throwing hand information
                try:
                    cur = conn.cursor()
                    pitchers_map = {}
                    for r in pas:
                        pid = r.get('pitcher_id')
                        if pid is None:
                            continue
                        p_throws = r.get('p_throws')
                        pname = r.get('pitcher_name')
                        if p_throws and pid not in pitchers_map:
                            pitchers_map[pid] = (pname or '', p_throws)

                    for pid, (pname, p_throws) in pitchers_map.items():
                        cur.execute('SELECT p_throws FROM pitchers WHERE pitcher_id = ?', (pid,))
                        ex = cur.fetchone()
                        if not ex:
                            cur.execute('INSERT OR REPLACE INTO pitchers (pitcher_id, pitcher_name, p_throws) VALUES (?, ?, ?)', (pid, pname, p_throws))
                        else:
                            if (ex[0] is None or ex[0] == '') and p_throws:
                                cur.execute('UPDATE pitchers SET p_throws = ?, pitcher_name = ? WHERE pitcher_id = ?', (p_throws, pname, pid))
                except Exception:
                    pass
            steals = parse_stolen_events(feed, season)
            if steals:
                insert_rows(conn, 'stolen_bases', steals, commit=False)

        # Single commit for all game data
        conn.commit()

    if progress_cb:
        progress_cb('statcast', 0, 1, None)
    enrich_with_statcast(conn, start_date, end_date)
    fetch_sprint_speeds(season, conn)
    if progress_cb:
        progress_cb('statcast', 1, 1, None)
    conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--start', help='Start date (YYYY-MM-DD)')
    p.add_argument('--end', help='End date (YYYY-MM-DD)')
    p.add_argument('--season', type=int, help='Season year (used for season column)')
    p.add_argument('--year', type=int, help='If provided, ingest the given year (overrides start/end). Only completed regular-season games will be fetched.')
    args = p.parse_args()

    # CLI: explicit year or date range
    if args.year:
        start = f"{args.year}-01-01"
        end = f"{args.year}-12-31"
        season = args.year
        only_completed = True
        run_pipeline(start, end, season, only_completed=only_completed)
        return

    if args.start and args.end and args.season:
        run_pipeline(args.start, args.end, args.season, only_completed=False)
        return

    # No CLI args provided: interactive year selection
    if not sys.stdin.isatty():
        p.error('Either provide CLI args or run interactively to select a year')

    current_year = date.today().year
    min_year = 2021
    years = list(range(min_year, current_year + 1))
    print('Select year to ingest (choose one):')
    for y in years:
        print(f'  - {y}')
    print("  - all   (ingest all years sequentially)")

    while True:
        choice = input(f"Enter a year between {min_year} and {current_year}, or 'all': ").strip().lower()
        if choice == 'all':
            for y in years:
                print(f"Starting ingest for {y}...")
                run_pipeline(f"{y}-01-01", f"{y}-12-31", y, only_completed=True)
            break
        try:
            y = int(choice)
        except ValueError:
            print('Invalid input — enter a year number or "all"')
            continue
        if y < min_year or y > current_year:
            print('Year out of range, try again.')
            continue
        run_pipeline(f"{y}-01-01", f"{y}-12-31", y, only_completed=True)
        break


if __name__ == '__main__':
    main()
