import csv
from datetime import datetime
import logging
import os
import pickle
import json
import sqlite3
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed

import _app_paths
from _http_utils import (
    create_http_session,
    TIMEOUT_DEFAULT  as _TIMEOUT_DEFAULT,
    TIMEOUT_SHORT    as _TIMEOUT_SHORT,
    TIMEOUT_LONG     as _TIMEOUT_LONG,
)

_log = logging.getLogger("seam.engine")

_session = create_http_session()


class MLBDataEngine:
    """Simplified API-only interface for MLB data."""
    
    # ── Generic JSON-on-disk cache with optional pickle migration ────
    class _JsonCache:
        __slots__ = ('_json_path', '_pkl_path', '_default_factory',
                     '_as_set', '_indent')

        def __init__(self, path, *, default_factory=dict, as_set=False,
                     migrate_pickle=False, indent=None):
            self._json_path = (path.replace('.pkl', '.json')
                               if path.endswith('.pkl') else path)
            self._pkl_path = path if migrate_pickle else None
            self._default_factory = default_factory
            self._as_set = as_set
            self._indent = indent

        def load(self):
            if os.path.exists(self._json_path):
                try:
                    with open(self._json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    return set(data) if self._as_set else data
                except Exception:
                    return self._default_factory()
            if self._pkl_path and os.path.exists(self._pkl_path):
                try:
                    with open(self._pkl_path, 'rb') as f:
                        data = pickle.load(f)
                    self.save(data)
                    os.remove(self._pkl_path)
                    return data
                except Exception:
                    return self._default_factory()
            return self._default_factory()

        def save(self, data):
            try:
                payload = list(data) if self._as_set else data
                with open(self._json_path, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, indent=self._indent)
            except Exception:
                _log.debug("Failed to save cache %s", self._json_path)

    def __init__(self):
        self.api_base = "https://statsapi.mlb.com/api/v1"
        self.id_cache_file = _app_paths.PLAYER_IDS_CACHE
        self._id_cache_mgr = self._JsonCache(
            self.id_cache_file, migrate_pickle=True)
        self.id_cache = self._id_cache_mgr.load()
        self.team_abbreviations = self._load_team_abbreviations()
        self.temp_lineup_cache_file = _app_paths.TEMP_LINEUP_CACHE
        self._lineup_cache_mgr = self._JsonCache(
            self.temp_lineup_cache_file, indent=2)
        self.temp_lineup_cache = self._lineup_cache_mgr.load()
        self.processed_dates_file = _app_paths.PROCESSED_DATES_CACHE
        self._dates_cache_mgr = self._JsonCache(
            self.processed_dates_file, default_factory=set,
            as_set=True, migrate_pickle=True)
        self.processed_dates = self._dates_cache_mgr.load()
        self.raw_db_file = _app_paths.RAW_DB
        self.suppress_output = False  # Flag to suppress informational output during shutdown
        self.probable_pitchers = {}  # {game_id_str: {"away": {"name": "F. Last", "id": 123}, "home": {...}}}
        self._game_status_cache = {}  # {game_id: (status_str, timestamp)}
        self._sp_cache = {}  # {game_id_str: {"away": ..., "home": ..., "ts": timestamp}}
        self._handedness_cache = {}  # {player_id: handedness_code}
        self._live_feed_cache = {}   # {game_id_str: (expires_mono, (plays, preview, count))}
        self._schedule_cache = {}    # {date_str: (expires_mono, games_list)}
        self._id_cache_dirty = False  # Track whether id_cache needs saving
        self._cleanup_done = False  # Only run cleanup once per session
        self._local = threading.local()  # Thread-local DB connections
        self._player_data_cache = {}  # {(player_id,is_pitcher,year): {'ts': monotonic, 'result': {...}}}
        self._player_data_cache_max = 256  # Evict oldest entries beyond this limit
        self._cache_lock = threading.Lock()  # Protects shared caches from concurrent access

    def _put_player_data_cache(self, key, value):
        """Insert into _player_data_cache with LRU-style eviction when over limit."""
        self._player_data_cache[key] = value
        if len(self._player_data_cache) > self._player_data_cache_max:
            # Evict oldest insertion (Python 3.7+ dicts maintain insertion order)
            oldest_key = next(iter(self._player_data_cache))
            self._player_data_cache.pop(oldest_key, None)

    def _load_cache(self):
        return self._id_cache_mgr.load()

    def _save_cache(self):
        self._id_cache_mgr.save(self.id_cache)

    def _load_temp_lineup_cache(self):
        return self._lineup_cache_mgr.load()

    def _save_temp_lineup_cache(self):
        self._lineup_cache_mgr.save(self.temp_lineup_cache)
    
    def _get_db_connection(self):
        """Get a thread-local reusable read-only database connection."""
        conn = getattr(self._local, 'db_conn', None)
        if conn is None and os.path.exists(self.raw_db_file):
            conn = sqlite3.connect(self.raw_db_file, check_same_thread=False)
            self._local.db_conn = conn
        return conn
    
    def _game_exists_in_database(self, game_id):
        """Check if a game_id exists in the raw data database."""
        try:
            conn = self._get_db_connection()
            if conn is None:
                return False
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM statcast WHERE game_id = ? LIMIT 1", (str(game_id),))
            result = cursor.fetchone()
            return result is not None
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                return False
            _log.warning(f"Error checking database for game {game_id}: {e}")
            return False
        except Exception as e:
            _log.warning(f"Error checking database for game {game_id}: {e}")
            return False
    
    def _cleanup_temp_lineups(self):
        """Clean up temporary lineups that now exist in the historical database."""
        games_to_remove = []
        
        for game_id_str in list(self.temp_lineup_cache.keys()):
            try:
                game_id = int(game_id_str)
                if self._game_exists_in_database(game_id):
                    games_to_remove.append(game_id_str)
                    _log.debug(f"Temp lineup for game {game_id} found in database, removing from temp cache")
            except Exception as e:
                _log.warning(f"Error checking game {game_id_str}: {e}")
        
        # Remove games that are now in the database
        for game_id_str in games_to_remove:
            del self.temp_lineup_cache[game_id_str]
        
        if games_to_remove:
            self._save_temp_lineup_cache()
            _log.debug(f"Cleaned up {len(games_to_remove)} temp lineup(s)")
    
    def _load_processed_dates(self):
        return self._dates_cache_mgr.load()

    def _save_processed_dates(self):
        self._dates_cache_mgr.save(self.processed_dates)
    
    def _load_team_abbreviations(self):
        """Load team name to abbreviation and logo mapping from CSV."""
        team_dict = {}
        candidates = [
            _app_paths.TEAM_ABBREV_CSV,
        ]
        csv_file = None
        for candidate in candidates:
            if os.path.exists(candidate):
                csv_file = candidate
                break

        if csv_file:
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name = row.get('team_name') or row.get('team')
                        if not name:
                            continue
                        team_dict[name] = {
                            'abbreviation': row.get('abbreviation') or row.get('abbr') or '',
                            'logo_url': row.get('logo_url', '')
                        }
            except Exception as e:
                _log.warning(f"Warning: Could not load team abbreviations from {csv_file}: {e}")
        else:
            _log.warning(f"Warning: team_abbreviations.csv not found, using fallback abbreviations")
        
        return team_dict
    
    def _get_team_abbreviation(self, team_name):
        """Get abbreviation for a team name, with fallback to first 3 chars if not found."""
        if team_name in self.team_abbreviations:
            return self.team_abbreviations[team_name]['abbreviation']
        # Fallback: use first 3 characters if not in CSV
        return team_name[:3].upper() if team_name else "???"
    
    def _get_team_logo_url(self, team_name):
        """Get logo URL for a team name."""
        if team_name in self.team_abbreviations:
            return self.team_abbreviations[team_name].get('logo_url', '')
        return ''
    
    def get_schedule(self, date_str, poll_mode=False):
        """Fetch schedule for a specific date from MLB Stats API.
        
        Includes only R, W, D, L, C game types (Regular season and Postseason).
        
        Args:
            date_str: Date in format 'YYYY-MM-DD'
            poll_mode: If True, use lighter hydration (skip probablePitcher)
                       to reduce payload on repeated poll cycles.
        
        Returns:
            List of dicts with keys: id, away, home, time, status
        """
        try:
            # Poll mode: probable pitchers are already cached from initial fetch,
            # so skip that hydration to reduce bandwidth.
            hydrate = "linescore,person" if poll_mode else "probablePitcher,linescore,person"

            # In poll mode, return cached schedule if CDN data can't have changed
            # (CDN max-age is 20s; we use 10s to stay responsive to score changes).
            if poll_mode:
                cached = self._schedule_cache.get(date_str)
                if cached and _time.monotonic() < cached[0]:
                    return cached[1]

            url = f"{self.api_base}/schedule?sportId=1&date={date_str}&hydrate={hydrate}"
            _log.debug(f"Fetching schedule for {date_str}...")
            response = _session.get(url, timeout=_TIMEOUT_DEFAULT)
            response.raise_for_status()
            games_data = response.json()
            
            games = []
            for date_entry in games_data.get('dates', []):
                for game_info in date_entry.get('games', []):
                    try:
                        # Only include R, W, D, L, C game types
                        game_type = game_info.get('gameType', 'R')
                        if game_type not in ['R', 'W', 'D', 'L', 'C']:
                            _log.debug(f"  Skipping non-qualifying game: {game_info.get('gamePk', 'unknown')} (type: {game_type})")
                            continue
                        
                        away_team = game_info.get('teams', {}).get('away', {}).get('team', {}).get('name', 'Unknown')
                        home_team = game_info.get('teams', {}).get('home', {}).get('team', {}).get('name', 'Unknown')
                        game_time_str = game_info.get('gameDate', '')  # Changed from gameDateTime
                        
                        # Extract probable pitchers from schedule data
                        # (skip in poll_mode — already cached from initial fetch)
                        game_id = game_info.get('gamePk', 0)
                        game_id_str = str(game_id)
                        if not poll_mode:
                            pp_data = {"away": None, "home": None}
                            for team_key in ["away", "home"]:
                                pp = game_info.get('teams', {}).get(team_key, {}).get('probablePitcher', {})
                                if pp and pp.get('fullName'):
                                    ph = pp.get('pitchHand', {})
                                    pp_data[team_key] = {
                                        "name": self._format_pitcher_name(pp['fullName']),
                                        "fullName": pp['fullName'],
                                        "id": pp.get('id'),
                                        "throws": ph.get('code', '') if ph else '',
                                    }
                            self.probable_pitchers[game_id_str] = pp_data
                        
                        # Parse time
                        time_str = "TBD"
                        if game_time_str:
                            try:
                                # Parse as UTC datetime
                                dt_utc = datetime.fromisoformat(game_time_str.replace('Z', '+00:00'))
                                # Convert to local timezone
                                dt_local = dt_utc.astimezone()
                                # Format as 12-hour time with period, removing leading zero from hour
                                time_str = dt_local.strftime('%I:%M %p').lstrip('0')
                            except Exception as e:
                                _log.warning(f"    Error parsing time '{game_time_str}': {e}")
                                time_str = "TBD"
                        
                        status = game_info.get('status', {}).get('detailedState', '')
                        abstract_state = game_info.get('status', {}).get('abstractGameState', '')
                        away_score = game_info.get('teams', {}).get('away', {}).get('score')
                        home_score = game_info.get('teams', {}).get('home', {}).get('score')
                        ls = game_info.get('linescore', {})
                        offense = ls.get('offense', {})
                        defense = ls.get('defense', {})
                        # Current batter / pitcher from linescore
                        _batter = offense.get('batter', {})
                        _pitcher = defense.get('pitcher', {})
                        _ondeck = offense.get('onDeck', {})
                        _inhole = offense.get('inHole', {})
                        # Per-inning scores
                        innings_detail = []
                        for inn in ls.get('innings', []):
                            innings_detail.append({
                                'num': inn.get('num'),
                                'away': inn.get('away', {}).get('runs'),
                                'home': inn.get('home', {}).get('runs'),
                            })
                        ls_teams = ls.get('teams', {})
                        games.append({
                            "id": game_info.get('gamePk', 0),
                            "away": self._get_team_abbreviation(away_team),
                            "home": self._get_team_abbreviation(home_team),
                            "time": time_str,
                            "status": status,
                            "abstract_state": abstract_state,
                            "away_score": away_score if away_score is not None else 0,
                            "home_score": home_score if home_score is not None else 0,
                            "inning": ls.get('currentInning'),
                            "inning_half": ls.get('inningHalf'),
                            "inning_state": ls.get('inningState'),
                            "on_first": bool(offense.get('first')),
                            "on_second": bool(offense.get('second')),
                            "on_third": bool(offense.get('third')),
                            "outs": ls.get('outs', 0),
                            "balls": ls.get('balls', 0),
                            "strikes": ls.get('strikes', 0),
                            "innings_detail": innings_detail,
                            "away_hits": ls_teams.get('away', {}).get('hits', 0),
                            "home_hits": ls_teams.get('home', {}).get('hits', 0),
                            "away_errors": ls_teams.get('away', {}).get('errors', 0),
                            "home_errors": ls_teams.get('home', {}).get('errors', 0),
                            "current_batter_name": _batter.get('fullName', ''),
                            "current_batter_hand": {'S': 'B'}.get((_batter.get('batSide') or {}).get('code', ''), (_batter.get('batSide') or {}).get('code', '')),
                            "current_pitcher_name": _pitcher.get('fullName', ''),
                            "current_pitcher_hand": (_pitcher.get('pitchHand') or {}).get('code', ''),
                            "ondeck_batter_name": _ondeck.get('fullName', ''),
                            "ondeck_batter_hand": {'S': 'B'}.get((_ondeck.get('batSide') or {}).get('code', ''), (_ondeck.get('batSide') or {}).get('code', '')),
                            "inhole_batter_name": _inhole.get('fullName', ''),
                            "inhole_batter_hand": {'S': 'B'}.get((_inhole.get('batSide') or {}).get('code', ''), (_inhole.get('batSide') or {}).get('code', '')),
                        })
                    except Exception as e:
                        _log.warning(f"  Warning: Could not parse game: {e}")
                        continue
            
            _log.debug(f"  Found {len(games)} games (spring training excluded)")
            # Cache for poll_mode to skip redundant requests within CDN freshness window
            self._schedule_cache[date_str] = (_time.monotonic() + 10, games)
            return games
        except Exception as e:
            _log.warning(f"Error fetching schedule: {e}")
            return []

    def fetch_game_plays(self, game_id):
        """Fetch play-by-play data for a single game from the live feed.

        Returns a list of dicts:
            [{'inning': 1, 'half': 'top', 'event': 'Strikeout',
              'description': '...', 'rbi': 0, 'away_score': 0, 'home_score': 0,
              'is_scoring': False, 'is_action': False}, ...]

        Mid-at-bat action events (stolen bases, caught stealing, wild pitches,
        passed balls, balks, errors, defensive indifference) are extracted from
        playEvents and interleaved chronologically with at-bat results.
        """
        _ACTION_EVENTS = {
            'stolen_base_2b', 'stolen_base_3b', 'stolen_base_home',
            'caught_stealing_2b', 'caught_stealing_3b', 'caught_stealing_home',
            'wild_pitch', 'passed_ball', 'balk',
            'defensive_indifference',
            'caught_stealing_double_play',
            'pickoff_1b', 'pickoff_2b', 'pickoff_3b',
            'pickoff_caught_stealing_2b', 'pickoff_caught_stealing_3b',
            'pickoff_caught_stealing_home',
            'error',
            # Substitutions
            'pitching_substitution', 'offensive_substitution',
            'defensive_substitution', 'defensive_switch',
        }
        try:
            url = f"{self.api_base.replace('/v1', '/v1.1')}/game/{game_id}/feed/live"
            # ── Time-based cache: CDN max-age is 10s, skip request if still fresh ──
            gid_str = str(game_id)
            cached_entry = self._live_feed_cache.get(gid_str)
            if cached_entry and _time.monotonic() < cached_entry[0]:
                return cached_entry[1]          # still fresh — skip network round-trip
            resp = _session.get(url, timeout=_TIMEOUT_DEFAULT)
            resp.raise_for_status()
            data = resp.json()
            all_plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
            result = []
            for play in all_plays:
                res = play.get('result', {})
                about = play.get('about', {})
                inning = about.get('inning', 0)
                half = about.get('halfInning', '')

                # Extract mid-at-bat action events from playEvents
                for pe in play.get('playEvents', []):
                    if pe.get('type') != 'action':
                        continue
                    pe_details = pe.get('details', {})
                    pe_event_type = pe_details.get('eventType', '')
                    if pe_event_type.lower() not in _ACTION_EVENTS:
                        continue
                    pe_event = pe_details.get('event', '')
                    pe_desc = pe_details.get('description', '')
                    if not pe_event and not pe_desc:
                        continue
                    pe_scoring = pe_details.get('isScoringPlay', False)
                    _is_sub = pe_event_type.lower() in (
                        'pitching_substitution', 'offensive_substitution',
                        'defensive_substitution', 'defensive_switch',
                    )
                    result.append({
                        'inning': inning,
                        'half': half,
                        'event': pe_event or '',
                        'description': pe_desc or '',
                        'rbi': 0,
                        'away_score': pe_details.get('awayScore', 0),
                        'home_score': pe_details.get('homeScore', 0),
                        'is_scoring': pe_scoring,
                        'is_action': True,
                        'is_substitution': _is_sub,
                    })

                # At-bat result
                event = res.get('event')
                desc = res.get('description')
                if not event and not desc:
                    continue

                # For home runs, extract hitData metrics from playEvents
                hr_details = ''
                if event and 'home run' in event.lower():
                    for pe in reversed(play.get('playEvents', [])):
                        hd = pe.get('hitData')
                        if hd:
                            dist = hd.get('totalDistance')
                            ev_speed = hd.get('launchSpeed')
                            la = hd.get('launchAngle')
                            parts = []
                            if dist is not None:
                                parts.append(f"Distance: {int(dist)}ft")
                            if ev_speed is not None:
                                parts.append(f"Velocity: {ev_speed:.1f}mph")
                            if la is not None:
                                parts.append(f"Launch Angle: {la:.0f}°")
                            if parts:
                                hr_details = '  '.join(parts)
                            break

                play_entry = {
                    'inning': inning,
                    'half': half,
                    'event': event or '',
                    'description': desc or '',
                    'rbi': res.get('rbi', 0),
                    'away_score': res.get('awayScore', 0),
                    'home_score': res.get('homeScore', 0),
                    'is_scoring': about.get('isScoringPlay', False),
                    'is_action': False,
                    'is_challenge': about.get('hasReview', False),
                }
                if hr_details:
                    play_entry['hr_details'] = hr_details
                result.append(play_entry)

            # Build a live preview string from the latest event in currentPlay
            live_preview = ""
            live_count = {"balls": 0, "strikes": 0}
            cur_play = data.get('liveData', {}).get('plays', {}).get('currentPlay')
            if cur_play:
                # Extract current count
                cp_count = cur_play.get('count', {})
                live_count["balls"] = cp_count.get('balls', 0) or 0
                live_count["strikes"] = cp_count.get('strikes', 0) or 0

                cp_events = cur_play.get('playEvents', [])
                if cp_events:
                    last_ev = cp_events[-1]
                    det = last_ev.get('details', {})
                    if last_ev.get('type') == 'action':
                        ev = det.get('event', '')
                        desc = det.get('description', '')
                        live_preview = f"{ev}: {desc}" if ev and desc else (ev or desc)
                    elif last_ev.get('type') == 'pitch':
                        pitch_num = sum(1 for e in cp_events if e.get('type') == 'pitch')
                        desc = det.get('description', '')
                        call = det.get('call', {}).get('description', '')
                        pitch_type = det.get('type', {}).get('description', '')
                        pitch_speed = last_ev.get('pitchData', {}).get('startSpeed')
                        base = call or desc
                        # Ball in dirt that batter swung at is a strike, not a ball
                        if 'dirt' in base.lower() and det.get('isStrike'):
                            base = "Swinging (In Dirt)"
                        if pitch_type or pitch_speed:
                            extra = pitch_type or ''
                            if pitch_speed:
                                extra = f"{extra} - {pitch_speed} mph" if extra else f"{pitch_speed} mph"
                            live_preview = f"{base} ({extra})" if base else extra
                        else:
                            live_preview = base
                        live_preview = f"{pitch_num}. {live_preview}"
                        if det.get('hasReview'):
                            live_preview = f"Challenge: {live_preview}"

                # Check if the current at-bat has a completed challenge
                cp_about = cur_play.get('about', {})
                cp_result = cur_play.get('result', {})
                cp_desc = cp_result.get('description', '')
                if cp_about.get('hasReview') and cp_desc:
                    live_preview = cp_desc
                elif cp_about.get('isComplete') and cp_desc:
                    # At-bat is finished — show the detailed result instead
                    # of the raw pitch call (e.g. "Groundout: ..." vs "In Play, Out(s)")
                    live_preview = cp_desc

            # Enrich live_count with linescore data from the same response
            # so sidebar + schedule diamonds can stay in sync.
            linescore = data.get('liveData', {}).get('linescore', {})
            if linescore:
                live_count['outs'] = linescore.get('outs', 0) or 0
                offense = linescore.get('offense', {})
                live_count['on_first'] = bool(offense.get('first'))
                live_count['on_second'] = bool(offense.get('second'))
                live_count['on_third'] = bool(offense.get('third'))

            # ── Cache result for 8s (CDN max-age is 10s) to skip redundant requests ──
            feed_result = (result, live_preview, live_count)
            self._live_feed_cache[gid_str] = (_time.monotonic() + 8, feed_result)

            return result, live_preview, live_count
        except Exception:
            return [], "", {"balls": 0, "strikes": 0}

    def _fetch_player_handedness(self, player_id):
        """Fetch handedness for a single player from the people endpoint.
        
        Args:
            player_id: MLB player ID
            
        Returns:
            Handedness code: 'L', 'R', 'B', or 'Unknown'
        """
        # Check in-memory cache first
        with self._cache_lock:
            if player_id in self._handedness_cache:
                return self._handedness_cache[player_id]
        
        try:
            url = f"{self.api_base}/people/{player_id}"
            response = _session.get(url, timeout=_TIMEOUT_SHORT)
            if response.status_code != 200:
                return 'Unknown'
            people_data = response.json()
            people = people_data.get('people', [])
            if not people:
                return 'Unknown'
            person_profile = people[0]
            # Try batSide first (for batters), then pitchHand (for pitchers)
            if 'batSide' in person_profile:
                hand = person_profile['batSide'].get('code', 'U')
                # API uses 'S' for switch hitters, we display as 'B'
                result = 'L' if hand == 'L' else 'R' if hand == 'R' else 'B' if hand == 'S' else 'Unknown'
            elif 'pitchHand' in person_profile:
                hand = person_profile['pitchHand'].get('code', 'U')
                result = 'L' if hand == 'L' else 'R' if hand == 'R' else 'Unknown'
            else:
                result = 'Unknown'
            with self._cache_lock:
                self._handedness_cache[player_id] = result
            return result
        except Exception:
            pass
        return 'Unknown'

    def _fetch_pitcher_hand(self, player_id):
        """Fetch throwing hand for a pitcher from the People API.

        Returns 'L', 'R', or '' on failure.  Uses the same cache as
        ``_fetch_player_handedness`` to avoid duplicate HTTP calls.
        """
        with self._cache_lock:
            cached = self._handedness_cache.get(player_id)
        if cached and cached in ('L', 'R'):
            return cached
        try:
            url = f"{self.api_base}/people/{player_id}"
            response = _session.get(url, timeout=_TIMEOUT_SHORT)
            if response.status_code != 200:
                return ''
            people = response.json().get('people', [])
            if not people:
                return ''
            person = people[0]
            ph = person.get('pitchHand', {})
            code = ph.get('code', '') if ph else ''
            result = code if code in ('L', 'R') else ''
            if result:
                with self._cache_lock:
                    self._handedness_cache[player_id] = result
            return result
        except Exception:
            return ''

    def _fetch_person_name(self, player_id):
        """Fetch full name for a player from the People API.

        Returns the full name string, or '' on failure.
        """
        try:
            url = f"{self.api_base}/people/{player_id}"
            response = _session.get(url, timeout=_TIMEOUT_SHORT)
            if response.status_code != 200:
                return ''
            people = response.json().get('people', [])
            if not people:
                return ''
            return people[0].get('fullName', '')
        except Exception:
            return ''

    def _fetch_player_season_stats(self, player_id, season, group='batting'):
        """Fetch season aggregate stats for a player from MLB Stats API.

        Caches results in memory for the session.
        Returns the `stat` dict from the first split if available, else None.
        """
        if not player_id or not season:
            return None
        key = (player_id, group, season)
        cached = self._player_data_cache.get(key)
        if cached and ( _time.time() - cached.get('ts', 0) ) < 60*60:
            return cached.get('result')

        try:
            url = f"{self.api_base}/people/{player_id}/stats"
            params = {'stats': 'season', 'season': int(season), 'group': group}
            r = _session.get(url, params=params, timeout=_TIMEOUT_SHORT)
            r.raise_for_status()
            data = r.json()
            stats_list = data.get('stats', [])
            if stats_list:
                splits = stats_list[0].get('splits', [])
                if splits:
                    stat = splits[0].get('stat', {})
                    self._put_player_data_cache(key, {'ts': _time.time(), 'result': stat})
                    return stat
        except Exception:
            pass
        self._put_player_data_cache(key, {'ts': _time.time(), 'result': None})
        return None
    
    def get_lineup(self, game_id, force_fresh=False):
        """Fetch lineup for a game from MLB Stats API or return cached data.
        
        Checks temporary cache first (persists across restarts). If not found,
        fetches from API and stores in temporary cache. Automatically cleans up
        temporary cache entries once the game appears in the historical database.
        
        Args:
            game_id: MLB game ID
            force_fresh: If True, bypass cache and fetch fresh from API
        
        Returns:
            Dict with 'away' and 'home' keys, each containing list of tuples:
            (position, name, handedness, player_id)
        """
        game_id_str = str(game_id)
        
        # Run cleanup once per session (not on every call)
        if not self._cleanup_done:
            self._cleanup_done = True
            self._cleanup_temp_lineups()
        
        # Check if we have cached lineup for this game (unless force_fresh is True)
        if not force_fresh and game_id_str in self.temp_lineup_cache:
            if not self.suppress_output:
                _log.debug(f"Using cached lineup for game {game_id}")
            cached = self.temp_lineup_cache[game_id_str]
            # Ensure all cached players have 4 elements (position, name, handedness, player_id)
            # Convert old 3-element format to 4-element with handedness
            away_fixed = []
            for player in cached["away"]:
                if len(player) == 3:
                    # Old format: (position, name, player_id) -> add empty handedness
                    away_fixed.append((player[0], player[1], '', player[2]))
                elif len(player) == 4:
                    away_fixed.append(tuple(player))
                else:
                    away_fixed.append(tuple(player))
            
            home_fixed = []
            for player in cached["home"]:
                if len(player) == 3:
                    # Old format: (position, name, player_id) -> add empty handedness
                    home_fixed.append((player[0], player[1], '', player[2]))
                elif len(player) == 4:
                    home_fixed.append(tuple(player))
                else:
                    home_fixed.append(tuple(player))
            
            return {
                "away": away_fixed,
                "home": home_fixed
            }
        
        try:
            url = f"{self.api_base}/game/{game_id}/boxscore"
            _log.debug(f"Fetching lineup for game {game_id}...")
            response = _session.get(url, timeout=_TIMEOUT_DEFAULT)
            response.raise_for_status()
            boxscore = response.json()
            
            lineups = {"away": [], "home": []}
            
            for team_key in ["away", "home"]:
                try:
                    teams_data = boxscore.get('teams', {})
                    if team_key not in teams_data:
                        continue
                    
                    # Get batters (using batting order filter for starters)
                    starters = self._get_starters_from_boxscore(teams_data[team_key])
                    
                    # If we found starters, use them; otherwise use all roster players
                    if starters:
                        lineups[team_key] = starters
                        if not self.suppress_output:
                            _log.debug(f"  {team_key}: Found {len(starters)} starter(s)")
                    else:
                        # Fall back to all players with valid positions
                        players = teams_data[team_key].get('players', {})
                        for player_id_str, player_info in players.items():
                            player_entry = self._parse_player_info(player_id_str, player_info)
                            if player_entry:
                                # append battingOrder when available to keep parity with starters format
                                bo = player_info.get('battingOrder')
                                try:
                                    bo = int(bo) if bo is not None else None
                                except Exception:
                                    bo = None
                                entry_list = list(player_entry)
                                entry_list.append(bo)
                                lineups[team_key].append(tuple(entry_list))
                        if not self.suppress_output:
                            _log.debug(f"  {team_key}: No starters found, showing all roster ({len(lineups[team_key])} players)")
                    
                    # Also add pitchers (kept simple - unchanged from original logic)
                    pitchers = self._get_pitchers_from_boxscore(teams_data[team_key])
                    lineups[team_key].extend(pitchers)
                        
                except Exception as e:
                    if not self.suppress_output:
                        _log.warning(f"  Warning: Error processing {team_key} team: {e}")
            
            # Store lineup in temp cache (persists until game appears in database)
            self.temp_lineup_cache[game_id_str] = {
                "away": [list(p) for p in lineups["away"]],
                "home": [list(p) for p in lineups["home"]],
                "pitchers": {}  # Will be filled by get_starting_pitchers()
            }
            self._save_temp_lineup_cache()
            if not self.suppress_output:
                _log.debug(f"  Lineup cached temporarily for game {game_id}")
            
            return lineups
        except Exception as e:
                if not self.suppress_output:
                    _log.warning(f"Error fetching lineup: {e}")
    
    def _get_starters_from_boxscore(self, team_data):
        """Extract starting players from team boxscore data with parallel handedness fetching.
        
        Looks for players in the batting order (starters). Uses ThreadPoolExecutor to 
        fetch handedness data in parallel, significantly speeding up the process.
        
        Returns:
            List of tuples (position, name, handedness, player_id) sorted by batting order
        """
        starters = []
        batting_order_map = {}  # Store with batting order for sorting
        players_data = []  # Collect players for parallel handedness fetch
        
        # Use the 'players' dict which contains active players in the game
        players = team_data.get('players', {})
        
        if not players:
            return starters
        
        # First pass: collect player data and IDs
        for player_id_str, player_info in players.items():
            try:
                # Only include players who have a battingOrder (are in the lineup)
                if 'battingOrder' not in player_info:
                    continue
                
                batting_order = player_info.get('battingOrder')
                
                # Skip if missing position or stats
                if 'position' not in player_info or 'stats' not in player_info:
                    continue
                
                if player_info['position'].get('abbreviation') is None:
                    continue
                
                position = player_info['position']['abbreviation']
                person = player_info.get('person', {})
                name = person.get('fullName', '')
                
                if not name:
                    continue
                
                # Extract player ID
                try:
                    player_id = int(player_id_str.replace('ID', '')) if 'ID' in player_id_str else int(player_id_str)
                except (ValueError, TypeError):
                    player_id = None
                
                if player_id:
                    # normalize batting order to int when possible
                    try:
                        bo = int(batting_order) if batting_order is not None else None
                    except Exception:
                        bo = None
                    players_data.append((bo, position, name, player_id))
                    
            except Exception as e:
                continue
        
        # Second pass: fetch all handedness data in parallel
        handedness_map = {}  # Map player_id -> handedness
        
        if players_data:
            try:
                # Use ThreadPoolExecutor to fetch handedness in parallel (max 10 concurrent requests)
                with ThreadPoolExecutor(max_workers=min(10, len(players_data))) as executor:
                    future_to_player_id = {}
                    
                    try:
                        # Submit all tasks - may fail if interpreter is shutting down
                        for _, _, _, player_id in players_data:
                            future_to_player_id[executor.submit(self._fetch_player_handedness, player_id)] = player_id
                    except RuntimeError as e:
                        # "cannot schedule new futures after interpreter shutdown"
                        # Gracefully handle by just marking all as unknown
                        if "interpreter shutdown" in str(e):
                            if not self.suppress_output:
                                _log.debug(f"  Note: Handedness fetch interrupted by shutdown, using defaults")
                            for _, _, _, player_id in players_data:
                                handedness_map[player_id] = 'Unknown'
                            return []  # Return empty to trigger fallback to all roster
                        raise
                    
                    for future in as_completed(future_to_player_id):
                        player_id = future_to_player_id[future]
                        try:
                            handedness = future.result()
                            handedness_map[player_id] = handedness
                        except Exception as e:
                            handedness_map[player_id] = 'Unknown'
            except RuntimeError as e:
                # If ThreadPoolExecutor creation itself fails during shutdown, gracefully return empty
                if "interpreter shutdown" in str(e):
                    if not self.suppress_output:
                        _log.debug(f"  Note: Thread pool creation interrupted by shutdown")
                    return []  # Return empty to trigger fallback to all roster
                raise
        
        # Third pass: build result tuples with handedness data
        cache_dirty = False
        for batting_order, position, name, player_id in players_data:
            handedness = handedness_map.get(player_id, 'Unknown')
            
            # Cache player ID by name
            if player_id and name not in self.id_cache:
                self.id_cache[name] = player_id
                cache_dirty = True
            
            # include batting order in the returned tuple for callers that need it
            starter_tuple = (position, name, handedness, player_id, batting_order)
            batting_order_map[batting_order] = starter_tuple
        
        if cache_dirty:
            self._save_cache()
        
        # Sort by batting order and return as list
        for _, starter_tuple in sorted(batting_order_map.items()):
            starters.append(starter_tuple)
        
        return starters
    
    def _get_pitchers_from_boxscore(self, team_data):
        """Extract pitchers from team boxscore data with parallel handedness fetching.
        
        Extracts all pitchers without the battingOrder filter. Uses ThreadPoolExecutor 
        to fetch handedness in parallel for better performance.
        
        Returns:
            List of tuples (position, name, handedness, player_id) for all pitchers
        """
        pitchers = []
        players_data = []  # Collect pitchers for parallel handedness fetch
        players = team_data.get('players', {})
        
        if not players:
            return pitchers
        
        # First pass: collect pitcher data and IDs
        for player_id_str, player_info in players.items():
            try:
                # Skip if missing position or stats
                if 'position' not in player_info or 'stats' not in player_info:
                    continue
                
                position = player_info['position'].get('abbreviation')
                if position != 'P':  # Only pitchers
                    continue
                
                person = player_info.get('person', {})
                name = person.get('fullName', '')
                
                if not name:
                    continue
                
                # Extract player ID
                try:
                    player_id = int(player_id_str.replace('ID', '')) if 'ID' in player_id_str else int(player_id_str)
                except (ValueError, TypeError):
                    player_id = None
                
                if player_id:
                    players_data.append((position, name, player_id))
                    
            except Exception as e:
                continue
        
        # Second pass: fetch all handedness data in parallel
        handedness_map = {}  # Map player_id -> handedness
        
        if players_data:
            try:
                # Use ThreadPoolExecutor to fetch handedness in parallel
                with ThreadPoolExecutor(max_workers=min(10, len(players_data))) as executor:
                    future_to_player_id = {}
                    
                    try:
                        # Submit all tasks - may fail if interpreter is shutting down
                        for _, _, player_id in players_data:
                            future_to_player_id[executor.submit(self._fetch_player_handedness, player_id)] = player_id
                    except RuntimeError as e:
                        # "cannot schedule new futures after interpreter shutdown"
                        # Gracefully handle by just marking all as unknown
                        if "interpreter shutdown" in str(e):
                            if not self.suppress_output:
                                _log.debug(f"  Note: Handedness fetch interrupted by shutdown, using defaults")
                            for _, _, player_id in players_data:
                                handedness_map[player_id] = 'Unknown'
                        else:
                            raise
                    
                    for future in as_completed(future_to_player_id):
                        player_id = future_to_player_id[future]
                        try:
                            handedness = future.result()
                            handedness_map[player_id] = handedness
                        except Exception as e:
                            handedness_map[player_id] = 'Unknown'
            except RuntimeError as e:
                # If ThreadPoolExecutor creation itself fails during shutdown, gracefully continue with unknowns
                if "interpreter shutdown" in str(e):
                    if not self.suppress_output:
                        _log.debug(f"  Note: Thread pool creation interrupted by shutdown")
                    for _, _, player_id in players_data:
                        handedness_map[player_id] = 'Unknown'
                else:
                    raise
        
        # Third pass: build result tuples with handedness data
        cache_dirty = False
        for position, name, player_id in players_data:
            handedness = handedness_map.get(player_id, 'Unknown')
            
            # Cache player ID by name
            if player_id and name not in self.id_cache:
                self.id_cache[name] = player_id
                cache_dirty = True
            
            pitcher_tuple = (position, name, handedness, player_id)
            pitchers.append(pitcher_tuple)
        
        if cache_dirty:
            self._save_cache()
        
        return pitchers
    
    def _parse_player_info(self, player_id_str, player_info):
        """Parse individual player information from boxscore.
        
        Returns:
            Tuple (position, name, handedness, player_id) or None if invalid
        """
        try:
            # Skip players not in the game
            if 'position' not in player_info or player_info['position'].get('abbreviation') is None:
                return None
            if 'stats' not in player_info:
                return None
            
            position = player_info['position']['abbreviation']
            name = player_info['person']['fullName']
            
            # Extract player ID (format: "ID123456")
            try:
                player_id = int(player_id_str.replace('ID', ''))
            except (ValueError, TypeError):
                player_id = None
            
            # Extract handedness
            handedness = 'Unknown'
            person = player_info.get('person', {})
            player_id = person.get('id')
            
            # Reuse the shared handedness fetcher (with caching)
            if player_id:
                handedness = self._fetch_player_handedness(player_id)
            
            # Cache player ID by name
            if player_id and name not in self.id_cache:
                self.id_cache[name] = player_id
                self._save_cache()
            
            return (position, name, handedness, player_id)
        except Exception as e:
            _log.warning(f"  Warning: Error parsing player: {e}")
            return None
    
    def get_player_id(self, name):
        """Lookup player ID by name with caching.
        
        Args:
            name: Player full name
        
        Returns:
            Player ID (int) or None if not found
        """
        # Check cache first
        if name in self.id_cache:
            return self.id_cache[name]
        
        try:
            clean_name = name.strip()
            
            # Remove suffixes (Jr., Sr., III, etc.)
            suffixes = [' Jr.', ' Sr.', ' III', ' II', ' IV', ' V']
            for suffix in suffixes:
                clean_name = clean_name.replace(suffix, '')
            
            parts = clean_name.split()
            
            if len(parts) >= 2:
                first = parts[0]
                last = parts[-1]
                _log.debug(f"  Looking up: {first} {last}")
                
                try:
                    from pybaseball import playerid_lookup
                except Exception:
                    lookup = None
                else:
                    lookup = playerid_lookup(last, first)
                if not lookup.empty:
                    player_id = lookup.iloc[0]['key_mlbam']
                    self.id_cache[name] = player_id
                    self._save_cache()
                    return player_id
                
                # Try alternate parsing for 3+ part names
                if len(parts) >= 3:
                    first = parts[0]
                    last = f"{parts[-2]} {parts[-1]}"
                    try:
                        from pybaseball import playerid_lookup
                    except Exception:
                        lookup = None
                    else:
                        lookup = playerid_lookup(last, first)
                    if lookup is not None and not lookup.empty:
                        player_id = lookup.iloc[0]['key_mlbam']
                        self.id_cache[name] = player_id
                        self._save_cache()
                        return player_id
        except Exception as e:
            _log.warning(f"  Error looking up player {name}: {e}")
        
        return None
    
    def get_starting_pitchers(self, game_id, force_fresh=False):
        """Fetch starting pitchers for a game from MLB Stats API.
        
        For upcoming/pre-game: uses probable pitchers from schedule API (most reliable).
        For live/completed: uses boxscore data.
        Falls back between sources as needed.
        Uses in-memory cache with 60s TTL for live games, longer for others.
        
        Args:
            game_id: MLB game ID
            force_fresh: If True, bypass cache and fetch fresh from API
        
        Returns:
            Dict with 'away' and 'home' keys containing pitcher names (str), or "TBD" if not confirmed
        """
        game_id_str = str(game_id)
        
        # Check in-memory SP cache first (avoids all API calls on repeated access)
        if not force_fresh and game_id_str in self._sp_cache:
            cached = self._sp_cache[game_id_str]
            age = _time.monotonic() - cached['ts']
            # 60s TTL for live/upcoming, indefinite for completed
            if age < 60 or cached.get('final'):
                return {"away": cached["away"], "home": cached["home"]}
        
        try:
            # First, check the game status to see if we need fresh data
            game_status = self._get_game_status(game_id)
            is_live_or_upcoming = game_status in ['Live', 'Pre-Game', 'Scheduled', 'In Progress']
            game_not_started = game_status in ['Pre-Game', 'Scheduled']
            
            # For completed games, use cache if available
            if not force_fresh and not is_live_or_upcoming and game_id_str in self.temp_lineup_cache:
                cached_pitchers = self.temp_lineup_cache[game_id_str].get('pitchers', {})
                if cached_pitchers:
                    if not self.suppress_output:
                        _log.debug(f"Using cached starting pitchers for game {game_id}")
                    # Also populate in-memory cache for fast repeated access
                    self._sp_cache[game_id_str] = {"away": cached_pitchers.get("away", "TBD"), "home": cached_pitchers.get("home", "TBD"), "ts": _time.monotonic(), "final": True}
                    return cached_pitchers
            
            pitchers = {"away": "TBD", "home": "TBD"}
            
            # For games that haven't started, prefer probable pitchers from schedule API
            # (boxscore can return wrong pitchers via fallback methods for upcoming games)
            pp_data = self.probable_pitchers.get(game_id_str, {})
            if game_not_started:
                for team_key in ["away", "home"]:
                    if pp_data.get(team_key):
                        pitchers[team_key] = pp_data[team_key]["name"]
                # If we got both from probable pitchers, we're done
                if pitchers["away"] != "TBD" and pitchers["home"] != "TBD":
                    self._sp_cache[game_id_str] = {"away": pitchers["away"], "home": pitchers["home"], "ts": _time.monotonic(), "final": False}
                    if game_id_str in self.temp_lineup_cache:
                        self.temp_lineup_cache[game_id_str]['pitchers'] = pitchers
                        self._save_temp_lineup_cache()
                    return pitchers
            
            # Fetch boxscore for live/completed games, or as fallback for upcoming
            url = f"{self.api_base}/game/{game_id}/boxscore"
            response = _session.get(url, timeout=_TIMEOUT_DEFAULT)
            response.raise_for_status()
            boxscore = response.json()
            
            for team_key in ["away", "home"]:
                if pitchers[team_key] != "TBD":
                    continue  # Already have from probable pitchers
                try:
                    teams_data = boxscore.get('teams', {})
                    if team_key not in teams_data:
                        continue
                    
                    starting_pitcher = self._find_starting_pitcher(teams_data[team_key])
                    if starting_pitcher:
                        pitchers[team_key] = starting_pitcher
                        
                except Exception as e:
                    if not self.suppress_output:
                        _log.warning(f"  Warning: Error processing {team_key} team pitchers: {e}")
            
            # Last fallback: use probable pitchers for any remaining TBDs
            for team_key in ["away", "home"]:
                if pitchers[team_key] == "TBD" and pp_data.get(team_key):
                    pitchers[team_key] = pp_data[team_key]["name"]
            
            # Cache in memory and on disk
            is_final = game_status in ['Final', 'Game Over', 'Completed Early']
            self._sp_cache[game_id_str] = {"away": pitchers["away"], "home": pitchers["home"], "ts": _time.monotonic(), "final": is_final}
            if game_id_str in self.temp_lineup_cache:
                self.temp_lineup_cache[game_id_str]['pitchers'] = pitchers
                self._save_temp_lineup_cache()
            
            return pitchers
            
        except Exception as e:
            if not self.suppress_output:
                _log.warning(f"Error fetching starting pitchers for game {game_id}: {e}")
            # Still try probable pitchers as last resort
            pitchers = {"away": "TBD", "home": "TBD"}
            pp_data = self.probable_pitchers.get(game_id_str, {})
            for team_key in ["away", "home"]:
                if pp_data.get(team_key):
                    pitchers[team_key] = pp_data[team_key]["name"]
            if pitchers["away"] != "TBD" or pitchers["home"] != "TBD":
                self._sp_cache[game_id_str] = {"away": pitchers["away"], "home": pitchers["home"], "ts": _time.monotonic(), "final": False}
            return pitchers
    
    def _get_game_status(self, game_id):
        """Get current game status (Live, Pre-Game, Scheduled, Final, etc.)
        
        Caches results for 30 seconds to avoid redundant API calls.
        """
        now = _time.monotonic()
        cached = self._game_status_cache.get(game_id)
        if cached:
            status, ts = cached
            if now - ts < 30:  # 30-second TTL
                return status
        
        try:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
            response = _session.get(url, timeout=_TIMEOUT_SHORT)
            if response.status_code == 200:
                game_data = response.json()
                status = game_data.get('gameData', {}).get('status', {}).get('detailedState', '')
                self._game_status_cache[game_id] = (status, now)
                return status
        except Exception:
            pass
        return ''
    
    def _find_starting_pitcher(self, team_data):
        """Find the starting pitcher for a team using the pitchers array.
        
        The boxscore data includes a 'pitchers' list at the team level where the
        first pitcher ID is the starting pitcher. This is the most reliable method.
        
        Returns:
            Formatted pitcher name (e.g., "J. Smith") or None if not found
        """
        pitcher_ids = team_data.get('pitchers', [])
        players = team_data.get('players', {})
        
        if not players:
            return None
        
        # Method 1: Use the pitchers array (most reliable)
        # The first pitcher ID in the array is the starting pitcher
        if pitcher_ids:
            starting_pitcher_id = pitcher_ids[0]
            
            # Find this pitcher in the players dict
            for player_id_str, player_info in players.items():
                player_id = player_info.get('person', {}).get('id')
                if player_id == starting_pitcher_id:
                    name = player_info.get('person', {}).get('fullName', 'Unknown')
                    return self._format_pitcher_name(name)
        
        # Method 2: Fallback - Look for a pitcher with inningsPitched > 0 (already pitched in this game)
        for player_id_str, player_info in players.items():
            pos = player_info.get('position', {}).get('abbreviation')
            if pos != 'P':
                continue
                
            stats = player_info.get('stats', {}).get('pitching', {})
            innings_pitched = stats.get('inningsPitched')
            
            # If pitcher has innings pitched, likely the starter
            if innings_pitched and (isinstance(innings_pitched, (int, float)) and innings_pitched > 0):
                name = player_info.get('person', {}).get('fullName', 'Unknown')
                return self._format_pitcher_name(name)
        
        # Method 3: Last resort - return first pitcher if nothing else worked
        for player_id_str, player_info in players.items():
            pos = player_info.get('position', {}).get('abbreviation')
            if pos == 'P':
                name = player_info.get('person', {}).get('fullName', 'Unknown')
                return self._format_pitcher_name(name)
        
        return None
    
    def _format_pitcher_name(self, name):
        """Format pitcher name as 'F. LastName'"""
        name_parts = name.split()
        if name_parts:
            first_initial = name_parts[0][0].upper()
            last_name = name_parts[-1]
            return f"{first_initial}. {last_name}"
        return name
    
    def get_player_data_with_fallback(self, player_id, is_pitcher=False, year=None, min_year=2020):
        """Fetch statcast data for a player from database.
        
        Args:
            player_id: MLB player ID
            is_pitcher: If True, fetch pitcher data
            year: Season year (current year if None)
            min_year: Unused (kept for backward compatibility)
        
        Returns:
            Dict with 'df' (DataFrame), 'handedness', and 'actual_year' keys
        """
        import pandas as pd
        if year is None:
            year = datetime.now().year

        result = {"df": pd.DataFrame(), "handedness": "Unknown", "actual_year": year}
        
        # Fetch data for the requested year (will be empty if year != 2025)
        player_result = self.get_player_data(player_id, is_pitcher=is_pitcher, year=year)
        
        if player_result and not player_result["df"].empty:
            result["df"] = player_result["df"]
            result["handedness"] = player_result["handedness"]
            result["actual_year"] = year
        
        return result
    
    def get_player_data(self, player_id, is_pitcher=False, year=None):
        """Fetch statcast data for a player from the database.
        
        Args:
            player_id: MLB player ID
            is_pitcher: If True, fetch pitcher data
            year: Season year (current year if None)
        
        Returns:
            Dict with 'df' (DataFrame) and 'handedness' keys
        """
        import pandas as pd
        if year is None:
            year = datetime.now().year
        
        try:
            # Simple in-memory cache to avoid repeated DB reads for the same player/year
            try:
                key = (int(player_id), bool(is_pitcher), int(year) if year is not None else datetime.now().year)
            except Exception:
                key = None
            if key is not None and key in self._player_data_cache:
                entry = self._player_data_cache[key]
                if _time.monotonic() - entry['ts'] < 30:  # 30s TTL
                    return entry['result']
            # Query the raw database for player data
            conn = self._get_db_connection()
            if conn is None:
                return {"df": pd.DataFrame(), "handedness": "Unknown"}
            
            # Build query based on whether we're looking for pitcher or batter
            if is_pitcher:
                query = """
                    SELECT * FROM plate_appearances 
                    WHERE pitcher_id = ? AND season = ?
                    ORDER BY game_datetime
                """
                handedness_col = 'p_throws'
            else:
                query = """
                    SELECT * FROM plate_appearances 
                    WHERE batter_id = ? AND season = ?
                    ORDER BY game_datetime
                """
                handedness_col = 'stand'
            
            df = pd.read_sql_query(query, conn, params=(int(player_id), year))
            
            # Extract handedness from the data
            handedness = "Unknown"
            if not df.empty and handedness_col in df.columns:
                # Get the most common handedness value (excluding NaN/None)
                valid_values = df[df[handedness_col].notna()][handedness_col].unique()
                if len(valid_values) > 0:
                    handedness = valid_values[0]
            
            result = {"df": df, "handedness": handedness}
            if key is not None:
                self._put_player_data_cache(key, {'ts': _time.monotonic(), 'result': result})
            return result
        
        except Exception as e:
            _log.warning(f"Error fetching player data for {player_id} (year {year}): {e}")
            return {"df": pd.DataFrame(), "handedness": "Unknown"}
    
    def _lookup_handedness(self, player_id, is_pitcher=False):
        """Get handedness for a player from MLB Stats API.
        
        Args:
            player_id: MLB player ID
            is_pitcher: If True, look up pitching hand; otherwise batting hand
        
        Returns:
            'L', 'R', 'S', or 'Unknown'
        """
        try:
            url = f"{self.api_base}/people/{player_id}"
            response = _session.get(url, timeout=_TIMEOUT_DEFAULT)
            response.raise_for_status()
            data = response.json()
            
            if 'people' not in data or not data['people']:
                return 'Unknown'
            
            person = data['people'][0]
            
            if is_pitcher:
                hand = person.get('pitchHand', {}).get('code', 'U')
            else:
                hand = person.get('batSide', {}).get('code', 'U')
            
            if hand == 'L':
                return 'L'
            elif hand == 'R':
                return 'R'
            else:
                return 'Unknown'
        except Exception as e:
            _log.warning(f"  Error getting handedness for player {player_id}: {e}")
            return 'Unknown'
    
    def calculate_pitching_stats(self, df):
        """Calculate K, H, HR, BB%, ERA, IP, and hit breakdowns for pitchers.
        
        Args:
            df: DataFrame with statcast data
        
        Returns:
            Dict with pitcher stats
        """
        import pandas as pd
        if df is None or df.empty:
            return {stat: "---" for stat in ['IP', 'K', 'H', 'HR', '1B', '2B', '3B', 'BB%', 'ERA']}
        
        pa = len(df[df['events'].notnull()])
        ks = len(df[df['events'] == 'strikeout'])
        hits = len(df[df['events'].isin(['single', 'double', 'triple', 'home_run'])])
        hrs = len(df[df['events'] == 'home_run'])
        singles = len(df[df['events'] == 'single'])
        doubles = len(df[df['events'] == 'double'])
        triples = len(df[df['events'] == 'triple'])
        walks = len(df[df['events'].isin(['walk', 'intent_walk'])])
        hbps = len(df[df['events'] == 'hit_by_pitch']) if 'events' in df.columns else 0
        sfs = len(df[df['events'] == 'sacrifice_fly']) if 'events' in df.columns else 0
        
        # Calculate walk %
        bb_pct = (walks / pa * 100) if pa > 0 else 0
        # Calculate strikeout % (K% = KS / PA * 100)
        k_pct = (ks / pa * 100) if pa > 0 else 0
        
        # Compute outs from event types rather than summing 'outs_when_up'
        # which represents outs before the PA, not outs recorded. Use an
        # event-based mapping: single-out events count as 1, double-play
        # events count as 2.
        ev_counts = df['events'].value_counts() if 'events' in df.columns else {}
        # Broaden event mapping to include all common out-producing event labels.
        # This helps ensure outs are counted accurately across different
        # data sources which may use slightly different event names.
        single_out_events = [
            'field_out', 'strikeout', 'force_out', 'fielders_choice',
            'runner_out', 'batter_interference', 'pop_out', 'line_out',
            'fly_out', 'ground_out', 'sac_bunt', 'sacrifice_bunt',
            'sac_fly', 'sacrifice_fly'
        ]
        double_out_events = [
            'grounded_into_double_play', 'double_play', 'strikeout_double_play'
        ]

        single_out_count = sum(int(ev_counts.get(ev, 0)) for ev in single_out_events)
        double_play_count = sum(int(ev_counts.get(ev, 0)) for ev in double_out_events)
        # Total outs recorded (approx): singles + 2 * double plays
        total_outs_recorded = single_out_count + (2 * double_play_count)

        # Heuristic: some plays are labeled as 'field_error' but also
        # produce an out (e.g., runner thrown out, tag play). Inspect the
        # description for common out-indicating phrases and count those as
        # additional outs.
        additional_outs = 0
        try:
            if 'events' in df.columns and 'description' in df.columns:
                error_rows = df[df['events'] == 'field_error']
                if not error_rows.empty:
                    out_pattern = '|'.join([
                        'out at', 'thrown out', 'tag out', 'caught stealing',
                        'picked off', 'double play', 'double-play', 'force out',
                        "fielder's choice out", 'fielders choice out', 'out,',
                        r'out\.'
                    ])
                    descs = error_rows['description'].fillna('').str.lower()
                    additional_outs += int(descs.str.contains(out_pattern, regex=True).sum())
                    # As a final heuristic, if a fielding error entry shows the
                    # batter did NOT record a hit (`is_hit` falsy) but the
                    # `outs_when_up` value is > 0, it's likely an out occurred
                    # elsewhere on the play (e.g., runner thrown out). Count
                    # these as outs as well to reconcile with official summaries.
                    try:
                        no_hit = error_rows['is_hit'].fillna(0).astype(bool) == False
                        outs_up = pd.to_numeric(error_rows['outs_when_up'], errors='coerce').fillna(0).astype(int)
                        additional_outs += int((no_hit & (outs_up > 0)).sum())
                    except Exception:
                        pass
        except Exception:
            additional_outs = 0

        total_outs_recorded += additional_outs

        # Optional: reconcile against MLB season totals (opt-in via env).
        # When enabled, query MLB Stats for the pitcher's season innings and
        # prefer that total if it indicates we've undercounted outs by a
        # small margin (guard: <= 5 outs). This is conservative and
        # avoids changing behavior unless explicitly requested.
        try:
            import os
            if os.environ.get('MLB_RECONCILE') in ('1', 'true', 'True'):
                # derive season and pitcher id from the DataFrame when possible
                season = None
                if 'season' in df.columns and not df['season'].dropna().empty:
                    try:
                        season = int(df['season'].max())
                    except Exception:
                        season = None
                if season is None:
                    try:
                        from datetime import datetime as _dt
                        season = _dt.now().year
                    except Exception:
                        season = None

                pitcher_id = None
                if 'pitcher_id' in df.columns and not df['pitcher_id'].dropna().empty:
                    try:
                        pitcher_id = int(df['pitcher_id'].iloc[0])
                    except Exception:
                        pitcher_id = None

                if pitcher_id and season:
                    try:
                        url = f"{self.api_base}/people/{pitcher_id}/stats"
                        params = {'stats': 'season', 'season': season, 'group': 'pitching'}
                        r = _session.get(url, params=params, timeout=_TIMEOUT_SHORT)
                        r.raise_for_status()
                        data = r.json()
                        stats_list = data.get('stats', [])
                        if stats_list:
                            splits = stats_list[0].get('splits', [])
                            if splits:
                                s = splits[0].get('stat', {})
                                mlb_ip = s.get('inningsPitched') or s.get('inningsPitched')
                                if mlb_ip:
                                    # convert MLB innings string (.1/.2 => thirds)
                                    try:
                                        sip = str(mlb_ip)
                                        parts = sip.split('.')
                                        whole = int(parts[0])
                                        frac = parts[1] if len(parts) > 1 else '0'
                                        if frac in ('0', '00'):
                                            mlb_ip_float = float(whole)
                                        elif frac.startswith('1'):
                                            mlb_ip_float = whole + (1.0/3.0)
                                        elif frac.startswith('2'):
                                            mlb_ip_float = whole + (2.0/3.0)
                                        else:
                                            mlb_ip_float = float(sip)
                                    except Exception:
                                        mlb_ip_float = None

                                    if isinstance(mlb_ip_float, (int, float)) and mlb_ip_float > 0:
                                        mlb_outs = int(round(mlb_ip_float * 3))
                                        # Only accept small corrections to avoid overfitting
                                        if mlb_outs > total_outs_recorded and (mlb_outs - total_outs_recorded) <= 5:
                                            total_outs_recorded = mlb_outs
                    except Exception:
                        pass
        except Exception:
            pass

        # If we have an explicit outs_when_up column and it seems to match
        # expectations (i.e., roughly equals total_outs_recorded), prefer
        # event-based total as it's a direct count of outs produced.
        estimated_ip = total_outs_recorded / 3.0 if total_outs_recorded > 0 else 0.0
        
        # Estimate earned runs: hits and HRs are typically earned
        estimated_er = hits + hrs
        era = (estimated_er * 9) / estimated_ip if estimated_ip > 0 else 0

        # Try to use MLB API season aggregates as authoritative when available
        try:
            pitcher_id = None
            season = None
            if 'pitcher_id' in df.columns and not df['pitcher_id'].dropna().empty:
                try:
                    pitcher_id = int(df['pitcher_id'].iloc[0])
                except Exception:
                    pitcher_id = None
            if 'season' in df.columns and not df['season'].dropna().empty:
                try:
                    season = int(df['season'].max())
                except Exception:
                    season = None

            mlb_stat = None
            if pitcher_id and season:
                mlb_stat = self._fetch_player_season_stats(pitcher_id, season, group='pitching')

            if mlb_stat:
                # MLB provides strikeouts and inningsPitched (string like 12.1)
                try:
                    mlb_k = int(mlb_stat.get('strikeouts')) if mlb_stat.get('strikeouts') is not None else None
                except Exception:
                    mlb_k = None
                mlb_ip_val = mlb_stat.get('inningsPitched') or mlb_stat.get('innings') or mlb_stat.get('inningsPitched')
                mlb_ip_float = None
                if mlb_ip_val is not None:
                    try:
                        sip = str(mlb_ip_val)
                        parts = sip.split('.')
                        whole = int(parts[0])
                        frac = parts[1] if len(parts) > 1 else '0'
                        if frac in ('0', '00'):
                            mlb_ip_float = float(whole)
                        elif frac.startswith('1'):
                            mlb_ip_float = whole + (1.0/3.0)
                        elif frac.startswith('2'):
                            mlb_ip_float = whole + (2.0/3.0)
                        else:
                            mlb_ip_float = float(sip)
                    except Exception:
                        mlb_ip_float = None

                # Override K and IP when MLB values are present
                if isinstance(mlb_k, int) and mlb_k >= 0:
                    ks = mlb_k
                if isinstance(mlb_ip_float, float) and mlb_ip_float >= 0:
                    estimated_ip = mlb_ip_float

        except Exception:
            pass

        # Compute K/9
        k_per9 = (ks / estimated_ip * 9) if estimated_ip > 0 else 0

        return {
            'PA': pa,
            'IP': estimated_ip,
            'K': ks,
            'K%': k_pct,
            'K/9': k_per9,
            'H': hits,
            'HR': hrs,
            '1B': singles,
            '2B': doubles,
            '3B': triples,
            'BB%': bb_pct,
            'ERA': era
        }
    
    def get_stats_breakdown(self, df, is_pitcher=False, time_period="Overall", split="Overall"):
        """Calculate stats for a player (pitcher or batter).
        
        Args:
            df: DataFrame with statcast data
            is_pitcher: If True, calculate pitcher stats; otherwise batter stats
            time_period: 'Overall', 'Last 5', 'Last 10', 'Last 20', 'Last 30'
            split: 'Overall', 'vs LHP/LHB', 'vs RHP/RHB'
        
        Returns:
            Dict with stats
        """
        import pandas as pd
        if df is None or df.empty:
            if is_pitcher:
                return {stat: "---" for stat in ['PA', 'K', 'K%', 'H', 'HR', '1B', '2B', '3B', 'BB%', 'ERA']}
            else:
                return {stat: "---" for stat in ['PA', 'BA', 'ISO', 'K%', 'BB%', '1B', '2B', '3B', 'HR', 'Barrel%', 'Pull%', 'EV']}
        
        filtered_df = df
        if time_period != "Season":
            all_dates = sorted(df['game_date'].unique())
            n = {'Last 5': 5, 'Last 10': 10, 'Last 20': 20, 'Last 30': 30}.get(time_period)
            if n is not None:
                filtered_df = df[df['game_date'].isin(all_dates[-n:])]
        
        if split == "vs LHP/LHB":
            if is_pitcher:
                filtered_df = filtered_df[filtered_df['stand'] == 'L']
            else:
                filtered_df = filtered_df[filtered_df['p_throws'] == 'L']
        elif split == "vs RHP/RHB":
            if is_pitcher:
                filtered_df = filtered_df[filtered_df['stand'] == 'R']
            else:
                filtered_df = filtered_df[filtered_df['p_throws'] == 'R']
        
        if is_pitcher:
            return self.calculate_pitching_stats(filtered_df)
        else:
            return self.calculate_batting_stats(filtered_df)
    
    def calculate_batting_stats(self, df):
        """Calculate PA, BA, ISO, K%, BB%, hit breakdowns for batters.
        
        Args:
            df: DataFrame with statcast data
        
        Returns:
            Dict with batter stats
        """
        import pandas as pd
        if df is None or df.empty:
            return {stat: "---" for stat in ['PA', 'BA', 'ISO', 'K%', 'BB%', '1B', '2B', '3B', 'HR', 'Barrel%', 'Pull%', 'EV']}

        pa = len(df[df['events'].notnull()])
        at_bats = df[df['events'].notnull() & ~df['events'].isin(['walk', 'hit_by_pitch', 'sacrifice_fly', 'sacrifice_bunt'])]
        ab_count = len(at_bats)

        singles = len(df[df['events'] == 'single'])
        doubles = len(df[df['events'] == 'double'])
        triples = len(df[df['events'] == 'triple'])
        hrs = len(df[df['events'] == 'home_run'])
        hits_count = singles + doubles + triples + hrs

        ba = hits_count / ab_count if ab_count > 0 else 0
        slg = (singles + 2*doubles + 3*triples + 4*hrs) / ab_count if ab_count > 0 else 0
        iso = slg - ba

        ks = len(df[df['events'] == 'strikeout'])
        bbs = len(df[df['events'].isin(['walk', 'intent_walk'])])
        k_pct = (ks / pa * 100) if pa > 0 else 0
        bb_pct = (bbs / pa * 100) if pa > 0 else 0

        if 'barrel' in df.columns:
            barrels = df['barrel'].sum()
        else:
            barrels = len(df[(df['launch_speed'] >= 98) & (df['launch_angle'] >= 26) & (df['launch_angle'] <= 30)])
        
        batted_balls = df[df['launch_speed'].notnull()]
        barrel_pct = (barrels / len(batted_balls) * 100) if len(batted_balls) > 0 else 0

        pulls = 0
        pull_pct = 0
        if len(batted_balls) > 0 and 'hc_x' in df.columns and 'stand' in df.columns:
            valid = batted_balls.dropna(subset=['hc_x', 'stand'])
            if len(valid) > 0:
                is_right = valid['stand'] == 'R'
                pulls = int(((is_right & (valid['hc_x'] < 100)) | (~is_right & (valid['hc_x'] > 150))).sum())
            pull_pct = (pulls / len(batted_balls) * 100) if len(batted_balls) > 0 else 0
        
        ev = df['launch_speed'].mean() if not df['launch_speed'].dropna().empty else 0

        # Try to use MLB API season aggregates for batters when available
        try:
            batter_id = None
            season = None
            if 'batter_id' in df.columns and not df['batter_id'].dropna().empty:
                try:
                    batter_id = int(df['batter_id'].iloc[0])
                except Exception:
                    batter_id = None
            if 'season' in df.columns and not df['season'].dropna().empty:
                try:
                    season = int(df['season'].max())
                except Exception:
                    season = None

            mlb_stat = None
            if batter_id and season:
                mlb_stat = self._fetch_player_season_stats(batter_id, season, group='batting')

            if mlb_stat:
                # map common keys: hits, r, rbi, onBasePercentage, avg
                try:
                    mlb_hits = int(mlb_stat.get('hits')) if mlb_stat.get('hits') is not None else None
                except Exception:
                    mlb_hits = None
                try:
                    mlb_runs = int(mlb_stat.get('runs')) if mlb_stat.get('runs') is not None else None
                except Exception:
                    mlb_runs = None
                try:
                    mlb_rbi = int(mlb_stat.get('rbi')) if mlb_stat.get('rbi') is not None else None
                except Exception:
                    mlb_rbi = None
                try:
                    mlb_obp = float(mlb_stat.get('onBasePercentage') or mlb_stat.get('onBasePct') or mlb_stat.get('obp')) if (mlb_stat.get('onBasePercentage') or mlb_stat.get('onBasePct') or mlb_stat.get('obp')) is not None else None
                except Exception:
                    mlb_obp = None

                if isinstance(mlb_hits, int) and mlb_hits >= 0:
                    hits_count = mlb_hits
                if isinstance(mlb_runs, int) and mlb_runs >= 0:
                    runs = mlb_runs
                else:
                    runs = mlb_stat.get('runs') if mlb_stat else None
                if isinstance(mlb_rbi, int) and mlb_rbi >= 0:
                    rbi = mlb_rbi
                else:
                    rbi = mlb_stat.get('rbi') if mlb_stat else None
                if isinstance(mlb_obp, float):
                    obp = mlb_obp

        except Exception:
            pass

        # Ensure variables exist for return
        try:
            runs
        except NameError:
            runs = None
        try:
            rbi
        except NameError:
            rbi = None
        try:
            obp
        except NameError:
            obp = None

        return {
            'PA': pa,
            'BA': ba,
            'ISO': iso,
            'K%': k_pct,
            'BB%': bb_pct,
            '1B': singles,
            '2B': doubles,
            '3B': triples,
            'HR': hrs,
            'H': hits_count,
            'R': runs,
            'RBI': rbi,
            'OBP': obp,
            'Barrel%': barrel_pct,
            'Pull%': pull_pct,
            'EV': ev
        }
    
    def mark_date_processed(self, date_str):
        """Mark a date as processed (archived) so games from previous days won't be fetched.
        
        Args:
            date_str: Date string in format 'YYYY-MM-DD'
        """
        self.processed_dates.add(date_str)
        self._save_processed_dates()
        _log.debug(f"Date {date_str} marked as processed")
    
    def is_date_processed(self, date_str):
        """Check if a date has been processed (archived).
        
        Args:
            date_str: Date string in format 'YYYY-MM-DD'
            
        Returns:
            True if date has been processed, False otherwise
        """
        return date_str in self.processed_dates
    
    def get_batter_stats(self, player_id, year=None, time_period="Overall", matchup="Both"):
        """Get calculated batting stats for a specific batter.
        
        Args:
            player_id: MLB player ID
            year: Optional year to filter data (defaults to current season)
            time_period: 'Overall', 'Last 5', 'Last 10', 'Last 20', 'Last 30'
            matchup: 'Both', 'RHP', 'LHP' for pitcher handedness matchup
        
        Returns:
            Dict with batting stats: PA, BA, ISO, K%, BB%, 1B, 2B, 3B, HR, Barrel%, Pull%, EV
        """
        try:
            # Get player data using existing method - returns dict with 'df' and 'handedness'
            player_data = self.get_player_data(player_id, is_pitcher=False, year=year)
            df = player_data.get('df')
            
            if df is None or df.empty:
                return {stat: '--' for stat in ['PA', 'BA', 'ISO', 'K%', 'BB%', '1B', '2B', '3B', 'HR', 'Barrel%', 'Pull%', 'EV']}
            
            # Convert matchup format from UI values to get_stats_breakdown format
            # UI: 'Both', 'RHP', 'LHP' → breakdown: 'Overall', 'vs RHP/RHB', 'vs LHP/LHB'
            split_filter = 'Overall'
            if matchup == 'RHP':
                split_filter = 'vs RHP/RHB'
            elif matchup == 'LHP':
                split_filter = 'vs LHP/LHB'
            
            # Calculate batting stats using breakdown method with time period and matchup filters
            stats = self.get_stats_breakdown(df, is_pitcher=False, time_period=time_period, split=split_filter)
            
            # Format numeric values for display
            formatted_stats = {}
            for key, val in stats.items():
                if isinstance(val, float):
                    if key == 'BA':
                        formatted_stats[key] = f"{val:.3f}"
                    elif key == 'ISO':
                        formatted_stats[key] = f"{val:.3f}"
                    elif key in ['K%', 'BB%', 'Barrel%', 'Pull%']:
                        formatted_stats[key] = f"{val:.1f}%"
                    elif key == 'EV':
                        formatted_stats[key] = f"{val:.1f}"
                    else:
                        formatted_stats[key] = val
                else:
                    formatted_stats[key] = val
            
            return formatted_stats
        except Exception as e:
            _log.warning(f"Error getting batter stats for player {player_id}: {e}")
            return {stat: '--' for stat in ['PA', 'BA', 'ISO', 'K%', 'BB%', '1B', '2B', '3B', 'HR', 'Barrel%', 'Pull%', 'EV']}
    
    def get_bvp_stats(self, batter_id, pitcher_id):
        """Get batter vs specific pitcher stats across all seasons.
        
        Args:
            batter_id: MLB batter player ID
            pitcher_id: MLB pitcher player ID
        
        Returns:
            Dict with batting stats: PA, BA, BABIP, ISO, WOBA, K%, BB%, 1B, 2B, 3B, HR
        """
        try:
            import pandas as pd
            conn = self._get_db_connection()
            if conn is None:
                return {stat: '-' for stat in ['PA', 'BA', 'BABIP', 'ISO', 'WOBA', 'K%', 'BB%', '1B', '2B', '3B', 'HR']}
            query = """
                SELECT * FROM plate_appearances 
                WHERE batter_id = ? AND pitcher_id = ?
                ORDER BY game_datetime
            """
            df = pd.read_sql_query(query, conn, params=(int(batter_id), int(pitcher_id)))
            
            if df.empty:
                return {stat: '-' for stat in ['PA', 'BA', 'BABIP', 'ISO', 'WOBA', 'K%', 'BB%', '1B', '2B', '3B', 'HR']}
            
            # Calculate stats from raw PAs
            pa = len(df[df['events'].notnull()])
            at_bats = df[df['events'].notnull() & ~df['events'].isin(['walk', 'hit_by_pitch', 'sacrifice_fly', 'sacrifice_bunt'])]
            ab_count = len(at_bats)
            
            singles = len(df[df['events'] == 'single'])
            doubles = len(df[df['events'] == 'double'])
            triples = len(df[df['events'] == 'triple'])
            hrs = len(df[df['events'] == 'home_run'])
            hits_count = singles + doubles + triples + hrs
            
            ba = hits_count / ab_count if ab_count > 0 else 0
            slg = (singles + 2*doubles + 3*triples + 4*hrs) / ab_count if ab_count > 0 else 0
            iso = slg - ba
            
            # BABIP = (H - HR) / (AB - K - HR + SF)
            ks = len(df[df['events'] == 'strikeout'])
            sfs = len(df[df['events'] == 'sacrifice_fly'])
            babip_denom = ab_count - ks - hrs + sfs
            babip = (hits_count - hrs) / babip_denom if babip_denom > 0 else 0
            
            # wOBA (simplified linear weights)
            bbs = len(df[df['events'].isin(['walk', 'intent_walk'])])
            hbps = len(df[df['events'] == 'hit_by_pitch'])
            woba_num = (0.69 * bbs + 0.72 * hbps + 0.88 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hrs)
            woba = woba_num / pa if pa > 0 else 0
            
            k_pct = (ks / pa * 100) if pa > 0 else 0
            bb_pct = (bbs / pa * 100) if pa > 0 else 0
            
            # Format for display
            return {
                'PA': pa,
                'BA': f"{ba:.3f}" if pa > 0 else "0",
                'BABIP': f"{babip:.3f}" if pa > 0 else "0",
                'ISO': f"{iso:.3f}" if pa > 0 else "0",
                'WOBA': f"{woba:.3f}" if pa > 0 else "0",
                'K%': f"{k_pct:.1f}" if pa > 0 else "0.0",
                'BB%': f"{bb_pct:.1f}" if pa > 0 else "0.0",
                '1B': singles,
                '2B': doubles,
                '3B': triples,
                'HR': hrs
            }
        except Exception as e:
            _log.warning(f"Error getting BvP stats for batter {batter_id} vs pitcher {pitcher_id}: {e}")
            return {stat: '-' for stat in ['PA', 'BA', 'BABIP', 'ISO', 'WOBA', 'K%', 'BB%', '1B', '2B', '3B', 'HR']}

    def get_pitcher_vs_lineup_stats(self, pitcher_id, batter_ids):
        """Get a pitcher's stats from only PAs against specific batters (all seasons).
        
        Args:
            pitcher_id: MLB pitcher player ID
            batter_ids: List of batter player IDs
        
        Returns:
            Dict with pitching stats: IP, K, H, HR, 1B, 2B, 3B, BB%, ERA
        """
        empty = {stat: '--' for stat in ['IP', 'K', 'H', 'HR', '1B', '2B', '3B', 'BB%', 'ERA']}
        try:
            import pandas as pd
            if not pitcher_id or not batter_ids:
                return empty
            
            conn = self._get_db_connection()
            if conn is None:
                return empty
            placeholders = ','.join('?' for _ in batter_ids)
            query = f"""
                SELECT * FROM plate_appearances 
                WHERE pitcher_id = ? AND batter_id IN ({placeholders})
                ORDER BY game_datetime
            """
            params = [int(pitcher_id)] + [int(b) for b in batter_ids]
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                return empty
            
            stats = self.calculate_pitching_stats(df)
            
            # Format for display
            return {
                'PA': int(stats.get('PA')) if isinstance(stats.get('PA'), (int, float)) else (stats.get('PA') if stats.get('PA') != None else '--'),
                'IP': f"{stats.get('IP', 0):.1f}" if isinstance(stats.get('IP'), (int, float)) else '--',
                'K': str(stats.get('K', '--')),
                'K%': f"{stats.get('K%', 0):.1f}%" if isinstance(stats.get('K%'), (int, float)) else '--',
                'H': str(stats.get('H', '--')),
                'HR': str(stats.get('HR', '--')),
                '1B': str(stats.get('1B', '--')),
                '2B': str(stats.get('2B', '--')),
                '3B': str(stats.get('3B', '--')),
                'BB%': f"{stats.get('BB%', 0):.1f}%" if isinstance(stats.get('BB%'), (int, float)) else '--',
                'ERA': f"{stats.get('ERA', 0):.2f}" if isinstance(stats.get('ERA'), (int, float)) else '--'
            }
        except Exception as e:
            _log.warning(f"Error getting pitcher vs lineup stats for pitcher {pitcher_id}: {e}")
            return empty

    def _get_steals_db_connection(self):
        """Get a thread-local reusable connection to the steals database."""
        conn = getattr(self._local, 'steals_conn', None)
        if conn is None:
            steals_db = _app_paths.STEALS_DB
            if os.path.exists(steals_db):
                conn = sqlite3.connect(steals_db, check_same_thread=False)
                self._local.steals_conn = conn
        return conn

    def get_pitcher_baserunning_stats(self, pitcher_id, season, time_window='overall'):
        """Get pre-calculated baserunning defense stats for a pitcher.
        
        Args:
            pitcher_id: MLB player ID
            season: Season year
            time_window: 'overall', 'last_5', 'last_10', 'last_20', 'last_30'
        
        Returns:
            Dict with pickoff_success, sb_allowed, steal_attempts, sb_allowed_avg
            or None if not found.
        """
        conn = self._get_steals_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT pitcher_name, pickoff_success, sb_allowed, steal_attempts, sb_allowed_avg, games_included, innings_pitched "
                "FROM pitcher_baserunning_stats WHERE pitcher_id = ? AND season = ? AND time_window = ?",
                (pitcher_id, season, time_window)
            )
            row = cur.fetchone()
            if row:
                return {
                    'name': row[0],
                    'pickoff_success': row[1],
                    'sb_allowed': row[2],
                    'steal_attempts': row[3],
                    'sb_allowed_avg': row[4],
                    'games_included': row[5],
                    'innings_pitched': row[6],
                }
            return None
        except Exception as e:
            _log.warning(f"Error getting pitcher baserunning stats: {e}")
            return None

    def get_catcher_baserunning_stats(self, catcher_id, season, time_window='overall'):
        """Get pre-calculated baserunning defense stats for a catcher.
        
        Args:
            catcher_id: MLB player ID
            season: Season year
            time_window: 'overall', 'last_5', 'last_10', 'last_20', 'last_30'
        
        Returns:
            Dict with cs_total, sb_allowed, steal_attempts, sb_allowed_avg
            or None if not found.
        """
        conn = self._get_steals_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT catcher_name, cs_total, sb_allowed, steal_attempts, sb_allowed_avg, games_included, games_played "
                "FROM catcher_baserunning_stats WHERE catcher_id = ? AND season = ? AND time_window = ?",
                (catcher_id, season, time_window)
            )
            row = cur.fetchone()
            if row:
                return {
                    'name': row[0],
                    'cs_total': row[1],
                    'sb_allowed': row[2],
                    'steal_attempts': row[3],
                    'sb_allowed_avg': row[4],
                    'games_included': row[5],
                    'games_played': row[6],
                }
            return None
        except Exception as e:
            _log.warning(f"Error getting catcher baserunning stats: {e}")
            return None

    def get_batter_baserunning_stats(self, batter_id, season, time_window='overall'):
        """Get pre-calculated baserunning stats for a batter/runner.
        
        Returns:
            Dict with steal_attempts, stolen_bases, picked_off, sprint_speed,
            bolts, competitive_runs, bolt_pct or None if not found.
        """
        conn = self._get_steals_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT batter_name, steal_attempts, stolen_bases, picked_off, "
                "sprint_speed, bolts, competitive_runs, bolt_pct "
                "FROM batter_baserunning_stats WHERE batter_id = ? AND season = ? AND time_window = ?",
                (batter_id, season, time_window)
            )
            row = cur.fetchone()
            if row:
                return {
                    'name': row[0],
                    'steal_attempts': row[1],
                    'stolen_bases': row[2],
                    'picked_off': row[3],
                    'sprint_speed': row[4],
                    'bolts': row[5],
                    'competitive_runs': row[6],
                    'bolt_pct': row[7],
                }
            return None
        except Exception as e:
            _log.warning(f"Error getting batter baserunning stats: {e}")
            return None

    def get_batter_baserunning_stats_split(self, batter_id, season, time_window='overall', pitcher_hand=None):
        """Get baserunning stats for a batter filtered by pitcher handedness.

        Args:
            batter_id: MLB player ID for the runner
            season: Season year
            time_window: 'overall' or 'last_N'
            pitcher_hand: None (both) or 'L' or 'R'

        Returns:
            Dict similar to get_batter_baserunning_stats or None
        """
        conn = self._get_steals_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT game_date, event_type, is_successful, pitcher_id "
                "FROM stolen_bases WHERE runner_id = ? AND season = ? ORDER BY game_date DESC",
                (batter_id, season)
            )
            rows = cur.fetchall()

            if not rows:
                return None

            # rows: list of (game_date, event_type, is_successful, pitcher_id)
            all_dates = []
            for row in rows:
                if row[0] not in all_dates:
                    all_dates.append(row[0])

            # Determine filtered events based on time_window
            if time_window == 'overall':
                filtered = rows
            else:
                try:
                    window_num = int(time_window.split('_')[1])
                except (ValueError, IndexError):
                    window_num = None
                if window_num is None:
                    filtered = rows
                else:
                    cutoff_dates = set(all_dates[:window_num])
                    filtered = [r for r in rows if r[0] in cutoff_dates]

            if not filtered:
                return None

            # If pitcher_hand filtering requested, build handedness map for pitchers seen
            pitcher_hand_map = {}
            if pitcher_hand in ('L', 'R'):
                pitcher_ids = set(r[3] for r in filtered if r[3] is not None)
                for pid in pitcher_ids:
                    if pid is None:
                        continue
                    try:
                        # Use get_player_data to infer handedness from plate appearances (cached if available)
                        pdata = self.get_player_data(pid, is_pitcher=True, year=season)
                        hand = pdata.get('handedness', 'Unknown')
                        if hand and hand in ('L', 'R'):
                            pitcher_hand_map[pid] = hand
                        else:
                            # Fallback to API lookup
                            pitcher_hand_map[pid] = self._lookup_handedness(pid, is_pitcher=True)
                    except Exception:
                        pitcher_hand_map[pid] = self._lookup_handedness(pid, is_pitcher=True)

            # Compute stats
            sb_success = 0
            caught = 0
            picked_off = 0
            games_included = len(set(r[0] for r in filtered))

            for game_date, et, is_successful, pid in filtered:
                if pitcher_hand in ('L', 'R') and pid is not None:
                    ph = pitcher_hand_map.get(pid, None)
                    if ph is None or ph == 'Unknown' or ph != pitcher_hand:
                        continue

                if et == 'stolen_base' and is_successful == 1:
                    sb_success += 1
                elif et == 'caught_stealing' and is_successful == 0:
                    caught += 1
                elif et == 'pickoff' and is_successful == 0:
                    picked_off += 1

            steal_attempts = sb_success + caught

            # Sprint stats lookup (season-level)
            sprint_cur = conn.cursor()
            sprint_cur.execute(
                "SELECT sprint_speed, bolts, competitive_runs FROM sprint_speeds WHERE player_id = ? AND season = ?",
                (batter_id, season)
            )
            sp_row = sprint_cur.fetchone()
            if sp_row:
                speed, bolts, comp_runs = sp_row[0], sp_row[1], sp_row[2]
            else:
                speed = None
                bolts = None
                comp_runs = None

            bolt_pct = (bolts / comp_runs) if (bolts and comp_runs) else None

            return {
                'name': None,
                'steal_attempts': steal_attempts,
                'stolen_bases': sb_success,
                'picked_off': picked_off,
                'sprint_speed': speed,
                'bolts': bolts,
                'competitive_runs': comp_runs,
                'bolt_pct': bolt_pct,
                'games_included': games_included,
            }
        except Exception as e:
            _log.warning(f"Error getting batter baserunning split stats: {e}")
            return None

    def close(self):
        """Close open database connections (current thread)."""
        for attr in ('db_conn', 'steals_conn'):
            conn = getattr(self._local, attr, None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                setattr(self._local, attr, None)

    def get_database_stats(self):
        """Get statistics about cached data (stub for compatibility)."""
        last_updated = "Never"
        if os.path.exists(self.id_cache_file):
            try:
                mod_time = os.path.getmtime(self.id_cache_file)
                last_updated = datetime.fromtimestamp(mod_time).strftime('%B %d, %Y at %I:%M %p')
            except Exception:
                pass
        
        return {
            "cached_seasons": [],
            "total_players": 0,
            "batter_records": 0,
            "pitcher_records": 0,
            "last_updated": last_updated
        }
