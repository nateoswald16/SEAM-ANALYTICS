"""
Daily update orchestrator (simplified)

This script uses the existing `build_raw_db.py` ingestion logic and the
`build_calculated_db.py` builder to perform a daily refresh. It is intended
to be invoked by scheduler once per day; it can also be run manually.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from datetime import date, timedelta
import sqlite3
from tqdm import tqdm

# Frozen windowed apps (PyInstaller --noconsole) set stdout/stderr to None.
# Even when present, Windows console streams may use cp1252 which chokes on
# tqdm's Unicode bar characters. Force UTF-8 with error replacement everywhere.
import io as _io
for _attr in ("stdout", "stderr"):
    _stream = getattr(sys, _attr, None)
    if _stream is None:
        setattr(sys, _attr, open(os.devnull, "w", encoding="utf-8", errors="replace"))
    elif hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            setattr(sys, _attr, open(os.devnull, "w", encoding="utf-8", errors="replace"))

# Ensure the `app` package path is importable when running from repo root
ROOT = os.path.dirname(__file__)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import _app_paths
import build_raw_db
import build_calculated_db


class ProgressTracker:
    """Drives a single tqdm bar across raw ingestion + calculated stats phases.

    If *gui_cb* is provided it is called as ``gui_cb(current, total, phase)``
    after every step so the Qt UI can show a determinate progress bar.
    """

    def __init__(self, gui_cb=None):
        self.bar: tqdm | None = None
        self._phase = ''
        self._raw_total = 0
        self._gui_cb = gui_cb

    def start(self, n_games: int):
        """Initialize the bar. Total = games + 1 (statcast) + calc players (set later)."""
        self._raw_total = n_games
        # Start with games + statcast; calc total added dynamically
        self.bar = tqdm(total=n_games + 1, unit='step', dynamic_ncols=True,
                        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        self._set_phase('Fetching games')

    def _set_phase(self, phase: str):
        self._phase = phase
        if self.bar:
            self.bar.set_description(phase)
        self._emit()

    def _emit(self):
        if self._gui_cb and self.bar:
            try:
                self._gui_cb(self.bar.n, self.bar.total, self._phase)
            except Exception:
                pass

    def on_raw(self, stage, current, total, info):
        if stage == 'raw_game':
            self._set_phase(f'Game {current+1}/{total}')
            if current > 0:
                self.bar.update(1)
                self._emit()
        elif stage == 'statcast':
            if current == 0:
                # Finish last game step if needed
                if self.bar.n < self._raw_total:
                    self.bar.update(self._raw_total - self.bar.n)
                self._set_phase('Statcast enrichment')
            elif current == 1:
                self.bar.update(1)
                self._emit()

    def on_calc(self, stage, current, total, info):
        if stage == 'calc_player':
            if current == 0:
                # First call — expand bar total to include calc players
                calc_steps = total
                self.bar.total += calc_steps
                self.bar.refresh()
            self._set_phase(f'Calc stats ({info}) {current}/{total}')
            # Update by 1 per player
            if current > 0:
                self.bar.update(1)
                self._emit()

    def finish(self):
        if self.bar:
            # Fill to 100%
            if self.bar.n < self.bar.total:
                self.bar.update(self.bar.total - self.bar.n)
            self.bar.set_description('Done')
            self.bar.close()


def run_daily(start: str, end: str, season: int, calc_seasons: list[int], gui_cb=None):
    tracker = ProgressTracker(gui_cb=gui_cb)

    # Pre-fetch schedule to know game count before starting the bar
    games = build_raw_db.fetch_schedule(start, end, only_completed=False)
    tracker.start(len(games))

    print(f"Running ingestion for {start} → {end} (season {season})")
    build_raw_db.run_pipeline(start, end, season, only_completed=False, progress_cb=tracker.on_raw, games=games)

    # After raw ingest, incrementally rebuild calculated stats for affected players only
    build_calculated_db.build_calculated_db_incremental(season, start, end, progress_cb=tracker.on_calc)

    tracker.finish()

    # Light summary from raw DB
    conn = sqlite3.connect(_app_paths.RAW_DB)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM games WHERE game_date BETWEEN ? AND ?", (start, end))
    games_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM plate_appearances WHERE game_date BETWEEN ? AND ?", (start, end))
    pa_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pitching_appearances WHERE game_date BETWEEN ? AND ?", (start, end))
    pitches_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stolen_bases WHERE game_date BETWEEN ? AND ?", (start, end))
    steals_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT pitcher_id) FROM pitchers WHERE p_throws IS NOT NULL")
    pitchers_w_hand = cur.fetchone()[0]
    conn.close()

    print(f"\n── Summary ({start} → {end}) ──")
    print(f"  Games:                {games_count}")
    print(f"  Plate appearances:    {pa_count}")
    print(f"  Pitching appearances: {pitches_count}")
    print(f"  Stolen-base events:   {steals_count}")
    print(f"  Pitchers w/ hand:     {pitchers_w_hand}")

    return {
        "range": f"{start} → {end}",
        "games": games_count,
        "plate_appearances": pa_count,
        "pitching_appearances": pitches_count,
        "stolen_bases": steals_count,
    }


def _parse_args(argv: list[str] | None = None):
    p = argparse.ArgumentParser(description='Daily update: ingest raw data and build calculated DB')
    p.add_argument('--start', help='Start date YYYY-MM-DD')
    p.add_argument('--end', help='End date YYYY-MM-DD')
    p.add_argument('--season', type=int, help='Season year (defaults to start year or current year)')
    p.add_argument('--calc-seasons', nargs='+', type=int, help='Seasons to (re)build calculated DB for', default=None)
    p.add_argument('--days-back', type=int, default=1, help='How many days back to include in ingestion (default 1)')
    return p.parse_args(argv)


def main(argv: list[str] | None = None, gui_cb=None):
    args = _parse_args(argv)

    DB_PATH = _app_paths.RAW_DB

    def _get_last_game_date(db_path: str):
        """Return the latest game_date with actual play data (plate_appearances), or None."""
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT MAX(game_date) FROM plate_appearances")
            r = cur.fetchone()
            conn.close()
            if r and r[0]:
                try:
                    return date.fromisoformat(r[0])
                except Exception:
                    return None
        except Exception as e:
            print('Warning checking last game date in DB:', e)
        return None

    def _find_dates_missing_statcast(db_path: str, max_age_days: int = 7):
        """Find dates that have plate_appearances but no statcast enrichment.
        Uses release_speed (Statcast-only field) as the indicator, since
        launch_speed can now be populated from the game feed hitData.
        Only looks back max_age_days from today to avoid re-scanning ancient data."""
        if not os.path.exists(db_path):
            return []
        cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT game_date FROM plate_appearances
                WHERE game_date >= ?
                  AND release_speed IS NULL
                EXCEPT
                SELECT DISTINCT game_date FROM plate_appearances
                WHERE game_date >= ?
                  AND release_speed IS NOT NULL
                ORDER BY 1
            """, (cutoff, cutoff))
            dates = [r[0] for r in cur.fetchall()]
            conn.close()
            return dates
        except Exception as e:
            print(f'Warning checking for missing statcast: {e}')
            return []

    # CLI explicit start/end still supported (keeps previous behavior)
    if args.start and args.end:
        start = args.start
        end = args.end
        season = args.season or int(start.split('-')[0])
        if args.calc_seasons:
            calc_seasons = args.calc_seasons
        else:
            calc_seasons = [season, season - 1]
        return run_daily(start, end, season, calc_seasons, gui_cb=gui_cb)

    # Default behavior: determine missing range from DB and fetch all missing games
    yesterday = date.today() - timedelta(days=1)
    last_game = _get_last_game_date(DB_PATH)

    ingested_new = False
    ingested_start = None
    ingested_end = None

    if last_game is None:
        # DB empty or missing → fall back to days_back window (preserve previous default)
        start_dt = (yesterday - timedelta(days=args.days_back - 1))
    else:
        start_dt = last_game + timedelta(days=1)

    end_dt = yesterday

    if start_dt > end_dt:
        print(f"No missing games to ingest (last game in DB: {last_game.isoformat() if last_game else 'none'}).")
    else:
        ingested_new = True
        ingested_start = start_dt
        ingested_end = end_dt

        # Ingest in season-sized segments so we pass the correct `season` param to the pipeline
        cur_start = start_dt
        cur_end = end_dt
        for y in range(start_dt.year, end_dt.year + 1):
            seg_start = cur_start if y == start_dt.year else date(y, 1, 1)
            seg_end = cur_end if y == end_dt.year else date(y, 12, 31)

            tracker = ProgressTracker(gui_cb=gui_cb)
            games = build_raw_db.fetch_schedule(seg_start.isoformat(), seg_end.isoformat(), only_completed=False)
            tracker.start(len(games))

            print(f"Running ingestion for {seg_start.isoformat()} → {seg_end.isoformat()} (season {y})")
            build_raw_db.run_pipeline(seg_start.isoformat(), seg_end.isoformat(), y, only_completed=False, progress_cb=tracker.on_raw, games=games)

            # Incremental calc rebuild — only players from newly ingested dates
            if args.calc_seasons:
                # Explicit override: full rebuild for requested seasons
                print(f"Building calculated stats (full) for seasons: {args.calc_seasons}")
                build_calculated_db.build_calculated_db(args.calc_seasons)
            else:
                build_calculated_db.build_calculated_db_incremental(y, seg_start.isoformat(), seg_end.isoformat(), progress_cb=tracker.on_calc)

            tracker.finish()

    # ── Statcast backfill: re-enrich dates that have PAs but no statcast data ──
    missing_dates = _find_dates_missing_statcast(DB_PATH)

    if missing_dates:
        print(f"\n── Statcast backfill: {len(missing_dates)} date(s) missing statcast data ──")
        print(f"  Dates: {', '.join(missing_dates)}")
        backfill_start = missing_dates[0]
        backfill_end = missing_dates[-1]
        conn = sqlite3.connect(DB_PATH)
        build_raw_db.enrich_with_statcast(conn, backfill_start, backfill_end)
        conn.commit()
        conn.close()

        # Check how many were actually enriched
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM plate_appearances
            WHERE game_date BETWEEN ? AND ?
              AND launch_speed IS NOT NULL
        """, (backfill_start, backfill_end))
        enriched = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM plate_appearances
            WHERE game_date BETWEEN ? AND ?
        """, (backfill_start, backfill_end))
        total_pa = cur.fetchone()[0]
        conn.close()

        print(f"  Enriched {enriched}/{total_pa} PAs with statcast data")

        # Re-run incremental calc for backfilled dates
        season = int(backfill_start[:4])
        print(f"  Rebuilding calculated stats for backfilled range...")
        build_calculated_db.build_calculated_db_incremental(season, backfill_start, backfill_end)
        print(f"  Statcast backfill complete.")

    # Light summary from raw DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if ingested_new:
        s, e = ingested_start.isoformat(), ingested_end.isoformat()
    elif missing_dates:
        s, e = missing_dates[0], missing_dates[-1]
    else:
        print("Nothing to do.")
        conn.close()
        return {"range": None, "games": 0, "plate_appearances": 0,
                "pitching_appearances": 0, "stolen_bases": 0,
                "statcast_backfill": 0, "up_to_date": True}
    cur.execute("SELECT COUNT(*) FROM games WHERE game_date BETWEEN ? AND ?", (s, e))
    games_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM plate_appearances WHERE game_date BETWEEN ? AND ?", (s, e))
    pa_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pitching_appearances WHERE game_date BETWEEN ? AND ?", (s, e))
    pitches_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stolen_bases WHERE game_date BETWEEN ? AND ?", (s, e))
    steals_count = cur.fetchone()[0]
    conn.close()

    print(f"\n── Summary ({s} → {e}) ──")
    print(f"  Games:                {games_count}")
    print(f"  Plate appearances:    {pa_count}")
    print(f"  Pitching appearances: {pitches_count}")
    print(f"  Stolen-base events:   {steals_count}")

    return {
        "range": f"{s} → {e}",
        "games": games_count,
        "plate_appearances": pa_count,
        "pitching_appearances": pitches_count,
        "stolen_bases": steals_count,
        "statcast_backfill": len(missing_dates) if missing_dates else 0,
    }


if __name__ == '__main__':
    main()
