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

    def _get_incomplete_date_ranges(db_path: str) -> list:
        """Find all disconnected date ranges that need ingestion, from Opening Day to today.
        Returns list of (start_date_str, end_date_str) tuples, one per contiguous gap.
        
        Example: missing April 15 and last 3 weeks returns:
            [('2026-04-15', '2026-04-15'), ('2026-05-18', '2026-06-07')]
        Each range is processed with only its relevant pickle files loaded.
        """
        if not os.path.exists(db_path):
            return []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            current_year = date.today().year
            today_str = date.today().isoformat()

            # Get season Opening Day
            cur.execute("""
                SELECT MIN(game_date) FROM games
                WHERE season = ? AND game_type = 'R'
            """, (current_year,))
            r = cur.fetchone()
            season_start = r[0] if r and r[0] else (
                date.today() - timedelta(days=7)).isoformat()

            # All game dates that are scheduled but not fully ingested
            cur.execute("""
                SELECT g.game_date
                FROM (
                    SELECT game_date, COUNT(*) AS scheduled
                    FROM games
                    WHERE game_date BETWEEN ? AND ?
                    AND game_type = 'R'
                    GROUP BY game_date
                ) g
                LEFT JOIN (
                    SELECT game_date, COUNT(DISTINCT game_id) AS ingested
                    FROM plate_appearances
                    WHERE game_date >= ?
                    GROUP BY game_date
                ) p ON g.game_date = p.game_date
                WHERE COALESCE(p.ingested, 0) < g.scheduled
                ORDER BY g.game_date ASC
            """, (season_start, today_str, season_start))

            missing_dates = [r[0] for r in cur.fetchall()]
            conn.close()

            if not missing_dates:
                return []

            # Group into contiguous ranges — split when gap between consecutive
            # missing dates exceeds 4 days (handles off days within a series
            # while correctly separating a stale April gap from recent weeks)
            ranges = []
            range_start = missing_dates[0]
            range_end = missing_dates[0]

            for d in missing_dates[1:]:
                prev = date.fromisoformat(range_end)
                curr = date.fromisoformat(d)
                if (curr - prev).days <= 4:
                    range_end = d  # Extend current range
                else:
                    ranges.append((range_start, range_end))
                    range_start = d
                    range_end = d

            ranges.append((range_start, range_end))
            return ranges

        except Exception as e:
            print(f'Warning finding incomplete date ranges: {e}')
            return []

    def _find_dates_missing_statcast(db_path: str, max_age_days: int = 7):
        """Find dates that have PAs missing Statcast enrichment — including
        dates with PARTIAL enrichment (some pitchers missing, not just fully empty dates)."""
        if not os.path.exists(db_path):
            return []
        cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            # OLD query used EXCEPT — excluded dates where ANY PA had release_speed,
            # missing partial enrichment gaps (e.g. new pitcher on otherwise enriched date)
            # NEW query: find dates where ANY PA is still missing release_speed
            cur.execute("""
                SELECT DISTINCT game_date FROM plate_appearances
                WHERE game_date >= ?
                AND batted_ball = 1
                AND launch_speed IS NULL
                ORDER BY 1
            """, (cutoff,))
            dates = [r[0] for r in cur.fetchall()]
            conn.close()
            return dates
        except Exception as e:
            print(f'Warning checking for missing statcast: {e}')
            return []
        
    def _find_incomplete_pickles(days_back: int = 60):
        """Find and delete pickle files where the Statcast data is incomplete —
        meaning pitchers exist in plate_appearances for that date range who have
        no rows in the pickle. Covers gaps from any date, not just recent ones."""
        import glob, pickle
        cache_dir = _app_paths.STATCAST_CACHE_DIR
        season = date.today().year
        pattern = os.path.join(cache_dir, f'{season}-*_{season}-*.pkl')
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()

        conn = sqlite3.connect(_app_paths.RAW_DB)
        cur = conn.cursor()

        refreshed = []
        for pkl_path in sorted(glob.glob(pattern)):
            basename = os.path.basename(pkl_path)
            try:
                parts = basename.replace('.pkl', '').split('_')
                pkl_start, pkl_end = parts[0], parts[1]
            except Exception:
                continue

            # Only check pickles that overlap with our lookback window
            if pkl_end < cutoff:
                continue

            # Get pitcher IDs that have plate_appearances in this date range
            cur.execute("""
                SELECT DISTINCT pitcher_id FROM plate_appearances
                WHERE game_date BETWEEN ? AND ?
                AND release_speed IS NULL
            """, (pkl_start, pkl_end))
            missing_pitcher_ids = {r[0] for r in cur.fetchall()}

            if not missing_pitcher_ids:
                continue  # Pickle is complete for this range

            # Only flag pickles that have meaningful data already
            # Avoids deleting brand-new or tiny pickles that just haven't
            # been enriched yet vs genuinely incomplete ones
            try:
                pkl_size_mb = os.path.getsize(pkl_path) / (1024 * 1024)
                if pkl_size_mb < 1.0:
                    continue  # Too small to be a meaningful complete pickle
            except Exception:
                pass

            # This pickle covers dates where some pitchers have NULL release_speed
            # Delete it so _ensure_statcast_cached re-fetches it
            os.remove(pkl_path)
            print(f'  Deleted incomplete pickle: {basename} '
                f'({len(missing_pitcher_ids)} pitcher(s) missing Statcast data)')
            refreshed.append(basename)

        conn.close()
        return refreshed

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
    # Use today (not yesterday) so completed games are ingested immediately;
    # statcast enrichment will backfill once data becomes available (24-48 h).
    today = date.today()
    # Find all disconnected gaps — each processed with only its relevant pickles
    ingested_new = False
    ingested_start = None
    ingested_end = None

    gap_ranges = _get_incomplete_date_ranges(DB_PATH)

    if not gap_ranges:
        fallback_start = today - timedelta(days=max(args.days_back, 1))
        if fallback_start <= today:
            gap_ranges = [(fallback_start.isoformat(), today.isoformat())]
        print(f"No missing games to ingest (DB is up to date through {today.isoformat()}).")
    else:
        ingested_new = True
        ingested_start = date.fromisoformat(gap_ranges[0][0])
        ingested_end = date.fromisoformat(gap_ranges[-1][1])

        print(f"Found {len(gap_ranges)} gap range(s) to ingest:")
        for gs, ge in gap_ranges:
            print(f"  {gs} → {ge}")

        for gap_start_str, gap_end_str in gap_ranges:
            gap_start = date.fromisoformat(gap_start_str)
            gap_end = date.fromisoformat(gap_end_str)

            # Process each gap year-by-year in case a gap spans Jan 1
            for y in range(gap_start.year, gap_end.year + 1):
                seg_start = gap_start if y == gap_start.year else date(y, 1, 1)
                seg_end = gap_end if y == gap_end.year else date(y, 12, 31)

                tracker = ProgressTracker(gui_cb=gui_cb)
                games = build_raw_db.fetch_schedule(
                    seg_start.isoformat(), seg_end.isoformat(), only_completed=False)
                tracker.start(len(games))

                print(f"Running ingestion for {seg_start.isoformat()} → {seg_end.isoformat()} (season {y})")
                build_raw_db.run_pipeline(
                    seg_start.isoformat(), seg_end.isoformat(), y,
                    only_completed=False, progress_cb=tracker.on_raw, games=games)

                if args.calc_seasons:
                    print(f"Building calculated stats (full) for seasons: {args.calc_seasons}")
                    build_calculated_db.build_calculated_db(args.calc_seasons)
                else:
                    print("  Checking for incomplete Statcast pickles...")
                    _find_incomplete_pickles(days_back=60)
                    build_calculated_db.build_calculated_db_incremental(
                        y, seg_start.isoformat(), seg_end.isoformat(),
                        progress_cb=tracker.on_calc)
                    tracker.finish()

    # ── Statcast backfill: re-enrich dates that have PAs but no statcast data ──
    # Use the actual gap size (or default 7 days) as the lookback window
    if ingested_start:
        _backfill_days = max(7, (date.today() - ingested_start).days + 1)
    else:
        _backfill_days = 7
    missing_dates = _find_dates_missing_statcast(DB_PATH, max_age_days=_backfill_days)

    if missing_dates:
        print(f"\n── Statcast backfill: {len(missing_dates)} date(s) missing statcast data ──")
        print(f"  Dates: {', '.join(missing_dates)}")
        backfill_start = missing_dates[0]
        backfill_end = missing_dates[-1]
        conn = sqlite3.connect(DB_PATH)
        # Cache pickle first — enrich_with_statcast now loads from pickle
        backfill_season = int(backfill_start[:4])
        build_raw_db.fetch_and_cache_statcast_incremental(backfill_start, backfill_end, backfill_season)
        build_raw_db.enrich_with_statcast(conn, backfill_start, backfill_end, season=backfill_season)
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

        # FIXED — reuse backfill_season already defined above
        print(f"  Rebuilding calculated stats for backfilled range...")
        build_calculated_db.build_calculated_db_incremental(backfill_season, backfill_start, backfill_end)
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


def _send_notification(summary: dict):
    """Send a Windows toast notification with the update summary."""
    try:
        from winotify import Notification
        if summary.get("up_to_date"):
            body = "Database is already up to date. Nothing to do."
        else:
            rng = summary.get("range", "")
            lines = [f"Range: {rng}"]
            if summary.get("games"):
                lines.append(f"Games: {summary['games']}")
            if summary.get("plate_appearances"):
                lines.append(f"PAs: {summary['plate_appearances']}")
            if summary.get("pitching_appearances"):
                lines.append(f"Pitching: {summary['pitching_appearances']}")
            if summary.get("stolen_bases"):
                lines.append(f"SBs: {summary['stolen_bases']}")
            if summary.get("statcast_backfill"):
                lines.append(f"Statcast backfill: {summary['statcast_backfill']} date(s)")
            body = "\n".join(lines)
        toast = Notification(
            app_id="Seam Analytics",
            title="Daily Update Complete",
            msg=body,
        )
        toast.show()
    except Exception:
        pass  # Notification is best-effort


if __name__ == '__main__':
    result = main()
    _send_notification(result or {})
