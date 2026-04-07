# Updates

All notable changes to Seam Analytics are documented here.

---

## v1.0.0-beta — 2026-04-07

### Initial Beta Release

**App**
- Live scoreboard with game cards, win probability, and score tracking
- Lineup viewer with batting order, position, and batter handedness
- Hitting stats tab: batting averages, ISO, K%, BB%, barrel rate, exit velocity, launch angle
- Pitching stats tab: ERA, WHIP, K/9, pitch mix, opposing batter stats
- Base running tab: stolen bases, sprint speed, catcher/pitcher defense metrics
- Batter vs Pitcher matchup tab
- Park factors tab: weather, wind, venue dimensions, park factor ratings
- Leaderboards: season leaders across hitting, pitching, and base running
- Dark theme UI with styled cards and tab navigation
- SVG info icon button for About dialog with hover color change
- Manual "Update Data" button in title bar with orange indeterminate progress bar
- Installer time picker for scheduled update task (hour/minute/AM-PM)

**Infrastructure**
- Centralized path resolution (`_app_paths.py`) for dev and frozen (exe) modes
- Rotating file logging to `seam.log`
- Crash handler dialog with log path reference
- Missing-database detection dialog on startup
- Version display in title bar and About dialog
- SQLite WAL journal mode for concurrent read/write safety during updates
- 30-second connection timeouts to prevent "database is locked" errors

**Data Pipeline**
- `build_raw_db.py` — builds raw database from MLB API + Baseball Savant
- `build_calculated_db.py` — builds aggregated stats database
- `daily_update.py` — incremental updates for recent games
- Parallel game feed fetching (6 concurrent workers) for faster updates
- Skip already-ingested games to avoid redundant API calls
- Batched DB commits (single commit per update pass instead of per-game)
- Batched statcast enrichment commits (every 500 rows instead of per-row)
- Eliminated duplicate `fetch_schedule` API calls between daily_update and build_raw_db
- Park factors: venue coordinate hydration, Open-Meteo circuit breaker, reduced timeouts

**Distribution**
- PyInstaller one-dir builds for main app and updater
- Inno Setup installer with optional daily scheduled task
- Pre-loaded databases shipped with installer
