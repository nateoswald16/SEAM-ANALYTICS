# Updates

All notable changes to Seam Analytics are documented here.

---

## v1.0.0-beta — 2025-07-06

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

**Infrastructure**
- Centralized path resolution (`_app_paths.py`) for dev and frozen (exe) modes
- Rotating file logging to `seam.log`
- Crash handler dialog with log path reference
- Missing-database detection dialog on startup
- Version display in title bar and About dialog

**Data Pipeline**
- `build_raw_db.py` — builds raw database from MLB API + Baseball Savant
- `build_calculated_db.py` — builds aggregated stats database
- `daily_update.py` — incremental updates for recent games

**Distribution**
- PyInstaller one-dir builds for main app and updater
- Inno Setup installer with optional daily scheduled task
- Pre-loaded databases shipped with installer
