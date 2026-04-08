# Updates

All notable changes to Seam Analytics are documented here.

---

## v1.0.2-beta — 2026-04-07

### Game Tracker

**Live Card Stability**
- Fixed cards reverting to pre-game state (all dashes) during inning transitions or API hiccups
- New `_merge_game()` preserves `status` and `innings_detail` when the MLB API temporarily returns empty data for a live game
- Applied to both sidebar cards and schedule page cards

**Live Detection Overhaul**
- Added `abstractGameState` from the MLB API (`Preview`, `Live`, `Final`) as the primary live-game signal
- Centralized all live checks into a shared `_is_game_live()` helper — replaces 8+ scattered inline checks
- Now correctly detects live games during manager challenges, umpire reviews, and other non-"In Progress" states
- Fixed `live` flag in `fetch_live_games()` which was always `False` (checked time string instead of API status)

**Uniform Card Sizing**
- All schedule cards (pre-game, live, final) now share a consistent fixed height
- Class-level collapsed height tracker ensures every card matches the tallest layout
- Removed per-state height overrides — all cards sized via a single `_resize_for_expansion()` path
- Cards still resize dynamically when the play log is expanded

### Park & Weather

**Wind Arrow Cycling**
- Mini-ballpark wind arrows now update when the hourly weather overlay cycles to a new hour
- Arrow direction, color (hitter/pitcher friendly), and wind speed all refresh in sync with the cycling weather icon and detail text
- Fixed sea-level venues (e.g. Oracle Park) showing "--" for altitude instead of "0 ft"

---

## v1.0.1-beta — 2026-04-07

### Navigation & Game Tracker

**Navbar Restructure**
- Renamed "Game Schedule" to "Game Tracker" in main navbar and page header
- Added new "Lineups" tab in main navbar — loads the first available game on click
- Removed click-through navigation from Game Tracker schedule cards
- Sidebar remains the primary way to select individual games

**Schedule Card Improvements**
- Box score with "-" placeholders now displayed on all cards, including pre-game
- "▸ Play Log" expand button shown on all cards regardless of game state
- Expanding play log on a scheduled game shows "No play-by-play info yet"
- Live/final games still show "Loading plays…" until data arrives
- Simplified card resize logic — all cards use dynamic content-based sizing

### Lineup & Base Running Fixes

**Catcher Display**
- Base running catcher section now shows all available catchers from the roster
- Pre-game rosters with multiple catchers (e.g. Campusano + Fermin) display all options
- Narrows to the confirmed starter once the lineup is finalized

**Probable Lineup Cleanup**
- Removed `_build_probable_lineup` inference logic and `confirmed`/`probable_players` cache fields
- Simplified `_format_and_cache_lineup`, `prefetch_lineups`, and `refresh_lineup` signatures

**Bug Fixes**
- Fixed batting sub-tab showing empty tables (stale `confirmed` check in `get_game_lineup`)
- Fixed nav highlight: clicking a sidebar game or the Lineups tab now highlights "LINEUPS" instead of "GAME TRACKER"

### Weather System Overhaul

**Weather Providers**
- NWS (api.weather.gov) as primary weather provider — free, no API key, most accurate forecasts
- WeatherAPI.com for barometric pressure + first weather fallback
- Open-Meteo as second weather fallback
- 4-hour game window for all providers (covers full game duration)
- Separate pressure fetch via WeatherAPI `current.json` endpoint

**Animated Weather Overlay**
- Hourly weather icon cycling with cross-fade animation (600ms fade, 3s hold)
- Night-aware icons: crescent moon for clear nighttime conditions using QPainter composition mode
- Broadened condition matching for NWS/WeatherAPI condition strings (partly, patchy, mostly clear)
- Per-hour precip % displayed on separate line below time label

**Wind Direction Fixes**
- Fixed 180° compass conversion: meteorological "from" degrees now correctly flipped to MLB "toward" labels
- Added stadium orientation support using MLB API `azimuthAngle` (compass bearing home plate → CF)
- Wind directions are now field-relative — accounts for parks not facing due north (e.g. Yankee Stadium 75°, Wrigley 37°)
- Affects all three weather providers (NWS, WeatherAPI, Open-Meteo) and the fallback path

### Update System

**Progress Bar**
- Manual update now shows a determinate progress bar (0–100%) instead of an indeterminate spinner
- Orange percentage label displayed next to the progress bar during updates
- Update button text shows live progress (e.g. "Updating… 42%")
- Progress resets cleanly on completion or error

**Update Summary Popup**
- On completion, a styled popup displays either "Already up to date" or a detailed ingestion summary
- Summary includes date range, games processed, plate appearances, pitching appearances, stolen bases, and statcast backfill count
- Errors during update also shown in a popup instead of silently failing

**Frozen App Stability**
- Fixed crash when `sys.stdout`/`sys.stderr` are `None` in windowed exe (console=False)
- Redirected standard streams to `os.devnull` at module load for safe tqdm/print usage
- Fixed `charmap` codec error on Windows: reconfigured stdout/stderr from cp1252 to UTF-8 with error replacement
- Removed `matplotlib` from PyInstaller excludes — required transitively by pybaseball

**Installer**
- Added `CloseApplications=force` so the installer automatically closes running instances during upgrades

**Hourly Data Cycling**
- Temperature and wind direction text below field updates in sync with weather icon transitions
- Wind direction converted to MLB-style labels (Out To CF, L To R, etc.) per hour

**Weather Cache**
- Fresh weather data fetched on every app launch (stale cache cleared on startup)
- Disk cache retained for mid-session use; old cache files cleaned up after 3 days

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
