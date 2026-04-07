# Updates

All notable changes to Seam Analytics are documented here.

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
