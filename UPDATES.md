# Updates

All notable changes to Seam Analytics are documented here.

---

## v1.0.4-beta — 2026-04-08

### Game Tracker

**Play Log — Mid-At-Bat Action Events**
- Play-by-play log now includes stolen bases, caught stealing, wild pitches, passed balls, balks, pickoffs, errors, and defensive indifference
- These mid-at-bat events are extracted from `playEvents` in the MLB API feed and interleaved chronologically with at-bat results
- Action events display with an `↳` prefix in amber; scoring actions show in green

**Live Event Preview**
- The last-event preview line on schedule cards now shows transient in-progress events (mound visits, batter timeouts, etc.) instead of holding on the previous at-bat result
- These events are extracted from `currentPlay` in the live feed and automatically replaced when the next play occurs

**Current Batter / Pitcher Display**
- Live game cards now show the current pitcher and batter below each team row (e.g. "P: M. Petersen (R)" / "AB: T. Stephenson (R)")
- Updates in real-time as each new plate appearance begins
- Displays on both sidebar and schedule page game cards during live games only

**Ball/Strike/Out Count on Diamond**
- MiniDiamondWidget now displays three rows of indicator dots below the base diamond: outs (orange), balls (blue), strikes (red)
- Count updates in real-time from the live feed's `currentPlay.count`
- Diamond widget only shown on live game cards — hidden on pre-game and final cards for a cleaner layout

**Consistent Card Heights**
- All game card states (pre-game, live, final) now include placeholder rows for current batter/pitcher fields
- Prevents card height shifting when games transition between states
- Added column spacing on non-live cards to prevent moneyline labels from overlapping scores

### App Updates

**Scheduled Task Preserved During Silent Upgrades**
- Fixed one-click updater deleting the daily data update scheduled task on every upgrade
- Root cause: Inno Setup's `/TASKS=` parameter is an explicit allowlist — passing only `refreshdb` deselected `scheduledupdate`, and the `checkedonce` flag meant it was unchecked by default on upgrades even without `/TASKS`
- Silent install command now always includes `scheduledupdate` in the task list
- Installer now checks if the task already exists before recreating it, preserving the user's original schedule time instead of resetting to the 6:00 AM default

**Settings Dialog**
- New "Settings" button in the bottom bar opens a popup dialog
- **Scheduled Task** section shows current task status (Active / Not found) and lets you create or repair the daily update task with a custom time picker
- Task creation now uses XML import instead of command-line flags, which fixes the path-splitting bug where spaces in the install path (e.g. `D:\Program Files\...`) caused `schtasks` to split the command at the first space — the task would silently fail with `0x80070002` (file not found)
- Tasks are created with `StartWhenAvailable=true` so missed schedules (e.g. PC was off at 3 AM) run automatically when the machine wakes
- Battery guards disabled — task runs even on battery power and won't be killed mid-update
- No execution time limit — large backfills (weeks/months of missed data) can run to completion

**Update Dialog — Scheduled Task Checkbox**
- The app update confirmation dialog now includes a checked-by-default "Ensure scheduled task exists for daily auto-updates" checkbox
- When checked, the silent installer recreates the task with the improved XML settings during the upgrade

**Dynamic Statcast Backfill Window**
- The daily updater's statcast backfill pass was hardcoded to only retry missing statcast data from the last 7 days
- Now scales the lookback window to match the actual data gap, so users returning after weeks or months get full statcast enrichment

**Version Comparison Fix**
- Fixed update checker treating any non-matching version as "newer" — a local build ahead of the latest release (e.g. v1.0.4 vs v1.0.3) would incorrectly prompt to downgrade
- Now uses proper semantic version comparison so only strictly newer releases trigger the update button

---

## v1.0.3-beta — 2026-04-08

### Game Tracker

**Warmup Inning Indicator**
- Sidebar and schedule cards now display "WARMUPS" instead of "TOP 1" when a game's status is "Warmup" (pre–first pitch)

### Window

**Multi-Monitor Geometry Persistence**
- Window position, size, and maximized state are now saved on close and restored on next launch
- The app remembers which monitor it was on and returns there after minimize/restart
- If a saved monitor is no longer connected, the window resets to the primary screen

### App Updates

**Pre-Release Detection Fix**
- Fixed update checker not detecting new releases — the GitHub API endpoint `/releases/latest` excludes pre-releases, so all beta tags were invisible
- Switched to `/releases` endpoint which includes pre-releases

**Skip Version**
- Update confirmation dialog now has a "Skip This Version" button
- Skipped versions are suppressed from automatic launch checks
- Clicking "Check for Updates" manually always shows available updates regardless of skip
- The skip resets automatically when a newer version is published

**Database Refresh Option**
- Update confirmation dialog now includes a "Replace local databases" checkbox
- When checked, the silent installer replaces both databases with the versions bundled in the new release
- Unchecked by default — existing databases are preserved unless explicitly opted in
- Full release notes from the GitHub release are shown in an expandable "Show Details" panel so users can review changes before deciding

### Game Detail

**BvP Contact% & Whiff% Fix**
- Fixed Contact% and Whiff% showing 0% / blank on all BvP pages
- Root cause: `plate_appearances.swing` was incorrectly mapped to `swing_length` (a bat-tracking float like 73.4) instead of actual swing counts; `contact` was never populated
- Corrected field mapping — `swing_length` and `swing_path_tilt` are now unmapped from plate appearances
- Added per-PA swing and contact aggregation from Statcast pitch descriptions during enrichment
- Backfilled swing/contact counts for ~960K existing plate appearances across 2021–2026

**Team Toggle Fix**
- Fixed home/away toggle not switching tables on Base Runners and BvP subpages
- Only the currently visible subpage's stack is fade-animated; non-visible stacks switch instantly via direct index change
- Newly built tabs now sync with the current toggle state immediately after construction

**Game Switching Crash Fix**
- Fixed `RuntimeError: wrapped C/C++ object of type QStackedWidget has been deleted` crash when loading 3+ games in a row
- Added generation counter (`_load_gen`) to reject stale background data signals from previous game loads
- All stack widget references are nullified before `deleteLater()` to prevent dangling pointer access
- Signal handler and tab builder wrapped in RuntimeError/SystemError guards

**Corrupt Cache Handling**
- Lineup cache files with invalid JSON are now detected, deleted, and re-fetched instead of crashing

### Leaderboards

**Stolen Base Queries — Pickoff Exclusion**
- Fixed HR+SB Games leaderboard and league SB stat cards counting pickoff attempts as stolen bases
- 61,962 pickoff rows with `is_successful = 1` were inflating SB counts (1,006 vs actual 243 for 2026) and causing players with zero real SBs (e.g. Murakami) to appear on the HR+SB list
- All three affected queries now filter on `event_type = 'stolen_base'`, matching the existing base running page queries

**Singles, Doubles, Triples Leaders**
- Added three new leaderboard cards to the batting page: Singles (1B), Doubles (2B), Triples (3B)
- Sourced from `calculated_batting_stats` for the current season, same as existing cards

### Park & Weather

**Open-Meteo Double Conversion Fix**
- Fixed hourly temperature and wind speed being double-converted when served by Open-Meteo (fallback provider)
- The API request already specifies `temperature_unit=fahrenheit` and `wind_speed_unit=mph`, but the hourly slot code was still applying C→F and km/h→mph conversions on top
- Most visible at non-US venues (e.g. Toronto) where NWS is unavailable and Open-Meteo is the primary source — 40°F was displayed as 104°F

### Data Pipeline

**Historical Statcast Data**
- Downloaded complete Statcast pitch-level data for 2021–2025 seasons (~3.5M pitches, ~4GB)
- Rebuilt calculated pitching database with advanced stats (barrel%, zone%, whiff%, velo) for 2025

---

## v1.0.2-beta — 2026-04-07

> **Database refresh recommended.** This release includes significant changes to database calculations and structure (barrel heuristic, foul ball exclusion, hitData extraction, historical re-enrichment). When running the installer, check **"Replace local databases with the latest bundled data"** to get the corrected datasets. You can also use the in-app Update Data button after installing.

### Game Tracker

**Date Selector**
- Added date navigation arrows on the right side of the main navbar — browse yesterday, today, or tomorrow
- Switching dates reloads sidebar game cards, schedule page, lineups, and weather for the selected day
- Tomorrow's schedule and weather are pre-fetched on launch for instant switching
- Live score polling pauses when viewing past or future dates
- Leaderboards re-filter to the selected day's lineups

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

**Weather Timezone Fix**
- All three weather providers (NWS, WeatherAPI, Open-Meteo) now convert forecast times to the user's local timezone
- Fixes hourly cycling showing wrong hour labels when viewing games in other time zones

### Leaderboards

**HR + SB Games**
- New leaderboard card showing players with the most games featuring both a home run and a stolen base
- Joins plate appearances and stolen bases data for the current season

### Data Pipeline

**Game Feed hitData Extraction**
- Now extracts `hitData` from the MLB API game feed for every plate appearance — provides exit velocity, launch angle, batted ball type, hit distance, and spray chart coordinates
- Data available immediately after games end, before Statcast publishes (typically ~2 day lag)
- Increased batted ball event coverage from ~1,200 to ~7,500 for early 2026

**Statcast Enrichment Fixes**
- Fixed foul ball data overwriting real hit data: batted-ball fields (`launch_speed`, `launch_angle`, `bb_type`, `hc_x`, `hc_y`, `hit_distance_sc`) now only applied from the result pitch, not from intermediate fouls
- Fixed barrel and pull flag derivation to only compute from the result pitch
- Statcast enrichment now skips null values instead of overwriting game feed data with blanks
- Fixed backfill detection: uses `release_speed` (Statcast-only) instead of `launch_speed` (now populated by game feed) so the daily updater correctly identifies dates needing Statcast enrichment

**Barrel Heuristic**
- Widened barrel zone formula to match MLB's definition: EV ≥ 98 mph with launch angle range expanding as EV increases
- Previous heuristic was too narrow, underreporting barrel rate

**Historical Data Re-enrichment**
- Cleaned and re-enriched all historical seasons (2021–2025) with the corrected Statcast pipeline
- Cleared ~915K plate appearances of foul-ball-contaminated BBE fields and re-applied Statcast data from scratch
- Batted ball event coverage restored from ~20K to ~122–125K per season

**Daily Updater Hardening**
- Fixed backfill window: removed fresh-date exclusion that could permanently skip Statcast data if the user missed multiple days
- Increased Statcast lookback window from 5 to 7 days

**Foul Ball Exclusion**
- Exit velocity and launch angle averages now exclude foul balls across all stat queries (calculated DB, batter stats, batter vs pitcher)
- Only batted ball events with a valid `bb_type` contribute to EV/LA metrics

### In-App Updates

**Check for Updates**
- Added "Check for Updates" button in the titlebar between the version label and Update Data button
- Automatically checks for new releases on launch (silent — no popup if already up to date)
- Manual check via the button shows a popup confirming you're on the latest version
- When a new version is available, the button turns green and displays "Update → vX.Y.Z"

**One-Click Install**
- Clicking the green update button shows release notes and a confirmation dialog
- Downloads the installer with a progress bar in the titlebar
- Runs the Inno Setup installer silently in the background — no wizard, no interruptions
- Automatically restarts the app after the update completes

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
