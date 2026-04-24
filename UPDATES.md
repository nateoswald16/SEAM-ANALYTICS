# Updates

All notable changes to Seam Analytics are documented here.

---

## v1.2.1 — 2026-04-24

> [!WARNING]
> **Database Rebuild Required for Existing Users**
> This update includes schema changes to both `mlb_raw.db` and `mlb_calculated.db`. Users updating from a previous version (not fresh-installing) **must** fully rebuild both databases before use. After the rebuild completes, immediately run **Daily Update** inside the app to ensure all data is fully populated and current. Fresh installs are unaffected.

### Network & Performance Optimizations

- **Faster startup**: schedule fetched from disk cache on repeat same-day launches (~5 ms vs. 300–800 ms network call); live scores refresh in background 800 ms after window appears
- **Lazy Park Factors page**: weather thread and network call deferred until the tab is first opened
- **Thread pool reuse**: logo prefetch and handedness fetching reuse module-level pools instead of creating/destroying one per call
- **Caching improvements**: logo miss-caching, boxscore failure caching, in-flight headshot dedup, and `PRAGMA table_info` results cached per table
- **Vectorized spray angle math**: `pulled_air_pct` and hit-zone calculations use numpy instead of Python row loops
- **JSON cache compression**: `_JsonCache` now writes `.json.gz` files; falls back to plain `.json` for migration
- **NWS grid pre-population**: venue grid endpoints resolved in background at startup so Park Factors cards open without per-card latency

### Filter Bar UI Polish

**Unified Filter Button Styling**
- Replaced stock QComboBox rendering with fully custom `paintEvent` on `_MenuComboBox` — transparent background, `t3` dim text, 1px `bdr` border, 3px radius; hover/open state switches to `bg3` fill, `bdrl` border, `t1` bright text, matching the COLS button exactly
- Added `_HamburgerButton` QPushButton subclass that draws the same 3-line hamburger icon on the right side via `paintEvent`; icon color tracks hover/pressed state
- COLS button converted from `QPushButton("☰  COLS")` to `_HamburgerButton("COLS")` with `padding-right:24px` to clear the icon
- All filter dropdown items uppercased via `.currentText().upper()` in `paintEvent`
- Dropdown font set via `QFont.setPixelSize(11)` applied to `cb.setFont()`, `cb.view().setFont()`, and `Qt.ItemDataRole.FontRole` on each model item to guarantee size regardless of Qt delegate path
- `showPopup()` override on `_MenuComboBox` repositions the popup 4px below the button so it never overlaps the filter bar
- Border crispness fix: border drawn on `QRectF` inset 0.5px on all sides with `Antialiasing` disabled, putting the 1px stroke exactly on the pixel grid
- Minimum widths bumped (season: 70px, pitcher: 66px, time: 100px) to prevent text truncation

### Column Visibility Picker

**Configurable Column Customization**
- Added `☰  COLS` button to the filter bar on all game detail tabs
- Clicking the button opens a floating popup listing all configurable columns for the active tab, grouped by section (Batting, Pitching, Baserunning, etc.)
- Each column has a checkbox — unchecking hides that column from the table; preferences persist across sessions in `column_prefs.json`
- All columns default to visible on first launch
- Added `COLUMN_PREFS_FILE` path constant to `_app_paths.py`
- Added module-level `_load_col_prefs()`, `_save_col_prefs()`, `_COL_PREFS` global, `_TAB_SECTION_KEYS`, and `_TAB_SECTION_LABELS` infrastructure
- Added `StatsTable.apply_col_visibility(hidden_cols)` method — hides/shows columns by name
- Added `_ColPickerPopup` class: floating `Qt.WindowType.Popup` QFrame with scrollable per-section checkboxes, emits `changed(section_key, col_name, visible)` signal
- Added `GameDetailPanel._open_col_picker()`, `_on_col_pref_changed()`, `_apply_col_prefs_to_tab()` methods
- `_build_tab` hooks populate `_tab_cols` for all 5 tabs; `_apply_col_prefs_to_tab` called after each tab build and on every data refresh
- `_on_subnav_changed` refreshes the open picker when switching tabs

**Column Picker Bug Fixes**
- Fixed popup opening off-screen: position now clamped to available screen geometry via `QApplication.screenAt()`; opens above button when insufficient room below, clamps right edge to screen
- Fixed checked checkbox color: indicator now uses orange (`#f07020`) for both fill and border, making checked vs. unchecked immediately distinct
- Fixed always-visible columns appearing in the picker: `#`, `POS`, `PLAYER`, `PITCHER`, `CATCHER`, `PA`, `IP` are locked — no checkbox shown and they can never be hidden
- Fixed section label "BATTING" / "PITCHING" etc. rendering with a visible container box: scoped popup `QFrame` border to root element only via `QFrame#colPickerPopup` object name selector

---

### Batting & Pitching Stat Columns

**New `calculated_batting_stats` Columns**
- `barrel_pct` (Brl%) — barrels (launch speed-angle = 6) / total BBE
- `pulled_air_pct` (PullAir%) — pulled airballs / all BBE; airball counted when spray angle < −17° on pull side (non-ground-balls only); per-row `stand` field corrects for switch hitters; raw DB formula uses hc_x/hc_y geometry, Statcast pkl preferred when it covers ≥ raw BBE count
- `ev50` (EV50) — average of top 50% exit velocities by BBE count (replaces simple avg_ev); pkl preferred over raw DB
- `max_ev` (MaxEV) — max exit velocity on any BBE
- `fb_pct` (FB%) — fly balls / total BBE (replaces avg launch angle)
- `hard_hit_pct` (Hard%) — BBE with exit velocity ≥ 95 mph / total BBE
- `avg_bat_speed` (BatSpd) — average bat speed across top 90% of tracked swings (Statcast methodology); raw DB fallback uses `AVG(bat_speed)` from `plate_appearances`
- `squared_up_rate` (SqUp%) — fraction of competitive swings (top 90% by bat_speed) where EV ≥ 80% of theoretical max EV (1.23 × bat_speed + 0.23 × plate_speed); plate_speed = release_speed × 0.92; fouls count in denominator but not numerator; bonus inclusion: any ≥60 mph swing producing ≥90 mph EV
- `blast_rate` (Blast%) — fraction of competitive swings where per-swing sq_pct × 100 + bat_speed ≥ 164; same denominator as `squared_up_rate`
- `chase_rate` (Chase%) — swings at out-of-zone pitches (zone > 9) / total out-of-zone pitches; Statcast sc_pitches preferred, raw DB zone column fallback

**Batting stat data sources**
- Counting stats (PA, AB, H, R, RBI, K, BB, etc.) always come from raw DB `plate_appearances` — covers all games regardless of Statcast coverage
- Batted-ball metrics (barrel%, hard%, fb%, max EV, EV50, PullAir%) use raw DB as baseline; Statcast pkl overrides when it covers ≥ the raw BBE count
- Bat speed, squared-up, blast, chase use pitch-level `sc_pitches` Statcast data; raw DB `plate_appearances` columns (`bat_speed`, `zone`, `description`) used as fallback

**New `calculated_pitching_stats` Columns**
- `slg_against` (SLG) — total bases allowed / AB against (AB = PA − BB − SF − HBP)
- `babip_against` — (H − HR) / (BBE − HR)
- `whip` — (BB + H) / IP
- `xoba_against` — mean `estimated_woba_using_speedangle` on BBE; pkl preferred over raw DB
- `barrel_pct` (Barrel%) — barrels (LSA = 6) / BBE
- `hard_pct` (Hard%) — BBE with EV ≥ 95 mph / total BBE
- `ld_pct` (LD%) — line drives / total BBE
- `soft_pct` — popups / total BBE
- `gb_pct` (GB%) — ground balls / total BBE
- `contact_pct` (Contact%) — contacts / swings
- `zone_pct` (Zone%) — pitches landing in zones 1–9 / all pitches with zone data
- `avg_velo` (Velo) — mean release speed; sc_pitches preferred, raw DB `release_speed` fallback
- `top_velo` (Top) — max release speed
- `swstr_pct` (SwStr%) — swinging strikes / total pitches; sc_pitches preferred, `pitching_appearances` fallback
- `fp_strike_pct` (F-Strike%) — first-pitch strikes (type S or X) / total first pitches; requires sc_pitches

**Pitcher time windows changed to pitch-count**
- Windows are now `last100p`, `last200p`, `last300p`, `last500p` (pitches thrown) instead of game-count windows (`last5`, `last10`, etc.)
- `_last_n_pitches_game_dates_for_pitcher()` resolves pitch-count windows by combining sc_pitches counts with raw `pitching_appearances` estimates for games not yet in pkl

**Calc DB schema bumped v3 → v11 over these changes**
- v3: added `h_per_9`, `k_per_9`, `bb_per_9` to pitching stats
- v4: bat speed / squared-up / hard hit / chase rate for batters; SwStr% / GB% / F-Strike% for pitchers
- v5–v7: pitcher windows migrated to pitch-count; window sizes refined to 100/200/300/500
- v8: `blast_rate` added; squared-up upgraded to plate_speed formula (release × 0.92) + competitive-swings denominator
- v9: formula corrections — barrel uses LSA = 6, soft% = popup%, avg_bat_speed = top-90% swings, SLG/BABIP denominators fixed
- v10: `avg_ev` → `ev50` (top-50% avg), `avg_launch_angle` → `fb_pct` (fly ball rate), `pull_pct` → `pulled_air_pct` (spray-angle airballs / all BBE)
- v11: forced full rebuild to apply v10 column renames that did not take effect

---



**Estadio Alfredo Harp Helú — Mexico City (venue 5340)**
- Added full park profile for the two April 25/27 MLB games at Estadio Alfredo Harp Helú
- Venue rated `137` in `VENUE_PARK_FACTORS` — more extreme than Coors (113) per altitude physics; ball flies 7–8% farther than MLB standard at 7,349 ft (2,200 ft above Coors)
- Dimensions: 325 ft foul poles / 375 ft gaps / 400 ft CF — half-circle wall shape; larger total OF area than a typical park but plays short due to altitude
- Artificial turf, 8 ft uniform walls, 20,062-seat compact venue (foul territory rating 2)
- All 13 `park_widget.py` data tables populated: `VENUE_PARK_FACTORS`, `VENUE_HIT_FACTORS`, `VENUE_DIMENSIONS`, `VENUE_WALL_HEIGHTS`, `VENUE_WX_PROFILE`, `_VENUE_ENDEMIC_WIND_CARRY`, `_VENUE_ENDEMIC_1B`, `_VENUE_ENDEMIC_XBH`, `_VENUE_TEMP_REF_OFFSET`, `_VENUE_MONTHLY_NEUTRAL`, `_VENUE_DAY_NIGHT`, `_VENUE_SKY_MULT`, `_VENUE_RAIN_MULT`
- Physics test results (neutral April game, S0+S1A): HR +35.7 pct-pts (PF ~136), XBH +23.6 pct-pts, 1B +22.9 pct-pts; HR advantage over Coors: +39.6 pct-pts

**Mexico City Weather Pipeline Fix (`park_factors.py`)**
- MLB Stats API returns `None` for all coordinates on venue 5340 (no lat/lon/elevation/azimuth)
- Added `_VENUE_COORD_OVERRIDE` dict to `park_factors.py` with static fallback coordinates for API-deficient venues
- Venue 5340 entry: `(19.3618, -99.1567, 7349 ft, 45°)` — NWS will 404 (US-only) and fall through to Open-Meteo as intended
- Confirmed live Open-Meteo fetch working: 781 hPa surface pressure, 0% precip, 64% RH

---

**Wall-Height Dimension Splits**
- Added `VENUE_WALL_HEIGHTS` dict with effective pull-zone wall heights (LF wall, RF wall) for all 30 MLB venues extracted from BallparkPal stadium diagrams
- New `_dimension_splits()` function computes zero-sum LHB/RHB offsets from outfield wall asymmetry
- Formula: short wall on pull side → more HR for that handedness; tall wall → more doubles off the wall (inverse of HR effect)
- Scaling constants: HR ±0.12 pct-pts/ft, XBH ±0.08 pct-pts/ft, 1B ±0.02 pct-pts/ft from 8' reference baseline
- All splits are zero-sum pairs — overall park ratings unchanged, only handedness differentials affected

**Dimension Splits Wired Into Pipeline**
- `_neutral_park()` now returns dimension split keys (`hr_dim_lhb`, `hr_dim_rhb`, `xbh_dim_*`, `s_dim_*`) via spread
- `_compute_hr_rating()` applies HR dimension offsets to `lhb_pct` and `rhb_pct` in both dome and open-air paths
- `_compute_hits_rating()` applies XBH and singles dimension offsets to per-handedness returns in both paths
- Notable splits: Fenway LHB +1.92 HR (short RF porch), RHB −1.92 HR (Green Monster suppresses); PNC/Target LHB −0.90 HR (tall RF walls)

**Fixed Dome Roof Check Bug**
- `_compute_hits_rating()` now checks `roof_status in ("dome", "closed", "Closed")` instead of `== "Closed"`
- Fixed Tropicana Field showing uneven LHB/RHB splits when it should be symmetric (fixed dome)

---

## v1.2.0 — 2026-04-18

### Park & Weather

**Hourly Precipitation & Humidity Updates**
- Conditions container now updates precipitation and humidity values when toggling between hourly weather slots
- Added `humidity` field to all three weather provider hourly slot dicts (NWS, Open-Meteo, WeatherAPI)

**Stadium Size Descriptor**
- Added `VENUE_SIZE_DESC` mapping all 30 venue IDs to size categories (Extra Small / Small / Medium / Large / Extra Large) based on foul-line distances and field area
- New "Stadium Size: {desc}" label displayed below venue name on each weather card

**Roof Status Label Cleanup**
- Venue name line now uses `_predict_roof_status()` for proper formatting: ALL CAPS status (OPEN/CLOSED) with `*` suffix when predicted
- Removed redundant roof badge from the card header row

**Color Grading Thresholds**
- Changed HR rating, hits rating, `_split_color()`, and `_hits_color()` thresholds from ±3% / ±2% to ±8%
- Removed amber from neutral values — all neutral ratings now display white

**Weather Overlay Fix**
- Widened `WeatherOverlay` from 200px to 320px and switched from fixed-position text to dynamic centering using `QFontMetricsF` text measurement
- Icon, temperature, and condition text are now measured and centered as a group — no more clipping on long descriptions like "Showers And Thunderstorms Likely"
- Reduced gaps between icon→temp (4px) and temp→condition (6px)

**Game Delay Hourly Re-Anchoring**
- Delayed starts: fetches `firstPitch` from the live feed; if ≥15 min after scheduled `gameDate`, re-anchors hourly weather window and displayed time to the actual start
- Suspended/resumed games: uses `resumeDateTime` from the live feed to re-anchor
- Mid-game rain delays: detects `status="Delayed"` + `currentInning > 0` and re-anchors hourly window to the current hour so weather slots show resume-time conditions
- Reuses already-fetched live feed JSON in the venue-coords fallback to avoid duplicate HTTP calls

**Per-Park Deviation Ranking Diagnostic**
- Created `_diag_park_rank.py` diagnostic tool that ranks venues by total prediction deviation (|HR Δ| + |XBH Δ| + |1B Δ|) against BallparkPal reference data
- Includes a simulation engine that tests proposed parameter changes without modifying source code
- Handles closed-roof venue suppression in simulations
- Uses correct MLB API elevation values (feet): Coors=5190, Wrigley=595, Target=828, PNC=780, Progressive=653, etc.

**7 Park-Specific Calibration Fixes (Total Deviation 90→63)**
- Target Field (3312): WX temperature multiplier 0.80→0.60
- Nationals Park (3309): Park factor 92→88, WX wind multiplier 1.60→1.40
- Wrigley Field (17): WX wind multiplier 3.00→3.50
- Fenway Park (3): Park factor 95→92, Hit factor 1B 1.07→1.04
- Yankee Stadium (3313): Park factor 99→97
- Sutter Health Park (2529): Park factor 101→99
- loanDepot park (4169): Hit factor 2B 1.04→1.00
- Final accuracy: Perfect(≤2): 3, Close(3-5): 10, Off(>5): 2

**Hourly Toggle Fix for Finished Games**
- Fixed hourly weather toggle showing only the current hour for finished games (e.g. "6 PM" instead of "2 PM, 3 PM, 4 PM, 5 PM")
- Root cause: NWS API only returns future forecast periods, so past game hours are lost
- Added Open-Meteo historical hourly fallback: after primary weather source, if `hourly_conditions` has fewer than 4 entries and a game UTC timestamp exists, fetches historical hourly data from Open-Meteo
- All games now populate 4 correct hourly slots regardless of game status

**Wind Insight Fixed Height**
- Wind insight description label now uses `setFixedHeight(lineSpacing * 2 + 4)` for a fixed 2-row height
- Prevents the weather card from resizing when toggling between hourly slots

### Game Status

**Delay Display**
- Sidebar and game tracker cards now show "DELAY TOP 4" / "DELAY BOT 7" etc. in amber when a game is in a delay, replacing the normal TOP/BOT/MID/END inning label

**Postponed Display**
- Postponed games now display "Postponed" instead of "PPD"
- Postponed games are treated as finished (scores shown, no start time displayed)

### Lineups

**Filter Behavior on Subpage Switch**
- Switching between subpages (Batting, Pitching, Base Running, BvP, Bullpen) now resets all filters (Season, Matchup, Time Window) to their defaults
- Team toggle (Away/Home) no longer resets other applied filters — switching teams preserves the current Season, Matchup, and Time Window selections

**Live Game Current Pitcher in Base Running**
- Base Running subpage now shows the current active pitcher (including relievers) instead of only the starting pitcher for live games
- New `get_current_pitchers()` method on `DataManager` fetches the boxscore API to identify the most recent pitcher per team
- `get_game_baserunning()` accepts new `away_current_pitcher`/`home_current_pitcher` parameters
- Both the initial data load and filter-triggered refreshes detect live games and pass current pitcher info
- Non-live games (scheduled/final) continue to show the starter as before

---

## v1.1.2 — 2026-04-15

### Top Pitching

**HR Allowed / Hits Allowed Filter**
- Added a filter bar on the Top Pitching page to toggle between HR Allowed and Hits Allowed stat table views
- HR Allowed table: PITCHER, TEAM, OPP, H, HR, BF, HR:BF%, Pull Air%, Pitches, HR:P%
- Hits Allowed table: PITCHER, TEAM, OPP, H, BF, H:BF%, Pitches, H:P%
- Filter bar sits between leaderboard cards and the stat table stack

**Leaderboard Layout Reorder**
- Moved pitching leaderboard cards above the stat tables for better visual hierarchy
- Order: Title → "TODAY'S LEADERS" → leaderboard card grid → filter bar → stat tables

**Three New Pitching Leaderboard Widgets**
- Contact% — percentage of pitches put in play (lower is better for pitchers)
- K/9 — strikeouts per 9 innings
- H/9 — hits per 9 innings

**Two-Way Player Filtering**
- `get_todays_player_info()` now returns a separate `pitcher_teams` dict containing only probable/starting pitcher IDs
- Pitching leaderboards use `pitcher_teams` so two-way players like Ohtani only appear when scheduled to pitch
- Batting and baserunning leaderboards continue to use the full `player_teams` dict

### Game Tracker

**Due Up / On Deck / In the Hole**
- Live game cards now show a row above the box score with the next batters due up
- During inning transitions: displays "Due Up" and "On Deck" batters
- Mid-inning: displays "On Deck" and "In the Hole" batters
- Styled with muted labels (t3) and bright player names (t1) with batter handedness

**Play-by-Play Substitutions**
- Pitching changes, pinch hitters, and pinch runners now appear in the play-by-play log
- Substitution events render in bold amber text to distinguish from regular plays

**AB:/P: Label Styling**
- Current batter and pitcher labels use rich text HTML with styled spans
- Label tags ("AB:", "P:") in muted color (t3), player names in bright color (t1)
- Player names are HTML-escaped to prevent injection

### Tables

**Benchmark-Based Color Grading**
- Migrated HR Allowed and Hits Allowed table grading from inline percentile-based grade_map to the MLB_AVG.py benchmark system
- New benchmarks added: H:P% (avg 4.75%, threshold ±0.75%), H:BF% (avg 20.5%, threshold ±2.5%)
- Adjusted HR:BF% benchmark to avg 2.75%, threshold ±0.75% (≥3.5% red, ≤2.0% green)
- All grade_map dicts removed from HR/Hits tables — every column now falls through to `grade_stat()`
- Grading colors: values beyond avg + threshold show red (above) or green (below) based on `higher_is_better` direction

### Park & Weather

**Humidity Data**
- Added humidity percentage to all three weather providers:
  - NWS: `relativeHumidity` from forecast periods
  - WeatherAPI: `humidity` from current and hourly data
  - Open-Meteo: `relative_humidity_2m` from current and hourly data
- Humidity displayed on mini park widget as "XX% humid" below precipitation row
- New third scale bar in the carry legend: "HUMIDITY → CARRY" showing effect on ball carry (humid air is less dense → slightly more carry)
- Legend spacing increased from 30px to 50px between the three bars

**Retractable Roof Status**
- Live/final games: reads `weather.condition` from the MLB API — `"Roof Closed"` or `"Dome"` → `"Retractable (CLOSED)"`, any other condition → `"Retractable (OPEN)"`
- Closed retractable roofs receive dome treatment (wind/carry factors suppressed)

**Predictive Retractable Roof Model**
- Pre-game retractable venues now show a predicted OPEN/CLOSED status with an asterisk (e.g. `"Retractable (CLOSED)*"`)
- Per-venue rules based on MLB operational guidelines:
  - **MIL**: closed if temp < 60°F, precip ≥ 15%, or wind ≥ 25 mph (comfort-focused, monitors hourly)
  - **HOU**: closed if temp < 55°F or > 95°F, precip ≥ 10%, wind ≥ 25 mph, or humid heat (zero-tolerance rain)
  - **SEA**: closed if temp < 60°F, precip ≥ 50%, or wind ≥ 25 mph ("carport" design — stays open as long as possible)
  - **ARI**: closed if temp > 85°F or < 60°F, precip ≥ 20%, or wind ≥ 25 mph (desert heat)
  - **TEX**: closed if temp > 80°F or < 60°F, precip ≥ 10%, or wind ≥ 25 mph (zero-tolerance rain, sensitive equipment)
  - **TOR**: closed if temp < 60°F, precip ≥ 20%, or wind ≥ 25 mph
  - **MIA**: closed if temp > 78°F or < 65°F, humidity > 60%, precip ≥ 15%, or wind ≥ 25 mph (almost always closed)

**Retractable Roof Filter Fix**
- Park filter for "RETRACTABLE" now uses `startswith("Retractable")` instead of exact match
- Correctly matches all variants: `"Retractable (OPEN)"`, `"Retractable (CLOSED)"`, and predicted statuses with asterisk

### Notepad

**In-App Notepad**
- Added a floating notepad panel accessible via a button in the title bar (after the search bar)
- Auto-saves to `assets/notepad.txt` with 500ms debounce on text changes
- Clear button to wipe content; close button to dismiss
- Panel is draggable from the top 32px header area and resizable from all edges/corners
- Custom `_DragResizeFrame` subclass handles edge detection, resize cursors, and drag logic
- Default size 380×420, minimum 260×200; monospace font (Cascadia Mono)

### Data Pipeline

**Baserunning Date Range Fix**
- Fixed `daily_update.py` using yesterday as `end_dt` for baserunning stats — now correctly uses today
- Ensures same-day baserunning data is included in incremental rebuilds

**Pitching Leaderboard ID Fix**
- Fixed pitching leaderboard missing pitcher IDs sourced from game dicts
- Ensures all probable/starting pitchers appear in leaderboard calculations

### Performance

**Correlated Subquery Elimination**
- Replaced 7 correlated subqueries in `build_calculated_db.py` with JOIN-based pitcher handedness lookups
- Batting, runner baserunning, pitcher baserunning, and catcher baserunning aggregation functions all used `(SELECT p_throws FROM pitchers WHERE pitcher_id = ...)` per row
- Now uses `JOIN pitchers pit ON pit.pitcher_id = ...` for a single index scan per query instead of one per row
- Affects all vs_lefty/vs_righty matchup calculations during database rebuilds

**Database Indexes**
- Added 4 composite indexes on `stolen_bases` for season-scoped baserunning queries: `(season, runner_id)`, `(season, pitcher_id)`, `(season, catcher_id)`, and `(game_date)`
- Eliminates full table scans on `stolen_bases` during calculated stats rebuilds

**HR Table Query Consolidation**
- Merged two separate raw DB queries (BF/HR/Hits + Pull Air%) into a single query on the Top Pitching page
- Reduces plate_appearances table scans from 2 to 1 per page load

**Pitcher Row Query Consolidation**
- Consolidated 3 sequential per-pitcher DB queries (resolve ID, resolve handedness, resolve name) into a single query in `_build_pitcher_row()`
- Reduces per-pitcher DB round trips from 3 to 1 when building pitching stat tables

**Bounded Player Data Cache**
- `_player_data_cache` in `mlb_data_engine.py` now evicts the oldest entry when exceeding 256 items
- Prevents unbounded memory growth from cached DataFrames during long sessions

**Bounded Label Style Cache**
- `_label_style_cache` in `seam_app.py` capped at 128 entries with FIFO eviction
- Prevents minor memory leak from accumulated stylesheet strings

## v1.1.1 — 2026-04-12

### Top Pitching

**HR Allowed Table**
- Added a new HR Allowed stat table to the Top Pitching page
- Columns: PITCHER, TEAM, OPP, H, HR, BF, HR:BF%, Pull Air%, Pitches, HR:P%
- Uses a rolling "since previous season" time window (`season >= current_year − 1`) to match industry-standard leaderboards
- BF, HR, and H are sourced from the raw plate appearances database for exact date coverage
- Pitches are sourced from the calculated pitching stats database
- Pull Air% shows the percentage of batted balls pulled in the air (launch angle ≥ 25°) — a strong HR predictor
- Sorted by HR descending with color grading on HR:BF%, Pull Air%, and HR:P% columns

### Player Card

**Spray Chart Venue Resolution**
- Spray charts now resolve the correct stadium overlay using schedule-based venue lookup
- `resolve_venue_team()` checks today's schedule for the player's upcoming game, falls back to the most recent raw DB game, then the player's own team
- Fixed a crash caused by NaN venue values being passed to `drawText()` — now safely skipped with a `pd.notna()` guard

### Data Pipeline

**Pitches Thrown Fallback**
- `build_calculated_db.py` now falls back to counting pitches from the `pitching_appearances` table when Statcast pickle data returns zero pitches for a pitcher
- Ensures pitch counts are populated even when Statcast data is incomplete

### Tables

**Column Layout & Alignment**
- All stat table data columns now use stretch layout to evenly fill available width — eliminates dead space on the right
- All non-name columns are now center-aligned for a cleaner, more consistent look
- Header font reduced from 10px to 9px to prevent clipping on dense tables (e.g. "Contact%")

### Security Hardening

- Upgraded ESPN odds API from HTTP to HTTPS
- Player names rendered in table cells are now HTML-escaped to prevent injection via malicious data
- Removed `shell=True` from subprocess cleanup — uses safe `os.remove()` / `shutil.rmtree()` instead
- SQL table and column names are now validated against whitelists before interpolation in queries
- Parameterized the stolen-bases matchup handedness filter (previously string-interpolated)
- Replaced `pickle.load()` cache files with JSON for player IDs and processed dates (auto-migrates old pickle files)
- `weatherapi_key.txt` confirmed in `.gitignore`

### Performance

**Database Indexes**
- Added 9 new indexes to the raw database for common query patterns
- Composite indexes on `(game_id, batter_is_home)`, `(game_date, pitcher_id)`, `(game_date, batter_id)`, `(season, pitcher_id)`, `(season, batter_id)` and more

**Batch Game Queries**
- Sidebar game loading reduced from 4N+1 queries to 2 total queries (scores via GROUP BY, starting pitchers via batch IN clause)

**Bounded Caches**
- Pitcher info, name lookup, and logo pixmap caches now use LRU eviction (max 512/512/128 entries) to prevent unbounded memory growth

**DataFrame Optimization**
- Removed 4 unnecessary `.copy()` calls in the Statcast data pipeline, reducing memory allocations during database builds

### Code Quality

**Shared Design Tokens**
- Extracted the color palette (`C` dict) into a single `_app_theme.py` module — `seam_app.py`, `player_card.py`, and `park_factors.py` now import from one source of truth instead of each defining their own copy

**Accent-Insensitive Player Search**
- Typing "Jose Ramirez" now matches "José Ramírez" — search strips diacritical marks via Unicode normalization before comparing
- Display names retain their proper accented characters

**Error Handling Standardization**
- Replaced 17 bare `except:` blocks in `mlb_data_engine.py` with typed catches (`except Exception:`, `except (ValueError, TypeError):`, `except (ValueError, IndexError):`)
- Added logging to previously silent cache save failures

**HTTP Timeout Constants**
- Defined `_TIMEOUT_DEFAULT`, `_TIMEOUT_SHORT`, `_TIMEOUT_LONG` constants in `mlb_data_engine.py` — all 9 HTTP calls now reference named constants instead of magic numbers

**Readability Improvements**
- Replaced nested lambda in `_fade_switch()` with a named `_on_fade_in_done()` function
- Added inline comments to format helper lambdas (`_fmt3`, `_fmt2`, `_fmt_pct`, etc.) documenting their intended stat types
- Renamed single-letter loop variables in complex contexts (`c` → `conn`, `p` → `player`, `b` → `batter_id`, etc.)
- Flattened deeply nested `_fetch_player_handedness()` with early returns
- Deduplicated handedness fetch logic — boxscore parser now reuses `_fetch_player_handedness()` instead of inlining its own copy

---

## v1.1.0 — 2026-04-10

> **Action Required:** This update includes a critical fix to the outs and earned runs data pipeline. We recommend all users rebuild their database using the bundled database included with this release, then on first app launch click **Update Data** in the title bar to run the manual data updater. This ensures all historical data is populated with correct values.

### Player Search

**Global Search Bar**
- Added a player search bar centered in the application title bar
- Type any player name to search across the full 1,267-player roster
- Compact design (320×28px) fits naturally in the title bar without stealing focus on app launch
- Floating dropdown (380px wide) appears below the input with up to 25 results, max 6 visible rows
- Each result row shows a headshot thumbnail (32×48 vertical rounded rect), player name, and subtitle (MLB | 2026 | Position | Team)
- Clicking a result opens the full Player Profile dialog with batting/pitching stats, spray chart, game log, and PA table
- Dropdown uses `Qt.WindowType.Tool` to avoid stealing keyboard focus from the main app

### Player Card

**Pitcher Outs Filter**
- The OUTS filter on the pitcher game log bar chart now displays correct per-game out totals
- Previously showed all zeros due to a data pipeline bug (see Data Pipeline section below)

### Pitching Tables

**Outs Column Added**
- Added an "OUTS" column to the Pitchers, Bullpen, and BvP pitching tables
- Positioned immediately after the IP (Innings Pitched) column for quick reference
- Shows the raw outs count alongside the formatted IP value (e.g. IP: 6.1, OUTS: 19)

### Data Pipeline

**Outs Recorded & Earned Runs Fix (Critical)**
- Fixed `outs_recorded` being zero for all plate appearances in the database
- Root cause: `parse_plays_to_pas()` in `build_raw_db.py` was checking `playEvents[].isOut` — a field that does not exist at the top level of play event objects in the MLB API
- Replaced the ~40-line broken calculation with a correct 4-line implementation using the `play.runners[]` array:
  - Outs: count runners where `movement.isOut == True`
  - Earned runs: count runners where `details.earned == True`
- Correctly handles all out types: strikeouts, field outs, force outs, double plays (2 outs), triple plays (3 outs), and baserunner outs
- Fixed `earned_runs` calculation which had the same issue — was relying on `playEvents` fields that didn't exist
- All future daily updates will automatically use the corrected logic

### Game Tracker

**Chronological Game Sorting**
- Fixed games sorting out of order when start times crossed a digit boundary (e.g. 10:00 PM appearing before 2:00 PM)
- Root cause: time strings were compared alphabetically — `"10:00 PM"` < `"2:00 PM"` because `'1' < '2'`
- `_game_sort_key` now parses the 12-hour time string into a `datetime.time` for proper chronological ordering
- Affects sidebar, schedule page, and all live re-sort paths
- Park factors page sort updated with the same fix

### Bullpen Page

**New Bullpen Sub-Tab**
- Added a "BULLPEN" tab to the game detail sub-navbar alongside Batting, Pitching, Base Running, and BvP
- Fetches active bullpen roster from the MLB API boxscore endpoint (`teams.{side}.bullpen[]`) for each game
- Displays full pitching stats table for every bullpen pitcher (same columns as the Pitching tab: IP, K, K%, BB, BB%, ERA, WHIP, xOBA, etc.)
- Dynamically sizes to fit any number of active bullpen pitchers
- Supports Season, Batter handedness (RHB/LHB), and Time window filters
- Away/Home team toggle works the same as all other sub-tabs

### Daily Updater

**Silent Execution with OS Notification**
- SeamUpdater.exe now runs without launching a console window (`console=False` in PyInstaller spec)
- On completion, a Windows toast notification is displayed via `winotify` with a summary of updated data (games, PAs, pitching appearances, stolen bases, statcast backfill)
- If the database is already up to date, the notification reports "Database is already up to date"
- Notification is best-effort — failures are silently ignored to avoid disrupting the update process

### Database Upgrade Safety

**Schema Version Tracking**
- Added a `schema_version` table to both `mlb_raw.db` and `mlb_calculated.db` to track the database schema version
- `build_raw_db.py` stamps the raw schema version after all column migrations complete
- `build_calculated_db.py` checks the stored version on startup — if stale, all calculated tables are cleared and rebuilt from scratch
- Version constants (`RAW_DB_SCHEMA_VERSION`, `CALC_DB_SCHEMA_VERSION`) defined in `_app_paths.py` as the single source of truth

**Name-Based Column Access**
- Converted all calculated DB reads in `seam_app.py` from fragile index-based access (`crow[0]`, `crow[1]`, …) to name-based access (`crow["column_name"]`)
- Applies to batting stats (23 columns), pitching stats (27 columns), pitcher/catcher/runner baserunning stats, and leaderboard name lookups
- `calc_connect()` now sets `conn.row_factory = sqlite3.Row` so all queries return dict-like rows
- Adding or reordering columns in the calculated DB no longer breaks the app

**Stale Database Detection**
- `seam_app.py` validates the calc DB schema version on first connection each session
- If the installed DB is from an older app version, stale calculated data is cleared automatically so the app falls back to raw aggregation instead of crashing
- Prevents column-shift errors when users upgrade without replacing their databases

**Version-Aware Installer**
- `.schema_version` marker files are now written alongside each database after builds
- Installer's `ShouldInstallDB()` reads the marker file and forces DB replacement when the schema version doesn't match the bundled version
- Legacy installs with no marker file are also detected and replaced
- Eliminates the need for users to manually check "Replace local databases" after an upgrade with schema changes

### Stolen Base & Caught Stealing Fixes

**Stolen Base Home Fix**
- Fixed stolen bases of home plate not being recorded in the database
- Root cause: `_parse_base()` returns `'Home'` as the target base, but the MLB API sets `movement.end` to `'score'` for home steals — these never matched
- Added an exception so `target_base == 'Home'` matches `end_base == 'score'`

**Caught Stealing Edge Cases**
- Fixed caught stealing events being skipped when the MLB API reports `isOut=False`
- MLB marks some CS plays as `isOut=False` when the runner is initially caught but advances on the same play (e.g. error, subsequent hit in the same at-bat)
- These are still counted as caught stealing in official stats — removed the `isOut` requirement for CS events

---

## v1.0.5-beta — 2026-04-09

### Game Tracker

**Diamond Widget Sync Fix**
- Fixed schedule card diamonds showing stale outs and runner positions — `update_plays()` was only syncing balls/strikes from the live feed, ignoring outs, on_first, on_second, on_third
- Schedule card diamonds now use `set_state()` with all 6 fields (runners, outs, balls, strikes) from the live feed, matching the sidebar diamond behavior
- Both sidebar and schedule diamonds now derive state from the same live feed API response, eliminating desync between the two views

**Single Source of Truth for Game Data**
- Sidebar and schedule cards previously did independent merges of fresh API data, leading to divergent state
- Refactored `_on_scores_fetched` so both card types read from the same master `self._games` dict

**Sidebar Update Latency**
- Eliminated 1–2 second delay on sidebar score updates — `scores_ready` signal now emits immediately after `fetch_live_games()` returns, before per-game play fetches begin

**3rd Out Play Event Display**
- Fixed the last event preview showing "In Play, Out(s)" instead of the actual play result (e.g. "Flyout: ...") when the final out of a half-inning completed the at-bat
- Now checks `isComplete` on the current play and uses the detailed result description

**Parallel Play Fetches**
- Per-game play-by-play API calls now run in parallel (up to 4 workers) in `_poll_scores`, `_prefetch_all_plays`, and `_poll_plays`
- Conditional diamond repaint — only redraws when values actually change
- Incremental play log rendering — appends only new plays instead of full rebuild on each update

**Sidebar Batter/Pitcher Change Detection**
- Added `current_batter_name` and `current_pitcher_name` to the change detection field set so sidebar cards update immediately when a new batter steps up or pitcher enters

### App Updates

**Auto-Relaunch After Silent Update**
- Fixed app not relaunching after a silent in-app update — the Inno Setup `[Run]` entry had `skipifsilent` which skipped the post-install launch during silent installs
- Added `RestartApplications=yes` to the installer so Inno Setup handles closing and relaunching the app
- Removed `/NORESTART` flag from the silent install command

**Update Dialog Checkbox Styling**
- "Replace local databases" and "Ensure scheduled task" checkboxes now render as proper toggle controls with visible indicators (orange fill when checked, border, hover effect) instead of plain text

**Previous-Day Box Scores & Play Logs**
- Box scores and play logs now display correctly when viewing previous days' games
- Game loading now prefers the MLB API (which includes per-inning linescore data) over the local database
- Play-by-play data is prefetched in the background after date navigation



## v1.0.4-beta — 2026-04-09

> **Action Required:** This update fixes a bug where the daily scheduled task was broken due to a path-quoting issue. After updating, go to **Settings → Scheduled Task** and click **Repair Scheduled Task** to ensure automatic daily updates are working. If your data is out of date, click **Update Data** in the bottom bar to manually backfill any missing days.

### Game Tracker

**Play Log — Mid-At-Bat Action Events**
- Play-by-play log now includes stolen bases, caught stealing, wild pitches, passed balls, balks, pickoffs, errors, and defensive indifference
- These mid-at-bat events are extracted from `playEvents` in the MLB API feed and interleaved chronologically with at-bat results
- Action events display in amber; scoring actions show in green

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

**Previous-Day Box Scores & Play Logs**
- Box scores and play logs now display correctly when viewing previous days' games
- Game loading now prefers the MLB API (which includes per-inning linescore data) over the local database (which only stores plate appearance totals)
- Play-by-play data is prefetched in the background after date navigation so play logs are populated for all final games

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
