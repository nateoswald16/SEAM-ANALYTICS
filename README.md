<p align="center">
  <img src="app/assets/Logo.png" alt="Seam Analytics" width="120">
</p>

<h1 align="center">Seam Analytics</h1>
<p align="center">
  <strong>Real-time MLB stats desktop app</strong><br>
  Live scores · Lineups · Hitting · Pitching · Base Running · Park Factors
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-1.2.1-orange" alt="version">
  <img src="https://img.shields.io/badge/python-3.14%2B-blue" alt="python">
  <img src="https://img.shields.io/badge/platform-Windows-lightgrey" alt="platform">
</p>

---

## Features

- **Live Scoreboard** — game cards with real-time scores, status, and win probability
- **Lineup Viewer** — starting lineups with batting order, position, and handedness
- **Hitting Stats** — batting averages, ISO, K%, BB%, barrel rate, exit velocity, launch angle
- **Pitching Stats** — ERA, WHIP, K/9, pitch mix, opposing stats
- **Base Running** — stolen base stats, sprint speed, catcher/pitcher defense metrics
- **Batter vs Pitcher** — head-to-head matchup stats
- **Park Factors** — weather, wind, venue dimensions, and park factor ratings
- **Leaderboards** — season leaders across hitting, pitching, and base running categories

## Installation

### Option A: Windows Installer (Recommended)

1. Download the latest `SeamAnalytics-Setup-X.X.X.exe` from the [Releases](../../releases) page
2. Double-click the installer and follow the wizard
3. The installer will:
   - Install the app to `Program Files\Seam Analytics`
   - Copy pre-loaded databases to `%LOCALAPPDATA%\SeamAnalytics`
   - Create a desktop shortcut and Start Menu entry
   - Optionally schedule a daily data update at 6:00 AM

### Option B: Run from Source

**Prerequisites:** Python 3.13+ and Git

```bash
git clone https://github.com/nateoswald16/SEAM-ANALYTICS.git
cd SEAM-ANALYTICS
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r app/requirements.txt
```

**Launch the app:**

```bash
cd app
python seam_app.py
```

> **Note:** Running from source requires the databases (`mlb_raw.db`, `mlb_calculated.db`) in the `app/` directory. These are too large for Git. You can build them from scratch:
>
> ```bash
> # Build the raw database (downloads from MLB API + Baseball Savant)
> python build_raw_db.py --year 2026
>
> # Build the calculated stats database
> python build_calculated_db.py
> ```

## Daily Updates

The app uses two SQLite databases that need periodic updates to stay current:

| Database | Contents | Size |
|----------|----------|------|
| `mlb_raw.db` | Play-by-play, statcast, game data | ~768 MB |
| `mlb_calculated.db` | Pre-aggregated stats, splits, leaderboards | ~11 MB |

**Automatic (installer users):** If you selected "Schedule daily data updates" during install, a Windows Task Scheduler job runs `SeamUpdater.exe` at 6:00 AM daily.

**Manual:**

```bash
cd app
python daily_update.py                      # update yesterday's games
python daily_update.py --days-back 3        # update last 3 days
python daily_update.py --start 2026-04-01 --end 2026-04-05 --season 2026
```

## App Updates

The app checks for new versions automatically on launch and can install updates with one click.

- **Auto-check:** On startup, the app silently checks for a new release on GitHub. If you're already up to date, nothing happens.
- **Manual check:** Click **Check for Updates** in the titlebar to see if a new version is available.
- **Install:** When an update is found, the button turns green and shows the new version. Click it to view release notes, download the installer, and update — all without leaving the app. The app restarts automatically after the update completes.

> Updates are distributed as GitHub Releases. Each release contains a full Inno Setup installer that is downloaded and run silently in the background.

## Project Structure

```
SeamAnalytics/
├── app/
│   ├── seam_app.py              # Main GUI application (PyQt6)
│   ├── mlb_data_engine.py       # MLB API client + DB interface
│   ├── park_factors.py          # Weather / venue page
│   ├── MLB_AVG.py               # Stat grading benchmarks
│   ├── _app_paths.py            # Centralized path resolution
│   ├── build_raw_db.py          # Raw database builder
│   ├── build_calculated_db.py   # Calculated stats builder
│   ├── daily_update.py          # Daily update orchestrator
│   ├── database_schema.py       # SQLite schema definitions
│   ├── requirements.txt         # Python dependencies
│   └── assets/
│       ├── Logo.png             # App icon
│       ├── Logo.ico             # Windows icon
│       └── team_abbreviations.csv
├── installer/
│   ├── seam_app.spec            # PyInstaller spec (main app)
│   ├── daily_update.spec        # PyInstaller spec (updater)
│   └── seam_setup.iss           # Inno Setup installer script
├── .gitignore
├── README.md
├── UPDATES.md
└── LICENSE
```

## Tech Stack

- **GUI:** PyQt6
- **Data:** MLB Stats API, Baseball Savant (via pybaseball)
- **Storage:** SQLite (raw + calculated databases)
- **Packaging:** PyInstaller (exe) + Inno Setup (installer)

## Data Sources

- [MLB Stats API](https://statsapi.mlb.com) — schedules, lineups, live scores, player info
- [Baseball Savant](https://baseballsavant.mlb.com) — statcast data (via [pybaseball](https://github.com/jldbc/pybaseball))
- [Open-Meteo](https://open-meteo.com) — weather forecasts for park factors

## License

This project is for personal and educational use. See [LICENSE](LICENSE) for details.
