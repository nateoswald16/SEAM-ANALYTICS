# Seam Analytics — Release Checklist

Step-by-step guide for verifying versions, building executables, and creating an installer for a new release.

---

## 1. Version Verification

Three version systems must stay in sync: **App Version**, **Raw DB Schema Version**, and **Calc DB Schema Version**.

### 1a. App Version

The app version follows semantic versioning (`MAJOR.MINOR.PATCH`). Bump it for every release.

| File | Constant / Variable | Example |
|------|---------------------|---------|
| `app/_app_paths.py` | `APP_VERSION = "X.Y.Z"` | `"1.1.2"` |
| `installer/seam_setup.iss` | `#define MyAppVersion "X.Y.Z"` | `"1.1.2"` |
| `README.md` | Badge URL `version-X.Y.Z-orange` | `version-1.1.2-orange` |

**Quick check:**
```bash
cd app
python -c "import _app_paths; print(_app_paths.APP_VERSION)"
```

Then confirm the same string appears in `installer/seam_setup.iss` line 9 and the README badge.

### 1b. Database Schema Versions

Only bump these when you change table structure (add/remove columns, rename tables, alter types). If the release only changes application logic, leave them alone.

| File | Constants | Current |
|------|-----------|---------|
| `app/_app_paths.py` | `RAW_DB_SCHEMA_VERSION`, `CALC_DB_SCHEMA_VERSION` | `2`, `3` |
| `installer/seam_setup.iss` | `#define RawDBSchemaVersion`, `#define CalcDBSchemaVersion` | `"2"`, `"3"` |

**When to bump Raw DB schema:**
- Added/removed columns in `plate_appearances`, `stolen_bases`, `game_info`, or `pitchers` tables
- Changed column types or constraints in `database_schema.py`

**When to bump Calc DB schema:**
- Added/removed columns in any `calculated_*` table
- Added/removed entire calculated tables
- Changed aggregation logic that produces different column sets

**What happens on mismatch:**
- **Raw DB:** The app runs `ALTER TABLE` migrations in `build_raw_db.create_db()` on startup — existing data is preserved.
- **Calc DB:** The installer replaces the calc DB entirely (it's derived from raw and can be rebuilt). At runtime, `build_calculated_db.py` detects version mismatch and clears stale tables before rebuilding.

**Quick check:**
```bash
cd app
python -c "import _app_paths; print('RAW:', _app_paths.RAW_DB_SCHEMA_VERSION, 'CALC:', _app_paths.CALC_DB_SCHEMA_VERSION)"
```

Compare with the `#define` values in `installer/seam_setup.iss` lines 13–14.

### 1c. Schema Version Marker Files

These plain-text files sit next to the databases and are read by the installer to decide whether to replace the calc DB:

| File | Contains |
|------|----------|
| `app/mlb_raw.db.schema_version` | Raw schema version (e.g. `2`) |
| `app/mlb_calculated.db.schema_version` | Calc schema version (e.g. `2`) |

These are written automatically by `build_raw_db.py` and `build_calculated_db.py` after a successful build. You generally don't need to edit them manually. If you bumped a schema version, rebuild the corresponding database (Step 3) and the marker file will be updated.

### 1d. PyInstaller Spec Hidden Imports

When you add new Python modules to the `app/` directory, add them to the `hiddenimports` list in the relevant `.spec` file(s):

| Module | Spec File(s) |
|--------|--------------|
| `_app_paths` | Both |
| `_app_theme` | `seam_app.spec` |
| `_http_utils` | Both |
| `_ui_utils` | `seam_app.spec` |
| `mlb_data_engine` | `seam_app.spec` |
| `park_factors` | `seam_app.spec` |
| `search` | `seam_app.spec` |
| `player_card` | `seam_app.spec` |
| `build_raw_db` | Both |
| `build_calculated_db` | Both |
| `MLB_AVG` | `seam_app.spec` |

---

## 2. Update UPDATES.md

Add a new version section at the top of `UPDATES.md` following the existing format:

```markdown
## vX.Y.Z — YYYY-MM-DD

### Section Name

**Feature Name**
- Description of change
```

This file is user-facing — it powers the release notes shown in the in-app update dialog and on the GitHub Releases page.

---

## 3. Build Databases (if schema changed)

Only needed when schema versions were bumped. Skip if this release is app-logic-only.

```bash
cd app

# Rebuild raw database (downloads all data — takes a long time)
python build_raw_db.py --year 2026

# Rebuild calculated stats database
python build_calculated_db.py
```

After rebuild, verify the marker files match:
```bash
type mlb_raw.db.schema_version
type mlb_calculated.db.schema_version
```

---

## 4. Build Executables

Both builds use PyInstaller with spec files. Run from the **repo root** (`D:\MLB Stats`).

```bash
cd "D:\MLB Stats"
```

### 4a. Build the main app

```bash
pyinstaller installer/seam_app.spec --noconfirm
```

Output: `dist/SeamAnalytics/` (a directory containing `SeamAnalytics.exe` and `_internal/`)

### 4b. Build the daily updater

```bash
pyinstaller installer/daily_update.spec --noconfirm
```

Output: `dist/SeamUpdater/` (a directory containing `SeamUpdater.exe` and `_internal/`)

### 4c. Verify the builds

```bash
# Quick smoke test — app should launch and show the main window
./dist/SeamAnalytics/SeamAnalytics.exe

# Updater smoke test — should run and exit (may show a toast notification)
./dist/SeamUpdater/SeamUpdater.exe
```

---

## 5. Build the Installer

The installer is built with [Inno Setup](https://jrsoftware.org/isinfo.php). You need `ISCC.exe` on your PATH (installed with Inno Setup).

### 5a. Compile the installer

```bash
"C:\Users\noswa\AppData\Local\Programs\Inno Setup 6\ISCC.exe" installer/seam_setup.iss
```

Output: `installer/output/SeamAnalytics-Setup-X.Y.Z.exe`

### 5b. Test the installer

1. Run the generated `.exe` — it should install to `Program Files\Seam Analytics`
2. Verify the app launches from the Start Menu shortcut
3. Verify databases are copied to `%LOCALAPPDATA%\SeamAnalytics\`
4. If you bumped the calc schema version, verify the old calc DB gets replaced
5. Verify the scheduled task is created if selected during install

---

## 6. Create GitHub Release

1. Commit and push all changes:
   ```bash
   cd "D:\MLB Stats"
   git add -A
   git commit -m "release: vX.Y.Z"
   git push origin main
   ```
2. Tag the release:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
3. On GitHub, create a new Release from the tag
4. Title: `vX.Y.Z`
5. Paste the relevant section from `UPDATES.md` as the release body
6. Attach `installer/output/SeamAnalytics-Setup-X.Y.Z.exe` as a release asset

The app's auto-update checker looks for the latest GitHub Release and compares the tag against `APP_VERSION`. Users will see the update notification on next launch.

---

## Quick Reference — All Files to Touch

| Step | File | What to change |
|------|------|----------------|
| Version bump | `app/_app_paths.py` | `APP_VERSION` |
| Version bump | `installer/seam_setup.iss` | `#define MyAppVersion` |
| Version bump | `README.md` | Badge URL |
| Schema bump | `app/_app_paths.py` | `RAW_DB_SCHEMA_VERSION` / `CALC_DB_SCHEMA_VERSION` |
| Schema bump | `installer/seam_setup.iss` | `#define RawDBSchemaVersion` / `#define CalcDBSchemaVersion` |
| Schema bump | `app/database_schema.py` | Table DDL (if columns changed) |
| New module | `installer/seam_app.spec` | `hiddenimports` list |
| New module | `installer/daily_update.spec` | `hiddenimports` list (if used by updater) |
| Release notes | `UPDATES.md` | New version section at top |

---

## Troubleshooting

**PyInstaller can't find a module:**
Add it to `hiddenimports` in the relevant `.spec` file. Common culprits are new local modules (`_http_utils`, `_ui_utils`) and lazy imports inside functions.

**Installer doesn't replace the calc DB on upgrade:**
Check that `CALC_DB_SCHEMA_VERSION` in `_app_paths.py` matches `#define CalcDBSchemaVersion` in `seam_setup.iss`, and that the `.schema_version` marker file was rebuilt.

**App shows wrong version in title bar:**
The window title reads from `_app_paths.APP_VERSION`. Make sure you bumped it and rebuilt the exe.

**Update checker doesn't detect the new release:**
The checker compares the GitHub Release tag (e.g. `v1.1.2`) against `APP_VERSION` (`1.1.2`). Ensure the tag starts with `v` followed by the exact version string.

**`sqlite3.OperationalError: table … has no column named X`:**
You added a column to a `CREATE TABLE` in `build_calculated_db.py` (or `database_schema.py` for raw) but didn't bump the schema version. `CREATE TABLE IF NOT EXISTS` never alters an existing table — the old column set persists. Fix:
1. Bump `CALC_DB_SCHEMA_VERSION` (or `RAW_DB_SCHEMA_VERSION`) in `_app_paths.py` **and** `seam_setup.iss`
2. Rebuild: `python build_calculated_db.py` (this drops & recreates the calc tables)
3. Rebuild executables and installer

---

## Appendix: Schema Change Walkthrough

Step-by-step guide for when you add, remove, or rename columns in calculated or raw tables.

### A. Calculated DB column change

1. **Edit the DDL** — Add/remove the column in the `CREATE TABLE` statement inside `_create_calc_tables()` in `build_calculated_db.py`.
2. **Edit the INSERT** — Update the matching `INSERT INTO … VALUES` statement (and the Python code that computes the value) in the same file.
3. **Bump the schema version:**
   ```python
   # app/_app_paths.py
   CALC_DB_SCHEMA_VERSION = 4   # was 3 — added xyz column
   ```
   ```ini
   ; installer/seam_setup.iss
   #define CalcDBSchemaVersion  "4"
   ```
4. **Rebuild the calculated database:**
   ```bash
   cd "D:\MLB Stats"
   cd app
   python build_calculated_db.py
   ```
   On startup `ensure_calc_schema()` detects the version mismatch, drops all calc tables, recreates them with the new columns, and rebuilds every season.
5. **Verify the marker file:**
   ```bash
   type mlb_calculated.db.schema_version
   ```
   Should print the new version number.
6. **Update the UI** (if applicable) — If `seam_app.py` displays the new column, add it to the relevant query / display code.
7. **Rebuild executables and installer** (Steps 4–5 of the main checklist).

### B. Raw DB column change

1. **Edit the DDL** — Add/remove the column in `database_schema.py`.
2. **Edit the ingestion code** — Update `build_raw_db.py` (and `pybaseball_to_schema_mapping.json` if the column comes from Statcast).
3. **Add a migration** — In `build_raw_db.create_db()`, add an `ALTER TABLE … ADD COLUMN` statement guarded by the new version so existing databases get the column without a full re-download.
4. **Bump the schema version:**
   ```python
   # app/_app_paths.py
   RAW_DB_SCHEMA_VERSION = 3   # was 2 — added xyz column
   ```
   ```ini
   ; installer/seam_setup.iss
   #define RawDBSchemaVersion  "3"
   ```
5. **Rebuild the raw database** (or run `daily_update.py` which triggers the migration on connect).
6. **Rebuild executables and installer.**

### C. Common pitfalls

| Pitfall | Symptom | Fix |
|---------|---------|-----|
| Forgot to bump schema version | `OperationalError: table … has no column named X` | Bump version in `_app_paths.py` + `seam_setup.iss`, rebuild DB |
| Bumped version in Python but not `.iss` | Installer ships stale calc DB; users see mismatch on first run | Keep both files in sync |
| Added column to DDL but not to INSERT | `OperationalError: table … has N columns but M values were supplied` | Match column list in CREATE and INSERT |
| Changed calc columns without full rebuild | Stale data from old schema persists | Run `python build_calculated_db.py` — the version bump triggers a full drop+recreate |
