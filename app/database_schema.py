SCHEMA_SQL = """
-- MLB Stats Raw Data Database Schema (minimal validated subset)
-- This file defines the core CREATE TABLE / INDEX / VIEW statements used
-- by the ingestion pipeline. Keep this file valid Python (a module)
-- so editors and imports don't fail; build_raw_db.py reads this file
-- as plain text and extracts SQL DDL statements using regex.

-- ============================================================================
-- TABLE 1: games
-- ============================================================================
CREATE TABLE IF NOT EXISTS games (
  game_id TEXT PRIMARY KEY,
  game_date DATE NOT NULL,
  season INTEGER NOT NULL,
  game_type TEXT,
  game_year INTEGER,
  away_team TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_score INTEGER,
  home_score INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TABLE 2: pitchers (reference)
-- ============================================================================
CREATE TABLE IF NOT EXISTS pitchers (
  pitcher_id INTEGER PRIMARY KEY,
  pitcher_name TEXT NOT NULL,
  p_throws TEXT CHECK (p_throws IN ('L','R')),
  team TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TABLE 3: plate_appearances (one row per PA)
-- ============================================================================
CREATE TABLE IF NOT EXISTS plate_appearances (
  pa_id TEXT PRIMARY KEY,
  game_id TEXT NOT NULL,
  batter_id INTEGER NOT NULL,
  pitcher_id INTEGER NOT NULL,
  game_date DATE NOT NULL,
  season INTEGER NOT NULL,
  game_year INTEGER,
  game_type TEXT,
  inning INTEGER,
  outs_when_up INTEGER,
  batter_name TEXT,
  stand TEXT CHECK (stand IN ('L','R')),
  position TEXT,
  pitcher_name TEXT,
  p_throws TEXT CHECK (p_throws IN ('L','R')),
  pitch_type_primary TEXT,
  pitch_name TEXT,
  is_vs_lefty INTEGER,
  events TEXT,
  des TEXT,
  description TEXT,
  result TEXT,
  is_hit INTEGER,
  is_home_run INTEGER,
  is_ab INTEGER,
  is_walk INTEGER,
  is_strikeout INTEGER,
  is_hbp INTEGER,
  runs INTEGER,
  rbi INTEGER,
  outs_recorded INTEGER DEFAULT 0,
  earned_runs INTEGER DEFAULT 0,
  is_single INTEGER,
  is_double INTEGER,
  is_triple INTEGER,
  total_bases INTEGER,
  is_sac_fly INTEGER,
  is_sac_bunt INTEGER,
  launch_speed REAL,
  bat_speed REAL,
  launch_angle REAL,
  spray_angle REAL,
  hc_x REAL,
  hc_y REAL,
  on_1b INTEGER,
  on_2b INTEGER,
  on_3b INTEGER,
  bb_type TEXT,
  hit_location INTEGER,
  hit_distance_sc REAL,
  estimated_ba_using_speedangle REAL,
  estimated_woba_using_speedangle REAL,
  woba_value REAL,
  woba_denom REAL,
  babip_value REAL,
  iso_value REAL,
  launch_speed_angle REAL,
  at_bat_number INTEGER,
  statcast_at_bat_number INTEGER,
  barrel INTEGER,
  barrel_intensity REAL,
  pull INTEGER,
  opposite_field INTEGER,
  up_middle INTEGER,
  balls INTEGER,
  strikes INTEGER,
  release_speed REAL,
  release_spin_rate INTEGER,
  spin_axis REAL,
  effective_speed REAL,
  release_extension REAL,
  pitch_call TEXT,
  swing INTEGER,
  contact INTEGER,
  zone INTEGER,
  batter_is_home INTEGER,
  home_team TEXT,
  away_team TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (game_id) REFERENCES games(game_id),
  FOREIGN KEY (pitcher_id) REFERENCES pitchers(pitcher_id)
);

-- ============================================================================
-- TABLE 4: pitching_appearances (one row per pitch)
-- ============================================================================
CREATE TABLE IF NOT EXISTS pitching_appearances (
  pitch_id TEXT PRIMARY KEY,
  game_id TEXT NOT NULL,
  pitcher_id INTEGER NOT NULL,
  batter_id INTEGER NOT NULL,
  game_date DATE NOT NULL,
  season INTEGER NOT NULL,
  inning INTEGER,
  pitch_number INTEGER,
  pitcher_name TEXT,
  p_throws TEXT CHECK (p_throws IN ('L','R')),
  batter_name TEXT,
  stand TEXT CHECK (stand IN ('L','R')),
  pitch_type TEXT,
  release_speed REAL,
  release_spin_rate INTEGER,
  spin_axis REAL,
  plate_x REAL,
  plate_z REAL,
  release_x REAL,
  release_z REAL,
  release_pos_x REAL,
  release_pos_y REAL,
  release_pos_z REAL,
  pfx_x REAL,
  pfx_z REAL,
  pitch_name TEXT,
  pitch_call TEXT,
  swing INTEGER,
  contact INTEGER,
  zone INTEGER,
  is_strikeout INTEGER,
  is_hit INTEGER,
  is_home_run INTEGER,
  balls INTEGER,
  strikes INTEGER,
  outs_in_inning INTEGER,
  pitcher_is_home INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (game_id) REFERENCES games(game_id),
  FOREIGN KEY (pitcher_id) REFERENCES pitchers(pitcher_id)
);

-- ============================================================================
-- TABLE 5: players_metadata
-- ============================================================================
CREATE TABLE IF NOT EXISTS players_metadata (
  player_id INTEGER PRIMARY KEY,
  player_name TEXT NOT NULL,
  handedness TEXT CHECK (handedness IN ('L','R','S')),
  p_throws TEXT CHECK (p_throws IN ('L','R')),
  position TEXT,
  team TEXT,
  debut_year INTEGER,
  is_active INTEGER,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- stolen_bases (event-level)
-- ============================================================================
CREATE TABLE IF NOT EXISTS stolen_bases (
  event_id TEXT PRIMARY KEY,
  game_id TEXT NOT NULL,
  season INTEGER NOT NULL,
  game_date DATE,
  runner_id INTEGER,
  runner_name TEXT,
  event_type TEXT,
  is_successful INTEGER,
  pitcher_id INTEGER,
  pitcher_name TEXT,
  catcher_id INTEGER,
  catcher_name TEXT,
  base TEXT,
  top_speed REAL,
  sprint_speed REAL,
  bolts INTEGER,
  competitive_runs INTEGER,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sprint speeds reference
CREATE TABLE IF NOT EXISTS sprint_speeds (
  player_id INTEGER NOT NULL,
  season INTEGER NOT NULL,
  sprint_speed REAL,
  bolts INTEGER,
  competitive_runs INTEGER,
  games_sampled INTEGER,
  PRIMARY KEY (player_id, season)
);

-- Pitcher delivery time to plate (Baseball Savant pitch-tempo leaderboard)
CREATE TABLE IF NOT EXISTS pitcher_tempo (
  player_id INTEGER NOT NULL,
  season INTEGER NOT NULL,
  total_pitches INTEGER,
  median_seconds_empty REAL,
  median_seconds_on_base REAL,
  time_to_home_avg REAL,
  secondary_lead_allowed REAL,
  PRIMARY KEY (player_id, season)
);

-- Catcher pop time (Baseball Savant poptime leaderboard)
CREATE TABLE IF NOT EXISTS catcher_poptime (
  player_id INTEGER NOT NULL,
  season INTEGER NOT NULL,
  pop_2b_sba_count INTEGER,
  pop_2b_sba REAL,
  pop_2b_cs REAL,
  pop_2b_sb REAL,
  pop_3b_sba_count INTEGER,
  pop_3b_sba REAL,
  csaa_per_throw REAL,
  exchange_2b_3b_sba REAL,
  PRIMARY KEY (player_id, season)
);

-- Runner primary/secondary lead distance (Baseball Savant running-game leaderboard)
CREATE TABLE IF NOT EXISTS runner_lead (
  player_id INTEGER NOT NULL,
  season INTEGER NOT NULL,
  primary_lead_avg REAL,
  secondary_lead_avg REAL,
  PRIMARY KEY (player_id, season)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_runner_lead_player_season ON runner_lead(player_id, season);
CREATE INDEX IF NOT EXISTS idx_stolen_runner ON stolen_bases(runner_id);
CREATE INDEX IF NOT EXISTS idx_stolen_pitcher ON stolen_bases(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_stolen_catcher ON stolen_bases(catcher_id);
CREATE INDEX IF NOT EXISTS idx_stolen_season ON stolen_bases(season);
CREATE INDEX IF NOT EXISTS idx_sprint_player_season ON sprint_speeds(player_id, season);
CREATE INDEX IF NOT EXISTS idx_pitcher_tempo_player_season ON pitcher_tempo(player_id, season);
CREATE INDEX IF NOT EXISTS idx_catcher_poptime_player_season ON catcher_poptime(player_id, season);

CREATE INDEX IF NOT EXISTS idx_games_game_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);
CREATE INDEX IF NOT EXISTS idx_pa_game_date ON plate_appearances(game_date);
CREATE INDEX IF NOT EXISTS idx_pa_season ON plate_appearances(season);
CREATE INDEX IF NOT EXISTS idx_pa_batter_id ON plate_appearances(batter_id);
CREATE INDEX IF NOT EXISTS idx_pa_pitcher_id ON plate_appearances(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pa_batter_season ON plate_appearances(batter_id, season);
CREATE INDEX IF NOT EXISTS idx_pa_pitcher_season ON plate_appearances(pitcher_id, season);
CREATE INDEX IF NOT EXISTS idx_pa_batter_pitcher ON plate_appearances(batter_id, pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pa_is_vs_lefty ON plate_appearances(is_vs_lefty);
CREATE INDEX IF NOT EXISTS idx_pa_events ON plate_appearances(events);

-- Composite indexes for common multi-column query patterns
CREATE INDEX IF NOT EXISTS idx_pa_game_id ON plate_appearances(game_id);
CREATE INDEX IF NOT EXISTS idx_pa_game_batter_home ON plate_appearances(game_id, batter_is_home);
CREATE INDEX IF NOT EXISTS idx_pa_game_date_pitcher ON plate_appearances(game_date, pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pa_game_date_batter ON plate_appearances(game_date, batter_id);
CREATE INDEX IF NOT EXISTS idx_pa_season_pitcher ON plate_appearances(season, pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pa_season_batter ON plate_appearances(season, batter_id);
CREATE INDEX IF NOT EXISTS idx_stolen_game_id ON stolen_bases(game_id);
CREATE INDEX IF NOT EXISTS idx_games_home_away ON games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_pitch_pitcher_season ON pitching_appearances(pitcher_id, season);

CREATE INDEX IF NOT EXISTS idx_pitch_game_date ON pitching_appearances(game_date);
CREATE INDEX IF NOT EXISTS idx_pitch_season ON pitching_appearances(season);
CREATE INDEX IF NOT EXISTS idx_pitch_pitcher_id ON pitching_appearances(pitcher_id);

-- Composite indexes on stolen_bases for season-scoped baserunning queries
CREATE INDEX IF NOT EXISTS idx_stolen_season_runner ON stolen_bases(season, runner_id);
CREATE INDEX IF NOT EXISTS idx_stolen_season_pitcher ON stolen_bases(season, pitcher_id);
CREATE INDEX IF NOT EXISTS idx_stolen_season_catcher ON stolen_bases(season, catcher_id);
CREATE INDEX IF NOT EXISTS idx_stolen_game_date ON stolen_bases(game_date);

-- Aggregated views (conservative/simple implementations)
CREATE VIEW IF NOT EXISTS batting_stats_season AS
SELECT
  season,
  batter_id,
  batter_name,
  COUNT(*) AS plate_appearances,
  SUM(COALESCE(is_ab,0)) AS at_bats,
  SUM(COALESCE(is_hit,0)) AS hits,
  SUM(COALESCE(is_single,0)) AS singles,
  SUM(COALESCE(is_double,0)) AS doubles,
  SUM(COALESCE(is_triple,0)) AS triples,
  SUM(COALESCE(is_home_run,0)) AS home_runs,
  SUM(COALESCE(runs,0)) AS runs,
  SUM(COALESCE(rbi,0)) AS rbis,
  SUM(COALESCE(total_bases,0)) AS total_bases,
  SUM(COALESCE(is_walk,0)) AS walks,
  SUM(COALESCE(is_strikeout,0)) AS strikeouts
FROM plate_appearances
GROUP BY season, batter_id;

CREATE VIEW IF NOT EXISTS pitching_stats_season AS
WITH pa AS (
  SELECT season, pitcher_id, pitcher_name,
    COUNT(*) AS plate_appearances,
    SUM(COALESCE(is_strikeout,0)) AS strikeouts,
    SUM(COALESCE(is_walk,0)) AS walks,
    SUM(COALESCE(runs,0)) AS runs_allowed,
    SUM(COALESCE(earned_runs,0)) AS earned_runs,
    SUM(COALESCE(outs_recorded,0)) AS outs_recorded
  FROM plate_appearances
  GROUP BY season, pitcher_id
), pitches AS (
  SELECT season, pitcher_id,
    COUNT(*) AS pitches_thrown,
    SUM(CASE WHEN COALESCE(swing,0)=1 AND COALESCE(contact,0)=0 THEN 1 ELSE 0 END) AS swinging_strikes
  FROM pitching_appearances
  GROUP BY season, pitcher_id
)
SELECT
  pa.season AS season,
  pa.pitcher_id AS pitcher_id,
  pa.pitcher_name AS pitcher_name,
  COALESCE(pa.plate_appearances,0) AS plate_appearances,
  COALESCE(pa.strikeouts,0) AS strikeouts,
  COALESCE(pa.walks,0) AS walks,
  COALESCE(pa.runs_allowed,0) AS runs_allowed,
  COALESCE(pa.earned_runs,0) AS earned_runs,
  COALESCE(pa.outs_recorded,0) AS outs_recorded,
  ROUND(COALESCE(CAST(pa.outs_recorded AS FLOAT)/3.0,0), 2) AS innings_pitched,
  ROUND(CAST(COALESCE(pa.strikeouts,0) AS FLOAT) / NULLIF(CAST(pa.outs_recorded AS FLOAT)/3.0,0) * 9.0, 2) AS k_per_9,
  ROUND(CAST(COALESCE(pa.walks,0) AS FLOAT) / NULLIF(CAST(pa.outs_recorded AS FLOAT)/3.0,0) * 9.0, 2) AS bb_per_9,
  ROUND(CAST(COALESCE(pa.earned_runs,0) AS FLOAT) * 9.0 / NULLIF(CAST(pa.outs_recorded AS FLOAT)/3.0,0), 2) AS era,
  ROUND(CAST(COALESCE(pitches.swinging_strikes,0) AS FLOAT) / NULLIF(COALESCE(pitches.pitches_thrown,0),0), 3) AS whiff_pct,
  COALESCE(pitches.pitches_thrown,0) AS pitches_thrown,
  COALESCE(pitches.swinging_strikes,0) AS swinging_strikes
FROM pa
LEFT JOIN pitches ON pa.season = pitches.season AND pa.pitcher_id = pitches.pitcher_id
UNION ALL
SELECT
  pitches.season AS season,
  pitches.pitcher_id AS pitcher_id,
  '' AS pitcher_name,
  0 AS plate_appearances,
  0 AS strikeouts,
  0 AS walks,
  0 AS runs_allowed,
  0 AS earned_runs,
  0 AS outs_recorded,
  0.0 AS innings_pitched,
  0.0 AS k_per_9,
  0.0 AS bb_per_9,
  0.0 AS era,
  ROUND(CAST(COALESCE(pitches.swinging_strikes,0) AS FLOAT) / NULLIF(COALESCE(pitches.pitches_thrown,0),0), 3) AS whiff_pct,
  COALESCE(pitches.pitches_thrown,0) AS pitches_thrown,
  COALESCE(pitches.swinging_strikes,0) AS swinging_strikes
FROM pitches
LEFT JOIN pa ON pa.season = pitches.season AND pa.pitcher_id = pitches.pitcher_id
WHERE pa.pitcher_id IS NULL;

"""

SCHEMA_EXPLANATION = """
Minimal schema used by ingestion and calculated builders. The full schema
originally contained many more Statcast fields; this module provides the
core tables and views required for daily ingestion and aggregation.
"""

# Helpful when running this file directly for quick inspection
if __name__ == '__main__':
    print(SCHEMA_EXPLANATION)
