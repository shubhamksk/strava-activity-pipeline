"""Bronze -> Silver -> Gold pipeline on DuckDB.

Design rules (each one answers a real failure mode):
  * Bronze is append-only and never edited, so we can always replay history.
  * Every layer is IDEMPOTENT: running the same input twice changes nothing.
  * Silver keeps ONE row per activity: the newest version wins.
  * Bad rows go to a quarantine table with a reason; they are never silently dropped.
  * Gold is rebuilt only for the athlete-days that changed (incremental).
  * Silver and Gold are written in ONE transaction, so a crash can never leave them out of sync.
"""
import sys
from pathlib import Path

import duckdb

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.load_log (
    source_file VARCHAR PRIMARY KEY, rows_loaded BIGINT, loaded_at TIMESTAMP DEFAULT now());

CREATE TABLE IF NOT EXISTS bronze.raw_events (
    activity_id VARCHAR, athlete_id VARCHAR, device_id VARCHAR, sport_type VARCHAR,
    started_at TIMESTAMP, distance_m DOUBLE, moving_time_s BIGINT, elevation_gain_m DOUBLE,
    avg_heartrate BIGINT, uploaded_at TIMESTAMP, version BIGINT,
    _source_file VARCHAR, _ingested_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS silver.activities (
    activity_id VARCHAR PRIMARY KEY, athlete_id VARCHAR NOT NULL, device_id VARCHAR,
    sport_type VARCHAR, started_at TIMESTAMP, activity_date DATE, distance_m DOUBLE,
    moving_time_s BIGINT, elevation_gain_m DOUBLE, avg_heartrate BIGINT,
    uploaded_at TIMESTAMP, version BIGINT);

CREATE TABLE IF NOT EXISTS silver.processed_files (source_file VARCHAR PRIMARY KEY);

CREATE TABLE IF NOT EXISTS silver.rejected_events (
    activity_id VARCHAR, athlete_id VARCHAR, reason VARCHAR, _source_file VARCHAR,
    rejected_at TIMESTAMP DEFAULT now());

CREATE TABLE IF NOT EXISTS gold.daily_athlete_stats (
    athlete_id VARCHAR, activity_date DATE, activities BIGINT, distance_km DOUBLE,
    moving_time_min DOUBLE, elevation_gain_m DOUBLE, PRIMARY KEY (athlete_id, activity_date));

CREATE OR REPLACE VIEW gold.weekly_leaderboard AS
SELECT date_trunc('week', activity_date) AS week_start, athlete_id,
       sum(distance_km) AS distance_km, sum(activities) AS activities,
       rank() OVER (PARTITION BY date_trunc('week', activity_date)
                    ORDER BY sum(distance_km) DESC) AS rank_in_week
FROM gold.daily_athlete_stats GROUP BY 1, 2;
"""


def connect(db_path="warehouse.duckdb"):
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    return con


def load_bronze(con, landing_dir):
    """Append new files only. load_log makes re-runs a no-op (file-level idempotency)."""
    loaded = 0
    for f in sorted(Path(landing_dir).glob("*.ndjson")):
        if con.execute("SELECT 1 FROM bronze.load_log WHERE source_file = ?", [f.name]).fetchone():
            continue
        con.execute("BEGIN")
        n = con.execute(f"""
            INSERT INTO bronze.raw_events
            SELECT activity_id, athlete_id, device_id, sport_type, started_at::TIMESTAMP,
                   distance_m, moving_time_s, elevation_gain_m, avg_heartrate,
                   uploaded_at::TIMESTAMP, version, ? AS _source_file, now() AS _ingested_at
            FROM read_json_auto('{f.as_posix()}', format='newline_delimited')""", [f.name]).fetchone()[0]
        con.execute("INSERT INTO bronze.load_log (source_file, rows_loaded) VALUES (?, ?)", [f.name, n])
        con.execute("COMMIT")
        loaded += n
    return loaded


def build_silver(con):
    """Validate, quarantine bad rows, dedupe, and upsert newest version per activity.
    Returns the set of (athlete_id, activity_date) whose silver data changed."""
    con.execute("CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM bronze.raw_events "
                "WHERE _source_file IN (SELECT source_file FROM bronze.load_log "
                "WHERE source_file NOT IN (SELECT source_file FROM silver.processed_files))")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE verdict AS
        SELECT *, CASE
            WHEN activity_id IS NULL                  THEN 'missing_activity_id'
            WHEN athlete_id IS NULL                   THEN 'missing_athlete_id'
            WHEN distance_m IS NULL OR distance_m < 0 THEN 'invalid_distance'
            WHEN moving_time_s IS NULL OR moving_time_s <= 0 THEN 'invalid_duration'
            WHEN started_at > now()                   THEN 'start_in_future'
        END AS reason FROM batch""")

    con.execute("""INSERT INTO silver.rejected_events (activity_id, athlete_id, reason, _source_file)
                   SELECT activity_id, athlete_id, reason, _source_file FROM verdict WHERE reason IS NOT NULL""")

    # Newest version wins; ties broken by latest upload. Candidates include what silver already holds,
    # so a late-arriving OLD version can never overwrite a newer one.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE winners AS
        SELECT * EXCLUDE rn FROM (
            SELECT *, row_number() OVER (PARTITION BY activity_id ORDER BY version DESC, uploaded_at DESC) AS rn
            FROM (
                SELECT activity_id, athlete_id, device_id, sport_type, started_at,
                       started_at::DATE AS activity_date, distance_m, moving_time_s,
                       elevation_gain_m, avg_heartrate, uploaded_at, version
                FROM verdict WHERE reason IS NULL
                UNION ALL
                SELECT * FROM silver.activities
                WHERE activity_id IN (SELECT activity_id FROM verdict WHERE reason IS NULL)
            )) WHERE rn = 1""")

    # Which athlete-days are touched? Include the OLD date in case an edit moved the activity.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE touched AS
        SELECT DISTINCT athlete_id, activity_date FROM winners
        UNION
        SELECT DISTINCT athlete_id, activity_date FROM silver.activities
        WHERE activity_id IN (SELECT activity_id FROM winners)""")

    con.execute("INSERT OR REPLACE INTO silver.activities SELECT * FROM winners")
    con.execute("INSERT INTO silver.processed_files SELECT source_file FROM bronze.load_log "
                "WHERE source_file NOT IN (SELECT source_file FROM silver.processed_files)")
    return con.execute("SELECT count(*) FROM touched").fetchone()[0]


def build_gold(con):
    """Recompute only the athlete-days that changed. The caller owns the transaction."""
    con.execute("""DELETE FROM gold.daily_athlete_stats d USING touched t
                   WHERE d.athlete_id = t.athlete_id AND d.activity_date = t.activity_date""")
    con.execute("""
        INSERT INTO gold.daily_athlete_stats
        SELECT a.athlete_id, a.activity_date, count(*), round(sum(a.distance_m) / 1000, 3),
               round(sum(a.moving_time_s) / 60.0, 2), round(sum(a.elevation_gain_m), 1)
        FROM silver.activities a JOIN touched t USING (athlete_id, activity_date)
        GROUP BY a.athlete_id, a.activity_date""")


def run(db_path="warehouse.duckdb", landing_dir="data/landing"):
    con = connect(db_path)
    loaded = load_bronze(con, landing_dir)
    # Silver + Gold commit together or not at all. If anything fails, the file stays
    # "unprocessed" and the next run redoes both layers from Bronze (no half-finished state).
    con.execute("BEGIN")
    try:
        touched = build_silver(con)
        build_gold(con)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return con, {"bronze_rows_loaded": loaded, "athlete_days_recomputed": touched}


if __name__ == "__main__":
    con, stats = run(*(sys.argv[1:3]))
    print(stats)
