# Strava-Style Activity Pipeline (DuckDB)

Offline-first activity ingestion with a Bronze -> Silver -> Gold architecture.

## Problems it solves
| Real-world problem | How the pipeline handles it |
|---|---|
| Phone uploads later (late arrivals) | Gold is recomputed for the activity's start date, not its upload date |
| App retries uploads (duplicates) | Silver keeps one row per activity_id |
| User edits an activity (new version) | Highest version wins; an older version arriving late cannot overwrite it |
| Edit changes the activity date | Both the old and new athlete-day are recomputed |
| Bad records | Quarantined in `silver.rejected_events` with a reason |
| Re-running a job | File-level load log + primary-key upserts make every run idempotent |

## Run
    pip install -r requirements.txt
    python src/generate_data.py data/landing
    python src/pipeline.py warehouse.duckdb data/landing
    python src/quality_checks.py warehouse.duckdb
    pytest -q

## Layers
- bronze.raw_events: append-only raw copy with source file and ingestion time
- silver.activities: validated, deduplicated, latest version per activity
- gold.daily_athlete_stats: incremental daily aggregates; gold.weekly_leaderboard view on top

## Sample result (3 daily batches)
665 raw events -> 600 clean activities, 9 quarantined, 56 duplicates/superseded versions;
re-running loads 0 rows and recomputes 0 athlete-days; 7/7 quality checks pass.
