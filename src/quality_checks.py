"""Post-run data quality gate. Exits non-zero if any check fails, so a scheduler can alert."""
import sys

CHECKS = {
    "silver primary key is unique":
        "SELECT count(*) - count(DISTINCT activity_id) FROM silver.activities",
    "silver has no NULL athlete_id":
        "SELECT count(*) FROM silver.activities WHERE athlete_id IS NULL",
    "silver has no negative distance":
        "SELECT count(*) FROM silver.activities WHERE distance_m < 0",
    "bronze rows reconcile: bronze = silver + rejected + superseded/duplicates (must be >= 0)":
        """SELECT CASE WHEN (SELECT count(*) FROM bronze.raw_events)
                           >= (SELECT count(*) FROM silver.activities) + (SELECT count(*) FROM silver.rejected_events)
                      THEN 0 ELSE 1 END""",
    "gold distance equals silver distance (km)":
        """SELECT abs((SELECT coalesce(sum(distance_km),0) FROM gold.daily_athlete_stats)
                    - (SELECT coalesce(sum(distance_m),0)/1000 FROM silver.activities)) > 0.01""",
    "gold activity count equals silver activity count":
        """SELECT (SELECT coalesce(sum(activities),0) FROM gold.daily_athlete_stats)
                - (SELECT count(*) FROM silver.activities)""",
    "no activity is dated in the future":
        "SELECT count(*) FROM silver.activities WHERE started_at > now()",
}


def run_checks(con):
    results = []
    for name, sql in CHECKS.items():
        bad = int(con.execute(sql).fetchone()[0])
        results.append((name, bad == 0))
    return results


if __name__ == "__main__":
    from pipeline import connect
    res = run_checks(connect(*(sys.argv[1:2])))
    for name, ok in res:
        print(("PASS  " if ok else "FAIL  ") + name)
    sys.exit(0 if all(ok for _, ok in res) else 1)
