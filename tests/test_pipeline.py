import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pipeline
import quality_checks


def ev(activity_id="A1", athlete="U1", version=1, distance=5000.0, started="2026-10-01T08:00:00",
       uploaded="2026-10-01T09:00:00"):
    return {"activity_id": activity_id, "athlete_id": athlete, "device_id": "D1", "sport_type": "Run",
            "started_at": started, "distance_m": distance, "moving_time_s": 1800, "elevation_gain_m": 10.0,
            "avg_heartrate": 140, "uploaded_at": uploaded, "version": version}


def write(land, name, rows):
    land.mkdir(exist_ok=True)
    (land / name).write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def setup(tmp_path):
    return tmp_path / "landing", str(tmp_path / "w.duckdb")


def test_rerun_is_idempotent(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1"), ev("A2")])
    pipeline.run(db, land)
    con, stats = pipeline.run(db, land)           # same input again
    assert stats["bronze_rows_loaded"] == 0
    assert con.execute("SELECT count(*) FROM silver.activities").fetchone()[0] == 2


def test_duplicate_uploads_collapse_to_one_row(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1"), ev("A1"), ev("A1")])
    con, _ = pipeline.run(db, land)
    assert con.execute("SELECT count(*) FROM silver.activities").fetchone()[0] == 1


def test_newer_version_wins_and_gold_updates(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1", distance=5000.0)])
    pipeline.run(db, land)
    write(land, "b2.ndjson", [ev("A1", version=2, distance=6000.0, uploaded="2026-10-02T09:00:00")])
    con, _ = pipeline.run(db, land)
    assert con.execute("SELECT distance_m FROM silver.activities").fetchone()[0] == 6000.0
    assert con.execute("SELECT distance_km FROM gold.daily_athlete_stats").fetchone()[0] == 6.0


def test_late_arriving_old_version_cannot_overwrite_newer(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1", version=2, distance=6000.0)])
    pipeline.run(db, land)
    write(land, "b2.ndjson", [ev("A1", version=1, distance=5000.0, uploaded="2026-10-05T09:00:00")])
    con, _ = pipeline.run(db, land)
    assert con.execute("SELECT version, distance_m FROM silver.activities").fetchone() == (2, 6000.0)


def test_bad_rows_are_quarantined_not_dropped(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1"), ev("A2", distance=-1.0), ev("A3", athlete=None),
                              ev("A4", started="2099-01-01T00:00:00")])
    con, _ = pipeline.run(db, land)
    reasons = dict(con.execute("SELECT activity_id, reason FROM silver.rejected_events").fetchall())
    assert reasons == {"A2": "invalid_distance", "A3": "missing_athlete_id", "A4": "start_in_future"}
    assert con.execute("SELECT count(*) FROM silver.activities").fetchone()[0] == 1


def test_edit_that_moves_the_date_updates_both_days(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1", started="2026-10-01T08:00:00")])
    pipeline.run(db, land)
    write(land, "b2.ndjson", [ev("A1", version=2, started="2026-10-03T08:00:00", uploaded="2026-10-03T10:00:00")])
    con, _ = pipeline.run(db, land)
    dates = [str(r[0]) for r in con.execute("SELECT activity_date FROM gold.daily_athlete_stats").fetchall()]
    assert dates == ["2026-10-03"]               # the stale 10-01 row must be gone


def test_quality_gate_passes_on_clean_run_and_fails_on_corruption(tmp_path):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1"), ev("A2")])
    con, _ = pipeline.run(db, land)
    assert all(ok for _, ok in quality_checks.run_checks(con))
    con.execute("UPDATE gold.daily_athlete_stats SET distance_km = distance_km + 100")
    assert not all(ok for _, ok in quality_checks.run_checks(con))


def test_crash_between_silver_and_gold_leaves_no_half_state(tmp_path, monkeypatch):
    land, db = setup(tmp_path)
    write(land, "b1.ndjson", [ev("A1"), ev("A2", distance=-1.0)])
    real_gold = pipeline.build_gold

    def boom(con):
        raise RuntimeError("simulated crash before gold finished")

    monkeypatch.setattr(pipeline, "build_gold", boom)
    try:
        pipeline.run(db, land)
    except RuntimeError:
        pass
    monkeypatch.setattr(pipeline, "build_gold", real_gold)   # the retry
    con, _ = pipeline.run(db, land)
    assert con.execute("SELECT count(*) FROM silver.activities").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM gold.daily_athlete_stats").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM silver.rejected_events").fetchone()[0] == 1
    assert all(ok for _, ok in quality_checks.run_checks(con))
