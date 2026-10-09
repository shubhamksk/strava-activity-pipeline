"""Simulate what a Strava-style backend receives from phones and watches.

Offline-first means devices record activities with no signal and upload later.
That produces three real-world problems the pipeline must survive:
  1. Late arrivals    - an activity started on day 1 can land on day 3.
  2. Duplicates       - the app retries an upload, so the same event arrives twice.
  3. Corrections      - the user edits an activity; a newer version arrives later.
Plus plain bad data (negative distance, missing athlete) to exercise validation.
"""
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

SPORTS = {"Run": (5, 9), "Ride": (15, 40), "Walk": (2, 6), "Swim": (1, 3)}  # km range


def make_activity(rng, activity_id, day):
    sport = rng.choice(list(SPORTS))
    lo, hi = SPORTS[sport]
    distance_m = round(rng.uniform(lo, hi) * 1000, 1)
    speed = {"Run": 2.9, "Ride": 7.5, "Walk": 1.4, "Swim": 0.9}[sport] * rng.uniform(0.8, 1.2)
    started = day + timedelta(hours=rng.randint(5, 20), minutes=rng.randint(0, 59))
    return {
        "activity_id": f"A{activity_id:06d}",
        "athlete_id": f"U{rng.randint(1, 40):04d}",
        "device_id": f"D{rng.randint(1, 60):04d}",
        "sport_type": sport,
        "started_at": started.isoformat(),
        "distance_m": distance_m,
        "moving_time_s": int(distance_m / speed),
        "elevation_gain_m": round(rng.uniform(0, 600), 1) if sport in ("Run", "Ride") else 0.0,
        "avg_heartrate": rng.randint(110, 175) if rng.random() > 0.2 else None,
        "uploaded_at": (started + timedelta(minutes=rng.randint(30, 120))).isoformat(),
        "version": 1,
    }


def generate(out_dir, days=3, per_day=200, seed=42, start="2026-10-01"):
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    base = datetime.fromisoformat(start)
    history, counter = [], 0
    for d in range(days):
        day = base + timedelta(days=d)
        rows = []
        for _ in range(per_day):
            counter += 1
            rows.append(make_activity(rng, counter, day))
        # Late arrivals: a few activities from earlier days only reach us today.
        for old in rng.sample(history, min(10, len(history))) if history else []:
            late = dict(old)
            late["uploaded_at"] = (day + timedelta(hours=rng.randint(1, 20))).isoformat()
            if rng.random() < 0.5:           # half of them are user edits
                late["version"] = old["version"] + 1
                late["distance_m"] = round(old["distance_m"] * 1.02, 1)
            rows.append(late)
        # Retried uploads: exact duplicates.
        rows += [dict(r) for r in rng.sample(rows, 12)]
        # Bad records.
        bad = make_activity(rng, 900000 + d * 10, day); bad["distance_m"] = -50.0
        no_athlete = make_activity(rng, 900001 + d * 10, day); no_athlete["athlete_id"] = None
        future = make_activity(rng, 900002 + d * 10, day); future["started_at"] = "2099-01-01T00:00:00"
        rows += [bad, no_athlete, future]
        history += [r for r in rows if r["version"] == 1 and r["distance_m"] > 0
                    and r["athlete_id"] and r["started_at"] < "2099"][:60]
        rng.shuffle(rows)
        path = out / f"batch_{day:%Y%m%d}.ndjson"
        path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
        print(f"wrote {path.name}: {len(rows)} events")


if __name__ == "__main__":
    generate(sys.argv[1] if len(sys.argv) > 1 else "data/landing")
