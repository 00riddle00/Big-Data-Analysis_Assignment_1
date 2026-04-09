"""
loiter.py - Anomaly B: Loitering / Ship-to-Ship Transfer Detection.

Why this is a separate module
------------------------------
Anomaly B requires comparing two *different* vessels against each other,
so it cannot be parallelised by MMSI the way Anomalies A/C/D can.
The solution (confirmed by the lecturer) is a two-step approach:

  Step 1 – detect.py emits LoiterCandidates: vessels that were slow
           (SOG < 1 kt) for at least 2 hours.  There are typically
           far fewer of these than total vessels.

  Step 2 – loiter.py loads all candidates and uses a bounding-box
           pre-filter to find candidate pairs quickly, then applies the
           exact Haversine formula only to those nearby pairs.

This runs sequentially because the candidate set is small.  With ~26M
Class-A rows, empirical experience suggests at most a few thousand
loiter candidates per two-day dataset.
"""

import csv
import logging
import os
from typing import List, Tuple

from config import (
    ANOMALY_B_PROXIMITY_METRES,
    ANOMALY_B_MIN_DURATION_HOURS,
    ANOMALY_B_BBOX_DEG,
    LOITERING_DIR,
    ANALYSIS_DIR,
)
from geo import haversine_metres, within_bbox, bounding_box
from models import LoiterCandidate, LoiterEvent

logger = logging.getLogger(__name__)


def _load_candidates(path: str) -> List[LoiterCandidate]:
    """Read loiter_candidates.csv produced by detect.py."""
    candidates: List[LoiterCandidate] = []
    if not os.path.exists(path):
        logger.warning("Loiter candidates file not found: %s", path)
        return candidates

    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            try:
                candidates.append(LoiterCandidate(
                    mmsi=rec["mmsi"],
                    start_ts=float(rec["start_ts"]),
                    end_ts=float(rec["end_ts"]),
                    avg_lat=float(rec["avg_lat"]),
                    avg_lon=float(rec["avg_lon"]),
                    min_sog=float(rec["min_sog"]),
                ))
            except (ValueError, KeyError):
                continue
    logger.info("Loaded %d loiter candidates", len(candidates))
    return candidates


def _time_overlap_hours(
    start_a: float, end_a: float,
    start_b: float, end_b: float,
) -> float:
    """Return the overlapping duration (hours) between two time intervals."""
    overlap_start = max(start_a, start_b)
    overlap_end   = min(end_a,   end_b)
    if overlap_end <= overlap_start:
        return 0.0
    return (overlap_end - overlap_start) / 3600.0


def detect_loitering(
    candidates: List[LoiterCandidate],
) -> List[LoiterEvent]:
    """
    Find pairs of LoiterCandidates that were:
      - within ANOMALY_B_PROXIMITY_METRES of each other (Haversine)
      - simultaneously slow for > ANOMALY_B_MIN_DURATION_HOURS

    Algorithm
    ---------
    For each candidate A, build a bounding box of ±ANOMALY_B_BBOX_DEG
    around its average position.  Quickly reject all candidates B whose
    average position falls outside this box.  For the survivors, compute
    the exact Haversine distance and check the time overlap.
    """
    events: List[LoiterEvent] = []
    n = len(candidates)

    if n == 0:
        return events

    logger.info("Checking %d loiter candidates for proximity pairs...", n)

    for i in range(n):
        a = candidates[i]
        lat_min, lat_max, lon_min, lon_max = bounding_box(
            a.avg_lat, a.avg_lon, ANOMALY_B_BBOX_DEG
        )

        for j in range(i + 1, n):
            b = candidates[j]

            # Skip same MMSI
            if a.mmsi == b.mmsi:
                continue

            # --- Fast bounding-box pre-filter ---
            if not within_bbox(b.avg_lat, b.avg_lon,
                               lat_min, lat_max, lon_min, lon_max):
                continue

            # --- Exact Haversine distance ---
            dist_m = haversine_metres(
                a.avg_lat, a.avg_lon,
                b.avg_lat, b.avg_lon,
            )
            if dist_m > ANOMALY_B_PROXIMITY_METRES:
                continue

            # --- Time overlap ---
            overlap_h = _time_overlap_hours(
                a.start_ts, a.end_ts,
                b.start_ts, b.end_ts,
            )
            if overlap_h < ANOMALY_B_MIN_DURATION_HOURS:
                continue

            # --- Confirmed loitering event ---
            midpoint_lat = (a.avg_lat + b.avg_lat) / 2
            midpoint_lon = (a.avg_lon + b.avg_lon) / 2

            events.append(LoiterEvent(
                mmsi_a=a.mmsi,
                mmsi_b=b.mmsi,
                start_ts=max(a.start_ts, b.start_ts),
                end_ts=min(a.end_ts, b.end_ts),
                duration_hours=overlap_h,
                distance_metres=round(dist_m, 1),
                lat=round(midpoint_lat, 6),
                lon=round(midpoint_lon, 6),
            ))

    logger.info("Found %d loitering event pairs", len(events))
    return events


def write_loitering_output(events: List[LoiterEvent]) -> None:
    """Write loitering_events.csv and loitering_aggregates.csv."""
    os.makedirs(LOITERING_DIR, exist_ok=True)

    # Per-event detail
    events_path = os.path.join(LOITERING_DIR, "loitering_events.csv")
    with open(events_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "mmsi_a", "mmsi_b", "start_ts", "end_ts",
            "duration_hours", "distance_metres", "lat", "lon",
        ])
        for e in events:
            writer.writerow([
                e.mmsi_a, e.mmsi_b,
                e.start_ts, e.end_ts,
                round(e.duration_hours, 3),
                e.distance_metres,
                e.lat, e.lon,
            ])

    # Per-vessel aggregate (used by scoring.py to flag Anomaly B vessels)
    agg_path = os.path.join(LOITERING_DIR, "loitering_aggregates.csv")
    # Collect all MMSIs involved in a loitering event
    loiter_mmsis: dict = {}
    for e in events:
        for mmsi in (e.mmsi_a, e.mmsi_b):
            if mmsi not in loiter_mmsis:
                loiter_mmsis[mmsi] = {"count": 0, "lat": e.lat, "lon": e.lon}
            loiter_mmsis[mmsi]["count"] += 1

    with open(agg_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["mmsi", "loiter_event_count", "sample_lat", "sample_lon"])
        for mmsi, data in loiter_mmsis.items():
            writer.writerow([mmsi, data["count"], data["lat"], data["lon"]])

    logger.info(
        "Loitering output written: %d events, %d unique vessels",
        len(events), len(loiter_mmsis),
    )


def run_loiter_detection() -> None:
    """Entry point called by pipeline.py after detect phase completes."""
    candidates_path = os.path.join(ANALYSIS_DIR, "loiter_candidates.csv")
    candidates = _load_candidates(candidates_path)

    if not candidates:
        logger.info("No loiter candidates found – skipping Anomaly B.")
        os.makedirs(LOITERING_DIR, exist_ok=True)
        # Write empty output files so scoring.py doesn't break
        write_loitering_output([])
        return

    events = detect_loitering(candidates)
    write_loitering_output(events)
