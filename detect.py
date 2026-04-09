"""
detect.py - Per-MMSI anomaly detection (Anomalies A, C, D).

This module is designed to be called by multiprocessing workers.
Each worker receives one shard file path, reads it, groups rows by MMSI,
and runs all three per-vessel anomaly detectors.

Anomaly B (loitering) requires cross-vessel comparison and is handled
separately in loiter.py via the LoiterCandidate output of this module.
"""

import csv
import logging
import os
from collections import defaultdict
from itertools import groupby
from typing import List, Dict, Tuple

from config import (
    ANOMALY_A_MIN_GAP_HOURS,
    ANOMALY_A_MIN_IMPLIED_SPEED_KNOTS,
    ANOMALY_B_MAX_SOG_KNOTS,
    ANOMALY_B_MIN_DURATION_HOURS,
    ANOMALY_C_MIN_BLACKOUT_HOURS,
    ANOMALY_C_MIN_DRAUGHT_CHANGE_FRACTION,
    ANOMALY_D_IMPOSSIBLE_SPEED_KNOTS,
    ANOMALY_D_MIN_WINDOW_MINUTES,
    ANALYSIS_DIR,
)
from geo import haversine_nm, implied_speed_knots
from models import (
    AISRow,
    GapEvent,
    DraftChangeEvent,
    CloningEvent,
    LoiterCandidate,
    VesselDetectionResult,
)
from partition import read_shard

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: group shard rows by MMSI
# ---------------------------------------------------------------------------

def _group_by_mmsi(rows: List[AISRow]) -> Dict[str, List[AISRow]]:
    """Return a dict mapping MMSI → sorted list of AISRow."""
    vessels: Dict[str, List[AISRow]] = defaultdict(list)
    for row in rows:
        vessels[row.mmsi].append(row)
    # Each vessel's pings are already sorted because read_shard() sorts the
    # whole shard chronologically, but we sort per-vessel too just to be safe.
    for mmsi in vessels:
        vessels[mmsi].sort()
    return vessels


# ---------------------------------------------------------------------------
# Anomaly A – "Going Dark"
# ---------------------------------------------------------------------------

def detect_going_dark(pings: List[AISRow]) -> List[GapEvent]:
    """
    Find AIS blackouts > 4 hours where the ship's implied speed
    indicates it kept moving (speed > ANOMALY_A_MIN_IMPLIED_SPEED_KNOTS).
    """
    events: List[GapEvent] = []

    for i in range(1, len(pings)):
        prev = pings[i - 1]
        curr = pings[i]

        gap_seconds = curr.timestamp - prev.timestamp
        gap_hours   = gap_seconds / 3600.0

        if gap_hours < ANOMALY_A_MIN_GAP_HOURS:
            continue

        # Calculate implied speed over the gap
        dist_nm = haversine_nm(prev.lat, prev.lon, curr.lat, curr.lon)
        speed   = implied_speed_knots(
            prev.lat, prev.lon, curr.lat, curr.lon, gap_seconds
        )

        if speed < ANOMALY_A_MIN_IMPLIED_SPEED_KNOTS:
            # Ship likely at anchor or GPS noise – not flagged
            continue

        events.append(GapEvent(
            mmsi=prev.mmsi,
            gap_start_ts=prev.timestamp,
            gap_end_ts=curr.timestamp,
            gap_hours=gap_hours,
            implied_speed_knots=speed,
            start_lat=prev.lat,
            start_lon=prev.lon,
            end_lat=curr.lat,
            end_lon=curr.lon,
            distance_nm=dist_nm,
        ))

    return events


# ---------------------------------------------------------------------------
# Anomaly C – Draft Changes at Sea
# ---------------------------------------------------------------------------

def detect_draft_changes(pings: List[AISRow]) -> List[DraftChangeEvent]:
    """
    Find cases where the vessel's draught changed by > 5% across an AIS
    blackout of > 2 hours, implying illegal cargo transfer during the gap.
    """
    events: List[DraftChangeEvent] = []

    for i in range(1, len(pings)):
        prev = pings[i - 1]
        curr = pings[i]

        # Both pings must have draught information
        if prev.draught is None or curr.draught is None:
            continue
        if prev.draught <= 0:
            continue

        gap_hours = (curr.timestamp - prev.timestamp) / 3600.0

        if gap_hours < ANOMALY_C_MIN_BLACKOUT_HOURS:
            continue

        change_fraction = abs(curr.draught - prev.draught) / prev.draught

        if change_fraction < ANOMALY_C_MIN_DRAUGHT_CHANGE_FRACTION:
            continue

        events.append(DraftChangeEvent(
            mmsi=prev.mmsi,
            blackout_start_ts=prev.timestamp,
            blackout_end_ts=curr.timestamp,
            blackout_hours=gap_hours,
            draught_before=prev.draught,
            draught_after=curr.draught,
            change_fraction=change_fraction,
        ))

    return events


# ---------------------------------------------------------------------------
# Anomaly D – Identity Cloning / Teleportation
# ---------------------------------------------------------------------------

def detect_identity_cloning(pings: List[AISRow]) -> List[CloningEvent]:
    """
    Detect sustained dual-trajectory (zigzag) patterns for the same MMSI.

    Method (as described by the lecturer):
    1. Find pairs of consecutive pings where the implied speed > 60 knots.
    2. Check that the zigzag persists for at least ANOMALY_D_MIN_WINDOW_MINUTES.
       A single bad ping is noise; a sustained pattern is cloning.
    """
    events: List[CloningEvent] = []

    # Sliding window: collect all impossible-speed jumps, then check
    # whether any window of ANOMALY_D_MIN_WINDOW_MINUTES contains multiple
    # impossible jumps (indicating two ships trading the same MMSI).

    impossible_jumps: List[Tuple[int, float]] = []  # (index, implied_speed)

    for i in range(1, len(pings)):
        prev = pings[i - 1]
        curr = pings[i]
        gap_s = curr.timestamp - prev.timestamp

        if gap_s <= 0:
            continue

        speed = implied_speed_knots(
            prev.lat, prev.lon, curr.lat, curr.lon, gap_s
        )
        if speed > ANOMALY_D_IMPOSSIBLE_SPEED_KNOTS:
            impossible_jumps.append((i, speed))

    if not impossible_jumps:
        return events

    # Group impossible jumps into sustained windows
    min_window_s = ANOMALY_D_MIN_WINDOW_MINUTES * 60.0

    visited = set()
    for idx, (jump_idx, speed) in enumerate(impossible_jumps):
        if jump_idx in visited:
            continue

        # Find all impossible jumps within the window starting at this ping
        window_start_ts = pings[jump_idx - 1].timestamp
        window_end_ts   = window_start_ts + min_window_s

        window_jumps = [
            (ji, sp) for ji, sp in impossible_jumps
            if pings[ji - 1].timestamp >= window_start_ts
            and pings[ji - 1].timestamp <= window_end_ts
        ]

        # Need at least 2 impossible jumps in the window to confirm cloning
        if len(window_jumps) < 2:
            continue

        # Pick the jump with the highest implied speed as the representative event
        best_ji, best_speed = max(window_jumps, key=lambda x: x[1])
        ping_a = pings[best_ji - 1]
        ping_b = pings[best_ji]

        dist_nm = haversine_nm(ping_a.lat, ping_a.lon, ping_b.lat, ping_b.lon)
        window_minutes = (pings[window_jumps[-1][0]].timestamp
                          - pings[window_jumps[0][0] - 1].timestamp) / 60.0

        events.append(CloningEvent(
            mmsi=ping_a.mmsi,
            ping_a_ts=ping_a.timestamp,
            ping_b_ts=ping_b.timestamp,
            lat_a=ping_a.lat,
            lon_a=ping_a.lon,
            lat_b=ping_b.lat,
            lon_b=ping_b.lon,
            implied_speed_knots=best_speed,
            distance_nm=dist_nm,
            window_minutes=window_minutes,
        ))

        for ji, _ in window_jumps:
            visited.add(ji)

    return events


# ---------------------------------------------------------------------------
# Loiter candidates (feeds Anomaly B in loiter.py)
# ---------------------------------------------------------------------------

def extract_loiter_candidates(pings: List[AISRow]) -> List[LoiterCandidate]:
    """
    Identify contiguous slow-moving segments (SOG < threshold for > 2 h).
    These are candidates for ship-to-ship transfer; loiter.py will check
    for proximity to other vessels.
    """
    candidates: List[LoiterCandidate] = []

    if not pings:
        return candidates

    from config import ANOMALY_B_MAX_SOG_KNOTS, ANOMALY_B_MIN_DURATION_HOURS

    seg_start = None
    seg_pings: List[AISRow] = []

    for ping in pings:
        if ping.sog <= ANOMALY_B_MAX_SOG_KNOTS:
            if seg_start is None:
                seg_start = ping
            seg_pings.append(ping)
        else:
            # End of slow segment
            if seg_pings:
                duration_h = (seg_pings[-1].timestamp
                              - seg_pings[0].timestamp) / 3600.0
                if duration_h >= ANOMALY_B_MIN_DURATION_HOURS:
                    avg_lat = sum(p.lat for p in seg_pings) / len(seg_pings)
                    avg_lon = sum(p.lon for p in seg_pings) / len(seg_pings)
                    candidates.append(LoiterCandidate(
                        mmsi=seg_pings[0].mmsi,
                        start_ts=seg_pings[0].timestamp,
                        end_ts=seg_pings[-1].timestamp,
                        avg_lat=avg_lat,
                        avg_lon=avg_lon,
                        min_sog=min(p.sog for p in seg_pings),
                    ))
            seg_pings = []
            seg_start = None

    # Handle segment that reaches the end of the data
    if seg_pings:
        duration_h = (seg_pings[-1].timestamp
                      - seg_pings[0].timestamp) / 3600.0
        if duration_h >= ANOMALY_B_MIN_DURATION_HOURS:
            avg_lat = sum(p.lat for p in seg_pings) / len(seg_pings)
            avg_lon = sum(p.lon for p in seg_pings) / len(seg_pings)
            candidates.append(LoiterCandidate(
                mmsi=seg_pings[0].mmsi,
                start_ts=seg_pings[0].timestamp,
                end_ts=seg_pings[-1].timestamp,
                avg_lat=avg_lat,
                avg_lon=avg_lon,
                min_sog=min(p.sog for p in seg_pings),
            ))

    return candidates


# ---------------------------------------------------------------------------
# Per-shard worker function (called by multiprocessing.Pool)
# ---------------------------------------------------------------------------

def process_shard(shard_path: str) -> VesselDetectionResult:
    """
    Top-level worker function.  Reads one shard, runs all per-vessel
    detectors, and returns an aggregated VesselDetectionResult.

    This function is picklable (no lambdas, no closures) so it works
    correctly with multiprocessing.Pool.imap_unordered.
    """
    rows = read_shard(shard_path)
    vessels = _group_by_mmsi(rows)

    # Aggregate results across all MMSIs in this shard
    combined = VesselDetectionResult(mmsi="__shard__")

    for mmsi, pings in vessels.items():
        if len(pings) < 2:
            continue  # Need at least 2 pings for any gap analysis

        gap_events     = detect_going_dark(pings)
        draft_events   = detect_draft_changes(pings)
        cloning_events = detect_identity_cloning(pings)
        loiter_cands   = extract_loiter_candidates(pings)

        combined.gap_events.extend(gap_events)
        combined.draft_events.extend(draft_events)
        combined.cloning_events.extend(cloning_events)
        combined.loiter_candidates.extend(loiter_cands)

    logger.debug(
        "Shard %s: %d gap / %d draft / %d cloning / %d loiter candidates",
        os.path.basename(shard_path),
        len(combined.gap_events),
        len(combined.draft_events),
        len(combined.cloning_events),
        len(combined.loiter_candidates),
    )
    return combined


# ---------------------------------------------------------------------------
# CSV writers for analysis output
# ---------------------------------------------------------------------------

def write_events_csv(results: List[VesselDetectionResult]) -> None:
    """Write anomaly event CSVs to the ANALYSIS_DIR."""
    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    gap_rows, draft_rows, cloning_rows, loiter_rows = [], [], [], []

    for res in results:
        for e in res.gap_events:
            gap_rows.append([
                e.mmsi, e.gap_start_ts, e.gap_end_ts, round(e.gap_hours, 3),
                round(e.implied_speed_knots, 2),
                e.start_lat, e.start_lon, e.end_lat, e.end_lon,
                round(e.distance_nm, 3),
            ])
        for e in res.draft_events:
            draft_rows.append([
                e.mmsi, e.blackout_start_ts, e.blackout_end_ts,
                round(e.blackout_hours, 3),
                round(e.draught_before, 2), round(e.draught_after, 2),
                round(e.change_fraction * 100, 2),
            ])
        for e in res.cloning_events:
            cloning_rows.append([
                e.mmsi, e.ping_a_ts, e.ping_b_ts,
                e.lat_a, e.lon_a, e.lat_b, e.lon_b,
                round(e.implied_speed_knots, 1),
                round(e.distance_nm, 3),
                round(e.window_minutes, 1),
            ])
        for c in res.loiter_candidates:
            loiter_rows.append([
                c.mmsi, c.start_ts, c.end_ts,
                round(c.avg_lat, 6), round(c.avg_lon, 6), c.min_sog,
            ])

    _write_csv(
        os.path.join(ANALYSIS_DIR, "gap_events.csv"),
        ["mmsi", "gap_start_ts", "gap_end_ts", "gap_hours",
         "implied_speed_knots", "start_lat", "start_lon",
         "end_lat", "end_lon", "distance_nm"],
        gap_rows,
    )
    _write_csv(
        os.path.join(ANALYSIS_DIR, "draft_events.csv"),
        ["mmsi", "blackout_start_ts", "blackout_end_ts", "blackout_hours",
         "draught_before_m", "draught_after_m", "change_pct"],
        draft_rows,
    )
    _write_csv(
        os.path.join(ANALYSIS_DIR, "cloning_events.csv"),
        ["mmsi", "ping_a_ts", "ping_b_ts",
         "lat_a", "lon_a", "lat_b", "lon_b",
         "implied_speed_knots", "distance_nm", "window_minutes"],
        cloning_rows,
    )
    _write_csv(
        os.path.join(ANALYSIS_DIR, "loiter_candidates.csv"),
        ["mmsi", "start_ts", "end_ts", "avg_lat", "avg_lon", "min_sog"],
        loiter_rows,
    )

    logger.info(
        "Wrote analysis CSVs: %d gap, %d draft, %d cloning, %d loiter candidates",
        len(gap_rows), len(draft_rows), len(cloning_rows), len(loiter_rows),
    )


def _write_csv(path: str, header: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
