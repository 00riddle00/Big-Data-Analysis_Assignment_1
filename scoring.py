"""
scoring.py - DFSI scoring and final vessel ranking.

DFSI formula (lecturer-defined, confirmed in Q&A session):
  DFSI = (max_gap_hours / 2)
       + (largest_impossible_jump_nm / 10)
       + (C * 15)

Where:
  max_gap_hours            = single longest AIS blackout (Anomaly A)
  largest_impossible_jump  = single largest distance jump (Anomaly D)
  C                        = number of illicit draft changes (Anomaly C)

Note: Anomaly B (loitering) flags the vessel but does not directly
contribute to the DFSI formula as written.  Vessels involved in
loitering events are still included in the ranking if they triggered
other anomalies; loitering is captured in anomaly_flags.
"""

import csv
import logging
import os
from collections import defaultdict
from typing import List, Dict

from config import (
    DFSI_GAP_DIVISOR,
    DFSI_JUMP_DIVISOR,
    DFSI_DRAFT_WEIGHT,
    TOP_N,
    ANALYSIS_DIR,
    LOITERING_DIR,
)
from models import (
    GapEvent,
    DraftChangeEvent,
    CloningEvent,
    VesselDetectionResult,
    ScoredVessel,
)

logger = logging.getLogger(__name__)


def _load_loiter_mmsis() -> Dict[str, int]:
    """Return a dict {mmsi: loiter_event_count} from loitering_aggregates.csv."""
    path = os.path.join(LOITERING_DIR, "loitering_aggregates.csv")
    result: Dict[str, int] = {}
    if not os.path.exists(path):
        return result
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            try:
                result[rec["mmsi"]] = int(rec["loiter_event_count"])
            except (ValueError, KeyError):
                continue
    return result


def score_vessels(
    detection_results: List[VesselDetectionResult],
) -> List[ScoredVessel]:
    """
    Aggregate per-shard detection results into per-vessel DFSI scores.

    Returns a list of ScoredVessel objects sorted descending by DFSI.
    """
    # Collect all events per MMSI across shards
    gap_by_mmsi:     Dict[str, List[GapEvent]]          = defaultdict(list)
    draft_by_mmsi:   Dict[str, List[DraftChangeEvent]]  = defaultdict(list)
    cloning_by_mmsi: Dict[str, List[CloningEvent]]      = defaultdict(list)

    for res in detection_results:
        for e in res.gap_events:
            gap_by_mmsi[e.mmsi].append(e)
        for e in res.draft_events:
            draft_by_mmsi[e.mmsi].append(e)
        for e in res.cloning_events:
            cloning_by_mmsi[e.mmsi].append(e)

    # Load Anomaly B flags
    loiter_mmsis = _load_loiter_mmsis()

    # Union of all flagged MMSIs
    all_flagged = (
        set(gap_by_mmsi)
        | set(draft_by_mmsi)
        | set(cloning_by_mmsi)
        | set(loiter_mmsis)
    )

    if not all_flagged:
        logger.warning("No anomalies detected – DFSI output will be empty.")
        return []

    scored: List[ScoredVessel] = []

    for mmsi in all_flagged:
        gaps     = gap_by_mmsi.get(mmsi, [])
        drafts   = draft_by_mmsi.get(mmsi, [])
        clonings = cloning_by_mmsi.get(mmsi, [])
        loiter_c = loiter_mmsis.get(mmsi, 0)

        # --- Max Cap in Hours: single longest blackout (Anomaly A) ---
        max_gap_h = max((e.gap_hours for e in gaps), default=0.0)

        # --- Total Impossible Distance Jump: single largest jump (Anomaly D) ---
        largest_jump_nm = max(
            (e.distance_nm for e in clonings), default=0.0
        )

        # --- C: count of illicit draft changes (Anomaly C) ---
        c = len(drafts)

        # --- DFSI ---
        dfsi = (
            max_gap_h      / DFSI_GAP_DIVISOR
            + largest_jump_nm / DFSI_JUMP_DIVISOR
            + c            * DFSI_DRAFT_WEIGHT
        )

        # Only include vessels with at least some score
        if dfsi == 0.0 and loiter_c == 0:
            continue

        # Best coordinates for the Folium map (prefer gap event location)
        if gaps:
            best = max(gaps, key=lambda e: e.gap_hours)
            map_lat, map_lon = best.start_lat, best.start_lon
        elif clonings:
            best = max(clonings, key=lambda e: e.distance_nm)
            map_lat, map_lon = best.lat_a, best.lon_a
        elif drafts:
            map_lat, map_lon = 0.0, 0.0  # no location for draft-only events
        else:
            map_lat, map_lon = 0.0, 0.0

        # Human-readable flags
        flags = []
        if gaps:     flags.append(f"A(gaps={len(gaps)},max={max_gap_h:.1f}h)")
        if loiter_c: flags.append(f"B(events={loiter_c})")
        if drafts:   flags.append(f"C(changes={c})")
        if clonings: flags.append(f"D(jumps={len(clonings)},max={largest_jump_nm:.0f}nm)")

        scored.append(ScoredVessel(
            mmsi=mmsi,
            dfsi=round(dfsi, 4),
            max_gap_hours=round(max_gap_h, 3),
            largest_jump_nm=round(largest_jump_nm, 3),
            draft_change_count=c,
            map_lat=map_lat,
            map_lon=map_lon,
            anomaly_flags=" | ".join(flags) if flags else "B_only",
        ))

    # Sort descending by DFSI score
    scored.sort(key=lambda v: v.dfsi, reverse=True)
    return scored


def write_scores(scored: List[ScoredVessel]) -> None:
    """Write vessel_scores.csv and top5_vessels.csv to ANALYSIS_DIR."""
    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    header = [
        "mmsi", "dfsi", "max_gap_hours", "largest_jump_nm",
        "draft_change_count", "map_lat", "map_lon", "anomaly_flags",
    ]

    all_path  = os.path.join(ANALYSIS_DIR, "vessel_scores.csv")
    top5_path = os.path.join(ANALYSIS_DIR, "top5_vessels.csv")

    def _row(v: ScoredVessel) -> list:
        return [
            v.mmsi, v.dfsi, v.max_gap_hours, v.largest_jump_nm,
            v.draft_change_count, v.map_lat, v.map_lon, v.anomaly_flags,
        ]

    with open(all_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(_row(v) for v in scored)

    with open(top5_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(_row(v) for v in scored[:TOP_N])

    logger.info(
        "Scoring complete: %d vessels scored, top %d written to %s",
        len(scored), min(TOP_N, len(scored)), top5_path,
    )

    # Print top-5 to console for quick inspection
    print("\n=== TOP {} VESSELS BY DFSI ===".format(min(TOP_N, len(scored))))
    for rank, v in enumerate(scored[:TOP_N], start=1):
        print(
            f"  #{rank}  MMSI={v.mmsi}  DFSI={v.dfsi:.2f}  "
            f"lat={v.map_lat:.4f}  lon={v.map_lon:.4f}  "
            f"flags={v.anomaly_flags}"
        )
