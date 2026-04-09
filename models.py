"""
models.py - Lightweight data classes used throughout the pipeline.

We avoid pandas DataFrames here to keep memory under control and to
remain compatible with multiprocessing (dataclasses pickle cleanly).
"""

from dataclasses import dataclass, field
from typing import Optional, List


# ---------------------------------------------------------------------------
# Raw AIS record (one CSV row after parsing)
# ---------------------------------------------------------------------------

@dataclass
class AISRow:
    """One parsed and validated AIS message."""
    timestamp: float          # Unix timestamp (seconds)
    mmsi: str
    lat: float
    lon: float
    sog: float                # Speed over ground, knots
    draught: Optional[float]  # Metres; None if not reported

    def __lt__(self, other: "AISRow") -> bool:
        """Allow sorting by timestamp."""
        return self.timestamp < other.timestamp


# ---------------------------------------------------------------------------
# Anomaly event records
# ---------------------------------------------------------------------------

@dataclass
class GapEvent:
    """Anomaly A – AIS blackout where the ship kept moving."""
    mmsi: str
    gap_start_ts: float      # Unix timestamp
    gap_end_ts: float
    gap_hours: float
    implied_speed_knots: float
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    distance_nm: float


@dataclass
class DraftChangeEvent:
    """Anomaly C – draught change during an AIS blackout."""
    mmsi: str
    blackout_start_ts: float
    blackout_end_ts: float
    blackout_hours: float
    draught_before: float
    draught_after: float
    change_fraction: float   # |after - before| / before


@dataclass
class CloningEvent:
    """Anomaly D – same MMSI at two physically impossible locations."""
    mmsi: str
    ping_a_ts: float
    ping_b_ts: float
    lat_a: float
    lon_a: float
    lat_b: float
    lon_b: float
    implied_speed_knots: float
    distance_nm: float
    window_minutes: float    # Duration of sustained zigzag pattern


@dataclass
class LoiterCandidate:
    """
    Slow-moving vessel segment emitted by detect.py.
    loiter.py groups these by proximity to find Anomaly B pairs.
    """
    mmsi: str
    start_ts: float
    end_ts: float
    avg_lat: float
    avg_lon: float
    min_sog: float


@dataclass
class LoiterEvent:
    """Anomaly B – two ships in close proximity at low speed."""
    mmsi_a: str
    mmsi_b: str
    start_ts: float
    end_ts: float
    duration_hours: float
    distance_metres: float
    lat: float               # Approximate midpoint
    lon: float


# ---------------------------------------------------------------------------
# Per-vessel detection results
# ---------------------------------------------------------------------------

@dataclass
class VesselDetectionResult:
    """Aggregated anomaly detections for a single MMSI."""
    mmsi: str
    gap_events:          List[GapEvent]          = field(default_factory=list)
    draft_events:        List[DraftChangeEvent]  = field(default_factory=list)
    cloning_events:      List[CloningEvent]      = field(default_factory=list)
    loiter_candidates:   List[LoiterCandidate]   = field(default_factory=list)


# ---------------------------------------------------------------------------
# Final scored vessel
# ---------------------------------------------------------------------------

@dataclass
class ScoredVessel:
    """DFSI score and supporting evidence for one vessel."""
    mmsi: str
    dfsi: float
    max_gap_hours: float
    largest_jump_nm: float
    draft_change_count: int   # C in the formula
    # Coordinates of the highest-DFSI event (for Folium map)
    map_lat: float
    map_lon: float
    # Human-readable summary of which anomalies fired
    anomaly_flags: str

    def __lt__(self, other: "ScoredVessel") -> bool:
        return self.dfsi < other.dfsi
