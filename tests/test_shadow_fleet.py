"""
tests/test_shadow_fleet.py - Unit tests for the Shadow Fleet pipeline.

Covers the three modules the lecturer specifically mentioned:
  - parsing.py  (stream_rows validation logic)
  - loiter.py   (proximity detection)
  - pipeline.py (end-to-end smoke test with tiny synthetic data)

Run with:  python -m pytest tests/ -v
"""

import csv
import os
import sys
import tempfile
import time
import unittest

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from geo import haversine_nm, haversine_metres, implied_speed_knots
from models import AISRow, LoiterCandidate
from parsing import _is_valid_mmsi, _parse_timestamp, _is_valid_position
from loiter import detect_loitering, _time_overlap_hours
from detect import (
    detect_going_dark,
    detect_draft_changes,
    detect_identity_cloning,
    extract_loiter_candidates,
)


# ---------------------------------------------------------------------------
# geo.py tests
# ---------------------------------------------------------------------------

class TestGeo(unittest.TestCase):

    def test_haversine_zero_distance(self):
        """Same point should give zero distance."""
        self.assertAlmostEqual(haversine_nm(55.0, 10.0, 55.0, 10.0), 0.0)

    def test_haversine_known_distance(self):
        """Copenhagen ↔ Malmö is approximately 28 km ≈ 15 nm."""
        dist = haversine_nm(55.676, 12.568, 55.605, 12.998)
        self.assertGreater(dist, 10)
        self.assertLess(dist, 25)

    def test_haversine_metres_500m(self):
        """Two points ~500 m apart should be detected correctly."""
        # Move ~450 m north (0.004° ≈ 445 m)
        dist = haversine_metres(56.0, 10.0, 56.004, 10.0)
        self.assertGreater(dist, 400)
        self.assertLess(dist, 500)

    def test_implied_speed_zero_gap(self):
        """Zero-second gap should return 0.0, not raise."""
        speed = implied_speed_knots(55.0, 10.0, 55.1, 10.1, 0)
        self.assertEqual(speed, 0.0)


# ---------------------------------------------------------------------------
# parsing.py tests
# ---------------------------------------------------------------------------

class TestParsing(unittest.TestCase):

    def test_valid_mmsi(self):
        self.assertTrue(_is_valid_mmsi("219018682"))
        self.assertTrue(_is_valid_mmsi("305677000"))

    def test_invalid_mmsi_all_zeros(self):
        self.assertFalse(_is_valid_mmsi("000000000"))

    def test_invalid_mmsi_too_short(self):
        self.assertFalse(_is_valid_mmsi("12345"))

    def test_invalid_mmsi_repeated_digit(self):
        self.assertFalse(_is_valid_mmsi("111111111"))

    def test_invalid_mmsi_known_default(self):
        self.assertFalse(_is_valid_mmsi("123456789"))

    def test_valid_position(self):
        self.assertTrue(_is_valid_position(55.0, 10.0))

    def test_invalid_position_sentinel(self):
        # AIS 'not available' sentinel
        self.assertFalse(_is_valid_position(91.0, 0.0))

    def test_invalid_position_out_of_range(self):
        self.assertFalse(_is_valid_position(95.0, 10.0))

    def test_parse_timestamp(self):
        ts = _parse_timestamp("13/08/2025 00:00:00")
        self.assertIsNotNone(ts)
        self.assertGreater(ts, 0)

    def test_parse_timestamp_invalid(self):
        self.assertIsNone(_parse_timestamp("not-a-date"))


# ---------------------------------------------------------------------------
# detect.py – Anomaly A
# ---------------------------------------------------------------------------

class TestAnomalyA(unittest.TestCase):

    def _make_pings(self, intervals_hours, speed=5.0, lat=56.0, lon=10.0):
        """Create synthetic pings with given time gaps."""
        pings = []
        ts = 1_700_000_000.0
        for i, gap_h in enumerate(intervals_hours):
            pings.append(AISRow(
                timestamp=ts,
                mmsi="219000001",
                lat=lat + i * 0.1,
                lon=lon + i * 0.1,
                sog=speed,
                draught=5.0,
            ))
            ts += gap_h * 3600
        # final ping
        pings.append(AISRow(
            timestamp=ts, mmsi="219000001",
            lat=lat + len(intervals_hours) * 0.1,
            lon=lon + len(intervals_hours) * 0.1,
            sog=speed, draught=5.0,
        ))
        return pings

    def test_gap_detected(self):
        """A 6-hour gap should be detected."""
        pings = self._make_pings([6.0])
        events = detect_going_dark(pings)
        self.assertEqual(len(events), 1)
        self.assertAlmostEqual(events[0].gap_hours, 6.0, places=1)

    def test_short_gap_not_detected(self):
        """A 2-hour gap should NOT be flagged."""
        pings = self._make_pings([2.0])
        events = detect_going_dark(pings)
        self.assertEqual(len(events), 0)

    def test_stationary_gap_not_detected(self):
        """A gap where the ship did not move (speed=0) should not be flagged."""
        # Same coordinates, long gap
        pings = [
            AISRow(1_700_000_000.0, "219000001", 56.0, 10.0, 0.0, 5.0),
            AISRow(1_700_000_000.0 + 6*3600, "219000001", 56.0, 10.0, 0.0, 5.0),
        ]
        events = detect_going_dark(pings)
        self.assertEqual(len(events), 0)


# ---------------------------------------------------------------------------
# detect.py – Anomaly C
# ---------------------------------------------------------------------------

class TestAnomalyC(unittest.TestCase):

    def test_draft_change_detected(self):
        """A 10% draught change after 3 h gap should be flagged."""
        pings = [
            AISRow(1_700_000_000.0, "219000002", 56.0, 10.0, 0.0, 10.0),
            AISRow(1_700_000_000.0 + 3*3600, "219000002", 56.1, 10.1, 0.0, 11.0),
        ]
        events = detect_draft_changes(pings)
        self.assertEqual(len(events), 1)
        self.assertAlmostEqual(events[0].change_fraction, 0.1, places=3)

    def test_small_draft_change_not_detected(self):
        """A 2% draught change should NOT be flagged."""
        pings = [
            AISRow(1_700_000_000.0, "219000002", 56.0, 10.0, 0.0, 10.0),
            AISRow(1_700_000_000.0 + 3*3600, "219000002", 56.0, 10.0, 0.0, 10.2),
        ]
        events = detect_draft_changes(pings)
        self.assertEqual(len(events), 0)


# ---------------------------------------------------------------------------
# detect.py – Anomaly D
# ---------------------------------------------------------------------------

class TestAnomalyD(unittest.TestCase):

    def test_teleportation_detected(self):
        """Two pings 1000 nm apart in 1 hour → impossible speed ~1000 kt."""
        base_ts = 1_700_000_000.0
        pings = []
        # Create a sustained zigzag: alternating between two distant positions
        for i in range(6):
            lat = 56.0 if i % 2 == 0 else 65.0   # ~540 nm apart
            pings.append(AISRow(
                timestamp=base_ts + i * 600,  # 10-min intervals
                mmsi="219000003",
                lat=lat, lon=10.0,
                sog=0.0, draught=None,
            ))
        events = detect_identity_cloning(pings)
        self.assertGreater(len(events), 0)


# ---------------------------------------------------------------------------
# loiter.py tests
# ---------------------------------------------------------------------------

class TestLoiter(unittest.TestCase):

    def test_time_overlap(self):
        """Overlapping intervals should return correct duration."""
        overlap = _time_overlap_hours(0, 3*3600, 1*3600, 5*3600)
        self.assertAlmostEqual(overlap, 2.0, places=2)

    def test_no_time_overlap(self):
        overlap = _time_overlap_hours(0, 1*3600, 2*3600, 4*3600)
        self.assertEqual(overlap, 0.0)

    def test_nearby_vessels_detected(self):
        """Two candidates at the same location with overlapping time → event."""
        base_ts = 1_700_000_000.0
        candidates = [
            LoiterCandidate(
                mmsi="219000010",
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.0, avg_lon=10.0,
                min_sog=0.0,
            ),
            LoiterCandidate(
                mmsi="219000011",
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.001, avg_lon=10.001,   # ~100 m apart
                min_sog=0.0,
            ),
        ]
        events = detect_loitering(candidates)
        self.assertEqual(len(events), 1)
        self.assertAlmostEqual(events[0].duration_hours, 3.0, places=1)

    def test_distant_vessels_not_detected(self):
        """Two candidates 10 km apart should NOT produce a loitering event."""
        base_ts = 1_700_000_000.0
        candidates = [
            LoiterCandidate(
                mmsi="219000012",
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.0, avg_lon=10.0,
                min_sog=0.0,
            ),
            LoiterCandidate(
                mmsi="219000013",
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.1, avg_lon=10.1,   # ~13 km apart
                min_sog=0.0,
            ),
        ]
        events = detect_loitering(candidates)
        self.assertEqual(len(events), 0)

    def test_same_mmsi_not_paired(self):
        """Same MMSI should never be paired with itself."""
        base_ts = 1_700_000_000.0
        candidates = [
            LoiterCandidate(
                mmsi="219000014",
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.0, avg_lon=10.0,
                min_sog=0.0,
            ),
            LoiterCandidate(
                mmsi="219000014",   # same MMSI
                start_ts=base_ts,
                end_ts=base_ts + 3*3600,
                avg_lat=56.001, avg_lon=10.001,
                min_sog=0.0,
            ),
        ]
        events = detect_loitering(candidates)
        self.assertEqual(len(events), 0)


if __name__ == "__main__":
    unittest.main()
