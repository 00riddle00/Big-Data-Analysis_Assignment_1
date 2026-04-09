"""
config.py - Central configuration for Shadow Fleet Detection Pipeline.

All thresholds and tunable parameters live here so scoring.py and
detect.py never have magic numbers scattered through them.
"""

import multiprocessing
import os

# ---------------------------------------------------------------------------
# Data input
# ---------------------------------------------------------------------------
# Comma-separated list of CSV paths to process (two-day dataset).
# Override via CLI --files argument.
DATA_FILES = [
    "data_arch/aisdk-2025-08-13.csv",
    "data_arch/aisdk-2025-08-14.csv",
]

# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------
# Number of MMSI-based shards written to disk.
# Default: one shard per logical CPU (leaves 1 core free for OS).
NUM_WORKERS = max(1, multiprocessing.cpu_count() - 1)

# Rows buffered in memory before flushing to a shard file.
# Tune via --chunk-size CLI argument for the benchmark task.
CHUNK_SIZE = 50_000

# Directory where shard CSV files are written.
PARTITION_DIR = "partitioned"

# ---------------------------------------------------------------------------
# Data filtering
# ---------------------------------------------------------------------------
# Only these AIS mobile types are relevant for shadow fleet detection.
VALID_MOBILE_TYPES = {"Class A"}

# MMSIs that are known defaults / unconfigured transponders.
INVALID_MMSI_EXACT = {
    "000000000",
    "111111111",
    "123456789",
    "999999999",
    "998334391",  # common AtoN default
}

# MMSIs shorter than 9 digits are invalid.
MMSI_MIN_LENGTH = 9

# Ships reporting SOG == 0.0 for ALL their pings carry no useful movement
# information for anomaly detection.  Filtered during partition phase.
# Note: we keep ships that are *sometimes* stationary (they may go dark).
FILTER_ALWAYS_STATIONARY = True

# Latitude / longitude validity ranges.
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

# Coordinates (91, 0) are the AIS "not available" sentinel values.
INVALID_LAT = 91.0
INVALID_LON = 0.0

# ---------------------------------------------------------------------------
# Anomaly A  –  "Going Dark"
# ---------------------------------------------------------------------------
# Minimum gap duration (hours) to flag as a potential blackout.
ANOMALY_A_MIN_GAP_HOURS = 4.0

# Minimum implied speed (knots) during the gap to conclude the ship kept
# moving (i.e. was NOT simply at anchor).
ANOMALY_A_MIN_IMPLIED_SPEED_KNOTS = 1.0

# ---------------------------------------------------------------------------
# Anomaly B  –  Loitering / Ship-to-Ship Transfer
# ---------------------------------------------------------------------------
# Two ships must be within this distance (metres) of each other.
ANOMALY_B_PROXIMITY_METRES = 500.0

# Both ships must report SOG below this threshold (knots).
ANOMALY_B_MAX_SOG_KNOTS = 1.0

# The proximity condition must persist for at least this long (hours).
ANOMALY_B_MIN_DURATION_HOURS = 2.0

# Bounding-box pre-filter half-width in decimal degrees (~0.01° ≈ 1 km).
ANOMALY_B_BBOX_DEG = 0.005   # ~550 m at mid-latitudes

# ---------------------------------------------------------------------------
# Anomaly C  –  Draft Changes at Sea
# ---------------------------------------------------------------------------
# Minimum AIS blackout (hours) that must surround the draft change.
ANOMALY_C_MIN_BLACKOUT_HOURS = 2.0

# Minimum fractional change in draught to flag (5 %).
ANOMALY_C_MIN_DRAUGHT_CHANGE_FRACTION = 0.05

# ---------------------------------------------------------------------------
# Anomaly D  –  Identity Cloning / "Teleportation"
# ---------------------------------------------------------------------------
# Speed (knots) above which travel between two consecutive pings is
# physically impossible for any commercial vessel.
ANOMALY_D_IMPOSSIBLE_SPEED_KNOTS = 60.0

# Minimum window (minutes) over which the zigzag pattern must persist
# before we declare a cloning event (not just one bad ping).
ANOMALY_D_MIN_WINDOW_MINUTES = 10.0

# ---------------------------------------------------------------------------
# DFSI formula weights  (lecturer-defined, do not change)
# ---------------------------------------------------------------------------
# DFSI = (max_gap_hours / DFSI_GAP_DIVISOR)
#       + (largest_jump_nm  / DFSI_JUMP_DIVISOR)
#       + (C * DFSI_DRAFT_WEIGHT)
DFSI_GAP_DIVISOR    = 2.0
DFSI_JUMP_DIVISOR   = 10.0
DFSI_DRAFT_WEIGHT   = 15.0

# Number of top vessels to report.
TOP_N = 5

# ---------------------------------------------------------------------------
# Output directories
# ---------------------------------------------------------------------------
ANALYSIS_DIR  = "analysis"
LOITERING_DIR = "loitering"

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
# Worker counts to test during the chunk/speedup benchmark.
BENCHMARK_WORKER_COUNTS = [1, 2, 4, 8, NUM_WORKERS]
# Chunk sizes to test.
BENCHMARK_CHUNK_SIZES   = [10_000, 50_000, 100_000]
