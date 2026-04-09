"""
partition.py - Low-memory streaming partitioner.

Strategy
--------
1. Read both daily CSV files line-by-line using parsing.stream_rows().
2. Hash each MMSI to one of N shard buckets (N = NUM_WORKERS).
   All rows for a given MMSI always land in the same shard, so
   detect.py workers can process each shard independently.
3. Buffer rows in memory per shard.  When a shard buffer reaches
   CHUNK_SIZE rows, flush it to disk.
4. After all input is consumed, flush any remaining buffered rows.

Memory guarantee
----------------
At most NUM_WORKERS * CHUNK_SIZE rows are held in RAM simultaneously.
With NUM_WORKERS=15, CHUNK_SIZE=50_000 and ~100 bytes per row that is
~75 MB – well inside the 1 GB/core budget.

Dirty-data trap
---------------
Stationary ships (SOG==0 for every single ping) are excluded from the
output shards because they contribute no useful movement information for
anomaly detection (lecturer confirmed this is correct).
"""

import csv
import logging
import os
from collections import defaultdict
from typing import Dict, List

from config import (
    CHUNK_SIZE,
    NUM_WORKERS,
    PARTITION_DIR,
    FILTER_ALWAYS_STATIONARY,
)
from models import AISRow
from parsing import stream_rows

logger = logging.getLogger(__name__)

# Header written to every shard file
SHARD_HEADER = ["timestamp", "mmsi", "lat", "lon", "sog", "draught"]


def _shard_id(mmsi: str, num_shards: int) -> int:
    """Assign an MMSI to a shard bucket using Python's built-in hash."""
    return hash(mmsi) % num_shards


def _shard_path(shard_id: int) -> str:
    """Return the file path for shard number *shard_id*."""
    return os.path.join(PARTITION_DIR, f"ais_shard_{shard_id:04d}.csv")


def _flush_buffer(
    shard_id: int,
    buffer: List[AISRow],
    writers: Dict,
    file_handles: Dict,
) -> None:
    """Write buffered rows for *shard_id* to its CSV file."""
    if not buffer:
        return

    # Open file in append mode if already exists, write mode otherwise
    path = _shard_path(shard_id)
    first_write = shard_id not in file_handles

    if first_write:
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.writer(fh)
        writer.writerow(SHARD_HEADER)
        file_handles[shard_id] = fh
        writers[shard_id] = writer
    else:
        writer = writers[shard_id]

    for row in buffer:
        writer.writerow([
            row.timestamp,
            row.mmsi,
            row.lat,
            row.lon,
            row.sog,
            row.draught if row.draught is not None else "",
        ])


def partition_files(
    filepaths: List[str],
    num_shards: int = NUM_WORKERS,
    chunk_size: int = CHUNK_SIZE,
) -> List[str]:
    """
    Stream all *filepaths*, partition rows into shard CSV files, and
    return the list of shard file paths that were written.

    Parameters
    ----------
    filepaths  : list of input CSV paths (e.g. both daily files)
    num_shards : number of output shards (should equal NUM_WORKERS)
    chunk_size : rows buffered per shard before flushing to disk
    """
    os.makedirs(PARTITION_DIR, exist_ok=True)

    # Remove any stale shard files from a previous run
    for fname in os.listdir(PARTITION_DIR):
        if fname.startswith("ais_shard_"):
            os.remove(os.path.join(PARTITION_DIR, fname))

    # buffers[shard_id] = list of AISRow objects not yet flushed
    buffers: Dict[int, List[AISRow]] = defaultdict(list)

    # Open file handles and csv.writer objects, keyed by shard_id.
    # We keep them open throughout to avoid repeated open/close overhead.
    file_handles: Dict[int, object] = {}
    writers:      Dict[int, object] = {}

    # Track per-MMSI movement to filter always-stationary ships
    # mmsi_has_movement[mmsi] = True if any ping had SOG > 0
    mmsi_has_movement: Dict[str, bool] = {}

    # We do a two-pass approach when FILTER_ALWAYS_STATIONARY is enabled:
    # Pass 1: stream all rows into per-MMSI in-memory lists (small enough
    #          because we only store the movement flag, not full rows).
    # Actually: we buffer everything then filter at flush time.
    # For very large files this uses too much RAM, so instead we track the
    # movement flag on-the-fly and do a second pass to write only moving ships.
    #
    # Simpler approach (used here): keep a set of MMSIs that HAVE movement.
    # First pass: collect that set.  Second pass: stream again and skip
    # always-stationary MMSIs.  Cost = two sequential reads (~10s on SSD).

    if FILTER_ALWAYS_STATIONARY:
        logger.info("Pass 1/2: collecting MMSIs with movement...")
        for fp in filepaths:
            for row in stream_rows(fp):
                if row.sog > 0:
                    mmsi_has_movement[row.mmsi] = True
                elif row.mmsi not in mmsi_has_movement:
                    mmsi_has_movement[row.mmsi] = False
        moving_mmsis = {m for m, v in mmsi_has_movement.items() if v}
        logger.info("  %d MMSIs have at least one moving ping", len(moving_mmsis))
    else:
        moving_mmsis = None  # accept all

    logger.info("Pass 2/2: streaming and partitioning rows...")
    total_rows = 0

    for fp in filepaths:
        for row in stream_rows(fp):
            # Skip always-stationary ships
            if moving_mmsis is not None and row.mmsi not in moving_mmsis:
                continue

            sid = _shard_id(row.mmsi, num_shards)
            buffers[sid].append(row)
            total_rows += 1

            # Flush when buffer is full
            if len(buffers[sid]) >= chunk_size:
                _flush_buffer(sid, buffers[sid], writers, file_handles)
                buffers[sid] = []

    # Final flush for any remaining rows
    for sid, buf in buffers.items():
        if buf:
            _flush_buffer(sid, buf, writers, file_handles)

    # Close all open file handles
    for fh in file_handles.values():
        fh.close()

    written_shards = sorted(file_handles.keys())
    shard_paths = [_shard_path(s) for s in written_shards]

    logger.info(
        "Partitioning complete: %d rows → %d shards in %s",
        total_rows, len(shard_paths), PARTITION_DIR,
    )
    return shard_paths


def read_shard(shard_path: str) -> List[AISRow]:
    """
    Read a shard CSV file back into a sorted list of AISRow objects.

    Called by worker processes inside detect.py.
    Rows are sorted chronologically so gap detection works correctly
    across the two-day boundary.
    """
    rows: List[AISRow] = []

    with open(shard_path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            try:
                draught_raw = rec["draught"].strip()
                draught = float(draught_raw) if draught_raw else None
                rows.append(AISRow(
                    timestamp=float(rec["timestamp"]),
                    mmsi=rec["mmsi"].strip(),
                    lat=float(rec["lat"]),
                    lon=float(rec["lon"]),
                    sog=float(rec["sog"]),
                    draught=draught,
                ))
            except (ValueError, KeyError):
                # Skip malformed rows silently
                continue

    # Sort chronologically – crucial for cross-day gap detection
    rows.sort()
    return rows
