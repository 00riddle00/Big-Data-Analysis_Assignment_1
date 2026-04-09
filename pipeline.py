"""
pipeline.py - Orchestrates the full Shadow Fleet Detection pipeline.

Stages
------
1. Partition   – stream input CSVs into MMSI-based shard files on disk
2. Detect      – run anomaly detectors (A/C/D) in parallel across shards
3. Loiter      – detect Anomaly B (ship-to-ship transfers) from candidates
4. Score       – calculate DFSI, write rankings
5. (Optional)  – generate Folium map of top-5 vessels

The parallel stage uses multiprocessing.Pool.imap_unordered, which is
the pattern preferred by the lecturer (see Lecture 4 / 6.1.py).
Each worker process handles one shard, independently, with no shared
memory – eliminating race conditions entirely.
"""

import json
import logging
import multiprocessing
import os
import time
from typing import List, Optional

from config import NUM_WORKERS, CHUNK_SIZE, DATA_FILES, ANALYSIS_DIR
from detect import process_shard, write_events_csv
from loiter import run_loiter_detection
from models import VesselDetectionResult
from partition import partition_files
from scoring import score_vessels, write_scores

logger = logging.getLogger(__name__)


def run(
    filepaths: Optional[List[str]] = None,
    num_workers: int = NUM_WORKERS,
    chunk_size:  int = CHUNK_SIZE,
    run_metadata_path: str = "run_metadata.json",
) -> dict:
    """
    Execute the full pipeline and return a metadata dict with timings.

    Parameters
    ----------
    filepaths      : input CSV paths (defaults to config.DATA_FILES)
    num_workers    : number of parallel worker processes
    chunk_size     : rows per shard flush (partition stage)
    run_metadata_path : path where JSON metadata is written
    """
    if filepaths is None:
        filepaths = DATA_FILES

    metadata: dict = {
        "files": filepaths,
        "num_workers": num_workers,
        "chunk_size": chunk_size,
        "timings": {},
    }

    # ------------------------------------------------------------------
    # Stage 1 – Partition
    # ------------------------------------------------------------------
    logger.info("=== Stage 1: Partitioning ===")
    t0 = time.perf_counter()

    shard_paths = partition_files(
        filepaths=filepaths,
        num_shards=num_workers,
        chunk_size=chunk_size,
    )
    metadata["num_shards"] = len(shard_paths)
    metadata["timings"]["partition_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Partition stage: %.1f s", metadata["timings"]["partition_s"])

    # ------------------------------------------------------------------
    # Stage 2 – Parallel Detection (A, C, D)
    # ------------------------------------------------------------------
    logger.info("=== Stage 2: Parallel Detection (%d workers) ===", num_workers)
    t0 = time.perf_counter()

    detection_results: List[VesselDetectionResult] = []

    # multiprocessing.Pool + imap_unordered:
    # - Pool manages a fixed set of worker processes (no process-per-shard overhead)
    # - imap_unordered yields results as soon as any worker finishes,
    #   keeping all workers busy even when shard processing times vary
    with multiprocessing.Pool(processes=num_workers) as pool:
        for result in pool.imap_unordered(process_shard, shard_paths):
            detection_results.append(result)
            logger.debug("Received shard result (total so far: %d)", len(detection_results))

    metadata["timings"]["detect_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Detection stage: %.1f s", metadata["timings"]["detect_s"])

    # Write all anomaly event CSVs to disk (used by scoring and loiter stages)
    write_events_csv(detection_results)

    # ------------------------------------------------------------------
    # Stage 3 – Loitering Detection (Anomaly B)
    # ------------------------------------------------------------------
    logger.info("=== Stage 3: Loitering Detection ===")
    t0 = time.perf_counter()
    run_loiter_detection()
    metadata["timings"]["loiter_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Loiter stage: %.1f s", metadata["timings"]["loiter_s"])

    # ------------------------------------------------------------------
    # Stage 4 – Merge & Score
    # ------------------------------------------------------------------
    logger.info("=== Stage 4: Scoring ===")
    t0 = time.perf_counter()
    scored = score_vessels(detection_results)
    write_scores(scored)
    metadata["timings"]["score_s"] = round(time.perf_counter() - t0, 2)
    metadata["vessels_scored"] = len(scored)
    logger.info("Score stage: %.1f s", metadata["timings"]["score_s"])

    # Total wall-clock time
    metadata["timings"]["total_s"] = round(
        sum(v for k, v in metadata["timings"].items() if k != "total_s"), 2
    )

    # ------------------------------------------------------------------
    # Write run metadata
    # ------------------------------------------------------------------
    with open(run_metadata_path, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)
    logger.info("Run metadata written to %s", run_metadata_path)

    return metadata


def run_sequential(
    filepaths: Optional[List[str]] = None,
    chunk_size: int = CHUNK_SIZE,
) -> dict:
    """
    Run the pipeline with a single worker (for speedup baseline measurement).

    Identical to run() but forces num_workers=1 so the detect stage
    processes all shards in one process sequentially.
    """
    return run(
        filepaths=filepaths,
        num_workers=1,
        chunk_size=chunk_size,
        run_metadata_path="run_metadata_sequential.json",
    )
