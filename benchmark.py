"""
benchmark.py - Hardware Performance Evaluation (Task 4).

Measures:
  1. Speedup S = T_sequential / T_parallel  for various worker counts
  2. Chunk-size impact on execution time
  3. Memory usage over time (via memory_profiler / mprof)

Usage
-----
  # Run full benchmark (generates graphs automatically):
  python benchmark.py

  # Memory profiling (run separately for clean mprof output):
  pip install memory-profiler
  mprof run python benchmark.py --memory-only
  mprof plot

Amdahl's Law bonus
------------------
After collecting speedup data, this module estimates the parallelisable
fraction P and plots the theoretical vs. observed speedup curve.
"""

import json
import logging
import os
import sys
import time
from typing import List, Dict

# matplotlib is optional – only needed for graphs
try:
    import matplotlib
    matplotlib.use("Agg")          # non-interactive backend
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("[WARNING] matplotlib not installed – skipping graphs")

from config import (
    BENCHMARK_WORKER_COUNTS,
    BENCHMARK_CHUNK_SIZES,
    DATA_FILES,
    CHUNK_SIZE,
    NUM_WORKERS,
)
from pipeline import run

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

BENCHMARK_OUTPUT_DIR = "benchmark_results"


# ---------------------------------------------------------------------------
# Speedup benchmark
# ---------------------------------------------------------------------------

def benchmark_worker_counts(
    filepaths: List[str] = DATA_FILES,
    worker_counts: List[int] = None,
) -> Dict[int, float]:
    """
    Run the full pipeline for each worker count in *worker_counts*.
    Returns {num_workers: total_time_seconds}.
    """
    if worker_counts is None:
        worker_counts = BENCHMARK_WORKER_COUNTS

    os.makedirs(BENCHMARK_OUTPUT_DIR, exist_ok=True)
    times: Dict[int, float] = {}

    for n_workers in worker_counts:
        logger.info("--- Benchmarking with %d worker(s) ---", n_workers)
        t0 = time.perf_counter()
        metadata = run(
            filepaths=filepaths,
            num_workers=n_workers,
            chunk_size=CHUNK_SIZE,
            run_metadata_path=os.path.join(
                BENCHMARK_OUTPUT_DIR, f"meta_w{n_workers}.json"
            ),
        )
        elapsed = time.perf_counter() - t0
        times[n_workers] = round(elapsed, 2)
        logger.info("  Workers=%d  Total time=%.2f s", n_workers, elapsed)

    # Compute speedup relative to sequential (1 worker)
    t_seq = times.get(1)
    speedup: Dict[int, float] = {}
    if t_seq:
        for n, t in times.items():
            speedup[n] = round(t_seq / t, 3)

    results = {"times": times, "speedup": speedup}
    with open(os.path.join(BENCHMARK_OUTPUT_DIR, "speedup_results.json"), "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Speedup results: %s", speedup)
    return times


def benchmark_chunk_sizes(
    filepaths: List[str] = DATA_FILES,
    chunk_sizes: List[int] = None,
) -> Dict[int, float]:
    """
    Run the full pipeline for each chunk size in *chunk_sizes*.
    Returns {chunk_size: total_time_seconds}.
    """
    if chunk_sizes is None:
        chunk_sizes = BENCHMARK_CHUNK_SIZES

    os.makedirs(BENCHMARK_OUTPUT_DIR, exist_ok=True)
    times: Dict[int, float] = {}

    for cs in chunk_sizes:
        logger.info("--- Benchmarking chunk size %d ---", cs)
        t0 = time.perf_counter()
        run(
            filepaths=filepaths,
            num_workers=NUM_WORKERS,
            chunk_size=cs,
            run_metadata_path=os.path.join(
                BENCHMARK_OUTPUT_DIR, f"meta_chunk{cs}.json"
            ),
        )
        elapsed = time.perf_counter() - t0
        times[cs] = round(elapsed, 2)
        logger.info("  ChunkSize=%d  Total time=%.2f s", cs, elapsed)

    with open(
        os.path.join(BENCHMARK_OUTPUT_DIR, "chunk_results.json"), "w"
    ) as f:
        json.dump(times, f, indent=2)

    return times


# ---------------------------------------------------------------------------
# Amdahl's Law analysis (bonus task)
# ---------------------------------------------------------------------------

def amdahls_law_speedup(p: float, n: int) -> float:
    """Theoretical speedup according to Amdahl's Law: S(n) = 1 / (1-p + p/n)"""
    return 1.0 / ((1.0 - p) + p / n)


def estimate_parallel_fraction(
    times: Dict[int, float],
) -> float:
    """
    Estimate P (parallelisable fraction) by fitting Amdahl's formula to
    the observed speedup at the highest worker count available.

    S_obs = T_1 / T_n
    S_amd = 1 / (1-P + P/n)
    Solve for P: P = (1/S_obs - 1) / (1/n - 1)
    """
    t_seq = times.get(1)
    if not t_seq:
        return 0.9  # default fallback

    best_n = max(k for k in times if k > 1)
    s_obs = t_seq / times[best_n]
    # Algebraic rearrangement of Amdahl's formula
    try:
        p = (1.0 / s_obs - 1.0) / (1.0 / best_n - 1.0)
        p = max(0.0, min(1.0, p))   # clamp to [0, 1]
    except ZeroDivisionError:
        p = 0.9
    return round(p, 3)


# ---------------------------------------------------------------------------
# Graph generation
# ---------------------------------------------------------------------------

def plot_speedup(times: Dict[int, float]) -> None:
    """Plot observed speedup vs Amdahl's theoretical curve."""
    if not HAS_MATPLOTLIB:
        return

    t_seq = times.get(1)
    if not t_seq:
        return

    worker_counts = sorted(times.keys())
    observed_speedup = [t_seq / times[n] for n in worker_counts]

    p = estimate_parallel_fraction(times)
    theoretical_speedup = [amdahls_law_speedup(p, n) for n in worker_counts]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(worker_counts, observed_speedup,  "bo-", label="Observed speedup")
    ax.plot(worker_counts, theoretical_speedup, "r--",
            label=f"Amdahl's Law (P={p:.2f})")
    ax.plot(worker_counts, worker_counts, "g:", label="Ideal linear speedup")

    ax.set_xlabel("Number of Workers")
    ax.set_ylabel("Speedup S = T₁ / Tₙ")
    ax.set_title("Parallel Speedup – Shadow Fleet Pipeline")
    ax.legend()
    ax.grid(True, alpha=0.3)

    path = os.path.join(BENCHMARK_OUTPUT_DIR, "speedup_graph.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Speedup graph saved: %s", path)


def plot_chunk_impact(times: Dict[int, float]) -> None:
    """Bar chart of chunk size vs execution time."""
    if not HAS_MATPLOTLIB:
        return

    chunk_sizes = sorted(times.keys())
    elapsed = [times[c] for c in chunk_sizes]
    labels  = [f"{c//1000}k" for c in chunk_sizes]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, elapsed, color="steelblue", edgecolor="white")
    ax.bar_label(bars, fmt="%.1f s", padding=3)
    ax.set_xlabel("Chunk Size (rows)")
    ax.set_ylabel("Total Execution Time (s)")
    ax.set_title("Chunk Size Impact on Execution Time")
    ax.grid(axis="y", alpha=0.3)

    path = os.path.join(BENCHMARK_OUTPUT_DIR, "chunk_impact_graph.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Chunk impact graph saved: %s", path)


# ---------------------------------------------------------------------------
# Memory-profiled entry point
# ---------------------------------------------------------------------------

def memory_profiled_run():
    """
    Decorated with @profile so mprof can track per-line memory usage.
    Run with:  mprof run python benchmark.py --memory-only
    """
    try:
        from memory_profiler import profile
    except ImportError:
        logger.warning("memory_profiler not installed; running without profiling.")
        run()
        return

    @profile
    def _inner():
        run()

    _inner()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    memory_only = "--memory-only" in sys.argv

    if memory_only:
        logger.info("Memory-profiling mode – single run only")
        memory_profiled_run()
    else:
        logger.info("Full benchmark mode")

        logger.info("=== Speedup Benchmark ===")
        speedup_times = benchmark_worker_counts()
        plot_speedup(speedup_times)

        # Estimate and log Amdahl's Law parallelisable fraction
        p = estimate_parallel_fraction(speedup_times)
        t_seq = speedup_times.get(1, None)
        if t_seq:
            logger.info(
                "Estimated parallelisable fraction P = %.3f  "
                "(theoretical max speedup with ∞ cores: %.1fx)",
                p, 1.0 / (1.0 - p) if p < 1.0 else float("inf"),
            )

        logger.info("=== Chunk Size Benchmark ===")
        chunk_times = benchmark_chunk_sizes()
        plot_chunk_impact(chunk_times)

        logger.info("Benchmark complete.  Results in: %s/", BENCHMARK_OUTPUT_DIR)
