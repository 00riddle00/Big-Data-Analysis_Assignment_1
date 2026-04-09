"""
cli.py - Command-line entry point for the Shadow Fleet Detection pipeline.

Usage examples
--------------
  # Run the full pipeline with defaults from config.py:
  python cli.py

  # Specify input files explicitly:
  python cli.py --files data_arch/aisdk-2025-08-13.csv data_arch/aisdk-2025-08-14.csv

  # Tune workers and chunk size:
  python cli.py --workers 8 --chunk-size 100000

  # Run benchmarks only:
  python cli.py --benchmark

  # Run in sequential mode (1 worker) for speedup baseline:
  python cli.py --sequential
"""

import argparse
import logging
import sys
import time

import config  # import first so other modules can import config safely


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Shadow Fleet Detection Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--files", nargs="+", default=config.DATA_FILES,
        metavar="CSV",
        help="Input AIS CSV file(s) to process",
    )
    parser.add_argument(
        "--workers", type=int, default=config.NUM_WORKERS,
        help="Number of parallel worker processes",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=config.CHUNK_SIZE,
        dest="chunk_size",
        help="Rows buffered per shard before flushing to disk",
    )
    parser.add_argument(
        "--sequential", action="store_true",
        help="Force 1 worker (for speedup baseline measurement)",
    )
    parser.add_argument(
        "--benchmark", action="store_true",
        help="Run full benchmark suite (speedup + chunk size tests)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    _setup_logging(args.verbose)
    logger = logging.getLogger("cli")

    if args.benchmark:
        logger.info("Starting benchmark suite...")
        import benchmark
        benchmark_times = benchmark.benchmark_worker_counts(args.files)
        benchmark.plot_speedup(benchmark_times)
        chunk_times = benchmark.benchmark_chunk_sizes(args.files)
        benchmark.plot_chunk_impact(chunk_times)
        p = benchmark.estimate_parallel_fraction(benchmark_times)
        logger.info("Parallelisable fraction P = %.3f", p)
        return

    from pipeline import run, run_sequential

    if args.sequential:
        logger.info("Running in sequential mode (1 worker)...")
        t0 = time.perf_counter()
        metadata = run_sequential(
            filepaths=args.files,
            chunk_size=args.chunk_size,
        )
    else:
        logger.info(
            "Running with %d workers, chunk size %d...",
            args.workers, args.chunk_size,
        )
        t0 = time.perf_counter()
        metadata = run(
            filepaths=args.files,
            num_workers=args.workers,
            chunk_size=args.chunk_size,
        )

    elapsed = time.perf_counter() - t0
    logger.info("Pipeline finished in %.1f seconds", elapsed)
    logger.info("Timings breakdown: %s", metadata.get("timings", {}))
    logger.info(
        "Results in: analysis/  loitering/  analysis/top5_vessels.csv"
    )


if __name__ == "__main__":
    main()
