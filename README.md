<!-- vim: set ft=markdown fenc=utf-8 tw=88 nu ai si et ts=2 sw=2: -->

# Assignment 1: Maritime "Shadow Fleet" Detection with Parallel Computing

**Course:** Big Data Analysis (10 ECTS), VU MIF, Spring 2026

**Study program:** MSc Data Science

**Team:** [Jonas Adomaitis](https://github.com/JonasIBM), [Gedas
Beržinskas](https://github.com/Berzinskass), [Tomas
Giedraitis](https://github.com/00riddle00)

## Table of Contents:

<!--toc:start-->
- [Part I — Assignment Specification](#part-i--assignment-specification)
  - [Introduction](#introduction)
  - [Our Goal](#our-goal)
  - [Dataset & Rules](#dataset--rules)
    - [ANTI-AI / BIG DATA CONSTRAINTS](#anti-ai--big-data-constraints)
  - [The Tasks](#the-tasks)
    - [Task 1: Low-Memory Parallel Partitioning](#task-1-low-memory-parallel-partitioning)
    - [Task 2: Implementation of Parallel Processing](#task-2-implementation-of-parallel-processing)
    - [Task 3: Shadow Fleet Detection Analytics & DFSI](#task-3-shadow-fleet-detection-analytics--dfsi)
      - [DFSI Calculation](#dfsi-calculation)
    - [Task 4: Hardware-Specific Performance Evaluation](#task-4-hardware-specific-performance-evaluation)
      - [Speedup Analysis](#speedup-analysis)
      - [Memory Profiling](#memory-profiling)
      - [Chunk Optimization](#chunk-optimization)
    - [Task 5: Presentation & Real-World Verification](#task-5-presentation--real-world-verification)
  - [Submission Guidelines](#submission-guidelines)
  - [Evaluation Criteria (10 Points Total + 1 Bonus)](#evaluation-criteria-10-points-total--1-bonus)
  - [Amdahl’s Law (Bonus)](#amdahls-law-bonus)
  - [Final Note](#final-note)
- [Part II — Our Implementation](#part-ii--our-implementation)
  - [Shadow Fleet Detection with Parallel Computing](#shadow-fleet-detection-with-parallel-computing)
    - [Architecture](#architecture)
    - [Setup](#setup)
    - [Running the Pipeline](#running-the-pipeline)
    - [Running Tests](#running-tests)
    - [Key Design Decisions](#key-design-decisions)
      - [Memory management (Anti-AI / Big Data constraint)](#memory-management-anti-ai--big-data-constraint)
      - [Dirty Data Trap](#dirty-data-trap)
      - [Parallelisation strategy](#parallelisation-strategy)
      - [Two-day continuity](#two-day-continuity)
    - [DFSI Formula](#dfsi-formula)
    - [Anomaly Thresholds](#anomaly-thresholds)
    - [Project Structure](#project-structure)
    - [Notes](#notes)
- [Part III — Conclusion](#part-iii--conclusion)
<!--toc:end-->

# Part I — Assignment Specification

## Introduction
The Baltic Sea and global shipping lanes are seeing a significant rise in "Shadow Fleet" activities. These are vessels intentionally manipulating, spoofing, or disabling their Automatic Identification System (AIS) transponders to evade sanctions, conduct illegal fishing, or perform illicit ship-to-ship cargo transfers.

## Our Goal
Process massive, gigabyte-scale vessel tracking datasets using parallel computing architectures to detect these illicit behaviors. You will focus on low-memory data streaming, efficient parallel partitioning, mathematical anomaly detection, and rigorous hardware performance evaluation.

---

## Dataset & Rules

- **Dataset:** Danish Maritime Authority AIS Data: http://aisdata.ais.dk/
- **Unique Selection:** Each student/group must select a single two day’s data. Pick a different days and coordinate in the class chat to avoid overlapping analyses.

**Our selection:**
- Dates: 2025-08-13 and 2025-08-14  
- Motivation: We specifically chose large files to practice real big data processing (zip files ~1GB each, ~5.5GB unzipped each)  
- Files:
  - aisdk-2025-08-13.csv  
  - aisdk-2025-08-14.csv  

### ANTI-AI / BIG DATA CONSTRAINTS

1. **The pandas Ban:**  
   You may not use `pandas.read_csv()` to load the entire dataset into memory. Real Big Data does not fit in RAM. You must parse the data using Python’s native `csv` module or file generators (`yield`), passing chunks or streams to your parallel workers.  
   *(Note: You may use pandas at the very end to format or graph your final top 5 results).*

2. **Memory Limit:**  
   Your solution must process the 2GB+ daily CSV file staying strictly under 1GB of RAM per CPU core.

---

## The Tasks

### Task 1: Low-Memory Parallel Partitioning
Strategize and implement the division of millions of AIS rows into parallelizable sub-tasks without loading the entire file into memory.

- Write a custom streaming partitioner that reads the file line-by-line and dispatches chunks to worker processes.
- **The "Dirty Data" Trap:** The dataset contains default/invalid MMSI numbers (e.g., 000000000, 111111111, 123456789) from unconfigured transponders. If you blindly group by MMSI, your workers will crash from memory overload on these massive default groups. You must explicitly filter or handle these in your partitioning stream.
- Keep in mind that not the only faulty data.

---

### Task 2: Implementation of Parallel Processing
Develop Python code to process the AIS data in parallel.

- Utilize native Python libraries such as `multiprocessing` or `concurrent.futures`.
- Implement a custom Map-Reduce style logic to track vessel states chronologically within your isolated parallel workers.

---

### Task 3: Shadow Fleet Detection Analytics & DFSI
Implement algorithms to detect the following four anomalies, then calculate the custom Shadow Fleet Suspicion Index (DFSI).

- **Anomaly A ("Going Dark"):** Find AIS gaps of > 4 hours where the geographic distance between the disappearance and reappearance coordinates implies the ship kept moving (it was not simply anchored).
- **Anomaly B (Loitering & Transfers):** Detect two distinct, valid MMSI numbers located within 500 meters of each other, maintaining a speed (SOG) of < 1 knot, for > 2 hours.
- **Anomaly C (Draft Changes at Sea):** Detect vessels whose draught (depth in water) changes by more than 5% during an AIS blackout of > 2 hours (implying cargo was loaded/unloaded illegally).
- **Anomaly D (Identity Cloning / "Teleportation"):** Identify instances where the same MMSI pings from two locations requiring an impossible travel speed (> 60 knots), indicating two physical ships are broadcasting the same stolen ID.

#### DFSI Calculation

$$
DFSI = \frac{Max\ Gap\ in\ Hours}{2} + \frac{Total\ Impossible\ Distance\ Jump\ (Nautical\ Miles)}{10} + (C \times 15)
$$

Where:
- C is the number of illicit Draft Changes detected for that vessel.

---

### Task 4: Hardware-Specific Performance Evaluation
Evaluate your parallel architecture's efficiency.

#### Speedup Analysis

$$
S = \frac{T(sequential)}{T(parallel)}
$$

- Compare execution time between sequential (1 core) and parallel implementations.

#### Memory Profiling
- Use a tool like `memory_profiler` or `mprof` to generate a graph showing your RAM usage over time.
- Prove you stayed under the memory limits.

#### Chunk Optimization
- Test different chunk sizes (e.g., 10,000 rows vs 100,000 rows per chunk) and plot the impact on execution time.

---

### Task 5: Presentation & Real-World Verification
Create a maximum of 6 slides to defend your architecture:

1. Explain your low-memory partitioning strategy and how you handled the Dirty Data trap.
2. Show your Speedup and Memory Profiling graphs.
3. **Real-World Proof:** Take the coordinates of your highest DFSI scoring vessel, plug them into Maps, and include a screenshot. Explain the geographical context (e.g., "This ship went dark exactly on the border of the Russian EEZ").
4. **Code Defense:** One student from each group will be randomly selected to explain specific lines of the parallel code during the presentation.

---

## Submission Guidelines

- **Deadline:** TBA

- **Code Repository:** Provide a link to your GitHub/GitLab repository. The repo must include a clean README.md, your .py scripts, and a requirements.txt. Do not upload the 2GB CSV file to GitHub.
- **Presentation:** Submit your 6 slides in PDF format.

---

## Evaluation Criteria (10 Points Total + 1 Bonus)

| Category | Points | Grading Criteria |
|----------|--------|-----------------|
| Code Architecture & Memory Management | 2 pts | Python parallel code is clean, well-commented, avoids pandas.read_csv for full-file loading, streams data efficiently, avoids Dirty Data trap |
| Shadow Fleet Analytics (Anomalies A-D & DFSI) | 3 pts | All 4 anomalies + DFSI calculated correctly with proper chronological sorting |
| Performance Profiling & Benchmarking | 3 pts | Speedup calculated correctly, memory graphs provided, chunk-size impact analyzed |
| Presentation & Geographical Proof | 2 pts | Clear 5-slide presentation, includes real-world map validation |
| Vilnius University HPC System (BONUS) | +1 pt | Code runs on VU HPC with SLURM and scaling logs |
| Amdahl's Law Analysis (BONUS) | +1 pt | Estimates parallel fraction and explains deviation from ideal speedup |

---

## Amdahl’s Law (Bonus)

$$
S = \frac{1}{(1 - P) + \frac{P}{N}}
$$

Where:
- P is the parallelizable fraction of the pipeline
- N is the number of cores

---

## Final Note
Remember, I am not just looking for code that "runs." I am looking for code that scales. Think deeply about how your data moves from your hard drive to your CPU cores.

---

# Part II — Our Implementation

## Shadow Fleet Detection with Parallel Computing

Dataset: Danish AIS Data – August 13–14, 2025  
Files processed: `aisdk-2025-08-13.csv` (5.4 GB) + `aisdk-2025-08-14.csv` (5.6 GB)  
Total rows: ~64 million

---

## Architecture

```
data_arch/*.csv  →  cli.py  →  pipeline.py  →  partition.py  →  parsing.py
                                                     ↓
                                          partitioned/ais_shard_*.csv
                                                     ↓
                                               detect.py  →  geo.py, models.py
                                                     ↓
                              analysis/*_events.csv, *_vessels.csv,
                              analysis/loiter_candidates.csv
                                          ↓                    ↓
                                    scoring.py           loiter.py
                                          ↓
                                 vessel_scores.csv
                                  top5_vessels.csv
```

---

## Setup

```bash
git clone <repo-url>
cd shadow_fleet

pip install -r requirements.txt

# Place the AIS CSV files in data_arch/:
mkdir -p data_arch
# (files not included – download from http://aisdata.ais.dk/)
```

---

## Running the Pipeline

```bash
# Full parallel run (uses all available cores - 1):
python cli.py

# Specify files explicitly:
python cli.py --files data_arch/aisdk-2025-08-13.csv data_arch/aisdk-2025-08-14.csv

# Sequential baseline (for speedup measurement):
python cli.py --sequential

# Custom worker count and chunk size:
python cli.py --workers 8 --chunk-size 100000

# Full benchmark suite (speedup + chunk size + graphs):
python cli.py --benchmark

# Memory profiling:
mprof run python cli.py
mprof plot

# Generate interactive Folium map of top-5 vessels:
python visualize.py
```

---

## Running Tests

```bash
python -m pytest tests/ -v
```

---

## Key Design Decisions

### Memory management (Anti-AI / Big Data constraint)
- **No `pandas.read_csv()`** – data is streamed line-by-line using Python's
  native `csv.reader` via a generator in `parsing.py`.
- **Shard buffering**: rows are accumulated in per-shard buffers of `CHUNK_SIZE`
  rows and flushed to disk when full. Peak RAM ≈ `NUM_WORKERS × CHUNK_SIZE × ~100 bytes`.
  With 15 workers and 50 000-row chunks: ~75 MB – well inside the 1 GB/core budget.
- **Two-pass stationary filter**: first pass collects MMSIs with at least one
  moving ping; second pass writes only those to shards. Stationary ships are
  excluded entirely (confirmed correct by lecturer).

### Dirty Data Trap
Filtered in `parsing.py`:
- Non-Class-A rows (Class B, Base Station, AtoN, SAR Airborne, etc.)
- Invalid MMSIs: `000000000`, `111111111`, `123456789`, all-same-digit patterns
- Coordinates equal to AIS "not available" sentinel `(91.0, 0.0)`
- Timestamps that cannot be parsed

### Parallelisation strategy
- **Anomalies A, C, D** – fully per-vessel; each MMSI hashes to a fixed shard,
  so workers process shards independently with no shared state.
- **Anomaly B** – requires cross-vessel comparison; solved with a two-step
  approach: (1) `detect.py` emits slow-moving `LoiterCandidate` segments,
  (2) `loiter.py` applies bounding-box pre-filter then exact Haversine.
- **`multiprocessing.Pool.imap_unordered`** used in the detect stage so results
  stream back as workers finish (no synchronisation stall).

### Two-day continuity
Both CSV files are processed together in a single pass (concatenated in
`stream_rows`). Shards are sorted chronologically in `read_shard()` before
anomaly detection, so AIS gaps spanning midnight (Aug 13 → Aug 14) are
detected as a single continuous event.

---

## DFSI Formula

```
DFSI = (Max_Gap_Hours / 2) + (Largest_Impossible_Jump_NM / 10) + (C × 15)
```

Where:
- `Max_Gap_Hours` = single longest AIS blackout (Anomaly A)
- `Largest_Impossible_Jump_NM` = single largest impossible distance jump (Anomaly D)
- `C` = number of illicit draught changes (Anomaly C)

---

## Anomaly Thresholds

| Anomaly | Threshold |
|---------|-----------|
| A – Going Dark | Gap > 4 h AND implied speed > 1 kt |
| B – Loitering | Distance < 500 m, SOG < 1 kt, duration > 2 h |
| C – Draft Change | Change > 5% during blackout > 2 h |
| D – Identity Cloning | Implied speed > 60 kt, sustained > 10 min |

---

## Project Structure

```
shadow_fleet/
├── cli.py          # Command-line entry point
├── config.py       # All thresholds and parameters
├── pipeline.py     # Pipeline orchestrator
├── partition.py    # Streaming MMSI-based partitioner
├── parsing.py      # csv.reader-based validator / filter
├── detect.py       # Anomalies A, C, D (parallel workers)
├── loiter.py       # Anomaly B (loitering / proximity)
├── geo.py          # Haversine, bounding box
├── models.py       # Data classes
├── scoring.py      # DFSI calculation and ranking
├── benchmark.py    # Speedup, chunk-size, Amdahl's Law
├── visualize.py    # Folium interactive map
├── requirements.txt
├── tests/
│   └── test_shadow_fleet.py
├── data_arch/      # (gitignored) – place CSV files here
├── partitioned/    # (gitignored) – shard CSV files
├── analysis/       # Anomaly event CSVs + scores
└── loitering/      # Loitering event CSVs
```

---

## Notes

- The 2 GB CSV files **are not uploaded to GitHub** (see `.gitignore`).
- All thresholds are tunable in `config.py` without touching other modules.
- If some anomaly types are absent from the chosen dates, that is expected
  and documented in the presentation (confirmed by lecturer).

---

# Part III — Conclusion

This project demonstrates scalable big data processing using streaming and parallel computation under strict memory constraints, successfully identifying anomalous maritime behavior.
