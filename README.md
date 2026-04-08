# Assignment 1: Maritime "Shadow Fleet" Detection with Parallel Computing

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
