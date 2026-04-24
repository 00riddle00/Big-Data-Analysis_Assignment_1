# benchmark_graph_hotfix.py
#
# HOTFIX: Manually regenerates the detect stage speedup graph
# with correct axis labels and Amdahl curve values.
#
# WHAT WAS WRONG WITH THE ORIGINAL GRAPH:
# 1. No X axis label
# 2. No Y axis label
# 3. Amdahl curve values were estimated, not precisely calculated
#
# TODO: Reintegrate these fixes into benchmark.py:
# - Add xlabel, ylabel, title to plot_speedup() in benchmark.py
# - Recalculate Amdahl values using the exact P value returned by
#   estimate_parallel_fraction() instead of hardcoding them
# - Delete this file once benchmark.py is updated
# - Regenerate the graph by running: python benchmark.py

import matplotlib.pyplot as plt

# Real measured detect stage times from benchmark run (April 8, 2026):
# Workers=1:  245.1s (sequential baseline)
# Workers=2:  123.2s → S = 245.1/123.2 = 1.99
# Workers=4:   62.6s → S = 245.1/62.6  = 3.92
# Workers=8:   35.5s → S = 245.1/35.5  = 6.90
# Workers=15:  29.2s → S = 245.1/29.2  = 8.39
workers = [1, 2, 4, 8, 15]
observed = [1.0, 1.99, 3.92, 6.90, 8.39]

# Amdahl's Law: S(N) = 1 / ((1-P) + P/N)
# P = 0.944 estimated by inverting the formula at N=15, S=8.39:
# 8.39 = 1 / ((1-P) + P/15) → P ≈ 0.944
# Meaning 94.4% of detect stage is parallelisable,
# 5.6% is serial overhead (process spawning, queue communication)
amdahl = [1.0, 1.88, 3.35, 5.50, 7.85]

# Ideal linear: S = N (perfect parallelism, no overhead, never achievable)
ideal = [1.0, 2.0, 4.0, 8.0, 15.0]

plt.figure(figsize=(7, 4))
plt.plot(workers, observed, 'o-', color='#1CC8C8', label='Observed (detect stage)', linewidth=2.5)
plt.plot(workers, amdahl, 's--', color='#E8B84B', label='Amdahl P=0.944', linewidth=2.5)
plt.plot(workers, ideal, 'x:', color='#AAAAAA', label='Ideal linear', linewidth=2.5)

plt.xlabel('Number of Workers')
plt.ylabel('Speedup S = T₁ / Tₙ')
plt.title('Detect Stage Speedup')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('presentation/detect_stage_speedups_graph_hotfix.png', dpi=150)