# vim: set ft=make tw=100 nu noet ts=8 sw=8:
# ==============================================================================
# Big Data Analysis — AIS Shadow Fleet Detection
#
# Prerequisites:
#   Linux/macOS: make is pre-installed
#   Windows:     install via Rtools4 or: choco install make
#                run from Git Bash or Rtools terminal, not PowerShell
# =============================================================================

# --- Configuration ------------------------------------------------------------

PYTHON     := python3
LATEXMK    := latexmk
STAMP_DATE := date "+%F %T %Z"

ifeq ($(OS),Windows_NT)
	VENV_BIN := $(CURDIR)/.venv/Scripts
else
	VENV_BIN := $(CURDIR)/.venv/bin
endif

PRES_DIR := presentation

# Input/output files
PRESENTATION := $(PRES_DIR)/presentation_1st.pdf
TOP5_CSV     := analysis/top5_vessels.csv
SPEEDUP_JSON := benchmark_results/speedup_results.json
MPROF_PNG    := profiling/mprof_RAM_graph.png
FOLIUM_MAP   := analysis/top5_map.html

# --- Phony targets ------------------------------------------------------------

.PHONY: all deps export-requirements import-requirements data run benchmark profile visualize \
	presentation test clean distclean help

# --- Default: full pipeline ---------------------------------------------------

all: deps data run benchmark profile visualize presentation
	@echo ""
	@echo "=== Full pipeline complete. ==="

# --- Help ---------------------------------------------------------------------

help:
	@echo ""
	@echo "  make all                  Run the full pipeline from scratch"
	@echo "  make deps                 Create virtual environment and install dependencies"
	@echo "  make export-requirements  Export uv dependencies to requirements.txt"
	@echo "  make import-requirements  Import requirements.txt into uv"
	@echo "  make data                 Show instructions for downloading AIS data"
	@echo "  make run                  Run the parallel detection pipeline"
	@echo "  make benchmark            Run speedup and chunk-size benchmark suite"
	@echo "  make profile              Run memory profiling (mprof)"
	@echo "  make visualize            Generate interactive Folium map of top-5 vessels"
	@echo "  make presentation         Compile LaTeX slides to PDF"
	@echo "  make test                 Run unit tests"
	@echo "  make clean                Remove LaTeX build artifacts and mprof raw data"
	@echo "  make distclean            Clean + remove partitioned shards"
	@echo ""

# --- Step 1: Dependencies -----------------------------------------------------

deps: .venv/.stamp

# Rebuilds venv if requirements.txt has a newer timestamp than .stamp.
# Note: git operations (pull, checkout) can update file timestamps,
# causing an unnecessary rebuild. This is harmless but slow —
# `python -m venv .venv` reinitializes the venv structure without wiping
# installed packages, and pip skips already-installed dependencies.
.venv/.stamp: requirements.txt
	$(PYTHON) -m venv .venv
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements.txt
	$(STAMP_DATE) > $@
	@echo "Python dependencies installed."

# Export uv-managed dependencies to classic requirements.txt for pip-based setup.
export-requirements:
	uv export \
	  --format requirements-txt \
	  --no-hashes \
	  --no-header \
	  --no-annotate \
	  > requirements.txt

# Import updated requirements.txt into uv workflow.
import-requirements:
	uv add -r requirements.txt
	uv lock
	uv sync

# --- Step 2: Data -------------------------------------------------------------

data:
	@echo ""
	@echo "  Download AIS data manually from: http://aisdata.ais.dk/"
	@echo "  Select dates: 2025-08-13 and 2025-08-14"
	@echo "  Place the extracted CSV files in: data_arch/"
	@echo ""
	@echo "  Expected files:"
	@echo "    data_arch/aisdk-2025-08-13.csv  (~5.4 GB)"
	@echo "    data_arch/aisdk-2025-08-14.csv  (~5.6 GB)"
	@echo ""

# --- Step 3: Run pipeline -----------------------------------------------------

run: $(TOP5_CSV)

$(TOP5_CSV): .venv/.stamp
	$(VENV_BIN)/python cli.py
	@echo "Pipeline complete. Results in analysis/ and loitering/"

# --- Step 4: Benchmark --------------------------------------------------------

benchmark: $(SPEEDUP_JSON)

$(SPEEDUP_JSON): .venv/.stamp
	$(VENV_BIN)/python cli.py --benchmark
	@echo "Benchmark complete. Results in benchmark_results/"

# --- Step 5: Memory profiling -------------------------------------------------

profile: $(MPROF_PNG)

$(MPROF_PNG): .venv/.stamp
	mkdir -p profiling
	$(VENV_BIN)/mprof run $(VENV_BIN)/python cli.py
	mv mprofile_*.dat profiling/
	$(VENV_BIN)/mprof plot profiling/mprofile_*.dat --output $(MPROF_PNG)
	@echo "Memory profile saved to profiling/"

# --- Step 6: Visualize --------------------------------------------------------

visualize: $(FOLIUM_MAP)

$(FOLIUM_MAP): $(TOP5_CSV)
	$(VENV_BIN)/python visualize.py
	@echo "Folium map saved to analysis/top5_map.html"

# --- Step 7: Presentation -----------------------------------------------------

presentation: $(PRESENTATION)

$(PRESENTATION): $(PRES_DIR)/presentation_1st.tex
	cd $(PRES_DIR) && $(LATEXMK) -xelatex -interaction=nonstopmode presentation_1st.tex
	@echo "Presentation compiled: $(PRESENTATION)"

# --- Tests --------------------------------------------------------------------

test: .venv/.stamp
	$(VENV_BIN)/pytest tests/ -v

# --- Clean --------------------------------------------------------------------

clean:
	# Remove mprof strays from root only — profiling/ dir holds intentionally committed archives
	# (mprof run always writes to root first; make recipe moves to profiling/ on success)
	rm -f mprofile_*.dat
	rm -f $(PRES_DIR)/*.aux $(PRES_DIR)/*.log $(PRES_DIR)/*.nav
	rm -f $(PRES_DIR)/*.out $(PRES_DIR)/*.snm $(PRES_DIR)/*.toc
	rm -f $(PRES_DIR)/*.fls $(PRES_DIR)/*.fdb_latexmk $(PRES_DIR)/*.xdv
	rm -f $(PRES_DIR)/*.synctex.gz $(PRES_DIR)/*.bcf $(PRES_DIR)/*.blg
	rm -f $(PRES_DIR)/*.bbl $(PRES_DIR)/*.run.xml
	@echo "Cleaned LaTeX build artifacts and mprof raw data."

distclean: clean
	rm -rf partitioned/
	@echo "Also removed partitioned shards."
