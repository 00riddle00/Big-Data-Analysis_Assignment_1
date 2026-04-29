# vim: set ft=make tw=100 nu noet ts=8 sw=8:
# ==============================================================================
# Big Data Analysis — AIS Shadow Fleet Detection
#
# Usage:
#   make all           Run the full pipeline from scratch
#   make deps          Create virtual environment and install dependencies
#   make data          Show instructions for downloading AIS data
#   make run           Run the parallel detection pipeline
#   make benchmark     Run speedup and chunk-size benchmark suite
#   make profile       Run memory profiling (mprof)
#   make visualize     Generate interactive Folium map of top-5 vessels
#   make presentation  Compile LaTeX slides to PDF
#   make test          Run unit tests (25 tests)
#   make clean         Remove LaTeX build artifacts and mprof raw data
#   make distclean     clean + remove partitioned shards
#   make help          Show all available targets
#
# Prerequisites:
#   Linux/macOS: make is pre-installed
#   Windows:     install via: choco install make
#                run from Git Bash, not PowerShell
#
# ==============================================================================

# --- Configuration ------------------------------------------------------------

PYTHON     := python3
PIP        := .venv/bin/pip
LATEXMK    := latexmk
STAMP_DATE := date "+%F %T %Z"

PRES_DIR     := presentation
PRESENTATION := $(PRES_DIR)/presentation_1st.pdf

VENV := .venv

TOP5_CSV     := analysis/top5_vessels.csv
SPEEDUP_JSON := benchmark_results/speedup_results.json
MPROF_PNG    := profiling/mprof.png
FOLIUM_MAP   := analysis/top5_map.html

# --- Phony targets ------------------------------------------------------------

.PHONY: all deps data run benchmark profile visualize presentation \
        test clean distclean help

# --- Default: full pipeline ---------------------------------------------------

all: deps data run benchmark profile visualize presentation
	@echo ""
	@echo "=== Full pipeline complete. ==="

# --- Help ---------------------------------------------------------------------

help:
	@echo ""
	@echo "  make all           Run the full pipeline from scratch"
	@echo "  make deps          Create virtual environment and install dependencies"
	@echo "  make data          Show instructions for downloading AIS data"
	@echo "  make run           Run the parallel detection pipeline"
	@echo "  make benchmark     Run speedup and chunk-size benchmark suite"
	@echo "  make profile       Run memory profiling (mprof)"
	@echo "  make visualize     Generate interactive Folium map of top-5 vessels"
	@echo "  make presentation  Compile LaTeX slides to PDF"
	@echo "  make test          Run unit tests"
	@echo "  make clean         Remove LaTeX build artifacts and mprof raw data"
	@echo "  make distclean     clean + remove partitioned shards"
	@echo ""

# --- Step 1: Dependencies -----------------------------------------------------

deps: $(VENV)/.stamp

# Rebuilds venv if requirements.txt has a newer timestamp than .stamp.
# Note: git operations (pull, checkout) can update file timestamps,
# causing an unnecessary rebuild. This is harmless but slow —
# `python -m venv .venv` reinitializes the venv structure without wiping
# installed packages, and pip skips already-installed dependencies.
$(VENV)/.stamp: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(STAMP_DATE) > $@
	@echo "Python dependencies installed in $(VENV)/"

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

$(TOP5_CSV): $(VENV)/.stamp
	$(VENV)/bin/python cli.py
	@echo "Pipeline complete. Results in analysis/ and loitering/"

# --- Step 4: Benchmark --------------------------------------------------------

benchmark: $(SPEEDUP_JSON)

$(SPEEDUP_JSON): $(VENV)/.stamp
	$(VENV)/bin/python cli.py --benchmark
	@echo "Benchmark complete. Results in benchmark_results/"

# --- Step 5: Memory profiling -------------------------------------------------

profile: $(MPROF_PNG)

$(MPROF_PNG): $(VENV)/.stamp
	mkdir -p profiling
	$(VENV)/bin/mprof run $(VENV)/bin/python cli.py
	mv mprofile_*.dat profiling/
	$(VENV)/bin/mprof plot profiling/mprofile_*.dat --output profiling/mprof.png
	@echo "Memory profile saved to profiling/"

# --- Step 6: Visualize --------------------------------------------------------

visualize: $(FOLIUM_MAP)

$(FOLIUM_MAP): $(TOP5_CSV)
	$(VENV)/bin/python visualize.py
	@echo "Folium map saved to analysis/top5_map.html"

# --- Step 7: Presentation -----------------------------------------------------

presentation: $(PRESENTATION)

$(PRESENTATION): $(PRES_DIR)/presentation_1st.tex
	cd $(PRES_DIR) && $(LATEXMK) -xelatex -interaction=nonstopmode presentation_1st.tex
	@echo "Presentation compiled: $(PRESENTATION)"

# --- Tests --------------------------------------------------------------------

test: $(VENV)/.stamp
	$(VENV)/bin/pytest tests/ -v

# --- Clean --------------------------------------------------------------------

clean:
	rm -f profiling/mprofile_*.dat
	rm -f $(PRES_DIR)/*.aux $(PRES_DIR)/*.log $(PRES_DIR)/*.nav
	rm -f $(PRES_DIR)/*.out $(PRES_DIR)/*.snm $(PRES_DIR)/*.toc
	rm -f $(PRES_DIR)/*.fls $(PRES_DIR)/*.fdb_latexmk $(PRES_DIR)/*.xdv
	rm -f $(PRES_DIR)/*.synctex.gz $(PRES_DIR)/*.bcf $(PRES_DIR)/*.blg
	rm -f $(PRES_DIR)/*.bbl $(PRES_DIR)/*.run.xml
	@echo "Cleaned LaTeX build artifacts and mprof raw data."

distclean: clean
	rm -rf partitioned/
	@echo "Also removed partitioned shards."
