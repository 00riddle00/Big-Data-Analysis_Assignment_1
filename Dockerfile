# Shadow Fleet Detection Pipeline
# Docker Hub: tomasososdev/shadow-fleet
#
# Build:
#   docker build -t tomasososdev/shadow-fleet .
#
# Run tests:
#   docker run --rm tomasososdev/shadow-fleet
#
# Run pipeline (mount your CSV files):
#   docker run --rm \
#     -v ./data_arch:/data \
#     -v ./analysis:/app/analysis \
#     -v ./loitering:/app/loitering \
#     tomasososdev/shadow-fleet \
#     python cli.py --files /data/aisdk-2025-08-13.csv /data/aisdk-2025-08-14.csv

FROM python:3.13-slim

WORKDIR /app

# Install dependencies first (separate layer — rebuilds only when requirements change)
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Copy source code
COPY *.py ./
COPY tests/ ./tests/

# Default command: run the test suite to verify the image works correctly
CMD ["python", "-m", "pytest", "tests/", "-v"]
