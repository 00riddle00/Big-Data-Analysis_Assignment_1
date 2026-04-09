"""
parsing.py - Low-memory CSV parsing for AIS data.

Key design decisions
--------------------
* Uses Python's native csv.reader – NO pandas.read_csv().
* Reads the file line-by-line via a generator (yield), so the full
  5 GB file is never loaded into RAM at once.
* Filters invalid / irrelevant rows as early as possible to reduce
  the data volume passed downstream.
"""

import csv
import logging
from datetime import datetime, timezone
from typing import Generator, Optional

from config import (
    VALID_MOBILE_TYPES,
    INVALID_MMSI_EXACT,
    MMSI_MIN_LENGTH,
    LAT_MIN, LAT_MAX,
    LON_MIN, LON_MAX,
    INVALID_LAT, INVALID_LON,
)
from models import AISRow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expected CSV column indices (based on dataset header):
# Timestamp,Type of mobile,MMSI,Latitude,Longitude,Navigational status,
# ROT,SOG,COG,Heading,IMO,Callsign,Name,Ship type,Cargo type,Width,Length,
# Type of position fixing device,Draught,Destination,ETA,Data source type,
# A,B,C,D
# ---------------------------------------------------------------------------
COL_TIMESTAMP   = 0
COL_MOBILE_TYPE = 1
COL_MMSI        = 2
COL_LAT         = 3
COL_LON         = 4
COL_SOG         = 7
COL_DRAUGHT     = 18

DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"


def _parse_timestamp(raw: str) -> Optional[float]:
    """Parse DD/MM/YYYY HH:MM:SS into a Unix timestamp (float seconds)."""
    try:
        dt = datetime.strptime(raw.strip(), DATETIME_FORMAT)
        # Treat as UTC (AIS timestamps are always UTC)
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def _parse_float(raw: str) -> Optional[float]:
    """Return float or None for empty / invalid strings."""
    raw = raw.strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _is_valid_mmsi(mmsi: str) -> bool:
    """Return True if the MMSI looks like a real vessel identifier."""
    mmsi = mmsi.strip()
    # Must be exactly 9 digits
    if len(mmsi) < MMSI_MIN_LENGTH or not mmsi.isdigit():
        return False
    # Known default / unconfigured values
    if mmsi in INVALID_MMSI_EXACT:
        return False
    # All-same-digit MMSIs (e.g. 222222222) are unconfigured transponders
    if len(set(mmsi)) == 1:
        return False
    return True


def _is_valid_position(lat: float, lon: float) -> bool:
    """Return True if the coordinates are within physical bounds and not the
    AIS 'not available' sentinel (91.0, 0.0)."""
    if lat == INVALID_LAT and lon == INVALID_LON:
        return False
    return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX


def stream_rows(filepath: str) -> Generator[AISRow, None, None]:
    """
    Generator that yields one validated AISRow at a time from *filepath*.

    Rows are filtered out if:
      - Type of mobile is not in VALID_MOBILE_TYPES (Class A only)
      - MMSI is invalid / a known default
      - Coordinates are out of range or the AIS 'not available' sentinel
      - Timestamp cannot be parsed

    SOG defaults to 0.0 if missing; draught defaults to None.
    """
    rows_read = 0
    rows_yielded = 0

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)  # skip header row

        if header is None:
            logger.warning("Empty file: %s", filepath)
            return

        for row in reader:
            rows_read += 1

            # Guard against malformed rows
            if len(row) <= COL_DRAUGHT:
                continue

            # --- Mobile type filter ---
            mobile_type = row[COL_MOBILE_TYPE].strip()
            if mobile_type not in VALID_MOBILE_TYPES:
                continue

            # --- MMSI filter ---
            mmsi = row[COL_MMSI].strip()
            if not _is_valid_mmsi(mmsi):
                continue

            # --- Timestamp ---
            ts = _parse_timestamp(row[COL_TIMESTAMP])
            if ts is None:
                continue

            # --- Coordinates ---
            lat = _parse_float(row[COL_LAT])
            lon = _parse_float(row[COL_LON])
            if lat is None or lon is None:
                continue
            if not _is_valid_position(lat, lon):
                continue

            # --- SOG (Speed Over Ground) ---
            sog_raw = _parse_float(row[COL_SOG])
            sog = sog_raw if sog_raw is not None else 0.0

            # --- Draught (optional) ---
            draught = _parse_float(row[COL_DRAUGHT])

            rows_yielded += 1
            yield AISRow(
                timestamp=ts,
                mmsi=mmsi,
                lat=lat,
                lon=lon,
                sog=sog,
                draught=draught,
            )

    logger.info(
        "Parsed %s: %d rows read, %d yielded after filtering",
        filepath, rows_read, rows_yielded,
    )
