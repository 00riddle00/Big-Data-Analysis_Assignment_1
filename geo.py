"""
geo.py - Geographical utility functions.

All distance calculations use the Haversine formula which is accurate
enough for the short distances involved in AIS anomaly detection.
"""

import math
from typing import Tuple

# Earth's mean radius in nautical miles and metres
EARTH_RADIUS_NM = 3440.065
EARTH_RADIUS_M  = 6_371_000.0


def haversine_nm(lat1: float, lon1: float,
                 lat2: float, lon2: float) -> float:
    """
    Return the great-circle distance between two points in nautical miles.

    Parameters
    ----------
    lat1, lon1 : float  – first point (decimal degrees)
    lat2, lon2 : float  – second point (decimal degrees)
    """
    # Convert degrees to radians
    phi1   = math.radians(lat1)
    phi2   = math.radians(lat2)
    dphi   = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_NM * c


def haversine_metres(lat1: float, lon1: float,
                     lat2: float, lon2: float) -> float:
    """Return the great-circle distance between two points in metres."""
    phi1    = math.radians(lat1)
    phi2    = math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_M * c


def implied_speed_knots(lat1: float, lon1: float,
                        lat2: float, lon2: float,
                        gap_seconds: float) -> float:
    """
    Calculate the implied speed (knots) needed to travel between two
    coordinates in the given time.

    Returns 0.0 if gap_seconds <= 0.
    """
    if gap_seconds <= 0:
        return 0.0
    distance_nm = haversine_nm(lat1, lon1, lat2, lon2)
    gap_hours   = gap_seconds / 3600.0
    return distance_nm / gap_hours


def bounding_box(lat: float, lon: float,
                 half_deg: float) -> Tuple[float, float, float, float]:
    """
    Return (lat_min, lat_max, lon_min, lon_max) for a bounding box of
    ±half_deg decimal degrees around (lat, lon).

    Used as a cheap first-pass filter before the exact Haversine check.
    """
    return (
        lat - half_deg,
        lat + half_deg,
        lon - half_deg,
        lon + half_deg,
    )


def within_bbox(lat: float, lon: float,
                lat_min: float, lat_max: float,
                lon_min: float, lon_max: float) -> bool:
    """Return True if (lat, lon) falls inside the bounding box."""
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max
