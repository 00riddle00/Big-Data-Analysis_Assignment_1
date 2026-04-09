"""
visualize.py - Generate an interactive Folium map for the top-5 DFSI vessels.

Usage
-----
  python visualize.py

Reads analysis/top5_vessels.csv and analysis/gap_events.csv,
analysis/cloning_events.csv to draw trajectories on a map.
Output: analysis/top5_map.html
"""

import csv
import logging
import os
from typing import List, Dict

logger = logging.getLogger(__name__)

try:
    import folium
    HAS_FOLIUM = True
except ImportError:
    HAS_FOLIUM = False
    logger.warning("folium not installed.  Run: pip install folium")

from config import ANALYSIS_DIR, TOP_N


def _load_top5() -> List[dict]:
    path = os.path.join(ANALYSIS_DIR, "top5_vessels.csv")
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            rows.append(rec)
    return rows


def _load_gap_events_by_mmsi() -> Dict[str, list]:
    path = os.path.join(ANALYSIS_DIR, "gap_events.csv")
    result: Dict[str, list] = {}
    if not os.path.exists(path):
        return result
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for rec in reader:
            mmsi = rec["mmsi"]
            if mmsi not in result:
                result[mmsi] = []
            result[mmsi].append(rec)
    return result


def generate_map() -> None:
    if not HAS_FOLIUM:
        print("Install folium to generate the map:  pip install folium")
        return

    top5 = _load_top5()
    if not top5:
        logger.warning("No top-5 vessels found.  Run the pipeline first.")
        return

    gap_events = _load_gap_events_by_mmsi()

    # Centre map on the mean position of top-5 vessels
    try:
        centre_lat = sum(float(v["map_lat"]) for v in top5) / len(top5)
        centre_lon = sum(float(v["map_lon"]) for v in top5) / len(top5)
    except (ValueError, ZeroDivisionError):
        centre_lat, centre_lon = 56.0, 10.0  # North Sea / Baltic default

    fmap = folium.Map(location=[centre_lat, centre_lon], zoom_start=6)

    # Colour palette for vessels
    colours = ["red", "blue", "green", "purple", "orange"]

    for rank, vessel in enumerate(top5):
        mmsi   = vessel["mmsi"]
        colour = colours[rank % len(colours)]
        lat    = float(vessel["map_lat"]) if vessel["map_lat"] else None
        lon    = float(vessel["map_lon"]) if vessel["map_lon"] else None

        popup_html = (
            f"<b>Rank #{rank + 1}</b><br>"
            f"MMSI: {mmsi}<br>"
            f"DFSI: {vessel['dfsi']}<br>"
            f"Anomalies: {vessel['anomaly_flags']}"
        )

        # Draw gap event trajectories (disappearance → reappearance)
        for gap in gap_events.get(mmsi, []):
            try:
                s_lat = float(gap["start_lat"])
                s_lon = float(gap["start_lon"])
                e_lat = float(gap["end_lat"])
                e_lon = float(gap["end_lon"])
                # Dashed line for the gap (ship was dark here)
                folium.PolyLine(
                    locations=[[s_lat, s_lon], [e_lat, e_lon]],
                    color=colour,
                    weight=2,
                    dash_array="10",
                    tooltip=(
                        f"MMSI {mmsi} – Gap {float(gap['gap_hours']):.1f} h "
                        f"| implied {float(gap['implied_speed_knots']):.1f} kt"
                    ),
                ).add_to(fmap)
                # Mark disappearance
                folium.CircleMarker(
                    location=[s_lat, s_lon],
                    radius=5,
                    color=colour,
                    fill=True,
                    fill_color=colour,
                    tooltip=f"MMSI {mmsi} – went dark",
                ).add_to(fmap)
                # Mark reappearance
                folium.CircleMarker(
                    location=[e_lat, e_lon],
                    radius=5,
                    color="black",
                    fill=True,
                    fill_color=colour,
                    tooltip=f"MMSI {mmsi} – reappeared",
                ).add_to(fmap)
            except (ValueError, KeyError):
                continue

        # Main marker for the vessel
        if lat and lon:
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=colour, icon="ship", prefix="fa"),
                tooltip=f"#{rank + 1} MMSI {mmsi} DFSI={vessel['dfsi']}",
            ).add_to(fmap)

    # Legend
    legend_html = """
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 10px; border-radius: 6px;
                border: 2px solid grey; font-size: 13px;">
    <b>Top-5 Shadow Fleet Suspects</b><br>
    &#9135;&#9135; Dashed line = AIS blackout trajectory<br>
    ● Disappearance point<br>
    ◉ Reappearance point
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))

    out_path = os.path.join(ANALYSIS_DIR, "top5_map.html")
    fmap.save(out_path)
    logger.info("Map saved: %s", out_path)
    print(f"Interactive map saved to: {out_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_map()
