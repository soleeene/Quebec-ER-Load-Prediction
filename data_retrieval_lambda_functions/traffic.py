"""
Connector for traffic data — TomTom Traffic API.
Needs a free API key in .env as TOMTOM_API_KEY (never commit it).

Two endpoints:
 - Traffic Incidents (v5): all incidents inside a bounding box
 - Traffic Flow (v4): current vs free-flow speed for the road at one point
"""
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["TOMTOM_API_KEY"]

INCIDENTS_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"
FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

INCIDENT_FIELDS = (
    "{incidents{type,geometry{type,coordinates},"
    "properties{iconCategory,magnitudeOfDelay,events{description},"
    "startTime,endTime,delay}}}"
)

# Bounding boxes: minLon,minLat,maxLon,maxLat
BBOXES = {
    "montreal": "-73.75,45.40,-73.47,45.70",
    "quebec_city": "-71.40,46.73,-71.13,46.90",
}

# Road points near major hospitals (approximate coordinates, adjust if needed)
FLOW_POINTS = {
    "montreal_chum": (45.5112, -73.5571),
    "montreal_jewish_general": (45.4972, -73.6303),
    "quebec_chul": (46.7745, -71.2830),
    "quebec_enfant_jesus": (46.8336, -71.2268),
}

# TomTom iconCategory codes (subset)
CATEGORY_NAMES = {
    1: "accident", 6: "jam", 7: "lane closed",
    8: "road closed", 9: "roadworks", 14: "broken down vehicle",
}


def get_incidents(bbox: str) -> dict:
    """All current incidents inside a bounding box."""
    params = {
        "key": API_KEY,
        "bbox": bbox,
        "fields": INCIDENT_FIELDS,
        "language": "en-GB",
    }
    r = requests.get(INCIDENTS_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_flow(lat: float, lon: float) -> dict:
    """Current and free-flow speed for the road segment closest to (lat, lon)."""
    params = {"key": API_KEY, "point": f"{lat},{lon}"}
    r = requests.get(FLOW_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def summarize_incidents(data: dict) -> dict:
    """Count incidents per category, for quick inspection."""
    counts = Counter(
        i["properties"]["iconCategory"] for i in data.get("incidents", [])
    )
    return {CATEGORY_NAMES.get(k, f"cat_{k}"): v for k, v in counts.items()}


def save_raw(name: str, data: dict) -> Path:
    """Save a raw snapshot locally (same layout as the S3 raw zone)."""
    now = datetime.now(timezone.utc)
    folder = Path("data/raw/traffic") / f"date={now:%Y-%m-%d}" / f"hour={now:%H}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


if __name__ == "__main__":
    for city, bbox in BBOXES.items():
        incidents = get_incidents(bbox)
        n = len(incidents.get("incidents", []))
        print(f"{city}: {n} incidents -> {summarize_incidents(incidents)}")
        print("  saved:", save_raw(f"incidents_{city}", incidents))

    for name, (lat, lon) in FLOW_POINTS.items():
        flow = get_flow(lat, lon)["flowSegmentData"]
        print(
            f"{name}: current {flow['currentSpeed']} km/h, "
            f"free-flow {flow['freeFlowSpeed']} km/h, "
            f"closed={flow['roadClosure']}"
        )
        save_raw(f"flow_{name}", {"point": name, "data": flow})
