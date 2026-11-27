"""
Lambda: traffic clean layer (silver).

Reads raw TomTom snapshots from
    raw/traffic/date=YYYY-MM-DD/hour=HH/
and writes one gzipped JSON-lines file per table and hour:
    clean/traffic/incidents/date=YYYY-MM-DD/hour=HH/incidents.jsonl.gz
    clean/traffic/flow/date=YYYY-MM-DD/hour=HH/flow.jsonl.gz

No hospital linking here: incidents keep their coordinates, and the
spatial features (within 2/5/10 km, nearest, region) are built in gold.

Invocation modes (event):
  - Success destination of traffic-ingest:
        {"responsePayload": {"saved": ["raw/traffic/...json", ...]}, ...}
  - Backfill one prefix (e.g. one day):
        {"prefix": "raw/traffic/date=2026-11-10/"}
  - Empty event {}: processes the current UTC hour (for a schedule at :10)

Env var: BUCKET
Re-running an hour overwrites its clean files (idempotent).
"""
import gzip
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import boto3

BUCKET = os.environ["BUCKET"]
RAW_PREFIX = "raw/traffic/"
CLEAN_PREFIX = "clean/traffic/"

# TomTom Incident Details v5 iconCategory codes
ICON_CATEGORY = {
    0: "unknown",
    1: "accident",
    2: "fog",
    3: "dangerous_conditions",
    4: "rain",
    5: "ice",
    6: "jam",
    7: "lane_closed",
    8: "road_closed",
    9: "road_works",
    10: "wind",
    11: "flooding",
    14: "broken_down_vehicle",
}
MAGNITUDE = {0: "unknown", 1: "minor", 2: "moderate", 3: "major", 4: "undefined"}

# Short-lived events that can plausibly affect ED load hour by hour
EVENT_CATEGORIES = {"accident", "jam", "broken_down_vehicle"}
LONG_TERM_HOURS = 72  # incidents older than this are treated as long-term

# Fill in with facility_id values from Solene's facility directory
FLOW_POINT_FACILITY = {
    "montreal_chum": None,
    "montreal_jewish_general": None,
    "quebec_chul": None,
    "quebec_enfant_jesus": None,
}

HOUR_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")

s3 = boto3.client("s3")


# ---------- S3 helpers ----------

def list_json_keys(prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_json(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def write_jsonl_gz(key, rows):
    lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=gzip.compress(lines.encode("utf-8")),
        ContentType="application/gzip",
    )


# ---------- parsing helpers ----------

def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def representative_point(geometry):
    """Mean of the geometry's coordinates. GeoJSON order is [lon, lat]."""
    coords = (geometry or {}).get("coordinates")
    if not coords:
        return None, None
    points = [coords] if isinstance(coords[0], (int, float)) else coords
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return round(lat, 6), round(lon, 6)


# ---------- row builders ----------

def incident_rows(data, city, snapshot_utc, source_key):
    snapshot = datetime.fromisoformat(snapshot_utc)
    rows = []
    for inc in data.get("incidents", []):
        p = inc.get("properties", {}) or {}
        category = ICON_CATEGORY.get(p.get("iconCategory"), "other")
        start = parse_ts(p.get("startTime"))
        end = parse_ts(p.get("endTime"))
        age_hours = round((snapshot - start).total_seconds() / 3600, 1) if start else None
        is_long_term = age_hours is not None and age_hours > LONG_TERM_HOURS
        lat, lon = representative_point(inc.get("geometry"))
        descriptions = [e.get("description") for e in (p.get("events") or []) if e.get("description")]

        # Raw data has no TomTom id, so build a stable key to follow an incident across hours
        incident_key = hashlib.md5(
            f"{city}|{category}|{p.get('startTime')}|{lat}|{lon}".encode("utf-8")
        ).hexdigest()[:16]

        rows.append({
            "snapshot_utc": snapshot_utc,
            "city": city,
            "incident_key": incident_key,
            "category": category,
            "icon_category": p.get("iconCategory"),
            "magnitude": MAGNITUDE.get(p.get("magnitudeOfDelay"), "unknown"),
            "delay_s": p.get("delay"),
            "description": "; ".join(descriptions) or None,
            "start_utc": start.isoformat() if start else None,
            "end_utc": end.isoformat() if end else None,
            "age_hours": age_hours,
            "is_long_term": is_long_term,
            "keep_for_analysis": category in EVENT_CATEGORIES and not is_long_term,
            "lat": lat,
            "lon": lon,
            "geometry_type": (inc.get("geometry") or {}).get("type"),
            "source_file": source_key,
        })
    return rows


def flow_row(obj, point_name, snapshot_utc, source_key):
    # AWS files: {"point": ..., "data": {...}}; local files may hold {"flowSegmentData": {...}}
    d = obj.get("data") or obj.get("flowSegmentData") or {}
    current = d.get("currentSpeed")
    free = d.get("freeFlowSpeed")
    return {
        "snapshot_utc": snapshot_utc,
        "point": point_name,
        "facility_id": FLOW_POINT_FACILITY.get(point_name),
        "current_speed_kmh": current,
        "free_flow_speed_kmh": free,
        "speed_ratio": round(current / free, 3) if current is not None and free else None,
        "current_travel_time_s": d.get("currentTravelTime"),
        "free_flow_travel_time_s": d.get("freeFlowTravelTime"),
        "road_closure": d.get("roadClosure"),
        "confidence": d.get("confidence"),
        "frc": d.get("frc"),
        "source_file": source_key,
    }


# ---------- per-hour processing ----------

def process_hour(date, hour, keys):
    snapshot_utc = f"{date}T{hour}:00:00+00:00"
    incidents, flows = [], []

    for key in keys:
        name = key.rsplit("/", 1)[-1][: -len(".json")]
        data = read_json(key)
        if name.startswith("incidents_"):
            incidents += incident_rows(data, name[len("incidents_"):], snapshot_utc, key)
        elif name.startswith("flow_"):
            flows.append(flow_row(data, name[len("flow_"):], snapshot_utc, key))

    part = f"date={date}/hour={hour}"
    if incidents:
        write_jsonl_gz(f"{CLEAN_PREFIX}incidents/{part}/incidents.jsonl.gz", incidents)
    if flows:
        write_jsonl_gz(f"{CLEAN_PREFIX}flow/{part}/flow.jsonl.gz", flows)
    return len(incidents), len(flows)


def lambda_handler(event, context):
    event = event or {}

    if "responsePayload" in event:
        keys = (event.get("responsePayload") or {}).get("saved", [])
    elif "prefix" in event:
        keys = list_json_keys(event["prefix"])
    else:
        now = datetime.now(timezone.utc)
        keys = list_json_keys(f"{RAW_PREFIX}date={now:%Y-%m-%d}/hour={now:%H}/")

    by_hour = defaultdict(list)
    for key in keys:
        match = HOUR_RE.search(key)
        if match and key.endswith(".json"):
            by_hour[match.groups()].append(key)

    summary = []
    for (date, hour), hour_keys in sorted(by_hour.items()):
        n_incidents, n_flow = process_hour(date, hour, hour_keys)
        summary.append({"hour": f"{date}T{hour}", "incidents": n_incidents, "flow": n_flow})

    return {
        "statusCode": 200,
        "hours_processed": len(summary),
        "last_hours": summary[-5:],
    }"""
Lambda: traffic clean layer (silver).

Reads raw TomTom snapshots from
    raw/traffic/date=YYYY-MM-DD/hour=HH/
and writes one gzipped JSON-lines file per table and hour:
    clean/traffic/incidents/date=YYYY-MM-DD/hour=HH/incidents.jsonl.gz
    clean/traffic/flow/date=YYYY-MM-DD/hour=HH/flow.jsonl.gz

No hospital linking here: incidents keep their coordinates, and the
spatial features (within 2/5/10 km, nearest, region) are built in gold.

Invocation modes (event):
  - Success destination of traffic-ingest:
        {"responsePayload": {"saved": ["raw/traffic/...json", ...]}, ...}
  - Backfill one prefix (e.g. one day):
        {"prefix": "raw/traffic/date=2026-11-10/"}
  - Empty event {}: processes the current UTC hour (for a schedule at :10)

Env var: BUCKET
Re-running an hour overwrites its clean files (idempotent).
"""
import gzip
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import boto3

BUCKET = os.environ["BUCKET"]
RAW_PREFIX = "raw/traffic/"
CLEAN_PREFIX = "clean/traffic/"

# TomTom Incident Details v5 iconCategory codes
ICON_CATEGORY = {
    0: "unknown",
    1: "accident",
    2: "fog",
    3: "dangerous_conditions",
    4: "rain",
    5: "ice",
    6: "jam",
    7: "lane_closed",
    8: "road_closed",
    9: "road_works",
    10: "wind",
    11: "flooding",
    14: "broken_down_vehicle",
}
MAGNITUDE = {0: "unknown", 1: "minor", 2: "moderate", 3: "major", 4: "undefined"}

# Short-lived events that can plausibly affect ED load hour by hour
EVENT_CATEGORIES = {"accident", "jam", "broken_down_vehicle"}
LONG_TERM_HOURS = 72  # incidents older than this are treated as long-term

# Fill in with facility_id values from Solene's facility directory
FLOW_POINT_FACILITY = {
    "montreal_chum": None,
    "montreal_jewish_general": None,
    "quebec_chul": None,
    "quebec_enfant_jesus": None,
}

HOUR_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")

s3 = boto3.client("s3")


# ---------- S3 helpers ----------

def list_json_keys(prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_json(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def write_jsonl_gz(key, rows):
    lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=gzip.compress(lines.encode("utf-8")),
        ContentType="application/gzip",
    )


# ---------- parsing helpers ----------

def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def representative_point(geometry):
    """Mean of the geometry's coordinates. GeoJSON order is [lon, lat]."""
    coords = (geometry or {}).get("coordinates")
    if not coords:
        return None, None
    points = [coords] if isinstance(coords[0], (int, float)) else coords
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return round(lat, 6), round(lon, 6)


# ---------- row builders ----------

def incident_rows(data, city, snapshot_utc, source_key):
    snapshot = datetime.fromisoformat(snapshot_utc)
    rows = []
    for inc in data.get("incidents", []):
        p = inc.get("properties", {}) or {}
        category = ICON_CATEGORY.get(p.get("iconCategory"), "other")
        start = parse_ts(p.get("startTime"))
        end = parse_ts(p.get("endTime"))
        age_hours = round((snapshot - start).total_seconds() / 3600, 1) if start else None
        is_long_term = age_hours is not None and age_hours > LONG_TERM_HOURS
        lat, lon = representative_point(inc.get("geometry"))
        descriptions = [e.get("description") for e in (p.get("events") or []) if e.get("description")]

        # Raw data has no TomTom id, so build a stable key to follow an incident across hours
        incident_key = hashlib.md5(
            f"{city}|{category}|{p.get('startTime')}|{lat}|{lon}".encode("utf-8")
        ).hexdigest()[:16]

        rows.append({
            "snapshot_utc": snapshot_utc,
            "city": city,
            "incident_key": incident_key,
            "category": category,
            "icon_category": p.get("iconCategory"),
            "magnitude": MAGNITUDE.get(p.get("magnitudeOfDelay"), "unknown"),
            "delay_s": p.get("delay"),
            "description": "; ".join(descriptions) or None,
            "start_utc": start.isoformat() if start else None,
            "end_utc": end.isoformat() if end else None,
            "age_hours": age_hours,
            "is_long_term": is_long_term,
            "keep_for_analysis": category in EVENT_CATEGORIES and not is_long_term,
            "lat": lat,
            "lon": lon,
            "geometry_type": (inc.get("geometry") or {}).get("type"),
            "source_file": source_key,
        })
    return rows


def flow_row(obj, point_name, snapshot_utc, source_key):
    # AWS files: {"point": ..., "data": {...}}; local files may hold {"flowSegmentData": {...}}
    d = obj.get("data") or obj.get("flowSegmentData") or {}
    current = d.get("currentSpeed")
    free = d.get("freeFlowSpeed")
    return {
        "snapshot_utc": snapshot_utc,
        "point": point_name,
        "facility_id": FLOW_POINT_FACILITY.get(point_name),
        "current_speed_kmh": current,
        "free_flow_speed_kmh": free,
        "speed_ratio": round(current / free, 3) if current is not None and free else None,
        "current_travel_time_s": d.get("currentTravelTime"),
        "free_flow_travel_time_s": d.get("freeFlowTravelTime"),
        "road_closure": d.get("roadClosure"),
        "confidence": d.get("confidence"),
        "frc": d.get("frc"),
        "source_file": source_key,
    }


# ---------- per-hour processing ----------

def process_hour(date, hour, keys):
    snapshot_utc = f"{date}T{hour}:00:00+00:00"
    incidents, flows = [], []

    for key in keys:
        name = key.rsplit("/", 1)[-1][: -len(".json")]
        data = read_json(key)
        if name.startswith("incidents_"):
            incidents += incident_rows(data, name[len("incidents_"):], snapshot_utc, key)
        elif name.startswith("flow_"):
            flows.append(flow_row(data, name[len("flow_"):], snapshot_utc, key))

    part = f"date={date}/hour={hour}"
    if incidents:
        write_jsonl_gz(f"{CLEAN_PREFIX}incidents/{part}/incidents.jsonl.gz", incidents)
    if flows:
        write_jsonl_gz(f"{CLEAN_PREFIX}flow/{part}/flow.jsonl.gz", flows)
    return len(incidents), len(flows)


def lambda_handler(event, context):
    event = event or {}

    if "responsePayload" in event:
        keys = (event.get("responsePayload") or {}).get("saved", [])
    elif "prefix" in event:
        keys = list_json_keys(event["prefix"])
    else:
        now = datetime.now(timezone.utc)
        keys = list_json_keys(f"{RAW_PREFIX}date={now:%Y-%m-%d}/hour={now:%H}/")

    by_hour = defaultdict(list)
    for key in keys:
        match = HOUR_RE.search(key)
        if match and key.endswith(".json"):
            by_hour[match.groups()].append(key)

    summary = []
    for (date, hour), hour_keys in sorted(by_hour.items()):
        n_incidents, n_flow = process_hour(date, hour, hour_keys)
        summary.append({"hour": f"{date}T{hour}", "incidents": n_incidents, "flow": n_flow})

    return {
        "statusCode": 200,
        "hours_processed": len(summary),
        "last_hours": summary[-5:],
    }"""
Lambda: traffic clean layer (silver).

Reads raw TomTom snapshots from
    raw/traffic/date=YYYY-MM-DD/hour=HH/
and writes one gzipped JSON-lines file per table and hour:
    clean/traffic/incidents/date=YYYY-MM-DD/hour=HH/incidents.jsonl.gz
    clean/traffic/flow/date=YYYY-MM-DD/hour=HH/flow.jsonl.gz

No hospital linking here: incidents keep their coordinates, and the
spatial features (within 2/5/10 km, nearest, region) are built in gold.

Invocation modes (event):
  - Success destination of traffic-ingest:
        {"responsePayload": {"saved": ["raw/traffic/...json", ...]}, ...}
  - Backfill one prefix (e.g. one day):
        {"prefix": "raw/traffic/date=2026-11-10/"}
  - Empty event {}: processes the current UTC hour (for a schedule at :10)

Env var: BUCKET
Re-running an hour overwrites its clean files (idempotent).
"""
import gzip
import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import boto3

BUCKET = os.environ["BUCKET"]
RAW_PREFIX = "raw/traffic/"
CLEAN_PREFIX = "clean/traffic/"

# TomTom Incident Details v5 iconCategory codes
ICON_CATEGORY = {
    0: "unknown",
    1: "accident",
    2: "fog",
    3: "dangerous_conditions",
    4: "rain",
    5: "ice",
    6: "jam",
    7: "lane_closed",
    8: "road_closed",
    9: "road_works",
    10: "wind",
    11: "flooding",
    14: "broken_down_vehicle",
}
MAGNITUDE = {0: "unknown", 1: "minor", 2: "moderate", 3: "major", 4: "undefined"}

# Short-lived events that can plausibly affect ED load hour by hour
EVENT_CATEGORIES = {"accident", "jam", "broken_down_vehicle"}
LONG_TERM_HOURS = 72  # incidents older than this are treated as long-term

# Fill in with facility_id values from Solene's facility directory
FLOW_POINT_FACILITY = {
    "montreal_chum": None,
    "montreal_jewish_general": None,
    "quebec_chul": None,
    "quebec_enfant_jesus": None,
}

HOUR_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")

s3 = boto3.client("s3")


# ---------- S3 helpers ----------

def list_json_keys(prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_json(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def write_jsonl_gz(key, rows):
    lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=gzip.compress(lines.encode("utf-8")),
        ContentType="application/gzip",
    )


# ---------- parsing helpers ----------

def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def representative_point(geometry):
    """Mean of the geometry's coordinates. GeoJSON order is [lon, lat]."""
    coords = (geometry or {}).get("coordinates")
    if not coords:
        return None, None
    points = [coords] if isinstance(coords[0], (int, float)) else coords
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return round(lat, 6), round(lon, 6)


# ---------- row builders ----------

def incident_rows(data, city, snapshot_utc, source_key):
    snapshot = datetime.fromisoformat(snapshot_utc)
    rows = []
    for inc in data.get("incidents", []):
        p = inc.get("properties", {}) or {}
        category = ICON_CATEGORY.get(p.get("iconCategory"), "other")
        start = parse_ts(p.get("startTime"))
        end = parse_ts(p.get("endTime"))
        age_hours = round((snapshot - start).total_seconds() / 3600, 1) if start else None
        is_long_term = age_hours is not None and age_hours > LONG_TERM_HOURS
        lat, lon = representative_point(inc.get("geometry"))
        descriptions = [e.get("description") for e in (p.get("events") or []) if e.get("description")]

        # Raw data has no TomTom id, so build a stable key to follow an incident across hours
        incident_key = hashlib.md5(
            f"{city}|{category}|{p.get('startTime')}|{lat}|{lon}".encode("utf-8")
        ).hexdigest()[:16]

        rows.append({
            "snapshot_utc": snapshot_utc,
            "city": city,
            "incident_key": incident_key,
            "category": category,
            "icon_category": p.get("iconCategory"),
            "magnitude": MAGNITUDE.get(p.get("magnitudeOfDelay"), "unknown"),
            "delay_s": p.get("delay"),
            "description": "; ".join(descriptions) or None,
            "start_utc": start.isoformat() if start else None,
            "end_utc": end.isoformat() if end else None,
            "age_hours": age_hours,
            "is_long_term": is_long_term,
            "keep_for_analysis": category in EVENT_CATEGORIES and not is_long_term,
            "lat": lat,
            "lon": lon,
            "geometry_type": (inc.get("geometry") or {}).get("type"),
            "source_file": source_key,
        })
    return rows


def flow_row(obj, point_name, snapshot_utc, source_key):
    # AWS files: {"point": ..., "data": {...}}; local files may hold {"flowSegmentData": {...}}
    d = obj.get("data") or obj.get("flowSegmentData") or {}
    current = d.get("currentSpeed")
    free = d.get("freeFlowSpeed")
    return {
        "snapshot_utc": snapshot_utc,
        "point": point_name,
        "facility_id": FLOW_POINT_FACILITY.get(point_name),
        "current_speed_kmh": current,
        "free_flow_speed_kmh": free,
        "speed_ratio": round(current / free, 3) if current is not None and free else None,
        "current_travel_time_s": d.get("currentTravelTime"),
        "free_flow_travel_time_s": d.get("freeFlowTravelTime"),
        "road_closure": d.get("roadClosure"),
        "confidence": d.get("confidence"),
        "frc": d.get("frc"),
        "source_file": source_key,
    }


# ---------- per-hour processing ----------

def process_hour(date, hour, keys):
    snapshot_utc = f"{date}T{hour}:00:00+00:00"
    incidents, flows = [], []

    for key in keys:
        name = key.rsplit("/", 1)[-1][: -len(".json")]
        data = read_json(key)
        if name.startswith("incidents_"):
            incidents += incident_rows(data, name[len("incidents_"):], snapshot_utc, key)
        elif name.startswith("flow_"):
            flows.append(flow_row(data, name[len("flow_"):], snapshot_utc, key))

    part = f"date={date}/hour={hour}"
    if incidents:
        write_jsonl_gz(f"{CLEAN_PREFIX}incidents/{part}/incidents.jsonl.gz", incidents)
    if flows:
        write_jsonl_gz(f"{CLEAN_PREFIX}flow/{part}/flow.jsonl.gz", flows)
    return len(incidents), len(flows)


def lambda_handler(event, context):
    event = event or {}

    if "responsePayload" in event:
        keys = (event.get("responsePayload") or {}).get("saved", [])
    elif "prefix" in event:
        keys = list_json_keys(event["prefix"])
    else:
        now = datetime.now(timezone.utc)
        keys = list_json_keys(f"{RAW_PREFIX}date={now:%Y-%m-%d}/hour={now:%H}/")

    by_hour = defaultdict(list)
    for key in keys:
        match = HOUR_RE.search(key)
        if match and key.endswith(".json"):
            by_hour[match.groups()].append(key)

    summary = []
    for (date, hour), hour_keys in sorted(by_hour.items()):
        n_incidents, n_flow = process_hour(date, hour, hour_keys)
        summary.append({"hour": f"{date}T{hour}", "incidents": n_incidents, "flow": n_flow})

    return {
        "statusCode": 200,
        "hours_processed": len(summary),
        "last_hours": summary[-5:],
    }