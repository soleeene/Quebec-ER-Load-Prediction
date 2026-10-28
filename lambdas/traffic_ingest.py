"""
Lambda: hourly TomTom traffic snapshot -> S3 raw zone.
Env vars: TOMTOM_API_KEY, BUCKET.
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

API_KEY = os.environ["TOMTOM_API_KEY"]
BUCKET = os.environ["BUCKET"]

INCIDENTS_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"
FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
FIELDS = (
    "{incidents{type,geometry{type,coordinates},"
    "properties{iconCategory,magnitudeOfDelay,events{description},"
    "startTime,endTime,delay}}}"
)

BBOXES = {
    "montreal": "-73.75,45.40,-73.47,45.70",
    "quebec_city": "-71.40,46.73,-71.13,46.90",
}
FLOW_POINTS = {
    "montreal_chum": (45.5112, -73.5571),
    "montreal_jewish_general": (45.4972, -73.6303),
    "quebec_chul": (46.7745, -71.2830),
    "quebec_enfant_jesus": (46.8336, -71.2268),
}

s3 = boto3.client("s3")


def get_json(url, params):
    full_url = url + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(full_url, timeout=20) as r:
        return json.loads(r.read())


def save(prefix, name, data):
    key = f"{prefix}/{name}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    prefix = f"raw/traffic/date={now:%Y-%m-%d}/hour={now:%H}"
    saved = []

    for city, bbox in BBOXES.items():
        data = get_json(INCIDENTS_URL, {
            "key": API_KEY, "bbox": bbox,
            "fields": FIELDS, "language": "en-GB",
        })
        saved.append(save(prefix, f"incidents_{city}", data))

    for name, (lat, lon) in FLOW_POINTS.items():
        data = get_json(FLOW_URL, {"key": API_KEY, "point": f"{lat},{lon}"})
        saved.append(save(prefix, f"flow_{name}",
                          {"point": name, "data": data["flowSegmentData"]}))

    return {"statusCode": 200, "saved": saved}
