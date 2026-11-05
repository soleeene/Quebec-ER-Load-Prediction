"""
Lambda: hourly weather snapshot from Open-Meteo -> S3 raw zone.
Same variables as API-access/weather.py. No API key needed.
Env var: BUCKET. Saves past 24 h + next 48 h of hourly weather per city.
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ["BUCKET"]
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "montreal": (45.5017, -73.5673),
    "quebec_city": (46.8139, -71.2080),
}

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "windspeed_10m",
    "relative_humidity_2m",
    "weathercode",
]

s3 = boto3.client("s3")


def get_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "past_days": 1,
        "forecast_days": 2,
        "timezone": "America/Toronto",
    }
    url = FORECAST_URL + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read())


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    prefix = f"raw/weather/date={now:%Y-%m-%d}/hour={now:%H}"
    saved = []

    for city, (lat, lon) in CITIES.items():
        data = get_weather(lat, lon)
        key = f"{prefix}/weather_{city}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )
        saved.append(key)

    return {"statusCode": 200, "saved": saved}
