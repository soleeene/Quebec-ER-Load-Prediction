"""
Connector for weather data — Open-Meteo (https://open-meteo.com/)
Free for non-commercial use, no API key. Two endpoints:
 - forecast API: next ~16 days
 - archive API: historical reanalysis (ERA5), back to 1940
"""
from __future__ import annotations
import requests

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "snowfall",
    "windspeed_10m",
    "relative_humidity_2m",
    "weathercode",
]


def get_forecast(lat: float, lon: float, days: int = 7) -> dict:
    """Hourly forecast for the next `days` days at (lat, lon)."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "forecast_days": days,
        "timezone": "America/Toronto",
    }
    r = requests.get(FORECAST_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def get_historical(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """
    Historical hourly weather between start_date and end_date (YYYY-MM-DD).
    Useful for building the training set that lines up with ER history.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "America/Toronto",
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


if __name__ == "__main__":
    # Coordinates for Saint-Hyacinthe (Honoré-Mercier hospital) as an example
    lat, lon = 45.6295, -72.9569
    fc = get_forecast(lat, lon, days=3)
    print("Forecast hours returned:", len(fc["hourly"]["time"]))

    hist = get_historical(lat, lon, "2026-08-10", "2026-09-01")
    print("Historical hours returned:", len(hist["hourly"]["time"]))
