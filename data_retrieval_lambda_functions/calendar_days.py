"""
Connector for day-of-week / holiday data.

Recommended approach: the `holidays` Python package. Quebec statutory
holidays are a fixed, well-known calendar — there's no real benefit to
paying an API round-trip (and a rate limit) for something you can compute
offline, for free, forever. `pip install holidays`.

An AbstractAPI-based version is included below (commented out) in case you
specifically want a hosted API — 1000 req/month free tier is comfortably
enough for one lookup per day.
"""
from __future__ import annotations
from datetime import date
import holidays as holidays_lib

_qc_holidays = holidays_lib.Canada(subdiv="QC")


def day_features(d: date) -> dict:
    """Local, offline: day-of-week + is_holiday + holiday name, for any date."""
    return {
        "date": d.isoformat(),
        "day_of_week": d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
        "is_holiday": d in _qc_holidays,
        "holiday_name": _qc_holidays.get(d),
    }


# ---------------------------------------------------------------------------
# Optional: AbstractAPI Holidays connector (https://www.abstractapi.com/api/holidays-api)
# Free tier: 1000 requests/month. Requires an API key.
# ---------------------------------------------------------------------------
# import requests
#
# ABSTRACT_API_KEY = "YOUR_KEY_HERE"
# ABSTRACT_URL = "https://holidays.abstractapi.com/v1/"
#
# def get_holiday_abstractapi(d: date, country: str = "CA") -> list[dict]:
#     params = {
#         "api_key": ABSTRACT_API_KEY,
#         "country": country,
#         "year": d.year,
#         "month": d.month,
#         "day": d.day,
#     }
#     r = requests.get(ABSTRACT_URL, params=params, timeout=15)
#     r.raise_for_status()
#     return r.json()  # empty list if not a holiday


if __name__ == "__main__":
    for d in [date(2026, 1, 1), date(2026, 9, 25), date(2026, 12, 25)]:
        print(day_features(d))
