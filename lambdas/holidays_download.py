"""
AWS Lambda: one-time download of Canadian public holidays (Nager.Date API).

Ingestion only - no filtering. Stores the API answer unchanged:
    raw/holidays/year=<YYYY>/holidays_CA.json

Run manually once (holidays do not change, so no schedule is needed).
Filtering to Québec holidays happens later, in the processing step.

Environment variables:
    BUCKET  S3 bucket name (required)
    YEARS   Comma-separated years (default "2026,2027")
"""
import json
import logging
import os
import urllib.request

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/CA"
HTTP_TIMEOUT_S = 20


def fetch_holidays(year):
    """Return the raw API answer (bytes) for `year`."""
    request = urllib.request.Request(
        API_URL.format(year=year),
        headers={"User-Agent": "HSLU-DWDL-student-project"},
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
        return response.read()


def ingest(s3, bucket, years):
    """Store the raw JSON for each year. Returns holidays found per year."""
    results = {}
    for year in years:
        body = fetch_holidays(year)
        holidays = json.loads(body)  # only to check the answer is valid
        if not holidays:
            raise ValueError(f"No holidays returned for {year}")
        s3.put_object(
            Bucket=bucket,
            Key=f"raw/holidays/year={year}/holidays_CA.json",
            Body=body,
            ContentType="application/json",
        )
        results[str(year)] = f"downloaded ({len(holidays)} holidays)"
        logger.info("%s: %s", year, results[str(year)])
    return results


def lambda_handler(event, context):
    """Entry point: run once manually with the test event {}."""
    import boto3  # imported here so the logic can be tested without boto3

    years = [int(y) for y in os.environ.get("YEARS", "2026,2027").split(",")]
    return ingest(boto3.client("s3"), os.environ["BUCKET"], years)
