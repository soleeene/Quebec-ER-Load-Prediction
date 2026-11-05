"""
AWS Lambda: weekly download of the INSPQ respiratory-virus report (PDF).

Ingestion only - no parsing. Each run:
    1. Builds the URLs of the last LOOKBACK weekly reports.
    2. Skips reports already stored in S3 (idempotent, safe to re-run).
    3. Stores new PDFs unchanged in raw/inspq/report=<YYYY-WW>/<YYYY-WW>.pdf

A report not published yet (HTTP 404) is skipped and picked up next run.
To load older reports once, run with a larger LOOKBACK (e.g. 12).

Environment variables:
    BUCKET        S3 bucket name (required)
    URL_TEMPLATE  Report URL with {year} and {week}, e.g.
                  https://www.inspq.qc.ca/.../{year}-{week:02d}.pdf
    LOOKBACK      Number of past weeks to check (default 3)
"""
import logging
import os
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HTTP_TIMEOUT_S = 30


def surveillance_week(saturday):
    """Return (year, week) of the Sunday-Saturday week ending on `saturday`.

    Week 1 is the first week with at least four days in the new year,
    i.e. the week containing the year's first Wednesday.
    """
    year = (saturday - timedelta(days=3)).year
    jan1 = date(year, 1, 1)
    first_wednesday = jan1 + timedelta(days=(2 - jan1.weekday()) % 7)
    week1_start = first_wednesday - timedelta(days=3)
    week_start = saturday - timedelta(days=6)
    return year, (week_start - week1_start).days // 7 + 1


def recent_report_weeks(today, lookback):
    """Yield (year, week) of the last `lookback` full weeks, newest first."""
    # Most recent Saturday strictly before today
    last_saturday = today - timedelta(days=(today.weekday() - 5) % 7 or 7)
    for i in range(lookback):
        yield surveillance_week(last_saturday - timedelta(weeks=i))


def download_pdf(url):
    """Return the PDF bytes, or None if not published yet (HTTP 404)."""
    request = urllib.request.Request(
        url, headers={"User-Agent": "HSLU-DWDL-student-project"}
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as resp:
            content = resp.read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise
    if not content.startswith(b"%PDF"):
        raise ValueError(f"Response from {url} is not a PDF")
    return content


def s3_key_exists(s3, bucket, key):
    """True if the object already exists in S3."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def ingest_report(s3, bucket, url, report_id):
    """Download one report into the raw zone. Returns a status string."""
    key = f"raw/inspq/report={report_id}/{report_id}.pdf"
    if s3_key_exists(s3, bucket, key):
        return "already ingested"
    pdf_bytes = download_pdf(url)
    if pdf_bytes is None:
        return "not published yet"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        Metadata={"source-url": url},
    )
    return f"downloaded ({len(pdf_bytes) // 1024} KB)"


def lambda_handler(event, context):
    """Entry point, triggered weekly by EventBridge (or manually)."""
    import boto3  # imported here so the logic can be tested without boto3

    bucket = os.environ["BUCKET"]
    template = os.environ["URL_TEMPLATE"]
    lookback = int(os.environ.get("LOOKBACK", "3"))
    s3 = boto3.client("s3")

    today = datetime.now(timezone.utc).date()
    results = {}
    for year, week in recent_report_weeks(today, lookback):
        report_id = f"{year}-{week:02d}"
        url = template.format(year=year, week=week)
        results[report_id] = ingest_report(s3, bucket, url, report_id)
        logger.info("%s: %s", report_id, results[report_id])
    return results
