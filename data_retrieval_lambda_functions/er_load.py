"""
lambda_function.py — Quebec ER load connector (sante.handled.tools) for AWS Lambda.

Handler: lambda_function.lambda_handler
Runtime: Python 3.12+ (stdlib + boto3 only, no packaging needed)
Trigger: EventBridge schedule `rate(1 hour)`

Env vars:
  S3_BUCKET        (required)
  S3_PREFIX        default "er-load"
  HISTORY_HOUR_UTC default "3"   -> full history refresh once a day at this UTC hour
  FORCE_HISTORY    default "0"   -> "1" forces a history refresh on every run
  MAX_WORKERS      default "10"

Data source: MSSS (Quebec health ministry) open data, CC-BY 4.0 — cite MSSS if published.

S3 layout:
  {prefix}/facilities/facilities.json
  {prefix}/now/year=YYYY/month=MM/day=DD/hour=HH/now.jsonl.gz
  {prefix}/history/facility_id={id}/history.jsonl.gz   (full history, overwritten)
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

BASE_URL = "https://sante.handled.tools/api"
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "er-load").strip("/")
HISTORY_HOUR_UTC = int(os.environ.get("HISTORY_HOUR_UTC", "3"))
FORCE_HISTORY = os.environ.get("FORCE_HISTORY", "0") == "1"
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))

log = logging.getLogger()
log.setLevel(logging.INFO)
s3 = boto3.client("s3")


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _get_json(path: str, timeout: int = 30, retries: int = 4) -> dict:
    url = f"{BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"Accept": "application/json", "User-Agent": "er-load-lambda/1.0"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
            last_exc = e
            if isinstance(e, urllib.error.HTTPError) and e.code < 500 and e.code != 429:
                break  # non-retryable client error
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"GET {url} failed: {last_exc}")


def get_facilities() -> list[dict]:
    return _get_json("/facilities")["rows"]


def get_current_status() -> list[dict]:
    return _get_json("/now")["rows"]


def get_facility_history(facility_id: str) -> list[dict]:
    return _get_json(f"/facility/{facility_id}/history", timeout=60)["rows"]


# --------------------------------------------------------------------------- #
# S3 helpers
# --------------------------------------------------------------------------- #
def _key(*parts: str) -> str:
    return "/".join(p for p in (S3_PREFIX, *parts) if p)


def _put_json(key: str, obj) -> None:
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def _put_jsonl_gz(key: str, rows: list[dict]) -> None:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for row in rows:
            gz.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )


def _exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #
def _history_key(facility_id: str) -> str:
    return _key("history", f"facility_id={facility_id}", "history.jsonl.gz")


def _fetch_and_store_history(facility_id: str) -> int:
    rows = get_facility_history(facility_id)
    for r in rows:
        r.setdefault("facility_id", facility_id)
    _put_jsonl_gz(_history_key(facility_id), rows)
    return len(rows)


def lambda_handler(event, context):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET env var is required")

    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    summary: dict = {"fetched_at": fetched_at}

    # 1. Facility directory
    facilities = get_facilities()
    _put_json(_key("facilities", "facilities.json"), facilities)
    summary["facilities"] = len(facilities)

    # 2. Current snapshot for all ERs
    current = get_current_status()
    for row in current:
        row["_fetched_at"] = fetched_at
    _put_jsonl_gz(
        _key(
            "now",
            f"year={now:%Y}",
            f"month={now:%m}",
            f"day={now:%d}",
            f"hour={now:%H}",
            "now.jsonl.gz",
        ),
        current,
    )
    summary["now_rows"] = len(current)

    # 3. Full history: daily refresh + backfill for any facility never stored
    ids = [str(f["id"]) for f in facilities if f.get("id") is not None]
    refresh_all = FORCE_HISTORY or now.hour == HISTORY_HOUR_UTC
    todo = ids if refresh_all else [i for i in ids if not _exists(_history_key(i))]

    ok, failed, total_rows = 0, [], 0
    if todo:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_fetch_and_store_history, i): i for i in todo}
            for fut in as_completed(futures):
                fid = futures[fut]
                try:
                    total_rows += fut.result()
                    ok += 1
                except Exception as e:  # keep going on per-facility failures
                    log.error("History failed for %s: %s", fid, e)
                    failed.append(fid)

    summary.update(history_ok=ok, history_failed=failed, history_rows=total_rows)
    log.info(json.dumps(summary))
    return summary


if __name__ == "__main__":
    print(lambda_handler({}, None))
