#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: pageviews_collector.py
Description: Collects Wikipedia pageview data for the "Michael Jackson" article.
             Combines archival hourly dumps for a configurable date range in 2009
             (streamed directly from the network without ever writing to disk)
             with the modern REST Pageviews API (2015-present). The 2009 range
             now covers 20 June - 10 July 2009 (3 days before the flashpoint plus
             14 days of decay afterwards), instead of just the three peak dates,
             so an exponential decay curve can later be fitted to the attention
             falloff. Days already present in the checkpoint file from a previous
             run are skipped automatically, so re-running this script only
             downloads the days that are still missing.
Author: [Your Name]
Date: 2026-09-09
Version: 1.6
"""

import time
import random
import gzip
import urllib3
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, UTC

# --- Configuration ---------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "DigitalHumanitiesProject/1.0 "
        "(student.email@university.it; Wikipedia account: Aim.btlv; "
        "culturomics research project)"
    )
}

ARTICLE = "Michael_Jackson"
SLEEP_BETWEEN_REQUESTS = 3.0
MAX_RETRIES = 5
CHECKPOINT_FILE = Path("pageviews_2009_partial.csv")

# The 2009 collection window: a few days of pre-flashpoint baseline,
# the peak itself, and 14 days of decay afterwards.
RANGE_2009_START = "2009-06-20"
RANGE_2009_END = "2009-07-10"


def request_with_backoff(method: str, url: str, **kwargs) -> requests.Response:
    """
    Perform an HTTP request with retries for HTTP-level errors (429, 503)
    and network-level exceptions (timeouts, dropped connections).
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.request(method, url, headers=HEADERS, timeout=60, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)
            print(f"Network error ({exc}). Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            print(f"Rate limited (429). Waiting {retry_after}s...")
            time.sleep(retry_after)
            continue

        if response.status_code == 503:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)
            print(f"Server unavailable (503). Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            continue

        if response.status_code == 404:
            return response

        response.raise_for_status()
        return response

    raise RuntimeError(f"Max retries exceeded while requesting {url}")


# --- Part A: 2009 archival dumps, streamed (no disk writes) -----------------
def get_article_count_streaming(date_str: str, hour: str, article: str, lang: str = "en") -> int:
    """
    Stream a single hourly pagecounts-raw file directly from the network,
    decompress it on the fly, and return the hit count for one article.
    Retries the whole download+parse attempt up to MAX_RETRIES times on
    transient network errors, including raw urllib3-level read timeouts.
    """
    year, month = date_str[:4], date_str[4:6]
    filename = f"pagecounts-{date_str}-{hour}.gz"
    url = f"https://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{month}/{filename}"
    target_prefix = f"{lang} {article} "

    for attempt in range(MAX_RETRIES):
        response = request_with_backoff("GET", url, stream=True)

        if response.status_code == 404:
            print(f"File not found on server, skipping: {filename}")
            return 0

        try:
            with gzip.GzipFile(fileobj=response.raw) as gz:
                for raw_line in gz:
                    line = raw_line.decode("utf-8", errors="ignore")
                    if line.startswith(target_prefix):
                        time.sleep(SLEEP_BETWEEN_REQUESTS)
                        return int(line.split()[2])
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return 0

        except (OSError, gzip.BadGzipFile, requests.exceptions.RequestException,
                urllib3.exceptions.HTTPError) as exc:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)
            print(f"Stream error while reading {filename} (attempt {attempt + 1}/{MAX_RETRIES}): "
                  f"{exc}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)
            continue
        finally:
            response.close()

    print(f"Giving up on {filename} after {MAX_RETRIES} attempts. Counting as 0 for this hour.")
    return 0


def load_completed_days() -> dict:
    """
    Read the checkpoint file from a previous run, if it exists, and return
    already-collected days as {date_str: views}. Returns an empty dict if
    no checkpoint exists yet, or if it can't be read.
    """
    if not CHECKPOINT_FILE.exists():
        return {}

    try:
        df = pd.read_csv(CHECKPOINT_FILE, parse_dates=["date"])
        completed = {row["date"].strftime("%Y%m%d"): int(row["views"]) for _, row in df.iterrows()}
        if completed:
            print(f"Resuming: found {len(completed)} day(s) already collected in a previous run.")
        return completed
    except Exception as exc:
        print(f"Could not read existing checkpoint ({exc}), starting fresh.")
        return {}


def collect_2009_range() -> pd.DataFrame:
    """
    Collect hourly-summed daily pageviews for every day in the configured
    2009 range (RANGE_2009_START to RANGE_2009_END). Skips any day already
    present in the checkpoint file from a previous run. Streams every
    remaining hourly file directly, saving an updated checkpoint after
    each day so progress is never lost.
    """
    target_dates = [
        d.strftime("%Y%m%d")
        for d in pd.date_range(RANGE_2009_START, RANGE_2009_END, freq="D")
    ]
    hours = [f"{h:02d}0000" for h in range(24)]

    completed = load_completed_days()
    records = [{"date": pd.to_datetime(d, format="%Y%m%d"), "views": v} for d, v in completed.items()]

    new_dates = [d for d in target_dates if d not in completed]
    print(f"Total days in range: {len(target_dates)}. Already collected: {len(completed)}. "
          f"Remaining to download: {len(new_dates)}.")

    for date_str in target_dates:
        if date_str in completed:
            print(f"Day {date_str} already collected ({completed[date_str]} views), skipping.")
            continue

        daily_total = 0
        for hour in hours:
            daily_total += get_article_count_streaming(date_str, hour, ARTICLE)

        records.append({"date": pd.to_datetime(date_str, format="%Y%m%d"), "views": daily_total})
        pd.DataFrame(records).sort_values("date").to_csv(CHECKPOINT_FILE, index=False)  # checkpoint
        print(f"Day {date_str} done: {daily_total} views")

    return pd.DataFrame(records).sort_values("date").reset_index(drop=True)


# --- Part B: 2015-present REST Pageviews API ---------------------------------
def collect_modern_pageviews(start: str = "2015070100") -> pd.DataFrame:
    """
    Fetch daily pageviews from the official Wikimedia REST API.
    The end date is computed dynamically as "yesterday", using a
    timezone-aware UTC datetime (no longer relying on the deprecated
    datetime.utcnow()).
    """
    end = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y%m%d00")

    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia/all-access/user/{ARTICLE}/daily/{start}/{end}"
    )
    response = request_with_backoff("GET", url)
    data = response.json()["items"]

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H")
    return df[["date", "views"]]


# --- Main --------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Начинаю сбор архивных данных за {RANGE_2009_START} — {RANGE_2009_END} "
          f"(потоковый режим, без записи на диск)...")
    df_2009 = collect_2009_range()

    print("Начинаю сбор данных с 2015 года по REST API...")
    df_modern = collect_modern_pageviews()

    combined = pd.concat([df_2009, df_modern], ignore_index=True)
    combined.to_csv("michael_jackson_pageviews.csv", index=False)
    print("Готово. Данные сохранены в michael_jackson_pageviews.csv")
    print(combined.head())
    print("...")
    print(combined.tail())