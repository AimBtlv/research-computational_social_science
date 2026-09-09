#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: pageviews_collector.py
Description: Collects Wikipedia pageview data for the "Michael Jackson" article.
             Combines archival hourly dumps (2009, streamed directly from the
             network without ever writing to disk) with the modern REST
             Pageviews API (2015-present). Includes retry with backoff for
             both HTTP-level and network-level errors, and per-day checkpoint
             saving so progress is never lost on interruption.
Author: [Your Name]
Date: 2026-09-09
Version: 1.2
"""

import time
import random
import gzip
import requests
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "DigitalHumanitiesProject/1.0 "
        "(student.email@university.it; Wikipedia account: Aim.btlv; "
        "culturomics research project)"
    )
}

ARTICLE = "Michael_Jackson"
SLEEP_BETWEEN_REQUESTS = 1.5
MAX_RETRIES = 5


def request_with_backoff(method: str, url: str, **kwargs) -> requests.Response:
    """
    Perform an HTTP request with retries for HTTP-level errors (429, 503)
    and network-level exceptions (timeouts, dropped connections).
    Returns the raw response object; the caller is responsible for streaming
    or reading the body as appropriate.
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
            return response  # let the caller decide how to handle a missing file

        response.raise_for_status()
        return response

    raise RuntimeError(f"Max retries exceeded while requesting {url}")


# --- Part A: 2009 archival dumps, streamed (no disk writes) -----------------
def get_article_count_streaming(date_str: str, hour: str, article: str, lang: str = "en") -> int:
    """
    Stream a single hourly pagecounts-raw file directly from the network,
    decompress it on the fly, and return the hit count for one article.
    The file is never written to disk: gzip.GzipFile reads straight from
    the response's raw socket stream.
    date_str format: YYYYMMDD, hour format: HHMMSS (e.g. '000000')
    """
    year, month = date_str[:4], date_str[4:6]
    filename = f"pagecounts-{date_str}-{hour}.gz"
    url = f"https://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{month}/{filename}"

    target_prefix = f"{lang} {article} "

    response = request_with_backoff("GET", url, stream=True)

    if response.status_code == 404:
        print(f"File not found on server, skipping: {filename}")
        return 0

    count = 0
    try:
        with gzip.GzipFile(fileobj=response.raw) as gz:
            for raw_line in gz:
                line = raw_line.decode("utf-8", errors="ignore")
                if line.startswith(target_prefix):
                    count = int(line.split()[2])
                    break  # found the article, no need to keep reading the stream
    except (OSError, gzip.BadGzipFile) as exc:
        # Stream was interrupted mid-file or the archive is corrupted server-side.
        # Treat this hour as missing rather than crashing the whole run.
        print(f"Stream error while reading {filename}: {exc}. Treating as 0 for this hour.")
        return 0
    finally:
        response.close()  # release the network connection promptly

    time.sleep(SLEEP_BETWEEN_REQUESTS)
    return count


def collect_2009_flashpoint() -> pd.DataFrame:
    """
    Collect hourly-summed daily pageviews for the three key dates in June 2009.
    Streams every hourly file directly, saving a checkpoint CSV after each day
    so partial progress survives an interruption.
    """
    target_dates = ["20090624", "20090625", "20090626"]
    hours = [f"{h:02d}0000" for h in range(24)]

    records = []
    for date_str in target_dates:
        daily_total = 0
        for hour in hours:
            daily_total += get_article_count_streaming(date_str, hour, ARTICLE)

        records.append({"date": pd.to_datetime(date_str, format="%Y%m%d"), "views": daily_total})
        pd.DataFrame(records).to_csv("pageviews_2009_partial.csv", index=False)  # checkpoint
        print(f"Day {date_str} done: {daily_total} views")

    return pd.DataFrame(records)


# --- Part B: 2015-present REST Pageviews API ---------------------------------
def collect_modern_pageviews(start: str = "2015070100") -> pd.DataFrame:
    """
    Fetch daily pageviews from the official Wikimedia REST API.
    The end date is computed dynamically as "yesterday" so the request
    never targets a future date the API cannot yet have data for.
    """
    end = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d00")

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
    print("Начинаю сбор архивных данных за июнь 2009 (потоковый режим, без записи на диск)...")
    df_2009 = collect_2009_flashpoint()

    print("Начинаю сбор данных с 2015 года по REST API...")
    df_modern = collect_modern_pageviews()

    combined = pd.concat([df_2009, df_modern], ignore_index=True)
    combined.to_csv("michael_jackson_pageviews.csv", index=False)
    print("Готово. Данные сохранены в michael_jackson_pageviews.csv")
    print(combined.head())