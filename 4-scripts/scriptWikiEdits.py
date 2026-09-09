#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: revisions_collector_auth.py
Description: Collects the full revision history (timestamp + byte size) of the
             "Michael Jackson" Wikipedia article via the MediaWiki Action API.
             Uses the simpler legacy login flow recommended for Bot Passwords,
             saves progress incrementally to avoid data loss on interruption,
             validates API-level errors (not just HTTP errors), and catches
             network-level exceptions gracefully.
Author: [Your Name]
Date: 2026-09-09
Version: 1.4
"""

import os
import time
import random
import requests
import pandas as pd
from dotenv import load_dotenv

# --- Configuration ---------------------------------------------------------
load_dotenv()

BOT_USERNAME = os.getenv("WIKI_BOT_USERNAME")
BOT_PASSWORD = os.getenv("WIKI_BOT_PASSWORD")

HEADERS = {
    "User-Agent": (
        "DigitalHumanitiesProject/1.0 "
        "(student.email@university.it; Wikipedia account: Aim.btlv; "
        "culturomics research project)"
    )
}

API_URL = "https://en.wikipedia.org/w/api.php"
ARTICLE = "Michael Jackson"

SLEEP_BETWEEN_REQUESTS = 2.5
MAX_RETRIES = 5
CHECKPOINT_FILE = "michael_jackson_revisions_partial.csv"
FINAL_FILE = "michael_jackson_revisions.csv"


def safe_json_response(response: requests.Response) -> dict:
    """
    Validate an HTTP response before parsing it as JSON.
    Raises a clear error instead of a cryptic JSONDecodeError.
    """
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        snippet = response.text[:300].replace("\n", " ")
        raise RuntimeError(
            f"Non-JSON response (status {response.status_code}, "
            f"Content-Type: {content_type}). First 300 chars: {snippet!r}"
        )
    return response.json()


def request_with_backoff(session: requests.Session, method: str, **kwargs) -> requests.Response:
    """
    Perform an HTTP request with retries for both HTTP-level errors
    (429, 503) and network-level exceptions (timeouts, connection drops).
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = session.request(method, API_URL, headers=HEADERS, timeout=30, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)
            print(f"Network error ({exc}). Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            print(f"Rate limited (429). Waiting {retry_after}s as requested by server...")
            time.sleep(retry_after)
            continue

        if response.status_code == 503 or "maxlag" in response.text.lower()[:500]:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)
            print(f"Server lagged (503/maxlag). Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)
            continue

        return response

    raise RuntimeError("Max retries exceeded while contacting the Wikipedia API.")


def login(session: requests.Session, username: str, password: str) -> None:
    """
    Authenticate using the legacy login flow (action=login), which is the
    flow explicitly recommended for Bot Passwords, since it never triggers
    a multi-step (2FA/CAPTCHA) response like clientlogin can.
    """
    if not username or not password:
        raise RuntimeError(
            "Missing credentials. Make sure WIKI_BOT_USERNAME and WIKI_BOT_PASSWORD "
            "are set in your local .env file."
        )

    token_response = request_with_backoff(
        session, "GET",
        params={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
    )
    login_token = safe_json_response(token_response)["query"]["tokens"]["logintoken"]

    time.sleep(1.0)

    login_response = request_with_backoff(
        session, "POST",
        data={
            "action": "login",
            "lgname": username,
            "lgpassword": password,
            "lgtoken": login_token,
            "format": "json",
        },
    )
    login_data = safe_json_response(login_response)
    result = login_data.get("login", {}).get("result")

    if result != "Success":
        raise RuntimeError(f"Login failed: {login_data}")

    print("Login successful, session is now authenticated.")


def fetch_revision_batch(session: requests.Session, rvcontinue: str | None = None) -> dict:
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": ARTICLE,
        "rvprop": "timestamp|size|ids",
        "rvlimit": "500",
        "rvdir": "newer",
        "maxlag": "5",
        "formatversion": "2",
    }
    if rvcontinue:
        params["rvcontinue"] = rvcontinue

    response = request_with_backoff(session, "GET", params=params)
    data = safe_json_response(response)

    # Check for API-level errors (different from HTTP-level errors)
    if "error" in data:
        raise RuntimeError(f"API returned an error: {data['error']}")

    return data


def collect_all_revisions(session: requests.Session) -> pd.DataFrame:
    """
    Paginate through the full revision history, saving a checkpoint CSV
    after every batch so progress is never lost if the script is interrupted.
    """
    all_revisions = []
    rvcontinue = None

    while True:
        data = fetch_revision_batch(session, rvcontinue)
        pages = data.get("query", {}).get("pages", [])

        if not pages:
            raise RuntimeError("API response contained no page data. Check the article title.")

        page = pages[0]
        if page.get("missing"):
            raise RuntimeError(f"Article '{ARTICLE}' does not exist according to the API.")

        revisions = page.get("revisions", [])
        all_revisions.extend(revisions)

        print(f"Collected {len(all_revisions)} revisions so far...")

        # Save a checkpoint after every batch, so an interruption never costs
        # you the full run again.
        pd.DataFrame(all_revisions).to_csv(CHECKPOINT_FILE, index=False)

        if "continue" in data:
            rvcontinue = data["continue"]["rvcontinue"]
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        else:
            break

    if not all_revisions:
        raise RuntimeError("No revisions were collected.")

    df = pd.DataFrame(all_revisions)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df[["timestamp", "size", "revid"]]


# --- Main --------------------------------------------------------------------
if __name__ == "__main__":
    print("Начинаю скачивание истории правок. Пожалуйста, подождите...")

    try:
        session = requests.Session()
        login(session, BOT_USERNAME, BOT_PASSWORD)

        df_revisions = collect_all_revisions(session)
        df_revisions.to_csv(FINAL_FILE, index=False)
        print(f"Готово. Данные сохранены в {FINAL_FILE}")
        print(df_revisions.describe())

    except RuntimeError as e:
        print(f"Ошибка при скачивании: {e}")
        print(f"Частичные данные (если есть) сохранены в {CHECKPOINT_FILE}")