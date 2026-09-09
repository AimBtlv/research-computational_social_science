#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: revisions_collector_auth.py
Description: Collects the full revision history (timestamp + byte size) of the
             "Michael Jackson" Wikipedia article via the MediaWiki Action API,
             using an authenticated session loaded from environment variables
             (never hardcoded), plus safe pagination and maxlag throttling.
Author: [Your Name]
Date: 2026-09-08
Version: 1.2
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# --- Configuration ---------------------------------------------------------
load_dotenv()  # reads variables from the local .env file, never committed to git

BOT_USERNAME = os.getenv("WIKI_BOT_USERNAME")
BOT_PASSWORD = os.getenv("WIKI_BOT_PASSWORD")

HEADERS = {
    "User-Agent": "DigitalHumanitiesProject/1.0 (student.email@university.it; culturomics research)"
}

API_URL = "https://en.wikipedia.org/w/api.php"
ARTICLE = "Michael Jackson"

SLEEP_BETWEEN_REQUESTS = 1.0
MAX_RETRIES = 5


def login(session: requests.Session, username: str, password: str) -> None:
    """
    Authenticate the session using a Bot Password loaded from environment variables.
    """
    if not username or not password:
        raise RuntimeError(
            "Missing credentials. Make sure WIKI_BOT_USERNAME and WIKI_BOT_PASSWORD "
            "are set in your local .env file."
        )

    token_response = session.get(
        API_URL,
        params={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
        headers=HEADERS,
        timeout=30,
    )
    login_token = token_response.json()["query"]["tokens"]["logintoken"]

    login_result = session.post(
        API_URL,
        data={
            "action": "clientlogin",
            "username": username,
            "password": password,
            "loginreturnurl": "https://en.wikipedia.org/",
            "logintoken": login_token,
            "format": "json",
        },
        headers=HEADERS,
        timeout=30,
    )

    status = login_result.json().get("clientlogin", {}).get("status")
    if status != "PASS":
        raise RuntimeError(f"Login failed: {login_result.json()}")

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

    for attempt in range(MAX_RETRIES):
        response = session.get(API_URL, params=params, headers=HEADERS, timeout=30)

        if response.status_code == 503 or "maxlag" in response.text.lower():
            wait_time = (attempt + 1) * 5
            print(f"Server lagged, waiting {wait_time}s before retry...")
            time.sleep(wait_time)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Max retries exceeded while fetching revisions.")


def collect_all_revisions(session: requests.Session) -> pd.DataFrame:
    all_revisions = []
    rvcontinue = None

    while True:
        data = fetch_revision_batch(session, rvcontinue)
        page = data["query"]["pages"][0]
        revisions = page.get("revisions", [])
        all_revisions.extend(revisions)

        print(f"Collected {len(all_revisions)} revisions so far...")

        if "continue" in data:
            rvcontinue = data["continue"]["rvcontinue"]
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        else:
            break

    df = pd.DataFrame(all_revisions)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df[["timestamp", "size", "revid"]]


# --- Main --------------------------------------------------------------------
if __name__ == "__main__":
    session = requests.Session()
    login(session, BOT_USERNAME, BOT_PASSWORD)

    df_revisions = collect_all_revisions(session)
    df_revisions.to_csv("michael_jackson_revisions.csv", index=False)
    print(df_revisions.describe())