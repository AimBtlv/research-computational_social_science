#!/usr/bin/env python3
# -*- coding: utf-8 -*-


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

HEADERS =HEADERS = {
    "User-Agent": (
        "DigitalHumanitiesProject"
        "(908423@stud.unive.it; Wikipedia account: Aim.btlv; "
        "culturomics research project)"
    )
}
API_URL = "https://en.wikipedia.org/w/api.php"
ARTICLE = "Michael Jackson"

SLEEP_BETWEEN_REQUESTS = 2.5  
MAX_RETRIES = 5


def safe_json_response(response: requests.Response) -> dict:
    """
    Validate an HTTP response before trying to parse it as JSON.
    Raises a clear, informative error instead of a cryptic JSONDecodeError
    when Wikipedia returns an HTML block page, empty body, or rate-limit notice.
    """
    content_type = response.headers.get("Content-Type", "")

    if "application/json" not in content_type:
        snippet = response.text[:300].replace("\n", " ")
        raise RuntimeError(
            f"Non-JSON response received (status {response.status_code}, "
            f"Content-Type: {content_type}). First 300 chars: {snippet!r}"
        )

    return response.json()


def request_with_backoff(session: requests.Session, method: str, **kwargs) -> requests.Response:
    """
    Perform an HTTP request with exponential backoff + jitter,
    explicitly handling 429 (Too Many Requests) and 503 (maxlag) responses.
    """
    for attempt in range(MAX_RETRIES):
        response = session.request(method, API_URL, headers=HEADERS, timeout=30, **kwargs)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            print(f"Rate limited (429). Waiting {retry_after}s as requested by server...")
            time.sleep(retry_after)
            continue

        if response.status_code == 503 or "maxlag" in response.text.lower()[:500]:
            wait_time = (attempt + 1) * 10 + random.uniform(0, 3)  # backoff + jitter
            print(f"Server lagged (503/maxlag). Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)
            continue

        return response

    raise RuntimeError("Max retries exceeded while contacting the Wikipedia API.")


def login(session: requests.Session, username: str, password: str) -> None:
    """
    Authenticate the session using a Bot Password loaded from environment variables.
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
    token_data = safe_json_response(token_response)
    login_token = token_data["query"]["tokens"]["logintoken"]

    time.sleep(1.0)  # small pause between token request and login itself

    login_response = request_with_backoff(
        session, "POST",
        data={
            "action": "clientlogin",
            "username": username,
            "password": password,
            "loginreturnurl": "https://en.wikipedia.org/",
            "logintoken": login_token,
            "format": "json",
        },
    )
    login_data = safe_json_response(login_response)

    status = login_data.get("clientlogin", {}).get("status")
    if status != "PASS":
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
    return safe_json_response(response)


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

    if not all_revisions:
        raise RuntimeError("No revisions were collected. Check the article title and API response.")

    df = pd.DataFrame(all_revisions)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df[["timestamp", "size", "revid"]]


# --- Main --------------------------------------------------------------------
if __name__ == "__main__":
    print("Strating downloading the history of Revisions, Wait ...")

    try:
        session = requests.Session()
        login(session, BOT_USERNAME, BOT_PASSWORD)

        df_revisions = collect_all_revisions(session)
        df_revisions.to_csv("michael_jackson_revisions.csv", index=False)
        print("Ready, data saved in michael_jackson_revisions.csv")
        print(df_revisions.describe())

    except RuntimeError as e:
        print(f"Errore during downloading: {e}")
        print("Data was downloaded, File not saved")