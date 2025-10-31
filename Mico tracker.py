#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Microlise TMC → Site Visits (Store 218) Scraper — Playwright (Python)

Dependencies:
  pip install playwright pandas beautifulsoup4 lxml requests

Usage:
  # (Recommended) Set environment variable for webhook
  export GOOGLE_CHAT_WEBHOOK_URL="your_new_webhook_url"

  # Run the script
  python Mico_tracker.py --username Store218 --password YourPassword
"""

import os
import json
import time
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import threading
from io import StringIO
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Page,
    BrowserContext,
    Response,
)

# --- Constants and Configuration ---
DEFAULT_USERNAME = os.getenv("MICROLISE_USERNAME", "Store218")
DEFAULT_PASSWORD = os.getenv("MICROLISE_PASSWORD", "Store218")
DEFAULT_AUTH_STATE = os.getenv("MICROLISE_AUTH_STATE", "auth_state.json")

# SECURITY WARNING: It is highly recommended to use an environment variable for the webhook.

DEFAULT_WEBHOOK_URL = os.getenv(
    "GOOGLE_CHAT_WEBHOOK_URL"
)

BASE = "https://live.microlise.com/MORRISONS"
AUTH_BASE = "https://auth.microlise.com"
VISITS_PATH_TMPL = "/TMCWebPortal/Site/Visits/{site_id}?siteIdEncoded=False"
LIST_VISITS_API_PATH_FRAGMENT = "/TMCWebPortal/Site/ListVisits"
LOGIN_KEYWORDS = ("login", "log in", "sign in", "authentication", "identifier")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("microlise-visits")


# --- Helper Functions ---

def looks_like_login(page: Page) -> bool:
    url = page.url.lower()
    if AUTH_BASE.lower() in url or any(k in url for k in LOGIN_KEYWORDS):
        return True
    try:
        if page.locator("input[type='password']").first.is_visible(): return True
        if page.locator("input[name='username']").first.is_visible(): return True
        if page.get_by_role("button", name=lambda n: n and ("log" in n.lower() or "sign" in n.lower() or "continue" in n.lower())).count():
            return True
    except Exception:
        pass
    return False

def try_fill_login(page: Page, username: str, password: str, timeout_ms: int = 40000) -> None:
    log.info("Attempting multi-step login...")
    try:
        user_box = page.locator("input[name='username'][id='username']").first
        user_box.wait_for(timeout=timeout_ms / 2)
        user_box.fill(username)
        page.locator("button[type='submit'][name='action'][value='default']._button-login-id").first.click()
        
        pass_box = page.locator("input[name='password'][id='password']").first
        pass_box.wait_for(timeout=timeout_ms)
        pass_box.fill(password)
        with page.expect_navigation(wait_until="networkidle", timeout=timeout_ms):
            page.locator("button[type='submit'][name='action'][value='default']._button-login-password").first.click()
    except Exception as e:
        raise RuntimeError(f"Failed to complete login process: {e}")
    log.info("Login process completed.")

def ensure_logged_in(
    context: BrowserContext,
    visits_url: str,
    username: str,
    password: str,
    timeout_ms: int,
    auth_state_path: Path,
) -> None:
    page = context.new_page()
    log.info("Opening visits URL to test auth…")
    try:
        page.goto(visits_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_load_state("networkidle", timeout=15000)
    except PWTimeout:
        log.warning("Network idle timeout during initial auth test, continuing...")
    
    if looks_like_login(page):
        log.info("Detected login page — performing login.")
        try_fill_login(page, username, password, timeout_ms=timeout_ms)
        if looks_like_login(page):
            page.wait_for_timeout(2000)
            if looks_like_login(page):
                raise RuntimeError("Login did not succeed (still on login page).")
        log.info("Login successful.")
        context.storage_state(path=str(auth_state_path))
    else:
        log.info("Existing auth state looks valid; no login needed.")
    page.close()

def normalize_payloads_to_df(payloads: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    if not payloads: return None
    api_response = payloads[0].get("data")
    if api_response and "Rows" in api_response and isinstance(api_response["Rows"], list):
        rows = api_response["Rows"]
        if rows: return pd.json_normalize(rows)
    return None

def parse_visible_table_to_df(page: Page) -> Optional[pd.DataFrame]:
    log.info("Starting precise HTML table parsing with BeautifulSoup.")
    
    header_table_locator = page.locator("div.ui-jqgrid-hdiv table.ui-jqgrid-htable").first
    if not header_table_locator.count():
        log.warning("Could not find the jqgrid header table.")
        return None
        
    header_html = header_table_locator.inner_html()
    soup_headers = BeautifulSoup(header_html, "lxml")
    headers = [th.get_text(strip=True) for th in soup_headers.select("thead tr th") if th.get_text(strip=True)]

    body_table_locator = page.locator("table.ui-jqgrid-btable:visible").first
    if not body_table_locator.count():
        log.warning("Could not find the jqgrid body table.")
        return None

    body_html = body_table_locator.inner_html()
    soup_body = BeautifulSoup(body_html, "lxml")
    
    data_rows = []
    for row in soup_body.select("tbody tr:not(.jqgfirstrow)"):
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if any(cells):
            data_rows.append(cells)

    if not data_rows:
        log.warning("No data rows found in the parsed HTML body.")
        return None

    df = pd.DataFrame(data_rows)
    
    if len(headers) == df.shape[1]:
        df.columns = headers
    else:
        log.warning(f"Header count ({len(headers)}) does not match column count ({df.shape[1]}).")
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]

    df.dropna(how='all', inplace=True)
    return df

def post_to_google_chat(df: pd.DataFrame, webhook_url: str, site_id: str):
    if not webhook_url:
        log.warning("Google Chat webhook URL is not set. Skipping notification.")
        return

    log.info("Filtering deliveries for today and formatting message...")

    time_col = 'PTA Time'
    date_col = 'PTA Date'
    quantity_col = 'Planned Quantity'
    salvage_col = 'Has Planned Asset Return'

    if date_col not in df.columns:
        log.error(f"Date column '{date_col}' not found. Cannot filter for today's deliveries.")
        todays_deliveries_df = df
    else:
        today_str = datetime.now().strftime('%d/%m/%Y')
        log.info(f"Filtering for deliveries on: {today_str}")
        todays_deliveries_df = df[df[date_col] == today_str].copy()

    delivery_lines = []
    if todays_deliveries_df.empty:
        log.info("No deliveries found for today.")
        full_message_text = "No deliveries scheduled for today."
    else:
        log.info(f"Found {len(todays_deliveries_df)} deliveries for today.")
        for index, row in todays_deliveries_df.iterrows():
            pta_time = row.get(time_col, "No Time")
            try:
                quantity = int(float(row.get(quantity_col, 0)))
                quantity_text = f"{quantity} Pallets"
            except (ValueError, TypeError):
                quantity_text = "0 Pallets"
            
            salvage_status = row.get(salvage_col, "No")
            salvage_text = "salvage" if str(salvage_status).lower() == "yes" else "no salvage"
            
            delivery_lines.append(f"{pta_time}: {quantity_text}, {salvage_text}")
        
        full_message_text = "\n".join(delivery_lines)

    message = {
        "cardsV2": [{
            "cardId": "delivery_plan_today",
            "card": {
                "header": {
                    "title": f"Today's Delivery Plan for Store {site_id}",
                    "subtitle": f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "imageUrl": "https://www.microlise.com/wp-content/uploads/2021/02/Microlise_Logo_Colour_RGB-1.png",
                    "imageType": "SQUARE"
                },
                "sections": [{"widgets": [{"textParagraph": {"text": full_message_text}}]}]
            }
        }]
    }

    log.info("Sending message to Google Chat webhook...")
    try:
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()
        log.info("Successfully posted message to Google Chat.")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to post message to Google Chat: {e}")


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="Microlise TMC → Site Visits scraper (Playwright).")
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--site-id", default="218")
    parser.add_argument("--auth-state", default=DEFAULT_AUTH_STATE)
    parser.add_argument("--headless", default="true", choices=["true", "false"])
    parser.add_argument("--timeout", type=int, default=60000)
    parser.add_argument("--csv", default="visits.csv")
    parser.add_argument("--json", default="visits_raw.json")
    parser.add_argument("--screenshot", default="debug_screenshot.png")
    parser.add_argument("--api-wait-timeout", type=int, default=25000)
    parser.add_argument("--webhook-url", default=DEFAULT_WEBHOOK_URL,
                        help="Google Chat webhook URL.")
    args = parser.parse_args()

    auth_state_path = Path(args.auth_state)
    headless = args.headless.lower() == "true"
    visits_url = f"{BASE}{VISITS_PATH_TMPL.format(site_id=args.site_id)}"

    log.info(f"Target visits URL: {visits_url}")
    log.info(f"Auth state file: {auth_state_path}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(storage_state=str(auth_state_path) if auth_state_path.exists() else None)
        page = None
        final_df = None

        try:
            ensure_logged_in(
                context=context,
                visits_url=visits_url,
                username=args.username,
                password=args.password,
                timeout_ms=args.timeout,
                auth_state_path=auth_state_path,
            )

            page = context.new_page()
            log.info("Navigating to visits page…")
            page.goto(visits_url, wait_until="domcontentloaded", timeout=args.timeout)

            api_data: Optional[Dict[str, Any]] = None
            response_captured_event = threading.Event()

            def _on_response(response: Response):
                nonlocal api_data
                if LIST_VISITS_API_PATH_FRAGMENT in response.url and response.status == 200:
                    log.info(f"Intercepted target API call: {response.url}")
                    try:
                        body = response.body()
                        json_data = json.loads(body)
                        if json_data and json_data.get("Rows") is not None:
                            api_data = json_data
                            response_captured_event.set()
                    except Exception as e:
                        log.warning(f"Could not parse JSON from API response body: {e}")

            log.info("Setting up API listener before refresh...")
            page.on("response", _on_response)

            log.info("Refreshing the page to trigger data grid...")
            page.reload(wait_until="domcontentloaded", timeout=30000)
            
            log.info(f"Waiting for ListVisits API response event (max {args.api_wait_timeout / 1000}s)...")
            response_captured_event.wait(timeout=args.api_wait_timeout / 1000)
            
            page.remove_listener("response", _on_response)

            if api_data:
                log.info("Successfully captured API data.")
                payloads = [{"url": "API_CAPTURE", "data": api_data}]
                df = normalize_payloads_to_df(payloads)
                if df is not None and not df.empty:
                    log.info(f"--- Scraped Data (from API) ---\n{df.to_string()}")
                    final_df = df
            
            if final_df is None:
                log.warning("API data capture failed or was empty. Falling back to HTML table parsing.")
                real_row_selector = "table.ui-jqgrid-btable tbody tr:not(.jqgfirstrow)"
                try:
                    log.info("Waiting for real data rows to appear in the HTML table...")
                    page.wait_for_selector(real_row_selector, timeout=20000)
                    log.info("Real data rows found in table. Proceeding with HTML parsing.")
                except PWTimeout:
                    log.error("Timed out waiting for real data rows in the HTML table.")
                    raise RuntimeError("Could not find any data via API or in the HTML table after refresh.")

                df = parse_visible_table_to_df(page)
                if df is None or df.empty:
                    raise RuntimeError("HTML table parsing failed even after waiting for real rows.")
                log.info(f"--- Scraped Data (from HTML Table) ---\n{df.to_string()}")
                final_df = df

            if final_df is not None and not final_df.empty:
                # Save the final data to CSV before posting
                final_df.to_csv(args.csv, index=False)
                log.info(f"Wrote final data to CSV: {args.csv} (rows={final_df.shape[0]})")
                # Post the result to Google Chat
                post_to_google_chat(final_df, args.webhook_url, args.site_id)

        except Exception as e:
            log.exception(f"Scrape failed: {e}")
            if page and not page.is_closed():
                page.screenshot(path=args.screenshot, full_page=True)
                log.info(f"Saved debug screenshot: {args.screenshot}")
            raise
        finally:
            if 'context' in locals() and context:
                context.close()
            if 'browser' in locals() and browser.is_connected():
                browser.close()

if __name__ == "__main__":
    main()
