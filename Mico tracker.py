#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Microlise TMC → Site Visits (Store 218) Scraper — Playwright (Python)
Final version, handles redirect to Home page after login.
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

DEFAULT_WEBHOOK_URL = os.getenv(
    "GOOGLE_CHAT_WEBHOOK_URL",
    "https://chat.googleapis.com/v1/spaces/AAAAE9Syx9g/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=yrFCiWB27_A0Wxoev2zRpvnkLrnYCmwGP86EeOZDTKE"
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
        # Use short timeouts here to avoid long waits
        if page.locator("input[type='password']").is_visible(timeout=2000): return True
        if page.locator("input[name='username']").is_visible(timeout=2000): return True
    except Exception:
        pass
    return False

def try_fill_login(page: Page, username: str, password: str, timeout_ms: int = 60000) -> None:
    log.info("Attempting multi-step login...")
    try:
        # Step 1: Username
        user_box = page.locator("input[name='username'][id='username']").first
        user_box.wait_for(state="visible", timeout=timeout_ms / 2)
        user_box.fill(username)
        page.locator("button[type='submit'][name='action'][value='default']._button-login-id").first.click()

        # Step 2: Password
        pass_box = page.locator("input[name='password'][id='password']").first
        pass_box.wait_for(state="visible", timeout=timeout_ms / 2)
        pass_box.fill(password)

        # Submit and wait for whatever navigation happens next.
        log.info("Submitting password and waiting for navigation to complete...")
        with page.expect_navigation(wait_until="load", timeout=timeout_ms):
            page.locator("button[type='submit'][name='action'][value='default']._button-login-password").first.click()

    except Exception as e:
        page.screenshot(path="debug_screenshot.png", full_page=True)
        raise RuntimeError(f"Failed during login form steps: {e}")

    log.info("Login form submitted.")


def ensure_logged_in(
    context: BrowserContext,
    visits_url: str,
    username: str,
    password: str,
    timeout_ms: int,
    auth_state_path: Path,
) -> None:
    page = context.new_page()
    log.info("Checking authentication status...")
    try:
        page.goto(visits_url, wait_until="load", timeout=25000)
    except PWTimeout:
        log.warning("Initial page load for auth check was slow, continuing...")

    if looks_like_login(page):
        log.info("Detected login page — performing login.")
        try_fill_login(page, username, password, timeout_ms=timeout_ms)
        # Trust that the login worked. We will verify by navigating to the page in main().
        log.info("Login attempt complete. Saving authentication state.")
        context.storage_state(path=str(auth_state_path))
    else:
        log.info("Already logged in; auth state is valid.")
    page.close()


def parse_visible_table_to_df(page: Page) -> Optional[pd.DataFrame]:
    log.info("Starting precise HTML table parsing...")
    header_table_locator = page.locator("div.ui-jqgrid-hdiv table.ui-jqgrid-htable").first
    if not header_table_locator.count(): return None
    header_html = header_table_locator.inner_html()
    soup_headers = BeautifulSoup(header_html, "lxml")
    headers = [th.get_text(strip=True) for th in soup_headers.select("thead tr th") if th.get_text(strip=True)]

    body_table_locator = page.locator("table.ui-jqgrid-btable:visible").first
    if not body_table_locator.count(): return None
    body_html = body_table_locator.inner_html()
    soup_body = BeautifulSoup(body_html, "lxml")
    data_rows = [
        [cell.get_text(strip=True) for cell in row.find_all("td")]
        for row in soup_body.select("tbody tr:not(.jqgfirstrow)")
        if any(cell.get_text(strip=True) for cell in row.find_all("td"))
    ]

    if not data_rows: return None
    df = pd.DataFrame(data_rows)
    if len(headers) == df.shape[1]:
        df.columns = headers
    else:
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
    df.dropna(how='all', inplace=True)
    return df


def post_to_google_chat(df: pd.DataFrame, webhook_url: str, site_id: str):
    if not webhook_url:
        log.warning("Google Chat webhook URL is not set. Skipping notification.")
        return

    log.info("Filtering deliveries for today and formatting message...")
    date_col = 'PTA Date'
    if date_col not in df.columns:
        log.error(f"Date column '{date_col}' not found. Cannot filter for today's deliveries.")
        todays_deliveries_df = df
    else:
        today_str = datetime.now().strftime('%d/%m/%Y')
        log.info(f"Filtering for deliveries on: {today_str}")
        todays_deliveries_df = df[df[date_col] == today_str].copy()

    if todays_deliveries_df.empty:
        log.info("No deliveries found for today.")
        full_message_text = "No deliveries scheduled for today."
    else:
        log.info(f"Found {len(todays_deliveries_df)} deliveries for today.")
        delivery_lines = []
        for index, row in todays_deliveries_df.iterrows():
            pta_time = row.get('PTA Time', "No Time")
            try:
                quantity = int(float(row.get('Planned Quantity', 0)))
                quantity_text = f"{quantity} Pallets"
            except (ValueError, TypeError):
                quantity_text = "0 Pallets"
            salvage_status = row.get('Has Planned Asset Return', "No")
            salvage_text = "salvage" if str(salvage_status).lower() == "yes" else "no salvage"
            delivery_lines.append(f"{pta_time}: {quantity_text}, {salvage_text}")
        full_message_text = "\n".join(delivery_lines)

    message = { "cardsV2": [{ "cardId": "delivery_plan_today", "card": { "header": { "title": f"Today's Delivery Plan for Store {site_id}", "subtitle": f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "imageUrl": "https://www.microlise.com/wp-content/uploads/2021/02/Microlise_Logo_Colour_RGB-1.png", "imageType": "SQUARE" }, "sections": [{"widgets": [{"textParagraph": {"text": full_message_text}}]}] } }] }
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
    parser.add_argument("--screenshot", default="debug_screenshot.png")
    parser.add_argument("--webhook-url", default=DEFAULT_WEBHOOK_URL)
    args = parser.parse_args()

    auth_state_path = Path(args.auth_state)
    visits_url = f"{BASE}{VISITS_PATH_TMPL.format(site_id=args.site_id)}"
    log.info(f"Target visits URL: {visits_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless.lower() == "true")
        context_opts = {}
        if auth_state_path.exists():
            context_opts["storage_state"] = str(auth_state_path)
        context = browser.new_context(**context_opts)
        
        page = None
        final_df = None

        try:
            # Step 1: Make sure we are logged in. This may leave us on the Home page.
            ensure_logged_in(
                context=context,
                visits_url=visits_url,
                username=args.username,
                password=args.password,
                timeout_ms=args.timeout,
                auth_state_path=auth_state_path,
            )

            # Step 2: Now that we are authenticated, navigate DIRECTLY to the visits page.
            page = context.new_page()
            log.info("Navigating to the Site Visits page to get data...")
            page.goto(visits_url, wait_until="load", timeout=args.timeout)

            # Step 3: Verify we are on the correct page. If not, fail gracefully.
            if looks_like_login(page):
                log.error("Authentication failed. Landed on login page when trying to access visits.")
                raise RuntimeError("Authentication failed; could not access protected page.")
            
            log.info("Successfully on the Site Visits page.")

            # Step 4: Refresh the page to trigger the data grid's API call.
            log.info("Refreshing page to ensure data grid populates...")
            page.reload(wait_until="domcontentloaded", timeout=30000)
            
            # Step 5: Wait for the grid to load its data.
            log.info("Waiting for data rows to appear in the HTML table...")
            real_row_selector = "table.ui-jqgrid-btable tbody tr:not(.jqgfirstrow)"
            try:
                page.wait_for_selector(real_row_selector, timeout=25000)
                log.info("Real data rows found in table.")
            except PWTimeout:
                log.error("Timed out waiting for data rows in the HTML table after refresh.")
                raise RuntimeError("Could not find data in the HTML table.")

            final_df = parse_visible_table_to_df(page)
            if final_df is None or final_df.empty:
                raise RuntimeError("HTML table parsing failed even after waiting for rows.")

            log.info(f"--- Scraped Data ---\n{final_df.to_string()}")
            final_df.to_csv(args.csv, index=False)
            log.info(f"Wrote final data to CSV: {args.csv} (rows={final_df.shape[0]})")
            post_to_google_chat(final_df, args.webhook_url, args.site_id)

        except Exception as e:
            log.exception(f"Scrape failed: {e}")
            if page and not page.is_closed():
                page.screenshot(path=args.screenshot, full_page=True)
                log.info(f"Saved debug screenshot: {args.screenshot}")
            raise
        finally:
            if 'context' in locals() and context: context.close()
            if 'browser' in locals() and browser.is_connected(): browser.close()

if __name__ == "__main__":
    main()
