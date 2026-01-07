from __future__ import annotations

import os
import re
from typing import List, Set, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date

UK_COUNTRY = "UK"

# Accept both URL shapes
UK_EVENT_PATTERNS = (
    "/product/relay-life/",
    "/get-involved/find-an-event/relay-for-life/",
)

def _debug_dir() -> str:
    d = os.environ.get("RELAY_DEBUG_DIR", "out/debug")
    os.makedirs(d, exist_ok=True)
    return d

def _dump(page_num: int, html: str, screenshot_bytes: bytes | None = None) -> None:
    d = _debug_dir()
    with open(os.path.join(d, f"uk_index_rendered_{page_num}.html"), "w", encoding="utf-8") as f:
        f.write(html)
    if screenshot_bytes:
        with open(os.path.join(d, f"uk_index_rendered_{page_num}.png"), "wb") as f:
            f.write(screenshot_bytes)

def _try_accept_cookies(page) -> None:
    # Try several common button texts/roles; ignore failures.
    candidates = [
        "text=Accept all cookies",
        "text=Accept cookies",
        "text=Accept all",
        "text=I accept",
        "text=Agree",
    ]
    for sel in candidates:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=2000)
                page.wait_for_timeout(1000)
                return
        except Exception:
            pass

def discover_event_urls(fetcher: Fetcher, template: str, page_start: int, page_max: int) -> List[str]:
    found: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Reduce noise / speed up
        page.set_viewport_size({"width": 1280, "height": 900})

        for pnum in range(page_start, page_max + 1):
            url = template.format(page=pnum)
            fetcher.log.info("UK Playwright goto page=%s url=%s", pnum, url)

            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1500)

            _try_accept_cookies(page)
            page.wait_for_timeout(2500)  # allow JS render

            html = page.content()
            soup = BeautifulSoup(html, "lxml")

            matched_this_page = 0
            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href:
                    continue
                if any(pat in href for pat in UK_EVENT_PATTERNS):
                    # Normalize absolute URL
                    if href.startswith("/"):
                        full = "https://www.cancerresearchuk.org" + href
                    elif href.startswith("http"):
                        full = href
                    else:
                        continue
                    # De-dup fragments
                    full = full.split("#")[0]
                    found.add(full)
                    matched_this_page += 1

            fetcher.log.info(
                "UK Playwright page=%s matched_on_page=%s total=%s",
                pnum, matched_this_page, len(found)
            )

            # Dump early pages if we're not finding anything
            if matched_this_page == 0 and pnum <= page_start + 2:
                try:
                    shot = page.screenshot(full_page=True)
                except Exception:
                    shot = None
