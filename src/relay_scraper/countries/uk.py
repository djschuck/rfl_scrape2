from __future__ import annotations

import os
from typing import List, Set, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date

UK_COUNTRY = "UK"


def _debug_dir() -> str:
    d = os.environ.get("RELAY_DEBUG_DIR", "out/debug")
    os.makedirs(d, exist_ok=True)
    return d


def _dump(page_idx: int, html: str, screenshot_bytes: bytes | None = None) -> None:
    d = _debug_dir()
    with open(os.path.join(d, f"uk_index_rendered_{page_idx}.html"), "w", encoding="utf-8") as f:
        f.write(html)
    if screenshot_bytes:
        with open(os.path.join(d, f"uk_index_rendered_{page_idx}.png"), "wb") as f:
            f.write(screenshot_bytes)


def _try_accept_cookies(page, fetcher: Fetcher) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "text=I accept cookies",
        "text=Accept cookies",
        "text=Accept all cookies",
        "button:has-text('I accept cookies')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=3000)
                page.wait_for_timeout(800)
                fetcher.log.info("UK clicked cookie accept via selector: %s", sel)
                return
        except Exception:
            continue
    fetcher.log.info("UK cookie accept: no matching button found (may already be accepted).")


def _extract_event_urls_from_html(html: str) -> Set[str]:
    """
    Extract event URLs only from the MAIN content to avoid nav/footer repeats.
    """
    soup = BeautifulSoup(html, "lxml")
    main = soup.select_one("main") or soup  # fallback if <main> missing

    urls: Set[str] = set()

    # Primary: event teaser nodes
    for a in main.select("article.node-cruk-event a[rel='bookmark'][href]"):
        href = (a.get("href") or "").strip()
        if href.startswith("/get-involved/find-an-event/relay-for-life"):
            urls.add("https://www.cancerresearchuk.org" + href.split("#")[0])

    # Fallback: any anchor in main that looks like a relay-for-life event page
    if not urls:
        for a in main.select("a[href]"):
            href = (a.get("href") or "").strip()
            if href.startswith("/get-involved/find-an-event/relay-for-life") and href != "/get-involved/find-an-event/relay-for-life":
                urls.add("https://www.cancerresearchuk.org" + href.split("#")[0])

    return urls


def _click_next(page) -> bool:
    """
    Click the 'Next' pagination link if present.
    Returns True if clicked, False if no next page.
    """
    candidates = [
        "a[rel='next']",
        "li.pager__item--next a",
        "a:has-text('Next')",
        "a:has-text('next')",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=5000)
                return True
        except Exception:
            continue
    return False


def discover_event_urls_via_next(fetcher: Fetcher, start_url: str, max_pages: int, stop_when_no_new: bool) -> List[str]:
    found: Set[str] = set()
    no_new_streak = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_viewport_size({"width": 1280, "height": 900})

        fetcher.log.info("UK Playwright start url=%s", start_url)
        page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1200)

        _try_accept_cookies(page, fetcher)

        for page_idx in range(1, max_pages + 1):
            # Give JS time to populate the list
            try:
                page.wait_for_selector("main", timeout=15000)
            except Exception:
                pass
            page.wait_for_timeout(1500)

            html = page.content()
            before = len(found)

            urls = _extract_event_urls_from_html(html)
            found |= urls

            added = len(found) - before
            fetcher.log.info("UK page_idx=%s extracted=%s total=%s (+%s)", page_idx, len(urls), len(found), added)

            # Dump first few pages always (for your “careful analysis”)
            if page_idx <= 3:
                try:
                    shot = page.screenshot(full_page=True)
                except Exception:
                    shot = None
                _dump(page_idx, html, shot)

            if added == 0:
                no_new_streak += 1
            else:
                no_new_streak = 0

            if stop_when_no_new and no_new_streak >= 3:
                fetcher.log.info("UK stopping after %s pages with no new URLs.", no_new_streak)
                break

            # Move to next page
            if not _click_next(page):
                fetcher.log.info("UK no 'Next' pagination link found; stopping.")
                break

            # Wait for navigation / DOM update
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(1200)

        browser.close()

    # Filter out obvious non-event / root pages
    cleaned = sorted(u for u in found if u.count("/") >= 6)
    return cleaned


def extract_event_date(soup: BeautifulSoup) -> str:
    label = soup.find(string=lambda s: isinstance(s, str) and s.strip().lower() == "event date")
    if not label:
        return ""
    nxt = label.parent.find_next()
    while nxt:
        txt = nxt.get_text(" ", strip=True)
        if txt and txt.lower() not in {"event date", "event time"}:
            return txt.strip()
        nxt = nxt.find_next()
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("UK event fetch failed: %s status=%s", url, res.status_code)
        return None

    soup = BeautifulSoup(res.text, "lxml")

    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()

    date_raw = extract_event_date(soup)
    nd = normalize_date(date_raw, UK_COUNTRY)

    emails = sorted(extract_emails(res.text))

    return EventRecord(
        country=UK_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )


def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    # Start exactly from the working URL pattern you validated
    start_url = config.get("start_url") or config["index_url_template"].format(page=int(config.get("page_start", 1)))

    urls = discover_event_urls_via_next(
        fetcher=fetcher,
        start_url=start_url,
        max_pages=int(config.get("page_max", 200)),
        stop_when_no_new=bool(config.get("stop_when_no_new", True)),
    )

    fetcher.log.info("UK total event urls discovered (Playwright-next): %s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
