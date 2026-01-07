from __future__ import annotations

from typing import List, Set, Optional
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date

UK_COUNTRY = "UK"


def discover_event_urls(template: str, page_start: int, page_max: int) -> List[str]:
    found: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for pnum in range(page_start, page_max + 1):
            url = template.format(page=pnum)
            page.goto(url, timeout=60000)
            page.wait_for_timeout(3000)  # allow JS render

            html = page.content()
            soup = BeautifulSoup(html, "lxml")

            matches = 0
            for a in soup.select("a[href^='/product/relay-life/']"):
                href = a.get("href", "").strip()
                if href.count("/") == 3:
                    found.add("https://www.cancerresearchuk.org" + href)
                    matches += 1

            if matches == 0:
                break  # no more pages

        browser.close()

    return sorted(found)


def extract_event_date(soup: BeautifulSoup) -> str:
    label = soup.find(string=lambda s: isinstance(s, str) and s.strip().lower() == "event date")
    if not label:
        return ""

    nxt = label.parent.find_next()
    while nxt:
        txt = nxt.get_text(" ", strip=True)
        if txt and txt.lower() not in {"event date", "event time"}:
            return txt
        nxt = nxt.find_next()
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
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
    urls = discover_event_urls(
        template=config["index_url_template"],
        page_start=int(config.get("page_start", 1)),
        page_max=int(config.get("page_max", 200)),
    )

    fetcher.log.info("UK total event urls discovered (Playwright): %s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
