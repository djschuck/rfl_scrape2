from __future__ import annotations

from typing import List
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date

US_COUNTRY = "US"

def parse_event_entry(fetcher: Fetcher, url: str) -> EventRecord | None:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "lxml")

    # Name: best-effort from h1/title
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()
    if not name:
        title = soup.select_one("title")
        name = (title.get_text(" ", strip=True) if title else "").strip()

    # Date: ACS pages vary a lot. Try to find a label containing "Date" or "When"
    date_raw = ""
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        l = line.strip()
        if any(k in l.lower() for k in ("event date", "date:", "when:", "when is", "event day")):
            # take the tail after colon if present
            if ":" in l:
                date_raw = l.split(":", 1)[1].strip()
            else:
                date_raw = l
            break

    nd = normalize_date(date_raw, US_COUNTRY)
    emails = sorted(extract_emails(res.text))

    if not name and not emails and not nd.raw:
        return None

    return EventRecord(
        country=US_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )

def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    # For now, only parse supplied URLs.
    # You can implement discovery via Playwright/zip search or search-engine later.
    entry_urls = config.get("event_entry_urls", [])
    records: List[EventRecord] = []
    for u in entry_urls:
        r = parse_event_entry(fetcher, u)
        if r:
            records.append(r)
    return records
