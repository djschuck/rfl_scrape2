from __future__ import annotations

from typing import List, Set
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

AU_COUNTRY = "AU"

def discover_event_urls(fetcher: Fetcher, index_urls: List[str], event_url_contains: List[str]) -> List[str]:
    found: Set[str] = set()
    for idx in index_urls:
        res = fetcher.get_text(idx)
        if res.status_code != 200:
            continue
        soup = BeautifulSoup(res.text, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            u = absolutize(idx, href)
            if not is_http_url(u):
                continue
            if any(fragment in u for fragment in event_url_contains):
                # avoid the master index itself and noise
                if "/event/" in u and "/events" not in u:
                    found.add(u.split("#")[0])
    return sorted(found)

def parse_event_page(fetcher: Fetcher, url: str) -> EventRecord | None:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "lxml")

    # Name: prefer h1
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()
    if not name:
        title = soup.select_one("title")
        name = (title.get_text(" ", strip=True) if title else "").strip()

    # Date: AU pages show a label "Event Date" with value nearby (site is somewhat templated)
    date_raw = ""
    # Try label-based extraction
    label = soup.find(string=lambda s: isinstance(s, str) and "Event Date" in s)
    if label:
        # Walk up and find a nearby container, then search for a likely date string
        parent = label.parent
        container = parent
        for _ in range(4):
            if container and container.name not in ("html", "body"):
                container = container.parent
        if container:
            text = container.get_text(" ", strip=True)
            # crude heuristic: take substring after "Event Date"
            if "Event Date" in text:
                date_raw = text.split("Event Date", 1)[1].strip(" :|-")

    # Fallback: look for "Sat" / month names
    if not date_raw:
        page_text = soup.get_text("\n", strip=True)
        for line in page_text.splitlines():
            l = line.strip()
            if any(m in l for m in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")):
                if any(ch.isdigit() for ch in l) and len(l) <= 50:
                    date_raw = l
                    break

    nd = normalize_date(date_raw, AU_COUNTRY)
    emails = sorted(extract_emails(res.text))

    if not name and not emails and not nd.raw:
        return None

    return EventRecord(
        country=AU_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )

def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    index_urls = config.get("index_urls", [])
    contains = config.get("event_url_contains", ["/event/"])
    urls = discover_event_urls(fetcher, index_urls=index_urls, event_url_contains=contains)

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)
    return records
