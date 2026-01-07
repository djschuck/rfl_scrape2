from __future__ import annotations

from typing import List, Set
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

UK_COUNTRY = "UK"

def discover_event_urls(fetcher: Fetcher, template: str, page_start: int, page_max: int, stop_when_no_new: bool, event_url_contains: List[str]) -> List[str]:
    found: Set[str] = set()
    consecutive_no_new = 0

    for p in range(page_start, page_max + 1):
        url = template.format(page=p)
        res = fetcher.get_text(url)
        if res.status_code != 200:
            consecutive_no_new += 1
            if stop_when_no_new and consecutive_no_new >= 3:
                break
            continue

        before = len(found)
        soup = BeautifulSoup(res.text, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            u = absolutize(url, href)
            if not is_http_url(u):
                continue
            if any(fragment in u for fragment in event_url_contains):
                found.add(u.split("#")[0])

        after = len(found)
        if after == before:
            consecutive_no_new += 1
        else:
            consecutive_no_new = 0

        if stop_when_no_new and consecutive_no_new >= 3:
            break

    return sorted(found)

def parse_event_page(fetcher: Fetcher, url: str) -> EventRecord | None:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "lxml")

    # Name
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()

    # Date: UK pages typically have a label "Event date"
    date_raw = ""
    # Look for dt/dd pattern
    for dt in soup.select("dt"):
        label = dt.get_text(" ", strip=True).lower()
        if "event date" in label:
            dd = dt.find_next("dd")
            if dd:
                date_raw = dd.get_text(" ", strip=True)
                break

    if not date_raw:
        # fallback: heuristic
        text = soup.get_text("\n", strip=True)
        for line in text.splitlines():
            if "Event date" in line:
                date_raw = line.split("Event date", 1)[-1].strip(" :|-")
                break

    nd = normalize_date(date_raw, UK_COUNTRY)
    emails = sorted(extract_emails(res.text))

    if not name and not emails and not nd.raw:
        return None

    return EventRecord(
        country=UK_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )

def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    template = config["index_url_template"]
    page_start = int(config.get("page_start", 0))
    page_max = int(config.get("page_max", 50))
    stop_when_no_new = bool(config.get("stop_when_no_new", True))
    contains = config.get("event_url_contains", ["/get-involved/find-an-event/relay-for-life/"])

    urls = discover_event_urls(
        fetcher,
        template=template,
        page_start=page_start,
        page_max=page_max,
        stop_when_no_new=stop_when_no_new,
        event_url_contains=contains,
    )

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)
    return records
