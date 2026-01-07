from __future__ import annotations

from typing import List, Set, Optional
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

UK_COUNTRY = "UK"


def discover_event_urls(
    fetcher: Fetcher,
    template: str,
    page_start: int,
    page_max: int,
    stop_when_no_new: bool,
) -> List[str]:
    """
    CRUK Relay For Life index pages use product cards:
      <a class="product-card__link" href="/product/relay-life/<slug>">
    """
    found: Set[str] = set()
    consecutive_no_new = 0

    for p in range(page_start, page_max + 1):
        idx_url = template.format(page=p)
        res = fetcher.get_text(idx_url)
        if res.status_code != 200:
            fetcher.log.warning("UK index fetch failed: %s status=%s", idx_url, res.status_code)
            consecutive_no_new += 1
            if stop_when_no_new and consecutive_no_new >= 3:
                break
            continue

        before = len(found)
        soup = BeautifulSoup(res.text, "lxml")

        # PRIMARY selector: product cards
        for a in soup.select("a.product-card__link[href]"):
            href = a.get("href", "")
            u = absolutize(idx_url, href)
            if is_http_url(u):
                found.add(u.split("#")[0])

        after = len(found)
        fetcher.log.info(
            "UK page=%s discovered_total=%s (+%s)",
            p, after, after - before
        )

        if after == before:
            consecutive_no_new += 1
        else:
            consecutive_no_new = 0

        if stop_when_no_new and consecutive_no_new >= 3:
            break

    return sorted(found)


def extract_event_date(soup: BeautifulSoup) -> str:
    """
    UK event pages render:
      Event date
      Saturday 20 June 2026
    """
    for tag in soup.find_all(["h2", "h3", "strong"]):
        if tag.get_text(strip=True).lower() == "event date":
            nxt = tag.find_next()
            while nxt:
                txt = nxt.get_text(" ", strip=True)
                if txt and txt.lower() not in {"event date", "event time"}:
                    return txt
                nxt = nxt.find_next()
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("UK event fetch failed: %s status=%s", url, res.status_code)
        return None

    soup = BeautifulSoup(res.text, "lxml")

    # Event name
    h1 = soup.select_one("h1")
    name = h1.get_text(" ", strip=True) if h1 else ""

    # Date
    date_raw = extract_event_date(soup)
    nd = normalize_date(date_raw, UK_COUNTRY)

    # Emails
    emails = sorted(extract_emails(res.text))

    if not name:
        fetcher.log.warning("UK event missing name: %s", url)

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
        fetcher=fetcher,
        template=config["index_url_template"],
        page_start=int(config.get("page_start", 0)),
        page_max=int(config.get("page_max", 120)),
        stop_when_no_new=bool(config.get("stop_when_no_new", True)),
    )

    fetcher.log.info("UK total event urls discovered: %s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
