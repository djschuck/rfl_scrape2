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
    event_url_contains: List[str],
) -> List[str]:
    """
    UK index pages list events as <h2><a href="...">Relay For Life X</a></h2>
    The hrefs are often /product/relay-life/<slug> which then redirect to
    /get-involved/find-an-event/relay-for-life/<slug>.
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

        # Prefer the event list structure: H2 headings with links
        for h2 in soup.select("h2"):
            a = h2.find("a", href=True)
            if not a:
                continue
            href = a.get("href") or ""
            u = absolutize(idx_url, href)
            if not is_http_url(u):
                continue
            if any(fragment in u for fragment in event_url_contains):
                found.add(u.split("#")[0])

        # Fallback: any link on page that matches the patterns
        if len(found) == before:
            for a in soup.select("a[href]"):
                href = a.get("href") or ""
                u = absolutize(idx_url, href)
                if not is_http_url(u):
                    continue
                if any(fragment in u for fragment in event_url_contains):
                    found.add(u.split("#")[0])

        after = len(found)
        if after == before:
            consecutive_no_new += 1
        else:
            consecutive_no_new = 0

        fetcher.log.info("UK page=%s discovered_total=%s (+%s)", p, after, after - before)

        if stop_when_no_new and consecutive_no_new >= 3:
            break

    return sorted(found)


def _extract_event_date_heading_style(soup: BeautifulSoup) -> str:
    """
    UK event pages typically render:
      ## Event date
      Saturday 20 June 2026
    i.e., heading-like element containing 'Event date', followed by the next text node.
    """
    # Match headings that contain "Event date"
    candidates = soup.find_all(
        lambda tag: tag.name in ("h2", "h3", "strong", "p", "div")
        and tag.get_text(" ", strip=True).lower() == "event date"
    )
    for tag in candidates:
        # Walk forward through siblings to find the next non-empty text
        nxt = tag
        for _ in range(20):
            nxt = nxt.find_next()
            if not nxt:
                break
            txt = nxt.get_text(" ", strip=True)
            # Skip the heading itself and other headings/labels
            if not txt:
                continue
            low = txt.lower()
            if low == "event date" or low == "event time" or low == "entry fee":
                continue
            # A real date line tends to contain a month name or digits
            if any(m in txt for m in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")) or any(ch.isdigit() for ch in txt):
                return txt
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("UK event fetch failed: %s status=%s", url, res.status_code)
        return None

    soup = BeautifulSoup(res.text, "lxml")

    # Name
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()

    # Date
    date_raw = _extract_event_date_heading_style(soup)

    nd = normalize_date(date_raw, UK_COUNTRY)
    emails = sorted(extract_emails(res.text))

    if not name and not emails and not nd.raw:
        fetcher.log.warning("UK parse produced empty record: %s", url)
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
    contains = config.get("event_url_contains", ["/product/relay-life/", "/get-involved/find-an-event/relay-for-life/"])

    urls = discover_event_urls(
        fetcher,
        template=template,
        page_start=page_start,
        page_max=page_max,
        stop_when_no_new=stop_when_no_new,
        event_url_contains=contains,
    )

    fetcher.log.info("UK total event urls discovered: %s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)
    return records
