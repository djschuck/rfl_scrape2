from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.us_api import probe_variant, search_events

US_COUNTRY = "US"


def event_id_to_str_url(event_id: str) -> str:
    return f"https://secure.acsevents.org/site/STR?pg=entry&fr_id={event_id}"


def discover_event_ids(fetcher: Fetcher, zip_codes: List[str], radius_miles: int) -> Set[str]:
    """
    Discover Relay For Life event IDs using the ACS fundraising API.
    """
    if not zip_codes:
        return set()

    # We use requests internally in us_api; but the Fetcher logger is what we want for consistency.
    fetcher.log.info("US probing fundraising API using zip=%s radius=%s", zip_codes[0], radius_miles)

    # Probe once to find the correct parameter variant; then reuse for all zips.
    variant = probe_variant(zip_codes[0], radius_miles)

    event_ids: Set[str] = set()
    for z in zip_codes:
        try:
            results = search_events(z, radius_miles, variant)
            fetcher.log.info("US zip=%s results=%s", z, len(results))
            for row in results:
                eid = str(row.get("eventId") or "").strip()
                if eid.isdigit():
                    event_ids.add(eid)
        except Exception as e:
            fetcher.log.warning("US zip=%s API search failed: %r", z, e)

    return event_ids


def _extract_event_name(soup: BeautifulSoup) -> str:
    # Most ACS pages have an h1 with the event name
    h1 = soup.select_one("h1")
    if h1:
        txt = h1.get_text(" ", strip=True)
        if txt:
            return txt

    # Fallback: title
    title = soup.select_one("title")
    if title:
        txt = title.get_text(" ", strip=True)
        if txt:
            return txt

    return "(unknown)"


def _extract_event_date_raw(soup: BeautifulSoup) -> str:
    """
    Try a few common ACS patterns.
    We keep it flexible because US pages vary and sometimes say TBD/TBA.
    """
    # Strategy 1: look for label-ish text then nearby content
    label_candidates = [
        "Event Date",
        "Date",
        "When",
        "Relay Date",
    ]
    for lab in label_candidates:
        node = soup.find(string=lambda s: isinstance(s, str) and lab.lower() in s.strip().lower())
        if node:
            # walk forward a little to find a meaningful text chunk
            cur = node.parent
            for _ in range(10):
                if not cur:
                    break
                txt = cur.get_text(" ", strip=True)
                if txt and txt.lower() not in {lab.lower()} and len(txt) <= 120:
                    # Often contains "Event Date: May 2, 2026"
                    # We return the whole line; normalize_date will interpret.
                    return txt
                cur = cur.find_next()

    # Strategy 2: meta / structured hints (sometimes)
    for sel in [
        "[data-testid*='event-date']",
        ".event-date",
        ".eventDetailsDate",
    ]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt

    # Strategy 3: global regex-ish fallback: find a date-like phrase in body text
    text = soup.get_text("\n", strip=True)
    # Keep it simple: let normalize_date do heavy lifting
    for marker in ["TBD", "TBA", "TBC"]:
        if marker in text:
            return marker
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("US event fetch failed: %s status=%s", url, res.status_code)
        return None

    soup = BeautifulSoup(res.text, "lxml")

    name = _extract_event_name(soup)
    date_raw = _extract_event_date_raw(soup)
    nd = normalize_date(date_raw, US_COUNTRY)

    emails = sorted(extract_emails(res.text))

    return EventRecord(
        country=US_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )


def scrape(fetcher: Fetcher, config: Dict[str, Any]) -> List[EventRecord]:
    """
    Entrypoint used by cli.py: {"US": us.scrape}
    Config expected in seeds.yml:

    US:
      enabled: true
      radius_miles: 50
      zip_codes:
        - "10001"
        - "30301"
    """
    radius = int(config.get("radius_miles", 50))
    zips = config.get("zip_codes") or config.get("zips") or []

    # Ensure zips are strings (YAML can parse as ints)
    zip_codes = [str(z).strip() for z in zips if str(z).strip()]

    if not zip_codes:
        fetcher.log.warning("US: no zip_codes provided; returning 0 events.")
        return []

    event_ids = discover_event_ids(fetcher, zip_codes, radius)
    fetcher.log.info("US discovered unique event_ids=%s", len(event_ids))

    urls = [event_id_to_str_url(eid) for eid in sorted(event_ids)]
    fetcher.log.info("US scraping STR urls=%s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
