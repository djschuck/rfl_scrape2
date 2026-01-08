from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

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
    if not zip_codes:
        return set()

    fetcher.log.info("US probing fundraising API using zip=%s radius=%s", zip_codes[0], radius_miles)
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
    h1 = soup.select_one("h1")
    if h1:
        txt = h1.get_text(" ", strip=True)
        if txt:
            return txt

    title = soup.select_one("title")
    if title:
        txt = title.get_text(" ", strip=True)
        if txt:
            return txt

    return "(unknown)"


def _jsonld_candidates(html: str) -> List[Dict[str, Any]]:
    """
    Parse any application/ld+json blocks, returning a list of dict objects.
    Fail-closed: returns [] if parsing fails.
    """
    soup = BeautifulSoup(html, "lxml")
    blocks = soup.select("script[type='application/ld+json']")
    out: List[Dict[str, Any]] = []
    for b in blocks:
        raw = (b.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                out.append(data)
            elif isinstance(data, list):
                out.extend([x for x in data if isinstance(x, dict)])
        except Exception:
            continue
    return out


def _extract_date_from_jsonld(html: str) -> Optional[str]:
    """
    Try to find startDate/endDate in JSON-LD.
    Returns a string (ISO date/time or similar) or None.
    """
    for obj in _jsonld_candidates(html):
        # Common schema.org keys
        start = obj.get("startDate") or obj.get("start_date") or obj.get("start")
        end = obj.get("endDate") or obj.get("end_date") or obj.get("end")
        if isinstance(start, str) and start.strip():
            if isinstance(end, str) and end.strip():
                return f"{start.strip()} - {end.strip()}"
            return start.strip()
    return None


_EVENT_DATE_LINE_RE = re.compile(
    r"(Event\s*Date|Relay\s*Date|Date)\s*[:\-]\s*([^\r\n<]{3,120})",
    re.IGNORECASE,
)

# A loose month-name matcher for fallback scanning
_MONTH_RE = re.compile(
    r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b",
    re.IGNORECASE,
)


def _extract_date_from_visible_text(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract date-ish content from visible text lines.
    """
    text = soup.get_text("\n", strip=True)

    # Handle explicit TBD/TBA/TBC
    for marker in ("TBD", "TBA", "TBC"):
        if re.search(rf"\b{marker}\b", text):
            return marker

    # Look for "Event Date: ...."
    m = _EVENT_DATE_LINE_RE.search(text)
    if m:
        return m.group(2).strip()

    # Fallback: find a line that looks date-like (contains a month name)
    for line in text.splitlines():
        line = line.strip()
        if 6 <= len(line) <= 120 and _MONTH_RE.search(line):
            # Avoid obvious non-date lines
            if "registration" in line.lower():
                continue
            return line

    return None


def _extract_event_date_raw(html: str, soup: BeautifulSoup, event_name: str) -> str:
    """
    Date extraction specifically tuned for ACS STR pages.
    """
    # 1) JSON-LD is often the cleanest
    d = _extract_date_from_jsonld(html)
    if d and d.strip() and d.strip() != event_name.strip():
        return d.strip()

    # 2) Visible text heuristic
    d = _extract_date_from_visible_text(soup)
    if d and d.strip() and d.strip() != event_name.strip():
        return d.strip()

    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("US event fetch failed: %s status=%s", url, res.status_code)
        return None

    html = res.text
    soup = BeautifulSoup(html, "lxml")

    name = _extract_event_name(soup)
    date_raw = _extract_event_date_raw(html, soup, name)
    nd = normalize_date(date_raw, US_COUNTRY)

    emails = sorted(extract_emails(html))

    return EventRecord(
        country=US_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )


def scrape(fetcher: Fetcher, config: Dict[str, Any]) -> List[EventRecord]:
    fetcher.log.info("US raw config received: %r", config)
    if isinstance(config, dict):
        fetcher.log.info("US config keys: %s", sorted(config.keys()))

    us_cfg = config
    if isinstance(config, dict) and "US" in config and isinstance(config["US"], dict):
        us_cfg = config["US"]

    radius = int((us_cfg or {}).get("radius_miles", 50))

    zips = (
        (us_cfg or {}).get("zip_codes")
        or (us_cfg or {}).get("zipCodes")
        or (us_cfg or {}).get("zips")
        or []
    )
    zip_codes = [str(z).strip() for z in zips if str(z).strip()]

    fetcher.log.info("US parsed zip_codes count=%s sample=%s", len(zip_codes), zip_codes[:5])

    if not zip_codes:
        fetcher.log.warning(
            "US: no zip_codes provided; returning 0 events. Available keys=%s",
            sorted((us_cfg or {}).keys()) if isinstance(us_cfg, dict) else type(us_cfg),
        )
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
