from __future__ import annotations

from typing import List, Set, Optional
from bs4 import BeautifulSoup

import re
from datetime import datetime

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

AU_COUNTRY = "AU"

# Matches: "3rd May 2025", "3 May 2025", "03 May 2025"
# (Full month names)
AU_DATE_RE_FULL = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"(\d{4})\b",
    re.IGNORECASE,
)

# Matches abbreviated months too: "3rd Sep 2025", "3 Sep 2025"
AU_DATE_RE_ABBR = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+"
    r"(\d{4})\b",
    re.IGNORECASE,
)

# Optional: sometimes AU pages include day-of-week before the date (we ignore it)
# e.g. "Sat 3rd May 2025 ..."
DOW_PREFIX_RE = re.compile(r"^\s*(Mon|Tue|Tues|Wed|Thu|Thur|Fri|Sat|Sun)\b\s+", re.IGNORECASE)


def _parse_au_start_date_from_text(text: str) -> str:
    """
    Given a messy AU date block (which can include start/end times, location, closed status),
    extract ONLY the first (start) date as 'D Month YYYY' if possible.
    Returns "" if no date found.
    """
    if not text:
        return ""

    t = " ".join(text.split())  # normalize whitespace
    t = DOW_PREFIX_RE.sub("", t)  # drop leading day-of-week if present

    # Prefer explicit TBD/TBA/TBC detection if it appears (rare for AU but safe)
    for marker in ("TBD", "TBA", "TBC"):
        if re.search(rf"\b{marker}\b", t, re.IGNORECASE):
            return marker

    m = AU_DATE_RE_FULL.search(t)
    if m:
        day, month, year = m.groups()
        return f"{int(day)} {month.title()} {year}"

    m = AU_DATE_RE_ABBR.search(t)
    if m:
        day, month, year = m.groups()
        # Normalize "Sept" -> "Sep" for strptime compatibility
        mon = month.title()
        if mon == "Sept":
            mon = "Sep"
        return f"{int(day)} {mon} {year}"

    return ""


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


def _extract_event_name(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()
    if not name:
        title = soup.select_one("title")
        name = (title.get_text(" ", strip=True) if title else "").strip()
    return name or "(unknown)"


def _extract_date_blob_near_event_date_label(soup: BeautifulSoup) -> str:
    """
    Extract the textual blob that includes the date information.
    AU pages often have a visible "Event Date" label with the value nearby.
    We return the blob and then later isolate the first actual date.
    """
    label = soup.find(string=lambda s: isinstance(s, str) and "Event Date" in s)
    if not label:
        return ""

    parent = label.parent
    container = parent

    # Walk up a few levels to reach the card/section that contains label + value
    for _ in range(5):
        if container and container.parent and container.name not in ("html", "body"):
            container = container.parent

    if not container:
        return ""

    text = container.get_text(" ", strip=True)
    if not text:
        return ""

    # Try to take substring after "Event Date"
    if "Event Date" in text:
        return text.split("Event Date", 1)[1].strip(" :|-")

    return text


def _fallback_find_date_line(soup: BeautifulSoup) -> str:
    """
    Fallback: scan visible text lines and pick a line that looks date-ish.
    We do NOT restrict by length because AU templates can pack lots of info in one line.
    """
    page_text = soup.get_text("\n", strip=True)

    # If we find any line with a month name + year + digit, try parsing it
    month_tokens = (
        "January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December",
        "Jan", "Feb", "Mar", "Apr", "Jun", "Jul", "Aug", "Sep", "Sept", "Oct", "Nov", "Dec",
    )

    for line in page_text.splitlines():
        l = " ".join(line.split())
        if not l:
            continue
        if any(tok in l for tok in month_tokens) and any(ch.isdigit() for ch in l):
            # Prefer lines that include a year
            if re.search(r"\b20\d{2}\b", l):
                return l

    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "lxml")

    name = _extract_event_name(soup)

    # 1) Pull the blob near "Event Date" if possible
    blob = _extract_date_blob_near_event_date_label(soup)

    # 2) If missing, do a broader fallback scan
    if not blob:
        blob = _fallback_find_date_line(soup)

    # 3) Extract ONLY the first (start) date from the blob
    start_date_raw = _parse_au_start_date_from_text(blob)

    # 4) Normalize
    # If we extracted a clean start date, normalize that.
    # Otherwise, pass the blob through normalize_date (may still recover something).
    candidate = start_date_raw or blob
    nd = normalize_date(candidate, AU_COUNTRY)

    emails = sorted(extract_emails(res.text))

    # Keep row if we have at least a name or email or date
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
