from __future__ import annotations

from typing import List, Set, Optional, Dict, Any, Tuple
import re
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

CA_COUNTRY = "CA"

# Luminate-style event pages typically look like:
# https://support.cancer.ca/site/TR/RelayForLife/RFLY_NW_odd_?pg=entry&fr_id=30883
# ...or variants of /site/TR/... with fr_id and pg=entry.
EVENT_URL_RE = re.compile(
    r"""(?P<url>
        https?://support\.cancer\.ca/site/TR/[^"'<> ]+?\bpg=entry\b[^"'<> ]*?\bfr_id=\d+
        |
        /site/TR/[^"'<> ]+?\bpg=entry\b[^"'<> ]*?\bfr_id=\d+
    )""",
    re.IGNORECASE | re.VERBOSE,
)

MONTH_RE = re.compile(
    r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b",
    re.IGNORECASE,
)

TBD_RE = re.compile(r"\b(TBD|TBA|TBC)\b", re.IGNORECASE)

# Sometimes the page includes multiple places with dates; we want the first "event date"
DATE_LABEL_RE = re.compile(r"\b(Event\s*Date|Date)\b", re.IGNORECASE)


def _canon_url(base: str, href: str) -> str:
    """Absolute URL + strip fragment."""
    u = absolutize(base, href)
    return u.split("#", 1)[0]


def _extract_event_urls_from_html(base_url: str, html: str) -> Set[str]:
    """
    Extract event URLs from both anchors and raw HTML regex.
    This is key for Canada where links can appear in scripts / unusual markup.
    """
    found: Set[str] = set()

    soup = BeautifulSoup(html, "lxml")

    # 1) Normal anchors
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        u = _canon_url(base_url, href)
        if "support.cancer.ca/site/TR/" in u and "pg=entry" in u and "fr_id=" in u:
            found.add(u)

    # 2) Raw regex over full HTML (captures links in JS, data attrs, etc.)
    for m in EVENT_URL_RE.finditer(html):
        raw = m.group("url")
        if not raw:
            continue
        u = raw.strip()

        # If it's relative, join to base
        if u.startswith("/"):
            u = urljoin(base_url, u)

        u = u.split("#", 1)[0]
        if "pg=entry" in u and "fr_id=" in u:
            found.add(u)

    return found


def _find_next_page_url(base_url: str, html: str) -> Optional[str]:
    """
    Attempt to follow pagination on PageServer indexes.
    We look for a 'Next' link or rel=next.
    """
    soup = BeautifulSoup(html, "lxml")

    # rel=next
    a = soup.select_one("a[rel='next'][href]")
    if a:
        return _canon_url(base_url, a["href"])

    # visible "Next" link
    for a in soup.select("a[href]"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if txt in {"next", "next >", ">", "›"}:
            return _canon_url(base_url, a.get("href") or "")

    return None


def discover_event_urls(fetcher: Fetcher, index_urls: List[str], max_pages_per_index: int = 20) -> List[str]:
    """
    Fetch each index URL, extract event links, and follow pagination (if any).
    """
    all_found: Set[str] = set()

    for idx in index_urls:
        fetcher.log.info("CA index: %s", idx)

        seen_pages: Set[str] = set()
        page_url = idx

        for page_num in range(1, max_pages_per_index + 1):
            if page_url in seen_pages:
                fetcher.log.info("CA index pagination loop detected; stopping at page %s", page_num)
                break
            seen_pages.add(page_url)

            res = fetcher.get_text(page_url)
            fetcher.log.info("CA index fetch status=%s url=%s", res.status_code, page_url)
            if res.status_code != 200:
                # If the site blocks bots, this will show up here
                fetcher.log.warning("CA index non-200 (%s) for %s", res.status_code, page_url)
                break

            html = res.text
            found_here = _extract_event_urls_from_html(page_url, html)
            fetcher.log.info("CA index page %s found event links: %s", page_num, len(found_here))
            all_found.update(found_here)

            next_url = _find_next_page_url(page_url, html)
            if not next_url or next_url == page_url:
                break
            page_url = next_url

    return sorted(all_found)


def _extract_event_name(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t
    title = soup.select_one("title")
    if title:
        t = title.get_text(" ", strip=True)
        if t:
            # Often "XYZ - Canadian Cancer Society" etc
            return t.strip()
    return "(unknown)"


def _extract_date_candidate(text: str) -> str:
    """
    From full page text, attempt to find an 'event date' candidate string.
    Canada pages can be template-y; we aim for something normalize_date() can handle.
    """
    if not text:
        return ""

    # TBD/TBA/TBC
    m = TBD_RE.search(text)
    if m:
        return m.group(1).upper()

    # Prefer lines near "Event Date"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        if DATE_LABEL_RE.search(ln):
            # Try same line after colon
            if ":" in ln:
                tail = ln.split(":", 1)[1].strip()
                if tail:
                    return tail
            # Try next 1-2 lines
            for j in (i + 1, i + 2):
                if j < len(lines):
                    cand = lines[j]
                    if MONTH_RE.search(cand) or re.search(r"\b20\d{2}\b", cand):
                        return cand

    # Fallback: first line that looks date-ish (month + year)
    for ln in lines:
        if MONTH_RE.search(ln) and re.search(r"\b20\d{2}\b", ln):
            return ln

    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    fetcher.log.info("CA event fetch status=%s url=%s", res.status_code, url)
    if res.status_code != 200:
        return None

    html = res.text
    soup = BeautifulSoup(html, "lxml")

    name = _extract_event_name(soup)

    # Date extraction from visible text
    page_text = soup.get_text("\n", strip=True)
    date_raw = _extract_date_candidate(page_text)

    nd = normalize_date(date_raw, CA_COUNTRY)
    emails = sorted(extract_emails(html))

    # Keep record if anything meaningful exists
    if (not name or name == "(unknown)") and not emails and not (nd.raw or nd.iso):
        return None

    return EventRecord(
        country=CA_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd.raw,
        date_iso=nd.iso,
        emails=emails,
        source_url=url,
    )


def scrape(fetcher: Fetcher, config: Dict[str, Any]) -> List[EventRecord]:
    """
    Config expected (either):
      CA:
        index_urls: [...]
        max_pages_per_index: 20
    """
    index_urls = config.get("index_urls", [])
    max_pages = int(config.get("max_pages_per_index", 20))

    if not index_urls:
        fetcher.log.warning("CA: no index_urls provided; returning 0 events.")
        return []

    urls = discover_event_urls(fetcher, index_urls=index_urls, max_pages_per_index=max_pages)
    fetcher.log.info("CA discovered event urls=%s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
