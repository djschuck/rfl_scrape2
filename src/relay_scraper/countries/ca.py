from __future__ import annotations

from typing import List, Set, Optional, Dict, Any
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize

CA_COUNTRY = "CA"

# Robustly capture fr_id in many encodings/forms:
# fr_id=123
# fr_id%3D123
# fr_id\u003d123   (in JSON strings)
# fr_id&#61;123     (HTML entity "=")
# fr_id&#x3D;123
# "fr_id":123
#
# Note: keep this regex simple and NOT in VERBOSE mode to avoid compile pitfalls.
FR_ID_RE = re.compile(
    r'(?:fr_id(?:=|%3[Dd]|\\u003[dD]|&#61;|&#x3[Dd];)\s*("?)(\d+)\1|'
    r'"fr_id"\s*:\s*("?)(\d+)\3)',
    re.IGNORECASE,
)

# Direct event page URLs sometimes appear explicitly
EVENT_URL_RE = re.compile(
    r'(?:(https?://support\.cancer\.ca/site/TR/[^"\'<> ]+?pg=entry[^"\'<> ]*?fr_id=\d+)'
    r'|(/site/TR/[^"\'<> ]+?pg=entry[^"\'<> ]*?fr_id=\d+))',
    re.IGNORECASE,
)

MONTH_RE = re.compile(
    r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b",
    re.IGNORECASE,
)
TBD_RE = re.compile(r"\b(TBD|TBA|TBC)\b", re.IGNORECASE)
DATE_LABEL_RE = re.compile(r"\b(Event\s*Date|Date)\b", re.IGNORECASE)

DEFAULT_COMMUNITY_ENTRY_TEMPLATE = (
    "https://support.cancer.ca/site/TR/RelayForLife/RFL_NW_even_?pg=entry&fr_id={fr_id}&s_locale=en_CA"
)
DEFAULT_SCHOOLS_ENTRY_TEMPLATE = (
    "https://support.cancer.ca/site/TR/RelayForLife/RFLY_NW_odd_?pg=entry&fr_id={fr_id}&s_locale=en_CA"
)


def _canon_url(base: str, href: str) -> str:
    u = absolutize(base, href)
    return u.split("#", 1)[0]


def _extract_event_urls_from_html(base_url: str, html: str) -> Set[str]:
    """Extract explicit event URLs from anchors + raw regex."""
    found: Set[str] = set()
    soup = BeautifulSoup(html, "lxml")

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        u = _canon_url(base_url, href)
        if "support.cancer.ca/site/TR/" in u and "pg=entry" in u and "fr_id=" in u:
            found.add(u)

    for m in EVENT_URL_RE.finditer(html):
        abs_u = m.group(1)
        rel_u = m.group(2)
        u = abs_u or rel_u
        if not u:
            continue
        if u.startswith("/"):
            u = urljoin(base_url, u)
        found.add(u.split("#", 1)[0])

    return found


def _extract_fr_ids_from_html(html: str) -> Set[str]:
    """Extract fr_id values even if links aren’t present as <a href>."""
    ids: Set[str] = set()
    for m in FR_ID_RE.finditer(html):
        # Pattern has two alternative capture locations
        if m.group(2):
            ids.add(m.group(2))
        elif m.group(4):
            ids.add(m.group(4))
    return ids


def _build_entry_url(template: str, fr_id: str) -> str:
    return template.format(fr_id=fr_id)


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
            return t.strip()
    return "(unknown)"


def _extract_date_candidate(text: str) -> str:
    if not text:
        return ""

    m = TBD_RE.search(text)
    if m:
        return m.group(1).upper()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for i, ln in enumerate(lines):
        if DATE_LABEL_RE.search(ln):
            if ":" in ln:
                tail = ln.split(":", 1)[1].strip()
                if tail:
                    return tail
            for j in (i + 1, i + 2):
                if j < len(lines):
                    cand = lines[j]
                    if MONTH_RE.search(cand) or re.search(r"\b20\d{2}\b", cand):
                        return cand

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

    page_text = soup.get_text("\n", strip=True)
    date_raw = _extract_date_candidate(page_text)
    nd = normalize_date(date_raw, CA_COUNTRY)

    emails = sorted(extract_emails(html))

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
    index_urls = config.get("index_urls", [])
    if not index_urls:
        fetcher.log.warning("CA: no index_urls provided; returning 0 events.")
        return []

    community_template = config.get("community_entry_template", DEFAULT_COMMUNITY_ENTRY_TEMPLATE)
    schools_template = config.get("schools_entry_template", DEFAULT_SCHOOLS_ENTRY_TEMPLATE)

    all_urls: Set[str] = set()

    for idx in index_urls:
        fetcher.log.info("CA index: %s", idx)
        res = fetcher.get_text(idx)
        fetcher.log.info("CA index fetch status=%s url=%s", res.status_code, idx)
        if res.status_code != 200:
            continue

        html = res.text

        explicit = _extract_event_urls_from_html(idx, html)
        fetcher.log.info("CA index explicit event links found: %s", len(explicit))
        all_urls.update(explicit)

        fr_ids = _extract_fr_ids_from_html(html)
        fetcher.log.info("CA index fr_id values found: %s", len(fr_ids))

        use_template = schools_template if "pagename=RFLY_" in idx else community_template

        built = 0
        for fr_id in fr_ids:
            all_urls.add(_build_entry_url(use_template, fr_id))
            built += 1
        fetcher.log.info("CA index built entry urls from fr_id: %s", built)

    urls = sorted(all_urls)
    fetcher.log.info("CA discovered event urls=%s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)

    return records
