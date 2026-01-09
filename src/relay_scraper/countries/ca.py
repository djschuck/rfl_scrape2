from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
import json
import re
import time
import urllib.parse
import urllib.request

from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date

CA_COUNTRY = "CA"

TEAMRAISER_API = "https://support.cancer.ca/site/CRTeamraiserAPI"
API_KEY = "CCSAPI"  # found in the index HTML (luminateExtend apiKey)

# Conservative: CA event pages usually have English dates; normalize_date already handles.
# We'll still treat TBD/TBA/TBC as "raw only".
TBD_RE = re.compile(r"\b(TBD|TBA|TBC|To Be Determined|To Be Announced|To Be Confirmed)\b", re.I)


@dataclass(frozen=True)
class _CAListSpec:
    label: str
    # These are the key filters observed in the captured HTML.
    # Community: list_filter_text=RFL_
    # Youth/Schools: list_filter_text=RFLY_ and event_type2=Youth
    list_filter_text: str
    event_type2: str = ""


def _post_form(url: str, form: Dict[str, str], timeout: int = 60) -> Tuple[int, str]:
    """
    Lightweight POST (no external deps).
    Returns (status_code, response_text).
    """
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        body = resp.read().decode("utf-8", errors="replace")
        return status, body


def _teamraiser_by_info(spec: _CAListSpec, page_size: int = 500, max_pages: int = 50) -> List[Dict]:
    """
    Calls CRTeamraiserAPI getTeamraisersByInfo with pagination.
    Returns the raw teamraiser objects.
    """
    results: List[Dict] = []
    seen_ids: Set[str] = set()

    # Pagination: list_page_offset is supported by Luminate.
    offset = 0
    for page in range(max_pages):
        # Base params that luminateExtend normally adds for you:
        form: Dict[str, str] = {
            "luminateExtend": "1.8.1",
            "api_key": API_KEY,
            "response_format": "json",
            "suppress_response_codes": "true",
            "v": "1.0",
            "method": "getTeamraisersByInfo",
            "name": "%",  # name=%25 in HTML, but urlencode will handle it
            "event_type": "Relay For Life",
            "list_page_size": str(page_size),
            "list_page_offset": str(offset),
            "list_sort_column": "name",
            "list_ascending": "true",
        }

        # These filters were observed directly in your captured index snapshots:
        form["list_filter_column"] = "county"
        form["list_filter_text"] = spec.list_filter_text

        if spec.event_type2:
            form["event_type2"] = spec.event_type2

        status, text = _post_form(TEAMRAISER_API, form, timeout=60)
        if status != 200:
            # If the endpoint changes, fail loudly in logs upstream.
            raise RuntimeError(f"CA API returned status={status} for {spec.label}")

        # Luminate responses are JSON; sometimes wrapped in leading/trailing whitespace.
        text = text.strip()
        try:
            payload = json.loads(text)
        except Exception as e:
            raise RuntimeError(f"CA API JSON parse failed for {spec.label}: {e}\nFirst 400 chars:\n{text[:400]}")

        teamraisers = (
            payload.get("getTeamraisersResponse", {}).get("teamraiser", [])
            if isinstance(payload, dict)
            else []
        )

        # No results => stop
        if not teamraisers:
            break

        added_this_page = 0
        for tr in teamraisers:
            # Prefer stable numeric id
            tid = str(tr.get("id") or tr.get("fr_id") or "").strip()
            if not tid:
                continue
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            results.append(tr)
            added_this_page += 1

        # If we didn't add anything new, or got fewer than page_size, stop.
        if added_this_page == 0 or len(teamraisers) < page_size:
            break

        offset += page_size
        time.sleep(0.2)

    return results


def _extract_event_url(tr: Dict) -> str:
    """
    Prefer event_url returned by the API (it includes pg=entry&fr_id=...).
    If missing, construct a reasonable fallback using area + id.
    """
    u = (tr.get("event_url") or "").strip()
    if u.startswith("http"):
        return u

    # fallback
    fr_id = str(tr.get("id") or tr.get("fr_id") or "").strip()
    area = (tr.get("area") or "").strip()
    if area and fr_id:
        # Many CCS pages include s_locale=en_CA, but it's optional.
        return f"https://support.cancer.ca/site/TR/RelayForLife/{area}?pg=entry&fr_id={fr_id}&s_locale=en_CA"

    if fr_id:
        # last resort: this sometimes still works if CCS routes the fr_id
        return f"https://support.cancer.ca/site/TR?pg=entry&fr_id={fr_id}&s_locale=en_CA"

    return ""


def _parse_ca_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    fetcher.log.info("CA event fetch status=%s url=%s", res.status_code, url)
    if res.status_code != 200:
        return None

    html = res.text
    soup = BeautifulSoup(html, "lxml")

    # Name
    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()
    if not name:
        title = soup.select_one("title")
        name = (title.get_text(" ", strip=True) if title else "").strip()

    # Date: CA TeamRaiser pages usually include "Event Date" or similar text blocks.
    page_text = soup.get_text("\n", strip=True)
    date_raw = ""

    # Try to find a line containing "Event Date"
    for line in page_text.splitlines():
        l = line.strip()
        if not l:
            continue
        if "Event Date" in l:
            # Sometimes it's "Event Date: June 1, 2026"
            date_raw = l.split("Event Date", 1)[-1].strip(" :\t-")
            break

    # Fallback: use normalize_date on a small window around "Event Date"
    if not date_raw:
        idx = page_text.find("Event Date")
        if idx != -1:
            window = page_text[idx : idx + 200]
            date_raw = window.replace("\n", " ").strip()

    # Another fallback: pick a short line that looks date-y
    if not date_raw:
        for line in page_text.splitlines():
            l = line.strip()
            if len(l) > 90:
                continue
            # contains month name and a digit
            if any(m in l for m in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")) and any(
                ch.isdigit() for ch in l
            ):
                date_raw = l
                break

    # Normalize (allow TBD/TBA/TBC to pass through as raw only)
    if TBD_RE.search(date_raw or ""):
        nd_raw = date_raw
        nd_iso = ""
    else:
        nd = normalize_date(date_raw, CA_COUNTRY)
        nd_raw = nd.raw
        nd_iso = nd.iso

    emails = sorted(extract_emails(html))

    if not name and not emails and not (nd_raw or nd_iso):
        return None

    return EventRecord(
        country=CA_COUNTRY,
        event_name=name or "(unknown)",
        date_raw=nd_raw,
        date_iso=nd_iso,
        emails=emails,
        source_url=url,
    )


def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    """
    Canada:
    - Discover events via CRTeamraiserAPI getTeamraisersByInfo (community + youth/schools)
    - For each returned event_url, scrape event page for name/date/emails.
    """
    index_urls = config.get(
        "index_urls",
        [
            "https://support.cancer.ca/site/PageServer?pagename=RFL_NW_Events",
            "https://support.cancer.ca/site/PageServer?pagename=RFLY_NW_Events",
        ],
    )

    fetcher.log.info("CA indexes configured: %s", index_urls)

    specs = [
        _CAListSpec(label="community", list_filter_text="RFL_"),
        _CAListSpec(label="youth", list_filter_text="RFLY_", event_type2="Youth"),
    ]

    all_teamraisers: List[Dict] = []
    for spec in specs:
        fetcher.log.info("CA API discovery starting: %s (filter=%s)", spec.label, spec.list_filter_text)
        trs = _teamraiser_by_info(spec, page_size=500, max_pages=50)
        fetcher.log.info("CA API discovery done: %s teamraisers=%s", spec.label, len(trs))
        all_teamraisers.extend(trs)

    # Deduplicate to unique event URLs
    urls: List[str] = []
    seen: Set[str] = set()
    for tr in all_teamraisers:
        u = _extract_event_url(tr)
        if not u or not u.startswith("http"):
            continue
        u = u.split("#")[0]
        if u in seen:
            continue
        seen.add(u)
        urls.append(u)

    fetcher.log.info("CA discovered event urls=%s", len(urls))

    records: List[EventRecord] = []
    for i, u in enumerate(urls, start=1):
        if i % 50 == 0:
            fetcher.log.info("CA progress: %s/%s", i, len(urls))
        r = _parse_ca_event_page(fetcher, u)
        if r:
            records.append(r)

        # be polite
        time.sleep(0.15)

    return records
