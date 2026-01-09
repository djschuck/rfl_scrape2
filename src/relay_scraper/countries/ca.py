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
API_KEY = "CCSAPI"  # from luminateExtend config (confirmed via your capture)

TBD_RE = re.compile(r"\b(TBD|TBA|TBC|To Be Determined|To Be Announced|To Be Confirmed)\b", re.I)


@dataclass(frozen=True)
class _CAListSpec:
    label: str
    list_filter_text: str
    event_type2: str = ""


def _post_form(url: str, form: Dict[str, str], timeout: int = 60) -> Tuple[int, str]:
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
    results: List[Dict] = []
    seen_ids: Set[str] = set()

    offset = 0
    for _ in range(max_pages):
        form: Dict[str, str] = {
            "luminateExtend": "1.8.1",
            "api_key": API_KEY,
            "response_format": "json",
            "suppress_response_codes": "true",
            "v": "1.0",
            "method": "getTeamraisersByInfo",
            "name": "%",  # urlencode handles % properly
            "event_type": "Relay For Life",
            "list_page_size": str(page_size),
            "list_page_offset": str(offset),
            "list_sort_column": "name",
            "list_ascending": "true",
            "list_filter_column": "county",
            "list_filter_text": spec.list_filter_text,
        }
        if spec.event_type2:
            form["event_type2"] = spec.event_type2

        status, text = _post_form(TEAMRAISER_API, form, timeout=60)
        if status != 200:
            raise RuntimeError(f"CA API returned status={status} for {spec.label}")

        text = text.strip()
        try:
            payload = json.loads(text)
        except Exception as e:
            raise RuntimeError(
                f"CA API JSON parse failed for {spec.label}: {e}\nFirst 400 chars:\n{text[:400]}"
            )

        teamraisers = payload.get("getTeamraisersResponse", {}).get("teamraiser", [])
        if not teamraisers:
            break

        added = 0
        for tr in teamraisers:
            tid = str(tr.get("id") or tr.get("fr_id") or "").strip()
            if not tid or tid in seen_ids:
                continue
            seen_ids.add(tid)
            results.append(tr)
            added += 1

        if added == 0 or len(teamraisers) < page_size:
            break

        offset += page_size
        time.sleep(0.2)

    return results


def _extract_event_url(tr: Dict) -> str:
    u = (tr.get("event_url") or "").strip()
    if u.startswith("http"):
        return u

    fr_id = str(tr.get("id") or tr.get("fr_id") or "").strip()
    area = (tr.get("area") or "").strip()
    if area and fr_id:
        return f"https://support.cancer.ca/site/TR/RelayForLife/{area}?pg=entry&fr_id={fr_id}&s_locale=en_CA"
    if fr_id:
        return f"https://support.cancer.ca/site/TR?pg=entry&fr_id={fr_id}&s_locale=en_CA"
    return ""


def _extract_api_name(tr: Dict) -> str:
    # TeamRaiser API provides the real event name here
    name = (tr.get("name") or "").strip()
    return " ".join(name.split()) if name else ""


def _parse_ca_event_page(fetcher: Fetcher, url: str) -> Tuple[str, str, List[str]]:
    """
    Returns (date_raw, date_iso, emails).
    Title comes from API; we keep page parsing focused.
    """
    res = fetcher.get_text(url)
    fetcher.log.info("CA event fetch status=%s url=%s", res.status_code, url)
    if res.status_code != 200:
        return "", "", []

    html = res.text
    soup = BeautifulSoup(html, "lxml")

    page_text = soup.get_text("\n", strip=True)
    date_raw = ""

    # Prefer "Event Date" line if present
    for line in page_text.splitlines():
        l = line.strip()
        if not l:
            continue
        if "Event Date" in l:
            date_raw = l.split("Event Date", 1)[-1].strip(" :\t-")
            break

    # fallback: window around Event Date
    if not date_raw:
        idx = page_text.find("Event Date")
        if idx != -1:
            window = page_text[idx : idx + 220]
            date_raw = window.replace("\n", " ").strip()

    # fallback: short date-ish line
    if not date_raw:
        for line in page_text.splitlines():
            l = line.strip()
            if len(l) > 90:
                continue
            if any(m in l for m in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")) and any(
                ch.isdigit() for ch in l
            ):
                date_raw = l
                break

    if TBD_RE.search(date_raw or ""):
        date_iso = ""
    else:
        nd = normalize_date(date_raw, CA_COUNTRY)
        date_raw = nd.raw
        date_iso = nd.iso

    emails = sorted(extract_emails(html))
    return date_raw, date_iso, emails


def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
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

    # Build list of (url, api_name) and dedupe by url
    items: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    for tr in all_teamraisers:
        u = _extract_event_url(tr)
        if not u.startswith("http"):
            continue
        u = u.split("#", 1)[0]
        if u in seen:
            continue
        seen.add(u)
        items.append((u, _extract_api_name(tr)))

    fetcher.log.info("CA discovered event urls=%s", len(items))

    records: List[EventRecord] = []
    used_api_name = 0
    used_fallback_name = 0

    for i, (url, api_name) in enumerate(items, start=1):
        if i % 50 == 0:
            fetcher.log.info("CA progress: %s/%s", i, len(items))

        date_raw, date_iso, emails = _parse_ca_event_page(fetcher, url)

        # Name: prefer API name; fallback to page title only if API missing
        name = api_name
        if name:
            used_api_name += 1
        else:
            # fallback (rare): derive from page <title>/<h1>
            try:
                res = fetcher.get_text(url)
                soup = BeautifulSoup(res.text, "lxml")
                h1 = soup.select_one("h1")
                name = (h1.get_text(" ", strip=True) if h1 else "").strip()
                if not name:
                    title = soup.select_one("title")
                    name = (title.get_text(" ", strip=True) if title else "").strip()
                name = name or "(unknown)"
            except Exception:
                name = "(unknown)"
            used_fallback_name += 1

        records.append(
            EventRecord(
                country=CA_COUNTRY,
                event_name=name,
                date_raw=date_raw,
                date_iso=date_iso,
                emails=emails,
                source_url=url,
            )
        )

        time.sleep(0.12)

    fetcher.log.info("CA titles from API: %s (fallback: %s)", used_api_name, used_fallback_name)
    return records
