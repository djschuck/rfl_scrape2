from __future__ import annotations

import os
import re
from typing import List, Set, Optional
from bs4 import BeautifulSoup

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord
from relay_scraper.core.extract import extract_emails
from relay_scraper.core.normalize import normalize_date
from relay_scraper.core.utils import absolutize, is_http_url

UK_COUNTRY = "UK"

# Event links on index pages are typically:
#   /product/relay-life/<slug>
UK_EVENT_PATH_RE = re.compile(r"^/product/relay-life/[^/?#]+$")


def _maybe_dump_debug_html(page: int, html: str) -> None:
    """
    When Actions returns a layout we don't expect, dump the HTML to out/ for inspection.
    """
    debug_dir = os.environ.get("RELAY_DEBUG_DIR", "out/debug")
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, f"uk_index_page_{page}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def discover_event_urls(
    fetcher: Fetcher,
    template: str,
    page_start: int,
    page_max: int,
    stop_when_no_new: bool,
) -> List[str]:
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

        # Strategy: find hrefs that look like /product/relay-life/<slug>
        matched_this_page = 0
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            # Only keep clean event slugs, not paging/filter links
            if UK_EVENT_PATH_RE.match(href):
                u = absolutize(idx_url, href)
                if is_http_url(u):
                    found.add(u.split("#")[0])
                    matched_this_page += 1

        after = len(found)
        fetcher.log.info("UK page=%s discovered_total=%s (+%s) matched_on_page=%s",
                         p, after, after - before, matched_this_page)

        # If we got zero matches on early pages, dump HTML for debugging
        if matched_this_page == 0 and p <= page_start + 2:
            _maybe_dump_debug_html(p, res.text)
            fetcher.log.warning("UK page=%s had 0 matches; dumped HTML to out/debug/", p)

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
    We locate the 'Event date' label and return the next meaningful text.
    """
    label = soup.find(string=lambda s: isinstance(s, str) and s.strip().lower() == "event date")
    if not label:
        return ""

    node = label.parent
    # Find next tag with non-empty text
    nxt = node.find_next()
    while nxt:
        txt = nxt.get_text(" ", strip=True)
        if txt and txt.strip().lower() not in {"event date", "event time"}:
            return txt.strip()
        nxt = nxt.find_next()
    return ""


def parse_event_page(fetcher: Fetcher, url: str) -> Optional[EventRecord]:
    res = fetcher.get_text(url)
    if res.status_code != 200:
        fetcher.log.warning("UK event fetch failed: %s status=%s", url, res.status_code)
        return None

    soup = BeautifulSoup(res.text, "lxml")

    h1 = soup.select_one("h1")
    name = (h1.get_text(" ", strip=True) if h1 else "").strip()

    date_raw = extract_event_date(soup)
    nd = normalize_date(date_raw, UK_COUNTRY)

    emails = sorted(extract_emails(res.text))

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
    page_start = int(config.get("page_start", 1))
    page_max = int(config.get("page_max", 200))
    stop_when_no_new = bool(config.get("stop_when_no_new", True))

    urls = discover_event_urls(
        fetcher=fetcher,
        template=template,
        page_start=page_start,
        page_max=page_max,
        stop_when_no_new=stop_when_no_new,
    )

    fetcher.log.info("UK total event urls discovered: %s", len(urls))

    records: List[EventRecord] = []
    for u in urls:
        r = parse_event_page(fetcher, u)
        if r:
            records.append(r)
    return records
