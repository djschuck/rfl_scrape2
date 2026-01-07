from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple

import requests

from relay_scraper.us_api import probe_variant, search_events


def discover_us_event_ids(zip_codes: List[str], radius_miles: int) -> Set[str]:
    """
    Use the ACS fundraising API to discover Relay For Life eventIds.
    """
    s = requests.Session()

    # Probe once using the first ZIP to lock down param names
    variant = probe_variant(zip_codes[0], radius_miles, session=s)

    event_ids: Set[str] = set()
    for z in zip_codes:
        results = search_events(z, radius_miles, variant, session=s)
        for row in results:
            eid = str(row.get("eventId") or "").strip()
            if eid.isdigit():
                event_ids.add(eid)

    return event_ids


def event_id_to_str_url(event_id: str) -> str:
    return f"https://secure.acsevents.org/site/STR?pg=entry&fr_id={event_id}"
