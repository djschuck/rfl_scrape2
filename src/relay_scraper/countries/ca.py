from __future__ import annotations

from typing import List
from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.models import EventRecord

CA_COUNTRY = "CA"

def scrape(fetcher: Fetcher, config: dict) -> List[EventRecord]:
    # Many CA pages may be blocked by robots / bot protections depending on environment.
    # This driver is intentionally scaffolded.
    # Implement only if allowed by site policy; otherwise skip.
    _ = (fetcher, config)
    return []
