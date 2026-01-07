from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import dateparser

TBA_MARKERS = {"tba", "tbd", "tbc", "to be announced", "to be confirmed", "to be determined"}

ORDINAL_RE = re.compile(r"(\d+)(st|nd|rd|th)\b", re.I)

@dataclass(frozen=True)
class NormalizedDate:
    raw: str
    iso: str

def normalize_date(raw: str, country: str) -> NormalizedDate:
    raw_clean = " ".join((raw or "").split()).strip()
    if not raw_clean:
        return NormalizedDate(raw=raw_clean, iso="")

    low = raw_clean.lower()
    if any(m in low for m in TBA_MARKERS):
        return NormalizedDate(raw=raw_clean, iso="")

    # remove ordinals: "2nd May" -> "2 May"
    no_ord = ORDINAL_RE.sub(r"\1", raw_clean)

    # country-specific date order hints
    # AU/UK: DMY, US: MDY. Default DMY.
    date_order = "DMY"
    if country.upper() == "US":
        date_order = "MDY"

    dt = dateparser.parse(
        no_ord,
        settings={
            "DATE_ORDER": date_order,
            "PREFER_DAY_OF_MONTH": "first",
            "RETURN_AS_TIMEZONE_AWARE": False,
        },
        languages=["en"],
    )

    if not dt:
        return NormalizedDate(raw=raw_clean, iso="")

    return NormalizedDate(raw=raw_clean, iso=dt.date().isoformat())
