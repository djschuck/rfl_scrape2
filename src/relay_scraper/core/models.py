from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

@dataclass(frozen=True)
class EventRecord:
    country: str
    event_name: str
    date_raw: str
    date_iso: str
    emails: List[str]
    source_url: str

    def to_row(self) -> Dict[str, Any]:
        # CSV-friendly
        return {
            "country": self.country,
            "event_name": self.event_name,
            "date": self.date_iso or self.date_raw,
            "emails": "; ".join(self.emails),
            "source_url": self.source_url,
        }

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)
