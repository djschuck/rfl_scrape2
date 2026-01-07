from __future__ import annotations

import csv
import json
import os
from typing import Iterable, List

from .models import EventRecord

def write_csv(path: str, records: List[EventRecord]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["country", "event_name", "date", "emails", "source_url"],
        )
        writer.writeheader()
        for r in records:
            writer.writerow(r.to_row())

def write_json(path: str, records: List[EventRecord]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump([r.to_json() for r in records], f, ensure_ascii=False, indent=2)
