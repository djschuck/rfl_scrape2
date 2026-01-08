from __future__ import annotations

import argparse
import sys
import os
import logging
from typing import Dict, List

import yaml
from rich.console import Console
from rich.table import Table

from relay_scraper.core.fetch import Fetcher
from relay_scraper.core.output import write_csv, write_json
from relay_scraper.core.models import EventRecord

from relay_scraper.countries import au, uk, us, ca

console = Console()

COUNTRY_DRIVERS = {
    "AU": au.scrape,
    "UK": uk.scrape,
    "US": us.scrape,
    "CA": ca.scrape,
}

def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape Relay For Life events globally")
    p.add_argument("--config", default="seeds.yml", help="Path to seeds.yml")
    p.add_argument("--countries", default="AU,UK", help="Comma-separated list, e.g. AU,UK,US,CA")
    p.add_argument("--out", default="out/events.csv", help="CSV output path")
    p.add_argument("--json", default="", help="Optional JSON output path")
    p.add_argument("--log", default="out/scrape.log", help="Log output path")
    p.add_argument("--no-cache", action="store_true", help="Disable HTTP cache")
    return p.parse_args(argv)

def setup_logging(log_path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    logger = logging.getLogger("relay_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # file
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

def render_preview(records: List[EventRecord], limit: int = 20) -> None:
    t = Table(title=f"Preview (first {min(limit, len(records))} of {len(records)})")
    t.add_column("country")
    t.add_column("event_name")
    t.add_column("date")
    t.add_column("emails")
    t.add_column("source_url")
    for r in records[:limit]:
        t.add_row(r.country, r.event_name, (r.date_iso or r.date_raw), "; ".join(r.emails), r.source_url)
    console.print(t)

def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    log = setup_logging(args.log)

    cfg = load_config(args.config)

    # Support both:
    # 1) New format: top-level keys per country (US:, AU:, UK:, CA:)
    # 2) Old format: countries: { US: {...}, AU: {...}, ... }
    countries_cfg = cfg.get("countries")
    if not isinstance(countries_cfg, dict):
        # Fall back to top-level layout
        countries_cfg = cfg

    selected = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    unknown = [c for c in selected if c not in COUNTRY_DRIVERS]
    if unknown:
        console.print(f"[red]Unknown country codes:[/red] {unknown}")
        return 2

    fetcher = Fetcher(log=log, use_cache=not args.no_cache)
    all_records: List[EventRecord] = []

    try:
        for c in selected:
            driver = COUNTRY_DRIVERS[c]
            # Prefer cfg["countries"][c] if present, otherwise fall back to top-level cfg[c]
            c_cfg = (countries_cfg.get(c) if isinstance(countries_cfg, dict) else None) or cfg.get(c, {}) or {}
            log.info("Scraping %s ...", c)
            recs = driver(fetcher, c_cfg)
            log.info("%s records: %s", c, len(recs))
            all_records.extend(recs)
    finally:
        fetcher.close()

    # Deduplicate by (country, source_url)
    dedup = {}
    for r in all_records:
        dedup[(r.country, r.source_url)] = r
    records = sorted(dedup.values(), key=lambda r: (r.country, r.event_name.lower()))

    write_csv(args.out, records)
    if args.json:
        write_json(args.json, records)

    render_preview(records)
    log.info("Wrote CSV: %s", args.out)
    if args.json:
        log.info("Wrote JSON: %s", args.json)
    log.info("Wrote LOG: %s", args.log)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
