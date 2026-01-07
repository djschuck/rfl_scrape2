# scripts/us_summarize_network.py

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

FR_ID_RE = re.compile(r"(?:\?|&)fr_id=(\d+)")
STR_RE = re.compile(r"/site/STR\?[^\"']*fr_id=\d+[^\"']*")
TR_RE = re.compile(r"/site/TR/[^\"']*\?[^\"']*fr_id=\d+[^\"']*")

def main() -> int:
    p = argparse.ArgumentParser(description="Summarize ACS ZIP-search network capture.")
    p.add_argument("--in", dest="inp", default="out/us_network.jsonl")
    p.add_argument("--top", type=int, default=25)
    args = p.parse_args()

    path = Path(args.inp)
    if not path.exists():
        raise SystemExit(f"Not found: {path}")

    responses = []
    fr_ids = set()
    str_urls = set()
    tr_urls = set()

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") != "response":
                continue
            url = rec.get("url", "")
            status = rec.get("status")
            body = rec.get("body") or ""
            if body:
                for m in FR_ID_RE.findall(body):
                    fr_ids.add(m)
                for m in STR_RE.findall(body):
                    str_urls.add(m)
                for m in TR_RE.findall(body):
                    tr_urls.add(m)
            responses.append((status, url, len(body), rec.get("interesting", False)))

    # show top by body size (often the search payload)
    responses.sort(key=lambda x: x[2], reverse=True)
    print("\nTop responses by body size:")
    for status, url, n, interesting in responses[: args.top]:
        flag = "*" if interesting else " "
        print(f"{flag} {status} bytes={n:>7} {url}")

    print(f"\nFound fr_ids in bodies: {len(fr_ids)}")
    if len(fr_ids) > 0:
        sample = sorted(fr_ids)[:20]
        print("Sample fr_ids:", ", ".join(sample))

    print(f"\nFound /site/STR links in bodies: {len(str_urls)}")
    for u in list(sorted(str_urls))[:10]:
        print("  ", u)

    print(f"\nFound /site/TR links in bodies: {len(tr_urls)}")
    for u in list(sorted(tr_urls))[:10]:
        print("  ", u)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
