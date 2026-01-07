# scripts/us_capture_zip_network.py

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

from playwright.sync_api import sync_playwright

START_URL = "https://secure.acsevents.org/site/SPageServer?pagename=relay_get_involved"

LIKELY_API_HINTS = (
    "SPageServer",
    "relay",
    "find",
    "search",
    "ajax",
    "json",
    "api",
    "site/CR",
    "site/Survey",
    "site/STR",
    "site/TR",
)


def looks_interesting(url: str) -> bool:
    u = url.lower()
    return any(h.lower() in u for h in LIKELY_API_HINTS)


def safe_text(s: Optional[str], limit: int = 200_000) -> str:
    if not s:
        return ""
    s = s.strip()
    if len(s) > limit:
        return s[:limit] + "\n...<truncated>..."
    return s


def main() -> int:
    p = argparse.ArgumentParser(description="Capture ACS ZIP-search network traffic via Playwright.")
    p.add_argument("--zip", dest="zip_code", default="10001", help="ZIP code to test (default 10001).")
    p.add_argument("--out", default="out/us_network.jsonl", help="Output JSONL file path.")
    p.add_argument("--headless", action="store_true", help="Run browser headless (default: headed).")
    p.add_argument("--wait", type=float, default=8.0, help="Seconds to wait after clicking (default: 8).")
    args = p.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    events: list[dict[str, Any]] = []
    request_id = 0
    resp_bodies: Dict[str, str] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()

        def on_request(req) -> None:
            nonlocal request_id
            request_id += 1
            rec = {
                "type": "request",
                "id": request_id,
                "ts": time.time(),
                "method": req.method,
                "url": req.url,
                "resource_type": req.resource_type,
                "headers": dict(req.headers),
                "post_data": req.post_data or "",
                "interesting": looks_interesting(req.url),
            }
            events.append(rec)

        def on_response(resp) -> None:
            rec: dict[str, Any] = {
                "type": "response",
                "ts": time.time(),
                "url": resp.url,
                "status": resp.status,
                "headers": dict(resp.headers),
                "interesting": looks_interesting(resp.url),
            }

            # Try to capture body for XHR/fetch or anything that looks interesting
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                if resp.request.resource_type in ("xhr", "fetch") or looks_interesting(resp.url):
                    body = ""
                    if "application/json" in ct:
                        body = safe_text(resp.text())
                    elif "text/html" in ct or "text/plain" in ct or "javascript" in ct:
                        body = safe_text(resp.text())
                    # store only if non-empty
                    if body:
                        rec["body"] = body
            except Exception as e:
                rec["body_error"] = repr(e)

            events.append(rec)

        page.on("request", on_request)
        page.on("response", on_response)

        page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1500)

        # Try to accept cookies if any (best-effort; ACS varies)
        for sel in [
            "text=Accept",
            "text=I Accept",
            "button:has-text('Accept')",
            "button:has-text('I Accept')",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0 and loc.first.is_visible():
                    loc.first.click(timeout=2000)
                    page.wait_for_timeout(500)
                    break
            except Exception:
                pass

        # Fill ZIP textbox (best-effort: first textbox on the page)
        # If ACS changes markup, you can refine this later based on captured HTML.
        page.get_by_role("textbox").first.fill(args.zip_code)

        # Click the "Join a Relay" button
        try:
            page.get_by_role("button", name=re.compile(r"join a relay", re.I)).click(timeout=10_000)
        except Exception:
            # fallback: click any button containing Join
            page.locator("button:has-text('Join')").first.click(timeout=10_000)

        page.wait_for_timeout(int(args.wait * 1000))

        # Save
        with out_path.open("w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(f"Wrote {len(events)} network records to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
