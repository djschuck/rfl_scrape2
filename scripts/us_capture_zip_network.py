# scripts/us_capture_zip_network.py

from __future__ import annotations

import argparse
import base64
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

from playwright.sync_api import sync_playwright

START_URL = "https://secure.acsevents.org/site/SPageServer?pagename=relay_get_involved"

LIKELY_API_HINTS = (
    "sPageServer",
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


def b64_bytes(b: Optional[bytes], limit: int = 200_000) -> str:
    if not b:
        return ""
    if len(b) > limit:
        b = b[:limit]
    return base64.b64encode(b).decode("ascii")


def try_accept_cookies(page) -> None:
    for sel in [
        "#onetrust-accept-btn-handler",
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
                return
        except Exception:
            pass


def find_zip_input(page):
    """
    Try a bunch of likely selectors. ACS pages vary.
    We return a Locator or None.
    """
    selectors = [
        # explicit attributes
        "input[name*='zip' i]",
        "input[id*='zip' i]",
        "input[placeholder*='zip' i]",
        "input[aria-label*='zip' i]",
        # common input types
        "input[type='search']",
        "input[type='text']",
        # sometimes it's a generic input in a form near the Join button
        "form input",
        # fallback: any input at all
        "input",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                # pick the first visible enabled one
                for i in range(min(loc.count(), 10)):
                    cand = loc.nth(i)
                    try:
                        if cand.is_visible() and cand.is_enabled():
                            return cand
                    except Exception:
                        continue
        except Exception:
            continue
    return None


def click_join_button(page) -> None:
    # Prefer accessible role/name, but include fallbacks.
    try:
        page.get_by_role("button", name=re.compile(r"join a relay", re.I)).click(timeout=10_000)
        return
    except Exception:
        pass

    # Fallback: anything button-like containing join
    for sel in [
        "button:has-text('Join a Relay')",
        "button:has-text('Join')",
        "a:has-text('Join a Relay')",
        "a:has-text('Join')",
        "[role='button']:has-text('Join a Relay')",
        "[role='button']:has-text('Join')",
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=10_000)
                return
        except Exception:
            continue

    raise RuntimeError("Could not find a 'Join' button with any selector.")


def main() -> int:
    p = argparse.ArgumentParser(description="Capture ACS ZIP-search network traffic via Playwright.")
    p.add_argument("--zip", dest="zip_code", default="10001", help="ZIP code to test (default 10001).")
    p.add_argument("--out", default="out/us_network.jsonl", help="Output JSONL file path.")
    p.add_argument("--headless", action="store_true", help="Run browser headless (default: headed).")
    p.add_argument("--wait", type=float, default=10.0, help="Seconds to wait after clicking (default: 10).")
    p.add_argument("--debug-html", default="out/us_debug_page.html", help="Write page HTML here on failure.")
    p.add_argument("--debug-png", default="out/us_debug_page.png", help="Write screenshot here on failure.")
    args = p.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    events: list[dict[str, Any]] = []
    request_id = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()

        def on_request(req) -> None:
            nonlocal request_id
            try:
                request_id += 1
                post_text = ""
                post_b64 = ""

                # IMPORTANT: req.post_data can raise UnicodeDecodeError internally.
                # Use buffer form safely.
                try:
                    buf = req.post_data_buffer
                    if buf:
                        # try utf-8 decode; if not, store base64
                        try:
                            post_text = buf.decode("utf-8", errors="strict")
                        except Exception:
                            post_b64 = b64_bytes(buf)
                except Exception:
                    # As a fallback, try plain post_data but guard it
                    try:
                        post_text = req.post_data or ""
                    except Exception:
                        post_text = ""

                rec = {
                    "type": "request",
                    "id": request_id,
                    "ts": time.time(),
                    "method": req.method,
                    "url": req.url,
                    "resource_type": req.resource_type,
                    "headers": dict(req.headers),
                    "post_data": safe_text(post_text),
                    "post_data_b64": post_b64,
                    "interesting": looks_interesting(req.url),
                }
                events.append(rec)
            except Exception as e:
                events.append({"type": "request_error", "ts": time.time(), "error": repr(e)})

        def on_response(resp) -> None:
            try:
                rec: dict[str, Any] = {
                    "type": "response",
                    "ts": time.time(),
                    "url": resp.url,
                    "status": resp.status,
                    "headers": dict(resp.headers),
                    "interesting": looks_interesting(resp.url),
                }

                # Try to capture body for XHR/fetch or anything "interesting"
                try:
                    ct = (resp.headers.get("content-type") or "").lower()
                    if resp.request.resource_type in ("xhr", "fetch") or looks_interesting(resp.url):
                        body = ""
                        if "application/json" in ct:
                            body = safe_text(resp.text())
                        elif "text/html" in ct or "text/plain" in ct or "javascript" in ct:
                            body = safe_text(resp.text())
                        if body:
                            rec["body"] = body
                except Exception as e:
                    rec["body_error"] = repr(e)

                events.append(rec)
            except Exception as e:
                events.append({"type": "response_error", "ts": time.time(), "error": repr(e)})

        page.on("request", on_request)
        page.on("response", on_response)

        page.goto(START_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(2000)

        try_accept_cookies(page)

        # Make sure the page is settled a bit
        page.wait_for_timeout(1500)

        zip_input = find_zip_input(page)
        if not zip_input:
            # Dump debug
            Path(args.debug_html).parent.mkdir(parents=True, exist_ok=True)
            Path(args.debug_png).parent.mkdir(parents=True, exist_ok=True)
            try:
                Path(args.debug_html).write_text(page.content(), encoding="utf-8")
            except Exception:
                pass
            try:
                page.screenshot(path=args.debug_png, full_page=True)
            except Exception:
                pass
            raise RuntimeError("Could not locate a ZIP input field on the page (see debug HTML/PNG).")

        # Fill ZIP
        zip_input.click(timeout=5000)
        zip_input.fill(args.zip_code, timeout=10_000)

        # Click Join
        click_join_button(page)

        # Wait for results + XHRs
        page.wait_for_timeout(int(args.wait * 1000))

        # Save JSONL
        with out_path.open("w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

        print(f"Wrote {len(events)} network records to {out_path}")

        browser.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
