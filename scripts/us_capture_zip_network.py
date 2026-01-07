# scripts/us_capture_zip_network.py

from __future__ import annotations

import argparse
import base64
import json
import re
import time
from pathlib import Path
from typing import Any, Optional

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


def dump_debug(page, html_path: str, png_path: str) -> None:
    Path(html_path).parent.mkdir(parents=True, exist_ok=True)
    Path(png_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        Path(html_path).write_text(page.content(), encoding="utf-8")
    except Exception:
        pass
    try:
        page.screenshot(path=png_path, full_page=True)
    except Exception:
        pass


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


def click_join_button(page) -> bool:
    """
    Try clicking something that triggers the ZIP search UI.
    Returns True if clicked.
    """
    patterns = [
        ("role", None),
        ("css", "button:has-text('Join a Relay')"),
        ("css", "button:has-text('Join')"),
        ("css", "a:has-text('Join a Relay')"),
        ("css", "a:has-text('Join')"),
        ("css", "[role='button']:has-text('Join a Relay')"),
        ("css", "[role='button']:has-text('Join')"),
    ]

    # role-based first
    try:
        page.get_by_role("button", name=re.compile(r"join a relay", re.I)).click(timeout=8000)
        return True
    except Exception:
        pass

    for _, sel in patterns[1:]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=8000)
                return True
        except Exception:
            continue

    return False


ZIP_SELECTORS = [
    "input[name*='zip' i]",
    "input[id*='zip' i]",
    "input[placeholder*='zip' i]",
    "input[aria-label*='zip' i]",
    "input[type='search']",
    "input[type='text']",
    "input",
]


def find_zip_input_any_frame(page):
    """
    Search for an input across all frames (including iframes).
    Returns (frame, locator) or (None, None).
    """
    for frame in page.frames:
        for sel in ZIP_SELECTORS:
            try:
                loc = frame.locator(sel)
                if loc.count() == 0:
                    continue
                # prefer visible enabled candidates
                for i in range(min(loc.count(), 15)):
                    cand = loc.nth(i)
                    try:
                        if cand.is_visible() and cand.is_enabled():
                            # Heuristic: if placeholder/aria/name mentions zip, pick immediately
                            attrs = ""
                            try:
                                attrs = (cand.get_attribute("placeholder") or "") + " " + (cand.get_attribute("aria-label") or "") + " " + (cand.get_attribute("name") or "") + " " + (cand.get_attribute("id") or "")
                                attrs = attrs.lower()
                            except Exception:
                                attrs = ""
                            if "zip" in attrs:
                                return frame, cand
                    except Exception:
                        continue
            except Exception:
                continue

    # Second pass: any visible input in any frame
    for frame in page.frames:
        try:
            loc = frame.locator("input")
            for i in range(min(loc.count(), 20)):
                cand = loc.nth(i)
                try:
                    if cand.is_visible() and cand.is_enabled():
                        return frame, cand
                except Exception:
                    continue
        except Exception:
            continue

    return None, None


def main() -> int:
    p = argparse.ArgumentParser(description="Capture ACS ZIP-search network traffic via Playwright (iframe-aware).")
    p.add_argument("--zip", dest="zip_code", default="10001", help="ZIP code to test (default 10001).")
    p.add_argument("--out", default="out/us_network.jsonl", help="Output JSONL file path.")
    p.add_argument("--headless", action="store_true", help="Run browser headless.")
    p.add_argument("--wait", type=float, default=12.0, help="Seconds to wait after clicking (default: 12).")
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
                try:
                    buf = req.post_data_buffer
                    if buf:
                        try:
                            post_text = buf.decode("utf-8", errors="strict")
                        except Exception:
                            post_b64 = b64_bytes(buf)
                except Exception:
                    pass

                events.append(
                    {
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
                )
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
        page.wait_for_timeout(2500)

        try_accept_cookies(page)
        page.wait_for_timeout(1000)

        # IMPORTANT: On ACS, the ZIP UI may only appear after clicking "Join a Relay"
        clicked = click_join_button(page)
        page.wait_for_timeout(2500)

        # Search for ZIP input across frames
        frame, zip_input = find_zip_input_any_frame(page)
        if not zip_input:
            # As a last attempt: scroll and retry after a moment (lazy-loaded)
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(2000)
            frame, zip_input = find_zip_input_any_frame(page)

        if not zip_input:
            dump_debug(page, args.debug_html, args.debug_png)
            # Still write whatever we captured so far, for debugging.
            with out_path.open("w", encoding="utf-8") as f:
                for e in events:
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
            raise RuntimeError("Could not locate a ZIP input field on the page (see out/us_debug_page.html/png).")

        # Fill ZIP (use the frame locator we found)
        zip_input.click(timeout=5000)
        zip_input.fill(args.zip_code, timeout=10_000)

        # If clicking Join opened a modal, there may be a separate "Search" / "Find" submit button.
        # Click Join again to trigger the XHR if needed.
        if clicked:
            page.wait_for_timeout(500)
        _ = click_join_button(page)

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
