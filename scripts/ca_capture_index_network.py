from __future__ import annotations

import argparse
import base64
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, Request, Response


CA_INDEX_URLS_DEFAULT = [
    "https://support.cancer.ca/site/PageServer?pagename=RFL_NW_Events",
    "https://support.cancer.ca/site/PageServer?pagename=RFLY_NW_Events",
]

# Heuristics to keep only likely “data” calls
LIKELY_DATA_RE = re.compile(
    r"(TeamRaiser|Teamraiser|CRTeamraiserAPI|PageServer|api|graphql|json|search|events)",
    re.IGNORECASE,
)


def safe_post_data(req: Request) -> Dict[str, Any]:
    """
    Playwright's req.post_data can throw decoding errors if it's not UTF-8.
    We handle safely and preserve something useful.
    """
    out: Dict[str, Any] = {"text": "", "base64": ""}

    try:
        txt = req.post_data or ""
        out["text"] = txt if isinstance(txt, str) else ""
        return out
    except Exception:
        pass

    # Try buffer (may not exist in some versions, so guard)
    try:
        buf = req.post_data_buffer
        if buf:
            out["base64"] = base64.b64encode(buf).decode("ascii")
    except Exception:
        pass

    return out


def safe_response_body(resp: Response, max_bytes: int) -> Dict[str, Any]:
    """
    Capture a limited response body for XHR/fetch responses.
    """
    meta: Dict[str, Any] = {"content_type": "", "text": "", "truncated": False}

    try:
        ct = resp.headers.get("content-type", "")
        meta["content_type"] = ct
    except Exception:
        meta["content_type"] = ""

    # Only attempt body for textual-ish content
    try:
        body = resp.body()
        if not body:
            return meta
        if len(body) > max_bytes:
            body = body[:max_bytes]
            meta["truncated"] = True

        # Try decode as utf-8, fallback latin-1
        try:
            meta["text"] = body.decode("utf-8", errors="replace")
        except Exception:
            meta["text"] = body.decode("latin-1", errors="replace")
    except Exception:
        pass

    return meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="out", help="Output directory")
    ap.add_argument("--max-body-bytes", type=int, default=200000, help="Max bytes to store per response body")
    ap.add_argument("--timeout-ms", type=int, default=60000, help="Page timeout in ms")
    ap.add_argument("--headless", action="store_true", help="Run headless")
    ap.add_argument("--urls", nargs="*", default=CA_INDEX_URLS_DEFAULT, help="Index URLs to capture")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    captured: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()

        def on_request(req: Request) -> None:
            try:
                url = req.url
                if not LIKELY_DATA_RE.search(url):
                    return
                rec = {
                    "type": "request",
                    "url": url,
                    "method": req.method,
                    "resource_type": req.resource_type,
                    "headers": dict(req.headers),
                    "post_data": safe_post_data(req),
                }
                captured.append(rec)
            except Exception:
                # do not crash listener
                return

        def on_response(resp: Response) -> None:
            try:
                url = resp.url
                if not LIKELY_DATA_RE.search(url):
                    return
                req = resp.request
                rec = {
                    "type": "response",
                    "url": url,
                    "status": resp.status,
                    "request_method": req.method,
                    "resource_type": req.resource_type,
                    "headers": dict(resp.headers),
                    "body": safe_response_body(resp, args.max_body_bytes),
                }
                captured.append(rec)
            except Exception:
                return

        context.on("request", on_request)
        context.on("response", on_response)

        page = context.new_page()
        page.set_default_timeout(args.timeout_ms)

        for u in args.urls:
            captured.append({"type": "marker", "url": u, "ts": time.time(), "note": "navigating"})
            page.goto(u, wait_until="domcontentloaded")
            # let XHRs populate
            page.wait_for_timeout(8000)
            # scroll to trigger any lazy loads
            try:
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(2000)
                page.mouse.wheel(0, 2500)
                page.wait_for_timeout(2000)
            except Exception:
                pass
            # wait for network quiet
            try:
                page.wait_for_load_state("networkidle", timeout=args.timeout_ms)
            except Exception:
                pass

            # snapshot HTML for offline debugging
            html_path = os.path.join(args.outdir, f"ca_index_snapshot_{'schools' if 'RFLY_' in u else 'community'}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(page.content())

        browser.close()

    # Write JSON
    json_path = os.path.join(args.outdir, "ca_network_summary.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(captured, f, indent=2, ensure_ascii=False)

    # Write human summary
    txt_path = os.path.join(args.outdir, "ca_network_summary.txt")
    lines: List[str] = []
    lines.append(f"Captured records: {len(captured)}")
    lines.append("")

    # Show top unique URLs
    req_urls = [r["url"] for r in captured if r.get("type") == "request"]
    unique = sorted(set(req_urls))
    lines.append(f"Unique request URLs (filtered): {len(unique)}")
    for uu in unique[:200]:
        lines.append(f"- {uu}")
    lines.append("")
    lines.append("Tip: look for CRTeamraiserAPI / TeamRaiserSearch / JSON endpoints above.")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Wrote: {json_path}")
    print(f"Wrote: {txt_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
