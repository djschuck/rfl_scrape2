from __future__ import annotations

from urllib.parse import urljoin, urlparse

def is_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https")
    except Exception:
        return False

def absolutize(base: str, href: str) -> str:
    return urljoin(base, href)
