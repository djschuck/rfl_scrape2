from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    from_cache: bool

class Fetcher:
    def __init__(
        self,
        cache_dir: str = ".cache/http",
        timeout_s: float = 30.0,
        min_delay_s: float = 0.5,
        max_delay_s: float = 2.0,
    ) -> None:
        self.cache_dir = cache_dir
        self.timeout_s = timeout_s
        self.min_delay_s = min_delay_s
        self.max_delay_s = max_delay_s
        os.makedirs(cache_dir, exist_ok=True)
        self._client = httpx.Client(
            timeout=timeout_s,
            headers={"User-Agent": DEFAULT_UA, "Accept-Language": "en-US,en;q=0.9"},
            follow_redirects=True,
        )
        self._last_request_ts = 0.0

    def close(self) -> None:
        self._client.close()

    def _sleep_polite(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_delay_s:
            time.sleep(self.min_delay_s - elapsed)
        self._last_request_ts = time.time()

    def _cache_path(self, url: str) -> str:
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return os.path.join(self.cache_dir, f"{h}.html")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
        reraise=True,
    )
    def get_text(self, url: str, use_cache: bool = True) -> FetchResult:
        cache_path = self._cache_path(url)
        if use_cache and os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                return FetchResult(url=url, status_code=200, text=f.read(), from_cache=True)

        self._sleep_polite()
        resp = self._client.get(url)
        text = resp.text

        if use_cache and resp.status_code == 200 and len(text) > 200:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(text)

        return FetchResult(url=url, status_code=resp.status_code, text=text, from_cache=False)
