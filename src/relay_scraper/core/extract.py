from __future__ import annotations

import re
from typing import Set
from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def decode_cfemail(hex_str: str) -> str:
    """
    Decode Cloudflare email protection.
    hex_str: the hex payload (no leading '#').
    """
    hex_str = hex_str.strip().lower()
    key = int(hex_str[:2], 16)
    out = []
    for i in range(2, len(hex_str), 2):
        out.append(chr(int(hex_str[i:i+2], 16) ^ key))
    return "".join(out)

def extract_emails(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "lxml")
    emails: Set[str] = set()

    # 1) mailto:
    for a in soup.select('a[href^="mailto:"]'):
        href = a.get("href", "")
        if href.lower().startswith("mailto:"):
            emails.add(href.split(":", 1)[1].split("?", 1)[0].strip())

    # 2) Cloudflare: <a class="__cf_email__" data-cfemail="...">[email protected]</a>
    for a in soup.select("a.__cf_email__"):
        hex_str = a.get("data-cfemail")
        if hex_str and re.fullmatch(r"[0-9a-fA-F]+", hex_str):
            try:
                emails.add(decode_cfemail(hex_str))
            except Exception:
                pass

    # 3) Cloudflare: href /cdn-cgi/l/email-protection#<hex>
    for a in soup.select('a[href*="/cdn-cgi/l/email-protection"]'):
        href = a.get("href", "")
        m = re.search(r"#([0-9a-fA-F]+)$", href)
        if m:
            try:
                emails.add(decode_cfemail(m.group(1)))
            except Exception:
                pass

    # 4) Obfuscated patterns
    text = soup.get_text(" ", strip=True)
    text = (
        text.replace("[at]", "@").replace("(at)", "@")
            .replace(" at ", "@")
            .replace("[dot]", ".").replace("(dot)", ".")
            .replace(" dot ", ".")
    )
    for m in EMAIL_RE.finditer(text):
        emails.add(m.group(0))

    # normalize
    normalized = set()
    for e in emails:
        e = e.strip().strip(".;,")
        if "@" in e:
            normalized.add(e.lower())
    return normalized
