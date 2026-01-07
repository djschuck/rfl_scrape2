from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests


API_BASE = "https://acsfundraising.cancer.org/api/event/find"


@dataclass
class ApiVariant:
    """One candidate set of parameter names."""
    zip_key: str
    radius_key: Optional[str]
    version_key: Optional[str]
    version_value: Optional[str]


DEFAULT_VARIANTS: List[ApiVariant] = [
    # The most likely based on your capture fragments:
    ApiVariant(zip_key="TextSearch", radius_key="Distance", version_key="ApiVersion", version_value="5.0"),
    ApiVariant(zip_key="TextSearch", radius_key="Radius",   version_key="ApiVersion", version_value="5.0"),
    ApiVariant(zip_key="TextSearch", radius_key="Distance", version_key="version",    version_value="5.0"),
    ApiVariant(zip_key="TextSearch", radius_key="Radius",   version_key="version",    version_value="5.0"),
    # Fall-backs (some APIs just accept TextSearch alone)
    ApiVariant(zip_key="TextSearch", radius_key=None,       version_key="ApiVersion", version_value="5.0"),
    ApiVariant(zip_key="TextSearch", radius_key=None,       version_key=None,         version_value=None),
]


def _is_success_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("successful") is False:
        return False
    results = payload.get("results")
    return isinstance(results, list)


def probe_variant(
    zip_code: str,
    radius_miles: int,
    *,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
    variants: List[ApiVariant] = DEFAULT_VARIANTS,
) -> ApiVariant:
    """
    Try a few parameter-name combinations until we get JSON back with a 'results' list.
    """
    s = session or requests.Session()

    for v in variants:
        params: Dict[str, Any] = {
            v.zip_key: zip_code,
            "EventType": "RelayForLife",
            "EventSubType": "",
            "EventSearchFilter": "25",  # keep consistent with your capture
        }
        if v.radius_key:
            params[v.radius_key] = str(radius_miles)
        if v.version_key and v.version_value:
            params[v.version_key] = v.version_value

        url = f"{API_BASE}?{urlencode(params)}"
        try:
            r = s.get(url, timeout=timeout, headers={"Accept": "application/json"})
            if r.status_code != 200:
                continue
            payload = r.json()
            if _is_success_payload(payload):
                return v
        except Exception:
            continue

    raise RuntimeError("Could not find a working ACS fundraising API parameter variant.")


def search_events(
    zip_code: str,
    radius_miles: int,
    variant: ApiVariant,
    *,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Returns the raw 'results' list from the API.
    """
    s = session or requests.Session()

    params: Dict[str, Any] = {
        variant.zip_key: zip_code,
        "EventType": "RelayForLife",
        "EventSubType": "",
        "EventSearchFilter": "25",
    }
    if variant.radius_key:
        params[variant.radius_key] = str(radius_miles)
    if variant.version_key and variant.version_value:
        params[variant.version_key] = variant.version_value

    url = f"{API_BASE}?{urlencode(params)}"
    r = s.get(url, timeout=timeout, headers={"Accept": "application/json"})
    r.raise_for_status()
    payload = r.json()

    if not _is_success_payload(payload):
        raise RuntimeError(f"Unexpected API payload (no results list). URL={url}")

    return payload["results"]
