import hashlib
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import requests

ENGINE = "google_ads_transparency_center"
REGION_BG = "2100"
ENDPOINT = "https://serpapi.com/search.json"
TPC_ARCHIVE_MARKER = "tpc.googlesyndication.com/archive/simgad/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "image/*,*/*;q=0.8",
    "Referer": "https://google.com",
}

CONTENT_TYPE_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/avif": ".avif",
}


def detect_format(creative: dict) -> str:
    for k in ("ad_creative_format", "creative_format", "format", "type", "ad_type"):
        v = creative.get(k)
        if isinstance(v, str) and "video" in v.lower():
            return "video"
    return "image"


def iter_urls(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from iter_urls(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from iter_urls(v)
    elif isinstance(obj, str) and obj.startswith("http"):
        yield obj


def best_preview_url(creative: dict) -> Optional[str]:
    for u in iter_urls(creative):
        if TPC_ARCHIVE_MARKER in u:
            return u
    return next(iter_urls(creative), None)


def fetch_preview_urls(
    serpapi_key: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
) -> List[str]:
    urls, seen = [], set()
    token = None

    while True:
        params = {
            "engine": ENGINE,
            "api_key": serpapi_key,
            "advertiser_id": advertiser_id,
            "region": REGION_BG,
            "start_date": start_date,
            "end_date": end_date,
            "num": "100",
        }
        if token:
            params["next_page_token"] = token

        r = requests.get(ENDPOINT, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        for c in data.get("ad_creatives", []):
            if detect_format(c) == "video":
                continue
            url = best_preview_url(c)
            if url and url not in seen:
                seen.add(url)
                urls.append(url)

        token = data.get("serpapi_pagination", {}).get("next_page_token")
        if not token:
            break

    return urls


def guess_ext(url: str, content_type: str) -> str:
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        ext = CONTENT_TYPE_TO_EXT.get(ct)
        if ext:
            return ext

    path = urlparse(url).path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif"):
        if path.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def download_image(url: str, folder: Path) -> Path:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    ext = guess_ext(url, r.headers.get("Content-Type", ""))
    h = hashlib.md5(r.content).hexdigest()[:10]
    path = folder / f"{h}{ext}"
    path.write_bytes(r.content)
    return path
