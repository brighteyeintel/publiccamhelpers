#!/usr/bin/env python3
"""
merge_webcamtaxi.py
Merges webcamtaxi.json (GeoJSON FeatureCollection) into combined.json.

- Extracts country from URL path slugs, with UK aliasing
- Parses subtitle from HTML description (locationaddress span)
- Fetches webcam pages to extract iframe src URLs
- Un-escapes all strings (backslashes, HTML entities)
- Truncates coordinates to 8 decimal places
- Outputs comprehensive statistics
"""

import json
import html
import re
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─── Configuration ────────────────────────────────────────────────────────────
WEBCAMTAXI_FILE = "webcamtaxi.json"
COMBINED_FILE = "combined.json"
OUTPUT_FILE = "combined.json"  # overwrite in place
BASE_URL = "https://www.webcamtaxi.com"
HTTP_TIMEOUT = 8
MAX_WORKERS = 40
VERIFIED_RADIUS = 10
UNVERIFIED_RADIUS = 200

# ─── Country slug → canonical name ────────────────────────────────────────────
COUNTRY_SLUG_MAP = {
    # UK constituent nations
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    # Renamed / special
    "usa": "United States",
    "czech-republic": "Czech Republic",
    "bosnia-herzegovina": "Bosnia and Herzegovina",
    "virgin-islands": "British Virgin Islands",
    "saint-barthelemy": "Saint Barthelemy",
    "saint-martin": "Sint Maarten",
    "united-arab-emirates": "United Arab Emirates",
    "south-africa": "South Africa",
    "south-korea": "South Korea",
    "new-zealand": "New Zealand",
    "north-macedonia": "North Macedonia",
    "french-polynesia": "French Polynesia",
    "costa-rica": "Costa Rica",
    "dominican-republic": "Dominican Republic",
    "el-salvador": "El Salvador",
    "dr-congo": "DR Congo",
    "saudi-arabia": "Saudi Arabia",
    "cape-verde": "Cape Verde",
    "cayman-islands": "Cayman Islands",
    "anguilla-island": "Anguilla",
    "turks-and-caicos-islands": "Turks And Caicos Islands",
    "bailiwick-of-jersey": "Jersey",
}


def slug_to_country(slug: str) -> str:
    """Convert a URL slug to a proper country name."""
    if slug in COUNTRY_SLUG_MAP:
        return COUNTRY_SLUG_MAP[slug]
    # Default: title-case and replace hyphens
    return slug.replace("-", " ").title()


def unescape_text(text: str) -> str:
    """Remove backslash escaping and HTML entities from text."""
    if not text:
        return text
    text = text.replace("\\/", "/")
    text = html.unescape(text)
    return text


def truncate_coord(value: float, decimals: int = 8) -> float:
    """Truncate a float to the given number of decimal places."""
    factor = 10 ** decimals
    return math.trunc(value * factor) / factor


def extract_subtitle(description: str) -> str:
    """Extract the locationaddress text from the description HTML."""
    match = re.search(r"<span class='locationaddress'>(.*?)</span>", description)
    if not match:
        return ""
    addr_html = match.group(1)
    # Replace HTML tags with separator
    addr_text = re.sub(r"<br\s*/?>", ", ", addr_html)
    addr_text = re.sub(r"<[^>]+>", "", addr_text)
    # Unescape HTML entities
    addr_text = html.unescape(addr_text)
    # Clean up whitespace and trailing separators
    addr_text = re.sub(r"\s+", " ", addr_text).strip()
    addr_text = addr_text.strip(", ").strip()
    return addr_text


def extract_subregion(url_path: str) -> str | None:
    """Extract region from URL path like /en/country/region/page.html → Region."""
    parts = url_path.strip("/").split("/")
    if len(parts) >= 3:
        region_slug = parts[2]
        return region_slug.replace("-", " ").title()
    return None


def fetch_iframe_url(page_url: str) -> tuple[str | None, bool]:
    """
    Fetch a webcamtaxi page and extract the first iframe src.
    Returns (iframe_src or None, success_bool).
    """
    try:
        resp = requests.get(
            page_url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; WebcamMerger/1.0)"},
        )
        if resp.status_code == 200:
            # Find first iframe src
            match = re.search(
                r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE
            )
            if match:
                iframe_src = match.group(1)
                iframe_src = unescape_text(iframe_src)
                return iframe_src, True
        return None, False
    except Exception:
        return None, False


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    t_start = time.time()

    # ── 1. Load data ──────────────────────────────────────────────────────
    print("Loading combined.json …")
    combined = load_json(COMBINED_FILE)
    existing_total = sum(len(recs) for recs in combined.values())

    print("Loading webcamtaxi.json …")
    wct_data = load_json(WEBCAMTAXI_FILE)
    features = wct_data["features"]
    wct_total = len(features)

    print(f"  Existing records in combined.json: {existing_total}")
    print(f"  WebcamTaxi features to integrate:  {wct_total}")

    # ── 2. Transform records ──────────────────────────────────────────────
    print("\nTransforming webcamtaxi records …")
    transformed: list[tuple[str, dict]] = []  # (country, record)
    uk_alias_counts: dict[str, int] = {}
    skipped_no_url = 0

    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        # URL path
        raw_url_path = props.get("url", "")
        raw_url_path = unescape_text(raw_url_path)

        # Country from URL slug
        parts = raw_url_path.strip("/").split("/")
        if len(parts) >= 3:
            country_slug = parts[1]
        else:
            country_slug = "unknown"

        # Skip oddball entries like "component"
        if country_slug == "component":
            skipped_no_url += 1
            continue

        country = slug_to_country(country_slug)

        # Track UK aliases
        if country_slug in ("england", "scotland", "wales"):
            uk_alias_counts[country_slug.title()] = (
                uk_alias_counts.get(country_slug.title(), 0) + 1
            )

        # Title
        title = unescape_text(props.get("name", ""))

        # Subtitle from description HTML
        description = unescape_text(props.get("description", ""))
        subtitle = extract_subtitle(description)

        # Subregion from URL path
        subregion = extract_subregion(raw_url_path)

        # Coordinates (GeoJSON is [lng, lat])
        try:
            longitude = truncate_coord(float(coords[0])) if coords[0] is not None else None
        except (ValueError, IndexError, TypeError):
            longitude = None
        try:
            latitude = truncate_coord(float(coords[1])) if coords[1] is not None else None
        except (ValueError, IndexError, TypeError):
            latitude = None

        # Full page URL to fetch
        page_url = BASE_URL + raw_url_path
        fallback_url = BASE_URL + raw_url_path

        record = {
            "title": title,
            "subtitle": subtitle,
            "url": None,  # filled after iframe extraction
            "mode": "automatic",
            "radius": None,
            "latitude": latitude,
            "longitude": longitude,
            "verified": None,
            "source": "WCT",
            "subregion": subregion,
            "_page_url": page_url,
            "_fallback_url": fallback_url,
        }
        transformed.append((country, record))

    print(f"  Transformed: {len(transformed)} records")
    if skipped_no_url:
        print(f"  Skipped (invalid URL slug): {skipped_no_url}")

    # ── 3. Fetch pages & extract iframe URLs concurrently ─────────────────
    print(
        f"\nFetching {len(transformed)} webcamtaxi pages ({MAX_WORKERS} workers, {HTTP_TIMEOUT}s timeout) …"
    )
    verified_count = 0
    failed_count = 0

    results = [None] * len(transformed)  # index → (iframe_url, success)
    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(fetch_iframe_url, rec["_page_url"]): idx
            for idx, (_, rec) in enumerate(transformed)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            iframe_url, success = future.result()
            results[idx] = (iframe_url, success)
            done += 1
            if done % 200 == 0 or done == len(transformed):
                pct = done / len(transformed) * 100
                print(f"  Progress: {done}/{len(transformed)} ({pct:.1f}%)")

    # Apply results
    for i, (country, rec) in enumerate(transformed):
        iframe_url, success = results[i]
        if success and iframe_url:
            rec["url"] = iframe_url
            rec["verified"] = True
            rec["radius"] = VERIFIED_RADIUS
            verified_count += 1
        else:
            rec["url"] = rec["_fallback_url"]
            rec["verified"] = False
            rec["radius"] = UNVERIFIED_RADIUS
            failed_count += 1

        # Remove internal fields
        del rec["_page_url"]
        del rec["_fallback_url"]

    print(f"  Verified (iframe found): {verified_count}")
    print(f"  Failed / no iframe:      {failed_count}")

    # ── 4. Merge into combined structure ──────────────────────────────────
    print("\nMerging into combined dataset …")
    new_countries = set()
    for country, rec in transformed:
        if country not in combined:
            combined[country] = []
            new_countries.add(country)
        combined[country].append(rec)

    # Sort countries alphabetically
    combined = dict(sorted(combined.items()))

    # ── 5. Write output ───────────────────────────────────────────────────
    print(f"\nWriting {OUTPUT_FILE} …")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    t_elapsed = time.time() - t_start

    # ── 6. Statistics ─────────────────────────────────────────────────────
    all_records = []
    for recs in combined.values():
        all_records.extend(recs)

    total = len(all_records)
    slw_records = [r for r in all_records if r.get("source") == "SLW"]
    ew_records = [r for r in all_records if r.get("source") == "EW"]
    wct_records = [r for r in all_records if r.get("source") == "WCT"]
    total_verified = sum(1 for r in all_records if r.get("verified") is True)

    print("\n" + "=" * 70)
    print("                         STATISTICS")
    print("=" * 70)

    print(f"\n  Total locations (combined):   {total}")
    print(f"  ├─ From SLW (Skyline):        {len(slw_records)}")
    print(f"  ├─ From EW  (EarthCam):       {len(ew_records)}")
    print(f"  └─ From WCT (WebcamTaxi):     {len(wct_records)}")
    print(
        f"\n  Total verified locations:     {total_verified} / {total} ({total_verified / total * 100:.1f}%)"
    )

    print(f"\n  WCT URL extraction results:")
    print(f"    ├─ Iframe found (verified): {verified_count}")
    print(f"    └─ Failed / no iframe:      {failed_count}")

    print(f"\n  Total countries:              {len(combined)}")
    if new_countries:
        print(f"  New countries added by WCT:   {len(new_countries)}")
        for nc in sorted(new_countries):
            print(f"    • {nc}")

    if uk_alias_counts:
        print(f"\n  UK alias merges (WCT):")
        for alias, cnt in sorted(uk_alias_counts.items()):
            print(f"    • {alias} → United Kingdom: {cnt} records")

    # Per-country breakdown
    print(f"\n  {'Country':<40} {'Count':>6}")
    print(f"  {'─' * 40} {'─' * 6}")
    for country in sorted(combined.keys()):
        cnt = len(combined[country])
        print(f"  {country:<40} {cnt:>6}")

    # Field coverage
    fields = [
        "title", "subtitle", "url", "mode", "radius", "latitude", "longitude",
        "verified", "source", "subregion",
    ]
    print(f"\n  Field coverage (% non-null across all {total} records):")
    print(f"  {'Field':<20} {'Non-null':>10} {'Coverage':>10}")
    print(f"  {'─' * 20} {'─' * 10} {'─' * 10}")
    for field in fields:
        non_null = sum(
            1
            for r in all_records
            if r.get(field) is not None and r.get(field) != ""
        )
        pct = non_null / total * 100 if total else 0
        print(f"  {field:<20} {non_null:>10} {pct:>9.1f}%")

    print(f"\n  Elapsed time: {t_elapsed:.1f}s")
    print("=" * 70)
    print(f"  Output written to: {OUTPUT_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
