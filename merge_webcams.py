#!/usr/bin/env python3
"""
merge_webcams.py
Merges earthcams.json into webcam_locations.json to produce combined.json.

- Adds source="SLW" and subregion=null to all existing webcam_locations records
- Transforms earthcam records: field mapping, URL un-escaping, URL verification
- Fuzzy country name matching and UK aliasing (England/Wales/Scotland → United Kingdom)
- Concurrent HTTP verification with 40 workers
- Outputs comprehensive statistics
"""

import json
import html
import re
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote

import requests

# ─── Configuration ────────────────────────────────────────────────────────────
EARTHCAMS_FILE = "earthcams.json"
WEBCAM_LOCATIONS_FILE = "webcam_locations.json"
OUTPUT_FILE = "combined.json"
HTTP_TIMEOUT = 8       # seconds
MAX_WORKERS = 40       # concurrent HTTP requests
VERIFIED_RADIUS = 10
UNVERIFIED_RADIUS = 200

# ─── Country name normalisation map ──────────────────────────────────────────
# Maps earthcam country names → canonical names used in output.
# Entries here override the raw country string.
COUNTRY_ALIASES = {
    # UK constituent nations
    "England": "United Kingdom",
    "Wales": "United Kingdom",
    "Scotland": "United Kingdom",
    "Great Britain": "United Kingdom",
    # Casing / spelling variants
    "Bosnia And Herzegovina": "Bosnia and Herzegovina",
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    "Turkiye": "Turkey",
    "Russian Federation": "Russia",
    "Slovak Republic": "Slovakia",
    # US territories
    "Virgin Islands, U.S.": "US Virgin Islands",
    "Virgin Islands, British": "British Virgin Islands",
}


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def unescape_url(url: str) -> str:
    """Remove backslash-escaping from URLs and apply HTML unescaping."""
    # Replace \/ with /
    url = url.replace("\\/", "/")
    # Standard HTML unescape (handles &amp;, &#39;, \uXXXX in JSON strings etc.)
    url = html.unescape(url)
    return url


def verify_url(url: str) -> bool:
    """Return True if url responds with HTTP 200 within the timeout."""
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": "Mozilla/5.0 (compatible; WebcamMerger/1.0)"})
        return resp.status_code == 200
    except Exception:
        return False


def resolve_country(raw_country: str) -> str:
    """Return canonical country name, applying alias mapping."""
    if not raw_country or not raw_country.strip():
        return "Unknown"
    return COUNTRY_ALIASES.get(raw_country, raw_country)


def main():
    t_start = time.time()

    # ── 1. Load data ──────────────────────────────────────────────────────
    print("Loading webcam_locations.json …")
    wl_data = load_json(WEBCAM_LOCATIONS_FILE)

    print("Loading earthcams.json …")
    ec_raw = load_json(EARTHCAMS_FILE)
    ec_places = ec_raw[0]["places"]  # list[dict]

    slw_total = sum(len(recs) for recs in wl_data.values())
    ec_total = len(ec_places)
    print(f"  SLW records loaded: {slw_total}")
    print(f"  EarthCam records loaded: {ec_total}")

    # ── 2. Enrich existing SLW records ────────────────────────────────────
    print("\nAdding source='SLW' and subregion=null to existing records …")
    for country, records in wl_data.items():
        for rec in records:
            rec["source"] = "SLW"
            rec["subregion"] = None

    # ── 3. Transform earthcam records ─────────────────────────────────────
    print("Transforming earthcam records …")
    transformed: list[dict] = []
    skipped_no_country = 0
    uk_alias_counts: dict[str, int] = {}  # track England/Wales/Scotland/Great Britain

    for ec in ec_places:
        raw_country = ec.get("country", "")
        canonical_country = resolve_country(raw_country)

        # Track UK alias usage
        if raw_country in ("England", "Wales", "Scotland", "Great Britain"):
            uk_alias_counts[raw_country] = uk_alias_counts.get(raw_country, 0) + 1

        if canonical_country == "Unknown":
            skipped_no_country += 1

        # Parse lat/lng from posn
        posn = ec.get("posn", [None, None])
        try:
            latitude = float(posn[0]) if posn[0] is not None else None
        except (ValueError, IndexError):
            latitude = None
        try:
            longitude = float(posn[1]) if posn[1] is not None else None
        except (ValueError, IndexError):
            longitude = None

        url = unescape_url(ec.get("url", ""))

        record = {
            "title": ec.get("name"),
            "subtitle": ec.get("location"),
            "url": url,
            "mode": "automatic",
            "radius": None,        # filled after verification
            "latitude": latitude,
            "longitude": longitude,
            "verified": None,      # filled after verification
            "source": "EW",
            "subregion": ec.get("city"),
        }
        transformed.append((canonical_country, record))

    print(f"  Transformed: {len(transformed)} records")
    if skipped_no_country:
        print(f"  Records with empty country (mapped to 'Unknown'): {skipped_no_country}")

    # ── 4. Verify URLs concurrently ───────────────────────────────────────
    print(f"\nVerifying {len(transformed)} earthcam URLs ({MAX_WORKERS} workers, {HTTP_TIMEOUT}s timeout) …")
    verified_count = 0
    failed_count = 0

    # Build lookup: index → url
    urls = [(i, rec["url"]) for i, (_, rec) in enumerate(transformed)]

    results = [None] * len(transformed)  # index → bool

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(verify_url, url): idx
            for idx, url in urls
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            ok = future.result()
            results[idx] = ok
            done += 1
            if done % 200 == 0 or done == len(urls):
                pct = done / len(urls) * 100
                print(f"  Progress: {done}/{len(urls)} ({pct:.1f}%)")

    # Apply verification results
    for i, (country, rec) in enumerate(transformed):
        ok = results[i]
        rec["verified"] = ok
        rec["radius"] = VERIFIED_RADIUS if ok else UNVERIFIED_RADIUS
        if ok:
            verified_count += 1
        else:
            failed_count += 1

    print(f"  Verified (200): {verified_count}")
    print(f"  Failed / non-200: {failed_count}")

    # ── 5. Merge into combined structure ──────────────────────────────────
    print("\nMerging into combined dataset …")
    combined = {}

    # Copy enriched SLW data
    for country, records in wl_data.items():
        combined[country] = list(records)  # shallow copy the list

    # Add earthcam records
    new_countries = set()
    for country, rec in transformed:
        if country not in combined:
            combined[country] = []
            new_countries.add(country)
        combined[country].append(rec)

    # Sort countries alphabetically
    combined = dict(sorted(combined.items()))

    # ── 6. Write output ───────────────────────────────────────────────────
    print(f"\nWriting {OUTPUT_FILE} …")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    t_elapsed = time.time() - t_start

    # ── 7. Statistics ─────────────────────────────────────────────────────
    all_records = []
    for recs in combined.values():
        all_records.extend(recs)

    total = len(all_records)
    slw_records = [r for r in all_records if r.get("source") == "SLW"]
    ew_records  = [r for r in all_records if r.get("source") == "EW"]
    total_verified = sum(1 for r in all_records if r.get("verified") is True)

    print("\n" + "=" * 70)
    print("                         STATISTICS")
    print("=" * 70)

    print(f"\n  Total locations (combined):   {total}")
    print(f"  ├─ From SLW (Skyline):        {len(slw_records)}")
    print(f"  └─ From EW  (EarthCam):       {len(ew_records)}")
    print(f"\n  Total verified locations:     {total_verified} / {total} ({total_verified/total*100:.1f}%)")

    print(f"\n  EW URL verification results:")
    print(f"    ├─ Successful (HTTP 200):   {verified_count}")
    print(f"    └─ Failed / non-200:        {failed_count}")

    print(f"\n  Total countries:              {len(combined)}")
    if new_countries:
        print(f"  New countries added:          {len(new_countries)}")
        for nc in sorted(new_countries):
            print(f"    • {nc}")

    if uk_alias_counts:
        print(f"\n  UK alias merges:")
        for alias, cnt in sorted(uk_alias_counts.items()):
            print(f"    • {alias} → United Kingdom: {cnt} records")

    # Per-country breakdown
    print(f"\n  {'Country':<40} {'Count':>6}")
    print(f"  {'─' * 40} {'─' * 6}")
    for country in sorted(combined.keys()):
        cnt = len(combined[country])
        print(f"  {country:<40} {cnt:>6}")

    # Field coverage
    fields = ["title", "subtitle", "url", "mode", "radius", "latitude", "longitude",
              "verified", "source", "subregion"]
    print(f"\n  Field coverage (% non-null across all {total} records):")
    print(f"  {'Field':<20} {'Non-null':>10} {'Coverage':>10}")
    print(f"  {'─' * 20} {'─' * 10} {'─' * 10}")
    for field in fields:
        non_null = sum(1 for r in all_records if r.get(field) is not None)
        pct = non_null / total * 100 if total else 0
        print(f"  {field:<20} {non_null:>10} {pct:>9.1f}%")

    print(f"\n  Elapsed time: {t_elapsed:.1f}s")
    print("=" * 70)
    print(f"  Output written to: {OUTPUT_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
