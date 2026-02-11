#!/usr/bin/env python3
"""
Geocoder for skyline webcam data.
Reads skyline_webcams.json, extracts common keywords from h1/h2 tags,
queries Google Maps Places API for lat/lng, and writes enriched data
to skyline_webcams_geocoded.json.
"""

import json
import math
import time
import random
import requests

# Paste your Google Maps API key here
GOOGLE_MAPS_API_KEY = "AIzaSyC0ZE5O5GKBfvfir45zWNYRb5Q-bSGBuyE"

INPUT_FILE = "skyline_webcams.json"
OUTPUT_FILE = "skyline_webcams_geocoded.json"

STOPWORDS = {"skylinewebcams", "cam", "cams", "live", "in"}

FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

# Maximum viewport span (degrees) before a result is considered an "area"
# rather than a specific point. ~0.02° ≈ ~2.2 km at the equator.
MAX_VIEWPORT_SPAN_DEG = 0.02


def clean_words(text: str) -> list[str]:
    """
    Lowercase, split by spaces, remove hyphens from each word,
    remove stopwords, and filter out empty strings.
    """
    words = text.lower().split()
    cleaned = []
    for word in words:
        word = word.replace("-", "")
        if word and word not in STOPWORDS:
            cleaned.append(word)
    return cleaned


def extract_search_query(h1: str, h2: str) -> str | None:
    """
    Extract common words between h1 and h2 after cleaning.
    Returns the joined query string, or None if either list is empty
    or the intersection is empty.
    """
    h1_words = clean_words(h1)
    h2_words = clean_words(h2)

    if not h1_words or not h2_words:
        return None

    # Find words common to both, preserving order from h1
    h2_set = set(h2_words)
    common = []
    seen = set()
    for word in h1_words:
        if word in h2_set and word not in seen:
            common.append(word)
            seen.add(word)

    if not common:
        return None

    return " ".join(common)


def geocode_query(query: str, session: requests.Session) -> dict | None:
    """
    Query Google Maps Find Place API with the given text.
    Returns a dict with 'lat', 'lng', and 'is_area' (True if the result
    represents a broad area rather than a specific point), or None.
    """
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "geometry",
        "key": GOOGLE_MAPS_API_KEY,
    }

    response = session.get(FIND_PLACE_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "OK" or not data.get("candidates"):
        return None

    geometry = data["candidates"][0].get("geometry", {})
    location = geometry.get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")

    if lat is None or lng is None:
        return None

    # Check viewport to determine if this is an area or a specific point
    is_area = False
    viewport = geometry.get("viewport", {})
    ne = viewport.get("northeast", {})
    sw = viewport.get("southwest", {})
    if ne and sw:
        lat_span = abs(ne.get("lat", 0) - sw.get("lat", 0))
        lng_span = abs(ne.get("lng", 0) - sw.get("lng", 0))
        if lat_span > MAX_VIEWPORT_SPAN_DEG or lng_span > MAX_VIEWPORT_SPAN_DEG:
            is_area = True

    return {"lat": lat, "lng": lng, "is_area": is_area,
            "viewport_span": (lat_span, lng_span) if ne and sw else None}


def main():
    if not GOOGLE_MAPS_API_KEY:
        print("ERROR: Please set GOOGLE_MAPS_API_KEY at the top of this script.")
        return

    # Read input data
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data: dict[str, list[list]] = json.load(f)

    session = requests.Session()

    total_records = sum(len(records) for records in data.values())
    geocoded = 0
    skipped_manual = 0
    skipped_no_query = 0
    remarked_manual = 0
    failed = 0

    output_data: dict[str, list[list]] = {}

    for country, records in data.items():
        print(f"\n=== {country} ({len(records)} records) ===")
        country_results = []

        for record in records:
            h1 = record[0] if len(record) > 0 else ""
            h2 = record[1] if len(record) > 1 else ""
            tag = record[3] if len(record) > 3 else ""

            # Skip records tagged "manual" — keep as-is
            if tag == "manual":
                country_results.append(record)
                skipped_manual += 1
                print(f"  SKIP (manual): {h1[:60]}")
                continue

            # Only geocode records tagged "automatic"
            if tag != "automatic":
                country_results.append(record)
                continue

            # Extract search query from common keywords
            query = extract_search_query(h1, h2)
            if not query:
                # Can't geocode — keep record as-is
                country_results.append(record)
                skipped_no_query += 1
                print(f"  SKIP (no common keywords): {h1[:50]}")
                continue

            print(f"  Query: \"{query}\"")

            # Call Google Maps Places API
            try:
                result = geocode_query(query, session)
            except requests.RequestException as e:
                print(f"    ERROR: {e}")
                country_results.append(record)
                failed += 1
                continue

            if result:
                lat = result["lat"]
                lng = result["lng"]
                is_area = result["is_area"]
                viewport_span = result.get("viewport_span")

                # Build enriched record: [h1, h2, url, tag, radius, lat, lng]
                enriched = list(record[:5])  # Preserve [h1, h2, url, tag, radius]

                if is_area:
                    # Result covers a broad area — re-mark as manual
                    enriched[3] = "manual"
                    enriched.append(lat)
                    enriched.append(lng)
                    country_results.append(enriched)
                    remarked_manual += 1
                    span_str = f"({viewport_span[0]:.4f}°×{viewport_span[1]:.4f}°)" if viewport_span else ""
                    print(f"    ⚠ AREA detected {span_str} → re-marked manual (lat={lat}, lng={lng})")
                else:
                    enriched.append(lat)
                    enriched.append(lng)
                    country_results.append(enriched)
                    geocoded += 1
                    print(f"    ✓ lat={lat}, lng={lng}")
            else:
                # No result — keep original record
                country_results.append(record)
                failed += 1
                print(f"    ✗ No results found")

            # Small delay to avoid rate limiting
            delay = random.uniform(0.1, 0.3)
            time.sleep(delay)

        output_data[country] = country_results

    # Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n{'='*50}")
    print(f"Done! Output saved to {OUTPUT_FILE}")
    print(f"  Total input records: {total_records}")
    print(f"  Skipped (manual): {skipped_manual}")
    print(f"  Skipped (no common keywords): {skipped_no_query}")
    print(f"  Geocoded successfully: {geocoded}")
    print(f"  Re-marked as manual (area too large): {remarked_manual}")
    print(f"  Failed to geocode: {failed}")


if __name__ == "__main__":
    main()

