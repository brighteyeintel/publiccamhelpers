#!/usr/bin/env python3
"""
Smart geocoder for webcam_locations.json.
Processes only unverified records, using NLP-style heuristics to extract
location-relevant phrases from titles and subtitles, then geocodes them
via Google Maps Find Place API.
"""

import json
import re
import time
import random
import unicodedata
import requests

# --- Configuration ---
DATA_FILE = "webcam_locations.json"
GOOGLE_MAPS_API_KEY = "AIzaSyC0ZE5O5GKBfvfir45zWNYRb5Q-bSGBuyE"
FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
MAX_VIEWPORT_SPAN_DEG = 0.02

# Words to strip from subtitles (lowercase)
FILLER_WORDS = {
    "view", "of", "the", "from", "with", "and", "in", "over", "a", "an",
    "panoramic", "panorama", "background", "beach", "live", "cam", "webcam",
    "skylinewebcams", "selection", "best", "web", "on", "to", "at", "by",
    "its", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "it", "for", "between",
    "city", "town", "village", "area", "region", "province", "district",
    "along", "near", "towards", "facing", "looking", "showing",
    "featuring", "including", "located", "situated",
}

# Words to strip from titles (lowercase)
TITLE_NOISE = {
    "live", "cam", "webcam", "skylinewebcams", "cams",
}


def strip_accents(text: str) -> str:
    """Remove diacritics/accents but keep base characters."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def extract_title_location(title: str) -> str:
    """
    Extract the place-name portion from a title like:
      "Porto Seguro - Praia de Taperapuan Live cam"
    Returns: "Porto Seguro Praia de Taperapuan"
    """
    # Split on common delimiters
    parts = re.split(r"\s*[-–—|]\s*", title)

    # Remove parts that are just noise like "SkylineWebcams", "Live cam", country names
    cleaned_parts = []
    for part in parts:
        part_lower = part.strip().lower()
        # Skip if the part is entirely noise words
        words = part_lower.split()
        meaningful = [w for w in words if w not in TITLE_NOISE]
        if meaningful:
            cleaned_parts.append(part.strip())

    # Join all meaningful parts
    result = " ".join(cleaned_parts)

    # Remove trailing "Live cam" etc.
    result = re.sub(r"\s*(live\s*cam|webcam|skylinewebcams)\s*$", "", result, flags=re.IGNORECASE)
    result = re.sub(r"\s*(live\s*cam|webcam|skylinewebcams)\s*", " ", result, flags=re.IGNORECASE)

    return result.strip()


def extract_subtitle_locations(subtitle: str) -> str:
    """
    From the original subtitle, extract words/phrases that are likely
    location names by keeping capitalised words (proper nouns).

    "Cusco, view of Plaza de Armas with the Catedral Basilica de la Virgen
     de la Asunción in the background"
    → "Cusco Plaza Armas Catedral Basilica Virgen Asuncion"
    """
    if not subtitle:
        return ""

    # Remove punctuation but keep spaces and accented chars
    cleaned = re.sub(r"[,;:!?\"'()\[\]{}/\\]", " ", subtitle)
    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    words = cleaned.split()
    kept = []

    for word in words:
        word_lower = word.lower()

        # Skip filler words
        if word_lower in FILLER_WORDS:
            continue

        # Skip very short words (articles, prepositions) like "de", "la", "el", "do", "da"
        if len(word) <= 2 and word_lower in {"de", "la", "el", "do", "da", "le", "di", "du", "al", "il", "lo", "os", "as", "no", "na", "em"}:
            continue

        # Keep if starts with uppercase (proper noun)
        if word[0].isupper():
            kept.append(strip_accents(word))
        # Also keep numbers (could be addresses, squares like "14 de Septiembre")
        elif word.isdigit():
            kept.append(word)

    return " ".join(kept)


def build_queries(title_loc: str, subtitle_loc: str, country: str) -> list[str]:
    """
    Build a cascade of queries from most specific to least specific.
    Returns a list of query strings to try in order.
    """
    queries = []

    # Deduplicate words across title and subtitle
    title_words = title_loc.split() if title_loc else []
    subtitle_words = subtitle_loc.split() if subtitle_loc else []

    # Combined: title + subtitle (deduped) + country
    all_words = []
    seen = set()
    for w in title_words + subtitle_words:
        w_lower = w.lower()
        if w_lower not in seen and w_lower != country.lower():
            all_words.append(w)
            seen.add(w_lower)

    if all_words:
        full_query = " ".join(all_words) + " " + country
        queries.append(full_query)

    # Title location + country
    if title_loc:
        title_country = title_loc + " " + country
        if title_country not in queries:
            queries.append(title_country)

    # Title location only
    if title_loc and title_loc not in queries:
        queries.append(title_loc)

    # Subtitle location + country
    if subtitle_loc:
        sub_country = subtitle_loc + " " + country
        if sub_country not in queries:
            queries.append(sub_country)

    return queries


def geocode_query(query: str, session: requests.Session) -> dict | None:
    """
    Query Google Maps Find Place API.
    Returns dict with 'lat', 'lng', 'is_area', or None.
    """
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "geometry",
        "key": GOOGLE_MAPS_API_KEY,
    }

    try:
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

        is_area = False
        viewport = geometry.get("viewport", {})
        ne = viewport.get("northeast", {})
        sw = viewport.get("southwest", {})
        if ne and sw:
            lat_span = abs(ne.get("lat", 0) - sw.get("lat", 0))
            lng_span = abs(ne.get("lng", 0) - sw.get("lng", 0))
            if lat_span > MAX_VIEWPORT_SPAN_DEG or lng_span > MAX_VIEWPORT_SPAN_DEG:
                is_area = True

        return {"lat": lat, "lng": lng, "is_area": is_area}

    except Exception as e:
        print(f"      API error: {e}")
        return None


def main():
    print(f"Loading {DATA_FILE}...")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    session = requests.Session()

    # Stats
    total_unverified = 0
    geocoded_point = 0
    geocoded_area = 0
    skipped_no_query = 0
    already_has_coords = 0
    failed = 0

    for country, records in data.items():
        unverified = [i for i, r in enumerate(records) if not r.get("verified", False)]
        if not unverified:
            continue

        print(f"\n{'='*60}")
        print(f"{country} ({len(unverified)} unverified of {len(records)})")
        print(f"{'='*60}")

        for idx in unverified:
            rec = records[idx]
            total_unverified += 1

            title = rec.get("title", "")
            subtitle = rec.get("subtitle", "")

            # Skip records that already have coords (just need manual review)
            if rec.get("latitude") is not None and rec.get("longitude") is not None:
                already_has_coords += 1
                continue

            # Extract location info
            title_loc = extract_title_location(title)
            subtitle_loc = extract_subtitle_locations(subtitle)

            print(f"\n  [{idx+1}] {title[:70]}")
            print(f"       Title loc:    \"{title_loc}\"")
            print(f"       Subtitle loc: \"{subtitle_loc}\"")

            # Build query cascade
            queries = build_queries(title_loc, subtitle_loc, country)

            if not queries:
                print(f"       ✗ No query could be built")
                skipped_no_query += 1
                continue

            # Try each query in cascade
            result = None
            used_query = None
            for q in queries:
                print(f"       Trying: \"{q}\"")
                result = geocode_query(q, session)
                if result:
                    used_query = q
                    break
                time.sleep(random.uniform(0.1, 0.2))

            if result:
                rec["latitude"] = result["lat"]
                rec["longitude"] = result["lng"]

                if result["is_area"]:
                    # Area — set coords but keep unverified for manual check
                    geocoded_area += 1
                    print(f"       ⚠ AREA: {result['lat']:.6f}, {result['lng']:.6f} (keep unverified)")
                else:
                    # Point — set coords and mark verified
                    rec["verified"] = True
                    geocoded_point += 1
                    print(f"       ✓ POINT: {result['lat']:.6f}, {result['lng']:.6f}")
            else:
                failed += 1
                print(f"       ✗ All queries failed")

            time.sleep(random.uniform(0.1, 0.3))

        # Intermediate save after each country
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # Final summary
    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"{'='*60}")
    print(f"  Total unverified processed: {total_unverified}")
    print(f"  Already had coordinates:    {already_has_coords}")
    print(f"  Geocoded (point):           {geocoded_point}")
    print(f"  Geocoded (area):            {geocoded_area}")
    print(f"  No query built:             {skipped_no_query}")
    print(f"  Failed (all queries):       {failed}")


if __name__ == "__main__":
    main()
