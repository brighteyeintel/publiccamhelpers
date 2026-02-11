#!/usr/bin/env python3
"""
Classifies webcam records as 'automatic' or 'manual' and assigns an
approximate accuracy radius (metres, max 2500).

Rules:
- Records already tagged "manual" keep that tag.
- Records starting with "Live Cams in" or "Liva Cams in" are dropped entirely.
- Records referencing large/vague areas (panoramas, cities, streets, traffic,
  skylines, generic country views, etc.) → "manual", larger radius.
- Records referencing specific landmarks (named beaches, plazas, churches,
  lighthouses, piers, airports, monuments, parks, restaurants, shops, etc.)
  → "automatic", smaller radius.
"""

import json
import re

INPUT_FILE = "skyline_webcams.json"
OUTPUT_FILE = "skyline_webcams.json"  # overwrite in place

# ------------------------------------------------------------------
# Keyword lists for classification
# ------------------------------------------------------------------

# Specific landmark keywords → "automatic" with small radius
SPECIFIC_KEYWORDS = [
    "beach", "playa", "sands", "reef", "cove", "bay",
    "plaza", "square", "piazza", "platz",
    "church", "cathedral", "basilica", "chapel", "mosque", "parish",
    "lighthouse", "faro",
    "pier", "dock", "wharf", "marina", "harbour", "harbor", "port",
    "airport", "runway", "station",
    "bridge", "puente",
    "monument", "obelisk", "statue", "memorial", "fountain",
    "tower", "castle", "fortress", "fort",
    "park", "garden", "zoo",
    "museum", "theater", "theatre", "stadium",
    "market", "shop", "restaurant", "bar", "cafe",
    "waterfall", "canyon", "cave", "volcano",
    "temple", "shrine", "monastery",
    "pool", "resort",
    "ski", "slope",
    "nest", "cahow", "falcon",
    "underwater", "coral",
    "crossing", "road", "street", "avenue", "avenida",
    "village", "köyü",
    "clock",
    "inspection",
]

# Large/vague area keywords → "manual" with large radius
VAGUE_KEYWORDS = [
    "panorama", "panoramic", "skyline",
    "northern lights",
    "traffic",
]

# Pattern: "CityName - Country Live cam" with h2 like "View from CityName in Country"
# (no specific landmark mentioned beyond city name)
GENERIC_CITY_PATTERNS = [
    # h1 patterns suggesting just a city/region view
    r"^[\w\s\.\'-áéíóúñüç]+ - [\w\s]+ live cam$",
]


def classify_record(h1: str, h2: str) -> tuple[str, int]:
    """
    Returns (tag, radius) where tag is 'automatic' or 'manual'.
    """
    combined = (h1 + " " + h2).lower()

    # Check for vague/large area keywords first
    for kw in VAGUE_KEYWORDS:
        if kw in combined:
            return ("manual", 2500)

    # Check for specific landmark keywords
    for kw in SPECIFIC_KEYWORDS:
        # Use word boundary matching to avoid false positives
        if re.search(r'\b' + re.escape(kw) + r'\b', combined, re.IGNORECASE):
            # Determine radius based on type
            if kw in ("beach", "playa", "sands", "cove", "bay", "reef"):
                return ("automatic", 200)
            elif kw in ("plaza", "square", "piazza", "platz", "fountain",
                        "monument", "obelisk", "statue", "memorial"):
                return ("automatic", 100)
            elif kw in ("church", "cathedral", "basilica", "chapel",
                        "mosque", "parish", "temple", "shrine", "monastery"):
                return ("automatic", 100)
            elif kw in ("lighthouse", "faro"):
                return ("automatic", 50)
            elif kw in ("pier", "dock", "wharf", "marina"):
                return ("automatic", 150)
            elif kw in ("harbour", "harbor", "port"):
                return ("automatic", 300)
            elif kw in ("airport", "runway"):
                return ("automatic", 500)
            elif kw in ("station"):
                return ("automatic", 200)
            elif kw in ("bridge", "puente"):
                return ("automatic", 200)
            elif kw in ("tower", "castle", "fortress", "fort"):
                return ("automatic", 150)
            elif kw in ("park", "garden", "zoo"):
                return ("automatic", 300)
            elif kw in ("museum", "theater", "theatre", "stadium"):
                return ("automatic", 200)
            elif kw in ("market", "shop", "restaurant", "bar", "cafe"):
                return ("automatic", 100)
            elif kw in ("waterfall", "canyon", "cave"):
                return ("automatic", 300)
            elif kw in ("volcano"):
                return ("automatic", 500)
            elif kw in ("pool", "resort"):
                return ("automatic", 200)
            elif kw in ("ski", "slope"):
                return ("automatic", 500)
            elif kw in ("nest", "cahow", "falcon"):
                return ("manual", 500)
            elif kw in ("underwater", "coral"):
                return ("manual", 300)
            elif kw in ("crossing", "road", "street", "avenue", "avenida"):
                return ("automatic", 200)
            elif kw in ("village", "köyü"):
                return ("automatic", 500)
            elif kw in ("clock"):
                return ("automatic", 100)
            elif kw in ("inspection"):
                return ("manual", 500)
            else:
                return ("automatic", 200)

    # Check for generic city/town patterns
    # If h1 is just "CityName - Country Live cam" and h2 is
    # "View from/over CityName" without specifics → manual
    h1_lower = h1.lower()

    # Generic patterns: "City - Country Live cam" without landmark info
    generic_patterns = [
        r"^.+ - .+ live cam$",  # "X - Y Live cam"
    ]

    # Check if h2 is also generic (just "View from/of/over X in Y")
    h2_lower = h2.lower()
    h2_is_generic = bool(re.match(
        r"^(view (from|of|over|on)|panoramic view)[\s\w,\.\'-áéíóúñüçê]+$",
        h2_lower
    ))

    # If both are generic with no specific landmark words in either
    landmark_absent = not any(
        re.search(r'\b' + re.escape(kw) + r'\b', combined)
        for kw in SPECIFIC_KEYWORDS
    )

    if h2_is_generic and landmark_absent:
        # Likely a generic city view
        return ("manual", 2000)

    # Default: if we can't determine, mark automatic with moderate radius
    return ("automatic", 500)


def estimate_radius_for_manual(h1: str, h2: str) -> int:
    """For records already tagged manual, estimate a radius."""
    combined = (h1 + " " + h2).lower()

    # Very specific things that got manual tag
    for kw in ["beach", "playa", "plaza", "square", "pier", "lighthouse"]:
        if kw in combined:
            return 200

    # City-level
    if any(kw in combined for kw in ["panorama", "panoramic", "skyline"]):
        return 2500

    # Traffic
    if "traffic" in combined:
        return 500

    # Default for manual
    return 1500


def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    stats = {"automatic": 0, "manual": 0, "filtered": 0, "already_manual": 0}

    output_data = {}

    for country, records in data.items():
        country_results = []

        for record in records:
            h1 = record[0] if len(record) > 0 else ""
            h2 = record[1] if len(record) > 1 else ""

            # Skip "Live Cams in..." records
            if h1.lower().startswith("live cams in") or h1.lower().startswith("liva cams in"):
                stats["filtered"] += 1
                continue

            # Already tagged manual by the user
            if len(record) >= 4 and record[3] == "manual":
                stats["already_manual"] += 1
                # Add radius if not present
                if len(record) < 5:
                    radius = estimate_radius_for_manual(h1, h2)
                    new_record = list(record[:4]) + [radius]
                else:
                    new_record = list(record)
                country_results.append(new_record)
                continue

            # Classify the record
            tag, radius = classify_record(h1, h2)
            stats[tag] += 1

            # Build new record: [h1, h2, url, tag, radius]
            new_record = list(record[:3]) + [tag, radius]
            country_results.append(new_record)

            if tag == "manual":
                print(f"  MANUAL: {h1[:70]} (radius={radius})")

        output_data[country] = country_results

    # Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nDone! Written to {OUTPUT_FILE}")
    print(f"  Filtered out (Live Cams in...): {stats['filtered']}")
    print(f"  Already tagged manual by user: {stats['already_manual']}")
    print(f"  Classified automatic: {stats['automatic']}")
    print(f"  Classified manual: {stats['manual']}")
    total = sum(stats.values())
    print(f"  Total processed: {total}")


if __name__ == "__main__":
    main()
