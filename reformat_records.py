#!/usr/bin/env python3
"""
Reformats skyline_webcams_geocoded.json from arrays into named dictionary objects.
"""

import json

INPUT_FILE = "skyline_webcams_geocoded.json"
OUTPUT_FILE = "skyline_webcams_final.json"


def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    output_data = {}

    for country, records in data.items():
        country_results = []
        for record in records:
            entry = {
                "title": record[0] if len(record) > 0 else "",
                "subtitle": record[1] if len(record) > 1 else "",
                "url": record[2] if len(record) > 2 else "",
                "mode": record[3] if len(record) > 3 else "",
                "radius": record[4] if len(record) > 4 else None,
                "latitude": record[5] if len(record) > 5 else None,
                "longitude": record[6] if len(record) > 6 else None,
                "verified": False,
            }
            country_results.append(entry)
        output_data[country] = country_results

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    total = sum(len(r) for r in output_data.values())
    with_coords = sum(
        1 for r in output_data.values()
        for e in r if e["latitude"] is not None
    )
    print(f"Done! Written to {OUTPUT_FILE}")
    print(f"  Total records: {total}")
    print(f"  With coordinates: {with_coords}")
    print(f"  Without coordinates: {total - with_coords}")


if __name__ == "__main__":
    main()
