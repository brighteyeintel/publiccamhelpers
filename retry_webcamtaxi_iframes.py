#!/usr/bin/env python3
"""
retry_webcamtaxi_iframes.py
Re-attempts iframe extraction for all unverified WCT records in combined.json
using realistic browser User-Agent headers.

Reads combined.json, finds WCT records with verified=False, fetches each URL
with browser-like headers, extracts <iframe> src, and updates verified/radius/url.
Overwrites combined.json with updated data.
"""

import json
import re
import html
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─── Configuration ────────────────────────────────────────────────────────────
COMBINED_FILE = "combined.json"
HTTP_TIMEOUT = 8
MAX_WORKERS = 40
VERIFIED_RADIUS = 10
UNVERIFIED_RADIUS = 200

# Realistic browser User-Agent strings (desktop + mobile)
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    # Safari on iPhone
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    # Samsung Browser on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/23.0 Chrome/115.0.0.0 Mobile Safari/537.36",
    # Chrome on ChromeOS
    "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


def unescape_text(text: str) -> str:
    """Remove backslash escaping and HTML entities from text."""
    if not text:
        return text
    text = text.replace("\\/", "/")
    text = html.unescape(text)
    return text


def get_browser_headers() -> dict:
    """Return realistic browser request headers with a random User-Agent."""
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def fetch_iframe_url(page_url: str) -> tuple[str | None, int, bool]:
    """
    Fetch a webcamtaxi page with browser-like headers and extract the first
    iframe src. Returns (iframe_src, http_status, success).
    """
    try:
        resp = requests.get(
            page_url,
            timeout=HTTP_TIMEOUT,
            allow_redirects=True,
            headers=get_browser_headers(),
        )
        status = resp.status_code
        if status == 200:
            # Find first iframe src
            match = re.search(
                r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE
            )
            if match:
                iframe_src = unescape_text(match.group(1))
                return iframe_src, status, True
        return None, status, False
    except requests.exceptions.Timeout:
        return None, 0, False
    except Exception:
        return None, -1, False


def main():
    t_start = time.time()

    # ── Load combined.json ────────────────────────────────────────────────
    print("Loading combined.json …")
    with open(COMBINED_FILE, "r", encoding="utf-8") as f:
        combined = json.load(f)

    # ── Collect unverified WCT records ────────────────────────────────────
    wct_targets: list[tuple[str, int, dict]] = []  # (country, index, record)
    for country, records in combined.items():
        for idx, rec in enumerate(records):
            if rec.get("source") == "WCT" and rec.get("verified") is False:
                wct_targets.append((country, idx, rec))

    total = len(wct_targets)
    print(f"  Found {total} unverified WCT records to retry\n")

    if total == 0:
        print("Nothing to do — all WCT records are already verified!")
        return

    # ── Fetch pages concurrently ──────────────────────────────────────────
    print(f"Fetching {total} pages ({MAX_WORKERS} workers, {HTTP_TIMEOUT}s timeout) …")

    status_counts: dict[int, int] = {}
    verified_count = 0
    failed_count = 0
    results = [None] * total
    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(fetch_iframe_url, rec["url"]): i
            for i, (_, _, rec) in enumerate(wct_targets)
        }
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            iframe_url, http_status, success = future.result()
            results[i] = (iframe_url, http_status, success)

            status_counts[http_status] = status_counts.get(http_status, 0) + 1

            done += 1
            if done % 200 == 0 or done == total:
                pct = done / total * 100
                print(f"  Progress: {done}/{total} ({pct:.1f}%)")

    # ── Apply results ─────────────────────────────────────────────────────
    for i, (country, idx, rec) in enumerate(wct_targets):
        iframe_url, http_status, success = results[i]
        if success and iframe_url:
            rec["url"] = iframe_url
            rec["verified"] = True
            rec["radius"] = VERIFIED_RADIUS
            verified_count += 1
        else:
            failed_count += 1

    # ── Write back ────────────────────────────────────────────────────────
    print(f"\nWriting updated combined.json …")
    with open(COMBINED_FILE, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    t_elapsed = time.time() - t_start

    # ── Statistics ────────────────────────────────────────────────────────
    all_records = [r for recs in combined.values() for r in recs]
    total_all = len(all_records)
    total_verified = sum(1 for r in all_records if r.get("verified") is True)
    wct_all = [r for r in all_records if r.get("source") == "WCT"]
    wct_verified = sum(1 for r in wct_all if r.get("verified") is True)

    print("\n" + "=" * 70)
    print("                     RETRY RESULTS")
    print("=" * 70)
    print(f"\n  WCT records retried:          {total}")
    print(f"  ├─ Now verified (iframe ok):   {verified_count}")
    print(f"  └─ Still unverified:           {failed_count}")

    print(f"\n  HTTP status breakdown:")
    for status in sorted(status_counts.keys()):
        label = {0: "Timeout", -1: "Error"}.get(status, str(status))
        print(f"    {label:>8}: {status_counts[status]}")

    print(f"\n  Overall dataset stats:")
    print(f"    Total records:     {total_all}")
    print(f"    Total verified:    {total_verified} ({total_verified / total_all * 100:.1f}%)")
    print(f"    WCT verified:      {wct_verified} / {len(wct_all)} ({wct_verified / len(wct_all) * 100:.1f}%)")

    print(f"\n  Elapsed time: {t_elapsed:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
