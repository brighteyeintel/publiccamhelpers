#!/usr/bin/env python3
"""
fetch_wct_youtube.py
Uses pyppeteer (Python Puppeteer) to visit each WCT-sourced webcam page in
combined.json, find YouTube iframes, and replace the URL with the YouTube URL.

Runs multiple concurrent browser pages for speed.

Usage:
    python3 fetch_wct_youtube.py                       # process all WCT records
    python3 fetch_wct_youtube.py --limit 20            # process first 20 only
    python3 fetch_wct_youtube.py --dry-run             # don't write changes
    python3 fetch_wct_youtube.py --start 50            # skip first 50 records
    python3 fetch_wct_youtube.py --concurrency 10      # 10 browser tabs at once
"""

import asyncio
import json
import argparse
import time

COMBINED_FILE = "combined.json"


async def create_page(browser):
    """Create a new browser page with anti-detection and realistic settings."""
    page = await browser.newPage()
    await page.setViewport({"width": 1280, "height": 800})
    await page.setUserAgent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    await page.evaluateOnNewDocument("""
        () => {
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        }
    """)
    return page


async def extract_youtube_url(page, url, index, total):
    """
    Navigate to a webcamtaxi URL, wait for page load, and look for a
    YouTube iframe. Returns the YouTube URL if found, or None.
    """
    prefix = f"  [{index+1}/{total}]"
    try:
        await page.goto(url, {"waitUntil": "networkidle0", "timeout": 30000})
        await asyncio.sleep(3)

        youtube_url = await page.evaluate("""
            () => {
                const iframes = document.querySelectorAll('iframe');
                for (const iframe of iframes) {
                    const src = iframe.src || iframe.getAttribute('data-src') || '';
                    if (src.includes('youtube.com') || src.includes('youtu.be')) {
                        return src;
                    }
                }
                const allElements = document.querySelectorAll(
                    '[src*="youtube"], [data-src*="youtube"]'
                );
                for (const el of allElements) {
                    const src = el.src || el.getAttribute('data-src') || '';
                    if (src.includes('youtube.com') || src.includes('youtu.be')) {
                        return src;
                    }
                }
                return null;
            }
        """)

        if youtube_url:
            youtube_url = youtube_url.replace("\\/", "/")
            print(f"{prefix} ✓ FOUND YouTube: {youtube_url}")
            return youtube_url
        else:
            print(f"{prefix} – No YouTube iframe: {url}")
            return None

    except Exception as e:
        error_msg = str(e)
        if len(error_msg) > 120:
            error_msg = error_msg[:120] + "..."
        print(f"{prefix} ✗ Error: {error_msg}")
        return None


async def worker(worker_id, page, queue, results, total):
    """
    Worker coroutine: pulls (index, record) from the queue, processes it,
    and stores results.
    """
    while True:
        try:
            index, rec = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        url = rec["url"]
        youtube_url = await extract_youtube_url(page, url, index, total)
        results[index] = youtube_url

        # Small delay between requests per-worker
        await asyncio.sleep(0.5)


async def main():
    parser = argparse.ArgumentParser(
        description="Extract YouTube iframe URLs from WCT records in combined.json"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Only process the first N WCT records (0 = all)"
    )
    parser.add_argument(
        "--start", type=int, default=0,
        help="Skip the first N WCT records"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't write changes to the file"
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser in headless mode (may trigger Cloudflare blocks)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=10,
        help="Number of concurrent browser tabs (default: 5)"
    )
    parser.add_argument(
        "--save-interval", type=int, default=30,
        help="Save progress to file every N seconds (0 = only at end)"
    )
    args = parser.parse_args()

    # ── Load data ─────────────────────────────────────────────────────────
    print(f"Loading {COMBINED_FILE}...")
    with open(COMBINED_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ── Collect WCT records ───────────────────────────────────────────────
    wct_records = []
    for country, records in data.items():
        for rec in records:
            if rec.get("source") == "WCT":
                url = rec.get("url", "")
                if "youtube.com" in url or "youtu.be" in url:
                    continue
                wct_records.append(rec)

    if args.start > 0:
        wct_records = wct_records[args.start:]
    if args.limit > 0:
        wct_records = wct_records[:args.limit]

    total = len(wct_records)
    concurrency = min(args.concurrency, total) if total > 0 else 1
    print(f"Found {total} WCT records to process with {concurrency} concurrent tabs")
    if total == 0:
        print("Nothing to do!")
        return

    # ── Launch browser ────────────────────────────────────────────────────
    from pyppeteer import launch

    print(f"\nLaunching browser (headless={args.headless}, tabs={concurrency})...")
    browser = await launch(
        headless=args.headless,
        executablePath="/usr/bin/google-chrome",
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1280,800",
        ],
        ignoreDefaultArgs=["--enable-automation"],
    )

    # Create a pool of browser pages
    pages = []
    for _ in range(concurrency):
        pages.append(await create_page(browser))

    # ── Process records in concurrent batches ─────────────────────────────
    t_start = time.time()
    last_save_time = t_start
    found_count = 0
    not_found_count = 0
    error_count = 0
    processed = 0

    # Process in batches of `concurrency` size
    batch_size = concurrency
    num_batches = (total + batch_size - 1) // batch_size

    print(f"\nProcessing {total} records in {num_batches} batches of up to {batch_size}...\n")

    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_end = min(batch_start + batch_size, total)
        batch = wct_records[batch_start:batch_end]
        actual_batch_size = len(batch)

        # Build queue for this batch
        queue = asyncio.Queue()
        for i, rec in enumerate(batch):
            queue.put_nowait((batch_start + i, rec))

        # Results dict keyed by global index
        results = {}

        # Launch workers
        tasks = []
        for w in range(min(actual_batch_size, concurrency)):
            tasks.append(
                asyncio.create_task(
                    worker(w, pages[w], queue, results, total)
                )
            )
        await asyncio.gather(*tasks)

        # Apply results
        for global_idx, youtube_url in results.items():
            rec = wct_records[global_idx]
            if youtube_url:
                rec["url"] = youtube_url
                found_count += 1
            else:
                not_found_count += 1

        processed += actual_batch_size
        elapsed = time.time() - t_start
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        print(
            f"  📊 Batch {batch_idx+1}/{num_batches} done | "
            f"Processed: {processed}/{total} | "
            f"YouTube: {found_count} | "
            f"Rate: {rate:.1f}/s | "
            f"ETA: {eta/60:.0f}m"
        )

        # Time-based incremental save
        now = time.time()
        if (
            args.save_interval > 0
            and (now - last_save_time) >= args.save_interval
            and not args.dry_run
            and found_count > 0
        ):
            print(f"  💾 Saving progress ({found_count} YouTube URLs so far)...")
            with open(COMBINED_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            last_save_time = now

    # ── Close browser ─────────────────────────────────────────────────────
    await browser.close()

    # ── Save results ──────────────────────────────────────────────────────
    elapsed = time.time() - t_start

    print("\n" + "=" * 60)
    print("                RESULTS")
    print("=" * 60)
    print(f"  Records processed:    {total}")
    print(f"  YouTube URLs found:   {found_count}")
    print(f"  No YouTube iframe:    {not_found_count}")
    print(f"  Concurrency:          {concurrency} tabs")
    print(f"  Elapsed time:         {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"  Avg per record:       {elapsed/total:.1f}s")
    print(f"  Effective rate:       {total/elapsed:.1f} records/s")
    print("=" * 60)

    if args.dry_run:
        print("\n  ⚠️  DRY RUN — no changes written to disk")
    elif found_count > 0:
        print(f"\n  Writing updated {COMBINED_FILE}...")
        with open(COMBINED_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print("  ✓ Done!")
    else:
        print("\n  No YouTube URLs found — file unchanged.")


if __name__ == "__main__":
    asyncio.run(main())
