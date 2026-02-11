#!/usr/bin/env python3
"""
Scraper for skylinewebcams.com - extracts individual webcam feed URLs per country.
"""

import json
import time
import random
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.skylinewebcams.com/en/webcam/{country}.html"

COUNTRIES = [
    # Americas
    "Argentina",
    "Barbados",
    "Belize",
    "Bermuda",
    "Bolivia",
    "Brazil",
    "Canada",
    "Caribbean Netherlands",
    "Chile",
    "Costa Rica",
    "Dominican Republic",
    "Ecuador",
    "El Salvador",
    "Grenada",
    "Guadeloupe",
    "Honduras",
    "Martinique",
    "Mexico",
    "Panama",
    "Peru",
    "Sint Maarten",
    "United States",
    "Uruguay",
    "US Virgin Islands",
    "Venezuela",
    # Europe
    "Albania",
    "Austria",
    "Bosnia and Herzegovina",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Faroe Islands",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Iceland",
    "Ireland",
    "Italy",
    "Luxembourg",
    "Malta",
    "Norway",
    "Poland",
    "Portugal",
    "Republic of San Marino",
    "Romania",
    "Slovenia",
    "Spain",
    "Switzerland",
    "United Kingdom",
    # Africa
    "Egypt",
    "Kenya",
    "Mauritius",
    "Morocco",
    "Senegal",
    "Seychelles",
    "South Africa",
    "Zambia",
    "Zanzibar",
    # Asia
    "China",
    "Jordan",
    "Maldives",
    "Philippines",
    "Sri Lanka",
    "Thailand",
    "Turkey",
    "United Arab Emirates",
    "Vietnam",
    # Oceania
    "Australia",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]


def format_country_slug(country_name: str) -> str:
    """Convert a country name to the URL slug format (lowercase, hyphen-separated)."""
    return country_name.lower().replace(" ", "-")


def scrape_feed_page(feed_url: str, session: requests.Session) -> tuple[str, str, str]:
    """
    Fetch an individual feed page and extract:
    - The h1 tag text
    - The h2 tag text
    - The final URL (YouTube if the video src is a YouTube address, otherwise the feed URL)
    """
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

    response = session.get(feed_url, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Find h1 and h2 inside the info container
    # XPath: /html/body/div[2]/div[1]/div[1]/div[2]
    h1_text = ""
    h2_text = ""

    h1_tag = soup.find("h1")
    if h1_tag:
        h1_text = h1_tag.get_text(strip=True)

    h2_tag = soup.find("h2")
    if h2_tag:
        h2_text = h2_tag.get_text(strip=True)

    # Check for a YouTube video source
    final_url = feed_url
    video_tag = soup.find("video")
    if video_tag:
        video_src = video_tag.get("src", "")
        if "youtube" in video_src or "youtu.be" in video_src:
            final_url = video_src
    # Also check for <source> tags inside <video>
    if final_url == feed_url and video_tag:
        source_tag = video_tag.find("source")
        if source_tag:
            source_src = source_tag.get("src", "")
            if "youtube" in source_src or "youtu.be" in source_src:
                final_url = source_src

    # Also check for embedded YouTube iframes (common pattern)
    if final_url == feed_url:
        iframe = soup.find("iframe")
        if iframe:
            iframe_src = iframe.get("src", "")
            if "youtube" in iframe_src or "youtu.be" in iframe_src:
                final_url = iframe_src

    return (h1_text, h2_text, final_url)


def scrape_country(country_name: str, session: requests.Session) -> list[tuple[str, str, str]]:
    """Scrape all webcam feed URLs for a given country, then fetch each feed page for details."""
    slug = format_country_slug(country_name)
    url = BASE_URL.format(country=slug)

    # Rotate user agent
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

    print(f"  Fetching: {url}")
    response = session.get(url, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the <div class="row list"> element (corresponds to XPath /html/body/div[2]/div/div)
    container = soup.find("div", class_="row list")
    if not container:
        print(f"  WARNING: Could not find 'row list' container for {country_name}")
        return []

    # Collect relative HREFs only — discard fully qualified external links
    relative_hrefs = []
    for a_tag in container.find_all("a", href=True):
        href = a_tag["href"]
        # Discard fully qualified URLs (starting with https://www.)
        if href.startswith("https://www.") or href.startswith("http://www."):
            continue
        relative_hrefs.append(href)

    # Deduplicate while preserving order
    seen = set()
    unique_hrefs = []
    for href in relative_hrefs:
        if href not in seen:
            seen.add(href)
            unique_hrefs.append(href)

    print(f"  Found {len(unique_hrefs)} feed links to scrape")

    # Now fetch each individual feed page for h1, h2, and video info
    results: list[tuple[str, str, str]] = []
    for j, href in enumerate(unique_hrefs, 1):
        feed_url = "https://www.skylinewebcams.com" + (href if href.startswith("/") else "/" + href)
        print(f"    [{j}/{len(unique_hrefs)}] Fetching feed: {feed_url}")
        try:
            h1, h2, final_url = scrape_feed_page(feed_url, session)
            results.append((h1, h2, final_url))
            print(f"      h1: {h1[:60]}{'...' if len(h1) > 60 else ''}")
            print(f"      h2: {h2[:60]}{'...' if len(h2) > 60 else ''}")
            if final_url != feed_url:
                print(f"      YouTube: {final_url}")
        except requests.RequestException as e:
            print(f"      ERROR: {e}")
            results.append(("", "", feed_url))

        # Short delay between individual feed requests
        delay = random.uniform(0.5, 1.5)
        time.sleep(delay)

    return results


def main():
    output_file = "skyline_webcams.json"
    data: dict[str, list[tuple[str, str, str]]] = {}

    session = requests.Session()
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    })

    total = len(COUNTRIES)
    for i, country in enumerate(COUNTRIES, 1):
        print(f"\n[{i}/{total}] Scraping: {country}")
        try:
            feed_data = scrape_country(country, session)
            data[country] = feed_data
            print(f"  Completed: {len(feed_data)} feeds scraped")
        except requests.RequestException as e:
            print(f"  ERROR: Failed to scrape {country}: {e}")
            data[country] = []

        # Random delay between countries
        if i < total:
            delay = random.uniform(1.0, 3.0)
            print(f"  Waiting {delay:.1f}s before next country...")
            time.sleep(delay)

    # Save to JSON (tuples become arrays in JSON)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Print summary
    total_feeds = sum(len(feeds) for feeds in data.values())
    countries_with_data = sum(1 for feeds in data.values() if feeds)
    youtube_count = sum(
        1
        for feeds in data.values()
        for _, _, url in feeds
        if "youtube" in url or "youtu.be" in url
    )
    print(f"\nDone! Saved to {output_file}")
    print(f"  Countries scraped: {total}/{total}")
    print(f"  Countries with data: {countries_with_data}")
    print(f"  Total feeds: {total_feeds}")
    print(f"  YouTube feeds: {youtube_count}")


if __name__ == "__main__":
    main()
