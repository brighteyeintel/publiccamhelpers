import json
import requests
import re
import time

INPUT_FILE = "webcam_locations.json"
OUTPUT_FILE = "webcam_locations.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def extract_youtube_id(html):
    # Pattern for youtube.com/embed/VIDEO_ID
    # looking for something like: src="https://www.youtube.com/embed/dQw4w9WgXcQ"
    # or just the id in some config
    
    # regex for embed URL
    embed_match = re.search(r'youtube\.com/embed/([a-zA-Z0-9_-]+)', html)
    if embed_match:
        return embed_match.group(1)
        
    return None

def main():
    print(f"Loading {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        return

    updated_count = 0
    total_checked = 0
    
    countries = list(data.keys())
    
    for country in countries:
        records = data[country]
        print(f"Processing {country} ({len(records)} records)...")
        
        for record in records:
            url = record.get('url', '')
            
            # Skip if already a YouTube link
            if 'youtube.com' in url or 'youtu.be' in url:
                continue
                
            # Skip if not skylinewebcams (just in case)
            if 'skylinewebcams.com' not in url:
                continue

            total_checked += 1
            
            try:
                # print(f"  Checking: {url}")
                response = requests.get(url, headers=HEADERS, timeout=10)
                if response.status_code == 200:
                    yt_id = extract_youtube_id(response.text)
                    if yt_id:
                        new_url = f"https://www.youtube.com/watch?v={yt_id}"
                        print(f"    FOUND YouTube ID: {yt_id} -> {new_url}")
                        print(f"    Replacing: {url}")
                        record['url'] = new_url
                        updated_count += 1
                    else:
                        # print("    No YouTube embed found.")
                        pass
                else:
                    print(f"    Failed to fetch (status {response.status_code}): {url}")
            
            except Exception as e:
                print(f"    Error processing {url}: {e}")
            
            # Be nice to the server
            time.sleep(0.5)

    print("-" * 30)
    print(f"Processing complete.")
    print(f"Total Skyline URLs checked: {total_checked}")
    print(f"Updated records: {updated_count}")

    if updated_count > 0:
        print(f"Saving changes to {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print("Done.")
    else:
        print("No changes made.")

if __name__ == "__main__":
    main()
