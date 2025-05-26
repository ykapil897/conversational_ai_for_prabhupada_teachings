import json
import re
import requests
import time
import random
from bs4 import BeautifulSoup
import os

def extract_verse_info(url, save_html=False, max_retries=3, backoff_factor=2):
    """Extract verse information from a Vedabase URL with retry logic"""
    print(f"Processing {url}")
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Add delay to avoid overloading the server (increase with each retry)
            delay = (backoff_factor ** retry_count) * 1.5
            time.sleep(delay)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # If HTML saving is enabled
                if save_html:
                    verse_id = url.rstrip('/').split('/')[-2:]
                    verse_id = '-'.join(verse_id)
                    os.makedirs("html_cache", exist_ok=True)
                    with open(f"html_cache/{verse_id}.html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                
                # Extract verse ID
                verse_id_elem = soup.select_one('.wrapper-breadcrumbs')
                if not verse_id_elem:
                    print(f"Cannot find verse ID for {url}")
                    return None
                
                verse_id = verse_id_elem.text.strip().split('»')[-1].strip()
                
                # Extract Devanagari text
                devanagari_elem = soup.select_one('.verse-sanskrit')
                devanagari = devanagari_elem.text.strip() if devanagari_elem else ""
                
                # Extract verse text (transliteration)
                verse_text_elem = soup.select_one('.verse-transliteration')
                verse_text = verse_text_elem.text.strip() if verse_text_elem else ""
                
                # Extract translation
                translation_elem = soup.select_one('.verse-translation')
                translation = translation_elem.text.strip() if translation_elem else ""
                
                # Extract purport
                purport_elem = soup.select_one('.verse-purport')
                purport = purport_elem.text.strip() if purport_elem else ""
                
                # Return structured data
                return {
                    "verse_id": verse_id,
                    "devanagari": devanagari,
                    "verse_text": verse_text,
                    "translation": translation,
                    "purport": purport,
                    "url": url
                }
            
            elif response.status_code == 404:
                print(f"Verse not found at {url} (404)")
                return None
            else:
                print(f"Unexpected status code {response.status_code} for {url}")
                retry_count += 1
                
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Request error: {e}")
            retry_count += 1
            print(f"Retrying {retry_count}/{max_retries}...")
        
    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None

def find_verse_insert_position(verses, chapter, verse_num):
    """Find the correct position to insert a verse into the ordered list"""
    for i, verse in enumerate(verses):
        if "verse_id" not in verse:
            continue
            
        match = re.search(r'Bg\.\s*(\d+)\.(\d+)', verse["verse_id"])
        if not match:
            continue
            
        v_chapter, v_verse = int(match.group(1)), int(match.group(2))
        
        if (v_chapter > chapter) or (v_chapter == chapter and v_verse > verse_num):
            return i
            
    return len(verses)  # Append at the end if no position found

def process_missing_combined_verses(base_url="https://vedabase.io/en/library/bg/"):
    """Process the JSON file to find and add missing combined verses"""
    try:
        # Load the existing progress file
        with open("gita_verses_progress.json", "r", encoding="utf-8") as f:
            all_verses = json.load(f)
            print(f"Loaded {len(all_verses)} verses from progress file.")
    except FileNotFoundError:
        print("Progress file not found.")
        return
        
    # Create a map of all existing verses
    existing_verses = {}
    for verse in all_verses:
        if "verse_id" in verse:
            match = re.search(r'Bg\.\s+(\d+)\.(\d+)', verse["verse_id"])
            if match:
                chapter, verse_num = int(match.group(1)), int(match.group(2))
                if chapter not in existing_verses:
                    existing_verses[chapter] = set()
                existing_verses[chapter].add(verse_num)
    
    # Define expected verse counts per chapter
    verses_per_chapter = {
        1: 46, 2: 72, 3: 43, 4: 42, 5: 29, 6: 47,
        7: 30, 8: 28, 9: 34, 10: 42, 11: 55, 12: 20,
        13: 35, 14: 27, 15: 20, 16: 24, 17: 28, 18: 78
    }
    
    # Track processed combined URLs to avoid duplicates
    processed_urls = {verse["url"] for verse in all_verses if "url" in verse}
    
    # Process each chapter to find missing verses
    for chapter in verses_per_chapter:
        if chapter not in existing_verses:
            existing_verses[chapter] = set()
            
        expected_verses = set(range(1, verses_per_chapter[chapter] + 1))
        missing_verses = [v for v in expected_verses if v not in existing_verses[chapter]]
        
        if missing_verses:
            print(f"Chapter {chapter} is missing verses: {missing_verses}")
            
            # Group consecutive missing verses
            missing_ranges = []
            start = None
            
            for i in sorted(missing_verses):
                if start is None:
                    start = i
                    end = i
                elif i == end + 1:
                    end = i
                else:
                    missing_ranges.append((start, end))
                    start = i
                    end = i
            
            if start is not None:
                missing_ranges.append((start, end))
            
            # Process each range of missing verses
            for start_verse, end_verse in missing_ranges:
                if end_verse > start_verse:  # Combined verses
                    combined_url = f"{base_url}{chapter}/{start_verse}-{end_verse}/"
                    
                    if combined_url in processed_urls:
                        print(f"Skipping already processed combined verses {combined_url}")
                        continue
                        
                    print(f"Trying combined verses: {combined_url}")
                    verse_data = extract_verse_info(combined_url, save_html=True)
                    
                    if verse_data:
                        # Find the correct position to insert this verse
                        insert_pos = find_verse_insert_position(all_verses, chapter, start_verse)
                        all_verses.insert(insert_pos, verse_data)
                        processed_urls.add(combined_url)
                        
                        print(f"Added combined verses {start_verse}-{end_verse} to chapter {chapter}")
                        
                        # Save progress after each verse
                        with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                            json.dump(all_verses, f, ensure_ascii=False, indent=2)
                    else:
                        print(f"Combined verse format failed for {combined_url}")
                
                else:  # Single verse
                    url = f"{base_url}{chapter}/{start_verse}/"
                    
                    if url in processed_urls:
                        print(f"Skipping already processed {url}")
                        continue
                        
                    verse_data = extract_verse_info(url, save_html=True)
                    if verse_data:
                        insert_pos = find_verse_insert_position(all_verses, chapter, start_verse)
                        all_verses.insert(insert_pos, verse_data)
                        processed_urls.add(url)
                        
                        print(f"Added verse {start_verse} to chapter {chapter}")
                        
                        # Save progress after each verse
                        with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                            json.dump(all_verses, f, ensure_ascii=False, indent=2)
                    else:
                        print(f"Verse not found: {url}")
                
                # Add random delay between requests
                random_delay = random.uniform(2.5, 5.0)
                time.sleep(random_delay)
    
    print(f"Processing complete. Total verses: {len(all_verses)}")
    
    # Save final version to both files
    with open("gita_verses.json", "w", encoding="utf-8") as f:
        json.dump(all_verses, f, ensure_ascii=False, indent=2)
    
    print("Updated data saved to 'gita_verses.json' and 'gita_verses_progress.json'")
    
    # Display a summary of verses per chapter
    verse_counts = {}
    for verse in all_verses:
        if "verse_id" in verse:
            match = re.search(r'Bg\.\s+(\d+)\.', verse["verse_id"])
            if match:
                chapter = int(match.group(1))
                verse_counts[chapter] = verse_counts.get(chapter, 0) + 1
    
    print("\nVerse count summary:")
    for chapter in sorted(verses_per_chapter.keys()):
        expected = verses_per_chapter[chapter]
        actual = verse_counts.get(chapter, 0)
        status = "✓" if expected == actual else "✗"
        print(f"Chapter {chapter}: {actual}/{expected} verses {status}")

def extract_verse_info_from_html(html_file_path):
    """Extract verse information from a cached HTML file"""
    print(f"Processing HTML file: {html_file_path}")
    
    try:
        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract verse ID from the heading
        verse_id_elem = soup.select_one('h1[id^="bb"]')
        if not verse_id_elem:
            # Try alternative selectors for verse ID
            verse_id_elem = soup.select_one('.text-center[id^="bb"]') or soup.select_one('[id^="bb"]')
            if not verse_id_elem:
                print(f"Cannot find verse ID in {html_file_path}")
                # Try to extract from filename
                filename = os.path.basename(html_file_path)
                match = re.search(r'bg_(\d+)_(\d+)', filename)
                if match:
                    chapter, verse = match.groups()
                    verse_id = f"Bg. {chapter}.{verse}"
                    print(f"Extracted verse ID from filename: {verse_id}")
                else:
                    return None
            else:
                verse_id = verse_id_elem.text.strip()
        else:
            verse_id = verse_id_elem.text.strip()
        
        # Extract Devanagari text
        devanagari = ""
        devanagari_div = soup.select_one('.av-devanagari')
        if devanagari_div:
            devanagari_elem = devanagari_div.select_one('.em\\:text-lg') or devanagari_div.select_one('.text-center')
            if devanagari_elem:
                devanagari = devanagari_elem.text.strip()
        
        # Extract verse text (transliteration)
        verse_text = ""
        verse_text_div = soup.select_one('.av-verse_text')
        if verse_text_div:
            verse_text_elem = verse_text_div.select_one('.text-center.italic') or verse_text_div.select_one('.italic')
            if verse_text_elem:
                verse_text = verse_text_elem.text.strip()
        
        # Extract translation
        translation = ""
        translation_div = soup.select_one('.av-translation')
        if translation_div:
            translation_elem = translation_div.select_one('.s-justify')
            if translation_elem:
                translation = translation_elem.text.strip()
        
        # Extract purport
        purport = ""
        purport_div = soup.select_one('.av-purport')
        if purport_div:
            purport_elems = purport_div.select('.s-justify')
            if purport_elems:
                purport = "\n\n".join([elem.text.strip() for elem in purport_elems])
        
        # Determine URL based on verse_id or breadcrumb
        url = None
        breadcrumb = soup.select_one('.breadcrumb')
        if breadcrumb:
            links = breadcrumb.select('a')
            if links and len(links) >= 3:
                # Extract chapter and verse from the breadcrumb links
                chapter_link = links[-2]['href'] if len(links) > 2 else ''
                match = re.search(r'/bg/(\d+)/', chapter_link)
                if match:
                    chapter = match.group(1)
                    verse_match = re.search(r'(\d+)(?:-(\d+))?$', verse_id)
                    if verse_match:
                        verse = verse_match.group(1)
                        url = f"https://vedabase.io/en/library/bg/{chapter}/{verse}/"
        
        if not url:
            # Try to reconstruct URL from verse_id
            match = re.search(r'Bg\.\s*(\d+)\.(\d+)', verse_id)
            if match:
                chapter, verse = match.groups()
                url = f"https://vedabase.io/en/library/bg/{chapter}/{verse}/"
            else:
                # Try to extract from filename
                filename = os.path.basename(html_file_path)
                match = re.search(r'(\d+)-(\d+)(?:-(\d+))?', filename)
                if match:
                    if match.group(3):  # If it's a range like 1-16-18
                        chapter, start_verse, end_verse = match.groups()
                        url = f"https://vedabase.io/en/library/bg/{chapter}/{start_verse}-{end_verse}/"
                    else:  # If it's a format like chapter-verse
                        chapter, verse = match.groups()
                        url = f"https://vedabase.io/en/library/bg/{chapter}/{verse}/"
        
        # Return structured data
        return {
            "verse_id": verse_id,
            "devanagari": devanagari,
            "verse_text": verse_text,
            "translation": translation,
            "purport": purport,
            "url": url
        }
    
    except Exception as e:
        print(f"Error processing HTML file {html_file_path}: {str(e)}")
        return None

def process_html_cache_files():
    """Process HTML cache files and add them to the verses JSON file"""
    # Make sure the HTML cache directory exists
    if not os.path.exists("html_cache"):
        print("HTML cache directory not found.")
        return
    
    try:
        # Load the existing progress file
        with open("gita_verses_progress.json", "r", encoding="utf-8") as f:
            all_verses = json.load(f)
            print(f"Loaded {len(all_verses)} verses from progress file.")
    except FileNotFoundError:
        print("Progress file not found. Creating a new one.")
        all_verses = []
    
    # Create a set of URLs that are already in the file
    existing_urls = {verse["url"] for verse in all_verses if "url" in verse}
    
    # Process all HTML files in the cache directory
    html_files = [f for f in os.listdir("html_cache") if f.endswith('.html')]
    processed_count = 0
    
    for html_file in html_files:
        html_file_path = os.path.join("html_cache", html_file)
        
        # Extract verse data from HTML
        verse_data = extract_verse_info_from_html(html_file_path)
        
        if verse_data and "url" in verse_data:
            if verse_data["url"] not in existing_urls:
                # Try to extract chapter and verse from verse_id
                match = re.search(r'Bg\.\s*(\d+)\.(\d+)', verse_data["verse_id"])
                if match:
                    chapter, verse_num = int(match.group(1)), int(match.group(2))
                    insert_pos = find_verse_insert_position(all_verses, chapter, verse_num)
                    all_verses.insert(insert_pos, verse_data)
                    existing_urls.add(verse_data["url"])
                    processed_count += 1
                    
                    print(f"Added verse {verse_data['verse_id']} from HTML cache")
                    
                    # Save progress after each verse
                    with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                        json.dump(all_verses, f, ensure_ascii=False, indent=2)
                else:
                    # Handle combined verses (like 1-16-18.html)
                    filename = os.path.basename(html_file_path)
                    match = re.search(r'(\d+)-(\d+)-(\d+)\.html', filename)
                    if match:
                        chapter, start_verse, end_verse = map(int, match.groups())
                        insert_pos = find_verse_insert_position(all_verses, chapter, int(start_verse))
                        all_verses.insert(insert_pos, verse_data)
                        existing_urls.add(verse_data["url"])
                        processed_count += 1
                        
                        print(f"Added combined verses {start_verse}-{end_verse} from chapter {chapter} from HTML cache")
                        
                        # Save progress after each verse
                        with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                            json.dump(all_verses, f, ensure_ascii=False, indent=2)
                    else:
                        print(f"Could not determine chapter and verse for {verse_data['verse_id']} or filename {filename}")
            else:
                print(f"Skipping already processed {verse_data['url']}")
        elif verse_data:
            print(f"No URL found for verse data extracted from {html_file_path}")
        else:
            print(f"No verse data could be extracted from {html_file_path}")
    
    print(f"Processing complete. Added {processed_count} verses from HTML cache.")
    
    # Save final version
    with open("gita_verses.json", "w", encoding="utf-8") as f:
        json.dump(all_verses, f, ensure_ascii=False, indent=2)
    
    print("Updated data saved to 'gita_verses.json' and 'gita_verses_progress.json'")

def process_raw_html_files():
    """Process HTML files from the raw_html directory and add them to the verses JSON file"""
    # Make sure the raw_html directory exists
    if not os.path.exists("raw_html"):
        print("raw_html directory not found.")
        return
    
    try:
        # Load the existing progress file
        with open("gita_verses_progress.json", "r", encoding="utf-8") as f:
            all_verses = json.load(f)
            print(f"Loaded {len(all_verses)} verses from progress file.")
    except FileNotFoundError:
        print("Progress file not found. Creating a new one.")
        all_verses = []
    
    # Create a set of URLs that are already in the file
    existing_urls = {verse["url"] for verse in all_verses if "url" in verse}
    
    # Process all HTML files in the raw_html directory
    html_files = [f for f in os.listdir("raw_html") if f.endswith('.html')]
    processed_count = 0
    
    for html_file in html_files:
        html_file_path = os.path.join("raw_html", html_file)
        
        # Extract verse data from HTML
        verse_data = extract_verse_info_from_html(html_file_path)
        
        if verse_data and "url" in verse_data and verse_data["url"] not in existing_urls:
            # Try to extract chapter and verse from filename
            filename = os.path.basename(html_file_path)
            match = re.search(r'bg_(\d+)_(\d+)', filename)
            if match:
                chapter, verse_num = int(match.group(1)), int(match.group(2))
                insert_pos = find_verse_insert_position(all_verses, chapter, verse_num)
                all_verses.insert(insert_pos, verse_data)
                existing_urls.add(verse_data["url"])
                processed_count += 1
                
                print(f"Added verse {verse_data['verse_id']} from raw_html file")
                
                # Save progress after each verse
                with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                    json.dump(all_verses, f, ensure_ascii=False, indent=2)
            else:
                # Try to extract from verse_id if filename parsing failed
                match = re.search(r'Bg\.\s*(\d+)\.(\d+)', verse_data["verse_id"])
                if match:
                    chapter, verse_num = int(match.group(1)), int(match.group(2))
                    insert_pos = find_verse_insert_position(all_verses, chapter, verse_num)
                    all_verses.insert(insert_pos, verse_data)
                    existing_urls.add(verse_data["url"])
                    processed_count += 1
                    
                    print(f"Added verse {verse_data['verse_id']} from raw_html file")
                    
                    # Save progress after each verse
                    with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                        json.dump(all_verses, f, ensure_ascii=False, indent=2)
                else:
                    print(f"Could not determine chapter and verse for {verse_data['verse_id']} or filename {filename}")
        elif verse_data:
            print(f"Skipping already processed {verse_data['url']}")
        else:
            print(f"No verse data could be extracted from {html_file_path}")
    
    print(f"Processing complete. Added {processed_count} verses from raw_html files.")
    
    # Save final version
    with open("gita_verses.json", "w", encoding="utf-8") as f:
        json.dump(all_verses, f, ensure_ascii=False, indent=2)
    
    print("Updated data saved to 'gita_verses.json' and 'gita_verses_progress.json'")

if __name__ == "__main__":
    # Make sure the HTML cache directory exists
    os.makedirs("html_cache", exist_ok=True)
    
    # Process HTML files from different directories
    process_html_cache_files()   # Process files from html_cache directory
    process_raw_html_files()     # Process files from raw_html directory
    
    # Process missing combined verses from the website
    process_missing_combined_verses()