# urls = [
#     "https://vedabase.io/en/library/bg/1/1/",
#     # "https://vedabase.io/en/library/bg/1/2/",
#     # ... so on
# ]

# import requests
# from bs4 import BeautifulSoup

# def extract_verse_info(url):
#     response = requests.get(url)
#     # print(f"response {response.content}")
#     soup = BeautifulSoup(response.content, 'html5lib')

#     with open("verse_page_pretty.html", "w", encoding="utf-8") as file:
#         file.write(soup.prettify())

#     # verse_id = soup.find('h1').text.strip()  # e.g., "Bg. 1.1"
    
#     # Find blocks by their tags or class names
#     # verse_blocks = soup.find_all("div", class_="verse-line")  # Sanskrit + transliteration
#     # verse_text = "\n".join([v.text.strip() for v in verse_blocks])
    
#     # translation = soup.find("div", class_="translation").text.strip()
#     # purport = soup.find("div", class_="purport").text.strip()

#     # return {
#     #     "verse_id": verse_id,
#     #     "text": verse_text,
#     #     "translation": translation,
#     #     "purport": purport
#     # }

# all_verses = [extract_verse_info(url) for url in urls]

# # import pandas as pd
# # df = pd.DataFrame(all_verses)
# # df.to_csv("gita_verses.csv", index=False)
# # df.to_json("gita_verses.json", indent=2)

import requests
import random
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
import os

# Create a directory for raw HTML files if needed
os.makedirs("raw_html", exist_ok=True)

# Define base URL and chapter/verse ranges to scrape
base_url = "https://vedabase.io/en/library/bg/"
chapters = range(7, 19)  # Chapters 1-18
verses_per_chapter = {
    1: 46, 2: 72, 3: 43, 4: 42, 5: 29, 6: 47, 7: 30, 8: 28, 9: 34,
    10: 42, 11: 55, 12: 20, 13: 35, 14: 27, 15: 20, 16: 24, 17: 28, 18: 78
}

def extract_verse_info(url, save_html=False, max_retries=3, backoff_factor=2):
    """Extract verse information from a Vedabase URL with retry logic"""
    print(f"Processing {url}")
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            # Add delay to avoid overloading the server (increase with each retry)
            delay = (backoff_factor ** retry_count) * 1.5
            time.sleep(delay)
            
            # Get the page content
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://vedabase.io/en/library/bg/',
                'Connection': 'keep-alive',
            }
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                # Parse HTML and continue with your existing code
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Save raw HTML if requested
                if save_html:
                    chapter_verse = re.search(r'bg/(\d+)/(\d+)', url)
                    if chapter_verse:
                        chapter, verse = chapter_verse.groups()
                        filename = f"raw_html/bg_{chapter}_{verse}.html"
                        with open(filename, "w", encoding="utf-8") as file:
                            file.write(soup.prettify())
                
                # Extract verse ID
                verse_id = soup.find('h1', id=lambda x: x and x.startswith('bb')).text.strip()
                
                # Extract devanagari
                devanagari_div = soup.find('div', class_='av-devanagari')
                devanagari = devanagari_div.find('div', class_='em:mb-4 em:leading-8 em:text-lg text-center').text.strip() if devanagari_div else ""
                
                # Extract verse text
                verse_text_div = soup.find('div', class_='av-verse_text')
                verse_text = verse_text_div.find('div', class_='em:mb-4 em:leading-8 em:text-base text-center italic').text.strip() if verse_text_div else ""
                
                # Extract translation
                translation_div = soup.find('div', class_='av-translation')
                translation = ""
                if translation_div:
                    translation_content = translation_div.find('div', class_=lambda x: x and 's-justify' in x)
                    if translation_content:
                        translation = translation_content.text.strip()
                
                # Extract purport
                purport_div = soup.find('div', class_='av-purport')
                purport = ""
                if purport_div:
                    purport_paragraphs = purport_div.find_all('div', class_=lambda x: x and 's-justify' in x)
                    purport = "\n\n".join([p.text.strip() for p in purport_paragraphs])
                
                # Return structured data
                return {
                    "verse_id": verse_id,
                    "devanagari": devanagari,
                    "verse_text": verse_text,
                    "translation": translation,
                    "purport": purport,
                    "url": url
                }
            else:
                print(f"Failed with status code {response.status_code}. Retrying ({retry_count+1}/{max_retries})...")
                
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Connection error: {e}. Retrying ({retry_count+1}/{max_retries})...")
        
        retry_count += 1
    
    print(f"Failed to retrieve {url} after {max_retries} attempts.")
    return None

def main():
    all_verses = []
    
    # Try to load existing progress if available
    try:
        with open("gita_verses_progress.json", "r", encoding="utf-8") as f:
            all_verses = json.load(f)
            print(f"Loaded {len(all_verses)} verses from progress file.")
    except FileNotFoundError:
        print("No progress file found. Starting from the beginning.")
    
    # Determine where to start from
    processed_urls = {verse["url"] for verse in all_verses if "url" in verse}
    
    # Create URLs for all chapters and verses
    for chapter in chapters:
        for verse in range(1, verses_per_chapter.get(chapter, 0) + 1):
            url = f"{base_url}{chapter}/{verse}/"
            
            # Skip already processed URLs
            if url in processed_urls:
                print(f"Skipping already processed {url}")
                continue
                
            verse_data = extract_verse_info(url, save_html=True)
            if verse_data:
                all_verses.append(verse_data)
                
                # Save progress after each verse (in case of interruption)
                with open("gita_verses_progress.json", "w", encoding="utf-8") as f:
                    json.dump(all_verses, f, ensure_ascii=False, indent=2)
            
            # Add random delay between requests to avoid pattern detection
            random_delay = random.uniform(2.5, 5.0)
            time.sleep(random_delay)
    
    # Convert to DataFrame and save
    df = pd.DataFrame(all_verses)
    
    # Save as CSV
    df.to_csv("gita_verses.csv", index=False, encoding="utf-8")
    
    # Save as JSON
    with open("gita_verses.json", "w", encoding="utf-8") as f:
        json.dump(all_verses, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully scraped {len(all_verses)} verses from the Bhagavad Gita.")
    print(f"Data saved to 'gita_verses.csv' and 'gita_verses.json'")

if __name__ == "__main__":
    main()