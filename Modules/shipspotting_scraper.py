#!/usr/bin/env python3
"""
shipspotting_scraper.py
Module for scraping vessel images from ShipSpotting - Fixed version
"""

import re
import time
import json
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from bs4 import BeautifulSoup
from PIL import Image

# ====================== Configuration ======================
BASE_URL = "https://www.shipspotting.com"
PHOTO_URL = BASE_URL + "/photos/{pid}"

# Performance settings
STEP_DELAY = 0.15  # Minimal delay between requests
MAX_PHOTOS_PER_IMO = 50  # Maximum photos per vessel
MAX_WORKERS = 5  # Parallel workers
BATCH_SIZE = 5  # IMOs per batch
RETRY_ATTEMPTS = 2  # Retry attempts

# Thread-safe session management
session_lock = threading.Lock()

# ====================== ShipSpotting Scraper Class ======================
class ShipSpottingScraper:
    def __init__(self, worker_id: int = 0):
        """Initialize scraper with worker ID"""
        self.worker_id = worker_id
        self.session = cloudscraper.create_scraper()
        self._initialize_session()
        
    def _initialize_session(self):
        """Initialize session with cookies"""
        try:
            self.session.get(BASE_URL, timeout=10)
        except Exception:
            pass
    
    def get_gallery_url(self, imo: str, sort_by: str = "newest", page: int = 1) -> str:
        """Construct the gallery URL with all parameters like the working code"""
        return (f"{BASE_URL}/photos/gallery?"
                f"shipName=&shipNameSearchMode=exact&imo={imo}&mmsi=&eni=&callSign="
                f"&category=&user=&country=&location=&viewType=normal"
                f"&sortBy={sort_by}&page={page}")
    
    def extract_photo_ids_from_html(self, html: str) -> Set[str]:
        """Extract photo IDs from HTML using BeautifulSoup like the working code"""
        photo_ids = set()
        soup = BeautifulSoup(html, "lxml")
        
        # Method 1: Find all <a> tags with href containing /photos/DIGITS
        photo_links = soup.find_all("a", href=re.compile(r"/photos/(\d+)"))
        
        for link in photo_links:
            href = link.get("href", "")
            match = re.search(r"/photos/(\d+)", href)
            if match:
                photo_id = match.group(1)
                # Filter out invalid IDs (navigation, etc)
                if photo_id.isdigit() and len(photo_id) >= 4:
                    photo_ids.add(photo_id)
        
        return photo_ids
    
    def get_photo_count(self, html: str) -> int:
        """Extract total photo count from gallery page"""
        patterns = [
            re.compile(r"(\d+)\s+photos?\s+found", re.I),
            re.compile(r"found\s+(\d+)\s+photo", re.I),
        ]
        
        for pattern in patterns:
            match = pattern.search(html)
            if match:
                return int(match.group(1))
        return -1
    
    def search_imo_gallery(self, imo: str) -> Tuple[List[str], int]:
        """Search for photos using multiple strategies like the working code"""
        all_photo_ids = set()
        total_photos = -1
        
        print(f"[Worker {self.worker_id}] 🔍 Searching for IMO {imo}...")
        
        # Strategy 1: Newest photos (most important)
        for page in range(1, 11):  # Check up to 10 pages
            try:
                url = self.get_gallery_url(imo, "newest", page)
                
                with session_lock:
                    time.sleep(STEP_DELAY)
                    response = self.session.get(url, timeout=15)
                
                if not response.ok:
                    break
                
                html = response.text
                
                # Get total count on first page
                if page == 1:
                    total_photos = self.get_photo_count(html)
                    if total_photos == 0:
                        print(f"[Worker {self.worker_id}] No photos found for IMO {imo}")
                        return [], 0
                    elif total_photos > 0:
                        print(f"[Worker {self.worker_id}] 📊 Found {total_photos} total photos")
                
                # Extract photo IDs
                page_photos = self.extract_photo_ids_from_html(html)
                
                if not page_photos:
                    break  # No more photos
                
                new_photos = page_photos - all_photo_ids
                all_photo_ids.update(page_photos)
                
                if len(all_photo_ids) >= MAX_PHOTOS_PER_IMO:
                    break
                    
            except Exception as e:
                print(f"[Worker {self.worker_id}] ⚠️  Error on page {page}: {str(e)[:50]}")
                break
        
        # Strategy 2: Try different sort orders to find more photos
        if len(all_photo_ids) < MAX_PHOTOS_PER_IMO:
            for sort_order in ['oldest', 'popular', 'rating']:
                try:
                    url = self.get_gallery_url(imo, sort_order, 1)
                    
                    with session_lock:
                        time.sleep(STEP_DELAY)
                        response = self.session.get(url, timeout=10)
                    
                    if response.ok:
                        page_photos = self.extract_photo_ids_from_html(response.text)
                        new_photos = page_photos - all_photo_ids
                        if new_photos:
                            all_photo_ids.update(new_photos)
                            
                        if len(all_photo_ids) >= MAX_PHOTOS_PER_IMO:
                            break
                            
                except Exception:
                    continue
        
        # Convert to list and limit
        photo_ids_list = list(all_photo_ids)[:MAX_PHOTOS_PER_IMO]
        
        if photo_ids_list:
            print(f"[Worker {self.worker_id}] 📷 Found {len(photo_ids_list)} photo IDs for IMO {imo}")
        
        return photo_ids_list, total_photos if total_photos > 0 else len(photo_ids_list)
    
    def construct_image_url(self, photo_id: str) -> str:
        """Construct the direct image URL from photo ID"""
        # ShipSpotting pattern: /photos/big/7/5/3/3737357.jpg
        # Takes last 3 digits reversed as path
        pid_str = str(photo_id)
        if len(pid_str) >= 3:
            # Take last 3 digits and reverse for path
            last_three = pid_str[-3:]
            path = '/'.join(reversed(last_three))
            return f"{BASE_URL}/photos/big/{path}/{photo_id}.jpg"
        else:
            # For short IDs
            padded = pid_str.zfill(3)
            path = '/'.join(reversed(padded))
            return f"{BASE_URL}/photos/big/{path}/{photo_id}.jpg"
    
    def download_image(self, photo_id: str, output_dir: Path) -> bool:
        """Download a single image"""
        jpg_path = output_dir / f"{photo_id}.jpg"
        
        # Skip if exists
        if jpg_path.exists():
            return True
        
        # Try multiple URL patterns
        url_patterns = [
            self.construct_image_url(photo_id),
            f"{BASE_URL}/photos/big/{photo_id}.jpg",
            f"{BASE_URL}/photos/large/{photo_id}.jpg",
        ]
        
        for img_url in url_patterns:
            try:
                with session_lock:
                    time.sleep(STEP_DELAY)
                    response = self.session.get(img_url, timeout=20)
                
                if response.ok and len(response.content) > 1000:
                    # Save image
                    img = Image.open(BytesIO(response.content)).convert("RGB")
                    img.save(jpg_path, "JPEG", quality=85)
                    
                    # Save simple metadata
                    json_path = output_dir / f"{photo_id}.json"
                    metadata = {
                        "photo_id": photo_id,
                        "image_url": img_url,
                        "page_url": PHOTO_URL.format(pid=photo_id),
                        "scraped_at": datetime.now().isoformat()
                    }
                    with open(json_path, 'w') as f:
                        json.dump(metadata, f, indent=2)
                    
                    return True
                    
            except Exception:
                continue
        
        return False
    
    def scrape_imo_complete(self, imo: str, vessel_name: str, output_dir: Path) -> Dict:
        """Complete scraping for one IMO"""
        start_time = time.time()
        
        # Create IMO folder
        imo_dir = output_dir / f"IMO_{imo}"
        imo_dir.mkdir(parents=True, exist_ok=True)
        
        # Search for photos
        photo_ids, total_photos = self.search_imo_gallery(imo)
        
        if not photo_ids:
            return {
                'imo': imo,
                'vessel_name': vessel_name,
                'downloaded': 0,
                'found': 0,
                'total_available': total_photos,
                'time_taken': time.time() - start_time
            }
        
        print(f"[Worker {self.worker_id}] 📥 Downloading {len(photo_ids)} photos for {vessel_name[:30]}")
        
        # Download photos
        downloaded = 0
        for i, photo_id in enumerate(photo_ids, 1):
            if self.download_image(photo_id, imo_dir):
                downloaded += 1
                if i % 5 == 0 or i == len(photo_ids):
                    print(f"[Worker {self.worker_id}] Progress: {i}/{len(photo_ids)} photos")
        
        elapsed = time.time() - start_time
        
        if downloaded > 0:
            print(f"[Worker {self.worker_id}] ✅ IMO {imo}: {downloaded}/{len(photo_ids)} photos downloaded")
        
        return {
            'imo': imo,
            'vessel_name': vessel_name,
            'downloaded': downloaded,
            'found': len(photo_ids),
            'total_available': total_photos,
            'time_taken': elapsed
        }

# ====================== Batch Processing Functions ======================
class BatchProcessor:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.stats = {
            'total_vessels': 0,
            'total_photos': 0,
            'failed_vessels': 0
        }
    
    def process_imos(self, imo_list: List[str], vessel_details: Dict[str, Dict]) -> Dict:
        """Process multiple IMOs with parallel workers"""
        if not imo_list:
            return self.stats
        
        print(f"\n🚀 Processing {len(imo_list)} IMOs with {MAX_WORKERS} workers")
        print(f"📸 Fetching up to {MAX_PHOTOS_PER_IMO} photos per vessel")
        self.stats['total_vessels'] = len(imo_list)
        
        # Process in batches
        for batch_start in range(0, len(imo_list), BATCH_SIZE):
            batch = imo_list[batch_start:batch_start + BATCH_SIZE]
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (len(imo_list) + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} IMOs)")
            
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
                # Create scrapers
                scrapers = [ShipSpottingScraper(i) for i in range(min(MAX_WORKERS, len(batch)))]
                
                # Submit tasks
                futures = {}
                for i, imo in enumerate(batch):
                    scraper = scrapers[i % len(scrapers)]
                    vessel_name = vessel_details.get(imo, {}).get('name', 'Unknown')
                    
                    future = executor.submit(
                        scraper.scrape_imo_complete,
                        imo, vessel_name, self.output_dir
                    )
                    futures[future] = (imo, vessel_name)
                
                # Process results
                for future in as_completed(futures):
                    imo, vessel_name = futures[future]
                    try:
                        result = future.result()
                        self.stats['total_photos'] += result['downloaded']
                        
                        if result['downloaded'] > 0:
                            print(f"  ✅ IMO {imo} ({vessel_name[:20]}): {result['downloaded']}/{result['found']} photos")
                        else:
                            print(f"  ⚠️  IMO {imo}: No photos downloaded (found {result['found']})")
                            self.stats['failed_vessels'] += 1
                            
                    except Exception as e:
                        print(f"  ❌ IMO {imo}: Error - {e}")
                        self.stats['failed_vessels'] += 1
            
            # Brief cooldown between batches
            if batch_start + BATCH_SIZE < len(imo_list):
                time.sleep(0.5)
        
        return self.stats

# ====================== Main Scraping Function ======================
def scrape_missing_imos(missing_imos: List[str], vessel_details: Dict[str, Dict], 
                        gallery_dir: Path) -> Dict:
    """Main function to scrape all missing IMOs"""
    if not missing_imos:
        print("✅ No IMOs to scrape - gallery is up to date!")
        return {'total_vessels': 0, 'total_photos': 0}
    
    # Create output directory with today's date
    output_dir = gallery_dir / datetime.now().strftime("%Y-%m-%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    
    # Process all IMOs
    processor = BatchProcessor(output_dir)
    stats = processor.process_imos(missing_imos, vessel_details)
    
    return stats

if __name__ == "__main__":
    # Test the module
    print("Testing ShipSpotting Scraper Module...")
    scraper = ShipSpottingScraper()
    photo_ids, total = scraper.search_imo_gallery("9169031")
    print(f"Found {len(photo_ids)} photo IDs out of {total} total photos")
    if photo_ids:
        print(f"First few IDs: {photo_ids[:5]}")