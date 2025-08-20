#!/usr/bin/env python3
"""
shipspotting_scraper.py
Enhanced hybrid module for scraping vessel images from ShipSpotting
Uses cloudscraper for gallery pages and async for image downloads
"""

import re
import time
import json
import asyncio
import random
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging

import httpx
import cloudscraper
from bs4 import BeautifulSoup

# ====================== Configuration ======================
BASE_URL = "https://www.shipspotting.com"
PHOTO_URL = BASE_URL + "/photos/{pid}"

# Performance settings - optimized for speed
MAX_PHOTOS_PER_IMO = 40  # Reduced from 50 for faster completion
MAX_GALLERY_PAGES = 10  # Max pages to check per sort
BATCH_SIZE = 10  # IMOs to process simultaneously

# Network settings
CONNECT_TIMEOUT = 8.0
READ_TIMEOUT = 12.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # Exponential backoff base
MIN_REQUEST_DELAY = 0.05  # 50ms minimum
MAX_REQUEST_DELAY = 0.12  # 120ms maximum

# Concurrency settings
GALLERY_WORKERS = 4  # Concurrent gallery searchers
IMAGE_DOWNLOAD_WORKERS = 12  # Concurrent image downloads
MAX_CONCURRENT_DOWNLOADS = 20  # Global limit on concurrent downloads

# Image download settings
STREAM_CHUNK_SIZE = 8192  # 8KB chunks for streaming

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ====================== Data Classes ======================
@dataclass
class ScrapeResult:
    """Result from scraping one IMO"""
    imo: str
    vessel_name: str
    downloaded: int
    found: int
    total_available: int
    time_taken: float
    errors: List[str] = None

# ====================== Shared Cloudscraper Session ======================
class CloudscraperSession:
    """Thread-safe cloudscraper session manager"""
    
    def __init__(self):
        self.session = None
        self.lock = threading.Lock()
        self.request_semaphore = threading.Semaphore(GALLERY_WORKERS)
        self._initialize()
    
    def _initialize(self):
        """Initialize the cloudscraper session"""
        logger.info("Initializing Cloudflare bypass...")
        self.session = cloudscraper.create_scraper()
        
        # Warm up the session
        try:
            response = self.session.get(BASE_URL, timeout=15)
            response.raise_for_status()
            
            # Test gallery access
            test_url = f"{BASE_URL}/photos/gallery?imo=9169031"
            test_response = self.session.get(test_url, timeout=15)
            
            if test_response.status_code == 403:
                logger.warning("Initial 403 - retrying with new session")
                self.session = cloudscraper.create_scraper(browser='chrome')
                test_response = self.session.get(test_url, timeout=15)
                
            logger.info(f"✅ Cloudflare bypass successful (status: {test_response.status_code})")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cloudflare bypass: {e}")
            raise
    
    def get(self, url: str, **kwargs) -> Optional[object]:
        """Thread-safe GET request with retry logic"""
        with self.request_semaphore:  # Limit concurrent requests
            # Add random delay
            time.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))
            
            for attempt in range(MAX_RETRIES):
                try:
                    with self.lock:  # Thread-safe access to session
                        response = self.session.get(url, timeout=kwargs.get('timeout', 15))
                    
                    if response.status_code == 429:  # Rate limited
                        backoff = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(f"Rate limited, backing off {backoff:.1f}s")
                        time.sleep(backoff)
                        continue
                    
                    if response.status_code == 403:  # Cloudflare challenge
                        logger.warning(f"Got 403 on attempt {attempt + 1}, reinitializing session")
                        with self.lock:
                            self._initialize()
                        continue
                    
                    if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                        backoff = RETRY_BACKOFF_BASE * (2 ** attempt)
                        time.sleep(backoff)
                        continue
                    
                    return response
                    
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        backoff = RETRY_BACKOFF_BASE * (2 ** attempt)
                        logger.debug(f"Request failed: {e}, retrying in {backoff:.1f}s")
                        time.sleep(backoff)
                    else:
                        logger.error(f"Failed after {MAX_RETRIES} attempts: {url}")
                        return None
            
            return None
    
    def get_cookies_and_headers(self) -> Tuple[Dict, Dict]:
        """Get cookies and headers for other clients"""
        with self.lock:
            return dict(self.session.cookies), dict(self.session.headers)

# Global session instance
_scraper_session = None

def get_scraper_session() -> CloudscraperSession:
    """Get or create the global scraper session"""
    global _scraper_session
    if _scraper_session is None:
        _scraper_session = CloudscraperSession()
    return _scraper_session

# ====================== Photo Finder ======================
class PhotoFinder:
    """Find photo IDs for vessels using cloudscraper"""
    
    def __init__(self):
        self.session = get_scraper_session()
    
    def get_gallery_url(self, imo: str, sort_by: str = "newest", page: int = 1) -> str:
        """Construct gallery URL"""
        return (f"{BASE_URL}/photos/gallery?"
                f"shipName=&shipNameSearchMode=exact&imo={imo}&mmsi=&eni=&callSign="
                f"&category=&user=&country=&location=&viewType=normal"
                f"&sortBy={sort_by}&page={page}")
    
    def extract_photo_ids(self, html: str) -> Set[str]:
        """Extract photo IDs from HTML"""
        photo_ids = set()
        soup = BeautifulSoup(html, "lxml")
        
        photo_links = soup.find_all("a", href=re.compile(r"/photos/(\d+)"))
        
        for link in photo_links:
            href = link.get("href", "")
            match = re.search(r"/photos/(\d+)", href)
            if match:
                photo_id = match.group(1)
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
    
    def search_gallery_pages_parallel(self, imo: str, sort_by: str, 
                                    max_pages: int, target_count: int) -> Tuple[Set[str], int]:
        """Search gallery pages using thread pool for parallelism"""
        all_photo_ids = set()
        total_photos = -1
        
        # First page to get total count
        url = self.get_gallery_url(imo, sort_by, 1)
        response = self.session.get(url)
        
        if not response or response.status_code != 200:
            return set(), -1
        
        html = response.text
        total_photos = self.get_photo_count(html)
        page1_ids = self.extract_photo_ids(html)
        all_photo_ids.update(page1_ids)
        
        if total_photos <= 0 or not page1_ids:
            return all_photo_ids, total_photos
        
        # Calculate how many pages we need
        photos_per_page = len(page1_ids)
        if photos_per_page == 0:
            return all_photo_ids, total_photos
        
        pages_needed = min(
            max_pages,
            (min(target_count, total_photos) + photos_per_page - 1) // photos_per_page
        )
        
        if pages_needed <= 1:
            return all_photo_ids, total_photos
        
        # Fetch remaining pages in parallel using threads
        def fetch_page(page_num):
            url = self.get_gallery_url(imo, sort_by, page_num)
            response = self.session.get(url)
            if response and response.status_code == 200:
                return self.extract_photo_ids(response.text)
            return set()
        
        with ThreadPoolExecutor(max_workers=min(4, pages_needed - 1)) as executor:
            futures = [executor.submit(fetch_page, page) for page in range(2, pages_needed + 1)]
            
            for future in futures:
                page_ids = future.result()
                all_photo_ids.update(page_ids)
                
                if len(all_photo_ids) >= target_count:
                    break
        
        return all_photo_ids, total_photos
    
    def find_photos(self, imo: str) -> Tuple[List[str], int]:
        """Find all photo IDs for an IMO"""
        all_photo_ids = set()
        total_photos = -1
        
        logger.info(f"🔍 Searching for IMO {imo}...")
        
        # Primary search: newest photos
        photo_ids, total_photos = self.search_gallery_pages_parallel(
            imo, "newest", MAX_GALLERY_PAGES, MAX_PHOTOS_PER_IMO
        )
        all_photo_ids.update(photo_ids)
        
        if total_photos == 0:
            logger.info(f"No photos found for IMO {imo}")
            return [], 0
        
        if total_photos > 0:
            logger.info(f"📊 Found {total_photos} total photos for IMO {imo}")
        
        # Stop early if we have enough
        if len(all_photo_ids) >= min(MAX_PHOTOS_PER_IMO, total_photos):
            photo_list = list(all_photo_ids)[:MAX_PHOTOS_PER_IMO]
            logger.info(f"📷 Collected {len(photo_list)} photo IDs for IMO {imo}")
            return photo_list, total_photos
        
        # Try other sort orders only if needed
        remaining_needed = min(MAX_PHOTOS_PER_IMO, total_photos) - len(all_photo_ids)
        
        if remaining_needed > 0:
            for sort_order in ['oldest', 'popular']:
                photo_ids, _ = self.search_gallery_pages_parallel(
                    imo, sort_order, 2, remaining_needed
                )
                
                new_ids = photo_ids - all_photo_ids
                if new_ids:
                    all_photo_ids.update(new_ids)
                    
                    if len(all_photo_ids) >= min(MAX_PHOTOS_PER_IMO, total_photos):
                        break
        
        photo_list = list(all_photo_ids)[:MAX_PHOTOS_PER_IMO]
        logger.info(f"📷 Collected {len(photo_list)} photo IDs for IMO {imo}")
        
        return photo_list, total_photos

# ====================== Async Image Downloader ======================
class AsyncImageDownloader:
    """Download images using async httpx with cloudscraper credentials"""
    
    def __init__(self):
        self.cookies, self.headers = get_scraper_session().get_cookies_and_headers()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    def construct_image_url(self, photo_id: str) -> List[str]:
        """Construct possible image URLs"""
        urls = []
        
        # Primary pattern
        pid_str = str(photo_id)
        if len(pid_str) >= 3:
            last_three = pid_str[-3:]
            path = '/'.join(reversed(last_three))
            urls.append(f"{BASE_URL}/photos/big/{path}/{photo_id}.jpg")
        
        # Fallback patterns
        urls.extend([
            f"{BASE_URL}/photos/big/{photo_id}.jpg",
            f"{BASE_URL}/photos/large/{photo_id}.jpg",
        ])
        
        return urls
    
    async def download_image_async(self, client: httpx.AsyncClient, photo_id: str, 
                                  output_dir: Path) -> bool:
        """Download a single image asynchronously"""
        jpg_path = output_dir / f"{photo_id}.jpg"
        
        if jpg_path.exists():
            return True
        
        async with self.semaphore:
            # Add small random delay
            await asyncio.sleep(random.uniform(0.01, 0.05))
            
            for img_url in self.construct_image_url(photo_id):
                try:
                    response = await client.get(img_url, timeout=10)
                    
                    if response.status_code == 200:
                        # Check content type
                        content_type = response.headers.get('content-type', '')
                        if 'image' not in content_type.lower():
                            continue
                        
                        # Save image
                        jpg_path.write_bytes(response.content)
                        
                        # Save metadata
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
                        
                except Exception as e:
                    logger.debug(f"Failed to download {img_url}: {e}")
                    continue
            
            return False
    
    async def download_batch(self, photo_ids: List[str], output_dir: Path) -> int:
        """Download multiple images concurrently"""
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40)
        ) as client:
            
            # Create tasks for all downloads
            tasks = [
                self.download_image_async(client, pid, output_dir) 
                for pid in photo_ids
            ]
            
            # Execute with progress tracking
            downloaded = 0
            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                result = await coro
                if result:
                    downloaded += 1
                
                # Progress update
                if i % 10 == 0 or i == len(tasks):
                    logger.info(f"  Progress: {i}/{len(tasks)} images processed, {downloaded} downloaded")
            
            return downloaded

# ====================== Main Scraper ======================
class ShipSpottingScraper:
    """Main scraper orchestrator"""
    
    def __init__(self):
        self.finder = PhotoFinder()
        self.downloader = AsyncImageDownloader()
    
    async def scrape_imo_async(self, imo: str, vessel_name: str, output_dir: Path) -> ScrapeResult:
        """Scrape one IMO with async image downloads"""
        start_time = time.time()
        
        # Create IMO folder
        imo_dir = output_dir / f"IMO_{imo}"
        imo_dir.mkdir(parents=True, exist_ok=True)
        
        # Find photos (using threads)
        photo_ids, total_photos = self.finder.find_photos(imo)
        
        if not photo_ids:
            return ScrapeResult(
                imo=imo,
                vessel_name=vessel_name,
                downloaded=0,
                found=0,
                total_available=total_photos,
                time_taken=time.time() - start_time
            )
        
        logger.info(f"📥 Downloading {len(photo_ids)} images for {vessel_name[:30]}...")
        
        # Download images (using async)
        downloaded = await self.downloader.download_batch(photo_ids, imo_dir)
        
        elapsed = time.time() - start_time
        
        if downloaded > 0:
            logger.info(f"✅ IMO {imo}: {downloaded}/{len(photo_ids)} images in {elapsed:.1f}s")
        else:
            logger.warning(f"⚠️ IMO {imo}: No images downloaded")
        
        return ScrapeResult(
            imo=imo,
            vessel_name=vessel_name,
            downloaded=downloaded,
            found=len(photo_ids),
            total_available=total_photos,
            time_taken=elapsed
        )
    
    def scrape_imo(self, imo: str, vessel_name: str, output_dir: Path) -> ScrapeResult:
        """Synchronous wrapper for compatibility"""
        return asyncio.run(self.scrape_imo_async(imo, vessel_name, output_dir))

# ====================== Batch Processor ======================
class BatchProcessor:
    """Process multiple IMOs efficiently"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.stats = {
            'total_vessels': 0,
            'total_photos': 0,
            'failed_vessels': 0,
            'total_time': 0
        }
    
    async def process_batch_async(self, batch: List[Tuple[str, str]]) -> List[ScrapeResult]:
        """Process a batch of IMOs concurrently"""
        scraper = ShipSpottingScraper()
        
        # Create tasks for each IMO
        tasks = []
        for imo, vessel_name in batch:
            tasks.append(scraper.scrape_imo_async(imo, vessel_name, self.output_dir))
        
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error processing IMO: {result}")
                self.stats['failed_vessels'] += 1
            elif isinstance(result, ScrapeResult):
                valid_results.append(result)
                self.stats['total_photos'] += result.downloaded
                if result.downloaded == 0:
                    self.stats['failed_vessels'] += 1
        
        return valid_results
    
    def process_imos(self, imo_list: List[str], vessel_details: Dict[str, Dict]) -> Dict:
        """Process all IMOs in batches"""
        if not imo_list:
            return self.stats
        
        start_time = time.time()
        
        logger.info(f"\n🚀 Processing {len(imo_list)} IMOs with hybrid approach")
        logger.info(f"📸 Fetching up to {MAX_PHOTOS_PER_IMO} photos per vessel")
        logger.info(f"⚡ Gallery workers: {GALLERY_WORKERS}, Image downloads: {IMAGE_DOWNLOAD_WORKERS}")
        
        self.stats['total_vessels'] = len(imo_list)
        
        # Initialize the global scraper session once
        get_scraper_session()
        
        all_results = []
        
        # Process in batches
        for batch_start in range(0, len(imo_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(imo_list))
            batch_imos = imo_list[batch_start:batch_end]
            
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (len(imo_list) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch_imos)} IMOs)")
            
            # Prepare batch data
            batch_data = [
                (imo, vessel_details.get(imo, {}).get('name', 'Unknown'))
                for imo in batch_imos
            ]
            
            # Process batch asynchronously
            batch_results = asyncio.run(self.process_batch_async(batch_data))
            all_results.extend(batch_results)
        
        # Calculate total time
        self.stats['total_time'] = time.time() - start_time
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("📊 SCRAPING COMPLETE - SUMMARY")
        logger.info("="*60)
        logger.info(f"Total vessels processed: {self.stats['total_vessels']}")
        logger.info(f"Total images downloaded: {self.stats['total_photos']}")
        logger.info(f"Failed vessels: {self.stats['failed_vessels']}")
        logger.info(f"⏱️  Total time: {self.stats['total_time']:.1f}s")
        
        if all_results:
            avg_time = sum(r.time_taken for r in all_results) / len(all_results)
            logger.info(f"⚡ Average time per vessel: {avg_time:.1f}s")
            
            if self.stats['total_photos'] > 0:
                imgs_per_sec = self.stats['total_photos'] / self.stats['total_time']
                logger.info(f"🚀 Download rate: {imgs_per_sec:.1f} images/second")
        
        return self.stats

# ====================== Main Entry Point ======================
def scrape_missing_imos(missing_imos: List[str], vessel_details: Dict[str, Dict], 
                        gallery_dir: Path) -> Dict:
    """Main function to scrape all missing IMOs"""
    if not missing_imos:
        logger.info("✅ No IMOs to scrape - gallery is up to date!")
        return {'total_vessels': 0, 'total_photos': 0}
    
    # Create output directory
    output_dir = gallery_dir / datetime.now().strftime("%Y-%m-%d")
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"📁 Output directory: {output_dir}")
    
    # Process all IMOs
    processor = BatchProcessor(output_dir)
    stats = processor.process_imos(missing_imos, vessel_details)
    
    return stats

# ====================== Test Functions ======================
if __name__ == "__main__":
    # Test the module
    print("Testing Enhanced ShipSpotting Scraper...")
    
    test_imos = ["9169031", "9289972"]
    test_details = {
        "9169031": {"name": "Test Vessel 1"},
        "9289972": {"name": "Test Vessel 2"}
    }
    
    stats = scrape_missing_imos(test_imos, test_details, Path("./test_output"))
    print(f"\nTest complete: {stats}")