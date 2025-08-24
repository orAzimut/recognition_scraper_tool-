#!/usr/bin/env python3
"""
shipspotting_scraper.py
Enhanced hybrid module for scraping vessel images from ShipSpotting
Uses cloudscraper for gallery pages and uploads directly to Google Cloud Storage
"""

import re
import time
import json
import yaml
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

# Import GCS helper
try:
    from .gcs_helper import get_gcs_manager
except ImportError:
    from gcs_helper import get_gcs_manager

# ====================== Load Configuration ======================
def load_config():
    """Load configuration from config.yaml"""
    config_paths = [
        Path("resources/config.yaml"),
        Path("../resources/config.yaml"),
        Path("./config.yaml"),
    ]
    
    for path in config_paths:
        if path.exists():
            with open(path, 'r') as f:
                return yaml.safe_load(f)
    
    # Fallback to defaults
    return {
        'scraping': {
            'max_photos_per_imo': 40,
            'max_gallery_pages': 10,
            'batch_size': 10,
            'connect_timeout': 8.0,
            'read_timeout': 12.0,
            'max_retries': 3,
            'retry_backoff_base': 1.0,
            'min_request_delay': 0.05,
            'max_request_delay': 0.12,
            'gallery_workers': 4,
            'image_download_workers': 12,
            'max_concurrent_downloads': 20,
            'stream_chunk_size': 8192
        }
    }

CONFIG = load_config()
SCRAPING_CONFIG = CONFIG['scraping']

# ====================== Configuration ======================
BASE_URL = "https://www.shipspotting.com"
PHOTO_URL = BASE_URL + "/photos/{pid}"

# Extract settings from config
MAX_PHOTOS_PER_IMO = SCRAPING_CONFIG['max_photos_per_imo']
MAX_GALLERY_PAGES = SCRAPING_CONFIG['max_gallery_pages']
BATCH_SIZE = SCRAPING_CONFIG['batch_size']
CONNECT_TIMEOUT = SCRAPING_CONFIG['connect_timeout']
READ_TIMEOUT = SCRAPING_CONFIG['read_timeout']
MAX_RETRIES = SCRAPING_CONFIG['max_retries']
RETRY_BACKOFF_BASE = SCRAPING_CONFIG['retry_backoff_base']
MIN_REQUEST_DELAY = SCRAPING_CONFIG['min_request_delay']
MAX_REQUEST_DELAY = SCRAPING_CONFIG['max_request_delay']
GALLERY_WORKERS = SCRAPING_CONFIG['gallery_workers']
IMAGE_DOWNLOAD_WORKERS = SCRAPING_CONFIG['image_download_workers']
MAX_CONCURRENT_DOWNLOADS = SCRAPING_CONFIG['max_concurrent_downloads']
STREAM_CHUNK_SIZE = SCRAPING_CONFIG['stream_chunk_size']

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Set specific loggers to WARNING to reduce HTTP debug noise
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('cloudscraper').setLevel(logging.WARNING)

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
        self.session = cloudscraper.create_scraper()
        
        # Warm up the session
        try:
            response = self.session.get(BASE_URL, timeout=15)
            response.raise_for_status()
            
            # Test gallery access
            test_url = f"{BASE_URL}/photos/gallery?imo=9169031"
            test_response = self.session.get(test_url, timeout=15)
            
            if test_response.status_code == 403:
                self.session = cloudscraper.create_scraper(browser='chrome')
                test_response = self.session.get(test_url, timeout=15)
                
            logger.info(f"✅ Cloudflare bypass successful")
            
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
                        time.sleep(backoff)
                        continue
                    
                    if response.status_code == 403:  # Cloudflare challenge
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
        """Extract total photo count from gallery page - FIXED VERSION"""
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text()
        
        # These patterns work on ShipSpotting
        patterns = [
            re.compile(r"(\d+)\s+photos?\s+found", re.I),  # "36 photos found"
            re.compile(r"found\s+(\d+)\s+photo", re.I),
            re.compile(r"(\d+)\s+results?\s+found", re.I),
        ]
        
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                count = int(match.group(1))
                return count
        
        return -1
    
    def search_gallery_pages_parallel(self, imo: str, sort_by: str, 
                                    max_pages: int, target_count: int) -> Tuple[Set[str], int]:
        """Search gallery pages using thread pool for parallelism - FIXED VERSION"""
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
        
        # CRITICAL FIX: Always fetch multiple pages if we got a full first page
        if len(page1_ids) >= 12:  # ShipSpotting shows 12 per page
            if total_photos <= 0:
                # Can't detect total, but got full page - assume at least 5 pages worth
                estimated_pages = min(max_pages, 5)
            else:
                # We know the total, calculate pages needed
                photos_per_page = 12  # ShipSpotting standard
                photos_to_fetch = min(target_count, total_photos)
                estimated_pages = min(
                    max_pages,
                    (photos_to_fetch + photos_per_page - 1) // photos_per_page
                )
            
            pages_needed = estimated_pages
        else:
            # Less than 12 photos on first page means that's all there is
            pages_needed = 1
        
        if pages_needed <= 1:
            return all_photo_ids, total_photos if total_photos > 0 else len(all_photo_ids)
        
        # Fetch remaining pages in parallel using threads - FAST!
        def fetch_page(page_num):
            url = self.get_gallery_url(imo, sort_by, page_num)
            response = self.session.get(url)
            if response and response.status_code == 200:
                ids = self.extract_photo_ids(response.text)
                return ids
            return set()
        
        with ThreadPoolExecutor(max_workers=min(GALLERY_WORKERS, pages_needed - 1)) as executor:
            futures = [executor.submit(fetch_page, page) for page in range(2, pages_needed + 1)]
            
            for future in futures:
                page_ids = future.result()
                all_photo_ids.update(page_ids)
                
                # Stop if we have enough
                if len(all_photo_ids) >= target_count:
                    break
        
        # If we still don't have the expected amount and total was detected, log it
        if total_photos > 0 and len(all_photo_ids) < total_photos:
            logger.debug(f"Found {len(all_photo_ids)} photo IDs but page shows {total_photos} total")
        
        return all_photo_ids, total_photos if total_photos > 0 else len(all_photo_ids)
    
    def find_photos(self, imo: str) -> Tuple[List[str], int]:
        """Find all photo IDs for an IMO - MAIN FUNCTION"""
        all_photo_ids = set()
        total_photos = -1
        
        # Primary search: newest photos - this usually gets everything
        photo_ids, total_photos = self.search_gallery_pages_parallel(
            imo, "newest", MAX_GALLERY_PAGES, MAX_PHOTOS_PER_IMO
        )
        all_photo_ids.update(photo_ids)
        
        if total_photos == 0:
            return [], 0
        
        # Check if we got everything we expected
        if total_photos > 0 and len(all_photo_ids) < min(total_photos, MAX_PHOTOS_PER_IMO):
            # We're missing some photos, try other sort orders
            missing_count = min(total_photos, MAX_PHOTOS_PER_IMO) - len(all_photo_ids)
            
            for sort_order in ['oldest', 'popular']:
                # Just fetch a couple pages of each sort to find unique photos
                extra_ids, _ = self.search_gallery_pages_parallel(
                    imo, sort_order, 3, missing_count
                )
                
                new_ids = extra_ids - all_photo_ids
                if new_ids:
                    all_photo_ids.update(new_ids)
                    
                    # Stop if we have enough
                    if len(all_photo_ids) >= min(total_photos, MAX_PHOTOS_PER_IMO):
                        break
        
        # Prepare final list
        photo_list = list(all_photo_ids)[:MAX_PHOTOS_PER_IMO]
        
        # Show only the final result
        logger.info(f"IMO {imo} found {len(photo_list)} images")
        
        return photo_list, total_photos if total_photos > 0 else len(photo_list)

# ====================== GCS Image Uploader ======================
class GCSImageUploader:
    """Download images and upload directly to Google Cloud Storage"""
    
    def __init__(self):
        self.cookies, self.headers = get_scraper_session().get_cookies_and_headers()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.gcs_manager = get_gcs_manager()
    
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
    
    async def download_and_upload_image(self, client: httpx.AsyncClient, 
                                       imo: str, photo_id: str) -> bool:
        """Download image and upload directly to GCS"""
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
                        
                        # Prepare metadata
                        metadata = {
                            "photo_id": photo_id,
                            "image_url": img_url,
                            "page_url": PHOTO_URL.format(pid=photo_id),
                            "scraped_at": datetime.now().isoformat()
                        }
                        
                        # Upload to GCS
                        success = self.gcs_manager.upload_image(
                            imo=imo,
                            photo_id=photo_id,
                            image_data=response.content,
                            metadata=metadata
                        )
                        
                        return success
                        
                except Exception as e:
                    logger.debug(f"Failed to download/upload {img_url}: {e}")
                    continue
            
            return False
    
    async def upload_batch(self, imo: str, photo_ids: List[str]) -> int:
        """Download and upload multiple images for an IMO"""
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40)
        ) as client:
            
            # Create tasks for all downloads/uploads
            tasks = [
                self.download_and_upload_image(client, imo, pid) 
                for pid in photo_ids
            ]
            
            # Execute with progress tracking
            uploaded = 0
            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                result = await coro
                if result:
                    uploaded += 1
                
            return uploaded

# ====================== Main Scraper ======================
class ShipSpottingScraper:
    """Main scraper orchestrator with GCS integration"""
    
    def __init__(self):
        self.finder = PhotoFinder()
        self.uploader = GCSImageUploader()
    
    async def scrape_imo_async(self, imo: str, vessel_name: str) -> ScrapeResult:
        """Scrape one IMO and upload to GCS"""
        start_time = time.time()
        
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
        
        # Download and upload images to GCS
        uploaded = await self.uploader.upload_batch(imo, photo_ids)
        
        elapsed = time.time() - start_time
        
        if uploaded > 0:
            logger.info(f"IMO {imo} downloaded {uploaded} frames")
        else:
            logger.warning(f"IMO {imo}: No images uploaded")
        
        return ScrapeResult(
            imo=imo,
            vessel_name=vessel_name,
            downloaded=uploaded,  # Now represents uploaded count
            found=len(photo_ids),
            total_available=total_photos,
            time_taken=elapsed
        )
    
    def scrape_imo(self, imo: str, vessel_name: str) -> ScrapeResult:
        """Synchronous wrapper for compatibility"""
        return asyncio.run(self.scrape_imo_async(imo, vessel_name))

# ====================== Batch Processor ======================
class BatchProcessor:
    """Process multiple IMOs efficiently with GCS upload"""
    
    def __init__(self):
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
            tasks.append(scraper.scrape_imo_async(imo, vessel_name))
        
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and track progress
        valid_results = []
        for i, result in enumerate(results):
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
        
        self.stats['total_vessels'] = len(imo_list)
        
        # Initialize the global scraper session once
        get_scraper_session()
        
        # Test GCS connection
        try:
            gcs = get_gcs_manager()
            if not gcs.test_connection():
                logger.error("Failed to connect to Google Cloud Storage")
                return self.stats
        except Exception as e:
            logger.error(f"Failed to initialize GCS: {e}")
            return self.stats
        
        all_results = []
        
        # Process in batches
        for batch_start in range(0, len(imo_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(imo_list))
            batch_imos = imo_list[batch_start:batch_end]
            
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
        logger.info("FINAL SUMMARY")
        logger.info("="*60)
        logger.info(f"Total vessels: {self.stats['total_vessels']}")
        logger.info(f"Total images: {self.stats['total_photos']}")
        logger.info(f"Failed: {self.stats['failed_vessels']}")
        logger.info(f"Total time: {self.stats['total_time']:.1f}s")
        
        return self.stats

# ====================== Main Entry Point ======================
def scrape_missing_imos(missing_imos: List[str], vessel_details: Dict[str, Dict], 
                        gallery_dir: Path = None) -> Dict:
    """Main function to scrape all missing IMOs and upload to GCS
    
    Note: gallery_dir parameter is kept for backward compatibility but ignored
    """
    if not missing_imos:
        return {'total_vessels': 0, 'total_photos': 0}
    
    # Process all IMOs
    processor = BatchProcessor()
    stats = processor.process_imos(missing_imos, vessel_details)
    
    return stats

# ====================== Test Functions ======================
if __name__ == "__main__":
    # Test the module
    print("Testing Enhanced ShipSpotting Scraper with GCS...")
    
    test_imos = ["9728239", "9289972"]
    test_details = {
        "9728239": {"name": "Test Vessel 1"},
        "9289972": {"name": "Test Vessel 2"}
    }
    
    stats = scrape_missing_imos(test_imos, test_details)
    print(f"\nTest complete: {stats}")