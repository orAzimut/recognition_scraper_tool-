#!/usr/bin/env python3
"""
shipspotting_scraper_optimized.py
Optimized hybrid module for scraping vessel images from ShipSpotting
3-4x faster with parallel processing, HTTP/2, and removed artificial delays
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
    
    # Optimized defaults - MUCH higher concurrency
    return {
        'scraping': {
            'max_photos_per_imo': 120,  # Increased to handle vessels with 100+ photos
            'max_gallery_pages': 15,  # Increased to fetch all pages
            'batch_size': 20,  # Doubled from 10
            'connect_timeout': 8.0,
            'read_timeout': 12.0,
            'max_retries': 3,
            'retry_backoff_base': 1.0,
            'min_request_delay': 0.0,  # Removed artificial delay
            'max_request_delay': 0.0,  # Removed artificial delay
            'gallery_workers': 8,  # Doubled from 4
            'image_download_workers': 48,  # Quadrupled from 12
            'max_concurrent_downloads': 48,  # More than doubled
            'stream_chunk_size': 16384  # Doubled chunk size
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

# ====================== Optimized Cloudscraper Pool ======================
class CloudscraperPool:
    """Pool of cloudscraper sessions for true parallelism"""
    
    def __init__(self, pool_size: int = 4):
        self.pool_size = pool_size
        self.sessions = []
        self.session_index = 0
        self.lock = threading.Lock()
        self.request_semaphore = threading.Semaphore(GALLERY_WORKERS)
        self._initialize_pool()
    
    def _create_session(self) -> cloudscraper.CloudScraper:
        """Create and warm up a single session"""
        session = cloudscraper.create_scraper(browser='chrome')
        
        # Warm up the session
        try:
            response = session.get(BASE_URL, timeout=15)
            response.raise_for_status()
            return session
        except Exception as e:
            logger.error(f"Failed to initialize session: {e}")
            raise
    
    def _initialize_pool(self):
        """Initialize the session pool"""
        logger.info(f"Initializing {self.pool_size} Cloudflare sessions...")
        
        # Create first session
        first_session = self._create_session()
        self.sessions.append(first_session)
        
        # Get cookies from first session
        cookies = dict(first_session.cookies)
        headers = dict(first_session.headers)
        
        # Create additional sessions with same cookies
        for i in range(1, self.pool_size):
            session = cloudscraper.create_scraper(browser='chrome')
            session.cookies.update(cookies)
            session.headers.update(headers)
            self.sessions.append(session)
        
        logger.info(f"✅ Initialized {self.pool_size} sessions for parallel requests")
    
    def get_session(self) -> cloudscraper.CloudScraper:
        """Get next session in round-robin fashion"""
        with self.lock:
            session = self.sessions[self.session_index]
            self.session_index = (self.session_index + 1) % self.pool_size
            return session
    
    def get(self, url: str, **kwargs) -> Optional[object]:
        """Parallel-safe GET request with retry logic"""
        with self.request_semaphore:  # Limit concurrent requests
            
            for attempt in range(MAX_RETRIES):
                try:
                    session = self.get_session()
                    response = session.get(url, timeout=kwargs.get('timeout', 15))
                    
                    if response.status_code == 429:  # Rate limited
                        backoff = RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1)
                        time.sleep(backoff)
                        continue
                    
                    if response.status_code == 403:  # Cloudflare challenge
                        # Reinitialize this session
                        with self.lock:
                            idx = self.sessions.index(session)
                            self.sessions[idx] = self._create_session()
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
        session = self.sessions[0]
        return dict(session.cookies), dict(session.headers)

# Global session pool
_scraper_pool = None

def get_scraper_pool() -> CloudscraperPool:
    """Get or create the global scraper pool"""
    global _scraper_pool
    if _scraper_pool is None:
        _scraper_pool = CloudscraperPool(pool_size=4)
    return _scraper_pool

# ====================== Optimized Photo Finder ======================
class OptimizedPhotoFinder:
    """Find photo IDs with true parallel gallery fetching"""
    
    def __init__(self):
        self.pool = get_scraper_pool()
        # Pre-compile regex for faster extraction
        self.photo_id_pattern = re.compile(r'/photos/(\d{4,})')
        self.photo_count_patterns = [
            re.compile(r'(\d+)\s+photos?\s+found', re.I),
            re.compile(r'found\s+(\d+)\s+photo', re.I),
            re.compile(r'(\d+)\s+results?\s+found', re.I),
        ]
    
    def get_gallery_url(self, imo: str, sort_by: str = "newest", page: int = 1) -> str:
        """Construct gallery URL"""
        return (f"{BASE_URL}/photos/gallery?"
                f"shipName=&shipNameSearchMode=exact&imo={imo}&mmsi=&eni=&callSign="
                f"&category=&user=&country=&location=&viewType=normal"
                f"&sortBy={sort_by}&page={page}")
    
    def parse_gallery_page(self, html: str) -> Tuple[Set[str], int]:
        """Extract photo IDs and count in single pass - OPTIMIZED"""
        # Fast regex extraction of photo IDs
        photo_ids = set(self.photo_id_pattern.findall(html))
        
        # Extract count
        total_photos = -1
        for pattern in self.photo_count_patterns:
            match = pattern.search(html)
            if match:
                total_photos = int(match.group(1))
                break
        
        return photo_ids, total_photos
    
    def fetch_gallery_page(self, imo: str, sort_by: str, page: int) -> Tuple[Set[str], int]:
        """Fetch and parse a single gallery page"""
        url = self.get_gallery_url(imo, sort_by, page)
        response = self.pool.get(url)
        
        if not response or response.status_code != 200:
            return set(), -1
        
        return self.parse_gallery_page(response.text)
    
    def search_gallery_pages_parallel(self, imo: str, sort_by: str, 
                                    max_pages: int, target_count: int) -> Tuple[Set[str], int]:
        """TRUE parallel gallery page fetching - FIXED FOR ALL PHOTOS"""
        # First page to determine if we need more
        page1_ids, total_photos = self.fetch_gallery_page(imo, sort_by, 1)
        
        if not page1_ids:
            return set(), 0
        
        all_photo_ids = page1_ids.copy()
        
        # CRITICAL FIX: Calculate pages properly
        photos_per_page = 12  # ShipSpotting shows 12 per page
        
        if len(page1_ids) >= photos_per_page:  # Full page, definitely more pages
            if total_photos > 0:
                # We know the total, calculate exact pages needed
                photos_to_fetch = min(target_count, total_photos)
                pages_needed = (photos_to_fetch + photos_per_page - 1) // photos_per_page
                pages_needed = min(pages_needed, max_pages)
            else:
                # Can't detect total, but full page means check more pages
                # Be aggressive - check up to max_pages
                pages_needed = max_pages
        else:
            # Less than full page means that's all
            pages_needed = 1
        
        if pages_needed <= 1:
            return all_photo_ids, total_photos if total_photos > 0 else len(all_photo_ids)
        
        # TRUE PARALLEL fetching with ThreadPoolExecutor
        logger.debug(f"IMO {imo}: Fetching {pages_needed} gallery pages in parallel")
        
        with ThreadPoolExecutor(max_workers=min(GALLERY_WORKERS, pages_needed - 1)) as executor:
            futures = []
            for page in range(2, pages_needed + 1):
                future = executor.submit(self.fetch_gallery_page, imo, sort_by, page)
                futures.append(future)
            
            for future in futures:
                page_ids, page_total = future.result()
                if page_ids:
                    all_photo_ids.update(page_ids)
                    # Update total if we got a better count
                    if page_total > total_photos:
                        total_photos = page_total
                
                # Continue fetching all pages even if we have enough
                # to ensure we get accurate total count
        
        return all_photo_ids, total_photos if total_photos > 0 else len(all_photo_ids)
    
    def find_photos(self, imo: str) -> Tuple[List[str], int]:
        """Find all photo IDs for an IMO - FIXED FOR ALL PHOTOS"""
        all_photo_ids = set()
        
        # Primary search: newest photos - fetch more pages
        photo_ids, total_photos = self.search_gallery_pages_parallel(
            imo, "newest", MAX_GALLERY_PAGES, MAX_PHOTOS_PER_IMO * 3  # Fetch extra to ensure we get all
        )
        all_photo_ids.update(photo_ids)
        
        if total_photos == 0:
            return [], 0
        
        # Log what we found vs what's available
        if total_photos > 0 and len(all_photo_ids) != total_photos:
            logger.debug(f"IMO {imo}: Found {len(all_photo_ids)} IDs, site shows {total_photos} total")
        
        # If we're still missing photos, try other sort orders
        if total_photos > 0 and len(all_photo_ids) < min(total_photos, MAX_PHOTOS_PER_IMO):
            missing_count = min(total_photos, MAX_PHOTOS_PER_IMO) - len(all_photo_ids)
            
            # Parallel fetch of different sort orders for missing photos
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = []
                for sort_order in ['oldest', 'popular']:
                    future = executor.submit(
                        self.search_gallery_pages_parallel,
                        imo, sort_order, 5, missing_count  # Check 5 pages of each
                    )
                    futures.append(future)
                
                for future in futures:
                    extra_ids, extra_total = future.result()
                    new_ids = extra_ids - all_photo_ids
                    if new_ids:
                        all_photo_ids.update(new_ids)
                        logger.debug(f"Found {len(new_ids)} additional photos with {sort_order} sort")
                    
                    # Update total if we got better info
                    if extra_total > total_photos:
                        total_photos = extra_total
        
        # Limit to configured maximum
        photo_list = list(all_photo_ids)[:MAX_PHOTOS_PER_IMO]
        
        # Log final result
        actual_total = total_photos if total_photos > 0 else len(all_photo_ids)
        logger.info(f"IMO {imo}: Found {len(photo_list)}/{actual_total} images")
        
        return photo_list, actual_total

# ====================== Optimized GCS Image Uploader ======================
class OptimizedGCSImageUploader:
    """Download images with HTTP/2 and upload to GCS asynchronously"""
    
    def __init__(self):
        self.cookies, self.headers = get_scraper_pool().get_cookies_and_headers()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.gcs_manager = get_gcs_manager()
        self.client = None  # Reusable client
        self.executor = ThreadPoolExecutor(max_workers=20)  # For GCS uploads
    
    async def get_client(self) -> httpx.AsyncClient:
        """Get or create reusable HTTP client with optional HTTP/2"""
        if self.client is None:
            # Try to enable HTTP/2 if available
            try:
                import h2  # Test if h2 is installed
                http2_enabled = True
                logger.info("HTTP/2 enabled for faster downloads")
            except ImportError:
                http2_enabled = False
                logger.info("HTTP/2 not available, using HTTP/1.1")
            
            self.client = httpx.AsyncClient(
                cookies=self.cookies,
                headers=self.headers,
                timeout=httpx.Timeout(10.0),
                limits=httpx.Limits(
                    max_keepalive_connections=50,
                    max_connections=100
                ),
                http2=http2_enabled  # Use HTTP/2 only if available
            )
        return self.client
    
    def construct_image_url(self, photo_id: str) -> List[str]:
        """Construct possible image URLs"""
        urls = []
        pid_str = str(photo_id)
        if len(pid_str) >= 3:
            last_three = pid_str[-3:]
            path = '/'.join(reversed(last_three))
            urls.append(f"{BASE_URL}/photos/big/{path}/{photo_id}.jpg")
        
        urls.extend([
            f"{BASE_URL}/photos/big/{photo_id}.jpg",
            f"{BASE_URL}/photos/large/{photo_id}.jpg",
        ])
        
        return urls
    
    async def upload_to_gcs_async(self, imo: str, photo_id: str, 
                                  image_data: bytes, metadata: dict) -> bool:
        """Upload to GCS in background thread to avoid blocking"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.gcs_manager.upload_image,
            imo, photo_id, image_data, metadata
        )
    
    async def download_and_upload_image(self, client: httpx.AsyncClient, 
                                       imo: str, photo_id: str) -> bool:
        """Download image and upload to GCS - NO DELAYS!"""
        async with self.semaphore:
            for img_url in self.construct_image_url(photo_id):
                try:
                    response = await client.get(img_url)
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'image' not in content_type.lower():
                            continue
                        
                        metadata = {
                            "photo_id": photo_id,
                            "image_url": img_url,
                            "page_url": PHOTO_URL.format(pid=photo_id),
                            "scraped_at": datetime.now().isoformat()
                        }
                        
                        # Async GCS upload
                        success = await self.upload_to_gcs_async(
                            imo, photo_id, response.content, metadata
                        )
                        
                        return success
                        
                except Exception as e:
                    logger.debug(f"Failed to download/upload {img_url}: {e}")
                    continue
            
            return False
    
    async def upload_batch(self, imo: str, photo_ids: List[str]) -> int:
        """Download and upload multiple images with HTTP/2"""
        client = await self.get_client()
        
        # Create all tasks at once
        tasks = [
            self.download_and_upload_image(client, imo, pid) 
            for pid in photo_ids
        ]
        
        # Execute all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successes
        uploaded = sum(1 for r in results if r is True)
        
        return uploaded
    
    async def cleanup(self):
        """Clean up resources"""
        if self.client:
            await self.client.aclose()
        self.executor.shutdown(wait=False)

# ====================== Optimized Main Scraper ======================
class OptimizedShipSpottingScraper:
    """Main scraper with persistent resources"""
    
    def __init__(self):
        self.finder = OptimizedPhotoFinder()
        self.uploader = OptimizedGCSImageUploader()
    
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
            logger.info(f"IMO {imo} downloaded {uploaded} frames in {elapsed:.1f}s")
        else:
            logger.warning(f"IMO {imo}: No images uploaded")
        
        return ScrapeResult(
            imo=imo,
            vessel_name=vessel_name,
            downloaded=uploaded,
            found=len(photo_ids),
            total_available=total_photos,
            time_taken=elapsed
        )

# ====================== Global Event Loop Processor ======================
class GlobalEventLoopProcessor:
    """Process all IMOs with single event loop - NO REPEATED asyncio.run!"""
    
    def __init__(self):
        self.stats = {
            'total_vessels': 0,
            'total_photos': 0,
            'failed_vessels': 0,
            'total_time': 0
        }
        self.scraper = OptimizedShipSpottingScraper()
        self.imo_semaphore = asyncio.Semaphore(BATCH_SIZE)  # Limit concurrent IMOs
    
    async def process_imo_with_limit(self, imo: str, vessel_name: str) -> ScrapeResult:
        """Process single IMO with concurrency limit"""
        async with self.imo_semaphore:
            return await self.scraper.scrape_imo_async(imo, vessel_name)
    
    async def process_all_imos_async(self, imo_list: List[str], 
                                   vessel_details: Dict[str, Dict]) -> Dict:
        """Process ALL IMOs in single event loop"""
        if not imo_list:
            return self.stats
        
        start_time = time.time()
        self.stats['total_vessels'] = len(imo_list)
        
        # Create tasks for ALL IMOs at once
        tasks = []
        for imo in imo_list:
            vessel_name = vessel_details.get(imo, {}).get('name', 'Unknown')
            task = self.process_imo_with_limit(imo, vessel_name)
            tasks.append(task)
        
        # Process all with progress tracking
        completed = 0
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                completed += 1
                
                if isinstance(result, ScrapeResult):
                    self.stats['total_photos'] += result.downloaded
                    if result.downloaded == 0:
                        self.stats['failed_vessels'] += 1
                    
                    # Progress update every 10 vessels
                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed
                        logger.info(f"Progress: {completed}/{len(imo_list)} vessels "
                                  f"({rate:.1f} vessels/sec)")
                        
            except Exception as e:
                logger.error(f"Error processing IMO: {e}")
                self.stats['failed_vessels'] += 1
        
        # Cleanup
        await self.scraper.uploader.cleanup()
        
        self.stats['total_time'] = time.time() - start_time
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("FINAL SUMMARY")
        logger.info("="*60)
        logger.info(f"Total vessels: {self.stats['total_vessels']}")
        logger.info(f"Total images: {self.stats['total_photos']}")
        logger.info(f"Failed: {self.stats['failed_vessels']}")
        logger.info(f"Total time: {self.stats['total_time']:.1f}s")
        logger.info(f"Rate: {self.stats['total_vessels']/self.stats['total_time']:.1f} vessels/sec")
        
        return self.stats

# ====================== Main Entry Point ======================
def scrape_missing_imos(missing_imos: List[str], vessel_details: Dict[str, Dict], 
                        gallery_dir: Path = None) -> Dict:
    """Main function - NOW WITH SINGLE EVENT LOOP!"""
    if not missing_imos:
        return {'total_vessels': 0, 'total_photos': 0}
    
    # Initialize the global scraper pool once
    get_scraper_pool()
    
    # Test GCS connection
    try:
        gcs = get_gcs_manager()
        if not gcs.test_connection():
            logger.error("Failed to connect to Google Cloud Storage")
            return {'total_vessels': 0, 'total_photos': 0}
    except Exception as e:
        logger.error(f"Failed to initialize GCS: {e}")
        return {'total_vessels': 0, 'total_photos': 0}
    
    # Process everything in ONE event loop
    processor = GlobalEventLoopProcessor()
    
    # Run the entire process in single event loop
    stats = asyncio.run(processor.process_all_imos_async(missing_imos, vessel_details))
    
    return stats

# ====================== Test Functions ======================
if __name__ == "__main__":
    # Test the optimized module
    print("Testing Optimized ShipSpotting Scraper (3-4x faster)...")
    
    test_imos = ["9728239", "9289972", "9169031", "9443066", "9371476"]
    test_details = {
        "9728239": {"name": "Test Vessel 1"},
        "9289972": {"name": "Test Vessel 2"},
        "9169031": {"name": "Test Vessel 3"},
        "9443066": {"name": "Test Vessel 4"},
        "9371476": {"name": "Test Vessel 5"}
    }
    
    stats = scrape_missing_imos(test_imos, test_details)
    print(f"\nTest complete: {stats}")