#!/usr/bin/env python3
"""
imo_extractor.py
Extract IMO numbers from Haifa Bay using Datalastic API and check existing gallery in GCS.
"""

import re
import time
import yaml
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple
from datetime import datetime

import requests

# Import GCS helper
try:
    from .gcs_helper import get_gcs_manager
except ImportError:
    from gcs_helper import get_gcs_manager

# ====================== Logging ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("IMOExtractor")

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
    
    # Fallback to defaults if config not found
    log.warning("Config file not found, using defaults")
    return {
        'api': {'datalastic_key': 'b123dc58-4c18-4b0c-9f04-82a06be63ff9'},
        'port': {
            'latitude': 32.8154,
            'longitude': 35.0043,
            'search_radius_km': 15
        }
    }

CONFIG = load_config()

# Extract configuration values
API_KEY = CONFIG['api']['datalastic_key']
SEARCH_RADIUS = CONFIG['port']['search_radius_km']
PORT_LAT = CONFIG['port']['latitude']
PORT_LON = CONFIG['port']['longitude']



# ====================== Haifa Bay Tracker ======================
class HaifaBayTracker:
    def __init__(self, api_key: str = API_KEY, port_lat: float = PORT_LAT, port_lon: float = PORT_LON):
        self.api_key = api_key
        self.api_base_url = "https://api.datalastic.com/api/v0"
        self.session = requests.Session()
        self.port_lat = port_lat
        self.port_lon = port_lon

    def get_haifa_vessels(self, radius: int = SEARCH_RADIUS) -> List[Dict]:
        """Get all vessels in specified port area using Datalastic API."""
        endpoint = f"{self.api_base_url}/vessel_inradius"
        params = {
            "api-key": self.api_key,
            "lat": self.port_lat,
            "lon": self.port_lon,
            "radius": radius,
        }
        try:
            resp = self.session.get(endpoint, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("meta", {}).get("success", False):
                log.warning("API request was not successful: %s", data.get("meta"))
                return []
            return data.get("data", {}).get("vessels", [])
        except Exception as e:
            log.warning("Error fetching vessels: %s", e)
            return []

    def get_imo_numbers_with_details(self, radius: int = SEARCH_RADIUS) -> Tuple[List[str], Dict[str, Dict]]:
        """Get IMO numbers with vessel details (API mode)."""
        vessels = self.get_haifa_vessels(radius)
        imo_list: List[str] = []
        vessel_details: Dict[str, Dict] = {}

        for vessel in vessels:
            imo = vessel.get("imo")
            if imo and imo.strip() and imo.strip().lower() not in {"null", "n/a", "none", "0"}:
                imo_clean = imo.strip()
                imo_list.append(imo_clean)
                vessel_details[imo_clean] = {
                    "name": vessel.get("name", "Unknown"),
                    "vessel_type": vessel.get("type", "Unknown"),
                    "mmsi": vessel.get("mmsi", ""),
                    "lat": vessel.get("lat", 0),
                    "lon": vessel.get("lon", 0),
                    "destination": vessel.get("destination", ""),
                    "speed": vessel.get("speed", 0),
                    "course": vessel.get("course", 0),
                    "timestamp": vessel.get("last_position_time", ""),
                    "extracted_at": datetime.now().isoformat(),
                }

        unique_imos = sorted(set(imo_list))
        return unique_imos, vessel_details

# ====================== Gallery Checker (GCS) ======================
class GCSGalleryChecker:
    def __init__(self):
        """Initialize GCS gallery checker"""
        self.gcs_manager = get_gcs_manager()
        
    def check_existing_imos(self) -> Set[str]:
        """Check GCS for existing IMOs"""
        existing_imos = self.gcs_manager.check_existing_imos()
        return existing_imos



# ====================== Public API ======================
def extract_haifa_imos() -> Tuple[List[str], Dict[str, Dict]]:
    """
    Extract IMOs from Haifa Bay using Datalastic API.
    Returns (imos, details) tuple.
    """
    log.info("📡 Extracting IMOs from Haifa Bay (%.0fkm radius)...", SEARCH_RADIUS)
    tracker = HaifaBayTracker()
    imo_list, vessel_details = tracker.get_imo_numbers_with_details()
    log.info("✅ Found %d vessels with valid IMO numbers.", len(imo_list))
    return imo_list, vessel_details

def find_missing_imos(haifa_imos: List[str]) -> Tuple[List[str], List[str]]:
    """Find missing IMOs by checking GCS instead of local gallery"""
    log.info("🔍 Checking existing gallery...")
    
    # Use GCS checker instead of local
    checker = GCSGalleryChecker()
    existing_imos = checker.check_existing_imos()
    
    missing_imos = [imo for imo in haifa_imos if imo not in existing_imos]
    existing_in_gallery = [imo for imo in haifa_imos if imo in existing_imos]

    log.info("📊 IMOs found: %d | Missing: %d", len(existing_in_gallery), len(missing_imos))
    return missing_imos, existing_in_gallery



# ====================== Main ======================
if __name__ == "__main__":
    t0 = time.perf_counter()

    # Test GCS connection first
    try:
        gcs = get_gcs_manager()
        if not gcs.test_connection():
            log.error("Failed to connect to Google Cloud Storage")
            exit(1)
    except Exception as e:
        log.error(f"Failed to initialize GCS: {e}")
        exit(1)

    log.info("🚀 Starting IMO extraction from Haifa Bay")

    t1 = time.perf_counter()
    # Extract IMOs
    imos, details = extract_haifa_imos()
    t2 = time.perf_counter()

    # Check GCS for existing IMOs
    missing, existing = find_missing_imos(imos)
    t3 = time.perf_counter()

    # Print summary
    print(f"\n📊 Found {len(imos)} vessels in Haifa Bay")
    print(f"✅ Already in gallery: {len(existing)}")
    print(f"🆕 New to scrape: {len(missing)}")
    print(f"⏱️  Total time: {t3 - t0:.1f}s")

    log.info("✅ Done.")