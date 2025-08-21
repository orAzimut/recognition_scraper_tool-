#!/usr/bin/env python3
"""
imo_extractor.py
Extract IMO numbers (API or hardcoded) and check existing gallery in GCS.

Modes:
  - API mode:       Live Datalastic calls (uses credits)
  - Hardcoded mode: 5 fixed IMOs (no API calls)  [DEFAULT in interactive prompt]

Mode resolution priority: CLI (--mode) > ENV (SCRAPER_MODE) > Interactive prompt
This file is backward-compatible with older code that calls extract_haifa_imos()
with no arguments.
"""

import os
import re
import time
import json
import yaml
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
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

# 5 sample IMOs for hardcoded testing (valid 7-digit format; arbitrary values)
HARD_CODED_IMOS: List[str] = [
    "9387421",
    "9734567",
    "9123456",
    "9701234",
    "9876543",
]
HARD_CODED_DETAILS: Dict[str, Dict] = {
    "9387421": {"name": "TEST VESSEL A", "vessel_type": "Container", "mmsi": "", "lat": 0, "lon": 0, "destination": "", "speed": 0, "course": 0, "timestamp": "", "extracted_at": ""},
    "9734567": {"name": "TEST VESSEL B", "vessel_type": "Tanker",    "mmsi": "", "lat": 0, "lon": 0, "destination": "", "speed": 0, "course": 0, "timestamp": "", "extracted_at": ""},
    "9123456": {"name": "TEST VESSEL C", "vessel_type": "Bulk",      "mmsi": "", "lat": 0, "lon": 0, "destination": "", "speed": 0, "course": 0, "timestamp": "", "extracted_at": ""},
    "9701234": {"name": "TEST VESSEL D", "vessel_type": "General",   "mmsi": "", "lat": 0, "lon": 0, "destination": "", "speed": 0, "course": 0, "timestamp": "", "extracted_at": ""},
    "9876543": {"name": "TEST VESSEL E", "vessel_type": "Ro-Ro",     "mmsi": "", "lat": 0, "lon": 0, "destination": "", "speed": 0, "course": 0, "timestamp": "", "extracted_at": ""},
}

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
        log.info("🔍 Checking existing IMOs in Google Cloud Storage...")
        existing_imos = self.gcs_manager.check_existing_imos()
        log.info(f"📂 Found {len(existing_imos)} IMOs in GCS gallery")
        return existing_imos

# ====================== Mode Selection ======================
def choose_mode_interactively(default: str = "hardcoded") -> str:
    log.info("Please choose data source mode:")
    print("1 = API mode (live Datalastic calls)  [uses credits]")
    print("2 = Hardcoded mode (test; no API)     [DEFAULT]")
    try:
        choice = input("Enter choice [1/2, default=2]: ").strip()
    except EOFError:
        # Non-interactive environment (e.g., piped/cron) → default safely
        choice = ""
    if choice == "1":
        return "api"
    if choice in {"2", ""}:
        return "hardcoded"
    log.warning("Unknown input '%s'. Using default: %s", choice, default)
    return default

def resolve_mode(cli_mode: Optional[str]) -> str:
    if cli_mode:
        mode = cli_mode.strip().lower()
        log.info("📝 Mode (CLI): %s", mode)
        return mode
    env_mode = os.getenv("SCRAPER_MODE")
    if env_mode:
        mode = env_mode.strip().lower()
        log.info("📝 Mode (ENV SCRAPER_MODE): %s", mode)
        return mode
    mode = choose_mode_interactively(default="hardcoded")
    log.info("📝 Mode (Interactive): %s", mode)
    return mode

# ====================== Public API (BACKWARD-COMPATIBLE) ======================
def extract_haifa_imos(mode: Optional[str] = None) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Returns (imos, details) depending on mode.
    - 'hardcoded': no network calls, 5 fixed IMOs
    - 'api': live Datalastic fetch around Haifa
    If mode is None, we resolve via CLI/ENV/interactive prompt.
    (Backward-compatible with older code that called extract_haifa_imos() with no args.)
    """
    if mode is None:
        mode = resolve_mode(cli_mode=None)

    if mode not in {"api", "hardcoded"}:
        log.warning("Unknown mode '%s'. Falling back to 'hardcoded' to save credits.", mode)
        mode = "hardcoded"

    if mode == "hardcoded":
        log.info("🧪 Using HARDCODED mode (no API calls).")
        now = datetime.now().isoformat()
        details = {imo: {**HARD_CODED_DETAILS.get(imo, {}), "extracted_at": now} for imo in HARD_CODED_IMOS}
        return HARD_CODED_IMOS.copy(), details

    log.info("📡 Using API mode: Extracting IMOs from Haifa Bay (%.0fkm radius)...", SEARCH_RADIUS)
    tracker = HaifaBayTracker()
    imo_list, vessel_details = tracker.get_imo_numbers_with_details()
    log.info("✅ Found %d vessels with valid IMO numbers (API).", len(imo_list))
    return imo_list, vessel_details

def find_missing_imos(haifa_imos: List[str]) -> Tuple[List[str], List[str]]:
    """Find missing IMOs by checking GCS instead of local gallery"""
    log.info("🔍 Checking existing gallery in Google Cloud Storage...")
    
    # Use GCS checker instead of local
    checker = GCSGalleryChecker()
    existing_imos = checker.check_existing_imos()
    
    missing_imos = [imo for imo in haifa_imos if imo not in existing_imos]
    existing_in_gallery = [imo for imo in haifa_imos if imo in existing_imos]

    log.info("🆕 %d new IMOs to scrape | ✅ %d already in gallery.", len(missing_imos), len(existing_in_gallery))
    return missing_imos, existing_in_gallery

# For backward compatibility - keep the old signature but ignore gallery_dir
def find_missing_imos_legacy(gallery_dir: Path, haifa_imos: List[str]) -> Tuple[List[str], List[str]]:
    """Legacy function signature for backward compatibility"""
    log.info("Note: Local gallery_dir parameter is ignored - using GCS instead")
    return find_missing_imos(haifa_imos)

# ====================== Summary Printer ======================
def print_final_summary(
    mode: str,
    total: int,
    already: int,
    new: int,
    total_time_s: float,
    extract_time_s: float,
    gallery_time_s: float,
    radius_km: int = SEARCH_RADIUS,
):
    print("\n======================================================================")
    print("                            FINAL SUMMARY")
    print("======================================================================\n")
    print("📊 Statistics:")
    print(f"  • Run Date: {datetime.now().date()}")
    if mode == "api":
        print(f"  • Mode: API (live)")
        print(f"  • Location: {CONFIG['port']['name']} ({radius_km}km radius)")
        print(f"  • Total vessels in area: {total}")
    else:
        print(f"  • Mode: HARDCODED (no API)")
        print("  • Source: Hardcoded test list")
        print(f"  • Total IMOs in test list: {total}")
    print(f"  • Already in gallery: {already}")
    print(f"  • New vessels to scrape: {new}\n")
    print("⏱️  Timing:")
    print(f"  • Total execution time: {total_time_s:.1f} seconds")
    print(f"  • IMO extraction: {extract_time_s:.1f} seconds")
    print(f"  • Gallery check: {gallery_time_s:.1f} seconds")
    print("\n📂 Storage: Google Cloud Storage")
    print(f"  • Bucket: {CONFIG['gcs']['bucket_name']}")
    print(f"  • Check path: {CONFIG['gcs']['paths']['check_base']}")

# ====================== CLI ======================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract IMOs via API or hardcoded list; check GCS gallery.")
    p.add_argument("--mode", choices=["api", "hardcoded"], default=None,
                   help="Data source mode (CLI overrides ENV; if omitted, you'll be prompted).")
    p.add_argument("--gallery-dir", type=Path, default=Path.cwd(),
                   help="DEPRECATED - Now using GCS. This parameter is ignored.")
    return p.parse_args()

# ====================== Main ======================
if __name__ == "__main__":
    args = parse_args()

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

    # Resolve mode with CLI/ENV/interactive (shown in logs)
    mode = resolve_mode(args.mode)
    log.info("🚀 Starting run (mode: %s)", mode.upper())

    t1 = time.perf_counter()
    # Extract IMOs
    imos, details = extract_haifa_imos(mode)
    t2 = time.perf_counter()

    # Check GCS for existing IMOs
    missing, existing = find_missing_imos(imos)
    t3 = time.perf_counter()

    # Final printed summary (mode-aware)
    print_final_summary(
        mode=mode,
        total=len(imos),
        already=len(existing),
        new=len(missing),
        total_time_s=(t3 - t0),
        extract_time_s=(t2 - t1),
        gallery_time_s=(t3 - t2),
        radius_km=SEARCH_RADIUS,
    )

    log.info("✅ Done.")