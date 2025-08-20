#!/usr/bin/env python3
"""
imo_extractor.py
Module for extracting IMO numbers from Haifa Bay and checking existing gallery
"""

import requests
import re
from pathlib import Path
from typing import List, Dict, Set, Tuple
from datetime import datetime

# ====================== Configuration ======================
API_KEY = "b123dc58-4c18-4b0c-9f04-82a06be63ff9"
SEARCH_RADIUS = 15  # km
PORT_LAT = 32.8154
PORT_LON = 35.0043

# ====================== Haifa Bay Tracker ======================
class HaifaBayTracker:
    def __init__(self, api_key: str = API_KEY, port_lat: float = PORT_LAT, port_lon: float = PORT_LON):
        self.api_key = api_key
        self.api_base_url = "https://api.datalastic.com/api/v0"
        self.session = requests.Session()
        self.port_lat = port_lat
        self.port_lon = port_lon
        
    def get_haifa_vessels(self, radius: int = SEARCH_RADIUS) -> List[Dict]:
        """Get all vessels in specified port area using Datalastic API"""
        endpoint = f"{self.api_base_url}/vessel_inradius"
        
        params = {
            'api-key': self.api_key,
            'lat': self.port_lat,
            'lon': self.port_lon,
            'radius': radius
        }
        
        try:
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('meta', {}).get('success', False):
                print("⚠️  API request was not successful")
                return []
            
            vessels = data.get('data', {}).get('vessels', [])
            return vessels
            
        except Exception as e:
            print(f"⚠️  Error fetching vessels: {e}")
            return []
    
    def get_imo_numbers_with_details(self, radius: int = SEARCH_RADIUS) -> Tuple[List[str], Dict[str, Dict]]:
        """Get IMO numbers with vessel details"""
        vessels = self.get_haifa_vessels(radius)
        
        imo_list = []
        vessel_details = {}
        
        for vessel in vessels:
            imo = vessel.get('imo')
            # Only add if IMO exists and is valid
            if imo and imo.strip() != '' and imo.strip().lower() not in ['null', 'n/a', 'none', '0']:
                imo_clean = imo.strip()
                imo_list.append(imo_clean)
                vessel_details[imo_clean] = {
                    'name': vessel.get('name', 'Unknown'),
                    'vessel_type': vessel.get('type', 'Unknown'),
                    'mmsi': vessel.get('mmsi', ''),
                    'lat': vessel.get('lat', 0),
                    'lon': vessel.get('lon', 0),
                    'destination': vessel.get('destination', ''),
                    'speed': vessel.get('speed', 0),
                    'course': vessel.get('course', 0),
                    'timestamp': vessel.get('last_position_time', ''),
                    'extracted_at': datetime.now().isoformat()
                }
        
        # Remove duplicates and return
        unique_imos = sorted(list(set(imo_list)))
        return unique_imos, vessel_details

# ====================== Gallery Checker ======================
class GalleryChecker:
    def __init__(self, gallery_base_dir: Path):
        self.gallery_base_dir = gallery_base_dir
        self.alternative_paths = [
            Path(r"C:\Users\OrGil.AzureAD\OneDrive - AMPC\Desktop\datasets"),
            Path(r"C:\Users\OrGil.AzureAD\OneDrive - AMPC\Desktop\Azimut.ai\webScrape\webScrapeByIMO\recognition dataset")
        ]
    
    def check_existing_imos(self) -> Set[str]:
        """Check all directories for existing IMOs"""
        all_existing = set()
        
        # Check main gallery
        all_existing.update(self._check_directory(self.gallery_base_dir))
        
        # Check alternative paths
        for path in self.alternative_paths:
            if path.exists():
                all_existing.update(self._check_directory(path))
        
        return all_existing
    
    def _check_directory(self, directory: Path) -> Set[str]:
        """Check a specific directory for IMO folders"""
        existing_imos = set()
        
        if not directory.exists():
            return existing_imos
        
        # Pattern to match IMO numbers
        imo_pattern = re.compile(r'(?:IMO[_\-\s]*)(\d{7})', re.I)
        
        # Search recursively for IMO folders
        for item in directory.rglob("*"):
            if item.is_dir():
                match = imo_pattern.search(item.name)
                if match:
                    existing_imos.add(match.group(1))
                # Also check for plain 7-digit numbers
                elif re.match(r'^\d{7}$', item.name):
                    existing_imos.add(item.name)
        
        return existing_imos

# ====================== Main Functions ======================
def extract_haifa_imos() -> Tuple[List[str], Dict[str, Dict]]:
    """Extract IMO numbers from Haifa Bay"""
    print("\n📡 Extracting IMOs from Haifa Bay...")
    tracker = HaifaBayTracker()
    imo_list, vessel_details = tracker.get_imo_numbers_with_details()
    print(f"✅ Found {len(imo_list)} vessels with valid IMO numbers")
    return imo_list, vessel_details

def find_missing_imos(gallery_dir: Path, haifa_imos: List[str]) -> Tuple[List[str], List[str]]:
    """Find which IMOs are missing from the gallery"""
    print("\n🔍 Checking existing gallery...")
    checker = GalleryChecker(gallery_dir)
    existing_imos = checker.check_existing_imos()
    print(f"📂 Found {len(existing_imos)} IMOs in gallery")
    
    # Find missing IMOs
    missing_imos = [imo for imo in haifa_imos if imo not in existing_imos]
    existing_in_gallery = [imo for imo in haifa_imos if imo in existing_imos]
    
    print(f"🆕 {len(missing_imos)} new IMOs to scrape")
    print(f"✅ {len(existing_in_gallery)} IMOs already in gallery")
    
    return missing_imos, existing_in_gallery

if __name__ == "__main__":
    # Test the module
    print("Testing IMO Extractor Module...")
    imos, details = extract_haifa_imos()
    print(f"Sample IMOs: {imos[:5] if imos else 'None found'}")