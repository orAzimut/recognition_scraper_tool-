#!/usr/bin/env python3
"""
main.py
Automated orchestrator for IMO extraction and ShipSpotting scraping
Now uses Google Cloud Storage with JSON-based IMO gallery tracking
"""

import time
import sys
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict

# Import the modules
try:
    from Modules.imo_extractor import extract_haifa_imos, find_missing_imos
    from Modules.shipspotting_scraper import scrape_missing_imos
    from Modules.gcs_helper import get_gcs_manager
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("   Make sure all modules are in the Modules directory")
    sys.exit(1)

# ====================== Load Configuration ======================
def load_config():
    """Load configuration from config.yaml"""
    config_paths = [
        Path("resources/config.yaml"),
        Path("./config.yaml"),
    ]
    
    for path in config_paths:
        if path.exists():
            with open(path, 'r') as f:
                return yaml.safe_load(f)
    
    print("❌ Config file not found in resources/config.yaml")
    sys.exit(1)

CONFIG = load_config()

# ====================== Utility Functions ======================

def display_summary(log_data: Dict):
    """Display final summary"""
    print("\n" + "="*60)
    print("📊 FINAL SUMMARY")
    print("="*60)
    
    # Basic stats
    print(f"📋 Total vessels in area: {log_data['total_haifa_vessels']}")
    print(f"✅ Already in gallery: {log_data['existing_vessels']}")
    print(f"🆕 New vessels scraped: {log_data['new_vessels_scraped']}")
    
    if log_data['new_vessels_scraped'] > 0:
        print(f"📸 Photos uploaded to GCS: {log_data['photos_downloaded']}")
        avg_photos = log_data['photos_downloaded'] / log_data['new_vessels_scraped']
        print(f"📊 Average photos/vessel: {avg_photos:.1f}")
    
    # Time stats
    print(f"\n⏱️  Total execution time: {log_data['total_time']:.1f} seconds")
    if log_data['new_vessels_scraped'] > 0:
        avg_time = log_data['scraping_time'] / log_data['new_vessels_scraped']
        print(f"⚡ Average time/vessel: {avg_time:.1f} seconds")
    
    # Storage location
    print(f"\n☁️  Storage: gs://{CONFIG['gcs']['bucket_name']}")
    print("=" * 60)

def test_gcs_connection() -> bool:
    """Test Google Cloud Storage connection"""
    try:
        gcs = get_gcs_manager()
        if gcs.test_connection():
            return True
        else:
            return False
    except Exception as e:
        print(f"❌ GCS connection error: {e}")
        return False

# ====================== Main Execution Function ======================
def main():
    """Main automated workflow"""
    start_time = time.time()
    
    # Initialize tracking data
    log_data = {}
    
    try:
        # Test GCS connection first
        if not test_gcs_connection():
            print("\n❌ Cannot proceed without GCS connection")
            return
        
        # Get GCS manager instance
        gcs = get_gcs_manager()
        
        # ============ STEP 1: Extract IMOs from Haifa Bay ============
        extraction_start = time.time()
        
        haifa_imos, vessel_details = extract_haifa_imos()
        
        log_data['extraction_time'] = time.time() - extraction_start
        log_data['total_haifa_vessels'] = len(haifa_imos)
        
        if not haifa_imos:
            print("❌ No vessels found in Haifa Bay!")
            return
        
        # ============ STEP 2: Find Missing IMOs ============
        gallery_check_start = time.time()
        
        missing_imos, existing_imos = find_missing_imos(haifa_imos)
        
        log_data['gallery_check_time'] = time.time() - gallery_check_start
        log_data['existing_vessels'] = len(existing_imos)
        log_data['new_vessels_to_scrape'] = len(missing_imos)
        
        if not missing_imos:
            print("\n🎉 Gallery is up to date! No new vessels to scrape.")
            return
        
        # ============ STEP 3: Scrape Missing IMOs ============
        scraping_start = time.time()
        
        # Scrape the missing IMOs (uploads directly to GCS)
        stats = scrape_missing_imos(missing_imos, vessel_details)
        
        log_data['scraping_time'] = time.time() - scraping_start
        log_data['new_vessels_scraped'] = stats.get('total_vessels', 0) - stats.get('failed_vessels', 0)
        log_data['photos_downloaded'] = stats.get('total_photos', 0)
        log_data['failed_vessels'] = stats.get('failed_vessels', 0)
        
        # ============ STEP 4: Update IMO Gallery JSON ============
        if log_data['new_vessels_scraped'] > 0:
            try:
                gcs.update_imo_gallery_json()
                log_data['gallery_json_updated'] = True
            except Exception as e:
                log_data['gallery_json_updated'] = False
                log_data['gallery_json_error'] = str(e)
        
        # ============ COMPLETION ============
        log_data['total_time'] = time.time() - start_time
        
        # Display summary
        display_summary(log_data)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
    
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        raise

# ====================== Entry Point ======================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)