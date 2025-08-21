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
import json
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

# Logging configuration
LOG_FILE = Path(f"scraping_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

# ====================== Utility Functions ======================
def print_header(text: str, char: str = "=", width: int = 70):
    """Print formatted header"""
    print(f"\n{char * width}")
    print(f"{text:^{width}}")
    print(f"{char * width}")

def print_progress_bar(current: int, total: int, prefix: str = "", width: int = 40):
    """Print a simple progress bar"""
    if total == 0:
        return
    percent = current / total
    filled = int(width * percent)
    bar = "█" * filled + "░" * (width - filled)
    print(f"\r{prefix} [{bar}] {current}/{total} ({percent*100:.1f}%)", end="", flush=True)

def save_log(log_data: Dict):
    """Save execution log to file"""
    with open(LOG_FILE, 'w') as f:
        json.dump(log_data, f, indent=2)

def display_summary(log_data: Dict):
    """Display final summary"""
    print_header("FINAL SUMMARY", "=", 70)
    
    # Basic stats
    print(f"\n📊 Statistics:")
    print(f"  • Run Date: {log_data['run_date']}")
    print(f"  • Location: {CONFIG['port']['name']} ({log_data['search_radius']}km radius)")
    print(f"  • Total vessels in area: {log_data['total_haifa_vessels']}")
    print(f"  • Already in gallery: {log_data['existing_vessels']}")
    print(f"  • New vessels scraped: {log_data['new_vessels_scraped']}")
    
    if log_data['new_vessels_scraped'] > 0:
        print(f"  • Photos uploaded to GCS: {log_data['photos_downloaded']}")
        avg_photos = log_data['photos_downloaded'] / log_data['new_vessels_scraped']
        print(f"  • Average photos/vessel: {avg_photos:.1f}")
    
    # Time stats
    print(f"\n⏱️  Timing:")
    print(f"  • Total execution time: {log_data['total_time']:.1f} seconds")
    print(f"  • IMO extraction: {log_data['extraction_time']:.1f} seconds")
    print(f"  • Gallery check: {log_data['gallery_check_time']:.1f} seconds")
    if log_data['new_vessels_scraped'] > 0:
        print(f"  • Scraping time: {log_data['scraping_time']:.1f} seconds")
        avg_time = log_data['scraping_time'] / log_data['new_vessels_scraped']
        print(f"  • Average time/vessel: {avg_time:.1f} seconds")
    
    # Storage location
    print(f"\n☁️  Storage:")
    print(f"  • Bucket: gs://{CONFIG['gcs']['bucket_name']}")
    print(f"  • Photos path: reidentification/bronze/raw_crops/ship_spotting/IMO_*/")
    print(f"  • JSON metadata path: reidentification/bronze/json_lables/ship_spotting/IMO_*/")
    print(f"  • IMO gallery index: reidentification/bronze/json_lables/ship_spotting/imo_galley.json")
    print(f"  • Log file: {LOG_FILE}")
    
    print("\n" + "=" * 70)

def test_gcs_connection() -> bool:
    """Test Google Cloud Storage connection"""
    try:
        print("\n🔍 Testing Google Cloud Storage connection...")
        gcs = get_gcs_manager()
        if gcs.test_connection():
            print(f"✅ Successfully connected to GCS bucket: {CONFIG['gcs']['bucket_name']}")
            return True
        else:
            print(f"❌ Failed to connect to GCS bucket: {CONFIG['gcs']['bucket_name']}")
            return False
    except Exception as e:
        print(f"❌ GCS connection error: {e}")
        return False

# ====================== Main Execution Function ======================
def main():
    """Main automated workflow"""
    start_time = time.time()
    
    # Initialize log
    log_data = {
        'run_date': datetime.now().strftime('%Y-%m-%d'),
        'run_time': datetime.now().strftime('%H:%M:%S'),
        'search_radius': CONFIG['port']['search_radius_km'],
        'gcs_bucket': CONFIG['gcs']['bucket_name'],
        'gcs_upload_path': 'reidentification/bronze/raw_crops/ship_spotting',
        'output_folder': 'N/A - Direct upload without timestamp folders'
    }
    
    # Welcome message
    print_header("HAIFA BAY VESSEL IMAGE SCRAPER (GCS)", "═", 70)
    print(f"\n🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"☁️  Storage: Google Cloud Storage")
    print(f"📦 Bucket: gs://{CONFIG['gcs']['bucket_name']}")
    print(f"📸 Photos path: reidentification/bronze/raw_crops/ship_spotting/")
    print(f"📄 JSON metadata: reidentification/bronze/json_lables/ship_spotting/")
    print(f"📋 IMO Gallery index: reidentification/bronze/json_lables/ship_spotting/imo_galley.json")
    
    try:
        # Test GCS connection first
        if not test_gcs_connection():
            print("\n❌ Cannot proceed without GCS connection")
            print("   Please check:")
            print(f"   1. Credentials file exists: {CONFIG['gcs']['credentials_path']}")
            print(f"   2. Bucket is accessible: {CONFIG['gcs']['bucket_name']}")
            print("   3. You have proper permissions")
            log_data['status'] = 'failed'
            log_data['error'] = 'GCS connection failed'
            save_log(log_data)
            return
        
        # Get GCS manager instance
        gcs = get_gcs_manager()
        
        # ============ STEP 1: Extract IMOs from Haifa Bay ============
        print_header("STEP 1: EXTRACTING IMOS FROM HAIFA BAY", "-", 70)
        extraction_start = time.time()
        
        # Note: extract_haifa_imos will prompt for mode if not specified
        haifa_imos, vessel_details = extract_haifa_imos()
        
        log_data['extraction_time'] = time.time() - extraction_start
        log_data['total_haifa_vessels'] = len(haifa_imos)
        
        if not haifa_imos:
            print("❌ No vessels found in Haifa Bay!")
            print("   Please check API key and internet connection")
            log_data['status'] = 'failed'
            log_data['error'] = 'No vessels found in Haifa Bay'
            save_log(log_data)
            return
        
        # Display some vessels
        print(f"\n📋 Sample vessels found:")
        for i, (imo, details) in enumerate(list(vessel_details.items())[:5], 1):
            print(f"  {i}. IMO {imo}: {details['name'][:40]:<40} | Type: {details.get('vessel_type', 'Unknown')}")
        if len(haifa_imos) > 5:
            print(f"  ... and {len(haifa_imos) - 5} more vessels")
        
        # ============ STEP 2: Find Missing IMOs ============
        print_header("STEP 2: CHECKING GCS FOR EXISTING IMOS", "-", 70)
        print("📄 Using JSON gallery for fast lookup...")
        gallery_check_start = time.time()
        
        missing_imos, existing_imos = find_missing_imos(haifa_imos)
        
        log_data['gallery_check_time'] = time.time() - gallery_check_start
        log_data['existing_vessels'] = len(existing_imos)
        log_data['new_vessels_to_scrape'] = len(missing_imos)
        
        print(f"⚡ Gallery check completed in {log_data['gallery_check_time']:.2f} seconds")
        
        if not missing_imos:
            print("\n🎉 Gallery is up to date! No new vessels to scrape.")
            log_data['status'] = 'up_to_date'
            log_data['new_vessels_scraped'] = 0
            log_data['photos_downloaded'] = 0
            log_data['scraping_time'] = 0
            log_data['total_time'] = time.time() - start_time
            save_log(log_data)
            display_summary(log_data)
            return
        
        # Display missing vessels
        print(f"\n📋 New vessels to scrape:")
        for i, imo in enumerate(missing_imos[:10], 1):
            vessel_name = vessel_details.get(imo, {}).get('name', 'Unknown')
            print(f"  {i:2d}. IMO {imo}: {vessel_name[:40]}")
        if len(missing_imos) > 10:
            print(f"  ... and {len(missing_imos) - 10} more vessels")
        
        # ============ STEP 3: Scrape Missing IMOs ============
        print_header("STEP 3: SCRAPING & UPLOADING TO GCS", "-", 70)
        scraping_start = time.time()
        
        # Ask for confirmation if many vessels
        if len(missing_imos) > 20:
            print(f"\n⚠️  About to scrape {len(missing_imos)} vessels.")
            print("   This may take a while...")
            response = input("   Continue? (y/n): ").strip().lower()
            if response != 'y':
                print("❌ Scraping cancelled by user")
                log_data['status'] = 'cancelled'
                save_log(log_data)
                return
        
        # Scrape the missing IMOs (uploads directly to GCS)
        stats = scrape_missing_imos(missing_imos, vessel_details)
        
        log_data['scraping_time'] = time.time() - scraping_start
        log_data['new_vessels_scraped'] = stats.get('total_vessels', 0) - stats.get('failed_vessels', 0)
        log_data['photos_downloaded'] = stats.get('total_photos', 0)  # Actually uploaded to GCS
        log_data['failed_vessels'] = stats.get('failed_vessels', 0)
        
        # ============ STEP 4: Update IMO Gallery JSON ============
        if log_data['new_vessels_scraped'] > 0:
            print_header("STEP 4: UPDATING IMO GALLERY JSON", "-", 70)
            print("📝 Updating IMO gallery JSON with new vessels...")
            
            try:
                gcs.update_imo_gallery_json()
                print("✅ IMO gallery JSON successfully updated")
                log_data['gallery_json_updated'] = True
            except Exception as e:
                print(f"⚠️  Failed to update IMO gallery JSON: {e}")
                log_data['gallery_json_updated'] = False
                log_data['gallery_json_error'] = str(e)
        
        # ============ COMPLETION ============
        log_data['total_time'] = time.time() - start_time
        log_data['status'] = 'completed'
        
        # Save log
        save_log(log_data)
        
        # Display summary
        display_summary(log_data)
        
        # Success message
        if log_data['photos_downloaded'] > 0:
            print("\n🎉 SUCCESS! Images and metadata have been uploaded to Google Cloud Storage.")
            print(f"📸 Photos: gs://{CONFIG['gcs']['bucket_name']}/reidentification/bronze/raw_crops/ship_spotting/IMO_*/")
            print(f"📄 JSONs: gs://{CONFIG['gcs']['bucket_name']}/reidentification/bronze/json_lables/ship_spotting/IMO_*/")
            if log_data.get('gallery_json_updated'):
                print("📋 IMO gallery index has been updated with new vessels")
        else:
            print("\n⚠️  Completed, but no photos were uploaded.")
            print("   This might be due to vessels not having photos on ShipSpotting.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        log_data['status'] = 'interrupted'
        log_data['total_time'] = time.time() - start_time
        save_log(log_data)
        print(f"📝 Partial log saved to: {LOG_FILE}")
        
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        log_data['status'] = 'error'
        log_data['error'] = str(e)
        log_data['total_time'] = time.time() - start_time
        save_log(log_data)
        print(f"📝 Error log saved to: {LOG_FILE}")
        raise

# ====================== Entry Point ======================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)