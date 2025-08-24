#!/usr/bin/env python3
"""
gcs_helper.py
Google Cloud Storage helper functions for the vessel scraping project
Optimized version using JSON file for fast IMO lookups
"""

import os
import io
import re
import json
import logging
from pathlib import Path
from typing import Set, List, Dict, Optional, Tuple
from datetime import datetime

import yaml
from google.cloud import storage
from google.oauth2 import service_account

# Setup logging
logger = logging.getLogger(__name__)

class GCSManager:
    """Manages Google Cloud Storage operations for vessel images"""
    
    def __init__(self, config_path: Path = None):
        """Initialize GCS Manager with configuration"""
        # Load configuration
        if config_path is None:
            # Try to find config.yaml in standard locations
            possible_paths = [
                Path("resources/config.yaml"),
                Path("../resources/config.yaml"),
                Path("./config.yaml"),
            ]
            for path in possible_paths:
                if path.exists():
                    config_path = path
                    break
            else:
                raise FileNotFoundError("Could not find config.yaml file")
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize GCS client
        self._init_gcs_client()
        
        # JSON file path for IMO gallery
        self.imo_json_path = "reidentification/bronze/json_lables/ship_spotting/imo_galley.json"
        
        # Separate paths for photos and JSONs
        self.photo_base = "reidentification/bronze/raw_crops/ship_spotting"
        self.json_base = "reidentification/bronze/json_lables/ship_spotting"
        
        # Cache for IMO list
        self._cached_imos = None
        self._new_imos_this_session = set()
        
    def _init_gcs_client(self):
        """Initialize Google Cloud Storage client"""
        credentials_path = self.config['gcs']['credentials_path']
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
        
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        
        self.client = storage.Client(credentials=credentials)
        self.bucket_name = self.config['gcs']['bucket_name']
        self.bucket = self.client.bucket(self.bucket_name)
        
        # Set paths - ignoring config paths, using hardcoded values
        self.upload_base = "reidentification/bronze/raw_crops/ship_spotting"  # For photos
        self.check_base = self.upload_base  # Check in the photo location
        
        # GCS client initialized silently
    
    def _load_imo_json(self) -> Set[str]:
        """Load the IMO gallery JSON file from GCS"""
        try:
            blob = self.bucket.blob(self.imo_json_path)
            
            if not blob.exists():
                logger.warning(f"IMO gallery JSON not found at {self.imo_json_path}, creating new one")
                return set()
            
            # Download and parse JSON
            json_content = blob.download_as_text()
            data = json.loads(json_content)
            
            # Handle different possible JSON structures
            if isinstance(data, list):
                # If it's a simple list of IMOs
                imos = set(str(imo) for imo in data if str(imo).isdigit() and len(str(imo)) == 7)
            elif isinstance(data, dict):
                # Check for our expected structure first
                if "imos" in data and isinstance(data["imos"], list):
                    # This is our expected structure
                    imos = set(str(imo) for imo in data["imos"] if str(imo).isdigit() and len(str(imo)) == 7)
                elif "imo_numbers" in data and isinstance(data["imo_numbers"], list):
                    # Alternative structure with imo_numbers key
                    imos = set(str(imo) for imo in data["imo_numbers"] if str(imo).isdigit() and len(str(imo)) == 7)
                else:
                    # Legacy structure - might have IMOs as keys
                    # Only take keys that look like IMO numbers (7 digits)
                    imos = set()
                    for key in data.keys():
                        if str(key).isdigit() and len(str(key)) == 7:
                            imos.add(str(key))
            else:
                logger.warning(f"Unexpected JSON structure in IMO gallery file")
                imos = set()
            
            # Validate IMO format (should be 7 digits)
            valid_imos = {imo for imo in imos if imo.isdigit() and len(imo) == 7}
            
            # Loaded IMOs silently
            return valid_imos
            
        except Exception as e:
            logger.error(f"Error loading IMO gallery JSON: {e}")
            # Fall back to empty set if there's an error
            return set()
    
    def _save_imo_json(self, imos: Set[str]):
        """Save the updated IMO list back to GCS"""
        try:
            # Filter to ensure we only save valid IMO numbers (7 digits)
            valid_imos = {imo for imo in imos if str(imo).isdigit() and len(str(imo)) == 7}
            
            # Convert set to sorted list for consistent JSON output
            imo_list = sorted(list(valid_imos))
            
            # Create JSON structure matching existing format
            # Using "imo_numbers" to match your existing structure
            data = {
                "last_updated": datetime.now().isoformat(),
                "imo_numbers": imo_list
            }
            
            # Upload to GCS
            blob = self.bucket.blob(self.imo_json_path)
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type='application/json'
            )
            
            # Updated IMO gallery JSON silently
            
        except Exception as e:
            logger.error(f"Error saving IMO gallery JSON: {e}")
    
    def check_existing_imos(self) -> Set[str]:
        """Check for existing IMO folders using the JSON file for fast lookup"""
        # Use cached IMOs if available, otherwise load from JSON
        if self._cached_imos is None:
            self._cached_imos = self._load_imo_json()
        
        return self._cached_imos.copy()
    
    def upload_image(self, imo: str, photo_id: str, image_data: bytes, 
                     metadata: Dict = None) -> bool:
        """Upload image and metadata to separate GCS locations"""
        try:
            # Upload image to raw_crops path
            image_path = f"{self.photo_base}/IMO_{imo}/{photo_id}.jpg"
            blob = self.bucket.blob(image_path)
            blob.upload_from_string(image_data, content_type='image/jpeg')
            
            # Upload metadata JSON to json_lables path
            if metadata:
                metadata_path = f"{self.json_base}/IMO_{imo}/{photo_id}.json"
                metadata_blob = self.bucket.blob(metadata_path)
                metadata_blob.upload_from_string(
                    json.dumps(metadata, indent=2),
                    content_type='application/json'
                )
            
            # Track this IMO as newly added
            self._new_imos_this_session.add(imo)
            
            return True
            
        except Exception as e:
            logger.error(f"Error uploading image {photo_id} for IMO {imo}: {e}")
            return False
    
    def upload_batch(self, imo: str, images: List[Tuple[str, bytes, Dict]]) -> int:
        """
        Upload multiple images and JSONs to separate GCS locations
        Photos go to raw_crops path, JSONs go to json_lables path
        
        Args:
            imo: IMO number
            images: List of tuples (photo_id, image_bytes, metadata_dict)
        
        Returns:
            Number of successfully uploaded images
        """
        uploaded = 0
        
        for photo_id, image_data, metadata in images:
            try:
                # Upload image to raw_crops path
                image_path = f"{self.photo_base}/IMO_{imo}/{photo_id}.jpg"
                image_blob = self.bucket.blob(image_path)
                image_blob.upload_from_string(image_data, content_type='image/jpeg')
                
                # Upload metadata JSON to json_lables path
                if metadata:
                    metadata_path = f"{self.json_base}/IMO_{imo}/{photo_id}.json"
                    metadata_blob = self.bucket.blob(metadata_path)
                    metadata_blob.upload_from_string(
                        json.dumps(metadata, indent=2),
                        content_type='application/json'
                    )
                
                uploaded += 1
                
            except Exception as e:
                logger.error(f"Error uploading {photo_id} for IMO {imo}: {e}")
        
        # Track this IMO as newly added
        if uploaded > 0:
            self._new_imos_this_session.add(imo)
        
        return uploaded
    
    def rebuild_imo_gallery_json(self) -> Set[str]:
        """
        Rebuild the IMO gallery JSON by scanning existing folders in GCS.
        Useful for recovering from corrupted JSON or initial setup.
        """
        logger.info("🔄 Rebuilding IMO gallery JSON...")
        
        existing_imos = set()
        
        # Pattern to match IMO folders (IMO_1234567 or just 1234567)
        imo_pattern = re.compile(r"(?:IMO[_\-\s]*)(\d{7})", re.I)
        
        try:
            # Scan the photo storage path for IMO folders
            prefix = self.photo_base.rstrip('/') + '/'
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter='/')
            
            # Get folder names (prefixes)
            for prefix in blobs.prefixes:
                # Extract folder name from path
                folder_name = prefix.rstrip('/').split('/')[-1]
                
                # Check if folder name matches IMO pattern
                match = imo_pattern.search(folder_name)
                if match:
                    imo = match.group(1)
                    if imo.isdigit() and len(imo) == 7:
                        existing_imos.add(imo)
                elif folder_name.isdigit() and len(folder_name) == 7:
                    existing_imos.add(folder_name)
            
            logger.info(f"📊 Found {len(existing_imos)} IMO folders")
            
            # Save the rebuilt list
            if existing_imos:
                self._save_imo_json(existing_imos)
                self._cached_imos = existing_imos
                logger.info(f"✅ Rebuilt IMO gallery JSON")
            else:
                logger.warning("No IMO folders found in GCS")
                
        except Exception as e:
            logger.error(f"Error rebuilding IMO gallery JSON: {e}")
            raise
        
        return existing_imos
    
    def check_imo_exists(self, imo: str) -> bool:
        """Check if a specific IMO exists using the JSON cache"""
        if self._cached_imos is None:
            self._cached_imos = self._load_imo_json()
        
        return imo in self._cached_imos
    
    def get_imo_image_count(self, imo: str) -> int:
        """Get the number of images for a specific IMO"""
        count = 0
        prefix = f"{self.photo_base}/IMO_{imo}/"
        
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        
        for blob in blobs:
            if blob.name.endswith('.jpg'):
                count += 1
        
        return count
    
    def update_imo_gallery_json(self):
        """Update the IMO gallery JSON with newly scraped IMOs"""
        if not self._new_imos_this_session:
            logger.info("No new IMOs to add to gallery JSON")
            return
        
        # Load current IMOs (force reload to get latest)
        try:
            # Force reload from GCS to ensure we have the latest data
            current_imos = self._load_imo_json()
        except Exception as e:
            logger.error(f"Error loading current IMO gallery: {e}")
            current_imos = set()
        
        # Filter valid IMOs from new session
        valid_new_imos = {imo for imo in self._new_imos_this_session 
                          if str(imo).isdigit() and len(str(imo)) == 7}
        
        # Add new IMOs
        initial_count = len(current_imos)
        updated_imos = current_imos.union(valid_new_imos)
        
        # Save updated list
        self._save_imo_json(updated_imos)
        
        added_count = len(updated_imos) - initial_count
        logger.info(f"📝 Added {added_count} new IMOs to gallery JSON")
        logger.info(f"📊 Total IMOs in gallery: {len(updated_imos)}")
        
        # Update cache
        self._cached_imos = updated_imos
        
        # Clear the session tracking
        self._new_imos_this_session.clear()
    
    def test_connection(self) -> bool:
        """Test the GCS connection"""
        try:
            # Try to list buckets to verify credentials
            buckets = list(self.client.list_buckets(max_results=1))
            
            # Check if our bucket exists
            if self.bucket.exists():
                # Test loading the IMO JSON
                self._cached_imos = self._load_imo_json()
                
                return True
            else:
                logger.error(f"❌ Bucket not accessible")
                return False
                
        except Exception as e:
            logger.error(f"❌ GCS connection failed: {e}")
            return False

# Singleton instance for easy import
_gcs_manager = None

def get_gcs_manager(config_path: Path = None) -> GCSManager:
    """Get or create the GCS manager singleton"""
    global _gcs_manager
    if _gcs_manager is None:
        _gcs_manager = GCSManager(config_path)
    return _gcs_manager