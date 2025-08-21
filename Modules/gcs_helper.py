#!/usr/bin/env python3
"""
gcs_helper.py
Google Cloud Storage helper functions for the vessel scraping project
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
        
        # Set paths from config
        self.upload_base = self.config['gcs']['paths']['upload_base']
        self.check_base = self.config['gcs']['paths']['check_base']
        
        logger.info(f"✅ GCS client initialized for bucket: {self.bucket_name}")
    
    def check_existing_imos(self) -> Set[str]:
        """Check for existing IMO folders in GCS"""
        existing_imos = set()
        
        # Pattern to match IMO folders (IMO_1234567 or just 1234567)
        imo_pattern = re.compile(r"(?:IMO[_\-\s]*)(\d{7})", re.I)
        
        try:
            # List all blobs under the check_base path
            prefix = self.check_base.rstrip('/') + '/'
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            
            # Track processed paths to avoid duplicates
            processed_paths = set()
            
            for blob in blobs:
                # Extract directory paths from blob names
                parts = blob.name.split('/')
                
                # Check each directory level for IMO patterns
                for i in range(len(parts) - 1):  # -1 to exclude the file name
                    dir_path = '/'.join(parts[:i+1])
                    
                    if dir_path in processed_paths:
                        continue
                    processed_paths.add(dir_path)
                    
                    dir_name = parts[i]
                    
                    # Check if directory name matches IMO pattern
                    match = imo_pattern.search(dir_name)
                    if match:
                        existing_imos.add(match.group(1))
                    elif re.fullmatch(r"\d{7}", dir_name):
                        existing_imos.add(dir_name)
            
            logger.info(f"📊 Found {len(existing_imos)} existing IMOs in GCS")
            
        except Exception as e:
            logger.error(f"Error checking existing IMOs in GCS: {e}")
            raise
        
        return existing_imos
    
    def upload_image(self, imo: str, photo_id: str, image_data: bytes, 
                     metadata: Dict = None) -> bool:
        """Upload a single image to GCS"""
        try:
            # Construct the blob path
            date_folder = datetime.now().strftime("%Y-%m-%d")
            image_path = f"{self.upload_base}/{date_folder}/IMO_{imo}/{photo_id}.jpg"
            
            # Upload image
            blob = self.bucket.blob(image_path)
            blob.upload_from_string(image_data, content_type='image/jpeg')
            
            # Upload metadata if provided
            if metadata:
                metadata_path = f"{self.upload_base}/{date_folder}/IMO_{imo}/{photo_id}.json"
                metadata_blob = self.bucket.blob(metadata_path)
                metadata_blob.upload_from_string(
                    json.dumps(metadata, indent=2),
                    content_type='application/json'
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error uploading image {photo_id} for IMO {imo}: {e}")
            return False
    
    def upload_batch(self, imo: str, images: List[Tuple[str, bytes, Dict]]) -> int:
        """
        Upload multiple images for an IMO
        
        Args:
            imo: IMO number
            images: List of tuples (photo_id, image_bytes, metadata_dict)
        
        Returns:
            Number of successfully uploaded images
        """
        uploaded = 0
        date_folder = datetime.now().strftime("%Y-%m-%d")
        base_path = f"{self.upload_base}/{date_folder}/IMO_{imo}"
        
        for photo_id, image_data, metadata in images:
            try:
                # Upload image
                image_path = f"{base_path}/{photo_id}.jpg"
                image_blob = self.bucket.blob(image_path)
                image_blob.upload_from_string(image_data, content_type='image/jpeg')
                
                # Upload metadata
                if metadata:
                    metadata_path = f"{base_path}/{photo_id}.json"
                    metadata_blob = self.bucket.blob(metadata_path)
                    metadata_blob.upload_from_string(
                        json.dumps(metadata, indent=2),
                        content_type='application/json'
                    )
                
                uploaded += 1
                
            except Exception as e:
                logger.error(f"Error uploading {photo_id} for IMO {imo}: {e}")
        
        return uploaded
    
    def create_imo_folder(self, imo: str) -> str:
        """
        Create a folder structure for an IMO (by creating a placeholder)
        Returns the folder path
        """
        date_folder = datetime.now().strftime("%Y-%m-%d")
        folder_path = f"{self.upload_base}/{date_folder}/IMO_{imo}/"
        
        # Create a placeholder file to establish the folder
        placeholder_blob = self.bucket.blob(f"{folder_path}.placeholder")
        placeholder_blob.upload_from_string(
            f"Folder created for IMO {imo} on {datetime.now().isoformat()}"
        )
        
        return folder_path
    
    def check_imo_exists(self, imo: str) -> bool:
        """Check if a specific IMO exists in GCS"""
        prefix = f"{self.check_base}/"
        
        # Look for any blob that contains this IMO in its path
        blobs = self.client.list_blobs(
            self.bucket_name, 
            prefix=prefix,
            max_results=1000  # Limit for performance
        )
        
        imo_pattern = re.compile(rf"(?:IMO[_\-\s]*)?{imo}(?:[/\.]|$)", re.I)
        
        for blob in blobs:
            if imo_pattern.search(blob.name):
                return True
        
        return False
    
    def get_imo_image_count(self, imo: str) -> int:
        """Get the number of images for a specific IMO"""
        count = 0
        prefix = f"{self.check_base}/"
        
        # Pattern to match this specific IMO
        imo_pattern = re.compile(rf"IMO[_\-\s]*{imo}/.*\.jpg$", re.I)
        
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        
        for blob in blobs:
            if imo_pattern.search(blob.name):
                count += 1
        
        return count
    
    def test_connection(self) -> bool:
        """Test the GCS connection"""
        try:
            # Try to list buckets to verify credentials
            buckets = list(self.client.list_buckets(max_results=1))
            logger.info(f"✅ GCS connection successful")
            
            # Check if our bucket exists
            if self.bucket.exists():
                logger.info(f"✅ Bucket '{self.bucket_name}' is accessible")
                return True
            else:
                logger.error(f"❌ Bucket '{self.bucket_name}' not found or not accessible")
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