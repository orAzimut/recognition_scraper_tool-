#!/usr/bin/env python3
"""
main_scheduled.py
Scheduled version that runs the scraping job every 2 hours
"""

import time
import schedule
import logging
from datetime import datetime
from main import main  # Import your existing main function

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_job():
    """Wrapper function to run the main job with error handling"""
    try:
        logger.info("="*60)
        logger.info("🚀 Starting scheduled scraping job")
        logger.info("="*60)
        
        main()
        
        logger.info("✅ Job completed successfully")
        logger.info(f"⏰ Next run scheduled in 2 hours")
        
    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}")
        logger.info("Will retry in 2 hours...")

def run_scheduler():
    """Run the scheduler"""
    # Run immediately on startup
    logger.info("🔄 Running initial job on startup...")
    run_job()
    
    # Schedule to run every 2 hours
    schedule.every(2).hours.do(run_job)
    
    logger.info("📅 Scheduler started - job will run every 2 hours")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise