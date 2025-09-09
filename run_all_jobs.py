#!/usr/bin/env python3
"""
Master script to run all data lineage jobs
"""

import os
import sys
import subprocess
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_job(script_name: str, description: str):
    """Run a single job script."""
    logger.info(f"Starting {description}...")
    
    try:
        # Change to the python_jobs directory
        os.chdir('lineage/python_jobs')
        
        # Activate virtual environment and run script
        result = subprocess.run([
            'bash', '-c', 
            f'source venv/bin/activate && python {script_name}'
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            if result.stdout:
                logger.info(f"Output: {result.stdout.strip()}")
        else:
            logger.error(f"❌ {description} failed")
            if result.stderr:
                logger.error(f"Error: {result.stderr.strip()}")
        
        # Change back to project root
        os.chdir('../..')
        
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {description} timed out")
    except Exception as e:
        logger.error(f"💥 {description} failed with exception: {str(e)}")

def main():
    """Run all data lineage jobs."""
    
    logger.info("🚀 Starting Data Lineage Job Suite")
    logger.info("=" * 50)
    
    # List of jobs to run
    jobs = [
        ("emit_lineage.py", "Customer Data Processing"),
        ("order_processing.py", "Order Processing"),
        ("financial_processing.py", "Financial Data Processing"),
        ("data_quality_monitoring.py", "Data Quality Monitoring")
    ]
    
    # Run each job
    for script_name, description in jobs:
        run_job(script_name, description)
        logger.info("-" * 30)
        time.sleep(2)  # Brief pause between jobs
    
    logger.info("=" * 50)
    logger.info("🎉 All jobs completed!")
    logger.info("")
    logger.info("📊 Check your lineage data:")
    logger.info("   Web UI: http://localhost:3000")
    logger.info("   API: http://localhost:5002/api/v1/jobs")
    logger.info("")
    logger.info("🔍 Example API queries:")
    logger.info("   curl http://localhost:5002/api/v1/jobs")
    logger.info("   curl http://localhost:5002/api/v1/namespaces/data-lineage-audit/datasets")

if __name__ == "__main__":
    main()
