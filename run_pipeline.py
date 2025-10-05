#!/usr/bin/env python3
"""
Main Pipeline Script for Firmable Data Engineering Assignment
This script orchestrates the complete data pipeline:
1. Extract scripts (ABR and CommonCrawl)
2. Transform scripts (ABR and CommonCrawl)
3. Entity matching script
4. Load scripts (PostgreSQL)
"""

import os
import sys
import time
import logging
import gc
import psutil
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def validate_paths():
    """Validate and create required directories."""
    required_paths = [
        "logs",
        "dump/abr_dump",
        "dump/commoncrawl_dump",
        "transformations/output/abr_clean",
        "transformations/output/commoncrawl_clean",
        "entity_matching/entity_matches/parquets",
        "entity_matching/entity_matches/csv"
    ]
    
    for path in required_paths:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")

def monitor_memory():
    """Monitor memory usage and trigger garbage collection if needed."""
    try:
        memory = psutil.virtual_memory()
        if memory.percent > 80:
            logger.warning(f"High memory usage: {memory.percent:.1f}% - triggering garbage collection")
            gc.collect()
            memory_after = psutil.virtual_memory()
            logger.info(f"Memory after GC: {memory_after.percent:.1f}%")
        else:
            logger.debug(f"Memory usage: {memory.percent:.1f}%")
    except Exception as e:
        logger.warning(f"Could not monitor memory: {e}")

def connect_with_retry(connection_params, max_retries=5):
    """Connect to PostgreSQL with retry logic."""
    import psycopg2
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting database connection (attempt {attempt + 1}/{max_retries})")
            conn = psycopg2.connect(**connection_params)
            logger.info("Database connection successful")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection failed (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("All database connection attempts failed")
                raise
        except Exception as e:
            logger.error(f"Unexpected error during database connection: {e}")
            raise

def find_parquet_file(directory, filename):
    """Find a parquet file in the specified directory."""
    import glob
    
    if not os.path.exists(directory):
        logger.error(f"Directory does not exist: {directory}")
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    # Look for exact filename first
    exact_path = os.path.join(directory, filename)
    if os.path.exists(exact_path):
        logger.info(f"Found parquet file: {exact_path}")
        return exact_path
    
    # Look for any parquet file with similar name
    pattern = os.path.join(directory, f"*{filename.split('.')[0]}*.parquet")
    matches = glob.glob(pattern)
    
    if matches:
        logger.info(f"Found parquet file: {matches[0]}")
        return matches[0]
    
    # Look for any parquet file in directory
    pattern = os.path.join(directory, "*.parquet")
    matches = glob.glob(pattern)
    
    if matches:
        logger.warning(f"Could not find {filename}, using first available parquet: {matches[0]}")
        return matches[0]
    
    logger.error(f"No parquet files found in {directory}")
    raise FileNotFoundError(f"No parquet files found in {directory}")

def run_extraction():
    """Run extraction scripts for ABR and CommonCrawl data."""
    logger.info("="*60)
    logger.info("STEP 1: RUNNING EXTRACTION SCRIPTS")
    logger.info("="*60)
    
    start_time = time.time()
    monitor_memory()
    
    try:
        # Import and run ABR extraction
        logger.info("Starting ABR extraction...")
        from extracts.ABR_extraction import ABRExtraction
        
        # ABR extraction configuration
        abr_extractor = ABRExtraction(
            dump_dir="dump/abr_dump",
            records_per_xml=20000,
            parquet_batch_size=10000
        )
        
        # ABR URLs to extract from
        abr_zip_urls = [
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-01-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-02-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-03-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-04-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-05-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-06-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-07-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-08-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-09-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-10-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-11-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-12-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-13-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-14-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-15-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-16-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-17-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-18-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-19-gst-entities.zip",
            "https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/4b084096-65e4-4c8e-abbe-5e54ff85f42f/download/20241201-20-gst-entities.zip"
        ]
        
        abr_results = abr_extractor.extract_from_urls(abr_zip_urls, max_workers=2)
        logger.info(f"ABR extraction completed: {abr_results['total_records']:,} records")
        
        # Import and run CommonCrawl extraction
        logger.info("Starting CommonCrawl extraction...")
        from extracts.commoncrawl import CommonCrawlExtractor
        
        # CommonCrawl extraction configuration
        cc_extractor = CommonCrawlExtractor(
            crawl="CC-MAIN-2024-06",
            base_url="https://data.commoncrawl.org",
            dump_dir="dump/commoncrawl_dump",
            batch_size=10000
        )
        
        cc_results = cc_extractor.fetch_index_optimized(
            target_records=200000,
            max_shards=200,
            max_workers=20,
            tld_filters=['au', 'com.au', 'org.au', 'net.au']
        )
        logger.info(f"CommonCrawl extraction completed: {cc_results['estimated_records']:,} records")
        
        extraction_time = time.time() - start_time
        logger.info(f"Extraction phase completed in {extraction_time:.2f} seconds")
        monitor_memory()
        
        return {
            'abr_records': abr_results['total_records'],
            'cc_records': cc_results['estimated_records'],
            'extraction_time': extraction_time
        }
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        raise

def run_transformation():
    """Run transformation scripts for ABR and CommonCrawl data."""
    logger.info("="*60)
    logger.info("STEP 2: RUNNING TRANSFORMATION SCRIPTS")
    logger.info("="*60)
    
    start_time = time.time()
    monitor_memory()
    
    try:
        # Import and run ABR transformation
        logger.info("Starting ABR transformation...")
        from transformations.transform_abr import ABRTransformer
        
        abr_transformer = ABRTransformer(
            input_path="dump/abr_dump",
            output_path="transformations/output/abr_clean"
        )
        abr_transformer.run_transformation(test_mode=False)
        logger.info("ABR transformation completed")
        
        # Import and run CommonCrawl transformation
        logger.info("Starting CommonCrawl transformation...")
        from transformations.transform_commoncrawl import CommonCrawlTransformer
        
        cc_transformer = CommonCrawlTransformer(
            input_path="dump/commoncrawl_dump",
            output_path="transformations/output/commoncrawl_clean"
        )
        cc_transformer.run_transformation(test_mode=False)
        logger.info("CommonCrawl transformation completed")
        
        transformation_time = time.time() - start_time
        logger.info(f"Transformation phase completed in {transformation_time:.2f} seconds")
        monitor_memory()
        
        return {'transformation_time': transformation_time}
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        raise

def run_entity_matching():
    """Run entity matching script."""
    logger.info("="*60)
    logger.info("STEP 3: RUNNING ENTITY MATCHING SCRIPT")
    logger.info("="*60)
    
    start_time = time.time()
    monitor_memory()
    
    try:
        # Import and run entity matching
        from entity_matching.entity_matching import EntityMatcher
        
        matcher = EntityMatcher(
            abr_data_path="transformations/output/abr_clean",
            commoncrawl_data_path="transformations/output/commoncrawl_clean",
            output_path="entity_matching/entity_matches"
        )
        
        # Get entity matching parameters from environment variables
        threshold = float(os.getenv('ENTITY_MATCHING_THRESHOLD', '0.9'))
        limit_records = int(os.getenv('ENTITY_MATCHING_LIMIT_RECORDS', '10000'))
        
        matcher.run_entity_matching(threshold=threshold, limit_records=limit_records)
        logger.info("Entity matching completed")
        
        matching_time = time.time() - start_time
        logger.info(f"Entity matching phase completed in {matching_time:.2f} seconds")
        monitor_memory()
        
        return {'matching_time': matching_time}
        
    except Exception as e:
        logger.error(f"Entity matching failed: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        raise

def run_loading():
    """Run PostgreSQL loading scripts."""
    logger.info("="*60)
    logger.info("STEP 4: RUNNING LOADING SCRIPTS")
    logger.info("="*60)
    
    start_time = time.time()
    monitor_memory()
    
    try:
        # Import and run PostgreSQL loading
        from postgres_db_load.load_all_tables import AllTablesLoader
        
        # Database connection parameters
        connection_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'firmable_db'),
            'user': os.getenv('POSTGRES_USER', 'firmable'),
            'password': os.getenv('POSTGRES_PASSWORD', 'firmable123'),
            'port': int(os.getenv('POSTGRES_PORT', 5432))
        }
        
        loader = AllTablesLoader(connection_params)
        # Use retry logic for connection
        loader.conn = connect_with_retry(connection_params)
        
        # Create all tables
        loader.create_all_tables()
        
        # Load data - dynamically find parquet files
        abr_parquet_path = find_parquet_file("entity_matching/entity_matches/parquets", "abr_final.parquet")
        cc_parquet_path = find_parquet_file("entity_matching/entity_matches/parquets", "commoncrawl_final.parquet")
        matched_parquet_path = find_parquet_file("entity_matching/entity_matches/parquets", "matched_entity_matches.parquet")
        
        abr_records = loader.load_abr_data(abr_parquet_path, truncate_first=True)
        cc_records = loader.load_cc_data(cc_parquet_path, truncate_first=True)
        matched_records = loader.load_matched_data(matched_parquet_path, truncate_first=True)
        
        # Get summary
        summary = loader.get_load_summary()
        loader.disconnect()
        
        loading_time = time.time() - start_time
        logger.info(f"Loading phase completed in {loading_time:.2f} seconds")
        monitor_memory()
        
        return {
            'loading_time': loading_time,
            'summary': summary
        }
        
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        raise

def main():
    """Main pipeline execution function."""
    logger.info("="*80)
    logger.info("STARTING FIRMABLE DATA PIPELINE")
    logger.info("="*80)
    logger.info(f"Pipeline started at: {datetime.now()}")
    
    # Validate and create required paths
    validate_paths()
    
    pipeline_start_time = time.time()
    results = {}
    
    try:
        # Step 1: Extraction
        results['extraction'] = run_extraction()
        
        # Step 2: Transformation
        results['transformation'] = run_transformation()
        
        # Step 3: Entity Matching
        results['entity_matching'] = run_entity_matching()
        
        # Step 4: Loading
        results['loading'] = run_loading()
        
        # Calculate total time
        total_time = time.time() - pipeline_start_time
        
        # Print final summary
        logger.info("="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Total Pipeline Time: {total_time:.2f} seconds")
        logger.info(f"Extraction Time: {results['extraction']['extraction_time']:.2f} seconds")
        logger.info(f"Transformation Time: {results['transformation']['transformation_time']:.2f} seconds")
        logger.info(f"Entity Matching Time: {results['entity_matching']['matching_time']:.2f} seconds")
        logger.info(f"Loading Time: {results['loading']['loading_time']:.2f} seconds")
        
        if 'summary' in results['loading']:
            summary = results['loading']['summary']
            logger.info(f"ABR Records Loaded: {summary['abr_table']['total_records']:,}")
            logger.info(f"CommonCrawl Records Loaded: {summary['commoncrawl_table']['total_records']:,}")
            logger.info(f"Matched Records: {summary['australian_companies_data']['total_records']:,}")
        
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()