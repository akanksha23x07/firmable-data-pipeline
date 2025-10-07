#!/usr/bin/env python3
"""
Direct Pipeline Script for Firmable Data Engineering Assignment
This script runs the entire data pipeline by importing and calling the modules directly:
1. extracts/ABR_extraction.py
2. extracts/commoncrawl.py
3. transformations/transform_abr.py
4. transformations/transform_commoncrawl.py
5. entity_matching/entity_matching.py
6. postgres_db_load/load_all_tables.py
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
        logging.FileHandler('logs/direct_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DirectPipelineRunner:
    """Direct pipeline runner that imports modules instead of using subprocess."""
    
    def __init__(self):
        """Initialize the pipeline runner."""
        self.start_time = None
        self.results = {}
        self.validate_environment()
        self.setup_directories()
    
    def validate_environment(self):
        """Validate that all required files exist."""
        logger.info("Validating environment...")
        
        required_files = [
            "extracts/ABR_extraction.py",
            "extracts/commoncrawl.py", 
            "transformations/transform_abr.py",
            "transformations/transform_commoncrawl.py",
            "entity_matching/entity_matching.py",
            "postgres_db_load/load_all_tables.py"
        ]
        
        missing_files = []
        for file_path in required_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            logger.error(f"Missing required files: {missing_files}")
            raise FileNotFoundError(f"Missing required files: {missing_files}")
        
        logger.info("Environment validation completed successfully")
    
    def setup_directories(self):
        """Create required directories."""
        required_dirs = [
            "logs",
            "dump/abr_dump",
            "dump/commoncrawl_dump", 
            "transformations/output/abr_clean",
            "transformations/output/commoncrawl_clean",
            "entity_matching/entity_matches"
        ]
        
        for dir_path in required_dirs:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Created/verified directory: {dir_path}")
    
    def monitor_memory(self):
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
    
    def run_abr_extraction(self) -> Dict[str, Any]:
        """Run ABR extraction."""
        logger.info("="*60)
        logger.info("STEP 1: ABR EXTRACTION")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
        try:
            # Import and run ABR extraction
            from extracts.ABR_extraction import ABRExtraction
            
            # ABR extraction configuration
            abr_extractor = ABRExtraction(
                dump_dir="dump/abr_dump",
                records_per_xml=20000,
                parquet_batch_size=10000
            )
            
            # ABR URLs to extract from (using the working URLs from the original script)
            abr_zip_urls = [
                "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
                "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip"
            ]
            
            abr_results = abr_extractor.extract_from_urls(abr_zip_urls, max_workers=2)
            execution_time = time.time() - start_time
            
            logger.info(f"ABR extraction completed: {abr_results['total_records']:,} records in {execution_time:.2f} seconds")
            
            result = {
                'success': True,
                'execution_time': execution_time,
                'records': abr_results['total_records']
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"ABR extraction failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['abr_extraction'] = result
        return result
    
    def run_commoncrawl_extraction(self) -> Dict[str, Any]:
        """Run CommonCrawl extraction."""
        logger.info("="*60)
        logger.info("STEP 2: COMMONCRAWL EXTRACTION")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
        try:
            # Import and run CommonCrawl extraction
            from extracts.commoncrawl import CommonCrawlExtractor
            
            # CommonCrawl extraction configuration
            cc_extractor = CommonCrawlExtractor(
                crawl="CC-MAIN-2025-38",  # Using the correct crawl from the original script
                base_url="https://data.commoncrawl.org",
                dump_dir="dump/commoncrawl_dump",
                batch_size=10000
            )
            
            cc_results = cc_extractor.fetch_index_optimized(
                target_records=200000,  # Using parameters closer to the original script
                max_shards=200,
                max_workers=15,
                tld_filters=['au', 'com.au', 'org.au', 'net.au']
            )
            execution_time = time.time() - start_time
            
            logger.info(f"CommonCrawl extraction completed: {cc_results['estimated_records']:,} records in {execution_time:.2f} seconds")
            
            result = {
                'success': True,
                'execution_time': execution_time,
                'records': cc_results['estimated_records']
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"CommonCrawl extraction failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['commoncrawl_extraction'] = result
        return result
    
    def run_abr_transformation(self) -> Dict[str, Any]:
        """Run ABR transformation."""
        logger.info("="*60)
        logger.info("STEP 3: ABR TRANSFORMATION")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
        try:
            # Import and run ABR transformation
            from transformations.transform_abr import ABRTransformer
            
            abr_transformer = ABRTransformer(
                input_path="dump/abr_dump",
                output_path="transformations/output/abr_clean"
            )
            abr_transformer.run_transformation(test_mode=False)
            execution_time = time.time() - start_time
            
            logger.info(f"ABR transformation completed in {execution_time:.2f} seconds")
            
            result = {
                'success': True,
                'execution_time': execution_time
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"ABR transformation failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['abr_transformation'] = result
        return result
    
    def run_commoncrawl_transformation(self) -> Dict[str, Any]:
        """Run CommonCrawl transformation."""
        logger.info("="*60)
        logger.info("STEP 4: COMMONCRAWL TRANSFORMATION")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
        try:
            # Import and run CommonCrawl transformation
            from transformations.transform_commoncrawl import CommonCrawlTransformer
            
            cc_transformer = CommonCrawlTransformer(
                input_path="dump/commoncrawl_dump",
                output_path="transformations/output/commoncrawl_clean"
            )
            cc_transformer.run_transformation(test_mode=False)
            execution_time = time.time() - start_time
            
            logger.info(f"CommonCrawl transformation completed in {execution_time:.2f} seconds")
            
            result = {
                'success': True,
                'execution_time': execution_time
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"CommonCrawl transformation failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['commoncrawl_transformation'] = result
        return result
    
    def run_entity_matching(self) -> Dict[str, Any]:
        """Run entity matching."""
        logger.info("="*60)
        logger.info("STEP 5: ENTITY MATCHING")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
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
            limit_records = int(os.getenv('ENTITY_MATCHING_LIMIT_RECORDS', '50000'))
            
            matcher.run_entity_matching(threshold=threshold, limit_records=limit_records)
            execution_time = time.time() - start_time
            
            logger.info(f"Entity matching completed in {execution_time:.2f} seconds")
            
            result = {
                'success': True,
                'execution_time': execution_time
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Entity matching failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['entity_matching'] = result
        return result
    
    def run_postgres_loading(self) -> Dict[str, Any]:
        """Run PostgreSQL loading."""
        logger.info("="*60)
        logger.info("STEP 6: POSTGRESQL LOADING")
        logger.info("="*60)
        
        start_time = time.time()
        self.monitor_memory()
        
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
            loader.connect()
            
            # Create all tables
            loader.create_all_tables()
            
            # Find the latest parquet files (they should be in daily folders now)
            import glob
            
            # Find ABR parquet file
            abr_pattern = "entity_matching/entity_matches/*/abr_with_match_flags_*.parquet"
            abr_files = glob.glob(abr_pattern)
            if not abr_files:
                # Fallback to old structure
                abr_pattern = "entity_matching/entity_matches/parquets/abr_final.parquet"
                abr_files = glob.glob(abr_pattern)
            
            # Find CommonCrawl parquet file
            cc_pattern = "entity_matching/entity_matches/*/commoncrawl_with_match_flags_*.parquet"
            cc_files = glob.glob(cc_pattern)
            if not cc_files:
                # Fallback to old structure
                cc_pattern = "entity_matching/entity_matches/parquets/commoncrawl_final.parquet"
                cc_files = glob.glob(cc_pattern)
            
            # Find matched parquet file
            matched_pattern = "entity_matching/entity_matches/*/matched_entity_matches_*.parquet"
            matched_files = glob.glob(matched_pattern)
            if not matched_files:
                # Fallback to old structure
                matched_pattern = "entity_matching/entity_matches/parquets/matched_entity_matches.parquet"
                matched_files = glob.glob(matched_pattern)
            
            if not abr_files or not cc_files or not matched_files:
                raise FileNotFoundError("Could not find required parquet files for loading")
            
            # Use the most recent files
            abr_parquet_path = max(abr_files, key=os.path.getmtime)
            cc_parquet_path = max(cc_files, key=os.path.getmtime)
            matched_parquet_path = max(matched_files, key=os.path.getmtime)
            
            logger.info(f"Using ABR file: {abr_parquet_path}")
            logger.info(f"Using CommonCrawl file: {cc_parquet_path}")
            logger.info(f"Using matched file: {matched_parquet_path}")
            
            abr_records = loader.load_abr_data(abr_parquet_path, truncate_first=True)
            cc_records = loader.load_cc_data(cc_parquet_path, truncate_first=True)
            matched_records = loader.load_matched_data(matched_parquet_path, truncate_first=True)
            
            # Get summary
            summary = loader.get_load_summary()
            loader.disconnect()
            execution_time = time.time() - start_time
            
            logger.info(f"PostgreSQL loading completed in {execution_time:.2f} seconds")
            logger.info(f"Loaded {abr_records:,} ABR records, {cc_records:,} CommonCrawl records, {matched_records:,} matched records")
            
            result = {
                'success': True,
                'execution_time': execution_time,
                'abr_records': abr_records,
                'cc_records': cc_records,
                'matched_records': matched_records,
                'summary': summary
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"PostgreSQL loading failed: {str(e)}")
            result = {
                'success': False,
                'execution_time': execution_time,
                'error': str(e)
            }
            raise
        
        self.results['postgres_loading'] = result
        return result
    
    def run_complete_pipeline(self):
        """Run the complete data pipeline."""
        logger.info("="*80)
        logger.info("STARTING COMPLETE DATA PIPELINE (DIRECT MODE)")
        logger.info("="*80)
        logger.info(f"Pipeline started at: {datetime.now()}")
        
        self.start_time = time.time()
        
        try:
            # Step 1: ABR Extraction
            self.run_abr_extraction()
            
            # Step 2: CommonCrawl Extraction
            self.run_commoncrawl_extraction()
            
            # Step 3: ABR Transformation
            self.run_abr_transformation()
            
            # Step 4: CommonCrawl Transformation
            self.run_commoncrawl_transformation()
            
            # Step 5: Entity Matching
            self.run_entity_matching()
            
            # Step 6: PostgreSQL Loading
            self.run_postgres_loading()
            
            # Calculate total time
            total_time = time.time() - self.start_time
            
            # Print final summary
            self.print_final_summary(total_time)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.print_failure_summary()
            raise
    
    def print_final_summary(self, total_time: float):
        """Print the final pipeline summary."""
        logger.info("="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Total Pipeline Time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info(f"Pipeline completed at: {datetime.now()}")
        
        # Print individual step times
        logger.info("\nStep Execution Times:")
        for step_name, result in self.results.items():
            if result['success']:
                logger.info(f"  {step_name.replace('_', ' ').title()}: {result['execution_time']:.2f} seconds")
        
        # Print record counts if available
        if 'abr_extraction' in self.results and 'records' in self.results['abr_extraction']:
            logger.info(f"\nRecords Processed:")
            logger.info(f"  ABR Records: {self.results['abr_extraction']['records']:,}")
        if 'commoncrawl_extraction' in self.results and 'records' in self.results['commoncrawl_extraction']:
            logger.info(f"  CommonCrawl Records: {self.results['commoncrawl_extraction']['records']:,}")
        if 'postgres_loading' in self.results and 'matched_records' in self.results['postgres_loading']:
            logger.info(f"  Matched Records: {self.results['postgres_loading']['matched_records']:,}")
        
        logger.info("="*80)
    
    def print_failure_summary(self):
        """Print summary when pipeline fails."""
        logger.error("="*80)
        logger.error("PIPELINE FAILED!")
        logger.error("="*80)
        
        if self.start_time:
            failed_time = time.time() - self.start_time
            logger.error(f"Pipeline failed after: {failed_time:.2f} seconds")
        
        # Print which steps succeeded
        if self.results:
            logger.error("\nCompleted Steps:")
            for step_name, result in self.results.items():
                if result['success']:
                    logger.error(f"  ✓ {step_name.replace('_', ' ').title()}: {result['execution_time']:.2f} seconds")
                else:
                    logger.error(f"  ✗ {step_name.replace('_', ' ').title()}: FAILED")
        
        logger.error("="*80)

def main():
    """Main function to run the complete pipeline."""
    try:
        runner = DirectPipelineRunner()
        runner.run_complete_pipeline()
        
        # Exit with success code
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
