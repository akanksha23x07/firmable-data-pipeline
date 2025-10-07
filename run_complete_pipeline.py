#!/usr/bin/env python3
"""
Complete Data Pipeline Script for Firmable Data Engineering Assignment
This script runs the entire data pipeline in the correct order:
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
import subprocess
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/complete_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineRunner:
    """Main pipeline runner class."""
    
    def __init__(self):
        """Initialize the pipeline runner."""
        self.start_time = None
        self.results = {}
        self.validate_environment()
        self.setup_directories()
    
    def validate_environment(self):
        """Validate that all required files and dependencies exist."""
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
    
    def run_script(self, script_path: str, script_name: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """Run a Python script and return results."""
        logger.info(f"Starting {script_name}...")
        start_time = time.time()
        
        try:
            # Run the script
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=os.getcwd()
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"{script_name} completed successfully in {execution_time:.2f} seconds")
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f"{script_name} failed with return code {result.returncode}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                return {
                    'success': False,
                    'execution_time': execution_time,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'return_code': result.returncode
                }
                
        except subprocess.TimeoutExpired:
            logger.error(f"{script_name} timed out after {timeout} seconds")
            return {
                'success': False,
                'execution_time': timeout,
                'error': 'Timeout',
                'timeout': True
            }
        except Exception as e:
            logger.error(f"Error running {script_name}: {str(e)}")
            return {
                'success': False,
                'execution_time': time.time() - start_time,
                'error': str(e)
            }
    
    def run_abr_extraction(self) -> Dict[str, Any]:
        """Run ABR extraction script."""
        logger.info("="*60)
        logger.info("STEP 1: ABR EXTRACTION")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("extracts/ABR_extraction.py", "ABR Extraction", timeout=3600)  # 1 hour timeout
        
        if not result['success']:
            raise RuntimeError(f"ABR extraction failed: {result.get('error', 'Unknown error')}")
        
        self.results['abr_extraction'] = result
        return result
    
    def run_commoncrawl_extraction(self) -> Dict[str, Any]:
        """Run CommonCrawl extraction script."""
        logger.info("="*60)
        logger.info("STEP 2: COMMONCRAWL EXTRACTION")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("extracts/commoncrawl.py", "CommonCrawl Extraction", timeout=3600)  # 1 hour timeout
        
        if not result['success']:
            raise RuntimeError(f"CommonCrawl extraction failed: {result.get('error', 'Unknown error')}")
        
        self.results['commoncrawl_extraction'] = result
        return result
    
    def run_abr_transformation(self) -> Dict[str, Any]:
        """Run ABR transformation script."""
        logger.info("="*60)
        logger.info("STEP 3: ABR TRANSFORMATION")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("transformations/transform_abr.py", "ABR Transformation", timeout=1800)  # 30 min timeout
        
        if not result['success']:
            raise RuntimeError(f"ABR transformation failed: {result.get('error', 'Unknown error')}")
        
        self.results['abr_transformation'] = result
        return result
    
    def run_commoncrawl_transformation(self) -> Dict[str, Any]:
        """Run CommonCrawl transformation script."""
        logger.info("="*60)
        logger.info("STEP 4: COMMONCRAWL TRANSFORMATION")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("transformations/transform_commoncrawl.py", "CommonCrawl Transformation", timeout=1800)  # 30 min timeout
        
        if not result['success']:
            raise RuntimeError(f"CommonCrawl transformation failed: {result.get('error', 'Unknown error')}")
        
        self.results['commoncrawl_transformation'] = result
        return result
    
    def run_entity_matching(self) -> Dict[str, Any]:
        """Run entity matching script."""
        logger.info("="*60)
        logger.info("STEP 5: ENTITY MATCHING")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("entity_matching/entity_matching.py", "Entity Matching", timeout=7200)  # 2 hour timeout
        
        if not result['success']:
            raise RuntimeError(f"Entity matching failed: {result.get('error', 'Unknown error')}")
        
        self.results['entity_matching'] = result
        return result
    
    def run_postgres_loading(self) -> Dict[str, Any]:
        """Run PostgreSQL loading script."""
        logger.info("="*60)
        logger.info("STEP 6: POSTGRESQL LOADING")
        logger.info("="*60)
        
        self.monitor_memory()
        result = self.run_script("postgres_db_load/load_all_tables.py", "PostgreSQL Loading", timeout=1800)  # 30 min timeout
        
        if not result['success']:
            raise RuntimeError(f"PostgreSQL loading failed: {result.get('error', 'Unknown error')}")
        
        self.results['postgres_loading'] = result
        return result
    
    def run_complete_pipeline(self):
        """Run the complete data pipeline."""
        logger.info("="*80)
        logger.info("STARTING COMPLETE DATA PIPELINE")
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
            logger.error(f"Pipeline failed at step: {str(e)}")
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
        runner = PipelineRunner()
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
