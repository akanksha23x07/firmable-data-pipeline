import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gzip
import duckdb
import pandas as pd
import os
import logging
import time
import json
import fastparquet
from dotenv import load_dotenv  # type: ignore
load_dotenv()
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime


class CommonCrawlExtractor:
    """Common Crawl data extraction class for Australian websites"""
    
    def __init__(self, crawl: str, base_url: str, dump_dir: str = "dump/commoncrawl_dump", batch_size: int = None) -> None:
        self.crawl = crawl
        self.base_url = base_url.rstrip("/") + "/"
        self.dump_dir = dump_dir
        
        # Set default batch size
        self.batch_size = batch_size or 10000
        self.logger = self._setup_logging()
        self.logger.info(f"Using batch_size: {self.batch_size}")
        
        self.lock = threading.Lock()
        
        # Create dump directory
        os.makedirs(self.dump_dir, exist_ok=True)
        # Create logs directory
        os.makedirs('logs', exist_ok=True)
        
        # Setup resilient HTTP session
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.logger.info(f"Initialized CommonCrawlExtractor with crawl: {crawl}, base_url: {self.base_url}")
        self.logger.info(f"Dump directory: {self.dump_dir}, Batch size: {self.batch_size}")

    def _get_daily_folder_path(self):
        """Get the current date folder path for organizing data by day"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        daily_folder = os.path.join(self.dump_dir, current_date)
        os.makedirs(daily_folder, exist_ok=True)
        return daily_folder

    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler('logs/commoncrawl_extraction_logs.log', encoding = 'utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(self.__class__.__name__)

    def _manifest_url(self) -> str:
        manifest_url = f"{self.base_url}crawl-data/{self.crawl}/cc-index-table.paths.gz"
        self.logger.debug(f"Generated manifest URL: {manifest_url}")
        return manifest_url

    def _fetch_manifest_paths(self) -> list[str]:
        manifest_url = self._manifest_url()
        self.logger.info(f"Fetching manifest from: {manifest_url}")
        
        try:
            resp = self.session.get(manifest_url, timeout=30)
            resp.raise_for_status()
            self.logger.info("Manifest download successful")
            
            paths = gzip.decompress(resp.content).decode("utf-8").strip().splitlines()
            self.logger.info(f"Found {len(paths)} Parquet shards in manifest")
            return paths
            
        except Exception as e:
            self.logger.error(f"Failed to fetch manifest: {e}")
            raise

    def _write_parquet_batch(self, batch_df: pd.DataFrame, batch_num: int):
        """Write batch to Parquet file with daywise organization and timestamp naming"""
        if batch_df.empty:
            self.logger.debug("Empty batch, skipping write")
            return
            
        try:
            # Get daily folder path
            daily_folder = self._get_daily_folder_path()
            
            # Create timestamp for unique file naming
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
            
            # Create parquet filename with timestamp
            parquet_file = os.path.join(daily_folder, f"commoncrawl_{timestamp}.parquet")
            
            self.logger.debug(f"Writing batch {batch_num} with {len(batch_df)} records to {parquet_file}")
            
            fastparquet.write(parquet_file, batch_df, compression='snappy')
            self.logger.info(f"Successfully wrote {len(batch_df)} records to {parquet_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to write batch {batch_num}: {e}")
            raise

    def _process_single_shard(self, shard_url: str, tld_filters: list, shard_num: int) -> Optional[pd.DataFrame]:
        """Process a single shard in parallel with multiple TLD filters"""
        try:
            # Create separate DuckDB connection for each thread
            con = duckdb.connect()
            
            # Try multiple TLD filters for better coverage
            all_results = []
            for tld_filter in tld_filters:
                try:
                    # Original query - get unique records per domain with crawl version as index
                    q = f"""
                    WITH pages AS (
                    SELECT
                        url,
                        url_host_registered_domain,
                        ROW_NUMBER() OVER (
                        PARTITION BY url_host_registered_domain
                        ORDER BY url
                        ) AS rn,
                        '{self.crawl}' AS index
                    FROM read_parquet('{shard_url}')
                    WHERE url_host_tld = '{tld_filter}'
                    AND content_mime_detected = 'text/html'
                    )
                    SELECT url, url_host_registered_domain, index
                    FROM pages
                    WHERE rn <= 1
                    """
                    
                    df_part = con.execute(q).df()
                    if not df_part.empty:
                        self.logger.info(f"✅ Found {len(df_part)} results in shard {shard_num} for TLD '{tld_filter}'")
                        all_results.append(df_part)
                        
                except Exception as e:
                    self.logger.debug(f"TLD '{tld_filter}' failed in shard {shard_num}: {e}")
            
            con.close()
            
            if all_results:
                combined_df = pd.concat(all_results, ignore_index=True)
                # Remove duplicates based on URL, keeping the first occurrence (lowest index)
                combined_df = combined_df.sort_values('index').drop_duplicates(subset=['url'], keep='first')
                self.logger.info(f"Shard {shard_num} total unique records: {len(combined_df)}")
                return combined_df
            else:
                self.logger.debug(f"No results found in shard {shard_num}")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Shard {shard_num} failed: {e}")
            return None

    def fetch_index_optimized(
        self,
        target_records: int = None,
        max_shards: int = None,
        max_workers: int = None,
        tld_filters: list = None,
    ) -> dict:
        """
        Highly optimized fetch for large-scale Common Crawl index data with parallel processing
        
        Args:
            target_records (int): Target number of records to fetch
            max_shards (int): Maximum number of shards to process
            max_workers (int): Number of parallel workers
            tld_filters (list): List of TLD filters to try
            
        Returns:
            dict: Summary of extraction results
        """
        # Set default values for parameters
        target_records = target_records or 200000
        max_shards = max_shards or 200
        max_workers = max_workers or 20
        self.logger.info(f"Using parameters: target_records={target_records}, max_shards={max_shards}, max_workers={max_workers}")
        
        if tld_filters is None:
            tld_filters = ['au', 'com.au', 'org.au', 'net.au', 'gov.au']
            
        self.logger.info(f"Starting highly optimized index fetch:")
        self.logger.info(f"  Target records: {target_records:,}")
        self.logger.info(f"  Max shards: {max_shards}")
        self.logger.info(f"  Max workers: {max_workers}")
        self.logger.info(f"  TLD filters: {tld_filters}")
        self.logger.info(f"  Batch size: {self.batch_size:,}")
        
        start_time = time.time()
        
        try:
            paths = self._fetch_manifest_paths()
            parquet_files = [self.base_url + p for p in paths]
            self.logger.info(f"Generated {len(parquet_files)} parquet file URLs")

            all_records = []
            batch_num = 0
            total_records = 0
            processed_shards = 0
            successful_shards = 0
            
            # Process shards in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all shards for processing
                futures = []
                for i, shard_url in enumerate(parquet_files[:max_shards]):
                    future = executor.submit(self._process_single_shard, shard_url, tld_filters, i+1)
                    futures.append(future)
                
                # Collect results as they complete
                for future in as_completed(futures):
                    processed_shards += 1
                    
                    if total_records >= target_records:
                        self.logger.info(f"Reached target records: {target_records:,}")
                        break
                    
                    try:
                        result_df = future.result()
                        if result_df is not None and not result_df.empty:
                            successful_shards += 1
                            all_records.append(result_df)
                            total_records += len(result_df)
                            
                            self.logger.info(f"Progress: {processed_shards}/{max_shards} shards, "
                                           f"{successful_shards} successful, {total_records:,} total records")
                            
                            # Write batch when we have enough records
                            if total_records >= self.batch_size:
                                combined_df = pd.concat(all_records, ignore_index=True)
                                # Ensure index column is properly maintained with crawl version
                                if 'index' not in combined_df.columns:
                                    combined_df['index'] = self.crawl
                                
                                # Split into batches of batch_size
                                for i in range(0, len(combined_df), self.batch_size):
                                    batch_df = combined_df.iloc[i:i + self.batch_size]
                                    self._write_parquet_batch(batch_df, batch_num)
                                    batch_num += 1
                                
                                # Reset for next batch
                                all_records = []
                                total_records = 0
                                
                    except Exception as e:
                        self.logger.error(f"Error processing shard result: {e}")

            # Write remaining records
            if all_records:
                combined_df = pd.concat(all_records, ignore_index=True)
                # Ensure index column is properly maintained with crawl version
                if 'index' not in combined_df.columns:
                    combined_df['index'] = self.crawl
                
                for i in range(0, len(combined_df), self.batch_size):
                    batch_df = combined_df.iloc[i:i + self.batch_size]
                    self._write_parquet_batch(batch_df, batch_num)
                    batch_num += 1

            self.logger.info(f"Processed {processed_shards} shards, {successful_shards} successful")
            self.logger.info(f"Total batches written: {batch_num}")

            # Final summary
            elapsed = time.time() - start_time
            rate = (batch_num * self.batch_size) / elapsed if elapsed > 0 else 0
            
            self.logger.info("=== EXTRACTION COMPLETE ===")
            self.logger.info(f"Total shards processed: {processed_shards}")
            self.logger.info(f"Successful shards: {successful_shards}")
            self.logger.info(f"Total batches written: {batch_num}")
            self.logger.info(f"Estimated total records: {batch_num * self.batch_size:,}")
            self.logger.info(f"Total time: {elapsed:.1f} seconds")
            self.logger.info(f"Processing rate: {rate:.1f} records/second")
            self.logger.info(f"Output directory: {self.dump_dir}")
            
            # Save progress
            progress = {
                "total_shards_processed": processed_shards,
                "successful_shards": successful_shards,
                "total_batches": batch_num,
                "estimated_records": batch_num * self.batch_size,
                "processing_time": elapsed,
                "rate": rate,
                "timestamp": time.time(),
                "target_records": target_records,
                "tld_filters_used": tld_filters
            }
            
            # Save progress in the daily folder
            daily_folder = self._get_daily_folder_path()
            progress_file = os.path.join(daily_folder, "progress.json")
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
            
            self.logger.info(f"Progress saved to: {progress_file}")
            
            return progress
            
        except Exception as e:
            self.logger.error(f"Error in fetch_index_optimized: {e}")
            raise


if __name__ == "__main__":
    crawl = os.environ.get("COMMONCRAWL_CRAWL", "CC-MAIN-2025-38")
    base = os.environ.get("COMMONCRAWL_BASE", "https://data.commoncrawl.org/")

    # Initialize with optimized settings
    extractor = CommonCrawlExtractor(
        crawl=crawl, 
        base_url=base,
        dump_dir="dump/commoncrawl_dump",
        batch_size=10000
    )
    
    # Run highly optimized extraction with parallel processing and multiple TLDs
    try:
        results = extractor.fetch_index_optimized(
            target_records=400000,
            max_shards=300,
            max_workers=20,
            tld_filters=['au', 'com.au', 'org.au', 'net.au']
        )
        print(f"\nExtraction completed successfully!")
        print(f"Total batches: {results['total_batches']}")
        print(f"Estimated records: {results['estimated_records']:,}")
        print(f"Processing time: {results['processing_time']:.1f} seconds")
        print(f"Processing rate: {results['rate']:.1f} records/second")
    except Exception as e:
        print(f"Extraction failed: {e}")
        raise