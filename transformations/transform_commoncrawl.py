"""
CommonCrawl Data Transformation Script
This script processes CommonCrawl parquet files with data cleaning, normalization, quality tests, and duplicate handling.
"""

import duckdb
import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime
import os
import json
import hashlib
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/commoncrawl_transformation_logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CommonCrawlTransformer:
    """Class for transforming CommonCrawl data."""
    
    def __init__(self, input_path: str, output_path: str):
        """
        Initialize the CommonCrawl transformer.
        
        Args:
            input_path: Path to CommonCrawl dump directory
            output_path: Path for output files
        """
        self.input_path = input_path
        self.output_path = output_path
        self.conn = duckdb.connect()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logger.info("CommonCrawlTransformer initialized")
    
    def _get_parquet_files_from_daily_folders(self) -> List[str]:
        """Get parquet files from current day folder only, or all files if no daily folders exist."""
        parquet_files = []
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Check if input_path contains daily folders (YYYY-MM-DD format)
        if os.path.isdir(self.input_path):
            # First, look for current day folder
            current_day_folder = os.path.join(self.input_path, current_date)
            if os.path.isdir(current_day_folder):
                # Process only current day folder
                logger.info(f"Processing parquet files from current day folder: {current_date}")
                for file in os.listdir(current_day_folder):
                    if file.endswith('.parquet'):
                        parquet_files.append(os.path.join(current_day_folder, file))
            else:
                # If no current day folder, look for any daily folders
                daily_folders = []
                for item in os.listdir(self.input_path):
                    item_path = os.path.join(self.input_path, item)
                    if os.path.isdir(item_path) and re.match(r'\d{4}-\d{2}-\d{2}', item):
                        daily_folders.append(item)
                
                if daily_folders:
                    # Use the most recent daily folder
                    latest_folder = sorted(daily_folders)[-1]
                    latest_folder_path = os.path.join(self.input_path, latest_folder)
                    logger.info(f"No current day folder found. Using latest available folder: {latest_folder}")
                    for file in os.listdir(latest_folder_path):
                        if file.endswith('.parquet'):
                            parquet_files.append(os.path.join(latest_folder_path, file))
                else:
                    # No daily folders, look for direct parquet files (backward compatibility)
                    logger.info("No daily folders found. Looking for direct parquet files.")
                    for file in os.listdir(self.input_path):
                        if file.endswith('.parquet'):
                            parquet_files.append(os.path.join(self.input_path, file))
        
        return parquet_files
    
    def _get_daily_output_folder(self) -> str:
        """Get the current date folder path for organizing output by day"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        daily_folder = os.path.join(self.output_path, current_date)
        os.makedirs(daily_folder, exist_ok=True)
        return daily_folder
    
    def load_data(self, test_mode: bool = False) -> pd.DataFrame:
        """Load all CommonCrawl parquet files from daily folders or root directory."""
        logger.info("Loading CommonCrawl data...")
        
        # Get all parquet files from daily folders or root directory
        parquet_files = self._get_parquet_files_from_daily_folders()
        logger.info(f"Found {len(parquet_files)} CommonCrawl parquet files")
        
        if not parquet_files:
            raise ValueError("No CommonCrawl parquet files found")
        
        if test_mode:
            # For testing, load only the first parquet file
            test_file = parquet_files[0]
            logger.info(f"TEST MODE: Loading only {test_file}")
            cc_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{test_file}')
            """).fetchdf()
        else:
            # Load all CommonCrawl files using the list of file paths
            file_list_str = "', '".join(parquet_files)
            cc_data = self.conn.execute(f"""
                SELECT * FROM read_parquet(['{file_list_str}'])
            """).fetchdf()
        
        logger.info(f"Loaded {len(cc_data)} CommonCrawl records")
        return cc_data
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize CommonCrawl data."""
        logger.info("Cleaning CommonCrawl data...")
        
        df_clean = df.copy()
        initial_count = len(df_clean)
        
        # 1. Clean URLs - remove invalid URLs
        logger.info("Cleaning URL field...")
        df_clean = df_clean[df_clean['url'].str.startswith(('http://', 'https://'))]
        
        # 2. Extract base URL only (remove query parameters, fragments, paths)
        logger.info("Extracting base URLs...")
        df_clean['base_url'] = df_clean['url'].apply(self._extract_base_url)
        df_clean = df_clean.dropna(subset=['base_url'])
        
        # 3. Use url_host_registered_domain as domain (already cleaned)
        logger.info("Using url_host_registered_domain as domain...")
        df_clean['domain'] = df_clean['url_host_registered_domain']
        df_clean = df_clean.dropna(subset=['domain'])
        
        # 4. Rename index column to extracted_index (no transformation needed)
        if 'index' in df_clean.columns:
            logger.info("Renaming index column to extracted_index...")
            df_clean = df_clean.rename(columns={'index': 'extracted_index'})
        else:
            logger.info("No index column found, creating sequential extracted_index...")
            df_clean['extracted_index'] = range(len(df_clean))
        
        # 5. Clean domain names - remove www, standardize
        logger.info("Cleaning domain names...")
        df_clean['domain'] = df_clean['domain'].str.lower()
        df_clean['domain'] = df_clean['domain'].str.replace('www.', '', regex=False)
        
        # 6. Filter for Australian domains (.com.au, .org.au, .net.au, .edu.au, .gov.au)
        logger.info("Filtering for Australian domains...")
        australian_domains = df_clean['domain'].str.endswith(('.com.au', '.org.au', '.net.au', '.edu.au', '.gov.au'))
        df_clean = df_clean[australian_domains]
        
        # 7. Extract company name from domain
        logger.info("Extracting company names from domains...")
        df_clean['cc_company_name'] = df_clean['domain'].apply(self._extract_company_name)
        df_clean = df_clean.dropna(subset=['cc_company_name'])
        
        # 8. Remove records with very short company names (likely invalid)
        df_clean = df_clean[df_clean['cc_company_name'].str.len() >= 2]
        
        logger.info(f"CommonCrawl data cleaned: {len(df_clean)} records remaining (removed {initial_count - len(df_clean)} records)")
        
        # Print sample of processed records for testing
        if len(df_clean) > 0:
            logger.info("Sample of 20 processed CommonCrawl records:")
            sample_records = df_clean.head(20)[['url', 'base_url', 'domain', 'cc_company_name', 'extracted_index']]
            for idx, row in sample_records.iterrows():
                logger.info(f"  {idx+1}. URL: {row['url'][:50]}..., Base: {row['base_url']}, Domain: {row['domain']}, Company: {row['cc_company_name']}, Index: {row['extracted_index']}")
        
        return df_clean
    
    def _extract_base_url(self, url: str) -> str:
        """Extract base URL (protocol + domain) from full URL."""
        try:
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
            return None
        except:
            return None
    
    def _extract_domain_name(self, url: str) -> str:
        """Extract domain name from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except:
            return None
    
    def _extract_company_name(self, domain: str) -> str:
        """Extract company name from domain."""
        if not domain:
            return None
        
        # Remove .com.au, .org.au, etc.
        name = re.sub(r'\.(com|org|net|edu|gov)\.au$', '', domain)
        
        # Remove common prefixes/suffixes
        name = re.sub(r'^(www\.|m\.|mobile\.)', '', name)
        
        # Replace hyphens and underscores with spaces
        name = name.replace('-', ' ').replace('_', ' ')
        
        # Clean up multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Convert to lowercase for consistency
        name = name.lower()
        
        # Remove common words that don't add value
        common_words = ['the', 'and', 'or', 'of', 'in', 'at', 'to', 'for', 'with', 'by']
        words = name.split()
        words = [word for word in words if word not in common_words]
        name = ' '.join(words)
        
        return name if name else None
    
    
    def handle_duplicates_and_inconsistencies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicates and inconsistencies in CommonCrawl data."""
        logger.info("Handling CommonCrawl duplicates and inconsistencies...")
        
        initial_count = len(df)
        
        # Handle duplicate URLs (same URL, different records)
        logger.info(f"CommonCrawl URL duplicates before: {df['url'].duplicated().sum()}")
        df_dedup = df.drop_duplicates(subset=['url'], keep='first')
        logger.info(f"CommonCrawl URL duplicates after: {df_dedup['url'].duplicated().sum()}")
        
        # Handle domain duplicates (same domain, different URLs)
        logger.info(f"CommonCrawl domain duplicates before: {df_dedup['domain'].duplicated().sum()}")
        df_clean = df_dedup.drop_duplicates(subset=['domain'], keep='first')
        logger.info(f"CommonCrawl domain duplicates after: {df_clean['domain'].duplicated().sum()}")
        
        # Handle base URL duplicates (same base URL, different full URLs)
        logger.info(f"CommonCrawl base URL duplicates before: {df_clean['base_url'].duplicated().sum()}")
        df_final = df_clean.drop_duplicates(subset=['base_url'], keep='first')
        logger.info(f"CommonCrawl base URL duplicates after: {df_final['base_url'].duplicated().sum()}")
        
        logger.info(f"CommonCrawl data after handling duplicates/inconsistencies: {len(df_final)} records (removed {initial_count - len(df_final)} records)")
        return df_final
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns to the dataset."""
        logger.info("Adding metadata columns...")
        
        df_meta = df.copy()
        
        # Add processing timestamp
        df_meta['processed_at'] = datetime.now()
        
        # Add match_flag column (default to unmatched)
        df_meta['match_flag'] = 0
        
        # Remove url_host_registered_domain column since we're using domain
        if 'url_host_registered_domain' in df_meta.columns:
            df_meta = df_meta.drop(columns=['url_host_registered_domain'])
        
        return df_meta
    
    def save_data(self, df: pd.DataFrame, test_mode: bool = False, processing_time: float = None):
        """Save transformed data and summary in daywise folders."""
        logger.info("Saving CommonCrawl transformed data...")
        
        # Get daily output folder
        daily_folder = self._get_daily_output_folder()
        
        # Create timestamp for unique file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
        
        # Save main data using DuckDB with timestamp
        output_file = os.path.join(daily_folder, f'commoncrawl_transformed_{timestamp}.parquet')
        self.conn.register('df_temp', df)
        self.conn.execute(f"""
            COPY (SELECT * FROM df_temp) TO '{output_file}' (FORMAT PARQUET)
        """)
        logger.info(f"CommonCrawl data saved to {output_file}")
        
        # Save sample CSV for testing (always generate)
        sample_df = df.head(20)
        csv_file = os.path.join(daily_folder, f'commoncrawl_sample_20_records_{timestamp}.csv')
        sample_df.to_csv(csv_file, index=False)
        logger.info(f"CommonCrawl sample data (20 records) saved to {csv_file}")
        
        # Save summary statistics
        summary = {
            'total_records': len(df),
            'unique_domains': df['domain'].nunique(),
            'unique_base_urls': df['base_url'].nunique(),
            'unique_company_names': df['cc_company_name'].nunique(),
            'processing_time_seconds': processing_time,
            'timestamp': datetime.now().isoformat(),
            'top_domains': df['domain'].value_counts().head(10).to_dict(),
            'top_company_names': df['cc_company_name'].value_counts().head(10).to_dict()
        }
        
        summary_file = os.path.join(daily_folder, f'commoncrawl_summary_{timestamp}.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"CommonCrawl summary saved to {summary_file}")
    
    def run_transformation(self, test_mode: bool = False):
        """Run the complete CommonCrawl transformation pipeline."""
        start_time = datetime.now()
        logger.info("Starting CommonCrawl transformation pipeline...")
        
        try:
            # 1. Load data
            cc_data = self.load_data(test_mode=test_mode)
            
            # 2. Clean data
            cc_clean = self.clean_data(cc_data)
            
            # 3. Handle duplicates and inconsistencies
            cc_final = self.handle_duplicates_and_inconsistencies(cc_clean)
            
            # 4. Add metadata
            cc_final = self.add_metadata(cc_final)
            
            # 5. Calculate processing time
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            # 6. Save results
            self.save_data(cc_final, test_mode=test_mode, processing_time=processing_time)
            
            logger.info("CommonCrawl transformation pipeline completed successfully!")
            logger.info(f"CommonCrawl transformation took {processing_time:.2f} seconds to transform and clean {len(cc_final):,} records")
            
            # Print summary
            print("\n" + "="*50)
            print("COMMONCRAWL TRANSFORMATION SUMMARY")
            print("="*50)
            print(f"Total Records: {len(cc_final):,}")
            print(f"Unique Domains: {cc_final['domain'].nunique():,}")
            print(f"Unique Company Names: {cc_final['cc_company_name'].nunique():,}")
            print(f"Processing Time: {processing_time:.2f} seconds")
            print("="*50)
            
        except Exception as e:
            logger.error(f"CommonCrawl transformation failed: {str(e)}")
            raise
        finally:
            self.conn.close()

def main():
    """Main function to run the CommonCrawl transformation."""
    # Configuration
    input_path = "dump/commoncrawl_dump"
    output_path = "transformations/output/commoncrawl_clean"
    test_mode = False  # Set to True to generate sample CSV
    
    # Run transformation
    transformer = CommonCrawlTransformer(input_path, output_path)
    transformer.run_transformation(test_mode=test_mode)

if __name__ == "__main__":
    main()
