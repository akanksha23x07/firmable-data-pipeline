"""
ABR Data Transformation Script
This script processes ABR parquet files with data cleaning, normalization, quality tests, and duplicate handling.
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/abr_transformation_logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ABRTransformer:
    """Class for transforming ABR data."""
    
    def __init__(self, input_path: str, output_path: str):
        """
        Initialize the ABR transformer.
        
        Args:
            input_path: Path to ABR dump directory
            output_path: Path for output files
        """
        self.input_path = input_path
        self.output_path = output_path
        self.conn = duckdb.connect()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logger.info("ABRTransformer initialized")
    
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
        """Load all ABR parquet files from daily folders or root directory."""
        logger.info("Loading ABR data...")
        
        # Get all parquet files from daily folders or root directory
        parquet_files = self._get_parquet_files_from_daily_folders()
        logger.info(f"Found {len(parquet_files)} ABR parquet files")
        
        if not parquet_files:
            raise ValueError("No ABR parquet files found")
        
        if test_mode:
            # For testing, load only the first parquet file
            test_file = parquet_files[0]
            logger.info(f"TEST MODE: Loading only {test_file}")
            abr_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{test_file}')
            """).fetchdf()
        else:
            # Load all ABR files using the list of file paths
            file_list_str = "', '".join(parquet_files)
            abr_data = self.conn.execute(f"""
                SELECT * FROM read_parquet(['{file_list_str}'])
            """).fetchdf()
        
        logger.info(f"Loaded {len(abr_data)} ABR records")
        return abr_data
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize ABR data."""
        logger.info("Cleaning ABR data...")
        
        df_clean = df.copy()
        initial_count = len(df_clean)
        
        # 1. Clean ABN - remove non-numeric characters and validate format
        logger.info("Cleaning ABN field...")
        df_clean['ABN'] = df_clean['ABN'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df_clean = df_clean[df_clean['ABN'].str.len() == 11]  # Valid ABN should be 11 digits
        
        # 2. Clean EntityName - remove extra whitespace, standardize case
        logger.info("Cleaning EntityName field...")
        df_clean['EntityName'] = df_clean['EntityName'].str.strip()
        
        # 3. Clean EntityType - standardize values
        logger.info("Cleaning EntityType field...")
        entity_type_mapping = {
            'Australian Public Company': 'Public Company',
            'Australian Private Company': 'Private Company',
            'Public Company': 'Public Company',
            'Private Company': 'Private Company'
        }
        df_clean['EntityType'] = df_clean['EntityType'].map(entity_type_mapping).fillna(df_clean['EntityType'])
        
        # 4. Clean EntityStatus - standardize values
        logger.info("Cleaning EntityStatus field...")
        status_mapping = {
            'ACT': 'Active',
            'CAN': 'Cancelled',
            'Active': 'Active',
            'Cancelled': 'Cancelled'
        }
        df_clean['EntityStatus'] = df_clean['EntityStatus'].map(status_mapping).fillna(df_clean['EntityStatus'])
        
        # 5. Clean Postcode - ensure 4 digits
        logger.info("Cleaning Postcode field...")
        df_clean['Postcode'] = df_clean['Postcode'].astype(str).str.zfill(4)
        df_clean = df_clean[df_clean['Postcode'].str.len() == 4]
        
        # 6. Clean State - standardize state codes
        logger.info("Cleaning State field...")
        state_mapping = {
            'NSW': 'NSW', 'VIC': 'VIC', 'QLD': 'QLD', 'WA': 'WA',
            'SA': 'SA', 'TAS': 'TAS', 'ACT': 'ACT', 'NT': 'NT'
        }
        df_clean['State'] = df_clean['State'].str.upper()
        df_clean = df_clean[df_clean['State'].isin(state_mapping.keys())]
        
        # 7. Clean StartDate - convert to proper date format
        logger.info("Cleaning StartDate field...")
        df_clean['StartDate'] = pd.to_datetime(df_clean['StartDate'], format='%Y%m%d', errors='coerce')
        df_clean = df_clean.dropna(subset=['StartDate'])
        
        # 8. Extract abr_company_name - lowercase, remove ltd/pty
        logger.info("Extracting abr_company_name...")
        df_clean['abr_company_name'] = df_clean['EntityName'].apply(self._extract_company_name)
        df_clean = df_clean.dropna(subset=['abr_company_name'])
        
        logger.info(f"ABR data cleaned: {len(df_clean)} records remaining (removed {initial_count - len(df_clean)} records)")
        
        # Print sample of processed records for testing
        if len(df_clean) > 0:
            logger.info("Sample of 20 processed ABR records:")
            sample_records = df_clean.head(20)[['ABN', 'EntityName', 'abr_company_name', 'EntityType', 'EntityStatus']]
            for idx, row in sample_records.iterrows():
                logger.info(f"  {idx+1}. ABN: {row['ABN']}, Entity: {row['EntityName']}, Clean: {row['abr_company_name']}, Type: {row['EntityType']}, Status: {row['EntityStatus']}")
        
        return df_clean
    
    def _extract_company_name(self, entity_name: str) -> str:
        """Extract clean company name from EntityName."""
        if not entity_name or pd.isna(entity_name):
            return None
        
        # Convert to lowercase
        name = entity_name.lower()
        
        # Remove common suffixes
        suffixes_to_remove = [
            'ltd', 'limited', 'pty', 'proprietary', 'pty ltd', 'proprietary limited',
            'inc', 'incorporated', 'corp', 'corporation', 'co', 'company',
            'pvt', 'private', 'public', 'group', 'holdings', 'holding',
            'international', 'intl', 'australia', 'au', 'australian'
        ]
        
        for suffix in suffixes_to_remove:
            # Remove suffix with various separators
            name = re.sub(rf'\b{suffix}\b', '', name)
            name = re.sub(rf'\({suffix}\)', '', name)
        
        # Clean up extra spaces and punctuation
        name = re.sub(r'[^\w\s]', ' ', name)  # Remove special characters except spaces
        name = re.sub(r'\s+', ' ', name)      # Replace multiple spaces with single space
        name = name.strip()                   # Remove leading/trailing spaces
        
        # Remove all spaces from the final name
        name = name.replace(' ', '')
        
        # Return None if name is too short or empty
        if len(name) < 2:
            return None
        
        return name
    
    
    def handle_duplicates_and_inconsistencies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicates and inconsistencies in ABR data."""
        logger.info("Handling ABR duplicates and inconsistencies...")
        
        initial_count = len(df)
        
        # Handle duplicate ABNs
        logger.info(f"ABR duplicates before: {df['ABN'].duplicated().sum()}")
        df_dedup = df.drop_duplicates(subset=['ABN'], keep='first')
        logger.info(f"ABR duplicates after: {df_dedup['ABN'].duplicated().sum()}")
        
        # Handle inconsistencies in ABR data
        # Group by ABN and resolve conflicts by taking the most recent or most complete record
        logger.info("Resolving ABR data inconsistencies...")
        df_clean = df_dedup.groupby('ABN').apply(self._resolve_abr_conflicts).reset_index(drop=True)
        
        logger.info(f"ABR data after handling duplicates/inconsistencies: {len(df_clean)} records (removed {initial_count - len(df_clean)} records)")
        return df_clean
    
    def _resolve_abr_conflicts(self, group: pd.DataFrame) -> pd.DataFrame:
        """Resolve conflicts in ABR data for the same ABN."""
        if len(group) == 1:
            return group
        
        # Priority: Active > Cancelled, Most recent StartDate, Most complete data
        group = group.sort_values([
            'EntityStatus',  # Active first
            'StartDate',     # Most recent first
            'EntityName'     # Most complete name
        ], ascending=[False, False, True])
        
        return group.iloc[[0]]  # Return first (best) record
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns to the dataset."""
        logger.info("Adding metadata columns...")
        
        df_meta = df.copy()
        
        # Add processing timestamp
        df_meta['processed_at'] = datetime.now()
        
        # Add address column combining state and postcode
        df_meta['address'] = df_meta.apply(
            lambda row: f"{self._get_full_state_name(row['State'])}, {row['Postcode']}",
            axis=1
        )
        
        # Add match_flag column (default to unmatched)
        df_meta['match_flag'] = 0
        
        return df_meta
    
    def _get_full_state_name(self, state_code: str) -> str:
        """Convert state code to full state name."""
        state_mapping = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria',
            'QLD': 'Queensland',
            'WA': 'Western Australia',
            'SA': 'South Australia',
            'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory',
            'NT': 'Northern Territory'
        }
        return state_mapping.get(state_code, state_code)
    
    def save_data(self, df: pd.DataFrame, test_mode: bool = False, processing_time: float = None):
        """Save transformed data and summary in daywise folders."""
        logger.info("Saving ABR transformed data...")
        
        # Get daily output folder
        daily_folder = self._get_daily_output_folder()
        
        # Create timestamp for unique file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
        
        # Save main data using DuckDB with timestamp
        output_file = os.path.join(daily_folder, f'abr_transformed_{timestamp}.parquet')
        self.conn.register('df_temp', df)
        self.conn.execute(f"""
            COPY (SELECT * FROM df_temp) TO '{output_file}' (FORMAT PARQUET)
        """)
        logger.info(f"ABR data saved to {output_file}")
        
        # Save sample CSV for testing (always generate)
        sample_df = df.head(20)
        csv_file = os.path.join(daily_folder, f'abr_sample_20_records_{timestamp}.csv')
        sample_df.to_csv(csv_file, index=False)
        logger.info(f"ABR sample data (20 records) saved to {csv_file}")
        
        # Save summary statistics
        summary = {
            'total_records': len(df),
            'unique_abns': df['ABN'].nunique(),
            'active_companies': len(df[df['EntityStatus'] == 'Active']),
            'cancelled_companies': len(df[df['EntityStatus'] == 'Cancelled']),
            'public_companies': len(df[df['EntityType'] == 'Public Company']),
            'private_companies': len(df[df['EntityType'] == 'Private Company']),
            'date_range': {
                'earliest_start_date': df['StartDate'].min().isoformat() if not df['StartDate'].empty else None,
                'latest_start_date': df['StartDate'].max().isoformat() if not df['StartDate'].empty else None
            },
            'processing_time_seconds': processing_time,
            'timestamp': datetime.now().isoformat()
        }
        
        summary_file = os.path.join(daily_folder, f'abr_summary_{timestamp}.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"ABR summary saved to {summary_file}")
    
    def run_transformation(self, test_mode: bool = False):
        """Run the complete ABR transformation pipeline."""
        start_time = datetime.now()
        logger.info("Starting ABR transformation pipeline...")
        
        try:
            # 1. Load data
            abr_data = self.load_data(test_mode=test_mode)
            
            # 2. Clean data
            abr_clean = self.clean_data(abr_data)
            
            # 3. Handle duplicates and inconsistencies
            abr_final = self.handle_duplicates_and_inconsistencies(abr_clean)
            
            # 4. Add metadata
            abr_final = self.add_metadata(abr_final)
            
            # 5. Calculate processing time
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            # 6. Save results
            self.save_data(abr_final, test_mode=test_mode, processing_time=processing_time)
            
            logger.info("ABR transformation pipeline completed successfully!")
            logger.info(f"ABR transformation took {processing_time:.2f} seconds to transform and clean {len(abr_final):,} records")
            
            # Print summary
            print("\n" + "="*50)
            print("ABR TRANSFORMATION SUMMARY")
            print("="*50)
            print(f"Total Records: {len(abr_final):,}")
            print(f"Unique ABNs: {abr_final['ABN'].nunique():,}")
            print(f"Active Companies: {len(abr_final[abr_final['EntityStatus'] == 'Active']):,}")
            print(f"Cancelled Companies: {len(abr_final[abr_final['EntityStatus'] == 'Cancelled']):,}")
            print(f"Processing Time: {processing_time:.2f} seconds")
            print("="*50)
            
        except Exception as e:
            logger.error(f"ABR transformation failed: {str(e)}")
            raise
        finally:
            self.conn.close()

def main():
    """Main function to run the ABR transformation."""
    # Configuration
    input_path = "dump/abr_dump"
    output_path = "transformations/output/abr_clean"
    test_mode = False  # Set to False for full processing
    
    # Run transformation
    transformer = ABRTransformer(input_path, output_path)
    transformer.run_transformation(test_mode=test_mode)

if __name__ == "__main__":
    main()
