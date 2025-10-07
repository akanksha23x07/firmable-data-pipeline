"""
PostgreSQL Data Loading Script for All Tables
This script loads data into three tables:
1. australian_business_registry_data (all ABR data with match flags)
2. commoncrawl_data (all CommonCrawl data with match flags)
3. australian_companies_data (matched records only)
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import os
from typing import Optional, Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/postgres_load_all.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AllTablesLoader:
    """Class for loading data into all three PostgreSQL tables."""
    
    def __init__(self, connection_params: dict):
        """
        Initialize the loader with PostgreSQL connection parameters.
        
        Args:
            connection_params: Dictionary with PostgreSQL connection details
        """
        self.connection_params = connection_params
        self.conn = None
        
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
    
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def disconnect(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def create_all_tables(self):
        """Create all three tables if they don't exist."""
        try:
            # Get the directory where this script is located
            script_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Create ABR table
            abr_sql_path = os.path.join(script_dir, 'create_australian_business_registry_table.sql')
            if not os.path.exists(abr_sql_path):
                raise FileNotFoundError(f"ABR SQL file not found: {abr_sql_path}")
            with open(abr_sql_path, 'r') as f:
                abr_ddl = f.read()
            
            # Create CommonCrawl table
            cc_sql_path = os.path.join(script_dir, 'create_common_crawl_table.sql')
            if not os.path.exists(cc_sql_path):
                raise FileNotFoundError(f"CommonCrawl SQL file not found: {cc_sql_path}")
            with open(cc_sql_path, 'r') as f:
                cc_ddl = f.read()
            
            # Create Australian Companies Data table
            acd_sql_path = os.path.join(script_dir, 'create_australian_companies_data_table.sql')
            if not os.path.exists(acd_sql_path):
                raise FileNotFoundError(f"Australian Companies Data SQL file not found: {acd_sql_path}")
            with open(acd_sql_path, 'r') as f:
                acd_ddl = f.read()
            
            with self.conn.cursor() as cursor:
                cursor.execute(abr_ddl)
                cursor.execute(cc_ddl)
                cursor.execute(acd_ddl)
                self.conn.commit()
                logger.info("All tables created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise
    
    def load_abr_data(self, abr_parquet_path: str, batch_size: int = 1000, 
                      truncate_first: bool = False) -> int:
        """Load ABR data into australian_business_registry_data table."""
        try:
            logger.info(f"Loading ABR data from {abr_parquet_path}")
            df = pd.read_parquet(abr_parquet_path)
            logger.info(f"Loaded {len(df)} ABR records from parquet file")
            
            # Note: truncate_first is ignored to preserve existing data
            logger.info("Preserving existing ABR data - using UPSERT logic")
            
            # Prepare records (all start with match_flag = 0)
            records = []
            for _, row in df.iterrows():
                record = (
                    row.get('ABN', ''),
                    row.get('EntityName', ''),
                    row.get('EntityType', ''),
                    row.get('EntityStatus', ''),
                    row.get('State', ''),
                    row.get('Postcode', ''),
                    row.get('StartDate', None),
                    row.get('address', ''),
                    0,  # match_flag = 0 (unmatched initially)
                    datetime.now()  # created_at
                )
                records.append(record)
            
            # Insert/Update in batches using UPSERT
            total_processed = self._insert_abr_batches(records, batch_size)
            logger.info(f"Successfully processed {total_processed} ABR records (inserted/updated)")
            return total_processed
            
        except Exception as e:
            logger.error(f"Failed to load ABR data: {str(e)}")
            raise
    
    def load_cc_data(self, cc_parquet_path: str, batch_size: int = 1000, 
                     truncate_first: bool = False) -> int:
        """Load CommonCrawl data into commoncrawl_data table."""
        try:
            logger.info(f"Loading CommonCrawl data from {cc_parquet_path}")
            df = pd.read_parquet(cc_parquet_path)
            logger.info(f"Loaded {len(df)} CommonCrawl records from parquet file")
            
            # Note: truncate_first is ignored to preserve existing data
            logger.info("Preserving existing CommonCrawl data - using UPSERT logic")
            
            # Prepare records (all start with match_flag = 0)
            records = []
            for _, row in df.iterrows():
                record = (
                    row.get('base_url', ''),
                    row.get('domain', ''),
                    row.get('cc_company_name', ''),
                    row.get('extracted_index', None),  # extracted_index
                    0,  # match_flag = 0 (unmatched initially)
                    datetime.now()  # created_at
                )
                records.append(record)
            
            # Insert/Update in batches using UPSERT
            total_processed = self._insert_cc_batches(records, batch_size)
            logger.info(f"Successfully processed {total_processed} CommonCrawl records (inserted/updated)")
            return total_processed
            
        except Exception as e:
            logger.error(f"Failed to load CommonCrawl data: {str(e)}")
            raise
    
    def load_matched_data(self, matched_parquet_path: str, batch_size: int = 1000, 
                          truncate_first: bool = False) -> int:
        """Load matched data into australian_companies_data table and update match flags."""
        try:
            logger.info(f"Loading matched data from {matched_parquet_path}")
            df = pd.read_parquet(matched_parquet_path)
            logger.info(f"Loaded {len(df)} matched records from parquet file")
            
            # Note: truncate_first is ignored to preserve existing data
            logger.info("Preserving existing matched data - using UPSERT logic")
            
            # Load matched records and update match flags using UPSERT
            total_processed = self._load_matched_records(df, batch_size)
            logger.info(f"Successfully processed {total_processed} matched records (inserted/updated)")
            return total_processed
            
        except Exception as e:
            logger.error(f"Failed to load matched data: {str(e)}")
            raise
    
    def _insert_abr_batches(self, records: list, batch_size: int) -> int:
        """Insert ABR records in batches using UPSERT logic."""
        upsert_sql = """
            INSERT INTO australian_business_registry_data (
                abn, entity_name, entity_type, entity_status, state, postcode,
                start_date, address, match_flag, created_at
            ) VALUES %s
            ON CONFLICT (abn) 
            DO UPDATE SET 
                entity_name = EXCLUDED.entity_name,
                entity_type = EXCLUDED.entity_type,
                entity_status = EXCLUDED.entity_status,
                state = EXCLUDED.state,
                postcode = EXCLUDED.postcode,
                start_date = EXCLUDED.start_date,
                address = EXCLUDED.address,
                updated_at = CURRENT_TIMESTAMP
        """
        return self._insert_batches(upsert_sql, records, batch_size, "ABR")
    
    def _insert_cc_batches(self, records: list, batch_size: int) -> int:
        """Insert CommonCrawl records in batches with duplicate checking."""
        total_processed = 0
        
        with self.conn.cursor() as cursor:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    # Process each record individually to handle duplicates
                    for record in batch:
                        base_url, domain, cc_company_name, extracted_index, match_flag, created_at = record
                        
                        # Check if record already exists using domain + cc_company_name
                        cursor.execute("""
                            SELECT id FROM commoncrawl_data 
                            WHERE domain = %s AND cc_company_name = %s
                        """, (domain, cc_company_name))
                        
                        existing = cursor.fetchone()
                        
                        if existing:
                            # Update existing record
                            cursor.execute("""
                                UPDATE commoncrawl_data 
                                SET base_url = %s, extracted_index = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE domain = %s AND cc_company_name = %s
                            """, (base_url, extracted_index, domain, cc_company_name))
                        else:
                            # Insert new record
                            cursor.execute("""
                                INSERT INTO commoncrawl_data (
                                    base_url, domain, cc_company_name, extracted_index, match_flag, created_at
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                            """, record)
                        
                        total_processed += 1
                    
                    self.conn.commit()
                    logger.info(f"Processed CommonCrawl batch {i//batch_size + 1}: {len(batch)} records (duplicate-safe)")
                    
                except Exception as e:
                    logger.error(f"Failed to process CommonCrawl batch {i//batch_size + 1}: {str(e)}")
                    self.conn.rollback()
                    raise
        
        return total_processed
    
    def _insert_batches(self, insert_sql: str, records: list, batch_size: int, table_name: str) -> int:
        """Generic batch insertion method."""
        total_inserted = 0
        
        with self.conn.cursor() as cursor:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    execute_values(
                        cursor, insert_sql, batch,
                        template=None, page_size=batch_size
                    )
                    self.conn.commit()
                    total_inserted += len(batch)
                    logger.info(f"Processed {table_name} batch {i//batch_size + 1}: {len(batch)} records (UPSERT)")
                    
                except Exception as e:
                    logger.error(f"Failed to insert {table_name} batch {i//batch_size + 1}: {str(e)}")
                    self.conn.rollback()
                    raise
        
        return total_inserted
    
    def _load_matched_records(self, df: pd.DataFrame, batch_size: int) -> int:
        """Load matched records using UPSERT logic and update match flags in source tables."""
        total_processed = 0
        
        with self.conn.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    # Check if matched record already exists using ABN + CC company name (more reliable than domain)
                    cursor.execute("""
                        SELECT id FROM australian_companies_data 
                        WHERE abn = %s AND cc_company_name = %s
                    """, (row.get('ABN', ''), row.get('cc_company_name', '')))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Update existing matched record
                        cursor.execute("""
                            UPDATE australian_companies_data 
                            SET entity_name = %s, entity_type = %s, entity_status = %s,
                                postcode = %s, state = %s, start_date = %s, address = %s,
                                base_url = %s, domain = %s, match_type = %s,
                                match_score = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE abn = %s AND cc_company_name = %s
                        """, (
                            row.get('EntityName', ''),
                            row.get('EntityType', ''),
                            row.get('EntityStatus', ''),
                            row.get('Postcode', ''),
                            row.get('State', ''),
                            row.get('StartDate', None),
                            row.get('address', ''),
                            row.get('base_url', ''),
                            row.get('domain', ''),
                            row.get('match_type', ''),
                            row.get('match_score', None),
                            row.get('ABN', ''),
                            row.get('cc_company_name', '')
                        ))
                        matched_id = existing[0]
                        is_inserted = False
                    else:
                        # Insert new matched record
                        cursor.execute("""
                            INSERT INTO australian_companies_data (
                                abn, entity_name, entity_type, entity_status, postcode,
                                state, start_date, address, base_url, domain,
                                cc_company_name, match_type, match_score, created_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                        """, (
                            row.get('ABN', ''),
                            row.get('EntityName', ''),
                            row.get('EntityType', ''),
                            row.get('EntityStatus', ''),
                            row.get('Postcode', ''),
                            row.get('State', ''),
                            row.get('StartDate', None),
                            row.get('address', ''),
                            row.get('base_url', ''),
                            row.get('domain', ''),
                            row.get('cc_company_name', ''),
                            row.get('match_type', ''),
                            row.get('match_score', None),
                            datetime.now()
                        ))
                        matched_id = cursor.fetchone()[0]
                        is_inserted = True
                    
                    # Update ABR table match flag (only if not already matched)
                    cursor.execute("""
                        UPDATE australian_business_registry_data 
                        SET match_flag = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE abn = %s AND match_flag = 0
                    """, (row.get('ABN', ''),))
                    
                    # Update CommonCrawl table match flag (only if not already matched)
                    cursor.execute("""
                        UPDATE commoncrawl_data 
                        SET match_flag = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE domain = %s AND cc_company_name = %s AND match_flag = 0
                    """, (row.get('domain', ''), row.get('cc_company_name', '')))
                    
                    total_processed += 1
                    
                    if total_processed % batch_size == 0:
                        self.conn.commit()
                        logger.info(f"Processed {total_processed} matched records")
                
                except Exception as e:
                    logger.error(f"Failed to process matched record: {str(e)}")
                    self.conn.rollback()
                    raise
            
            # Final commit
            self.conn.commit()
        
        return total_processed
    
    def get_load_summary(self) -> dict:
        """Get summary statistics of all loaded data."""
        try:
            with self.conn.cursor() as cursor:
                # ABR table stats
                cursor.execute("SELECT COUNT(*) FROM australian_business_registry_data")
                abr_total = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM australian_business_registry_data WHERE match_flag = 1")
                abr_matched = cursor.fetchone()[0]
                
                # CommonCrawl table stats
                cursor.execute("SELECT COUNT(*) FROM commoncrawl_data")
                cc_total = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM commoncrawl_data WHERE match_flag = 1")
                cc_matched = cursor.fetchone()[0]
                
                # Australian Companies Data stats
                cursor.execute("SELECT COUNT(*) FROM australian_companies_data")
                acd_total = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT match_type, COUNT(*) as count
                    FROM australian_companies_data
                    GROUP BY match_type
                """)
                match_type_dist = dict(cursor.fetchall())
                
                summary = {
                    'abr_table': {
                        'total_records': abr_total,
                        'matched_records': abr_matched,
                        'unmatched_records': abr_total - abr_matched
                    },
                    'commoncrawl_table': {
                        'total_records': cc_total,
                        'matched_records': cc_matched,
                        'unmatched_records': cc_total - cc_matched
                    },
                    'australian_companies_data': {
                        'total_records': acd_total,
                        'match_type_distribution': match_type_dist
                    },
                    'timestamp': datetime.now().isoformat()
                }
                
                return summary
                
        except Exception as e:
            logger.error(f"Failed to get load summary: {str(e)}")
            raise

def main():
    """Main function to run the complete data loading process."""
    
    # PostgreSQL connection parameters
    connection_params = {
        'host': 'localhost',
        'database': 'firmable_db',
        'user': 'firmable',
        'password': 'firmable123',
        'port': 5432
    }
    
    # File paths - find the latest files from daily folders
    import glob
    from datetime import datetime
    
    # Find the latest ABR file
    abr_pattern = "entity_matching/entity_matches/*/abr_with_match_flags_*.parquet"
    abr_files = glob.glob(abr_pattern)
    if not abr_files:
        # Fallback to old structure
        abr_parquet_path = "entity_matching/entity_matches/parquets/abr_final.parquet"
    else:
        abr_parquet_path = max(abr_files, key=os.path.getmtime)
    
    # Find the latest CommonCrawl file
    cc_pattern = "entity_matching/entity_matches/*/commoncrawl_with_match_flags_*.parquet"
    cc_files = glob.glob(cc_pattern)
    if not cc_files:
        # Fallback to old structure
        cc_parquet_path = "entity_matching/entity_matches/parquets/commoncrawl_final.parquet"
    else:
        cc_parquet_path = max(cc_files, key=os.path.getmtime)
    
    # Find the latest matched file
    matched_pattern = "entity_matching/entity_matches/*/matched_entity_matches_*.parquet"
    matched_files = glob.glob(matched_pattern)
    if not matched_files:
        # Fallback to old structure
        matched_parquet_path = "entity_matching/entity_matches/parquets/matched_entity_matches.parquet"
    else:
        matched_parquet_path = max(matched_files, key=os.path.getmtime)
    
    print(f"Using ABR file: {abr_parquet_path}")
    print(f"Using CommonCrawl file: {cc_parquet_path}")
    print(f"Using matched file: {matched_parquet_path}")
    
    # Initialize loader
    loader = AllTablesLoader(connection_params)
    
    try:
        # Connect to database
        loader.connect()
        
        # Create all tables
        loader.create_all_tables()
        
        # Load ABR data (preserve existing data)
        abr_records = loader.load_abr_data(abr_parquet_path, truncate_first=False)
        
        # Load CommonCrawl data (preserve existing data)
        cc_records = loader.load_cc_data(cc_parquet_path, truncate_first=False)
        
        # Load matched data (preserve existing data, this will also update match flags)
        matched_records = loader.load_matched_data(matched_parquet_path, truncate_first=False)
        
        # Get and print summary
        summary = loader.get_load_summary()
        
        print("\n" + "="*80)
        print("POSTGRESQL DATA LOADING SUMMARY")
        print("="*80)
        print(f"ABR Records in Database: {summary['abr_table']['total_records']:,}")
        print(f"  - Matched: {summary['abr_table']['matched_records']:,}")
        print(f"  - Unmatched: {summary['abr_table']['unmatched_records']:,}")
        print(f"CommonCrawl Records in Database: {summary['commoncrawl_table']['total_records']:,}")
        print(f"  - Matched: {summary['commoncrawl_table']['matched_records']:,}")
        print(f"  - Unmatched: {summary['commoncrawl_table']['unmatched_records']:,}")
        print(f"Matched Companies in Database: {summary['australian_companies_data']['total_records']:,}")
        print(f"Match Type Distribution: {summary['australian_companies_data']['match_type_distribution']}")
        print("\nNote: Data preserved using UPSERT logic - no truncation performed")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        raise
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
