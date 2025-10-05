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
            
            if truncate_first:
                with self.conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE australian_business_registry_data RESTART IDENTITY")
                    self.conn.commit()
                    logger.info("ABR table truncated successfully")
            
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
            
            # Insert in batches
            total_inserted = self._insert_abr_batches(records, batch_size)
            logger.info(f"Successfully loaded {total_inserted} ABR records")
            return total_inserted
            
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
            
            if truncate_first:
                with self.conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE commoncrawl_data RESTART IDENTITY")
                    self.conn.commit()
                    logger.info("CommonCrawl table truncated successfully")
            
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
            
            # Insert in batches
            total_inserted = self._insert_cc_batches(records, batch_size)
            logger.info(f"Successfully loaded {total_inserted} CommonCrawl records")
            return total_inserted
            
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
            
            if truncate_first:
                with self.conn.cursor() as cursor:
                    cursor.execute("TRUNCATE TABLE australian_companies_data RESTART IDENTITY")
                    # Reset match flags in source tables
                    cursor.execute("UPDATE australian_business_registry_data SET match_flag = 0")
                    cursor.execute("UPDATE commoncrawl_data SET match_flag = 0")
                    self.conn.commit()
                    logger.info("All tables truncated and match flags reset")
            
            # Load matched records and update match flags
            total_inserted = self._load_matched_records(df, batch_size)
            logger.info(f"Successfully loaded {total_inserted} matched records")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to load matched data: {str(e)}")
            raise
    
    def _insert_abr_batches(self, records: list, batch_size: int) -> int:
        """Insert ABR records in batches."""
        insert_sql = """
            INSERT INTO australian_business_registry_data (
                abn, entity_name, entity_type, entity_status, state, postcode,
                start_date, address, match_flag, created_at
            ) VALUES %s
        """
        return self._insert_batches(insert_sql, records, batch_size, "ABR")
    
    def _insert_cc_batches(self, records: list, batch_size: int) -> int:
        """Insert CommonCrawl records in batches."""
        insert_sql = """
            INSERT INTO commoncrawl_data (
                base_url, domain, cc_company_name, extracted_index, match_flag, created_at
            ) VALUES %s
        """
        return self._insert_batches(insert_sql, records, batch_size, "CommonCrawl")
    
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
                    logger.info(f"Inserted {table_name} batch {i//batch_size + 1}: {len(batch)} records")
                    
                except Exception as e:
                    logger.error(f"Failed to insert {table_name} batch {i//batch_size + 1}: {str(e)}")
                    self.conn.rollback()
                    raise
        
        return total_inserted
    
    def _load_matched_records(self, df: pd.DataFrame, batch_size: int) -> int:
        """Load matched records and update match flags in source tables."""
        total_inserted = 0
        
        with self.conn.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    # Insert into australian_companies_data
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
                    
                    # Update ABR table match flag
                    cursor.execute("""
                        UPDATE australian_business_registry_data 
                        SET match_flag = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE abn = %s
                    """, (row.get('ABN', ''),))
                    
                    # Update CommonCrawl table match flag
                    cursor.execute("""
                        UPDATE commoncrawl_data 
                        SET match_flag = 1, updated_at = CURRENT_TIMESTAMP
                        WHERE domain = %s AND cc_company_name = %s
                    """, (row.get('domain', ''), row.get('cc_company_name', '')))
                    
                    total_inserted += 1
                    
                    if total_inserted % batch_size == 0:
                        self.conn.commit()
                        logger.info(f"Processed {total_inserted} matched records")
                
                except Exception as e:
                    logger.error(f"Failed to process matched record: {str(e)}")
                    self.conn.rollback()
                    raise
            
            # Final commit
            self.conn.commit()
        
        return total_inserted
    
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
    
    # File paths (use files with updated match flags)
    abr_parquet_path = "entity_matching/entity_matches/parquets/abr_final.parquet"
    cc_parquet_path = "entity_matching/entity_matches/parquets/commoncrawl_final.parquet"
    matched_parquet_path = "entity_matching/entity_matches/parquets/matched_entity_matches.parquet"
    
    # Initialize loader
    loader = AllTablesLoader(connection_params)
    
    try:
        # Connect to database
        loader.connect()
        
        # Create all tables
        loader.create_all_tables()
        
        # Load ABR data
        abr_records = loader.load_abr_data(abr_parquet_path, truncate_first=True)
        
        # Load CommonCrawl data
        cc_records = loader.load_cc_data(cc_parquet_path, truncate_first=True)
        
        # Load matched data (this will also update match flags)
        matched_records = loader.load_matched_data(matched_parquet_path, truncate_first=True)
        
        # Get and print summary
        summary = loader.get_load_summary()
        
        print("\n" + "="*80)
        print("POSTGRESQL DATA LOADING SUMMARY")
        print("="*80)
        print(f"ABR Records Loaded: {summary['abr_table']['total_records']:,}")
        print(f"  - Matched: {summary['abr_table']['matched_records']:,}")
        print(f"  - Unmatched: {summary['abr_table']['unmatched_records']:,}")
        print(f"CommonCrawl Records Loaded: {summary['commoncrawl_table']['total_records']:,}")
        print(f"  - Matched: {summary['commoncrawl_table']['matched_records']:,}")
        print(f"  - Unmatched: {summary['commoncrawl_table']['unmatched_records']:,}")
        print(f"Australian Companies Data: {summary['australian_companies_data']['total_records']:,}")
        print(f"Match Type Distribution: {summary['australian_companies_data']['match_type_distribution']}")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        raise
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
