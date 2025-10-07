import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import io
import os
import tempfile
import json
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import xml.etree.ElementTree as ET
import pandas as pd
import fastparquet
from datetime import datetime


class ABRExtraction:
    """ABR (Australian Business Register) data extraction class"""
    
    def __init__(self, dump_dir="dump/abr_dump", records_per_xml=None, parquet_batch_size=None):
        """
        Initialize ABR extraction class
        
        Args:
            dump_dir (str): Directory to save Parquet files
            records_per_xml (int): Maximum records to extract per XML file
            parquet_batch_size (int): Batch size for Parquet writing
        """
        self.dump_dir = dump_dir
        
        # Set default values for parameters
        self.records_per_xml = records_per_xml or 10000
        self.parquet_batch_size = parquet_batch_size or 10000
        self.logger = self._setup_logging()
        self.logger.info(f"Using parameters: records_per_xml={self.records_per_xml}, parquet_batch_size={self.parquet_batch_size}")
        
        # Keep backward compatibility
        self.batch_size = self.parquet_batch_size
        
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
        
        self.logger.info(f"Initialized ABRExtraction with dump_dir: {self.dump_dir}")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler('logs/abr_extraction_logs.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(self.__class__.__name__)
    
    def _get_daily_folder_path(self):
        """Get the current date folder path for organizing data by day"""
        current_date = datetime.now().strftime("%Y-%m-%d")
        daily_folder = os.path.join(self.dump_dir, current_date)
        os.makedirs(daily_folder, exist_ok=True)
        return daily_folder
    
    def _pick_text(self, entity, paths):
        """Helper function to extract text from XML element using multiple paths"""
        for p in paths:
            el = entity.find(p)
            if el is not None and el.text:
                t = el.text.strip()
                if t:
                    self.logger.debug(f"Found text in path '{p}': {t[:50]}...")
                    return t
        self.logger.debug("No text found in any of the provided paths")
        return None
    
    def _extract_abr_record(self, entity):
        """Extract single ABR record from XML element"""
        self.logger.debug("Extracting ABR record from XML element")
        
        abn_el = entity.find("ABN")
        abn = abn_el.text.strip() if abn_el is not None and abn_el.text else None
        status = abn_el.get("status") if abn_el is not None else None
        start_date = abn_el.get("ABNStatusFromDate") if abn_el is not None else None
        
        entity_type = self._pick_text(entity, [
            "EntityType/EntityTypeText",
            "EntityType/EntityTypeDescription",
        ])
        
        name = self._pick_text(entity, [
            "MainEntity/NonIndividualName/NonIndividualNameText",
        ])
        if not name:
            given = self._pick_text(entity, ["LegalEntity/IndividualName/GivenName"]) or ""
            family = self._pick_text(entity, ["LegalEntity/IndividualName/FamilyName"]) or ""
            full = (given + " " + family).strip()
            name = full if full else None
        
        postcode = self._pick_text(entity, [
            "MainEntity/BusinessAddress/AddressDetails/Postcode",
            "LegalEntity/BusinessAddress/AddressDetails/Postcode",
        ])
        state = self._pick_text(entity, [
            "MainEntity/BusinessAddress/AddressDetails/State",
            "LegalEntity/BusinessAddress/AddressDetails/State",
        ])
        
        record = {
            "ABN": abn,
            "EntityName": name,
            "EntityType": entity_type,
            "EntityStatus": status,
            "Postcode": postcode,
            "State": state,
            "StartDate": start_date,
        }
        
        self.logger.debug(f"Extracted record for ABN: {abn}")
        return record
    
    def _write_parquet_batch(self, batch, xml_name, batch_num):
        """Write batch to Parquet file with daywise organization and timestamp naming"""
        if not batch:
            self.logger.debug("Empty batch, skipping write")
            return
        
        try:
            # Get daily folder path
            daily_folder = self._get_daily_folder_path()
            
            # Clean XML name for filename
            clean_name = xml_name.replace('.xml', '').replace('20251001_', '')
            
            # Create timestamp for unique file naming
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
            
            # Create parquet filename with timestamp
            parquet_file = os.path.join(daily_folder, f"{clean_name}_{timestamp}.parquet")
            
            self.logger.debug(f"Writing batch {batch_num} with {len(batch)} records to {parquet_file}")
            
            df = pd.DataFrame(batch)
            fastparquet.write(parquet_file, df, compression='snappy')
            
            self.logger.info(f"Successfully wrote {len(batch)} records to {parquet_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to write batch {batch_num}: {e}")
            raise
    
    def _stream_xml_to_parquet(self, zip_path: str, member_name: str):
        """Stream XML and write to Parquet files in batches"""
        self.logger.info(f"Starting XML processing: {member_name} (limit: {self.records_per_xml:,} records)")
        
        batch = []
        batch_num = 0
        records_processed = 0
        start_time = time.time()
        
        try:
            with zipfile.ZipFile(zip_path) as zf:
                self.logger.debug(f"Opened ZIP file: {zip_path}")
                with zf.open(member_name) as f:
                    self.logger.debug(f"Opened XML member: {member_name}")
                    context = ET.iterparse(f, events=("end",))
                    for event, elem in context:
                        if elem.tag == "ABR":
                            record = self._extract_abr_record(elem)
                            if record:
                                batch.append(record)
                                records_processed += 1
                            
                            elem.clear()
                            
                            # Check if we've hit the limit
                            if records_processed >= self.records_per_xml:
                                self.logger.info(f"Reached limit of {self.records_per_xml:,} records")
                                break
                            
                            # Write batch when full
                            if len(batch) >= self.parquet_batch_size:
                                self._write_parquet_batch(batch, member_name, batch_num)
                                batch_num += 1
                                batch = []
                                
                                # Progress update
                                elapsed = time.time() - start_time
                                rate = records_processed / elapsed if elapsed > 0 else 0
                                self.logger.info(f"Processed {records_processed} records ({rate:.1f} rec/sec)")
                    
                    # Write remaining records
                    if batch:
                        self._write_parquet_batch(batch, member_name, batch_num)
                        batch_num += 1
            
            elapsed = time.time() - start_time
            rate = records_processed / elapsed if elapsed > 0 else 0
            self.logger.info(f"Completed {member_name}: {records_processed} records in {elapsed:.1f}s ({rate:.1f} rec/sec)")
            return records_processed
            
        except Exception as e:
            self.logger.error(f"Error processing {member_name}: {e}")
            raise
    
    def _analyze_zip_contents(self, zip_path: str):
        """Analyze ZIP contents and return detailed information"""
        self.logger.info("Analyzing ZIP contents...")
        
        with zipfile.ZipFile(zip_path) as z:
            all_files = z.namelist()
            xml_files = [f for f in all_files if f.lower().endswith('.xml')]
            other_files = [f for f in all_files if not f.lower().endswith('.xml')]
            
            # Get file sizes
            xml_sizes = []
            for xml_file in xml_files:
                file_info = z.getinfo(xml_file)
                xml_sizes.append(file_info.file_size)
            
            total_xml_size = sum(xml_sizes)
            avg_xml_size = total_xml_size / len(xml_files) if xml_files else 0
            
            analysis = {
                'total_files': len(all_files),
                'xml_files': len(xml_files),
                'other_files': len(other_files),
                'xml_file_names': xml_files,
                'other_file_names': other_files,
                'total_xml_size_mb': round(total_xml_size / (1024 * 1024), 2),
                'avg_xml_size_mb': round(avg_xml_size / (1024 * 1024), 2)
            }
            
            # Log detailed analysis
            self.logger.info(f"ZIP Analysis Results:")
            self.logger.info(f"  Total files: {analysis['total_files']}")
            self.logger.info(f"  XML files: {analysis['xml_files']}")
            self.logger.info(f"  Other files: {analysis['other_files']}")
            self.logger.info(f"  Total XML size: {analysis['total_xml_size_mb']} MB")
            self.logger.info(f"  Average XML size: {analysis['avg_xml_size_mb']} MB")
            self.logger.info(f"  XML file names: {xml_files}")
            
            return analysis
    
    def _process_zip_completely(self, zip_url, max_records_per_zip=None):
        """Process entire ZIP file with all XMLs"""
        self.logger.info(f"Starting ZIP processing: {zip_url}")
        
        try:
            # Download ZIP
            self.logger.info("Downloading ZIP file...")
            resp = self.session.get(zip_url, stream=True, timeout=30)
            resp.raise_for_status()
            self.logger.info("ZIP download completed")
            
            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tf:
                for chunk in resp.iter_content(chunk_size=8192):
                    tf.write(chunk)
                zip_path = tf.name
            
            self.logger.debug(f"ZIP saved to temp file: {zip_path}")
            
            try:
                # Analyze ZIP contents first (reuse downloaded file)
                zip_analysis = self._analyze_zip_contents(zip_path)
                
                # Get all XML files
                with zipfile.ZipFile(zip_path) as z:
                    xml_entries = [n for n in z.namelist() if n.lower().endswith('.xml')]
                    
                    total_records = 0
                    for i, xml_name in enumerate(xml_entries):
                        if max_records_per_zip and total_records >= max_records_per_zip:
                            self.logger.info(f"Reached ZIP limit of {max_records_per_zip} records")
                            break
                        
                        self.logger.info(f"Processing XML {i+1}/{len(xml_entries)}: {xml_name}")
                        
                        records_count = self._stream_xml_to_parquet(zip_path, xml_name)
                        total_records += records_count
                        
                        if max_records_per_zip and total_records >= max_records_per_zip:
                            self.logger.info(f"Reached ZIP limit of {max_records_per_zip} records")
                            break
                
                self.logger.info(f"ZIP processing complete: {total_records} total records from {len(xml_entries)} XML files")
                return {
                    'total_records': total_records,
                    'xml_count': len(xml_entries),
                    'zip_analysis': zip_analysis
                }
                
            finally:
                # Clean up temp file
                try:
                    os.remove(zip_path)
                    self.logger.debug(f"Cleaned up temp file: {zip_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temp file: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error processing ZIP {zip_url}: {e}")
            raise
    
    def extract_from_urls(self, zip_urls, max_workers=2):
        """
        Extract ABR data from multiple ZIP URLs
        
        Args:
            zip_urls (list): List of ZIP file URLs
            max_workers (int): Maximum number of parallel workers
            
        Returns:
            dict: Summary of extraction results
        """
        self.logger.info("Starting ABR extraction process")
        self.logger.info(f"Total ZIP files: {len(zip_urls)}")
        self.logger.info(f"Records per XML (limited): {self.records_per_xml:,}")
        self.logger.info(f"Output directory: {self.dump_dir}")
        
        start_time = time.time()
        total_records = 0
        total_xmls = 0
        zip_results = []
        
        try:
            self.logger.info(f"Starting parallel processing with {max_workers} workers")
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Submit all ZIPs for processing
                futures = []
                for i, zip_url in enumerate(zip_urls):
                    self.logger.info(f"Submitting ZIP {i+1} for processing")
                    future = executor.submit(self._process_zip_completely, zip_url)
                    futures.append(future)
                
                # Collect results
                for i, future in enumerate(as_completed(futures)):
                    try:
                        result = future.result()
                        total_records += result['total_records']
                        total_xmls += result['xml_count']
                        zip_results.append(result)
                        self.logger.info(f"ZIP {i+1}/{len(zip_urls)} completed: {result['total_records']} records from {result['xml_count']} XML files")
                    except Exception as e:
                        self.logger.error(f"ZIP {i+1} failed: {e}")
            
            # Final summary
            elapsed = time.time() - start_time
            rate = total_records / elapsed if elapsed > 0 else 0
            
            self.logger.info("=== EXTRACTION COMPLETE ===")
            self.logger.info(f"Total ZIP files processed: {len(zip_urls)}")
            self.logger.info(f"Total XML files processed: {total_xmls}")
            self.logger.info(f"Total records processed: {total_records:,}")
            self.logger.info(f"Total time: {elapsed:.1f} seconds")
            self.logger.info(f"Processing rate: {rate:.1f} records/second")
            self.logger.info(f"Output directory: {self.dump_dir}")
            
            # Save progress
            progress = {
                "total_records": total_records,
                "total_xmls": total_xmls,
                "total_zips": len(zip_urls),
                "processing_time": elapsed,
                "rate": rate,
                "timestamp": time.time(),
                "zip_results": zip_results
            }
            
            # Save progress in the daily folder
            daily_folder = self._get_daily_folder_path()
            progress_file = os.path.join(daily_folder, "progress.json")
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
            
            self.logger.info(f"Progress saved to: {progress_file}")
            
            return progress
            
        except Exception as e:
            self.logger.error(f"Fatal error in extraction process: {e}")
            raise

if __name__ == '__main__':
    # Configuration
    zip_urls = [
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
    ]
    
    # Initialize ABR extraction
    extractor = ABRExtraction(
        dump_dir="dump/abr_dump",
        records_per_xml=30000,
        parquet_batch_size=10000
    )
    
    # Run extraction
    try:
        results = extractor.extract_from_urls(zip_urls, max_workers=2)
        print(f"\nExtraction completed successfully!")
        print(f"Total ZIP files: {results['total_zips']}")
        print(f"Total XML files: {results['total_xmls']}")
        print(f"Total records: {results['total_records']:,}")
        print(f"Processing time: {results['processing_time']:.1f} seconds")
        print(f"Processing rate: {results['rate']:.1f} records/second")
    except Exception as e:
        print(f"Extraction failed: {e}")
        raise