"""
Entity Matching Script
This script performs entity matching between ABR and CommonCrawl data using:
1. Exact matching via left join on company names
2. Semantic matching using BGE-small embeddings and FAISS
"""

import pandas as pd
import numpy as np
import duckdb
import logging
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
import os
import json
from sentence_transformers import SentenceTransformer
import faiss
import pickle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/entity_matching_logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EntityMatcher:
    """Class for performing entity matching between ABR and CommonCrawl data."""
    
    def __init__(self, abr_data_path: str, commoncrawl_data_path: str, output_path: str):
        """
        Initialize the entity matcher.
        
        Args:
            abr_data_path: Path to ABR transformed data
            commoncrawl_data_path: Path to CommonCrawl transformed data
            output_path: Path for output files
        """
        self.abr_data_path = abr_data_path
        self.commoncrawl_data_path = commoncrawl_data_path
        self.output_path = output_path
        self.conn = duckdb.connect()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Initialize BGE-small embedding model
        logger.info("Loading BGE-small embedding model...")
        self.model_name = 'BAAI/bge-small-en-v1.5'
        
        try:
            self.embedding_model = SentenceTransformer(self.model_name)
            logger.info("BGE-small model loaded successfully")
            self.use_embeddings = True
        except Exception as e:
            logger.error(f"Failed to load BGE-small model: {str(e)}")
            self.embedding_model = None
            self.use_embeddings = False
        
        # FAISS index for semantic matching
        self.faiss_index = None
        self.cc_embeddings = None
        self.cc_company_names = None
        
        logger.info("EntityMatcher initialized")
    
    def _find_parquet_file(self, file_path):
        """Find a parquet file, handling both file paths and directory paths."""
        import glob
        
        # If it's already a file path, check if it exists
        if os.path.isfile(file_path):
            logger.info(f"Found parquet file: {file_path}")
            return file_path
        
        # If it's a directory, look for parquet files
        if os.path.isdir(file_path):
            pattern = os.path.join(file_path, "*.parquet")
            matches = glob.glob(pattern)
            
            if matches:
                logger.info(f"Found parquet file in directory: {matches[0]}")
                return matches[0]
            else:
                logger.error(f"No parquet files found in directory: {file_path}")
                raise FileNotFoundError(f"No parquet files found in {file_path}")
        
        # If it's neither file nor directory, try to find similar files
        directory = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        
        if os.path.exists(directory):
            # Look for files with similar name
            pattern = os.path.join(directory, f"*{filename.split('.')[0]}*.parquet")
            matches = glob.glob(pattern)
            
            if matches:
                logger.info(f"Found similar parquet file: {matches[0]}")
                return matches[0]
            
            # Look for any parquet file in directory
            pattern = os.path.join(directory, "*.parquet")
            matches = glob.glob(pattern)
            
            if matches:
                logger.warning(f"Could not find {filename}, using first available parquet: {matches[0]}")
                return matches[0]
        
        logger.error(f"Could not find parquet file: {file_path}")
        raise FileNotFoundError(f"Could not find parquet file: {file_path}")
    
    def load_data(self, limit_records: int = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load ABR and CommonCrawl transformed data."""
        logger.info("Loading transformed data...")
        
        # Find parquet files dynamically
        abr_file = self._find_parquet_file(self.abr_data_path)
        cc_file = self._find_parquet_file(self.commoncrawl_data_path)
        
        # Load ABR data with optional limit
        if limit_records:
            abr_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{abr_file}') LIMIT {limit_records}
            """).fetchdf()
            logger.info(f"Loaded {len(abr_data)} ABR records (limited to {limit_records})")
        else:
            abr_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{abr_file}')
            """).fetchdf()
            logger.info(f"Loaded {len(abr_data)} ABR records")
        
        # Load CommonCrawl data with optional limit
        if limit_records:
            cc_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{cc_file}') LIMIT {limit_records}
            """).fetchdf()
            logger.info(f"Loaded {len(cc_data)} CommonCrawl records (limited to {limit_records})")
        else:
            cc_data = self.conn.execute(f"""
                SELECT * FROM read_parquet('{cc_file}')
            """).fetchdf()
            logger.info(f"Loaded {len(cc_data)} CommonCrawl records")
        
        return abr_data, cc_data
    
    def exact_matching(self, abr_data: pd.DataFrame, cc_data: pd.DataFrame) -> pd.DataFrame:
        """Perform exact matching using left join on company names."""
        logger.info("Performing exact matching...")
        
        # Perform left join on company names
        exact_matches = pd.merge(
            abr_data,
            cc_data[['base_url', 'domain', 'cc_company_name']],
            left_on='abr_company_name',
            right_on='cc_company_name',
            how='left',
            suffixes=('_abr', '_cc')
        )
        
        # Add match type column
        exact_matches['match_type'] = exact_matches['cc_company_name'].apply(
            lambda x: 'exact' if pd.notna(x) else 'no_match'
        )
        
        # Add match score for exact matches
        exact_matches['match_score'] = exact_matches['match_type'].apply(
            lambda x: 1.0 if x == 'exact' else 0.0
        )
        
        exact_match_count = len(exact_matches[exact_matches['match_type'] == 'exact'])
        logger.info(f"Found {exact_match_count} exact matches out of {len(exact_matches)} ABR records")
        
        return exact_matches
    
    def generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings using BGE-small model."""
        if not self.use_embeddings:
            raise ValueError("BGE-small model not available. Please load the model first.")
        
        # Use sentence-transformers to generate embeddings
        embeddings = self.embedding_model.encode(texts, convert_to_numpy=True)
        
        # Normalize embeddings
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        
        return embeddings
    
    def _jaccard_similarity(self, text1: str, text2: str, n: int = 3) -> float:
        """Calculate Jaccard similarity using character n-grams."""
        def get_ngrams(text: str, n: int) -> set:
            """Get character n-grams from text."""
            text = text.lower().replace(' ', '')
            return set(text[i:i+n] for i in range(len(text)-n+1))
        
        ngrams1 = get_ngrams(text1, n)
        ngrams2 = get_ngrams(text2, n)
        
        if not ngrams1 and not ngrams2:
            return 1.0
        if not ngrams1 or not ngrams2:
            return 0.0
        
        intersection = len(ngrams1.intersection(ngrams2))
        union = len(ngrams1.union(ngrams2))
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_hybrid_score(self, semantic_score: float, lexical_score: float) -> float:
        """Calculate hybrid score: 0.7 * semantic + 0.3 * lexical."""
        return 0.7 * semantic_score + 0.3 * lexical_score
    
    def _generate_fallback_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate simple fallback embeddings using character n-grams."""
        from collections import Counter
        import re
        
        def text_to_ngrams(text: str, n: int = 3) -> List[str]:
            """Convert text to character n-grams."""
            text = re.sub(r'[^a-zA-Z0-9]', '', text.lower())
            return [text[i:i+n] for i in range(len(text)-n+1)]
        
        # Create vocabulary from all texts
        all_ngrams = set()
        for text in texts:
            all_ngrams.update(text_to_ngrams(text))
        
        vocab = list(all_ngrams)
        vocab_size = len(vocab)
        
        # Generate embeddings
        embeddings = np.zeros((len(texts), vocab_size))
        for i, text in enumerate(texts):
            ngrams = text_to_ngrams(text)
            ngram_counts = Counter(ngrams)
            for j, ngram in enumerate(vocab):
                embeddings[i, j] = ngram_counts.get(ngram, 0)
        
        # Normalize embeddings
        embeddings = embeddings / (np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-8)
        
        return embeddings
    
    def prepare_embeddings(self, cc_data: pd.DataFrame):
        """Prepare embeddings for CommonCrawl company names."""
        logger.info("Preparing embeddings for CommonCrawl company names...")
        
        # Get unique company names
        unique_cc_names = cc_data['cc_company_name'].unique()
        logger.info(f"Generating embeddings for {len(unique_cc_names)} unique CommonCrawl company names")
        
        # Generate embeddings using BGE-small
        self.cc_embeddings = self.generate_embeddings(unique_cc_names.tolist())
        self.cc_company_names = unique_cc_names
        
        # Create FAISS index for efficient similarity search
        dimension = self.cc_embeddings.shape[1]
        self.faiss_index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
        
        # Normalize embeddings for cosine similarity
        faiss.normalize_L2(self.cc_embeddings)
        self.faiss_index.add(self.cc_embeddings.astype('float32'))
        
        logger.info(f"FAISS index created with {self.faiss_index.ntotal} vectors")
        logger.info(f"Embeddings generated with shape {self.cc_embeddings.shape}")
    
    def semantic_matching(self, abr_data: pd.DataFrame, threshold: float = 0.9) -> Dict:
        """Perform semantic matching using embeddings and FAISS."""
        logger.info(f"Performing semantic matching with threshold {threshold}...")
        
        if self.faiss_index is None:
            raise ValueError("FAISS index not initialized. Call prepare_embeddings first.")
        
        # Get ABR company names that don't have exact matches
        abr_names = abr_data['abr_company_name'].unique()
        logger.info(f"Generating embeddings for {len(abr_names)} ABR company names")
        
        # Generate embeddings for ABR company names using BGE-small
        abr_embeddings = self.generate_embeddings(abr_names.tolist())
        
        # Normalize ABR embeddings for FAISS search
        faiss.normalize_L2(abr_embeddings)
        
        # Search for similar vectors using FAISS
        k = min(10, self.faiss_index.ntotal)  # Get top 10 matches for better filtering
        scores, indices = self.faiss_index.search(abr_embeddings.astype('float32'), k)
        
        # Create mapping for hybrid semantic + lexical matches
        semantic_matches = {}
        for i, abr_name in enumerate(abr_names):
            best_hybrid_score = 0.0
            best_match = None
            
            # Check multiple top matches and calculate hybrid scores
            for j in range(min(5, len(scores[i]))):  # Check top 5 matches
                semantic_score = scores[i][j]
                cc_name = self.cc_company_names[indices[i][j]]
                
                # Calculate lexical similarity using Jaccard
                lexical_score = self._jaccard_similarity(abr_name, cc_name)
                
                # Calculate hybrid score
                hybrid_score = self._calculate_hybrid_score(semantic_score, lexical_score)
                
                # Apply business logic filtering
                if (hybrid_score >= threshold and 
                    self._is_valid_business_match(abr_name, cc_name, hybrid_score) and
                    hybrid_score > best_hybrid_score):
                    
                    best_hybrid_score = hybrid_score
                    best_match = {
                        'cc_company_name': cc_name,
                        'match_score': float(hybrid_score),
                        'semantic_score': float(semantic_score),
                        'lexical_score': float(lexical_score),
                        'match_type': 'hybrid'
                    }
            
            if best_match:
                semantic_matches[abr_name] = best_match
        
        logger.info(f"Found {len(semantic_matches)} semantic matches above threshold {threshold}")
        
        return semantic_matches
    
    def _is_valid_business_match(self, abr_name: str, cc_name: str, hybrid_score: float) -> bool:
        """Apply business logic to validate if two company names are likely the same business."""
        
        # Skip very short names (likely generic terms)
        if len(abr_name) < 4 or len(cc_name) < 4:
            return False
        
        # Skip if one name is a substring of the other (likely different businesses)
        if abr_name in cc_name or cc_name in abr_name:
            return False
        
        # Skip generic business terms
        generic_terms = ['insurance', 'hotel', 'restaurant', 'cafe', 'shop', 'store', 'service', 'group', 'company']
        if any(term in abr_name.lower() or term in cc_name.lower() for term in generic_terms):
            return False
        
        # Check for significant word overlap (at least 50% of words should match)
        abr_words = set(abr_name.lower().split())
        cc_words = set(cc_name.lower().split())
        
        if len(abr_words) > 1 and len(cc_words) > 1:
            overlap = len(abr_words.intersection(cc_words))
            min_words = min(len(abr_words), len(cc_words))
            word_overlap_ratio = overlap / min_words
            
            # Require at least 50% word overlap for multi-word names
            if word_overlap_ratio < 0.5:
                return False
        
        # For high hybrid scores (>0.95), be more lenient
        if hybrid_score > 0.95:
            return True
        
        # For medium scores (0.9-0.95), apply stricter rules
        if hybrid_score >= 0.9:
            # Require at least some word overlap
            if len(abr_words) > 1 and len(cc_words) > 1:
                overlap = len(abr_words.intersection(cc_words))
                return overlap > 0
        
        return True
    
    def create_unified_table(self, exact_matches: pd.DataFrame, semantic_matches: Dict) -> pd.DataFrame:
        """Create unified table with all matched data."""
        logger.info("Creating unified table...")
        
        unified_data = exact_matches.copy()
        
        # Update hybrid matches
        for abr_name, match_info in semantic_matches.items():
            # Find rows with this ABR company name that don't have exact matches
            mask = (unified_data['abr_company_name'] == abr_name) & (unified_data['match_type'] == 'no_match')
            
            if mask.any():
                # Update the first matching row
                idx = unified_data[mask].index[0]
                unified_data.loc[idx, 'cc_company_name'] = match_info['cc_company_name']
                unified_data.loc[idx, 'match_type'] = match_info['match_type']
                unified_data.loc[idx, 'match_score'] = match_info['match_score']
                
                # Add additional score information for hybrid matches
                if 'semantic_score' in match_info:
                    unified_data.loc[idx, 'semantic_score'] = match_info['semantic_score']
                if 'lexical_score' in match_info:
                    unified_data.loc[idx, 'lexical_score'] = match_info['lexical_score']
        
        # Add base_url and domain for semantic matches
        cc_lookup = {}
        cc_data = self.conn.execute(f"""
            SELECT cc_company_name, base_url, domain FROM read_parquet('{self.commoncrawl_data_path}')
        """).fetchdf()
        
        for _, row in cc_data.iterrows():
            cc_lookup[row['cc_company_name']] = {
                'base_url': row['base_url'],
                'domain': row['domain']
            }
        
        # Fill in base_url and domain for semantic matches
        for idx, row in unified_data.iterrows():
            if row['match_type'] == 'semantic' and pd.notna(row['cc_company_name']):
                if row['cc_company_name'] in cc_lookup:
                    unified_data.loc[idx, 'base_url'] = cc_lookup[row['cc_company_name']]['base_url']
                    unified_data.loc[idx, 'domain'] = cc_lookup[row['cc_company_name']]['domain']
        
        # Add metadata
        unified_data['matched_at'] = datetime.now()
        unified_data['total_matches'] = len(unified_data[unified_data['match_type'] != 'no_match'])
        
        logger.info(f"Unified table created with {len(unified_data)} records")
        logger.info(f"Total matches: {unified_data['total_matches'].iloc[0]}")
        logger.info(f"Exact matches: {len(unified_data[unified_data['match_type'] == 'exact'])}")
        logger.info(f"Hybrid matches: {len(unified_data[unified_data['match_type'] == 'hybrid'])}")
        logger.info(f"No matches: {len(unified_data[unified_data['match_type'] == 'no_match'])}")
        
        return unified_data
    
    def save_results(self, unified_data: pd.DataFrame, exact_matches: pd.DataFrame, semantic_matches: Dict):
        """Save matching results and reports."""
        logger.info("Saving entity matching results...")
        
        # Update match flags in the data before saving
        logger.info("Updating match flags for matched records...")
        unified_data['match_flag'] = unified_data['match_type'].apply(
            lambda x: 1 if x != 'no_match' else 0
        )
        
        # Prepare data for saving
        matched_data = unified_data[unified_data['match_type'] != 'no_match']
        
        # Prepare ABR data with match flags
        abr_updated = unified_data[['ABN', 'EntityName', 'EntityType', 'EntityStatus', 
                                   'State', 'Postcode', 'StartDate', 'address', 'match_flag']].copy()
        
        # Remove duplicates from ABR data (same ABN + EntityName combination)
        abr_updated = abr_updated.drop_duplicates(subset=['ABN', 'EntityName'], keep='first')
        
        # Prepare CommonCrawl data with match flags
        cc_columns = ['base_url', 'domain', 'cc_company_name', 'match_flag']
        if 'extracted_index' in unified_data.columns:
            cc_columns.append('extracted_index')
        
        cc_updated = unified_data[cc_columns].copy()
        cc_updated = cc_updated.dropna(subset=['domain', 'cc_company_name'])  # Remove rows without CC data
        
        # Remove duplicates from CommonCrawl data (same domain/company can match multiple ABR records)
        cc_updated = cc_updated.drop_duplicates(subset=['domain', 'cc_company_name'], keep='first')
        
        # Save sample CSV files (10 records each) before saving parquet files
        logger.info("Saving sample CSV files (10 records each)...")
        
        # 1. Save ABR sample CSV
        abr_sample = abr_updated.head(10)
        abr_csv_file = os.path.join(self.output_path, 'abr_sample_10_records.csv')
        abr_sample.to_csv(abr_csv_file, index=False)
        logger.info(f"ABR sample (10 records) saved to {abr_csv_file}")
        
        # 2. Save CommonCrawl sample CSV
        if len(cc_updated) > 0:
            cc_sample = cc_updated.head(10)
            cc_csv_file = os.path.join(self.output_path, 'commoncrawl_sample_10_records.csv')
            cc_sample.to_csv(cc_csv_file, index=False)
            logger.info(f"CommonCrawl sample (10 records) saved to {cc_csv_file}")
        else:
            logger.warning("No CommonCrawl data available for sample CSV")
        
        # 3. Save matched data sample CSV
        if len(matched_data) > 0:
            # Apply same column selection for sample CSV
            required_columns = [
                'ABN', 'EntityName', 'base_url', 'EntityType', 'EntityStatus', 
                'Postcode', 'State', 'StartDate', 'cc_company_name', 'address', 
                'domain', 'match_type', 'match_score', 'matched_at'
            ]
            
            # Filter to only include columns that exist in the data
            available_columns = [col for col in required_columns if col in matched_data.columns]
            matched_sample = matched_data[available_columns].copy()
            
            # Rename matched_at to created_at
            if 'matched_at' in matched_sample.columns:
                matched_sample = matched_sample.rename(columns={'matched_at': 'created_at'})
            
            matched_sample = matched_sample.head(10)
            matched_csv_file = os.path.join(self.output_path, 'matched_companies_sample_10_records.csv')
            matched_sample.to_csv(matched_csv_file, index=False)
            logger.info(f"Matched companies sample (10 records) saved to {matched_csv_file}")
        else:
            logger.warning("No matched records available for sample CSV")
        
        # Save parquet files
        logger.info("Saving parquet files...")
        
        # Save only matched records (exclude no-match records) as parquet
        matched_file = os.path.join(self.output_path, 'matched_entity_matches.parquet')
        
        if len(matched_data) > 0:
            # Select only required columns for matched companies data
            required_columns = [
                'ABN', 'EntityName', 'base_url', 'EntityType', 'EntityStatus', 
                'Postcode', 'State', 'StartDate', 'cc_company_name', 'address', 
                'domain', 'match_type', 'match_score', 'matched_at'
            ]
            
            # Filter to only include columns that exist in the data
            available_columns = [col for col in required_columns if col in matched_data.columns]
            matched_data_renamed = matched_data[available_columns].copy()
            
            # Rename matched_at to created_at
            if 'matched_at' in matched_data_renamed.columns:
                matched_data_renamed = matched_data_renamed.rename(columns={'matched_at': 'created_at'})
            
            self.conn.register('matched_temp', matched_data_renamed)
            self.conn.execute(f"""
                COPY (SELECT * FROM matched_temp) TO '{matched_file}' (FORMAT PARQUET)
            """)
            logger.info(f"Matched records ({len(matched_data)} records) saved to {matched_file}")
        else:
            logger.warning("No matched records found to save")
        
        # Save updated ABR data with match flags
        abr_updated_file = os.path.join(self.output_path, 'abr_with_match_flags.parquet')
        self.conn.register('abr_updated_temp', abr_updated)
        self.conn.execute(f"""
            COPY (SELECT * FROM abr_updated_temp) TO '{abr_updated_file}' (FORMAT PARQUET)
        """)
        logger.info(f"Updated ABR data with match flags saved to {abr_updated_file}")
        
        # Save updated CommonCrawl data with match flags
        if len(cc_updated) > 0:
            cc_updated_file = os.path.join(self.output_path, 'commoncrawl_with_match_flags.parquet')
            self.conn.register('cc_updated_temp', cc_updated)
            self.conn.execute(f"""
                COPY (SELECT * FROM cc_updated_temp) TO '{cc_updated_file}' (FORMAT PARQUET)
            """)
            logger.info(f"Updated CommonCrawl data with match flags saved to {cc_updated_file}")
        else:
            logger.warning("No CommonCrawl data with match flags to save")
        
        # Save sample CSV for testing (only matched records)
        if len(matched_data) > 0:
            sample_df = matched_data.head(50)
            csv_file = os.path.join(self.output_path, 'matched_entity_matches_sample_50_records.csv')
            sample_df.to_csv(csv_file, index=False)
            logger.info(f"Sample matched data (50 records) saved to {csv_file}")
        else:
            logger.warning("No matched records available for sample CSV")
        
        # Save matching summary
        summary = {
            'total_abr_records': len(unified_data),
            'exact_matches': len(unified_data[unified_data['match_type'] == 'exact']),
            'hybrid_matches': len(unified_data[unified_data['match_type'] == 'hybrid']),
            'no_matches': len(unified_data[unified_data['match_type'] == 'no_match']),
            'total_matches': len(unified_data[unified_data['match_type'] != 'no_match']),
            'match_rate': len(unified_data[unified_data['match_type'] != 'no_match']) / len(unified_data) * 100,
            'avg_exact_match_score': unified_data[unified_data['match_type'] == 'exact']['match_score'].mean(),
            'avg_hybrid_match_score': unified_data[unified_data['match_type'] == 'hybrid']['match_score'].mean(),
            'avg_semantic_score': unified_data[unified_data['match_type'] == 'hybrid']['semantic_score'].mean() if 'semantic_score' in unified_data.columns else None,
            'avg_lexical_score': unified_data[unified_data['match_type'] == 'hybrid']['lexical_score'].mean() if 'lexical_score' in unified_data.columns else None,
            'timestamp': datetime.now().isoformat()
        }
        
        summary_file = os.path.join(self.output_path, 'entity_matching_summary.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Matching summary saved to {summary_file}")
        
        # Save hybrid matches details
        semantic_details = {
            'hybrid_matches': semantic_matches,
            'threshold_used': 0.9,
            'total_hybrid_matches': len(semantic_matches),
            'scoring_method': 'hybrid (0.7 * semantic + 0.3 * lexical)',
            'lexical_method': 'Jaccard similarity with character 3-grams'
        }
        
        semantic_file = os.path.join(self.output_path, 'semantic_matches_details.json')
        with open(semantic_file, 'w') as f:
            json.dump(semantic_details, f, indent=2, default=str)
        logger.info(f"Semantic matches details saved to {semantic_file}")
    
    def run_entity_matching(self, threshold: float = None, limit_records: int = None):
        """Run the complete entity matching pipeline."""
        logger.info("Starting entity matching pipeline...")
        
        # Get parameters from environment variables or use defaults
        import os
        threshold = threshold or float(os.getenv('ENTITY_MATCHING_THRESHOLD', '0.9'))
        limit_records = limit_records or int(os.getenv('ENTITY_MATCHING_LIMIT_RECORDS', '10000'))
        logger.info(f"Using parameters: threshold={threshold}, limit_records={limit_records}")
        
        try:
            # 1. Load data
            abr_data, cc_data = self.load_data(limit_records=limit_records)
            
            # 2. Perform exact matching
            exact_matches = self.exact_matching(abr_data, cc_data)
            
            # 3. Prepare embeddings for semantic matching
            self.prepare_embeddings(cc_data)
            
            # 4. Perform semantic matching
            semantic_matches = self.semantic_matching(abr_data, threshold)
            
            # 5. Create unified table
            unified_data = self.create_unified_table(exact_matches, semantic_matches)
            
            # 6. Save results
            self.save_results(unified_data, exact_matches, semantic_matches)
            
            logger.info("Entity matching pipeline completed successfully!")
            
            # Print summary
            print("\n" + "="*60)
            print("ENTITY MATCHING SUMMARY")
            print("="*60)
            print(f"Total ABR Records: {len(unified_data):,}")
            print(f"Exact Matches: {len(unified_data[unified_data['match_type'] == 'exact']):,}")
            print(f"Hybrid Matches: {len(unified_data[unified_data['match_type'] == 'hybrid']):,}")
            print(f"No Matches: {len(unified_data[unified_data['match_type'] == 'no_match']):,}")
            print(f"Total Matches: {len(unified_data[unified_data['match_type'] != 'no_match']):,}")
            print(f"Match Rate: {len(unified_data[unified_data['match_type'] != 'no_match']) / len(unified_data) * 100:.2f}%")
            print(f"Average Hybrid Match Score: {unified_data[unified_data['match_type'] == 'hybrid']['match_score'].mean():.3f}")
            if 'semantic_score' in unified_data.columns:
                print(f"Average Semantic Score: {unified_data[unified_data['match_type'] == 'hybrid']['semantic_score'].mean():.3f}")
            if 'lexical_score' in unified_data.columns:
                print(f"Average Lexical Score: {unified_data[unified_data['match_type'] == 'hybrid']['lexical_score'].mean():.3f}")
            print("="*60)
            
        except Exception as e:
            logger.error(f"Entity matching failed: {str(e)}")
            raise
        finally:
            self.conn.close()

def main():
    """Main function to run entity matching."""
    import os
    
    # Configuration - use directory paths for dynamic file discovery
    abr_data_path = "transformations/output/abr_clean"
    commoncrawl_data_path = "transformations/output/commoncrawl_clean"
    output_path = "entity_matching/entity_matches"
    
    # Get parameters from environment variables or use defaults
    threshold = float(os.getenv('ENTITY_MATCHING_THRESHOLD', '0.9'))
    limit_records = int(os.getenv('ENTITY_MATCHING_LIMIT_RECORDS', '10000'))
    
    logger.info(f"Entity matching configuration:")
    logger.info(f"  - Threshold: {threshold} (90% = strict matching, 70% = loose matching)")
    logger.info(f"  - Limit records: {limit_records} (None = all records, 10000 = test mode)")
    
    # Run entity matching
    matcher = EntityMatcher(abr_data_path, commoncrawl_data_path, output_path)
    matcher.run_entity_matching(threshold=threshold, limit_records=limit_records)

if __name__ == "__main__":
    main()
