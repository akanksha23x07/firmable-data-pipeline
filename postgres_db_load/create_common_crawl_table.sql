-- PostgreSQL DDL for CommonCrawl Table
-- This table stores all CommonCrawl data with match flags for future matching operations

CREATE TABLE IF NOT EXISTS commoncrawl_data (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- CommonCrawl fields
    base_url TEXT NOT NULL,
    domain VARCHAR(255) NOT NULL,
    cc_company_name VARCHAR(500) NOT NULL,
    extracted_index VARCHAR(50),
    
    -- Matching information
    match_flag INTEGER DEFAULT 0 CHECK (match_flag IN (0, 1)),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_cc_domain ON commoncrawl_data(domain);
CREATE INDEX IF NOT EXISTS idx_cc_company_name ON commoncrawl_data(cc_company_name);
CREATE INDEX IF NOT EXISTS idx_cc_match_flag ON commoncrawl_data(match_flag);
CREATE INDEX IF NOT EXISTS idx_cc_base_url ON commoncrawl_data(base_url);
CREATE INDEX IF NOT EXISTS idx_cc_created_at ON commoncrawl_data(created_at);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_cc_match_flag_company_name ON commoncrawl_data(match_flag, cc_company_name);
CREATE INDEX IF NOT EXISTS idx_cc_domain_company_name ON commoncrawl_data(domain, cc_company_name);

-- Add comments for documentation
COMMENT ON TABLE commoncrawl_data IS 'All CommonCrawl data with match flags for future matching operations';
COMMENT ON COLUMN commoncrawl_data.base_url IS 'Base URL from CommonCrawl data';
COMMENT ON COLUMN commoncrawl_data.domain IS 'Domain name from CommonCrawl data';
COMMENT ON COLUMN commoncrawl_data.cc_company_name IS 'Company name extracted from CommonCrawl data';
COMMENT ON COLUMN commoncrawl_data.extracted_index IS 'CommonCrawl crawl version (e.g., CC-MAIN-2025-38)';
COMMENT ON COLUMN commoncrawl_data.match_flag IS 'Flag indicating if entity has been matched (1=matched, 0=unmatched)';
COMMENT ON COLUMN commoncrawl_data.created_at IS 'Timestamp when the record was first created';
COMMENT ON COLUMN commoncrawl_data.updated_at IS 'Timestamp when the record was last updated';

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to update updated_at timestamp
DROP TRIGGER IF EXISTS update_cc_updated_at ON commoncrawl_data;
CREATE TRIGGER update_cc_updated_at 
    BEFORE UPDATE ON commoncrawl_data 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
