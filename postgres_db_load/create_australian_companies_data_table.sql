-- PostgreSQL DDL for Australian Companies Data Table
-- This table stores only matched records from ABR and CommonCrawl

CREATE TABLE IF NOT EXISTS australian_companies_data (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- ABR (Australian Business Register) fields
    abn VARCHAR(11) NOT NULL,
    entity_name VARCHAR(500) NOT NULL,
    entity_type VARCHAR(100),
    entity_status VARCHAR(50),
    postcode VARCHAR(10),
    state VARCHAR(50),
    start_date DATE,
    address TEXT,
    
    -- CommonCrawl fields
    base_url TEXT,
    domain VARCHAR(255),
    cc_company_name VARCHAR(500),
    
    -- Matching information
    match_type VARCHAR(20) CHECK (match_type IN ('exact', 'hybrid')),
    match_score DECIMAL(5,4),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_abn_format CHECK (abn ~ '^[0-9]{11}$'),
    CONSTRAINT chk_match_score CHECK (match_score >= 0 AND match_score <= 1),
    CONSTRAINT uk_acd_abn_cc_company UNIQUE (abn, cc_company_name)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_acd_abn ON australian_companies_data(abn);
CREATE INDEX IF NOT EXISTS idx_acd_entity_name ON australian_companies_data(entity_name);
CREATE INDEX IF NOT EXISTS idx_acd_domain ON australian_companies_data(domain);
CREATE INDEX IF NOT EXISTS idx_acd_match_type ON australian_companies_data(match_type);
CREATE INDEX IF NOT EXISTS idx_acd_state ON australian_companies_data(state);
CREATE INDEX IF NOT EXISTS idx_acd_postcode ON australian_companies_data(postcode);
CREATE INDEX IF NOT EXISTS idx_acd_created_at ON australian_companies_data(created_at);
CREATE INDEX IF NOT EXISTS idx_acd_updated_at ON australian_companies_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_acd_cc_company_name ON australian_companies_data(cc_company_name);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_acd_match_type_score ON australian_companies_data(match_type, match_score);
CREATE INDEX IF NOT EXISTS idx_acd_state_postcode ON australian_companies_data(state, postcode);

-- Add comments for documentation
COMMENT ON TABLE australian_companies_data IS 'Matched records from ABR and CommonCrawl data';
COMMENT ON COLUMN australian_companies_data.abn IS 'Australian Business Number (11 digits)';
COMMENT ON COLUMN australian_companies_data.entity_name IS 'Company name from ABR data';
COMMENT ON COLUMN australian_companies_data.entity_type IS 'Type of company entity (e.g., Private Company, Public Company)';
COMMENT ON COLUMN australian_companies_data.entity_status IS 'Current status of the company (e.g., Active, Cancelled)';
COMMENT ON COLUMN australian_companies_data.postcode IS 'Postal code';
COMMENT ON COLUMN australian_companies_data.state IS 'Australian state or territory';
COMMENT ON COLUMN australian_companies_data.start_date IS 'Company registration start date';
COMMENT ON COLUMN australian_companies_data.address IS 'Full address combining state and postcode';
COMMENT ON COLUMN australian_companies_data.base_url IS 'Base URL from CommonCrawl data';
COMMENT ON COLUMN australian_companies_data.domain IS 'Domain name from CommonCrawl data';
COMMENT ON COLUMN australian_companies_data.cc_company_name IS 'Company name from CommonCrawl data';
COMMENT ON COLUMN australian_companies_data.match_type IS 'Type of match: exact or hybrid';
COMMENT ON COLUMN australian_companies_data.match_score IS 'Overall match score (0-1)';
COMMENT ON COLUMN australian_companies_data.created_at IS 'Timestamp when the record was created/matched';
COMMENT ON COLUMN australian_companies_data.updated_at IS 'Timestamp when the record was last updated';

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to update updated_at timestamp
DROP TRIGGER IF EXISTS update_acd_updated_at ON australian_companies_data;
CREATE TRIGGER update_acd_updated_at 
    BEFORE UPDATE ON australian_companies_data 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
