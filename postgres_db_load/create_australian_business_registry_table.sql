-- PostgreSQL DDL for Australian Business Registry Table
-- This table stores all ABR data with match flags for future matching operations

CREATE TABLE IF NOT EXISTS australian_business_registry_data (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- ABR (Australian Business Register) fields
    abn VARCHAR(11) NOT NULL UNIQUE,
    entity_name VARCHAR(500) NOT NULL,
    entity_type VARCHAR(100),
    entity_status VARCHAR(50),
    state VARCHAR(50),
    postcode VARCHAR(10),
    start_date DATE,
    address TEXT,
    
    -- Matching information
    match_flag INTEGER DEFAULT 0 CHECK (match_flag IN (0, 1)),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_abn_format CHECK (abn ~ '^[0-9]{11}$')
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_abr_abn ON australian_business_registry_data(abn);
CREATE INDEX IF NOT EXISTS idx_abr_entity_name ON australian_business_registry_data(entity_name);
CREATE INDEX IF NOT EXISTS idx_abr_match_flag ON australian_business_registry_data(match_flag);
CREATE INDEX IF NOT EXISTS idx_abr_state ON australian_business_registry_data(state);
CREATE INDEX IF NOT EXISTS idx_abr_postcode ON australian_business_registry_data(postcode);
CREATE INDEX IF NOT EXISTS idx_abr_entity_status ON australian_business_registry_data(entity_status);
CREATE INDEX IF NOT EXISTS idx_abr_created_at ON australian_business_registry_data(created_at);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_abr_match_flag_entity_name ON australian_business_registry_data(match_flag, entity_name);
CREATE INDEX IF NOT EXISTS idx_abr_state_postcode ON australian_business_registry_data(state, postcode);

-- Add comments for documentation
COMMENT ON TABLE australian_business_registry_data IS 'All Australian Business Registry data with match flags for future matching operations';
COMMENT ON COLUMN australian_business_registry_data.abn IS 'Australian Business Number (11 digits) - UNIQUE';
COMMENT ON COLUMN australian_business_registry_data.entity_name IS 'Company/Entity name from ABR';
COMMENT ON COLUMN australian_business_registry_data.entity_type IS 'Type of entity (e.g., Private Company, Public Company)';
COMMENT ON COLUMN australian_business_registry_data.entity_status IS 'Current status (e.g., Active, Cancelled)';
COMMENT ON COLUMN australian_business_registry_data.state IS 'Australian state or territory';
COMMENT ON COLUMN australian_business_registry_data.postcode IS 'Postal code';
COMMENT ON COLUMN australian_business_registry_data.start_date IS 'Entity registration start date';
COMMENT ON COLUMN australian_business_registry_data.address IS 'Full address combining state and postcode';
COMMENT ON COLUMN australian_business_registry_data.match_flag IS 'Flag indicating if entity has been matched (1=matched, 0=unmatched)';
COMMENT ON COLUMN australian_business_registry_data.created_at IS 'Timestamp when the record was first created';
COMMENT ON COLUMN australian_business_registry_data.updated_at IS 'Timestamp when the record was last updated';

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_abr_updated_at ON australian_business_registry_data;
CREATE TRIGGER update_abr_updated_at 
    BEFORE UPDATE ON australian_business_registry_data 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
