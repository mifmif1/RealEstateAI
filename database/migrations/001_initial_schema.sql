-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;


-- ====================================================================
-- Shared function to keep updated_at in sync on UPDATE
-- ====================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';


-- ====================================================================
-- Table: potential_assets  (candidate deals / our assets)
-- ====================================================================
CREATE TABLE IF NOT EXISTS potential_assets (
    id SERIAL PRIMARY KEY,

    -- spatial
    location GEOGRAPHY(POINT, 4326) NOT NULL,

    -- core numbers
    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    level INTEGER,
    parking BOOLEAN,
    construction_year INTEGER,

    -- identifiers / provenance
    source VARCHAR(50),               -- e.g., 'ReInvest', 'eauction'
    portfolio VARCHAR(50),
    source_unique_code VARCHAR(100),

    -- descriptive text
    title TEXT,
    description TEXT,
    address TEXT,
    municipality VARCHAR(100),
    prefecture VARCHAR(100),
    url TEXT,

    -- bookkeeping
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for potential_assets
CREATE INDEX IF NOT EXISTS idx_potential_assets_location
    ON potential_assets USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_potential_assets_sqm
    ON potential_assets (sqm);

CREATE INDEX IF NOT EXISTS idx_potential_assets_price
    ON potential_assets (price);

CREATE INDEX IF NOT EXISTS idx_potential_assets_source
    ON potential_assets (source);

CREATE INDEX IF NOT EXISTS idx_potential_assets_portfolio
    ON potential_assets (portfolio);

CREATE INDEX IF NOT EXISTS idx_potential_assets_source_portfolio_code
    ON potential_assets (source, portfolio, source_unique_code);

CREATE INDEX IF NOT EXISTS idx_potential_assets_created_at
    ON potential_assets (created_at);

-- Trigger to keep updated_at in sync for potential_assets
CREATE TRIGGER update_potential_assets_updated_at
BEFORE UPDATE ON potential_assets
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Table: comparable_assets  (market comparables, e.g. Spitogatos)
-- ====================================================================
CREATE TABLE IF NOT EXISTS comparable_assets (
    id SERIAL PRIMARY KEY,

    -- spatial
    location GEOGRAPHY(POINT, 4326) NOT NULL,

    -- core numbers
    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    level INTEGER,
    parking BOOLEAN,
    construction_year INTEGER,

    -- identifiers / provenance
    source VARCHAR(50),               -- e.g., 'spitogatos'
    portfolio VARCHAR(50),
    source_unique_code VARCHAR(100),

    -- descriptive text
    title TEXT,
    description TEXT,
    address TEXT,
    municipality VARCHAR(100),
    prefecture VARCHAR(100),
    url TEXT,

    -- bookkeeping
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for comparable_assets
CREATE INDEX IF NOT EXISTS idx_comparable_assets_location
    ON comparable_assets USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_sqm
    ON comparable_assets (sqm);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_price
    ON comparable_assets (price);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_source
    ON comparable_assets (source);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_portfolio
    ON comparable_assets (portfolio);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_source_portfolio_code
    ON comparable_assets (source, portfolio, source_unique_code);

CREATE INDEX IF NOT EXISTS idx_comparable_assets_created_at
    ON comparable_assets (created_at);

-- Trigger to keep updated_at in sync for comparable_assets
CREATE TRIGGER update_comparable_assets_updated_at
BEFORE UPDATE ON comparable_assets
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Table: potential_comparison_summary
-- One aggregated row per potential asset with comparison metrics
-- ====================================================================
CREATE TABLE IF NOT EXISTS potential_comparison_summary (
    potential_asset_id INTEGER PRIMARY KEY
        REFERENCES potential_assets(id) ON DELETE CASCADE,

    -- how many comparables were used
    assets_count INTEGER,

    -- price-per-sqm statistics over comparables
    comparison_average FLOAT,
    comparison_min FLOAT,
    comparison_max FLOAT,
    comparison_median FLOAT,
    comparison_std FLOAT,

    -- valuation outputs
    normalized_mean FLOAT,            -- normalized mean price/sqm used for valuation
    revaluated_price_meter FLOAT,     -- chosen price/sqm for the potential asset
    revaluation_total_price FLOAT,    -- final revaluated absolute price
    max_buy_price FLOAT,
    score FLOAT,

    -- geometry / search
    searched_radius FLOAT,

    -- convenience URLs (e.g. first comparable / eauction)
    spitogatos_url TEXT,
    eauctions_url TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_potential_comparison_summary_created_at
    ON potential_comparison_summary (created_at);

-- Trigger to keep updated_at in sync for potential_comparison_summary
CREATE TRIGGER update_potential_comparison_summary_updated_at
BEFORE UPDATE ON potential_comparison_summary
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Views: statistics over potential and comparable assets
-- ====================================================================
CREATE OR REPLACE VIEW potential_asset_statistics AS
SELECT 
    source,
    COUNT(*) as potential_asset_count,
    AVG(price) as avg_price,
    AVG(price / NULLIF(sqm, 0)) as avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) as min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) as max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) as stddev_price_per_sqm,
    AVG(sqm) as avg_sqm
FROM potential_assets
GROUP BY source;


CREATE OR REPLACE VIEW comparable_asset_statistics AS
SELECT 
    source,
    COUNT(*) as comparable_asset_count,
    AVG(price) as avg_price,
    AVG(price / NULLIF(sqm, 0)) as avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) as min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) as max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) as stddev_price_per_sqm,
    AVG(sqm) as avg_sqm
FROM comparable_assets
GROUP BY source;

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create potential assets table with spatial geometry column
CREATE TABLE IF NOT EXISTS potential_assets (
    id SERIAL PRIMARY KEY,
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    level INTEGER,
    parking BOOLEAN DEFAULT NULL,
    construction_year INTEGER,

    source VARCHAR(50), -- e.g., 'spitogatos', 'reonline', 'eauctions'
    portfolio VARCHAR(50),
    source_unique_code VARCHAR(50),
    title VARCHAR(50),
    description TEXT,

    address TEXT,
    municipality VARCHAR(50),
    perfecture VARCHAR(50),
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index on location for fast spatial queries
CREATE INDEX IF NOT EXISTS idx_potential_assets_location ON potential_assets USING GIST (location);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_potential_assets_sqm ON potential_assets (sqm);
CREATE INDEX IF NOT EXISTS idx_potential_assets_price ON potential_assets (price);
CREATE INDEX IF NOT EXISTS idx_potential_assets_source ON potential_assets (source);
CREATE INDEX IF NOT EXISTS idx_potential_assets_created_at ON potential_assets (created_at);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_potential_assets_updated_at BEFORE UPDATE ON potential_assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create view for potential asset statistics by location
CREATE OR REPLACE VIEW potential_asset_statistics AS
SELECT 
    source,
    COUNT(*) as potential_asset_count,
    AVG(price) as avg_price,
    AVG(price / NULLIF(sqm, 0)) as avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) as min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) as max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) as stddev_price_per_sqm,
    AVG(sqm) as avg_sqm
FROM potential_assets
GROUP BY source;

