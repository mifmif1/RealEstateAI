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
    location GEOGRAPHY(POINT, 4326) NOT NULL,

    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    level INTEGER,
    parking BOOLEAN,
    construction_year INTEGER,

    source VARCHAR(50),
    portfolio VARCHAR(50),
    source_unique_code VARCHAR(100),

    title TEXT,
    description TEXT,
    address TEXT,
    municipality VARCHAR(100),
    prefecture VARCHAR(100),
    url TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TRIGGER update_potential_assets_updated_at
BEFORE UPDATE ON potential_assets
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Table: comparison_assets  (market comparables)
-- ====================================================================
CREATE TABLE IF NOT EXISTS comparison_assets (
    id SERIAL PRIMARY KEY,
    location GEOGRAPHY(POINT, 4326) NOT NULL,

    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    level INTEGER,
    parking BOOLEAN,
    construction_year INTEGER,

    source VARCHAR(50),
    portfolio VARCHAR(50),
    source_unique_code VARCHAR(100),

    title TEXT,
    description TEXT,
    address TEXT,
    municipality VARCHAR(100),
    prefecture VARCHAR(100),
    url TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_location
    ON comparison_assets USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_sqm
    ON comparison_assets (sqm);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_price
    ON comparison_assets (price);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_source
    ON comparison_assets (source);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_portfolio
    ON comparison_assets (portfolio);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_source_portfolio_code
    ON comparison_assets (source, portfolio, source_unique_code);

CREATE INDEX IF NOT EXISTS idx_comparison_assets_created_at
    ON comparison_assets (created_at);

CREATE TRIGGER update_comparison_assets_updated_at
BEFORE UPDATE ON comparison_assets
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Table: potential_comparison_summary
-- One aggregated row per potential asset
-- ====================================================================
CREATE TABLE IF NOT EXISTS potential_comparison_summary (
    potential_asset_id INTEGER PRIMARY KEY
        REFERENCES potential_assets(id) ON DELETE CASCADE,

    assets_count INTEGER,

    comparison_average FLOAT,
    comparison_min FLOAT,
    comparison_max FLOAT,
    comparison_median FLOAT,
    comparison_std FLOAT,

    normalized_mean FLOAT,
    revaluated_price_meter FLOAT,
    revaluation_total_price FLOAT,
    max_buy_price FLOAT,
    score FLOAT,

    searched_radius FLOAT,

    spitogatos_url TEXT,
    eauctions_url TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_potential_comparison_summary_created_at
    ON potential_comparison_summary (created_at);

CREATE TRIGGER update_potential_comparison_summary_updated_at
BEFORE UPDATE ON potential_comparison_summary
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ====================================================================
-- Views
-- ====================================================================
CREATE OR REPLACE VIEW potential_asset_statistics AS
SELECT 
    source,
    COUNT(*) AS potential_asset_count,
    AVG(price) AS avg_price,
    AVG(price / NULLIF(sqm, 0)) AS avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) AS min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) AS max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) AS stddev_price_per_sqm,
    AVG(sqm) AS avg_sqm
FROM potential_assets
GROUP BY source;


CREATE OR REPLACE VIEW comparison_asset_statistics AS
SELECT 
    source,
    COUNT(*) AS comparison_asset_count,
    AVG(price) AS avg_price,
    AVG(price / NULLIF(sqm, 0)) AS avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) AS min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) AS max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) AS stddev_price_per_sqm,
    AVG(sqm) AS avg_sqm
FROM comparison_assets
GROUP BY source;

