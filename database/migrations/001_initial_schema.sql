-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create assets table with spatial geometry column
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    sqm FLOAT NOT NULL,
    price FLOAT NOT NULL,
    url TEXT,
    level INTEGER,
    address TEXT,
    new_state BOOLEAN,
    searched_radius FLOAT,
    revaluated_price_meter FLOAT,
    source VARCHAR(50), -- e.g., 'spitogatos', 'reonline', 'eauctions'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index on location for fast spatial queries
CREATE INDEX IF NOT EXISTS idx_assets_location ON assets USING GIST (location);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_assets_sqm ON assets (sqm);
CREATE INDEX IF NOT EXISTS idx_assets_price ON assets (price);
CREATE INDEX IF NOT EXISTS idx_assets_source ON assets (source);
CREATE INDEX IF NOT EXISTS idx_assets_created_at ON assets (created_at);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_assets_updated_at BEFORE UPDATE ON assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create view for asset statistics by location
CREATE OR REPLACE VIEW asset_statistics AS
SELECT 
    source,
    COUNT(*) as asset_count,
    AVG(price) as avg_price,
    AVG(price / NULLIF(sqm, 0)) as avg_price_per_sqm,
    MIN(price / NULLIF(sqm, 0)) as min_price_per_sqm,
    MAX(price / NULLIF(sqm, 0)) as max_price_per_sqm,
    STDDEV(price / NULLIF(sqm, 0)) as stddev_price_per_sqm,
    AVG(sqm) as avg_sqm
FROM assets
GROUP BY source;

