-- ====================================================================
-- Table: landea_assets  (Landea listing snapshots)
-- One row per LandeaAsset; location for spatial queries (circle/rectangle)
-- ====================================================================
CREATE TABLE IF NOT EXISTS landea_assets (
    landea_id VARCHAR(100) PRIMARY KEY,
    url_id VARCHAR(200) NOT NULL,

    url TEXT,
    sqm FLOAT,
    lat FLOAT,
    lon FLOAT,
    location GEOGRAPHY(POINT, 4326),

    title TEXT,
    floor VARCHAR(50),
    is_hot BOOLEAN,
    price FLOAT,
    address TEXT,
    bedrooms INTEGER,
    auction_date VARCHAR(50),
    construction_year INTEGER,

    fetch_date TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_landea_assets_location
    ON landea_assets USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_landea_assets_sqm
    ON landea_assets (sqm);

CREATE INDEX IF NOT EXISTS idx_landea_assets_price
    ON landea_assets (price);

CREATE INDEX IF NOT EXISTS idx_landea_assets_fetch_date
    ON landea_assets (fetch_date);

CREATE OR REPLACE FUNCTION update_modified_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER update_landea_assets_modified_at
BEFORE UPDATE ON landea_assets
FOR EACH ROW EXECUTE FUNCTION update_modified_at_column();
