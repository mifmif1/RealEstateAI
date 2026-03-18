-- ====================================================================
-- Schema: geography
-- Table: geography.athens_neighborhood (Athens neighborhood boundaries)
-- Source: utils/scripts/athens_wgs84.json
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS geography;

CREATE TABLE IF NOT EXISTS geography.athens_neighborhood (
    name_en TEXT PRIMARY KEY,

    tags JSONB NOT NULL DEFAULT '{}'::jsonb,

    geom GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_athens_neighborhood_geom
    ON geography.athens_neighborhood USING GIST (geom);

DROP TRIGGER IF EXISTS update_athens_neighborhood_updated_at ON geography.athens_neighborhood;
CREATE TRIGGER update_athens_neighborhood_updated_at
BEFORE UPDATE ON geography.athens_neighborhood
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

