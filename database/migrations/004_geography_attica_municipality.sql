-- ====================================================================
-- Schema: geography
-- Table: geography.attica_municipality (Attica administrative boundaries)
-- Source: utils/scripts/attica_regions.geojson (OSM/Overpass export)
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS geography;

CREATE TABLE IF NOT EXISTS geography.attica_municipality (
    osm_relation_id BIGINT PRIMARY KEY,

    admin_level SMALLINT,
    boundary TEXT,

    name_el TEXT NOT NULL,
    name_en TEXT,
    sorting_name TEXT,
    ref TEXT,

    population INTEGER,
    website TEXT,
    wikidata TEXT,
    wikipedia TEXT,

    tags JSONB NOT NULL DEFAULT '{}'::jsonb,

    geom GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_attica_municipality_geom
    ON geography.attica_municipality USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_attica_municipality_admin_level
    ON geography.attica_municipality (admin_level);

CREATE INDEX IF NOT EXISTS idx_attica_municipality_name_en
    ON geography.attica_municipality (name_en);

CREATE TRIGGER update_attica_municipality_updated_at
BEFORE UPDATE ON geography.attica_municipality
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

