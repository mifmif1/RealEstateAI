-- ====================================================================
-- Table: spitogatos_data  (Spitogatos listing snapshots)
-- One row per Spitogatos_Asset; location for spatial queries
-- ====================================================================
CREATE TABLE IF NOT EXISTS spitogatos_data (
    id VARCHAR(100) PRIMARY KEY,

    name TEXT NOT NULL,
    category VARCHAR(100) NOT NULL,
    subtype INTEGER NOT NULL,
    buy_or_rent INTEGER NOT NULL,
    sqm INTEGER NOT NULL,
    price INTEGER NOT NULL,
    price_reduced BOOLEAN NOT NULL,
    price_pre_reduction INTEGER,
    price_change_percentage INTEGER,
    main_image_url TEXT NOT NULL,
    geography VARCHAR(200) NOT NULL,
    geocode_type VARCHAR(50) NOT NULL,

    location GEOGRAPHY(POINT, 4326) NOT NULL,

    floor_number INTEGER NOT NULL,
    rooms INTEGER NOT NULL,
    total_rooms INTEGER NOT NULL,
    no_of_bathrooms INTEGER NOT NULL,
    kitchens INTEGER NOT NULL,
    living_rooms INTEGER NOT NULL,
    within_city_plan INTEGER NOT NULL,
    agricultural_use INTEGER NOT NULL,
    description TEXT NOT NULL,
    new_development INTEGER NOT NULL,
    website_modified TIMESTAMP NOT NULL,
    website_uploaded TIMESTAMP NOT NULL,
    image_ids INTEGER[] NOT NULL DEFAULT '{}',
    has_vtour BOOLEAN NOT NULL,
    has_video BOOLEAN NOT NULL,
    agent_id INTEGER NOT NULL,
    enquirer_id INTEGER NOT NULL,
    re_agent VARCHAR(200) NOT NULL,
    published VARCHAR(100) NOT NULL,
    first_publish_date TIMESTAMP NOT NULL,
    top_vip BOOLEAN NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_spitogatos_data_location
    ON spitogatos_data USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_spitogatos_data_sqm
    ON spitogatos_data (sqm);

CREATE INDEX IF NOT EXISTS idx_spitogatos_data_price
    ON spitogatos_data (price);

CREATE INDEX IF NOT EXISTS idx_spitogatos_data_website_modified
    ON spitogatos_data (website_modified);
