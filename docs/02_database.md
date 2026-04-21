# 02 - Database

This document covers everything you need to know about the persistence layer:
tech stack, how we connect, every migration, every table, the DAO patterns we
reuse, and the issues we hit and solved along the way.

## Tech stack

- PostgreSQL 16 with the PostGIS 3.4 extension. In Docker we use
  `postgis/postgis:16-3.4` (see [docker-compose.yml](../docker-compose.yml)).
- `psycopg2-binary` for the driver, `psycopg2.extras.execute_values` for
  bulk upserts, `psycopg2.pool.ThreadedConnectionPool` for pooling.
- `SQLAlchemy` is listed in [requirements.txt](../requirements.txt) but the
  active code path is raw psycopg2 against a pool. Do not introduce the
  SQLAlchemy ORM without a very good reason - keep SQL explicit.

## Configuration

Connection parameters are driven entirely by environment variables, with
localhost defaults suitable for running against a local Postgres:

```9:27:database/config.py
@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', '5432'))
    database: str = os.getenv('DB_NAME', 'realestate_ai')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', 'postgres')

    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def psycopg2_connection_string(self) -> str:
        """Get connection string for psycopg2"""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"
```

The Docker compose file injects `DB_HOST=db` so the api container talks to
the `postgis/postgis` container over the compose network. A large
pre-populated dump is included at the repo root (`spitogatos_backup.sql`) and
can be restored with `psql -U postgres -d realestate_ai -f spitogatos_backup.sql`.

## Connecting

Always go through the singleton pool, never `psycopg2.connect` directly:

```93:98:database/connection.py
def get_db_connection() -> DatabaseConnection:
    """Get or create the global database connection instance"""
    global _db_connection
    if _db_connection is None:
        _db_connection = DatabaseConnection()
    return _db_connection
```

`DatabaseConnection` gives you three ways to talk to Postgres:

- `get_connection()` - low-level context manager. Use it only when you need
  manual transaction control.
- `get_cursor(dict_cursor=True)` - commits on exit, rolls back on exception,
  returns a `RealDictCursor` by default. Use this for all SELECTs where you
  want keyed results.
- `execute_query(query, params)` / `execute_update(query, params)` - one-shot
  convenience wrappers for simple statements.

The pool is configured with `minconn=1, maxconn=10` (
[database/connection.py](../database/connection.py)). One DAO instance ==
one pool reference.

## Migrations

All schema changes live under [database/migrations/](../database/migrations/)
as SQL files with a numeric prefix. They are idempotent (`IF NOT EXISTS`,
`CREATE OR REPLACE`, `DROP TRIGGER IF EXISTS ... CREATE TRIGGER`) so re-running
them is safe.

[database/setup.py](../database/setup.py) runs every file in sorted order:

```51:73:database/setup.py
def setup_database():
    """Set up the database schema"""
    config = get_db_config()

    try:
        logger.info(f"Connecting to database: {config.database}")
        conn = psycopg2.connect(config.psycopg2_connection_string)

        # Get migration files
        migrations_dir = Path(__file__).parent / "migrations"
        migration_files = sorted(migrations_dir.glob("*.sql"))

        if not migration_files:
            logger.warning("No migration files found")
            return

        logger.info(f"Found {len(migration_files)} migration file(s)")

        # Run migrations
        for migration_file in migration_files:
            run_migration(migration_file, conn)
```

The Docker entrypoint calls this on every container start before launching
uvicorn, so the API is always against a migrated DB:

```1:5:docker-entrypoint.sh
#!/bin/sh
echo "Running database migrations..."
python -m database.setup
echo "Starting application..."
exec "$@"
```

### Adding a new migration

1. Pick the next numeric prefix (e.g. `006_foo.sql`).
2. Use only idempotent statements: `CREATE TABLE IF NOT EXISTS`,
   `CREATE INDEX IF NOT EXISTS`, `DROP TRIGGER IF EXISTS ... CREATE TRIGGER`,
   `CREATE OR REPLACE FUNCTION`.
3. Do not embed `DROP TABLE` unless you really mean it (there is no rollback
   file).
4. If you add a spatial column, always declare it as
   `GEOGRAPHY(POINT, 4326)` (or `GEOMETRY(MULTIPOLYGON, 4326)` for boundary
   polygons) and add a GIST index.
5. Run `python database/setup.py` locally and confirm it is green.
6. If the new table has an `updated_at` column, wire it into the shared
   `update_updated_at_column()` trigger defined in `001_initial_schema.sql`.

### Migration inventory

- [001_initial_schema.sql](../database/migrations/001_initial_schema.sql) -
  `CREATE EXTENSION postgis`, the `update_updated_at_column()` trigger, and
  the core tables `potential_assets`, `comparison_assets`, and the
  aggregated summary `potential_comparison_summary`. Also creates the two
  views `potential_asset_statistics` and `comparison_asset_statistics`.
- [002_spitogatos_data.sql](../database/migrations/002_spitogatos_data.sql) -
  `spitogatos_data` snapshot table with 30+ columns and a GIST index on
  `location`.
- [003_landea_assets.sql](../database/migrations/003_landea_assets.sql) -
  `landea_assets` table, plus its own `update_modified_at_column()` trigger.
- [004_geography_attica_municipality.sql](../database/migrations/004_geography_attica_municipality.sql) -
  creates the `geography` schema and the `geography.attica_municipality`
  table (MULTIPOLYGON + JSONB tags).
- [005_geography_athens_neighborhood.sql](../database/migrations/005_geography_athens_neighborhood.sql) -
  `geography.athens_neighborhood` table, keyed by `name_en`.

## Schema reference

### `potential_assets` (from 001)

The deals we own or consider buying. One row per asset.

- `id SERIAL PRIMARY KEY` - our internal id.
- `location GEOGRAPHY(POINT, 4326) NOT NULL` - WGS84 point. GIST-indexed
  (`idx_potential_assets_location`).
- `sqm FLOAT NOT NULL`, `price FLOAT NOT NULL`.
- `level INTEGER`, `parking BOOLEAN`, `construction_year INTEGER`.
- `source VARCHAR(50)` - where the asset came from (e.g. `byhand`, `landea`,
  `spitogatos`).
- `portfolio VARCHAR(50)` - business portfolio grouping.
- `source_unique_code VARCHAR(100)` - the source's own id to let us dedupe.
- `title`, `description`, `address TEXT`, `municipality`, `prefecture
  VARCHAR(100)`, `url TEXT`.
- `created_at` / `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`.
- Indexes on `sqm`, `price`, `source`, `portfolio`,
  `(source, portfolio, source_unique_code)`, `created_at` and spatial GIST on
  `location`.
- Trigger `update_potential_assets_updated_at` keeps `updated_at` fresh on
  UPDATE.

### `comparison_assets` (from 001)

Identical shape to `potential_assets` but for market comparables.
[database/asset_dao.py](../database/asset_dao.py) inserts into this table
when given a generic `TargetAsset`. Indexes and triggers mirror
`potential_assets`.

### `potential_comparison_summary` (from 001)

One row per potential asset; this is the aggregated revaluation result.

- `potential_asset_id INTEGER PRIMARY KEY REFERENCES potential_assets(id)
  ON DELETE CASCADE`.
- `assets_count INTEGER` - number of comparables used.
- `comparison_average`, `comparison_min`, `comparison_max`, `comparison_median`,
  `comparison_std FLOAT` - raw price-per-sqm stats from comparables.
- `normalized_mean FLOAT` - mean adjusted for floor / renewal.
- `revaluated_price_meter FLOAT` - our fair price per sqm.
- `revaluation_total_price FLOAT` - `revaluated_price_meter * sqm`.
- `max_buy_price FLOAT` - the highest price we would still pay.
- `score FLOAT` - composite ranking number used in the dashboard.
- `searched_radius FLOAT` - the final radius used by the radius-expansion
  search (see [04_statistics_and_revaluation.md](04_statistics_and_revaluation.md)).
- `spitogatos_url TEXT`, `eauctions_url TEXT` - convenience deep links.
- `created_at` / `updated_at`. Trigger
  `update_potential_comparison_summary_updated_at` keeps `updated_at` fresh.

### Views from 001

`potential_asset_statistics` and `comparison_asset_statistics` give you a
per-`source` rollup: count, `AVG(price)`, `AVG(price/sqm)`, min/max/stddev
price per sqm, `AVG(sqm)`. Useful for one-off SQL sanity checks.

### `spitogatos_data` (from 002)

Full Spitogatos listing snapshot - the main market-comparables table. Key
columns:

- `id VARCHAR(100) PRIMARY KEY` - **Spitogatos's** own id (not a SERIAL).
  This is what makes the `ON CONFLICT (id) DO UPDATE` upsert work.
- `location GEOGRAPHY(POINT, 4326) NOT NULL`, GIST-indexed.
- Commerce/shape columns: `category`, `subtype`, `buy_or_rent`, `sqm INTEGER`,
  `price INTEGER`, `price_reduced BOOLEAN`, `price_pre_reduction`,
  `price_change_percentage`, `main_image_url`, `geography VARCHAR(200)`,
  `geocode_type VARCHAR(50)`.
- Building details: `floor_number`, `rooms`, `total_rooms`,
  `no_of_bathrooms`, `kitchens`, `living_rooms`, `within_city_plan`,
  `agricultural_use`, `new_development`.
- Listing metadata: `description TEXT`, `website_modified`,
  `website_uploaded`, `image_ids INTEGER[]`, `has_vtour`, `has_video`,
  `agent_id`, `enquirer_id`, `re_agent`, `published`, `first_publish_date`.
- Ingest timestamps: `fetched_date`, `modified_date`.
- Indexes: GIST on `location`, plus B-tree on `sqm`, `price`, and
  `website_modified`.

Note the type mismatch with `potential_assets` / `comparison_assets`:
`spitogatos_data.id` is `VARCHAR(100)`, the others are `SERIAL`. Do not join
these ids naively.

### `landea_assets` (from 003)

Two-stage ingestion: stage 1 fills the listing-level fields, stage 2 enriches
with coordinates from the detail pages. Key columns:

- `landea_id VARCHAR(100) PRIMARY KEY`, `url_id VARCHAR(200) NOT NULL`.
- `sqm FLOAT`, `lat FLOAT`, `lon FLOAT`,
  `location GEOGRAPHY(POINT, 4326)` (nullable - set on enrichment).
- `price FLOAT`, `title`, `floor VARCHAR(50)`, `is_hot BOOLEAN`,
  `bedrooms`, `bathrooms`, `construction_year`, `features TEXT[]`,
  `description`, `auction_date VARCHAR(50)`.
- Trigger `update_landea_assets_modified_at` keeps `modified_at` fresh; note
  this migration defines its own `update_modified_at_column()` function
  instead of reusing the shared one from 001.

### `geography.attica_municipality` (from 004)

Attica administrative boundaries loaded from
[utils/scripts/attica_regions.geojson](../utils/scripts/attica_regions.geojson).

- `osm_relation_id BIGINT PRIMARY KEY` - OSM relation id.
- `name_el TEXT NOT NULL`, `name_en TEXT` - used as the join key from
  `SpitogatosDAO.search_by_attica_municipality`.
- `admin_level SMALLINT`, `boundary TEXT`, `ref TEXT`, `population`,
  `website`, `wikidata`, `wikipedia TEXT`.
- `tags JSONB NOT NULL DEFAULT '{}'::jsonb` - all remaining OSM tags.
- `geom GEOMETRY(MULTIPOLYGON, 4326) NOT NULL`, GIST-indexed.
- Trigger `update_attica_municipality_updated_at`.

### `geography.athens_neighborhood` (from 005)

Athens neighborhoods loaded from
[utils/scripts/athens_wgs84.json](../utils/scripts/athens_wgs84.json).

- `name_en TEXT PRIMARY KEY`.
- `tags JSONB NOT NULL DEFAULT '{}'::jsonb`.
- `geom GEOMETRY(MULTIPOLYGON, 4326) NOT NULL`, GIST-indexed.
- Trigger `update_athens_neighborhood_updated_at`.

## DAO patterns to reuse

Here are the patterns you should copy when adding a new DAO. They are all
lifted verbatim from `database/*_dao.py`.

### Bulk upsert with `execute_values` + `ON CONFLICT DO UPDATE`

This is how we ingest every scraped listing. From
[database/spitogatos_dao.py](../database/spitogatos_dao.py):

```43:96:database/spitogatos_dao.py
        query = """
            INSERT INTO spitogatos_data (
                id, category, subtype, buy_or_rent, sqm, price,
                price_reduced, price_pre_reduction, price_change_percentage,
                main_image_url, geography, geocode_type, location,
                floor_number, rooms, total_rooms, no_of_bathrooms, kitchens,
                living_rooms, within_city_plan, agricultural_use, description,
                new_development, website_modified, website_uploaded, image_ids,
                has_vtour, has_video, agent_id, enquirer_id, re_agent,
                published, first_publish_date
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                category = EXCLUDED.category,
                subtype = EXCLUDED.subtype,
                buy_or_rent = EXCLUDED.buy_or_rent,
                sqm = EXCLUDED.sqm,
                price = EXCLUDED.price,
                price_reduced = EXCLUDED.price_reduced,
                price_pre_reduction = EXCLUDED.price_pre_reduction,
                price_change_percentage = EXCLUDED.price_change_percentage,
                main_image_url = EXCLUDED.main_image_url,
                geography = EXCLUDED.geography,
                geocode_type = EXCLUDED.geocode_type,
                location = EXCLUDED.location,
                floor_number = EXCLUDED.floor_number,
                rooms = EXCLUDED.rooms,
                total_rooms = EXCLUDED.total_rooms,
                no_of_bathrooms = EXCLUDED.no_of_bathrooms,
                kitchens = EXCLUDED.kitchens,
                living_rooms = EXCLUDED.living_rooms,
                within_city_plan = EXCLUDED.within_city_plan,
                agricultural_use = EXCLUDED.agricultural_use,
                description = EXCLUDED.description,
                new_development = EXCLUDED.new_development,
                website_modified = EXCLUDED.website_modified,
                website_uploaded = EXCLUDED.website_uploaded,
                image_ids = EXCLUDED.image_ids,
                has_vtour = EXCLUDED.has_vtour,
                has_video = EXCLUDED.has_video,
                agent_id = EXCLUDED.agent_id,
                enquirer_id = EXCLUDED.enquirer_id,
                re_agent = EXCLUDED.re_agent,
                published = EXCLUDED.published,
                first_publish_date = EXCLUDED.first_publish_date,
                modified_date = CURRENT_TIMESTAMP
        """
```

Things to notice and copy:

- `VALUES %s` combined with `psycopg2.extras.execute_values(..., template=...,
  page_size=500)` gives us efficient multi-row inserts.
- The per-row template inlines `ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography`
  so the spatial type is constructed in SQL, not in Python.
- `ON CONFLICT (id) DO UPDATE SET <every column> = EXCLUDED.<col>,
  modified_date = CURRENT_TIMESTAMP` makes the call idempotent - refetching
  the same page just bumps the ingest timestamp.
- Right before the upsert we fetch the current rows for the batched ids and
  log a diff of every changed field. This is invaluable when debugging
  scraper regressions.

### Rectangle search

`location && ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)::geography`
uses the GIST index for a fast bounding-box filter:

```234:242:database/spitogatos_dao.py
        where_clauses = [
            "location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography"
        ]
        params = [
            rectangle.min_lon,
            rectangle.min_lat,
            rectangle.max_lon,
            rectangle.max_lat,
        ]
```

### Circle / radius search

`ST_DWithin(location, center::geography, radius_meters)` is the canonical
radius query. Because `location` is `GEOGRAPHY`, the radius is in meters -
no projection required:

```305:316:database/spitogatos_dao.py
        where_clauses = [
            "ST_DWithin("
            "location, "
            "ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, "
            "%s"
            ")"
        ]
        params = [
            circle.center_lon,
            circle.center_lat,
            circle.radius,
        ]
```

The same query also computes a `distance` column via
`ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography)` so
results can be ordered nearest-first.

### Polygon join (neighborhood / municipality)

Assets-within-polygon uses `ST_Contains` against the polygon table. Note that
we cast `location::geometry` because PostGIS only lets you intersect a
GEOGRAPHY polygon against a GEOMETRY polygon by explicit cast. From
[database/spitogatos_dao.py](../database/spitogatos_dao.py):

```352:368:database/spitogatos_dao.py
        base_query = """
            SELECT
                s.id, s.category, s.subtype, s.buy_or_rent, s.sqm, s.price,
                s.price_reduced, s.price_pre_reduction, s.price_change_percentage,
                s.main_image_url, s.geography, s.geocode_type,
                ST_X(s.location::geometry) AS longitude,
                ST_Y(s.location::geometry) AS latitude,
                s.floor_number, s.rooms, s.total_rooms, s.no_of_bathrooms, s.kitchens,
                s.living_rooms, s.within_city_plan, s.agricultural_use, s.description,
                s.new_development, s.website_modified, s.website_uploaded, s.image_ids,
                s.has_vtour, s.has_video, s.agent_id, s.enquirer_id, s.re_agent,
                s.published, s.first_publish_date
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON n.name_en = %s
            WHERE ST_Contains(n.geom, s.location::geometry)
        """
```

### GeoJSON aggregation straight from Postgres

For map layers we build the GeoJSON in Postgres rather than in Python; this
keeps the payload deterministic and lets us stream straight out to the HTTP
client:

```27:42:database/geography_dao.py
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(jsonb_agg(
                    jsonb_build_object(
                        'type', 'Feature',
                        'properties', jsonb_build_object(
                            'name_en', name_en
                        ) || tags,
                        'geometry', ST_AsGeoJSON(geom)::jsonb
                    )
                    ORDER BY name_en
                ), '[]'::jsonb)
            ) AS geojson
            FROM geography.athens_neighborhood
        """
```

### Loading GeoJSON into the DB

The one-shot loader at
[database/load_athens_neighborhood.py](../database/load_athens_neighborhood.py)
is the template for bulk-loading any polygon dataset:

- Read the file with `json.loads(path.read_text(encoding="utf-8"))`.
- `psycopg2.extras.Json(props)` safely handles the JSONB column.
- `ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))` converts GeoJSON to a
  PostGIS MULTIPOLYGON in one expression.
- Use `execute_values(..., template=...)` so the per-row SQL expression is
  applied batch-wide. The template is important because the GeoJSON column
  can't be passed as a plain `%s`.

## Known issues and lessons learned

- **PostGIS must be enabled first.** `001_initial_schema.sql` opens with
  `CREATE EXTENSION IF NOT EXISTS postgis;`. If you are setting up a fresh
  cluster by hand, that statement requires superuser privileges - either
  enable the extension as postgres, or bake it into the image (we use
  `postgis/postgis:16-3.4` for exactly this reason).
- **`id` semantics differ across tables.** `spitogatos_data.id` is
  `VARCHAR(100)` (the site's id), while `potential_assets.id` and
  `comparison_assets.id` are `SERIAL`. Never treat them interchangeably.
- **`comparison_average = 0` means "no comparison".** The dashboard has to
  guard against this before computing discount percentages:

  ```102:106:dashboard/app.py
  comparison_safe = df["comparison_average"].replace({0: pd.NA})
  df["price_avg_discount_pct"] = (
      (comparison_safe - df["price_per_sqm"]) / comparison_safe
  ) * 100
  ```

- **`get_area_summary` had a slow correlated-subquery path.**
  [analytics/generate_plots.py](../analytics/generate_plots.py) bypasses the
  flow layer and queries psycopg2 directly; its header explains this. When
  writing new batch analytics, prefer CTE + window / `PERCENTILE_CONT`
  aggregates over correlated subqueries (see the `fetch_summary_df` CTE in
  [analytics/generate_plots.py](../analytics/generate_plots.py)).
- **Windows cp1252 logging.** On a Windows console, logging a Greek-language
  diff can crash the process. The DAO wraps every potentially-Greek log
  string through `_safe_log_text()` before calling `logger.info`. Copy that
  helper when you add new diff logging.
- **"command cannot affect row a second time".** If a single call to
  `execute_values` contains two rows with the same conflicting key, Postgres
  aborts the whole statement. The DAO guards against this by collecting
  `ids_in_batch = [a.id for a in assets]` and relying on the caller
  deduping; when you add new bulk upsert DAOs, dedupe on the conflict key
  before executing.
- **Large SQL artifacts from AI edits.** `database/setup.py` filters out
  `C```assistant to=functions.ApplyPatch` sentinels that occasionally got
  appended to migration files during AI-assisted edits. If you ever see a
  migration fail mid-file, check for that sentinel.
- **`SQLAlchemy` vs `psycopg2`.** `sqlalchemy>=2.0.0` is in
  [requirements.txt](../requirements.txt) but we do not use the ORM. Treat
  it as dead weight; do not import from it in new code.

## Debugging cheat sheet

- `pg_isready -h localhost -p 5432` - quick Postgres health check.
- `psql -d realestate_ai -c "SELECT PostGIS_version();"` - confirm the
  extension is enabled.
- `psql -l | grep realestate_ai` - confirm the database exists.
- `psql -d realestate_ai -c "SELECT COUNT(*) FROM spitogatos_data;"` -
  smoke-test ingest.
- `docker compose logs db` - see Postgres logs when migrations fail.
- `docker exec -it realestateai-db psql -U postgres -d realestate_ai` - get
  an interactive `psql` prompt inside the container.
