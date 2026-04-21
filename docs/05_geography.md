# 05 - Geography and PostGIS

Everything spatial - coordinates, polygons, radius searches, by-area
statistics - relies on PostGIS. This doc is a short but load-bearing
reference: the rules we follow, the datasets we load, and the SQL recipes
we reuse.

## The two PostGIS column types

PostGIS gives you `GEOMETRY` and `GEOGRAPHY`. We use both, deliberately.

### `GEOGRAPHY(POINT, 4326)` for listing locations

Every `location` column in a listings-style table
(`potential_assets.location`, `comparison_assets.location`,
`spitogatos_data.location`, `landea_assets.location`) is a
`GEOGRAPHY(POINT, 4326)`:

- `POINT` because a listing is a single lat/lon.
- `4326` because we store WGS84 (the standard world-wide lat/lon frame used
  by GPS, OSM, and every map tile provider).
- `GEOGRAPHY` (not `GEOMETRY`) because distances on `GEOGRAPHY` are reported
  in **meters** on the true ellipsoid. `ST_DWithin(geography, geography, 100)`
  means "within 100 meters", full stop - no projection, no SRID juggling.

Writing a point is always `ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography`.
Longitude first, then latitude - this is the PostGIS convention and it bites
anyone who mixes it up with the geographic `(lat, lon)` convention.

### `GEOMETRY(MULTIPOLYGON, 4326)` for administrative boundaries

Neighborhood and municipality polygons come from OSM and are loaded as
`GEOMETRY(MULTIPOLYGON, 4326)`. They stay in the planar `GEOMETRY` world
because:

- We never compute polygon-to-polygon distance.
- We only do containment / intersection, which is faster on `GEOMETRY`.
- We cast points to `::geometry` at query time when we intersect them with
  a polygon - see the pattern below.

Tables: `geography.attica_municipality.geom` and
`geography.athens_neighborhood.geom`.

## GIST indexes on every spatial column

Every spatial column carries a `GIST` index:

- `idx_potential_assets_location`
- `idx_comparison_assets_location`
- `idx_spitogatos_data_location`
- `idx_landea_assets_location`
- `idx_attica_municipality_geom`
- `idx_athens_neighborhood_geom`

Without the GIST index, a radius query degrades to a full table scan. When
you add a new spatial column, always add:

```sql
CREATE INDEX IF NOT EXISTS idx_<table>_<col>
    ON <table> USING GIST (<col>);
```

## How boundaries are loaded

Two one-shot Python scripts live in [database/](../database/). They read the
source GeoJSON files under [utils/scripts/](../utils/scripts/) and upsert into
the `geography.*` tables.

### Athens neighborhoods

- Source: [utils/scripts/athens_wgs84.json](../utils/scripts/athens_wgs84.json).
- Target: `geography.athens_neighborhood`.
- Loader: [database/load_athens_neighborhood.py](../database/load_athens_neighborhood.py).
- Key: `name_en`. Every query against this table joins on the English name.

The loader converts each feature's geometry to a PostGIS MULTIPOLYGON in a
single SQL expression:

```75:80:database/load_athens_neighborhood.py
    template = (
        "("
        "%s,%s,"
        "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
        ")"
    )
```

`ST_GeomFromGeoJSON(%s)` parses the JSON, `ST_SetSRID(..., 4326)` tags it as
WGS84, and `ST_Multi(...)` promotes single-polygon geometries to MULTIPOLYGON
(the column type demands MULTIPOLYGON, so a plain POLYGON would fail).

### Attica municipalities

- Source: [utils/scripts/attica_regions.geojson](../utils/scripts/attica_regions.geojson) (OSM/Overpass export).
- Target: `geography.attica_municipality`.
- Loader: [database/load_attica_municipality.py](../database/load_attica_municipality.py).
- Key: `osm_relation_id`. We also keep `name_el` and `name_en` for user-facing
  joins.

### Converting external GeoJSON to WGS84

If a source dataset is not in WGS84 already (for example the original Athens
neighborhood export), use
[utils/scripts/athens_neighborhoods_to_wgs84.py](../utils/scripts/athens_neighborhoods_to_wgs84.py)
to reproject it. Keep all polygon sources in SRID 4326 before loading - the
loader assumes it.

## Spatial-query cookbook

Copy these snippets when writing new DAO methods. They are lifted from
[database/spitogatos_dao.py](../database/spitogatos_dao.py) and
[database/geography_dao.py](../database/geography_dao.py).

### Insert a point

```sql
INSERT INTO spitogatos_data (..., location, ...)
VALUES (..., ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, ...)
```

Pass `(lon, lat)` in that order.

### Circle / radius filter (in meters)

```sql
WHERE ST_DWithin(
    location,
    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
    %s  -- radius in meters
)
```

Order results by actual distance:

```sql
ORDER BY ST_Distance(
    location,
    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
) ASC
```

### Bounding-box filter (rectangle)

Uses the `&&` operator on the GIST index; very fast:

```sql
WHERE location && ST_MakeEnvelope(
    %s,  -- min_lon
    %s,  -- min_lat
    %s,  -- max_lon
    %s,  -- max_lat
    4326
)::geography
```

`&&` is the bounding-box-overlap operator; `ST_Covers` / `ST_Within` are
stricter if you need exact containment.

### Point-in-polygon join (neighborhood / municipality)

```sql
FROM spitogatos_data s
JOIN geography.athens_neighborhood n
  ON n.name_en = %s
WHERE ST_Contains(n.geom, s.location::geometry)
```

Three things to notice:

- `n.geom` is `GEOMETRY(MULTIPOLYGON, 4326)`.
- `s.location::geometry` explicitly casts the GEOGRAPHY point to GEOMETRY so
  the operator matches.
- We filter by a cheap key (`n.name_en = %s`) before `ST_Contains`, which
  means PostgreSQL picks a single polygon row before doing any geometry
  math - orders of magnitude faster than "list all neighborhoods that
  contain this point".

### Polygon-to-all-points (batch stats)

For analytics we aggregate every listing grouped by containing polygon:

```sql
FROM spitogatos_data s
JOIN geography.athens_neighborhood n
  ON ST_Contains(n.geom, s.location::geometry)
```

This is the hot path in `analytics/generate_plots.py` and in
`SpitogatosDAO.get_area_summary`. Keep polygon tables small (dozens to
hundreds of rows) and point tables GIST-indexed - this join becomes O(points
x log polygons) in practice.

### Reading a point back

When returning a row that has a GEOGRAPHY point, extract `lon`/`lat` with
ST_X / ST_Y after casting to GEOMETRY:

```sql
SELECT
    ...,
    ST_X(location::geometry) AS longitude,
    ST_Y(location::geometry) AS latitude
FROM spitogatos_data
```

### Building GeoJSON directly from Postgres

Used by [database/geography_dao.py](../database/geography_dao.py) so the API
can return map-ready FeatureCollections without hand-assembling JSON in
Python:

```sql
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', COALESCE(jsonb_agg(
        jsonb_build_object(
            'type', 'Feature',
            'properties', jsonb_build_object('name_en', name_en) || tags,
            'geometry', ST_AsGeoJSON(geom)::jsonb
        )
        ORDER BY name_en
    ), '[]'::jsonb)
) AS geojson
FROM geography.athens_neighborhood
```

`ST_AsGeoJSON(geom)::jsonb` is much cheaper than parsing the polygon in
Python. Use it whenever you return map layers.

## Helpers above PostGIS

### `model.geographical_model`

The three primitives everything uses:

```1:16:model/geographical_model.py
from pydantic import BaseModel

class Rectangle(BaseModel):
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

class Circle(BaseModel):
    center_lat: float
    center_lon: float
    radius: float

class Point(BaseModel):
    lat: float
    lon: float
```

### `GeopyData.rectangle_from_point`

Builds a square bounding box around a point given a radius in meters -
useful when you have a point and a "radius" requirement but need to call a
rectangle-based API (like Spitogatos's map-search):

```17:24:data_source/geopy_data.py
    def rectangle_from_point(self, start_point: Point, radius_meters: float) -> Rectangle:
        diagonal_meters = math.sqrt(2 * math.pow(radius_meters, 2))
        left_up = distance(kilometers=diagonal_meters / 1000).destination(point=(start_point.lat, start_point.lon),
                                                                          bearing=45)
        down_bottom = distance(kilometers=diagonal_meters / 1000).destination(point=(start_point.lat, start_point.lon),
                                                                              bearing=225)
        return Rectangle(min_lat=down_bottom.latitude, min_lon=down_bottom.longitude, max_lat=left_up.latitude,
                         max_lon=left_up.longitude)
```

## Pitfalls to avoid

- **Do not cross coordinate orders.** It is `(lon, lat)` in SQL, `(lat, lon)`
  in Pydantic `Point` / `Circle`. Keep the conversion at the DAO boundary.
- **Do not drop the explicit cast** when joining GEOGRAPHY points against
  GEOMETRY polygons (`s.location::geometry`). Without it PostGIS either
  errors out or silently falls off the index.
- **Do not use `ST_Within` where `ST_Contains` is intended.** The argument
  order is opposite: `ST_Contains(polygon, point)` vs `ST_Within(point,
  polygon)`. Both work; pick one convention per file.
- **Do not load polygons without `ST_Multi`.** The `geom` columns are
  declared `MULTIPOLYGON`; a plain `POLYGON` insert fails.
- **Do not forget the GIST index** when adding a new spatial column. The
  radius search will silently degrade to sequential scan.
- **Keep polygons in WGS84.** Every radius query assumes `::geography`
  math; a rogue SRID=3857 polygon will give nonsense distances.
