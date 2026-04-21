# 06 - API and Dashboard

This doc covers the two user-facing surfaces: the FastAPI service and the
Dash dashboard.

## FastAPI

A single module: [api/app.py](../api/app.py). Routers are grouped by prefix:

- `geography_router` - boundary GeoJSON layers + polygon-scoped stats.
- `spitogatos_router` - market listings, comparables, per-asset revaluation.
- `analytics_router` (mounted under `/spitogatos/analytics`) - summary /
  distribution / trend / relationship endpoints for the dashboard.
- `landea_router` - two-stage Landea pipeline triggers.
- Ungrouped: the Excel-enrichment endpoint `/reonline/add-sqm` and the
  catch-all `/`.

Heavy work is always wrapped in `fastapi.concurrency.run_in_threadpool(...)`
so long scrape / DB calls do not block the event loop.

### Running locally

```bash
python -m uvicorn api.app:app --reload
```

Dev server on http://localhost:8000. The bundled Swagger UI is at
http://localhost:8000/docs and ReDoc at http://localhost:8000/redoc. The root
`GET /` returns a JSON index of the canonical endpoints; keep it up to date
when you add a new route.

### Running in Docker

`docker compose up --build`. The api container runs
[docker-entrypoint.sh](../docker-entrypoint.sh) which executes all migrations
then `uvicorn api.app:app --host 0.0.0.0 --port 8000`. The compose file maps
port 8000 to the host.

### Endpoint inventory

- `GET /` - index payload listing canonical endpoints.
- `GET /geography/athens-neighborhoods` -> `GeoJsonFeatureCollection`. All
  Athens neighborhood polygons.
- `GET /geography/attica-municipalities` -> `GeoJsonFeatureCollection`. All
  Attica municipality polygons.
- `GET /geography/neighborhood-statistics?neighborhood_name_en=...` ->
  `AreaStatisticsModel` (404 if empty). Price/sqm summary for a polygon.
- `GET /geography/municipality-statistics?municipality_name_en=...` ->
  `AreaStatisticsModel`.
- `POST /spitogatos/get-all-athens?start_offset=0&max_pages=...` - kicks off
  the long-running Athens fetch loop in a thread-pool. Synchronous -
  completes only when the loop ends. Body is `{ "status", "message",
  "start_offset", "max_pages" }`.
- `GET /spitogatos/assets-by-circle?lon=&lat=&radius_meters=&...` -> list of
  `SpitogatosAsset`. Hard cap `limit=100` is applied by the flow.
- `GET /spitogatos/assets-by-athens-neighborhood?neighborhood_name_en=...`
  -> list of `SpitogatosAsset`.
- `GET /spitogatos/assets-by-attica-municipality?municipality_name_en=...`
  -> list of `SpitogatosAsset`.
- `GET /spitogatos/neighborhood-statistics?neighborhood_name_en=...` and
  `GET /spitogatos/municipality-statistics?municipality_name_en=...` ->
  `AreaStatisticsModel`. (Same shape as the `/geography/...-statistics`
  endpoints but routed through `SpitogatosFlow` instead of `GeographyFlow`
  - keeping the two paths lets the frontend choose which flow it trusts.)
- `POST /spitogatos/asset-statistics-by-radius` body `TargetAsset` with
  `radius_meters`, `min_assets`, `limit` query params
  -> `ComparisonDataModel`. Uses the radius-expansion search.
- `POST /spitogatos/asset-statistics-by-comparisons` body
  `{ asset: TargetAsset, comparison_assets: List[SpitogatosAsset] }`
  -> `ComparisonDataModel`. Skip the search, caller provides the
  comparables.
- `GET /spitogatos/analytics/table-distribution?metric=...` ->
  `TableDistributionPayload`. Summary rows + histogram series per area.
- `GET /spitogatos/analytics/trends?metric=...&granularity=day|week|month`
  -> `TrendPayload`.
- `GET /spitogatos/analytics/relationships?x_metric=...&y_metric=...&sample_limit=3000`
  -> `RelationshipPayload`.
- `POST /reonline/add-sqm` - multipart `.xlsx`/`.xlsb` upload with a `Link`
  column. Returns the enriched Excel as a `FileResponse`.
- `POST /landea/run-stage1?start_page=1&max_pages=...` - crawl Landea search
  results and upsert into `landea_assets`.
- `POST /landea/run-stage2?batch_size=100` - enrich rows missing
  coordinates.

All date filters share the same four query parameters across endpoints:
`website_modified_from`, `website_modified_to`, `website_uploaded_from`,
`website_uploaded_to`. Dates are ISO-8601 and the filters are all inclusive
(`>=` for `_from`, `<=` for `_to`).

### Error shape

FastAPI returns `HTTPException(detail=...)` as `{ "detail": "..." }`:

- `422` - a metric/granularity not in `_VALID_METRICS` / `{day, week,
  month}`. Raised as `ValueError` in the flow, rewritten to 422 in the
  route.
- `404` - `None` returned from a flow's stats function (no assets).
- `500` - everything else, with the exception message appended.

### Uploaded files

`/reonline/add-sqm` and any future Excel endpoints write to
`api/uploads/` (created on startup). The current `add_sqm` flow saves a
timestamped output file like
`input_<name>_sqm_enrich_10122025-1542.xlsx` and returns it as a
`FileResponse`. Do not expose `api/uploads/` over static hosting - it is a
disposable scratch directory.

### Adding a new endpoint (checklist)

1. Implement the business logic in a flow method. Accept Pydantic models,
   not dicts. Return Pydantic models or primitives.
2. If the flow needs a new query, add it to the relevant DAO and (if
   applicable) the analytics data facade.
3. Add a route in [api/app.py](../api/app.py) under the right router.
   Always wrap the call in `run_in_threadpool` - the flow may be slow.
4. Set `response_model=...` so the OpenAPI schema is precise.
5. Map flow-layer `ValueError` to 422, `None` to 404 if semantically
   appropriate, everything else to 500.
6. Register the route in the `GET /` index payload and keep the `include_router`
   order at the bottom of `api/app.py` alphabetic.
7. Add an entry to this doc's endpoint inventory.

## Dash dashboard

[dashboard/app.py](../dashboard/app.py) is a single-file Dash app nicknamed
"VAR Opportunity Explorer". It loads the comparison Excel at
`excel_db/all_assets.xlsx` (resolved from the repo root) and presents:

- A header card with filters (portfolio, source, price, sqm, score, discount).
- A map with markers coloured by `price_avg_discount_pct` and sized by
  `price`.
- A summary KPI row: count, mean price, mean discount, etc.
- A paged, sortable table with raw and derived columns.
- A per-asset side panel with a link out to Spitogatos and eAuctions.
- An analytics section appended at line 1198 that queries the
  `/spitogatos/analytics/*` endpoints so the same data backs both the
  offline PNGs and the live dashboard.

### Running locally

```bash
python dashboard/app.py
```

Starts on http://localhost:8050 with `debug=True`. The dashboard reads the
Excel file lazily through `@lru_cache(maxsize=1)` - restart the process (or
clear the cache) after you regenerate the file.

### Data source

The dashboard primarily loads `excel_db/all_assets.xlsx` - a by-hand output
from our offline comparison pipeline that merges potential assets with
their revaluation summary. It also has a path at
`040326-assets-stats.xlsx` in older revisions; the current file name is
`all_assets.xlsx`. Do not rename it without updating the `DATA_PATH`
constant at the top of [dashboard/app.py](../dashboard/app.py):

```21:21:dashboard/app.py
DATA_PATH = Path(__file__).resolve().parents[1] / "excel_db" / "all_assets.xlsx"
```

For analytics sections the dashboard calls the FastAPI analytics endpoints
over HTTP (`urllib.request`) against `localhost:8000` by default. Set
`REALESTATEAI_API_BASE` if you run the api on a different host/port.

### Derived columns (documented once, use everywhere)

The dashboard derives a handful of business metrics from the raw comparison
columns:

- `price_per_sqm` - `pd.to_numeric(df["price/sqm"])`.
- `price-market_discount` - `price_under_market * 100`. Pre-computed
  server-side; the dashboard just scales it.
- `price_avg_discount_pct` - `(comparison_average - price_per_sqm) /
  comparison_average * 100`. Guarded with
  `comparison_average.replace({0: pd.NA})` because 0 means "no
  comparables were found", not "free market".
- Coordinates - `lat`/`lon` are extracted once with
  `parse_coordinate_pair(coords)` if the Excel only has the combined
  `coords` string; the parser handles both `"37.9838, 23.7275"` decimal
  form and DMS form (`37°59'01"N, 23°43'39"E`).

### Adding a new column to the dashboard

1. Add the column to whatever pipeline writes `all_assets.xlsx`.
2. Coerce it numerically at the top of `load_dataset()` -
   `df["<col>"] = pd.to_numeric(df.get("<col>"), errors="coerce")`.
3. Add it to the `DataTable` column config (see the `columns=[...]` list).
4. If it is a filter, add a `dbc.Input`/`dcc.RangeSlider` and wire it into
   the main callback's `State`/`Input` lists.
5. If it is a map-colour source, make sure the value is bounded - clamp
   with `.clip(-100, 100)` before feeding it to Plotly.

### Known gotchas

- `app.layout.children.append(_analytics_section)` at line 1330 mutates the
  layout after it is created; this relies on `app.layout` being a
  `dbc.Container`. If you change the root component, update that append.
- The dashboard does not require the API for the core map/table view (it
  reads Excel directly). The analytics section silently degrades to empty
  if `http://localhost:8000` is unreachable - check the panel is hidden
  before assuming a bug.
- `app.run(debug=True)` at the bottom of the file enables hot reload; do not
  enable this in production.

## Putting it together

In a typical workflow you will:

1. Start the DB: `docker compose up db`.
2. Start the API (fresh migrations + hot reload): `uvicorn api.app:app
   --reload`.
3. Start the dashboard in another shell: `python dashboard/app.py`.
4. When the comparison Excel is regenerated (by the offline pipeline),
   refresh the browser tab.
5. When analytics queries feel stale, rerun `python
   analytics/generate_plots.py` (or via `docker exec realestateai-api ...`).
