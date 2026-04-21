# 07 - Operations and Workflows

How to run the system locally, how to run it in Docker, the common developer
workflows as numbered checklists, the conventions we already follow, and
every known issue / open TODO worth knowing about.

## Local setup (host venv)

Prerequisites: Python >= 3.11 (3.12 in the Docker image), PostgreSQL 16 and
PostGIS 3.x running locally. On Windows the easiest path is to use the
`postgis/postgis:16-3.4` container from
[docker-compose.yml](../docker-compose.yml) and run the Python side against
it.

1. Create and activate a virtualenv.

   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1   # PowerShell
   # source .venv/bin/activate    # bash / zsh
   ```

2. Install dependencies.

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. Export DB environment variables if the defaults don't match your local
   Postgres (see [database/config.py](../database/config.py)):

   ```powershell
   $env:DB_HOST = "localhost"
   $env:DB_PORT = "5432"
   $env:DB_NAME = "realestate_ai"
   $env:DB_USER = "postgres"
   $env:DB_PASSWORD = "postgres"
   ```

4. Run migrations.

   ```bash
   python -m database.setup
   ```

5. Optional: restore the bundled data snapshot.

   ```bash
   psql -U postgres -d realestate_ai -f spitogatos_backup.sql
   ```

6. Optional: load polygon datasets if they are not already in the snapshot.

   ```bash
   python -m database.load_attica_municipality
   python -m database.load_athens_neighborhood
   ```

7. Start the services you need.

   ```bash
   python -m uvicorn api.app:app --reload         # FastAPI on :8000
   python dashboard/app.py                        # Dash on :8050
   python analytics/generate_plots.py             # offline PNGs
   ```

## Docker setup

`docker compose up --build` brings up two containers:

- `realestateai-db` - the `postgis/postgis:16-3.4` image, seeded with the
  schema via `python -m database.setup` (the entrypoint runs it on every
  api container start, against the same DB).
- `realestateai-api` - the image built from [Dockerfile](../Dockerfile). The
  entrypoint runs migrations and then `uvicorn api.app:app --host 0.0.0.0
  --port 8000`.

Typical commands:

- Build / start: `docker compose up --build`.
- Start / detach: `docker compose up -d`.
- Stream API logs: `docker compose logs -f api`.
- Rebuild api only after `requirements.txt` changes:
  `docker compose build api && docker compose up -d api`.
- Regenerate plots in the api container:
  `docker exec realestateai-api python analytics/generate_plots.py`.
- Open a psql shell inside the DB container:
  `docker exec -it realestateai-db psql -U postgres -d realestate_ai`.
- Wipe everything and start fresh: `docker compose down -v` (the `-v` drops
  the `db_data` volume!).

The compose file mounts the repo into `/app` in the api container
(`volumes: - .:/app`), so code changes on the host are live.

## Developer workflows

### 1. Add a new scraper for a new site

1. Draft a Pydantic model at `model/<site>_asset_model.py` that captures
   **only** the fields you will persist. Keep field names snake_case.
2. Create `data_source/<site>_data.py` with a single class (`<Site>Data` or
   `<Site>Scraper`). Use `requests.Session` and set headers from
   [utils/consts/apis.py](../utils/consts/apis.py) (add constants if the
   site needs its own). Return typed objects.
3. Write a migration `database/migrations/00N_<site>_assets.sql` with a
   PRIMARY KEY on the site's natural id, a `GEOGRAPHY(POINT, 4326) location`
   column (GIST-indexed), and the shared `update_updated_at_column()`
   trigger.
4. Add `database/<site>_dao.py` with `insert_list` using
   `ON CONFLICT (id) DO UPDATE` + `execute_values`.
5. Add `flow/<site>_flow.py` orchestrating "fetch -> upsert -> log".
6. Expose it on FastAPI with a thin `POST /<site>/...` route in
   [api/app.py](../api/app.py) that calls the flow via
   `run_in_threadpool`.
7. Document the site in [03_scraping.md](03_scraping.md) and add it to the
   index.

### 2. Refresh Spitogatos data

1. Make sure DB is up (locally or via `docker compose up db`).
2. Refresh the cookie in `utils/consts/apis.py:ApisConsts.SPITOGATOS_COOKIE`
   if it has expired.
3. Trigger the fetch:

   ```bash
   curl -X POST "http://localhost:8000/spitogatos/get-all-athens?start_offset=0"
   # or from Python:
   python - <<'PY'
   from flow.spitogatos_flow import SpitogatosFlow
   SpitogatosFlow().fetch_all_athens(start_offset=0)
   PY
   ```

4. Watch `debug.log` (created by `SpitogatosFlow`'s `logging.basicConfig`)
   for "Persisted Athens page offset=... N rows affected" lines.
5. Sanity check: `SELECT COUNT(*), MAX(website_modified) FROM spitogatos_data;`.
6. Regenerate offline analytics: `python analytics/generate_plots.py` (or
   `docker exec realestateai-api python analytics/generate_plots.py`).

### 3. Add a revaluation parameter

1. Edit `reevaluate_asset_by_comparisons` in
   [flow/spitogatos_flow.py](../flow/spitogatos_flow.py). Keep static rank
   tables at the top of the function so reviewers can see them.
2. If the parameter is a new input (e.g. `asset.parking`), add it to
   [model/asset_model.py](../model/asset_model.py) as `Optional`.
3. If the parameter affects output, extend
   [model/comparison_data_model.py](../model/comparison_data_model.py) with
   a new optional field.
4. Migration: add a column to `potential_comparison_summary` (NULL default
   so existing rows stay valid) - see
   [001_initial_schema.sql](../database/migrations/001_initial_schema.sql)
   for the existing columns.
5. Wire the DAO that writes `potential_comparison_summary` to include the
   new column.
6. Add a numeric coercion in [dashboard/app.py](../dashboard/app.py)
   (`df["<col>"] = pd.to_numeric(df.get("<col>"), errors="coerce")`) and
   expose the column in the `DataTable`.
7. Update the formula section of
   [04_statistics_and_revaluation.md](04_statistics_and_revaluation.md) so
   the next engineer sees why the new rank exists.

### 4. Debug a bad revaluation

1. Reproduce the call. Grab the raw asset row from
   `potential_assets` (or the input Excel) and POST to
   `/spitogatos/asset-statistics-by-radius`.
2. Check `debug.log` - is it succeeding after only 10 comparables? If so,
   `min_assets` might be too low for this geography.
3. Inspect the radius loop: look for "Not enough assets to compare with" -
   the expansion logs give you the final radius.
4. Verify the comparables: run `/spitogatos/assets-by-circle` with the
   same lat/lon/radius and eyeball the list. Are there outliers the
   formula inflated?
5. Verify the formula inputs: `asset.level`, `asset.construction_year`
   (`None` means "no uplift at all", which is usually what biases the
   estimate low).
6. Compare vs the polygon-scoped mean from
   `/geography/neighborhood-statistics?neighborhood_name_en=...`. If the
   circle gives a very different answer, the radius is too tight.

### 5. Add a new FastAPI endpoint

Covered in [06_api_and_dashboard.md](06_api_and_dashboard.md); the short
version: flow method -> `run_in_threadpool` call -> `response_model` on the
route -> update the `GET /` index.

### 6. Load a new polygon layer

1. Drop the raw GeoJSON under `utils/scripts/<layer>.json`. Make sure it is
   WGS84 (SRID 4326); use
   [utils/scripts/athens_neighborhoods_to_wgs84.py](../utils/scripts/athens_neighborhoods_to_wgs84.py)
   as a reprojection template otherwise.
2. Write a migration `00N_geography_<layer>.sql` that creates the table
   with `geom GEOMETRY(MULTIPOLYGON, 4326)` + GIST index + trigger (model
   it on
   [005_geography_athens_neighborhood.sql](../database/migrations/005_geography_athens_neighborhood.sql)).
3. Copy [database/load_athens_neighborhood.py](../database/load_athens_neighborhood.py)
   to `database/load_<layer>.py`, change the source path / table name.
4. Add a new method on `GeographyDAO` that builds the GeoJSON response.
5. Add a flow wrapper and a FastAPI route.

### 7. Ship a schema change

1. Write an idempotent migration (numeric prefix, `IF NOT EXISTS`, etc.).
2. Test locally: drop and rebuild the DB or run migrations against a fresh
   database.
3. In PR description, call out whether the migration is online-safe. If it
   rewrites a large table, prefer a two-step deploy (create new column NULL,
   backfill, then `NOT NULL` in a follow-up).
4. Verify `docker compose up` succeeds from scratch before merging.

## Cross-cutting best practices

- **Never `print`, always `logging`.** Non-entry-point modules use
  `logger = logging.getLogger(__name__)` and nothing else.
- **Never `psycopg2.connect` directly from a DAO.** Always go through
  `get_db_connection()` in [database/connection.py](../database/connection.py).
- **Never return dicts from scrapers.** Scrapers return
  `model/*_asset_model.py` objects so every layer above is typed.
- **Always upsert with `ON CONFLICT`.** Blind INSERT is a data-loss bug.
- **Always dedupe on the conflict key before bulk upsert.** Postgres will
  abort with "command cannot affect row a second time" if a batch contains
  two rows with the same conflict key.
- **Always use WGS84 (SRID 4326), GEOGRAPHY for points, GEOMETRY(MULTIPOLYGON)
  for boundaries.** Never mix.
- **Always wrap slow work in `run_in_threadpool` inside FastAPI.** Never
  block the event loop.
- **Keep migrations idempotent.** Always `CREATE ... IF NOT EXISTS`,
  `CREATE OR REPLACE`, `DROP TRIGGER IF EXISTS ... CREATE TRIGGER`.
- **Keep analytics scripts read-only.** `analytics/generate_plots.py` must
  not write to the DB.
- **One concern per file.** If a scraper grows into two sites or a DAO
  starts holding business logic, split it.
- **Validate metrics at the edge.** `_VALID_METRICS` in
  [flow/spitogatos_flow.py](../flow/spitogatos_flow.py) is the single
  source of truth; whitelist inputs before interpolating into SQL.

## Known global issues and open TODOs

- **Spitogatos 30-result page cap** -
  [data_source/spitogatos_data.py](../data_source/spitogatos_data.py) L22,
  `# todo: find why you get only 30 assets`. Workaround: split the map into
  many rectangles/polygons and iterate offsets within each.
- **Spitogatos `get_by_id` not implemented** - same file L87. Individual
  listings have extra data (photos, construction year) we do not fetch.
- **`get_area_summary` slow on old path** -
  [analytics/generate_plots.py](../analytics/generate_plots.py) bypasses
  the flow to avoid correlated subqueries. Long-term: rewrite the flow
  path using the CTE / `PERCENTILE_CONT` pattern.
- **`googletrans==4.0.0rc1` is brittle** - sometimes returns a coroutine,
  occasionally breaks when Google rotates tokens. We tolerate both but
  keep an eye on it during dashboard refreshes.
- **Windows cp1252 logging** - `_safe_log_text` in
  [database/spitogatos_dao.py](../database/spitogatos_dao.py) is the only
  guard against `UnicodeEncodeError` when logging Greek or emoji.
  Copy it into any DAO that diff-logs a user-facing string.
- **Mixed `SQLAlchemy` + `psycopg2` dependency** - SQLAlchemy is in
  [requirements.txt](../requirements.txt) but unused in active code. Do
  not import from it without a plan to migrate.
- **`ReOnlineFlow.add_sqm` has a reversed loop** -
  [flow/reonline_flow.py](../flow/reonline_flow.py) line 23 iterates
  `for row, index in df.iterrows()` (should be `for index, row`) and the
  `ReOnlineData.get_sqm` stub returns raw bytes rather than a parsed float.
  If this endpoint becomes important, fix both.
- **`eauctions_data.Eacutions_data`** - typo in the class name and only a
  placeholder; reports from eauction.gr are fetched manually.
- **`SpitogatosFlow` basicConfig side effect** - importing the module
  reconfigures the root logger (adds `debug.log`). Expect test harnesses
  to produce a `debug.log` in the cwd.
- **Hard-coded cookie in `ApisConsts.SPITOGATOS_COOKIE`** - a real
  browser cookie blob. It expires silently; rotate by copying fresh
  values from the network tab in Chrome DevTools.
- **Dashboard Excel path is hard-coded** to `excel_db/all_assets.xlsx`.
  Renames will silently give an empty dashboard.

## When things are on fire

- DB unreachable from api -> `docker compose logs db`, check the volume
  exists (`docker volume ls | grep db_data`), check `DB_HOST` env var.
- Migration fails -> look for the `C```assistant to=functions.ApplyPatch`
  sentinel at the end of the offending .sql file (the setup script strips
  it, but a stale copy in a custom migration could still break).
- Scrapers return empty -> cookies, headers, or IP banned. Re-check
  `ApisConsts`, try a different IP, reduce concurrency.
- Dashboard blank -> `excel_db/all_assets.xlsx` missing, or
  `comparison_average = 0` on every row (`price_avg_discount_pct` becomes
  `NaN`).
- Analytics PNGs empty -> verify `sqm > 30 AND sqm < 200` is not cutting
  your dataset to zero; adjust `SQM_MIN` / `SQM_MAX` in
  [analytics/generate_plots.py](../analytics/generate_plots.py) for your
  test scenario, then reset them before committing.
