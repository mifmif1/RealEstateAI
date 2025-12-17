# Database Setup Guide

This guide explains how to set up and use the PostGIS database for location-based real estate data storage.

## Prerequisites

1. **PostgreSQL** (version 12 or higher)
2. **PostGIS extension** (version 3.0 or higher)

### Installing PostgreSQL and PostGIS

#### Windows
1. Download PostgreSQL from https://www.postgresql.org/download/windows/
2. During installation, select "PostGIS" from the stack builder
3. Or install PostGIS separately using Stack Builder

#### macOS
```bash
brew install postgresql postgis
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib postgis
```

## Configuration

Database connection settings are configured via environment variables or defaults in `database/config.py`:

- `DB_HOST` - Database host (default: localhost)
- `DB_PORT` - Database port (default: 5432)
- `DB_NAME` - Database name (default: realestate_ai)
- `DB_USER` - Database user (default: postgres)
- `DB_PASSWORD` - Database password (default: postgres)

### Setting Environment Variables

#### Windows (PowerShell)
```powershell
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="realestate_ai"
$env:DB_USER="postgres"
$env:DB_PASSWORD="your_password"
```

#### Linux/macOS
```bash
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="realestate_ai"
export DB_USER="postgres"
export DB_PASSWORD="your_password"
```

Or create a `.env` file and use `python-dotenv` to load it.

## Database Setup

### Step 1: Create Database

Connect to PostgreSQL and create the database:

```sql
CREATE DATABASE realestate_ai;
```

### Step 2: Enable PostGIS Extension

Connect to the `realestate_ai` database and enable PostGIS:

```sql
\c realestate_ai
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Step 3: Run Migration Script

Run the setup script to create tables and indexes:

```bash
python database/setup.py
```

Or manually run the SQL migration:

```bash
psql -U postgres -d realestate_ai -f database/migrations/001_initial_schema.sql
```

## Usage

### Basic Usage

```python
from database.asset_dao import AssetDAO
from model.asset_model import Asset
from model.geographical_model import Point, Rectangle

# Initialize DAO
dao = AssetDAO()

# Insert an asset
asset = Asset(
    location=Point(lat=37.9838, lon=23.7275),  # Athens coordinates
    sqm=80.0,
    price=150000.0,
    url="https://example.com/property",
    level=3,
    address="Example Street 123",
    new_state=False
)

asset_id = dao.insert_asset(asset, source="spitogatos")
print(f"Inserted asset with ID: {asset_id}")

# Query assets by rectangle
rectangle = Rectangle(
    min_lat=37.9700,
    min_lon=23.7000,
    max_lat=38.0000,
    max_lon=23.7500
)

assets = dao.get_assets_by_rectangle(
    rectangle,
    min_sqm=50.0,
    max_sqm=100.0,
    source="spitogatos"
)

print(f"Found {len(assets)} assets")

# Query assets by radius
center = Point(lat=37.9838, lon=23.7275)
assets = dao.get_assets_by_radius(
    center,
    radius_meters=500.0,  # 500 meters radius
    min_sqm=50.0,
    max_sqm=100.0
)

# Get nearest assets
nearest = dao.get_nearest_assets(
    center,
    limit=10,
    min_sqm=50.0,
    max_sqm=100.0
)
```

### Integration with Existing Flows

You can integrate the DAO into your existing flows:

```python
from database.asset_dao import AssetDAO
from data_source.spitogatos_data import SpitogatosData

class SpitogatosFlow:
    def __init__(self):
        self._spitogatos_data_source = SpitogatosData()
        self._asset_dao = AssetDAO()
    
    def fetch_and_store_assets(self, location: Rectangle):
        # Fetch from API
        assets = self._spitogatos_data_source.get_by_location(location)
        
        # Store in database
        if assets:
            ids = self._asset_dao.insert_assets_batch(assets, source="spitogatos")
            print(f"Stored {len(ids)} assets")
        
        return assets
```

## Database Schema

### Assets Table

The `assets` table stores real estate properties with spatial data:

- `id` - Primary key (auto-increment)
- `location` - PostGIS GEOGRAPHY(POINT, 4326) - Location coordinates
- `sqm` - Square meters (float)
- `price` - Price (float)
- `url` - Property URL (text, nullable)
- `level` - Floor level (integer, nullable)
- `address` - Address (text, nullable)
- `new_state` - New construction flag (boolean, nullable)
- `searched_radius` - Search radius used (float, nullable)
- `revaluated_price_meter` - Revaluated price per sqm (float, nullable)
- `source` - Data source (varchar, nullable)
- `created_at` - Creation timestamp
- `updated_at` - Last update timestamp

### Indexes

- **Spatial Index** on `location` using GIST for fast spatial queries
- Indexes on `sqm`, `price`, `source`, and `created_at` for common query patterns

### Views

- `asset_statistics` - Aggregated statistics by source

## Spatial Queries

PostGIS provides powerful spatial query capabilities:

### Rectangle Queries
```python
# Get all assets within a bounding box
assets = dao.get_assets_by_rectangle(rectangle)
```

### Radius Queries
```python
# Get all assets within 500 meters of a point
assets = dao.get_assets_by_radius(center, radius_meters=500)
```

### Nearest Neighbor Queries
```python
# Get 10 nearest assets
assets = dao.get_nearest_assets(center, limit=10)
```

## Performance Tips

1. **Use Indexes**: The spatial GIST index on `location` is crucial for performance
2. **Batch Inserts**: Use `insert_assets_batch()` for multiple inserts
3. **Connection Pooling**: The connection pool is configured automatically
4. **Filter Early**: Use `min_sqm`, `max_sqm`, and `source` filters to reduce result sets

## Troubleshooting

### Connection Issues
- Verify PostgreSQL is running: `pg_isready`
- Check credentials in `database/config.py`
- Ensure database exists: `psql -l | grep realestate_ai`

### PostGIS Extension Issues
- Verify PostGIS is installed: `psql -d realestate_ai -c "SELECT PostGIS_version();"`
- If not installed, enable it: `CREATE EXTENSION postgis;`

### Migration Issues
- Check PostgreSQL logs for errors
- Ensure PostGIS extension is enabled before running migrations
- Verify user has CREATE privileges

## Next Steps

- Integrate with existing data sources (spitogatos, reonline, eauctions)
- Add data validation and duplicate detection
- Implement caching layer for frequently accessed locations
- Add data export functionality

