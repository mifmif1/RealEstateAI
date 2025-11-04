# Database Package

This package contains database models, connections, and data access objects for the RealEstateAI project.

## Structure

- `models.py` - SQLAlchemy database models
- `connection.py` - Database connection and session management
- `config.py` - Database configuration
- `asset_dao.py` - Data Access Object for Asset operations
- `setup.py` - Database initialization script
- `migrations/` - Database migration files (Alembic)

## Usage

### Initialize Database

```python
from database.setup import setup_database
setup_database()
```

Or run directly:
```bash
python database/setup.py
```

### Using Database Session

```python
from database.connection import get_db_session
from database.asset_dao import AssetDAO
from model.asset_model import Asset
from model.geographical_model import Point

# Create an asset
asset = Asset(
    location=Point(lat=37.9838, lon=23.7275),
    sqm=80.0,
    price=150000.0,
    address="Athens, Greece"
)

# Use with context manager
with get_db_session() as session:
    dao = AssetDAO(session)
    db_asset = dao.create(asset, source="spitogatos")
```

### Search Assets by Location

```python
from database.connection import get_db_session
from database.asset_dao import AssetDAO
from model.geographical_model import Point

center = Point(lat=37.9838, lon=23.7275)

with get_db_session() as session:
    dao = AssetDAO(session)
    # Search within 500 meters, sqm between 70-100
    assets = dao.search_by_location(
        center_point=center,
        radius_meters=500,
        sqm_min=70.0,
        sqm_max=100.0,
        limit=50
    )
```

## Configuration

Database configuration is in `database/config.py`. By default, it uses SQLite with a file at `database/realestateai.db`.

To change the database, modify `DATABASE_URL` in `config.py`:
- SQLite: `sqlite:///path/to/database.db`
- PostgreSQL: `postgresql://user:password@localhost/dbname`
- MySQL: `mysql://user:password@localhost/dbname`

## Requirements

Make sure SQLAlchemy is installed:
```bash
pip install sqlalchemy
```

