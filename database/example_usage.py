"""Example usage of the database package."""
from database.connection import get_db_session, init_db
from database.asset_dao import AssetDAO
from model.asset_model import Asset
from model.geographical_model import Point


def example_create_asset():
    """Example: Create an asset in the database."""
    # First, initialize the database
    init_db()
    
    # Create an asset
    asset = Asset(
        location=Point(lat=37.9838, lon=23.7275),
        sqm=80.0,
        price=150000.0,
        url="https://example.com/property/123",
        level=3,
        address="Athens, Greece",
        new_state=False,
        searched_radius=100.0,
        revaluated_price_meter=1875.0
    )
    
    # Use with context manager
    with get_db_session() as session:
        dao = AssetDAO(session)
        db_asset = dao.create(asset, source="spitogatos")
        print(f"Created asset with ID: {db_asset.id}")


def example_search_assets():
    """Example: Search assets by location."""
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
        print(f"Found {len(assets)} assets")
        for asset in assets:
            print(f"  - Asset {asset.id}: {asset.address}, {asset.sqm} sqm, €{asset.price}")


def example_get_all_assets():
    """Example: Get all assets with pagination."""
    with get_db_session() as session:
        dao = AssetDAO(session)
        assets = dao.get_all(limit=10, offset=0)
        print(f"Retrieved {len(assets)} assets")
        for asset in assets:
            print(f"  - Asset {asset.id}: {asset.address}")


if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    
    print("\n1. Creating an asset...")
    example_create_asset()
    
    print("\n2. Getting all assets...")
    example_get_all_assets()
    
    print("\n3. Searching assets by location...")
    example_search_assets()

