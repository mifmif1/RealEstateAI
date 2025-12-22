"""
Example usage of the AssetDAO for location-based queries
"""
from database.asset_dao import AssetDAO
from model.asset_model import Asset
from model.geographical_model import Point, Rectangle


def example_basic_usage():
    """Basic usage examples"""
    # Initialize DAO
    dao = AssetDAO()
    
    # Example 1: Insert an asset
    print("Example 1: Inserting an asset")
    asset = Asset(
        location=Point(lat=37.9838, lon=23.7275),  # Athens coordinates
        sqm=80.0,
        price=150000.0,
        url="https://example.com/property",
        level=3,
        address="Example Street 123, Athens",
        new_state=False
    )
    
    asset_id = dao.insert_asset(asset, source="spitogatos")
    print(f"Inserted asset with ID: {asset_id}\n")
    
    # Example 2: Query by rectangle
    print("Example 2: Querying assets by rectangle")
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
    print(f"Found {len(assets)} assets in rectangle\n")
    
    # Example 3: Query by radius
    print("Example 3: Querying assets by radius")
    center = Point(lat=37.9838, lon=23.7275)
    assets = dao.get_assets_by_radius(
        center,
        radius_meters=500.0,  # 500 meters radius
        min_sqm=50.0,
        max_sqm=100.0
    )
    print(f"Found {len(assets)} assets within 500m radius\n")
    
    # Example 4: Get nearest assets
    print("Example 4: Getting nearest assets")
    nearest = dao.get_nearest_assets(
        center,
        limit=10,
        min_sqm=50.0,
        max_sqm=100.0
    )
    print(f"Found {len(nearest)} nearest assets\n")
    
    # Example 5: Get statistics
    print("Example 5: Getting asset statistics")
    stats = dao.get_asset_statistics(
        rectangle=rectangle,
        source="spitogatos"
    )
    print(f"Statistics: {stats}\n")


def example_batch_operations():
    """Example of batch operations"""
    dao = AssetDAO()
    
    # Create multiple assets
    assets = [
        Asset(
            location=Point(lat=37.9838 + i * 0.001, lon=23.7275 + i * 0.001),
            sqm=80.0 + i * 10,
            price=150000.0 + i * 10000,
            url=f"https://example.com/property{i}",
            level=i % 7,
            address=f"Example Street {i}, Athens",
            new_state=i % 2 == 0
        )
        for i in range(10)
    ]
    
    # Insert batch
    ids = dao.insert_assets_batch(assets, source="spitogatos")
    print(f"Inserted {len(ids)} assets with IDs: {ids}")


def example_integration_with_existing_flow():
    """Example of integrating with existing flow"""
    from data_source.spitogatos_data import SpitogatosData
    from data_source.geopy_data import GeopyData
    
    dao = AssetDAO()
    geopy_data = GeopyData()
    spitogatos_data = SpitogatosData()
    
    # Get location from address
    address = "Syntagma Square, Athens"
    point = geopy_data.coords_from_address(address)
    
    # Create search rectangle
    rectangle = geopy_data.rectangle_from_point(point, radius_meters=500)
    
    # Fetch from API
    assets = spitogatos_data.get_by_location(
        location=rectangle,
        min_area=50,
        max_area=150
    )
    
    # Store in database
    if assets:
        ids = dao.insert_assets_batch(assets, source="spitogatos")
        print(f"Stored {len(ids)} assets from Spitogatos")
        
        # Query back from database
        stored_assets = dao.get_assets_by_rectangle(
            rectangle,
            min_sqm=50,
            max_sqm=150,
            source="spitogatos"
        )
        print(f"Retrieved {len(stored_assets)} assets from database")


if __name__ == "__main__":
    print("=" * 60)
    print("AssetDAO Usage Examples")
    print("=" * 60)
    print()
    
    try:
        example_basic_usage()
        print()
        example_batch_operations()
        print()
        example_integration_with_existing_flow()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure PostgreSQL is running and database is set up correctly.")
        print("Run 'python database/setup.py' to initialize the database.")

