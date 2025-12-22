"""Data Access Object for Assets."""
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from database.models import AssetModel
from model.asset_model import Asset
from model.geographical_model import Point


class AssetDAO:
    """Data Access Object for Asset operations."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def create(self, asset: Asset, source: Optional[str] = None) -> AssetModel:
        """Create a new asset in the database."""
        db_asset = AssetModel(
            latitude=asset.location.lat,
            longitude=asset.location.lon,
            sqm=asset.sqm,
            price=asset.price,
            url=asset.url,
            level=asset.level,
            address=asset.address,
            new_state=asset.new_state,
            searched_radius=asset.searched_radius,
            revaluated_price_meter=asset.revaluated_price_meter,
            source=source
        )
        self.session.add(db_asset)
        self.session.flush()
        return db_asset
    
    def get_by_id(self, asset_id: int) -> Optional[AssetModel]:
        """Get asset by ID."""
        return self.session.query(AssetModel).filter(AssetModel.id == asset_id).first()
    
    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> List[AssetModel]:
        """Get all assets with optional pagination."""
        query = self.session.query(AssetModel)
        if limit:
            query = query.limit(limit).offset(offset)
        return query.all()
    
    def search_by_location(
        self,
        center_point: Point,
        radius_meters: float,
        sqm_min: Optional[float] = None,
        sqm_max: Optional[float] = None,
        limit: Optional[int] = None
    ) -> List[AssetModel]:
        """Search assets by location within radius."""
        # Simple bounding box search (can be improved with PostGIS for better accuracy)
        # Approximate: 1 degree latitude ≈ 111 km, 1 degree longitude ≈ 111 km * cos(latitude)
        lat_degrees = radius_meters / 111000.0
        lon_degrees = radius_meters / (111000.0 * abs(center_point.lat) / 90.0 if center_point.lat != 0 else 111000.0)
        
        query = self.session.query(AssetModel).filter(
            and_(
                AssetModel.latitude >= center_point.lat - lat_degrees,
                AssetModel.latitude <= center_point.lat + lat_degrees,
                AssetModel.longitude >= center_point.lon - lon_degrees,
                AssetModel.longitude <= center_point.lon + lon_degrees
            )
        )
        
        if sqm_min is not None:
            query = query.filter(AssetModel.sqm >= sqm_min)
        if sqm_max is not None:
            query = query.filter(AssetModel.sqm <= sqm_max)
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def update(self, asset_id: int, **kwargs) -> Optional[AssetModel]:
        """Update asset fields."""
        db_asset = self.get_by_id(asset_id)
        if not db_asset:
            return None
        
        for key, value in kwargs.items():
            if hasattr(db_asset, key):
                setattr(db_asset, key, value)
        
        self.session.flush()
        return db_asset
    
    def delete(self, asset_id: int) -> bool:
        """Delete asset by ID."""
        db_asset = self.get_by_id(asset_id)
        if not db_asset:
            return False
        
        self.session.delete(db_asset)
        self.session.flush()
        return True
    
    def to_asset_model(self, db_asset: AssetModel) -> Asset:
        """Convert database model to domain model."""
        return Asset(
            location=Point(lat=db_asset.latitude, lon=db_asset.longitude),
            sqm=db_asset.sqm,
            price=db_asset.price,
            url=db_asset.url,
            level=db_asset.level,
            address=db_asset.address,
            new_state=db_asset.new_state,
            searched_radius=db_asset.searched_radius,
            revaluated_price_meter=db_asset.revaluated_price_meter
        )

