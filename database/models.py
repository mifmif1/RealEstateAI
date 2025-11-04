"""SQLAlchemy database models."""
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class AssetModel(Base):
    """Asset database model."""
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, index=True)
    # Location
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    
    # Asset properties
    sqm = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    url = Column(String, nullable=True)
    level = Column(Integer, nullable=True)
    address = Column(String, nullable=True)
    new_state = Column(Boolean, nullable=True)
    searched_radius = Column(Float, nullable=True)
    revaluated_price_meter = Column(Float, nullable=True)
    
    # Metadata
    source = Column(String, nullable=True)  # e.g., 'spitogatos', 'reonline', 'eauction'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<AssetModel(id={self.id}, address={self.address}, price={self.price}, sqm={self.sqm})>"

