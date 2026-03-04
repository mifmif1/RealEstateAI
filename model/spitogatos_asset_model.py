from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class SpitogatosAsset(BaseModel):
    id: str
    category: str
    subtype: int
    buy_or_rent: int
    sqm: int
    price: int
    # Optional field; not all rows will have a revised price stored.
    revised_price: Optional[float] = None
    price_reduced: bool
    price_pre_reduction: Optional[int]
    price_change_percentage: Optional[int]
    main_image_URL: str
    geography: str
    geocodeType: str
    longitude: float
    latitude: float
    floor_number: int
    rooms: int
    total_rooms: int
    bathrooms: int
    kitchens: int
    living_rooms: int
    within_city_plan: int
    agricultural_use: int
    description: str
    new_development: int
    website_modified: datetime
    website_uploaded: datetime
    imageIds: List[int]
    has_VTour: bool
    has_video: bool
    agent_id: int
    enquirer_id: int
    reAgent: str
    published: str
    first_publish_date: datetime
