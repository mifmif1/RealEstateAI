from typing import List, Optional

from pydantic import BaseModel

class AreaStatisticsModel(BaseModel):
    std: float
    min: float
    max: float
    mean: float
    median: float
    no_assets: int
