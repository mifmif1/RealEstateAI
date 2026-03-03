from typing import List, Optional

from pydantic import BaseModel

class ComparisonDataModel(BaseModel):
    std: float
    min: float
    max: float
    mean: float
    median: float
    no_assets: int
    revaluation: Optional[float]
    spitogatos_comparison_assets: List[str] # ids