from typing import List

from pydantic import BaseModel

from model.asset_model import Asset


class AssetComparison(BaseModel):
    main_asset: str
    compared_assets: List[Asset] = []


class AssetsComparison(BaseModel):
    list_of_assets: List[AssetComparison]
