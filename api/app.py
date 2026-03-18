"""
FastAPI application for RealEstateAI flow functions.
Exposes all functions from the flow folder as REST API endpoints.
"""
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI, UploadFile, File, HTTPException, APIRouter
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from flow.landea_flow import LandeaFlow
from flow.reonline_flow import ReOnlineFlow
from flow.spitogatos_flow import SpitogatosFlow
from model.asset_model import TargetAsset
from model.area_statistics_model import AreaStatisticsModel
from model.comparison_data_model import ComparisonDataModel
from model.spitogatos_asset_model import SpitogatosAsset

app = FastAPI(
    title="RealEstateAI Flow API",
    description="API for all functions in the flow folder",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize flow classes
spitogatos_flow = SpitogatosFlow()
reonline_flow = ReOnlineFlow()
landea_flow = LandeaFlow()

# Routers (keep URLs grouped by prefix)
spitogatos_router = APIRouter(prefix="/spitogatos", tags=["spitogatos"])
landea_router = APIRouter(prefix="/landea", tags=["landea"])

# Temporary directory for file uploads
UPLOAD_DIR = Path("api/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@app.get("/")
async def root():
    return {
        "message": "RealEstateAI Flow API",
        "version": "1.0.0",
        "endpoints": {
            "spitogatos": "/spitogatos/expand-excel-comparison",
            "spitogatos_get_all_athens": "/spitogatos/get-all-athens",
            "spitogatos_assets_by_circle": "/spitogatos/assets-by-circle",
            "spitogatos_asset_statistics_by_radius": "/spitogatos/asset-statistics-by-radius",
            "spitogatos_asset_statistics_by_comparisons": "/spitogatos/asset-statistics-by-comparisons",
            "reonline": "/reonline/add-sqm",
            "landea_stage1": "/landea/run-stage1",
            "landea_stage2": "/landea/run-stage2",
        }
    }


@spitogatos_router.post("/get-all-athens")
async def get_all_athens(
        start_offset: int = 0,
        max_pages: Optional[int] = None,
):
    """
    Trigger fetching and persisting all Athens listings from Spitogatos.

    This runs the long-running scraping job in a background thread so the API
    request does not block the event loop.
    """
    try:
        await run_in_threadpool(spitogatos_flow.fetch_all_athens, start_offset, max_pages)
        return {
            "status": "ok",
            "message": "Completed get_all_athens run.",
            "start_offset": start_offset,
            "max_pages": max_pages,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running get_all_athens: {str(e)}")


@spitogatos_router.get(
    "/assets-by-circle",
    response_model=List[SpitogatosAsset],
    summary="Get Spitogatos assets within a circle",
)
async def get_assets_by_circle(
        lon: float,
        lat: float,
        radius_meters: float = 100,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
):
    """
    Return all Spitogatos assets stored in the DB that lie within a circle
    defined by a center point (lon, lat) and radius in meters.
    """
    try:
        assets = await run_in_threadpool(
            spitogatos_flow.get_assets_by_circle,
            lon,
            lat,
            radius_meters,
            100,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return assets
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching assets by circle: {str(e)}",
        )


@spitogatos_router.get(
    "/assets-by-athens-neighborhood",
    response_model=List[SpitogatosAsset],
    summary="Get Spitogatos assets within an Athens neighborhood",
)
async def get_assets_by_athens_neighborhood(
    neighborhood_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    try:
        assets = await run_in_threadpool(
            spitogatos_flow.get_assets_by_athens_neighborhood,
            neighborhood_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return assets
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching assets by Athens neighborhood: {str(e)}",
        )


@spitogatos_router.get(
    "/assets-by-attica-municipality",
    response_model=List[SpitogatosAsset],
    summary="Get Spitogatos assets within an Attica municipality",
)
async def get_assets_by_attica_municipality(
    municipality_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    try:
        assets = await run_in_threadpool(
            spitogatos_flow.get_assets_by_attica_municipality,
            municipality_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return assets
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching assets by Attica municipality: {str(e)}",
        )


@spitogatos_router.get(
    "/neighborhood-statistics",
    response_model=AreaStatisticsModel,
    summary="Get price-per-sqm statistics for an Athens neighborhood",
)
async def get_neighborhood_statistics(
    neighborhood_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    try:
        result = await run_in_threadpool(
            spitogatos_flow.get_neighborhood_statistics,
            neighborhood_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="No assets found to compute neighborhood statistics",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing neighborhood statistics: {str(e)}",
        )


@spitogatos_router.get(
    "/municipality-statistics",
    response_model=AreaStatisticsModel,
    summary="Get price-per-sqm statistics for an Attica municipality",
)
async def get_municipality_statistics(
    municipality_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    try:
        result = await run_in_threadpool(
            spitogatos_flow.get_municipality_statistics,
            municipality_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="No assets found to compute municipality statistics",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing municipality statistics: {str(e)}",
        )


@spitogatos_router.post(
    "/asset-statistics-by-radius",
    response_model=ComparisonDataModel,
    summary="Get price statistics for a target asset using nearby assets",
)
async def get_asset_statistics_by_radius(
        asset: TargetAsset,
        radius_meters: int = 100,
        min_assets: int = 10,
        limit: int = 100,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
):
    """
    Compute price-per-sqm statistics for a target asset based on Spitogatos
    assets within a growing radius until at least `min_assets` are found.
    """
    try:
        result = await run_in_threadpool(
            spitogatos_flow.get_asset_statistics_by_radius,
            asset,
            radius_meters,
            min_assets,
            limit,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Not enough nearby assets to compute statistics",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing asset statistics by radius: {str(e)}",
        )


@spitogatos_router.post(
    "/asset-statistics-by-comparisons",
    response_model=ComparisonDataModel,
    summary="Get price statistics for a target asset using explicit comparison assets",
)
async def get_asset_statistics_by_comparisons(
        asset: TargetAsset,
        comparison_assets: List[SpitogatosAsset],
):
    """
    Compute price-per-sqm statistics for a target asset given an explicit list
    of comparison Spitogatos assets.
    """
    try:
        result = await run_in_threadpool(
            spitogatos_flow.get_asset_statistics_by_comparisons,
            asset,
            comparison_assets,
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing asset statistics by comparisons: {str(e)}",
        )


@app.post("/reonline/add-sqm")
async def add_sqm_reonline(
        file: UploadFile = File(..., description="Excel file (.xlsx or .xlsb) with 'Link' column")
):
    temp_input_path = None
    try:
        if not (file.filename.endswith('.xlsx') or file.filename.endswith('.xlsb')):
            raise HTTPException(status_code=400, detail="File must be .xlsx or .xlsb format")

        temp_input_path = UPLOAD_DIR / f"input_{file.filename}"
        with open(temp_input_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        reonline_flow.add_sqm(excel_path=str(temp_input_path))

        output_files = list(UPLOAD_DIR.glob(f"input_{file.filename.rsplit('.', 1)[0]}_sqm_enrich_*.xlsx"))
        if not output_files:
            output_files = list(UPLOAD_DIR.glob(f"input_{file.filename.rsplit('.', 1)[0]}_sqm_enrich_*.xlsb"))
        if not output_files:
            raise HTTPException(status_code=500, detail="Output file not found after processing")

        output_file = max(output_files, key=lambda p: p.stat().st_mtime)
        if temp_input_path and temp_input_path.exists():
            temp_input_path.unlink()

        return FileResponse(
            path=str(output_file),
            filename=output_file.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except HTTPException:
        raise
    except Exception as e:
        if temp_input_path and temp_input_path.exists():
            temp_input_path.unlink()
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@landea_router.post("/run-stage1")
async def landea_run_stage1(
        start_page: int = 1,
        max_pages: Optional[int] = None,
):
    """
    Trigger Landea Stage 1:
    - Crawl search pages from Landea and upsert listing-level data into DB.
    """
    try:
        affected = await run_in_threadpool(
            landea_flow.run_stage1,
            max_pages,
            start_page,
        )
        return {
            "status": "ok",
            "message": "Completed Landea Stage 1.",
            "start_page": start_page,
            "max_pages": max_pages,
            "rows_affected": affected,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running Landea Stage 1: {str(e)}")


@landea_router.post("/run-stage2")
async def landea_run_stage2(
        batch_size: int = 100,
):
    """
    Trigger Landea Stage 2:
    - Enrich DB rows missing coordinates from their detail pages.
    """
    try:
        total_updated = await run_in_threadpool(
            landea_flow.run_stage2,
            batch_size,
        )
        return {
            "status": "ok",
            "message": "Completed Landea Stage 2.",
            "batch_size": batch_size,
            "rows_enriched": total_updated,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running Landea Stage 2: {str(e)}")


app.include_router(spitogatos_router)
app.include_router(landea_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
