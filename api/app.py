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

from flow.geography_flow import GeographyFlow
from flow.landea_flow import LandeaFlow
from flow.reonline_flow import ReOnlineFlow
from flow.spitogatos_flow import SpitogatosFlow
from model.area_statistics_model import AreaStatisticsModel
from model.asset_model import TargetAsset
from model.comparison_data_model import ComparisonDataModel
from model.geojson_model import GeoJsonFeatureCollection
from model.spitogatos_asset_model import SpitogatosAsset
from model.spitogatos_analytics_models import (
    TableDistributionPayload,
    TrendPayload,
    RelationshipPayload,
)

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
geography_flow = GeographyFlow()
spitogatos_flow = SpitogatosFlow()
reonline_flow = ReOnlineFlow()
landea_flow = LandeaFlow()

# Routers (keep URLs grouped by prefix)
geography_router = APIRouter(prefix="/geography", tags=["geography"])
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
            "geography_athens_neighborhoods": "/geography/athens-neighborhoods",
            "geography_attica_municipalities": "/geography/attica-municipalities",
            "geography_neighborhood_statistics": "/geography/neighborhood-statistics",
            "geography_municipality_statistics": "/geography/municipality-statistics",
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


@geography_router.get(
    "/athens-neighborhoods",
    response_model=GeoJsonFeatureCollection,
    summary="Get all Athens neighborhood boundaries as GeoJSON",
)
async def get_athens_neighborhoods():
    """
    Return all Athens neighborhood polygons as a GeoJSON FeatureCollection
    for frontend map rendering.
    """
    try:
        return await run_in_threadpool(geography_flow.get_athens_neighborhoods)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Athens neighborhoods: {str(e)}",
        )


@geography_router.get(
    "/attica-municipalities",
    response_model=GeoJsonFeatureCollection,
    summary="Get all Attica municipality boundaries as GeoJSON",
)
async def get_attica_municipalities():
    """
    Return all Attica municipality polygons as a GeoJSON FeatureCollection
    for frontend map rendering.
    """
    try:
        return await run_in_threadpool(geography_flow.get_attica_municipalities)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Attica municipalities: {str(e)}",
        )


@geography_router.get(
    "/neighborhood-statistics",
    response_model=AreaStatisticsModel,
    summary="Get Spitogatos price-per-sqm statistics for an Athens neighborhood",
)
async def get_neighborhood_statistics_geography(
    neighborhood_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    """
    Compute price-per-sqm statistics for all Spitogatos assets inside the
    given Athens neighborhood polygon. Returns 404 if no assets are found.
    """
    try:
        result = await run_in_threadpool(
            geography_flow.get_neighborhood_statistics,
            neighborhood_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No assets found for neighborhood '{neighborhood_name_en}'",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing neighborhood statistics: {str(e)}",
        )


@geography_router.get(
    "/municipality-statistics",
    response_model=AreaStatisticsModel,
    summary="Get Spitogatos price-per-sqm statistics for an Attica municipality",
)
async def get_municipality_statistics_geography(
    municipality_name_en: str,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    """
    Compute price-per-sqm statistics for all Spitogatos assets inside the
    given Attica municipality polygon. Returns 404 if no assets are found.
    """
    try:
        result = await run_in_threadpool(
            geography_flow.get_municipality_statistics,
            municipality_name_en,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No assets found for municipality '{municipality_name_en}'",
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing municipality statistics: {str(e)}",
        )


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


analytics_router = APIRouter(prefix="/spitogatos/analytics", tags=["spitogatos-analytics"])


@analytics_router.get(
    "/table-distribution",
    response_model=TableDistributionPayload,
    summary="Summary statistics table + shared-axis distribution series for all areas",
)
async def get_analytics_table_distribution(
    metric: str,
    n_buckets: int = 20,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    """
    Returns per-area summary statistics (count, min, max, mean, median, std,
    percentiles, IQR, CV, skewness, kurtosis) and histogram distribution series
    for all neighborhoods and municipalities for the selected metric.

    metric: upload_time | floor_number | price | sqm | price_per_sqm | new_development
    """
    try:
        result = await run_in_threadpool(
            spitogatos_flow.get_table_distribution,
            metric,
            n_buckets,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing table/distribution: {str(e)}")


@analytics_router.get(
    "/trends",
    response_model=TrendPayload,
    summary="Time-series trend data per area for a selected metric",
)
async def get_analytics_trends(
    metric: str,
    granularity: str = "month",
    area_type: Optional[str] = None,
    area_names: Optional[str] = None,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    """
    Returns monthly/weekly/daily mean, median, p25, p75 trend series per area.

    metric: upload_time | floor_number | price | sqm | price_per_sqm | new_development
    granularity: day | week | month
    area_names: comma-separated list of area names to filter (optional)
    """
    try:
        names_list = [n.strip() for n in area_names.split(",")] if area_names else None
        result = await run_in_threadpool(
            spitogatos_flow.get_analytics_trends,
            metric,
            granularity,
            area_type,
            names_list,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing trends: {str(e)}")


@analytics_router.get(
    "/relationships",
    response_model=RelationshipPayload,
    summary="Sampled (x, y) variable pairs for scatter/relationship charts",
)
async def get_analytics_relationships(
    x_metric: str,
    y_metric: str,
    area_type: Optional[str] = None,
    area_names: Optional[str] = None,
    sample_limit: int = 3000,
    website_modified_from: Optional[datetime] = None,
    website_modified_to: Optional[datetime] = None,
    website_uploaded_from: Optional[datetime] = None,
    website_uploaded_to: Optional[datetime] = None,
):
    """
    Returns up to sample_limit (x, y) metric pairs for relationship/scatter charts.

    x_metric, y_metric: upload_time | floor_number | price | sqm | price_per_sqm | new_development
    area_names: comma-separated list to filter (optional)
    """
    try:
        names_list = [n.strip() for n in area_names.split(",")] if area_names else None
        result = await run_in_threadpool(
            spitogatos_flow.get_analytics_relationships,
            x_metric,
            y_metric,
            area_type,
            names_list,
            sample_limit,
            website_modified_from,
            website_modified_to,
            website_uploaded_from,
            website_uploaded_to,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing relationships: {str(e)}")


app.include_router(geography_router)
app.include_router(spitogatos_router)
app.include_router(analytics_router)
app.include_router(landea_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
