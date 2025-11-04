"""
FastAPI application for RealEstateAI flow functions.
Exposes all functions from the flow folder as REST API endpoints.
"""
import shutil
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

from flow.spitogatos_flow import SpitogatosFlow
from flow.reonline_flow import ReOnlineFlow

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
            "reonline": "/reonline/add-sqm"
        }
    }


@app.post("/spitogatos/expand-excel-comparison")
async def expand_excel_spitogatos_comparison(
    file: UploadFile = File(..., description="Excel file (.xlsx or .xlsb)"),
    must_columns: str = Form(..., description="Comma-separated list of required columns"),
    location_tolerance: float = Form(100, description="Location tolerance in meters"),
    sqm_tolerance: Optional[int] = Form(None, description="Square meter tolerance"),
    skip_sqm_lt: Optional[float] = Form(None, description="Skip rows where sqm < this value"),
    skip_if_has_comparison: bool = Form(False, description="Skip rows that already have comparison_average"),
    skip_if_has_percent: bool = Form(False, description="Skip rows with '%' in TitleGR"),
    skip_if_not_residential: bool = Form(False, description="Skip rows that are not residential (not Διαμέρισμα, Μεζονέτα, or Μονοκατοικία)")
):
    temp_input_path = None
    try:
        if not (file.filename.endswith('.xlsx') or file.filename.endswith('.xlsb')):
            raise HTTPException(status_code=400, detail="File must be .xlsx or .xlsb format")

        columns_list = [col.strip() for col in must_columns.split(',')]

        temp_input_path = UPLOAD_DIR / f"input_{file.filename}"
        with open(temp_input_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        def row_conditions(row):
            conditions = []
            if skip_sqm_lt is not None and 'sqm' in row.keys():
                conditions.append(row['sqm'] < skip_sqm_lt)
            if skip_if_has_comparison and 'comparison_average' in row.keys():
                conditions.append(not pd.isna(row['comparison_average']))
            if skip_if_has_percent and 'TitleGR' in row.keys():
                conditions.append('%' in str(row['TitleGR']))
            if skip_if_not_residential and 'SubCategoryGR' in row.keys():
                subcategory = str(row['SubCategoryGR'])
                conditions.append(
                    ('Διαμέρισμα' not in subcategory) and
                    ('Μεζονέτα' not in subcategory) and
                    ('Μονοκατοικία' not in subcategory)
                )
            return any(conditions)

        spitogatos_flow.expand_excel__spitogatos_comparison(
            excel_path=str(temp_input_path),
            must_columns=columns_list,
            row_conditions=row_conditions,
            location_tolerance=location_tolerance,
            sqm_tolerance=sqm_tolerance
        )

        output_files = list(UPLOAD_DIR.glob(f"input_{file.filename.rsplit('.', 1)[0]}_spitogatos_comparison_*.xlsx"))
        if not output_files:
            output_files = list(UPLOAD_DIR.glob(f"input_{file.filename.rsplit('.', 1)[0]}_spitogatos_comparison_*.xlsb"))
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)