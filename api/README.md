# RealEstateAI Flow API

This API exposes all functions from the `flow` folder as REST API endpoints.

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the API

Start the API server:
```bash
python -m uvicorn api.app:app --reload
```

Or directly:
```bash
python api/app.py
```

The API will be available at `http://localhost:8000`

## API Documentation

Once the server is running, you can access:
- Interactive API docs (Swagger UI): `http://localhost:8000/docs`
- Alternative API docs (ReDoc): `http://localhost:8000/redoc`

## Endpoints

### 1. Spitogatos Flow - Expand Excel Comparison

**Endpoint:** `POST /spitogatos/expand-excel-comparison`

Expands an Excel file with Spitogatos comparison data.

**Parameters:**
- `file` (file): Excel file (.xlsx or .xlsb)
- `must_columns` (string): Comma-separated list of required columns (e.g., "sqm,price,coords,UniqueCode")
- `location_tolerance` (float, optional): Location tolerance in meters (default: 100)
- `sqm_tolerance` (int, optional): Square meter tolerance
- `skip_sqm_lt` (float, optional): Skip rows where sqm < this value
- `skip_if_has_comparison` (bool, optional): Skip rows that already have comparison_average (default: False)
- `skip_if_has_percent` (bool, optional): Skip rows with '%' in TitleGR (default: False)
- `skip_if_not_residential` (bool, optional): Skip rows that are not residential (default: False)

**Returns:** Processed Excel file with comparison data

**Example using curl:**
```bash
curl -X POST "http://localhost:8000/spitogatos/expand-excel-comparison" \
  -F "file=@path/to/your/file.xlsx" \
  -F "must_columns=sqm,price,coords,UniqueCode" \
  -F "location_tolerance=100" \
  -o output.xlsx
```

**Example using Python requests:**
```python
import requests

url = "http://localhost:8000/spitogatos/expand-excel-comparison"
files = {"file": open("path/to/your/file.xlsx", "rb")}
data = {
    "must_columns": "sqm,price,coords,UniqueCode",
    "location_tolerance": 100,
    "skip_sqm_lt": 30,
    "skip_if_has_comparison": True
}
response = requests.post(url, files=files, data=data)
with open("output.xlsx", "wb") as f:
    f.write(response.content)
```

### 2. ReOnline Flow - Add SQM

**Endpoint:** `POST /reonline/add-sqm`

Adds square meter (sqm) data to Excel file using ReOnline data source.

**Parameters:**
- `file` (file): Excel file (.xlsx or .xlsb) must contain a 'Link' column

**Returns:** Excel file enriched with sqm data

**Example using curl:**
```bash
curl -X POST "http://localhost:8000/reonline/add-sqm" \
  -F "file=@path/to/your/file.xlsx" \
  -o output.xlsx
```

**Example using Python requests:**
```python
import requests

url = "http://localhost:8000/reonline/add-sqm"
files = {"file": open("path/to/your/file.xlsx", "rb")}
response = requests.post(url, files=files)
with open("output.xlsx", "wb") as f:
    f.write(response.content)
```

## Notes

- Uploaded files are temporarily stored in `api/uploads/` directory
- Output files are automatically cleaned up after being served
- The API supports both `.xlsx` and `.xlsb` Excel file formats
- All endpoints return the processed Excel file as a download

## Error Handling

The API returns appropriate HTTP status codes:
- `200`: Success
- `400`: Bad Request (invalid file format, missing parameters)
- `500`: Internal Server Error (processing errors)

Errors are returned in JSON format with a `detail` field containing the error message.