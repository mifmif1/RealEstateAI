import pandas as pd
import numpy as np
import requests
import time
from pathlib import Path
from typing import Tuple, Optional

excel_path = Path("excel_db/assets_raw_100526_enriched.xlsx")

print("Loading Excel file...")
df = pd.read_excel(excel_path)

print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Find the SQM column
sqm_col = None
for col in df.columns:
    if col.lower() == 'sqm':
        sqm_col = col
        break

if sqm_col is None:
    sqm_cols = [c for c in df.columns if 'sqm' in c.lower()]
    if sqm_cols:
        sqm_col = sqm_cols[0]

print(f"\nWorking with column: {sqm_col}")
sqm_count_before = df[sqm_col].notna().sum()
print(f"Rows with sqm values: {sqm_count_before}/329")

def fix_sqm_value(val):
    if pd.isna(val):
        return val
    val_str = str(val).strip()
    if not val_str:
        return val
    val_str = val_str.replace(',', '.')
    try:
        return float(val_str)
    except ValueError:
        return val

df[sqm_col] = df[sqm_col].apply(fix_sqm_value)
print(f"Fixed SQM decimal notation")

# Find coordinate and address columns
lat_col = None
lon_col = None
addr_col = None

for col in df.columns:
    col_lower = col.lower()
    if 'latitude' in col_lower or col_lower == 'lat':
        lat_col = col
    if 'longitude' in col_lower or col_lower in ['lon', 'lng']:
        lon_col = col
    if col.upper() == 'ADDRESS' or col.lower() == 'address':
        addr_col = col

if lat_col is None:
    lat_col = 'latitude'
    df[lat_col] = np.nan

if lon_col is None:
    lon_col = 'longitude'
    df[lon_col] = np.nan

print(f"\nFound columns:")
print(f"  Latitude: {lat_col}")
print(f"  Longitude: {lon_col}")
print(f"  Address: {addr_col}")

coords_count_before = df[(df[lat_col].notna()) & (df[lon_col].notna())].shape[0]
print(f"\nRows with coordinates before: {coords_count_before}/329")

def geocode_address(address: str, max_retries: int = 5) -> Optional[Tuple[float, float]]:
    if not address or pd.isna(address):
        return None
    
    address_str = str(address).strip()
    if not address_str:
        return None
    
    strategies = [
        f"{address_str}, Greece",
        address_str,
    ]
    if ',' in address_str:
        strategies.append(address_str.split(',')[0].strip() + ", Greece")
        strategies.append(address_str.split(',')[-1].strip() + ", Greece")
    
    for query in strategies:
        for retry in range(max_retries):
            try:
                time.sleep(1.1)
                response = requests.get(
                    'https://nominatim.openstreetmap.org/search',
                    params={
                        'q': query,
                        'format': 'json',
                        'limit': 1,
                        'timeout': 10
                    },
                    headers={'User-Agent': 'RealEstateAI/1.0'}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        lat = float(data[0]['lat'])
                        lon = float(data[0]['lon'])
                        print(f"  OK: {query} -> ({lat:.4f}, {lon:.4f})")
                        return (lat, lon)
                elif response.status_code == 429:
                    wait_time = 2 ** retry
                    time.sleep(wait_time)
                    continue
            except Exception as e:
                if retry < max_retries - 1:
                    wait_time = 2 ** retry
                    time.sleep(wait_time)
                continue
    
    return None

new_coords_count = 0
rows_to_geocode = df[(df[lat_col].isna()) | (df[lon_col].isna())].copy()
print(f"\nRows needing geocoding: {len(rows_to_geocode)}/329")

for idx, (i, row) in enumerate(rows_to_geocode.iterrows()):
    address = row[addr_col]
    print(f"[{idx + 1}/{len(rows_to_geocode)}] {address}")
    
    coords = geocode_address(address)
    if coords:
        df.at[i, lat_col] = coords[0]
        df.at[i, lon_col] = coords[1]
        new_coords_count += 1

coords_count_after = df[(df[lat_col].notna()) & (df[lon_col].notna())].shape[0]

floor_col = None
for col in df.columns:
    if col.lower() == 'floor' or 'floor' in col.lower():
        floor_col = col
        break

floor_count = 0
if floor_col:
    floor_count = df[floor_col].notna().sum()

print(f"\nSaving to {excel_path}")
df.to_excel(excel_path, index=False)

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total rows: 329")
print(f"Rows with sqm: {sqm_count_before}/329")
print(f"Rows with floor: {floor_count}/329")
print(f"Rows with coordinates (before): {coords_count_before}/329")
print(f"Rows with coordinates (after): {coords_count_after}/329")
print(f"New coordinates found: {new_coords_count}")
print("="*60)