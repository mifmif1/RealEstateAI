# -*- coding: utf-8 -*-
import sys
import os
import io

if sys.platform.startswith('win'):
    import codecs
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import re
from urllib.parse import urlparse, parse_qs

def extract_coords_from_google_maps_url(url):
    """Extract latitude/longitude from Google Maps URL"""
    if not url or not isinstance(url, str):
        return None, None
    
    try:
        match = re.search(r'@([-\d.]+),([-\d.]+)', url)
        if match:
            lat, lng = match.groups()
            return float(lat), float(lng)
    except:
        pass
    
    return None, None

# Load files
print("Loading original file...")
try:
    original_df = pd.read_excel('excel_db/assets_raw_100526.xlsx', engine='openpyxl')
    print(f"Loaded: {original_df.shape}")
except Exception as e:
    print(f"Error loading original: {e}")
    sys.exit(1)

print("Loading enriched file...")
try:
    enriched_df = pd.read_excel('excel_db/assets_raw_100526_enriched.xlsx', engine='openpyxl')
    print(f"Loaded: {enriched_df.shape}")
except Exception as e:
    print(f"Error loading enriched: {e}")
    sys.exit(1)

# Find columns
print("\nAnalyzing columns...")
print(f"Original columns: {list(original_df.columns)}")
print(f"Enriched columns: {list(enriched_df.columns)}")

link_col = original_df.columns[10] if len(original_df.columns) > 10 else None
lat_col = None
lon_col = None

for col in enriched_df.columns:
    col_lower = str(col).lower()
    if 'latitude' in col_lower or 'lat' in col_lower:
        lat_col = col
    if 'longitude' in col_lower or 'lon' in col_lower:
        lon_col = col

print(f"\nLINK column: {link_col}")
print(f"Latitude column: {lat_col}")
print(f"Longitude column: {lon_col}")

# Check null coordinates
null_mask = enriched_df[[lat_col, lon_col]].isnull().all(axis=1)
null_count_before = null_mask.sum()
print(f"\nRows with NULL coordinates: {null_count_before}")

# Step 1: Extract from LINK column
print("\nStep 1: Extracting coordinates from LINK column...")
coords_from_links = 0

for idx in enriched_df[null_mask].index:
    if idx < len(original_df):
        link = original_df.iloc[idx][link_col]
        lat, lon = extract_coords_from_google_maps_url(link)
        if lat is not None and lon is not None:
            enriched_df.at[idx, lat_col] = lat
            enriched_df.at[idx, lon_col] = lon
            coords_from_links += 1
            print(f"  Row {idx}: ({lat}, {lon})")

print(f"Found from LINK: {coords_from_links}")

# Update null mask
null_mask = enriched_df[[lat_col, lon_col]].isnull().all(axis=1)
null_count_after = null_mask.sum()

print("\n" + "="*80)
print("SUMMARY:")
print(f"  Rows with NULL coordinates BEFORE: {null_count_before}")
print(f"  Rows with NULL coordinates AFTER:  {null_count_after}")
print(f"  NEW coordinates found: {coords_from_links}")
print("="*80)

# Save file
print("\nSaving file...")
try:
    enriched_df.to_excel('excel_db/assets_raw_100526_enriched.xlsx', index=False, engine='openpyxl')
    print("File saved successfully!")
except Exception as e:
    print(f"Error saving: {e}")
    sys.exit(1)

