# -*- coding: utf-8 -*-
import sys
import os
import io

# Fix encoding for Windows
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import openpyxl
import pandas as pd
import re
import time
import requests
from urllib.parse import urlparse, parse_qs

def extract_coords_from_google_maps_url(url):
    """Extract latitude/longitude from Google Maps URL"""
    if not url or not isinstance(url, str):
        return None, None
    
    try:
        # Handle different Google Maps URL formats
        # Pattern 1: Direct coordinates in URL with @ symbol
        match = re.search(r'@([-\d.]+),([-\d.]+)', url)
        if match:
            lat, lng = match.groups()
            return float(lat), float(lng)
        
        # Pattern 2: Coordinates in query parameters
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'q' in params:
            coords = params['q'][0]
            parts = coords.split(',')
            if len(parts) >= 2:
                try:
                    lat, lng = float(parts[0]), float(parts[1])
                    return lat, lng
                except:
                    pass
        
        # Pattern 3: Look for ll parameter (lat,long)
        if 'll' in params:
            coords = params['ll'][0]
            parts = coords.split(',')
            if len(parts) >= 2:
                try:
                    lat, lng = float(parts[0]), float(parts[1])
                    return lat, lng
                except:
                    pass
                    
    except Exception as e:
        pass
    
    return None, None

def geocode_nominatim(address, retry_count=3):
    """Geocode address using Nominatim with retry logic"""
    if not address or not isinstance(address, str):
        return None, None
    
    address = str(address).strip()
    if not address or len(address) < 3:
        return None, None
    
    for attempt in range(retry_count):
        try:
            headers = {
                'User-Agent': 'RealEstateAI-Geocoder/1.0'
            }
            params = {
                'q': address,
                'format': 'json',
                'timeout': 10
            }
            
            response = requests.get(
                'https://nominatim.openstreetmap.org/search',
                params=params,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    lat = float(data[0]['lat'])
                    lon = float(data[0]['lon'])
                    return lat, lon
            
            if attempt < retry_count - 1:
                time.sleep(1 + attempt)
                
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(1 + attempt)
            continue
    
    return None, None

def extract_city_from_address(address):
    """Extract city/area from comma-separated address"""
    if not address or not isinstance(address, str):
        return None
    
    parts = str(address).split(',')
    if len(parts) >= 2:
        return parts[-2].strip()
    elif len(parts) == 1:
        return parts[0].strip()
    
    return None

# Load original file
print("Loading original file: excel_db/assets_raw_100526.xlsx")
original_df = pd.read_excel('excel_db/assets_raw_100526.xlsx')
print(f"Original file shape: {original_df.shape}")
print(f"Columns: {[str(c) for c in original_df.columns]}")

# Find LINK column (column 11 = index 10)
link_column_name = original_df.columns[10] if len(original_df.columns) > 10 else None
print(f"\nColumn 11 (index 10): {link_column_name}")
print("Sample LINK values:")
if link_column_name:
    for i, val in enumerate(original_df[link_column_name].head(10)):
        print(f"  {i}: {str(val)[:100]}")

# Load enriched file
print("\n" + "="*80)
print("Loading enriched file: excel_db/assets_raw_100526_enriched.xlsx")
enriched_df = pd.read_excel('excel_db/assets_raw_100526_enriched.xlsx')
print(f"Enriched file shape: {enriched_df.shape}")
print(f"Columns: {[str(c) for c in enriched_df.columns]}")

# Check latitude/longitude columns
lat_col = None
lon_col = None
for col in enriched_df.columns:
    col_lower = str(col).lower()
    if 'latitude' in col_lower or 'lat' in col_lower:
        lat_col = col
    if 'longitude' in col_lower or 'lon' in col_lower:
        lon_col = col

print(f"\nLatitude column: {lat_col}")
print(f"Longitude column: {lon_col}")

# Count current NULL rows
null_count_before = enriched_df[[lat_col, lon_col]].isnull().all(axis=1).sum()
print(f"Rows with NULL coordinates: {null_count_before}")

# Find rows with NULL coordinates
null_mask = enriched_df[[lat_col, lon_col]].isnull().all(axis=1)
null_rows = enriched_df[null_mask].copy()
print(f"\nProcessing {len(null_rows)} rows with NULL coordinates...")

# Step 1: Extract coordinates from LINK column
coords_found_from_links = 0
if link_column_name and link_column_name in enriched_df.columns:
    print(f"\nStep 1: Extracting coordinates from {link_column_name} column...")
    for idx in null_rows.index:
        link = enriched_df.loc[idx, link_column_name]
        lat, lon = extract_coords_from_google_maps_url(link)
        if lat is not None and lon is not None:
            enriched_df.loc[idx, lat_col] = lat
            enriched_df.loc[idx, lon_col] = lon
            coords_found_from_links += 1
            print(f"  Row {idx}: Found coords from LINK: ({lat}, {lon})")

print(f"Coordinates found from LINK column: {coords_found_from_links}")

# Refresh null mask
null_mask = enriched_df[[lat_col, lon_col]].isnull().all(axis=1)
print(f"Remaining NULL rows: {null_mask.sum()}")

# Step 2: Geocode with full address
print("\nStep 2: Geocoding remaining rows with full address...")
coords_found_full = 0
for idx in enriched_df[null_mask].index:
    address_col = None
    for col in enriched_df.columns:
        if 'address' in str(col).lower():
            address_col = col
            break
    
    if address_col:
        address = enriched_df.loc[idx, address_col]
        if address and isinstance(address, str):
            lat, lon = geocode_nominatim(address)
            if lat is not None and lon is not None:
                enriched_df.loc[idx, lat_col] = lat
                enriched_df.loc[idx, lon_col] = lon
                coords_found_full += 1
                addr_preview = str(address)[:50] if address else "N/A"
                print(f"  Row {idx}: Geocoded '{addr_preview}': ({lat}, {lon})")
            
            time.sleep(0.5)

print(f"Coordinates found from full address: {coords_found_full}")

# Refresh null mask
null_mask = enriched_df[[lat_col, lon_col]].isnull().all(axis=1)
print(f"Remaining NULL rows: {null_mask.sum()}")

# Step 3: Try geocoding city/area only
print("\nStep 3: Geocoding remaining rows with city/area only...")
coords_found_city = 0
for idx in enriched_df[null_mask].index:
    address_col = None
    for col in enriched_df.columns:
        if 'address' in str(col).lower():
            address_col = col
            break
    
    if address_col:
        address = enriched_df.loc[idx, address_col]
        city = extract_city_from_address(address)
        if city and isinstance(city, str) and len(city) > 2:
            lat, lon = geocode_nominatim(city)
            if lat is not None and lon is not None:
                enriched_df.loc[idx, lat_col] = lat
                enriched_df.loc[idx, lon_col] = lon
                coords_found_city += 1
                print(f"  Row {idx}: Geocoded city '{city}': ({lat}, {lon})")
            
            time.sleep(0.5)

print(f"Coordinates found from city/area: {coords_found_city}")

# Final statistics
null_count_after = enriched_df[[lat_col, lon_col]].isnull().all(axis=1).sum()
total_found = coords_found_from_links + coords_found_full + coords_found_city

print("\n" + "="*80)
print("SUMMARY:")
print(f"  Rows with NULL coordinates BEFORE: {null_count_before}")
print(f"  Rows with NULL coordinates AFTER:  {null_count_after}")
print(f"  NEW coordinates found: {total_found}")
print(f"    - From LINK column: {coords_found_from_links}")
print(f"    - From full address: {coords_found_full}")
print(f"    - From city/area: {coords_found_city}")
print(f"  Total rows with coordinates: {enriched_df[[lat_col, lon_col]].notna().all(axis=1).sum()}")
print("="*80)

# Save updated file
print("\nSaving updated file: excel_db/assets_raw_100526_enriched.xlsx")
enriched_df.to_excel('excel_db/assets_raw_100526_enriched.xlsx', index=False)
print("File saved successfully!")

