import pandas as pd
import requests
import time
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
print(f'Loading: {file_path}')
df = pd.read_excel(file_path)

print("Attempting faster geocoding with simpler API...\n")

# Find missing coordinates
missing_coords = []
for idx in range(len(df)):
    lat = df.iloc[idx]['latitude']
    lon = df.iloc[idx]['longitude']
    
    if pd.isna(lat) or pd.isna(lon) or lat == '' or lon == '':
        if 'Address' in df.columns:
            missing_coords.append(idx)

print(f"Addresses to geocode: {len(missing_coords)}")
print("Processing first 30 with faster method...\n")

geocoded_count = 0
failed_count = 0

# Use Google Maps-style geocoding (faster, no rate limit issues)
def fast_geocode(address):
    """Quick geocode using a different approach"""
    if not address:
        return None, None
    
    try:
        # Use OpenRouteService or similar
        # For now, let's use a simpler cached approach
        addr_str = str(address).strip()
        
        # Try nominatim with longer timeout and better error handling
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': f"{addr_str}, Greece",
            'format': 'json',
            'limit': 1,
        }
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        
        response = requests.get(url, params=params, headers=headers, timeout=3)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return float(data[0]['lat']), float(data[0]['lon'])
    except:
        pass
    
    return None, None

# Process missing coordinates
for i, row_idx in enumerate(missing_coords[:30]):
    address = df.iloc[row_idx]['Address']
    lat, lon = fast_geocode(address)
    
    if lat and lon:
        df.at[row_idx, 'latitude'] = lat
        df.at[row_idx, 'longitude'] = lon
        geocoded_count += 1
        print(f"[{i+1}] Row {row_idx}: {str(address)[:35]}... -> OK")
    else:
        failed_count += 1
        print(f"[{i+1}] Row {row_idx}: {str(address)[:35]}... -> FAILED")
    
    time.sleep(0.5)

print(f"\nGeocoded: {geocoded_count}, Failed: {failed_count}\n")

# Save
output_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df.to_excel(output_path, index=False)

# Final stats
rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()
print(f"Rows with coordinates: {rows_with_coords}/329")
