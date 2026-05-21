import pandas as pd
import requests
import time
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
print(f'Loading: {file_path}')
df = pd.read_excel(file_path)
print(f'Loaded {len(df)} rows\n')

# Geocoding function using OpenStreetMap/Nominatim API
def geocode_simple(address, retries=3):
    """Simple geocoding using Nominatim without geopy library"""
    if pd.isna(address) or str(address).strip() == '':
        return None, None
    
    address_str = str(address).strip()
    
    # Try different versions of the address
    attempts = [
        f"{address_str}, Greece",
        f"{address_str.split(',')[0]}, Greece" if ',' in address_str else address_str,
        address_str.split(',')[0] if ',' in address_str else address_str,
    ]
    
    for attempt in attempts:
        for retry in range(retries):
            try:
                url = "https://nominatim.openstreetmap.org/search"
                params = {
                    'q': attempt,
                    'format': 'json',
                    'limit': 1,
                }
                headers = {'User-Agent': 'RealEstateAI/1.0'}
                
                response = requests.get(url, params=params, headers=headers, timeout=5)
                time.sleep(1)  # Rate limiting
                
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        lat = float(data[0]['lat'])
                        lon = float(data[0]['lon'])
                        if lat and lon:
                            return lat, lon
            except Exception as e:
                time.sleep(1)
    
    return None, None

# Find and geocode missing coordinates
print("Geocoding addresses without coordinates...\n")
geocoded = 0
rows_to_geocode = []

for idx in range(len(df)):
    lat = df.iloc[idx]['latitude']
    lon = df.iloc[idx]['longitude']
    
    if pd.isna(lat) or pd.isna(lon) or lat == '' or lon == '':
        if 'Address' in df.columns:
            rows_to_geocode.append(idx)

print(f"Found {len(rows_to_geocode)} rows to geocode\n")

# Geocode in batches with slower rate
for idx, row_idx in enumerate(rows_to_geocode[:50]):  # Limit to first 50 to avoid rate limits
    if 'Address' in df.columns:
        address = df.iloc[row_idx]['Address']
        new_lat, new_lon = geocode_simple(address)
        
        if new_lat and new_lon:
            df.at[row_idx, 'latitude'] = new_lat
            df.at[row_idx, 'longitude'] = new_lon
            geocoded += 1
            addr_display = str(address)[:40] if address else "N/A"
            print(f"  [{idx+1}] Row {row_idx}: {addr_display} -> ({new_lat:.4f}, {new_lon:.4f})")
        else:
            addr_display = str(address)[:40] if address else "N/A"
            print(f"  [{idx+1}] Row {row_idx}: {addr_display} -> NOT FOUND")
        
        # Respect rate limits
        if (idx + 1) % 5 == 0:
            time.sleep(2)

print(f"\nGeocoded {geocoded} addresses\n")

# Save updated file
output_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df.to_excel(output_path, index=False)
print(f"Saved to: {output_path}\n")

# Print summary
rows_with_sqm = df['sqm'].notna().sum()
rows_with_floor = df['floor'].notna().sum()
rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()

print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total rows: 329")
print(f"Rows with sqm: {rows_with_sqm}/329")
print(f"Rows with floor: {rows_with_floor}/329")
print(f"Rows with coordinates: {rows_with_coords}/329")
print("=" * 60)
