import re
import time
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import sys
import io

# Set encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

excel_path = Path('excel_db/assets_raw_100526.xlsx')
df = pd.read_excel(excel_path)
print(f'Loaded {len(df)} records')

def extract_sqm(desc):
    if pd.isna(desc) or not isinstance(desc, str):
        return None
    patterns = [
        r'total\s+area\s+of\s+(\d+[.,]\d+|\d+)',
        r'(\d+[.,]\d+|\d+)\s*(?:sqm|sq\.m|m²|m2)'
    ]
    for pattern in patterns:
        matches = re.findall(pattern, str(desc).lower())
        if matches:
            try:
                return float(matches[0].replace(',', '.'))
            except:
                pass
    return None

def extract_floor(desc):
    if pd.isna(desc) or not isinstance(desc, str):
        return None
    desc_lower = str(desc).lower()
    if any(w in desc_lower for w in ['basement', 'hypogeo']):
        return 'Basement'
    if any(w in desc_lower for w in ['ground floor', 'ground']):
        return 'Ground'
    pattern = r'(\d+)(?:st|nd|rd|th)?\s*(?:floor|story)'
    m = re.search(pattern, desc_lower)
    if m:
        return f'Floor {m.group(1)}'
    return None

print('Extracting SQM...')
df['sqm'] = df['Description'].apply(extract_sqm)

print('Extracting Floor...')
df['floor'] = df['Description'].apply(extract_floor)

print('Geocoding addresses...')
geocoder = Nominatim(user_agent='realestate')
geocode_cache = {}
geocoded_count = 0

def geocode_address(address, area=None):
    global geocoded_count
    if pd.isna(address) or not isinstance(address, str):
        return None, None
    address = address.strip()
    if not address:
        return None, None
    cache_key = f'{address}|{area or ""}'
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]
    full_location = f'{address}, {area}' if area and pd.notna(area) else address
    if not full_location.lower().endswith('greece'):
        full_location = f'{full_location}, Greece'
    try:
        time.sleep(1.1)
        location = geocoder.geocode(full_location, timeout=10)
        if location:
            geocoded_count += 1
            result = (location.latitude, location.longitude)
            geocode_cache[cache_key] = result
            if geocoded_count % 10 == 0:
                print(f'  Geocoded {geocoded_count}...')
            return result
    except:
        pass
    return None, None

df[['latitude', 'longitude']] = df.apply(
    lambda row: pd.Series(geocode_address(row.get('Address', ''), row.get('Area', ''))),
    axis=1
)

output_path = Path('excel_db/assets_raw_100526_enriched.xlsx')
df.to_excel(output_path, index=False)

print(f'\nSummary:')
print(f'  SQM: {df["sqm"].notna().sum()}/{len(df)}')
print(f'  Floor: {df["floor"].notna().sum()}/{len(df)}')
print(f'  Geocoded: {df["latitude"].notna().sum()}/{len(df)}')
print(f'  Output: {output_path}')
