import pandas as pd
from geopy.geocoders import ArcGIS
import time
from pathlib import Path

DIR = Path(__file__).resolve().parent
INPUT_FILE = DIR / 'DVG_with_floor.xlsx'
OUTPUT_FILE = DIR / 'DVG_with_floor_geocoded.xlsx'

df = pd.read_excel(INPUT_FILE)

# Switch to ArcGIS (more reliable, no API key needed, higher rate limits)
geolocator = ArcGIS(timeout=10)

def build_full_address(address, region, area):
    if pd.notna(address) and str(address).strip().lower().startswith('http'):
        return pd.NA

    parts = []
    for val in (address, region, area):
        if pd.notna(val) and str(val).strip():
            text = str(val).replace(" - Google Maps", "").strip()
            if text:
                parts.append(text)

    if not parts:
        return pd.NA

    return ' '.join(parts) + ", Greece"

def get_coordinates(full_address):
    if pd.isna(full_address):
        return pd.NA, pd.NA

    try:
        location = geolocator.geocode(full_address)
        if location:
            time.sleep(0.5)
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Skipping {full_address} due to error: {e}")

    return pd.NA, pd.NA

print("Geocoding addresses with ArcGIS... This will take a few minutes.")

# Apply the geocoding using Address + Region + Area
df[['lat', 'lon']] = df.apply(
    lambda row: pd.Series(
        get_coordinates(build_full_address(row['Address'], row['Region'], row['Area']))
    ),
    axis=1,
)

df.to_excel(OUTPUT_FILE, index=False)
print(f"Finished! Saved to {OUTPUT_FILE}")