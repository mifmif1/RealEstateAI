import pandas as pd
import re

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

# Simple Greek cities database (common real estate locations)
greek_cities = {
    'athens': (37.9838, 23.7275),
    'attiki': (37.9838, 23.7275),
    'glyka nera': (37.9842, 23.7331),
    'piraeus': (37.9267, 23.6402),
    'thessaloniki': (40.6401, 22.9444),
    'patras': (38.2466, 21.7353),
    'larissa': (39.6361, 22.4103),
    'heraklion': (35.3387, 25.1442),
    'volos': (39.3675, 22.9475),
    'kalamata': (37.0290, 22.1163),
    'rhodes': (36.4104, 28.2260),
    'corfu': (39.6270, 19.9211),
    'crete': (35.3387, 25.1442),
    'santorini': (36.4172, 25.4615),
    'mykonos': (37.4467, 25.3283),
    'naxos': (37.0842, 25.3734),
}

print("Attempting to geocode using city database...\n")

geocoded = 0
for idx in range(len(df)):
    lat = df.iloc[idx]['latitude']
    lon = df.iloc[idx]['longitude']
    
    # Check if missing
    if pd.isna(lat) or pd.isna(lon) or lat == '' or lon == '':
        if 'Address' in df.columns:
            address = str(df.iloc[idx]['Address']).lower()
            
            # Try to find city in address
            for city, (city_lat, city_lon) in greek_cities.items():
                if city in address:
                    df.at[idx, 'latitude'] = city_lat
                    df.at[idx, 'longitude'] = city_lon
                    geocoded += 1
                    print(f"Row {idx}: Geocoded to {city} ({city_lat}, {city_lon})")
                    break

print(f"\nGeocoded {geocoded} addresses using city database")

# Save
output_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df.to_excel(output_path, index=False)

# Final stats
rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()
print(f"Final: Rows with coordinates: {rows_with_coords}/329")
