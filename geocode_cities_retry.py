import pandas as pd
import time
import os

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'

# Wait a moment
time.sleep(1)

try:
    df = pd.read_excel(file_path)
    
    # Simple Greek cities database
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
    }
    
    geocoded = 0
    for idx in range(len(df)):
        lat = df.iloc[idx]['latitude']
        lon = df.iloc[idx]['longitude']
        
        if pd.isna(lat) or pd.isna(lon) or lat == '' or lon == '':
            if 'Address' in df.columns:
                address = str(df.iloc[idx]['Address']).lower()
                
                for city, (city_lat, city_lon) in greek_cities.items():
                    if city in address:
                        df.at[idx, 'latitude'] = city_lat
                        df.at[idx, 'longitude'] = city_lon
                        geocoded += 1
                        break
    
    print(f"Geocoded {geocoded} addresses")
    
    # Save with backup
    backup_path = r'excel_db\assets_raw_100526_enriched_backup.xlsx'
    df.to_excel(backup_path, index=False)
    
    # Try to replace original
    if os.path.exists(file_path):
        os.remove(file_path)
    
    df.to_excel(file_path, index=False)
    print(f"Saved to {file_path}")
    
    rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()
    print(f"Rows with coordinates: {rows_with_coords}/329")
    
except Exception as e:
    print(f"Error: {e}")
