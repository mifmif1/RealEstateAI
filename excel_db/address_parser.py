
import pandas as pd
from geopy.geocoders import ArcGIS
import time

# Load the file
df = pd.read_excel('properties_parsed_110526.xlsx')

# Switch to ArcGIS (more reliable, no API key needed, higher rate limits)
geolocator = ArcGIS(timeout=10)

def get_coordinates(address):
    if pd.isna(address) or str(address).startswith('http'):
        return pd.NA, pd.NA
    
    clean_addr = str(address).replace(" - Google Maps", "").strip() + ", Greece"
    
    try:
        location = geolocator.geocode(clean_addr)
        if location:
            # Adding a tiny sleep so we don't spam the server too fast
            time.sleep(0.5) 
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Skipping {clean_addr} due to error: {e}")
        
    return pd.NA, pd.NA

print("Geocoding addresses with ArcGIS... This will take a few minutes.")

# Apply the geocoding
df[['lat', 'lon']] = df['Address'].apply(lambda addr: pd.Series(get_coordinates(addr)))

# Save the populated file
df.to_excel('properties_geocoded_final.xlsx', index=False)
print("Finished!")