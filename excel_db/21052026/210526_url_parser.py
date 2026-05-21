import pandas as pd
import requests
import re
import time

# Load your most recent file
df = pd.read_excel('properties_geocoded_final.xlsx')

def get_coords_from_link(url):
    # Only process if it's actually a web link
    if not isinstance(url, str) or not url.startswith('http'):
        return pd.NA, pd.NA
        
    try:
        # We use a standard browser User-Agent so Google doesn't block the automated request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # Send the request and force it to follow all redirects to the final page
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        final_url = response.url
        
        # Method 1: Look for the standard Google Maps @lat,lon pattern in the address bar
        match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', final_url)
        if match:
            return float(match.group(1)), float(match.group(2))
            
        # Method 2: Look for coordinates hidden in query parameters (e.g., ?q=37.123,23.456)
        match_query = re.search(r'(?:ll|q)=(-?\d+\.\d+)[,%](-?\d+\.\d+)', final_url)
        if match_query:
            return float(match_query.group(1)), float(match_query.group(2))
            
        # Method 3: If the URL doesn't have it, check the raw HTML code of the page for map center coordinates
        html = response.text
        match_html = re.search(r'center(?:%3D|=)(-?\d+\.\d+)%2C(-?\d+\.\d+)', html)
        if match_html:
            return float(match_html.group(1)), float(match_html.group(2))

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        
    # Return empty if no coordinates were found
    return pd.NA, pd.NA

# Target only the rows where Address starts with 'http' and latitude is currently empty
link_mask = df['Address'].astype(str).str.startswith('http') & df['lat'].isna()
print(f"Found {link_mask.sum()} links to scrape. Starting extraction...")

# Iterate through each link and update the dataframe
for index in df[link_mask].index:
    url = df.at[index, 'Address']
    lat, lon = get_coords_from_link(url)
    
    if pd.notna(lat):
        df.at[index, 'lat'] = lat
        df.at[index, 'lon'] = lon
        print(f"Success: {url} -> {lat}, {lon}")
    else:
        print(f"Failed to find coordinates in: {url}")
    
    # Pause for 1.5 seconds between clicks to avoid getting temporarily banned by Google
    time.sleep(1.5)

# Save to a new final Excel file
output_file = 'properties_fully_geocoded.xlsx'
df.to_excel(output_file, index=False)
print(f"\nFinished! Saved results to {output_file}")