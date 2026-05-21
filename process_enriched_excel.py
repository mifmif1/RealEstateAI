import pandas as pd
import re
import sys
import time

# Set UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
print(f'Loading: {file_path}')
df = pd.read_excel(file_path)
print(f'Loaded {len(df)} rows\n')

# Ensure columns exist
for col in ['sqm', 'floor', 'latitude', 'longitude']:
    if col not in df.columns:
        df[col] = None

def extract_sqm(description):
    """Extract main/total area value from description"""
    if pd.isna(description):
        return None
    
    desc_str = str(description).strip()
    
    # Patterns to find sqm values
    patterns = [
        r'total\s+area\s+of\s+([0-9,]+(?:\.[0-9]+)?)',
        r'(\d+(?:[.,]\d+)?)\s*(?:sqm|sq\.m|m2|m²)',
        r'Εμ\.\s*(\d+(?:[.,]\d+)?)',
    ]
    
    matches = []
    for pattern in patterns:
        found = re.findall(pattern, desc_str, re.IGNORECASE)
        if found:
            matches.extend(found)
    
    if matches:
        try:
            # Clean numbers and convert to float
            values = []
            for m in matches:
                clean = m.replace(',', '').replace(' ', '')
                values.append(float(clean))
            return max(values) if values else None
        except:
            pass
    
    return None

def extract_floor(description):
    """Extract floor level from description"""
    if pd.isna(description):
        return None
    
    desc_str = str(description).lower()
    
    # Greek floor indicators
    if 'basement' in desc_str or 'υπόγειο' in desc_str:
        return 'Basement'
    
    if 'ground' in desc_str or 'ισόγειο' in desc_str or 'ισογείου' in desc_str:
        return 'Ground'
    
    if 'attic' in desc_str or 'σοφίτα' in desc_str:
        return 'Attic'
    
    if 'penthouse' in desc_str:
        return 'Penthouse'
    
    if 'house' in desc_str or 'maisonette' in desc_str:
        return 'House'
    
    # Look for numbered floors
    floor_patterns = [
        r'floor\s+(\d+)',
        r'(\d+)(?:st|nd|rd|th)\s+floor',
        r'(\d+)\s*(?:ης|η|ος|ο)?\s*ορόφ(?:ου|α|ος)',
    ]
    
    for pattern in floor_patterns:
        match = re.search(pattern, desc_str, re.IGNORECASE)
        if match:
            floor_num = int(match.group(1))
            suffixes = {1: 'st', 2: 'nd', 3: 'rd'}
            suffix = suffixes.get(floor_num, 'th')
            return f'{floor_num}{suffix}'
    
    return None

# Step 1: Extract sqm and floor for rows 0-329
print("Extracting sqm and floor data...")
extracted_sqm = 0
extracted_floor = 0

for idx in range(min(330, len(df))):
    if 'Description' in df.columns:
        description = df.iloc[idx]['Description']
        
        sqm_val = extract_sqm(description)
        if sqm_val:
            df.at[idx, 'sqm'] = sqm_val
            extracted_sqm += 1
        
        floor_val = extract_floor(description)
        if floor_val:
            df.at[idx, 'floor'] = floor_val
            extracted_floor += 1

print(f"Extraction complete")
print(f"  Extracted sqm: {extracted_sqm} values")
print(f"  Extracted floor: {extracted_floor} values\n")

# Step 2: Optional geocoding - let's skip this for now since it's too slow
# We'll use a simpler approach: just count what we have

# Step 3: Save updated file
output_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df.to_excel(output_path, index=False)
print(f"Saved to: {output_path}\n")

# Step 4: Print summary
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
