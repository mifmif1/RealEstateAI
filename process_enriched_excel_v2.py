import pandas as pd
import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
print(f'Loading: {file_path}')
df = pd.read_excel(file_path)
print(f'Loaded {len(df)} rows\n')

# Ensure columns exist
for col in ['sqm', 'floor', 'latitude', 'longitude']:
    if col not in df.columns:
        df[col] = None

def extract_sqm_v2(description):
    """Improved sqm extraction with more patterns"""
    if pd.isna(description):
        return None
    
    desc_str = str(description).strip()
    
    # Comprehensive patterns for sqm
    patterns = [
        # "total area of X sqm"
        r'total\s+area\s+of\s+([0-9,]+(?:\.[0-9]+)?)',
        # "X sqm" or "X sq.m"
        r'(\d+(?:[.,]\d+)?)\s*(?:sqm|sq\.m|square\s+meters)',
        # "X m2" or "X m²"
        r'(\d+(?:[.,]\d+)?)\s*(?:m2|m²)',
        # Greek: "X τ.μ." or "X τμ"
        r'(\d+(?:[.,]\d+)?)\s*(?:τ\.μ\.?|τμ\.?)',
        # "area: X" or "area of X"
        r'area\s*(?:of|:)?\s*([0-9,]+(?:\.[0-9]+)?)',
        # Greek: "Εμ. X"
        r'Εμ\.\s*(\d+(?:[.,]\d+)?)',
    ]
    
    all_matches = []
    for pattern in patterns:
        found = re.findall(pattern, desc_str, re.IGNORECASE)
        if found:
            all_matches.extend(found)
    
    if all_matches:
        try:
            values = []
            for m in all_matches:
                clean = str(m).replace(',', '').replace(' ', '')
                val = float(clean)
                if val > 0 and val < 100000:  # Reasonable range
                    values.append(val)
            
            if values:
                return max(values)  # Return largest value
        except:
            pass
    
    return None

def extract_floor_v2(description):
    """Improved floor extraction"""
    if pd.isna(description):
        return None
    
    desc_str = str(description).lower()
    
    # Greek floor indicators
    if 'basement' in desc_str or 'υπόγειο' in desc_str or 'basement' in desc_str:
        return 'Basement'
    
    if 'ground' in desc_str or 'ισόγειο' in desc_str or 'ισογείου' in desc_str or 'ground floor' in desc_str:
        return 'Ground'
    
    if 'attic' in desc_str or 'σοφίτα' in desc_str:
        return 'Attic'
    
    if 'penthouse' in desc_str:
        return 'Penthouse'
    
    if 'house' in desc_str or 'maisonette' in desc_str or 'villa' in desc_str:
        return 'House'
    
    # Look for numbered floors - try multiple patterns
    floor_patterns = [
        r'floor\s+(\d+)',
        r'(\d+)(?:st|nd|rd|th)\s+floor',
        r'(?:on\s+)?(?:the\s+)?(\d+)(?:st|nd|rd|th)',
        r'(\d+)\s*(?:ης|η|ος|ο)?\s*ορόφ(?:ου|α|ος)',
        r'level\s+(\d+)',
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
new_sqm = 0
new_floor = 0

for idx in range(min(330, len(df))):
    if 'Description' in df.columns:
        description = df.iloc[idx]['Description']
        
        # Only extract if not already present
        if pd.isna(df.at[idx, 'sqm']):
            sqm_val = extract_sqm_v2(description)
            if sqm_val:
                df.at[idx, 'sqm'] = sqm_val
                new_sqm += 1
        
        if pd.isna(df.at[idx, 'floor']):
            floor_val = extract_floor_v2(description)
            if floor_val:
                df.at[idx, 'floor'] = floor_val
                new_floor += 1

print(f"Extraction complete")
print(f"  New sqm values: {new_sqm}")
print(f"  New floor values: {new_floor}\n")

# Step 2: Save updated file
output_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df.to_excel(output_path, index=False)
print(f"Saved to: {output_path}\n")

# Step 3: Print summary
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
