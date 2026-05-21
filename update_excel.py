import pandas as pd
import re
import numpy as np
import sys

# Fix encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load the enriched Excel file
file_path = r"excel_db\assets_raw_100526_enriched.xlsx"
df = pd.read_excel(file_path)

print("Loaded Excel file with {} rows".format(len(df)))
print("Columns: {}".format(df.columns.tolist()))
print()

# Create sqm and floor columns if they don't exist
if 'sqm' not in df.columns:
    df['sqm'] = np.nan
if 'floor' not in df.columns:
    df['floor'] = np.nan

# Update with provided mappings (rows 0-19)
provided_data = {
    0: {'sqm': 3257, 'floor': 'Basement'},
    1: {'sqm': 1434.1, 'floor': 'Ground'},
    2: {'sqm': 645, 'floor': 'Basement'},
    3: {'sqm': 293.91, 'floor': 'Ground'},
    4: {'sqm': 136.34, 'floor': 'Basement'},
    5: {'sqm': 98.95, 'floor': '5th'},
    6: {'sqm': 78.85, 'floor': 'Ground'},
    7: {'sqm': 85, 'floor': '2nd'},
    8: {'sqm': 129.91, 'floor': '2nd-3rd'},
    9: {'sqm': 101.30, 'floor': '2nd'},
    10: {'sqm': 154.55, 'floor': 'Ground'},
    11: {'sqm': 118.74, 'floor': '1st'},
    12: {'sqm': 162.9, 'floor': '1st'},
    13: {'sqm': 161.07, 'floor': '2nd'},
    14: {'sqm': 102, 'floor': '2nd'},
    15: {'sqm': 77.76, 'floor': 'Basement'},
    16: {'sqm': 242.72, 'floor': 'House'},
    17: {'sqm': 130.50, 'floor': '1st'},
    18: {'sqm': 4100.2, 'floor': 'Basement'},
    19: {'sqm': 52.13, 'floor': 'Ground'},
}

updated_count = 0

# Apply provided mappings
for row_idx, values in provided_data.items():
    if row_idx < len(df):
        df.at[row_idx, 'sqm'] = values['sqm']
        df.at[row_idx, 'floor'] = values['floor']
        updated_count += 1

print("Updated {} rows with provided mappings (rows 0-19)".format(updated_count))

# Extract sqm and floor from Description column for remaining rows
def extract_sqm_and_floor(description):
    """Extract sqm and floor from description text"""
    if pd.isna(description):
        return None, None
    
    description = str(description)
    
    # Extract sqm value - look for numeric value followed by m2, sqm, sq.m, etc.
    sqm_match = re.search(r'(\d+[.,]\d+|\d+)\s*(?:m2|sqm|sq\.?m?)', description, re.IGNORECASE)
    sqm = None
    if sqm_match:
        sqm_str = sqm_match.group(1).replace(',', '.')
        sqm = float(sqm_str)
    
    # Extract floor - look for common floor patterns
    floor = None
    floor_patterns = [
        (r'(?:basement|basements)', 'Basement'),
        (r'(?:penthouse|pent house)', 'Penthouse'),
        (r'(?:attic|loft)', 'Attic'),
        (r'(?:ground floor|ground)', 'Ground'),
        (r'(?:house|single family)', 'House'),
        (r'(?:5th floor|5th)', '5th'),
        (r'(?:4th floor|4th)', '4th'),
        (r'(?:3rd floor|3rd)', '3rd'),
        (r'(?:2nd(?:–|-|/)?3rd|2nd-3rd)', '2nd-3rd'),
        (r'(?:2nd floor|2nd)', '2nd'),
        (r'(?:1st floor|1st)', '1st'),
    ]
    
    description_lower = description.lower()
    for pattern, floor_name in floor_patterns:
        if re.search(pattern, description_lower):
            floor = floor_name
            break
    
    return sqm, floor

# Extract for rows 20 onwards
extracted_count = 0
for row_idx in range(20, len(df)):
    if pd.isna(df.at[row_idx, 'sqm']) or pd.isna(df.at[row_idx, 'floor']):
        description = df.at[row_idx, 'Description'] if 'Description' in df.columns else None
        sqm, floor = extract_sqm_and_floor(description)
        
        if sqm is not None:
            df.at[row_idx, 'sqm'] = sqm
            extracted_count += 1
        
        if floor is not None:
            df.at[row_idx, 'floor'] = floor
            extracted_count += 1

print("Extracted data for rows 20 onwards: {} new values".format(extracted_count))

# Save the updated file
df.to_excel(file_path, index=False)
print("Updated file saved to: {}".format(file_path))

# Print summary
total_sqm_filled = df['sqm'].notna().sum()
total_floor_filled = df['floor'].notna().sum()
print("Summary:")
print("  Total rows with sqm: {}/{}".format(total_sqm_filled, len(df)))
print("  Total rows with floor: {}/{}".format(total_floor_filled, len(df)))

# Show sample of updated rows
print("Sample of updated rows (first 20):")
sample_df = df[['Description', 'sqm', 'floor']].head(20)
print(sample_df.to_string())
