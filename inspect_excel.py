import pandas as pd
import re
import sys
import time

# Set UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
print(f'Loading: {file_path}')
df = pd.read_excel(file_path)

print(f'Loaded {len(df)} rows')
print(f'Columns: {list(df.columns)}')
print()

# Show sample
if 'Description' in df.columns:
    print('Sample Description (row 0):')
    print(repr(str(df.iloc[0]['Description'])[:150]))
    print()

# Check current coordinates
has_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()
print(f'Rows with coordinates: {has_coords}')
