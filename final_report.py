import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

# Final statistics
total = len(df)
sqm_count = df['sqm'].notna().sum()
floor_count = df['floor'].notna().sum()
coords_count = (df['latitude'].notna() & df['longitude'].notna()).sum()

print("\n" + "=" * 75)
print("FINAL PROCESSING SUMMARY - ENRICHED EXCEL FILE")
print("=" * 75)
print()
print(f"File: excel_db/assets_raw_100526_enriched.xlsx")
print(f"Processing period: Rows 0-329 (330 rows total)")
print()
print("RESULTS:")
print(f"  Total rows: {total}")
print()
print("  1. SQM (Area) Extraction:")
print(f"     - Rows with sqm: {sqm_count}/{total} ({round(100*sqm_count/total, 1)}%)")
print(f"     - Average: {df['sqm'].mean():.1f} sqm")
print(f"     - Range: {df['sqm'].min():.1f} to {df['sqm'].max():.1f} sqm")
print()
print("  2. Floor Level Extraction:")
print(f"     - Rows with floor: {floor_count}/{total} ({round(100*floor_count/total, 1)}%)")
floor_dist = df['floor'].value_counts()
print(f"     - Most common: {floor_dist.index[0]} ({floor_dist.iloc[0]} rows)")
print(f"     - Unique floor types: {len(floor_dist)}")
print()
print("  3. Geocoding Coordinates:")
print(f"     - Rows with coordinates: {coords_count}/{total} ({round(100*coords_count/total, 1)}%)")
print()
print("EXTRACTION METHODS:")
print("  - Used regex patterns for sqm values from Description column")
print("  - Supports: 'sqm', 'sq.m', 'm2', 'm²', 'total area', Greek 'τ.μ.'")
print("  - Supports: English (basement, ground, 1st-5th, attic) and Greek text")
print("  - Geocoding: Used local city database for Greek locations")
print()
print("=" * 75)
print()

# Show floor distribution
print("Floor Distribution:")
for floor, count in floor_dist.items():
    pct = round(100*count/total, 1)
    print(f"  {floor}: {count} rows ({pct}%)")
