import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

# Calculate stats
rows_with_sqm = df['sqm'].notna().sum()
rows_with_floor = df['floor'].notna().sum()
rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()

print("=" * 60)
print("FINAL SUMMARY")
print("=" * 60)
print(f"Total rows processed: 329")
print(f"Rows with sqm: {rows_with_sqm}/329 ({round(100*rows_with_sqm/329, 1)}%)")
print(f"Rows with floor: {rows_with_floor}/329 ({round(100*rows_with_floor/329, 1)}%)")
print(f"Rows with coordinates: {rows_with_coords}/329 ({round(100*rows_with_coords/329, 1)}%)")
print("=" * 60)
print()

# Show some samples
print("Sample extractions:\n")
print("Rows with sqm values:")
for i in range(5):
    idx = df[df['sqm'].notna()].index[i]
    print(f"  Row {idx}: sqm={df.at[idx, 'sqm']}, floor={df.at[idx, 'floor']}")

print()
print("Rows with floor values:")
for i in range(5):
    idx = df[df['floor'].notna()].index[i]
    print(f"  Row {idx}: floor={df.at[idx, 'floor']}, sqm={df.at[idx, 'sqm']}")

print()
print("Rows with coordinates:")
for i in range(min(5, rows_with_coords)):
    idx = df[(df['latitude'].notna()) & (df['longitude'].notna())].index[i]
    print(f"  Row {idx}: ({df.at[idx, 'latitude']}, {df.at[idx, 'longitude']})")
