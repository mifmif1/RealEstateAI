import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

# Get comprehensive stats
total = len(df)
sqm_count = df['sqm'].notna().sum()
floor_count = df['floor'].notna().sum()
coords_count = (df['latitude'].notna() & df['longitude'].notna()).sum()

print("\n" + "=" * 70)
print("ENRICHED EXCEL FILE - FINAL RESULTS")
print("=" * 70)
print(f"\nFile: excel_db/assets_raw_100526_enriched.xlsx")
print(f"Rows processed: {total}")
print()
print(f"DATA EXTRACTION SUMMARY:")
print(f"  [SUCCESS] Rows with sqm (area): {sqm_count}/{total} ({round(100*sqm_count/total, 1)}%)")
print(f"  [SUCCESS] Rows with floor level: {floor_count}/{total} ({round(100*floor_count/total, 1)}%)")
print(f"  [PARTIAL] Rows with coordinates: {coords_count}/{total} ({round(100*coords_count/total, 1)}%)")
print()
print("EXTRACTION DETAILS:")
print(f"  - sqm values extracted from Description column")
print(f"  - floor values extracted from Description column")
print(f"  - Patterns support English and Greek text")
print(f"  - Greek patterns include: τ.μ., ισόγειο, υπόγειο, etc.")
print()
print("=" * 70)
print()

# Show some stats about the data
print("Data Statistics:")
print(f"  Average sqm: {df['sqm'].mean():.1f}")
print(f"  Min sqm: {df['sqm'].min():.1f}")
print(f"  Max sqm: {df['sqm'].max():.1f}")
print()
print(f"Floor distribution (top 10):")
floor_dist = df['floor'].value_counts()
for floor, count in floor_dist.head(10).items():
    print(f"  {floor}: {count} rows")
