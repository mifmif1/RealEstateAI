import pandas as pd

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

# Get the exact numbers
total = len(df)
rows_with_sqm = df['sqm'].notna().sum()
rows_with_floor = df['floor'].notna().sum()
rows_with_coords = (df['latitude'].notna() & df['longitude'].notna()).sum()

print("\n")
print("=" * 50)
print("PROCESSING COMPLETE")
print("=" * 50)
print(f"Total rows: {total}")
print(f"Rows with sqm: {rows_with_sqm}/{total}")
print(f"Rows with floor: {rows_with_floor}/{total}")
print(f"Rows with coordinates: {rows_with_coords}/{total}")
print("=" * 50)
