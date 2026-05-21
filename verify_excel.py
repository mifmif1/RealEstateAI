import pandas as pd

# Load the updated Excel file
file_path = r"excel_db\assets_raw_100526_enriched.xlsx"
df = pd.read_excel(file_path)

# Show some statistics and samples
print("DETAILED SUMMARY")
print("=" * 80)
print("Total rows: {}".format(len(df)))
print()

print("Rows with sqm filled: {}".format(df['sqm'].notna().sum()))
print("Rows with floor filled: {}".format(df['floor'].notna().sum()))
print("Rows missing sqm: {}".format(df['sqm'].isna().sum()))
print("Rows missing floor: {}".format(df['floor'].isna().sum()))
print()

print("Floor distribution:")
print(df['floor'].value_counts().sort_index())
print()

print("Sample of rows 20-30 (extracted data):")
print(df[['Description', 'sqm', 'floor']].iloc[20:30].to_string())
print()

print("Sample of rows with missing data:")
missing_sqm = df[df['sqm'].isna()].head(5)
if len(missing_sqm) > 0:
    print("Missing sqm (first 5):")
    print(missing_sqm[['Description', 'sqm', 'floor']].to_string())
print()

missing_floor = df[df['floor'].isna()].head(5)
if len(missing_floor) > 0:
    print("Missing floor (first 5):")
    print(missing_floor[['Description', 'sqm', 'floor']].to_string())
