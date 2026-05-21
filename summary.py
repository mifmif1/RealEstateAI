import pandas as pd

file_path = r"excel_db\assets_raw_100526_enriched.xlsx"
df = pd.read_excel(file_path)

print("")
print("=== FINAL SUMMARY ===")
print("")
print("Total rows: {}".format(len(df)))
print("Rows with sqm data: {}/{}".format(df['sqm'].notna().sum(), len(df)))
print("Rows with floor data: {}/{}".format(df['floor'].notna().sum(), len(df)))
print("")
print("Rows missing sqm: {}".format(df['sqm'].isna().sum()))
print("Rows missing floor: {}".format(df['floor'].isna().sum()))
print("")
print("=== Floor Level Distribution ===")
counts = df['floor'].value_counts().sort_index()
for floor_level, count in counts.items():
    print("{}: {}".format(floor_level, count))

