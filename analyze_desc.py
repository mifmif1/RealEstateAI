import pandas as pd
import re

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

print("Analyzing descriptions with missing sqm:\n")
print("Sample rows without sqm extraction:\n")

count = 0
for idx in range(min(330, len(df))):
    if 'Description' in df.columns and pd.isna(df.at[idx, 'sqm']):
        desc = str(df.iloc[idx]['Description'])
        if len(desc) > 50:
            print(f"Row {idx}: {desc[:100]}...\n")
            count += 1
            if count >= 5:
                break
