# -*- coding: utf-8 -*-
import sys
import io

if sys.platform.startswith('win'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd

# Load original file
original_df = pd.read_excel('excel_db/assets_raw_100526.xlsx', engine='openpyxl')

print("Sample LINK column values:")
for i in range(min(20, len(original_df))):
    val = original_df.iloc[i]['LINK']
    if pd.notna(val):
        val_str = str(val)[:100]
        print(f"  Row {i}: {val_str}")

