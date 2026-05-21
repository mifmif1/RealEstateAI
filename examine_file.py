# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys

# Set output encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'excel_db/assets_raw_100526_enriched.xlsx'

if os.path.exists(excel_path):
    df = pd.read_excel(excel_path)
    print('Excel file loaded successfully!')
    print(f'Shape: {df.shape}')
    print(f'Columns: {", ".join(str(c) for c in df.columns)}')
    print()
    print('First 3 rows:')
    for idx in range(min(3, len(df))):
        print(f'Row {idx}:')
        for col in df.columns:
            val = df.iloc[idx][col]
            if val is not None and str(val).strip():
                print(f'  {col}: {val}')
else:
    print(f'File not found: {excel_path}')
