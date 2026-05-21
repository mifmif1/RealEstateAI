import pandas as pd
import os

excel_path = r'excel_db/assets_raw_100526_enriched.xlsx'

if os.path.exists(excel_path):
    df = pd.read_excel(excel_path)
    print('Current file status:')
    print('Total rows:', len(df))
    print()
    print('Sqm column - non-null values:', df['sqm'].notna().sum())
    print('Floor column - non-null values:', df['floor'].notna().sum())
    print()
    print('Sample of non-null sqm values:')
    print(df[df['sqm'].notna()][['Asset id', 'sqm']].head(10))
    print()
    print('Sample of non-null floor values:')
    print(df[df['floor'].notna()][['Asset id', 'floor']].head(10))
