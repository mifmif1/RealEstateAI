# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'excel_db/assets_raw_100526_enriched.xlsx'

if os.path.exists(excel_path):
    df = pd.read_excel(excel_path)
    
    # Find rows with empty sqm
    empty_sqm_df = df[df['sqm'].isna()]
    print('Sample rows with empty SQM values:')
    print('Total empty sqm rows:', len(empty_sqm_df))
    print()
    
    for idx in range(min(5, len(empty_sqm_df))):
        row = empty_sqm_df.iloc[idx]
        desc = str(row['Description']) if row['Description'] else '[NO DESCRIPTION]'
        if len(desc) > 100:
            desc = desc[:100] + '...'
        print('Row ' + str(idx + 1))
        print('  Asset ID: ' + str(row['Asset id']))
        print('  Type: ' + str(row['Type']))
        print('  Description: ' + desc)
        print()
    
    print('='*80)
    print()
    
    # Find rows with empty floor
    empty_floor_df = df[df['floor'].isna()]
    print('Sample rows with empty FLOOR values:')
    print('Total empty floor rows:', len(empty_floor_df))
    print()
    
    for idx in range(min(5, len(empty_floor_df))):
        row = empty_floor_df.iloc[idx]
        desc = str(row['Description']) if row['Description'] else '[NO DESCRIPTION]'
        if len(desc) > 100:
            desc = desc[:100] + '...'
        print('Row ' + str(idx + 1))
        print('  Asset ID: ' + str(row['Asset id']))
        print('  Type: ' + str(row['Type']))
        print('  Description: ' + desc)
        print()
