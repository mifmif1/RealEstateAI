import sys

import pandas as pd

from pathlib import Path



sys.path.insert(0, '..')



from model.asset_model import TargetAsset

from flow.spitogatos_flow import SpitogatosFlow



input_file = Path('properties_fully_geocoded.xlsx')

output_file = Path('assets_stats_110526.xlsx')



print(f'Loading {input_file}...')

df = pd.read_excel(input_file)

print(f'Loaded {len(df)} records\n')



stats_columns = [

    'no_assets',

    'reevaluated_price',

    'min_price_per_sqm',

    'max_price_per_sqm',

    'mean_price_per_sqm',

    'median_price_per_sqm',

    'std_price_per_sqm',

    'discount',

]

for col in stats_columns:

    df[col] = None



df['notes'] = None



flow = SpitogatosFlow()

error_count = 0

success_count = 0



for idx, row in df.iterrows():

    if (idx + 1) % 50 == 0:

        print(f'Progress: {idx + 1}/{len(df)}')



    notes = []

    lon_val = row.get('lon')

    lat_val = row.get('lat')

    sqm_val = row.get('sqm')

    price_val = row.get('Price')



    if pd.isna(lon_val) or pd.isna(lat_val):

        notes.append('no coords')

    if pd.isna(sqm_val):

        notes.append('no sqm')

    if pd.isna(price_val):

        notes.append('no price')



    if notes:

        df.at[idx, 'notes'] = '; '.join(notes)

        error_count += 1

        continue



    try:

        asset = TargetAsset(

            source=str(row.get('source', 'unknown')),

            portfolio="quick_11may26",

            id=str(row.get('Asset id', idx)),

            lon=float(lon_val),

            lat=float(lat_val),

            sqm=float(sqm_val),

            price=float(price_val),

            url=str(row.get('LINK', '')) if pd.notna(row.get('LINK')) else None,

            level=int(row['level']) if pd.notna(row.get('level')) else None,

            construction_year=None,

        )



        stats = flow.get_asset_statistics_by_radius(

            asset=asset,

            radius_meters=100,

            min_assets=10,

            limit=200

        )



        if stats:

            df.at[idx, 'no_assets'] = stats.no_assets

            df.at[idx, 'reevaluated_price'] = stats.reevaluated_price

            df.at[idx, 'min_price_per_sqm'] = stats.min

            df.at[idx, 'max_price_per_sqm'] = stats.max

            df.at[idx, 'mean_price_per_sqm'] = stats.mean

            df.at[idx, 'median_price_per_sqm'] = stats.median

            df.at[idx, 'std_price_per_sqm'] = stats.std

            df.at[idx, 'discount'] = stats.discount

            success_count += 1

        else:

            df.at[idx, 'notes'] = 'too few assets to compare with'

            error_count += 1

    except Exception as e:

        df.at[idx, 'notes'] = f'error: {str(e)[:100]}'

        error_count += 1

        if idx < 10:

            print(f'Row {idx}: Error - {str(e)[:100]}')



print(f'\nSaving results to {output_file}...')

df.to_excel(output_file, index=False)



print('='*60)

print('COMPLETED')

print('='*60)

print(f'Total rows: {len(df)}')

print(f'Success: {success_count}')

print(f'Errors/Skipped: {error_count}')

print(f'Output: {output_file}')

print('='*60)

