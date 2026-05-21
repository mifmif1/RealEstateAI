import sys
from pathlib import Path

import pandas as pd

DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(DIR.parent.parent))

from model.asset_model import TargetAsset
from flow.spitogatos_flow import SpitogatosFlow

INPUT_FILE = DIR / 'DVG_with_floor_geocoded.xlsx'
OUTPUT_FILE = DIR / 'DVG_stats_210526.xlsx'

INITIAL_RADIUS_M = 100
MIN_ASSETS = 10
LIMIT = 200
RADIUS_MULTIPLIER = 1.5
MAX_RADIUS_ATTEMPTS = 3  # initial query + up to 3 expansions

print(f'Loading {INPUT_FILE}...')
df = pd.read_excel(INPUT_FILE)
print(f'Loaded {len(df)} records\n')

stats_columns = [
    'radius',
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
            source="DVG",
            portfolio="quick_25may26",
            id=str(row.get('Asset Code', idx)),
            lon=float(lon_val),
            lat=float(lat_val),
            sqm=float(sqm_val),
            price=float(price_val),
            url=str(row.get('Link', '')) if pd.notna(row.get('Link')) else None,
            level=int(row['level']) if pd.notna(row.get('level')) else None,
            construction_year=None,
        )

        radius_m = INITIAL_RADIUS_M
        stats = None
        for _ in range(MAX_RADIUS_ATTEMPTS):
            assets = flow.get_assets_by_circle(
                lon=asset.lon,
                lat=asset.lat,
                radius_meters=radius_m,
                limit=LIMIT,
            )
            if len(assets) >= MIN_ASSETS:
                stats = flow.get_asset_statistics_by_comparisons(asset, assets)
                df.at[idx, 'radius'] = radius_m
                break
            radius_m *= RADIUS_MULTIPLIER

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

print(f'\nSaving results to {OUTPUT_FILE}...')
df.to_excel(OUTPUT_FILE, index=False)

print('=' * 60)
print('COMPLETED')
print('=' * 60)
print(f'Total rows: {len(df)}')
print(f'Success: {success_count}')
print(f'Errors/Skipped: {error_count}')
print(f'Output: {OUTPUT_FILE}')
print('=' * 60)
