# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys
import re

# Set output encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'excel_db/assets_raw_100526_enriched.xlsx'

def extract_sqm(text):
    """Extract sqm value from text using simple patterns"""
    if not text or pd.isna(text):
        return None
    
    text_str = str(text).lower()
    
    # Patterns to search for sqm values
    patterns = [
        r'(\d+[.,]\d+)\s*sq\s*\.?\s*m',  # XXX sq.m or XXX sq m
        r'(\d+[.,]\d+)\s*sqm',           # XXX sqm
        r'(\d+[.,]\d+)\s*m[2²]',         # XXX m2 or XXX m²
        r'total area[:\s]+(\d+[.,]\d+)',  # total area XXX
        r'area[:\s]+(\d+[.,]\d+)',       # area XXX
        r'(\d+[.,]\d+)\s*square meters',  # XXX square meters
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_str)
        if matches:
            # Get the first match
            try:
                value = float(matches[0].replace(',', '.'))
                return value
            except:
                continue
    
    return None

def extract_floor(text):
    """Extract floor information from text"""
    if not text or pd.isna(text):
        return None
    
    text_str = str(text).lower()
    
    # Floor indicators
    floor_patterns = {
        'basement c': 'Basement C',
        'basement b': 'Basement B',
        'basement a': 'Basement A',
        'basement': 'Basement',
        'semi-basement': 'Semi-basement',
        'ground floor': 'Ground floor',
        '1st floor': '1st floor',
        'first floor': '1st floor',
        '2nd floor': '2nd floor',
        'second floor': '2nd floor',
        '3rd floor': '3rd floor',
        'third floor': '3rd floor',
        '4th floor': '4th floor',
        'fourth floor': '4th floor',
        'penthouse': 'Penthouse',
        'attic': 'Attic',
    }
    
    # Check for each pattern
    for pattern, label in floor_patterns.items():
        if pattern in text_str:
            return label
    
    return None

# Load the Excel file
if os.path.exists(excel_path):
    df = pd.read_excel(excel_path)
    print('Excel file loaded successfully!')
    print('Total rows:', len(df))
    print()
    
    # Extract sqm and floor values
    sqm_count = 0
    floor_count = 0
    samples = []
    
    for idx, row in df.iterrows():
        description = row['Description']
        
        # Extract sqm
        sqm_value = extract_sqm(description)
        if sqm_value is not None and pd.isna(row['sqm']):
            df.at[idx, 'sqm'] = sqm_value
            sqm_count += 1
        
        # Extract floor
        floor_value = extract_floor(description)
        if floor_value is not None and pd.isna(row['floor']):
            df.at[idx, 'floor'] = floor_value
            floor_count += 1
            
            # Collect sample for display
            if len(samples) < 10:
                desc_preview = str(description)[:100] + '...' if len(str(description)) > 100 else description
                samples.append({
                    'id': row['Asset id'],
                    'description_preview': desc_preview,
                    'extracted_floor': floor_value
                })
    
    # Save the updated file
    df.to_excel(excel_path, index=False)
    print('File saved to:', excel_path)
    print()
    print('EXTRACTION SUMMARY:')
    print('  Sqm values found and filled:', sqm_count)
    print('  Floor values found and filled:', floor_count)
    print()
    print('SAMPLE ROWS (first 10 floor extractions):')
    for i, sample in enumerate(samples, 1):
        print('  ' + str(i) + '. Asset ID:', sample['id'])
        print('     Description:', sample['description_preview'])
        print('     Extracted Floor:', sample['extracted_floor'])
        print()
else:
    print('File not found:', excel_path)
