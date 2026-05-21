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
    
    # Patterns to search for sqm values - prioritize specific patterns
    patterns = [
        r'total area[:\s]+(\d+[.,]\d+)\s*sq',  # total area XXX sq
        r'(\d+[.,]\d+)\s*sq\s*\.?\s*m\b',      # XXX sq.m or XXX sq m (word boundary)
        r'(\d+[.,]\d+)\s*sqm\b',               # XXX sqm (word boundary)
        r'(\d+[.,]\d+)\s*m[2²]\b',             # XXX m2 or XXX m² (word boundary)
        r'area[:\s]+(\d+[.,]\d+)\s*sq',        # area XXX sq
        r'(\d+[.,]\d+)\s*square meters',       # XXX square meters
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_str)
        if matches:
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
    
    # Floor indicators with word boundaries to avoid false matches
    floor_patterns = [
        (r'\bbasement\s+c\b', 'Basement C'),
        (r'\bbasement\s+b\b', 'Basement B'),
        (r'\bbasement\s+a\b', 'Basement A'),
        (r'\bbasement\b', 'Basement'),
        (r'\bsemi-basement\b', 'Semi-basement'),
        (r'\bground\s+floor\b', 'Ground floor'),
        (r'\b1st\s+floor\b', '1st floor'),
        (r'\bfirst\s+floor\b', '1st floor'),
        (r'\b2nd\s+floor\b', '2nd floor'),
        (r'\bsecond\s+floor\b', '2nd floor'),
        (r'\b3rd\s+floor\b', '3rd floor'),
        (r'\bthird\s+floor\b', '3rd floor'),
        (r'\b4th\s+floor\b', '4th floor'),
        (r'\bfourth\s+floor\b', '4th floor'),
        (r'\bpenthouse\b', 'Penthouse'),
        (r'\battic\b', 'Attic'),
    ]
    
    # Check for each pattern with word boundaries
    for pattern, label in floor_patterns:
        if re.search(pattern, text_str):
            return label
    
    return None

# Load the Excel file
if os.path.exists(excel_path):
    df = pd.read_excel(excel_path)
    print('Excel file loaded successfully!')
    print('Total rows:', len(df))
    print()
    
    # Reset to original and re-extract with improved logic
    # First, reload to get clean state
    df = pd.read_excel('excel_db/assets_raw_100526_enriched.xlsx')
    
    # Extract sqm and floor values
    sqm_count = 0
    floor_count = 0
    samples = []
    
    for idx, row in df.iterrows():
        description = row['Description']
        
        # Extract sqm only if empty
        if pd.isna(row['sqm']) or (isinstance(row['sqm'], float) and pd.isna(row['sqm'])):
            sqm_value = extract_sqm(description)
            if sqm_value is not None:
                df.at[idx, 'sqm'] = sqm_value
                sqm_count += 1
        
        # Extract floor only if empty
        if pd.isna(row['floor']) or (isinstance(row['floor'], float) and pd.isna(row['floor'])):
            floor_value = extract_floor(description)
            if floor_value is not None:
                df.at[idx, 'floor'] = floor_value
                floor_count += 1
                
                # Collect sample for display
                if len(samples) < 15:
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
    print('SAMPLE ROWS (floor extractions):')
    for i, sample in enumerate(samples, 1):
        print('  ' + str(i) + '. Asset ID:', sample['id'])
        print('     Description:', sample['description_preview'])
        print('     Extracted Floor:', sample['extracted_floor'])
        print()
else:
    print('File not found:', excel_path)
