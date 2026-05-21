import pandas as pd
import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

file_path = r'excel_db\assets_raw_100526_enriched.xlsx'
df = pd.read_excel(file_path)

def extract_sqm_v2(description):
    """Improved sqm extraction with more patterns"""
    if pd.isna(description):
        return None
    
    desc_str = str(description).strip()
    
    # Comprehensive patterns for sqm
    patterns = [
        # "total area of X sqm"
        r'total\s+area\s+of\s+([0-9,]+(?:\.[0-9]+)?)',
        # "X sqm" or "X sq.m"
        r'(\d+(?:[.,]\d+)?)\s*(?:sqm|sq\.m|square\s+meters)',
        # "X m2" or "X m²"
        r'(\d+(?:[.,]\d+)?)\s*(?:m2|m²)',
        # Greek: "X τ.μ." or "X τμ"
        r'(\d+(?:[.,]\d+)?)\s*(?:τ\.μ\.?|τμ\.?)',
        # "area: X" or "area of X"
        r'area\s*(?:of|:)?\s*([0-9,]+(?:\.[0-9]+)?)',
        # Greek: "Εμ. X"
        r'Εμ\.\s*(\d+(?:[.,]\d+)?)',
        # "X - Y sqm" (take first or both)
        r'(\d+)\s*-\s*\d+\s*(?:sqm|m2|m²)',
    ]
    
    all_matches = []
    for pattern in patterns:
        found = re.findall(pattern, desc_str, re.IGNORECASE)
        if found:
            all_matches.extend(found)
    
    if all_matches:
        try:
            values = []
            for m in all_matches:
                clean = str(m).replace(',', '').replace(' ', '')
                val = float(clean)
                if val > 0 and val < 100000:  # Reasonable range
                    values.append(val)
            
            if values:
                return max(values)  # Return largest value
        except:
            pass
    
    return None

print("Testing improved sqm extraction:\n")

# Test on first 50 rows
found = 0
for idx in range(min(50, len(df))):
    if 'Description' in df.columns:
        desc = df.iloc[idx]['Description']
        sqm = extract_sqm_v2(desc)
        if sqm:
            found += 1
            print(f"Row {idx}: {sqm} sqm")

print(f"\nFound sqm in {found}/50 rows")
