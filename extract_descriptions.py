import openpyxl
import sys

# Set stdout to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# Load the workbook
wb = openpyxl.load_workbook('excel_db/assets_raw_100526.xlsx')
ws = wb.active

# Get column D (Description) for rows 20-330
for row_num in range(20, 331):
    cell_value = ws[f'D{row_num}'].value
    if cell_value is not None:
        print(f'Row {row_num}: {str(cell_value)}')
    else:
        print(f'Row {row_num}: (empty)')
