from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Font
import os

output_path = 'static/bh_import_template.xlsx'

# Create Workbook
wb = Workbook()
ws = wb.active
ws.title = "Import Template"

# Headers
headers = ['平台', 'AccID', '名稱', 'Budget', 'StartDate', 'EndDate', 'CPCGoal', 'CPAGoal', 'R的cv定義', 'Token']
ws.append(headers)

# Style Headers
for cell in ws[1]:
    cell.font = Font(bold=True)

# Sample Data
data = [
    # R Platform example
    ['R', '111222', 'Rixbee 範例帳戶', 50000, '2024-01-01', '2024-12-31', 15, 250, 'CompleteCheckout', ''],
    # D Platform example
    ['D', '333444', 'Discovery 範例帳戶', 30000, '2024-02-01', '2024-06-30', 10, 200, '', 'example_token_abcdef123']
]

for row in data:
    ws.append(row)

# --- Formatting ---
# Col B (AccID): Number format "0"
# Col E, F (Dates): Date format "yyyy-mm-dd"

# Set column widths
ws.column_dimensions['A'].width = 8
ws.column_dimensions['B'].width = 15
ws.column_dimensions['C'].width = 25
ws.column_dimensions['D'].width = 12
ws.column_dimensions['E'].width = 12
ws.column_dimensions['F'].width = 12
ws.column_dimensions['G'].width = 10
ws.column_dimensions['H'].width = 10
ws.column_dimensions['I'].width = 25
ws.column_dimensions['J'].width = 30

# Apply formatting to all rows (1-1000)
for row in ws.iter_rows(min_row=2, max_row=1000):
    # AccID (Col 2)
    row[1].number_format = '0'
    # Dates (Col 5, 6)
    row[4].number_format = 'yyyy-mm-dd'
    row[5].number_format = 'yyyy-mm-dd'

# --- Data Validation ---

# 1. Platform (Col A)
dv_platform = DataValidation(type="list", formula1='"R,D"', allow_blank=False)
dv_platform.error = '必須填寫 R 或 D'
dv_platform.errorTitle = '輸入錯誤'
ws.add_data_validation(dv_platform)
dv_platform.add('A2:A1000')

# 2. CV Definition (Col I)
cv_options = [
    'CompleteCheckout', 
    'AddToCart', 
    'ViewContent', 
    'Checkout', 
    'Bookmark', 
    'Search', 
    'CompleteRegistration'
]
# Create formula string "Option1,Option2,..."
dv_formula = '"' + ','.join(cv_options) + '"'

dv_cv = DataValidation(type="list", formula1=dv_formula, allow_blank=True)
dv_cv.error = '請選擇清單中的項目'
dv_cv.errorTitle = '輸入無效'
dv_cv.prompt = '請從選單選擇轉換定義'
dv_cv.promptTitle = '選擇轉換定義'

ws.add_data_validation(dv_cv)
dv_cv.add('I2:I1000')

# Save
wb.save(output_path)
print(f"Generated {output_path} with data validation.")
