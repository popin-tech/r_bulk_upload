"""⚠️⚠️ 警告：不要拿這支去覆蓋線上的 static/bh_import_template.xlsx。

committed 的 static/bh_import_template.xlsx 是**手工維護**的，不是這支產生的：
  1. 它內含真實客戶範例列（juliart_覺亞髮品），帶真的 D / MGID token，AE 是照著那三列填的；
  2. tests/test_bh_token_import.py 把它當成「恰好 3 列 R/D/M」的實測夾具，
     斷言 result == {"total": 3, "inserted": 3, "errors": []}。

直接跑這支會把上面兩者一起毀掉（2026-09-02 實際踩過）。

要改「平台」下拉清單時，請就地改 data validation、不要重產整份檔案，例如：

    from openpyxl import load_workbook
    wb = load_workbook('static/bh_import_template.xlsx'); ws = wb.active
    dv = next(d for d in ws.data_validations.dataValidation if str(d.sqref).startswith('A2'))
    dv.formula1 = '"R,D,M,P,V"'
    wb.save('static/bh_import_template.xlsx')

這支保留下來當「欄位與驗證規則的可讀文件」，以及全新建檔時的起點。
"""
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
headers = ['平台', 'AccID', '名稱', 'Budget', 'StartDate', 'EndDate', 'CPCGoal', 'CPAGoal', 'R的cv定義', 'D&MGID的Token']
ws.append(headers)

# Style Headers
for cell in ws[1]:
    cell.font = Font(bold=True)

# Sample Data
data = [
    # R Platform example
    ['R', 111222, 'Rixbee 範例帳戶', 50000, '2024-01-01', '2024-12-31', 15, 250, 'CompleteCheckout', ''],
    # D Platform example
    ['D', 333444, 'Discovery 範例帳戶', 30000, '2024-02-01', '2024-06-30', 10, 200, '', 'example_token_abcdef123'],
    # P Platform（Prism）example：AccID＝廣告主 id，格式 233-688-3595。
    # 平台無轉換追蹤，CPAGoal 留空。
    ['P', '292-462-3142', 'Prism 範例帳戶', 80000, '2026-08-01', '2026-08-31', 12, None, '', ''],
    # V Platform（D1 影音）example：AccID＝D1 影音帳戶字串，大小寫必須完全一致。
    # 平台無轉換追蹤，CPAGoal 留空。
    ['V', 'CPM_MundoPixarExperience', 'D1影音 範例帳戶', 150000, '2026-08-01', '2026-08-31', 20, None, '', ''],
]

for row in data:
    ws.append(row)

# --- Formatting ---
# Col B (AccID): Number format "0"
# Col F, G (Dates): Date format "yyyy-mm-dd" (Shifted by 1 due to inserted column)

# Set column widths
ws.column_dimensions['A'].width = 8
ws.column_dimensions['B'].width = 15
ws.column_dimensions['C'].width = 25  # Name
ws.column_dimensions['D'].width = 12  # Budget
ws.column_dimensions['E'].width = 12  # StartDate
ws.column_dimensions['F'].width = 12  # EndDate
ws.column_dimensions['G'].width = 10  # CPC
ws.column_dimensions['H'].width = 10  # CPA
ws.column_dimensions['I'].width = 25  # CV Def
ws.column_dimensions['J'].width = 30  # D 與 MGID 共用 Token

# Apply formatting to all rows (1-1000)
for row in ws.iter_rows(min_row=2, max_row=1000):
    # AccID (Col 2)
    row[1].number_format = '0'
    # Dates (Col 5, 6) -> Index 4, 5
    row[4].number_format = 'yyyy-mm-dd'
    row[5].number_format = 'yyyy-mm-dd'

# --- Data Validation ---

# 1. Platform (Col A)
dv_platform = DataValidation(type="list", formula1='"R,D,M,P,V"', allow_blank=False)
dv_platform.error = '必須填寫 R、D、M、P（Prism）或 V（D1影音）'
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
    'CompleteRegistration',
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
