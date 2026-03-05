import json, openpyxl, warnings
warnings.filterwarnings("ignore")

BASE = r'c:\Users\Zen\Desktop\Software_With_Front_END\opening balance'

# ---- Load Tally raw ----
with open(BASE + r'\Tally_TrialBal.json', 'r', encoding='utf-16') as f:
    data = json.load(f)

lines = data['dspaccbody']['dspaccline']

# Check some with positive DR vs negative DR and how they appear visually
print("=== ALL TALLY SIGN PATTERNS (first 30 non-zero) ===")
count = 0
for l in lines:
    name = l.get('dspaccname', {}).get('dspdispname', '').strip()
    info = l.get('dspaccinfo', [{}])
    dr_raw = info[0].get('dspcldramt', {}).get('dspcldramta', None)
    cr_raw = info[0].get('dspclcramt', {}).get('dspclcramta', None)
    if dr_raw is None and cr_raw is None:
        continue
    if (dr_raw or 0) == 0 and (cr_raw or 0) == 0:
        continue
    count += 1
    if count > 30:
        break
    dr_sign = '+' if (dr_raw or 0) > 0 else ('-' if (dr_raw or 0) < 0 else '0')
    cr_sign = '+' if (cr_raw or 0) > 0 else ('-' if (cr_raw or 0) < 0 else '0')
    print(f"  {name:45s} | dspcldramta:{str(dr_raw):>18}({dr_sign})  dspclcramta:{str(cr_raw):>18}({cr_sign})")

# Also check Zoho for Volvo XC 60B5 and Waterlogic
print("\n=== ZOHO ROWS (check cols) ===")
wb = openpyxl.load_workbook(BASE + r'\zoho_report.xlsx')
ws = wb.active
check = ['volvo', 'waterlogic', 'igst input', 'cgst input', 'car innova', 'tds receivable', 'opening stock']
for row in ws.iter_rows(values_only=True):
    if row[0] is None: continue
    name = str(row[0]).strip().lstrip().lower()
    if any(c in name for c in check):
        print(f"  {str(row[0]).strip().lstrip():45s} | Net Debit={row[1]}  Net Credit={row[2]}")
