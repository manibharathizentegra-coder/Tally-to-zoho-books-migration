import sys; sys.path.insert(0, '.')
from opening_balance_converter import convert

tests = [
    ('opening balance/Tally_TrialBal.json', 'FORMAT A - Closing Balance (flat)'),
    ('TrialBal.json',                        'FORMAT B - Opening Balance (nested)'),
]

for fname, label in tests:
    print(f'=== {label} ===')
    with open(fname, 'rb') as f:
        raw = f.read()
    output_bytes, summary, errors = convert(raw, fname.split('/')[-1], '31/03/2025')
    if errors:
        print('  ERRORS:', errors)
    else:
        print(f'  Accounts   : {summary["accounts_found"]}')
        print(f'  Zoho rows  : {summary["zoho_rows"]}')
        print(f'  Total Debit: {summary["total_debit"]:,.2f}')
        print(f'  Total Credit:{summary["total_credit"]:,.2f}')
        print(f'  File type  : {summary["file_type"]}')
        print(f'  Preview:')
        for row in summary['preview'][:5]:
            print(f'    {row[0][:35]:<35} {row[1]:<8} {row[2]:>15,.2f}')
    print()
