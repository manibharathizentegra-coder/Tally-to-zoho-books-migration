import json
import openpyxl
import warnings
warnings.filterwarnings("ignore")

BASE = r'c:\Users\Zen\Desktop\Software_With_Front_END\opening balance'

# ================================================================
# TALLY JSON SIGN CONVENTION (confirmed from debug):
#   dspcldramta = NEGATIVE  → DEBIT balance  (abs value is the amount)
#   dspcldramta = None  AND  dspclcramta = POSITIVE → CREDIT balance
#   dspclcramta = NEGATIVE  → DEBIT balance (occasionally, same logic)
# ================================================================

with open(BASE + r'\Tally_TrialBal.json', 'r', encoding='utf-16') as f:
    data = json.load(f)

lines = data['dspaccbody']['dspaccline']
tally = {}
for l in lines:
    name = l.get('dspaccname', {}).get('dspdispname', '').strip()
    info = l.get('dspaccinfo', [{}])
    if not info:
        continue
    dr_raw = info[0].get('dspcldramt', {}).get('dspcldramta', None)
    cr_raw = info[0].get('dspclcramt', {}).get('dspclcramta', None)

    dr = 0.0
    cr = 0.0

    if dr_raw is not None:
        val = float(dr_raw)
        if val < 0:
            # Negative dspcldramta = DEBIT balance in Tally
            dr = abs(val)
        elif val > 0:
            # Positive dspcldramta (rare) = might be a credit shown in dr col
            cr = val
        # val == 0 → zero, skip

    if cr_raw is not None:
        val = float(cr_raw)
        if val > 0:
            # Positive dspclcramta = CREDIT balance in Tally
            cr = val
        elif val < 0:
            # Negative dspclcramta = DEBIT balance
            dr = abs(val)

    if dr > 0 or cr > 0:
        tally[name] = {'dr': round(dr, 2), 'cr': round(cr, 2)}

print(f'Tally ledgers (non-zero): {len(tally)}')

# Quick validation with known ledgers from photos
for chk in ['Volvo XC 60B5', 'Waterlogic Machines', 'IGST Input- Import', 'CGST Input 9%',
            'TDS Receivable', 'Car Innova -Crysta']:
    if chk in tally:
        t = tally[chk]
        print(f"  CHECK {chk}: DR={t['dr']:>14,.2f}  CR={t['cr']:>14,.2f}")

# ---- Load Zoho ----
wb = openpyxl.load_workbook(BASE + r'\zoho_report.xlsx')
ws = wb.active

zoho = {}
skip_keywords = ['Total for', 'demo2zentegra', 'Account ', 'Trial Balance', 'Assets', 'Liabilities',
                 'Income', 'Equity', 'Expenses', 'Other', 'Purchase Accounts']
for row in ws.iter_rows(values_only=True):
    acc, net_dr, net_cr = row[0], row[1], row[2]
    if acc is None:
        continue
    acc_clean = str(acc).strip()
    skip = False
    for kw in skip_keywords:
        if kw.lower() in acc_clean.lower():
            skip = True
            break
    if skip:
        continue
    if net_dr is None and net_cr is None:
        continue
    try:
        dr = float(net_dr) if net_dr not in (None, '') else 0.0
        cr = float(net_cr) if net_cr not in (None, '') else 0.0
    except (ValueError, TypeError):
        continue
    if dr == 0 and cr == 0:
        continue
    acc_key = acc_clean.lstrip()
    zoho[acc_key] = {'dr': round(dr, 2), 'cr': round(cr, 2)}

print(f'Zoho accounts (non-zero): {len(zoho)}')

# ---- Normalize ----
def norm(s):
    return s.lower().replace('  ', ' ').strip()

tally_norm = {norm(k): (k, v) for k, v in tally.items()}
zoho_norm  = {norm(k): (k, v) for k, v in zoho.items()}

# ---- Compare ----
matched   = []
mismatched = []
only_tally = []
only_zoho  = []

for nk, (tk, tv) in tally_norm.items():
    if nk in zoho_norm:
        zk, zv = zoho_norm[nk]
        # Compare DR and CR separately (with small tolerance)
        dr_diff = round(abs(tv['dr'] - zv['dr']), 2)
        cr_diff = round(abs(tv['cr'] - zv['cr']), 2)
        total_diff = round(dr_diff + cr_diff, 2)

        if total_diff < 0.05:
            matched.append({
                'ledger': tk, 'tally_dr': tv['dr'], 'tally_cr': tv['cr'],
                'zoho_dr': zv['dr'], 'zoho_cr': zv['cr'], 'diff': 0
            })
        else:
            # Detect reversal pattern
            reversed_dr_cr = (abs(tv['dr'] - zv['cr']) < 0.05 and abs(tv['cr'] - zv['dr']) < 0.05)
            mismatched.append({
                'ledger': tk,
                'tally_dr': tv['dr'], 'tally_cr': tv['cr'],
                'zoho_dr': zv['dr'], 'zoho_cr': zv['cr'],
                'dr_diff': dr_diff, 'cr_diff': cr_diff,
                'total_diff': total_diff,
                'reversed': reversed_dr_cr,
                'issue_type': 'DR_CR_REVERSAL' if reversed_dr_cr else 'AMOUNT_DIFF'
            })
    else:
        only_tally.append({'ledger': tk, 'tally_dr': tv['dr'], 'tally_cr': tv['cr']})

for nk, (zk, zv) in zoho_norm.items():
    if nk not in tally_norm:
        only_zoho.append({'ledger': zk, 'zoho_dr': zv['dr'], 'zoho_cr': zv['cr']})

print(f'\n=== SUMMARY ===')
print(f'Matched (correct):           {len(matched)}')
print(f'Mismatched:                  {len(mismatched)}')
reversal_count = sum(1 for r in mismatched if r['reversed'])
print(f'  -> DR/CR Reversed:         {reversal_count}')
print(f'  -> Actual amount diff:     {len(mismatched) - reversal_count}')
print(f'Only in Tally (missing Zoho): {len(only_tally)}')
print(f'Only in Zoho (extra):         {len(only_zoho)}')

print(f'\n=== MISMATCHED ===')
for r in sorted(mismatched, key=lambda x: -x['total_diff'])[:30]:
    tag = '[REVERSED]' if r['reversed'] else '[AMT DIFF]'
    print(f"  [{tag}] {r['ledger']}")
    print(f"    Tally: DR={r['tally_dr']:>14,.2f}  CR={r['tally_cr']:>14,.2f}")
    print(f"    Zoho:  DR={r['zoho_dr']:>14,.2f}  CR={r['zoho_cr']:>14,.2f}")
    print(f"    Diff={r['total_diff']:>12,.2f}")

# Save
import json as _json
result = {
    'summary': {
        'matched': len(matched),
        'mismatched': len(mismatched),
        'only_tally': len(only_tally),
        'only_zoho': len(only_zoho),
        'reversal_count': reversal_count,
        'pure_diff_count': len(mismatched) - reversal_count
    },
    'mismatched': sorted(mismatched, key=lambda x: -x['total_diff']),
    'only_tally': only_tally,
    'only_zoho': only_zoho,
    'matched': matched
}
with open(BASE + r'\comparison_result.json', 'w', encoding='utf-8') as f:
    _json.dump(result, f, indent=2)
print('\nSaved comparison_result.json OK')
