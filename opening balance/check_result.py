import json
with open(r'c:\Users\Zen\Desktop\Software_With_Front_END\opening balance\comparison_result.json', 'r', encoding='utf-8') as f:
    d = json.load(f)
print('Summary:', d['summary'])
print('Top 5 mismatches:')
for r in d['mismatched'][:5]:
    name = r['ledger']
    diff = r['diff']
    print(f"  {name} | Diff={diff:,.2f} | T_DR={r['tally_dr']:,.2f} T_CR={r['tally_cr']:,.2f} | Z_DR={r['zoho_dr']:,.2f} Z_CR={r['zoho_cr']:,.2f}")
