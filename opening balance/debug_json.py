import json, warnings
warnings.filterwarnings("ignore")

with open(r'c:\Users\Zen\Desktop\Software_With_Front_END\opening balance\Tally_TrialBal.json', 'r', encoding='utf-16') as f:
    data = json.load(f)

lines = data['dspaccbody']['dspaccline']

# Check specific ledgers from the photos
check = ['Volvo XC 60B5', 'Waterlogic Machines', 'IGST Input 18%', 'IGST Input- Import',
         'Car Innova -Crysta', 'Opening Stock', 'TDS Receivable', 'CGST Input 9%']

print("=== RAW JSON VALUES ===")
for l in lines:
    name = l.get('dspaccname', {}).get('dspdispname', '').strip()
    if any(c.lower() in name.lower() for c in check):
        info = l.get('dspaccinfo', [{}])
        dr_raw = info[0].get('dspcldramt', {})
        cr_raw = info[0].get('dspclcramt', {})
        print(f"\nLedger: {name}")
        print(f"  dspcldramt full: {dr_raw}")
        print(f"  dspclcramt full: {cr_raw}")
        dr_val = dr_raw.get('dspcldramta', 'MISSING')
        cr_val = cr_raw.get('dspclcramta', 'MISSING')
        print(f"  DR value: {dr_val}")
        print(f"  CR value: {cr_val}")
