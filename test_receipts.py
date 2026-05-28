import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.append('.')
import receipts.receipts_backend as rb

print("Fetching receipts from Tally...")
data = rb.get_all_receipts_data("20250401", "20250430")
if data:
    if "receipts" in data:
        print(f"Got {len(data['receipts'])} receipts")
    else:
        print("Data:", data)
else:
    print("None returned")
