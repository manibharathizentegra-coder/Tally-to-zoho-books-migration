import re

with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace all my injected backend calls with the proper module aliases
content = content.replace('invoice_backend.parse_tally_json', 'invoice_module.parse_tally_json')
content = content.replace('bills_backend.parse_tally_json', 'bills_module.parse_tally_json')
content = content.replace('receipts_backend.parse_tally_json', 'receipts_module.parse_tally_json')
content = content.replace('sale_backend.parse_tally_json', 'sales_order_module.parse_tally_json')
content = content.replace('purchase_order_backend.parse_tally_json', 'purchase_order_module.parse_tally_json')
content = content.replace('journel_backend.parse_tally_json', 'journel_module.parse_tally_json')

# Wait, app.py also needs UPLOAD_DIR defined. Let's make sure it is.
if 'UPLOAD_DIR =' not in content:
    content = content.replace('import json', 'import json\nimport os\n\nUPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")\nos.makedirs(UPLOAD_DIR, exist_ok=True)')

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)
print(" Fixed app.py aliases")
