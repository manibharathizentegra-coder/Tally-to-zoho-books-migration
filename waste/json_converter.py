import re
import os

FILES_TO_PROCESS = [
    "invoices/invoice_backend.py",
    "bills/bills_backend.py",
    "receipts/receipts_backend.py",
    "sales_order/sale_backend.py",
    "purchase_order/purchase_order_backend.py",
    "journel/journel_backend.py" # it's spelt journel in the codebase
]

def process_file(filepath):
    # Some paths might have different spelling, e.g. invoices vs invoice
    if not os.path.exists(filepath):
        # try removing s
        alt = filepath.replace('invoices', 'invoice')
        if os.path.exists(alt): filepath = alt
        else:
            print(f" File not found: {filepath}")
            return
            
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if 'def parse_tally_json(' in content:
        print(f"Skipping {filepath}, already has parse_tally_json")
        return

    # Find the fetch_tally_* function body
    match = re.search(r'def fetch_tally_([a-z_]+)\(.*?\):(.*?)^\w', content, re.DOTALL | re.MULTILINE)
    if not match:
        match = re.search(r'def fetch_tally_([a-z_]+)\(.*?\):(.*?)$', content, re.DOTALL | re.MULTILINE)
        if not match:
            print(f" Could not find fetch_tally_* in {filepath}")
            return
            
    fetch_name = match.group(0).split('(')[0]
    body = match.group(2)

    # Extract the main loop over vouchers
    loop_match = re.search(r'^[ \t]*for (?:idx, )?v in vouchers:(.*?)return ', body, re.DOTALL | re.MULTILINE)
    if not loop_match:
        print(f" Could not find loop in {filepath}")
        return
        
    loop_body = loop_match.group(1)

    # The wrapper logic
    header = """
def parse_tally_json(json_path):
    \"\"\"Automatically generated JSON parser mirroring fetch_tally_* logic.\"\"\"
    import json
    import re
    try:
        with open(json_path, 'r', encoding='utf-16') as f:
            data = json.load(f)
    except:
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f" Could not decode JSON: {e}")
            return []

    # If the JSON is already a list of dicts (from app backup export)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]:
        return data

    vouchers = data.get('tallymessage', [])
    if not isinstance(vouchers, list):
        if isinstance(vouchers, dict): vouchers = [vouchers]
        else: vouchers = []

    parsed_data = []

    def get_list(obj, *keys):
        for k in keys:
            val = obj.get(k)
            if val:
                return val if isinstance(val, list) else [val]
        return []

    def get_text(obj, *keys):
        for k in keys:
            val = obj.get(k)
            if val is not None:
                if isinstance(val, dict):
                    # If it's a dict, get its first value
                    return str(list(val.values())[0]).strip() if val else ""
                return str(val).strip()
        return ""

    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
"""

    new_loop = loop_body
    
    # Indent new_loop exactly 8 spaces since we removed the original "for v in vouchers:"
    lines = new_loop.split('\n')
    base_indent = -1
    for l in lines:
        if l.strip():
            base_indent = len(l) - len(l.lstrip())
            break
            
    indented_lines = []
    for l in lines:
        if len(l) >= base_indent:
            indented_lines.append("        " + l[base_indent:])
        else:
            indented_lines.append(l)
            
    new_loop = "\n".join(indented_lines)

    # Replace specific beautifulsoup BS4 methods with get_text and get_list
    
    # 1. obj.find('FIELD').text -> get_text(obj, 'field')
    new_loop = re.sub(r'([a-zA-Z0-9_]+)\.find\([\'"]([A-Z.]+)[\'"]\)\.text', 
                 lambda m: f"get_text({m.group(1)}, '{m.group(2).lower()}')", 
                 new_loop)

    # 2. obj.find('FIELD') as check -> obj.get('field', {})
    new_loop = re.sub(r'([a-zA-Z0-9_]+)\.find\([\'"]([A-Z.]+)[\'"]\)', 
                 lambda m: f"{m.group(1)}.get('{m.group(2).lower()}', dict())", 
                 new_loop)

    # 3. obj.find_all('FIELD1') or obj.find_all('FIELD2') -> get_list(obj, 'field1', 'field2')
    new_loop = re.sub(r'([a-zA-Z0-9_]+)\.find_all\([\'"]([A-Z.]+)[\'"]\)(?:\s*or\s*[a-zA-Z0-9_]+\.find_all\([\'"]([A-Z.]+)[\'"]\))?', 
                 lambda m: f"get_list({m.group(1)}, '{m.group(2).lower()}'" + (f", '{m.group(3).lower()}'" if m.group(3) else "") + ")", 
                 new_loop)
                 
    # 4. obj.find_all('FIELD1') -> get_list(obj, 'field1')
    new_loop = re.sub(r'([a-zA-Z0-9_]+)\.find_all\([\'"]([A-Z.]+)[\'"]\)', 
                 lambda m: f"get_list({m.group(1)}, '{m.group(2).lower()}')", 
                 new_loop)
                 
    # Handle str(voucher) conversion since voucher is now dict
    new_loop = new_loop.replace('str(v)', 'json.dumps(v)')
    new_loop = new_loop.replace('str(voucher)', 'json.dumps(v)')

    # Find the collection variable 'xxx_data.append' mapping
    data_var_match = re.search(r'([a-zA-Z_]+)\.append\(', new_loop)
    if data_var_match:
        data_var = data_var_match.group(1)
        new_loop = new_loop.replace(f'{data_var}.append', 'parsed_data.append')
    
    footer = f"""
    return parsed_data
"""

    final_fn = header + new_loop + footer
    
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write("\n\n" + final_fn + "\n")
    print(f" Injected parse_tally_json into {filepath}")

for f in FILES_TO_PROCESS:
    process_file(f)
    
