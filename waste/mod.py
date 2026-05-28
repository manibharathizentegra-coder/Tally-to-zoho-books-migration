import re
import os

# --- 1. THE INJECTIONS FOR BACKEND FILES ---

def append_to_file(filepath, content):
    # Try alternating path if not found
    if not os.path.exists(filepath):
        # Tidy up path names
        if 'invoices' in filepath: filepath = filepath.replace('invoices', 'invoice')
        if 'journels' in filepath: filepath = filepath.replace('journels', 'journel')
    
    if not os.path.exists(filepath):
        print(f"Skipping {filepath}, file not found.")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        file_content = f.read()

    if 'def parse_tally_json(' in file_content:
        print(f" {filepath} already has parse_tally_json. Skipping injection.")
        return

    with open(filepath, 'a', encoding='utf-8') as f:
        f.write("\n" + content + "\n")
    print(f" Injected parse_tally_json into {filepath}")


PARSE_JSON_INVOICES = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    invoice_data = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        v_date = str(v.get('date', '')).strip()
        v_no = str(v.get('vouchernumber', '')).strip()
        customer_name = str(v.get('partyname', '')).strip()
        narration = str(v.get('narration', '')).strip()
        po_number = str(v.get('basicpurchaseorderno', '')).strip()
        
        buyer_address = []
        basicbuyer = v.get('basicbuyeraddress.list', v.get('basicbuyeraddress', []))
        if not isinstance(basicbuyer, list): basicbuyer = [basicbuyer]
        for addr in basicbuyer:
            if isinstance(addr, dict) and 'basicbuyeraddress' in addr:
                buyer_address.append(str(addr['basicbuyeraddress']).strip())
            elif isinstance(addr, str):
                buyer_address.append(addr.strip())
                
        irn = str(v.get('irn', '')).strip()
        irn_ack_no = str(v.get('irnackno', '')).strip()
        irn_ack_date = str(v.get('irnackdate', '')).strip()
        
        payment_terms = str(v.get('basicduedateofpymt', '')).strip()
        if not payment_terms:
            bill_allocs = v.get('billallocations.list', v.get('billallocations', []))
            if not isinstance(bill_allocs, list): bill_allocs = [bill_allocs]
            for ba in bill_allocs:
                if isinstance(ba, dict) and ba.get('billcreditperiod'):
                    payment_terms = str(ba['billcreditperiod']).strip()
                    break

        sales_ledger = ""
        inventory_entries = v.get('inventoryentries.list', v.get('inventoryentries', v.get('allinventoryentries.list', v.get('allinventoryentries', []))))
        if not isinstance(inventory_entries, list): inventory_entries = [inventory_entries]
        for item in inventory_entries:
            if isinstance(item, dict) and item.get('ledgername'):
                sales_ledger = str(item['ledgername']).strip()
                break

        ledger_entries = v.get('ledgerentries.list', v.get('ledgerentries', v.get('allledgerentries.list', v.get('allledgerentries', []))))
        if not isinstance(ledger_entries, list): ledger_entries = [ledger_entries]
        
        if not sales_ledger:
            max_neg = 0
            for entry in ledger_entries:
                if not isinstance(entry, dict): continue
                lname = str(entry.get('ledgername', '')).strip()
                amt_str = str(entry.get('amount', '0'))
                nums = re.findall(r'[-\d.]+', amt_str)
                amt = float(nums[-1]) if nums else 0.0
                lname_lower = lname.lower()
                if lname == customer_name or 'cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower or 'rounding' in lname_lower:
                    continue
                if amt < max_neg:
                    max_neg = amt; sales_ledger = lname

        line_items = []; subtotal = 0
        for item in inventory_entries:
            if not isinstance(item, dict): continue
            item_name = str(item.get('stockitemname', '')).strip()
            quantity = str(item.get('actualqty', item.get('billedqty', '0'))).strip()
            
            rate_str = str(item.get('rate', '0')).split('/')[0].strip()
            nums = re.findall(r'[-\d.]+', rate_str)
            rate = float(nums[-1]) if nums else 0.0
            
            discount = str(item.get('discount', '0')).strip()
            
            amt_str = str(item.get('amount', '0')).strip()
            nums = re.findall(r'[-\d.]+', amt_str)
            amount = float(nums[-1]) if nums else 0.0
            
            cat_alloc = item.get('categoryallocations.list', item.get('categoryallocations', {}))
            if isinstance(cat_alloc, list) and len(cat_alloc) > 0: cat_alloc = cat_alloc[0]
            category = str(cat_alloc.get('category', '')).strip() if isinstance(cat_alloc, dict) else ""
            
            cost_centre = ""
            if isinstance(cat_alloc, dict):
                cc_alloc = cat_alloc.get('costcentreallocations.list', cat_alloc.get('costcentreallocations', {}))
                if isinstance(cc_alloc, list) and len(cc_alloc) > 0: cc_alloc = cc_alloc[0]
                cost_centre = str(cc_alloc.get('name', '')).strip() if isinstance(cc_alloc, dict) else ""

            line_items.append({"item_name": item_name, "quantity": quantity, "rate": rate, "discount": discount, "amount": abs(amount), "category": category, "cost_centre": cost_centre})
            subtotal += abs(amount)

        taxes = []; tax_total = 0; rounding_off = 0.0
        for entry in ledger_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            
            lname_lower = lname.lower()
            if ('cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower) and 'output' in lname_lower:
                tax_rate = ""
                if '%' in lname: tax_rate = lname.split('%')[0].split()[-1]
                tax_type = "CGST" if 'cgst' in lname_lower else ("SGST" if 'sgst' in lname_lower else "IGST")
                taxes.append({"tax_name": lname, "tax_type": tax_type, "tax_rate": tax_rate, "tax_amount": abs(amt)})
                tax_total += abs(amt)
            elif 'rounding' in lname_lower:
                rounding_off = amt
                
        total_amount = subtotal + tax_total + rounding_off
        invoice_data.append({"date": v_date, "invoice_number": v_no, "customer_name": customer_name, "po_number": po_number, "buyer_address": buyer_address, "payment_terms": payment_terms, "irn": irn, "irn_ack_no": irn_ack_no, "irn_ack_date": irn_ack_date, "sales_ledger": sales_ledger, "narration": narration, "line_items": line_items, "taxes": taxes, "rounding_off": rounding_off, "subtotal": round(subtotal, 2), "tax_total": round(tax_total, 2), "total_amount": round(total_amount, 2)})
    return invoice_data
"""

PARSE_JSON_BILLS = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    bill_data = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        v_date = str(v.get('date', '')).strip()
        v_no = str(v.get('vouchernumber', '')).strip()
        vendor_name = str(v.get('partyname', '')).strip()
        narration = str(v.get('narration', '')).strip()
        po_number = str(v.get('basicpurchaseorderno', '')).strip()
        reference_number = str(v.get('reference', '')).strip()
        
        vendor_address = []
        buyer_addr = v.get('basicbuyeraddress.list', v.get('basicbuyeraddress', []))
        if not isinstance(buyer_addr, list): buyer_addr = [buyer_addr]
        for addr in buyer_addr:
            if isinstance(addr, dict) and 'basicbuyeraddress' in addr:
                vendor_address.append(str(addr['basicbuyeraddress']).strip())
            elif isinstance(addr, str): vendor_address.append(addr.strip())
            
        payment_terms = str(v.get('basicduedateofpymt', '')).strip()
        if not payment_terms:
            bill_allocs = v.get('billallocations.list', v.get('billallocations', []))
            if not isinstance(bill_allocs, list): bill_allocs = [bill_allocs]
            for ba in bill_allocs:
                if isinstance(ba, dict) and ba.get('billcreditperiod'):
                    payment_terms = str(ba['billcreditperiod']).strip()
                    break

        purchase_ledger = ""
        inventory_entries = v.get('inventoryentries.list', v.get('inventoryentries', v.get('allinventoryentries.list', v.get('allinventoryentries', []))))
        if not isinstance(inventory_entries, list): inventory_entries = [inventory_entries]
        for item in inventory_entries:
            if isinstance(item, dict) and item.get('ledgername'):
                purchase_ledger = str(item['ledgername']).strip()
                break

        ledger_entries = v.get('ledgerentries.list', v.get('ledgerentries', v.get('allledgerentries.list', v.get('allledgerentries', []))))
        if not isinstance(ledger_entries, list): ledger_entries = [ledger_entries]
        
        if not purchase_ledger:
            max_neg = 0
            for entry in ledger_entries:
                if not isinstance(entry, dict): continue
                lname = str(entry.get('ledgername', '')).strip()
                amt_str = str(entry.get('amount', '0'))
                nums = re.findall(r'[-\d.]+', amt_str)
                amt = float(nums[-1]) if nums else 0.0
                lname_lower = lname.lower()
                if lname == vendor_name or 'cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower or 'rounding' in lname_lower: continue
                if amt < max_neg: max_neg = amt; purchase_ledger = lname

        line_items = []
        for item in inventory_entries:
            if not isinstance(item, dict): continue
            item_name = str(item.get('stockitemname', '')).strip()
            quantity = str(item.get('actualqty', item.get('billedqty', '0'))).strip()
            rate_str = str(item.get('rate', '0')).split('/')[0].strip()
            nums = re.findall(r'[-\d.]+', rate_str)
            rate = float(nums[-1]) if nums else 0.0
            discount = str(item.get('discount', '0')).strip()
            amt_str = str(item.get('amount', '0')).strip()
            nums = re.findall(r'[-\d.]+', amt_str)
            amount = float(nums[-1]) if nums else 0.0
            
            cat_alloc = item.get('categoryallocations.list', item.get('categoryallocations', {}))
            if isinstance(cat_alloc, list) and len(cat_alloc) > 0: cat_alloc = cat_alloc[0]
            category = str(cat_alloc.get('category', '')).strip() if isinstance(cat_alloc, dict) else ""
            
            cost_centre = ""
            if isinstance(cat_alloc, dict):
                cc_alloc = cat_alloc.get('costcentreallocations.list', cat_alloc.get('costcentreallocations', {}))
                if isinstance(cc_alloc, list) and len(cc_alloc) > 0: cc_alloc = cc_alloc[0]
                cost_centre = str(cc_alloc.get('name', '')).strip() if isinstance(cc_alloc, dict) else ""

            line_items.append({"item_name": item_name, "quantity": quantity, "rate": rate, "discount": discount, "amount": abs(amount), "category": category, "cost_centre": cost_centre})

        taxes = []; rounding_off = 0.0
        for entry in ledger_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            lname_lower = lname.lower()
            if ('cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower) and 'input' in lname_lower:
                tax_rate = ""
                if '%' in lname: tax_rate = lname.split('%')[0].split()[-1]
                tax_type = "CGST" if 'cgst' in lname_lower else ("SGST" if 'sgst' in lname_lower else "IGST")
                taxes.append({"tax_name": lname, "tax_type": tax_type, "tax_rate": tax_rate, "tax_amount": abs(amt)})
            elif 'rounding' in lname_lower: rounding_off = amt
                
        bill_data.append({"date": v_date, "bill_number": v_no, "vendor_name": vendor_name, "po_number": po_number, "reference_number": reference_number, "vendor_address": vendor_address, "payment_terms": payment_terms, "purchase_ledger": purchase_ledger, "line_items": line_items, "taxes": taxes, "rounding_off": rounding_off, "narration": narration})
    return bill_data
"""

PARSE_JSON_SALES_ORDERS = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    so_data = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        v_date = str(v.get('date', '')).strip()
        v_no = str(v.get('vouchernumber', '')).strip()
        customer_name = str(v.get('partyname', '')).strip()
        narration = str(v.get('narration', '')).strip()
        reference_number = str(v.get('reference', '')).strip()
        
        customer_address = []
        buyer_addr = v.get('basicbuyeraddress.list', v.get('basicbuyeraddress', []))
        if not isinstance(buyer_addr, list): buyer_addr = [buyer_addr]
        for addr in buyer_addr:
            if isinstance(addr, dict) and 'basicbuyeraddress' in addr:
                customer_address.append(str(addr['basicbuyeraddress']).strip())
            elif isinstance(addr, str): customer_address.append(addr.strip())
            
        payment_terms = str(v.get('basicduedateofpymt', '')).strip()
        order_status = str(v.get('orderstatus', 'Pending')).strip()
        sales_ledger = ""
        
        inventory_entries = v.get('inventoryentries.list', v.get('inventoryentries', v.get('allinventoryentries.list', v.get('allinventoryentries', []))))
        if not isinstance(inventory_entries, list): inventory_entries = [inventory_entries]
        for item in inventory_entries:
            if isinstance(item, dict) and item.get('ledgername'):
                sales_ledger = str(item['ledgername']).strip()
                break

        ledger_entries = v.get('ledgerentries.list', v.get('ledgerentries', v.get('allledgerentries.list', v.get('allledgerentries', []))))
        if not isinstance(ledger_entries, list): ledger_entries = [ledger_entries]
        
        if not sales_ledger:
            max_neg = 0
            for entry in ledger_entries:
                if not isinstance(entry, dict): continue
                lname = str(entry.get('ledgername', '')).strip()
                amt_str = str(entry.get('amount', '0'))
                nums = re.findall(r'[-\d.]+', amt_str)
                amt = float(nums[-1]) if nums else 0.0
                lname_lower = lname.lower()
                if lname == customer_name or 'cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower or 'rounding' in lname_lower: continue
                if amt < max_neg: max_neg = amt; sales_ledger = lname

        line_items = []
        for item in inventory_entries:
            if not isinstance(item, dict): continue
            item_name = str(item.get('stockitemname', '')).strip()
            quantity = str(item.get('actualqty', item.get('billedqty', '0'))).strip()
            rate_str = str(item.get('rate', '0')).split('/')[0].strip()
            nums = re.findall(r'[-\d.]+', rate_str)
            rate = float(nums[-1]) if nums else 0.0
            discount = str(item.get('discount', '0')).strip()
            amt_str = str(item.get('amount', '0')).strip()
            nums = re.findall(r'[-\d.]+', amt_str)
            amount = float(nums[-1]) if nums else 0.0
            line_items.append({"item_name": item_name, "quantity": quantity, "rate": rate, "discount": discount, "amount": abs(amount)})

        taxes = []; rounding_off = 0.0
        for entry in ledger_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            lname_lower = lname.lower()
            if ('cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower) and 'output' in lname_lower:
                tax_rate = ""
                if '%' in lname: tax_rate = lname.split('%')[0].split()[-1]
                tax_type = "CGST" if 'cgst' in lname_lower else ("SGST" if 'sgst' in lname_lower else "IGST")
                taxes.append({"tax_name": lname, "tax_type": tax_type, "rate": tax_rate, "amount": abs(amt)})
            elif 'rounding' in lname_lower: rounding_off = float(nums[-1]) if nums else 0.0
                
        so_data.append({"sales_order_number": v_no, "date": v_date, "customer_name": customer_name, "reference_number": reference_number, "customer_address": customer_address, "payment_terms": payment_terms, "order_status": order_status, "sales_ledger": sales_ledger, "line_items": line_items, "taxes": taxes, "rounding_off": rounding_off, "narration": narration})
    return so_data
"""

PARSE_JSON_PURCHASE_ORDERS = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    po_data = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        v_date = str(v.get('date', '')).strip()
        v_no = str(v.get('vouchernumber', '')).strip()
        vendor_name = str(v.get('partyname', '')).strip()
        narration = str(v.get('narration', '')).strip()
        reference_number = str(v.get('reference', '')).strip()
        
        vendor_address = []
        buyer_addr = v.get('basicbuyeraddress.list', v.get('basicbuyeraddress', []))
        if not isinstance(buyer_addr, list): buyer_addr = [buyer_addr]
        for addr in buyer_addr:
            if isinstance(addr, dict) and 'basicbuyeraddress' in addr:
                vendor_address.append(str(addr['basicbuyeraddress']).strip())
            elif isinstance(addr, str): vendor_address.append(addr.strip())
            
        payment_terms = str(v.get('basicduedateofpymt', '')).strip()
        order_status = str(v.get('orderstatus', 'Pending')).strip()
        purchase_ledger = ""
        
        inventory_entries = v.get('inventoryentries.list', v.get('inventoryentries', v.get('allinventoryentries.list', v.get('allinventoryentries', []))))
        if not isinstance(inventory_entries, list): inventory_entries = [inventory_entries]
        for item in inventory_entries:
            if isinstance(item, dict) and item.get('ledgername'):
                purchase_ledger = str(item['ledgername']).strip()
                break

        ledger_entries = v.get('ledgerentries.list', v.get('ledgerentries', v.get('allledgerentries.list', v.get('allledgerentries', []))))
        if not isinstance(ledger_entries, list): ledger_entries = [ledger_entries]
        
        if not purchase_ledger:
            max_neg = 0
            for entry in ledger_entries:
                if not isinstance(entry, dict): continue
                lname = str(entry.get('ledgername', '')).strip()
                amt_str = str(entry.get('amount', '0'))
                nums = re.findall(r'[-\d.]+', amt_str)
                amt = float(nums[-1]) if nums else 0.0
                lname_lower = lname.lower()
                if lname == vendor_name or 'cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower or 'rounding' in lname_lower: continue
                if amt < max_neg: max_neg = amt; purchase_ledger = lname

        line_items = []
        for item in inventory_entries:
            if not isinstance(item, dict): continue
            item_name = str(item.get('stockitemname', '')).strip()
            quantity = str(item.get('actualqty', item.get('billedqty', '0'))).strip()
            rate_str = str(item.get('rate', '0')).split('/')[0].strip()
            nums = re.findall(r'[-\d.]+', rate_str)
            rate = float(nums[-1]) if nums else 0.0
            discount = str(item.get('discount', '0')).strip()
            amt_str = str(item.get('amount', '0')).strip()
            nums = re.findall(r'[-\d.]+', amt_str)
            amount = float(nums[-1]) if nums else 0.0
            line_items.append({"item_name": item_name, "quantity": quantity, "rate": rate, "discount": discount, "amount": abs(amount)})

        taxes = []; rounding_off = 0.0
        for entry in ledger_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            lname_lower = lname.lower()
            if ('cgst' in lname_lower or 'sgst' in lname_lower or 'igst' in lname_lower) and 'input' in lname_lower:
                tax_rate = ""
                if '%' in lname: tax_rate = lname.split('%')[0].split()[-1]
                tax_type = "CGST" if 'cgst' in lname_lower else ("SGST" if 'sgst' in lname_lower else "IGST")
                taxes.append({"tax_name": lname, "tax_type": tax_type, "rate": tax_rate, "amount": abs(amt)})
            elif 'rounding' in lname_lower: rounding_off = float(nums[-1]) if nums else 0.0
                
        po_data.append({"purchase_order_number": v_no, "date": v_date, "vendor_name": vendor_name, "reference_number": reference_number, "vendor_address": vendor_address, "payment_terms": payment_terms, "order_status": order_status, "purchase_ledger": purchase_ledger, "line_items": line_items, "taxes": taxes, "rounding_off": rounding_off, "narration": narration})
    return po_data
"""

PARSE_JSON_JOURNELS = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    journels = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        j_date = str(v.get('date', '')).strip()
        j_no = str(v.get('vouchernumber', '')).strip()
        voucher_type = str(v.get('vouchertypename', 'Journal')).strip()
        tally_guid = str(v.get('guid', '')).strip()
        narration = str(v.get('narration', '')).strip()

        ledger_entries = []
        cost_centers_dict = {}

        all_entries = v.get('allledgerentries.list', v.get('allledgerentries', []))
        if not isinstance(all_entries, list): all_entries = [all_entries]
        
        for entry in all_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            
            is_deemed_positive = entry.get('isdeemedpositive', False)
            if str(is_deemed_positive).lower() == 'true' or is_deemed_positive is True or 'yes' in str(is_deemed_positive).lower():
                debit = abs(amt); credit = 0.0
            else: debit = 0.0; credit = abs(amt)
            
            ledger_entries.append({"ledger_name": lname, "debit": debit, "credit": credit})
            
            cats = entry.get('categoryallocations.list', entry.get('categoryallocations', []))
            if not isinstance(cats, list): cats = [cats]
            for ca in cats:
                if not isinstance(ca, dict): continue
                cat_name = str(ca.get('category', '')).strip()
                ccs = ca.get('costcentreallocations.list', ca.get('costcentreallocations', []))
                if not isinstance(ccs, list): ccs = [ccs]
                for cc in ccs:
                    if not isinstance(cc, dict): continue
                    cc_name = str(cc.get('name', '')).strip()
                    cc_amt_str = str(cc.get('amount', '0'))
                    ccnums = re.findall(r'[-\d.]+', cc_amt_str)
                    cc_amt = float(ccnums[-1]) if ccnums else 0.0
                    full = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                    if full: cost_centers_dict[full] = abs(cc_amt)

        cost_center_allocations = [{"category": k, "amount": v} for k, v in cost_centers_dict.items()]
        journels.append({"date": j_date, "journal_number": j_no, "voucher_type": voucher_type, "tally_guid": tally_guid, "narration": narration, "ledger_entries": ledger_entries, "cost_center_allocations": cost_center_allocations})
    return journels
"""

PARSE_JSON_RECEIPTS = """
def parse_tally_json(json_path):
    import json, re
    try:
        with open(json_path, 'r', encoding='utf-16') as f: data = json.load(f)
    except:
        with open(json_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if isinstance(data, list) and len(data) > 0 and 'date' in data[0]: return data
    vouchers = data.get('tallymessage', [])
    if isinstance(vouchers, dict): vouchers = [vouchers]
    
    receipts = []
    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue
        
        r_date = str(v.get('date', '')).strip()
        r_no = str(v.get('vouchernumber', '')).strip()
        voucher_type = str(v.get('vouchertypename', 'Receipt')).strip()
        tally_guid = str(v.get('guid', '')).strip()
        customer_name = str(v.get('partyname', '')).strip()
        narration = str(v.get('narration', '')).strip()
        ref_no = str(v.get('reference', v.get('chequenumber', ''))).strip()
        
        ledger_entries = []; cost_centers_dict = {}
        bank_account = ""; payment_mode = ""; account_current_balance = 0.0
        rounding_amount = 0.0; rounding_ledger = ""
        customer_ledger_amount = 0.0
        against_reference = ""
        
        all_entries = v.get('allledgerentries.list', v.get('allledgerentries', []))
        if not isinstance(all_entries, list): all_entries = [all_entries]
        
        for entry in all_entries:
            if not isinstance(entry, dict): continue
            lname = str(entry.get('ledgername', '')).strip()
            amt_str = str(entry.get('amount', '0'))
            nums = re.findall(r'[-\d.]+', amt_str)
            amt = float(nums[-1]) if nums else 0.0
            
            ledger_entries.append({"ledger_name": lname, "amount": amt, "current_balance": 0.0})
            
            lname_lower = lname.lower()
            if amt > 0 and not any(k in lname_lower for k in ['cash', 'bank', 'cgst', 'sgst', 'igst', 'rounding']):
                if not customer_name: customer_name = lname
                customer_ledger_amount = abs(amt)
            if amt < 0 and 'rounding' not in lname_lower:
                if 'cash' in lname_lower: payment_mode = "Cash"; bank_account = lname
                elif 'bank' in lname_lower: payment_mode = "Bank Transfer"; bank_account = lname
                elif not payment_mode: payment_mode = "Other"; bank_account = lname
            if 'rounding' in lname_lower:
                rounding_amount = amt; rounding_ledger = lname
                
            cats = entry.get('categoryallocations.list', entry.get('categoryallocations', []))
            if not isinstance(cats, list): cats = [cats]
            for ca in cats:
                if not isinstance(ca, dict): continue
                cat_name = str(ca.get('category', '')).strip()
                ccs = ca.get('costcentreallocations.list', ca.get('costcentreallocations', []))
                if not isinstance(ccs, list): ccs = [ccs]
                for cc in ccs:
                    if not isinstance(cc, dict): continue
                    cc_name = str(cc.get('name', '')).strip()
                    cc_amt_str = str(cc.get('amount', '0'))
                    ccnums = re.findall(r'[-\d.]+', cc_amt_str)
                    cc_amt = float(ccnums[-1]) if ccnums else 0.0
                    full = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                    if full: cost_centers_dict[full] = abs(cc_amt)
                    
        invoice_allocations = []
        for entry in all_entries:
            bills = entry.get('billallocations.list', entry.get('billallocations', []))
            if not isinstance(bills, list): bills = [bills]
            for ba in bills:
                if not isinstance(ba, dict): continue
                bname = str(ba.get('name', '')).strip()
                bamt_str = str(ba.get('amount', '0'))
                bnums = re.findall(r'[-\d.]+', bamt_str)
                bamt = float(bnums[-1]) if bnums else 0.0
                btype = str(ba.get('billtype', 'Agst Ref')).strip()
                if not bname and btype == "On Account": bname = "On Account"
                if bname:
                    if not against_reference: against_reference = bname
                    invoice_allocations.append({"invoice_number": bname, "bill_type": btype, "amount": abs(bamt) if bamt != 0 else customer_ledger_amount})

        cost_center_allocations = [{"category": k, "amount": v} for k, v in cost_centers_dict.items()]
        receipts.append({"date": r_date, "receipt_number": r_no, "voucher_type": voucher_type, "customer_name": customer_name, "customer_ledger_amount": customer_ledger_amount, "payment_mode": payment_mode, "bank_account": bank_account, "account_current_balance": account_current_balance, "amount": customer_ledger_amount, "reference_number": ref_no, "against_reference": against_reference, "narration": narration, "invoice_allocations": invoice_allocations, "ledger_entries": ledger_entries, "cost_center_allocations": cost_center_allocations, "rounding_amount": rounding_amount, "rounding_ledger": rounding_ledger, "tally_guid": tally_guid})
    return receipts
"""

append_to_file("invoices/invoice_backend.py", PARSE_JSON_INVOICES)
append_to_file("bills/bills_backend.py", PARSE_JSON_BILLS)
append_to_file("receipts/receipts_backend.py", PARSE_JSON_RECEIPTS)
append_to_file("sales_order/sale_backend.py", PARSE_JSON_SALES_ORDERS)
append_to_file("purchase_order/purchase_order_backend.py", PARSE_JSON_PURCHASE_ORDERS)
append_to_file("journel/journel_backend.py", PARSE_JSON_JOURNELS)


# --- 2. THE INJECTIONS FOR APP.PY ---

APP_PY_INJECTIONS = [
    # INVOICE
    ("def get_invoices():", """
@app.route('/api/invoice/upload', methods=['POST'])
def upload_tally_invoice_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = invoice_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json
            from datetime import datetime
            now = datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "invoice_number": d.get("invoice_number", ""), "date": d.get("date", ""),
                    "customer_name": d.get("customer_name", ""), "po_number": d.get("po_number", ""),
                    "buyer_address": json.dumps(d.get("buyer_address", [])), "payment_terms": d.get("payment_terms", ""),
                    "sales_ledger": d.get("sales_ledger", ""), "narration": d.get("narration", ""),
                    "irn": d.get("irn", ""), "irn_ack_no": d.get("irn_ack_no", ""), "irn_ack_date": d.get("irn_ack_date", ""),
                    "line_items": json.dumps(d.get("line_items", [])), "taxes": json.dumps(d.get("taxes", [])),
                    "rounding_off": d.get("rounding_off", 0), "subtotal": d.get("subtotal", 0),
                    "tax_total": d.get("tax_total", 0), "total_amount": d.get("total_amount", 0),
                    "from_date": "", "to_date": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_invoices(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} invoices", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
"""),
    
    # BILLS
    ("def get_bills():", """
@app.route('/api/bills/upload', methods=['POST'])
def upload_tally_bills_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = bills_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json, datetime
            now = datetime.datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "bill_number": d.get("bill_number", ""), "date": d.get("date", ""),
                    "vendor_name": d.get("vendor_name", ""), "po_number": d.get("po_number", ""),
                    "reference_number": d.get("reference_number", ""), "vendor_address": json.dumps(d.get("vendor_address", [])),
                    "payment_terms": d.get("payment_terms", ""), "purchase_ledger": d.get("purchase_ledger", ""),
                    "narration": d.get("narration", ""), "line_items": json.dumps(d.get("line_items", [])),
                    "taxes": json.dumps(d.get("taxes", [])), "rounding_off": d.get("rounding_off", 0),
                    "from_date": "", "to_date": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_bills(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} bills", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
"""),

    # RECEIPTS
    ("def get_receipts():", """
@app.route('/api/receipts/upload', methods=['POST'])
def upload_tally_receipts_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = receipts_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json, datetime
            now = datetime.datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "receipt_number": d.get("receipt_number", ""), "voucher_type": d.get("voucher_type", ""),
                    "date": d.get("date", ""), "customer_name": d.get("customer_name", ""),
                    "customer_ledger_amount": d.get("customer_ledger_amount", 0), "payment_mode": d.get("payment_mode", ""),
                    "bank_account": d.get("bank_account", ""), "account_current_balance": d.get("account_current_balance", 0),
                    "amount": d.get("amount", 0), "reference_number": d.get("reference_number", ""),
                    "against_reference": d.get("against_reference", ""), "narration": d.get("narration", ""),
                    "invoice_allocations": json.dumps(d.get("invoice_allocations", [])), "ledger_entries": json.dumps(d.get("ledger_entries", [])),
                    "cost_center_allocations": json.dumps(d.get("cost_center_allocations", [])), "rounding_amount": d.get("rounding_amount", 0),
                    "rounding_ledger": d.get("rounding_ledger", ""), "tally_guid": d.get("tally_guid", ""),
                    "company_name": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_receipts(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} receipts", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
"""),

    # SALES ORDER
    ("def get_sales_order():", """
@app.route('/api/sales_order/upload', methods=['POST'])
def upload_tally_sales_order_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = sale_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json, datetime
            now = datetime.datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "sales_order_number": d.get("sales_order_number", ""), "date": d.get("date", ""),
                    "customer_name": d.get("customer_name", ""), "reference_number": d.get("reference_number", ""),
                    "customer_address": json.dumps(d.get("customer_address", [])), "payment_terms": d.get("payment_terms", ""),
                    "order_status": d.get("order_status", ""), "sales_ledger": d.get("sales_ledger", ""),
                    "line_items": json.dumps(d.get("line_items", [])), "taxes": json.dumps(d.get("taxes", [])),
                    "rounding_off": d.get("rounding_off", 0), "narration": d.get("narration", ""),
                    "from_date": "", "to_date": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_sales_orders(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} sales orders", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
"""),

    # PURCHASE ORDER
    ("def get_purchase_orders():", """
@app.route('/api/purchase_order/upload', methods=['POST'])
def upload_tally_purchase_order_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = purchase_order_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json, datetime
            now = datetime.datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "purchase_order_number": d.get("purchase_order_number", ""), "date": d.get("date", ""),
                    "vendor_name": d.get("vendor_name", ""), "reference_number": d.get("reference_number", ""),
                    "vendor_address": json.dumps(d.get("vendor_address", [])), "payment_terms": d.get("payment_terms", ""),
                    "order_status": d.get("order_status", ""), "purchase_ledger": d.get("purchase_ledger", ""),
                    "line_items": json.dumps(d.get("line_items", [])), "taxes": json.dumps(d.get("taxes", [])),
                    "rounding_off": d.get("rounding_off", 0), "narration": d.get("narration", ""),
                    "from_date": "", "to_date": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_purchase_orders(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} purchase orders", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
"""),

    # JOURNELS
    ("def get_journals():", """
@app.route('/api/journel/upload', methods=['POST'])
def upload_tally_journels_json():
    if 'file' not in request.files: return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({"error": "No file selected"}), 400
    if not file.filename.endswith('.json'): return jsonify({"error": "Only JSON files allowed"}), 400
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    file.save(filepath)
    try:
        data = journel_backend.parse_tally_json(filepath)
        if database_manager and data:
            import json, datetime
            now = datetime.datetime.now().isoformat()
            db_data = []
            for d in data:
                db_data.append({
                    "journal_number": d.get("journal_number", ""), "date": d.get("date", ""),
                    "voucher_type": d.get("voucher_type", ""), "narration": d.get("narration", ""),
                    "ledger_entries": json.dumps(d.get("ledger_entries", [])), "cost_center_allocations": json.dumps(d.get("cost_center_allocations", [])),
                    "tally_guid": d.get("tally_guid", ""), "company_name": "", "created_at": now, "updated_at": now
                })
            database_manager.bulk_save_journals(db_data)
        return jsonify({"message": f"Successfully parsed {len(data)} journals", "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
""")
]

def apply_app_py_injections():
    app_py_path = "app.py"
    if not os.path.exists(app_py_path):
        print("app.py not found.")
        return
        
    with open(app_py_path, 'r', encoding='utf-8') as f:
        app_content = f.read()
        
    for target, injection in APP_PY_INJECTIONS:
        if "def upload_tally" in app_content and injection.split('def ')[1].split('(')[0] in app_content:
            print(f"Skipping app.py injection for {target}, already exists.")
            continue
            
        app_content = app_content.replace(target, injection + "\n@" + target.split('def ')[0] + "app.route" + target.split('def')[0] + "\ndef " + target.split('def ')[1])
        # A simpler way, just find the route function and add it above
        if target in app_content:
            app_content = app_content.replace(target, injection + "\n" + target)

    with open(app_py_path, 'w', encoding='utf-8') as f:
        f.write(app_content)
    print(" Injected app.py routes")

apply_app_py_injections()

# --- 3. THE INJECTIONS FOR HTML FILES ---

HTML_FILES = {
    "invoices.html": "allInvoices",
    "bills.html": "allBills",
    "receipts.html": "allReceipts",
    "sales_orders.html": "allSalesOrders",
    "purchase_orders.html": "allPurchaseOrders",
    "journals.html": "allJournals"
}

def apply_html_injections():
    for html_file, varName in HTML_FILES.items():
        filepath = os.path.join("templates", html_file)
        if not os.path.exists(filepath): continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Insert UI elements next to "Export to JSON" or "Fetch from Tally"
        if 'id="jsonUpload"' not in content:
            # We look for the export button or fetch button
            upload_html = f"""
            <input type="file" id="jsonUpload" accept=".json" style="display: none;" onchange="uploadJson(event)">
            <button class="btn btn-outline-primary" onclick="document.getElementById('jsonUpload').click()">
                <i class="bi bi-upload me-2"></i>Upload JSON
            </button>
            """
            content = content.replace('<button class="btn btn-outline-success" onclick="exportToJson()">', 
                                     upload_html + '\n<button class="btn btn-outline-success" onclick="exportToJson()">')
            content = content.replace('<button class="btn btn-primary ms-2" onclick="exportToJson()">', 
                                     upload_html + '\n<button class="btn btn-primary ms-2" onclick="exportToJson()">')
            pass

        # Insert JS function
        if 'async function uploadJson(event)' not in content:
            # The API endpoint path maps to HTML file names vaguely, let's map it.
            # invoices.html -> /api/invoice/upload
            api_endpoint = "/api/invoice/upload"
            if "bills" in html_file: api_endpoint = "/api/bills/upload"
            elif "receipts" in html_file: api_endpoint = "/api/receipts/upload"
            elif "sales_orders" in html_file: api_endpoint = "/api/sales_order/upload"
            elif "purchase_orders" in html_file: api_endpoint = "/api/purchase_order/upload"
            elif "journals" in html_file: api_endpoint = "/api/journel/upload"
            
            # The render method names
            render_method = "renderInvoices"
            if "bills" in html_file: render_method = "renderBills"
            elif "receipts" in html_file: render_method = "renderReceipts"
            elif "sales_orders" in html_file: render_method = "renderSalesOrders"
            elif "purchase_orders" in html_file: render_method = "renderPurchaseOrders"
            elif "journals" in html_file: render_method = "renderJournals"
            
            js_fn = f"""
        // Upload JSON Function
        async function uploadJson(event) {{
            const file = event.target.files[0];
            if (!file) return;

            const formData = new FormData();
            formData.append('file', file);

            const uploadBtn = event.target.nextElementSibling || document.querySelector('button[onclick="document.getElementById(\\'jsonUpload\\').click()"]');
            const originalText = uploadBtn.innerHTML;
            uploadBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Uploading...';
            uploadBtn.disabled = true;

            try {{
                const response = await fetch('{api_endpoint}', {{
                    method: 'POST',
                    body: formData
                }});
                const result = await response.json();

                if (response.ok) {{
                    {varName} = result.data || result.invoices || result.bills || result.receipts || result.sales_orders || result.purchase_orders || result.journals || [];
                    localStorage.setItem('{varName}', JSON.stringify({varName}));
                    alert(result.message || 'JSON uploaded successfully!');
                    {render_method}();
                }} else {{
                    alert('Error uploading JSON: ' + (result.error || 'Unknown error'));
                }}
            }} catch (error) {{
                console.error('Error:', error);
                alert('Failed to upload JSON file. See console for details.');
            }} finally {{
                uploadBtn.innerHTML = originalText;
                uploadBtn.disabled = false;
                event.target.value = ''; // Reset file input
            }}
        }}
        """
            # Insert before </script>
            pos = content.rfind('</script>')
            if pos != -1:
                content = content[:pos] + js_fn + content[pos:]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f" HTML Injection completed for {html_file}")

apply_html_injections()
