import requests
from bs4 import BeautifulSoup

TALLY_URL = "http://localhost:9000"

def fetch_all_purchase_orders(num_orders=5):
    """Fetch Purchase Orders using XML approach similar to sales orders"""
    
    # XML request to get all Purchase Order vouchers
    # Using Voucher Register report which is reliable
    xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Purchase Order</VOUCHERTYPENAME>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=30)
        
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            return
        
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        # Find all vouchers
        all_vouchers = soup.find_all('VOUCHER')
        
        if not all_vouchers:
            print("No Purchase Orders found!")
            print("\nChecking if Tally is running and accessible...")
            return
        
        # Limit to the requested number
        vouchers = all_vouchers[:num_orders]
        
        print(f"\nShowing {len(vouchers)} of {len(all_vouchers)} total Purchase Order(s)")
        print("=" * 150)
        
        for idx, v in enumerate(vouchers, 1):
            # Extract header information
            date = v.find('DATE')
            v_no = v.find('VOUCHERNUMBER')
            party = v.find('PARTYNAME')
            ref = v.find('REFERENCE')
            
            date_str = date.text if date else ""
            v_no_str = v_no.text if v_no else ""
            party_str = party.text if party else ""
            ref_str = ref.text if ref else ""
            
            # Get Payment Terms
            payment_terms = ""
            bill_alloc = v.find('BILLALLOCATIONS.LIST')
            if bill_alloc:
                bill_credit = bill_alloc.find('BILLCREDITPERIOD')
                if bill_credit and bill_credit.text:
                    payment_terms = bill_credit.text.strip()
            
            # If not found, check BASICDUEDATEOFPYMT
            if not payment_terms:
                due_date = v.find('BASICDUEDATEOFPYMT')
                if due_date and due_date.text:
                    payment_terms = due_date.text.strip()
            
            # Get Order Status
            order_status = v.find('ORDERSTATUS').text if v.find('ORDERSTATUS') else "Pending"
            
            # Get Purchase Ledger using HIERARCHY METHOD (same as sales order)
            # Method 1: Try to get from stock item's ledger account
            purchase_ledger = ""
            purchase_ledger_from_item = ""
            
            # First, try to get purchase ledger from inventory entries
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                # Check if there's a ledger associated with this item
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    purchase_ledger_from_item = item_ledger.text.strip()
                    break
            
            # Method 2: If not found in items, find the ledger with LARGEST POSITIVE amount
            # (excluding vendor, taxes, and rounding) - for purchases, the ledger has positive amount
            if not purchase_ledger_from_item:
                max_positive_amount = 0
                for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                    name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amt_tag = entry.find('AMOUNT')
                    if amt_tag and amt_tag.text:
                        import re
                        amount_text = amt_tag.text.strip()
                        numbers = re.findall(r'[-\d.]+', amount_text)
                        if numbers:
                            amt = float(numbers[-1])
                        else:
                            amt = 0.0
                    else:
                        amt = 0.0
                    
                    # Skip vendor ledger, tax ledgers, and rounding off
                    name_lower = name.lower()
                    if name == party_str:  # Skip vendor
                        continue
                    if 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower:  # Skip taxes
                        continue
                    if 'rounding' in name_lower:  # Skip rounding
                        continue
                    
                    # Find the ledger with largest positive amount (this is the purchase ledger)
                    if amt > max_positive_amount:
                        max_positive_amount = amt
                        purchase_ledger = name
            else:
                purchase_ledger = purchase_ledger_from_item
            
            print(f"\n{'='*150}")
            print(f"PURCHASE ORDER #{idx}")
            print(f"{'='*150}")
            print(f"Date: {date_str}")
            print(f"PO Number: {v_no_str}")
            print(f"Party Name (Vendor): {party_str}")
            print(f"Reference: {ref_str}")
            print(f"Purchase Ledger: {purchase_ledger}")
            print(f"Payment Terms: {payment_terms if payment_terms else '(not specified)'}")
            print(f"Order Status: {order_status}")
            
            # Get Item Details
            print(f"\n{'ITEM NAME':<50} | {'QUANTITY':<12} | {'RATE':<12} | {'AMOUNT':<15} | {'CATEGORY':<20} | {'COST CENTRE':<20}")
            print("-" * 180)
            
            # Find inventory entries
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                # Get quantity
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                # Get rate
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    import re
                    numbers = re.findall(r'[-\d.]+', rate_text)
                    if numbers:
                        rate = float(numbers[-1])
                    else:
                        rate = 0.0
                else:
                    rate = 0.0
                
                # Get amount
                amount_tag = item.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    import re
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    if numbers:
                        amount = float(numbers[-1])
                    else:
                        amount = 0.0
                else:
                    amount = 0.0
                
                # Get reporting tags (Category and Cost Centre)
                category = ""
                cost_centre = ""
                cat_alloc = item.find('CATEGORYALLOCATIONS.LIST')
                if cat_alloc:
                    category_tag = cat_alloc.find('CATEGORY')
                    if category_tag:
                        category = category_tag.text.strip()
                    cc_list = cat_alloc.find('COSTCENTREALLOCATIONS.LIST')
                    if cc_list:
                        cc_name = cc_list.find('NAME')
                        if cc_name:
                            cost_centre = cc_name.text.strip()
                
                print(f"{item_name:<50} | {quantity:<12} | {rate:<12.2f} | {abs(amount):<15.2f} | {category:<20} | {cost_centre:<20}")
            
            # Get Tax Details
            print(f"\n{'TAX TYPE':<40} | {'AMOUNT':<15}")
            print("-" * 150)
            
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    import re
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    if numbers:
                        amt = float(numbers[-1])
                    else:
                        amt = 0.0
                else:
                    amt = 0.0
                
                # Check for tax ledgers
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower):
                    print(f"{name:<40} | {abs(amt):<15.2f}")
            
            # Get Narration
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            print(f"\nNarration: {narration if narration else '(blank)'}")
            print("-" * 150)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=" * 80)
    print("TALLY PURCHASE ORDER VIEWER")
    print("=" * 80)
    
    # Get user input for number of purchase orders
    try:
        num = input("\nHow many purchase orders do you want to fetch? (default: 5): ").strip()
        if num == "":
            num_orders = 5
        else:
            num_orders = int(num)
            if num_orders <= 0:
                print("Invalid number. Using default: 5")
                num_orders = 5
    except ValueError:
        print("Invalid input. Using default: 5")
        num_orders = 5
    
    print(f"\nFetching {num_orders} purchase order(s)...\n")
    fetch_all_purchase_orders(num_orders)