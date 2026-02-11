import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import re

TALLY_URL = "http://localhost:9000"

def fetch_customer_payment_terms(customer_name):
    """Fetch payment terms from customer ledger master in Tally"""
    if not customer_name:
        return ""
    
    # XML request to fetch specific ledger details
    ledger_xml = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Ledgers</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        res = requests.post(TALLY_URL, data=ledger_xml, timeout=15)
        soup = BeautifulSoup(res.content, 'lxml-xml')
        
        # Find the specific customer ledger
        for ledger in soup.find_all('LEDGER'):
            name = ledger.get('NAME', '').strip()
            if name.lower() == customer_name.lower():
                # Check for CREDITPERIOD field
                credit_period = ledger.find('CREDITPERIOD')
                if credit_period and credit_period.text:
                    return credit_period.text.strip()
                
                # Alternative: Check for BILLCREDITPERIOD in ledger
                bill_credit = ledger.find('BILLCREDITPERIOD')
                if bill_credit and bill_credit.text:
                    return bill_credit.text.strip()
                
                break
    except:
        pass
    
    return ""

def get_payment_terms_hierarchical(voucher, party_name):
    """
    Extract payment terms using hierarchical method:
    1. Check BILLALLOCATIONS.LIST → BILLCREDITPERIOD
    2. Check BASICDUEDATEOFPYMT field
    3. Search for patterns like "30 days", "45 days" in sales order text
    4. Fetch from customer ledger master (CREDITPERIOD field)
    """
    # Method 1: Check BILLALLOCATIONS.LIST → BILLCREDITPERIOD
    bill_alloc = voucher.find('BILLALLOCATIONS.LIST')
    if bill_alloc:
        bill_credit = bill_alloc.find('BILLCREDITPERIOD')
        if bill_credit and bill_credit.text:
            return bill_credit.text.strip(), "sales_order"
    
    # Method 2: Check BASICDUEDATEOFPYMT
    due_date = voucher.find('BASICDUEDATEOFPYMT')
    if due_date and due_date.text:
        return due_date.text.strip(), "sales_order"
    
    # Method 3: Search for payment term patterns in entire sales order text
    # Pattern: "30 days", "45 days", "net 30", etc.
    voucher_text = str(voucher)
    patterns = [
        r'(\d+)\s*days?',  # "30 days" or "30 day"
        r'net\s*(\d+)',     # "net 30"
        r'(\d+)\s*days?\s*credit',  # "30 days credit"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, voucher_text, re.IGNORECASE)
        if match:
            days = match.group(1)
            return f"{days} Days", "sales_order"
    
    # Method 4: Fetch from customer ledger master
    customer_terms = fetch_customer_payment_terms(party_name)
    if customer_terms:
        return customer_terms, "customer"
    
    return "", "none"

def get_ledger_map():
    """Builds a map that traces custom groups back to Sundry Debtors (Customers)."""
    # 1. Fetch all Groups to build the 'Family Tree'
    group_xml = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Accounts</REPORTNAME>
    <STATICVARIABLES><ACCOUNTTYPE>Groups</ACCOUNTTYPE><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    children_map = defaultdict(list)
    try:
        res = requests.post(TALLY_URL, data=group_xml, timeout=15)
        soup = BeautifulSoup(res.content, 'lxml-xml')
        for g in soup.find_all('GROUP'):
            name = g.get('NAME', '').strip()
            parent = g.find('PARENT').text.strip() if g.find('PARENT') else ""
            if name: children_map[parent].append(name)
    except: pass

    # Recursive function to find ALL children/grandchildren of a group
    def get_all_subgroups(group_name, visited=None):
        if visited is None: visited = set()
        if group_name in visited: return set()
        visited.add(group_name)
        results = {group_name}
        for child in children_map.get(group_name, []):
            results.update(get_all_subgroups(child, visited))
        return results

    # Get all customer groups (Sundry Debtors)
    debtor_groups = get_all_subgroups("Sundry Debtors")

    # 2. Fetch all Ledgers and map them
    ledger_xml = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Ledgers</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    l_map = {}
    try:
        res = requests.post(TALLY_URL, data=ledger_xml, timeout=15)
        soup = BeautifulSoup(res.content, 'lxml-xml')
        for l in soup.find_all('LEDGER'):
            name = l.get('NAME', '').strip()
            parent = l.find('PARENT').text.strip() if l.find('PARENT') else ""
            if parent in debtor_groups: l_map[name] = "(customers)"
            else: l_map[name] = "(others)"
    except: pass
    return l_map

def fetch_sales_orders(num_orders=5):
    l_map = get_ledger_map()
    
    # Fetch Sales Order vouchers for April 1-7, 2025 (1 week to avoid timeout)
    xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Sales Order</VOUCHERTYPENAME>
    <SVFROMDATE>20250401</SVFROMDATE><SVTODATE>20250407</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=30)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        # Take the specified number of vouchers
        vouchers = soup.find_all('VOUCHER')[:num_orders]

        print(f"Showing first {num_orders} Sales Order vouchers from April 1-7, 2025...")
        print("=" * 160)
        
        for idx, v in enumerate(vouchers, 1):
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            
            # Get Party A/C Name (Customer) from PARTYNAME field
            party_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            
            # Get Purchase Order Number
            po_number = v.find('REFERENCE').text if v.find('REFERENCE') else ""
            
            # Get Buyer Address
            buyer_address = []
            buyer_addr_list = v.find('BASICBUYERADDRESS.LIST')
            if buyer_addr_list:
                for addr in buyer_addr_list.find_all('BASICBUYERADDRESS'):
                    if addr.text:
                        buyer_address.append(addr.text.strip())
            
            # Get Payment Terms using hierarchical method
            payment_terms, terms_source = get_payment_terms_hierarchical(v, party_name)
            
            # Get Order Status
            order_status = v.find('ORDERSTATUS').text if v.find('ORDERSTATUS') else "Pending"
            
            print(f"\n{'='*160}")
            print(f"SALES ORDER #{idx}")
            print(f"{'='*160}")
            print(f"Date: {v_date}")
            print(f"Sales Order No: {v_no}")
            print(f"Party A/C Name: {party_name}")
            print(f"Purchase Order No: {po_number}")
            print(f"Buyer Address: {', '.join(buyer_address) if buyer_address else '(not provided)'}")
            print(f"Order Status: {order_status}")
            # Display payment terms with source indicator
            if payment_terms:
                source_label = f" (from {terms_source})" if terms_source == "customer" else ""
                print(f"Payment Terms: {payment_terms}{source_label}")
            else:
                print(f"Payment Terms: (not specified)")
            
            # Get Sales Ledger using HIERARCHY METHOD
            # Method 1: Try to get from stock item's ledger account
            sales_ledger = ""
            sales_ledger_from_item = ""
            
            # First, try to get sales ledger from inventory entries
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                # Check if there's a ledger associated with this item
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    sales_ledger_from_item = item_ledger.text.strip()
                    break
            
            # Method 2: If not found in items, find the ledger with LARGEST NEGATIVE amount
            # (excluding customer, taxes, and rounding)
            if not sales_ledger_from_item:
                max_negative_amount = 0
                for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                    name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amt = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0
                    
                    # Skip customer ledger, tax ledgers, and rounding off
                    name_lower = name.lower()
                    if name == party_name:  # Skip customer
                        continue
                    if 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower:  # Skip taxes
                        continue
                    if 'rounding' in name_lower:  # Skip rounding
                        continue
                    
                    # Find the ledger with largest negative amount
                    if amt < max_negative_amount:
                        max_negative_amount = amt
                        sales_ledger = name
            else:
                sales_ledger = sales_ledger_from_item
            
            print(f"Sales Ledger: {sales_ledger}")
            
            # Get Item Details
            print(f"\n{'ITEM NAME':<50} | {'QUANTITY':<12} | {'RATE':<12} | {'DISC %':<8} | {'AMOUNT':<15} | {'CATEGORY':<20} | {'COST CENTRE':<20}")
            print("-" * 180)
            
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                # Get quantity - handle both ACTUALQTY and BILLEDQTY
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                # Get rate - handle currency conversion strings
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    # Extract only numeric part (handle currency symbols and conversion strings)
                    import re
                    numbers = re.findall(r'[-\d.]+', rate_text)
                    if numbers:
                        # Use the last number (usually the converted amount)
                        rate = float(numbers[-1])
                    else:
                        rate = 0.0
                else:
                    rate = 0.0
                
                # Get discount
                discount_tag = item.find('DISCOUNT')
                discount = discount_tag.text.strip() if discount_tag else "0"
                
                # Get amount - handle currency conversion strings
                amount_tag = item.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    # Extract only numeric part (handle currency symbols and conversion strings)
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    if numbers:
                        # Use the last number (usually the converted amount)
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
                    category = cat_alloc.find('CATEGORY').text if cat_alloc.find('CATEGORY') else ""
                    cc_list = cat_alloc.find('COSTCENTREALLOCATIONS.LIST')
                    if cc_list:
                        cost_centre = cc_list.find('NAME').text if cc_list.find('NAME') else ""
                
                print(f"{item_name:<50} | {quantity:<12} | {rate:<12.2f} | {discount:<8} | {abs(amount):<15.2f} | {category:<20} | {cost_centre:<20}")
            
            # Get Tax Details (ALL TAXES - CGST, SGST, IGST, etc.) from LEDGERENTRIES.LIST
            print(f"\n{'TAX TYPE':<40} | {'RATE':<10} | {'AMOUNT':<15}")
            print("-" * 180)
            
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                # Get amount - handle currency conversion strings
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    if numbers:
                        amt = float(numbers[-1])
                    else:
                        amt = 0.0
                else:
                    amt = 0.0
                
                # Check for ANY tax ledger (CGST, SGST, IGST, etc.) - look for "output" or "gst" in name
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower) and 'output' in name_lower:
                    # Extract rate from ledger name (e.g., "CGST Output 6%" or "IGST Output 12%")
                    rate = ""
                    if '%' in name:
                        rate = name.split('%')[0].split()[-1] + '%'
                    print(f"{name:<40} | {rate:<10} | {abs(amt):<15.2f}")
            
            # Get Rounding Off
            rounding_off = 0.0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                # Get amount - handle currency conversion strings
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    if numbers:
                        amt = float(numbers[-1])
                    else:
                        amt = 0.0
                else:
                    amt = 0.0
                
                if 'rounding' in name.lower():
                    rounding_off = amt
                    print(f"{name:<40} | {'':<10} | {abs(amt):<15.2f}")
                    break
            
            # Get Narration (show empty space if blank)
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            print(f"\nNarration: {narration if narration else '(blank)'}")
            
            print("-" * 160)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=" * 80)
    print("TALLY SALES ORDER VIEWER")
    print("=" * 80)
    
    # Get user input for number of sales orders
    try:
        num = input("\nHow many sales orders do you want to fetch? (default: 5): ").strip()
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
    
    print(f"\nFetching {num_orders} sales order(s)...\n")
    fetch_sales_orders(num_orders)
