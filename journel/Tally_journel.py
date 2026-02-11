import requests
from bs4 import BeautifulSoup
from collections import defaultdict

TALLY_URL = "http://localhost:9000"

def get_ledger_map():
    """Builds a map that traces custom groups (like Zip Tap Vendors) back to Sundry Creditors."""
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

    # Now we know every group name that counts as a Vendor or Customer
    creditor_groups = get_all_subgroups("Sundry Creditors")
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
            if parent in creditor_groups: l_map[name] = "(vendors)"
            elif parent in debtor_groups: l_map[name] = "(customers)"
            else: l_map[name] = "(others)"
    except: pass
    return l_map

def fetch_journals(num_journals=5, from_date="20250401", to_date="20250407"):
    """
    Fetch journal vouchers from Tally
    
    Args:
        num_journals: Number of journal entries to display (default: 5)
        from_date: Start date in YYYYMMDD format (default: April 1, 2025)
        to_date: End date in YYYYMMDD format (default: April 7, 2025 - 1 week)
    
    Note: Using 1 week default instead of full month to prevent timeout.
          Journal vouchers are much more numerous than invoices/bills.
    """
    l_map = get_ledger_map()
    
    # Fetch Journal vouchers for specified date range
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Journal</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=90)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        # Take only the specified number of vouchers
        vouchers = soup.find_all('VOUCHER')[:num_journals]

        print(f"Showing first {num_journals} Journal vouchers from {from_date} to {to_date}...")
        print("=" * 130)
        print(f"{'DATE':<12} | {'JRNL NO':<8} | {'LEDGER NAME (TYPE)':<50} | {'DEBIT':<12} | {'CREDIT':<12}")
        print("=" * 130)

        for v in vouchers:
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip()
                amt = float(entry.find('AMOUNT').text or 0)
                l_type = l_map.get(name, "(others)")
                
                debit = abs(amt) if amt < 0 else 0.0
                credit = amt if amt > 0 else 0.0
                
                print(f"{v_date:<12} | {v_no:<8} | {name + ' ' + l_type :<50} | {debit:<12.2f} | {credit:<12.2f}")
                
                # Reporting Tags
                cat_alloc = entry.find('CATEGORYALLOCATIONS.LIST')
                if cat_alloc:
                    cat = cat_alloc.find('CATEGORY').text if cat_alloc.find('CATEGORY') else ""
                    cc = cat_alloc.find('COSTCENTREALLOCATIONS.LIST').find('NAME').text if cat_alloc.find('COSTCENTREALLOCATIONS.LIST') else ""
                    print(f"{'':<12} | {'':<8} |    --> Zoho Reporting Tag: {cat} > {cc}")

            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            if narration: print(f"Narration: {narration}")
            print("-" * 130)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=" * 80)
    print("TALLY JOURNAL VIEWER")
    print("=" * 80)
    
    # Get user input for number of journals
    try:
        num = input("\nHow many journals do you want to fetch? (default: 5): ").strip()
        if num == "":
            num_journals = 5
        else:
            num_journals = int(num)
            if num_journals <= 0:
                print("Invalid number. Using default: 5")
                num_journals = 5
    except ValueError:
        print("Invalid input. Using default: 5")
        num_journals = 5
    
    # Get user input for date range (optional)
    print("\nDate range options:")
    print("1. First week of April 2025 (default - recommended)")
    print("2. Full month of April 2025 (may be slow)")
    print("3. Custom date range")
    
    choice = input("\nSelect option (1/2/3, default: 1): ").strip()
    
    if choice == "2":
        from_date = "20250401"
        to_date = "20250430"
        print("⚠️  Warning: Full month may take longer or timeout if too many journals exist")
    elif choice == "3":
        from_date = input("Enter FROM date (YYYYMMDD, e.g., 20250401): ").strip()
        to_date = input("Enter TO date (YYYYMMDD, e.g., 20250407): ").strip()
    else:
        from_date = "20250401"
        to_date = "20250407"
        print("Using first week of April 2025")
    
    print(f"\nFetching {num_journals} journal(s) from {from_date} to {to_date}...\n")
    fetch_journals(num_journals, from_date, to_date)
