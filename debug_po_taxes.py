import requests
from bs4 import BeautifulSoup

TALLY_URL = "http://localhost:9000"

# Fetch one purchase order
xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
<BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
<VOUCHERTYPENAME>Purchase Order</VOUCHERTYPENAME>
<SVFROMDATE>20250401</SVFROMDATE><SVTODATE>20250430</SVTODATE>
</STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

try:
    response = requests.post(TALLY_URL, data=xml_request, timeout=30)
    soup = BeautifulSoup(response.content, 'lxml-xml')
    
    vouchers = soup.find_all('VOUCHER')
    if vouchers:
        v = vouchers[0]  # First purchase order
        
        print("=" * 80)
        print("LEDGER ENTRIES (ALL):")
        print("=" * 80)
        
        for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
            name = entry.find('LEDGERNAME')
            amount = entry.find('AMOUNT')
            
            if name and amount:
                print(f"Ledger: {name.text.strip()}")
                print(f"Amount: {amount.text.strip()}")
                print("-" * 40)
        
        print("\n" + "=" * 80)
        print("TAX LEDGERS ONLY (containing cgst/sgst/igst):")
        print("=" * 80)
        
        for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
            name = entry.find('LEDGERNAME')
            amount = entry.find('AMOUNT')
            
            if name:
                name_text = name.text.strip()
                name_lower = name_text.lower()
                
                if 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower:
                    print(f"✓ TAX LEDGER: {name_text}")
                    if amount:
                        print(f"  Amount: {amount.text.strip()}")
                    print("-" * 40)
    else:
        print("No purchase orders found!")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
