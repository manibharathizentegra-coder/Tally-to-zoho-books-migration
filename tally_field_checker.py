"""
Test script to fetch and display cost center allocations from Tally vouchers
"""

import requests
from bs4 import BeautifulSoup

TALLY_URL = "http://localhost:9000"

def test_cost_center_extraction():
    """Fetch a voucher and show cost center allocation structure"""
    
    # Request voucher with cost center details
    xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Receipt</VOUCHERTYPENAME>
    <SVFROMDATE>20250401</SVFROMDATE><SVTODATE>20250430</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        print("🔍 Fetching receipts from Tally...")
        response = requests.post(TALLY_URL, data=xml_request, timeout=10)
        soup = BeautifulSoup(response.text, 'xml')
        
        vouchers = soup.find_all('VOUCHER')
        print(f"✅ Found {len(vouchers)} vouchers\n")
        
        if not vouchers:
            print("❌ No vouchers found!")
            return
        
        # Check first voucher
        v = vouchers[0]
        
        print("=" * 100)
        print("📄 ANALYZING FIRST VOUCHER")
        print("=" * 100)
        
        # Get basic info
        voucher_num = v.find('VOUCHERNUMBER')
        print(f"\n📋 Voucher Number: {voucher_num.text if voucher_num else 'N/A'}")
        
        # Look for ALL possible cost center related tags
        print("\n🔍 Searching for cost center related tags...")
        print("-" * 100)
        
        # Method 1: CATEGORYALLOCATIONS.LIST
        print("\n1️⃣ CATEGORYALLOCATIONS.LIST:")
        cat_allocs = v.find_all('CATEGORYALLOCATIONS.LIST')
        print(f"   Found {len(cat_allocs)} CATEGORYALLOCATIONS.LIST elements")
        
        for idx, cat_alloc in enumerate(cat_allocs[:5], 1):
            print(f"\n   Allocation {idx}:")
            # Print all children
            for child in cat_alloc.children:
                if hasattr(child, 'name') and child.name:
                    child_val = child.get_text(strip=True)
                    print(f"     <{child.name}>: {child_val}")
        
        # Method 2: COSTCENTREALLOCATIONS.LIST
        print("\n2️⃣ COSTCENTREALLOCATIONS.LIST:")
        cc_allocs = v.find_all('COSTCENTREALLOCATIONS.LIST')
        print(f"   Found {len(cc_allocs)} COSTCENTREALLOCATIONS.LIST elements")
        
        for idx, cc_alloc in enumerate(cc_allocs[:5], 1):
            print(f"\n   Allocation {idx}:")
            for child in cc_alloc.children:
                if hasattr(child, 'name') and child.name:
                    child_val = child.get_text(strip=True)
                    print(f"     <{child.name}>: {child_val}")
        
        # Method 3: Check within LEDGERENTRIES
        print("\n3️⃣ Cost Centers within LEDGERENTRIES:")
        ledger_entries = v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST')
        print(f"   Found {len(ledger_entries)} ledger entries")
        
        for idx, entry in enumerate(ledger_entries[:3], 1):
            ledger_name = entry.find('LEDGERNAME')
            print(f"\n   Ledger {idx}: {ledger_name.text if ledger_name else 'N/A'}")
            
            # Check for cost center allocations within ledger entry
            cat_in_ledger = entry.find_all('CATEGORYALLOCATIONS.LIST')
            cc_in_ledger = entry.find_all('COSTCENTREALLOCATIONS.LIST')
            
            if cat_in_ledger:
                print(f"     → Has {len(cat_in_ledger)} CATEGORYALLOCATIONS.LIST")
                for cat in cat_in_ledger[:2]:
                    category = cat.find('CATEGORY')
                    cost_centre_name = cat.find('COSTCENTRENAME')
                    amount = cat.find('AMOUNT')
                    print(f"        CATEGORY: {category.text if category else 'N/A'}")
                    print(f"        COSTCENTRENAME: {cost_centre_name.text if cost_centre_name else 'N/A'}")
                    print(f"        AMOUNT: {amount.text if amount else 'N/A'}")
            
            if cc_in_ledger:
                print(f"     → Has {len(cc_in_ledger)} COSTCENTREALLOCATIONS.LIST")
                for cc in cc_in_ledger[:2]:
                    name = cc.find('NAME')
                    amount = cc.find('AMOUNT')
                    print(f"        NAME: {name.text if name else 'N/A'}")
                    print(f"        AMOUNT: {amount.text if amount else 'N/A'}")
        
        # Method 4: Check within INVENTORYENTRIES
        print("\n4️⃣ Cost Centers within INVENTORYENTRIES:")
        inv_entries = v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST')
        print(f"   Found {len(inv_entries)} inventory entries")
        
        for idx, entry in enumerate(inv_entries[:2], 1):
            item_name = entry.find('STOCKITEMNAME')
            print(f"\n   Item {idx}: {item_name.text if item_name else 'N/A'}")
            
            cat_in_inv = entry.find_all('CATEGORYALLOCATIONS.LIST')
            cc_in_inv = entry.find_all('COSTCENTREALLOCATIONS.LIST')
            
            if cat_in_inv:
                print(f"     → Has {len(cat_in_inv)} CATEGORYALLOCATIONS.LIST")
            if cc_in_inv:
                print(f"     → Has {len(cc_in_inv)} COSTCENTREALLOCATIONS.LIST")
        
        # Save full voucher XML
        with open('voucher_cost_center_debug.xml', 'w', encoding='utf-8') as f:
            f.write(v.prettify())
        print("\n✅ Full voucher XML saved to: voucher_cost_center_debug.xml")
        
        print("\n" + "=" * 100)
        print("🎯 KEY FINDINGS:")
        print("=" * 100)
        print(f"CATEGORYALLOCATIONS.LIST count: {len(cat_allocs)}")
        print(f"COSTCENTREALLOCATIONS.LIST count: {len(cc_allocs)}")
        print("\nCheck the saved XML file for complete structure!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cost_center_extraction()
