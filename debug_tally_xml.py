"""
Debug Tally Receipt XML
This script fetches ONE receipt from Tally and prints the raw XML to see what data is available
"""

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()

TALLY_URL = os.getenv("TALLY_URL", "http://localhost:9000")

def debug_tally_receipt():
    """Fetch and print raw XML for one receipt"""
    
    # Fetch just 1 receipt
    xml_request = f"""
    <ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>Receipts</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                    <EXPLODEFLAG>Yes</EXPLODEFLAG>
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="Receipts" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
                            <TYPE>Voucher</TYPE>
                            <FETCH>DATE, VOUCHERNUMBER, VOUCHERTYPENAME, PARTYNAME, NARRATION, GUID, REFERENCE, CHEQUENUMBER</FETCH>
                            <FETCH>ALLLEDGERENTRIES.LIST, BILLALLOCATIONS.LIST, CATEGORYALLOCATIONS.LIST</FETCH>
                            <FILTER>VoucherTypeFilter</FILTER>
                        </COLLECTION>
                        <SYSTEM TYPE="Formulae" NAME="VoucherTypeFilter">$VoucherTypeName = "Receipt"</SYSTEM>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>
    """
    
    try:
        print("🔍 Fetching receipt from Tally...")
        response = requests.post(TALLY_URL, data=xml_request, headers={'Content-Type': 'application/xml'})
        
        if response.status_code != 200:
            print(f"❌ Error: HTTP {response.status_code}")
            return
        
        soup = BeautifulSoup(response.text, 'xml')
        vouchers = soup.find_all('VOUCHER')
        
        if not vouchers:
            print("❌ No receipts found!")
            return
        
        # Get first voucher
        v = vouchers[0]
        
        print("=" * 80)
        print("📄 FIRST RECEIPT - RAW XML")
        print("=" * 80)
        print(v.prettify())
        print("=" * 80)
        
        # Extract key fields
        print("\n📊 EXTRACTED FIELDS:")
        print("-" * 80)
        print(f"Receipt Number: {v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else 'N/A'}")
        print(f"Date: {v.find('DATE').text if v.find('DATE') else 'N/A'}")
        print(f"Party Name: {v.find('PARTYNAME').text if v.find('PARTYNAME') else 'N/A'}")
        print(f"Narration: {v.find('NARRATION').text if v.find('NARRATION') else 'N/A'}")
        
        print("\n📋 BILL ALLOCATIONS:")
        print("-" * 80)
        bill_allocs = v.find_all('BILLALLOCATIONS.LIST')
        if bill_allocs:
            for idx, bill in enumerate(bill_allocs, 1):
                name = bill.find('NAME').text if bill.find('NAME') else 'N/A'
                amount = bill.find('AMOUNT').text if bill.find('AMOUNT') else 'N/A'
                print(f"  {idx}. Name: {name}")
                print(f"     Amount: {amount}")
        else:
            print("  ⚠️ No BILLALLOCATIONS.LIST found!")
        
        print("\n🏢 CATEGORY ALLOCATIONS:")
        print("-" * 80)
        cat_allocs = v.find_all('CATEGORYALLOCATIONS.LIST')
        if cat_allocs:
            for idx, cat in enumerate(cat_allocs, 1):
                category = cat.find('CATEGORY').text if cat.find('CATEGORY') else 'N/A'
                amount = cat.find('AMOUNT').text if cat.find('AMOUNT') else 'N/A'
                print(f"  {idx}. Category: {category}")
                print(f"     Amount: {amount}")
        else:
            print("  ⚠️ No CATEGORYALLOCATIONS.LIST found!")
        
        print("\n💰 LEDGER ENTRIES:")
        print("-" * 80)
        ledger_entries = v.find_all('ALLLEDGERENTRIES.LIST') or v.find_all('LEDGERENTRIES.LIST')
        if ledger_entries:
            for idx, entry in enumerate(ledger_entries, 1):
                ledger = entry.find('LEDGERNAME').text if entry.find('LEDGERNAME') else 'N/A'
                amount = entry.find('AMOUNT').text if entry.find('AMOUNT') else 'N/A'
                balance = entry.find('CURRENTBALANCE').text if entry.find('CURRENTBALANCE') else 'N/A'
                print(f"  {idx}. Ledger: {ledger}")
                print(f"     Amount: {amount}")
                print(f"     Current Balance: {balance}")
        else:
            print("  ⚠️ No LEDGERENTRIES found!")
        
        print("=" * 80)
        
        # Save to file
        with open('tally_receipt_debug.xml', 'w', encoding='utf-8') as f:
            f.write(v.prettify())
        print("\n✅ Raw XML saved to: tally_receipt_debug.xml")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_tally_receipt()
