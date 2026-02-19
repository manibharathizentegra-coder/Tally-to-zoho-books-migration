"""
Debug Tally Cost Center XML Fields
This script fetches receipts/vouchers from Tally and prints ALL XML fields
to identify the correct tags for cost center names like "Distribution Model" and "Corporate-Head Office"
"""

import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv()

TALLY_URL = os.getenv("TALLY_URL", "http://localhost:9000")

def debug_cost_center_fields():
    """Fetch and print raw XML to identify cost center fields"""
    
    # Fetch receipts with ALL possible fields
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
                            <FETCH>*</FETCH>
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
        print("🔍 Fetching receipts from Tally with ALL fields...")
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
        
        print("=" * 100)
        print("📄 FIRST RECEIPT - COMPLETE RAW XML")
        print("=" * 100)
        print(v.prettify())
        print("=" * 100)
        
        # Save full XML to file
        with open('tally_receipt_full.xml', 'w', encoding='utf-8') as f:
            f.write(v.prettify())
        print("\n✅ Full XML saved to: tally_receipt_full.xml")
        
        # Now search for specific patterns
        print("\n" + "=" * 100)
        print("🔍 SEARCHING FOR COST CENTER RELATED FIELDS")
        print("=" * 100)
        
        # Search for all tags containing "COST", "CENTRE", "CENTER", "CATEGORY", "ALLOCATION"
        keywords = ['COST', 'CENTRE', 'CENTER', 'CATEGORY', 'ALLOCATION', 'DISTRIBUTION', 'CORPORATE']
        
        for keyword in keywords:
            print(f"\n🔎 Tags containing '{keyword}':")
            print("-" * 80)
            
            # Find all tags
            all_tags = v.find_all()
            found_tags = set()
            
            for tag in all_tags:
                if keyword.lower() in tag.name.upper():
                    found_tags.add(tag.name)
            
            if found_tags:
                for tag_name in sorted(found_tags):
                    elements = v.find_all(tag_name)
                    print(f"\n  Tag: <{tag_name}> (Found {len(elements)} instances)")
                    
                    # Show first 3 instances
                    for idx, elem in enumerate(elements[:3], 1):
                        print(f"    Instance {idx}:")
                        # Show the element and its immediate children
                        if elem.string and elem.string.strip():
                            print(f"      Value: {elem.string.strip()}")
                        else:
                            # Show children
                            for child in elem.children:
                                if hasattr(child, 'name') and child.name:
                                    child_val = child.string.strip() if child.string else ""
                                    print(f"        <{child.name}>: {child_val}")
            else:
                print(f"  ⚠️ No tags found")
        
        # Specific searches for the names you mentioned
        print("\n" + "=" * 100)
        print("🎯 SEARCHING FOR SPECIFIC VALUES")
        print("=" * 100)
        
        search_values = ['Distribution Model', 'Corporate', 'Carpets', 'Corporate-Head Office']
        
        for search_val in search_values:
            print(f"\n🔎 Searching for text: '{search_val}'")
            print("-" * 80)
            
            # Search in all text
            all_tags = v.find_all(string=lambda text: text and search_val.lower() in text.lower())
            
            if all_tags:
                for idx, tag in enumerate(all_tags[:5], 1):
                    parent = tag.parent
                    print(f"  Match {idx}:")
                    print(f"    Text: {tag.strip()}")
                    print(f"    Parent Tag: <{parent.name}>")
                    
                    # Show parent's XML
                    print(f"    Parent XML:")
                    parent_lines = str(parent).split('\n')
                    for line in parent_lines[:10]:  # Show first 10 lines
                        print(f"      {line}")
            else:
                print(f"  ⚠️ Not found in this voucher")
        
        # Check LEDGERENTRIES for cost center info
        print("\n" + "=" * 100)
        print("💰 LEDGER ENTRIES ANALYSIS")
        print("=" * 100)
        
        ledger_entries = v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST')
        
        for idx, entry in enumerate(ledger_entries, 1):
            print(f"\n📋 Ledger Entry {idx}:")
            print("-" * 80)
            
            # Print all children
            for child in entry.children:
                if hasattr(child, 'name') and child.name:
                    # Check if this child has sub-children
                    if list(child.children):
                        has_text_only = all(not hasattr(c, 'name') for c in child.children)
                        if has_text_only:
                            child_val = child.get_text(strip=True)
                            print(f"  <{child.name}>: {child_val}")
                        else:
                            print(f"  <{child.name}>:")
                            # Print sub-children
                            for subchild in child.children:
                                if hasattr(subchild, 'name') and subchild.name:
                                    subchild_val = subchild.get_text(strip=True)
                                    print(f"    <{subchild.name}>: {subchild_val}")
        
        # Check INVENTORY ENTRIES
        print("\n" + "=" * 100)
        print("📦 INVENTORY ENTRIES ANALYSIS")
        print("=" * 100)
        
        inv_entries = v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST')
        
        if inv_entries:
            for idx, entry in enumerate(inv_entries[:3], 1):
                print(f"\n📦 Inventory Entry {idx}:")
                print("-" * 80)
                print(entry.prettify()[:1000])  # First 1000 chars
        else:
            print("  ⚠️ No inventory entries found")
        
        print("\n" + "=" * 100)
        print("✅ DEBUG COMPLETE")
        print("=" * 100)
        print("\nNext steps:")
        print("1. Check 'tally_receipt_full.xml' for the complete XML structure")
        print("2. Look for tags containing cost center names in the output above")
        print("3. Identify the correct XML path to extract cost center names")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def debug_cost_center_master():
    """Fetch cost center master data from Tally"""
    
    print("\n" + "=" * 100)
    print("🏢 FETCHING COST CENTER MASTER DATA")
    print("=" * 100)
    
    xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Accounts</REPORTNAME>
    <STATICVARIABLES><ACCOUNTTYPE>CostCentres</ACCOUNTTYPE></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=10)
        soup = BeautifulSoup(response.text, 'xml')
        
        print("\n📄 COST CENTRE MASTER XML:")
        print("-" * 100)
        print(soup.prettify()[:3000])  # First 3000 chars
        
        # Save to file
        with open('tally_costcentre_master.xml', 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        print("\n✅ Cost Centre Master XML saved to: tally_costcentre_master.xml")
        
        # Extract cost centres
        centres = soup.find_all('COSTCENTRE')
        print(f"\n📊 Found {len(centres)} cost centres:")
        print("-" * 100)
        
        for idx, centre in enumerate(centres[:10], 1):
            name = centre.get('NAME', 'N/A')
            category = centre.find('CATEGORY')
            parent = centre.find('PARENT')
            
            print(f"\n{idx}. Cost Centre:")
            print(f"   NAME attribute: {name}")
            print(f"   CATEGORY: {category.text if category else 'N/A'}")
            print(f"   PARENT: {parent.text if parent else 'N/A'}")
            
            # Print all children
            print(f"   All fields:")
            for child in centre.children:
                if hasattr(child, 'name') and child.name:
                    child_val = child.get_text(strip=True)
                    print(f"     <{child.name}>: {child_val}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Tally Cost Center Debug")
    print("=" * 100)
    
    # Debug voucher data
    debug_cost_center_fields()
    
    # Debug master data
    debug_cost_center_master()
    
    print("\n" + "=" * 100)
    print("🎉 ALL DEBUGGING COMPLETE")
    print("=" * 100)
