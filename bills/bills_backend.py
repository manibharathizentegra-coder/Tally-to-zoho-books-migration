import os
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from dotenv import load_dotenv
import json
import re
from fuzzywuzzy import fuzz

# Load environment variables
load_dotenv()

TALLY_URL = "http://localhost:9000"
BASE_URL = "https://www.zohoapis.in/books/v3"
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

# Cache for vendor payment terms to avoid repeated queries
vendor_payment_terms_cache = {}

def fetch_vendor_payment_terms(vendor_name):
    """Fetch payment terms from vendor ledger master in Tally"""
    if not vendor_name:
        return ""
    
    # Check cache first
    if vendor_name in vendor_payment_terms_cache:
        return vendor_payment_terms_cache[vendor_name]
    
    # XML request to fetch specific ledger details
    ledger_xml = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Ledgers</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        res = requests.post(TALLY_URL, data=ledger_xml, timeout=15)
        soup = BeautifulSoup(res.content, 'lxml-xml')
        
        # Find the specific vendor ledger
        for ledger in soup.find_all('LEDGER'):
            name = ledger.get('NAME', '').strip()
            if name.lower() == vendor_name.lower():
                # Check for CREDITPERIOD field
                credit_period = ledger.find('CREDITPERIOD')
                if credit_period and credit_period.text:
                    terms = credit_period.text.strip()
                    vendor_payment_terms_cache[vendor_name] = terms
                    return terms
                
                # Alternative: Check for BILLCREDITPERIOD in ledger
                bill_credit = ledger.find('BILLCREDITPERIOD')
                if bill_credit and bill_credit.text:
                    terms = bill_credit.text.strip()
                    vendor_payment_terms_cache[vendor_name] = terms
                    return terms
                
                break
    except:
        pass
    
    # Cache empty result to avoid repeated queries
    vendor_payment_terms_cache[vendor_name] = ""
    return ""

def get_payment_terms_hierarchical(voucher, party_name):
    """
    Extract payment terms using hierarchical method:
    1. Check BILLALLOCATIONS.LIST → BILLCREDITPERIOD
    2. Check BASICDUEDATEOFPYMT field
    3. Search for patterns like "30 days", "45 days" in bill text
    4. Fetch from vendor ledger master (CREDITPERIOD field)
    """
    # Method 1: Check BILLALLOCATIONS.LIST → BILLCREDITPERIOD
    bill_alloc = voucher.find('BILLALLOCATIONS.LIST')
    if bill_alloc:
        bill_credit = bill_alloc.find('BILLCREDITPERIOD')
        if bill_credit and bill_credit.text:
            return bill_credit.text.strip()
    
    # Method 2: Check BASICDUEDATEOFPYMT
    due_date = voucher.find('BASICDUEDATEOFPYMT')
    if due_date and due_date.text:
        return due_date.text.strip()
    
    # Method 3: Search for payment term patterns in entire bill text
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
            return f"{days} Days"
    
    # Method 4: Fetch from vendor ledger master
    vendor_terms = fetch_vendor_payment_terms(party_name)
    if vendor_terms:
        return vendor_terms
    
    return ""

def get_access_token():
    """Get Zoho Books access token"""
    payload = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    res = requests.post("https://accounts.zoho.in/oauth/v2/token", data=payload)
    return res.json().get("access_token")

def get_ledger_map_from_tally():
    """Builds a map that traces custom groups back to Sundry Creditors (Vendors)."""
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

    # Get all vendor groups (Sundry Creditors)
    creditor_groups = get_all_subgroups("Sundry Creditors")

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
            else: l_map[name] = "(others)"
    except: pass
    return l_map

def fetch_tally_bills(bill_number="11"):
    """Fetch a specific bill by voucher number from Tally"""
    ledger_map = get_ledger_map_from_tally()
    
    print(f"[TALLY] Fetching bill with voucher number: {bill_number}...")
    
    # Use specific voucher type to narrow search (like invoice.py does)
    # Common bill voucher types: "Purchase", "Purchase Invoice", "Bill"
    # Adjust the VOUCHERTYPENAME based on your Tally setup
    xml_request = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Purchase</VOUCHERTYPENAME>
    <SVFROMDATE>20250401</SVFROMDATE><SVTODATE>20250430</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        print(f"[TALLY] Searching Purchase vouchers in April 2025...")
        response = requests.post(TALLY_URL, data=xml_request, timeout=30)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        # Get all vouchers and filter by bill number
        all_vouchers = soup.find_all('VOUCHER')
        print(f"[TALLY] Total Purchase vouchers found: {len(all_vouchers)}")
        
        vouchers = []
        for v in all_vouchers:
            v_no = v.find('VOUCHERNUMBER')
            if v_no and v_no.text.strip() == bill_number:
                vouchers.append(v)
                print(f"[TALLY] ✓ Found bill #{bill_number}")
                break
        
        if not vouchers:
            print(f"[ERROR] Bill #{bill_number} not found in Purchase vouchers!")
            print(f"[ERROR] Please check:")
            print(f"  1. Bill number is correct")
            print(f"  2. Bill is in April 2025 date range")
            print(f"  3. Voucher type is 'Purchase' (adjust VOUCHERTYPENAME if different)")
            return []

        bill_data = []
        for idx, v in enumerate(vouchers, 1):
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            # Get vendor from PARTYNAME field
            vendor_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            
            # Get Purchase Order Number
            po_number = v.find('BASICPURCHASEORDERNO').text if v.find('BASICPURCHASEORDERNO') else ""
            
            # Get Reference Number (Vendor Invoice Number)
            reference_number = v.find('REFERENCE').text if v.find('REFERENCE') else ""
            
            # Get Vendor Address
            vendor_address = []
            vendor_addr_list = v.find('BASICBUYERADDRESS.LIST')
            if vendor_addr_list:
                for addr in vendor_addr_list.find_all('BASICBUYERADDRESS'):
                    if addr.text:
                        vendor_address.append(addr.text.strip())
            
            # Get Payment Terms using hierarchical method
            payment_terms = get_payment_terms_hierarchical(v, vendor_name)
            
            # Get Purchase Ledger using HIERARCHY METHOD
            purchase_ledger = ""
            purchase_ledger_from_item = ""
            
            # First, try to get purchase ledger from inventory entries
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                # Check if there's a ledger associated with this item
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    purchase_ledger_from_item = item_ledger.text.strip()
                    break
            
            # Method 2: If not found in items, find the ledger with LARGEST NEGATIVE amount
            if not purchase_ledger_from_item:
                max_negative_amount = 0
                for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                    name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amt = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0
                    
                    # Skip vendor ledger, tax ledgers, and rounding off
                    name_lower = name.lower()
                    if name == vendor_name:  # Skip vendor
                        continue
                    if 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower:  # Skip taxes
                        continue
                    if 'rounding' in name_lower:  # Skip rounding
                        continue
                    
                    # Find the ledger with largest negative amount
                    if amt < max_negative_amount:
                        max_negative_amount = amt
                        purchase_ledger = name
            else:
                purchase_ledger = purchase_ledger_from_item
            
            # Get line items
            line_items = []
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                # Get quantity
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                # Get rate - handle currency conversion strings
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    # Extract only numeric part (handle currency symbols and conversion strings)
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
                
                line_items.append({
                    "item_name": item_name,
                    "quantity": quantity,
                    "rate": rate,
                    "discount": discount,
                    "amount": abs(amount),
                    "category": category,
                    "cost_centre": cost_centre
                })
            
            # Get tax details from LEDGERENTRIES.LIST (ALL TAX TYPES)
            taxes = []
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
                
                # Check for ANY tax ledger (CGST, SGST, IGST, etc.) - look for "input" in name
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower) and 'input' in name_lower:
                    # Extract rate from ledger name (e.g., "CGST Input 6%" or "IGST Input 12%")
                    rate = ""
                    if '%' in name:
                        rate = name.split('%')[0].split()[-1]
                    
                    tax_type = "CGST" if 'cgst' in name_lower else ("SGST" if 'sgst' in name_lower else "IGST")
                    taxes.append({
                        "tax_name": name,
                        "tax_type": tax_type,
                        "tax_rate": rate,
                        "tax_amount": abs(amt)
                    })
            
            # Get rounding off
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
                    break
            
            bill_data.append({
                "date": v_date,
                "bill_number": v_no,
                "vendor_name": vendor_name,
                "po_number": po_number,
                "reference_number": reference_number,
                "vendor_address": vendor_address,
                "payment_terms": payment_terms,
                "purchase_ledger": purchase_ledger,
                "line_items": line_items,
                "taxes": taxes,
                "rounding_off": rounding_off,
                "narration": narration if narration else ""
            })
        
        return bill_data
    except Exception as e:
        print(f"Error fetching bills from Tally: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_zoho_contacts(token):
    """Fetch all VENDOR contacts from Zoho Books with pagination"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    
    all_vendors = {}
    page = 1
    per_page = 200  # Maximum allowed by Zoho Books API
    
    try:
        while True:
            params = {
                "organization_id": ORGANIZATION_ID,
                "page": page,
                "per_page": per_page
            }
            
            res = requests.get(f"{BASE_URL}/contacts", headers=headers, params=params)
            if res.status_code == 200 and res.json().get("code") == 0:
                contacts = res.json().get("contacts", [])
                
                if not contacts:
                    # No more contacts to fetch
                    break
                
                # Filter to only vendors and add to dictionary
                for c in contacts:
                    if c.get("contact_type") == "vendor":
                        all_vendors[c["contact_name"].lower()] = c
                
                # Check if we've fetched all contacts
                page_context = res.json().get("page_context", {})
                has_more_page = page_context.get("has_more_page", False)
                
                if not has_more_page:
                    break
                
                page += 1
            else:
                print(f"Error fetching contacts on page {page}: {res.status_code}")
                break
        
        return all_vendors
    except Exception as e:
        print(f"Error fetching contacts: {e}")
    return {}

def find_or_create_contact(token, contact_map, contact_name):
    """Find existing vendor contact using FUZZY MATCHING - NO AUTO-CREATE"""
    contact_key = contact_name.lower().strip()
    
    # Exact match first
    if contact_key in contact_map:
        print(f"  [EXACT MATCH] Found vendor: {contact_map[contact_key]['contact_name']}")
        return contact_map[contact_key]
    
    # Fuzzy matching to find similar names
    best_match = None
    best_score = 0
    best_name = ""
    
    for existing_name, contact_data in contact_map.items():
        # Calculate similarity score
        score = fuzz.ratio(contact_key, existing_name)
        
        if score > best_score:
            best_score = score
            best_match = contact_data
            best_name = existing_name
    
    # If similarity is >= 75%, use the match
    if best_match and best_score >= 75:
        print(f"  [FUZZY MATCH] Found similar vendor: {best_match['contact_name']} (Score: {best_score}%)")
        # Add to map with the new key for faster future lookups
        contact_map[contact_key] = best_match
        return best_match
    
    # NO AUTO-CREATE - Require manual intervention
    print(f"\n  [ERROR] Vendor '{contact_name}' not found in Zoho Books!")
    if best_match:
        print(f"  [SUGGESTION] Closest match: '{best_match['contact_name']}' (Score: {best_score}%)")
    print(f"  [ACTION REQUIRED] Please create this vendor in Zoho Books manually and run again.")
    print(f"  [SKIPPING] Skipping this bill...\n")
    return None

def get_zoho_accounts(token):
    """Fetch all accounts from Zoho Books"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        res = requests.get(f"{BASE_URL}/chartofaccounts", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            return {a["account_name"].lower(): a["account_id"] for a in res.json().get("chartofaccounts", [])}
    except Exception as e:
        print(f"Error fetching accounts: {e}")
    return {}

def get_zoho_tags(token):
    """Fetch all tags from Zoho Books using reporting_tags API"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    tag_map = {}
    try:
        # Get list of all tag categories
        res = requests.get(f"{BASE_URL}/settings/tags", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            # Use 'reporting_tags' key instead of 'tags'
            categories = res.json().get("reporting_tags", [])
            
            # Loop through each category to get detailed options
            for category in categories:
                tag_id = category.get("tag_id")
                tag_name = category.get("tag_name")
                
                # Get detailed options for this tag
                detail_res = requests.get(f"{BASE_URL}/settings/tags/{tag_id}", headers=headers, params=params)
                if detail_res.status_code == 200:
                    detail_data = detail_res.json()
                    tag_obj = detail_data.get("tag", detail_data.get("reporting_tag", {}))
                    # Use 'tag_options' instead of 'tag_option'
                    options = tag_obj.get("tag_options", [])
                    
                    for option in options:
                        option_name = option.get("tag_option_name", "")
                        option_id = option.get("tag_option_id")
                        if option_name and option_id:
                            # Map by option name for easier lookup
                            tag_map[option_name.lower()] = {
                                "tag_id": tag_id,
                                "tag_option_id": option_id,
                                "tag_name": tag_name,
                                "tag_option_name": option_name
                            }
    except Exception as e:
        print(f"  [WARNING] Error fetching tags: {e}")
    return tag_map

def get_zoho_payment_terms_list(token):
    """Fetch all payment terms from Zoho Books"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        res = requests.get(f"{BASE_URL}/settings/paymentterms", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            terms_data = res.json().get("data", {})
            terms_list = terms_data.get("payment_terms", [])
            # Create mapping: "net 30" -> payment_terms_id
            terms_map = {}
            for term in terms_list:
                term_label = term.get("payment_terms_label", "")
                term_id = term.get("payment_terms_id")
                term_days = term.get("payment_terms")
                if term_label and term_id:
                     # Map by label (e.g., "Net 30")
                    terms_map[term_label.lower()] = term_id
            return terms_map
    except Exception as e:
        print(f"  [WARNING] Error fetching payment terms: {e}")
    return {}

def map_payment_terms(tally_terms, zoho_terms_map):
    """Map Tally payment terms to Zoho Books payment terms ID"""
    if not tally_terms or not zoho_terms_map:
        return None
    
    tally_terms_lower = tally_terms.lower().strip()
    
    # Try exact match first
    if tally_terms_lower in zoho_terms_map:
        return zoho_terms_map[tally_terms_lower]
    
    # Tally sends "30 Days" - try to extract the number and match
    import re
    numbers = re.findall(r'\d+', tally_terms)
    if numbers:
        days = numbers[0]
        # Try variations
        variations = [
            f"net {days}",      # "net 30"
            f"{days} days",     # "30 days"
            f"net{days}",       # "net30"
        ]
        
        for variation in variations:
            if variation in zoho_terms_map:
                return zoho_terms_map[variation]
    
    # If no match found, use "Due on Receipt" as default
    if "due on receipt" in zoho_terms_map:
        return zoho_terms_map["due on receipt"]
    
    return None

def get_zoho_taxes(token):
    """Fetch all taxes AND tax groups from Zoho Books"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    tax_map = {}
    gst_taxes = {}  # Separate map for GST (CGST+SGST) taxes
    igst_taxes = {}  # Separate map for IGST taxes
    
    # Fetch individual taxes
    try:
        res = requests.get(f"{BASE_URL}/settings/taxes", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            taxes = res.json().get("taxes", [])
            for tax in taxes:
                tax_name = tax.get("tax_name", "")
                tax_percentage = tax.get("tax_percentage", 0)
                tax_id = tax.get("tax_id")
                if tax_id:
                    tax_info = {
                        "tax_id": tax_id,
                        "tax_name": tax_name
                    }
                    
                    # Separate GST and IGST
                    if "IGST" in tax_name:
                        igst_taxes[float(tax_percentage)] = tax_info
                    elif "GST" in tax_name:
                        gst_taxes[float(tax_percentage)] = tax_info
                        # Prioritize GST over IGST for percentage mapping
                        tax_map[float(tax_percentage)] = tax_info
                    
                    # Also map by name pattern
                    if "GST" in tax_name or "IGST" in tax_name:
                        tax_map[tax_name.lower()] = tax_info
    except Exception as e:
        print(f"  [WARNING] Error fetching taxes: {e}")
    
    # Fetch tax groups (compound taxes like GST12 [12%])
    try:
        res = requests.get(f"{BASE_URL}/settings/taxgroups", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            tax_groups = res.json().get("tax_groups", [])
            for group in tax_groups:
                group_name = group.get("tax_group_name", "")
                group_percentage = group.get("tax_group_percentage", 0)
                group_id = group.get("tax_group_id")
                if group_id:
                    # Map by percentage (prioritize tax groups over individual taxes)
                    tax_map[float(group_percentage)] = {
                        "tax_id": group_id,
                        "tax_name": group_name
                    }
                    # Also map by name pattern (e.g., "GST12" -> 12%)
                    if "GST" in group_name or "IGST" in group_name:
                        # Extract number from name (e.g., "GST12" -> "gst12")
                        tax_map[group_name.lower().replace(" ", "").replace("[", "").replace("]", "").replace("%", "")] = {
                            "tax_id": group_id,
                            "tax_name": group_name
                        }
    except Exception as e:
        print(f"  [WARNING] Error fetching tax groups: {e}")
    
    # Store GST and IGST maps for later use
    tax_map["_gst_taxes"] = gst_taxes
    tax_map["_igst_taxes"] = igst_taxes
    
    return tax_map

def calculate_total_tax_rate(taxes):
    """Calculate total tax rate from CGST + SGST or IGST"""
    total_rate = 0
    for tax in taxes:
        if tax.get("tax_rate"):
            try:
                total_rate += float(tax["tax_rate"])
            except:
                pass
    return total_rate

def create_zoho_bill(token, bill_data, contact_map, account_map, payment_terms_map, tax_map, tag_map):
    """Create bill in Zoho Books with FULL AUTOMATION"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {
        "organization_id": ORGANIZATION_ID,
        "ignore_auto_number_generation": "true"  # Use Tally bill number
    }
    
    print(f"\n{'='*100}")
    print(f"[BILL] Processing Bill #{bill_data['bill_number']} - Date: {bill_data['date']}")
    print(f"{'='*100}")
    
    # Find or create vendor
    vendor_info = find_or_create_contact(token, contact_map, bill_data["vendor_name"])
    if not vendor_info:
        error_msg = f"Failed to find or create vendor: {bill_data['vendor_name']}"
        print(f"  [ERROR] {error_msg}")
        return {"success": False, "error": error_msg}
    
    print(f"  [VENDOR] {vendor_info['contact_name']} (ID: {vendor_info['contact_id']})")
    
    # Display additional info
    if bill_data.get('po_number'):
        print(f"  [PO] {bill_data['po_number']}")
    if bill_data.get('reference_number'):
        print(f"  [REF] {bill_data['reference_number']}")
    if bill_data.get('payment_terms'):
        print(f"  [TERMS] {bill_data['payment_terms']}")
    
    # Build line items
    zoho_line_items = []
    
    # Calculate total tax rate (CGST + SGST = GST or IGST)
    total_tax_rate = calculate_total_tax_rate(bill_data["taxes"])
    
    # Get tax ID from Zoho based on total rate
    tax_info = tax_map.get(total_tax_rate)
    
    # If exact tax not found, use default 18% tax
    if not tax_info and total_tax_rate > 0:
        print(f"  [WARNING] No tax found for {total_tax_rate}% in Zoho Books")
        print(f"  [WARNING] Available taxes: {', '.join([str(k) for k in tax_map.keys() if isinstance(k, float)])}")
        
        # Check if this is an IGST transaction (interstate) or GST transaction (intrastate)
        is_igst_transaction = False
        for tax in bill_data["taxes"]:
            if tax.get("tax_type") == "IGST":
                is_igst_transaction = True
                break
        
        # Use appropriate default tax based on transaction type
        if is_igst_transaction:
            # Interstate transaction - use IGST18
            default_tax = tax_map.get("_igst_taxes", {}).get(18.0)
            if not default_tax:
                # Try to find IGST18 by name
                for key, val in tax_map.items():
                    if isinstance(key, str) and "igst" in key.lower() and "18" in key:
                        default_tax = val
                        break
            if default_tax:
                print(f"  [DEFAULT] Using IGST18 for interstate transaction instead of {total_tax_rate}%")
        else:
            # Intrastate transaction - use GST18
            default_tax = tax_map.get("_gst_taxes", {}).get(18.0) or tax_map.get(18.0) or tax_map.get("gst18")
            if not default_tax:
                # If GST18 not found, try to find any 18% tax that's not IGST
                for key, val in tax_map.items():
                    if isinstance(key, str) and "18" in key and "igst" not in key.lower():
                        default_tax = val
                        break
            if default_tax:
                print(f"  [DEFAULT] Using GST18 for intrastate transaction instead of {total_tax_rate}%")
        
        if default_tax:
            tax_info = default_tax
        else:
            print(f"  [ERROR] No default 18% tax found!")
    elif tax_info:
        print(f"  [TAX] Using Zoho tax: {tax_info['tax_name']} ({total_tax_rate}%)")
    
    for item in bill_data["line_items"]:
        print(f"  [ITEM] {item['item_name']} - Qty: {item['quantity']} @ Rs.{item['rate']}")
        if item.get('category') or item.get('cost_centre'):
            print(f"     [TAG] Category: {item.get('category', 'N/A')}, Cost Centre: {item.get('cost_centre', 'N/A')}")
        
        # Get purchase account ID
        purchase_account_id = account_map.get(bill_data["purchase_ledger"].lower()) if bill_data.get("purchase_ledger") else None
        
        # Parse quantity to get numeric value
        qty_str = item['quantity'].split()[0] if item['quantity'] else "1"
        try:
            qty = float(qty_str)
        except:
            qty = 1.0
        
        # Parse discount
        try:
            discount = float(item['discount']) if item['discount'] and item['discount'] != '0' else 0
        except:
            discount = 0
        
        line_item = {
            "name": item['item_name'],
            "description": item['item_name'],
            "rate": item['rate'],
            "quantity": qty,
            "discount": discount,
        }
        
        # Add tax ID - REQUIRED for bills (unlike invoices)
        # Use the tax found earlier, or default to 18% GST
        if tax_info:
            line_item["tax_id"] = tax_info["tax_id"]
        
        # Add account if found
        if purchase_account_id:
            line_item["account_id"] = purchase_account_id
            print(f"     [ACCOUNT] Using purchase account: {bill_data['purchase_ledger']}")
        
        # Add reporting tags (Category and Cost Centre)
        tags = []
        if item.get('category'):
            category_tag = tag_map.get(item['category'].lower())
            if category_tag:
                tags.append({
                    "tag_id": category_tag["tag_id"],
                    "tag_option_id": category_tag["tag_option_id"]
                })
                print(f"     [TAG] Category: {item['category']}")
        
        if item.get('cost_centre'):
            cc_tag = tag_map.get(item['cost_centre'].lower())
            if cc_tag:
                tags.append({
                    "tag_id": cc_tag["tag_id"],
                    "tag_option_id": cc_tag["tag_option_id"]
                })
                print(f"     [TAG] Cost Centre: {item['cost_centre']}")
        
        if tags:
            line_item["tags"] = tags
        
        zoho_line_items.append(line_item)
    
    # Display taxes
    if bill_data["taxes"]:
        print(f"\n  [TAX] Taxes:")
        for tax in bill_data["taxes"]:
            print(f"     {tax['tax_type']} {tax['tax_rate']}%: Rs.{tax['tax_amount']}")
        print(f"     Total Tax Rate: {total_tax_rate}%")
    
    # Display rounding off
    if bill_data.get("rounding_off"):
        print(f"  [ROUNDING] Rs.{abs(bill_data['rounding_off'])}")
    
    # Convert date format (YYYYMMDD -> YYYY-MM-DD)
    tally_date = bill_data["date"]
    zoho_date = f"{tally_date[:4]}-{tally_date[4:6]}-{tally_date[6:8]}"
    
    # Map payment terms
    payment_terms_id = map_payment_terms(bill_data.get("payment_terms", ""), payment_terms_map)
    print(f"\n  [DEBUG] Payment Terms Mapping:")
    print(f"    Tally: '{bill_data.get('payment_terms', '')}'")
    print(f"    Mapped ID: {payment_terms_id}")
    print(f"    Available terms: {list(payment_terms_map.keys())}")
    
    if payment_terms_id:
        print(f"  [PAYMENT TERMS] Mapped '{bill_data.get('payment_terms')}' to ID: {payment_terms_id}")
    else:
        if bill_data.get("payment_terms"):
            print(f"  [WARNING] Payment term '{bill_data.get('payment_terms')}' not found in Zoho Books")
    
    # Build payload
    payload = {
        "vendor_id": vendor_info["contact_id"],
        "bill_number": bill_data["bill_number"],
        "reference_number": bill_data.get("reference_number", ""),
        "date": zoho_date,
        "line_items": zoho_line_items,
        "notes": bill_data["narration"][:1000] if bill_data["narration"] else ""
    }
    
    # Add payment terms if available
    if payment_terms_id:
        # Zoho Books expects days number (30), not ID - extract from Tally terms
        tally_terms = bill_data.get("payment_terms", "")
        numbers = re.findall(r'\d+', tally_terms)
        payload["payment_terms"] = int(numbers[0]) if numbers else 0
        print(f"  [PAYMENT TERMS APPLIED] Payment Terms ID: {payment_terms_id}")
    else:
        print(f"  [WARNING] Payment terms not mapped - will use default")
    
    # Add adjustment for rounding off
    if bill_data.get("rounding_off"):
        payload["adjustment"] = bill_data["rounding_off"]
        payload["adjustment_description"] = "Rounding Off"
    
    print(f"\n  [CREATE] Creating bill in Zoho Books...")
    print(f"  Payload: {json.dumps(payload, indent=2)}")
    
    try:
        res = requests.post(f"{BASE_URL}/bills", headers=headers, params=params, json=payload)
        
        # Log full response for debugging
        with open("bill_response.log", "w") as f:
            f.write(f"Status Code: {res.status_code}\n")
            f.write(f"Response: {json.dumps(res.json(), indent=2)}\n")
        
        if res.status_code in [200, 201] and res.json().get("code") == 0:
            bill_id = res.json().get("bill", {}).get("bill_id", "N/A")
            print(f"  [SUCCESS] Bill created with ID: {bill_id}")
            return {"success": True, "bill_id": bill_id}
        else:
            error_data = res.json()
            error_msg = error_data.get("message", "Unknown error")
            print(f"  [FAILED] Status: {res.status_code}")
            print(f"  Response: {json.dumps(error_data, indent=2)}")
            print(f"  [INFO] Full response saved to bill_response.log")
            return {"success": False, "error": f"{error_msg} (Code: {error_data.get('code', 'N/A')})"}
    except Exception as e:
        print(f"  [ERROR] Error creating bill: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

# ----------------------------------------------------------
# API WRAPPER FOR FRONTEND
# ----------------------------------------------------------

def get_all_bills_data(from_date="20250401", to_date="20250430", limit=None):
    """
    Wrapper function for API to get bill data
    Returns formatted data for frontend display
    """
    try:
        bills = fetch_tally_bills_range(from_date, to_date, limit)
        
        if not bills:
            return None
        
        # Calculate stats
        total_bills = len(bills)
        total_amount = sum(bill.get("total_amount", 0) for bill in bills)
        
        return {
            "bills": bills,
            "stats": {
                "total_bills": total_bills,
                "total_amount": round(total_amount, 2),
                "from_date": from_date,
                "to_date": to_date
            }
        }
    except Exception as e:
        print(f"❌ Error in get_all_bills_data: {e}")
        import traceback
        traceback.print_exc()
        return None

def fetch_tally_bills_range(from_date="20250401", to_date="20250430", limit=None):
    """
    Fetch Purchase bills from Tally with ALL fields (matching invoice structure)
    
    Args:
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of bills to fetch
    """
    ledger_map = get_ledger_map_from_tally()
    
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Purchase</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        print(f"📥 Fetching bills from Tally ({from_date} to {to_date})...")
        response = requests.post(TALLY_URL, data=xml_request, timeout=90)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        vouchers = soup.find_all('VOUCHER')
        if limit:
            vouchers = vouchers[:limit]
        
        bill_data = []
        
        for v in vouchers:
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            vendor_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            # Get Purchase Order Number
            po_number = v.find('BASICPURCHASEORDERNO').text if v.find('BASICPURCHASEORDERNO') else ""
            
            # Get Reference Number (Vendor Invoice Number)
            reference_number = v.find('REFERENCE').text if v.find('REFERENCE') else ""
            
            # Get Vendor Address
            vendor_address = []
            vendor_addr_list = v.find('BASICBUYERADDRESS.LIST')
            if vendor_addr_list:
                for addr in vendor_addr_list.find_all('BASICBUYERADDRESS'):
                    if addr.text:
                        vendor_address.append(addr.text.strip())
            
            # Get Payment Terms
            payment_terms = get_payment_terms_hierarchical(v, vendor_name)
            
            # Get Purchase Ledger
            purchase_ledger = ""
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    purchase_ledger = item_ledger.text.strip()
                    break
            
            if not purchase_ledger:
                max_negative_amount = 0
                for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                    name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amount_tag = entry.find('AMOUNT')
                    if amount_tag and amount_tag.text:
                        numbers = re.findall(r'[-\d.]+', amount_tag.text)
                        amt = float(numbers[-1]) if numbers else 0.0
                    else:
                        amt = 0.0
                    
                    name_lower = name.lower()
                    if name == vendor_name or 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower or 'rounding' in name_lower:
                        continue
                    
                    if amt < max_negative_amount:
                        max_negative_amount = amt
                        purchase_ledger = name
            
            # Get line items
            line_items = []
            subtotal = 0
            
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    numbers = re.findall(r'[-\d.]+', rate_text)
                    rate = float(numbers[-1]) if numbers else 0.0
                else:
                    rate = 0.0
                
                discount_tag = item.find('DISCOUNT')
                discount = discount_tag.text.strip() if discount_tag else "0"
                
                amount_tag = item.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amount = float(numbers[-1]) if numbers else 0.0
                else:
                    amount = 0.0
                
                category = ""
                cost_centre = ""
                cat_alloc = item.find('CATEGORYALLOCATIONS.LIST')
                if cat_alloc:
                    category = cat_alloc.find('CATEGORY').text if cat_alloc.find('CATEGORY') else ""
                    cc_list = cat_alloc.find('COSTCENTREALLOCATIONS.LIST')
                    if cc_list:
                        cost_centre = cc_list.find('NAME').text if cc_list.find('NAME') else ""
                
                line_items.append({
                    "item_name": item_name,
                    "quantity": quantity,
                    "rate": rate,
                    "discount": discount,
                    "amount": abs(amount),
                    "category": category,
                    "cost_centre": cost_centre
                })
                
                subtotal += abs(amount)
            
            # Get tax details
            taxes = []
            tax_total = 0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amt = float(numbers[-1]) if numbers else 0.0
                else:
                    amt = 0.0
                
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower) and 'input' in name_lower:
                    tax_rate = ""
                    if '%' in name:
                        tax_rate = name.split('%')[0].split()[-1]
                    
                    tax_type = "CGST" if 'cgst' in name_lower else ("SGST" if 'sgst' in name_lower else "IGST")
                    taxes.append({
                        "tax_name": name,
                        "tax_type": tax_type,
                        "tax_rate": tax_rate,
                        "tax_amount": abs(amt)
                    })
                    tax_total += abs(amt)
            
            # Get rounding off
            rounding_off = 0.0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                if 'rounding' in name.lower():
                    amount_tag = entry.find('AMOUNT')
                    if amount_tag and amount_tag.text:
                        numbers = re.findall(r'[-\d.]+', amount_tag.text)
                        rounding_off = float(numbers[-1]) if numbers else 0.0
                    break
            
            total_amount = subtotal + tax_total + rounding_off
            
            bill_data.append({
                "date": v_date,
                "bill_number": v_no,
                "vendor_name": vendor_name,
                "po_number": po_number,
                "reference_number": reference_number,
                "vendor_address": vendor_address,
                "payment_terms": payment_terms,
                "purchase_ledger": purchase_ledger,
                "narration": narration,
                "line_items": line_items,
                "taxes": taxes,
                "rounding_off": rounding_off,
                "subtotal": round(subtotal, 2),
                "tax_total": round(tax_total, 2),
                "total_amount": round(total_amount, 2)
            })
        
        print(f"✅ Fetched {len(bill_data)} bill(s)")
        return bill_data
    
    except Exception as e:
        print(f"❌ Error fetching Tally bills: {e}")
        import traceback
        traceback.print_exc()
        return []

def sync_bills_to_zoho(selected_bills=None, from_date="20250401", to_date="20250430", limit=None):
    """
    Sync bills to Zoho Books
    
    Args:
        selected_bills: List of bill objects to sync (if None, fetches from Tally)
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of bills to sync
    """
    try:
        print("🚀 Starting Zoho Sync (Bills)...")
        
        # Get access token
        token = get_access_token()
        if not token:
            return {"status": "error", "message": "Failed to get access token"}
        
        # Get Zoho data
        contact_map = get_zoho_contacts(token)
        account_map = get_zoho_accounts(token)
        payment_terms_map = get_zoho_payment_terms_list(token)
        tax_map = get_zoho_taxes(token)
        tag_map = get_zoho_tags(token)
        
        # Get bills to sync
        if not selected_bills:
            bills_to_sync = fetch_tally_bills_range(from_date, to_date, limit)
        else:
            bills_to_sync = selected_bills
            if limit and len(bills_to_sync) > limit:
                bills_to_sync = bills_to_sync[:limit]
        
        if not bills_to_sync:
            return {"status": "error", "message": "No bills to sync"}
        
        print(f"📊 Syncing {len(bills_to_sync)} bill(s) to Zoho Books...")
        
        stats = {"created": 0, "failed": 0, "errors": []}
        
        for bill in bills_to_sync:
            result = create_zoho_bill(token, bill, contact_map, account_map, payment_terms_map, tax_map, tag_map)
            if result.get("success"):
                stats["created"] += 1
                print(f"✅ Synced Bill #{bill['bill_number']}")
            else:
                stats["failed"] += 1
                stats["errors"].append({
                    "bill_number": bill['bill_number'],
                    "vendor": bill['vendor_name'],
                    "error": result.get("error", "Unknown error")
                })
                print(f"❌ Failed Bill #{bill['bill_number']}")
        
        return {"status": "success", "stats": stats}
        
    except Exception as e:
        print(f"❌ Error in sync_bills_to_zoho: {e}")
        return {"status": "error", "message": str(e)}

def main():
    """Main function to migrate bill #11 from Tally to Zoho Books"""
    print("="*100)
    print("TALLY TO ZOHO BOOKS BILL MIGRATION - BILL #11 TEST")
    print("="*100)
    
    # Get access token
    print("\n[AUTH] Authenticating with Zoho Books...")
    token = get_access_token()
    if not token:
        print("[ERROR] Failed to get access token")
        return
    print("[SUCCESS] Authentication successful")
    
    # Fetch contacts, accounts, payment terms, taxes, and tags
    print("\n[FETCH] Fetching Zoho Books data...")
    contact_map = get_zoho_contacts(token)
    account_map = get_zoho_accounts(token)
    payment_terms_map = get_zoho_payment_terms_list(token)
    tax_map = get_zoho_taxes(token)
    tag_map = get_zoho_tags(token)
    print(f"[SUCCESS] Loaded {len(contact_map)} vendors, {len(account_map)} accounts, {len(payment_terms_map)} payment terms, {len([k for k in tax_map.keys() if isinstance(k, float)])} taxes, {len(tag_map)} tags")
    
    # Fetch bill #11 from Tally
    print("\n[FETCH] Fetching bill #11 from Tally...")
    bills = fetch_tally_bills(bill_number="11")
    
    if not bills:
        print("[ERROR] No bills found in Tally")
        return
    
    print(f"[SUCCESS] Found {len(bills)} bill(s)")
    
    # Process bill
    success_count = 0
    for bill in bills:
        if create_zoho_bill(token, bill, contact_map, account_map, payment_terms_map, tax_map, tag_map):
            success_count += 1
    
    print(f"\n{'='*100}")
    print(f"[COMPLETE] MIGRATION COMPLETE: {success_count}/{len(bills)} bill(s) created successfully")
    print(f"{'='*100}")

if __name__ == "__main__":
    main()
