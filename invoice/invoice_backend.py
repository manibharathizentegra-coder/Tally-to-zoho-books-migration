import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import re
import sys
from pathlib import Path

# Add parent directory to path to access shared cache
sys.path.append(str(Path(__file__).parent.parent))

# Import shared cache functions from journel module
from journel.journel_backend import (
    get_access_token,
    get_zoho_contacts,
    find_or_create_contact
)

# Load credentials
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

# URLs
BASE_URL = "https://www.zohoapis.in/books/v3"
TALLY_URL = "http://localhost:9000"

def fetch_tally_invoices(from_date="20250401", to_date="20250430", limit=None):
    """
    Fetch Tax Invoice vouchers from Tally with ALL fields
    Matches the complete field extraction from invoice.py
    
    Args:
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of invoices to fetch
    """
    # Use 'Tax Invoice' voucher type
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Tax Invoice</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        print(f"📥 Fetching invoices from Tally ({from_date} to {to_date})...")
        response = requests.post(TALLY_URL, data=xml_request, timeout=90)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        vouchers = soup.find_all('VOUCHER')
        if limit:
            vouchers = vouchers[:limit]
        
        invoice_data = []
        
        for v in vouchers:
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            customer_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            # Get Purchase Order Number
            po_number = v.find('BASICPURCHASEORDERNO').text if v.find('BASICPURCHASEORDERNO') else ""
            
            # Get Buyer Address
            buyer_address = []
            buyer_addr_list = v.find('BASICBUYERADDRESS.LIST')
            if buyer_addr_list:
                for addr in buyer_addr_list.find_all('BASICBUYERADDRESS'):
                    if addr.text:
                        buyer_address.append(addr.text.strip())
            
            # Get Hidden Fields (IRN, Ack No, Ack Date)
            irn = v.find('IRN').text if v.find('IRN') else ""
            irn_ack_no = v.find('IRNACKNO').text if v.find('IRNACKNO') else ""
            irn_ack_date = v.find('IRNACKDATE').text if v.find('IRNACKDATE') else ""
            
            # Get Payment Terms (hierarchical method)
            payment_terms = get_payment_terms_hierarchical(v, customer_name)
            
            # Get Sales Ledger
            sales_ledger = ""
            # First try from inventory entries
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    sales_ledger = item_ledger.text.strip()
                    break
            
            # If not found, find ledger with largest negative amount
            if not sales_ledger:
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
                    if name == customer_name or 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower or 'rounding' in name_lower:
                        continue
                    
                    if amt < max_negative_amount:
                        max_negative_amount = amt
                        sales_ledger = name
            
            # Get line items from INVENTORYENTRIES.LIST
            line_items = []
            subtotal = 0
            
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                # Get quantity
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                # Get rate - handle currency conversion
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    numbers = re.findall(r'[-\d.]+', rate_text)
                    rate = float(numbers[-1]) if numbers else 0.0
                else:
                    rate = 0.0
                
                # Get discount
                discount_tag = item.find('DISCOUNT')
                discount = discount_tag.text.strip() if discount_tag else "0"
                
                # Get amount - handle currency conversion
                amount_tag = item.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amount = float(numbers[-1]) if numbers else 0.0
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
                
                subtotal += abs(amount)
            
            # Get tax details
            taxes = []
            tax_total = 0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                
                # Get amount - handle currency conversion
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amt = float(numbers[-1]) if numbers else 0.0
                else:
                    amt = 0.0
                
                # Check for tax ledgers
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower) and 'output' in name_lower:
                    # Extract rate from ledger name
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
            
            invoice_data.append({
                "date": v_date,
                "invoice_number": v_no,
                "customer_name": customer_name,
                "po_number": po_number,
                "buyer_address": buyer_address,
                "payment_terms": payment_terms,
                "irn": irn,
                "irn_ack_no": irn_ack_no,
                "irn_ack_date": irn_ack_date,
                "sales_ledger": sales_ledger,
                "narration": narration,
                "line_items": line_items,
                "taxes": taxes,
                "rounding_off": rounding_off,
                "subtotal": round(subtotal, 2),
                "tax_total": round(tax_total, 2),
                "total_amount": round(total_amount, 2)
            })
        
        print(f"✅ Fetched {len(invoice_data)} invoice(s)")
        return invoice_data
    
    except Exception as e:
        print(f"❌ Error fetching Tally invoices: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_payment_terms_hierarchical(voucher, party_name):
    """Extract payment terms using hierarchical method"""
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
    
    # Method 3: Search for payment term patterns
    voucher_text = str(voucher)
    patterns = [
        r'(\d+)\s*days?',
        r'net\s*(\d+)',
        r'(\d+)\s*days?\s*credit',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, voucher_text, re.IGNORECASE)
        if match:
            days = match.group(1)
            return f"{days} Days"
    
    return ""

def sync_invoices_to_zoho(selected_invoices=None, from_date="20250401", to_date="20250430", limit=None):
    """
    Sync invoices to Zoho Books
    
    Args:
        selected_invoices: List of invoice objects to sync (if None, fetches from Tally)
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of invoices to sync
    """
    try:
        print("🚀 Starting Zoho Sync (Invoices)...")
        
        # Get access token
        token = get_access_token()
        if not token:
            return {"status": "error", "message": "Failed to get access token"}
        
        # Force refresh contacts to get latest structure
        print("   🔄 Refreshing contacts from Zoho Books...")
        contact_map = get_zoho_contacts(token, use_cache=False, force_refresh=True)
        print(f"   ✅ Loaded {len(contact_map)} contacts")
        
        # Get invoices to sync
        if not selected_invoices:
            invoices_to_sync = fetch_tally_invoices(from_date, to_date, limit)
        else:
            invoices_to_sync = selected_invoices
            if limit and len(invoices_to_sync) > limit:
                invoices_to_sync = invoices_to_sync[:limit]
        
        if not invoices_to_sync:
            return {"status": "error", "message": "No invoices to sync"}
        
        print(f"📊 Syncing {len(invoices_to_sync)} invoice(s) to Zoho Books...")
        
        stats = {"created": 0, "failed": 0, "errors": []}
        
        for invoice in invoices_to_sync:
            result = create_zoho_invoice(token, invoice, contact_map)
            if result["success"]:
                stats["created"] += 1
                print(f"✅ Synced Invoice #{invoice['invoice_number']}")
            else:
                # Skip error and continue with next invoice
                stats["failed"] += 1
                error_info = {
                    "invoice_number": invoice['invoice_number'],
                    "customer": invoice['customer_name'],
                    "error": result["error"]
                }
                stats["errors"].append(error_info)
                print(f"❌ Failed Invoice #{invoice['invoice_number']}")
                print(f"❌ Error: {result['error']}")
                print(f"⏭️  Skipping to next invoice...\n")
        
        return {"status": "success", "stats": stats}
        
    except Exception as e:
        print(f"❌ Error in sync_invoices_to_zoho: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

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

def get_zoho_accounts(token):
    """Fetch all accounts from Zoho Books"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        res = requests.get(f"{BASE_URL}/chartofaccounts", headers=headers, params=params)
        if res.status_code == 200 and res.json().get("code") == 0:
            return {a["account_name"].lower(): a["account_id"] for a in res.json().get("chartofaccounts", [])}
    except Exception as e:
        print(f"  [WARNING] Error fetching accounts: {e}")
    return {}

def create_zoho_invoice(token, invoice_data, contact_map):
    """Create an invoice in Zoho Books - returns success status and error details"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {
        "organization_id": ORGANIZATION_ID,
        "ignore_auto_number_generation": "true"  # ✅ Use Tally invoice number
    }
    
    print(f"\n{'='*80}")
    print(f"📝 Processing Invoice #{invoice_data['invoice_number']} - Customer: {invoice_data['customer_name']}")
    print(f"{'='*80}")
    
    # Find or create customer
    customer_name = invoice_data["customer_name"]
    contact_info = find_or_create_contact(token, contact_map, customer_name, "customer")
    
    if not contact_info:
        error_msg = f"Failed to find/create customer: {customer_name}"
        print(f"  ❌ {error_msg}")
        return {"success": False, "error": error_msg}
    
    print(f"  ✅ Customer: {contact_info.get('original_name', customer_name)}")

    
    # Get payment terms, tags, and accounts
    payment_terms_map = get_zoho_payment_terms_list(token)
    tag_map = get_zoho_tags(token)
    account_map = get_zoho_accounts(token)  # ✅ Fetch accounts for sales ledger mapping
    
    # Convert date format
    tally_date = invoice_data["date"]
    zoho_date = f"{tally_date[:4]}-{tally_date[4:6]}-{tally_date[6:8]}"
    
    # Get sales account ID from sales_ledger
    sales_account_id = None
    if invoice_data.get("sales_ledger"):
        sales_account_id = account_map.get(invoice_data["sales_ledger"].lower())
        if sales_account_id:
            print(f"  💰 Sales Account: {invoice_data['sales_ledger']}")
        else:
            print(f"  ⚠️  Sales account '{invoice_data['sales_ledger']}' not found in Zoho")
    
    # Build line items with reporting tags and account
    zoho_line_items = []
    for item in invoice_data["line_items"]:
        # Parse quantity
        qty_str = item['quantity'].split()[0] if item['quantity'] else "1"
        try:
            qty = float(qty_str)
        except:
            qty = 1.0
        
        line_item = {
            "name": item["item_name"],
            "description": item["item_name"],
            "rate": item["rate"],
            "quantity": qty
        }
        
        # Add sales account if found (e.g., Carpet Dealers instead of Sales)
        if sales_account_id:
            line_item["account_id"] = sales_account_id
        
        # Add reporting tags (Category and Cost Centre)
        tags = []
        if item.get('category'):
            category_tag = tag_map.get(item['category'].lower())
            if category_tag:
                tags.append({
                    "tag_id": category_tag["tag_id"],
                    "tag_option_id": category_tag["tag_option_id"]
                })
                print(f"  🏷️  Category: {item['category']}")
            else:
                print(f"  ⚠️  Category '{item['category']}' not found in Zoho")
        
        if item.get('cost_centre'):
            cc_tag = tag_map.get(item['cost_centre'].lower())
            if cc_tag:
                tags.append({
                    "tag_id": cc_tag["tag_id"],
                    "tag_option_id": cc_tag["tag_option_id"]
                })
                print(f"  🏷️  Cost Centre: {item['cost_centre']}")
            else:
                print(f"  ⚠️  Cost Centre '{item['cost_centre']}' not found in Zoho")
        
        if tags:
            line_item["tags"] = tags
        
        zoho_line_items.append(line_item)
    
    # Build payload
    payload = {
        "customer_id": contact_info["contact_id"],
        "invoice_number": invoice_data["invoice_number"],  # ✅ Use Tally invoice number
        "reference_number": invoice_data.get("po_number", ""),  # ✅ Use PO number as reference
        "date": zoho_date,
        "line_items": zoho_line_items,
        "notes": invoice_data.get("narration", "")[:1000] if invoice_data.get("narration") else ""
    }
    
    # Map payment terms
    if invoice_data.get("payment_terms"):
        payment_terms_id = map_payment_terms(invoice_data["payment_terms"], payment_terms_map)
        if payment_terms_id:
            # Extract days number from Tally terms (e.g., "30 Days" -> 30)
            numbers = re.findall(r'\d+', invoice_data["payment_terms"])
            if numbers:
                payload["payment_terms"] = int(numbers[0])
                print(f"  ✅ Payment Terms: {invoice_data['payment_terms']} -> {numbers[0]} days")
        else:
            print(f"  ⚠️  Payment term '{invoice_data['payment_terms']}' not found in Zoho")
    
    print(f"  📤 Creating invoice in Zoho Books...")
    res = requests.post(f"{BASE_URL}/invoices", headers=headers, params=params, json=payload)
    
    if res.status_code in [200, 201] and res.json().get("code") == 0:
        invoice_id = res.json().get("invoice", {}).get("invoice_id", "N/A")
        print(f"  ✅ SUCCESS! Invoice created with ID: {invoice_id}")
        return {"success": True, "invoice_id": invoice_id}
    else:
        error_data = res.json()
        error_msg = error_data.get("message", "Unknown error")
        print(f"  ❌ FAILED! Status: {res.status_code}")
        print(f"  Response: {json.dumps(error_data, indent=2)}")
        return {"success": False, "error": f"{error_msg} (Code: {error_data.get('code', 'N/A')})"}

# ----------------------------------------------------------
# API WRAPPER FOR FRONTEND
# ----------------------------------------------------------

def get_all_invoices_data(from_date="20250401", to_date="20250430", limit=None):
    """
    Wrapper function for API to get invoice data
    Returns formatted data for frontend display
    """
    try:
        invoices = fetch_tally_invoices(from_date, to_date, limit)
        
        if not invoices:
            return None
        
        # Calculate stats
        total_invoices = len(invoices)
        total_amount = sum(inv.get("total_amount", 0) for inv in invoices)
        
        return {
            "invoices": invoices,
            "stats": {
                "total_invoices": total_invoices,
                "total_amount": round(total_amount, 2),
                "from_date": from_date,
                "to_date": to_date
            }
        }
    except Exception as e:
        print(f"❌ Error in get_all_invoices_data: {e}")
        import traceback
        traceback.print_exc()
        return None
