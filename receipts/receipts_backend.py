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
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

# Import database manager
try:
    import database_manager
except ImportError:
    print("️ Warning: Could not import database_manager. SQLite sync will be skipped.")
    database_manager = None

# Load environment variables
load_dotenv()

# Zoho API credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

# URLs
BASE_URL = "https://www.zohoapis.com/books/v3"
TALLY_URL = "http://localhost:9000"

# ----------------------------------------------------------
# JOB HELPERS (SSE-friendly logging + stop)
# ----------------------------------------------------------

def _make_emitter(log=None):
    def _emit(msg: str):
        try:
            if callable(log):
                log(msg)
            else:
                print(msg)
        except Exception:
            pass
    return _emit

def _should_stop(stop_event) -> bool:
    try:
        return bool(stop_event and getattr(stop_event, "is_set", None) and stop_event.is_set())
    except Exception:
        return False

def _iter_days(from_yyyymmdd: str, to_yyyymmdd: str):
    start = datetime.strptime(from_yyyymmdd, "%Y%m%d")
    end = datetime.strptime(to_yyyymmdd, "%Y%m%d")
    cur = start
    from datetime import timedelta
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

def fetch_tally_receipts_day_by_day(from_date="20250401", to_date="20250430", limit=None, company_name=None, *, log=None, stop_event=None):
    """
    Day-by-day fetch to avoid large payload/timeouts.
    Ensures each request uses the current loop date.
    """
    _emit = _make_emitter(log)
    receipts = []
    total_days = 0

    _emit(f"Fetching receipts: {from_date} -> {to_date} (day by day)...")

    for day in _iter_days(from_date, to_date):
        if _should_stop(stop_event):
            _emit("Stopped by user.")
            break

        total_days += 1
        day_receipts = []  # IMPORTANT: clear per-day results

        xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
        <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
        <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
        <VOUCHERTYPENAME>Receipt</VOUCHERTYPENAME>
        <SVFROMDATE>{day}</SVFROMDATE><SVTODATE>{day}</SVTODATE>
        </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

        try:
            response = requests.post(TALLY_URL, data=xml_request, timeout=45)
            soup = BeautifulSoup(response.content, 'lxml-xml')
            vouchers = soup.find_all('VOUCHER')
            if limit:
                vouchers = vouchers[:limit]

            for v in vouchers:
                receipt_date = v.find('DATE').text.strip() if v.find('DATE') else day
                receipt_number = v.find('VOUCHERNUMBER').text.strip() if v.find('VOUCHERNUMBER') else ""
                voucher_type = v.find('VOUCHERTYPENAME').text.strip() if v.find('VOUCHERTYPENAME') else "Receipt"
                tally_guid = v.find('GUID').text.strip() if v.find('GUID') else ""

                customer_name = v.find('PARTYNAME').text.strip() if v.find('PARTYNAME') else ""
                customer_ledger_amount = 0.0

                ledger_entries = []
                payment_mode = ""
                bank_account = ""
                account_current_balance = 0.0
                rounding_amount = 0.0
                rounding_ledger = ""
                against_reference = ""

                raw_entries = v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST')
                for entry in raw_entries:
                    ledger_name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amount = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0.0

                    current_balance = 0.0
                    cb_tag = entry.find('CURRENTBALANCE')
                    if cb_tag:
                        cb_text = cb_tag.text.strip()
                        m = re.search(r'([\\d,]+\\.?\\d*)', cb_text)
                        if m:
                            current_balance = float(m.group(1).replace(',', ''))
                            if 'Dr' in cb_text:
                                current_balance = -current_balance

                    ledger_entries.append({
                        "ledger_name": ledger_name,
                        "amount": amount,
                        "current_balance": current_balance
                    })

                    lname = ledger_name.lower()
                    if 'rounding' in lname:
                        rounding_amount = amount
                        rounding_ledger = ledger_name
                    elif any(k in lname for k in ['bank', 'cash', 'sbi', 'hdfc', 'icici', 'axis', 'kotak', 'idfc']):
                        bank_account = ledger_name
                        payment_mode = "Cash" if 'cash' in lname else "Bank Transfer"
                        account_current_balance = current_balance
                    elif amount > 0 and not any(k in lname for k in ['cash', 'bank', 'cgst', 'sgst', 'igst', 'rounding']):
                        if not customer_name:
                            customer_name = ledger_name
                        customer_ledger_amount = abs(amount)

                total_amount = customer_ledger_amount
                reference_number = v.find('REFERENCE').text.strip() if v.find('REFERENCE') else ""
                if not reference_number:
                    reference_number = v.find('CHEQUENUMBER').text.strip() if v.find('CHEQUENUMBER') else ""
                narration = v.find('NARRATION').text.strip() if v.find('NARRATION') else ""

                invoice_allocations = []
                bill_allocs_found = v.find_all('BILLALLOCATIONS.LIST')
                for bill_alloc in bill_allocs_found:
                    inv_name = bill_alloc.find('NAME').text.strip() if bill_alloc.find('NAME') else ""
                    raw_amt = float(bill_alloc.find('AMOUNT').text or 0) if bill_alloc.find('AMOUNT') else 0.0
                    bill_type = bill_alloc.find('BILLTYPE').text.strip() if bill_alloc.find('BILLTYPE') else "Agst Ref"

                    if not inv_name and bill_type == "On Account":
                        inv_name = "On Account"

                    if not inv_name:
                        continue

                    if not against_reference:
                        against_reference = inv_name

                    dr_cr = "Cr" if raw_amt < 0 else "Dr"
                    final_amount = abs(raw_amt) if raw_amt != 0 else customer_ledger_amount
                    invoice_allocations.append({
                        "invoice_number": inv_name,
                        "bill_type": bill_type,
                        "amount": float(final_amount or 0),
                        "dr_cr": dr_cr,
                    })

                day_receipts.append({
                    "date": receipt_date,
                    "receipt_number": receipt_number,
                    "voucher_type": voucher_type,
                    "customer_name": customer_name,
                    "customer_ledger_amount": customer_ledger_amount,
                    "payment_mode": payment_mode,
                    "bank_account": bank_account,
                    "account_current_balance": account_current_balance,
                    "amount": total_amount,
                    "reference_number": reference_number,
                    "against_reference": against_reference,
                    "narration": narration,
                    "invoice_allocations": invoice_allocations,
                    "ledger_entries": ledger_entries,
                    "cost_center_allocations": [],
                    "rounding_amount": rounding_amount,
                    "rounding_ledger": rounding_ledger,
                    "tally_guid": tally_guid,
                })

            receipts.extend(day_receipts)
            day_iso = datetime.strptime(day, "%Y%m%d").strftime("%Y-%m-%d")
            _emit(f"[{day_iso}] Fetched {len(day_receipts)} records")

        except requests.exceptions.ConnectionError as e:
            raise Exception(f"ConnectionError: Failed to connect to Tally on {TALLY_URL}. Is Tally running and configured for XML export? Error: {str(e)}")
        except Exception as e:
            day_iso = datetime.strptime(day, "%Y%m%d").strftime("%Y-%m-%d")
            _emit(f"[{day_iso}] Error: {e}")

    _emit(f"Done. Total fetched: {len(receipts)}")
    return receipts

# ----------------------------------------------------------
# TALLY RECEIPT FETCHING
# ----------------------------------------------------------

def fetch_tally_receipts(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    """
    Fetch Receipt vouchers from Tally with ALL fields
    
    Args:
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of receipts to fetch
        company_name: Specific company name to filter (if None, uses current company)
    
    Returns:
        List of receipt dictionaries
    """
    
    # Build XML request for Receipt vouchers
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Receipt</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=30)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        vouchers = soup.find_all('VOUCHER')
        if limit:
            vouchers = vouchers[:limit]
        
        receipts = []
        
        for v in vouchers:
            # Basic fields
            receipt_date = v.find('DATE').text if v.find('DATE') else ""
            receipt_number = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            voucher_type = v.find('VOUCHERTYPENAME').text if v.find('VOUCHERTYPENAME') else "Receipt"
            tally_guid = v.find('GUID').text if v.find('GUID') else ""
            
            # Get customer name from PARTYNAME or from ledger entries
            customer_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            customer_ledger_amount = 0.0
            
            # Extract ALL ledger entries
            ledger_entries = []
            payment_mode = ""
            bank_account = ""
            account_current_balance = 0.0
            rounding_amount = 0.0
            rounding_ledger = ""
            against_reference = ""
            
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                ledger_name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                amount = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0
                
                # Get current balance if available
                current_balance_tag = entry.find('CURRENTBALANCE')
                current_balance = 0.0
                if current_balance_tag:
                    current_balance_text = current_balance_tag.text.strip()
                    # Extract numeric value from "4,11,07,348.31 Cr" format
                    import re
                    balance_match = re.search(r'([\d,]+\.?\d*)', current_balance_text)
                    if balance_match:
                        current_balance = float(balance_match.group(1).replace(',', ''))
                        if 'Dr' in current_balance_text:
                            current_balance = -current_balance
                
                # Store ledger entry
                ledger_entry = {
                    "ledger_name": ledger_name,
                    "amount": amount,
                    "current_balance": current_balance
                }
                ledger_entries.append(ledger_entry)
                
                # Identify customer ledger (positive amount, not bank/cash/tax)
                if amount > 0 and not any(keyword in ledger_name.lower() for keyword in ['cash', 'bank', 'cgst', 'sgst', 'igst', 'rounding']):
                    if not customer_name:
                        customer_name = ledger_name
                    customer_ledger_amount = abs(amount)
                
                # Identify bank/cash account (negative amount)
                if amount < 0 and not any(keyword in ledger_name.lower() for keyword in ['rounding']):
                    if 'cash' in ledger_name.lower():
                        payment_mode = "Cash"
                        bank_account = ledger_name
                        account_current_balance = current_balance
                    elif 'bank' in ledger_name.lower():
                        payment_mode = "Bank Transfer"
                        bank_account = ledger_name
                        account_current_balance = current_balance
                    else:
                        if not payment_mode:  # Only set if not already set
                            payment_mode = "Other"
                            bank_account = ledger_name
                            account_current_balance = current_balance
                
                # Identify rounding entries
                if 'rounding' in ledger_name.lower():
                    rounding_amount = amount
                    rounding_ledger = ledger_name
            
            # Extract cost center allocations
            cost_center_allocations = []
            category_allocs_found = v.find_all('CATEGORYALLOCATIONS.LIST')
            
            # Debug logging
            if receipt_number == "1":  # Log for first receipt
                print(f"\n DEBUG Cost Centers for Receipt #{receipt_number}:")
                print(f"   Found {len(category_allocs_found)} CATEGORYALLOCATIONS.LIST elements")
            
            for category_alloc in category_allocs_found:
                category_name = category_alloc.find('CATEGORY').text if category_alloc.find('CATEGORY') else ""
                
                # Find nested cost centers
                cost_centre_allocs = category_alloc.find_all('COSTCENTREALLOCATIONS.LIST')
                
                if cost_centre_allocs:
                    for cc_alloc in cost_centre_allocs:
                        cc_name = cc_alloc.find('NAME').text if cc_alloc.find('NAME') else ""
                        cc_amount = float(cc_alloc.find('AMOUNT').text or 0) if cc_alloc.find('AMOUNT') else 0
                        
                        # Combine Category and Cost Center Name to show BOTH
                        # Format: "Carpets - Distribution Model"
                        full_name = f"{category_name} - {cc_name}" if category_name and cc_name else (cc_name or category_name)
                        
                        # Debug logging
                        if receipt_number == "1":
                            print(f"   - Found Cost Center: '{full_name}' | Amount: {cc_amount}")
                        
                        if full_name:
                            cost_center_allocations.append({
                                "category": full_name,  # Shows "Category - CostCenter"
                                "amount": abs(cc_amount)
                            })
                else:
                    # Fallback: if no nested cost centers, check for direct amount
                    direct_amount_tag = category_alloc.find('AMOUNT', recursive=False)
                    if direct_amount_tag:
                        amount = float(direct_amount_tag.text or 0)
                        if amount != 0:
                            if receipt_number == "1":
                                print(f"   - Found Direct Category: '{category_name}' | Amount: {amount}")
                                
                            cost_center_allocations.append({
                                "category": category_name,
                                "amount": abs(amount)
                            })
            
            if receipt_number == "1":
                print(f"   Total cost_center_allocations: {len(cost_center_allocations)}")
            
            # Get total amount (from customer ledger - positive amount)
            total_amount = customer_ledger_amount
            
            # Get reference/cheque number
            reference_number = v.find('REFERENCE').text if v.find('REFERENCE') else ""
            if not reference_number:
                reference_number = v.find('CHEQUENUMBER').text if v.find('CHEQUENUMBER') else ""
            
            # Get narration
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            # Get invoice allocations (which invoices this payment is applied to)
            invoice_allocations = []
            bill_allocs_found = v.find_all('BILLALLOCATIONS.LIST')
            
            # Debug logging
            if receipt_number in ["1", "323", "152"]:  # Log for specific receipts
                print(f"\n DEBUG Receipt #{receipt_number}:")
                print(f"   Found {len(bill_allocs_found)} BILLALLOCATIONS.LIST elements")
            
            for bill_alloc in bill_allocs_found:
                invoice_name = bill_alloc.find('NAME').text if bill_alloc.find('NAME') else ""
                invoice_amount = float(bill_alloc.find('AMOUNT').text or 0) if bill_alloc.find('AMOUNT') else 0
                bill_type = bill_alloc.find('BILLTYPE').text if bill_alloc.find('BILLTYPE') else "Agst Ref"
                
                # Handle On Account entries which have no name but need to be captured
                if not invoice_name and bill_type == "On Account":
                    invoice_name = "On Account"
                
                # Debug logging
                if receipt_number in ["1", "323", "152"]:
                    print(f"   - Invoice Name: {invoice_name}")
                    print(f"   - Bill Type: {bill_type}")
                    print(f"   - Invoice Amount: {invoice_amount}")
                
                if invoice_name:  # Only require invoice name, not amount
                    # Store the invoice reference
                    if not against_reference:
                        against_reference = invoice_name
                    
                    # Use customer_ledger_amount if invoice_amount is 0
                    final_amount = abs(invoice_amount) if invoice_amount != 0 else customer_ledger_amount
                    
                    invoice_allocations.append({
                        "invoice_number": invoice_name,
                        "bill_type": bill_type, # Added dynamic bill type
                        "amount": final_amount
                    })
                    
                    if receipt_number in ["1", "323", "152"]:
                        print(f"    Added to invoice_allocations: {invoice_name} - {final_amount}")
            
            if receipt_number in ["1", "323", "152"]:
                print(f"   Total invoice_allocations: {len(invoice_allocations)}")
                print(f"   against_reference: {against_reference}")
            
            receipt = {
                "date": receipt_date,
                "receipt_number": receipt_number,
                "voucher_type": voucher_type,
                "customer_name": customer_name,
                "customer_ledger_amount": customer_ledger_amount,
                "payment_mode": payment_mode,
                "bank_account": bank_account,
                "account_current_balance": account_current_balance,
                "amount": total_amount,
                "reference_number": reference_number,
                "against_reference": against_reference,
                "narration": narration,
                "invoice_allocations": invoice_allocations,
                "ledger_entries": ledger_entries,
                "cost_center_allocations": cost_center_allocations,
                "rounding_amount": rounding_amount,
                "rounding_ledger": rounding_ledger,
                "tally_guid": tally_guid
            }
            
            receipts.append(receipt)
        
        print(f" Fetched {len(receipts)} receipts from Tally")
        
        return receipts
        
    except requests.exceptions.RequestException as e:
        print(f" Error connecting to Tally on {TALLY_URL}: {e}")
        raise Exception(f"Failed to connect to Tally on port 9000. Is Tally running and configured for XML export? Error: {str(e)}")
    except Exception as e:
        print(f" Error parsing receipts from Tally: {e}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Error parsing Tally data: {str(e)}")

# ----------------------------------------------------------
# ZOHO BOOKS INTEGRATION
# ----------------------------------------------------------

def get_access_token():
    """Get Zoho access token using refresh token"""
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    
    try:
        response = requests.post(url, params=params)
        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            print(f" Failed to get access token: {response.text}")
            return None
    except Exception as e:
        print(f" Error getting access token: {e}")
        return None

def get_zoho_customers(token):
    """Fetch all customers from Zoho Books"""
    url = f"{BASE_URL}/contacts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            contacts = response.json().get("contacts", [])
            # Create a map of customer name to customer ID
            customer_map = {}
            for contact in contacts:
                customer_map[contact["contact_name"]] = {
                    "customer_id": contact["contact_id"],
                    "email": contact.get("email", "")
                }
            return customer_map
        else:
            print(f" Failed to fetch customers: {response.text}")
            return {}
    except Exception as e:
        print(f" Error fetching customers: {e}")
        return {}

def get_zoho_invoices(token, customer_id=None):
    """Fetch invoices from Zoho Books for a specific customer"""
    url = f"{BASE_URL}/invoices"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    if customer_id:
        params["customer_id"] = customer_id
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            invoices = response.json().get("invoices", [])
            # Create a map of invoice number to invoice ID and balance
            invoice_map = {}
            for invoice in invoices:
                invoice_map[invoice["invoice_number"]] = {
                    "invoice_id": invoice["invoice_id"],
                    "balance": float(invoice.get("balance", 0)),
                    "total": float(invoice.get("total", 0))
                }
            return invoice_map
        else:
            print(f" Failed to fetch invoices: {response.text}")
            return {}
    except Exception as e:
        print(f" Error fetching invoices: {e}")
        return {}

def get_zoho_bank_accounts(token):
    """Fetch all bank accounts from Zoho Books"""
    url = f"{BASE_URL}/bankaccounts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            accounts = response.json().get("bankaccounts", [])
            # Create a map of account name to account ID
            account_map = {}
            for account in accounts:
                account_map[account["account_name"]] = account["account_id"]
            return account_map
        else:
            print(f" Failed to fetch bank accounts: {response.text}")
            return {}
    except Exception as e:
        print(f" Error fetching bank accounts: {e}")
        return {}

def create_zoho_payment_received(token, receipt_data, customer_map, invoice_map, bank_account_map):
    """
    Create a payment received in Zoho Books
    
    Args:
        token: Zoho access token
        receipt_data: Receipt data from Tally
        customer_map: Map of customer names to customer IDs
        invoice_map: Map of invoice numbers to invoice IDs
        bank_account_map: Map of bank account names to account IDs
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    
    # Get customer ID
    customer_name = receipt_data.get("customer_name", "")
    if customer_name not in customer_map:
        return False, f"Customer '{customer_name}' not found in Zoho Books"
    
    customer_id = customer_map[customer_name]["customer_id"]
    
    # Get bank account ID
    bank_account_name = receipt_data.get("bank_account", "")
    account_id = None
    
    # Try to match bank account
    for acc_name, acc_id in bank_account_map.items():
        if bank_account_name.lower() in acc_name.lower() or acc_name.lower() in bank_account_name.lower():
            account_id = acc_id
            break
    
    if not account_id:
        # Use first available bank account as default
        if bank_account_map:
            account_id = list(bank_account_map.values())[0]
        else:
            return False, "No bank accounts found in Zoho Books"
    
    # Convert date format from YYYYMMDD to YYYY-MM-DD
    date_str = receipt_data.get("date", "")
    if len(date_str) == 8:
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    else:
        formatted_date = datetime.now().strftime("%Y-%m-%d")
    
    # Build payment data
    payment_data = {
        "customer_id": customer_id,
        "payment_mode": receipt_data.get("payment_mode", "cash"),
        "amount": receipt_data.get("amount", 0),
        "date": formatted_date,
        "reference_number": receipt_data.get("reference_number", ""),
        "description": receipt_data.get("narration", ""),
        "account_id": account_id,
        "invoices": []
    }
    
    # Add invoice allocations
    for allocation in receipt_data.get("invoice_allocations", []):
        invoice_number = allocation.get("invoice_number", "")
        if invoice_number in invoice_map:
            payment_data["invoices"].append({
                "invoice_id": invoice_map[invoice_number]["invoice_id"],
                "amount_applied": allocation.get("amount", 0)
            })
    
    # If no invoice allocations, this is an advance payment
    # Zoho Books will automatically handle it
    
    url = f"{BASE_URL}/customerpayments"
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/json"
    }
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        response = requests.post(
            url,
            headers=headers,
            params=params,
            json={"JSONString": json.dumps(payment_data)}
        )
        
        if response.status_code in [200, 201]:
            print(f" Created payment received for {customer_name}: ₹{receipt_data.get('amount', 0)}")
            return True, None
        else:
            error_msg = response.json().get("message", response.text)
            print(f" Failed to create payment: {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = str(e)
        print(f" Error creating payment: {error_msg}")
        return False, error_msg

# ----------------------------------------------------------
# SYNC FUNCTION
# ----------------------------------------------------------

def sync_receipts_to_zoho(selected_receipts=None, from_date="20250401", to_date="20250430", limit=None, company_name=None):
    """
    Sync receipts to Zoho Books
    
    Args:
        selected_receipts: List of receipt objects to sync (if None, fetches from Tally)
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of receipts to sync
        company_name: Specific company name to filter
    
    Returns:
        Dictionary with sync results
    """
    
    # Get access token
    token = get_access_token()
    if not token:
        return {"status": "error", "message": "Failed to get Zoho access token"}
    
    # Fetch receipts if not provided
    if selected_receipts is None:
        receipts = fetch_tally_receipts(from_date, to_date, limit, company_name)
    else:
        receipts = selected_receipts
    
    if not receipts:
        return {"status": "error", "message": "No receipts to sync"}
    
    # Get Zoho data
    print(" Fetching Zoho Books data...")
    customer_map = get_zoho_customers(token)
    bank_account_map = get_zoho_bank_accounts(token)
    
    # Sync each receipt
    results = {
        "total": len(receipts),
        "success": 0,
        "failed": 0,
        "errors": []
    }
    
    for receipt in receipts:
        # Get invoices for this customer
        customer_name = receipt.get("customer_name", "")
        customer_id = customer_map.get(customer_name, {}).get("customer_id")
        
        invoice_map = {}
        if customer_id:
            invoice_map = get_zoho_invoices(token, customer_id)
        
        success, error = create_zoho_payment_received(
            token, receipt, customer_map, invoice_map, bank_account_map
        )
        
        if success:
            results["success"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({
                "receipt_number": receipt.get("receipt_number", ""),
                "customer": customer_name,
                "error": error
            })
    
    results["status"] = "success"
    results["message"] = f"Synced {results['success']} out of {results['total']} receipts"
    
    return results

# ----------------------------------------------------------
# API WRAPPER FOR FRONTEND
# ----------------------------------------------------------

def get_all_receipts_data(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    """
    Wrapper function for API to get receipt data
    Returns formatted data for frontend display
    Saves data to SQLite database
    """
    # Initialize DB if possible
    if database_manager:
        database_manager.init_db()
    
    receipts = fetch_tally_receipts(from_date, to_date, limit, company_name)
    
    # Save each receipt to database
    if database_manager and receipts:
        from datetime import datetime
        db_data_list = []
        
        for receipt in receipts:
            db_data = {
                "receipt_number": receipt.get("receipt_number", ""),
                "voucher_type": receipt.get("voucher_type", ""),
                "date": receipt.get("date", ""),
                "customer_name": receipt.get("customer_name", ""),
                "customer_ledger_amount": receipt.get("customer_ledger_amount", 0) or 0,
                "payment_mode": receipt.get("payment_mode", ""),
                "bank_account": receipt.get("bank_account", ""),
                "account_current_balance": receipt.get("account_current_balance", 0) or 0,
                "amount": receipt.get("amount", 0) or 0,
                "reference_number": receipt.get("reference_number", ""),
                "against_reference": receipt.get("against_reference", ""),
                "narration": receipt.get("narration", ""),
                "invoice_allocations": json.dumps(receipt.get("invoice_allocations", [])),
                "ledger_entries": json.dumps(receipt.get("ledger_entries", [])),
                "cost_center_allocations": json.dumps(receipt.get("cost_center_allocations", [])),
                "rounding_amount": receipt.get("rounding_amount", 0) or 0,
                "rounding_ledger": receipt.get("rounding_ledger", ""),
                "tally_guid": receipt.get("tally_guid", ""),
                "company_name": company_name or "",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            db_data_list.append(db_data)
            
        # Bulk save to prevent database lock issues
        database_manager.bulk_save_receipts(db_data_list)
        print(f" Saved {len(receipts)} receipts to database")
    
    # Calculate totals
    total_amount = sum(r.get("amount", 0) for r in receipts)
    
    return {
        "receipts": receipts,
        "count": len(receipts),
        "total_amount": total_amount,
        "from_date": from_date,
        "to_date": to_date
    }


def get_all_receipts_data_day_by_day(from_date="20250401", to_date="20250430", limit=None, company_name=None, *, log=None, stop_event=None):
    """
    Job-friendly wrapper:
    - Fetches receipts day-by-day (prevents repeated counts / stale dates)
    - Upserts into SQLite preferring tally_guid
    """
    _emit = _make_emitter(log)

    if database_manager:
        try:
            database_manager.init_db()
        except Exception:
            pass

    receipts = fetch_tally_receipts_day_by_day(from_date, to_date, limit, company_name, log=log, stop_event=stop_event)

    if database_manager and receipts:
        from datetime import datetime as _dt
        db_data_list = []
        for receipt in receipts:
            db_data_list.append({
                "receipt_number": receipt.get("receipt_number", ""),
                "voucher_type": receipt.get("voucher_type", ""),
                "date": receipt.get("date", ""),
                "customer_name": receipt.get("customer_name", ""),
                "customer_ledger_amount": receipt.get("customer_ledger_amount", 0) or 0,
                "payment_mode": receipt.get("payment_mode", ""),
                "bank_account": receipt.get("bank_account", ""),
                "account_current_balance": receipt.get("account_current_balance", 0) or 0,
                "amount": receipt.get("amount", 0) or 0,
                "reference_number": receipt.get("reference_number", ""),
                "against_reference": receipt.get("against_reference", ""),
                "narration": receipt.get("narration", ""),
                "invoice_allocations": json.dumps(receipt.get("invoice_allocations", []), ensure_ascii=False),
                "ledger_entries": json.dumps(receipt.get("ledger_entries", []), ensure_ascii=False),
                "cost_center_allocations": json.dumps(receipt.get("cost_center_allocations", []), ensure_ascii=False),
                "rounding_amount": receipt.get("rounding_amount", 0) or 0,
                "rounding_ledger": receipt.get("rounding_ledger", ""),
                "tally_guid": receipt.get("tally_guid", ""),
                "company_name": company_name or "",
                "created_at": _dt.now().isoformat(),
                "updated_at": _dt.now().isoformat(),
            })

        save_fn = getattr(database_manager, "bulk_save_receipts_by_guid", None) or getattr(database_manager, "bulk_save_receipts", None)
        if callable(save_fn):
            save_fn(db_data_list)
            _emit(f"Saved {len(db_data_list)} receipts to SQLite")

    total_amount = sum(float(r.get("amount", 0) or 0) for r in receipts)
    return {
        "status": "success",
        "receipts": receipts,
        "count": len(receipts),
        "total_amount": total_amount,
        "from_date": from_date,
        "to_date": to_date,
    }


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


# ----------------------------------------------------------
# ZOHO SYNC (JOB MODE) — RECEIPTS ROUTING LOGIC
# ----------------------------------------------------------

def sync_receipts_to_zoho_job(from_date="20250401", to_date="20250430", limit=None, company_name=None, *, cutoff_date="2025-03-31", opening_invoice_id="", log=None, stop_event=None):
    """
    Sync Receipts to Zoho Books using allocation-level routing rules:
    - Advance/On Account (Cr) -> Customer Advance
    - Agst Ref (Cr) -> Customer Payment applied to invoices (with prev/current year accumulation)
    - New Ref (Cr) -> if ref in Sales_Invoice_Master -> Invoice Payment else Customer Advance
    """
    _emit = _make_emitter(log)

    try:
        from modules.zoho_connector import zoho
    except Exception as e:
        return {"status": "error", "message": f"Zoho connector not available: {e}"}

    if not database_manager:
        return {"status": "error", "message": "database_manager not available"}

    try:
        database_manager.init_db()
    except Exception:
        pass

    # Build Sales_Invoice_Master from local invoices table
    sales_invoice_master = set()
    try:
        for inv in (database_manager.get_all_invoices() or []):
            no = (inv.get("invoice_number") or "").strip()
            if no:
                sales_invoice_master.add(no)
    except Exception:
        pass

    def _tally_to_iso(d: str) -> str:
        try:
            return datetime.strptime(str(d or ""), "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            return ""

    def _parse_cutoff(cut: str):
        try:
            return datetime.strptime((cut or "").strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    cutoff_dt = _parse_cutoff(cutoff_date)

    def _safe_float(x) -> float:
        try:
            return float(x or 0)
        except Exception:
            return 0.0

    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    # Cache: customers by normalized name
    customer_cache = {}

    def _load_customers():
        page = 1
        while True:
            resp = zoho.api_call("GET", "/contacts", params={"page": page, "per_page": 200, "contact_type": "customer"})
            if resp.get("code") != 0:
                break
            items = resp.get("contacts", []) or []
            for c in items:
                nm = (c.get("contact_name") or "").strip()
                cid = (c.get("contact_id") or "").strip()
                if nm and cid:
                    customer_cache[_norm(nm)] = cid
            has_more = resp.get("page_context", {}).get("has_more_page", False)
            if not has_more:
                break
            page += 1

    def _get_customer_id(customer_name: str) -> str:
        if not customer_cache:
            _emit("Loading Zoho customers...")
            _load_customers()
            _emit(f"Loaded {len(customer_cache)} customers")
        return customer_cache.get(_norm(customer_name), "")

    # Cache: bank accounts
    bank_account_cache = {}

    def _load_bank_accounts():
        resp = zoho.api_call("GET", "/bankaccounts")
        if resp.get("code") == 0:
            accounts = resp.get("bankaccounts", []) or []
            for acc in accounts:
                nm = (acc.get("account_name") or "").strip()
                aid = (acc.get("account_id") or "").strip()
                if nm and aid:
                    bank_account_cache[_norm(nm)] = aid

    def _get_account_id(bank_name: str) -> str:
        if not bank_account_cache:
            _emit("Loading Zoho bank accounts...")
            _load_bank_accounts()
            _emit(f"Loaded {len(bank_account_cache)} bank accounts")
        if not bank_name:
            # Fallback to first available if no bank name provided
            return list(bank_account_cache.values())[0] if bank_account_cache else ""
        norm_bank = _norm(bank_name)
        # 1) Exact match
        if norm_bank in bank_account_cache:
            return bank_account_cache[norm_bank]
        # 2) Partial match
        for nm, aid in bank_account_cache.items():
            if norm_bank in nm or nm in norm_bank:
                return aid
        # 3) Fallback
        return list(bank_account_cache.values())[0] if bank_account_cache else ""

    # Cache: invoice_number -> zoho invoice_id
    invoice_id_cache = {}

    def _resolve_invoice_id(invoice_number: str) -> str:
        key = (invoice_number or "").strip()
        if not key:
            return ""
        if key in invoice_id_cache:
            return invoice_id_cache[key]
        resp = zoho.api_call("GET", "/invoices", params={"search_text": key, "per_page": 200})
        if resp.get("code") == 0:
            for inv in resp.get("invoices", []) or []:
                if (inv.get("invoice_number") or "").strip() == key:
                    iid = (inv.get("invoice_id") or "").strip()
                    invoice_id_cache[key] = iid
                    return iid
        invoice_id_cache[key] = ""
        return ""

    # Read receipts from DB (active company DB is already set in session via before_request)
    receipts = database_manager.get_all_receipts() or []
    # filter by date range (YYYYMMDD strings)
    receipts = [r for r in receipts if (r.get("date") or "") >= from_date and (r.get("date") or "") <= to_date]
    if limit:
        try:
            receipts = receipts[: int(limit)]
        except Exception:
            pass

    if not receipts:
        return {"status": "error", "message": "No receipts found in DB for the selected date range"}

    stats = {"total": len(receipts), "payments_created": 0, "advances_created": 0, "skipped": 0, "failed": 0}
    errors = []

    for idx, r in enumerate(receipts, 1):
        if _should_stop(stop_event):
            _emit("Stopped by user.")
            return {"status": "stopped", "stats": stats, "errors": errors}

        receipt_no = r.get("receipt_number") or ""
        receipt_date_iso = _tally_to_iso(r.get("date") or "")
        customer_name = (r.get("customer_name") or "").strip()
        customer_id = _get_customer_id(customer_name)
        if not customer_id:
            stats["failed"] += 1
            errors.append({"receipt_number": receipt_no, "error": f"Customer not found in Zoho: {customer_name}"})
            _emit(f"[{idx}/{stats['total']}] Customer not found: {customer_name} (receipt {receipt_no})")
            continue

        try:
            allocs = r.get("invoice_allocations") or "[]"
            if isinstance(allocs, str):
                allocs = json.loads(allocs) if allocs.strip() else []
        except Exception:
            allocs = []

        if not isinstance(allocs, list):
            allocs = []

        prev_sum = 0.0
        curr_sum = 0.0
        invoice_lines = []
        advance_lines = []

        for a in allocs:
            btype = str((a or {}).get("bill_type") or (a or {}).get("billtype") or "").strip() or "Agst Ref"
            ref = str((a or {}).get("invoice_number") or (a or {}).get("invoice") or "").strip()
            amount = _safe_float((a or {}).get("amount"))
            drcr = str((a or {}).get("dr_cr") or (a or {}).get("drcr") or "").strip() or "Cr"

            if not ref:
                continue

            if drcr.lower() != "cr":
                # Spec: log error for Dr
                _emit(f"Receipt {receipt_no}: unsupported Dr line '{ref}' ({btype}) amount={amount}")
                continue

            btype_norm = btype.lower()

            # 1) Advance / On Account
            if "on account" in btype_norm or "advance" in btype_norm:
                advance_lines.append({"ref": ref, "amount": amount})
                continue

            # 2) Agst Ref
            if "agst" in btype_norm:
                # route to invoice payment
                pass
            # 3) New Ref
            elif "new ref" in btype_norm or "new" == btype_norm:
                if ref in sales_invoice_master:
                    pass  # invoice payment
                else:
                    advance_lines.append({"ref": ref, "amount": amount})
                    continue

            # Invoice Payment accumulation
            inv_row = None
            try:
                inv_row = database_manager.get_invoice_by_number(ref)
            except Exception:
                inv_row = None

            inv_date = (inv_row or {}).get("date") if isinstance(inv_row, dict) else None
            inv_dt = None
            try:
                inv_dt = datetime.strptime(str(inv_date or ""), "%Y%m%d").date()
            except Exception:
                inv_dt = None

            is_prev_year = bool(cutoff_dt and inv_dt and inv_dt <= cutoff_dt)
            if cutoff_dt and not inv_dt:
                # If invoice date missing, treat as previous year for safety (opening bucket)
                is_prev_year = True

            if is_prev_year:
                if not opening_invoice_id:
                    _emit(f"Receipt {receipt_no}: opening_invoice_id not set, cannot post previous-year allocation for ref '{ref}'")
                    continue
                prev_sum += amount
                # map to opening invoice id
                invoice_lines.append({"invoice_id": opening_invoice_id, "amount_applied": amount, "ref": ref, "bucket": "previous"})
            else:
                zoho_invoice_id = _resolve_invoice_id(ref)
                if not zoho_invoice_id:
                    _emit(f"Receipt {receipt_no}: Zoho invoice not found for '{ref}' -> routing to advance")
                    advance_lines.append({"ref": ref, "amount": amount})
                    continue
                curr_sum += amount
                invoice_lines.append({"invoice_id": zoho_invoice_id, "amount_applied": amount, "ref": ref, "bucket": "current"})

        bank_account_name = (r.get("bank_account") or "").strip()
        account_id = _get_account_id(bank_account_name)

        # Build and submit ONE customer payment (accumulated)
        total_payment = prev_sum + curr_sum
        if total_payment > 0 and invoice_lines:
            payload = {
                "customer_id": customer_id,
                "payment_mode": "banktransfer" if (r.get("payment_mode") or "").lower().startswith("bank") else "cash",
                "amount": round(total_payment, 2),
                "date": receipt_date_iso or datetime.now().strftime("%Y-%m-%d"),
                "reference_number": (r.get("reference_number") or "").strip(),
                "description": (r.get("narration") or "").strip(),
                "invoices": [{"invoice_id": x["invoice_id"], "amount_applied": round(_safe_float(x["amount_applied"]), 2)} for x in invoice_lines],
            }
            if account_id:
                payload["account_id"] = account_id

            resp = zoho.api_call("POST", "/customerpayments", payload=payload)
            if resp.get("code") == 0:
                stats["payments_created"] += 1
                _emit(f"[{idx}/{stats['total']}] Payment created: receipt {receipt_no} amount={round(total_payment,2)}")
            else:
                stats["failed"] += 1
                msg = resp.get("message") or "Zoho error"
                inv_nums = ", ".join([x.get("ref", "") for x in invoice_lines])
                errors.append({"Receipt Number": receipt_no, "Customer": customer_name, "Invoice Numbers": inv_nums, "Type": "Invoice Payment", "Error Message": msg})
                _emit(f"[{idx}/{stats['total']}] Payment failed: receipt {receipt_no} ({msg})")

        # Submit Customer Advances per line
        for adv in advance_lines:
            if _should_stop(stop_event):
                _emit("Stopped by user.")
                break
            payload = {
                "customer_id": customer_id,
                "payment_mode": "banktransfer" if (r.get("payment_mode") or "").lower().startswith("bank") else "cash",
                "amount": round(_safe_float(adv.get("amount")), 2),
                "date": receipt_date_iso or datetime.now().strftime("%Y-%m-%d"),
                "reference_number": str(adv.get("ref") or "")[:100],
                "description": (r.get("narration") or "").strip(),
            }
            if account_id:
                payload["account_id"] = account_id

            resp = zoho.api_call("POST", "/customeradvances", payload=payload)
            if resp.get("code") == 0:
                stats["advances_created"] += 1
                _emit(f"[{idx}/{stats['total']}] Advance created: receipt {receipt_no} ref='{adv.get('ref')}' amount={payload['amount']}")
            else:
                stats["failed"] += 1
                msg = resp.get("message") or "Zoho error"
                errors.append({"Receipt Number": receipt_no, "Customer": customer_name, "Invoice Numbers": adv.get("ref"), "Type": "Customer Advance", "Error Message": msg})
                _emit(f"[{idx}/{stats['total']}] Advance failed: receipt {receipt_no} ref='{adv.get('ref')}' ({msg})")

        if idx % 25 == 0 or idx == 1 or idx == stats["total"]:
            _emit(f"Progress: {idx}/{stats['total']} payments={stats['payments_created']} advances={stats['advances_created']} failed={stats['failed']}")

    if errors:
        try:
            import pandas as pd
            import os
            from datetime import datetime
            df = pd.DataFrame(errors)
            filename = f"Receipts_Received_Errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            filepath = os.path.join(os.getcwd(), filename)
            df.to_excel(filepath, index=False)
            _emit(f"Errors exported to Excel file: {filename}")
        except Exception as e:
            _emit(f"Failed to export errors to Excel: {e}")

    return {"status": "success", "stats": stats, "errors": errors}

