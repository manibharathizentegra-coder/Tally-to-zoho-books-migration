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
    print("⚠️ Warning: Could not import database_manager. SQLite sync will be skipped.")
    database_manager = None

# Load environment variables
load_dotenv()

# Zoho API credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

# URLs
BASE_URL = "https://www.zohoapis.in/books/v3"
TALLY_URL = "http://localhost:9000"

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
                print(f"\n🏢 DEBUG Cost Centers for Receipt #{receipt_number}:")
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
                print(f"\n🔍 DEBUG Receipt #{receipt_number}:")
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
                        print(f"   ✅ Added to invoice_allocations: {invoice_name} - {final_amount}")
            
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
        
        print(f"✅ Fetched {len(receipts)} receipts from Tally")
        
        return receipts
        
    except Exception as e:
        print(f"❌ Error fetching receipts from Tally: {e}")
        import traceback
        traceback.print_exc()
        return []

# ----------------------------------------------------------
# ZOHO BOOKS INTEGRATION
# ----------------------------------------------------------

def get_access_token():
    """Get Zoho access token using refresh token"""
    url = "https://accounts.zoho.in/oauth/v2/token"
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
            print(f"❌ Failed to get access token: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error getting access token: {e}")
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
            print(f"❌ Failed to fetch customers: {response.text}")
            return {}
    except Exception as e:
        print(f"❌ Error fetching customers: {e}")
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
            print(f"❌ Failed to fetch invoices: {response.text}")
            return {}
    except Exception as e:
        print(f"❌ Error fetching invoices: {e}")
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
            print(f"❌ Failed to fetch bank accounts: {response.text}")
            return {}
    except Exception as e:
        print(f"❌ Error fetching bank accounts: {e}")
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
            print(f"✅ Created payment received for {customer_name}: ₹{receipt_data.get('amount', 0)}")
            return True, None
        else:
            error_msg = response.json().get("message", response.text)
            print(f"❌ Failed to create payment: {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error creating payment: {error_msg}")
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
    print("📥 Fetching Zoho Books data...")
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
        print(f"💾 Saved {len(receipts)} receipts to database")
    
    # Calculate totals
    total_amount = sum(r.get("amount", 0) for r in receipts)
    
    return {
        "receipts": receipts,
        "count": len(receipts),
        "total_amount": total_amount,
        "from_date": from_date,
        "to_date": to_date
    }
