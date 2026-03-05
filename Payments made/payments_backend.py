import requests
import os
import re
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup

def _date_range(from_date_str, to_date_str):
    """Yield all dates between from and to as YYYYMMDD strings."""
    start = datetime.strptime(from_date_str, "%Y%m%d")
    end   = datetime.strptime(to_date_str,   "%Y%m%d")
    cur   = start
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


def _fetch_day(date_str, retries=2):
    """Fetch Payment vouchers for a single day. Retries on timeout."""
    import time
    xml = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Payment</VOUCHERTYPENAME>
    <SVFROMDATE>{date_str}</SVFROMDATE><SVTODATE>{date_str}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    timeouts = [45, 90]   # giving Tally more time (some days take ~33s to compute)
    for attempt in range(retries):
        try:
            response = requests.post(TALLY_URL, data=xml.encode('utf-8'), timeout=timeouts[attempt])
            soup = BeautifulSoup(response.content, 'lxml-xml')
            return soup.find_all('VOUCHER')
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                print(f"  ⏱ Timeout for {date_str} (attempt {attempt+1}) — retrying in 3s...")
                time.sleep(3)
            else:
                print(f"  ⏱ Timeout for {date_str} after {retries} attempts — skipping")
                return []
        except Exception as e:
            print(f"  ❌ Error for {date_str}: {e}")
            return []
    return []


def fetch_tally_payments(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    """
    Fetch Payment vouchers from Tally day by day.
    Single-day queries are used because Tally's Voucher Register times out
    on full-month Payment queries for large datasets.
    """
    import time
    print(f"📥 Fetching payments: {from_date} → {to_date} (day by day)...")
    payments = []

    for day in _date_range(from_date, to_date):
        vouchers = _fetch_day(day)
        if vouchers:
            print(f"  ✅ {day}: {len(vouchers)} payment(s)")
        time.sleep(1)   # give Tally a 1s gap between requests

        for v in vouchers:
            payment_date   = v.find('DATE').text.strip()            if v.find('DATE')          else day
            payment_number = v.find('VOUCHERNUMBER').text.strip()   if v.find('VOUCHERNUMBER') else ""
            voucher_type   = v.find('VOUCHERTYPENAME').text.strip() if v.find('VOUCHERTYPENAME') else "Payment"
            tally_guid     = v.find('GUID').text.strip()            if v.find('GUID')           else ""
            narration      = v.find('NARRATION').text.strip()       if v.find('NARRATION')      else ""
            reference_number = v.find('REFERENCE').text.strip()    if v.find('REFERENCE')      else ""
            if not reference_number:
                reference_number = v.find('CHEQUENUMBER').text.strip() if v.find('CHEQUENUMBER') else ""

            # ---- Parse ledger entries ----
            ledger_entries = []
            bank_account   = ""
            payment_mode   = ""
            account_current_balance = 0.0
            rounding_amount = 0.0
            rounding_ledger = ""
            vendor_name     = ""
            vendor_ledger_amount = 0.0

            raw_entries = v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST')
            for entry in raw_entries:
                ename = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                eamt  = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0.0

                # Current balance
                cb_tag = entry.find('CURRENTBALANCE')
                current_balance = 0.0
                if cb_tag:
                    cb_text = cb_tag.text.strip()
                    m = re.search(r'([\d,]+\.?\d*)', cb_text)
                    if m:
                        current_balance = float(m.group(1).replace(',', ''))
                        if 'Dr' in cb_text:
                            current_balance = -current_balance

                ledger_entries.append({
                    "ledger_name": ename,
                    "amount": eamt,
                    "current_balance": current_balance
                })

                ename_lower = ename.lower()
                if 'rounding' in ename_lower:
                    rounding_amount = eamt
                    rounding_ledger = ename
                elif any(k in ename_lower for k in ['bank', 'cash', 'sbi', 'hdfc', 'icici', 'axis', 'kotak', 'idfc']):
                    bank_account = ename
                    payment_mode = "Cash" if 'cash' in ename_lower else "Bank Transfer"
                    account_current_balance = current_balance
                else:
                    # Vendor entry: credit side = negative amount
                    if eamt < 0:
                        vendor_name = ename
                        vendor_ledger_amount = abs(eamt)

            # ---- Bill allocations ----
            bill_allocations = []
            against_reference = ""
            for ba in v.find_all('BILLALLOCATIONS.LIST'):
                bname  = ba.find('NAME').text.strip()   if ba.find('NAME')    else ""
                bamt   = float(ba.find('AMOUNT').text or 0) if ba.find('AMOUNT') else 0.0
                btype  = ba.find('BILLTYPE').text.strip() if ba.find('BILLTYPE') else "Agst Ref"
                if not bname and btype == "On Account":
                    bname = "On Account"
                if bname:
                    if not against_reference:
                        against_reference = bname
                    bill_allocations.append({
                        "bill_number": bname,
                        "bill_type":   btype,
                        "amount":      abs(bamt) if bamt != 0 else vendor_ledger_amount
                    })

            # ---- Cost center allocations ----
            cost_center_allocations = []
            for ca in v.find_all('CATEGORYALLOCATIONS.LIST'):
                cat_name = ca.find('CATEGORY').text.strip() if ca.find('CATEGORY') else ""
                for cc in ca.find_all('COSTCENTREALLOCATIONS.LIST'):
                    cc_name = cc.find('NAME').text.strip()   if cc.find('NAME')   else ""
                    cc_amt  = float(cc.find('AMOUNT').text or 0) if cc.find('AMOUNT') else 0.0
                    full    = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                    if full:
                        cost_center_allocations.append({"category": full, "amount": abs(cc_amt)})

            payments.append({
                "date":                     payment_date,
                "payment_number":           payment_number,
                "voucher_type":             voucher_type,
                "vendor_name":              vendor_name,
                "vendor_ledger_amount":     vendor_ledger_amount,
                "payment_mode":             payment_mode,
                "bank_account":             bank_account,
                "account_current_balance":  account_current_balance,
                "amount":                   vendor_ledger_amount,
                "reference_number":         reference_number,
                "against_reference":        against_reference,
                "narration":                narration,
                "bill_allocations":         bill_allocations,
                "ledger_entries":           ledger_entries,
                "cost_center_allocations":  cost_center_allocations,
                "rounding_amount":          rounding_amount,
                "rounding_ledger":          rounding_ledger,
                "tally_guid":               tally_guid
            })

            if limit and len(payments) >= limit:
                print(f"  Limit {limit} reached. Stopping early.")
                return payments

    print(f"✅ Fetched {len(payments)} payments from Tally")
    return payments


def parse_tally_json(json_path):
    """
    Parse a JSON file exported from Tally containing Payment vouchers.
    Dynamically extracts all fields using robust, defensive methods (dict.get, type handling, loops).
    """
    import json
    try:
        with open(json_path, 'r', encoding='utf-16') as f:
            data = json.load(f)
    except UnicodeError:
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Could not decode JSON file: {e}")
            return []
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return []

    # Defensively get the main voucher list
    vouchers = data.get('tallymessage', [])
    if not isinstance(vouchers, list):
        if isinstance(vouchers, dict):
            vouchers = [vouchers]
        else:
            vouchers = []

    payments = []

    for v in vouchers:
        # Tally messages might randomly have nested outer tags or missing fields, so we do type checks
        if not isinstance(v, dict):
            continue
            
        # Ignore purely structural objects with no actual voucher data
        if 'vouchernumber' not in v and 'vouchertypename' not in v:
            continue
            
        # 1. Base Voucher Identifiers
        payment_date = str(v.get('date', '')).strip()
        payment_number = str(v.get('vouchernumber', '')).strip()
        voucher_type = str(v.get('vouchertypename', 'Payment')).strip()
        tally_guid = str(v.get('guid', '')).strip()
        narration = str(v.get('narration', '')).strip()

        # Many Tally versions export the main party explicitely:
        explicit_vendor_name = str(v.get('partyledgername', '')).strip()
        
        # 2. Defensively parse arrays
        all_entries = v.get('allledgerentries', [])
        if not isinstance(all_entries, list):
            all_entries = [all_entries] if all_entries else []

        # Containers for aggregated data
        reference_number = ""
        ledger_entries = []
        bank_account = ""
        payment_mode = ""
        account_current_balance = 0.0
        rounding_amount = 0.0
        rounding_ledger = ""
        vendor_name = explicit_vendor_name
        vendor_ledger_amount = 0.0
        against_reference = ""
        bill_allocations = []
        # Use dict instead of list for deduplication using full_name as key
        cost_centers_dict = {}

        # 3. Process Ledger Entries dynamically
        for entry in all_entries:
            if not isinstance(entry, dict):
                continue
                
            ename = str(entry.get('ledgername', '')).strip()
            
            # Robust float conversion
            try: eamt = float(entry.get('amount', '0'))
            except (ValueError, TypeError): eamt = 0.0

            # Usually current balance is missing in JSON exports, default to 0
            current_balance = 0.0
            ledger_entries.append({
                "ledger_name": ename,
                "amount": eamt,
                "current_balance": current_balance
            })

            ename_lower = ename.lower()
            if 'rounding' in ename_lower:
                rounding_amount = eamt
                rounding_ledger = ename
                
            elif any(k in ename_lower for k in ['bank', 'cash', 'sbi', 'hdfc', 'icici', 'axis', 'kotak', 'idfc']):
                bank_account = ename
                payment_mode = "Cash" if 'cash' in ename_lower else "Bank Transfer"
                account_current_balance = current_balance
                
                # Dynamic array check for bank allocations
                bank_allocs = entry.get('bankallocations', [])
                if not isinstance(bank_allocs, list):
                    bank_allocs = [bank_allocs] if bank_allocs else []
                    
                if bank_allocs and isinstance(bank_allocs[0], dict):
                    alloc = bank_allocs[0]
                    reference_number = str(alloc.get('uniquereferencenumber', '')).strip()
                    if not reference_number:
                        reference_number = str(alloc.get('instrumentnumber', '')).strip()
            else:
                # Fallback mapping: If explicit_vendor_name was empty, grab the biggest non-bank ledger
                if not vendor_name:
                    if abs(eamt) > vendor_ledger_amount:
                        vendor_name = ename
                        vendor_ledger_amount = abs(eamt)
                
                # Grab the amount specifically for the vendor ledger
                if ename == vendor_name:
                    vendor_ledger_amount = abs(eamt)
            
            # 4. Bill Allocations parsing (defensive)
            bills = entry.get('billallocations', [])
            if not isinstance(bills, list):
                bills = [bills] if bills else []
                
            for ba in bills:
                if not isinstance(ba, dict): continue
                
                bname = str(ba.get('name', '')).strip()
                try: bamt = float(ba.get('amount', '0'))
                except (ValueError, TypeError): bamt = 0.0
                btype = str(ba.get('billtype', 'Agst Ref')).strip()
                
                if not bname and btype == "On Account":
                    bname = "On Account"
                
                if bname:
                    if not against_reference:
                        against_reference = bname
                    # Only append if we haven't already for this exact bill to prevent double-entry duplicates
                    exists = any(b['bill_number'] == bname for b in bill_allocations)
                    if not exists:
                        bill_allocations.append({
                            "bill_number": bname,
                            "bill_type": btype,
                            "amount": abs(bamt)
                        })
                    
            # 5. Cost center allocations parsing (defensive array/dict checks)
            cats = entry.get('categoryallocations', [])
            if not isinstance(cats, list):
                cats = [cats] if cats else []
                
            for ca in cats:
                if not isinstance(ca, dict): continue
                
                cat_name = str(ca.get('category', '')).strip()
                ccs = ca.get('costcentreallocations', [])
                if not isinstance(ccs, list):
                    ccs = [ccs] if ccs else []
                    
                for cc in ccs:
                    if not isinstance(cc, dict): continue
                    
                    cc_name = str(cc.get('name', '')).strip()
                    try: cc_amt = float(cc.get('amount', '0'))
                    except (ValueError, TypeError): cc_amt = 0.0
                    
                    full_name = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                    if full_name:
                        # Deduplicate by using dictionary
                        cost_centers_dict[full_name] = abs(cc_amt)

        # Convert back to list
        cost_center_allocations = [{"category": k, "amount": v} for k, v in cost_centers_dict.items()]

        # 6. Final Data Assembly with Fallbacks
        payments.append({
            "date": payment_date,
            "payment_number": payment_number,
            "voucher_type": voucher_type,
            "vendor_name": vendor_name or "Unknown Vendor",
            "vendor_ledger_amount": vendor_ledger_amount,
            "payment_mode": payment_mode or "Other",
            "bank_account": bank_account,
            "account_current_balance": account_current_balance,
            "amount": vendor_ledger_amount,
            "reference_number": reference_number,
            "against_reference": against_reference,
            "narration": narration,
            "bill_allocations": bill_allocations,
            "ledger_entries": ledger_entries,
            "cost_center_allocations": cost_center_allocations,
            "rounding_amount": rounding_amount,
            "rounding_ledger": rounding_ledger,
            "tally_guid": tally_guid
        })

    print(f"✅ Defensively parsed {len(payments)} payments from JSON")
    return payments





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
BASE_URL = "https://www.zohoapis.com/books/v3"
TALLY_URL = "http://localhost:9000"

# ----------------------------------------------------------
# ZOHO BOOKS INTEGRATION
# ----------------------------------------------------------

def get_access_token():
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
        return None
    except Exception:
        return None

def get_zoho_vendors(token):
    """Fetch all vendors from Zoho Books"""
    url = f"{BASE_URL}/contacts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID, "contact_type": "vendor"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            contacts = response.json().get("contacts", [])
            vendor_map = {}
            for contact in contacts:
                vendor_map[contact["contact_name"]] = {
                    "vendor_id": contact["contact_id"],
                    "email": contact.get("email", "")
                }
            return vendor_map
        return {}
    except Exception:
        return {}

def get_zoho_bills(token, vendor_id=None):
    """Fetch bills from Zoho Books for a specific vendor"""
    url = f"{BASE_URL}/bills"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    if vendor_id:
        params["vendor_id"] = vendor_id
        
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            bills = response.json().get("bills", [])
            bill_map = {}
            for bill in bills:
                # Map using bill_number
                bill_map[bill["bill_number"]] = {
                    "bill_id": bill["bill_id"],
                    "balance": float(bill.get("balance", 0)),
                    "total": float(bill.get("total", 0))
                }
            return bill_map
        return {}
    except Exception:
        return {}

def get_zoho_bank_accounts(token):
    url = f"{BASE_URL}/bankaccounts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            accounts = response.json().get("bankaccounts", [])
            account_map = {}
            for account in accounts:
                account_map[account["account_name"]] = account["account_id"]
            return account_map
        return {}
    except Exception:
        return {}

def create_zoho_payment_made(token, payment_data, vendor_map, bill_map, bank_account_map):
    """Create a payment made (vendor payment) in Zoho Books"""
    vendor_name = payment_data.get("vendor_name", "")
    if vendor_name not in vendor_map:
        return False, f"Vendor '{vendor_name}' not found in Zoho Books"
    
    vendor_id = vendor_map[vendor_name]["vendor_id"]
    
    bank_account_name = payment_data.get("bank_account", "")
    account_id = None
    for acc_name, acc_id in bank_account_map.items():
        if bank_account_name.lower() in acc_name.lower() or acc_name.lower() in bank_account_name.lower():
            account_id = acc_id
            break
            
    if not account_id:
        if bank_account_map:
            account_id = list(bank_account_map.values())[0]
        else:
            return False, "No bank accounts found in Zoho Books"
            
    date_str = payment_data.get("date", "")
    if len(date_str) == 8:
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    else:
        formatted_date = datetime.now().strftime("%Y-%m-%d")
        
    payload = {
        "vendor_id": vendor_id,
        "payment_mode": payment_data.get("payment_mode", "cash"),
        "amount": payment_data.get("amount", 0),
        "date": formatted_date,
        "reference_number": payment_data.get("reference_number", ""),
        "description": payment_data.get("narration", ""),
        "account_id": account_id,
        "bills": []
    }
    
    for allocation in payment_data.get("bill_allocations", []):
        bill_number = allocation.get("bill_number", "")
        if bill_number in bill_map:
            payload["bills"].append({
                "bill_id": bill_map[bill_number]["bill_id"],
                "amount_applied": allocation.get("amount", 0)
            })
            
    url = f"{BASE_URL}/vendorpayments"
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/json"
    }
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        response = requests.post(url, headers=headers, params=params, json={"JSONString": json.dumps(payload)})
        if response.status_code in [200, 201]:
            print(f"✅ Created payment made for {vendor_name}: ₹{payment_data.get('amount', 0)}")
            return True, None
        else:
            error_msg = response.json().get("message", response.text)
            print(f"❌ Failed to create payment: {error_msg}")
            return False, error_msg
    except Exception as e:
        return False, str(e)

def sync_payments_to_zoho(selected_payments=None, from_date="20250401", to_date="20250430", limit=None, company_name=None):
    token = get_access_token()
    if not token:
        return {"status": "error", "message": "Failed to get Zoho access token"}
        
    if selected_payments is None:
        payments = fetch_tally_payments(from_date, to_date, limit, company_name)
    else:
        payments = selected_payments
        
    if not payments:
        return {"status": "error", "message": "No payments to sync"}
        
    print("📥 Fetching Zoho Books data...")
    vendor_map = get_zoho_vendors(token)
    bank_account_map = get_zoho_bank_accounts(token)
    
    results = {"total": len(payments), "success": 0, "failed": 0, "errors": []}
    
    for payment in payments:
        vendor_name = payment.get("vendor_name", "")
        vendor_id = vendor_map.get(vendor_name, {}).get("vendor_id")
        
        bill_map = {}
        if vendor_id:
            bill_map = get_zoho_bills(token, vendor_id)
            
        success, error = create_zoho_payment_made(token, payment, vendor_map, bill_map, bank_account_map)
        if success:
            results["success"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({
                "payment_number": payment.get("payment_number", ""),
                "vendor": vendor_name,
                "error": error
            })
            
    results["status"] = "success"
    results["message"] = f"Synced {results['success']} out of {results['total']} payments"
    return results

def get_all_payments_data(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    if database_manager:
        database_manager.init_db()
        
    payments = fetch_tally_payments(from_date, to_date, limit, company_name)
    
    if database_manager and payments:
        db_data_list = []
        for payment in payments:
            db_data = {
                "payment_number": payment.get("payment_number", ""),
                "voucher_type": payment.get("voucher_type", ""),
                "date": payment.get("date", ""),
                "vendor_name": payment.get("vendor_name", ""),
                "vendor_ledger_amount": payment.get("vendor_ledger_amount", 0) or 0,
                "payment_mode": payment.get("payment_mode", ""),
                "bank_account": payment.get("bank_account", ""),
                "account_current_balance": payment.get("account_current_balance", 0) or 0,
                "amount": payment.get("amount", 0) or 0,
                "reference_number": payment.get("reference_number", ""),
                "against_reference": payment.get("against_reference", ""),
                "narration": payment.get("narration", ""),
                "bill_allocations": json.dumps(payment.get("bill_allocations", [])),
                "ledger_entries": json.dumps(payment.get("ledger_entries", [])),
                "cost_center_allocations": json.dumps(payment.get("cost_center_allocations", [])),
                "rounding_amount": payment.get("rounding_amount", 0) or 0,
                "rounding_ledger": payment.get("rounding_ledger", ""),
                "tally_guid": payment.get("tally_guid", ""),
                "company_name": company_name or "",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            db_data_list.append(db_data)
            
        try:
            database_manager.bulk_save_payments_made(db_data_list)
        except AttributeError:
            # We'll need to define this function in database_manager.py
            print("Warning: database_manager.bulk_save_payments_made not found yet. Needs to be created.")
            pass
            
    total_amount = sum(p.get("amount", 0) for p in payments)
    return {"payments": payments, "total_amount": total_amount}

if __name__ == "__main__":
    import json
    data = get_all_payments_data(limit=5)
    print(json.dumps(data, indent=2))
