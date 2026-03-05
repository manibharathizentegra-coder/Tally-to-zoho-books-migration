import requests
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path

# Add parent directory to path to access shared modules
parent_dir = Path(__file__).parent.parent
import sys
sys.path.append(str(parent_dir))

from dotenv import load_dotenv
import os

try:
    import database_manager
    print("✅ Successfully imported database_manager for Contra module")
except ImportError:
    print("⚠️ Warning: Could not import database_manager.")
    database_manager = None

load_dotenv()
TALLY_URL = "http://localhost:9000"
BASE_URL = "https://www.zohoapis.com/books/v3"
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

def _date_range(from_date_str, to_date_str):
    start = datetime.strptime(from_date_str, "%Y%m%d")
    end   = datetime.strptime(to_date_str,   "%Y%m%d")
    cur   = start
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


def _fetch_day_contra(date_str, retries=2):
    import time
    xml = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Contra</VOUCHERTYPENAME>
    <SVFROMDATE>{date_str}</SVFROMDATE><SVTODATE>{date_str}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    timeouts = [45, 90]
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


def fetch_tally_contra(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    import time
    print(f"📥 Fetching Contra vouchers: {from_date} → {to_date} (day by day)...")
    contras = []

    for day in _date_range(from_date, to_date):
        vouchers = _fetch_day_contra(day)
        if vouchers:
            print(f"  ✅ {day}: {len(vouchers)} contra(s)")
        time.sleep(1)

        for v in vouchers:
            contra_date   = v.find('DATE').text.strip() if v.find('DATE') else day
            contra_number = v.find('VOUCHERNUMBER').text.strip() if v.find('VOUCHERNUMBER') else ""
            tally_guid    = v.find('GUID').text.strip() if v.find('GUID') else ""
            narration     = v.find('NARRATION').text.strip() if v.find('NARRATION') else ""

            # Explicit party if any
            explicit_party = v.find('PARTYLEDGERNAME').text.strip() if v.find('PARTYLEDGERNAME') else ""

            ledger_entries = []
            from_account_name = "" # Credit side (negative amount)
            to_account_name = ""   # Debit side (positive amount)
            amount = 0.0

            raw_entries = v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST')
            for entry in raw_entries:
                ename = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                eamt  = float(entry.find('AMOUNT').text or 0) if entry.find('AMOUNT') else 0.0
                
                ledger_entries.append({
                    "ledger_name": ename,
                    "amount": eamt
                })

                if eamt > 0:
                    # Positive amount = Debit (receiving account)
                    to_account_name = ename
                    if amount == 0:
                        amount = abs(eamt)
                elif eamt < 0:
                    # Negative amount = Credit (sending account)
                    from_account_name = ename
                    if amount == 0:
                        amount = abs(eamt)

            # ---- Cost center allocations (XML fetch) ----
            cost_center_allocations = []
            seen_cc = set()
            for entry in raw_entries:
                for ca in entry.find_all('CATEGORYALLOCATIONS.LIST'):
                    cat_name = ca.find('CATEGORY').text.strip() if ca.find('CATEGORY') else ''
                    for cc in ca.find_all('COSTCENTREALLOCATIONS.LIST'):
                        cc_name = cc.find('NAME').text.strip() if cc.find('NAME') else ''
                        try: cc_amt = float(cc.find('AMOUNT').text or 0) if cc.find('AMOUNT') else 0.0
                        except: cc_amt = 0.0
                        full = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                        if full and full not in seen_cc:
                            seen_cc.add(full)
                            cost_center_allocations.append({"category": full, "amount": abs(cc_amt)})

            contras.append({
                "date": contra_date,
                "contra_number": contra_number,
                "voucher_type": "Contra",
                "from_account": from_account_name,
                "to_account": to_account_name,
                "amount": amount,
                "narration": narration,
                "ledger_entries": ledger_entries,
                "cost_center_allocations": cost_center_allocations,
                "tally_guid": tally_guid
            })

            if limit and len(contras) >= limit:
                print(f"  Limit {limit} reached. Stopping early.")
                return contras

    print(f"✅ Fetched {len(contras)} contra vouchers from Tally")
    return contras


def parse_tally_json(json_path):
    """Parse JSON for Contra vouchers."""
    try:
        with open(json_path, 'r', encoding='utf-16') as f:
            data = json.load(f)
    except UnicodeError:
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Could not decode JSON: {e}")
            return []
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return []

    vouchers = data.get('tallymessage', [])
    if not isinstance(vouchers, list):
        if isinstance(vouchers, dict): vouchers = [vouchers]
        else: vouchers = []

    contras = []

    for v in vouchers:
        if not isinstance(v, dict): continue
        if 'vouchernumber' not in v and 'vouchertypename' not in v: continue

        contra_date = str(v.get('date', '')).strip()
        contra_number = str(v.get('vouchernumber', '')).strip()
        voucher_type = str(v.get('vouchertypename', 'Contra')).strip()
        
        # Only process Contra types just in case JSON includes multiple types
        if voucher_type.lower() != 'contra':
            continue
            
        tally_guid = str(v.get('guid', '')).strip()
        narration = str(v.get('narration', '')).strip()

        all_entries = v.get('allledgerentries', [])
        if not isinstance(all_entries, list):
            all_entries = [all_entries] if all_entries else []

        ledger_entries = []
        from_account_name = ""
        to_account_name = ""
        amount = 0.0

        for entry in all_entries:
            if not isinstance(entry, dict): continue
            ename = str(entry.get('ledgername', '')).strip()
            try: eamt = float(entry.get('amount', '0'))
            except (ValueError, TypeError): eamt = 0.0

            ledger_entries.append({"ledger_name": ename, "amount": eamt})

            # In Tally JSON, typically Debit is negative and Credit is positive (or vice versa depending on exact export configs).
            # Usually for Bank/Cash: Debit (money IN) is positive, Credit (money OUT) is negative.
            # But in the JSON `allledgerentries`, usually Tally XML is positive=Debit in Contra. In JSON, wait, let's look at `allledgerentries`.
            # Typically Debit = Negative, Credit = Positive in Tally internal signs, but let's standardise by checking Tally's `isdeemedpositive`.
            is_deemed_positive = entry.get('isdeemedpositive', False)
            
            if is_deemed_positive:
                to_account_name = ename
                if amount == 0.0: amount = abs(eamt)
            else:
                from_account_name = ename
                if amount == 0.0: amount = abs(eamt)

            # ---- Cost center allocations (JSON parse, deduplicated) ----
            cost_centers_dict = {}
            for entry in all_entries:
                if not isinstance(entry, dict): continue
                cats = entry.get('categoryallocations', [])
                if not isinstance(cats, list): cats = [cats] if cats else []
                for ca in cats:
                    if not isinstance(ca, dict): continue
                    cat_name = str(ca.get('category', '')).strip()
                    ccs = ca.get('costcentreallocations', [])
                    if not isinstance(ccs, list): ccs = [ccs] if ccs else []
                    for cc in ccs:
                        if not isinstance(cc, dict): continue
                        cc_name = str(cc.get('name', '')).strip()
                        try: cc_amt = float(cc.get('amount', '0'))
                        except (ValueError, TypeError): cc_amt = 0.0
                        full = f"{cat_name} - {cc_name}" if cat_name and cc_name else (cc_name or cat_name)
                        if full:
                            cost_centers_dict[full] = abs(cc_amt)
            cost_center_allocations = [{"category": k, "amount": v} for k, v in cost_centers_dict.items()]

            contras.append({
                "date": contra_date,
                "contra_number": contra_number,
                "voucher_type": voucher_type,
                "from_account": from_account_name,
                "to_account": to_account_name,
                "amount": amount,
                "narration": narration,
                "ledger_entries": ledger_entries,
                "cost_center_allocations": cost_center_allocations,
                "tally_guid": tally_guid
            })

    print(f"✅ Defensively parsed {len(contras)} contra vouchers from JSON")
    return contras


def get_access_token():
    from database_manager import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT access_token FROM zoho_tokens ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    if row: return row['access_token']
    return None

def get_zoho_bank_accounts(token):
    # Retrieve Chart of Accounts that are of type Bank or Cash
    url = f"{BASE_URL}/bankaccounts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            accounts = response.json().get("bankaccounts", [])
            account_map = {}
            for account in accounts:
                account_map[account["account_name"].lower()] = account["account_id"]
            return account_map
        return {}
    except Exception:
        return {}


def create_zoho_transfer(token, contra_data, bank_account_map):
    """
    Create a bank transfer in Zoho Books.
    Note: Zoho Books doesn't have a specific "Contra" endpoint, but it has /banktransactions/transfer
    or standard Journal entries. We'll use Bank Transfers if both accounts are bank/cash.
    """
    url = f"{BASE_URL}/bankaccounts/transfer"
    headers = {"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"}
    
    from_account = contra_data.get("from_account", "").lower()
    to_account = contra_data.get("to_account", "").lower()
    
    from_account_id = None
    to_account_id = None
    
    for acc_name, acc_id in bank_account_map.items():
        if from_account in acc_name or acc_name in from_account:
            from_account_id = acc_id
        if to_account in acc_name or acc_name in to_account:
            to_account_id = acc_id
            
    if not getattr(create_zoho_transfer, "warned", False) and (not from_account_id or not to_account_id):
        print(f"⚠️ Warning: Some Bank/Cash accounts in Tally not mapped correctly to Zoho: '{from_account}' -> '{to_account}'")
        create_zoho_transfer.warned = True

    if not from_account_id or not to_account_id:
        return False, f"Account Mapping Failed: {from_account} to {to_account}"

    payload = {
        "from_account_id": from_account_id,
        "to_account_id": to_account_id,
        "amount": contra_data.get("amount", 0),
        "date": f"{contra_data['date'][:4]}-{contra_data['date'][4:6]}-{contra_data['date'][6:]}",
        "reference_number": contra_data.get("contra_number", ""),
        "description": contra_data.get("narration", "")
    }

    try:
        response = requests.post(url, headers=headers, params={"organization_id": ORGANIZATION_ID}, json=payload)
        if response.status_code in [200, 201]:
            return True, response.json().get("message", "Success")
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)


def sync_contra_to_zoho(selected_contras=None, from_date="20250401", to_date="20250430", limit=None, company_name=None):
    if database_manager:
        database_manager.init_db()
        if selected_contras:
            contras = [database_manager.get_contra_by_number(c) for c in selected_contras if database_manager.get_contra_by_number(c)]
        else:
            contras = database_manager.get_all_contra()
            if not contras:
                # Fallback to Tally fetch
                contras = fetch_tally_contra(from_date, to_date, limit, company_name)
    else:
        contras = fetch_tally_contra(from_date, to_date, limit, company_name)

    if isinstance(contras, dict) and "error" in contras:
        return contras

    if not contras:
        return {"status": "error", "message": "No contra vouchers found to sync."}

    token = get_access_token()
    if not token:
        return {"status": "error", "message": "Zoho access token not found."}

    bank_account_map = get_zoho_bank_accounts(token)

    results = {"total": len(contras), "success": 0, "failed": 0, "errors": []}

    for contra in contras:
        # Convert sqlite3.Row object to dict if necessary
        if hasattr(contra, 'keys'):
            contra_data = dict(contra)
            if isinstance(contra_data.get('ledger_entries'), str):
                try: contra_data['ledger_entries'] = json.loads(contra_data['ledger_entries'])
                except: contra_data['ledger_entries'] = []
        else:
            contra_data = contra

        success, error = create_zoho_transfer(token, contra_data, bank_account_map)
        if success:
            results["success"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({
                "contra_number": contra_data.get("contra_number", ""),
                "error": error
            })

    results["status"] = "success"
    results["message"] = f"Synced {results['success']} out of {results['total']} contra vouchers"
    return results


def get_all_contra_data(from_date="20250401", to_date="20250430", limit=None, company_name=None):
    if database_manager:
        database_manager.init_db()

    contras = fetch_tally_contra(from_date, to_date, limit, company_name)

    if database_manager and contras:
        db_data_list = []
        for contra in contras:
            db_data = {
                "contra_number": contra.get("contra_number", ""),
                "voucher_type": contra.get("voucher_type", ""),
                "date": contra.get("date", ""),
                "from_account": contra.get("from_account", ""),
                "to_account": contra.get("to_account", ""),
                "amount": contra.get("amount", 0) or 0,
                "narration": contra.get("narration", ""),
                "ledger_entries": json.dumps(contra.get("ledger_entries", [])),
                "cost_center_allocations": json.dumps(contra.get("cost_center_allocations", [])),
                "tally_guid": contra.get("tally_guid", ""),
                "company_name": company_name or "",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            db_data_list.append(db_data)

        try:
            database_manager.bulk_save_contra(db_data_list)
        except AttributeError:
            print("Warning: database_manager.bulk_save_contra not found yet.")

    total_amount = sum(c.get("amount", 0) for c in contras)
    return {"contra_vouchers": contras, "total_amount": total_amount}
