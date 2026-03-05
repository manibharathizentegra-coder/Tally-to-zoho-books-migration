
import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from collections import defaultdict

# Load credentials
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")   # ← static fallback token from .env

# URLs
BASE_URL = "https://www.zohoapis.in/books/v3"
TALLY_URL = "http://localhost:9000"

def get_access_token():
    """
    Get Zoho Books access token.
    Strategy:
      1. Try to get a fresh token via the Refresh Token flow (best).
      2. If refresh fails (wrong secret / expired), fall back to the
         static ACCESS_TOKEN stored in .env.
    """
    # ------------------------------------------------------------------
    # Step 1 — Try Refresh Token
    # ------------------------------------------------------------------
    if REFRESH_TOKEN and CLIENT_ID and CLIENT_SECRET:
        print(f"\n🔑 Requesting Zoho access token via refresh_token...")
        print(f"   CLIENT_ID     : {CLIENT_ID}")
        print(f"   REFRESH_TOKEN : {REFRESH_TOKEN[:20]}...")

        try:
            payload = {
                "refresh_token": REFRESH_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token"
            }
            res = requests.post(
                "https://accounts.zoho.in/oauth/v2/token",
                data=payload, timeout=15
            )
            data = res.json()
            token = data.get("access_token")

            if token:
                print(f"   ✅ Fresh access token obtained via refresh_token")
                return token
            else:
                error = data.get("error", "unknown_error")
                print(f"   ⚠️  Refresh token failed → {error}: {data.get('error_description','')}")
                print(f"   ↪️  Falling back to ACCESS_TOKEN from .env ...")
        except Exception as e:
            print(f"   ⚠️  Network error during refresh: {e}")
            print(f"   ↪️  Falling back to ACCESS_TOKEN from .env ...")
    else:
        print(f"   ⚠️  Refresh credentials missing — using ACCESS_TOKEN from .env directly ...")

    # ------------------------------------------------------------------
    # Step 2 — Fall back to static ACCESS_TOKEN in .env
    # ------------------------------------------------------------------
    if ACCESS_TOKEN:
        print(f"   ✅ Using static ACCESS_TOKEN from .env: {ACCESS_TOKEN[:20]}...")
        return ACCESS_TOKEN

    print(f"   ❌ No valid token available! Please set ACCESS_TOKEN or fix REFRESH_TOKEN in .env")
    return None

# ----------------------------------------------------------
# SQLITE CACHING FOR PERFORMANCE
# ----------------------------------------------------------

import sqlite3
from pathlib import Path

# Database file location
DB_FILE = Path(__file__).parent / "tally_cache.db"

def init_cache_db():
    """Initialize SQLite cache database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Tally data tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            name TEXT PRIMARY KEY,
            parent TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledgers (
            name TEXT PRIMARY KEY,
            ledger_type TEXT
        )
    ''')
    
    # Zoho Books data tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zoho_contacts (
            contact_id TEXT PRIMARY KEY,
            contact_name TEXT,
            contact_name_lower TEXT,
            contact_type TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zoho_accounts (
            account_id TEXT PRIMARY KEY,
            account_name TEXT,
            account_name_lower TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_contact_name_lower ON zoho_contacts(contact_name_lower)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_name_lower ON zoho_accounts(account_name_lower)')
    
    conn.commit()
    conn.close()

def get_ledger_map_from_cache():
    """Get ledger map from SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, ledger_type FROM ledgers")
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            ledger_map = {name: ledger_type for name, ledger_type in rows}
            print(f"   ✅ Loaded {len(ledger_map)} ledgers from cache")
            return ledger_map
        return None
    except:
        return None

def save_ledger_map_to_cache(ledger_map, groups_dict):
    """Save ledger map and groups to SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM ledgers")
        cursor.execute("DELETE FROM groups")
        
        # Save groups
        for group_name, parent in groups_dict.items():
            cursor.execute("INSERT OR REPLACE INTO groups (name, parent) VALUES (?, ?)", 
                         (group_name, parent))
        
        # Save ledgers
        for ledger_name, ledger_type in ledger_map.items():
            cursor.execute("INSERT OR REPLACE INTO ledgers (name, ledger_type) VALUES (?, ?)", 
                         (ledger_name, ledger_type))
        
        # Update metadata
        cursor.execute("INSERT OR REPLACE INTO cache_metadata (key, value) VALUES (?, ?)",
                     ("last_updated", datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        print(f"   💾 Cached {len(ledger_map)} ledgers and {len(groups_dict)} groups to database")
    except Exception as e:
        print(f"   ⚠️  Failed to cache data: {e}")

# ----------------------------------------------------------
# ZOHO BOOKS CACHING
# ----------------------------------------------------------

def save_zoho_contacts_to_cache(contact_map):
    """Save Zoho contacts to SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Clear existing contacts
        cursor.execute("DELETE FROM zoho_contacts")
        
        # Save contacts
        for contact_name_lower, contact_info in contact_map.items():
            cursor.execute("""
                INSERT OR REPLACE INTO zoho_contacts 
                (contact_id, contact_name, contact_name_lower, contact_type) 
                VALUES (?, ?, ?, ?)
            """, (
                contact_info["contact_id"],
                contact_info["original_name"],
                contact_name_lower,
                contact_info["contact_type"]
            ))
        
        # Update metadata
        cursor.execute("INSERT OR REPLACE INTO cache_metadata (key, value) VALUES (?, ?)",
                     ("zoho_contacts_updated", datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        print(f"   💾 Cached {len(contact_map)} Zoho contacts to database")
    except Exception as e:
        print(f"   ⚠️  Failed to cache Zoho contacts: {e}")

def get_zoho_contacts_from_cache():
    """Get Zoho contacts from SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT contact_id, contact_name, contact_name_lower, contact_type FROM zoho_contacts")
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            contact_map = {}
            for contact_id, contact_name, contact_name_lower, contact_type in rows:
                contact_map[contact_name_lower] = {
                    "contact_id": contact_id,
                    "original_name": contact_name,
                    "contact_type": contact_type
                }
            print(f"   ✅ Loaded {len(contact_map)} Zoho contacts from cache")
            return contact_map
        return None
    except:
        return None

def save_zoho_accounts_to_cache(account_map):
    """Save Zoho accounts to SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Clear existing accounts
        cursor.execute("DELETE FROM zoho_accounts")
        
        # Save accounts (account_map is {name_lower: account_id})
        for account_name_lower, account_id in account_map.items():
            cursor.execute("""
                INSERT OR REPLACE INTO zoho_accounts 
                (account_id, account_name_lower) 
                VALUES (?, ?)
            """, (account_id, account_name_lower))
        
        # Update metadata
        cursor.execute("INSERT OR REPLACE INTO cache_metadata (key, value) VALUES (?, ?)",
                     ("zoho_accounts_updated", datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        print(f"   💾 Cached {len(account_map)} Zoho accounts to database")
    except Exception as e:
        print(f"   ⚠️  Failed to cache Zoho accounts: {e}")

def get_zoho_accounts_from_cache():
    """Get Zoho accounts from SQLite cache"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT account_id, account_name_lower FROM zoho_accounts")
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            account_map = {account_name_lower: account_id for account_id, account_name_lower in rows}
            print(f"   ✅ Loaded {len(account_map)} Zoho accounts from cache")
            return account_map
        return None
    except:
        return None


def get_ledger_map_from_tally(use_cache=True, force_refresh=False):
    """
    FULLY DYNAMIC: Builds ledger map by analyzing Tally's group hierarchy.
    Now with SQLite caching for performance!
    
    Args:
        use_cache: If True, try to load from cache first
        force_refresh: If True, ignore cache and fetch fresh from Tally
    """
    # Initialize database
    init_cache_db()
    
    # Try cache first (unless force refresh)
    if use_cache and not force_refresh:
        cached_map = get_ledger_map_from_cache()
        if cached_map:
            return cached_map
    
    print("\n🔍 Building DYNAMIC ledger map from Tally...")
    
    # Step 1: Fetch all Groups to build the hierarchy
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
            if name:
                children_map[parent].append(name)
        print(f"   ✅ Found {len(children_map)} group relationships")
    except Exception as e:
        print(f"   ⚠️  Could not fetch Tally groups: {e}")
        return {}

    # Step 2: Recursive function to find ALL descendants of a group
    def get_all_descendants(group_name, visited=None):
        if visited is None:
            visited = set()
        if group_name in visited:
            return set()
        visited.add(group_name)
        results = {group_name}
        for child in children_map.get(group_name, []):
            results.update(get_all_descendants(child, visited))
        return results

    # Step 3: Identify all vendor and customer groups
    vendor_groups = get_all_descendants("Sundry Creditors")
    customer_groups = get_all_descendants("Sundry Debtors")
    
    print(f"   📊 Vendor groups (under Sundry Creditors): {len(vendor_groups)}")
    print(f"   📊 Customer groups (under Sundry Debtors): {len(customer_groups)}")
    
    # Show some examples
    if vendor_groups:
        examples = list(vendor_groups)[:5]
        print(f"      Vendor examples: {', '.join(examples)}")
    if customer_groups:
        examples = list(customer_groups)[:5]
        print(f"      Customer examples: {', '.join(examples)}")

    # Step 4: Fetch ALL Ledgers using the correct XML format
    # Use the SAME format as Tally_journel.py which works
    ledger_xml = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Accounts</REPORTNAME>
    <STATICVARIABLES><ACCOUNTTYPE>Ledgers</ACCOUNTTYPE><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
    </REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""
    
    ledger_map = {}
    try:
        print(f"   🔄 Fetching ledgers from Tally (this may take a minute)...")
        res = requests.post(TALLY_URL, data=ledger_xml, timeout=60)  # Increased timeout to 60s
        soup = BeautifulSoup(res.content, 'lxml-xml')
        
        # Debug: Check what we got
        all_ledgers = soup.find_all('LEDGER')
        print(f"   🔍 Raw ledger count from Tally: {len(all_ledgers)}")
        
        vendor_count = 0
        customer_count = 0
        account_count = 0
        
        for ledger in all_ledgers:
            name = ledger.get('NAME', '').strip()
            if not name:
                continue
                
            parent = ledger.find('PARENT')
            parent_name = parent.text.strip() if parent and parent.text else ""
            
            # Debug first few ledgers
            if len(ledger_map) < 5:
                print(f"      Debug: Ledger '{name}' -> Parent '{parent_name}'")
            
            if parent_name in vendor_groups:
                ledger_map[name] = "vendor"
                vendor_count += 1
            elif parent_name in customer_groups:
                ledger_map[name] = "customer"
                customer_count += 1
            else:
                ledger_map[name] = "account"
                account_count += 1
        
        print(f"   ✅ Classified {len(ledger_map)} ledgers:")
        print(f"      - Vendors: {vendor_count}")
        print(f"      - Customers: {customer_count}")
        print(f"      - Accounts: {account_count}")
        
        # Show some vendor/customer examples
        if vendor_count > 0:
            vendor_examples = [name for name, type in list(ledger_map.items())[:20] if type == "vendor"][:3]
            if vendor_examples:
                print(f"      Vendor ledger examples: {', '.join(vendor_examples)}")
        
        if customer_count > 0:
            customer_examples = [name for name, type in list(ledger_map.items())[:20] if type == "customer"][:3]
            if customer_examples:
                print(f"      Customer ledger examples: {', '.join(customer_examples)}")
        
    except requests.exceptions.Timeout:
        print(f"   ⚠️  Tally ledger fetch timed out after 60s")
        print(f"   💡 Your Tally database may have many ledgers. Continuing with empty map...")
    except Exception as e:
        print(f"   ⚠️  Could not fetch Tally ledgers: {e}")
        import traceback
        traceback.print_exc()
    
    # Save to cache for next time
    if ledger_map:
        # Build groups dict for caching
        groups_dict = {}
        for parent, children in children_map.items():
            for child in children:
                groups_dict[child] = parent
        save_ledger_map_to_cache(ledger_map, groups_dict)
    
    return ledger_map

def fetch_tally_journals(from_date="20250401", to_date="20250430", limit=None):
    """Fetch journal vouchers from Tally with DYNAMIC ledger classification"""
    ledger_map = get_ledger_map_from_tally()
    
    def get_ledger_type_fuzzy(ledger_name):
        """
        Get ledger type with fuzzy matching to handle name variations.
        Tally sometimes returns different names in journals vs ledger list.
        E.g., 'MATAJI ELECTRICAL & LIGHT HOUSE' becomes 'MATAJI ELECTRICAL  LIGHT HOUSE'
        """
        # Try exact match first
        if ledger_name in ledger_map:
            return ledger_map[ledger_name]
        
        # Normalize the name for fuzzy matching
        # Remove '&', replace multiple spaces with single space
        def normalize_name(name):
            return ' '.join(name.replace('&', '').split())
        
        normalized_search = normalize_name(ledger_name).lower()
        
        # Try fuzzy match
        for map_name, ledger_type in ledger_map.items():
            normalized_map = normalize_name(map_name).lower()
            if normalized_search == normalized_map:
                # Found a match!
                if ledger_type != "account":
                    print(f"      🔍 Fuzzy matched '{ledger_name}' -> '{map_name}' ({ledger_type})")
                return ledger_type
        
        # Default to account
        return "account"
    
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Journal</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        response = requests.post(TALLY_URL, data=xml_request, timeout=90)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        vouchers = soup.find_all('VOUCHER')
        if limit:
            vouchers = vouchers[:limit]
        
        journal_data = []
        
        for v in vouchers:
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            line_items = []
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip()
                
                # FIX: Handle currency conversion strings
                amt_text = entry.find('AMOUNT').text if entry.find('AMOUNT') else "0"
                try:
                    # Try direct conversion first
                    amt = float(amt_text)
                except ValueError:
                    # Handle currency conversion format: '-$1116.86 @ ? 88.1409/$ = -? 98441.05'
                    # Extract the final amount after '='
                    import re
                    if '=' in amt_text:
                        # Get the amount after '='
                        final_part = amt_text.split('=')[-1].strip()
                        # Extract number (remove currency symbols)
                        match = re.search(r'-?\d+\.?\d*', final_part.replace('?', '').replace(',', ''))
                        amt = float(match.group()) if match else 0.0
                    else:
                        # Just extract first number found
                        match = re.search(r'-?\d+\.?\d*', amt_text.replace(',', ''))
                        amt = float(match.group()) if match else 0.0
                
                # Use fuzzy matching to get ledger type
                l_type = get_ledger_type_fuzzy(name)
                
                # Get reporting tags
                tag_category = ""
                tag_option = ""
                cat_alloc = entry.find('CATEGORYALLOCATIONS.LIST')
                if cat_alloc:
                    tag_category = cat_alloc.find('CATEGORY').text if cat_alloc.find('CATEGORY') else ""
                    cc_list = cat_alloc.find('COSTCENTREALLOCATIONS.LIST')
                    if cc_list:
                        tag_option = cc_list.find('NAME').text if cc_list.find('NAME') else ""
                
                line_items.append({
                    "ledger_name": name,
                    "ledger_type": l_type,
                    "amount": abs(amt),
                    "debit_or_credit": "debit" if amt < 0 else "credit",
                    "tag_category": tag_category,
                    "tag_option": tag_option
                })
            
            journal_data.append({
                "date": v_date,
                "journal_number": v_no,
                "narration": narration,
                "line_items": line_items
            })
        
        return journal_data
    
    except Exception as e:
        print(f"❌ Error fetching Tally journals: {e}")
        return []

def find_tag_ids_by_name(token, target_tag_name, target_option_name):
    """Find reporting tag IDs by name"""
    if not target_tag_name or not target_option_name:
        return None, None
        
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    try:
        res = requests.get(f"{BASE_URL}/settings/tags", headers=headers, params=params)
        categories = res.json().get("reporting_tags", [])

        tag_id = next((cat.get("tag_id") for cat in categories if cat.get("tag_name", "").strip().lower() == target_tag_name.lower()), None)
        if not tag_id:
            return None, None

        detail_res = requests.get(f"{BASE_URL}/settings/tags/{tag_id}", headers=headers, params=params)
        detail_data = detail_res.json()
        tag_obj = detail_data.get("tag", detail_data.get("reporting_tag", {}))
        options = tag_obj.get("tag_options", [])

        tag_option_id = next((opt.get("tag_option_id") for opt in options if opt.get("tag_option_name", "").strip().lower() == target_option_name.lower()), None)
        return tag_id, tag_option_id
    except:
        return None, None

def get_zoho_accounts(token, use_cache=True, force_refresh=False):
    """
    Fetch all Zoho Books chart of accounts with caching
    
    Args:
        token: Zoho OAuth token
        use_cache: If True, try to load from cache first
        force_refresh: If True, ignore cache and fetch fresh from Zoho
    """
    # Try cache first (unless force refresh)
    if use_cache and not force_refresh:
        cached_accounts = get_zoho_accounts_from_cache()
        if cached_accounts:
            return cached_accounts
    
    print("   🔄 Fetching accounts from Zoho Books...")
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    res = requests.get(f"{BASE_URL}/chartofaccounts", headers=headers, params=params)
    accounts = res.json().get("chartofaccounts", [])
    
    account_map = {}
    for acc in accounts:
        account_map[acc["account_name"].lower().strip()] = acc["account_id"]
    
    print(f"   ✅ Fetched {len(account_map)} accounts")
    
    # Save to cache
    save_zoho_accounts_to_cache(account_map)
    
    return account_map

def get_zoho_contacts(token, use_cache=True, force_refresh=False):
    """
    Fetch all Zoho Books contacts with pagination and caching
    
    Args:
        token: Zoho OAuth token
        use_cache: If True, try to load from cache first
        force_refresh: If True, ignore cache and fetch fresh from Zoho
    """
    # Try cache first (unless force refresh)
    if use_cache and not force_refresh:
        cached_contacts = get_zoho_contacts_from_cache()
        if cached_contacts:
            return cached_contacts
    
    print("   🔄 Fetching contacts from Zoho Books...")
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID, "per_page": 200}
    
    contact_map = {}
    page = 1
    total_contacts = 0
    
    while True:
        params["page"] = page
        res = requests.get(f"{BASE_URL}/contacts", headers=headers, params=params)
        data = res.json()
        
        contacts = data.get("contacts", [])
        if not contacts:
            break
        
        for contact in contacts:
            contact_name = contact["contact_name"].lower().strip()
            contact_map[contact_name] = {
                "contact_id": contact["contact_id"],
                "contact_type": contact["contact_type"],
                "original_name": contact["contact_name"]
            }
            total_contacts += 1
        
        page_context = data.get("page_context", {})
        if not page_context.get("has_more_page", False):
            break
        
        page += 1
    
    print(f"   ✅ Fetched {total_contacts} contacts across {page} page(s)")
    
    # Save to cache
    save_zoho_contacts_to_cache(contact_map)
    
    return contact_map

def create_contact_in_zoho(token, contact_name, contact_type):
    """
    AUTOMATIC CONTACT CREATION
    Creates a new contact (vendor or customer) in Zoho Books
    """
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    payload = {
        "contact_name": contact_name,
        "contact_type": contact_type,  # "vendor" or "customer"
        "company_name": contact_name
    }
    
    try:
        res = requests.post(f"{BASE_URL}/contacts", headers=headers, params=params, json=payload)
        data = res.json()
        if res.status_code in [200, 201] and data.get("code") == 0:
            contact_data = data.get("contact", {})
            contact_id = contact_data.get("contact_id")
            print(f"     ✨ Created new {contact_type}: {contact_name} (ID: {contact_id})")
            return {
                "contact_id": contact_id,
                "contact_type": contact_type,
                "original_name": contact_name
            }
        elif data.get("code") == 3062:
            # Contact already exists in Zoho — search and fetch it
            print(f"     🔍 Already exists in Zoho, searching for '{contact_name}'...")
            search_res = requests.get(
                f"{BASE_URL}/contacts",
                headers=headers,
                params={"organization_id": ORGANIZATION_ID, "contact_name": contact_name}
            )
            contacts = search_res.json().get("contacts", [])
            if contacts:
                c = contacts[0]
                print(f"     ✅ Found existing contact: {c['contact_name']} (ID: {c['contact_id']})")
                return {
                    "contact_id": c["contact_id"],
                    "contact_type": c.get("contact_type", contact_type),
                    "original_name": c["contact_name"]
                }
            print(f"     ⚠️  Could not find existing contact in search")
            return None
        else:
            print(f"     ⚠️  Failed to create contact: {data}")
            return None
    except Exception as e:
        print(f"     ⚠️  Error creating contact: {e}")
        return None

def find_or_create_contact(token, contact_map, contact_name, contact_type):
    """
    Find contact in Zoho Books, or create it if it doesn't exist
    """
    # Try exact match first
    contact_lower = contact_name.lower().strip()
    if contact_lower in contact_map:
        return contact_map[contact_lower]
    
    # Try fuzzy match
    for existing_name, contact_info in contact_map.items():
        if contact_lower in existing_name or existing_name in contact_lower:
            # Handle both contact structures (original_name or contact_name)
            display_name = contact_info.get('original_name') or contact_info.get('contact_name', existing_name)
            print(f"     🔍 Fuzzy matched '{contact_name}' to '{display_name}'")
            return contact_info
    
    # Contact doesn't exist - create it!
    print(f"     🆕 Contact '{contact_name}' not found - creating new {contact_type}...")
    new_contact = create_contact_in_zoho(token, contact_name, contact_type)
    
    if new_contact:
        # Add to cache
        contact_map[contact_lower] = new_contact
        return new_contact
    
    return None

def create_zoho_journal(token, journal_data, account_map, contact_map):
    """Create a journal entry in Zoho Books with FULL AUTOMATION"""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"organization_id": ORGANIZATION_ID}
    
    print(f"\n{'='*80}")
    print(f"📝 Processing Journal #{journal_data['journal_number']} - Date: {journal_data['date']}")
    print(f"{'='*80}")
    
    # Get AP account — try multiple names (Zoho India uses "Sundry Creditors")
    ap_names = ["accounts payable", "sundry creditors", "creditors", "trade payables", "creditor"]
    ap_account_id = next((account_map.get(n) for n in ap_names if account_map.get(n)), None)
    ap_account_name = next((n for n in ap_names if account_map.get(n)), "accounts payable")

    # Get AR account — try multiple names (Zoho India uses "Sundry Debtors")
    ar_names = ["accounts receivable", "sundry debtors", "debtors", "trade receivables", "debtor"]
    ar_account_id = next((account_map.get(n) for n in ar_names if account_map.get(n)), None)
    ar_account_name = next((n for n in ar_names if account_map.get(n)), "accounts receivable")

    if not ap_account_id:
        print(f"  ⚠️  No AP account found (tried: {ap_names})")
        # Print available accounts for debugging
        print(f"  📋 Available accounts (first 20): {list(account_map.keys())[:20]}")
    else:
        print(f"  ✅ AP account: '{ap_account_name}'")
    if not ar_account_id:
        print(f"  ⚠️  No AR account found (tried: {ar_names})")
    else:
        print(f"  ✅ AR account: '{ar_account_name}'")
    
    # Build line items
    zoho_line_items = []
    
    
    for item in journal_data["line_items"]:
        ledger_name = item["ledger_name"]
        ledger_type = item["ledger_type"]
        amount = item["amount"]
        debit_or_credit = item["debit_or_credit"]
        
        print(f"  📌 {ledger_name} ({ledger_type}): {debit_or_credit.upper()} ₹{amount:,.2f}")
        
        # Determine account ID
        account_id = None
        
        if ledger_type == "vendor":
            # AP is a control account — Zoho blocks it in journals (error 11016)
            # Try non-control payable accounts in order
            vendor_acct_names = [
                ledger_name.lower().strip(),          # vendor name as direct account
                "sundry creditors",
                "creditors",
                "other current liabilities",
                "current liabilities",
            ]
            account_id = next((account_map.get(n) for n in vendor_acct_names if account_map.get(n)), None)
            if not account_id:
                # Last resort: use AP (will likely fail, but log clearly)
                account_id = ap_account_id
                print(f"     ⚠️  No non-control account found for vendor — falling back to AP (may fail)")
                print(f"     📋 Tried: {vendor_acct_names}")
            else:
                matched = next((n for n in vendor_acct_names if account_map.get(n)), "?")
                print(f"     💼 Vendor account: '{matched}'")

        elif ledger_type == "customer":
            # AR is a control account — same issue
            customer_acct_names = [
                ledger_name.lower().strip(),          # customer name as direct account
                "sundry debtors",
                "debtors",
                "other current assets",
                "current assets",
            ]
            account_id = next((account_map.get(n) for n in customer_acct_names if account_map.get(n)), None)
            if not account_id:
                account_id = ar_account_id
                print(f"     ⚠️  No non-control account found for customer — falling back to AR (may fail)")
                print(f"     📋 Tried: {customer_acct_names}")
            else:
                matched = next((n for n in customer_acct_names if account_map.get(n)), "?")
                print(f"     💼 Customer account: '{matched}'")
            
        else:
            # Regular account
            account_id = account_map.get(ledger_name.lower().strip())
            if not account_id:
                print(f"     ⚠️  Account '{ledger_name}' not found in Zoho Books - SKIPPING")
                return False
            print(f"     ✅ Found account: {ledger_name}")
        
        # Build line item
        line_item = {
            "account_id": account_id,
            "amount": amount,
            "debit_or_credit": debit_or_credit
        }
        
        # Add contact for vendors/customers (with auto-creation!)
        if ledger_type in ["vendor", "customer"]:
            contact_info = find_or_create_contact(token, contact_map, ledger_name, ledger_type)
            if contact_info:
                line_item["contact_id"] = contact_info["contact_id"]
                print(f"     ✅ Mapped to {ledger_type}: {contact_info['original_name']} (ID: {contact_info['contact_id']})")
            else:
                print(f"     ❌ Failed to find or create contact '{ledger_name}' - SKIPPING")
                return False
        
        # Add reporting tags
        if item["tag_category"] and item["tag_option"]:
            t_id, o_id = find_tag_ids_by_name(token, item["tag_category"], item["tag_option"])
            if t_id and o_id:
                line_item["tags"] = [{"tag_id": t_id, "tag_option_id": o_id}]
                print(f"     🏷️  Tag: {item['tag_category']} > {item['tag_option']}")
        
        zoho_line_items.append(line_item)
    
    # Convert date format
    tally_date = journal_data["date"]
    zoho_date = f"{tally_date[:4]}-{tally_date[4:6]}-{tally_date[6:8]}"
    
    # Build notes: always include Tally journal number for traceability
    tally_ref = journal_data['journal_number']
    narration_text = journal_data['narration'][:900] if journal_data['narration'] else ""
    notes_text = f"Tally Journal #: {tally_ref}"
    if narration_text:
        notes_text += f"\n{narration_text}"

    # Let Zoho auto-generate journal number — Tally # stored only in notes
    payload = {
        "journal_date": zoho_date,
        "notes": notes_text,
        "line_items": zoho_line_items,
        "status": "published"
    }
    
    print(f"\n  [DEBUG] Payload:")
    print(f"    Tally Reference #: {tally_ref}   (stored in reference_number & notes)")
    print(f"    Journal Date: {zoho_date}")
    print(f"    Line Items: {len(zoho_line_items)}")
    print(f"    Notes: {notes_text[:80]}...")
    
    print(f"\n  📤 Creating journal in Zoho Books...")
    res = requests.post(f"{BASE_URL}/journals", headers=headers, params=params, json=payload)
    
    if res.status_code in [200, 201] and res.json().get("code") == 0:
        journal_id = res.json().get("journal", {}).get("journal_id", "N/A")
        print(f"  ✅ SUCCESS! Journal created with ID: {journal_id}")
        return True
    else:
        print(f"  ❌ FAILED! Status: {res.status_code}")
        print(f"  Response: {json.dumps(res.json(), indent=2)}")
        return False

def main():
    print("🚀 FULLY DYNAMIC Journal Migration: Tally → Zoho Books")
    print("="*80)
    print("Features:")
    print("   - Automatic vendor/customer detection from Tally groups")
    print("   - Automatic contact creation in Zoho Books")
    print("   - Zero manual configuration required!")
    print("="*80)
    
    # Get access token
    print("\n🔐 Authenticating with Zoho Books...")
    token = get_access_token()
    if not token:
        print("❌ Failed to get access token")
        return
    print("✅ Authentication successful")
    
    # Fetch Zoho data
    print("\n📊 Fetching Zoho Books accounts and contacts...")
    account_map = get_zoho_accounts(token)
    contact_map = get_zoho_contacts(token)
    print(f"✅ Found {len(account_map)} accounts and {len(contact_map)} contacts")
    
    # Fetch Tally journals (first 5 for testing)
    print("\n📥 Fetching journals from Tally...")
    journals = fetch_tally_journals(from_date="20250401", to_date="20250407", limit=5)
    print(f"✅ Found {len(journals)} journal(s) to migrate")
    
    # Process each journal
    success_count = 0
    fail_count = 0
    created_contacts = []
    
    for journal in journals:
        result = create_zoho_journal(token, journal, account_map, contact_map)
        if result:
            success_count += 1
        else:
            fail_count += 1
    
    # Summary
    print(f"\n{'='*80}")
    print(f"📊 MIGRATION SUMMARY")
    print(f"{'='*80}")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {fail_count}")
    print(f"📝 Total: {len(journals)}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()

# ----------------------------------------------------------
# API WRAPPER FOR FRONTEND
# ----------------------------------------------------------

def get_all_journals_data(from_date="20250401", to_date="20250430", limit=None):
    """
    Wrapper function for API to get journal data
    Returns formatted data for frontend display
    """
    try:
        journals = fetch_tally_journals(from_date, to_date, limit)
        
        if not journals:
            return None
        
        # Calculate stats
        total_journals = len(journals)
        total_debit = 0
        total_credit = 0
        
        for journal in journals:
            for item in journal.get("line_items", []):
                if item["debit_or_credit"] == "debit":
                    total_debit += item["amount"]
                else:
                    total_credit += item["amount"]
        
        return {
            "journals": journals,
            "stats": {
                "total_journals": total_journals,
                "total_debit": round(total_debit, 2),
                "total_credit": round(total_credit, 2),
                "from_date": from_date,
                "to_date": to_date
            }
        }
    except Exception as e:
        print(f"❌ Error in get_all_journals_data: {e}")
        return None

def sync_journals_to_zoho(selected_journals=None, from_date="20250401", to_date="20250430", limit=None):
    """
    Sync journals to Zoho Books
    If selected_journals is None, fetches and syncs all journals in date range
    
    Args:
        selected_journals: List of journal objects to sync (if None, fetches from Tally)
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of journals to sync (respects user input)
    """
    try:
        print("🚀 Starting Zoho Sync (Journals)...")
        
        # Get access token
        token = get_access_token()
        if not token:
            return {"status": "error", "message": "Failed to get access token"}
        
        # Fetch Zoho data
        account_map = get_zoho_accounts(token)
        contact_map = get_zoho_contacts(token)
        
        # Get journals to sync
        if not selected_journals:
            journals_to_sync = fetch_tally_journals(from_date, to_date, limit)
        else:
            journals_to_sync = selected_journals
            # Apply limit if provided
            if limit and len(journals_to_sync) > limit:
                journals_to_sync = journals_to_sync[:limit]
        
        if not journals_to_sync:
            return {"status": "error", "message": "No journals to sync"}
        
        print(f"📊 Syncing {len(journals_to_sync)} journal(s) to Zoho Books...")
        
        stats = {"created": 0, "failed": 0}
        
        for journal in journals_to_sync:
            result = create_zoho_journal(token, journal, account_map, contact_map)
            if result:
                stats["created"] += 1
                print(f"✅ Synced Journal #{journal['journal_number']}")
            else:
                stats["failed"] += 1
                print(f"❌ Failed Journal #{journal['journal_number']}")
        
        return {"status": "success", "stats": stats}
        
    except Exception as e:
        print(f"❌ Error in sync_journals_to_zoho: {e}")
        return {"status": "error", "message": str(e)}

