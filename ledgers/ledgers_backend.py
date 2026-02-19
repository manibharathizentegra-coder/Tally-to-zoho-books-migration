import requests
import re
from collections import defaultdict
import sys
import os
import json
import mapping_manager

# Ensure root directory is in path to import database_manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import database_manager
except ImportError:
    print("⚠️ Warning: Could not import database_manager. SQLite sync will be skipped.")
    database_manager = None

# ... (rest of imports)

# ----------------------------------------------------------
# XML HELPERS
# ----------------------------------------------------------

def extract_field(xml_block, tag):
    """Extract first occurrence <TAG>value</TAG>"""
    match = re.search(rf"<{tag}>(.*?)</{tag}>", xml_block, re.DOTALL)
    return match.group(1).strip() if match else ""


def extract_all_fields(xml_block, tag):
    """Extract all repeated tags like <ADDRESS>"""
    matches = re.findall(rf"<{tag}>(.*?)</{tag}>", xml_block, re.DOTALL)
    return [m.strip() for m in matches if m.strip()]


# ----------------------------------------------------------
# FETCH GROUPS
# ----------------------------------------------------------

def fetch_groups_from_tally():
    # Initialize DB if possible
    if database_manager:
        database_manager.init_db()

    xml_request = """<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        <REPORTNAME>List of Accounts</REPORTNAME>
        <STATICVARIABLES>
          <ACCOUNTTYPE>Groups</ACCOUNTTYPE>
        </STATICVARIABLES>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>"""

    try:
        response = requests.post("http://localhost:9000",
                                 data=xml_request.encode("utf-8"),
                                 timeout=60)
        xml_data = response.text

        group_blocks = re.findall(
            r'<GROUP NAME="([^"]*)"[^>]*>(.*?)</GROUP>',
            xml_data, re.DOTALL
        )

        groups = []

        for name, block in group_blocks:
            if not name or name == "?":
                continue

            group_data = {
                "name": name.replace("&amp;", "&"),
                "parent": extract_field(block, "PARENT")
            }
            groups.append(group_data)
            
            # ------------------------------------------------
            # SAVE TO SQLITE
            # ------------------------------------------------
            if database_manager:
                database_manager.insert_or_update_group({
                    "name": group_data["name"],
                    "parent": group_data["parent"],
                    "primary_group": "" # Tally doesn't explicitly give this easily here without traversal
                })

        print(f"✅ Groups fetched: {len(groups)}")
        return groups

    except Exception as e:
        print("❌ Error fetching groups:", e)
        return []


# ----------------------------------------------------------
# FETCH LEDGERS
# ----------------------------------------------------------

def fetch_ledgers_from_tally():

    xml_request = """<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        <REPORTNAME>List of Accounts</REPORTNAME>
        <STATICVARIABLES>
          <ACCOUNTTYPE>Ledgers</ACCOUNTTYPE>
        </STATICVARIABLES>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>"""

    try:
        print("📡 Connecting to Tally on port 9000...")
        response = requests.post(
            "http://localhost:9000",
            data=xml_request.encode("utf-8"),
            timeout=60
        )
        xml_data = response.text

        ledger_blocks = re.findall(
            r'<LEDGER NAME="([^"]*)"[^>]*>(.*?)</LEDGER>',
            xml_data, re.DOTALL
        )

        ledgers = []

        for ledger_name, ledger_block in ledger_blocks:

            ledger_name = ledger_name.replace("&amp;", "&")

            # MULTI-LINE ADDRESS FIX
            address_lines = extract_all_fields(ledger_block, "ADDRESS")

            # FIX — STATE FALLBACK LOGIC
            state = (
                extract_field(ledger_block, "STATENAME")
                or extract_field(ledger_block, "LEDSTATENAME")
                or extract_field(ledger_block, "MAILSTATENAME")
                or extract_field(ledger_block, "PRIORSTATENAME")
            )

            ledgers.append({
                "name": ledger_name,
                "parent": extract_field(ledger_block, "PARENT"),

                "opening_balance": extract_field(ledger_block, "OPENINGBALANCE"),
                "closing_balance": extract_field(ledger_block, "CLOSINGBALANCE"),

                "gstin": extract_field(ledger_block, "GSTIN"),
                "gst_reg_type": extract_field(ledger_block, "GSTREGISTRATIONTYPE"),
                "pan": extract_field(ledger_block, "INCOMETAXNUMBER"),

                "address_lines": address_lines,
                "address": "\n".join(address_lines),

                "state": state,
                "country": extract_field(ledger_block, "COUNTRY"),
                "pincode": extract_field(ledger_block, "PINCODE"),

                "phone": extract_field(ledger_block, "PHONE"),
                "email": extract_field(ledger_block, "EMAIL"),
            })

        print(f"✅ Ledgers fetched: {len(ledgers)}")
        return ledgers

    except Exception as e:
        print("❌ Error fetching ledgers:", e)
        return []


# ----------------------------------------------------------
# GROUP HIERARCHY
# ----------------------------------------------------------

def build_group_hierarchy(groups):
    children_map = defaultdict(list)
    for g in groups:
        children_map[g["parent"]].append(g["name"])
    return children_map


def get_all_descendants(group_name, children_map, visited=None):
    if visited is None:
        visited = set()

    if group_name in visited:
        return []

    visited.add(group_name)
    descendants = []

    children = children_map.get(group_name, [])
    descendants.extend(children)

    for child in children:
        descendants.extend(get_all_descendants(child, children_map, visited))

    return descendants


# ----------------------------------------------------------
# LEDGER CLASSIFICATION
# ----------------------------------------------------------


def analyze_ledgers(ledgers, groups):
    children_map = build_group_hierarchy(groups)
    
    # Initialize DB if possible
    if database_manager:
        database_manager.init_db()

    # All sub-groups under Sundry Debtors
    sd = {"Sundry Debtors"}
    sd.update(get_all_descendants("Sundry Debtors", children_map))

    # All sub-groups under Sundry Creditors
    sc = {"Sundry Creditors"}
    sc.update(get_all_descendants("Sundry Creditors", children_map))

    sundry_debtors = []
    sundry_creditors = []
    other_ledgers = []

    for ledger in ledgers:
        parent = ledger["parent"]
        ledger_type = "other"

        if parent in sd:
            ledger_type = "customer"
            sundry_debtors.append(ledger)

        elif parent in sc:
            ledger_type = "vendor"
            sundry_creditors.append(ledger)

        else:
            other_ledgers.append(ledger)
        
        ledger["type"] = ledger_type

        # ------------------------------------------------
        # SAVE TO SQLITE
        # ------------------------------------------------
        if database_manager:
            db_data = {
                "name": ledger["name"],
                "parent": ledger["parent"],
                "type": ledger_type,
                "address": ledger.get("address", ""),
                "state": ledger.get("state", ""),
                "country": ledger.get("country", ""),
                "pincode": ledger.get("pincode", ""),
                "email": ledger.get("email", ""),
                "phone": ledger.get("phone", ""),
                "gstin": ledger.get("gstin", ""),
                "gst_reg_type": ledger.get("gst_reg_type", ""),
                "pan": ledger.get("pan", ""),
                "opening_balance": ledger.get("opening_balance", 0) or 0,
                "closing_balance": ledger.get("closing_balance", 0) or 0
            }
            database_manager.insert_or_update_ledger(db_data)

    return {
        "ledgers": ledgers,
        "sundry_debtors": sundry_debtors,
        "sundry_creditors": sundry_creditors,
        "other_ledgers": other_ledgers
    }


# ----------------------------------------------------------
# SEARCH ENGINE
# ----------------------------------------------------------

def search_ledger(query, ledgers):

    query = query.lower().strip()
    matches = [l for l in ledgers if query in l["name"].lower()]

    if not matches:
        print(f"\n❌ No ledger found for '{query}'")
        return

    print(f"\n🔎 Found {len(matches)} result(s) for '{query}':")

    for l in matches:
        print("\n" + "="*80)
        print(f"📌 NAME           : {l['name']}")
        print(f"📁 UNDER GROUP    : {l['parent']}")
        print(f"🏷️ LEDGER TYPE    : {l['type']}")

        print("📨 ADDRESS:")
        for line in l["address_lines"]:
            print(f"   {line}")

        print(f"🌍 STATE          : {l.get('state','')}")
        print(f"🌏 COUNTRY        : {l.get('country','')}")
        print(f"📮 PINCODE        : {l.get('pincode','')}")
        print(f"🆔 PAN            : {l.get('pan','')}")
        print(f"🔢 GSTIN          : {l.get('gstin','')}")
        print(f"🧾 GST REG TYPE   : {l.get('gst_reg_type','')}")

        opening = l.get("opening_balance", "")
        if not opening:
            print("💰 OPENING BAL    : ❌ No opening balance in Tally")
        else:
            print(f"💰 OPENING BAL    : {opening}")

        print(f"💰 CLOSING BAL    : {l.get('closing_balance','')}")
        print("="*80)


# ----------------------------------------------------------
# MAIN PROGRAM
# ----------------------------------------------------------

if __name__ == "__main__":

    print("🚀 TALLY LEDGER ANALYSIS + SEARCH TOOL")
    print("="*80)

    print("\n📂 Fetching Groups...")
    groups = fetch_groups_from_tally()

    print("\n📋 Fetching Ledgers...")
    ledgers = fetch_ledgers_from_tally()

    if not ledgers:
        print("❌ No ledgers fetched from Tally.")
        exit()

    print("\n🔍 Analyzing Ledger Hierarchy...")
    analysis = analyze_ledgers(ledgers, groups)

    print("\n🎯 SUMMARY")
    print(f"   Customers : {len(analysis['sundry_debtors'])}")
    print(f"   Vendors   : {len(analysis['sundry_creditors'])}")
    print(f"   Others    : {len(analysis['other_ledgers'])}")

    # SEARCH LOOP
    while True:
        query = input("\n🔍 Enter Ledger/Customer/Vendor to search (or EXIT): ")
        if query.lower() == "exit":
            print("👋 Exiting tool.")
            break

        search_ledger(query, analysis["ledgers"])


# ----------------------------------------------------------
# API WRAPPER
# ----------------------------------------------------------

# ----------------------------------------------------------
# COST CENTERS IMPORT
# ----------------------------------------------------------
try:
    from cost_centers import cost_center_backend
except ImportError:
    print("⚠️ Warning: Could not import cost_center_backend")
    cost_center_backend = None


def analyze_ledgers_and_groups():
    """Wrapper function for API to get all data"""
    # 1. Fetch Groups
    groups = fetch_groups_from_tally()
    
    # 2. Fetch Ledgers
    ledgers = fetch_ledgers_from_tally()

    # 3. Fetch Cost Centers (New)
    if cost_center_backend:
        print("\n💰 Fetching Cost Categories...")
        cats = cost_center_backend.fetch_cost_categories()
        print(f"✅ Cost Categories fetched: {len(cats)}")

        print("\n💰 Fetching Cost Centres...")
        cents = cost_center_backend.fetch_cost_centres()
        print(f"✅ Cost Centres fetched: {len(cents)}")
    
    if not ledgers: return None
    
    analysis = analyze_ledgers(ledgers, groups)
    children_map = build_group_hierarchy(groups)
    
    return {
        "ledgers": analysis["ledgers"],
        "customers": analysis["sundry_debtors"],
        "vendors": analysis["sundry_creditors"],
        "others": analysis["other_ledgers"],
        "groups": groups,
        "group_hierarchy": children_map,
        "stats": {
            "total_ledgers": len(ledgers),
            "total_customers": len(analysis["sundry_debtors"]),
            "total_vendors": len(analysis["sundry_creditors"]),
            "total_others": len(analysis["other_ledgers"]),
            "total_groups": len(groups)
        }
    }

# ----------------------------------------------------------
# ZOHO SYNC
# ----------------------------------------------------------

def save_groups_mapping(mapping):
    """Save the group mapping to a persistent file."""
    return mapping_manager.save_mapping(mapping)

def get_groups_mapping():
    """Retrieve the saved group mapping."""
    return mapping_manager.load_mapping()

def sync_groups_to_zoho(mapping=None):
    if mapping is None:
        print("📂 Loading saved mapping...")
        mapping = mapping_manager.load_mapping()

    if not mapping:
        return {"status": "error", "message": "No mapping found. Please save mapping first."}

    try:
        from modules.zoho_connector import zoho
    except ImportError:
        try:
            from zoho_connector import zoho
        except:
            return {"status": "error", "message": "Zoho Connector missing"}

    print(f"🚀 Starting Zoho Sync (Groups & Ledgers)... Mapping size: {len(mapping)}")
    stats = {"created": 0, "updated": 0, "failed": 0, "skipped": 0, "children_created": 0, "ledgers_created": 0}
    failed_ledgers = []   # Ledgers that failed due to type mismatch
    duplicates = []       # Duplicate account names found in Zoho

    # 1. Fetch Source Data (From DB as requested)
    print("📂 Fetching Groups & Ledgers from Local Database...")
    if database_manager:
        tally_groups = database_manager.get_all_groups()
        tally_ledgers = database_manager.get_all_ledgers()
        print(f"✅ Loaded {len(tally_groups)} Groups and {len(tally_ledgers)} Ledgers from DB.")
    else:
        print("⚠️ Database Manager not loaded. Skipping child sync.")
        tally_groups = []
        tally_ledgers = []

    # 2. Get existing Chart of Accounts
    existing_accounts = {}
    page = 1
    has_more = True
    
    print("🔍 Fetching existing Chart of Accounts from Zoho...")
    while has_more:
        res = zoho.api_call("GET", "/chartofaccounts", params={"page": page, "per_page": 200})
        if res.get("code") != 0: 
            print(f"⚠️ Error fetching accounts page {page}: {res.get('message')}")
            break
        
        accounts = res.get("chartofaccounts", [])
        if not accounts: break
        
        # Track duplicates: if name already seen, it's a duplicate
        seen_in_page = {}
        for acc in accounts:
            key = acc["account_name"].lower()
            if key in existing_accounts:
                # Duplicate found
                duplicates.append({
                    "name": acc["account_name"],
                    "id1": existing_accounts[key].get("account_id"),
                    "id2": acc.get("account_id"),
                    "type": acc.get("account_type", "")
                })
            else:
                existing_accounts[key] = acc
            
        has_more = res.get("page_context", {}).get("has_more_page", False)
        page += 1

    print(f"📊 Found {len(existing_accounts)} existing accounts in Zoho.")

    # 2b. Detect TALLY-side duplicates (same ledger name, different parents)
    tally_duplicates = []
    tally_ledger_name_map = {}  # name.lower() -> list of {name, parent}
    for ledger in tally_ledgers:
        key = ledger["name"].lower()
        if key not in tally_ledger_name_map:
            tally_ledger_name_map[key] = []
        tally_ledger_name_map[key].append({"name": ledger["name"], "parent": ledger.get("parent", "")})

    for key, entries in tally_ledger_name_map.items():
        if len(entries) > 1:
            tally_duplicates.append({
                "name": entries[0]["name"],
                "occurrences": entries  # list of {name, parent}
            })

    if tally_duplicates:
        print(f"⚠️ Found {len(tally_duplicates)} duplicate ledger names in Tally data!")

    # Track Valid Parents for subsequent phases (Name -> {id, type})
    valid_parents = {}

    # ---------------------------------------------------------
    # PHASE 1: Sync Mapped PARENT Groups
    # ---------------------------------------------------------
    print(f"🔹 PHASE 1: Syncing {len(mapping)} Mapped Parent Groups...")
    
    for group_name, user_type in mapping.items():
        if not user_type:
            continue
            
        group_key = group_name.lower()
        account_type = user_type.lower().replace(" ", "_")
        
        # Check existence
        if group_key in existing_accounts:
            acc_id = existing_accounts[group_key]["account_id"]
            valid_parents[group_name] = {"id": acc_id, "type": account_type}
            continue

        # Create Parent
        print(f"✨ Creating Parent Account: '{group_name}'...")
        payload = {
            "account_name": group_name,
            "account_type": account_type
        }

        res = zoho.api_call("POST", "/chartofaccounts", payload=payload)

        if res.get("code") == 0:
            # FIXED: Helper to get account from response (singular 'chart_of_account')
            new_acc = res.get("chart_of_account", {}) 
            print(f"✅ Created Parent: {group_name}")
            stats["created"] += 1
            
            existing_accounts[group_key] = new_acc
            valid_parents[group_name] = {"id": new_acc.get("account_id"), "type": account_type}
        else:
            print(f"❌ Failed to create Parent '{group_name}': {res.get('message')}")
            stats["failed"] += 1

    # ---------------------------------------------------------
    # PHASE 2: Sync CHILD Groups (Sub-Accounts) 
    # ---------------------------------------------------------
    print(f"🔹 PHASE 2: Syncing Child Groups under Mapped Parents...")
    
    for grp in tally_groups:
        tally_name = grp["name"]
        tally_parent = grp["parent"] # The immediate parent in Tally
        
        # Check if Parent is Valid (Mapped)
        if tally_parent in valid_parents:
            parent_info = valid_parents[tally_parent]
            parent_zoho_id = parent_info["id"]
            account_type = parent_info["type"] # Inherit Type

            # Check existence and Parent Link
            if tally_name.lower() in existing_accounts:
                existing = existing_accounts[tally_name.lower()]
                acc_id = existing["account_id"]
                current_parent_id = existing.get("parent_account_id", "")
                
                # Update Parent Link if mismatched
                if str(current_parent_id) != str(parent_zoho_id):
                    print(f"🔄 Correcting Parent for Group '{tally_name}'...")
                    res = zoho.api_call("PUT", f"/chartofaccounts/{acc_id}", payload={
                         "parent_account_id": parent_zoho_id
                    })
                    if res.get("code") == 0:
                        print(f"✅ Re-linked Group Parent: {tally_name}")
                        stats["updated"] += 1
                else:
                    stats["skipped"] += 1
                
                valid_parents[tally_name] = {"id": acc_id, "type": account_type}
                continue
            
            print(f"🌱 Creating Child Group: '{tally_name}' under '{tally_parent}'...")
            
            payload = {
                "account_name": tally_name,
                "account_type": account_type,
                "parent_account_id": parent_zoho_id,
                "is_sub_account": True,
    
            }
            
            res = zoho.api_call("POST", "/chartofaccounts", payload=payload)
            
            if res.get("code") == 0:
                new_acc = res.get("chart_of_account", {}) # FIXED
                print(f"✅ Created Child Group: {tally_name}")
                stats["children_created"] += 1
                
                existing_accounts[tally_name.lower()] = new_acc
                valid_parents[tally_name] = {"id": new_acc.get("account_id"), "type": account_type}
            else:
                print(f"❌ Failed to create Child Group '{tally_name}': {res.get('message')}")
                stats["failed"] += 1

    # ---------------------------------------------------------
    # PHASE 3: Sync LEDGERS (as Sub-Accounts)
    # ---------------------------------------------------------
    print(f"🔹 PHASE 3: Syncing Ledgers under Valid Groups...")

    for ledger in tally_ledgers:
        ledger_name = ledger["name"]
        ledger_parent = ledger["parent"]
        
        # Check if Ledger's Parent is in our Valid Scope (Mapped or Created Child)
        if ledger_parent in valid_parents:
            parent_info = valid_parents[ledger_parent]
            parent_zoho_id = parent_info["id"]
            account_type = parent_info["type"] # Inherit Type
            
            # Check existence and Parent Link
            if ledger_name.lower() in existing_accounts:
                existing = existing_accounts[ledger_name.lower()]
                acc_id = existing["account_id"]
                current_parent_id = existing.get("parent_account_id", "")
                
                # Update Parent Link if mismatched
                if str(current_parent_id) != str(parent_zoho_id):
                    print(f"🔄 Correcting Parent for Ledger '{ledger_name}'...")
                    res = zoho.api_call("PUT", f"/chartofaccounts/{acc_id}", payload={
                         "parent_account_id": parent_zoho_id
                    })
                    if res.get("code") == 0:
                        print(f"✅ Re-linked Ledger Parent: {ledger_name}")
                        stats["updated"] += 1
                else:
                    stats["skipped"] += 1
                continue
                
            print(f"📝 Creating Ledger Account: '{ledger_name}' under '{ledger_parent}'...")
            
            payload = {
                "account_name": ledger_name,
                "account_type": account_type,
                "parent_account_id": parent_zoho_id,
                "is_sub_account": True,
                "description": f"Imported from Tally Ledger: {ledger_name}"
            }

            res = zoho.api_call("POST", "/chartofaccounts", payload=payload)
            
            if res.get("code") == 0:
                print(f"✅ Created Ledger: {ledger_name}")
                stats["ledgers_created"] += 1
                existing_accounts[ledger_name.lower()] = res.get("chart_of_account") # FIXED
            else:
                error_msg = res.get('message', 'Unknown error')
                print(f"❌ Failed to create Ledger '{ledger_name}': {error_msg}")
                stats["failed"] += 1
                # Collect failed ledgers for frontend review
                failed_ledgers.append({
                    "name": ledger_name,
                    "parent": ledger_parent,
                    "error": error_msg,
                    "inherited_type": account_type
                })

    # Return combined stats
    stats["total_created"] = stats["created"] + stats["children_created"] + stats["ledgers_created"]
    return {
        "status": "success",
        "stats": stats,
        "failed_ledgers": failed_ledgers,
        "duplicates": duplicates,           # Zoho-side duplicates
        "tally_duplicates": tally_duplicates  # Tally-side duplicates (same name, diff parent)
    }

def get_gst_treatment(ledger):
    gstin = ledger.get("gstin", "")
    reg_type = ledger.get("gst_reg_type", "").lower()
    place = ledger.get("state", "").lower()
    
    if gstin:
        return "business_gst"
    if "consumer" in reg_type:
        return "consumer"
    if "composition" in reg_type:
        return "business_composition"
    if "unregistered" in reg_type:
        return "business_none"

def create_standalone_account(ledger_name, account_type):
    """
    Create a ledger in Zoho Books WITHOUT a parent account.
    Used when the normal sub-account creation fails due to type mismatch.
    The migration team selects the correct account_type manually.
    """
    try:
        from modules.zoho_connector import zoho
    except ImportError:
        try:
            from zoho_connector import zoho
        except:
            return {"status": "error", "message": "Zoho Connector missing"}

    # Zoho requires snake_case for account_type e.g. "other_asset", "bank", "long_term_liability"
    zoho_account_type = account_type.lower().replace(" ", "_")

    payload = {
        "account_name": ledger_name,
        "account_type": zoho_account_type
    }

    print(f"📝 Creating Standalone Account: '{ledger_name}' as '{zoho_account_type}'...")
    res = zoho.api_call("POST", "/chartofaccounts", payload=payload)

    if res.get("code") == 0:
        new_acc = res.get("chart_of_account", {})
        print(f"✅ Created Standalone: {ledger_name}")
        return {
            "status": "success",
            "account_id": new_acc.get("account_id"),
            "account_name": ledger_name,
            "account_type": account_type
        }
    else:
        error_msg = res.get("message", "Unknown error")
        print(f"❌ Failed Standalone '{ledger_name}': {error_msg}")
        return {"status": "error", "message": error_msg}

    if "overseas" in place or "foreign" in place:
        return "overseas"
    return "business_none" # Default

def sync_ledgers_to_zoho(selected_ledgers=None, contact_type_filter=None):  # OPTIMISED
    """
    Syncs ledgers to Zoho Books as Contacts.
    
    Args:
        selected_ledgers: Optional list of specific ledgers to sync.
        contact_type_filter: 'customer' or 'vendor' — if set, only syncs that type.
                             If None, syncs both customers AND vendors.
    """
    try:
        from modules.zoho_connector import zoho
    except ImportError:
        try:
            from zoho_connector import zoho
        except:
            print("❌ Could not import Zoho Connector")
            return {"status": "error", "message": "Zoho Connector missing"}

    print(f"🚀 Starting Zoho Sync (Ledgers) — Filter: {contact_type_filter or 'all'}...")

    # Get fresh data if not provided
    if not selected_ledgers:
        if database_manager:
            print("💾 Fetching ledgers from SQLite Database...")
            all_ledgers = database_manager.get_all_ledgers()
            # Filter by type
            if contact_type_filter:
                ledgers_to_sync = [l for l in all_ledgers if l['type'] == contact_type_filter]
            else:
                ledgers_to_sync = [l for l in all_ledgers if l['type'] in ['customer', 'vendor']]
            
            if not ledgers_to_sync:
                print("⚠️ No matching ledgers found in DB. Trying Tally fetch...")
                data = analyze_ledgers_and_groups()
                if data:
                    if contact_type_filter == 'customer':
                        ledgers_to_sync = data["customers"]
                    elif contact_type_filter == 'vendor':
                        ledgers_to_sync = data["vendors"]
                    else:
                        ledgers_to_sync = data["customers"] + data["vendors"]
        else:
            print("📡 Fetching ledgers directly from Tally...")
            data = analyze_ledgers_and_groups()
            if not data: return {"status": "error", "message": "No Tally Data"}
            if contact_type_filter == 'customer':
                ledgers_to_sync = data["customers"]
            elif contact_type_filter == 'vendor':
                ledgers_to_sync = data["vendors"]
            else:
                ledgers_to_sync = data["customers"] + data["vendors"]
    else:
        # If a filter is passed with selected_ledgers, still honour it
        if contact_type_filter:
            ledgers_to_sync = [l for l in selected_ledgers if l.get('type') == contact_type_filter]
        else:
            ledgers_to_sync = selected_ledgers

    # ─────────────────────────────────────────────────────────────────────────
    # OPTIMISATION: Pre-load ALL existing Zoho contacts ONCE (bulk paginated).
    # For 1000+ vendors this avoids N individual search API calls and
    # replaces them with ~5-10 paginated GETs total.
    # ─────────────────────────────────────────────────────────────────────────
    print("📥 Pre-loading existing Zoho contacts (bulk fetch — this may take a moment)...")
    existing_contacts = {}   # key: contact_name.lower() -> contact dict
    per_page = 200           # Zoho max per page

    # Pre-load only the contact type(s) we will sync
    types_to_preload = [contact_type_filter] if contact_type_filter else ["customer", "vendor"]

    for ctype in types_to_preload:
        page = 1
        while True:
            res = zoho.api_call("GET", "/contacts", params={
                "contact_type": ctype,
                "page": page,
                "per_page": per_page
            })
            if res.get("code") != 0:
                print(f"⚠️ Could not pre-load {ctype} contacts page {page}: {res.get('message')}")
                break

            contacts_page = res.get("contacts", [])
            for c in contacts_page:
                key = c["contact_name"].lower().strip()
                existing_contacts[key] = c

            has_more = res.get("page_context", {}).get("has_more_page", False)
            print(f"   📄 Loaded {ctype} page {page} — {len(contacts_page)} contacts (has_more={has_more})")
            if not has_more:
                break
            page += 1

    print(f"✅ Pre-loaded {len(existing_contacts)} existing Zoho contacts into memory.")

    # ─────────────────────────────────────────────────────────────────────────
    # MAIN SYNC LOOP — uses local map for existence check (zero extra GETs)
    # ─────────────────────────────────────────────────────────────────────────
    stats = {"created": 0, "updated": 0, "failed": 0, "skipped": 0}
    failed_names = []  # track which contacts failed for frontend display
    total = len(ledgers_to_sync)

    def clean(val): return (val or "").replace("\r", "").replace("\n", "").strip()

    for idx, l in enumerate(ledgers_to_sync, 1):
        # Strip \r \n and extra whitespace — Tally data often has carriage returns embedded
        name = l["name"].replace("\r", "").replace("\n", "").strip()
        name_key = name.lower()

        # Progress log every 50 records
        if idx % 50 == 0 or idx == 1 or idx == total:
            print(f"   🔄 Progress: {idx}/{total} | created={stats['created']} skipped={stats['skipped']} failed={stats['failed']}")

        # Determine contact type — use the ledger's own type field
        contact_type = "customer"
        if l.get("type") == "vendor":
            contact_type = "vendor"

        # LOCAL existence check — no API call needed
        if name_key in existing_contacts:
            stats["skipped"] += 1
            continue  # Already in Zoho, skip silently (uncomment print below if needed)
            # print(f"⏭️  Skipped (already exists): {name}")

        address_str = clean(l.get("address", ""))
        city        = ""
        state       = clean(l.get("state", ""))
        zip_code    = clean(l.get("pincode", ""))
        country     = clean(l.get("country", ""))
        email       = clean(l.get("email", ""))
        phone       = clean(l.get("phone", ""))  # Tally phone → used as mobile in Zoho

        payload = {
            "contact_name": name,
            "company_name": name,
            "contact_type": contact_type,
            # "gst_treatment": get_gst_treatment(l),
            # "gst_no": l.get("gstin", ""),
            "email":  email,
            "phone":  phone,    # Work Phone
            "mobile": phone,    # Mobile (Tally phone is usually mobile)
            "billing_address": {
                "address": address_str,
                "city":    city,
                "state":   state,
                "zip":     zip_code,
                "country": country
            },
            "shipping_address": {
                "address": address_str,
                "city":    city,
                "state":   state,
                "zip":     zip_code,
                "country": country
            }
        }

        # Add contact_persons when email OR phone exists — Zoho Books stores
        # email and mobile inside contact_persons (not just top level).
        # last_name = customer/vendor name, as confirmed from Zoho API response.
        if email or phone:
            payload["contact_persons"] = [
                {
                    "first_name":          "",
                    "last_name":           name,
                    "email":               email,
                    "phone":               phone,   # Work Phone
                    "mobile":              phone,   # Mobile
                    "is_primary_contact":  True
                }
            ]

        # CREATE — new contact (no per-vendor search needed)
        res = zoho.api_call("POST", "/contacts", payload=payload)
        if res.get("code") == 0:
            stats["created"] += 1
            # Add to local map so intra-run duplicates are also caught
            existing_contacts[name_key] = res.get("contact", {})
            print(f"✨ Created ({contact_type}): {name}")
        else:
            stats["failed"] += 1
            err_msg = res.get('message', 'Unknown error')
            failed_names.append({"name": name, "reason": err_msg})
            print(f"❌ Create Failed {name}: {err_msg}")

    print(f"\n🏁 Sync Complete — Created: {stats['created']}, Skipped: {stats['skipped']}, Failed: {stats['failed']}")
    return {"status": "success", "stats": stats, "failed_contacts": failed_names}
