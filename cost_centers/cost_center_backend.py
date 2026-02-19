import requests
import re
import sys
import os

# Ensure root directory is in path to import database_manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import database_manager
except ImportError:
    print("⚠️ Warning: Could not import database_manager. SQLite sync will be skipped.")
    database_manager = None

TALLY_URL = "http://localhost:9000"

def extract_field(xml, tag):
    if not xml: return ""
    m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.DOTALL)
    return m.group(1).strip() if m else ""

# ----------------------------------------------------------
# FETCH COST CATEGORIES
# ----------------------------------------------------------
def fetch_cost_categories():
    if database_manager: database_manager.init_db()

    xml_req = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER><BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Accounts</REPORTNAME><STATICVARIABLES><ACCOUNTTYPE>CostCategories</ACCOUNTTYPE></STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        res = requests.post(TALLY_URL, data=xml_req, timeout=10)
        xml = res.text
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return []

    categories = []
    blocks = re.findall(r'<COSTCATEGORY NAME="([^"]*)"[^>]*>(.*?)</COSTCATEGORY>', xml, re.DOTALL)

    for name, block in blocks:
        data = {
            "name": name.replace("&amp;", "&"),
            "allocate_revenue": extract_field(block, "ALLOCATEREVENUE"),
            "allocate_non_revenue": extract_field(block, "ALLOCATENONREVENUE")
        }
        
        if database_manager:
            database_manager.insert_or_update_cost_category(data)
        
        categories.append(data)
    
    return categories

# ----------------------------------------------------------
# FETCH COST CENTRES
# ----------------------------------------------------------
def fetch_cost_centres():
    if database_manager: database_manager.init_db()

    xml_req = """<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER><BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Accounts</REPORTNAME><STATICVARIABLES><ACCOUNTTYPE>CostCentres</ACCOUNTTYPE></STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        res = requests.post(TALLY_URL, data=xml_req, timeout=10)
        xml = res.text
    except Exception as e:
        print(f"Error fetching cost centers: {e}")
        return []

    centres = []
    blocks = re.findall(r'<COSTCENTRE NAME="([^"]*)"[^>]*>(.*?)</COSTCENTRE>', xml, re.DOTALL)

    for name, block in blocks:
        data = {
            "name": name.replace("&amp;", "&"),
            "category": extract_field(block, "CATEGORY"),
            "parent": extract_field(block, "PARENT")
        }
        
        if database_manager:
            database_manager.insert_or_update_cost_centre(data)
            
        centres.append(data)

    return centres

def get_all_cost_data():
    cats = fetch_cost_categories()
    cents = fetch_cost_centres()
    return {"categories": cats, "centres": cents}


# ----------------------------------------------------------
# SYNC COST CATEGORIES → ZOHO REPORTING TAGS
# ----------------------------------------------------------
def sync_reporting_tags_to_zoho():
    """
    Syncs Tally Cost Categories → Zoho Reporting Tags.
    Each Cost Category becomes a Reporting Tag in Zoho.
    Each Cost Centre under that category becomes a Tag Option.

    KEY FIX: Zoho requires at least one tag_option in the CREATE call.
    So we bundle all options into the initial POST /settings/tags payload.
    For already-existing tags, we add missing options individually.
    """
    try:
        from modules.zoho_connector import zoho
    except ImportError:
        try:
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from modules.zoho_connector import zoho
        except Exception as e:
            return {"status": "error", "message": f"Zoho Connector missing: {e}"}

    stats = {
        "tags_created": 0,
        "tags_skipped": 0,
        "tags_failed": 0,
        "options_created": 0,
        "options_skipped": 0,
        "options_failed": 0,
    }
    results = []

    # 1. Load from DB
    if not database_manager:
        return {"status": "error", "message": "Database Manager not available"}

    categories = database_manager.get_all_cost_categories()
    centres    = database_manager.get_all_cost_centres()

    if not categories:
        return {"status": "error", "message": "No Cost Categories found in DB. Please import from Tally first."}

    # Group centres by category name (lowercase key for matching)
    centres_by_cat = {}
    for c in centres:
        cat_key = (c.get("category") or "").strip().lower()
        if cat_key not in centres_by_cat:
            centres_by_cat[cat_key] = []
        centres_by_cat[cat_key].append(c)

    # 2. Fetch existing Zoho Reporting Tags
    print("🔍 Fetching existing Zoho Reporting Tags...")
    existing_tags = {}  # tag_name.lower() -> tag_id
    tags_res = zoho.api_call("GET", "/settings/tags")
    if tags_res.get("code") == 0:
        for tag in tags_res.get("reporting_tags", []):
            existing_tags[tag["tag_name"].lower()] = tag["tag_id"]
        print(f"✅ Found {len(existing_tags)} existing tags in Zoho.")
    else:
        print(f"⚠️ Could not fetch existing tags: {tags_res.get('message')}")

    # 3. Process each category
    for cat in categories:
        cat_name = cat.get("name", "").strip()
        if not cat_name:
            continue

        cat_key = cat_name.lower()
        cat_centres = centres_by_cat.get(cat_key, [])
        centre_names = [c.get("name", "").strip() for c in cat_centres if c.get("name", "").strip()]

        cat_result = {
            "name": cat_name,
            "tag_id": None,
            "status": "",
            "options_created": 0,
            "options_skipped": 0,
            "options_failed": 0,
            "option_details": []
        }

        if cat_key in existing_tags:
            # ── Tag already exists: add missing options individually ──
            tag_id = existing_tags[cat_key]
            cat_result["tag_id"] = tag_id
            cat_result["status"] = "skipped"
            stats["tags_skipped"] += 1
            print(f"⏭️  Tag already exists: '{cat_name}' (ID: {tag_id})")

            # Fetch existing options to avoid duplicates
            existing_options = set()
            detail_res = zoho.api_call("GET", f"/settings/tags/{tag_id}")
            if detail_res.get("code") == 0:
                tag_detail = detail_res.get("tag", detail_res.get("reporting_tag", {}))
                for opt in tag_detail.get("tag_options", []):
                    existing_options.add(opt.get("tag_option_name", "").lower())

            for centre_name in centre_names:
                if centre_name.lower() in existing_options:
                    cat_result["options_skipped"] += 1
                    stats["options_skipped"] += 1
                    cat_result["option_details"].append({"name": centre_name, "status": "skipped"})
                    continue

                opt_res = zoho.api_call("POST", f"/settings/tags/{tag_id}/options",
                                        payload={"tag_option_name": centre_name})
                if opt_res.get("code") == 0:
                    cat_result["options_created"] += 1
                    stats["options_created"] += 1
                    cat_result["option_details"].append({"name": centre_name, "status": "created"})
                    print(f"   ✅ Option added: '{centre_name}'")
                else:
                    err = opt_res.get("message", "Unknown")
                    cat_result["options_failed"] += 1
                    stats["options_failed"] += 1
                    cat_result["option_details"].append({"name": centre_name, "status": "failed", "error": err})
                    print(f"   ❌ Option failed '{centre_name}': {err}")

        else:
            # ── New tag: CREATE with all options bundled in one call ──
            if not centre_names:
                print(f"⚠️  Skipping '{cat_name}' — no cost centres found (Zoho requires at least 1 option)")
                cat_result["status"] = "skipped_no_options"
                cat_result["error"] = "No cost centres found for this category"
                results.append(cat_result)
                continue

            print(f"✨ Creating Tag '{cat_name}' with {len(centre_names)} options...")
            payload = {
                "tag_name": cat_name,
                "tag_options": [{"tag_option_name": n} for n in centre_names]
            }
            create_res = zoho.api_call("POST", "/settings/tags", payload=payload)

            if create_res.get("code") == 0:
                tag_obj = create_res.get("tag", create_res.get("reporting_tag", {}))
                tag_id = tag_obj.get("tag_id")
                cat_result["tag_id"] = tag_id
                cat_result["status"] = "created"
                cat_result["options_created"] = len(centre_names)
                stats["tags_created"] += 1
                stats["options_created"] += len(centre_names)
                existing_tags[cat_key] = tag_id
                cat_result["option_details"] = [{"name": n, "status": "created"} for n in centre_names]
                print(f"✅ Created Tag '{cat_name}' with {len(centre_names)} options → ID: {tag_id}")
            else:
                err_msg = create_res.get("message", "Unknown error")
                cat_result["status"] = "failed"
                cat_result["error"] = err_msg
                stats["tags_failed"] += 1
                print(f"❌ Failed to create Tag '{cat_name}': {err_msg}")

        results.append(cat_result)

    return {
        "status": "success",
        "stats": stats,
        "results": results
    }

