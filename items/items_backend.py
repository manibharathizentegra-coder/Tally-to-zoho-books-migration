import requests
import re
from datetime import datetime

TALLY_URL = "http://localhost:9000"

# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------

def extract_field(xml, tag):
    if not xml:
        return ""
    m = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.DOTALL)
    return m.group(1).strip() if m else ""

def extract_number(text):
    if not text:
        return ""
    m = re.search(r"-?\d+(\.\d+)?", text)
    return float(m.group()) if m else ""

def extract_number_and_unit(text):
    if not text:
        return "", ""
    num = extract_number(text)
    unit = re.sub(r"[-\d.\s/]", "", text)
    return num, unit

def parse_date(val):
    try:
        return datetime.strptime(val, "%Y%m%d")
    except:
        return datetime.min

def pick_latest(blocks):
    latest = ""
    latest_dt = datetime.min
    for b in blocks:
        dt = parse_date(extract_field(b, "APPLICABLEFROM"))
        if dt >= latest_dt:
            latest_dt = dt
            latest = b
    return latest

def extract_supply_type(block, latest_gst, group):
    # 1. Item GST details
    val = extract_field(latest_gst, "SUPPLYTYPE")
    if val:
        return val

    # 2. Item master level
    val = extract_field(block, "GSTTYPEOFSUPPLY")
    if val:
        return val

    # 3. Group level
    return group.get("supply_type", "")

# ----------------------------------------------------------
# GST RATE LOGIC
# ----------------------------------------------------------

def calculate_gst_rate(gst_block):
    if not gst_block:
        return ""

    rates = re.findall(r"<RATEDETAILS.LIST>(.*?)</RATEDETAILS.LIST>", gst_block, re.DOTALL)

    igst = 0
    cgst = 0
    sgst = 0

    for r in rates:
        head = extract_field(r, "GSTRATEDUTYHEAD")
        rate = extract_number(extract_field(r, "GSTRATE")) or 0

        if head == "IGST":
            igst = rate
        elif head == "CGST":
            cgst = rate
        elif head == "SGST/UTGST":
            sgst = rate

    if igst:
        return igst
    if cgst or sgst:
        return cgst + sgst

    return ""


def normalize_applicability(value):
    """
    Converts Tally values like:
    '&#4; Applicable' -> 'Applicable'
    '&#4; Not Applicable' -> 'Non Applicable'
    '' -> ''
    """
    if not value:
        return ""

    val = value.replace("&#4;", "").strip().lower()

    if "applicable" in val and "not" not in val:
        return "Applicable"
    if "not applicable" in val or "non applicable" in val:
        return "Non Applicable"

    return value.strip()


# ----------------------------------------------------------
# FETCH STOCK GROUPS (HSN + GST MASTER)
# ----------------------------------------------------------

def fetch_stock_groups():

    xml_req = """
<ENVELOPE>
 <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Accounts</REPORTNAME>
    <STATICVARIABLES>
     <ACCOUNTTYPE>StockGroups</ACCOUNTTYPE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
"""

    res = requests.post(TALLY_URL, data=xml_req.encode(), timeout=120)
    xml = res.text

    groups = {}

    blocks = re.findall(
        r'<STOCKGROUP NAME="([^"]*)"[^>]*>(.*?)</STOCKGROUP>',
        xml, re.DOTALL
    )

    for name, block in blocks:

        # HSN
        hsn_blocks = re.findall(r"<HSNDETAILS.LIST>(.*?)</HSNDETAILS.LIST>", block, re.DOTALL)
        latest_hsn = pick_latest(hsn_blocks)

        # GST
        gst_blocks = re.findall(r"<GSTDETAILS.LIST>(.*?)</GSTDETAILS.LIST>", block, re.DOTALL)
        latest_gst = pick_latest(gst_blocks)

        groups[name.replace("&amp;", "&")] = {
            "hsn_source": extract_field(latest_hsn, "SRCOFHSNDETAILS"),
            "hsn": extract_field(latest_hsn, "HSNCODE"),
            "description": extract_field(latest_hsn, "HSN"),

            "gst_rate": calculate_gst_rate(latest_gst),
            "taxability": extract_field(latest_gst, "TAXABILITY"),
            "supply_type": extract_field(latest_gst, "SUPPLYTYPE"),
        }

    return groups

# ----------------------------------------------------------
# FETCH STOCK ITEMS (WITH INHERITANCE)
# ----------------------------------------------------------

def fetch_stock_items(groups):

    xml_req = """
<ENVELOPE>
 <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>List of Accounts</REPORTNAME>
    <STATICVARIABLES>
     <ACCOUNTTYPE>StockItems</ACCOUNTTYPE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
"""

    res = requests.post(TALLY_URL, data=xml_req.encode(), timeout=120)
    xml = res.text

    items = []

    blocks = re.findall(
        r'<STOCKITEM NAME="([^"]*)"[^>]*>(.*?)</STOCKITEM>',
        xml, re.DOTALL
    )

    for name, block in blocks:

        parent = extract_field(block, "PARENT")
        group = groups.get(parent, {})

        # Extract HSN details from STOCKITEM itself (item-level)
        hsn_blocks = re.findall(r"<HSNDETAILS.LIST>(.*?)</HSNDETAILS.LIST>", block, re.DOTALL)
        latest_hsn = pick_latest(hsn_blocks)

        # Extract GST details from STOCKITEM itself (item-level)
        gst_blocks = re.findall(r"<GSTDETAILS.LIST>(.*?)</GSTDETAILS.LIST>", block, re.DOTALL)
        latest_gst = pick_latest(gst_blocks)

        # Item-level data (if available)
        item_hsn_source = extract_field(latest_hsn, "SRCOFHSNDETAILS")
        item_hsn = extract_field(latest_hsn, "HSNCODE")
        item_description = extract_field(latest_hsn, "HSN")
        item_gst_rate = calculate_gst_rate(latest_gst)
        item_taxability = extract_field(latest_gst, "TAXABILITY")
        
        # Type of Supply is at STOCKITEM level, not in GSTDETAILS
        item_supply_type = extract_supply_type(block, latest_gst, group)

        gst_rate = item_gst_rate if item_gst_rate else group.get("gst_rate", "")
        
        # Additional fields requested
        raw_app = extract_field(latest_gst, "GSTAPPLICABLE")
        
        item_gst_rate_source = extract_field(latest_gst, "SRCOFGSTDETAILS")  # GST Rate Details source
        item_rate_of_duty = extract_field(block, "BASICRATEOFEXCISE")  # Rate of Duty
        if raw_app:
            item_gst_applicable = normalize_applicability(raw_app)
        elif latest_gst or hsn:
            item_gst_applicable = "Applicable"
        else:
            item_gst_applicable = "Non Applicable"

        # Use item-level data if available, otherwise fall back to group-level
        hsn_source = item_hsn_source if item_hsn_source else group.get("hsn_source", "")
        hsn = item_hsn if item_hsn else group.get("hsn", "")
        description = item_description if item_description else group.get("description", "")
        
        taxability = item_taxability if item_taxability else group.get("taxability", "")
        if item_supply_type:
            supply_type = item_supply_type
        elif group.get("supply_type"):
            supply_type = group.get("supply_type")
        else:
            supply_type = "Goods"
        
        # New fields (no group fallback needed as they're item-specific)
        gst_applicable = item_gst_applicable
        gst_rate_source = item_gst_rate_source
        rate_of_duty = item_rate_of_duty

        qty, qty_unit = extract_number_and_unit(extract_field(block, "OPENINGBALANCE"))
        rate, rate_unit = extract_number_and_unit(extract_field(block, "OPENINGRATE"))
        value = extract_number(extract_field(block, "OPENINGVALUE"))
        value = abs(value) if value else ""

        items.append({
            "name": name.replace("&amp;", "&"),
            "group": parent,
            "unit": extract_field(block, "BASEUNITS"),

            # HSN/GST details (item-level first, then group-level)
            "hsn_source": hsn_source,
            "hsn": hsn,
            "description": description,

            "gst_rate": gst_rate,
            "taxability": taxability,
            "supply_type": supply_type,
            
            # Additional fields
            "gst_applicable": gst_applicable,
            "gst_rate_source": gst_rate_source,
            "rate_of_duty": rate_of_duty,

            "qty": qty,
            "qty_unit": qty_unit,
            "rate": rate,
            "rate_unit": rate_unit,
            "value": value,
        })

    return items

# ----------------------------------------------------------
# SEARCH & DISPLAY
# ----------------------------------------------------------

def search_item(query, items):

    found = [i for i in items if query.lower() in i["name"].lower()]

    if not found:
        print("❌ No item found")
        return

    for i in found:
        print("\n" + "="*80)
        print(f"📦 ITEM NAME            : {i['name']}")
        print(f"📁 STOCK GROUP          : {i['group']}")
        print(f"📏 UNIT                 : {i['unit']}\n")

        print(f"📌 SOURCE OF HSN DETAILS: {i['hsn_source']}")
        print(f"🧾 HSN / SAC            : {i['hsn']}")
        print(f"📝 DESCRIPTION          : {i['description']}\n")
        
        print(f"✅ GST APPLICABILITY    : {i['gst_applicable']}")
        print(f"📊 GST RATE SOURCE      : {i['gst_rate_source']}")
        print(f"💸 GST RATE             : {i['gst_rate']}")
        print(f"📄 TAXABILITY TYPE      : {i['taxability']}")
        
        print(f"📦 TYPE OF SUPPLY       : {i['supply_type']}")
        
        print(f"💰 RATE OF DUTY         : {i['rate_of_duty']}\n")

        print(f"📊 OPENING QTY          : {i['qty']} {i['qty_unit']}")
        print(f"💰 RATE / UNIT          : {i['rate']} / {i['rate_unit']}")
        print(f"💰 TOTAL VALUE          : {i['value']}")
        print("="*80)

# ----------------------------------------------------------
# API WRAPPER
# ----------------------------------------------------------

def get_all_items_data():
    """Wrapper function for API to get all items with stats"""
    groups = fetch_stock_groups()
    items = fetch_stock_items(groups)
    
    # Calculate stats
    total_items = len(items)
    categories = list(set(i.get('group', '') for i in items if i.get('group')))
    total_categories = len(categories)
    
    return {
        "items": items,
        "stats": {
            "total_items": total_items,
            "active_items": total_items, # Logic can be improved
            "categories": total_categories,
            # "total_value": sum(float(i.get('value', 0) or 0) for i in items)  # ❌ Disabled as requested
            "total_value": 0  # Disabled - not calculating stock value
        }
    }

# ----------------------------------------------------------
# ZOHO SYNC
# ----------------------------------------------------------

def sync_items_to_zoho(selected_items=None):
    try:
        from modules.zoho_connector import zoho
    except ImportError:
        try:
            from zoho_connector import zoho
        except:
            return {"status": "error", "message": "Zoho Connector missing"}

    print("🚀 Starting Zoho Sync (Items)...")
    
    if not selected_items:
        data = get_all_items_data()
        if not data: return {"status": "error", "message": "No Tally Data"}
        items_to_sync = data["items"]
    else:
        items_to_sync = selected_items

    stats = {"created": 0, "updated": 0, "failed": 0}

    # Pre-fetch taxes to map percentage -> ID
    taxes_resp = zoho.api_call("GET", "/settings/taxes")
    tax_map = {} # Rate -> ID
    if taxes_resp.get("code") == 0:
        for t in taxes_resp.get("taxes", []):
            tax_map[float(t.get("tax_percentage", 0))] = t.get("tax_id")

    for i in items_to_sync:
        name = i["name"]
        rate = float(i.get("rate", 0) or 0)
        hsn = i.get("hsn", "")
        gst_percent = float(i.get("gst_rate", 0) or 0)
        
        # Find Tax ID
        tax_id = tax_map.get(gst_percent, "") 
        # If no exact match, maybe skip tax or just leave empty? Leave empty for safety.

        payload = {
            "name": name,
            "rate": rate,
            "hsn_or_sac": hsn,
            "tax_id": tax_id,
            "product_type": "goods" # Default
        }

        # Search
        search = zoho.api_call("GET", "/items", params={"name": name})
        
        if search.get("code") == 0:
            existing_items = search.get("items", [])
            existing = next((item for item in existing_items if item["name"].lower() == name.lower()), None)
            
            if existing:
                item_id = existing["item_id"]
                res = zoho.api_call("PUT", f"/items/{item_id}", payload=payload)
                if res.get("code") == 0:
                    stats["updated"] += 1
                    print(f"✅ Updated Item: {name}")
                else:
                    stats["failed"] += 1
                    print(f"❌ Item Update Failed {name}: {res.get('message')}")
            else:
                res = zoho.api_call("POST", "/items", payload=payload)
                if res.get("code") == 0:
                    stats["created"] += 1
                    print(f"✨ Created Item: {name}")
                else:
                    stats["failed"] += 1
                    print(f"❌ Item Create Failed {name}: {res.get('message')}")
        else:
            stats["failed"] += 1

    return {"status": "success", "stats": stats}

if __name__ == "__main__":
    pass
