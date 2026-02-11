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
