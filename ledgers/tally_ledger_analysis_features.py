import requests
import re
from collections import defaultdict

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

            groups.append({
                "name": name.replace("&amp;", "&"),
                "parent": extract_field(block, "PARENT")
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

        if parent in sd:
            ledger["type"] = "customer"
            sundry_debtors.append(ledger)

        elif parent in sc:
            ledger["type"] = "vendor"
            sundry_creditors.append(ledger)

        else:
            ledger["type"] = "other"
            other_ledgers.append(ledger)

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
