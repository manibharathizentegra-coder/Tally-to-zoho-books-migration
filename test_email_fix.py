"""
STEP 2: Read 2 customers from DB → Create in Zoho with contact_persons (email fix)
Run: python test_email_fix.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Only the names — everything else comes from DB
TARGET_NAMES = [
    "Manipal Health Enterprises Pvt.Ltd.,",
    "Schloss Chennai Pvt Ltd",
]

def clean(val):
    return (val or "").replace("\r", "").replace("\n", "").strip()

try:
    from modules.zoho_connector import zoho
except ImportError:
    from zoho_connector import zoho

import database_manager

print("\n" + "="*60)
print("  DB → ZOHO CREATE (with contact_persons email fix)")
print("="*60)

for name in TARGET_NAMES:
    print(f"\n>>> {name}")

    # Pull full record from DB
    ledger = database_manager.get_ledger_by_name(name)
    if not ledger:
        print(f"  ❌ Not found in DB — skipping")
        continue

    email   = clean(ledger.get("email", ""))
    phone   = clean(ledger.get("phone", ""))
    address = clean(ledger.get("address", ""))
    state   = clean(ledger.get("state", ""))
    pincode = clean(ledger.get("pincode", ""))
    country = clean(ledger.get("country", ""))

    print(f"  DB email   : '{email}'")
    print(f"  DB phone   : '{phone}'")
    print(f"  DB state   : '{state}'")

    payload = {
        "contact_name": name,
        "company_name": name,
        "contact_type": "customer",
        "email": email,
        "phone": phone,
        "contact_persons": [
            {
                "first_name": "",
                "last_name": name,
                "email": email,
                "phone": phone,
                "is_primary_contact": True
            }
        ],
        "billing_address": {
            "address": address,
            "city": "",
            "state": state,
            "zip": pincode,
            "country": country
        },
        "shipping_address": {
            "address": address,
            "city": "",
            "state": state,
            "zip": pincode,
            "country": country
        }
    }

    print(f"  Sending POST /contacts ...")
    res = zoho.api_call("POST", "/contacts", payload=payload)

    if res.get("code") == 0:
        c = res.get("contact", {})
        persons = c.get("contact_persons", [])
        person_email = persons[0].get("email", "") if persons else ""
        print(f"  ✅ CREATED!")
        print(f"     contact_id   : {c.get('contact_id')}")
        print(f"     top email    : '{c.get('email', '')}'")
        print(f"     person email : '{person_email}'")
    else:
        print(f"  ❌ FAILED: code={res.get('code')} | {res.get('message')}")
        if res.get("code") == 3062:
            print(f"  ℹ️  Already exists in Zoho — delete it first then rerun.")

print("\n" + "="*60)
print("  DONE — Check Zoho UI for email in both contacts")
print("="*60)
