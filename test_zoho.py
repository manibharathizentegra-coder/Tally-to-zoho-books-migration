"""
Quick test — run this to check if Zoho API is working again.
Usage: python test_zoho.py
"""
from modules.zoho_connector import zoho

print("Testing Zoho connection...")
r = zoho.api_call("GET", "/contacts", params={"page": 1, "per_page": 1})

if r.get("code") == 0:
    contacts = r.get("contacts", [])
    print(f"✅ Zoho API is WORKING! Got {len(contacts)} contact(s) in test.")
else:
    print(f"❌ Still blocked or error: {r.get('message')}")
    print(f"   Full response: {r}")
