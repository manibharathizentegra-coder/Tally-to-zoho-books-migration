import os
import requests
import json
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
# Ensure these match your region (.in, .com, .eu)
AUTH_URL = "https://accounts.zoho.in/oauth/v2/token"
BASE_URL = "https://www.zohoapis.in/books/v3"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")

def get_access_token():
    """Generates a fresh Access Token using the Refresh Token."""
    params = {
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    try:
        response = requests.post(AUTH_URL, data=params)
        response.raise_for_status()
        return response.json().get('access_token')
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return None

def get_all_payment_details():
    token = get_access_token()
    if not token:
        return

    headers = {
        'Authorization': f"Zoho-oauthtoken {token}",
        'X-com-zoho-books-organizationid': ORGANIZATION_ID
    }

    # Step 1: Get the list of ALL payments
    # We use 'page=1' and 'per_page=200' to get as many as possible per call
    print("--- Fetching Payment List ---")
    list_url = f"{BASE_URL}/customerpayments?per_page=200"
    
    try:
        res = requests.get(list_url, headers=headers)
        if res.status_code != 200:
            print(f"Error fetching list: {res.text}")
            return

        payments_summary = res.json().get("customerpayments", [])
        print(f"✅ Found {len(payments_summary)} payments in list.")

        all_full_payments = []

        # Step 2: Loop through each payment to get FULL details
        print("\n--- Fetching FULL Details for each Payment ---")
        
        for index, summary in enumerate(payments_summary):
            payment_id = summary.get('payment_id')
            
            # API Call for specific payment details
            detail_url = f"{BASE_URL}/customerpayments/{payment_id}"
            detail_res = requests.get(detail_url, headers=headers)
            
            if detail_res.status_code == 200:
                full_data = detail_res.json().get("payment")
                all_full_payments.append(full_data)
                print(f"[{index+1}/{len(payments_summary)}] Fetched full data for Payment #{full_data.get('payment_number')}")
            else:
                print(f"❌ Failed to fetch details for ID {payment_id}")

            # RATE LIMIT HANDLING: Sleep briefly to avoid hitting Zoho's limit (100 req/min)
            time.sleep(0.2) 

        # Step 3: Print or Save the Data
        print("\n--- DONE! Example of FIRST Full Payment Record ---")
        if all_full_payments:
            print(json.dumps(all_full_payments[0], indent=4))
            
            # Optional: Save to file
            with open("all_payments_full.json", "w") as f:
                json.dump(all_full_payments, f, indent=4)
                print("\n✅ Saved all data to 'all_payments_full.json'")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    get_all_payment_details()