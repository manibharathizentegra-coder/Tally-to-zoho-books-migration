import requests
import os
import time
from dotenv import load_dotenv

# Load explicitly from the Software_With_Front_END folder or parent
load_dotenv() 

BASE_URL = "https://www.zohoapis.in/books/v3"
AUTH_URL = "https://accounts.zoho.in/oauth/v2/token"

class ZohoConnector:
    def __init__(self):
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.refresh_token = os.getenv("REFRESH_TOKEN")
        self.org_id = os.getenv("ORGANIZATION_ID")
        self.access_token = None
        self.token_expiry = 0

    def get_access_token(self):
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token

        print("🔄 Refreshing Zoho Access Token...")
        params = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token"
        }
        try:
            resp = requests.post(AUTH_URL, data=params)
            data = resp.json()
            if "access_token" in data:
                self.access_token = data["access_token"]
                self.token_expiry = time.time() + (data.get("expires_in", 3600) - 60)
                print("✅ Access Token Refreshed")
                return self.access_token
            else:
                print(f"❌ Failed to refresh token: {data}")
                return None
        except Exception as e:
            print(f"❌ Connection error during auth: {e}")
            return None

    def get_headers(self):
        token = self.get_access_token()
        if not token: return None
        return {
            "Authorization": f"Zoho-oauthtoken {token}",
            "Content-Type": "application/json"
        }

    def api_call(self, method, endpoint, payload=None, params=None):
        headers = self.get_headers()
        if not headers: return {"code": 1, "message": "Auth Failed"}

        url = f"{BASE_URL}{endpoint}"
        if not params: params = {}
        params["organization_id"] = self.org_id

        try:
            if method == "GET":
                resp = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                resp = requests.post(url, headers=headers, params=params, json=payload)
            elif method == "PUT":
                resp = requests.put(url, headers=headers, params=params, json=payload)
            
            return resp.json()
        except Exception as e:
            return {"code": 1, "message": str(e)}

# Singleton instance
zoho = ZohoConnector()
