import requests
import os
import time
import sys
from dotenv import load_dotenv

# Explicitly load .env from the project root (one level up from modules/)
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_path = os.path.join(_project_root, ".env")
load_dotenv(_env_path)

# ── Rate limit settings ──────────────────────────────────────────────────────
API_CALL_DELAY   = 0.4   # seconds between every API call (~150 calls/min max)
RATE_LIMIT_BACKOFF = 15  # seconds to wait on 429 error
MAX_RETRIES      = 3     # retry attempts for rate-limited calls

# ── Credentials (loaded from .env) ──────────────────────────────────────────
_CREDS = {
    "client_id":     os.getenv("CLIENT_ID"),
    "client_secret": os.getenv("CLIENT_SECRET"),
    "refresh_token": os.getenv("REFRESH_TOKEN"),
    "org_id":        os.getenv("ORGANIZATION_ID"),
    "auth_url":      "https://accounts.zoho.com/oauth/v2/token",
}

BASE_URL = "https://www.zohoapis.com/books/v3"


class ZohoConnector:
    def __init__(self):
        self.client_id     = _CREDS.get("client_id")
        self.client_secret = _CREDS.get("client_secret")
        self.refresh_token = _CREDS.get("refresh_token")
        self.org_id        = _CREDS.get("org_id")
        self.auth_url      = _CREDS.get("auth_url",
                                        "https://accounts.zoho.com/oauth/v2/token")
        self.access_token    = None
        self.token_expiry    = 0
        self._last_call_time = 0

    def get_access_token(self):
        """Returns a valid access token, refreshing if expired."""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token

        if not self.client_id or not self.client_secret or not self.refresh_token:
            print("❌ Missing credentials — check zoho_creatials.py")
            print(f"   client_id     : {self.client_id}")
            print(f"   client_secret : {'SET' if self.client_secret else 'MISSING'}")
            print(f"   refresh_token : {'SET' if self.refresh_token else 'MISSING'}")
            print(f"   org_id        : {self.org_id}")
            return None

        print("🔄 Refreshing Zoho Access Token...")
        params = {
            "refresh_token": self.refresh_token,
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
            "grant_type":    "refresh_token"
        }

        for attempt in range(1, 4):
            try:
                resp = requests.post(self.auth_url, data=params, timeout=15)
                data = resp.json()
                if "access_token" in data:
                    self.access_token = data["access_token"]
                    self.token_expiry = time.time() + (data.get("expires_in", 3600) - 60)
                    print("✅ Access Token Refreshed")
                    return self.access_token
                else:
                    print(f"❌ Failed to refresh token (attempt {attempt}): {data}")
                    if attempt < 3:
                        wait = RATE_LIMIT_BACKOFF * attempt
                        print(f"⏳ Waiting {wait}s before retry...")
                        time.sleep(wait)
            except Exception as e:
                print(f"❌ Connection error during auth: {e}")
                if attempt < 3:
                    time.sleep(RATE_LIMIT_BACKOFF)
        return None

    def get_headers(self):
        token = self.get_access_token()
        if not token:
            return None
        return {
            "Authorization": f"Zoho-oauthtoken {token}",
            "Content-Type":  "application/json"
        }

    def _throttle(self):
        """Enforce minimum gap between API calls to avoid rate limiting."""
        elapsed = time.time() - self._last_call_time
        if elapsed < API_CALL_DELAY:
            time.sleep(API_CALL_DELAY - elapsed)
        self._last_call_time = time.time()

    def api_call(self, method, endpoint, payload=None, params=None):
        """
        Makes a Zoho Books API call with:
        - Throttling  : 0.4s minimum gap between calls
        - Retry       : up to 3 retries on 429 / rate-limit responses
        - Token refresh: auto re-fetches token if it expires mid-sync
        """
        url = f"{BASE_URL}{endpoint}"
        if not params:
            params = {}
        params["organization_id"] = self.org_id

        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()

            headers = self.get_headers()
            if not headers:
                return {"code": 1, "message": "Auth Failed"}

            try:
                if method == "GET":
                    resp = requests.get(url, headers=headers, params=params, timeout=30)
                elif method == "POST":
                    resp = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
                elif method == "PUT":
                    resp = requests.put(url, headers=headers, params=params, json=payload, timeout=30)
                else:
                    return {"code": 1, "message": f"Unknown method: {method}"}

                # HTTP 429 — Too Many Requests
                if resp.status_code == 429:
                    wait = RATE_LIMIT_BACKOFF * attempt
                    print(f"⚠️ HTTP 429 rate limit. Waiting {wait}s (attempt {attempt}/{MAX_RETRIES})...")
                    time.sleep(wait)
                    continue

                result = resp.json()

                # Zoho JSON rate-limit codes
                if result.get("code") in (429, 57, 58):
                    wait = RATE_LIMIT_BACKOFF * attempt
                    print(f"⚠️ Zoho rate limit code {result.get('code')}. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                # Auth expired mid-session → force refresh and retry
                if result.get("code") == 14 or "invalid_token" in str(result.get("message", "")):
                    print("🔄 Auth expired mid-session, refreshing token...")
                    self.access_token = None
                    self.token_expiry = 0
                    continue

                return result

            except requests.exceptions.Timeout:
                print(f"⏱️ Request timed out (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue
                return {"code": 1, "message": "Request timed out"}
            except Exception as e:
                return {"code": 1, "message": str(e)}

        return {"code": 1, "message": "Max retries exceeded — Zoho rate limit"}


# Singleton instance used across the app
zoho = ZohoConnector()
