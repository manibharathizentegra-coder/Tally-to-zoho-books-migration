import requests
import os
import time
import sys
import random
from dotenv import load_dotenv

# Explicitly load .env from the project root (one level up from modules/)
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_path = os.path.join(_project_root, ".env")
load_dotenv(_env_path, override=True)

# ── Rate limit settings ──────────────────────────────────────────────────────
def _env_float(key: str, default: float) -> float:
    try:
        return float((os.getenv(key) or "").strip() or default)
    except Exception:
        return default

def _env_int(key: str, default: int) -> int:
    try:
        return int(float((os.getenv(key) or "").strip() or default))
    except Exception:
        return default

# Defaults are conservative to avoid 429 during bulk imports.
API_CALL_DELAY = _env_float("ZOHO_API_CALL_DELAY", 0.6)  # seconds between calls
RATE_LIMIT_BACKOFF = _env_int("ZOHO_RATE_LIMIT_BACKOFF", 20)  # seconds to wait on 429 / busy
MAX_RETRIES = _env_int("ZOHO_MAX_RETRIES", 5)  # retry attempts for rate-limited calls
JITTER_MAX_SECONDS = _env_float("ZOHO_API_JITTER_MAX", 0.15)  # prevents burst patterns

# ── Credentials (loaded from .env) ──────────────────────────────────────────
ZOHO_DC = (os.getenv("ZOHO_DC") or "com").strip().lower()  # set to "in" for India DC
ACCOUNTS_DOMAIN = "accounts.zoho.in" if ZOHO_DC == "in" else "accounts.zoho.com"
API_DOMAIN = "www.zohoapis.in" if ZOHO_DC == "in" else "www.zohoapis.com"

_CREDS = {
    "client_id":     (os.getenv("CLIENT_ID") or "").strip(),
    "client_secret": (os.getenv("CLIENT_SECRET") or "").strip(),
    "refresh_token": (os.getenv("REFRESH_TOKEN") or "").strip(),
    "org_id":        (os.getenv("ORGANIZATION_ID") or "").strip(),
    "auth_url":      f"https://{ACCOUNTS_DOMAIN}/oauth/v2/token",
}

BASE_URL = f"https://{API_DOMAIN}/books/v3"

class ZohoConnector:
    def __init__(self):
        self.client_id     = (os.getenv("CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("CLIENT_SECRET") or "").strip()
        self.refresh_token = (os.getenv("REFRESH_TOKEN") or "").strip()
        self.org_id        = (os.getenv("ORGANIZATION_ID") or "").strip()
        self.auth_url      = f"https://{ACCOUNTS_DOMAIN}/oauth/v2/token"
        self.access_token    = None
        self.token_expiry    = 0
        self._last_call_time = 0

    def get_access_token(self):
        """Returns a valid access token, refreshing if expired."""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token

        if not self.client_id or not self.client_secret or not self.refresh_token:
            print(f" Missing credentials in ZohoConnector. client_id: {bool(self.client_id)}, client_secret: {bool(self.client_secret)}, refresh_token: {bool(self.refresh_token)}")
            return None

        params = {
            "refresh_token": self.refresh_token,
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
            "grant_type":    "refresh_token"
        }

        try:
            resp = requests.post(self.auth_url, data=params, timeout=15)
            data = resp.json()
            if "access_token" in data:
                self.access_token = data["access_token"]
                self.token_expiry = time.time() + (data.get("expires_in", 3600) - 60)
                return self.access_token
            else:
                print(f" Failed to refresh Zoho token: {data}")
        except Exception as e:
            print(f" Connection error during Zoho auth: {e}")
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
        elapsed = time.time() - self._last_call_time
        delay = max(0.0, API_CALL_DELAY - elapsed)
        if delay > 0:
            time.sleep(delay + (random.random() * JITTER_MAX_SECONDS))
        self._last_call_time = time.time()

    def api_call(self, method, endpoint, payload=None, params=None):
        """
        Makes a Zoho Books API call with throttling and auto-retry.
        """
        # Ensure endpoint starts with /
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
            
        url = f"{BASE_URL}{endpoint}"
        if not params:
            params = {}
        
        if not self.org_id:
            # Try reloading org_id from environment if missing
            self.org_id = os.getenv("ORGANIZATION_ID")
            
        params["organization_id"] = self.org_id

        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()

            headers = self.get_headers()
            if not headers:
                return {"code": 1, "message": "Auth Failed"}

            try:
                print(f" Zoho API {method} {url} | Params: {params if params else '{}'}")
                req_headers = headers.copy()
                
                if method == "GET":
                    resp = requests.get(url, headers=req_headers, params=params, timeout=30)
                elif method == "POST":
                    if isinstance(payload, dict) and "JSONString" in payload:
                        req_headers.pop("Content-Type", None)
                        resp = requests.post(url, headers=req_headers, params=params, data=payload, timeout=30)
                    else:
                        resp = requests.post(url, headers=req_headers, params=params, json=payload, timeout=30)
                elif method == "PUT":
                    if isinstance(payload, dict) and "JSONString" in payload:
                        req_headers.pop("Content-Type", None)
                        resp = requests.put(url, headers=req_headers, params=params, data=payload, timeout=30)
                    else:
                        resp = requests.put(url, headers=req_headers, params=params, json=payload, timeout=30)
                else:
                    return {"code": 1, "message": f"Unknown method: {method}"}

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait = int(float(retry_after)) if retry_after else (RATE_LIMIT_BACKOFF * attempt)
                    except Exception:
                        wait = RATE_LIMIT_BACKOFF * attempt
                    time.sleep(wait)
                    continue

                try:
                    result = resp.json()
                    print(f" Zoho Response: {result.get('code')} - {result.get('message')}")
                except:
                    print(f" Zoho Response (Raw): {resp.text[:200]}")
                    return {"code": 1, "message": f"Non-JSON response: {resp.status_code}"}

                if result.get("code") in (429, 57, 58):
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait = int(float(retry_after)) if retry_after else (RATE_LIMIT_BACKOFF * attempt)
                    except Exception:
                        wait = RATE_LIMIT_BACKOFF * attempt
                    time.sleep(wait)
                    continue

                if result.get("code") == 14 or "invalid_token" in str(result.get("message", "")):
                    self.access_token = None
                    continue

                return result

            except Exception as e:
                print(f" Zoho API Error: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue
                return {"code": 1, "message": str(e)}

        return {"code": 1, "message": "Max retries exceeded — Zoho rate limit"}

    def get_reporting_tags(self):
        """Fetch and cache all Zoho Reporting Tags and options"""
        if hasattr(self, '_tags_cache') and self._tags_cache is not None:
            return self._tags_cache
            
        resp = self.api_call("GET", "/settings/tags")
        tags = []
        if resp.get("code") == 0:
            for t in resp.get("reporting_tags", []):
                tag_id = t["tag_id"]
                t_resp = self.api_call("GET", f"/settings/tags/{tag_id}")
                if t_resp.get("code") == 0:
                    options = t_resp.get("reporting_tag", {}).get("tag_options", [])
                    tag_data = {
                        "tag_id": str(tag_id),
                        "tag_name": str(t.get("tag_name", "")).lower(),
                        "options": {str(o.get("tag_option_name", "")).lower(): str(o.get("tag_option_id", "")) for o in options}
                    }
                    tags.append(tag_data)
                
        self._tags_cache = tags
        return tags


# Singleton instance used across the app
zoho = ZohoConnector()
