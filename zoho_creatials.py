import requests
import sys

# =====================================================
# PASTE YOUR SELF CLIENT DETAILS HERE
# =====================================================
CLIENT_ID = "1000.VA7O75D030OCA8KAJG2BFPUP8RJZEV"
CLIENT_SECRET = "2cf0336459433f86edb62823b9fffa666ebf2c8e64"
GRANT_CODE = "1000.4895cdaa58e4e6442dcf0c11ee14dbf7.01552c6de9121da64d832f0e398a8ef5"

# INDIA DATA CENTER
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

# =====================================================
# STEP: EXCHANGE GRANT CODE → TOKENS
# =====================================================
def exchange_grant_code():
    print("🔑 Exchanging grant code for tokens...")

    payload = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": GRANT_CODE,
        "redirect_url" : "https://books.zoho.com/"
    }

    res = requests.post(TOKEN_URL, data=payload)
    print("res - ",res.json())
    print("🔍 Status Code:", res.status_code)
    print("🔍 Raw Response:", res.text)

    if res.status_code != 200:
        print("❌ Token server error")
        sys.exit(1)

    data = res.json()

    if "refresh_token" not in data:
        print("❌ Refresh token not generated")
        sys.exit(1)

    print("✅ Tokens generated successfully")
    print("📌 ACCESS TOKEN :", data.get("access_token"))
    print("📌 REFRESH TOKEN:", data.get("refresh_token"))

    return data

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    exchange_grant_code()
