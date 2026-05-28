import requests
import sys

# =====================================================
# PASTE YOUR SELF CLIENT DETAILS HERE
# =====================================================
CLIENT_ID = "1000.4906B4KIW4U1R10T8BB1SARC8KOL2E"
CLIENT_SECRET = "bcf8dfdad1cde0157e631bedb6d413526933c7c9f5"
GRANT_CODE = "1000.b1987155cc1f77fec661b565ff268bbb.0d259d82c75c908fc77f919844551d59"

# INDIA DATA CENTER
TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"

# =====================================================
# STEP: EXCHANGE GRANT CODE → TOKENS
# =====================================================
def exchange_grant_code():
    print(" Exchanging grant code for tokens...")

    payload = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": GRANT_CODE,
        "redirect_url" : "https://books.zoho.in/"
    }

    res = requests.post(TOKEN_URL, data=payload)
    print("res - ",res.json())
    print(" Status Code:", res.status_code)
    print(" Raw Response:", res.text)

    if res.status_code != 200:
        print(" Token server error")
        sys.exit(1)

    data = res.json()

    if "refresh_token" not in data:
        print(" Refresh token not generated")
        sys.exit(1)

    print(" Tokens generated successfully")
    print(" ACCESS TOKEN :", data.get("access_token"))
    print(" REFRESH TOKEN:", data.get("refresh_token"))

    return data

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    exchange_grant_code()
