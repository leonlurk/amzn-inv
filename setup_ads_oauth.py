"""
Amazon Advertising API OAuth Setup.
Run this once to get your Refresh Token and Profile ID.

Steps:
1. Set ADS_API_CLIENT_ID and ADS_API_CLIENT_SECRET in .env
2. Run: python setup_ads_oauth.py
3. Click the link that appears
4. Authorize in the browser
5. Script captures the token and saves to .env
"""
import os
import sys
import json
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path

import requests
from dotenv import load_dotenv, set_key

ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

REDIRECT_URI = 'https://localhost:3000/callback'
TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
ADS_API_BASE = 'https://advertising-api.amazon.com'

# Will be set by the callback handler
auth_code = None
server_ready = threading.Event()


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handle the OAuth redirect callback."""

    def do_GET(self):
        global auth_code
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if 'code' in params:
            auth_code = params['code'][0]
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
            <html><body style="font-family: Arial; text-align: center; padding: 50px;">
                <h1>Authorization Successful!</h1>
                <p>You can close this tab and return to the terminal.</p>
            </body></html>
            """)
        else:
            error = params.get('error', ['unknown'])[0]
            self.send_response(400)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(f"""
            <html><body style="font-family: Arial; text-align: center; padding: 50px;">
                <h1>Authorization Failed</h1>
                <p>Error: {error}</p>
            </body></html>
            """.encode())

    def log_message(self, format, *args):
        pass  # Suppress server logs


def exchange_code_for_tokens(client_id, client_secret, code):
    """Exchange authorization code for access + refresh tokens."""
    response = requests.post(TOKEN_URL, data={
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
        'client_id': client_id,
        'client_secret': client_secret,
    })
    response.raise_for_status()
    return response.json()


def get_profiles(access_token, client_id):
    """Get advertising profiles to find the Profile ID."""
    response = requests.get(
        f'{ADS_API_BASE}/v2/profiles',
        headers={
            'Authorization': f'Bearer {access_token}',
            'Amazon-Advertising-API-ClientId': client_id,
        }
    )
    response.raise_for_status()
    return response.json()


def manual_flow(client_id, client_secret):
    """Manual flow - user copies the code from the URL bar."""
    auth_url = (
        f'https://www.amazon.com/ap/oa'
        f'?client_id={client_id}'
        f'&scope=advertising::campaign_management'
        f'&response_type=code'
        f'&redirect_uri={REDIRECT_URI}'
    )

    print(f"\n{'=' * 60}")
    print("AMAZON ADVERTISING API - OAUTH SETUP")
    print(f"{'=' * 60}")
    print(f"\nOpen this URL in your browser:\n")
    print(f"  {auth_url}\n")
    print("After authorizing, you'll be redirected to a page that won't load.")
    print("That's OK! Copy the FULL URL from the browser address bar.")
    print(f"It will look like: {REDIRECT_URI}?code=XXXXX\n")

    webbrowser.open(auth_url)

    redirect_url = input("Paste the full redirect URL here: ").strip()

    # Extract the code
    parsed = urlparse(redirect_url)
    params = parse_qs(parsed.query)
    code = params.get('code', [None])[0]

    if not code:
        print("[ERROR] Could not extract authorization code from URL")
        sys.exit(1)

    return code


def main():
    client_id = os.getenv('ADS_API_CLIENT_ID', '').strip()
    client_secret = os.getenv('ADS_API_CLIENT_SECRET', '').strip()

    if not client_id or not client_secret:
        print("[ERROR] Missing credentials!")
        print("Set ADS_API_CLIENT_ID and ADS_API_CLIENT_SECRET in .env first.")
        print("\nFind these in the Amazon Advertising Console:")
        print("  advertising.amazon.com > Settings > API Access")
        sys.exit(1)

    # Use manual flow (redirect goes to https://localhost which won't load,
    # but user can copy the URL with the code)
    code = manual_flow(client_id, client_secret)

    # Exchange code for tokens
    print("\n[2/3] Exchanging code for tokens...")
    try:
        tokens = exchange_code_for_tokens(client_id, client_secret, code)
    except requests.HTTPError as e:
        print(f"[ERROR] Token exchange failed: {e}")
        print(f"Response: {e.response.text}")
        sys.exit(1)

    refresh_token = tokens.get('refresh_token')
    access_token = tokens.get('access_token')

    if not refresh_token:
        print(f"[ERROR] No refresh token in response: {json.dumps(tokens, indent=2)}")
        sys.exit(1)

    print(f"  [OK] Got refresh token: {refresh_token[:20]}...")

    # Save refresh token to .env
    set_key(str(ENV_PATH), 'ADS_API_REFRESH_TOKEN', refresh_token)
    print("  [OK] Saved ADS_API_REFRESH_TOKEN to .env")

    # Get Profile ID
    print("\n[3/3] Getting Advertising Profile ID...")
    try:
        profiles = get_profiles(access_token, client_id)

        if not profiles:
            print("  [WARN] No profiles found. You may need to create campaigns first.")
        else:
            print(f"\n  Found {len(profiles)} profile(s):")
            for p in profiles:
                print(f"\n    Profile ID:   {p.get('profileId')}")
                print(f"    Country:      {p.get('countryCode')}")
                print(f"    Account:      {p.get('accountInfo', {}).get('name', 'N/A')}")
                print(f"    Type:         {p.get('accountInfo', {}).get('type', 'N/A')}")
                print(f"    Marketplace:  {p.get('accountInfo', {}).get('marketplaceStringId', 'N/A')}")

            # Auto-select US profile
            us_profiles = [p for p in profiles if p.get('countryCode') == 'US']
            if us_profiles:
                profile_id = str(us_profiles[0]['profileId'])
                set_key(str(ENV_PATH), 'ADS_API_PROFILE_ID', profile_id)
                print(f"\n  [OK] Saved ADS_API_PROFILE_ID={profile_id} to .env (US profile)")
            else:
                # Just use the first one
                profile_id = str(profiles[0]['profileId'])
                set_key(str(ENV_PATH), 'ADS_API_PROFILE_ID', profile_id)
                print(f"\n  [OK] Saved ADS_API_PROFILE_ID={profile_id} to .env")

    except requests.HTTPError as e:
        print(f"  [ERROR] Could not get profiles: {e}")
        print(f"  Response: {e.response.text}")
        print("  You can manually set ADS_API_PROFILE_ID in .env later.")

    print(f"\n{'=' * 60}")
    print("[DONE] Ads API setup complete!")
    print("Run 'python test_ads_api.py' to verify the connection.")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
