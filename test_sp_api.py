"""
Test SP-API connection with sandbox credentials.
"""
import os

# Set sandbox environment BEFORE importing sp_api
os.environ['AWS_ENV'] = 'SANDBOX'

from src.config import Config


def test_sp_api_connection():
    print("[TEST] Testing SP-API Connection (Sandbox)")
    print("=" * 50)

    # Check credentials
    print(f"[INFO] Client ID: {Config.SP_API_CLIENT_ID[:20]}...")
    print(f"[INFO] Refresh Token: {Config.SP_API_REFRESH_TOKEN[:20]}...")
    print(f"[INFO] Marketplace: {Config.MARKETPLACE_ID}")
    print(f"[INFO] AWS_ENV: {os.environ.get('AWS_ENV', 'not set')}")

    if not Config.validate_sp_api():
        print("[ERROR] SP-API credentials not valid")
        return False

    print("[OK] Credentials configured")

    try:
        from sp_api.api import Sellers
        from sp_api.base import Marketplaces

        credentials = {
            'lwa_app_id': Config.SP_API_CLIENT_ID,
            'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
            'refresh_token': Config.SP_API_REFRESH_TOKEN,
        }

        # Try to connect using Sellers API (simpler endpoint)
        print("\n[INFO] Attempting to connect to SP-API Sandbox...")

        sellers_api = Sellers(
            credentials=credentials,
            marketplace=Marketplaces.US
        )

        # Try getMarketplaceParticipation - simple API call that works in sandbox
        print("[INFO] Making test API call (get_marketplace_participation)...")
        response = sellers_api.get_marketplace_participation()

        print(f"[OK] API Response received!")

        if hasattr(response, 'payload') and response.payload:
            print(f"[INFO] Marketplaces: {len(response.payload)} found")
            for mp in response.payload[:3]:  # Show first 3
                print(f"       - {mp}")

        print("\n" + "=" * 50)
        print("[SUCCESS] SP-API Sandbox Connection Working!")
        print("=" * 50)
        return True

    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")
        return False


if __name__ == '__main__':
    test_sp_api_connection()
