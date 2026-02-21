"""
Test SP-API Production connection.
"""
from src.config import Config


def main():
    print("[TEST] SP-API Production Connection")
    print("=" * 50)

    print(f"[INFO] Client ID: {Config.SP_API_CLIENT_ID[:20]}...")
    print(f"[INFO] Refresh Token: {Config.SP_API_REFRESH_TOKEN[:20]}...")
    print(f"[INFO] Sandbox: {Config.USE_SANDBOX}")

    from sp_api.api import Sellers
    from sp_api.base import Marketplaces

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    try:
        print("\n[INFO] Connecting to SP-API Production...")
        sellers_api = Sellers(
            credentials=credentials,
            marketplace=Marketplaces.US
        )

        response = sellers_api.get_marketplace_participation()
        print("[OK] Connection successful!")

        if hasattr(response, 'payload') and response.payload:
            for mp in response.payload:
                marketplace = mp.get('marketplace', {})
                store = mp.get('storeName', 'Unknown')
                name = marketplace.get('name', '')
                country = marketplace.get('countryCode', '')
                print(f"[INFO] Store: {store} | Marketplace: {name} ({country})")

        print("\n" + "=" * 50)
        print("[SUCCESS] Production connection working!")
        print("=" * 50)

    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")


if __name__ == '__main__':
    main()
