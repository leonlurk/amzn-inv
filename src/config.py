"""Configuration loader for Amazon API credentials.

Supports two sources (in priority order):
1. Streamlit secrets (st.secrets) — used on Streamlit Cloud
2. Environment variables / .env file — used locally
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (no-op on Streamlit Cloud)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)


def _get(key: str, default: str = '') -> str:
    """Get config value from st.secrets first, then env vars."""
    try:
        import streamlit as st
        if key in st.secrets:
            return str(st.secrets[key])
    except (ImportError, Exception):
        pass
    return os.getenv(key, default)


class Config:
    """Amazon API configuration."""

    # SP-API Credentials
    SP_API_CLIENT_ID = _get('SP_API_CLIENT_ID')
    SP_API_CLIENT_SECRET = _get('SP_API_CLIENT_SECRET')
    SP_API_REFRESH_TOKEN = _get('SP_API_REFRESH_TOKEN')
    SP_API_APP_ID = _get('SP_API_APP_ID')

    # Marketplace
    MARKETPLACE_ID = _get('MARKETPLACE_ID', 'ATVPDKIKX0DER')

    # Advertising API Credentials
    ADS_API_CLIENT_ID = _get('ADS_API_CLIENT_ID')
    ADS_API_CLIENT_SECRET = _get('ADS_API_CLIENT_SECRET')
    ADS_API_REFRESH_TOKEN = _get('ADS_API_REFRESH_TOKEN')
    ADS_API_PROFILE_ID = _get('ADS_API_PROFILE_ID')

    # Sandbox mode
    USE_SANDBOX = _get('USE_SANDBOX', 'true').lower() == 'true'

    # Google Sheets
    GOOGLE_SHEET_ID = _get('GOOGLE_SHEET_ID')
    GOOGLE_SHEET_NAME = _get('GOOGLE_SHEET_NAME', 'Sheet1')

    @classmethod
    def validate_sp_api(cls) -> bool:
        """Check if SP-API credentials are configured."""
        required = [cls.SP_API_CLIENT_ID, cls.SP_API_CLIENT_SECRET, cls.SP_API_REFRESH_TOKEN]
        return all(required) and cls.SP_API_REFRESH_TOKEN != 'PENDING_AUTHORIZATION'

    @classmethod
    def validate_ads_api(cls) -> bool:
        """Check if Advertising API credentials are configured."""
        required = [cls.ADS_API_CLIENT_ID, cls.ADS_API_CLIENT_SECRET, cls.ADS_API_REFRESH_TOKEN]
        return all(required)

    @classmethod
    def get_sp_api_credentials(cls) -> dict:
        """Return SP-API credentials as dict for the library."""
        return {
            'lwa_app_id': cls.SP_API_CLIENT_ID,
            'lwa_client_secret': cls.SP_API_CLIENT_SECRET,
            'refresh_token': cls.SP_API_REFRESH_TOKEN,
        }


if __name__ == '__main__':
    print(f"SP-API configured: {Config.validate_sp_api()}")
    print(f"Ads API configured: {Config.validate_ads_api()}")
    print(f"Marketplace: {Config.MARKETPLACE_ID}")
    print(f"Sandbox mode: {Config.USE_SANDBOX}")
