"""Configuration loader for Amazon API credentials."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)


class Config:
    """Amazon API configuration."""

    # SP-API Credentials
    SP_API_CLIENT_ID = os.getenv('SP_API_CLIENT_ID')
    SP_API_CLIENT_SECRET = os.getenv('SP_API_CLIENT_SECRET')
    SP_API_REFRESH_TOKEN = os.getenv('SP_API_REFRESH_TOKEN')
    SP_API_APP_ID = os.getenv('SP_API_APP_ID')

    # Marketplace
    MARKETPLACE_ID = os.getenv('MARKETPLACE_ID', 'ATVPDKIKX0DER')  # Default US

    # Advertising API Credentials
    ADS_API_CLIENT_ID = os.getenv('ADS_API_CLIENT_ID')
    ADS_API_CLIENT_SECRET = os.getenv('ADS_API_CLIENT_SECRET')
    ADS_API_REFRESH_TOKEN = os.getenv('ADS_API_REFRESH_TOKEN')
    ADS_API_PROFILE_ID = os.getenv('ADS_API_PROFILE_ID')

    # Sandbox mode
    USE_SANDBOX = os.getenv('USE_SANDBOX', 'true').lower() == 'true'

    # Google Sheets
    GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
    GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'Sheet1')

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
