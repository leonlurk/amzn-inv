"""
Test full pipeline: SP-API Sandbox -> Process -> Google Sheets
Sandbox has limited report support, so we'll test what works.
"""
import os
os.environ['AWS_ENV'] = 'SANDBOX'

from datetime import datetime, timedelta
from src.config import Config


def test_reports_api():
    """Test Reports API with sandbox-supported operations."""
    print("\n[TEST] Reports API Sandbox Test")
    print("=" * 60)

    from sp_api.api import Reports
    from sp_api.base import Marketplaces

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    reports_api = Reports(
        credentials=credentials,
        marketplace=Marketplaces.US
    )

    # Try to get list of reports (this usually works in sandbox)
    print("[INFO] Getting list of reports...")
    try:
        response = reports_api.get_reports(
            reportTypes=['GET_MERCHANT_LISTINGS_ALL_DATA'],
            processingStatuses=['DONE']
        )
        print(f"[OK] Reports list: {response}")
        return True
    except Exception as e:
        print(f"[INFO] get_reports error: {e}")

    return False


def test_with_mock_data_to_sheets():
    """Since sandbox is limited, demonstrate the full flow with mock data."""
    print("\n[TEST] Full Pipeline with Mock Data -> Google Sheets")
    print("=" * 60)

    from src.sp_api_client import get_mock_sales_data
    from src.ads_api_client import get_mock_ads_data
    from src.metrics import CombinedMetrics
    from src.output import export_to_google_sheets

    # Generate mock data (simulating what we'd get from API)
    start_date = datetime.now() - timedelta(days=7)
    days = 7

    print(f"[INFO] Generating {days} days of data...")
    sales_data = get_mock_sales_data(start_date, days)
    ads_data = get_mock_ads_data(start_date, days)

    # Process into metrics
    print("[INFO] Processing metrics...")
    daily_metrics = []
    for sales, ads in zip(sales_data, ads_data):
        daily_metrics.append(CombinedMetrics.from_data(sales, ads))

    # Show sample of what we'd send
    print("\n[INFO] Sample data (Day 1):")
    sample = daily_metrics[0].to_report_row()
    for key, value in list(sample.items())[:5]:
        print(f"       {key}: {value}")
    print("       ...")

    # Write to Google Sheets
    print(f"\n[INFO] Writing to Google Sheets...")
    success = export_to_google_sheets(
        daily_metrics,
        Config.GOOGLE_SHEET_ID,
        Config.GOOGLE_SHEET_NAME
    )

    if success:
        print("\n" + "=" * 60)
        print("[SUCCESS] Pipeline test complete!")
        print("=" * 60)
        print("\nData flow verified:")
        print("  [Mock Data] -> [Metrics Processing] -> [Google Sheets]")
        print("\nWith Production access, the flow will be:")
        print("  [Amazon SP-API] -> [Metrics Processing] -> [Google Sheets]")
        return True

    return False


def main():
    print("[TEST] Amazon Report Tool - Pipeline Test")
    print("=" * 60)

    # Test 1: Verify SP-API connection still works
    print("\n[STEP 1] Verifying SP-API connection...")
    from sp_api.api import Sellers
    from sp_api.base import Marketplaces

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    try:
        sellers_api = Sellers(credentials=credentials, marketplace=Marketplaces.US)
        response = sellers_api.get_marketplace_participation()
        print("[OK] SP-API connection verified!")
    except Exception as e:
        print(f"[ERROR] SP-API connection failed: {e}")
        return

    # Test 2: Try Reports API
    test_reports_api()

    # Test 3: Full pipeline with mock data to sheets
    test_with_mock_data_to_sheets()


if __name__ == '__main__':
    main()
