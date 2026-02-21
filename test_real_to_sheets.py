"""
Test: Real Amazon SP-API data -> Google Sheets
Uses real sales data + mock ads data (Ads API not set up yet).
"""
from datetime import datetime, timedelta

from src.sp_api_client import SPAPIClient, SalesData
from src.ads_api_client import get_mock_ads_data
from src.metrics import CombinedMetrics
from src.output import export_to_google_sheets
from src.config import Config


def main():
    print("[TEST] Real Amazon Data -> Google Sheets")
    print("=" * 60)

    # Step 1: Fetch real sales data from SP-API
    print("\n[STEP 1] Fetching real sales data from Amazon...")
    client = SPAPIClient()

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    print(f"[INFO] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    sales_data = client.fetch_sales_data(
        start_date=start_date,
        end_date=end_date,
        granularity='DAY'
    )

    print(f"[OK] Got {len(sales_data)} days of real sales data!")

    for s in sales_data:
        print(f"  {s.date}: ${s.revenue:.2f} | {s.orders} orders | {s.units} units | {s.sessions} sessions")

    # Step 2: Generate mock ads data (Ads API not set up yet)
    print(f"\n[STEP 2] Generating mock ads data (Ads API pending)...")
    ads_data = get_mock_ads_data(start_date, len(sales_data))
    print(f"[OK] Generated {len(ads_data)} days of mock ads data")

    # Step 3: Combine into metrics
    print(f"\n[STEP 3] Processing metrics...")
    daily_metrics = []
    for sales, ads in zip(sales_data, ads_data):
        daily_metrics.append(CombinedMetrics.from_data(sales, ads))

    print(f"[OK] {len(daily_metrics)} daily metrics created")

    # Preview
    print(f"\n[INFO] Preview:")
    for m in daily_metrics:
        row = m.to_report_row()
        print(f"  {m.date}: Revenue={row['Revenue']} | Orders={row['Orders']} | Conv={row['Conv. Rate']}")

    # Step 4: Write to Google Sheets
    print(f"\n[STEP 4] Writing to Google Sheets...")
    print(f"[INFO] Sheet: {Config.GOOGLE_SHEET_ID}")
    print(f"[INFO] Worksheet: {Config.GOOGLE_SHEET_NAME}")

    success = export_to_google_sheets(
        daily_metrics,
        Config.GOOGLE_SHEET_ID,
        Config.GOOGLE_SHEET_NAME
    )

    if success:
        print("\n" + "=" * 60)
        print("[SUCCESS] Real Amazon data written to Google Sheets!")
        print("=" * 60)
        print("\nSALES data = REAL (from Amazon SP-API)")
        print("MEDIA data = MOCK (Ads API not configured yet)")
        print("HEALTH data = Mixed (uses both)")
    else:
        print("\n[ERROR] Failed to write to Google Sheets")


if __name__ == '__main__':
    main()
