"""
Test writing mock data to Google Sheets.
Writes daily data (one column per day) as requested by Mike.
"""
from datetime import datetime, timedelta

from src.sp_api_client import get_mock_sales_data
from src.ads_api_client import get_mock_ads_data
from src.metrics import CombinedMetrics
from src.output import export_to_google_sheets
from src.config import Config


def main():
    print("[TEST] Writing Daily Mock Data to Google Sheets")
    print("=" * 50)

    # Generate 7 days of mock data
    start_date = datetime.now() - timedelta(days=7)
    days = 7

    print(f"[INFO] Generating {days} days of mock data...")

    sales_data = get_mock_sales_data(start_date, days)
    ads_data = get_mock_ads_data(start_date, days)

    # Combine into daily metrics (one per day)
    daily_metrics = []
    for sales, ads in zip(sales_data, ads_data):
        daily_metrics.append(CombinedMetrics.from_data(sales, ads))

    print(f"[INFO] Generated {len(daily_metrics)} daily records")
    print(f"[INFO] Dates: {daily_metrics[0].date} to {daily_metrics[-1].date}")

    print(f"[INFO] Writing to Google Sheet: {Config.GOOGLE_SHEET_ID}")
    print(f"[INFO] Worksheet: {Config.GOOGLE_SHEET_NAME}")

    # Export daily data to Google Sheets (each column = one day)
    success = export_to_google_sheets(
        daily_metrics,  # Send all daily metrics
        Config.GOOGLE_SHEET_ID,
        Config.GOOGLE_SHEET_NAME
    )

    if success:
        print("\n" + "=" * 50)
        print("[SUCCESS] Daily data written to Google Sheets!")
        print(f"[INFO] {len(daily_metrics)} columns created (one per day)")
        print("Check your spreadsheet now.")
        print("=" * 50)
    else:
        print("\n[ERROR] Failed to write to Google Sheets")


if __name__ == '__main__':
    main()
