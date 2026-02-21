"""
Quick test script to verify the tool works with mock data.
Run this to see sample output before having real API access.
"""
from datetime import datetime, timedelta

from src.sp_api_client import get_mock_sales_data
from src.ads_api_client import get_mock_ads_data
from src.metrics import CombinedMetrics, aggregate_weekly
from src.output import export_to_csv, print_report


def main():
    print("[TEST] Amazon Report Tool - Mock Data Test")
    print("=" * 50)

    # Generate 7 days of mock data
    start_date = datetime.now() - timedelta(days=7)
    days = 7

    print(f"\n[DATE] Generating {days} days of mock data starting {start_date.strftime('%Y-%m-%d')}")

    # Get mock data
    sales_data = get_mock_sales_data(start_date, days)
    ads_data = get_mock_ads_data(start_date, days)

    print(f"[OK] Generated {len(sales_data)} sales records")
    print(f"[OK] Generated {len(ads_data)} advertising records")

    # Combine into metrics
    metrics = []
    for sales, ads in zip(sales_data, ads_data):
        metrics.append(CombinedMetrics.from_data(sales, ads))

    # Print daily report
    print("\n" + "=" * 50)
    print("DAILY BREAKDOWN")
    print_report(metrics)

    # Aggregate to weekly
    print("\n" + "=" * 50)
    print("WEEKLY AGGREGATE")
    weekly = aggregate_weekly(metrics)
    print_report([weekly])

    # Export to CSV
    print("\n" + "=" * 50)
    csv_path = export_to_csv(metrics)
    print(f"\n[OK] Daily report exported to: {csv_path}")

    weekly_csv = export_to_csv([weekly], output_path='output/weekly_report.csv')
    print(f"[OK] Weekly report exported to: {weekly_csv}")

    print("\n[SUCCESS] Test completed successfully!")
    print("\nNext steps:")
    print("  1. Get your Refresh Token by authorizing the app in Amazon")
    print("  2. Update .env with the refresh token")
    print("  3. Set USE_SANDBOX=false")
    print("  4. Run: python -m src.main")


if __name__ == '__main__':
    main()
