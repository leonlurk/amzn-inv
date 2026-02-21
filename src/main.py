"""
Amazon Report Tool - Main Entry Point

Fetches sales and advertising data from Amazon APIs,
calculates metrics, and exports to CSV/Google Sheets.
"""
import argparse
from datetime import datetime, timedelta
from typing import Optional

from .config import Config
from .sp_api_client import SPAPIClient, SalesData, get_mock_sales_data
from .ads_api_client import AmazonAdsClient, AdsData, get_mock_ads_data
from .metrics import CombinedMetrics, aggregate_weekly
from .output import export_to_csv, export_to_google_sheets, print_report


def fetch_data(
    start_date: datetime,
    end_date: datetime,
    use_mock: bool = False
) -> tuple[list[SalesData], list[AdsData]]:
    """
    Fetch sales and advertising data.

    Args:
        start_date: Start of date range
        end_date: End of date range
        use_mock: Use mock data instead of real API calls

    Returns:
        Tuple of (sales_data, ads_data)
    """
    days = (end_date - start_date).days + 1

    if use_mock:
        print("🧪 Using mock data (sandbox mode)...")
        sales_data = get_mock_sales_data(start_date, days)
        ads_data = get_mock_ads_data(start_date, days)
    else:
        print("📡 Fetching data from Amazon APIs...")

        # Fetch sales data
        if Config.validate_sp_api():
            print("  → Fetching SP-API sales data...")
            sp_client = SPAPIClient()
            sales_data = sp_client.fetch_sales_data(start_date, end_date)
        else:
            print("  ⚠️  SP-API not configured, using mock sales data")
            sales_data = get_mock_sales_data(start_date, days)

        # Fetch advertising data
        if Config.validate_ads_api():
            print("  → Fetching Advertising API data...")
            ads_client = AmazonAdsClient()
            ads_data = ads_client.fetch_ads_data(start_date, end_date)
        else:
            print("  ⚠️  Ads API not configured, using mock ads data")
            ads_data = get_mock_ads_data(start_date, days)

    return sales_data, ads_data


def generate_report(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    days: int = 7,
    use_mock: bool = False,
    output_csv: bool = True,
    output_sheets: bool = False,
    spreadsheet_id: Optional[str] = None,
    aggregate: bool = False
) -> list[CombinedMetrics]:
    """
    Generate the Amazon report.

    Args:
        start_date: Start date (default: days ago)
        end_date: End date (default: yesterday)
        days: Number of days to report (if start_date not specified)
        use_mock: Use mock data
        output_csv: Export to CSV
        output_sheets: Export to Google Sheets
        spreadsheet_id: Google Sheets ID
        aggregate: Aggregate into single row (weekly total)

    Returns:
        List of CombinedMetrics
    """
    # Calculate date range
    if end_date is None:
        end_date = datetime.now() - timedelta(days=1)  # Yesterday
    if start_date is None:
        start_date = end_date - timedelta(days=days - 1)

    print(f"\n📅 Report period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("=" * 50)

    # Fetch data
    sales_data, ads_data = fetch_data(start_date, end_date, use_mock)

    # Combine data
    print("\n⚙️  Calculating metrics...")
    metrics = []

    # Match sales and ads data by date
    sales_by_date = {s.date: s for s in sales_data}
    ads_by_date = {a.date: a for a in ads_data}

    all_dates = sorted(set(sales_by_date.keys()) | set(ads_by_date.keys()))

    for date in all_dates:
        sales = sales_by_date.get(date)
        ads = ads_by_date.get(date)

        if sales and ads:
            metrics.append(CombinedMetrics.from_data(sales, ads))
        else:
            print(f"  ⚠️  Missing data for {date}")

    if not metrics:
        print("❌ No data available for the selected period")
        return []

    # Aggregate if requested
    if aggregate and len(metrics) > 1:
        print("📊 Aggregating into weekly totals...")
        metrics = [aggregate_weekly(metrics)]

    # Output
    print("\n📤 Generating output...")

    # Print to console
    print_report(metrics)

    # Export to CSV
    if output_csv:
        csv_path = export_to_csv(metrics)
        print(f"✅ CSV saved: {csv_path}")

    # Export to Google Sheets
    if output_sheets and spreadsheet_id:
        if export_to_google_sheets(metrics, spreadsheet_id):
            print(f"✅ Google Sheets updated")
        else:
            print("❌ Failed to update Google Sheets")

    return metrics


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Amazon Report Tool - Pura Vitalia',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate report for last 7 days with mock data
  python -m src.main --mock

  # Generate report for specific date range
  python -m src.main --start 2026-01-01 --end 2026-01-07

  # Generate weekly aggregate
  python -m src.main --days 7 --aggregate

  # Export to Google Sheets
  python -m src.main --sheets --spreadsheet-id YOUR_SHEET_ID
        """
    )

    parser.add_argument(
        '--start',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to report (default: 7)'
    )
    parser.add_argument(
        '--mock',
        action='store_true',
        help='Use mock data instead of real API calls'
    )
    parser.add_argument(
        '--no-csv',
        action='store_true',
        help='Do not export to CSV'
    )
    parser.add_argument(
        '--sheets',
        action='store_true',
        help='Export to Google Sheets'
    )
    parser.add_argument(
        '--spreadsheet-id',
        type=str,
        help='Google Sheets spreadsheet ID'
    )
    parser.add_argument(
        '--aggregate',
        action='store_true',
        help='Aggregate data into single weekly total'
    )

    args = parser.parse_args()

    # Determine if we should use mock data
    use_mock = args.mock or Config.USE_SANDBOX

    # Generate report
    generate_report(
        start_date=args.start,
        end_date=args.end,
        days=args.days,
        use_mock=use_mock,
        output_csv=not args.no_csv,
        output_sheets=args.sheets,
        spreadsheet_id=args.spreadsheet_id,
        aggregate=args.aggregate
    )


if __name__ == '__main__':
    main()
