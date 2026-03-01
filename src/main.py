"""
Amazon Report Tool - Main Entry Point

Fetches sales, advertising, inventory, and order data from Amazon APIs,
calculates metrics, and exports to CSV/Google Sheets.
"""
import argparse
from datetime import datetime, timedelta
from typing import Optional

from .config import Config
from .sp_api_client import SPAPIClient, SalesData, get_mock_sales_data
from .ads_api_client import AmazonAdsClient, AdsData, get_mock_ads_data
from .inventory_client import InventoryClient, InventoryItem, get_mock_inventory
from .orders_client import OrdersClient, DailyOrders, get_mock_daily_orders
from .metrics import CombinedMetrics, aggregate_weekly
from .output import export_to_csv, export_to_google_sheets, print_report


def fetch_data(
    start_date: datetime,
    end_date: datetime,
    use_mock: bool = False
) -> tuple[list[SalesData], list[AdsData]]:
    """Fetch sales and advertising data."""
    days = (end_date - start_date).days + 1

    if use_mock:
        print("Using mock data...")
        return get_mock_sales_data(start_date, days), get_mock_ads_data(start_date, days)

    print("Fetching data from Amazon APIs...")

    # Fetch sales data
    if Config.validate_sp_api():
        print("  Fetching SP-API sales data...")
        sp_client = SPAPIClient()
        sales_data = sp_client.fetch_sales_data(start_date, end_date)
    else:
        print("  [WARN] SP-API not configured, using mock sales data")
        sales_data = get_mock_sales_data(start_date, days)

    # Fetch advertising data
    if Config.validate_ads_api():
        print("  Fetching Ads API data...")
        ads_client = AmazonAdsClient()
        ads_data = ads_client.fetch_ads_data(start_date, end_date)
    else:
        print("  [WARN] Ads API not configured, using mock ads data")
        ads_data = get_mock_ads_data(start_date, days)

    return sales_data, ads_data


def fetch_inventory(use_mock: bool = False) -> list[InventoryItem]:
    """Fetch current FBA inventory."""
    if use_mock:
        return get_mock_inventory()

    if Config.validate_sp_api():
        print("\nFetching inventory data...")
        client = InventoryClient()
        return client.fetch_inventory()
    else:
        print("  [WARN] SP-API not configured, using mock inventory")
        return get_mock_inventory()


def fetch_orders(
    start_date: datetime,
    end_date: datetime,
    use_mock: bool = False
) -> list[DailyOrders]:
    """Fetch daily orders with payment status."""
    days = (end_date - start_date).days + 1

    if use_mock:
        return get_mock_daily_orders(start_date, days)

    if Config.validate_sp_api():
        print("\nFetching orders data...")
        client = OrdersClient()
        return client.fetch_orders_by_day(start_date, end_date)
    else:
        print("  [WARN] SP-API not configured, using mock orders")
        return get_mock_daily_orders(start_date, days)


def generate_report(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    days: int = 7,
    use_mock: bool = False,
    output_csv: bool = True,
    output_sheets: bool = False,
    spreadsheet_id: Optional[str] = None,
    sheet_name: Optional[str] = None,
    aggregate: bool = False,
    include_inventory: bool = True,
    include_orders: bool = True,
) -> list[CombinedMetrics]:
    """Generate the Amazon report."""
    if end_date is None:
        end_date = datetime.now() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=days - 1)

    print(f"\nReport: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("=" * 50)

    # Fetch sales + ads data
    sales_data, ads_data = fetch_data(start_date, end_date, use_mock)

    # Combine data by date
    print("\nCalculating metrics...")
    sales_by_date = {s.date: s for s in sales_data}
    ads_by_date = {a.date: a for a in ads_data}
    all_dates = sorted(set(sales_by_date.keys()) | set(ads_by_date.keys()))

    metrics = []
    for date in all_dates:
        sales = sales_by_date.get(date)
        ads = ads_by_date.get(date)

        if sales and ads:
            metrics.append(CombinedMetrics.from_data(sales, ads))
        elif sales:
            # No ads data for this day - create zero ads
            zero_ads = AdsData(date=date, spend=0, attributed_orders=0,
                               attributed_revenue=0, attributed_units=0,
                               clicks=0, impressions=0, acos=0, roas=0)
            metrics.append(CombinedMetrics.from_data(sales, zero_ads))
        else:
            print(f"  [WARN] Missing sales data for {date}, skipping")

    if not metrics:
        print("[ERROR] No data available")
        return []

    # Aggregate if requested
    if aggregate and len(metrics) > 1:
        print("Aggregating into weekly totals...")
        metrics = [aggregate_weekly(metrics)]

    # Fetch inventory and orders
    inventory = None
    daily_orders = None

    if include_inventory:
        try:
            inventory = fetch_inventory(use_mock)
        except Exception as e:
            print(f"  [WARN] Could not fetch inventory: {e}")

    if include_orders:
        try:
            daily_orders = fetch_orders(start_date, end_date, use_mock)
        except Exception as e:
            print(f"  [WARN] Could not fetch orders: {e}")

    # Output
    print_report(metrics, inventory, daily_orders)

    if output_csv:
        export_to_csv(metrics, inventory=inventory, daily_orders=daily_orders)

    if output_sheets:
        sid = spreadsheet_id or Config.GOOGLE_SHEET_ID
        sname = sheet_name or Config.GOOGLE_SHEET_NAME
        if sid:
            export_to_google_sheets(metrics, sid, sname,
                                    inventory=inventory, daily_orders=daily_orders)
        else:
            print("[WARN] No spreadsheet ID configured")

    return metrics


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description='Amazon Report Tool - Pura Vitalia')

    parser.add_argument('--start', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='Number of days (default: 7)')
    parser.add_argument('--mock', action='store_true', help='Use mock data')
    parser.add_argument('--no-csv', action='store_true', help='Skip CSV export')
    parser.add_argument('--sheets', action='store_true', help='Export to Google Sheets')
    parser.add_argument('--spreadsheet-id', type=str, help='Google Sheets ID')
    parser.add_argument('--sheet-name', type=str, help='Sheet tab name')
    parser.add_argument('--aggregate', action='store_true', help='Aggregate into weekly total')
    parser.add_argument('--no-inventory', action='store_true', help='Skip inventory data')
    parser.add_argument('--no-orders', action='store_true', help='Skip orders data')

    args = parser.parse_args()

    use_mock = args.mock or Config.USE_SANDBOX

    generate_report(
        start_date=args.start,
        end_date=args.end,
        days=args.days,
        use_mock=use_mock,
        output_csv=not args.no_csv,
        output_sheets=args.sheets,
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet_name,
        aggregate=args.aggregate,
        include_inventory=not args.no_inventory,
        include_orders=not args.no_orders,
    )


if __name__ == '__main__':
    main()
