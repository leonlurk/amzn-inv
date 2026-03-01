"""Output module for exporting data to CSV and Google Sheets."""
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from .metrics import CombinedMetrics
from .inventory_client import InventoryItem
from .orders_client import DailyOrders


# Row order matching Mike's sheet layout
REPORT_ROWS = [
    # Section: SALES
    'SALES',
    'Total Sales',
    'PPC Sales',
    'Attribution Sales',
    'Organic Sales',
    '',
    'Total Units Sold',
    'PPC Units Sold',
    'Attribution Units Sold',
    'Organic Units Sold',
    '',
    'Total Orders',
    'Conv. Rate',
    '',
    '',
    # Section: MEDIA
    'MEDIA',
    'PPC Spend',
    'Attributed Orders',
    'Attributed Revenue',
    'CPA',
    'ROAS',
    'ACoS',
    '',
    # Section: HEALTH
    'HEALTH',
    'TACoS',
    '% Orders Organic',
    '% Orders PPC',
    'Ad spend per unit',
]

SECTION_HEADERS = {'SALES', 'MEDIA', 'HEALTH', ''}

# Orders rows to display per day
ORDERS_ROWS = [
    ('Total Orders', 'total'),
    ('Paid Orders', 'paid'),
    ('Pending (Not Paid)', 'pending'),
    ('Shipped', 'shipped'),
    ('Unshipped (Paid)', 'unshipped'),
    ('Cancelled', 'cancelled'),
]


def _build_data_matrix(
    metrics_list: list[CombinedMetrics],
    inventory: Optional[list[InventoryItem]] = None,
    daily_orders: Optional[list[DailyOrders]] = None,
) -> list[list]:
    """Build the data matrix for CSV or Google Sheets."""
    data = []
    num_cols = len(metrics_list)

    # Row 1: dates header
    header = [''] + [m.date for m in metrics_list]
    data.append(header)

    # Data rows
    for row_label in REPORT_ROWS:
        row = [row_label]

        if row_label in SECTION_HEADERS:
            row.extend([''] * num_cols)
        else:
            for metric in metrics_list:
                value = metric.to_report_row().get(row_label, '')
                row.append(value)

        data.append(row)

    # ORDERS section (by day, aligned with date columns)
    if daily_orders:
        orders_by_date = {o.date: o for o in daily_orders}
        dates = [m.date for m in metrics_list]

        data.append([''] * (num_cols + 1))
        data.append(['ORDERS'] + [''] * num_cols)

        for label, field in ORDERS_ROWS:
            row = [label]
            for date in dates:
                order = orders_by_date.get(date)
                row.append(str(getattr(order, field, 0)) if order else '0')
            data.append(row)

    # INVENTORY section
    if inventory:
        data.append([''] * (num_cols + 1))
        data.append(['INVENTORY (Current Snapshot)'] + [''] * num_cols)
        data.append(['Product', 'ASIN', 'Available', 'Reserved', 'Unsellable', 'Researching', 'Inbound', 'Total'])
        for item in inventory:
            inbound = item.inbound_working + item.inbound_shipped + item.inbound_receiving
            data.append([
                item.product_name[:40],
                item.asin,
                str(item.fulfillable),
                str(item.reserved),
                str(item.unsellable),
                str(item.researching),
                str(inbound),
                str(item.total_quantity),
            ])
        # Totals row
        data.append([
            'TOTAL', '',
            str(sum(i.fulfillable for i in inventory)),
            str(sum(i.reserved for i in inventory)),
            str(sum(i.unsellable for i in inventory)),
            str(sum(i.researching for i in inventory)),
            str(sum(i.inbound_working + i.inbound_shipped + i.inbound_receiving for i in inventory)),
            str(sum(i.total_quantity for i in inventory)),
        ])

    return data


def export_to_csv(
    metrics_list: list[CombinedMetrics],
    output_path: Optional[str] = None,
    inventory: Optional[list[InventoryItem]] = None,
    daily_orders: Optional[list[DailyOrders]] = None,
) -> str:
    """Export metrics to CSV file."""
    if output_path is None:
        output_dir = Path(__file__).parent.parent / 'output'
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    data = _build_data_matrix(metrics_list, inventory, daily_orders)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(data)

    print(f"CSV exported to: {output_path}")
    return str(output_path)


def export_to_google_sheets(
    metrics_list: list[CombinedMetrics],
    spreadsheet_id: str,
    sheet_name: str = 'Sheet1',
    credentials_path: Optional[str] = None,
    inventory: Optional[list[InventoryItem]] = None,
    daily_orders: Optional[list[DailyOrders]] = None,
) -> bool:
    """Export metrics to Google Sheets."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Error: gspread and google-auth libraries required.")
        return False

    if credentials_path is None:
        credentials_path = Path(__file__).parent.parent / 'config' / 'google_credentials.json'

    if not Path(credentials_path).exists():
        print(f"Error: Credentials file not found at {credentials_path}")
        return False

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(credentials)

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"Error opening spreadsheet: {e}")
        return False

    data = _build_data_matrix(metrics_list, inventory, daily_orders)

    try:
        sheet.clear()
        sheet.update('A1', data)
        print(f"Google Sheets updated: {sheet_name}")
        return True
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")
        return False


def print_report(
    metrics_list: list[CombinedMetrics],
    inventory: Optional[list[InventoryItem]] = None,
    daily_orders: Optional[list[DailyOrders]] = None,
):
    """Print a formatted report to console."""
    print("\n" + "=" * 60)
    print("AMAZON WEEKLY REPORT - PURA VITALIA")
    print("=" * 60)

    for metric in metrics_list:
        print(f"\n[{metric.date}]")
        print("-" * 40)

        report = metric.to_report_row()

        print("\n[SALES]")
        for key in ['Total Sales', 'PPC Sales', 'Attribution Sales', 'Organic Sales']:
            print(f"   {key}: {report[key]}")
        print()
        for key in ['Total Units Sold', 'PPC Units Sold', 'Attribution Units Sold', 'Organic Units Sold']:
            print(f"   {key}: {report[key]}")
        print()
        for key in ['Total Orders', 'Conv. Rate']:
            print(f"   {key}: {report[key]}")

        print("\n[MEDIA]")
        for key in ['PPC Spend', 'Attributed Orders', 'Attributed Revenue', 'CPA', 'ROAS', 'ACoS']:
            print(f"   {key}: {report[key]}")

        print("\n[HEALTH]")
        for key in ['TACoS', '% Orders Organic', '% Orders PPC', 'Ad spend per unit']:
            print(f"   {key}: {report[key]}")

    if daily_orders:
        print(f"\n[ORDERS] By Day")
        print("-" * 40)
        for day in daily_orders:
            print(f"   {day.date}: {day.total} total ({day.paid} paid, {day.pending} pending, {day.cancelled} cancelled)")
        total_all = sum(d.total for d in daily_orders)
        total_paid = sum(d.paid for d in daily_orders)
        total_pending = sum(d.pending for d in daily_orders)
        print(f"   TOTAL: {total_all} orders ({total_paid} paid, {total_pending} pending)")

    if inventory:
        print(f"\n[INVENTORY] Current Snapshot")
        print("-" * 40)
        for item in inventory:
            inbound = item.inbound_working + item.inbound_shipped + item.inbound_receiving
            print(f"   {item.product_name[:35]}")
            print(f"     Available: {item.fulfillable} | Reserved: {item.reserved} | Researching: {item.researching} | Inbound: {inbound} | Total: {item.total_quantity}")
        total_avail = sum(i.fulfillable for i in inventory)
        total_all = sum(i.total_quantity for i in inventory)
        print(f"   TOTAL: {total_avail} available / {total_all} total")

    print("\n" + "=" * 60)
