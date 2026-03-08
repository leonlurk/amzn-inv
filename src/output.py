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


def export_settlements_to_sheets(
    spreadsheet_id: str,
    credentials_path: Optional[str] = None,
) -> bool:
    """Export all available settlement periods to Google Sheets, one tab per period.

    Each tab is named by date range (e.g. "Jan28-Feb10") and contains
    every transaction row from that settlement.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Error: gspread and google-auth libraries required.")
        return False

    from .finances_client import FinancesClient

    if credentials_path is None:
        credentials_path = Path(__file__).parent.parent / 'config' / 'google_credentials.json'

    if not Path(credentials_path).exists():
        print(f"Error: Credentials file not found at {credentials_path}")
        return False

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    gc = gspread.authorize(credentials)

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"Error opening spreadsheet: {e}")
        return False

    # Pull all available settlements from API
    print("Fetching settlement reports from Amazon...")
    client = FinancesClient()
    settlements = client.get_latest_settlements(count=10)
    print(f"Found {len(settlements)} settlement periods")

    if not settlements:
        print("No settlements found.")
        return False

    for sel in settlements:
        # Build tab name from date range: "Jan28-Feb10"
        try:
            # Dates come as "2026-02-10 18:56:38 UTC"
            start_str = sel.start_date.replace(' UTC', '').strip()
            end_str = sel.end_date.replace(' UTC', '').strip()
            start = datetime.fromisoformat(start_str)
            end = datetime.fromisoformat(end_str)
            tab_name = f"{start.strftime('%b%d')}-{end.strftime('%b%d')}"
        except (ValueError, AttributeError):
            tab_name = f"Settlement-{sel.settlement_id[-6:]}"

        print(f"\n  {tab_name}: {len(sel.rows)} transactions, payout ${sel.total_amount:.2f}")

        # Build data: header + all rows
        df = sel.rows_as_dataframe()
        header = list(df.columns)
        summary_rows = [
            ['Settlement ID', sel.settlement_id, 'Payout', f'${sel.total_amount:.2f}',
             'Sum of Rows', f'${sel.sum_of_rows:.2f}', 'Reconciled', str(sel.reconciles)],
            [],  # blank separator
            header,
        ]
        data_rows = df.values.tolist()
        all_rows = summary_rows + data_rows

        # Create or get the worksheet tab
        try:
            ws = spreadsheet.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=len(all_rows) + 5, cols=len(header) + 2)

        ws.update(range_name='A1', values=all_rows)
        print(f"    -> Tab '{tab_name}' written ({len(data_rows)} rows)")

    print(f"\nDone! {len(settlements)} tabs written to Google Sheet.")
    return True


def export_reconciliation_to_sheets(
    spreadsheet_id: str,
    credentials_path: Optional[str] = None,
    count: int = 5,
    fetch_order_dates: bool = True,
) -> bool:
    """Export settlement reconciliation to Google Sheets with full detail tabs.

    Creates tabs (per Mike's specification):
    - Reconciliation: A/B/D/E structure with 4a-4h adjustments (summary)
    - 4c Service Fee Timing: Individual timing fee transactions
    - 4e Prior Period Orders: Per-order fees with PurchaseDate detection
    - 4f Fees Not in Sales: Shipping labels, deal fees
    - 4g Cross-Period Refunds: Refunds with PurchaseDate detection
    - 4h Opening Balance: Other adjustments, reimbursements
    - JE Summary: Journal Entry ready format by GL account
    - SKU Sales: Units and revenue by SKU per period
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Error: gspread and google-auth libraries required.")
        return False

    from .finances_client import FinancesClient, GL_ACCOUNTS

    if credentials_path is None:
        credentials_path = Path(__file__).parent.parent / 'config' / 'google_credentials.json'

    if not Path(credentials_path).exists():
        print(f"Error: Credentials file not found at {credentials_path}")
        return False

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    gc = gspread.authorize(credentials)

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"Error opening spreadsheet: {e}")
        return False

    # Pull settlements and run reconciliation WITH order dates for accurate detection
    print("Fetching settlement reports and running reconciliation...")
    if fetch_order_dates:
        print("(Fetching PurchaseDate for accurate 4e/4g detection - this may take a moment)")
    client = FinancesClient()
    settlements = client.get_latest_settlements(count=count)

    if not settlements:
        print("No settlements found.")
        return False

    # Run reconciliation for each settlement and store results
    recon_results = []
    for sel in settlements:
        recon = client.reconcile_settlement(sel, fetch_order_dates=fetch_order_dates)
        recon_results.append((sel, recon))

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1: RECONCILIATION SUMMARY (Cover Sheet)
    # ══════════════════════════════════════════════════════════════════════════
    print("\nWriting Reconciliation tab...")
    recon_rows = [
        ['SETTLEMENT RECONCILIATION - Pura Vitalia'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        ['Order Dates Fetched:', 'Yes' if fetch_order_dates else 'No'],
        [],
        ['Period', 'Settlement ID', 'A - Amazon Payment', 'B - Adjust Payment', 'A-B Net',
         'D - Sales Data', '4a Taxes', '4b Unsettled', '4c Fee Timing', '4d Ad Timing',
         '4e Prior Orders', '4f Non-Sales Fees', '4g Cross Refunds', '4h Opening',
         'E - Total Adj', 'D-E Adjusted', 'Final Diff', 'Status'],
    ]

    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        status = "RECONCILED" if recon.is_reconciled else "DIFF"

        recon_rows.append([
            period, sel.settlement_id,
            recon.amazon_payment, recon.adjust_payment, recon.adjusted_amazon_payment,
            recon.sales_data_total,
            recon.adj_4a_taxes, recon.adj_4b_unsettled_orders, recon.adj_4c_service_fee_timing,
            recon.adj_4d_ad_spend_timing, recon.adj_4e_prior_period_orders,
            recon.adj_4f_fees_not_in_sales, recon.adj_4g_cross_period_refunds,
            recon.adj_4h_opening_balance, recon.total_adjustments,
            recon.adjusted_sales_data, recon.final_difference, status,
        ])

    # Add notes section for exceptions
    has_exceptions = any(not r.is_reconciled or r.exceptions for _, r in recon_results)
    if has_exceptions:
        recon_rows.append([])
        recon_rows.append(['NOTES (Exceptions Only)'])
        note_num = 1
        for sel, recon in recon_results:
            if not recon.is_reconciled:
                period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
                recon_rows.append([f'Note {note_num}:', f'{period} - Final difference ${recon.final_difference:.2f}'])
                note_num += 1
            for exc in recon.exceptions:
                recon_rows.append([f'Note {note_num}:', exc.get('message', 'Unresolved item')])
                note_num += 1

    _write_sheet(spreadsheet, gc, "Reconciliation", recon_rows)
    print(f"  -> Reconciliation tab: {len(settlements)} periods")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2: 4c SERVICE FEE TIMING (Detail)
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing 4c Service Fee Timing tab...")
    rows_4c = [
        ['4c SERVICE FEE TIMING - Per Mike Spec'],
        ['Fees assessed period N, deducted period N+1: Subscription, Storage, Inbound, Disposal, Coupon'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Order ID', 'Amount', 'Description', 'Posted Date', 'Timing Note'],
    ]
    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        for item in recon.detail_4c:
            rows_4c.append([
                period,
                item.get('order_id', ''),
                item.get('amount', 0),
                item.get('description', ''),
                item.get('posted_date', ''),
                item.get('timing_note', ''),
            ])
    _write_sheet(spreadsheet, gc, "4c Service Fee Timing", rows_4c)
    print(f"  -> 4c tab: {sum(len(r.detail_4c) for _, r in recon_results)} items")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3: 4e PRIOR PERIOD ORDERS (Detail)
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing 4e Prior Period Orders tab...")
    rows_4e = [
        ['4e PRIOR PERIOD ORDERS - Per Mike Spec'],
        ['Per-order fees (FBA fulfillment + Commission) on orders settling in this payout'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Order ID', 'Amount', 'Description', 'Posted Date', 'Purchase Date', 'Is Prior Period'],
    ]
    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        for item in recon.detail_4e:
            rows_4e.append([
                period,
                item.get('order_id', ''),
                item.get('amount', 0),
                item.get('description', ''),
                item.get('posted_date', ''),
                item.get('purchase_date', 'N/A'),
                'Yes' if item.get('is_prior_period', True) else 'No',
            ])
    _write_sheet(spreadsheet, gc, "4e Prior Period Orders", rows_4e)
    print(f"  -> 4e tab: {sum(len(r.detail_4e) for _, r in recon_results)} items")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4: 4f FEES NOT IN SALES (Detail)
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing 4f Fees Not in Sales tab...")
    rows_4f = [
        ['4f FEES NOT IN SALES DATA - Per Mike Spec'],
        ['Items with NO sales data entry: Buy Shipping Labels, Amazon Deal fees, Promotions'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Order ID', 'Amount', 'Description', 'Posted Date', 'Note'],
    ]
    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        for item in recon.detail_4f:
            rows_4f.append([
                period,
                item.get('order_id', ''),
                item.get('amount', 0),
                item.get('description', ''),
                item.get('posted_date', ''),
                item.get('note', ''),
            ])
    _write_sheet(spreadsheet, gc, "4f Fees Not in Sales", rows_4f)
    print(f"  -> 4f tab: {sum(len(r.detail_4f) for _, r in recon_results)} items")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5: 4g CROSS-PERIOD REFUNDS (Detail)
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing 4g Cross-Period Refunds tab...")
    rows_4g = [
        ['4g CROSS-PERIOD REFUNDS - Per Mike Spec'],
        ['Refunds for orders from different settlement periods'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Order ID', 'Amount', 'Description', 'Posted Date', 'Purchase Date', 'Is Cross-Period'],
    ]
    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        for item in recon.detail_4g:
            rows_4g.append([
                period,
                item.get('order_id', ''),
                item.get('amount', 0),
                item.get('description', ''),
                item.get('posted_date', ''),
                item.get('purchase_date', 'N/A'),
                'Yes' if item.get('is_cross_period', True) else 'No',
            ])
    _write_sheet(spreadsheet, gc, "4g Cross-Period Refunds", rows_4g)
    print(f"  -> 4g tab: {sum(len(r.detail_4g) for _, r in recon_results)} items")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 6: 4h OPENING BALANCE (Detail)
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing 4h Opening Balance tab...")
    rows_4h = [
        ['4h OPENING BALANCE / OTHER ADJUSTMENTS - Per Mike Spec'],
        ['Pre-data structural adjustments, reimbursements, uncategorized items'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Order ID', 'Amount', 'Description', 'Posted Date', 'Category', 'Note'],
    ]
    for sel, recon in recon_results:
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"
        for item in recon.detail_4h:
            rows_4h.append([
                period,
                item.get('order_id', ''),
                item.get('amount', 0),
                item.get('description', ''),
                item.get('posted_date', ''),
                item.get('category', ''),
                item.get('note', ''),
            ])
    _write_sheet(spreadsheet, gc, "4h Opening Balance", rows_4h)
    print(f"  -> 4h tab: {sum(len(r.detail_4h) for _, r in recon_results)} items")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 7: JE SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing JE Summary tab...")
    je_rows = [
        ['JOURNAL ENTRY SUMMARY - By GL Account'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'Account', 'Account Name', 'Debit', 'Credit', 'Description'],
    ]

    for sel, _ in recon_results:
        je_data = sel.je_summary()
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"

        for je in je_data:
            je_rows.append([
                period, je['account'], je['name'],
                je['debit'], je['credit'], je['description'],
            ])
        je_rows.append([])  # Blank row between periods

    _write_sheet(spreadsheet, gc, "JE Summary", je_rows)
    print(f"  -> JE Summary tab: {len(settlements)} periods")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 8: SKU SALES
    # ══════════════════════════════════════════════════════════════════════════
    print("Writing SKU Sales tab...")
    sku_rows = [
        ['SKU SALES BY PERIOD'],
        ['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')],
        [],
        ['Period', 'SKU', 'Units Sold', 'Gross Revenue', 'Refund Units',
         'Refund Amount', 'Net Units', 'Net Revenue'],
    ]

    for sel, _ in recon_results:
        sku_data = sel.sku_sales_summary()
        period = f"{sel.start_date[:10]} to {sel.end_date[:10]}"

        for sku in sku_data:
            sku_rows.append([
                period, sku['sku'], sku['units_sold'], sku['gross_revenue'],
                sku['refund_units'], sku['refund_amount'],
                sku['net_units'], sku['net_revenue'],
            ])
        sku_rows.append([])  # Blank row between periods

    _write_sheet(spreadsheet, gc, "SKU Sales", sku_rows)
    print(f"  -> SKU Sales tab: {len(settlements)} periods")

    print(f"\nDone! Exported to Google Sheet:")
    print(f"  - Reconciliation (summary)")
    print(f"  - 4c Service Fee Timing (detail)")
    print(f"  - 4e Prior Period Orders (detail)")
    print(f"  - 4f Fees Not in Sales (detail)")
    print(f"  - 4g Cross-Period Refunds (detail)")
    print(f"  - 4h Opening Balance (detail)")
    print(f"  - JE Summary")
    print(f"  - SKU Sales")
    return True


def _write_sheet(spreadsheet, gc, tab_name: str, rows: list):
    """Helper to write rows to a Google Sheets tab, creating if needed."""
    import gspread
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=max(len(rows) + 10, 100), cols=15)
    ws.update(range_name='A1', values=rows)


def _flatten_financial_events(events: dict) -> list[list]:
    """Flatten financial events API response into tabular rows.

    Returns list of [Date, Type, Order ID, SKU, Qty, Category, Description, Amount].
    """
    rows = []

    def _amt(obj, key='CurrencyAmount'):
        try:
            return float(obj.get(key, 0))
        except (TypeError, ValueError, AttributeError):
            return 0.0

    # Shipment events (orders — revenue + fees per item)
    for s in events.get('ShipmentEventList', []):
        date = (s.get('PostedDate') or '')[:10]
        order_id = s.get('AmazonOrderId', '')
        for item in s.get('ShipmentItemList', []):
            sku = item.get('SellerSKU', '')
            qty = item.get('QuantityShipped', 0)
            for charge in item.get('ItemChargeList', []):
                amt = _amt(charge.get('ChargeAmount', {}))
                if amt != 0:
                    ctype = charge.get('ChargeType', '')
                    cat = 'Tax' if 'Tax' in ctype else ('Shipping' if 'Shipping' in ctype else 'Revenue')
                    rows.append([date, 'Order', order_id, sku, qty, cat, ctype, amt])
            for fee in item.get('ItemFeeList', []):
                amt = _amt(fee.get('FeeAmount', {}))
                if amt != 0:
                    rows.append([date, 'Order', order_id, sku, qty, 'Fee', fee.get('FeeType', ''), amt])
            for promo in item.get('PromotionList', []):
                amt = _amt(promo.get('PromotionAmount', {}))
                if amt != 0:
                    rows.append([date, 'Order', order_id, sku, qty, 'Promotion', promo.get('PromotionType', promo.get('PromotionId', '')), amt])

    # Refund events
    for r in events.get('RefundEventList', []):
        date = (r.get('PostedDate') or '')[:10]
        order_id = r.get('AmazonOrderId', '')
        for item in r.get('ShipmentItemAdjustmentList', r.get('ShipmentItemList', [])):
            sku = item.get('SellerSKU', '')
            qty = item.get('QuantityShipped', 0)
            for charge in item.get('ItemChargeAdjustmentList', item.get('ItemChargeList', [])):
                amt = _amt(charge.get('ChargeAmount', {}))
                if amt != 0:
                    ctype = charge.get('ChargeType', '')
                    cat = 'Tax' if 'Tax' in ctype else 'Refund'
                    rows.append([date, 'Refund', order_id, sku, qty, cat, ctype, amt])
            for fee in item.get('ItemFeeAdjustmentList', item.get('ItemFeeList', [])):
                amt = _amt(fee.get('FeeAmount', {}))
                if amt != 0:
                    rows.append([date, 'Refund', order_id, sku, qty, 'Fee Reversal', fee.get('FeeType', ''), amt])
            for promo in item.get('PromotionAdjustmentList', item.get('PromotionList', [])):
                amt = _amt(promo.get('PromotionAmount', {}))
                if amt != 0:
                    rows.append([date, 'Refund', order_id, sku, qty, 'Promo Reversal', promo.get('PromotionType', promo.get('PromotionId', '')), amt])

    # Advertising payments
    for a in events.get('ProductAdsPaymentEventList', []):
        date = (a.get('postedDate') or '')[:10]
        amt = _amt(a.get('transactionValue', {}))
        if amt != 0:
            rows.append([date, 'Advertising', '', '', 0, 'Ad Spend',
                         a.get('invoiceId', a.get('transactionType', '')), amt])

    # Service fees
    for sf in events.get('ServiceFeeEventList', []):
        date = (sf.get('PostedDate') or sf.get('postedDate') or '')[:10]
        order_id = sf.get('AmazonOrderId', sf.get('amazonOrderId', ''))
        reason = sf.get('FeeReason', sf.get('feeReason', ''))
        for fee in sf.get('FeeList', sf.get('feeList', [])):
            amt = _amt(fee.get('FeeAmount', fee.get('feeAmount', {})))
            if amt != 0:
                rows.append([date, 'ServiceFee', order_id, '', 0, 'Service Fee',
                             fee.get('FeeType', fee.get('feeType', reason)), amt])

    # Adjustments
    for adj in events.get('AdjustmentEventList', []):
        date = (adj.get('PostedDate') or '')[:10]
        adj_type = adj.get('AdjustmentType', '')
        for item in adj.get('AdjustmentItemList', []):
            amt = _amt(item.get('TotalAmount', {}))
            if amt != 0:
                rows.append([date, 'Adjustment', '', item.get('SellerSKU', item.get('ASIN', '')),
                             int(item.get('Quantity', 0) or 0), 'Adjustment', adj_type, amt])

    # Coupons
    for c in events.get('CouponPaymentEventList', []):
        date = (c.get('PostedDate') or '')[:10]
        amt = _amt(c.get('CouponValue', c.get('TotalAmount', {})))
        if amt != 0:
            rows.append([date, 'Coupon', '', '', 0, 'Coupon',
                         c.get('CouponId', c.get('PaymentEventId', '')), amt])

    # Debt recovery
    for d in events.get('DebtRecoveryEventList', []):
        date = (d.get('PostedDate') or '')[:10]
        for item in d.get('DebtRecoveryItemList', []):
            amt = _amt(item.get('RecoveryAmount', {}))
            if amt != 0:
                rows.append([date, 'DebtRecovery', '', '', 0, 'Debt Recovery',
                             d.get('DebtRecoveryType', ''), amt])
        amt = _amt(d.get('OverPaymentCredit', {}))
        if amt != 0:
            rows.append([date, 'DebtRecovery', '', '', 0, 'Overpayment Credit', '', amt])

    return rows


def export_financial_history_to_sheets(
    spreadsheet_id: str,
    start_year: int = 2025,
    start_month: int = 1,
    credentials_path: Optional[str] = None,
) -> bool:
    """Export full financial history to Google Sheets, one tab per month.

    Uses listFinancialEvents API which goes back 1+ year.
    Each tab (e.g. "2025-01 Jan") contains every transaction flattened to rows.
    """
    import time

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Error: gspread and google-auth libraries required.")
        return False

    from sp_api.api import Finances as FinancesAPI
    from sp_api.base import Marketplaces
    from .config import Config

    if credentials_path is None:
        credentials_path = Path(__file__).parent.parent / 'config' / 'google_credentials.json'

    if not Path(credentials_path).exists():
        print(f"Error: Credentials file not found at {credentials_path}")
        return False

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    gc = gspread.authorize(creds)

    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"Error opening spreadsheet: {e}")
        return False

    fin = FinancesAPI(credentials=Config.get_sp_api_credentials(), marketplace=Marketplaces.US)

    header = ['Date', 'Type', 'Order ID', 'SKU', 'Qty', 'Category', 'Description', 'Amount']
    now = datetime.now()
    month_cursor = datetime(start_year, start_month, 1)
    tabs_written = 0

    while month_cursor <= now:
        # Calculate month boundaries
        year, month = month_cursor.year, month_cursor.month
        if month == 12:
            end_dt = datetime(year + 1, 1, 1)
        else:
            end_dt = datetime(year, month + 1, 1)

        tab_name = month_cursor.strftime('%Y-%m %b')
        print(f"\n  {tab_name}: fetching...", end='', flush=True)

        # Paginate through all events for this month
        all_events = {}
        next_token = None
        pages = 0
        while True:
            pages += 1
            kwargs = {
                'PostedAfter': month_cursor.strftime('%Y-%m-%dT00:00:00Z'),
                'PostedBefore': end_dt.strftime('%Y-%m-%dT00:00:00Z'),
                'MaxResultsPerPage': 100,
            }
            if next_token:
                kwargs['NextToken'] = next_token

            response = fin.list_financial_events(**kwargs)
            events = response.payload.get('FinancialEvents', {})

            for key, val in events.items():
                if isinstance(val, list) and val:
                    all_events.setdefault(key, []).extend(val)

            next_token = response.payload.get('NextToken') or getattr(response, 'next_token', None)
            if not next_token:
                break
            time.sleep(1)  # rate limit

        # Flatten to rows
        data_rows = _flatten_financial_events(all_events)
        data_rows.sort(key=lambda r: r[0])  # sort by date

        # Summary stats
        total_revenue = sum(r[7] for r in data_rows if r[5] == 'Revenue')
        total_fees = sum(r[7] for r in data_rows if r[5] in ('Fee', 'Service Fee'))
        total_ads = sum(r[7] for r in data_rows if r[5] == 'Ad Spend')
        total_refunds = sum(r[7] for r in data_rows if r[1] == 'Refund' and r[5] == 'Refund')
        net = sum(r[7] for r in data_rows)

        summary = [
            ['Month', tab_name, 'Rows', str(len(data_rows)),
             'Revenue', f'${total_revenue:.2f}', 'Fees', f'${total_fees:.2f}',
             'Ads', f'${total_ads:.2f}', 'Refunds', f'${total_refunds:.2f}',
             'Net', f'${net:.2f}'],
            [],
            header,
        ]
        all_rows = summary + data_rows

        # Write to sheet
        try:
            ws = spreadsheet.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=tab_name, rows=len(all_rows) + 5, cols=len(header) + 8)

        ws.update(range_name='A1', values=all_rows)
        tabs_written += 1
        print(f" {len(data_rows)} rows, {pages} pages | Rev ${total_revenue:.2f} | Fees ${total_fees:.2f} | Ads ${total_ads:.2f} | Net ${net:.2f}")

        # Next month
        month_cursor = end_dt
        time.sleep(2)  # rate limit between months

    print(f"\nDone! {tabs_written} monthly tabs written to Google Sheet.")
    return True
