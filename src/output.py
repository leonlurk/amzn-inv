"""Output module for exporting data to CSV and Google Sheets."""
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from .metrics import CombinedMetrics


# Row order matching the template
REPORT_ROWS = [
    # Section: SALES
    'SALES',  # Header
    'Revenue',
    'Orders',
    'Units',
    'Conv. Rate',
    '',  # Empty row
    # Section: MEDIA
    'MEDIA',  # Header
    'PPC Spend',
    'Attributed Orders',
    'Attributed Revenue',
    'CPA',
    'ROAS',
    'ACoS',
    '',  # Empty row
    # Section: HEALTH
    'HEALTH',  # Header
    'TACoS',
    '% Orders Organic',
    '% Orders PPC',
    'Ad spend per unit',
]


def export_to_csv(
    metrics_list: list[CombinedMetrics],
    output_path: Optional[str] = None,
    include_headers: bool = True
) -> str:
    """
    Export metrics to CSV file in the template format.

    Args:
        metrics_list: List of CombinedMetrics (one per period/week)
        output_path: Path to save CSV (default: output/report_YYYYMMDD.csv)
        include_headers: Whether to include the row labels column

    Returns:
        Path to the saved file
    """
    if output_path is None:
        output_dir = Path(__file__).parent.parent / 'output'
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Build the data matrix
    # First column: row labels
    # Subsequent columns: data for each period

    # Header row with dates/periods
    header = [''] + [m.date for m in metrics_list]

    # Build rows
    rows = [header]

    for row_label in REPORT_ROWS:
        row = [row_label]

        if row_label in ('SALES', 'MEDIA', 'HEALTH', ''):
            # Section header or empty row - add empty cells
            row.extend([''] * len(metrics_list))
        else:
            # Data row - get value from each metric
            for metric in metrics_list:
                report_row = metric.to_report_row()
                row.append(report_row.get(row_label, ''))

        rows.append(row)

    # Write CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"CSV exported to: {output_path}")
    return str(output_path)


def export_to_google_sheets(
    metrics_list: list[CombinedMetrics],
    spreadsheet_id: str,
    sheet_name: str = 'Sheet1',
    credentials_path: Optional[str] = None
) -> bool:
    """
    Export metrics to Google Sheets.

    Requires:
    - gspread library
    - Google Service Account credentials JSON file
    - Spreadsheet shared with the service account email

    Args:
        metrics_list: List of CombinedMetrics
        spreadsheet_id: Google Sheets document ID
        sheet_name: Name of the sheet/tab to update
        credentials_path: Path to service account JSON

    Returns:
        True if successful
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Error: gspread and google-auth libraries required.")
        print("Install with: pip install gspread google-auth")
        return False

    # Default credentials path
    if credentials_path is None:
        credentials_path = Path(__file__).parent.parent / 'config' / 'google_credentials.json'

    if not Path(credentials_path).exists():
        print(f"Error: Credentials file not found at {credentials_path}")
        print("Please download your Google Service Account credentials JSON.")
        return False

    # Authenticate
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(credentials)

    # Open spreadsheet
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet {spreadsheet_id} not found or not shared with service account.")
        return False

    # Build data matrix (same as CSV)
    data = []

    # Header row with dates
    header = ['']
    for metric in metrics_list:
        header.append(metric.date)
    data.append(header)

    for row_label in REPORT_ROWS:
        row = [row_label]

        if row_label in ('SALES', 'MEDIA', 'HEALTH', ''):
            row.extend([''] * len(metrics_list))
        else:
            for metric in metrics_list:
                report_row = metric.to_report_row()
                value = report_row.get(row_label, '')
                # Keep formatted strings as-is for Google Sheets
                # This preserves $, %, x formatting
                row.append(value)

        data.append(row)

    # Update sheet
    # Start at A1 and update the range
    try:
        sheet.update('A1', data)
        print(f"Google Sheets updated: {spreadsheet_id}")
        return True
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")
        return False


def print_report(metrics_list: list[CombinedMetrics]):
    """Print a formatted report to console."""
    print("\n" + "=" * 60)
    print("AMAZON WEEKLY REPORT - PURA VITALIA")
    print("=" * 60)

    for metric in metrics_list:
        print(f"\n[PERIOD] {metric.date}")
        print("-" * 40)

        report = metric.to_report_row()

        print("\n[SALES]")
        print(f"   Revenue:      {report['Revenue']}")
        print(f"   Orders:       {report['Orders']}")
        print(f"   Units:        {report['Units']}")
        print(f"   Conv. Rate:   {report['Conv. Rate']}")

        print("\n[MEDIA - PPC]")
        print(f"   PPC Spend:          {report['PPC Spend']}")
        print(f"   Attributed Orders:  {report['Attributed Orders']}")
        print(f"   Attributed Revenue: {report['Attributed Revenue']}")
        print(f"   CPA:                {report['CPA']}")
        print(f"   ROAS:               {report['ROAS']}")
        print(f"   ACoS:               {report['ACoS']}")

        print("\n[HEALTH]")
        print(f"   TACoS:              {report['TACoS']}")
        print(f"   % Orders Organic:   {report['% Orders Organic']}")
        print(f"   % Orders PPC:       {report['% Orders PPC']}")
        print(f"   Ad spend per unit:  {report['Ad spend per unit']}")

    print("\n" + "=" * 60)


if __name__ == '__main__':
    # Test with mock data
    from datetime import datetime, timedelta
    from .sp_api_client import get_mock_sales_data
    from .ads_api_client import get_mock_ads_data
    from .metrics import CombinedMetrics

    print("Testing output module...")

    start = datetime.now() - timedelta(days=7)
    sales_data = get_mock_sales_data(start)
    ads_data = get_mock_ads_data(start)

    metrics = [
        CombinedMetrics.from_data(s, a)
        for s, a in zip(sales_data, ads_data)
    ]

    # Print report
    print_report(metrics)

    # Export CSV
    csv_path = export_to_csv(metrics)
    print(f"\nCSV saved to: {csv_path}")
