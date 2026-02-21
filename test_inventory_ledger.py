"""
Test: Pull FBA Inventory Ledger reports (Summary + Detail) from SP-API.
These are the SOURCE OF TRUTH for inventory reconciliation.
"""
import time
import gzip
import csv
import io

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from datetime import datetime, timedelta
from src.config import Config


def download_report(reports_api, report_type, start_date=None, end_date=None, options=None):
    kwargs = {'reportType': report_type}
    if start_date:
        kwargs['dataStartTime'] = start_date.strftime('%Y-%m-%dT00:00:00Z')
    if end_date:
        kwargs['dataEndTime'] = end_date.strftime('%Y-%m-%dT23:59:59Z')
    if options:
        kwargs['reportOptions'] = options

    print(f"  Requesting: {report_type}")
    response = reports_api.create_report(**kwargs)
    report_id = response.payload.get('reportId')
    print(f"  Report ID: {report_id}")

    for attempt in range(30):
        time.sleep(10)
        status = reports_api.get_report(report_id).payload
        processing_status = status.get('processingStatus')
        print(f"  Status: {processing_status} (attempt {attempt + 1})")

        if processing_status == 'DONE':
            doc_id = status.get('reportDocumentId')
            break
        elif processing_status in ('CANCELLED', 'FATAL'):
            return None
    else:
        return None

    doc_response = reports_api.get_report_document(doc_id)
    doc_info = doc_response.payload
    download_url = doc_info.get('url')
    compression = doc_info.get('compressionAlgorithm', '')

    r = requests.get(download_url)
    r.raise_for_status()

    if compression == 'GZIP':
        content = gzip.decompress(r.content)
        return content.decode('utf-8')
    return r.text


def main():
    print("[TEST] FBA Inventory Ledger Reports")
    print("=" * 60)

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    reports_api = Reports(
        credentials=credentials,
        marketplace=Marketplaces.US,
    )

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=89)  # 90 days

    # ---- TEST 1: Ledger Summary ----
    print(f"\n[TEST 1] GET_LEDGER_SUMMARY_VIEW_DATA")
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("-" * 60)
    try:
        raw_text = download_report(
            reports_api,
            'GET_LEDGER_SUMMARY_VIEW_DATA',
            start_date,
            end_date,
            options={'aggregateByLocation': 'FC'}
        )
        if raw_text:
            print(f"  [OK] {len(raw_text)} characters")
            reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
            rows = list(reader)
            print(f"  Columns: {reader.fieldnames}")
            print(f"  Rows: {len(rows)}")
            for row in rows:
                print(f"\n  --- {row.get('sku', row.get('SKU', 'Unknown'))} ---")
                for key, val in row.items():
                    if val and val != '0':
                        print(f"    {key}: {val}")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")

    # ---- TEST 2: Ledger Detail (last 7 days, Lavender 32oz) ----
    print(f"\n\n[TEST 2] GET_LEDGER_DETAIL_VIEW_DATA")
    detail_start = end_date - timedelta(days=6)
    print(f"  Date range: {detail_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("-" * 60)
    try:
        raw_text = download_report(
            reports_api,
            'GET_LEDGER_DETAIL_VIEW_DATA',
            detail_start,
            end_date,
            options={'aggregateByLocation': 'FC'}
        )
        if raw_text:
            print(f"  [OK] {len(raw_text)} characters")
            reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
            rows = list(reader)
            print(f"  Columns: {reader.fieldnames}")
            print(f"  Total rows: {len(rows)}")

            # Filter for Lavender 32oz (B0D2JH6H9M)
            lavender_rows = [r for r in rows if 'B0D2JH6H9M' in str(r.values())]
            print(f"\n  Lavender 32oz events: {lavender_rows and len(lavender_rows) or 0}")

            # Show all events
            for row in rows[:30]:  # First 30 rows
                print(f"\n  ---")
                for key, val in row.items():
                    if val:
                        print(f"    {key}: {val}")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")

    # ---- TEST 3: Reserved Inventory ----
    print(f"\n\n[TEST 3] GET_RESERVED_INVENTORY_DATA")
    print("-" * 60)
    try:
        raw_text = download_report(reports_api, 'GET_RESERVED_INVENTORY_DATA')
        if raw_text:
            print(f"  [OK] {len(raw_text)} characters")
            reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
            rows = list(reader)
            print(f"  Columns: {reader.fieldnames}")
            print(f"  Rows: {len(rows)}")
            for row in rows:
                print(f"\n  ---")
                for key, val in row.items():
                    if val and val != '0' and val != '':
                        print(f"    {key}: {val}")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")

    print(f"\n{'=' * 60}")
    print("[DONE]")


if __name__ == '__main__':
    main()
