"""
Test: Pull FBA Inventory Adjustment data from SP-API Production.
This replaces the manual CSV export from Seller Central.
"""
import time
import json
import gzip
import csv
import io
from datetime import datetime, timedelta

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from src.config import Config


def download_report(reports_api, report_type, start_date=None, end_date=None):
    """Request, wait, and download a report."""

    kwargs = {'reportType': report_type}
    if start_date:
        kwargs['dataStartTime'] = start_date.strftime('%Y-%m-%dT00:00:00Z')
    if end_date:
        kwargs['dataEndTime'] = end_date.strftime('%Y-%m-%dT23:59:59Z')

    print(f"[INFO] Requesting report: {report_type}")
    response = reports_api.create_report(**kwargs)
    report_id = response.payload.get('reportId')
    print(f"[OK] Report ID: {report_id}")

    # Wait for completion
    for attempt in range(30):
        time.sleep(10)
        status = reports_api.get_report(report_id).payload
        processing_status = status.get('processingStatus')
        print(f"[INFO] Status: {processing_status} (attempt {attempt + 1})")

        if processing_status == 'DONE':
            doc_id = status.get('reportDocumentId')
            break
        elif processing_status in ('CANCELLED', 'FATAL'):
            print(f"[ERROR] Report failed: {processing_status}")
            return None
    else:
        print("[ERROR] Timed out")
        return None

    # Download
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
    print("[TEST] FBA Inventory Data from SP-API")
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

    # Try inventory adjustments report (same data as raw-data.csv)
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=89)  # 90 days like the original

    print(f"\n[INFO] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Report type for FBA inventory adjustments
    report_type = 'GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA'

    try:
        raw_text = download_report(reports_api, report_type, start_date, end_date)

        if raw_text:
            print(f"\n[OK] Downloaded {len(raw_text)} characters")

            # This is TSV data, parse it
            reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
            rows = list(reader)

            print(f"[INFO] Columns: {reader.fieldnames}")
            print(f"[INFO] Total rows: {len(rows)}")

            if rows:
                # Show first 5 rows
                print(f"\n[INFO] Sample rows:")
                for i, row in enumerate(rows[:5]):
                    print(f"\n  Row {i+1}:")
                    for key, val in row.items():
                        if val and val != '0':
                            print(f"    {key}: {val}")

                # Group by ASIN
                asins = {}
                for row in rows:
                    asin = row.get('asin', row.get('ASIN', 'Unknown'))
                    if asin not in asins:
                        asins[asin] = {
                            'title': row.get('sku', row.get('SKU', '')),
                            'count': 0
                        }
                    asins[asin]['count'] += 1

                print(f"\n[INFO] Products found:")
                for asin, info in asins.items():
                    print(f"  {asin}: {info['title']} ({info['count']} rows)")

            print("\n" + "=" * 60)
            print("[SUCCESS] Inventory data retrieved!")
            print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

        # Try alternative report type
        print("\n[INFO] Trying alternative: GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA")
        try:
            raw_text = download_report(reports_api, 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA')

            if raw_text:
                print(f"\n[OK] Downloaded {len(raw_text)} characters")
                reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
                rows = list(reader)

                print(f"[INFO] Columns: {reader.fieldnames}")
                print(f"[INFO] Total rows: {len(rows)}")

                if rows:
                    print(f"\n[INFO] Sample rows:")
                    for i, row in enumerate(rows[:5]):
                        print(f"\n  Row {i+1}:")
                        for key, val in row.items():
                            if val:
                                print(f"    {key}: {val}")

                print("\n" + "=" * 60)
                print("[SUCCESS] Inventory data retrieved!")
                print("=" * 60)

        except Exception as e2:
            print(f"[ERROR] {type(e2).__name__}: {e2}")
            traceback.print_exc()


if __name__ == '__main__':
    main()
