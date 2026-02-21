"""
Test various FBA inventory report types to find which ones work.
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


REPORT_TYPES = [
    'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA',      # Current inventory
    'GET_FBA_MYI_ALL_INVENTORY_DATA',                # All inventory (incl. suppressed)
    'GET_FBA_INVENTORY_AGED_DATA',                   # Aged inventory
    'GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT',  # Restock recommendations
    'GET_FBA_STORAGE_FEE_CHARGES_DATA',              # Storage fees
    'GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA',     # Inventory health
]


def download_report(reports_api, report_type, start_date=None, end_date=None):
    kwargs = {'reportType': report_type}
    if start_date:
        kwargs['dataStartTime'] = start_date.strftime('%Y-%m-%dT00:00:00Z')
    if end_date:
        kwargs['dataEndTime'] = end_date.strftime('%Y-%m-%dT23:59:59Z')

    response = reports_api.create_report(**kwargs)
    report_id = response.payload.get('reportId')

    for attempt in range(30):
        time.sleep(10)
        status = reports_api.get_report(report_id).payload
        processing_status = status.get('processingStatus')

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
    print("[TEST] Finding Available Inventory Reports")
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

    for report_type in REPORT_TYPES:
        print(f"\n--- {report_type} ---")
        try:
            raw_text = download_report(reports_api, report_type)
            if raw_text:
                reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
                rows = list(reader)
                print(f"[OK] {len(rows)} rows | Columns: {reader.fieldnames}")
                if rows:
                    # Show first row
                    for key, val in rows[0].items():
                        if val:
                            print(f"  {key}: {val}")
            else:
                print("[WARN] No data returned")
        except Exception as e:
            print(f"[SKIP] {type(e).__name__}: {e}")


if __name__ == '__main__':
    main()
