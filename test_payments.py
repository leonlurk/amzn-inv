"""
Test: Pull payment/financial data from SP-API Production.
"""
import time
import gzip
import csv
import io
import json

import requests
from sp_api.api import Reports, Finances
from sp_api.base import Marketplaces

from datetime import datetime, timedelta
from src.config import Config


def download_report(reports_api, report_type, start_date=None, end_date=None):
    kwargs = {'reportType': report_type}
    if start_date:
        kwargs['dataStartTime'] = start_date.strftime('%Y-%m-%dT00:00:00Z')
    if end_date:
        kwargs['dataEndTime'] = end_date.strftime('%Y-%m-%dT23:59:59Z')

    print(f"  Requesting: {report_type}")
    response = reports_api.create_report(**kwargs)
    report_id = response.payload.get('reportId')

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


def test_finances_api():
    """Test the Finances API directly."""
    print("\n[TEST 1] Finances API - List Financial Event Groups")
    print("-" * 60)

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    try:
        finances = Finances(
            credentials=credentials,
            marketplace=Marketplaces.US,
        )

        # List financial event groups (payment batches)
        response = finances.list_financial_event_groups(
            MaxResultsPerPage=10,
            FinancialEventGroupStartedAfter=(datetime.now() - timedelta(days=90)).isoformat()
        )

        groups = response.payload.get('FinancialEventGroupList', [])
        print(f"[OK] Found {len(groups)} payment groups")

        for g in groups:
            group_id = g.get('FinancialEventGroupId', '')
            start = g.get('FinancialEventGroupStart', '')
            end = g.get('FinancialEventGroupEnd', '')
            status = g.get('ProcessingStatus', '')
            original = g.get('OriginalTotal', {})
            converted = g.get('ConvertedTotal', {})

            amount = original.get('CurrencyAmount', 0)
            currency = original.get('CurrencyCode', 'USD')

            print(f"\n  Group: {group_id}")
            print(f"  Period: {start[:10] if start else 'N/A'} to {end[:10] if end else 'N/A'}")
            print(f"  Status: {status}")
            print(f"  Amount: ${amount} {currency}")

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


def test_financial_reports():
    """Test financial report types."""
    print("\n[TEST 2] Financial Reports")
    print("-" * 60)

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
    start_date = end_date - timedelta(days=30)

    report_types = [
        'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE',
        'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
        'GET_FLAT_FILE_PAYMENT_SETTLEMENT_DATA',
    ]

    for report_type in report_types:
        print(f"\n--- {report_type} ---")
        try:
            raw_text = download_report(reports_api, report_type, start_date, end_date)
            if raw_text:
                print(f"[OK] {len(raw_text)} characters")
                # Try TSV parse
                reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
                rows = list(reader)
                print(f"  Rows: {len(rows)} | Columns: {reader.fieldnames}")
                if rows:
                    print(f"  Sample row 1:")
                    for key, val in rows[0].items():
                        if val:
                            print(f"    {key}: {val}")
        except Exception as e:
            print(f"[SKIP] {type(e).__name__}: {str(e)[:100]}")


def main():
    print("[TEST] Amazon Payments / Financial Data")
    print("=" * 60)

    test_finances_api()
    test_financial_reports()

    print("\n" + "=" * 60)
    print("[DONE]")


if __name__ == '__main__':
    main()
