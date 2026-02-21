"""
Test fetching REAL sales data from SP-API Production.
"""
import time
import json
import gzip
import requests
from datetime import datetime, timedelta

from sp_api.api import Reports
from sp_api.base import Marketplaces

from src.config import Config


def main():
    print("[TEST] Fetching Real Sales Data from Production")
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

    # Request last 7 days of sales data
    end_date = datetime.now() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=6)  # 7 days total

    print(f"[INFO] Requesting report: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    try:
        # Step 1: Create report request
        response = reports_api.create_report(
            reportType='GET_SALES_AND_TRAFFIC_REPORT',
            dataStartTime=start_date.strftime('%Y-%m-%dT00:00:00Z'),
            dataEndTime=end_date.strftime('%Y-%m-%dT23:59:59Z'),
            reportOptions={
                'dateGranularity': 'DAY',
                'asinGranularity': 'SKU'
            }
        )

        report_id = response.payload.get('reportId')
        print(f"[OK] Report requested! ID: {report_id}")

        # Step 2: Wait for report to complete
        print("[INFO] Waiting for report to be ready...")
        report_document_id = None
        for attempt in range(30):
            time.sleep(10)
            status_response = reports_api.get_report(report_id)
            status = status_response.payload
            processing_status = status.get('processingStatus')
            print(f"[INFO] Status: {processing_status} (attempt {attempt + 1})")

            if processing_status == 'DONE':
                report_document_id = status.get('reportDocumentId')
                print(f"[OK] Report ready! Document ID: {report_document_id}")
                break
            elif processing_status in ('CANCELLED', 'FATAL'):
                print(f"[ERROR] Report failed: {processing_status}")
                return
        else:
            print("[ERROR] Report timed out after 5 minutes")
            return

        # Step 3: Get the download URL
        print("[INFO] Getting download URL...")
        doc_response = reports_api.get_report_document(report_document_id)
        doc_info = doc_response.payload
        download_url = doc_info.get('url')
        compression = doc_info.get('compressionAlgorithm', '')

        print(f"[INFO] Compression: {compression}")

        # Step 4: Download the actual report content
        print("[INFO] Downloading report content...")
        r = requests.get(download_url)
        r.raise_for_status()

        # Decompress if needed
        if compression == 'GZIP':
            content = gzip.decompress(r.content)
            report_text = content.decode('utf-8')
        else:
            report_text = r.text

        print(f"[OK] Downloaded {len(report_text)} characters")

        # Parse JSON
        report_data = json.loads(report_text)
        print(f"[INFO] Keys: {list(report_data.keys())}")

        # Check for salesAndTrafficByDate
        daily_data = report_data.get('salesAndTrafficByDate', [])
        if daily_data:
            print(f"\n[OK] Found {len(daily_data)} days of data!")
            print("-" * 60)

            for day in daily_data:
                date = day.get('date', 'Unknown')
                sales = day.get('salesByDate', {})
                traffic = day.get('trafficByDate', {})

                revenue_data = sales.get('orderedProductSales', {})
                revenue = revenue_data.get('amount', 0)
                currency = revenue_data.get('currencyCode', 'USD')
                units = sales.get('unitsOrdered', 0)
                orders = sales.get('totalOrderItems', 0)
                sessions = traffic.get('sessions', 0)
                page_views = traffic.get('pageViews', 0)
                conv_rate = traffic.get('unitSessionPercentage', 0)

                print(f"\n[{date}]")
                print(f"  Revenue:    ${revenue} {currency}")
                print(f"  Orders:     {orders}")
                print(f"  Units:      {units}")
                print(f"  Sessions:   {sessions}")
                print(f"  Page Views: {page_views}")
                print(f"  Conv Rate:  {conv_rate}%")
        else:
            print("[WARN] No salesAndTrafficByDate found")
            # Print first 3000 chars of raw data to see structure
            print(json.dumps(report_data, indent=2, default=str)[:3000])

        print("\n" + "=" * 60)
        print("[SUCCESS] Real Amazon data retrieved!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
