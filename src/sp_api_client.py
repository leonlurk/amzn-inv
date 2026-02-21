"""SP-API client for fetching sales and traffic data."""
import json
import gzip
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces, SellingApiException

from .config import Config


@dataclass
class SalesData:
    """Sales data from SP-API."""
    date: str
    revenue: float
    orders: int
    units: int
    sessions: int
    page_views: int
    conversion_rate: float  # unitSessionPercentage

    @classmethod
    def from_api_response(cls, data: dict) -> 'SalesData':
        """Parse API response into SalesData object."""
        sales = data.get('salesByDate', data.get('salesByAsin', {}))
        traffic = data.get('trafficByDate', data.get('trafficByAsin', {}))

        # Extract sales metrics
        ordered_product_sales = sales.get('orderedProductSales', {})
        revenue = float(ordered_product_sales.get('amount', 0))
        units = int(sales.get('unitsOrdered', 0))
        orders = int(sales.get('totalOrderItems', units))  # fallback to units

        # Extract traffic metrics
        sessions = int(traffic.get('sessions', 0))
        page_views = int(traffic.get('pageViews', 0))
        conversion_rate = float(traffic.get('unitSessionPercentage', 0))

        return cls(
            date=data.get('date', ''),
            revenue=revenue,
            orders=orders,
            units=units,
            sessions=sessions,
            page_views=page_views,
            conversion_rate=conversion_rate
        )


class SPAPIClient:
    """Client for Amazon Selling Partner API."""

    REPORT_TYPE = 'GET_SALES_AND_TRAFFIC_REPORT'

    def __init__(self):
        self.credentials = Config.get_sp_api_credentials()
        self.marketplace = self._get_marketplace()
        self.use_sandbox = Config.USE_SANDBOX

    def _get_marketplace(self) -> Marketplaces:
        """Get marketplace enum from config."""
        marketplace_map = {
            'ATVPDKIKX0DER': Marketplaces.US,
            'A2EUQ1WTGCTBG2': Marketplaces.CA,
            'A1AM78C64UM0Y8': Marketplaces.MX,
        }
        return marketplace_map.get(Config.MARKETPLACE_ID, Marketplaces.US)

    def _get_reports_api(self) -> Reports:
        """Initialize Reports API client."""
        return Reports(
            credentials=self.credentials,
            marketplace=self.marketplace,
        )

    def request_sales_report(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        granularity: str = 'DAY'
    ) -> str:
        """
        Request a sales and traffic report.

        Args:
            start_date: Report start date
            end_date: Report end date (defaults to start_date)
            granularity: DAY, WEEK, or MONTH

        Returns:
            Report ID for checking status
        """
        if end_date is None:
            end_date = start_date

        reports = self._get_reports_api()

        try:
            response = reports.create_report(
                reportType=self.REPORT_TYPE,
                dataStartTime=start_date.strftime('%Y-%m-%dT00:00:00Z'),
                dataEndTime=end_date.strftime('%Y-%m-%dT23:59:59Z'),
                reportOptions={
                    'dateGranularity': granularity,
                    'asinGranularity': 'SKU'
                }
            )
            return response.payload.get('reportId')
        except SellingApiException as e:
            print(f"Error requesting report: {e}")
            raise

    def get_report_status(self, report_id: str) -> dict:
        """Check the status of a report request."""
        reports = self._get_reports_api()

        try:
            response = reports.get_report(report_id)
            return response.payload
        except SellingApiException as e:
            print(f"Error getting report status: {e}")
            raise

    def download_report(self, report_document_id: str) -> dict:
        """Download a completed report from S3 URL."""
        reports = self._get_reports_api()

        try:
            response = reports.get_report_document(report_document_id)
            doc_info = response.payload
            download_url = doc_info.get('url')
            compression = doc_info.get('compressionAlgorithm', '')

            # Download from S3
            r = requests.get(download_url)
            r.raise_for_status()

            # Decompress if GZIP
            if compression == 'GZIP':
                content = gzip.decompress(r.content)
                report_text = content.decode('utf-8')
            else:
                report_text = r.text

            return json.loads(report_text)
        except SellingApiException as e:
            print(f"Error downloading report: {e}")
            raise

    def fetch_sales_data(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        granularity: str = 'DAY',
        wait_for_completion: bool = True,
        max_wait_seconds: int = 300
    ) -> list[SalesData]:
        """
        Fetch sales and traffic data for a date range.

        This is a convenience method that handles the full flow:
        1. Request report
        2. Wait for completion
        3. Download and parse data

        Args:
            start_date: Report start date
            end_date: Report end date
            granularity: DAY, WEEK, or MONTH
            wait_for_completion: Whether to wait for report
            max_wait_seconds: Maximum time to wait

        Returns:
            List of SalesData objects
        """
        import time

        # Request report
        report_id = self.request_sales_report(start_date, end_date, granularity)
        print(f"Report requested: {report_id}")

        if not wait_for_completion:
            return []

        # Wait for completion
        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            status = self.get_report_status(report_id)
            processing_status = status.get('processingStatus')

            if processing_status == 'DONE':
                report_document_id = status.get('reportDocumentId')
                break
            elif processing_status in ('CANCELLED', 'FATAL'):
                raise Exception(f"Report failed with status: {processing_status}")

            print(f"Report status: {processing_status}, waiting...")
            time.sleep(10)
        else:
            raise TimeoutError(f"Report not ready after {max_wait_seconds} seconds")

        # Download and parse
        report_data = self.download_report(report_document_id)

        # Parse response
        results = []
        sales_and_traffic = report_data.get('salesAndTrafficByDate', [])
        for entry in sales_and_traffic:
            results.append(SalesData.from_api_response(entry))

        return results


# Sandbox mock data for testing
def get_mock_sales_data(start_date: datetime, days: int = 7) -> list[SalesData]:
    """Generate mock sales data for sandbox testing."""
    import random

    results = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        sessions = random.randint(100, 500)
        units = random.randint(10, 50)

        results.append(SalesData(
            date=date.strftime('%Y-%m-%d'),
            revenue=round(random.uniform(500, 2000), 2),
            orders=random.randint(8, 40),
            units=units,
            sessions=sessions,
            page_views=sessions * random.randint(2, 4),
            conversion_rate=round((units / sessions) * 100, 2) if sessions > 0 else 0
        ))

    return results


if __name__ == '__main__':
    # Test with mock data
    from datetime import datetime

    print("Testing with mock data...")
    mock_data = get_mock_sales_data(datetime.now() - timedelta(days=7))
    for data in mock_data:
        print(f"{data.date}: ${data.revenue:.2f} revenue, {data.orders} orders, {data.units} units")
