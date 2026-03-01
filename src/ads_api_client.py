"""Amazon Advertising API client for fetching PPC/campaign data (v3 API)."""
import json
import time
import gzip
import io
import requests
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass

from .config import Config


@dataclass
class AdsData:
    """Advertising data from Amazon Ads API."""
    date: str
    spend: float  # PPC Spend
    attributed_orders: int  # Orders from ads (purchases7d)
    attributed_revenue: float  # Sales from ads (sales7d)
    attributed_units: int  # Units sold from ads (unitsSoldClicks7d)
    clicks: int
    impressions: int
    acos: float  # Advertising Cost of Sales (%)
    roas: float  # Return on Ad Spend

    @property
    def cpa(self) -> float:
        """Cost Per Acquisition (calculated)."""
        if self.attributed_orders > 0:
            return round(self.spend / self.attributed_orders, 2)
        return 0.0


class AmazonAdsClient:
    """Client for Amazon Advertising API (v3)."""

    TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
    API_BASE = 'https://advertising-api.amazon.com'

    def __init__(self):
        self.client_id = Config.ADS_API_CLIENT_ID
        self.client_secret = Config.ADS_API_CLIENT_SECRET
        self.refresh_token = Config.ADS_API_REFRESH_TOKEN
        self.profile_id = Config.ADS_API_PROFILE_ID
        self._access_token = None
        self._token_expiry = None

    def _get_access_token(self) -> str:
        """Get or refresh access token."""
        if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._access_token

        response = requests.post(self.TOKEN_URL, data={
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        })
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data['access_token']
        self._token_expiry = datetime.now() + timedelta(seconds=token_data.get('expires_in', 3600) - 60)
        return self._access_token

    def _base_headers(self) -> dict:
        """Base headers for all API requests."""
        return {
            'Authorization': f'Bearer {self._get_access_token()}',
            'Amazon-Advertising-API-ClientId': self.client_id,
            'Amazon-Advertising-API-Scope': self.profile_id,
        }

    def fetch_ads_data(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        max_wait_seconds: int = 900
    ) -> list[AdsData]:
        """
        Fetch advertising performance data using v3 reporting API.

        Creates a single DAILY report for the full date range,
        then aggregates per day across all campaigns.

        Args:
            start_date: Report start date
            end_date: Report end date (default: same as start)
            max_wait_seconds: Maximum wait for report generation

        Returns:
            List of AdsData objects, one per day
        """
        if end_date is None:
            end_date = start_date

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        # Request report
        report_id = self._create_report(start_str, end_str)
        print(f"  Ads report requested: {report_id}")

        # Poll for completion
        report_data = self._wait_for_report(report_id, max_wait_seconds)

        if report_data is None:
            print("  [WARN] Ads report timed out, returning empty data")
            return []

        # Aggregate by date
        return self._aggregate_by_date(report_data)

    def _create_report(self, start_date: str, end_date: str) -> str:
        """Create an async report request."""
        headers = self._base_headers()
        headers['Content-Type'] = 'application/vnd.createasyncreportrequest.v3+json'
        headers['Accept'] = 'application/vnd.createasyncreportrequest.v3+json'

        payload = {
            'name': 'PPC Daily Report',
            'startDate': start_date,
            'endDate': end_date,
            'configuration': {
                'adProduct': 'SPONSORED_PRODUCTS',
                'groupBy': ['campaign'],
                'columns': [
                    'campaignName', 'impressions', 'clicks',
                    'spend', 'sales7d', 'purchases7d',
                    'unitsSoldClicks7d',
                    'date',
                ],
                'reportTypeId': 'spCampaigns',
                'timeUnit': 'DAILY',
                'format': 'GZIP_JSON',
            }
        }

        response = requests.post(
            f'{self.API_BASE}/reporting/reports',
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()['reportId']

    def _wait_for_report(self, report_id: str, max_wait: int) -> Optional[list]:
        """Poll until report is COMPLETED or timeout."""
        headers = self._base_headers()
        start_time = time.time()

        while time.time() - start_time < max_wait:
            response = requests.get(
                f'{self.API_BASE}/reporting/reports/{report_id}',
                headers=headers
            )
            response.raise_for_status()
            status = response.json()

            report_status = status.get('status')
            elapsed = int(time.time() - start_time)
            print(f"  Ads report: {report_status} ({elapsed}s)")

            if report_status == 'COMPLETED':
                return self._download_report(status['url'])
            elif report_status == 'FAILURE':
                print(f"  [ERROR] Report failed: {status.get('failureReason')}")
                return None

            time.sleep(10)

        return None

    def _download_report(self, url: str) -> list:
        """Download and decompress a completed report."""
        response = requests.get(url)
        response.raise_for_status()

        try:
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                return json.loads(f.read().decode('utf-8'))
        except Exception:
            return response.json()

    def _aggregate_by_date(self, records: list[dict]) -> list[AdsData]:
        """Aggregate campaign-level records into daily totals."""
        daily = {}

        for record in records:
            date = record.get('date', '')
            if date not in daily:
                daily[date] = {
                    'spend': 0.0, 'sales': 0.0, 'orders': 0,
                    'units': 0, 'clicks': 0, 'impressions': 0,
                }

            d = daily[date]
            d['spend'] += float(record.get('spend', 0))
            d['sales'] += float(record.get('sales7d', 0))
            d['orders'] += int(record.get('purchases7d', 0))
            d['units'] += int(record.get('unitsSoldClicks7d', 0))
            d['clicks'] += int(record.get('clicks', 0))
            d['impressions'] += int(record.get('impressions', 0))

        results = []
        for date in sorted(daily.keys()):
            d = daily[date]
            spend = round(d['spend'], 2)
            sales = round(d['sales'], 2)
            acos = round((spend / sales) * 100, 2) if sales > 0 else 0
            roas = round(sales / spend, 2) if spend > 0 else 0

            results.append(AdsData(
                date=date,
                spend=spend,
                attributed_orders=d['orders'],
                attributed_revenue=sales,
                attributed_units=d['units'],
                clicks=d['clicks'],
                impressions=d['impressions'],
                acos=acos,
                roas=roas,
            ))

        return results


# Mock data for testing
def get_mock_ads_data(start_date: datetime, days: int = 7) -> list[AdsData]:
    """Generate mock advertising data for testing."""
    import random

    results = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        spend = round(random.uniform(50, 200), 2)
        revenue = round(spend * random.uniform(2, 5), 2)
        orders = random.randint(5, 25)
        units = orders + random.randint(0, 5)

        acos = round((spend / revenue) * 100, 2) if revenue > 0 else 0
        roas = round(revenue / spend, 2) if spend > 0 else 0

        results.append(AdsData(
            date=date.strftime('%Y-%m-%d'),
            spend=spend,
            attributed_orders=orders,
            attributed_revenue=revenue,
            attributed_units=units,
            clicks=random.randint(100, 500),
            impressions=random.randint(5000, 20000),
            acos=acos,
            roas=roas,
        ))

    return results
