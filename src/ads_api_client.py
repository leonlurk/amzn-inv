"""Amazon Advertising API client for fetching PPC/campaign data."""
import json
import time
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
    attributed_orders: int  # Orders from ads
    attributed_revenue: float  # Sales from ads (7-day attribution)
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

    @classmethod
    def from_api_response(cls, data: dict, date: str) -> 'AdsData':
        """Parse API response into AdsData object."""
        spend = float(data.get('cost', data.get('spend', 0)))
        attributed_revenue = float(data.get('sales', data.get('attributedSales7d', 0)))
        attributed_orders = int(data.get('purchases', data.get('attributedConversions7d', 0)))

        # Calculate ACoS and ROAS if not provided
        acos = float(data.get('acos', 0))
        roas = float(data.get('roas', 0))

        if acos == 0 and attributed_revenue > 0:
            acos = round((spend / attributed_revenue) * 100, 2)
        if roas == 0 and spend > 0:
            roas = round(attributed_revenue / spend, 2)

        return cls(
            date=date,
            spend=spend,
            attributed_orders=attributed_orders,
            attributed_revenue=attributed_revenue,
            clicks=int(data.get('clicks', 0)),
            impressions=int(data.get('impressions', 0)),
            acos=acos,
            roas=roas
        )


class AmazonAdsClient:
    """Client for Amazon Advertising API."""

    TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
    API_BASE_URL = 'https://advertising-api.amazon.com'
    SANDBOX_BASE_URL = 'https://advertising-api-test.amazon.com'

    def __init__(self):
        self.client_id = Config.ADS_API_CLIENT_ID
        self.client_secret = Config.ADS_API_CLIENT_SECRET
        self.refresh_token = Config.ADS_API_REFRESH_TOKEN
        self.profile_id = Config.ADS_API_PROFILE_ID
        self.use_sandbox = Config.USE_SANDBOX
        self._access_token = None
        self._token_expiry = None

    @property
    def base_url(self) -> str:
        """Get API base URL based on sandbox setting."""
        return self.SANDBOX_BASE_URL if self.use_sandbox else self.API_BASE_URL

    def _get_access_token(self) -> str:
        """Get or refresh access token."""
        if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._access_token

        response = requests.post(
            self.TOKEN_URL,
            data={
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
            }
        )
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data['access_token']
        self._token_expiry = datetime.now() + timedelta(seconds=token_data.get('expires_in', 3600) - 60)

        return self._access_token

    def _get_headers(self) -> dict:
        """Get headers for API requests."""
        return {
            'Authorization': f'Bearer {self._get_access_token()}',
            'Amazon-Advertising-API-ClientId': self.client_id,
            'Amazon-Advertising-API-Scope': self.profile_id,
            'Content-Type': 'application/json',
        }

    def get_profiles(self) -> list[dict]:
        """Get advertising profiles (needed to get profile_id)."""
        response = requests.get(
            f'{self.base_url}/v2/profiles',
            headers={
                'Authorization': f'Bearer {self._get_access_token()}',
                'Amazon-Advertising-API-ClientId': self.client_id,
            }
        )
        response.raise_for_status()
        return response.json()

    def request_sp_report(
        self,
        start_date: datetime,
        end_date: datetime,
        metrics: Optional[list[str]] = None
    ) -> str:
        """
        Request a Sponsored Products performance report.

        Args:
            start_date: Report start date
            end_date: Report end date
            metrics: List of metrics to include

        Returns:
            Report ID
        """
        if metrics is None:
            metrics = [
                'impressions', 'clicks', 'cost', 'purchases7d',
                'sales7d', 'attributedSales7d', 'attributedConversions7d'
            ]

        payload = {
            'reportDate': start_date.strftime('%Y%m%d'),
            'metrics': ','.join(metrics),
            'segment': 'placement',
            'creativeType': 'all',
        }

        response = requests.post(
            f'{self.base_url}/v2/sp/campaigns/report',
            headers=self._get_headers(),
            json=payload
        )
        response.raise_for_status()

        return response.json().get('reportId')

    def get_report_status(self, report_id: str) -> dict:
        """Check report generation status."""
        response = requests.get(
            f'{self.base_url}/v2/reports/{report_id}',
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()

    def download_report(self, report_url: str) -> list[dict]:
        """Download completed report."""
        response = requests.get(report_url, headers=self._get_headers())
        response.raise_for_status()

        # Reports are gzipped JSON
        import gzip
        import io

        try:
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                return json.loads(f.read().decode('utf-8'))
        except:
            return response.json()

    def fetch_ads_data(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        wait_for_completion: bool = True,
        max_wait_seconds: int = 300
    ) -> list[AdsData]:
        """
        Fetch advertising performance data.

        Args:
            start_date: Report start date
            end_date: Report end date
            wait_for_completion: Whether to wait for report
            max_wait_seconds: Maximum wait time

        Returns:
            List of AdsData objects
        """
        if end_date is None:
            end_date = start_date

        results = []

        # Request reports for each day (API limitation)
        current_date = start_date
        while current_date <= end_date:
            report_id = self.request_sp_report(current_date, current_date)
            print(f"Report requested for {current_date.strftime('%Y-%m-%d')}: {report_id}")

            if wait_for_completion:
                start_time = time.time()
                while time.time() - start_time < max_wait_seconds:
                    status = self.get_report_status(report_id)

                    if status.get('status') == 'SUCCESS':
                        report_url = status.get('location')
                        report_data = self.download_report(report_url)

                        # Aggregate data for the day
                        day_data = self._aggregate_report_data(
                            report_data,
                            current_date.strftime('%Y-%m-%d')
                        )
                        results.append(day_data)
                        break
                    elif status.get('status') == 'FAILURE':
                        print(f"Report failed for {current_date}")
                        break

                    time.sleep(5)

            current_date += timedelta(days=1)

        return results

    def _aggregate_report_data(self, report_data: list[dict], date: str) -> AdsData:
        """Aggregate campaign-level data into daily totals."""
        total_spend = 0.0
        total_sales = 0.0
        total_orders = 0
        total_clicks = 0
        total_impressions = 0

        for campaign in report_data:
            total_spend += float(campaign.get('cost', 0))
            total_sales += float(campaign.get('sales7d', campaign.get('attributedSales7d', 0)))
            total_orders += int(campaign.get('purchases7d', campaign.get('attributedConversions7d', 0)))
            total_clicks += int(campaign.get('clicks', 0))
            total_impressions += int(campaign.get('impressions', 0))

        # Calculate metrics
        acos = (total_spend / total_sales * 100) if total_sales > 0 else 0
        roas = (total_sales / total_spend) if total_spend > 0 else 0

        return AdsData(
            date=date,
            spend=round(total_spend, 2),
            attributed_orders=total_orders,
            attributed_revenue=round(total_sales, 2),
            clicks=total_clicks,
            impressions=total_impressions,
            acos=round(acos, 2),
            roas=round(roas, 2)
        )


# Mock data for sandbox testing
def get_mock_ads_data(start_date: datetime, days: int = 7) -> list[AdsData]:
    """Generate mock advertising data for testing."""
    import random

    results = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        spend = round(random.uniform(50, 200), 2)
        revenue = round(spend * random.uniform(2, 5), 2)  # ROAS between 2-5x
        orders = random.randint(5, 25)

        acos = round((spend / revenue) * 100, 2) if revenue > 0 else 0
        roas = round(revenue / spend, 2) if spend > 0 else 0

        results.append(AdsData(
            date=date.strftime('%Y-%m-%d'),
            spend=spend,
            attributed_orders=orders,
            attributed_revenue=revenue,
            clicks=random.randint(100, 500),
            impressions=random.randint(5000, 20000),
            acos=acos,
            roas=roas
        ))

    return results


if __name__ == '__main__':
    print("Testing with mock advertising data...")
    mock_data = get_mock_ads_data(datetime.now() - timedelta(days=7))
    for data in mock_data:
        print(f"{data.date}: ${data.spend:.2f} spend, ${data.attributed_revenue:.2f} revenue, "
              f"ROAS: {data.roas:.2f}x, ACoS: {data.acos:.1f}%")
