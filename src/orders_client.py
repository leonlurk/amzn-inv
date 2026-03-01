"""Orders client - fetches order payment status via SP-API."""
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sp_api.api import Orders
from sp_api.base import Marketplaces

from .config import Config


@dataclass
class DailyOrders:
    """Order counts for a single day."""
    date: str
    total: int
    paid: int  # Unshipped + PartiallyShipped + Shipped
    pending: int  # Payment not yet authorized
    shipped: int
    unshipped: int
    cancelled: int


class OrdersClient:
    """Client for fetching order data from SP-API."""

    def __init__(self):
        self.credentials = Config.get_sp_api_credentials()
        self.marketplace = Marketplaces.US

    def _get_orders_api(self) -> Orders:
        return Orders(credentials=self.credentials, marketplace=self.marketplace)

    def fetch_orders_by_day(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
    ) -> list[DailyOrders]:
        """
        Fetch orders grouped by day with payment status.

        Amazon order statuses:
        - Pending: Order placed, payment NOT yet authorized
        - Unshipped: Payment authorized, ready to ship
        - PartiallyShipped: Some items shipped
        - Shipped: Fully shipped
        - Cancelled: Order cancelled
        """
        if end_date is None:
            end_date = start_date

        orders_api = self._get_orders_api()
        start_str = start_date.strftime('%Y-%m-%dT00:00:00Z')
        end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')

        print(f"  Fetching orders {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

        # Collect orders grouped by date
        daily_counts = defaultdict(lambda: {
            'Pending': 0, 'Unshipped': 0, 'PartiallyShipped': 0,
            'Shipped': 0, 'Cancelled': 0,
        })

        next_token = None
        page = 0
        total_fetched = 0
        while True:
            page += 1
            if next_token:
                response = orders_api.get_orders(
                    NextToken=next_token,
                    MarketplaceIds=[Config.MARKETPLACE_ID],
                )
            else:
                response = orders_api.get_orders(
                    CreatedAfter=start_str,
                    CreatedBefore=end_str,
                    MarketplaceIds=[Config.MARKETPLACE_ID],
                    MaxResultsPerPage=100,
                )

            payload = response.payload
            orders_list = payload.get('Orders', [])
            total_fetched += len(orders_list)
            print(f"  Orders page {page}: {len(orders_list)} orders")

            for order in orders_list:
                status = order.get('OrderStatus', 'Unknown')
                purchase_date = order.get('PurchaseDate', '')
                # PurchaseDate is ISO format: 2026-02-22T15:30:00Z
                date_key = purchase_date[:10] if purchase_date else 'Unknown'
                if status in daily_counts[date_key]:
                    daily_counts[date_key][status] += 1

            next_token = payload.get('NextToken')
            if not next_token or not orders_list:
                break

        print(f"  Total: {total_fetched} orders across {len(daily_counts)} days")

        # Build daily results
        results = []
        for date in sorted(daily_counts.keys()):
            counts = daily_counts[date]
            paid = counts['Unshipped'] + counts['PartiallyShipped'] + counts['Shipped']
            results.append(DailyOrders(
                date=date,
                total=sum(counts.values()),
                paid=paid,
                pending=counts['Pending'],
                shipped=counts['Shipped'],
                unshipped=counts['Unshipped'],
                cancelled=counts['Cancelled'],
            ))

        return results


def get_mock_daily_orders(start_date: datetime, days: int = 7) -> list[DailyOrders]:
    """Mock daily orders data for testing."""
    import random
    results = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        shipped = random.randint(3, 8)
        unshipped = random.randint(0, 2)
        pending = random.randint(0, 2)
        cancelled = random.randint(0, 1)
        results.append(DailyOrders(
            date=date.strftime('%Y-%m-%d'),
            total=shipped + unshipped + pending + cancelled,
            paid=shipped + unshipped,
            pending=pending,
            shipped=shipped,
            unshipped=unshipped,
            cancelled=cancelled,
        ))
    return results
