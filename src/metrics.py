"""Metrics calculation module - combines SP-API and Ads data."""
from dataclasses import dataclass
from typing import Optional

from .sp_api_client import SalesData
from .ads_api_client import AdsData


@dataclass
class CombinedMetrics:
    """Combined metrics for the weekly report."""

    # Date
    date: str

    # SALES (from SP-API)
    revenue: float
    orders: int
    units: int
    sessions: int  # Added for conversion rate calculation
    conversion_rate: float

    # MEDIA (from Ads API)
    ppc_spend: float
    attributed_orders: int
    attributed_revenue: float
    cpa: float  # Cost Per Acquisition
    roas: float  # Return on Ad Spend
    acos: float  # Advertising Cost of Sales

    # HEALTH (calculated from both)
    tacos: float  # Total Advertising Cost of Sales
    percent_orders_organic: float
    percent_orders_ppc: float
    ad_spend_per_unit: float

    @classmethod
    def from_data(cls, sales: SalesData, ads: AdsData) -> 'CombinedMetrics':
        """
        Create combined metrics from sales and advertising data.

        Args:
            sales: Sales data from SP-API
            ads: Advertising data from Ads API

        Returns:
            CombinedMetrics with all calculated values
        """
        # SALES metrics (direct from SP-API)
        revenue = sales.revenue
        orders = sales.orders
        units = sales.units
        conversion_rate = sales.conversion_rate

        # MEDIA metrics (direct from Ads API)
        ppc_spend = ads.spend
        attributed_orders = ads.attributed_orders
        attributed_revenue = ads.attributed_revenue
        cpa = ads.cpa  # Pre-calculated in AdsData
        roas = ads.roas
        acos = ads.acos

        # HEALTH metrics (calculated)
        tacos = calculate_tacos(ppc_spend, revenue)
        percent_orders_organic = calculate_percent_organic(orders, attributed_orders)
        percent_orders_ppc = calculate_percent_ppc(orders, attributed_orders)
        ad_spend_per_unit = calculate_ad_spend_per_unit(ppc_spend, units)

        return cls(
            date=sales.date,
            revenue=revenue,
            orders=orders,
            units=units,
            sessions=sales.sessions,
            conversion_rate=conversion_rate,
            ppc_spend=ppc_spend,
            attributed_orders=attributed_orders,
            attributed_revenue=attributed_revenue,
            cpa=cpa,
            roas=roas,
            acos=acos,
            tacos=tacos,
            percent_orders_organic=percent_orders_organic,
            percent_orders_ppc=percent_orders_ppc,
            ad_spend_per_unit=ad_spend_per_unit
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for output."""
        return {
            'date': self.date,
            # SALES
            'revenue': self.revenue,
            'orders': self.orders,
            'units': self.units,
            'conversion_rate': self.conversion_rate,
            # MEDIA
            'ppc_spend': self.ppc_spend,
            'attributed_orders': self.attributed_orders,
            'attributed_revenue': self.attributed_revenue,
            'cpa': self.cpa,
            'roas': self.roas,
            'acos': self.acos,
            # HEALTH
            'tacos': self.tacos,
            'percent_orders_organic': self.percent_orders_organic,
            'percent_orders_ppc': self.percent_orders_ppc,
            'ad_spend_per_unit': self.ad_spend_per_unit,
        }

    def to_report_row(self) -> dict:
        """
        Convert to the report format matching the spreadsheet template.

        Returns dict with keys matching the row labels in the template.
        """
        return {
            # SALES
            'Revenue': f"${self.revenue:,.2f}",
            'Orders': str(self.orders),
            'Units': str(self.units),
            'Conv. Rate': f"{self.conversion_rate:.2f}%",
            # MEDIA
            'PPC Spend': f"${self.ppc_spend:,.2f}",
            'Attributed Orders': str(self.attributed_orders),
            'Attributed Revenue': f"${self.attributed_revenue:,.2f}",
            'CPA': f"${self.cpa:.2f}",
            'ROAS': f"{self.roas:.2f}x",
            'ACoS': f"{self.acos:.2f}%",
            # HEALTH
            'TACoS': f"{self.tacos:.2f}%",
            '% Orders Organic': f"{self.percent_orders_organic:.2f}%",
            '% Orders PPC': f"{self.percent_orders_ppc:.2f}%",
            'Ad spend per unit': f"${self.ad_spend_per_unit:.2f}",
        }


# Calculation functions

def calculate_tacos(ad_spend: float, total_revenue: float) -> float:
    """
    Calculate TACoS (Total Advertising Cost of Sales).

    Formula: (Ad Spend / Total Revenue) * 100

    TACoS shows what percentage of your total revenue goes to advertising.
    Lower is better - indicates organic sales are growing.
    """
    if total_revenue <= 0:
        return 0.0
    return round((ad_spend / total_revenue) * 100, 2)


def calculate_percent_organic(total_orders: int, ad_attributed_orders: int) -> float:
    """
    Calculate percentage of orders that are organic (not from ads).

    Formula: ((Total Orders - Ad Orders) / Total Orders) * 100
    """
    if total_orders <= 0:
        return 0.0
    organic_orders = max(0, total_orders - ad_attributed_orders)
    return round((organic_orders / total_orders) * 100, 2)


def calculate_percent_ppc(total_orders: int, ad_attributed_orders: int) -> float:
    """
    Calculate percentage of orders from PPC ads.

    Formula: (Ad Orders / Total Orders) * 100
    """
    if total_orders <= 0:
        return 0.0
    # Cap at 100% in case attribution exceeds total (can happen with multi-touch)
    pct = min(100, (ad_attributed_orders / total_orders) * 100)
    return round(pct, 2)


def calculate_ad_spend_per_unit(ad_spend: float, total_units: int) -> float:
    """
    Calculate advertising spend per unit sold.

    Formula: Ad Spend / Total Units
    """
    if total_units <= 0:
        return 0.0
    return round(ad_spend / total_units, 2)


def calculate_cpa(ad_spend: float, attributed_orders: int) -> float:
    """
    Calculate CPA (Cost Per Acquisition).

    Formula: Ad Spend / Attributed Orders
    """
    if attributed_orders <= 0:
        return 0.0
    return round(ad_spend / attributed_orders, 2)


def calculate_roas(attributed_revenue: float, ad_spend: float) -> float:
    """
    Calculate ROAS (Return on Ad Spend).

    Formula: Attributed Revenue / Ad Spend
    Higher is better. 3x+ is generally considered good.
    """
    if ad_spend <= 0:
        return 0.0
    return round(attributed_revenue / ad_spend, 2)


def calculate_acos(ad_spend: float, attributed_revenue: float) -> float:
    """
    Calculate ACoS (Advertising Cost of Sales).

    Formula: (Ad Spend / Attributed Revenue) * 100
    Lower is better. Inverse of ROAS.
    """
    if attributed_revenue <= 0:
        return 0.0
    return round((ad_spend / attributed_revenue) * 100, 2)


def aggregate_weekly(metrics_list: list[CombinedMetrics]) -> CombinedMetrics:
    """
    Aggregate daily metrics into weekly totals.

    Args:
        metrics_list: List of daily CombinedMetrics

    Returns:
        Single CombinedMetrics with aggregated values
    """
    if not metrics_list:
        raise ValueError("Cannot aggregate empty metrics list")

    # Sum totals
    total_revenue = sum(m.revenue for m in metrics_list)
    total_orders = sum(m.orders for m in metrics_list)
    total_units = sum(m.units for m in metrics_list)
    total_ppc_spend = sum(m.ppc_spend for m in metrics_list)
    total_attributed_orders = sum(m.attributed_orders for m in metrics_list)
    total_attributed_revenue = sum(m.attributed_revenue for m in metrics_list)
    total_sessions = sum(m.sessions for m in metrics_list)

    # Recalculate rates from totals
    conversion_rate = (total_units / total_sessions * 100) if total_sessions > 0 else 0
    cpa = calculate_cpa(total_ppc_spend, total_attributed_orders)
    roas = calculate_roas(total_attributed_revenue, total_ppc_spend)
    acos = calculate_acos(total_ppc_spend, total_attributed_revenue)
    tacos = calculate_tacos(total_ppc_spend, total_revenue)
    percent_organic = calculate_percent_organic(total_orders, total_attributed_orders)
    percent_ppc = calculate_percent_ppc(total_orders, total_attributed_orders)
    ad_spend_per_unit = calculate_ad_spend_per_unit(total_ppc_spend, total_units)

    # Date range
    dates = sorted([m.date for m in metrics_list])
    date_range = f"{dates[0]} to {dates[-1]}"

    return CombinedMetrics(
        date=date_range,
        revenue=round(total_revenue, 2),
        orders=total_orders,
        units=total_units,
        sessions=total_sessions,
        conversion_rate=round(conversion_rate, 2),
        ppc_spend=round(total_ppc_spend, 2),
        attributed_orders=total_attributed_orders,
        attributed_revenue=round(total_attributed_revenue, 2),
        cpa=cpa,
        roas=roas,
        acos=acos,
        tacos=tacos,
        percent_orders_organic=percent_organic,
        percent_orders_ppc=percent_ppc,
        ad_spend_per_unit=ad_spend_per_unit
    )


if __name__ == '__main__':
    # Test calculations
    print("Testing metric calculations...")

    # Mock data
    from .sp_api_client import get_mock_sales_data
    from .ads_api_client import get_mock_ads_data
    from datetime import datetime, timedelta

    start = datetime.now() - timedelta(days=7)
    sales_data = get_mock_sales_data(start)
    ads_data = get_mock_ads_data(start)

    # Combine and print
    for sales, ads in zip(sales_data, ads_data):
        combined = CombinedMetrics.from_data(sales, ads)
        print(f"\n{combined.date}:")
        for key, value in combined.to_report_row().items():
            print(f"  {key}: {value}")
