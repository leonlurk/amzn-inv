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

    # SALES (from SP-API + Ads API breakdown)
    total_sales: float
    ppc_sales: float
    attribution_sales: float
    organic_sales: float
    total_units: int
    ppc_units: int
    attribution_units: int
    organic_units: int
    total_orders: int
    sessions: int
    conversion_rate: float

    # MEDIA (from Ads API)
    ppc_spend: float
    attributed_orders: int
    attributed_revenue: float
    cpa: float
    roas: float
    acos: float

    # HEALTH (calculated from both)
    tacos: float
    percent_orders_organic: float
    percent_orders_ppc: float
    ad_spend_per_unit: float

    @classmethod
    def from_data(cls, sales: SalesData, ads: AdsData) -> 'CombinedMetrics':
        """Create combined metrics from sales and advertising data."""
        # SALES breakdown
        total_sales = sales.revenue
        ppc_sales = ads.attributed_revenue
        attribution_sales = 0.0  # Amazon Attribution (external traffic) - not connected
        organic_sales = max(0, total_sales - ppc_sales - attribution_sales)

        total_units = sales.units
        ppc_units = ads.attributed_units
        attribution_units = 0  # Amazon Attribution - not connected
        organic_units = max(0, total_units - ppc_units - attribution_units)

        total_orders = sales.orders

        # MEDIA metrics
        ppc_spend = ads.spend
        attributed_orders = ads.attributed_orders
        attributed_revenue = ads.attributed_revenue
        cpa = ads.cpa
        roas = ads.roas
        acos = ads.acos

        # HEALTH metrics
        tacos = _tacos(ppc_spend, total_sales)
        percent_organic = _pct_organic(total_orders, attributed_orders)
        percent_ppc = _pct_ppc(total_orders, attributed_orders)
        ad_spend_per_unit = _ad_spend_per_unit(ppc_spend, total_units)

        return cls(
            date=sales.date,
            total_sales=total_sales,
            ppc_sales=ppc_sales,
            attribution_sales=attribution_sales,
            organic_sales=organic_sales,
            total_units=total_units,
            ppc_units=ppc_units,
            attribution_units=attribution_units,
            organic_units=organic_units,
            total_orders=total_orders,
            sessions=sales.sessions,
            conversion_rate=sales.conversion_rate,
            ppc_spend=ppc_spend,
            attributed_orders=attributed_orders,
            attributed_revenue=attributed_revenue,
            cpa=cpa,
            roas=roas,
            acos=acos,
            tacos=tacos,
            percent_orders_organic=percent_organic,
            percent_orders_ppc=percent_ppc,
            ad_spend_per_unit=ad_spend_per_unit,
        )

    def to_report_row(self) -> dict:
        """Convert to the report format matching the spreadsheet template."""
        return {
            # SALES
            'Total Sales': f"${self.total_sales:,.2f}",
            'PPC Sales': f"${self.ppc_sales:,.2f}",
            'Attribution Sales': f"${self.attribution_sales:,.2f}",
            'Organic Sales': f"${self.organic_sales:,.2f}",
            'Total Units Sold': str(self.total_units),
            'PPC Units Sold': str(self.ppc_units),
            'Attribution Units Sold': str(self.attribution_units),
            'Organic Units Sold': str(self.organic_units),
            'Total Orders': str(self.total_orders),
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


# Calculation helpers

def _tacos(ad_spend: float, total_revenue: float) -> float:
    if total_revenue <= 0:
        return 0.0
    return round((ad_spend / total_revenue) * 100, 2)


def _pct_organic(total_orders: int, ad_orders: int) -> float:
    if total_orders <= 0:
        return 0.0
    return round((max(0, total_orders - ad_orders) / total_orders) * 100, 2)


def _pct_ppc(total_orders: int, ad_orders: int) -> float:
    if total_orders <= 0:
        return 0.0
    return round(min(100, (ad_orders / total_orders) * 100), 2)


def _ad_spend_per_unit(ad_spend: float, total_units: int) -> float:
    if total_units <= 0:
        return 0.0
    return round(ad_spend / total_units, 2)


def aggregate_weekly(metrics_list: list[CombinedMetrics]) -> CombinedMetrics:
    """Aggregate daily metrics into weekly totals."""
    if not metrics_list:
        raise ValueError("Cannot aggregate empty metrics list")

    total_sales = sum(m.total_sales for m in metrics_list)
    ppc_sales = sum(m.ppc_sales for m in metrics_list)
    attribution_sales = sum(m.attribution_sales for m in metrics_list)
    organic_sales = sum(m.organic_sales for m in metrics_list)
    total_units = sum(m.total_units for m in metrics_list)
    ppc_units = sum(m.ppc_units for m in metrics_list)
    attribution_units = sum(m.attribution_units for m in metrics_list)
    organic_units = sum(m.organic_units for m in metrics_list)
    total_orders = sum(m.total_orders for m in metrics_list)
    total_sessions = sum(m.sessions for m in metrics_list)
    total_spend = sum(m.ppc_spend for m in metrics_list)
    total_attributed_orders = sum(m.attributed_orders for m in metrics_list)
    total_attributed_revenue = sum(m.attributed_revenue for m in metrics_list)

    conversion_rate = (total_units / total_sessions * 100) if total_sessions > 0 else 0
    cpa = round(total_spend / total_attributed_orders, 2) if total_attributed_orders > 0 else 0
    roas = round(total_attributed_revenue / total_spend, 2) if total_spend > 0 else 0
    acos = round((total_spend / total_attributed_revenue) * 100, 2) if total_attributed_revenue > 0 else 0

    dates = sorted([m.date for m in metrics_list])
    date_range = f"{dates[0]} to {dates[-1]}"

    return CombinedMetrics(
        date=date_range,
        total_sales=round(total_sales, 2),
        ppc_sales=round(ppc_sales, 2),
        attribution_sales=round(attribution_sales, 2),
        organic_sales=round(organic_sales, 2),
        total_units=total_units,
        ppc_units=ppc_units,
        attribution_units=attribution_units,
        organic_units=organic_units,
        total_orders=total_orders,
        sessions=total_sessions,
        conversion_rate=round(conversion_rate, 2),
        ppc_spend=round(total_spend, 2),
        attributed_orders=total_attributed_orders,
        attributed_revenue=round(total_attributed_revenue, 2),
        cpa=cpa,
        roas=roas,
        acos=acos,
        tacos=_tacos(total_spend, total_sales),
        percent_orders_organic=_pct_organic(total_orders, total_attributed_orders),
        percent_orders_ppc=_pct_ppc(total_orders, total_attributed_orders),
        ad_spend_per_unit=_ad_spend_per_unit(total_spend, total_units),
    )
