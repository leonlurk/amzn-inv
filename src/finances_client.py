"""Finances client - pulls settlement reports and financial events via SP-API."""
import csv
import io
import gzip
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import requests
from sp_api.api import Reports, Finances
from sp_api.base import Marketplaces, SellingApiException

from .config import Config


@dataclass
class SettlementRow:
    """Single transaction row from a settlement report."""
    settlement_id: str
    settlement_start: str
    settlement_end: str
    deposit_date: str
    total_amount: float  # header-level: same on every row
    currency: str
    transaction_type: str  # Order, Refund, other-transaction, etc.
    order_id: str
    amount_type: str  # ItemPrice, ItemFees, Promotion, Other
    amount_description: str  # Principal, Commission, FBAPerOrderFulfillmentFee, etc.
    amount: float  # the actual transaction amount
    sku: str
    quantity: int
    posted_date: str


@dataclass
class SettlementSummary:
    """Summarized settlement period with categorized totals."""
    settlement_id: str
    start_date: str
    end_date: str
    deposit_date: str
    total_amount: float  # the payout (bank deposit)
    currency: str
    rows: list[SettlementRow] = field(default_factory=list)

    # Categorized totals (calculated from rows)
    # Categorized totals matching Amazon Seller Central Sankey exactly.
    # Mapping verified against real settlement 2/10-2/24 ($255.80 payout).
    #
    # REVENUE SIDE ("Sales" in Sankey):
    product_charges: float = 0.0       # Order/ItemPrice/Principal
    shipping_revenue: float = 0.0      # Order/ItemPrice/Shipping (excl tax)
    inventory_reimbursements: float = 0.0  # other-transaction/FBA Inventory Reimbursement
    refunded_expenses: float = 0.0     # Refund/ItemFees (all — fee reversals)
    #
    # REFUNDS:
    refunded_sales: float = 0.0        # Refund/ItemPrice/Principal
    #
    # EXPENSES:
    promo_rebates: float = 0.0         # Order/Promotion (all)
    fba_fees: float = 0.0              # other-transaction: Inbound/Placement fees
    advertising_costs: float = 0.0     # ServiceFee/Cost of Advertising
    shipping_charges: float = 0.0      # other-transaction: Shipping labels + Adjustment
    amazon_fees: float = 0.0           # Order/ItemFees (ALL) + Subscription Fee
    other_fees: float = 0.0            # Anything else
    #
    # TAX (nets to zero — excluded from Sankey):
    tax_collected: float = 0.0
    tax_withheld: float = 0.0
    #
    other_income: float = 0.0

    @property
    def sum_of_rows(self) -> float:
        """Sum all individual amount rows."""
        return round(sum(r.amount for r in self.rows), 2)

    @property
    def reconciles(self) -> bool:
        """Does the sum of rows match the total payout?"""
        return abs(self.sum_of_rows - self.total_amount) < 0.01

    @property
    def unique_order_ids(self) -> list[str]:
        """All unique order-ids in this settlement (excluding blanks)."""
        return sorted(set(
            r.order_id for r in self.rows
            if r.order_id and r.transaction_type in ('Order', 'Refund')
        ))

    def categorize(self):
        """Categorize rows into summary buckets matching Amazon Seller Central's breakdown.

        Based on actual settlement report field values:
        - transaction-type: Order, Refund, ServiceFee, other-transaction, (blank)
        - amount-type: ItemPrice, ItemFees, ItemWithheldTax, Promotion,
                       Cost of Advertising, FBA Inventory Reimbursement, other-transaction
        - amount-description: Principal, Shipping, Tax, Commission,
                              FBAPerUnitFulfillmentFee, ShippingChargeback, etc.
        """
        # Reset all
        self.product_charges = 0.0
        self.shipping_revenue = 0.0
        self.inventory_reimbursements = 0.0
        self.refunded_expenses = 0.0
        self.refunded_sales = 0.0
        self.promo_rebates = 0.0
        self.fba_fees = 0.0
        self.advertising_costs = 0.0
        self.shipping_charges = 0.0
        self.amazon_fees = 0.0
        self.other_fees = 0.0
        self.tax_collected = 0.0
        self.tax_withheld = 0.0
        self.other_income = 0.0

        for r in self.rows:
            desc = r.amount_description or ''
            atype = r.amount_type or ''
            ttype = r.transaction_type or ''

            # ── Orders ──
            if ttype == 'Order':
                if atype == 'ItemPrice':
                    if 'Principal' in desc:
                        self.product_charges += r.amount
                    elif 'Tax' in desc:
                        self.tax_collected += r.amount
                    elif 'Shipping' in desc:
                        self.shipping_revenue += r.amount
                    else:
                        self.product_charges += r.amount
                elif atype == 'ItemFees':
                    # ALL order fees → "Amazon fees" (Commission, FBA fulfillment, ShippingChargeback)
                    self.amazon_fees += r.amount
                elif atype == 'ItemWithheldTax':
                    self.tax_withheld += r.amount
                elif atype == 'Promotion':
                    self.promo_rebates += r.amount
                else:
                    self.other_fees += r.amount

            # ── Refunds ──
            elif ttype == 'Refund':
                if atype == 'ItemPrice':
                    if 'Tax' in desc:
                        self.tax_collected += r.amount
                    else:
                        self.refunded_sales += r.amount
                elif atype == 'ItemFees':
                    self.refunded_expenses += r.amount
                elif atype == 'ItemWithheldTax':
                    self.tax_withheld += r.amount
                elif atype == 'Promotion':
                    self.promo_rebates += r.amount
                else:
                    self.refunded_sales += r.amount

            # ── ServiceFee (Advertising) ──
            elif ttype == 'ServiceFee':
                if 'dvertising' in atype:  # "Cost of Advertising"
                    self.advertising_costs += r.amount
                else:
                    self.other_fees += r.amount

            # ── other-transaction ──
            elif ttype == 'other-transaction':
                if 'Reimbursement' in atype:
                    self.inventory_reimbursements += r.amount
                elif 'Subscription' in desc:
                    # Subscription Fee → grouped under "Amazon fees"
                    self.amazon_fees += r.amount
                elif 'Inbound' in desc or 'Placement' in desc:
                    # Inbound Transportation / FBA Inbound Placement → "FBA fees"
                    self.fba_fees += r.amount
                else:
                    # Shipping labels, Adjustments, etc. → "Shipping charges"
                    self.shipping_charges += r.amount

            # ── Blank or unknown ──
            else:
                if r.amount != 0:
                    if r.amount > 0:
                        self.other_income += r.amount
                    else:
                        self.other_fees += r.amount

        # Round everything
        for attr in ['product_charges', 'shipping_revenue', 'inventory_reimbursements',
                     'refunded_expenses', 'refunded_sales', 'promo_rebates',
                     'fba_fees', 'advertising_costs', 'shipping_charges',
                     'amazon_fees', 'other_fees', 'tax_collected', 'tax_withheld',
                     'other_income']:
            setattr(self, attr, round(getattr(self, attr), 2))


class FinancesClient:
    """Client for pulling settlement reports and financial data."""

    def __init__(self):
        self.credentials = Config.get_sp_api_credentials()
        self.marketplace = Marketplaces.US

    def _get_reports_api(self) -> Reports:
        return Reports(credentials=self.credentials, marketplace=self.marketplace)

    def _get_finances_api(self) -> Finances:
        return Finances(credentials=self.credentials, marketplace=self.marketplace)

    # ── Settlement Reports ──────────────────────────────────────────

    def list_settlement_reports(
        self,
        created_since: Optional[datetime] = None,
        max_results: int = 10,
    ) -> list[dict]:
        """List available settlement reports (most recent first)."""
        if created_since is None:
            created_since = datetime.now() - timedelta(days=30)

        reports = self._get_reports_api()
        response = reports.get_reports(
            reportTypes=['GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2'],
            processingStatuses=['DONE'],
            createdSince=created_since.strftime('%Y-%m-%dT00:00:00Z'),
            pageSize=max_results,
        )
        return response.payload.get('reports', [])

    def download_settlement_report(self, report_document_id: str) -> str:
        """Download raw settlement report content (TSV)."""
        reports = self._get_reports_api()
        response = reports.get_report_document(report_document_id)
        doc_info = response.payload

        download_url = doc_info.get('url')
        compression = doc_info.get('compressionAlgorithm', '')

        r = requests.get(download_url)
        r.raise_for_status()

        if compression == 'GZIP':
            content = gzip.decompress(r.content)
            return content.decode('utf-8')
        return r.text

    def parse_settlement_tsv(self, tsv_content: str) -> SettlementSummary:
        """Parse a settlement flat file (TSV) into structured data."""
        reader = csv.DictReader(io.StringIO(tsv_content), delimiter='\t')

        rows = []
        settlement_id = ''
        start_date = ''
        end_date = ''
        deposit_date = ''
        total_amount = 0.0
        currency = ''

        for record in reader:
            # Header fields (same on every row)
            if not settlement_id:
                settlement_id = record.get('settlement-id', '')
                start_date = record.get('settlement-start-date', '')
                end_date = record.get('settlement-end-date', '')
                deposit_date = record.get('deposit-date', '')
                try:
                    total_amount = float(record.get('total-amount', 0))
                except (ValueError, TypeError):
                    total_amount = 0.0
                currency = record.get('currency', 'USD')

            # Parse amount
            try:
                amount = float(record.get('amount', 0))
            except (ValueError, TypeError):
                amount = 0.0

            # Parse quantity
            try:
                quantity = int(record.get('quantity-purchased', 0))
            except (ValueError, TypeError):
                quantity = 0

            row = SettlementRow(
                settlement_id=settlement_id,
                settlement_start=start_date,
                settlement_end=end_date,
                deposit_date=deposit_date,
                total_amount=total_amount,
                currency=currency,
                transaction_type=record.get('transaction-type', ''),
                order_id=record.get('order-id', ''),
                amount_type=record.get('amount-type', ''),
                amount_description=record.get('amount-description', ''),
                amount=amount,
                sku=record.get('sku', ''),
                quantity=quantity,
                posted_date=record.get('posted-date', ''),
            )
            rows.append(row)

        summary = SettlementSummary(
            settlement_id=settlement_id,
            start_date=start_date,
            end_date=end_date,
            deposit_date=deposit_date,
            total_amount=total_amount,
            currency=currency,
            rows=rows,
        )
        summary.categorize()
        return summary

    def get_settlement(self, report_id: str) -> SettlementSummary:
        """Fetch and parse a single settlement report by report ID."""
        reports = self._get_reports_api()

        # Get report details to find document ID
        response = reports.get_report(report_id)
        report_info = response.payload
        doc_id = report_info.get('reportDocumentId')

        if not doc_id:
            raise ValueError(f"Report {report_id} has no document ID (may not be ready)")

        tsv = self.download_settlement_report(doc_id)
        return self.parse_settlement_tsv(tsv)

    def get_latest_settlements(self, count: int = 5) -> list[SettlementSummary]:
        """Fetch the N most recent settlement reports."""
        report_list = self.list_settlement_reports(max_results=count)
        settlements = []

        for report in report_list:
            try:
                doc_id = report.get('reportDocumentId')
                if doc_id:
                    tsv = self.download_settlement_report(doc_id)
                    summary = self.parse_settlement_tsv(tsv)
                    settlements.append(summary)
            except Exception as e:
                print(f"Error downloading settlement {report.get('reportId')}: {e}")
                continue

        return settlements

    # ── Finances API (per-order fee breakdown) ──────────────────────

    def get_financial_events_for_order(self, order_id: str) -> dict:
        """Get detailed financial events for a specific order.

        Returns fee breakdowns (commission, FBA fees, etc.) that
        the Orders API does not provide.
        """
        finances = self._get_finances_api()

        try:
            response = finances.get_financial_events_for_order(order_id)
            return response.payload.get('FinancialEvents', {})
        except SellingApiException as e:
            print(f"Error getting financial events for {order_id}: {e}")
            return {}

    def get_order_fees(self, order_id: str) -> dict:
        """Get a simplified fee summary for an order.

        Returns dict with keys like:
        {
            'principal': 29.99,
            'commission': -4.50,
            'fba_fee': -3.22,
            'shipping': 0.0,
            'promo': 0.0,
            'other_fees': 0.0,
            'net': 22.27,
        }
        """
        events = self.get_financial_events_for_order(order_id)
        shipment_events = events.get('ShipmentEventList', [])

        totals = {
            'principal': 0.0,
            'commission': 0.0,
            'fba_fee': 0.0,
            'shipping': 0.0,
            'promo': 0.0,
            'other_fees': 0.0,
        }

        for event in shipment_events:
            for item in event.get('ShipmentItemList', []):
                # Item charges (revenue side)
                for charge in item.get('ItemChargeList', []):
                    charge_type = charge.get('ChargeType', '').lower()
                    amount = float(charge.get('ChargeAmount', {}).get('CurrencyAmount', 0))
                    if 'principal' in charge_type:
                        totals['principal'] += amount
                    elif 'shipping' in charge_type:
                        totals['shipping'] += amount
                    else:
                        totals['principal'] += amount

                # Item fees (cost side, typically negative)
                for fee in item.get('ItemFeeList', []):
                    fee_type = fee.get('FeeType', '').lower()
                    amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0))
                    if 'commission' in fee_type:
                        totals['commission'] += amount
                    elif 'fba' in fee_type:
                        totals['fba_fee'] += amount
                    else:
                        totals['other_fees'] += amount

                # Promotions
                for promo in item.get('PromotionList', []):
                    amount = float(promo.get('PromotionAmount', {}).get('CurrencyAmount', 0))
                    totals['promo'] += amount

        totals['net'] = round(sum(totals.values()), 2)
        for k in totals:
            totals[k] = round(totals[k], 2)

        return totals


if __name__ == '__main__':
    client = FinancesClient()

    print("Listing settlement reports...")
    reports = client.list_settlement_reports()
    print(f"Found {len(reports)} settlement reports\n")

    for report in reports[:3]:
        print(f"Report ID: {report.get('reportId')}")
        print(f"  Created: {report.get('createdTime')}")
        print(f"  Document ID: {report.get('reportDocumentId')}")
        print()

    if reports:
        print("Downloading most recent settlement...")
        doc_id = reports[0].get('reportDocumentId')
        if doc_id:
            tsv = client.download_settlement_report(doc_id)
            summary = client.parse_settlement_tsv(tsv)

            print(f"\nSettlement {summary.settlement_id}")
            print(f"  Period: {summary.start_date} to {summary.end_date}")
            print(f"  Deposit date: {summary.deposit_date}")
            print(f"  Total (payout): ${summary.total_amount:.2f}")
            print(f"  Sum of rows:    ${summary.sum_of_rows:.2f}")
            print(f"  Reconciles:     {summary.reconciles}")
            print(f"\n  Breakdown (matching Amazon Seller Central):")
            print(f"    Product charges:         ${summary.product_charges:.2f}")
            print(f"    Shipping:                ${summary.shipping_revenue:.2f}")
            print(f"    Inventory reimbursements:${summary.inventory_reimbursements:.2f}")
            print(f"    Refunded expenses:       ${summary.refunded_expenses:.2f}")
            print(f"    Refunded sales:          ${summary.refunded_sales:.2f}")
            print(f"    Promo rebates:           ${summary.promo_rebates:.2f}")
            print(f"    FBA fees:                ${summary.fba_fees:.2f}")
            print(f"    Cost of Advertising:     ${summary.advertising_costs:.2f}")
            print(f"    Shipping charges:        ${summary.shipping_charges:.2f}")
            print(f"    Amazon fees (commission): ${summary.amazon_fees:.2f}")
            print(f"    Other fees:              ${summary.other_fees:.2f}")
            print(f"    Tax collected:           ${summary.tax_collected:.2f}")
            print(f"    Tax withheld:            ${summary.tax_withheld:.2f}")
            print(f"    Other income:            ${summary.other_income:.2f}")
            print(f"\n  Unique orders: {len(summary.unique_order_ids)}")
