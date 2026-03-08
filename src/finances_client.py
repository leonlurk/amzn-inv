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


# ═══════════════════════════════════════════════════════════════════════════
# GL ACCOUNT MAPPING - Matches Pura Vitalia Chart of Accounts
# Based on: PuraVitalia_Amazon_SP_API_JE_Spec.docx
# ═══════════════════════════════════════════════════════════════════════════
GL_ACCOUNTS = {
    '4010': {'name': 'Merchandise Sales', 'type': 'Revenue'},
    '4130': {'name': 'Sales Returns', 'type': 'Contra-Revenue'},
    '4140': {'name': 'Inventory Reimbursement Income', 'type': 'Revenue'},
    '5030': {'name': 'Amazon Fees', 'type': 'Expense'},
    '6010': {'name': 'Advertising & Promotion', 'type': 'Expense'},
    '1020': {'name': 'Amazon Seller Account', 'type': 'Asset'},
}

# Transaction mapping rules: (transaction_type, amount_type, description_contains) -> GL Account
GL_MAPPING_RULES = [
    # Revenue: Order payments
    ('Order', 'ItemPrice', 'Principal', '4010'),
    ('Order', 'ItemPrice', 'Shipping', '4010'),  # Shipping revenue goes to 4010
    # Refunds: Sales returns
    ('Refund', 'ItemPrice', 'Principal', '4130'),
    ('Refund', 'ItemPrice', 'Shipping', '4130'),
    # Advertising
    ('ServiceFee', 'Cost of Advertising', None, '6010'),
    # Amazon fees: Commission, FBA fees, etc.
    ('Order', 'ItemFees', None, '5030'),
    ('Refund', 'ItemFees', None, '5030'),  # Fee reversals
    ('other-transaction', None, 'Subscription', '5030'),
    ('other-transaction', None, 'FBA', '5030'),
    ('other-transaction', None, 'Inbound', '5030'),
    ('other-transaction', None, 'Placement', '5030'),
    ('other-transaction', None, 'Storage', '5030'),
    ('other-transaction', None, 'Disposal', '5030'),
    ('other-transaction', None, 'Removal', '5030'),
    ('other-transaction', None, 'Coupon', '5030'),
    ('other-transaction', None, 'Shipping', '5030'),
    ('ServiceFee', None, None, '5030'),  # Catch-all service fees
    # Inventory reimbursements
    ('other-transaction', 'FBA Inventory Reimbursement', None, '4140'),
    # Promotions go to contra-revenue or expense depending on context
    ('Order', 'Promotion', None, '5030'),
    ('Refund', 'Promotion', None, '5030'),
]


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

    def _row_category(self, r: 'SettlementRow') -> str:
        """Map a single row to its Sankey category name."""
        desc = r.amount_description or ''
        atype = r.amount_type or ''
        ttype = r.transaction_type or ''

        if ttype == 'Order':
            if atype == 'ItemPrice':
                if 'Tax' in desc:
                    return 'Tax'
                elif 'Shipping' in desc:
                    return 'Shipping'
                return 'Product Charges'
            elif atype == 'ItemFees':
                return 'Amazon Fees'
            elif atype == 'ItemWithheldTax':
                return 'Tax'
            elif atype == 'Promotion':
                return 'Promo Rebates'
        elif ttype == 'Refund':
            if atype == 'ItemPrice':
                if 'Tax' in desc:
                    return 'Tax'
                return 'Refunded Sales'
            elif atype == 'ItemFees':
                return 'Refunded Expenses'
            elif atype == 'ItemWithheldTax':
                return 'Tax'
            elif atype == 'Promotion':
                return 'Promo Rebates'
            return 'Refunded Sales'
        elif ttype == 'ServiceFee':
            if 'dvertising' in atype:
                return 'Cost of Advertising'
            return 'Other Fees'
        elif ttype == 'other-transaction':
            if 'Reimbursement' in atype:
                return 'Inv. Reimbursements'
            elif 'Subscription' in desc:
                return 'Amazon Fees'
            elif 'Inbound' in desc or 'Placement' in desc:
                return 'FBA Fees'
            elif 'Reserve' in desc or 'Reserve' in atype:
                return 'Reserve'
            return 'Shipping Charges'

        if r.amount > 0:
            return 'Other Income'
        return 'Other Fees'

    def rows_as_dataframe(self):
        """Convert rows to a pandas DataFrame with a category column."""
        import pandas as pd
        data = []
        for r in self.rows:
            data.append({
                'Posted Date': r.posted_date,
                'Order ID': r.order_id,
                'SKU': r.sku,
                'Qty': r.quantity,
                'Type': r.transaction_type,
                'Amount Type': r.amount_type,
                'Description': r.amount_description,
                'Amount': r.amount,
                'Category': self._row_category(r),
            })
        return pd.DataFrame(data)

    def per_order_breakdown(self) -> list[dict]:
        """Group rows by order_id and compute P&L per order."""
        from collections import defaultdict
        orders = defaultdict(lambda: {
            'sku': '', 'qty': 0, 'gross': 0.0, 'fees': 0.0,
            'promos': 0.0, 'refunds': 0.0, 'net': 0.0, 'posted': '',
        })

        for r in self.rows:
            if not r.order_id:
                continue
            o = orders[r.order_id]
            if r.sku and not o['sku']:
                o['sku'] = r.sku
            if r.quantity:
                o['qty'] += r.quantity
            if r.posted_date and not o['posted']:
                o['posted'] = r.posted_date

            cat = self._row_category(r)
            if cat in ('Product Charges', 'Shipping'):
                o['gross'] += r.amount
            elif cat in ('Amazon Fees', 'FBA Fees', 'Cost of Advertising', 'Shipping Charges', 'Other Fees'):
                o['fees'] += r.amount
            elif cat == 'Promo Rebates':
                o['promos'] += r.amount
            elif cat in ('Refunded Sales', 'Refunded Expenses'):
                o['refunds'] += r.amount
            o['net'] += r.amount

        result = []
        for oid, o in orders.items():
            gross = round(o['gross'], 2)
            fee_pct = round(abs(o['fees']) / gross * 100, 1) if gross > 0 else 0.0
            result.append({
                'Order ID': oid,
                'SKU': o['sku'],
                'Qty': o['qty'],
                'Gross Sale': round(o['gross'], 2),
                'Fees': round(o['fees'], 2),
                'Promos': round(o['promos'], 2),
                'Refunds': round(o['refunds'], 2),
                'Net': round(o['net'], 2),
                'Fee %': fee_pct,
                'Posted': o['posted'],
            })
        result.sort(key=lambda x: x['Net'], reverse=True)
        return result

    def sku_profitability(self) -> list[dict]:
        """Group rows by SKU and compute profitability per product."""
        from collections import defaultdict
        skus = defaultdict(lambda: {
            'units': 0, 'revenue': 0.0, 'fees': 0.0,
            'refunds': 0.0, 'promos': 0.0, 'net': 0.0,
        })

        for r in self.rows:
            if not r.sku:
                continue
            s = skus[r.sku]
            if r.quantity and r.transaction_type == 'Order':
                s['units'] += r.quantity

            cat = self._row_category(r)
            if cat in ('Product Charges', 'Shipping'):
                s['revenue'] += r.amount
            elif cat in ('Amazon Fees', 'FBA Fees', 'Cost of Advertising', 'Shipping Charges', 'Other Fees'):
                s['fees'] += r.amount
            elif cat == 'Promo Rebates':
                s['promos'] += r.amount
            elif cat in ('Refunded Sales', 'Refunded Expenses'):
                s['refunds'] += r.amount
            s['net'] += r.amount

        result = []
        for sku, s in skus.items():
            rev = round(s['revenue'], 2)
            net = round(s['net'], 2)
            margin = round(net / rev * 100, 1) if rev > 0 else 0.0
            result.append({
                'SKU': sku,
                'Units': s['units'],
                'Revenue': rev,
                'Fees': round(s['fees'], 2),
                'Refunds': round(s['refunds'], 2),
                'Promos': round(s['promos'], 2),
                'Net': net,
                'Margin %': margin,
            })
        result.sort(key=lambda x: x['Net'], reverse=True)
        return result

    def fee_ratios(self) -> dict:
        """Compute each fee category as a percentage of gross revenue."""
        gross = self.product_charges + self.shipping_revenue
        if gross <= 0:
            return {'amazon_fees_pct': 0, 'fba_pct': 0, 'ads_pct': 0, 'shipping_pct': 0, 'total_fee_pct': 0}
        total_fees = abs(self.amazon_fees) + abs(self.fba_fees) + abs(self.advertising_costs) + abs(self.shipping_charges)
        return {
            'amazon_fees_pct': round(abs(self.amazon_fees) / gross * 100, 1),
            'fba_pct': round(abs(self.fba_fees) / gross * 100, 1),
            'ads_pct': round(abs(self.advertising_costs) / gross * 100, 1),
            'shipping_pct': round(abs(self.shipping_charges) / gross * 100, 1),
            'total_fee_pct': round(total_fees / gross * 100, 1),
        }

    def detect_reserves(self) -> dict:
        """Detect reserve holds that explain payout discrepancies."""
        previous = 0.0
        current = 0.0
        reserve_rows = []
        for r in self.rows:
            desc = (r.amount_description or '').lower()
            atype = (r.amount_type or '').lower()
            if 'reserve' in desc or 'reserve' in atype:
                reserve_rows.append(r)
                if 'previous' in desc:
                    previous += r.amount
                elif 'current' in desc:
                    current += r.amount
                else:
                    current += r.amount
        return {
            'previous_reserve': round(previous, 2),
            'current_reserve': round(current, 2),
            'net_change': round(previous + current, 2),
            'has_reserves': len(reserve_rows) > 0,
            'reserve_rows': reserve_rows,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # GL ACCOUNT MAPPING & JE PREPARATION
    # ═══════════════════════════════════════════════════════════════════════

    def _row_to_gl(self, r: 'SettlementRow') -> str:
        """Map a settlement row to its GL account code."""
        ttype = r.transaction_type or ''
        atype = r.amount_type or ''
        desc = r.amount_description or ''

        # Revenue: Order payments (Principal + Shipping)
        if ttype == 'Order' and atype == 'ItemPrice':
            if 'Tax' not in desc:
                return '4010'  # Merchandise Sales

        # Refunds: Sales returns
        if ttype == 'Refund' and atype == 'ItemPrice':
            if 'Tax' not in desc:
                return '4130'  # Sales Returns

        # Advertising costs
        if ttype == 'ServiceFee' and 'dvertising' in atype:
            return '6010'  # Advertising & Promotion

        # Inventory reimbursements
        if ttype == 'other-transaction' and 'Reimbursement' in atype:
            return '4140'  # Inventory Reimbursement Income

        # All other fees → Amazon Fees
        if ttype == 'Order' and atype == 'ItemFees':
            return '5030'
        if ttype == 'Refund' and atype == 'ItemFees':
            return '5030'
        if ttype == 'Order' and atype == 'Promotion':
            return '5030'
        if ttype == 'Refund' and atype == 'Promotion':
            return '5030'
        if ttype == 'ServiceFee':
            return '5030'
        if ttype == 'other-transaction':
            return '5030'

        # Default: if positive = revenue, if negative = expense
        return '4010' if r.amount > 0 else '5030'

    def je_summary(self) -> list[dict]:
        """Generate Journal Entry summary by GL account.

        Returns a list ready for JE preparation:
        [
            {'account': '4010', 'name': 'Merchandise Sales', 'debit': 0, 'credit': 287.97, 'description': '...'},
            {'account': '5030', 'name': 'Amazon Fees', 'debit': 45.22, 'credit': 0, 'description': '...'},
            ...
        ]
        """
        from collections import defaultdict

        # Aggregate by GL account
        gl_totals = defaultdict(lambda: {'debit': 0.0, 'credit': 0.0, 'descriptions': set()})

        for r in self.rows:
            gl = self._row_to_gl(r)
            desc = r.amount_description or r.amount_type or r.transaction_type or 'Other'
            gl_totals[gl]['descriptions'].add(desc)

            # Amazon convention: positive = credit to seller, negative = debit
            # Accounting: Revenue accounts are credited, expense accounts are debited
            if gl in ('4010', '4130', '4140'):  # Revenue accounts
                if r.amount > 0:
                    gl_totals[gl]['credit'] += r.amount
                else:
                    gl_totals[gl]['debit'] += abs(r.amount)
            else:  # Expense accounts (5030, 6010)
                if r.amount < 0:
                    gl_totals[gl]['debit'] += abs(r.amount)
                else:
                    gl_totals[gl]['credit'] += r.amount

        # Build result
        result = []
        for gl, data in sorted(gl_totals.items()):
            result.append({
                'account': gl,
                'name': GL_ACCOUNTS.get(gl, {}).get('name', 'Unknown'),
                'debit': round(data['debit'], 2),
                'credit': round(data['credit'], 2),
                'description': ', '.join(sorted(data['descriptions']))[:100],
            })

        # Add balancing entry for 1020 Amazon Seller Account
        total_debits = sum(r['debit'] for r in result)
        total_credits = sum(r['credit'] for r in result)
        net = round(total_credits - total_debits, 2)

        result.append({
            'account': '1020',
            'name': 'Amazon Seller Account',
            'debit': round(net, 2) if net > 0 else 0,
            'credit': round(abs(net), 2) if net < 0 else 0,
            'description': f'Net payout ${self.total_amount:,.2f}',
        })

        return result

    def sku_sales_summary(self) -> list[dict]:
        """Get SKU-level sales data for this settlement period.

        Returns units sold and gross revenue by SKU, ready for COGS calculation.
        """
        from collections import defaultdict

        skus = defaultdict(lambda: {'units': 0, 'gross_revenue': 0.0, 'refund_units': 0, 'refund_amount': 0.0})

        for r in self.rows:
            if not r.sku:
                continue

            # Order revenue (ItemPrice/Principal or Shipping)
            if r.transaction_type == 'Order' and r.amount_type == 'ItemPrice':
                if 'Tax' not in (r.amount_description or ''):
                    skus[r.sku]['gross_revenue'] += r.amount
                    if r.quantity:
                        skus[r.sku]['units'] += r.quantity

            # Refunds
            if r.transaction_type == 'Refund' and r.amount_type == 'ItemPrice':
                if 'Tax' not in (r.amount_description or ''):
                    skus[r.sku]['refund_amount'] += abs(r.amount)
                    if r.quantity:
                        skus[r.sku]['refund_units'] += abs(r.quantity)

        result = []
        for sku, data in sorted(skus.items()):
            net_units = data['units'] - data['refund_units']
            net_revenue = data['gross_revenue'] - data['refund_amount']
            result.append({
                'sku': sku,
                'units_sold': data['units'],
                'gross_revenue': round(data['gross_revenue'], 2),
                'refund_units': data['refund_units'],
                'refund_amount': round(data['refund_amount'], 2),
                'net_units': net_units,
                'net_revenue': round(net_revenue, 2),
            })

        result.sort(key=lambda x: x['net_revenue'], reverse=True)
        return result

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


@dataclass
class ReconciliationResult:
    """Full reconciliation result matching Mike's spreadsheet structure.

    Structure:
    A = Amazon Payment (payout from settlement)
    B = Adjust Payment (seller repayments - billing charges)
    D = Sales Data (orders that SHOULD be in this period based on PurchaseDate)
    E = Adjustments (4a through 4h explaining A-B vs D differences)

    Formula: A - B = D - E  (or: A - B + E = D)
    Final Difference should be $0.00 if fully reconciled.
    """
    settlement_id: str
    period_start: str
    period_end: str

    # A - Amazon Payment (payout)
    amazon_payment: float = 0.0

    # B - Adjust Payment (seller repayments/billing)
    adjust_payment: float = 0.0

    # D - Sales Data (calculated from orders in period)
    sales_data_total: float = 0.0

    # E - Adjustments breakdown (4a through 4h)
    adj_4a_taxes: float = 0.0                    # Taxes excluded from payout
    adj_4b_unsettled_orders: float = 0.0         # Orders not yet settled
    adj_4c_service_fee_timing: float = 0.0       # Fee timing differences
    adj_4d_ad_spend_timing: float = 0.0          # Advertising timing
    adj_4e_prior_period_orders: float = 0.0      # Orders from prior period settling now
    adj_4f_fees_not_in_sales: float = 0.0        # Shipping labels, promo fees
    adj_4g_cross_period_refunds: float = 0.0     # Refunds crossing periods
    adj_4h_opening_balance: float = 0.0          # Opening period adjustment

    # Supporting detail rows for each adjustment
    detail_4a: list = field(default_factory=list)
    detail_4b: list = field(default_factory=list)
    detail_4c: list = field(default_factory=list)
    detail_4d: list = field(default_factory=list)
    detail_4e: list = field(default_factory=list)
    detail_4f: list = field(default_factory=list)
    detail_4g: list = field(default_factory=list)
    detail_4h: list = field(default_factory=list)

    # Exceptions/notes (items that couldn't be resolved)
    exceptions: list = field(default_factory=list)

    # Order date fetching metadata
    order_dates_fetched: bool = False
    orders_with_dates: int = 0
    orders_without_dates: int = 0

    # Same-period items (reduce D directly, NOT in E)
    # Per Mike's spec: only cross-period items go to E
    same_period_fees: float = 0.0       # Fees for orders placed IN this period
    same_period_refunds: float = 0.0    # Refunds for orders placed IN this period

    @property
    def prior_period_stats(self) -> dict:
        """Statistics about prior vs same-period orders (when dates fetched)."""
        if not self.order_dates_fetched:
            return {'note': 'Order dates not fetched - all orders assumed prior period'}

        # detail_4e now ONLY contains prior-period orders
        # same_period_fees tracks same-period fees (not in detail_4e)
        return {
            '4e_prior_period_orders': len(self.detail_4e),
            'same_period_fees_in_D': self.same_period_fees,
            'note': '4e now contains ONLY prior-period orders per Mike spec',
        }

    @property
    def cross_period_stats(self) -> dict:
        """Statistics about cross vs same-period refunds (when dates fetched)."""
        if not self.order_dates_fetched:
            return {'note': 'Order dates not fetched - all refunds assumed cross-period'}

        # detail_4g now ONLY contains cross-period refunds
        # same_period_refunds tracks same-period refunds (not in detail_4g)
        return {
            '4g_cross_period_refunds': len(self.detail_4g),
            'same_period_refunds_in_D': self.same_period_refunds,
            'note': '4g now contains ONLY cross-period refunds per Mike spec',
        }

    @property
    def adjusted_amazon_payment(self) -> float:
        """A - B"""
        return round(self.amazon_payment - self.adjust_payment, 2)

    @property
    def total_adjustments(self) -> float:
        """Sum of all E adjustments (4a through 4h)"""
        return round(
            self.adj_4a_taxes +
            self.adj_4b_unsettled_orders +
            self.adj_4c_service_fee_timing +
            self.adj_4d_ad_spend_timing +
            self.adj_4e_prior_period_orders +
            self.adj_4f_fees_not_in_sales +
            self.adj_4g_cross_period_refunds +
            self.adj_4h_opening_balance,
            2
        )

    @property
    def adjusted_sales_data(self) -> float:
        """D - E"""
        return round(self.sales_data_total - self.total_adjustments, 2)

    @property
    def final_difference(self) -> float:
        """Should be $0.00 if fully reconciled. (A - B) - (D - E)"""
        return round(self.adjusted_amazon_payment - self.adjusted_sales_data, 2)

    @property
    def is_reconciled(self) -> bool:
        """True if final difference is within $0.01"""
        return abs(self.final_difference) < 0.01

    def to_summary_dict(self) -> dict:
        """Return reconciliation summary as dict for display/export."""
        summary = {
            'Settlement ID': self.settlement_id,
            'Period': f"{self.period_start[:10]} to {self.period_end[:10]}",
            'A - Amazon Payment': self.amazon_payment,
            'B - Adjust Payment': self.adjust_payment,
            'A-B Adjusted Payment': self.adjusted_amazon_payment,
            'D - Sales Data': self.sales_data_total,
            '4a - Taxes': self.adj_4a_taxes,
            '4b - Unsettled Orders': self.adj_4b_unsettled_orders,
            '4c - Service Fee Timing': self.adj_4c_service_fee_timing,
            '4d - Ad Spend Timing': self.adj_4d_ad_spend_timing,
            '4e - Prior Period Orders': self.adj_4e_prior_period_orders,
            '4f - Fees Not in Sales': self.adj_4f_fees_not_in_sales,
            '4g - Cross-Period Refunds': self.adj_4g_cross_period_refunds,
            '4h - Opening Balance': self.adj_4h_opening_balance,
            'E - Total Adjustments': self.total_adjustments,
            'D-E Adjusted Sales': self.adjusted_sales_data,
            'Final Difference': self.final_difference,
            'Reconciled': self.is_reconciled,
            'Exceptions': len(self.exceptions),
            'Order Dates Fetched': self.order_dates_fetched,
        }

        # Add detailed stats when order dates were fetched
        if self.order_dates_fetched:
            summary['Orders With Dates'] = self.orders_with_dates
            summary['Orders Without Dates'] = self.orders_without_dates
            summary['Same-Period Fees (in D)'] = self.same_period_fees
            summary['Same-Period Refunds (in D)'] = self.same_period_refunds
            summary['4e Stats'] = self.prior_period_stats
            summary['4g Stats'] = self.cross_period_stats

        return summary


class FinancesClient:
    """Client for pulling settlement reports and financial data."""

    def __init__(self):
        self.credentials = Config.get_sp_api_credentials()
        self.marketplace = Marketplaces.US

    def _get_reports_api(self) -> Reports:
        return Reports(credentials=self.credentials, marketplace=self.marketplace)

    def _get_finances_api(self) -> Finances:
        return Finances(credentials=self.credentials, marketplace=self.marketplace)

    def _get_orders_api(self):
        """Get Orders API client for fetching order details."""
        from sp_api.api import Orders
        return Orders(credentials=self.credentials, marketplace=self.marketplace)

    # ── Settlement Reports ──────────────────────────────────────────

    def list_settlement_reports(
        self,
        created_since: Optional[datetime] = None,
        max_results: int = 10,
    ) -> list[dict]:
        """List available settlement reports (most recent first)."""
        if created_since is None:
            created_since = datetime.now() - timedelta(days=89)

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

    # ═══════════════════════════════════════════════════════════════════════
    # RECONCILIATION ENGINE - Mike's Exact 4a-4h Specification
    # ═══════════════════════════════════════════════════════════════════════
    #
    # Formula: (A - B) = (D - E)
    # - A = Amazon Payment (payout from settlement)
    # - B = Adjust Payment (seller repayments/billing)
    # - D = Sales Data (gross revenue that SHOULD be in this period)
    # - E = Adjustments (timing differences explaining why D ≠ A-B)
    #
    # Mike's EXACT 4a-4h definitions (from mike-claude-chat.txt):
    # - 4a: Taxes - Tax amounts excluded from payout (collected vs withheld)
    # - 4b: Unsettled Orders - Orders in sales data not yet settled by Amazon
    # - 4c: Service Fee Timing - Fees assessed period N, deducted period N+1:
    #       (Subscription, FBA Storage, Long-Term Storage, Inbound Transportation,
    #        Inbound Placement, AWD, Coupon, Removal/Disposal fees)
    # - 4d: Ad Spend Timing - Advertising charges
    # - 4e: Prior Period Orders - Per-order fees (FBA fulfillment + Commission)
    #       on orders settling in this payout
    # - 4f: Fees Not in Sales Data - Items with NO sales entry:
    #       (Buy Shipping Labels, Amazon Deal fees)
    # - 4g: Cross-Period Refunds - Refunds for orders from different periods
    # - 4h: Opening Balance - Pre-data structural adjustments
    # ═══════════════════════════════════════════════════════════════════════

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various Amazon date formats to datetime."""
        if not date_str:
            return None
        date_str = date_str.replace(' UTC', '').replace('Z', '').strip()
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str[:19], fmt)
            except ValueError:
                continue
        return None

    def _is_service_fee_timing(self, desc: str) -> bool:
        """Check if fee is a Service Fee Timing item (4c).

        Per Mike's spec: Fees assessed in period N but deducted in period N+1.
        These are NOT per-order fees - they're account-level recurring fees.
        """
        timing_fee_keywords = [
            'subscription',
            'storage fee',
            'long-term storage',
            'fba storage',
            'inbound transportation',
            'inbound placement',
            'awd',
            'coupon redemption',
            'removal',
            'disposal',
        ]
        desc_lower = desc.lower()
        return any(kw in desc_lower for kw in timing_fee_keywords)

    def _is_fee_not_in_sales(self, desc: str, ttype: str) -> bool:
        """Check if this is a Fee Not in Sales Data item (4f).

        Per Mike's spec: Items that appear in payout but have NO sales data entry.
        - Buy Shipping Labels
        - Amazon Deal fees (not order-level promotions)
        """
        desc_lower = desc.lower()

        # Shipping label purchases
        if 'shipping label' in desc_lower:
            return True

        # Amazon deal fees (Lightning Deal, Best Deal, etc.)
        if 'deal' in desc_lower and ('fee' in desc_lower or ttype == 'other-transaction'):
            return True

        return False

    def _fetch_order_purchase_dates(self, order_ids: list[str]) -> dict[str, str]:
        """Fetch PurchaseDate for a list of order IDs."""
        if not order_ids:
            return {}

        orders_api = self._get_orders_api()
        purchase_dates = {}

        print(f"  Fetching purchase dates for {len(order_ids)} orders...")

        for order_id in order_ids[:100]:
            try:
                response = orders_api.get_order(order_id)
                order_data = response.payload
                purchase_date = order_data.get('PurchaseDate', '')
                if purchase_date:
                    purchase_dates[order_id] = purchase_date
            except Exception:
                continue
            time.sleep(0.2)

        print(f"  Retrieved {len(purchase_dates)} purchase dates")
        return purchase_dates

    def reconcile_settlement(
        self,
        settlement: SettlementSummary,
        fetch_order_dates: bool = False,
        previous_settlement: Optional[SettlementSummary] = None,
        is_opening_period: bool = False,
    ) -> ReconciliationResult:
        """Perform full reconciliation using Mike's EXACT 4a-4h specification.

        Formula: (A - B) = (D - E)
        - A = Amazon Payment (payout)
        - B = Adjust Payment (seller repayments)
        - D = Sales Data (gross revenue: Product Charges + Shipping + Reimbursements)
        - E = Adjustments (timing/data differences)

        Mike's EXACT 4a-4h buckets:
        - 4a: Taxes (net tax withheld)
        - 4b: Unsettled Orders (reserves/holds)
        - 4c: Service Fee Timing (Subscription, Storage, Inbound, Disposal, Coupon)
        - 4d: Ad Spend Timing (advertising)
        - 4e: Prior Period Orders (per-order FBA + Commission fees WHERE PurchaseDate < period_start)
        - 4f: Fees Not in Sales (shipping labels, deal fees ONLY)
        - 4g: Cross-Period Refunds (refunds WHERE original order PurchaseDate outside period)
        - 4h: Opening Balance (other adjustments)

        Args:
            settlement: The settlement to reconcile
            fetch_order_dates: If True, fetch PurchaseDate from Orders API for TRUE 4e/4g detection
            previous_settlement: Optional previous period for comparison
            is_opening_period: Whether this is the first period being reconciled

        Returns:
            ReconciliationResult with D - E = A verification
        """
        result = ReconciliationResult(
            settlement_id=settlement.settlement_id,
            period_start=settlement.start_date,
            period_end=settlement.end_date,
            amazon_payment=settlement.total_amount,
        )

        # ══════════════════════════════════════════════════════════════════
        # PURCHASE DATE FETCHING (for true 4e/4g detection)
        # ══════════════════════════════════════════════════════════════════
        purchase_dates: dict[str, str] = {}
        period_start_dt = self._parse_date(settlement.start_date)
        period_end_dt = self._parse_date(settlement.end_date)

        if fetch_order_dates and period_start_dt and period_end_dt:
            # Collect all unique order IDs
            unique_order_ids = list(set(
                row.order_id for row in settlement.rows
                if row.order_id and row.transaction_type in ('Order', 'Refund')
            ))
            print(f"Fetching purchase dates for {len(unique_order_ids)} orders...")
            purchase_dates = self._fetch_order_purchase_dates(unique_order_ids)
            print(f"Successfully retrieved {len(purchase_dates)} purchase dates")

        # D components (Revenue)
        product_charges = 0.0
        shipping_income = 0.0
        reimbursements = 0.0
        refunded_expenses = 0.0

        # Same-period deductions (reduce D, NOT E)
        # Per Mike's spec: fees/refunds for orders placed IN this period affect D directly
        same_period_fees = 0.0      # Fees for orders placed within this settlement period
        same_period_refunds = 0.0   # Refunds for orders placed within this settlement period

        # E components (stored as positive for subtraction)
        # Per Mike's spec: ONLY timing differences go to E
        taxes_net = 0.0           # 4a
        reserves = 0.0            # 4b
        service_fee_timing = 0.0  # 4c - Subscription, Storage, Inbound, etc.
        ad_spend = 0.0            # 4d
        order_fees = 0.0          # 4e - ONLY prior-period order fees (PurchaseDate < period_start)
        fees_not_in_sales = 0.0   # 4f - Shipping labels, deal fees
        refunded_sales = 0.0      # 4g - ONLY cross-period refunds (PurchaseDate outside period)
        other_adjustments = 0.0   # 4h

        # B component
        adjust_payment = 0.0

        # Process each row with Mike's exact categorization
        for row in settlement.rows:
            ttype = row.transaction_type or ''
            atype = row.amount_type or ''
            desc = row.amount_description or ''
            desc_lower = desc.lower()
            amount = row.amount

            # ══════════════════════════════════════════════════════════════
            # B: ADJUST PAYMENT (seller repayments, billing)
            # ══════════════════════════════════════════════════════════════
            if 'Repayment' in ttype or 'Billing' in desc:
                adjust_payment += amount
                continue

            # ══════════════════════════════════════════════════════════════
            # D: REVENUE CATEGORIES (Sales Data)
            # ══════════════════════════════════════════════════════════════

            # Product Charges (Principal)
            if atype == 'ItemPrice' and 'Principal' in desc and ttype == 'Order':
                product_charges += amount
                continue

            # Shipping Income
            if atype == 'ItemPrice' and 'Shipping' in desc and 'Tax' not in desc and ttype == 'Order':
                shipping_income += amount
                continue

            # Reimbursements (positive = we get money)
            if 'reimbursement' in desc_lower or 'Reimbursement' in atype:
                if amount > 0:
                    reimbursements += amount
                result.detail_4h.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                    'category': 'Reimbursement',
                })
                continue

            # Refunded Expenses (fee reversals from refunds - we get money back)
            if ttype == 'Refund' and atype == 'ItemFees':
                refunded_expenses += amount
                continue

            # ══════════════════════════════════════════════════════════════
            # E: ADJUSTMENT CATEGORIES (4a through 4h)
            # ══════════════════════════════════════════════════════════════

            # 4a: TAXES (collected vs withheld, usually nets to ~$0)
            if 'Tax' in desc or 'Tax' in atype:
                taxes_net += amount
                result.detail_4a.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                })
                continue

            # 4b: UNSETTLED ORDERS (reserves held by Amazon)
            if 'reserve' in desc_lower or 'reserve' in atype.lower():
                reserves += amount
                result.detail_4b.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                })
                continue

            # 4d: AD SPEND TIMING (advertising - check before 4c)
            if ttype == 'ServiceFee' and ('advertising' in desc_lower or 'dvertising' in atype.lower()):
                ad_spend += abs(amount)
                result.detail_4d.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                })
                continue

            # 4c: SERVICE FEE TIMING (Subscription, Storage, Inbound, Disposal, Coupon)
            # These are account-level fees assessed period N, deducted period N+1
            if self._is_service_fee_timing(desc):
                service_fee_timing += abs(amount)
                result.detail_4c.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                    'timing_note': 'Fee assessed prior period, deducted this period',
                })
                continue

            # 4f: FEES NOT IN SALES DATA (shipping labels, deal fees)
            # Check this BEFORE other-transaction catch-all
            if self._is_fee_not_in_sales(desc, ttype):
                fees_not_in_sales += abs(amount) if amount < 0 else 0
                result.detail_4f.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                    'note': 'No corresponding sales data entry',
                })
                continue

            # 4e: PRIOR PERIOD ORDERS (per-order fees: FBA fulfillment + Commission)
            # Per Mike's spec: ONLY orders where PurchaseDate < period_start go to E
            # Same-period orders reduce D directly (not E)
            if atype == 'ItemFees':
                is_prior_period = True  # Default: assume prior period if no date
                purchase_date_str = purchase_dates.get(row.order_id, '')
                purchase_dt = self._parse_date(purchase_date_str) if purchase_date_str else None

                if purchase_dt and period_start_dt:
                    # True detection: order purchased BEFORE this settlement period
                    is_prior_period = purchase_dt < period_start_dt

                if is_prior_period:
                    # 4e: Prior Period Order fees (PurchaseDate < period_start) → goes to E
                    order_fees += abs(amount)
                    result.detail_4e.append({
                        'order_id': row.order_id,
                        'amount': amount,
                        'description': desc,
                        'posted_date': row.posted_date,
                        'purchase_date': purchase_date_str or 'N/A',
                        'is_prior_period': True,
                    })
                else:
                    # Same-period order: fees reduce D directly, NOT E
                    # These are fees on THIS period's orders - they're part of this period's P&L
                    same_period_fees += abs(amount)
                    # NOT added to detail_4e - these don't belong in adjustments
                continue

            # 4g: CROSS-PERIOD REFUNDS (refunds WHERE original order PurchaseDate outside period)
            # Per Mike's spec: ONLY refunds for orders from different settlement periods go to E
            # Same-period refunds reduce D directly (not E)
            if ttype == 'Refund' and atype == 'ItemPrice':
                is_cross_period = True  # Default: assume cross-period if no date
                purchase_date_str = purchase_dates.get(row.order_id, '')
                purchase_dt = self._parse_date(purchase_date_str) if purchase_date_str else None

                if purchase_dt and period_start_dt and period_end_dt:
                    # True detection: order was purchased OUTSIDE this settlement period
                    is_cross_period = purchase_dt < period_start_dt or purchase_dt > period_end_dt

                if is_cross_period:
                    # 4g: Cross-Period Refund (original order from different period) → goes to E
                    refunded_sales += abs(amount)
                    result.detail_4g.append({
                        'order_id': row.order_id,
                        'amount': amount,
                        'description': desc,
                        'posted_date': row.posted_date,
                        'purchase_date': purchase_date_str or 'N/A',
                        'is_cross_period': True,
                    })
                else:
                    # Same-period refund: reduces D directly, NOT E
                    # This is a refund for an order placed during this period - part of this period's P&L
                    same_period_refunds += abs(amount)
                    # NOT added to detail_4g - these don't belong in adjustments
                continue

            # Order-level promotions go to 4f (reduce revenue, no separate sales entry)
            if atype == 'Promotion':
                fees_not_in_sales += abs(amount) if amount < 0 else 0
                result.detail_4f.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc or 'Promotion',
                    'posted_date': row.posted_date,
                })
                continue

            # Other-transaction items (not already caught above)
            if ttype == 'other-transaction':
                if amount < 0:
                    other_adjustments += abs(amount)
                else:
                    reimbursements += amount
                result.detail_4h.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                })
                continue

            # ServiceFee not caught above (non-advertising)
            if ttype == 'ServiceFee':
                ad_spend += abs(amount)
                result.detail_4d.append({
                    'order_id': row.order_id,
                    'amount': amount,
                    'description': desc,
                    'posted_date': row.posted_date,
                })
                continue

            # Catch-all for anything else
            if amount < 0:
                other_adjustments += abs(amount)
            else:
                reimbursements += amount
            result.detail_4h.append({
                'order_id': row.order_id,
                'amount': amount,
                'description': desc,
                'posted_date': row.posted_date,
                'note': 'Uncategorized item',
            })

        # ══════════════════════════════════════════════════════════════════
        # FINAL CALCULATIONS
        # ══════════════════════════════════════════════════════════════════

        # D = Net Sales Data for this period
        # Gross revenue minus same-period fees and refunds (orders placed IN this period)
        d_sales_data = (
            product_charges + shipping_income + reimbursements + refunded_expenses
            - same_period_fees      # Fees for orders placed in THIS period reduce D
            - same_period_refunds   # Refunds for orders placed in THIS period reduce D
        )

        # E = Total Adjustments (ONLY timing differences - prior/cross-period items)
        # Per Mike's spec: Only items that cross period boundaries go to E
        e_adjustments = service_fee_timing + ad_spend + order_fees + fees_not_in_sales + refunded_sales + other_adjustments

        # Handle reserves (negative = held, positive = released)
        if reserves < 0:
            e_adjustments += abs(reserves)
        else:
            d_sales_data += reserves

        # Handle tax netting (collected - withheld, usually ~0)
        if taxes_net < 0:
            e_adjustments += abs(taxes_net)

        # Set results
        result.adjust_payment = round(adjust_payment, 2)
        result.sales_data_total = round(d_sales_data, 2)

        # Map to 4a-4h (Mike's exact labels)
        result.adj_4a_taxes = round(abs(taxes_net) if taxes_net < 0 else 0, 2)
        result.adj_4b_unsettled_orders = round(abs(reserves) if reserves < 0 else 0, 2)
        result.adj_4c_service_fee_timing = round(service_fee_timing, 2)
        result.adj_4d_ad_spend_timing = round(ad_spend, 2)
        result.adj_4e_prior_period_orders = round(order_fees, 2)
        result.adj_4f_fees_not_in_sales = round(fees_not_in_sales, 2)
        result.adj_4g_cross_period_refunds = round(refunded_sales, 2)
        result.adj_4h_opening_balance = round(other_adjustments, 2)

        # Set order date metadata
        if fetch_order_dates and purchase_dates:
            result.order_dates_fetched = True
            result.orders_with_dates = len(purchase_dates)
            unique_order_ids = set(
                row.order_id for row in settlement.rows
                if row.order_id and row.transaction_type in ('Order', 'Refund')
            )
            result.orders_without_dates = len(unique_order_ids) - len(purchase_dates)

        # Set same-period amounts (these reduce D, not E)
        result.same_period_fees = round(same_period_fees, 2)
        result.same_period_refunds = round(same_period_refunds, 2)

        # Verify: D - E should equal A (no plugs allowed per Mike)
        calculated_payout = d_sales_data - e_adjustments
        actual_payout = settlement.total_amount

        diff = round(actual_payout - calculated_payout, 2)
        if abs(diff) > 0.01:
            result.exceptions.append({
                'type': 'unresolved_difference',
                'amount': diff,
                'message': f'Unresolved difference of ${diff:.2f} - NO PLUGS ALLOWED',
                'note': 'Every item must be individually identified per Mike spec',
            })

        return result

    def reconcile_latest(self, count: int = 1, fetch_order_dates: bool = False) -> list[ReconciliationResult]:
        """Reconcile the N most recent settlement periods.

        Args:
            count: Number of periods to reconcile
            fetch_order_dates: Whether to fetch order purchase dates (slower)

        Returns:
            List of ReconciliationResult objects
        """
        settlements = self.get_latest_settlements(count=count + 1)  # +1 for previous period comparison
        results = []

        for i, settlement in enumerate(settlements[:count]):
            previous = settlements[i + 1] if i + 1 < len(settlements) else None
            result = self.reconcile_settlement(
                settlement,
                fetch_order_dates=fetch_order_dates,
                previous_settlement=previous,
            )
            results.append(result)

        return results


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
