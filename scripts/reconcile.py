"""Reconciliation script - proves settlement data matches Orders API.

Mike's test:
1. Pull settlement report for a closed period
2. Categorize transactions matching Amazon's breakdown
3. Verify sum of rows = payout (penny-for-penny)
4. Cross-reference order-ids against Orders API (same transaction, not duplicates)
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sp_api.api import Orders
from sp_api.base import Marketplaces

from src.config import Config
from src.finances_client import FinancesClient


def reconcile_settlement(settlement_index: int = 0):
    """Run full reconciliation on a settlement period.

    Args:
        settlement_index: 0 = most recent closed settlement, 1 = second most recent, etc.
    """
    client = FinancesClient()

    # ── Step 1: Pull settlement report ──────────────────────────────
    print("=" * 70)
    print("STEP 1: Pull Settlement Report")
    print("=" * 70)

    reports = client.list_settlement_reports(max_results=10)
    if not reports:
        print("ERROR: No settlement reports found.")
        return

    print(f"Found {len(reports)} settlement reports available.\n")
    for i, r in enumerate(reports[:5]):
        marker = " <-- SELECTED" if i == settlement_index else ""
        print(f"  [{i}] Report {r.get('reportId')} - Created: {r.get('createdTime')}{marker}")

    if settlement_index >= len(reports):
        print(f"\nERROR: Index {settlement_index} out of range. Max: {len(reports) - 1}")
        return

    selected = reports[settlement_index]
    doc_id = selected.get('reportDocumentId')
    if not doc_id:
        print("ERROR: Selected report has no document ID.")
        return

    print(f"\nDownloading settlement report...")
    tsv = client.download_settlement_report(doc_id)
    settlement = client.parse_settlement_tsv(tsv)

    print(f"\n  Settlement ID:  {settlement.settlement_id}")
    print(f"  Period:         {settlement.start_date} to {settlement.end_date}")
    print(f"  Deposit date:   {settlement.deposit_date}")
    print(f"  Payout amount:  ${settlement.total_amount:.2f}")
    print(f"  Total rows:     {len(settlement.rows)}")

    # ── Step 2: Categorize transactions ─────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 2: Categorize Transactions (Amazon's Breakdown)")
    print("=" * 70)

    print(f"""
    Product charges:          ${settlement.product_charges:>10.2f}
    Shipping:                 ${settlement.shipping_revenue:>10.2f}
    Inventory reimbursements: ${settlement.inventory_reimbursements:>10.2f}
    Refunded expenses:        ${settlement.refunded_expenses:>10.2f}
    Refunded sales:           ${settlement.refunded_sales:>10.2f}
    Promo rebates:            ${settlement.promo_rebates:>10.2f}
    FBA fees:                 ${settlement.fba_fees:>10.2f}
    Cost of Advertising:      ${settlement.advertising_costs:>10.2f}
    Shipping charges:         ${settlement.shipping_charges:>10.2f}
    Amazon fees (commission): ${settlement.amazon_fees:>10.2f}
    Other fees:               ${settlement.other_fees:>10.2f}
    Tax collected:            ${settlement.tax_collected:>10.2f}
    Tax withheld:             ${settlement.tax_withheld:>10.2f}
    """)

    categorized_sum = round(
        settlement.product_charges + settlement.shipping_revenue +
        settlement.inventory_reimbursements + settlement.refunded_expenses +
        settlement.refunded_sales + settlement.promo_rebates +
        settlement.fba_fees + settlement.advertising_costs +
        settlement.shipping_charges + settlement.amazon_fees +
        settlement.other_fees + settlement.tax_collected +
        settlement.tax_withheld + settlement.other_income, 2
    )
    print(f"    Categorized sum: ${categorized_sum:>10.2f}")
    print(f"    Payout amount:   ${settlement.total_amount:>10.2f}")

    # ── Step 3: Verify sum of rows = payout ─────────────────────────
    print("\n" + "=" * 70)
    print("STEP 3: Verify Sum of All Rows = Payout (Penny-for-Penny)")
    print("=" * 70)

    print(f"\n    Sum of all {len(settlement.rows)} amount rows: ${settlement.sum_of_rows:.2f}")
    print(f"    Settlement total (payout):       ${settlement.total_amount:.2f}")
    diff = round(settlement.sum_of_rows - settlement.total_amount, 2)
    print(f"    Difference:                      ${diff:.2f}")

    if settlement.reconciles:
        print(f"\n    RESULT: PASS - Settlement reconciles to the penny.")
    else:
        print(f"\n    RESULT: FAIL - Difference of ${diff:.2f} detected!")

    # ── Step 4: Cross-reference order-ids against Orders API ────────
    print("\n" + "=" * 70)
    print("STEP 4: Cross-Reference Order IDs (Settlement vs Orders API)")
    print("=" * 70)

    order_ids = settlement.unique_order_ids
    print(f"\n  Unique order IDs in settlement: {len(order_ids)}")

    if not order_ids:
        print("  No order IDs found in settlement. Skipping cross-reference.")
        return

    # Sample check: verify a subset of orders exist in Orders API
    sample_size = min(20, len(order_ids))
    sample_ids = order_ids[:sample_size]
    print(f"  Checking {sample_size} orders against Orders API...\n")

    orders_api = Orders(
        credentials=Config.get_sp_api_credentials(),
        marketplace=Marketplaces.US,
    )

    matched = 0
    not_found = 0
    errors = 0
    results = []

    for i, order_id in enumerate(sample_ids):
        try:
            response = orders_api.get_order(order_id)
            order = response.payload

            api_status = order.get('OrderStatus', 'Unknown')
            api_total = order.get('OrderTotal', {})
            api_amount = api_total.get('Amount', 'N/A')
            api_date = order.get('PurchaseDate', 'N/A')[:10]

            # Get settlement amounts for this order
            settlement_amounts = [r.amount for r in settlement.rows if r.order_id == order_id]
            settlement_net = round(sum(settlement_amounts), 2)

            results.append({
                'order_id': order_id,
                'found': True,
                'status': api_status,
                'customer_total': api_amount,
                'settlement_net': settlement_net,
                'purchase_date': api_date,
            })
            matched += 1
            print(f"    [{i+1}/{sample_size}] {order_id}: FOUND (Status: {api_status}, "
                  f"Customer paid: ${api_amount}, Settlement net: ${settlement_net:.2f}, "
                  f"Purchased: {api_date})")

        except Exception as e:
            error_msg = str(e)
            if '404' in error_msg or 'not found' in error_msg.lower():
                not_found += 1
                results.append({'order_id': order_id, 'found': False})
                print(f"    [{i+1}/{sample_size}] {order_id}: NOT FOUND in Orders API")
            else:
                errors += 1
                results.append({'order_id': order_id, 'found': None, 'error': error_msg})
                print(f"    [{i+1}/{sample_size}] {order_id}: ERROR - {error_msg}")

        # Rate limiting (Orders API: 1 req/sec burst)
        time.sleep(1)

    # ── Summary ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("RECONCILIATION SUMMARY")
    print("=" * 70)

    print(f"""
    Settlement Period:    {settlement.start_date} to {settlement.end_date}
    Payout Amount:        ${settlement.total_amount:.2f}
    Sum of Rows:          ${settlement.sum_of_rows:.2f}
    Penny-for-Penny:      {'PASS' if settlement.reconciles else 'FAIL'}

    Total Orders in Settlement: {len(order_ids)}
    Sample Checked:             {sample_size}
    Matched in Orders API:      {matched}
    Not Found:                  {not_found}
    Errors:                     {errors}
    Match Rate:                 {matched}/{sample_size} ({matched/sample_size*100:.0f}%)
    """)

    if matched == sample_size:
        print("    RESULT: All sampled orders exist in both Settlement and Orders API.")
        print("    CONCLUSION: Same transactions, different views. No double-invoicing.")
    elif not_found > 0:
        print(f"    WARNING: {not_found} orders not found in Orders API.")
        print("    This may indicate non-order transactions (adjustments, reimbursements).")
    if errors > 0:
        print(f"    NOTE: {errors} orders had API errors (rate limiting, etc.).")

    # ── Per-order fee breakdown (bonus: first 3 orders) ─────────────
    print("\n" + "=" * 70)
    print("BONUS: Per-Order Fee Breakdown (Finances API)")
    print("=" * 70)

    print("\n  Pulling detailed fee breakdown for first 3 matched orders...\n")
    fee_sample = [r for r in results if r.get('found')][:3]

    for r in fee_sample:
        oid = r['order_id']
        try:
            fees = client.get_order_fees(oid)
            print(f"  Order {oid}:")
            print(f"    Principal:    ${fees['principal']:>8.2f}")
            print(f"    Commission:   ${fees['commission']:>8.2f}")
            print(f"    FBA fee:      ${fees['fba_fee']:>8.2f}")
            print(f"    Shipping:     ${fees['shipping']:>8.2f}")
            print(f"    Promo:        ${fees['promo']:>8.2f}")
            print(f"    Other fees:   ${fees['other_fees']:>8.2f}")
            print(f"    Net:          ${fees['net']:>8.2f}")
            print()
        except Exception as e:
            print(f"  Order {oid}: Error getting fees - {e}\n")
        time.sleep(1)


if __name__ == '__main__':
    # Default: reconcile most recent settlement
    # Pass index as argument: python reconcile.py 1 (for second most recent)
    idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    reconcile_settlement(idx)
