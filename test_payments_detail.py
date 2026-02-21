"""
Pull detailed breakdown of each payment group from Finances API.
"""
from datetime import datetime, timedelta

from sp_api.api import Finances
from sp_api.base import Marketplaces

from src.config import Config


def main():
    print("[TEST] Detailed Payment Breakdown")
    print("=" * 60)

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }

    finances = Finances(
        credentials=credentials,
        marketplace=Marketplaces.US,
    )

    # Get payment groups
    response = finances.list_financial_event_groups(
        MaxResultsPerPage=10,
        FinancialEventGroupStartedAfter=(datetime.now() - timedelta(days=90)).isoformat()
    )

    groups = response.payload.get('FinancialEventGroupList', [])

    for g in groups:
        group_id = g.get('FinancialEventGroupId', '')
        start = g.get('FinancialEventGroupStart', '')
        end = g.get('FinancialEventGroupEnd', '')
        status = g.get('ProcessingStatus', '')
        original = g.get('OriginalTotal', {})
        amount = original.get('CurrencyAmount', 0)
        currency = original.get('CurrencyCode', 'USD')

        if currency != 'USD':
            continue

        print(f"\n{'=' * 60}")
        print(f"Payment: {start[:10] if start else 'N/A'} to {end[:10] if end else 'ongoing'} | {status} | ${amount}")
        print(f"{'=' * 60}")

        # Get detailed financial events for this group
        try:
            events_response = finances.list_financial_events_by_group_id(
                group_id,
                MaxResultsPerPage=100
            )
            events = events_response.payload.get('FinancialEvents', {})

            # Shipment events (sales)
            shipment_events = events.get('ShipmentEventList', [])
            if shipment_events:
                print(f"\n  [SALES/SHIPMENTS] ({len(shipment_events)} events)")
                total_principal = 0
                total_commission = 0
                total_fba_fee = 0
                total_shipping = 0
                total_other = 0

                for event in shipment_events:
                    items = event.get('ShipmentItemList', [])
                    for item in items:
                        charges = item.get('ItemChargeList', [])
                        fees = item.get('ItemFeeList', [])

                        for charge in charges:
                            charge_type = charge.get('ChargeType', '')
                            charge_amount = float(charge.get('ChargeAmount', {}).get('CurrencyAmount', 0))
                            if charge_type == 'Principal':
                                total_principal += charge_amount
                            elif charge_type == 'Tax':
                                pass  # skip tax
                            else:
                                total_other += charge_amount

                        for fee in fees:
                            fee_type = fee.get('FeeType', '')
                            fee_amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0))
                            if fee_type == 'Commission':
                                total_commission += fee_amount
                            elif fee_type == 'FBAPerUnitFulfillmentFee':
                                total_fba_fee += fee_amount
                            elif 'Shipping' in fee_type:
                                total_shipping += fee_amount
                            else:
                                total_other += fee_amount

                print(f"    Product Sales:      ${total_principal:>10.2f}")
                print(f"    Commission (ref):   ${total_commission:>10.2f}")
                print(f"    FBA Fulfillment:    ${total_fba_fee:>10.2f}")
                print(f"    Shipping:           ${total_shipping:>10.2f}")
                print(f"    Other:              ${total_other:>10.2f}")

            # Refund events
            refund_events = events.get('RefundEventList', [])
            if refund_events:
                print(f"\n  [REFUNDS] ({len(refund_events)} events)")
                total_refund = 0
                for event in refund_events:
                    items = event.get('ShipmentItemAdjustmentList', [])
                    for item in items:
                        charges = item.get('ItemChargeAdjustmentList', [])
                        for charge in charges:
                            if charge.get('ChargeType') == 'Principal':
                                total_refund += float(charge.get('ChargeAmount', {}).get('CurrencyAmount', 0))
                print(f"    Refund total:       ${total_refund:>10.2f}")

            # Service fee events
            service_events = events.get('ServiceFeeEventList', [])
            if service_events:
                print(f"\n  [SERVICE FEES] ({len(service_events)} events)")
                for event in service_events:
                    reason = event.get('FeeReason', 'Unknown')
                    fee_list = event.get('FeeList', [])
                    for fee in fee_list:
                        fee_amount = float(fee.get('FeeAmount', {}).get('CurrencyAmount', 0))
                        print(f"    {reason}: ${fee_amount:.2f}")

            # Adjustment events
            adjust_events = events.get('AdjustmentEventList', [])
            if adjust_events:
                print(f"\n  [ADJUSTMENTS] ({len(adjust_events)} events)")
                for event in adjust_events:
                    adj_type = event.get('AdjustmentType', 'Unknown')
                    adj_amount = float(event.get('AdjustmentAmount', {}).get('CurrencyAmount', 0))
                    print(f"    {adj_type}: ${adj_amount:.2f}")

            # Product ads payment events
            product_ads = events.get('ProductAdsPaymentEventList', [])
            if product_ads:
                print(f"\n  [PRODUCT ADS / PPC]")
                for event in product_ads:
                    trans_type = event.get('transactionType', 'Unknown')
                    amount_val = float(event.get('transactionAmount', {}).get('CurrencyAmount', 0))
                    print(f"    {trans_type}: ${amount_val:.2f}")

            # All other event types
            other_keys = [k for k in events.keys() if k not in (
                'ShipmentEventList', 'RefundEventList', 'ServiceFeeEventList',
                'AdjustmentEventList', 'ProductAdsPaymentEventList'
            ) and events[k]]
            if other_keys:
                print(f"\n  [OTHER EVENT TYPES]")
                for key in other_keys:
                    print(f"    {key}: {len(events[key])} events")

        except Exception as e:
            print(f"  [ERROR] {type(e).__name__}: {e}")

    print(f"\n{'=' * 60}")
    print("[DONE]")


if __name__ == '__main__':
    main()
