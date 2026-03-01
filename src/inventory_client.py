"""Inventory client - fetches current FBA inventory levels via SP-API."""
from dataclasses import dataclass

from sp_api.api import Inventories
from sp_api.base import Marketplaces

from .config import Config


@dataclass
class InventoryItem:
    """Current inventory for a single product."""
    asin: str
    sku: str
    product_name: str
    total_quantity: int
    fulfillable: int  # Available to sell
    reserved: int
    unsellable: int
    inbound_working: int
    inbound_shipped: int
    inbound_receiving: int
    researching: int


class InventoryClient:
    """Fetches current FBA inventory using the Inventories API (instant, no report needed)."""

    def __init__(self):
        self.credentials = Config.get_sp_api_credentials()
        self.marketplace = Marketplaces.US

    def fetch_inventory(self) -> list[InventoryItem]:
        """Fetch current FBA inventory snapshot via Inventories API."""
        inv_api = Inventories(credentials=self.credentials, marketplace=self.marketplace)

        print("  Fetching inventory via API...")
        response = inv_api.get_inventory_summary_marketplace(
            details=True,
            granularityType='Marketplace',
            granularityId=Config.MARKETPLACE_ID,
        )

        summaries = response.payload.get('inventorySummaries', [])

        # Aggregate by ASIN (API can return multiple entries per ASIN for different conditions)
        asin_data = {}
        for item in summaries:
            asin = item.get('asin', '')
            total = item.get('totalQuantity', 0)

            # Skip entries with 0 total and no product name
            if total == 0 and asin in asin_data:
                continue

            details = item.get('inventoryDetails', {})
            reserved_info = details.get('reservedQuantity', {})
            researching_info = details.get('researchingQuantity', {})
            unfulfillable_info = details.get('unfulfillableQuantity', {})

            entry = {
                'asin': asin,
                'sku': item.get('sellerSku', ''),
                'product_name': item.get('productName', ''),
                'fulfillable': details.get('fulfillableQuantity', 0),
                'reserved': reserved_info.get('totalReservedQuantity', 0),
                'unsellable': unfulfillable_info.get('totalUnfulfillableQuantity', 0),
                'inbound_working': details.get('inboundWorkingQuantity', 0),
                'inbound_shipped': details.get('inboundShippedQuantity', 0),
                'inbound_receiving': details.get('inboundReceivingQuantity', 0),
                'researching': researching_info.get('totalResearchingQuantity', 0),
                'total': total,
            }

            if asin not in asin_data or total > 0:
                asin_data[asin] = entry

        items = []
        for data in asin_data.values():
            if data['total'] == 0:
                continue
            items.append(InventoryItem(
                asin=data['asin'],
                sku=data['sku'],
                product_name=data['product_name'],
                total_quantity=data['total'],
                fulfillable=data['fulfillable'],
                reserved=data['reserved'],
                unsellable=data['unsellable'],
                inbound_working=data['inbound_working'],
                inbound_shipped=data['inbound_shipped'],
                inbound_receiving=data['inbound_receiving'],
                researching=data['researching'],
            ))

        print(f"  Got inventory for {len(items)} products")
        return items


def get_mock_inventory() -> list[InventoryItem]:
    """Mock inventory data for testing."""
    return [
        InventoryItem(
            asin='B0EXAMPLE1', sku='PV-LAV-32',
            product_name='Pura Vitalia Castile Soap Lavender 32 oz',
            total_quantity=150, fulfillable=120, reserved=25,
            unsellable=5, inbound_working=0, inbound_shipped=50, inbound_receiving=0,
            researching=0,
        ),
        InventoryItem(
            asin='B0EXAMPLE2', sku='PV-PEP-32',
            product_name='Pura Vitalia Castile Soap Peppermint 32 oz',
            total_quantity=85, fulfillable=70, reserved=10,
            unsellable=5, inbound_working=0, inbound_shipped=0, inbound_receiving=0,
            researching=0,
        ),
    ]
