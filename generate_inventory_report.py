"""
Generate Pura Vitalia Inventory Reconciliation Report.
Pulls Ledger Summary + Current + Reserved data from SP-API and outputs a clean CSV.
"""
import time
import gzip
import csv
import io
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from src.config import Config

OUTPUT_CSV = "inventory_report.csv"


def download_report(reports_api, report_type, start_date=None, end_date=None, options=None):
    kwargs = {'reportType': report_type}
    if start_date:
        kwargs['dataStartTime'] = start_date.strftime('%Y-%m-%dT00:00:00Z')
    if end_date:
        kwargs['dataEndTime'] = end_date.strftime('%Y-%m-%dT23:59:59Z')
    if options:
        kwargs['reportOptions'] = options

    print(f"  Requesting: {report_type}")
    response = reports_api.create_report(**kwargs)
    report_id = response.payload.get('reportId')

    for attempt in range(30):
        time.sleep(10)
        status = reports_api.get_report(report_id).payload
        processing_status = status.get('processingStatus')
        print(f"  Status: {processing_status} (attempt {attempt + 1})")
        if processing_status == 'DONE':
            doc_id = status.get('reportDocumentId')
            break
        elif processing_status in ('CANCELLED', 'FATAL'):
            print(f"  [WARN] Report {report_type} returned {processing_status}")
            return None
    else:
        return None

    doc_response = reports_api.get_report_document(doc_id)
    doc_info = doc_response.payload
    download_url = doc_info.get('url')
    compression = doc_info.get('compressionAlgorithm', '')

    r = requests.get(download_url)
    r.raise_for_status()

    if compression == 'GZIP':
        content = gzip.decompress(r.content)
        return content.decode('utf-8')
    return r.text


def parse_tsv(raw_text):
    if not raw_text:
        return [], []
    reader = csv.DictReader(io.StringIO(raw_text), delimiter='\t')
    return list(reader), reader.fieldnames


def short_name(title):
    scent = "Unknown"
    for s in ["Lavender", "Peppermint", "Citrus", "Unscented"]:
        if s in title:
            scent = s
            break
    size = ""
    if "32 oz" in title:
        size = "32 oz"
    elif "16 oz" in title:
        size = "16 oz"
    return f"{scent} {size}".strip()


def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def parse_ledger_date(date_str):
    """Parse MM/YYYY format to comparable (year, month) tuple."""
    parts = date_str.split('/')
    if len(parts) == 2:
        return (int(parts[1]), int(parts[0]))  # (year, month)
    return (0, 0)


def main():
    print("[INVENTORY REPORT] Pura Vitalia")
    print("=" * 60)

    credentials = {
        'lwa_app_id': Config.SP_API_CLIENT_ID,
        'lwa_client_secret': Config.SP_API_CLIENT_SECRET,
        'refresh_token': Config.SP_API_REFRESH_TOKEN,
    }
    reports_api = Reports(credentials=credentials, marketplace=Marketplaces.US)

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=89)

    # --- Pull 3 reports with delays to avoid rate limiting ---
    print("\n[1/3] Ledger Summary (90 days)...")
    summary_text = download_report(
        reports_api, 'GET_LEDGER_SUMMARY_VIEW_DATA',
        start_date, end_date, options={'aggregateByLocation': 'FC'}
    )

    print("\n  Waiting 30s before next request (rate limit)...")
    time.sleep(30)

    print("[2/3] Current Inventory...")
    current_text = download_report(reports_api, 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA')

    print("\n  Waiting 30s before next request (rate limit)...")
    time.sleep(30)

    print("[3/3] Reserved Inventory...")
    reserved_text = download_report(reports_api, 'GET_RESERVED_INVENTORY_DATA')

    # --- Parse ---
    summary_rows, _ = parse_tsv(summary_text)
    current_rows, _ = parse_tsv(current_text)
    reserved_rows, _ = parse_tsv(reserved_text)

    # --- Aggregate summary by ASIN ---
    products = {}
    agg = defaultdict(lambda: {
        'starting': 0, 'receipts': 0, 'sales': 0, 'returns': 0,
        'vendor_ret': 0, 'transfers': 0, 'in_transit': 0,
        'found': 0, 'lost': 0, 'damaged': 0, 'disposed': 0,
        'other': 0, 'unknown': 0, 'ending': 0,
    })

    # Track dispositions separately
    disposition_totals = defaultdict(lambda: defaultdict(int))

    # Find date boundaries per ASIN+location+disposition using proper date parsing
    earliest_dates = {}  # (asin, loc, disp) -> (year, month)
    latest_dates = {}

    for r in summary_rows:
        asin = r['ASIN']
        loc = r.get('Location', '')
        disp = r.get('Disposition', 'SELLABLE')
        date_tuple = parse_ledger_date(r['Date'])
        key = (asin, loc, disp)

        if key not in earliest_dates or date_tuple < earliest_dates[key]:
            earliest_dates[key] = date_tuple
        if key not in latest_dates or date_tuple > latest_dates[key]:
            latest_dates[key] = date_tuple

    for r in summary_rows:
        asin = r['ASIN']
        loc = r.get('Location', '')
        disposition = r.get('Disposition', 'SELLABLE')
        date_tuple = parse_ledger_date(r['Date'])
        key = (asin, loc, disposition)

        if asin not in products:
            products[asin] = short_name(r.get('Title', ''))

        # Track disposition ending balances (only from latest month)
        if date_tuple == latest_dates[key]:
            disposition_totals[asin][disposition] += safe_int(r.get('Ending Warehouse Balance', 0))

        # Only aggregate SELLABLE for the main reconciliation
        if disposition != 'SELLABLE':
            continue

        a = agg[asin]
        a['receipts'] += safe_int(r.get('Receipts', 0))
        a['sales'] += safe_int(r.get('Customer Shipments', 0))
        a['returns'] += safe_int(r.get('Customer Returns', 0))
        a['vendor_ret'] += safe_int(r.get('Vendor Returns', 0))
        a['transfers'] += safe_int(r.get('Warehouse Transfer In/Out', 0))
        a['in_transit'] += safe_int(r.get('In Transit Between Warehouses', 0))
        a['found'] += safe_int(r.get('Found', 0))
        a['lost'] += safe_int(r.get('Lost', 0))
        a['damaged'] += safe_int(r.get('Damaged', 0))
        a['disposed'] += safe_int(r.get('Disposed', 0))
        a['other'] += safe_int(r.get('Other Events', 0))
        a['unknown'] += safe_int(r.get('Unknown Events', 0))

        # Starting: only from earliest month for this location
        if date_tuple == earliest_dates[key]:
            a['starting'] += safe_int(r.get('Starting Warehouse Balance', 0))

        # Ending: only from latest month for this location
        if date_tuple == latest_dates[key]:
            a['ending'] += safe_int(r.get('Ending Warehouse Balance', 0))

    # --- Parse current inventory ---
    current = {}
    for r in current_rows:
        asin = r['asin']
        current[asin] = {
            'total': safe_int(r.get('afn-total-quantity', 0)),
            'fulfillable': safe_int(r.get('afn-fulfillable-quantity', 0)),
            'reserved': safe_int(r.get('afn-reserved-quantity', 0)),
            'unsellable': safe_int(r.get('afn-unsellable-quantity', 0)),
            'inbound_working': safe_int(r.get('afn-inbound-working-quantity', 0)),
            'inbound_shipped': safe_int(r.get('afn-inbound-shipped-quantity', 0)),
            'inbound_receiving': safe_int(r.get('afn-inbound-receiving-quantity', 0)),
            'researching': safe_int(r.get('afn-researching-quantity', 0)),
        }

    # --- Parse reserved ---
    reserved = {}
    for r in reserved_rows:
        asin = r['asin']
        reserved[asin] = {
            'total': safe_int(r.get('reserved_qty', 0)),
            'customer_orders': safe_int(r.get('reserved_customerorders', 0)),
            'fc_transfers': safe_int(r.get('reserved_fc-transfers', 0)),
            'fc_processing': safe_int(r.get('reserved_fc-processing', 0)),
        }

    # --- Write CSV ---
    sorted_asins = sorted(products.keys())

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)

        # Title
        w.writerow([f"Pura Vitalia Inventory Report | {start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}"])
        w.writerow([])

        # === SECTION 1: Inventory Movement ===
        w.writerow(["INVENTORY MOVEMENT (Sellable Units)"])
        w.writerow([
            "ASIN", "Product",
            "Starting", "Receipts", "Sales", "Returns", "Vendor Ret.",
            "Transfers Net", "In Transit",
            "Found", "Lost", "Damaged", "Disposed", "Other", "Unknown",
            "Ending (Ledger)", "Computed", "Match?"
        ])

        grand = defaultdict(int)
        for asin in sorted_asins:
            a = agg[asin]
            computed = (
                a['starting'] + a['receipts'] + a['sales'] + a['returns']
                + a['vendor_ret'] + a['transfers'] + a['found'] + a['lost']
                + a['damaged'] + a['disposed'] + a['other'] + a['unknown']
            )
            match = "OK" if computed == a['ending'] else f"OFF by {a['ending'] - computed}"

            w.writerow([
                asin, products[asin],
                a['starting'], a['receipts'], a['sales'], a['returns'], a['vendor_ret'],
                a['transfers'], a['in_transit'],
                a['found'], a['lost'], a['damaged'], a['disposed'], a['other'], a['unknown'],
                a['ending'], computed, match
            ])

            for key in a:
                grand[key] += a[key]

        # Grand total
        grand_computed = (
            grand['starting'] + grand['receipts'] + grand['sales'] + grand['returns']
            + grand['vendor_ret'] + grand['transfers'] + grand['found'] + grand['lost']
            + grand['damaged'] + grand['disposed'] + grand['other'] + grand['unknown']
        )
        grand_match = "OK" if grand_computed == grand['ending'] else f"OFF by {grand['ending'] - grand_computed}"
        w.writerow([
            "", "TOTAL",
            grand['starting'], grand['receipts'], grand['sales'], grand['returns'], grand['vendor_ret'],
            grand['transfers'], grand['in_transit'],
            grand['found'], grand['lost'], grand['damaged'], grand['disposed'], grand['other'], grand['unknown'],
            grand['ending'], grand_computed, grand_match
        ])

        w.writerow([])
        w.writerow([])

        # === SECTION 2: Current Snapshot ===
        w.writerow(["CURRENT INVENTORY SNAPSHOT (right now)"])
        if current:
            w.writerow([
                "ASIN", "Product",
                "Total", "Fulfillable", "Reserved", "Unsellable",
                "Inbound (Working)", "Inbound (Shipped)", "Inbound (Receiving)", "Researching"
            ])
            for asin in sorted_asins:
                c = current.get(asin, {})
                w.writerow([
                    asin, products[asin],
                    c.get('total', 0), c.get('fulfillable', 0), c.get('reserved', 0), c.get('unsellable', 0),
                    c.get('inbound_working', 0), c.get('inbound_shipped', 0), c.get('inbound_receiving', 0),
                    c.get('researching', 0)
                ])
        else:
            w.writerow(["(Report unavailable - rate limited, try again in a few minutes)"])

        w.writerow([])
        w.writerow([])

        # === SECTION 3: Reserved Breakdown ===
        w.writerow(["RESERVED BREAKDOWN"])
        if reserved:
            w.writerow([
                "ASIN", "Product",
                "Total Reserved", "Customer Orders", "FC Transfers", "FC Processing"
            ])
            for asin in sorted_asins:
                r = reserved.get(asin, {})
                w.writerow([
                    asin, products[asin],
                    r.get('total', 0), r.get('customer_orders', 0),
                    r.get('fc_transfers', 0), r.get('fc_processing', 0)
                ])
        else:
            w.writerow(["(Report unavailable - rate limited, try again in a few minutes)"])

        w.writerow([])
        w.writerow([])

        # === SECTION 4: Disposition Breakdown ===
        w.writerow(["DISPOSITION BREAKDOWN (ending units by status)"])
        all_dispositions = set()
        for asin in sorted_asins:
            all_dispositions.update(disposition_totals[asin].keys())
        disp_list = sorted(all_dispositions)

        w.writerow(["ASIN", "Product"] + disp_list + ["Total"])

        for asin in sorted_asins:
            row = [asin, products[asin]]
            total = 0
            for d in disp_list:
                val = disposition_totals[asin].get(d, 0)
                row.append(val)
                total += val
            row.append(total)
            w.writerow(row)

    print(f"\n[OK] Report saved to: {OUTPUT_CSV}")
    print(f"[INFO] {len(sorted_asins)} products, 4 sections")

    # Quick console preview
    print(f"\n{'=' * 60}")
    print("QUICK SUMMARY")
    print(f"{'=' * 60}")
    for asin in sorted_asins:
        a = agg[asin]
        c = current.get(asin, {})
        computed = (
            a['starting'] + a['receipts'] + a['sales'] + a['returns']
            + a['vendor_ret'] + a['transfers'] + a['found'] + a['lost']
            + a['damaged'] + a['disposed'] + a['other'] + a['unknown']
        )
        match = "OK" if computed == a['ending'] else f"OFF by {a['ending'] - computed}"
        print(f"\n  {products[asin]} ({asin})")
        print(f"    Ledger:  Start {a['starting']} + movements = {computed} | Ending {a['ending']} | {match}")
        if c:
            print(f"    Current: Total {c.get('total', 0)} = {c.get('fulfillable', 0)} available + {c.get('reserved', 0)} reserved + {c.get('unsellable', 0)} unsellable")


if __name__ == '__main__':
    main()
