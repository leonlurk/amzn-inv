"""
Test: Verify Amazon Advertising API connection and pull real PPC data.
"""
import json
import time
import gzip
import io
from datetime import datetime, timedelta

import requests

from src.config import Config


ADS_API_BASE = 'https://advertising-api.amazon.com'
TOKEN_URL = 'https://api.amazon.com/auth/o2/token'


def get_access_token():
    """Get access token from refresh token."""
    response = requests.post(TOKEN_URL, data={
        'grant_type': 'refresh_token',
        'refresh_token': Config.ADS_API_REFRESH_TOKEN,
        'client_id': Config.ADS_API_CLIENT_ID,
        'client_secret': Config.ADS_API_CLIENT_SECRET,
    })
    response.raise_for_status()
    return response.json()['access_token']


def get_headers(access_token):
    """Standard headers for Ads API."""
    return {
        'Authorization': f'Bearer {access_token}',
        'Amazon-Advertising-API-ClientId': Config.ADS_API_CLIENT_ID,
        'Amazon-Advertising-API-Scope': Config.ADS_API_PROFILE_ID,
        'Content-Type': 'application/json',
    }


def main():
    print("[TEST] Amazon Advertising API Connection")
    print("=" * 60)

    # Validate config
    if not Config.validate_ads_api():
        print("[ERROR] Ads API credentials not configured!")
        print("Run 'python setup_ads_oauth.py' first.")
        return

    # Test 1: Get access token
    print("\n[1/4] Getting access token...")
    try:
        access_token = get_access_token()
        print(f"  [OK] Token: {access_token[:20]}...")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")
        return

    # Test 2: List profiles
    print("\n[2/4] Listing advertising profiles...")
    try:
        response = requests.get(
            f'{ADS_API_BASE}/v2/profiles',
            headers={
                'Authorization': f'Bearer {access_token}',
                'Amazon-Advertising-API-ClientId': Config.ADS_API_CLIENT_ID,
            }
        )
        response.raise_for_status()
        profiles = response.json()
        print(f"  [OK] Found {len(profiles)} profile(s)")
        for p in profiles:
            print(f"    {p.get('countryCode')} | {p.get('profileId')} | {p.get('accountInfo', {}).get('name', 'N/A')}")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")

    # Test 3: List campaigns
    print("\n[3/4] Listing Sponsored Products campaigns...")
    headers = get_headers(access_token)
    try:
        response = requests.get(
            f'{ADS_API_BASE}/v2/sp/campaigns',
            headers=headers
        )
        response.raise_for_status()
        campaigns = response.json()
        print(f"  [OK] Found {len(campaigns)} campaign(s)")
        for c in campaigns[:10]:
            print(f"    {c.get('name', 'N/A')} | State: {c.get('state')} | Budget: ${c.get('dailyBudget', 0)}")
    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")

    # Test 4: Request a Sponsored Products report (yesterday)
    print("\n[4/4] Requesting SP performance report (yesterday)...")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    try:
        payload = {
            'reportDate': yesterday,
            'metrics': 'impressions,clicks,cost,purchases7d,sales7d',
            'segment': 'placement',
            'creativeType': 'all',
        }
        response = requests.post(
            f'{ADS_API_BASE}/v2/sp/campaigns/report',
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        report_info = response.json()
        report_id = report_info.get('reportId')
        print(f"  Report ID: {report_id}")

        # Wait for report
        for attempt in range(20):
            time.sleep(5)
            status_response = requests.get(
                f'{ADS_API_BASE}/v2/reports/{report_id}',
                headers=headers
            )
            status_response.raise_for_status()
            status = status_response.json()
            print(f"  Status: {status.get('status')} (attempt {attempt + 1})")

            if status.get('status') == 'SUCCESS':
                report_url = status.get('location')
                # Download report
                report_response = requests.get(report_url, headers=headers)
                report_response.raise_for_status()

                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(report_response.content)) as f:
                        report_data = json.loads(f.read().decode('utf-8'))
                except:
                    report_data = report_response.json()

                print(f"\n  [OK] Report data ({len(report_data)} records):")

                total_spend = 0
                total_sales = 0
                total_orders = 0
                total_clicks = 0
                total_impressions = 0

                for record in report_data:
                    total_spend += float(record.get('cost', 0))
                    total_sales += float(record.get('sales7d', 0))
                    total_orders += int(record.get('purchases7d', 0))
                    total_clicks += int(record.get('clicks', 0))
                    total_impressions += int(record.get('impressions', 0))

                print(f"    Impressions:  {total_impressions:,}")
                print(f"    Clicks:       {total_clicks:,}")
                print(f"    Spend:        ${total_spend:.2f}")
                print(f"    Ad Sales:     ${total_sales:.2f}")
                print(f"    Ad Orders:    {total_orders}")
                if total_sales > 0:
                    print(f"    ACoS:         {(total_spend/total_sales)*100:.1f}%")
                if total_spend > 0:
                    print(f"    ROAS:         {total_sales/total_spend:.2f}x")
                break

            elif status.get('status') == 'FAILURE':
                print(f"  [ERROR] Report generation failed")
                break
        else:
            print("  [TIMEOUT] Report didn't complete in time")

    except Exception as e:
        print(f"  [ERROR] {type(e).__name__}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text[:500]}")

    print(f"\n{'=' * 60}")
    print("[DONE]")


if __name__ == '__main__':
    main()
