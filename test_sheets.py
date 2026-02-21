"""
Test Google Sheets connection.
Run this to verify the Service Account can access the sheet.
"""
from pathlib import Path

def test_sheets_connection():
    print("[TEST] Testing Google Sheets Connection")
    print("=" * 50)

    # Load config
    from src.config import Config

    sheet_id = Config.GOOGLE_SHEET_ID
    if not sheet_id:
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return False

    print(f"[SHEET] ID: {sheet_id}")

    # Check credentials file
    creds_path = Path(__file__).parent / 'config' / 'google_credentials.json'
    if not creds_path.exists():
        print(f"[ERROR] Credentials not found at: {creds_path}")
        return False

    print(f"[OK] Credentials file found")

    # Try to connect
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
        client = gspread.authorize(credentials)

        print("[OK] Google API authenticated")

        # Try to open the sheet
        spreadsheet = client.open_by_key(sheet_id)
        print(f"[OK] Spreadsheet opened: {spreadsheet.title}")

        # List worksheets
        worksheets = spreadsheet.worksheets()
        print(f"[INFO] Worksheets found: {[ws.title for ws in worksheets]}")

        # Try to read first cell
        sheet = worksheets[0]
        cell_value = sheet.acell('A1').value
        print(f"[INFO] Cell A1 value: {cell_value}")

        # Try to write a test value
        test_cell = 'Z1'  # Far away cell to not disturb data
        sheet.update_acell(test_cell, 'TEST_CONNECTION_OK')
        print(f"[OK] Write test successful (wrote to {test_cell})")

        # Clean up test
        sheet.update_acell(test_cell, '')
        print(f"[OK] Cleaned up test cell")

        print("\n" + "=" * 50)
        print("[SUCCESS] Google Sheets connection working!")
        print("=" * 50)
        return True

    except gspread.exceptions.SpreadsheetNotFound:
        print("\n[ERROR] Spreadsheet not found!")
        print("   Make sure you shared the sheet with:")
        print("   amazon-report-bot@pura-vitalia-reports.iam.gserviceaccount.com")
        return False

    except gspread.exceptions.APIError as e:
        print(f"\n[ERROR] API Error: {e}")
        if "403" in str(e):
            print("   The Service Account doesn't have permission.")
            print("   Make sure you shared the sheet as EDITOR with:")
            print("   amazon-report-bot@pura-vitalia-reports.iam.gserviceaccount.com")
        return False

    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")
        return False


if __name__ == '__main__':
    test_sheets_connection()
