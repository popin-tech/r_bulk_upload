import logging
import sys
import time
from datetime import datetime, timedelta
# from app import app, db
# from database import BHAccount
from services.bh_clients.r_client import RixbeeClient

# Configure Logging to stdout
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def test_api_limit():
    # with app.app_context():
        # 1. Find an active R account
        # acc = BHAccount.query.filter_by(platform='R', status='active').first()
    
    # Use Hardcoded Account ID (from R_Client default token user_id)
    # Hoping this has data or at least is a valid account
    account_id = '7161' 
    
    print(f"Testing with Account: {account_id}")
    
    client = RixbeeClient()
    
    # Test Ranges
    # ranges = [1, 2, 7, 30, 31, 60, 90]
    ranges = [1, 7, 30, 60]
    
    # Use yesterday as end date
    yesterday = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
    end_date_str = yesterday.strftime('%Y-%m-%d')
    
    print(f"End Date: {end_date_str}")
    print("-" * 40)
    
    for days in ranges:
        start_dt = yesterday - timedelta(days=days-1) # inclusive
        start_date_str = start_dt.strftime('%Y-%m-%d')
        
        print(f"Testing {days} days range ({start_date_str} to {end_date_str})...", end=" ")
        sys.stdout.flush()
        
        try:
            start_time = time.time()
            data = client.get_report_data([account_id], start_date_str, end_date_str)
            elapsed = time.time() - start_time
            
            # Check actual returned days count? 
            # API returns list of items. We can count unique 'day'.
            unique_days = set()
            for item in data:
                if 'day' in item: unique_days.add(item['day'])
            
            print(f"SUCCESS! ({elapsed:.2f}s)")
            print(f"   -> Items returned: {len(data)}, Unique Days: {len(unique_days)}")
        
        except Exception as e:
            print(f"FAILED!")
            print(f"   -> Error: {str(e)}")
        
        print("-" * 40)
        time.sleep(1) # Be nice

if __name__ == "__main__":
    test_api_limit()
