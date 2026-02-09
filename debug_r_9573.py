
import os
import json
from datetime import datetime
from services.bh_clients.r_client import RixbeeClient

# from dotenv import load_dotenv

# Load env if present
# load_dotenv()

def debug_9573():
    print("--- Debugging Account 9573 (R Platform) ---")
    
    # 1. Initialize Client
    try:
        client = RixbeeClient()
        print("RixbeeClient initialized.")
    except Exception as e:
        print(f"Failed to init client: {e}")
        return

    # 2. Define Parameters
    acc_id = 9573
    # Rixbee limit: max 7 days per request
    chunks = [
        ("2024-01-19", "2024-01-25"),
        ("2024-01-26", "2024-01-31")
    ]
    
    print(f"Fetching report for Account {acc_id} in chunks...")

    # 3. Test ALL Tokens
    tokens_to_test = ['default', 'direct', 'super']
    
    # Target specific date as requested
    chunks = [("2025-01-19", "2025-01-19")]
    
    import requests # Ensure requests is imported locally or globally
    
    for token_name in tokens_to_test:
        print(f"\n>>> Testing Token: {token_name.upper()} <<<")
        creds = client.TOKENS.get(token_name)
        if not creds: continue
        
        # user_id = creds['user_id'] # API User ID
        
        # Manually construct request to inspect it
        # Based on r_client.py:
        # params.append(('x-userid', creds['user_id']))
        # params.append(('x-authorization', creds['token']))
        
        params = [
            ('start_date', '2023-01-19'),
            ('end_date', '2023-01-19'),
            ('timezone', 'UTC+8'),
            ('currency', 'TWD'),
            ('dimensions[]', 'day'),  # Dimensions
            ('dimensions[]', 'user_id'), # Add user_id to output
            ('x-userid', creds['user_id']),      # API User ID (7161 etc)
            ('x-authorization', creds['token']), # API Token
        ]
        # Add target account ID
        params.append(('user_id[]', str(acc_id)))
        
        try:
            print(f"  Requesting 2024-01-19...")
            response = requests.get(client.API_URL, params=params)
            
            print(f"  [REQUEST URL] {response.url}")
            print(f"  [STATUS] {response.status_code}")
            
            try:
                data = response.json()
                print(f"  [RESPONSE JSON] {json.dumps(data, indent=2, ensure_ascii=False)}")
                if isinstance(data, list):
                    print(f"  [COUNT] {len(data)} items")
                else:
                    print(f"  [NOTE] Response is not a list.")
            except:
                print(f"  [RESPONSE TEXT] {response.text}")

        except Exception as e:
            print(f"  [ERROR] Token {token_name} failed: {e}")

if __name__ == "__main__":
    debug_9573()
