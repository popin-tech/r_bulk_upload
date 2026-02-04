import requests
import time
import base64
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

class DiscoveryClient:
    AUTH_URL = 'https://s2s.popin.cc/data/v1/authentication'
    CAMPAIGN_LIST_URL = 'https://s2s.popin.cc/discovery/api/v2/campaign/lists'
    AD_LIST_BASE_URL = 'https://s2s.popin.cc/discovery/api/v2/ad/{}/lists'
    REPORT_BASE_URL = 'https://s2s.popin.cc/discovery/api/v2/ad/{}/{}/{}/{}/date_reporting'

    def __init__(self, raw_token: str):
        self.raw_token = raw_token
        self.access_token = None
        self.token_expiry = 0

    def _get_access_token(self):
        # Initial or refresh token
        # Basic Auth with raw_token
        # raw_token is already base64 encoded? 
        # In discovery.php: $baseToken = base64_encode($this->token);
        # So we need to base64 encode the input token.
        
        b64_token = base64.b64encode(self.raw_token.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            'Authorization': f'Basic {b64_token}',
            'Content-Length': '0'
        }
        
        print(f"--- D API AUTH ---")
        print(f"Raw Token: {self.raw_token}")
        print(f"B64 Token: {b64_token}")
        
        resp = requests.post(self.AUTH_URL, headers=headers, timeout=30)
        print(f"AUTH RESPONSE ({resp.status_code}): {resp.text}")
        
        if resp.status_code != 200:
            raise Exception(f"Discovery Auth Failed: {resp.text}")
            
        data = resp.json()
        self.access_token = data.get('access_token')
        # Token valid for? Usually 1 hour. Set simple expiry (55 mins)
        self.token_expiry = time.time() + 3300 
        return self.access_token

    def _get_headers(self):
        if not self.access_token or time.time() > self.token_expiry:
            self._get_access_token()
        return {'Authorization': f'Bearer {self.access_token}'}

    def fetch_daily_stats(self, account_ids: list[str], start_date: str, end_date: str) -> dict:
        """
        Fetch stats for Discovery platform.
        Output: { (account_id, date): {spend, impressions, clicks, conversions} }
        Note: account_ids might not be directly usable if token is for specific accounts?
        In discovery.php, token seems strictly bound to account(s).
        One token -> Multiple campaigns -> Ads.
        If `account_ids` list contains IDs that this token cannot access, they will be skipped naturally.
        
        NOTE: This client instantiation usually takes ONE token.
        If we have multiple accounts with DIFFERENT tokens, we need multiple instances of DiscoveryClient.
        The caller (sync service) should group accounts by Token.
        """
        
        # 1. Get Campaigns
        # discovery.php: getCampaignData
        headers = self._get_headers()
        params = {'country_id': 'tw'} # From PHP
        
        print(f"Fetching D Campaigns List from {self.CAMPAIGN_LIST_URL}...")
        resp = requests.get(self.CAMPAIGN_LIST_URL, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch campaigns: {resp.text}")
            print(f"Failed to fetch campaigns: {resp.text}")
            return {}
            
        campaigns = resp.json().get('data', [])
        print(f"Found {len(campaigns)} campaigns.")
        
        if not campaigns:
            return {}

        # 2. Get Ads for each Campaign
        stats_result = {}
        
        for i, cam in enumerate(campaigns):
            cam_id = str(cam.get('mongo_id'))
            acc_id = str(cam.get('account_id')) 
            
            # Fallback: If API returns no ID, but we were given a specific single ID to fetch, assume it belongs to that ID.
            if (not acc_id or acc_id == 'None') and account_ids and len(account_ids) == 1:
                acc_id = str(account_ids[0])
                # print(f"  [Fallback] Using provided Account ID {acc_id} for Campaign {cam.get('id')}")

            if not acc_id or acc_id == 'None':
                print(f"  [WARNING] No Account ID for Campaign {cam.get('id')} (Acc None), skipping...")
                continue
            
            # Optimization: Skip expired campaigns (Logic from discovery.php)
            # if start_date > (campaign_end_date + 1 month), skip.
            cam_end_str = cam.get('end_date')
            if cam_end_str:
                try:
                    # Parse YYYY-MM-DD
                    # cam_end_str might be full datetime or date? PHP uses strtotime. 
                    # Assuming YYYY-MM-DD based on typical API.
                    # Handle potential time part if present
                    if ' ' in cam_end_str:
                        cam_end = datetime.strptime(cam_end_str, '%Y-%m-%d %H:%M:%S')
                    else:
                        cam_end = datetime.strptime(cam_end_str, '%Y-%m-%d')
                    
                    # Add 30 days (approx 1 month)
                    cutoff_date = cam_end + timedelta(days=30)
                    
                    # Request Start Date
                    req_start = datetime.strptime(start_date, '%Y-%m-%d')
                    
                    if req_start > cutoff_date:
                        # print(f"  Skipping expired campaign {cam_id} (Ended {cam_end_str})")
                        continue
                        
                except Exception as e:
                    print(f"  Date check warning for cam {cam_id}: {e}")

            # Debug log every campaign
            print(f"Processing Campaign {i+1}/{len(campaigns)}: ID {cam_id} (Acc {acc_id})")
            # print(f"[DEBUG CAMPAIGN DATA] {cam}")
            
            # Fetch Ads
            ad_list_url = self.AD_LIST_BASE_URL.format(cam_id)
            try:
                ad_resp = requests.get(ad_list_url, headers=headers, timeout=20)
                if ad_resp.status_code != 200:
                    print(f"  Failed to fetch ads for cam {cam_id}: {ad_resp.status_code}")
                    continue
            except Exception as e:
                print(f"  Error fetching ads for cam {cam_id}: {e}")
                continue
                
            ads = ad_resp.json().get('data', [])
            if not ads:
                # print(f"  No ads found for cam {cam_id}")
                continue
            
            # 3. Get Report for each Ad
            for j, ad in enumerate(ads):
                ad_id = str(ad.get('mongo_id'))
                # PHP uses $ad['campaign'] for the first param. 
                # Ideally this matches cam_id, but good to be explicit if API returns it.
                # If 'campaign' key is missing, fall back to cam_id.
                
                # Check keys for debugging if data is found later
                # print(f"    [Ad Keys] {ad.keys()}")
                
                report_cam_id = str(ad.get('campaign', cam_id))
                
                # API expects YYYYMMDD format (no hyphens)
                s_date = start_date.replace('-', '')
                e_date = end_date.replace('-', '')
                
                # URL: /ad/{cam_id}/{ad_id}/{start}/{end}/date_reporting
                report_url = self.REPORT_BASE_URL.format(report_cam_id, ad_id, s_date, e_date)
                
                # print(f"    Fetching report for Ad {ad_id}...")
                # print(f"    [D-URL] {report_url}") # Debug URL structure
                
                # Fetch Report - Retry Logic
                retry_count = 0
                max_retries = 3
                report_data = []
                
                while retry_count < max_retries:
                    try:
                        rep_resp = requests.get(report_url, headers=headers, timeout=20)
                        if rep_resp.status_code == 200:
                            r_json = rep_resp.json()
                            
                            # LOG RAW DATA (ALWAYS)
                            # print(f"    [D-RAW-JSON] {json.dumps(r_json)}") 
                            
                            # Check "code":1 msg "operateTooMuch"
                            if r_json.get('code') == 1 and 'operateTooMuch' in r_json.get('msg', ''):
                                print("    Rate limit hit, sleeping 1s...")
                                time.sleep(1)
                                retry_count += 1
                                continue
                                
                            report_data = r_json.get('data', [])
                            
                            # Debug: Print response summary
                            # print(f"    [D-RESP] Code: {r_json.get('code')}, Msg: {r_json.get('msg', 'N/A')}, Data Len: {len(report_data)}")

                            # If data exists, print it raw to see structure
                            if report_data:
                                print(f"    [D-RAW-DATA] Cam {cam_id} Ad {ad_id}: {json.dumps(report_data)}")
                            else:
                                pass
                                # Optional: Print empty to confirm it's not hanging
                                # print(f"    [D-RESP-EMPTY] Cam {cam_id} Ad {ad_id}")

                            break
                        else:
                            print(f"    Error {rep_resp.status_code} fetching report.")
                            retry_count += 1
                            time.sleep(1)
                    except Exception as e:
                         print(f"    Exc fetching report: {e}")
                         retry_count += 1
                         time.sleep(1)
                
                # Aggregate Stats
                if report_data:
                    # Fix: Handle dict response (keyed by date string) vs list response
                    data_list = report_data.values() if isinstance(report_data, dict) else report_data
                    
                    # print(f"    Got {len(report_data)} report items.")
                    for day_stat in data_list:
                        # Log 'charge' which is the actual field from API
                        print(f"    [DATA FOUND] Cam {cam_id} Ad {ad_id} Date {day_stat.get('date')}: Cost={day_stat.get('charge')} Imp={day_stat.get('imp')} Clicks={day_stat.get('click')}")
                        
                        # date: "2024-01-01"
                        date_str = day_stat.get('date', day_stat.get('day')) # PHP uses 'day' or 'date'? PHP: $item['day']
                        if not date_str:
                            continue
                            
                        # Key: (acc_id, date)
                        key = (acc_id, date_str)
                        if key not in stats_result:
                            stats_result[key] = {
                                'spend': 0.0,
                                'impressions': 0,
                                'clicks': 0,
                                'conversions': 0
                            }
                            
                        stats_result[key]['spend'] += float(day_stat.get('charge', day_stat.get('cost', 0))) # D 'charge' -> Spend
                        stats_result[key]['impressions'] += int(day_stat.get('imp', 0))
                        stats_result[key]['clicks'] += int(day_stat.get('click', 0))
                        stats_result[key]['conversions'] += int(day_stat.get('cv', 0))

            # Debug: Print running total for this account
            # We need to know which key to look at. Since we loop campaigns, we know acc_id.
            # We assume target_date is relevant, but loop might be over range.
            # Let's verify what keys exist for this acc_id.
            relevant_keys = [k for k in stats_result.keys() if k[0] == acc_id]
            for rk in relevant_keys:
                print(f"\n[DEBUG AGGREGATED] Account {acc_id} Date {rk[1]}: {stats_result[rk]}")

        return stats_result
