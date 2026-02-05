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
        b64_token = base64.b64encode(self.raw_token.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            'Authorization': f'Basic {b64_token}',
            'Content-Length': '0'
        }
        
        # Removed verbose AUTH logs
        
        resp = requests.post(self.AUTH_URL, headers=headers, timeout=30)
        # print(f"AUTH RESPONSE ({resp.status_code}): {resp.text}")
        
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

    def fetch_daily_stats(self, account_ids: list[str], start_date: str, end_date: str, log_tag: str = '[BH-D-Daily-Sync]') -> dict:
        """
        Fetch stats for Discovery platform.
        Output: { (account_id, date): {spend, impressions, clicks, conversions} }
        """
        
        # 1. Get Campaigns
        headers = self._get_headers()
        params = {'country_id': 'tw'} # From PHP
        
        # print(f"Fetching D Campaigns List from {self.CAMPAIGN_LIST_URL}...")
        resp = requests.get(self.CAMPAIGN_LIST_URL, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch campaigns: {resp.text}")
            print(f"Failed to fetch campaigns: {resp.text}")
            return {}
            
        campaigns = resp.json().get('data', [])
        # print(f"Found {len(campaigns)} campaigns.")
        
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

            if not acc_id or acc_id == 'None':
                # print(f"  [WARNING] No Account ID for Campaign {cam.get('id')} (Acc None), skipping...")
                continue
            
            # Optimization: Skip expired campaigns
            cam_end_str = cam.get('end_date')
            if cam_end_str:
                try:
                    if ' ' in cam_end_str:
                        cam_end = datetime.strptime(cam_end_str, '%Y-%m-%d %H:%M:%S')
                    else:
                        cam_end = datetime.strptime(cam_end_str, '%Y-%m-%d')
                    
                    cutoff_date = cam_end + timedelta(days=30)
                    req_start = datetime.strptime(start_date, '%Y-%m-%d')
                    
                    if req_start > cutoff_date:
                        continue
                        
                except Exception as e:
                    # print(f"  Date check warning for cam {cam_id}: {e}")
                    pass

            # Removed verbose processing log: print(f"Processing Campaign...")
            
            # Fetch Ads
            ad_list_url = self.AD_LIST_BASE_URL.format(cam_id)
            try:
                ad_resp = requests.get(ad_list_url, headers=headers, timeout=20)
                if ad_resp.status_code != 200:
                    # print(f"  Failed to fetch ads for cam {cam_id}: {ad_resp.status_code}")
                    continue
            except Exception as e:
                # print(f"  Error fetching ads for cam {cam_id}: {e}")
                continue
                
            ads = ad_resp.json().get('data', [])
            if not ads:
                continue
            
            # 3. Get Report for each Ad
            for j, ad in enumerate(ads):
                ad_id = str(ad.get('mongo_id'))
                report_cam_id = str(ad.get('campaign', cam_id))
                
                # API expects YYYYMMDD format (no hyphens)
                s_date = start_date.replace('-', '')
                e_date = end_date.replace('-', '')
                
                report_url = self.REPORT_BASE_URL.format(report_cam_id, ad_id, s_date, e_date)
                
                # Fetch Report - Retry Logic
                retry_count = 0
                max_retries = 3
                report_data = []
                
                while retry_count < max_retries:
                    try:
                        rep_resp = requests.get(report_url, headers=headers, timeout=20)
                        if rep_resp.status_code == 200:
                            r_json = rep_resp.json()
                            
                            if r_json.get('code') == 1 and 'operateTooMuch' in r_json.get('msg', ''):
                                print("    Rate limit hit, sleeping 1s...")
                                time.sleep(1)
                                retry_count += 1
                                continue
                                
                            report_data = r_json.get('data', [])
                            # Removed [D-RAW-DATA] log
                            break
                        else:
                            # print(f"    Error {rep_resp.status_code} fetching report.")
                            retry_count += 1
                            time.sleep(1)
                    except Exception as e:
                         # print(f"    Exc fetching report: {e}")
                         retry_count += 1
                         time.sleep(1)
                
                # Aggregate Stats
                if report_data:
                    data_list = report_data.values() if isinstance(report_data, dict) else report_data
                    
                    for day_stat in data_list:
                        # Removed [DATA FOUND] log
                        
                        date_str = day_stat.get('date', day_stat.get('day'))
                        if not date_str:
                            continue
                            
                        key = (acc_id, date_str)
                        if key not in stats_result:
                            stats_result[key] = {
                                'spend': 0.0,
                                'impressions': 0,
                                'clicks': 0,
                                'conversions': 0
                            }
                            
                        stats_result[key]['spend'] += float(day_stat.get('charge', day_stat.get('cost', 0)))
                        stats_result[key]['impressions'] += int(day_stat.get('imp', 0))
                        stats_result[key]['clicks'] += int(day_stat.get('click', 0))
                        stats_result[key]['conversions'] += int(day_stat.get('cv', 0))

            # Debug: Print running total for this account
            relevant_keys = [k for k in stats_result.keys() if k[0] == acc_id]
            for rk in relevant_keys:
                # RENAMED LOG
                print(f"{log_tag} Account {acc_id} Date {rk[1]}: {stats_result[rk]}")

        return stats_result
