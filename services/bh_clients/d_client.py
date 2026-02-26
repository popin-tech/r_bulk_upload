import requests
import time
import base64
from datetime import datetime, timedelta
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

    def fetch_daily_stats(self, account_ids: list[str], start_date: str, end_date: str, log_tag: str = '[BH-D-Daily-Sync]', executor: ThreadPoolExecutor = None) -> dict:
        """
        Fetch stats for Discovery platform using Multithreading.
        Output: { (account_id, date): {spend, impressions, clicks, conversions} }
        """
        
        # 1. Get Campaigns (Sequential)
        headers = self._get_headers()
        params = {'country_id': 'tw'}
        
        resp = requests.get(self.CAMPAIGN_LIST_URL, headers=headers, params=params, timeout=30)
 
        if resp.status_code != 200:
            logger.error(f"Failed to fetch campaigns: {resp.text}")
            print(f"Failed to fetch campaigns: {resp.text}")
            return {}
            
        campaigns = resp.json().get('data', [])
        
        if not campaigns:
            return {}

        stats_result = {}
        stats_lock = threading.Lock()
        
        # Helper: Fetch Ads for a Campaign
        def _fetch_ads(cam):
            cam_id = str(cam.get('mongo_id'))
            acc_id = str(cam.get('account_id')) 
            
            # Fallback acc_id logic
            unique_ids = list(set(account_ids)) if account_ids else []
            if (not acc_id or acc_id == 'None') and unique_ids and len(unique_ids) == 1:
                acc_id = str(unique_ids[0])

            if not acc_id or acc_id == 'None':
                return []
            
            # Status and Date Check Logic
            # Condition 1: If status is active (1 or True), allow directly.
            cam_status = cam.get('status')
            is_active = (cam_status == 1 or cam_status == True or str(cam_status).lower() == 'true')
            
            if not is_active:
                # Condition 2: If inactive, check grace period against end_date
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
                            return [] # Campaign is dead and past grace period
                    except Exception:
                        pass
                # If inactive but no end_date exists (or parsing failed), we implicitly allow it to proceed and check ads.

            ad_list_url = self.AD_LIST_BASE_URL.format(cam_id)
            try:
                ad_resp = requests.get(ad_list_url, headers=headers, timeout=20)

                if ad_resp.status_code != 200:
                    return []
                ads = ad_resp.json().get('data', [])
                if not ads: return []
                
                # Attach context to ads
                for ad in ads:
                    ad['__cam_id'] = cam_id
                    ad['__acc_id'] = acc_id
                return ads
            except Exception:
                return []

        # Helper: Fetch Report for an Ad
        def _fetch_report(ad):
            ad_id = str(ad.get('mongo_id'))
            cam_id = ad.get('__cam_id')
            report_cam_id = str(ad.get('campaign', cam_id))
            acc_id = ad.get('__acc_id')
            
            s_date = start_date.replace('-', '')
            e_date = end_date.replace('-', '')
            
            report_url = self.REPORT_BASE_URL.format(report_cam_id, ad_id, s_date, e_date)
            
            retry_count = 0
            max_retries = 3
            report_data = []
            
            while retry_count < max_retries:
                try:
                    rep_resp = requests.get(report_url, headers=headers, timeout=20)
                    if rep_resp.status_code == 200:
                        r_json = rep_resp.json()

                        if r_json.get('code') == 1 and 'operateTooMuch' in r_json.get('msg', ''):
                            logger.warning(f"[D-API-Block] Rate limit hit for Ad {ad_id}: {r_json}")
                            time.sleep(1)
                            retry_count += 1
                            continue
                        
                        report_data = r_json.get('data', [])
                        if report_data:
                            # 簡化日誌輸出，只印出日期與特定指標
                            simplified_data = {}
                            if isinstance(report_data, dict):
                                for k, v in report_data.items():
                                    simplified_data[k] = {'charge': v.get('charge', 0), 'imp': v.get('imp', 0), 'click': v.get('click', 0)}
                            elif isinstance(report_data, list):
                                for v in report_data:
                                    k = v.get('date', v.get('day', 'unknown'))
                                    simplified_data[k] = {'charge': v.get('charge', 0), 'imp': v.get('imp', 0), 'click': v.get('click', 0)}
                            
                            print(f"[D-API-Response] Aid {acc_id} Ad {ad_id} (Code {rep_resp.status_code}): {simplified_data}", flush=True)
                        
                        break
                    else:
                        logger.error(f"[D-API-Error] Aid {acc_id} Status {rep_resp.status_code} for Ad {ad_id}: {rep_resp.text}")
                        retry_count += 1
                        time.sleep(1)
                except Exception as e:
                    logger.exception(f"[D-API-Exception] Aid {acc_id} Ad {ad_id}: {str(e)}")
                    retry_count += 1
                    time.sleep(1)
            
            if not report_data:
                return
                
            data_list = report_data.values() if isinstance(report_data, dict) else report_data
            
            with stats_lock:
                for day_stat in data_list:
                    date_str = day_stat.get('date', day_stat.get('day'))
                    if not date_str: continue
                        
                    key = (acc_id, date_str)
                    if key not in stats_result:
                        stats_result[key] = {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                        
                    stats_result[key]['spend'] += float(day_stat.get('charge', day_stat.get('cost', 0)))
                    stats_result[key]['impressions'] += int(day_stat.get('imp', 0))
                    stats_result[key]['clicks'] += int(day_stat.get('click', 0))
                    stats_result[key]['conversions'] += int(day_stat.get('cv', 0))

        # 2. Parallel Fetch Ads
        all_ads = []
        
        # Use passed executor or create local one
        if executor:
            # Use shared executor
            futures = [executor.submit(_fetch_ads, cam) for cam in campaigns]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_ads.extend(result)
        else:
            # Fallback: Create local pool
            with ThreadPoolExecutor(max_workers=3) as local_executor:
                futures = [local_executor.submit(_fetch_ads, cam) for cam in campaigns]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        all_ads.extend(result)
        
        # 3. Parallel Fetch Reports
        if all_ads:
            if executor:
                futures = [executor.submit(_fetch_report, ad) for ad in all_ads]
                for future in as_completed(futures):
                    future.result()
            else:
                 with ThreadPoolExecutor(max_workers=3) as local_executor:
                    futures = [local_executor.submit(_fetch_report, ad) for ad in all_ads]
                    for future in as_completed(futures):
                        future.result()

        # Debug: Print Final Totals Logic
        # Group keys by Account for logging
        acc_keys = {}
        for (acc_id, date_str) in stats_result.keys():
            if acc_id not in acc_keys: acc_keys[acc_id] = []
            acc_keys[acc_id].append(date_str)
            
        for acc_id, dates in acc_keys.items():
            for d in dates:
                key = (acc_id, d)
                print(f"{log_tag} Account {acc_id} Date {d}: {stats_result[key]}", flush=True)

        return stats_result
