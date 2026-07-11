import os
import requests
import time
import json
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class RixbeeClient:
    # Token Configurations
    # 優先讀環境變數（RIXBEE_*_TOKEN / RIXBEE_*_USERID），未設定時退回原預設值以免線上中斷。
    # 正式環境建議用 Cloud Run --set-secrets / Secret Manager 注入，勿依賴程式內預設。
    TOKENS = {
        'taiwan': {  # 台客 (agency)
            'token': os.getenv('RIXBEE_AGENCY_TOKEN', 'f3c1b67f25e4423001cd9a29fb310998'),
            'user_id': os.getenv('RIXBEE_AGENCY_USERID', '7161'),
        },
        '4a': {  # 4A (direct)
            'token': os.getenv('RIXBEE_DIRECT_TOKEN', 'f3f63d0b878569c7b824096b1a0f14b2'),
            'user_id': os.getenv('RIXBEE_DIRECT_USERID', '7168'),
        },
        'super': {  # Super (總管帳號，看得到全部)
            'token': os.getenv('RIXBEE_SUPER_TOKEN', 'e36da40d2fe00d708464c0269c051140'),
            'user_id': os.getenv('RIXBEE_SUPER_USERID', '7153'),
        },
    }

    API_URL = 'https://broadciel.rpt.rixbeedesk.com/api/report/v1'

    def __init__(self):
        pass

    def get_report_data(self, account_ids: list[str], start_date: str, end_date: str, agent_id: int = None) -> list[dict]:
        """
        Fetch daily report for given accounts.
        Implements failover: Taiwan -> 4A.
        If agent_id is provided (7168/7161), forces that token.
        """
        # Strict Token Selection based on Agent
        # Normalize agent_id to int if possible
        agent_id_int = None
        try:
            if agent_id is not None:
                agent_id_int = int(agent_id)
        except (ValueError, TypeError):
            pass

        if agent_id_int == 7168:
            # 4A -> 4a Token
            return self._fetch_with_token('4a', account_ids, start_date, end_date)
        elif agent_id_int == 7161:
            # 台客 -> Taiwan Token
            return self._fetch_with_token('taiwan', account_ids, start_date, end_date)
        elif agent_id_int == 7153:
            # Super -> Super Token
            return self._fetch_with_token('super', account_ids, start_date, end_date)

        # agent 未指定：自動偵測帳號類型（台客 -> 4A -> Super），回第一個「有資料」的結果。
        # 任一型 token 缺失/出錯視為該型無資料，繼續試下一型；三型皆查無才回空。
        last_data = []
        last_err = None
        for token_type in ('taiwan', '4a', 'super'):
            try:
                data = self._fetch_with_token(token_type, account_ids, start_date, end_date)
                if data:
                    return data  # 有資料即採用
                last_data = data
            except Exception as e:
                last_err = e
                logger.warning(f"Rixbee {token_type} token failed: {e}. Trying next type...")
                continue
        if last_err and not last_data:
            logger.error(f"Rixbee all token types failed, last error: {last_err}")
        return last_data

    def _post_with_retry(self, url, headers, json_body, timeout=60, max_retries=4):
        """暫時性錯誤退避重試（逾時/連線錯誤/429/5xx）。
        R 的業務錯誤碼（如 1003 每日上限）以 HTTP 200 + status.code 回傳、非暫時性，
        由呼叫端處理、不在此重試。退避節奏對稱 D client。"""
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
                if (resp.status_code == 429 or resp.status_code >= 500) and attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return resp
            except requests.RequestException as e:
                if attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise e
        return resp

    def _fetch_with_token(self, token_type: str, account_ids: list[str], start_date: str, end_date: str) -> list[dict]:
        creds = self.TOKENS.get(token_type)
        if not creds:
            raise ValueError(f"Invalid token type: {token_type}")

        # 認證走 header：避免 token 出現在 URL query string 被各層存取日誌記錄（安全性）
        headers = {
            'x-userid': creds['user_id'],
            'x-authorization': creds['token'],
            'Content-Type': 'application/json',
        }

        # 參數走 POST JSON body。start/end 補分頁上界：R API 預設 end=500 會靜默截斷。
        body = {
            'start_date': start_date,
            'end_date': end_date,
            'timezone': 'UTC+8',
            'currency': 'TWD',
            'dimensions': ['day', 'user_id'],   # user_id 確保回應可依帳號拆分
            'user_id': [str(a) for a in account_ids],
            'start': 0,
            'end': 10000,
        }

        response = self._post_with_retry(self.API_URL, headers, body, timeout=60)

        if response.status_code != 200:
            raise Exception(f"API Error {response.status_code}: {response.text}")

        res_json = response.json()

        # 業務錯誤：status.code != 0（如 1003 每日上限）中止並帶訊息
        status = res_json.get('status', {})
        code = status.get('code')
        if code != 0:
            msg = status.get('message', 'Unknown Error')
            raise Exception(f"Rixbee API Code {code}: {msg}")

        # 資料在 data.data（每列含 payment_revenue 花費、behavior0-6 轉換）
        return res_json.get('data', {}).get('data', [])

    def process_daily_stats(self, raw_data: list[dict], cv_definition: str = None) -> dict:
        """
        Aggregates raw data by (Account ID, Date).
        Returns dict: { (account_id, date): {spend, impressions, clicks, conversions} }
        """
        # CV Mapping (from User description / PHP)
        # behavior1: CompleteCheckout
        # behavior4: AddToCart
        # behavior0: ViewContent
        # behavior2: Checkout
        # behavior3: Bookmark
        # behavior5: Search
        # behavior6: CompleteRegistration
        
        # Normalized CV Mapping (lowercase, no symbols)
        CV_MAP = {
            'completecheckout': 'behavior1',
            'addtocart': 'behavior4',
            'viewcontent': 'behavior0',
            'checkout': 'behavior2',
            'bookmark': 'behavior3',
            'search': 'behavior5',
            'completeregistration': 'behavior6'
        }
        
        target_behaviors = []
        if cv_definition:
            # "CompleteCheckout,AddToCart"
            for cv_name in cv_definition.split(','):
                # Normalize: lowercase and remove all non-alphanumeric characters (including spaces/underscores)
                clean_name = re.sub(r'[^a-zA-Z0-9]', '', cv_name).lower()
                if clean_name in CV_MAP:
                    target_behaviors.append(CV_MAP[clean_name])
        
        # If no definition, maybe default to behavior1? Or 0?
        # User said: "R的cv定義" is user-defined in Excel.
        
        result = {} # (acc_id, date) -> stats
        
        for item in raw_data:
            # PHP logic: $item['user_id'] is likely the account ID since we queried by user_id[]
            # Wait, the PHP response structure has 'user_name' but maybe not 'user_id' in the item?
            # PHP code uses $rixBeeData[$nowDate] which iterates date.
            # The item in PHP code loop seems to NOT have user_id explicitly mapped?
            # But the query has specific user_id[].
            # If we query multiple accounts, how do we distinguish?
            # Actually, `dimensions[]` usually includes `user_id` if we want to split by user.
            # But PHP script didn't add `dimensions[]=user_id`.
            # PHP Usage: "start_date" ... "user_id[]=". $accountId
            # If PHP loops accounts and calls API for each (or batches), let's check.
            # PHP `getRixbeeData` impl:
            # `$accountIds = explode(',', $this->rixbeeAccountIds);`
            # It sends ALL account IDs in one request.
            # But it does NOT add `user_id` to dimensions.
            # Wait, if I query multiple accounts without grouping by user_id, Rixbee might return aggregated data?
            # OR does the returned data contain 'user_id' by default?
            # Let's assume we need to add `dimensions[]=user_id` to be safe/able to split.
            # Or I should check `discovery.php`... wait this is `rixbee.php`.
            # `rixbee.php` dimensions: day, country, group_id, cr_id, cpg_id, ad_channel, ad_target...
            # It groups by almost everything.
            # Since `user_id` is the top level, maybe it's implicit?
            # Actually, looking at `rixbee.php` line 161: 'user_name' => 'brandname'. 
            # It seems user_name is returned. Maybe user_id too?
            # To be safe, I will add `dimensions[]=user_id` to the request params.
            
            acc_id = str(item.get('user_id', '')) # We need to ensure API returns this
            date_str = item.get('day', '') # YYYY-MM-DD
            
            if not acc_id or not date_str:
                continue
                
            key = (acc_id, date_str)
            if key not in result:
                result[key] = {
                    'spend': 0.0,
                    'impressions': 0,
                    'clicks': 0,
                    'conversions': 0
                }
            
            result[key]['spend'] += float(item.get('payment_revenue', 0))
            result[key]['impressions'] += int(item.get('impression', 0))
            result[key]['clicks'] += int(item.get('click', 0))
            
            # Sum Conversions
            cv_count = 0
            for b_field in target_behaviors:
                cv_count += int(item.get(b_field, 0))
            result[key]['conversions'] += cv_count

        return result
