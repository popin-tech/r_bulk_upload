import requests
import time
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class RixbeeClient:
    # Token Configurations (Ported from rixbee.php)
    TOKENS = {
        'default': {'token': 'f3c1b67f25e4423001cd9a29fb310998', 'user_id': '7161'},
        'direct': {'token': 'f3f63d0b878569c7b824096b1a0f14b2', 'user_id': '7168'},
        'super': {'token': 'e36da40d2fe00d708464c0269c051140', 'user_id': '7153'}
    }

    API_URL = 'https://broadciel.rpt.rixbeedesk.com/api/report/v1'

    def __init__(self):
        pass

    def get_report_data(self, account_ids: list[str], start_date: str, end_date: str) -> list[dict]:
        """
        Fetch daily report for given accounts.
        Implements failover: Default -> Direct.
        """
        # Try Default Token first
        try:
            return self._fetch_with_token('default', account_ids, start_date, end_date)
        except Exception as e:
            logger.warning(f"Rixbee Default token failed: {e}. Trying Direct token...")
            # Try Direct Token
            try:
                return self._fetch_with_token('direct', account_ids, start_date, end_date)
            except Exception as e2:
                logger.error(f"Rixbee Direct token also failed: {e2}")
                raise e2

    def _fetch_with_token(self, token_type: str, account_ids: list[str], start_date: str, end_date: str) -> list[dict]:
        creds = self.TOKENS.get(token_type)
        if not creds:
            raise ValueError(f"Invalid token type: {token_type}")

        # Construct params
        # Note: rixbee.php uses &user_id[]=123 for accounts. 
        # Requests handles list params if passed as a list of tuples or list with '[]' in key?
        # Let's inspect PHP: `&user_id[]=` . $accountId
        
        params = [
            ('start_date', start_date),
            ('end_date', end_date),
            ('timezone', 'UTC+8'),
            ('currency', 'TWD'),
            ('dimensions[]', 'day'),
            #('dimensions[]', 'group_id'), # Removed to aggregate by account/day only? 
            # User requirement: "昨日花費" per account. 
            # PHP script fetches detailed breakdown (group, cr_id, etc.)
            # But BH only needs Account-level daily Stats.
            # Let's check implementation_plan: "bh_daily_stats (Daily Performance)"
            # So we should aggregate by Account + Day.
            # However, the API might require specific dimensions. 
            # PHP usages: dimensions[]=day, country, group_id, cr_id, cpg_id, ad_channel, ad_target
            # If I drop them, maybe the API fails?
            # Let's try to fetch minimal dimensions first: day. 
            # Wait, `user_id` in calls seems to be the "Account ID" (from PHP: $this->rixbeeAccountIds passed as user_id[]).
            # So we group by user_id implicitly if we query multiple?
            # Let's keep it simple: Query by Account ID list, dimension=day.
        ]
        
        # Add dimensions
        # If we only want account-level, maybe we don't need group_id/cr_id.
        # But to be safe and consistent with PHP, if PHP fetches all, maybe we can too?
        # But for BH, we only store account-day summaries. 
        # Aggregating locally is fine.
        params.append(('dimensions[]', 'day'))
        params.append(('dimensions[]', 'user_id')) # Ensure user_id is returned in data
        
        # Add headers (none needed if auth is in params)
        headers = {}
        
        # Add auth to params (PHP implementation uses query string)
        # Note: requests params dict/list is automatically URL-encoded.
        params.append(('x-userid', creds['user_id']))
        params.append(('x-authorization', creds['token']))
        
        # Add account IDs (user_id[])
        # In requests, duplicate keys are handled by passing a list of tuples
        for aid in account_ids:
            params.append(('user_id[]', aid))

        # Debug Logging
        # print(f"--- R API REQUEST ({token_type}) ---")
        
        response = requests.get(self.API_URL, headers=headers, params=params, timeout=60)
        
        # print(f"--- R API RESPONSE ({response.status_code}) ---")

        
        if response.status_code != 200:
            raise Exception(f"API Error {response.status_code}: {response.text}")

        res_json = response.json()
        
        # Check 'status' in body
        # PHP: if($resAry['status']['code'] != 0)
        status = res_json.get('status', {})
        code = status.get('code')
        if code != 0:
            msg = status.get('message', 'Unknown Error')
            # Map specific errors as per PHP
            # '1000' => 'R API 異常', '1003' => '每日上限', etc.
            raise Exception(f"Rixbee API Code {code}: {msg}")

        # Data is in res_json['data']['data']
        # Structure: list of dicts
        # items has 'payment_revenue' (Spend), 'behaviorX' (CVs)
        
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
        
        CV_MAP = {
            'CompleteCheckout': 'behavior1',
            'AddToCart': 'behavior4',
            'ViewContent': 'behavior0',
            'Checkout': 'behavior2',
            'Bookmark': 'behavior3',
            'Search': 'behavior5',
            'CompleteRegistration': 'behavior6'
        }
        
        target_behaviors = []
        if cv_definition:
            # "CompleteCheckout,AddToCart"
            for cv_name in cv_definition.split(','):
                cv_name = cv_name.strip()
                if cv_name in CV_MAP:
                    target_behaviors.append(CV_MAP[cv_name])
        
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
