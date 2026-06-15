import requests
import time
import base64
from datetime import datetime, timedelta
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logger = logging.getLogger(__name__)


def _is_rate_limited(status_code: int, text: str) -> bool:
    """popin 兩種限流：報表流量 ReportFlowLimit.operateTooMuch（code:1）與
    IP 速率 IpLimit.operateTooMuch（HTTP 429）。兩者皆 code:1、data 空，
    不重試會被當「查無資料」靜默吞掉導致少抓。一律以 429 或訊息含
    operateTooMuch 判定為限流。（移植自 ad_tools/src/core/http.ts）"""
    return status_code == 429 or 'operateTooMuch' in (text or '')


def _parse_loose_date(v):
    """寬容解析 campaign 日期（popin 日期格式不可靠）。
    回傳 datetime 或 None（解析不出回 None，呼叫端應保留該 campaign 防誤殺）。
    支援：epoch 秒/毫秒、YYYY-MM-DD[ HH:MM:SS]、YYYY/MM/DD。"""
    if v is None or v == '':
        return None
    s = str(v).strip()
    # 純數字＝epoch（10 位=秒、13 位=毫秒）
    if s.isdigit():
        n = int(s)
        if len(s) >= 13:
            n = n // 1000
        try:
            return datetime.fromtimestamp(n)
        except Exception:
            return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S', '%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


class DiscoveryClient:
    AUTH_URL = 'https://s2s.popin.cc/data/v1/authentication'
    CAMPAIGN_LIST_URL = 'https://s2s.popin.cc/discovery/api/v2/campaign/lists'
    AD_LIST_BASE_URL = 'https://s2s.popin.cc/discovery/api/v2/ad/{}/lists'
    REPORT_BASE_URL = 'https://s2s.popin.cc/discovery/api/v2/ad/{}/{}/{}/{}/date_reporting'
    # §3.6 Multiple ad reports：bulk 端點，一次回多支 ad 的日報表（含 imp/click/charge/cv）
    BULK_REPORT_URL = 'https://s2s.popin.cc/discovery/api/v2/ad/{}/{}/date_reporting'

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

        resp = requests.post(self.AUTH_URL, headers=headers, timeout=30)

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

    def _request_with_retry(self, method, url, headers=None, params=None, timeout=30, max_retries=4):
        """統一的限流退避重試（429 / operateTooMuch）。
        其餘非 200 直接回傳交呼叫端判斷；逾時/連線錯誤亦退避重試。"""
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
                if _is_rate_limited(resp.status_code, resp.text) and attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return resp
            except requests.RequestException as e:
                if attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise e
        return resp

    def fetch_daily_stats(self, account_ids: list, start_date: str, end_date: str,
                          log_tag: str = '[BH-D-Daily-Sync]', executor: ThreadPoolExecutor = None) -> dict:
        """
        取得 Discovery 平台日報表。
        Output: { (account_id, date): {spend, impressions, clicks, conversions} }

        優先走 §3.6 bulk 端點（請求數 -97%、且 imp/click/charge/cv 一次到位）；
        bulk 端點層級失敗（HTTP/limit/code 非 0）才退回原本「逐 ad date_reporting」流程兜底。
        """
        try:
            return self._fetch_via_bulk(account_ids, start_date, end_date, log_tag)
        except Exception as e:
            logger.warning(f"{log_tag} bulk 流程失敗，退回逐 ad 兜底：{e}")
            print(f"{log_tag} bulk 流程失敗，退回逐 ad 兜底：{e}", flush=True)
            return self._fetch_via_per_ad(account_ids, start_date, end_date, log_tag, executor)

    # ---------------------------------------------------------------
    # 主流程：bulk 端點（§3.6）
    # ---------------------------------------------------------------
    def _fetch_via_bulk(self, account_ids: list, start_date: str, end_date: str, log_tag: str) -> dict:
        headers = self._get_headers()

        # 取 campaign 清單：建 campaign_id -> account_id 對照（多帳號共用同一 token 時可正確歸屬）
        resp = self._request_with_retry('GET', self.CAMPAIGN_LIST_URL, headers=headers,
                                        params={'country_id': 'tw'})
        if resp.status_code != 200:
            raise Exception(f"campaign list 失敗 {resp.status_code}: {resp.text[:120]}")
        campaigns = resp.json().get('data', []) or []
        if not campaigns:
            return {}

        cam_to_acc = {str(c.get('mongo_id')): str(c.get('account_id'))
                      for c in campaigns if c.get('mongo_id')}
        all_cam_ids = [str(c.get('mongo_id')) for c in campaigns if c.get('mongo_id')]
        unique_ids = list({str(a) for a in account_ids}) if account_ids else []

        # 歸屬：單帳號時全部歸該帳號（與原流程 fallback 行為一致、最穩）；
        # 多帳號共 token 時用 campaign_id -> account_id 對照
        def attribute(campaign_id):
            if len(unique_ids) == 1:
                return unique_ids[0]
            acc = cam_to_acc.get(str(campaign_id))
            return acc if acc and acc != 'None' else None

        s = start_date.replace('-', '')
        e = end_date.replace('-', '')
        url = self.BULK_REPORT_URL.format(s, e)

        stats = {}
        # CampaignIds header 上限 10 個 -> 分組；PageSize 上限 100 -> 依 total 翻頁
        groups = [all_cam_ids[i:i + 10] for i in range(0, len(all_cam_ids), 10)]
        for group in groups:
            page = 1
            while True:
                h = dict(headers)
                h['CampaignIds'] = ','.join(group)
                h['PageSize'] = '100'
                h['CurrentPage'] = str(page)
                r = self._request_with_retry('GET', url, headers=h)
                if r.status_code != 200:
                    raise Exception(f"bulk {r.status_code}: {r.text[:120]}")
                j = r.json()
                if str(j.get('code')) != '0':
                    raise Exception(f"bulk code={j.get('code')} msg={j.get('msg')}")

                data = j.get('data', {}) or {}
                detail = data.get('detail', []) or []
                total = int(data.get('total', 0) or 0)

                for row in detail:
                    d = row.get('date')
                    if not d:
                        continue
                    acc = attribute(row.get('campaign_id'))
                    if not acc or acc == 'None':
                        continue
                    key = (acc, d)
                    a = stats.setdefault(key, {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                    a['spend'] += float(row.get('charge', 0) or 0)
                    a['impressions'] += int(row.get('imp', 0) or 0)
                    a['clicks'] += int(row.get('click', 0) or 0)
                    a['conversions'] += int(row.get('cv', 0) or 0)

                if page * 100 >= total or not detail:
                    break
                page += 1

        for (acc, d), v in stats.items():
            print(f"{log_tag}[bulk] Account {acc} Date {d}: {v}", flush=True)
        return stats

    # ---------------------------------------------------------------
    # 兜底流程：逐 ad date_reporting（保留原行為，campaign 過濾改用 created/updated）
    # ---------------------------------------------------------------
    def _fetch_via_per_ad(self, account_ids: list, start_date: str, end_date: str,
                          log_tag: str, executor: ThreadPoolExecutor = None) -> dict:
        # 1. Get Campaigns (Sequential)
        headers = self._get_headers()
        params = {'country_id': 'tw'}

        resp = self._request_with_retry('GET', self.CAMPAIGN_LIST_URL, headers=headers, params=params)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch campaigns: {resp.text}")
            print(f"Failed to fetch campaigns: {resp.text}")
            return {}

        campaigns = resp.json().get('data', [])
        if not campaigns:
            return {}

        stats_result = {}
        stats_lock = threading.Lock()

        # 走期邊界（用於 campaign 過濾）
        try:
            req_start = datetime.strptime(start_date, '%Y-%m-%d')
            req_end = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # 含當日
        except Exception:
            req_start = None
            req_end = None

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

            # --- Campaign 過濾（不可用 status：停用的 campaign 走期內可能投放過，實證 status=0 仍有資料）---
            # 三條規則任一成立就跳過（解析不出的日期一律保留，避免誤殺）：
            if req_start is not None:
                # 1) end_date + 30 天仍早於走期開始（已結束很久）
                end_ts = _parse_loose_date(cam.get('end_date'))
                if end_ts is not None and req_start > end_ts + timedelta(days=30):
                    return []
                # 2) created_at 晚於走期結束（建立前不可能投放，100% 安全）
                created_ts = _parse_loose_date(cam.get('created_at'))
                if created_ts is not None and created_ts > req_end:
                    return []
                # 3) updated_at 早於走期開始前 30 天（投放中系統會更新 updated_at）
                updated_ts = _parse_loose_date(cam.get('updated_at'))
                if updated_ts is not None and updated_ts < req_start - timedelta(days=30):
                    return []

            ad_list_url = self.AD_LIST_BASE_URL.format(cam_id)
            try:
                ad_resp = self._request_with_retry('GET', ad_list_url, headers=headers, timeout=20)
                if ad_resp.status_code != 200:
                    return []
                ads = ad_resp.json().get('data', [])
                if not ads:
                    return []

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

            report_data = []
            try:
                rep_resp = self._request_with_retry('GET', report_url, headers=headers, timeout=20)
                if rep_resp.status_code == 200:
                    r_json = rep_resp.json()
                    report_data = r_json.get('data', [])
                    if report_data:
                        # 簡化日誌輸出
                        simplified_data = {}
                        if isinstance(report_data, dict):
                            for k, v in report_data.items():
                                simplified_data[k] = {'charge': v.get('charge', 0), 'imp': v.get('imp', 0), 'click': v.get('click', 0)}
                        elif isinstance(report_data, list):
                            for v in report_data:
                                k = v.get('date', v.get('day', 'unknown'))
                                simplified_data[k] = {'charge': v.get('charge', 0), 'imp': v.get('imp', 0), 'click': v.get('click', 0)}
                        print(f"[D-API-Response] Aid {acc_id} Ad {ad_id} (Code {rep_resp.status_code}): {simplified_data}", flush=True)
                else:
                    logger.error(f"[D-API-Error] Aid {acc_id} Status {rep_resp.status_code} for Ad {ad_id}: {rep_resp.text}")
            except Exception as e:
                logger.exception(f"[D-API-Exception] Aid {acc_id} Ad {ad_id}: {str(e)}")

            if not report_data:
                return

            data_list = report_data.values() if isinstance(report_data, dict) else report_data

            with stats_lock:
                for day_stat in data_list:
                    date_str = day_stat.get('date', day_stat.get('day'))
                    if not date_str:
                        continue

                    key = (acc_id, date_str)
                    if key not in stats_result:
                        stats_result[key] = {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0}

                    stats_result[key]['spend'] += float(day_stat.get('charge', day_stat.get('cost', 0)))
                    stats_result[key]['impressions'] += int(day_stat.get('imp', 0))
                    stats_result[key]['clicks'] += int(day_stat.get('click', 0))
                    stats_result[key]['conversions'] += int(day_stat.get('cv', 0))

        # 2. Parallel Fetch Ads
        all_ads = []
        if executor:
            futures = [executor.submit(_fetch_ads, cam) for cam in campaigns]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_ads.extend(result)
        else:
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

        # Debug: Print Final Totals
        acc_keys = {}
        for (acc_id, date_str) in stats_result.keys():
            if acc_id not in acc_keys:
                acc_keys[acc_id] = []
            acc_keys[acc_id].append(date_str)

        for acc_id, dates in acc_keys.items():
            for d in dates:
                key = (acc_id, d)
                print(f"{log_tag} Account {acc_id} Date {d}: {stats_result[key]}", flush=True)

        return stats_result
