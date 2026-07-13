import time
import requests

# MGID 轉換固定用 conversionsBuy（買家轉換）：不再由 Excel「R的cv定義」下拉選，直接鎖定為預設。
# 已用真 token 打 statistics-reports 實測驗證此欄位名真實存在且回傳非零值。
MGID_CV_METRICS = ['conversionsBuy']


def _flatten_amount(v):
    """MGID 金額欄位是物件 {'amount':'20','currency':'TWD'}；攤平取 amount。
    也容忍 API 直接回數字/字串（含千分位逗號）的情形。"""
    if isinstance(v, dict):
        v = v.get('amount', 0)
    if isinstance(v, str):
        v = v.replace(',', '')  # 防禦：MGID 若回帶千分位的金額字串，避免 float() 噴錯靜默丟 0
    try:
        return float(v or 0)
    except (ValueError, TypeError):
        return 0.0


class MgidClient:
    # 白牌 host（打 api.mgid.com 會回 token 無效）；Bearer 認證；URL 路徑用 api_client_id。
    BASE_URL = 'https://api.native.broadciel.com/v1'

    def __init__(self, token: str):
        self.token = token

    def _headers(self):
        return {'Authorization': f'Bearer {self.token}', 'Accept': 'application/json'}

    def _request_with_retry(self, url, params, timeout=60, max_retries=4):
        """MGID 併發 6+ 會 429；一律退避重試（比照 D client 的限流處理）。"""
        resp = None
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=timeout)
                if resp.status_code == 429 and attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return resp
            except requests.RequestException as e:
                if attempt < max_retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise e
        return resp

    def fetch_daily_stats(self, api_client_id, start_date, end_date):
        """抓 MGID 日報表，回統一 map（key 的 account_id＝api_client_id）。
        單次 API 區間上限 90 天，呼叫端須自行切段（見 bh_sync）。
        MGID 轉換固定用 conversionsBuy，不吃 Excel 的 cv 定義。"""
        cv_metrics = MGID_CV_METRICS

        url = f"{self.BASE_URL}/goodhits/clients/{api_client_id}/statistics-reports"
        # 日界用台北 +08:00：MGID 的 day 維度以帳戶當地時區（Asia/Taipei）分桶。
        # 實測用 UTC 'Z' 日界會多回邊界那天的整日列（day=end+1，雖不會被寫入但多餘）；
        # 改用 +08:00 對齊整日、回應乾淨，也與 R client 既有的 timezone=UTC+8 一致。
        params = [
            ('filters[dateRange][dateFrom]', f'{start_date}T00:00:00.000+08:00'),
            ('filters[dateRange][dateTo]', f'{end_date}T23:59:59.999+08:00'),
            ('dimensions[]', 'day'),
            ('metrics[]', 'impressions'),
            ('metrics[]', 'clicks'),
            ('metrics[]', 'spent'),
            ('limit', '1000'),
            ('offset', '0'),
        ]
        for m in cv_metrics:
            params.append(('metrics[]', m))

        resp = self._request_with_retry(url, params)
        if resp.status_code != 200:
            raise Exception(f"MGID API {resp.status_code}: {resp.text[:200]}")
        rows = resp.json().get('data', []) or []

        stats = {}
        for row in rows:
            # 已用真 token（api_client_id=860502/860507 等）打 statistics-reports 實測驗證：
            # dimensions[]=day 回傳鍵名就是 'day'，值已是 'YYYY-MM-DD'；'date' fallback 保留防禦。
            d = row.get('day') or row.get('date')
            if not d:
                continue
            d = str(d)[:10]  # 取 YYYY-MM-DD
            key = (str(api_client_id), d)
            a = stats.setdefault(key, {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
            a['spend'] += _flatten_amount(row.get('spent'))
            a['impressions'] += int(row.get('impressions', 0) or 0)
            a['clicks'] += int(row.get('clicks', 0) or 0)
            for m in cv_metrics:
                a['conversions'] += int(row.get(m, 0) or 0)
        return stats
