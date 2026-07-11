import time
import requests
import logging

logger = logging.getLogger(__name__)

# MGID 可選轉換 metric（比照 R 的 cv_definition，帳戶層可設定要加總哪幾個）
# 已用真 token 打 statistics-reports 實測驗證：這三個欄位名真實存在於回應中
# （多帳號樣本：860507/860509/860511 等實際回傳非零值，非僅預設 0）。
MGID_CV_METRICS = ['conversionsInterest', 'conversionsDecision', 'conversionsBuy']


def _flatten_amount(v):
    """MGID 金額欄位是物件 {'amount':'20','currency':'TWD'}；攤平取 amount。
    也容忍 API 直接回數字/字串的情形。"""
    if isinstance(v, dict):
        v = v.get('amount', 0)
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

    def _resolve_cv_metrics(self, cv_definition):
        """把帳戶的 cv_definition 字串解析成要加總的 MGID 轉換 metric 清單。
        比照 R：逗號分隔、大小寫不敏感；未設定則不算轉換（回空）。"""
        if not cv_definition:
            return []
        wanted = {s.strip().lower() for s in cv_definition.split(',') if s.strip()}
        return [m for m in MGID_CV_METRICS if m.lower() in wanted]

    def fetch_daily_stats(self, api_client_id, start_date, end_date, cv_definition=None):
        """抓 MGID 日報表，回統一 map（key 的 account_id＝api_client_id）。
        單次 API 區間上限 90 天，呼叫端須自行切段（見 bh_sync）。"""
        cv_metrics = self._resolve_cv_metrics(cv_definition)

        url = f"{self.BASE_URL}/goodhits/clients/{api_client_id}/statistics-reports"
        # ISO8601；日界含整日
        params = [
            ('filters[dateRange][dateFrom]', f'{start_date}T00:00:00.000Z'),
            ('filters[dateRange][dateTo]', f'{end_date}T23:59:59.999Z'),
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
