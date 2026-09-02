import requests
from datetime import datetime

# Prism（P 平台 / PAC platform）報表 API。
#
# 跟 D/R/M 最大的不同：沒有 OAuth、沒有換 token、沒有區間上限、沒有分頁、
# 未觀察到限流 —— 一發 POST 打完收工，所以本檔沒有切段與退避邏輯，那是 D/M 的包袱。
#
# 一把 token 看得到所有廣告主 ⇒ dimensions 帶 date+advertiser 就能一發拿回全部帳戶，
# 每日同步只要 1 個 request（見 bh_sync 的 P 分支）。

# 合法欄位白名單（取自 popIn_Audience_Center app.py 的 dim_map / metric_map）。
# ⚠️ 後端對不合法欄位的處理很危險：不合法 metric 永遠靜默吞掉（HTTP 200 但少一整欄）；
#    不合法 dimension 若排在合法的前面會 500 "Column N contains an aggregation function"。
#    所以送出前一定要自己擋。
PRISM_DIMENSIONS = {
    'date', 'campaign_id', 'adgroup_id', 'creative_id', 'advertiser',
    'domain', 'slot', 'device', 'country', 'city',
    'title', 'ad_description', 'cta_label',
}
PRISM_METRICS = {
    'impressions', 'clicks', 'ctr', 'spend', 'viewable_impressions',
    'viewability', 'view_25', 'view_50', 'view_75', 'view_100', 'vtr',
}


def parse_prism_date(value):
    """Prism 的 JSON date 是 `Fri, 28 Aug 2026 00:00:00 GMT`（Flask jsonify 序列化
    datetime.date 的產物）。⚠️ 字尾寫 GMT 但值其實是**台北日**（後端 SQL 是
    `DATE(received_at, 'Asia/Taipei')`），**不可做時區換算**。回 'YYYY-MM-DD'。"""
    s = str(value).strip()
    try:
        return datetime.strptime(s, '%a, %d %b %Y %H:%M:%S %Z').date().isoformat()
    except ValueError:
        # 防禦：若哪天後端改回乾淨 ISO（csv/xls 格式就是），直接吃前 10 碼
        return s[:10]


class PrismClient:
    URL = 'https://ads.pacplatform.net/api/external/reports/generate'

    def __init__(self, token):
        # token 走環境變數注入，沒有預設值：這把 token 是全域的、不綁廣告主，
        # 等於全體廣告主的報表讀取權，絕不可寫死在程式裡。
        if not token:
            raise ValueError('缺少 Prism token（環境變數 PRISM_API_TOKEN 未設定）')
        self.token = token

    def fetch_daily_stats(self, start_date, end_date, advertiser_ids=None, timeout=120):
        """抓 Prism 日報表，回統一 map {(advertiser_id, 'YYYY-MM-DD'): stats}。

        advertiser_ids 省略或為 None ⇒ 回所有廣告主（BH 每日同步就走這條，1 發搞定）。
        ⚠️ advertiser_ids=[] 不可送出（後端會組出空的 IN () 而 500），故空值一律不帶。

        失敗一律 raise，不回半套資料——上層拿到空 map 會寫 0，而補洞檢查只看
        「那天有沒有列」，寫下去就永遠不會被修正（見 plan 的 fail-closed 條款）。
        """
        dimensions = ['date', 'advertiser']
        metrics = ['impressions', 'clicks', 'spend']

        bad = [d for d in dimensions if d not in PRISM_DIMENSIONS] + \
              [m for m in metrics if m not in PRISM_METRICS]
        if bad:
            raise ValueError(f'Prism 不合法欄位（會被靜默吞掉或 500）：{bad}')

        body = {
            'token': self.token,
            'start_date': start_date,   # YYYY-MM-DD（不是 D 平台的 YYYYMMDD）
            'end_date': end_date,       # inclusive，台北時區
            'dimensions': dimensions,
            'metrics': metrics,
            'format': 'json',           # 預設是 csv，一定要明寫
        }
        if advertiser_ids:              # 空 list / None 都不帶
            body['advertiser_ids'] = [str(a) for a in advertiser_ids]

        resp = requests.post(self.URL, json=body, timeout=timeout)
        if resp.status_code != 200:
            raise Exception(f'Prism API {resp.status_code}: {resp.text[:200]}')
        rows = resp.json().get('data', []) or []

        # ⚠️ headers 是「你請求的順序」，含後端根本沒給的欄位；不能拿它當真實欄位。
        #    要用實際 row 的 key 反查，缺欄當場拋錯，否則會拿到看似正常、實際少一欄的報表。
        if rows:
            missing = [c for c in dimensions + metrics if c not in rows[0]]
            if missing:
                raise Exception(f'Prism 回應缺欄位 {missing}（欄位名打錯會被靜默忽略）')

        stats = {}
        for row in rows:
            adv = row.get('advertiser')
            # advertiser 可能是 null（實測 advertiser_name="Unknown" 那批）。
            # BH 以 account_id 當鍵，這種列必須丟掉，不可變成 account_id=None 的鬼帳戶。
            if adv is None or str(adv).strip() == '':
                continue
            key = (str(adv), parse_prism_date(row.get('date')))
            a = stats.setdefault(key, {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
            a['spend'] += float(row.get('spend', 0) or 0)
            a['impressions'] += int(row.get('impressions', 0) or 0)
            a['clicks'] += int(row.get('clicks', 0) or 0)
            # Prism 沒有實作轉換追蹤（prism_events 全表零筆 conversion 事件），
            # 不是 API 限制。conversions 恆為 0，UI 顯示 — 的處理見 Task 5。
        return stats
