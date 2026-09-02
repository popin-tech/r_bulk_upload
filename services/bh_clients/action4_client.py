import json
import re
import threading
import time
from datetime import datetime

import requests

# Action4 —— D1 後台報表數字的真實來源（HBase/OBKV 聚合）。
#
# D1 的影音成效（曝光、25/50/75/100% 播放、金額）在 D 平台報表 API（s2s.popin.cc）
# 一格都沒有，只有這裡有：實測 242 帳號、8302 個 campaign 的 campaign/lists，type
# 100% 是 native，一支 video 都沒有；D1 原始碼 Api/Campaign.php:607 is_video → 80000。
#
# 端點 action4.popin.cc → df-gw.bdjp.io，**公網可直接打、無認證**（Cloud Run 直連即可，
# 不需要跳板或 VPC connector）。
#
# 三個實測到的坑：
#  ① 查詢區間上限 12 個月（D1 apiUtils.php:115 寫死）。超過會靜默回 {"result":1} 不帶
#     任何日期、不報錯 → 會被誤讀成「這段沒有投放」。故送出前先擋。
#  ② 一定要帶 categories=ca_all，否則回應塞滿 ca_/cc_/ab_ 分類子表（實測同一支 campaign
#     12 個月：11,379 → 1,425 bytes，省 87%），我們一格都用不到。
#  ③ 回應只含「有量的日子」，沒投放的日期整個不出現（不是回 0）。

BASE = 'https://action4.popin.cc/popin-action/'

# Action4 可查詢的最長回溯（D1 後台自己的限制，超過會靜默回空）。
ACTION4_MAX_MONTHS = 12

# ⚠️ 全域併發上限。只有併發 6 經過實測（ad_tools 一輪 18 支約 1.5 秒）。
#    刻意放在模組層而不是交給呼叫端的 executor：v_client 每個帳戶開 6 條、bh_sync 的
#    每日同步又同時跑多個帳戶，只靠 max_workers 實際併發會變成 6 × 帳戶數。
#    這個 semaphore 是整個 process 對 Action4 的真正上限。
ACTION4_MAX_CONCURRENCY = 6
_CONCURRENCY = threading.BoundedSemaphore(ACTION4_MAX_CONCURRENCY)

_YMD = re.compile(r'^(\d{4})(\d{2})(\d{2})$')


def _parse_ymd(ymd):
    m = _YMD.match(str(ymd))
    if not m:
        raise ValueError(f'日期格式須為 YYYYMMDD：{ymd}')
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), 12)


def exceeds_action4_window(start, stop):
    """區間是否超出 Action4 的 12 個月上限。
    判準照抄 D1 apiUtils.php:115：`start < strtotime(stop . '-12 month')`。純函式。"""
    limit = _parse_ymd(stop)
    try:
        limit = limit.replace(year=limit.year - 1)
    except ValueError:
        # 2/29 往前推一年不存在 → 夾到 2/28（保守，寧可少抓一天也不要踩到靜默回空）
        limit = limit.replace(year=limit.year - 1, day=28)
    return _parse_ymd(start) < limit


def is_date_key(k):
    """Action4 回應中哪些鍵是日期（其餘如 result 要濾掉）。純函式。"""
    return bool(_YMD.match(str(k)))


def fetch_campaign_stats(campaign_id, start, stop, timeout=60, retries=2):
    """抓單一 campaign 在指定區間的每日原始統計。
    回 {'YYYYMMDD': {原始欄位...}}，只含有量的日子。口徑換算交給 d1_video_metrics。
    失敗一律 raise（fail-closed），不回空 dict——回空會被上層當成「沒投放」寫 0。"""
    if exceeds_action4_window(start, stop):
        raise ValueError(
            f'Action4 查詢區間上限 {ACTION4_MAX_MONTHS} 個月（{start}~{stop} 超出，會靜默回空）'
        )

    url = (f'{BASE}?op=article&nid={campaign_id}&country='
           f'&start={start}&stop={stop}&categories=ca_all')

    last_err = None
    for attempt in range(retries + 1):
        try:
            with _CONCURRENCY:
                resp = requests.get(url, timeout=timeout)
            if resp.status_code != 200:
                raise Exception(f'Action4 HTTP {resp.status_code}')
            payload = json.loads(resp.text)
            # result 非 1 = Action4 那端有問題，不能當「查無資料」靜默吞掉
            if str(payload.get('result')) != '1':
                raise Exception(f'Action4 回應 result={payload.get("result")}')
            return {k: v for k, v in payload.items()
                    if is_date_key(k) and isinstance(v, dict)}
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
    raise Exception(f'Action4 抓取失敗（campaign {campaign_id}）：{last_err}')
