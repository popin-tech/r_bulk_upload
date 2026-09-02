import difflib
import os
import threading
import time

# D1 平台的 campaign 設定殼（Firestore article-action，走 MongoDB 相容協定）。
#
# 為什麼是這裡：D1 的影音廣告完全不在 D 平台報表 API（s2s.popin.cc）裡——實測 242 個帳號、
# 8302 個 campaign 的 campaign/lists，type 100% 是 native，一支 video 都沒有。
# 成效只能走 Action4，而 Action4 只認 campaign id、不給名稱，帳戶／活動名稱／影音旗標靠這裡補。
#
# 連線：<uuid>.asia-northeast2.firestore.goog:443，公網 IP、Google 憑證，認證走 URI 內嵌的
# SCRAM-SHA-256 帳密（不是 GCP IAM）⇒ 跨專案無妨，Cloud Run 一般對外網路即可，免 VPC connector。
#
# ⚠️ 這張表只是「設定殼」：預算/開關/走期在 Redis（內網）與 D1 MySQL（百度內網），這裡都沒有。
#    BH 的走期本來就是 AE 自己在 Excel 填的，不受影響。
#
# ⚠️ campaign doc 全部 33 個欄位裡沒有任何 numeric account id，唯一的帳戶識別是 account
#    字串（實測 170 個不重複值）。它與 D 平台的數字 account_id 是兩套命名空間，無法互轉。
#
# ⚠️ list_video_campaigns 回的是**含已刪除**的全集，list_accounts 也從全集推導。
#    BH 做預算追蹤，campaign 被刪掉不代表它花過的錢要從帳戶預算裡消失。
#    取數端（v_client）務必用 include_deleted=True，否則會出現「驗證通過但同步全 0」。

COUNTRY_ID = os.getenv('D1_COUNTRY_ID', 'tw')

# 目錄一天內不會變太多；快取 5 分鐘，避免每次上傳／同步都重連 Firestore。
_CACHE_TTL_SECONDS = 300
_cache = {'at': 0.0, 'data': None}
_lock = threading.Lock()
_client = None


def d1_firestore_available():
    return bool(os.getenv('D1_FIRESTORE_URI'))


def _campaign_collection():
    """共用單一 MongoClient（pymongo 自帶連線池，thread-safe）。"""
    global _client
    uri = os.getenv('D1_FIRESTORE_URI')
    if not uri:
        raise ValueError('未設定 D1_FIRESTORE_URI（Firestore article-action 連線字串）')
    if _client is None:
        import certifi
        from pymongo import MongoClient
        # ⚠️ 明確指定 CA bundle。Firestore 的 Mongo 相容端點走 TLS，而不同環境的
        #    預設信任存放區不一致：macOS 的 Python.framework 沒有裝 CA，會噴
        #    `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate`
        #    （2026-09-02 本機實際踩到）。用 certifi 讓本機與 Cloud Run 行為一致。
        #    certifi 是 requests 的傳遞依賴，requirements 已明列以免被移除。
        _client = MongoClient(uri, serverSelectionTimeoutMS=20000,
                              tlsCAFile=certifi.where())
    return _client.get_database().get_collection('campaign')


def list_video_campaigns(force_refresh=False):
    """取台灣所有影音 campaign，**含已刪除**（由呼叫端決定要不要濾）。

    ⚠️ video / vertical_video / deleted 在這張表裡是**真正的 boolean**，不是字串 'True'。
       用 'True' 查會靜默回 0 筆。
    """
    with _lock:
        fresh = (time.time() - _cache['at']) < _CACHE_TTL_SECONDS
        if _cache['data'] is not None and fresh and not force_refresh:
            return _cache['data']

        docs = _campaign_collection().find(
            {'country_id': COUNTRY_ID, 'video': True},
            {'name': 1, 'account': 1, 'agency': 1, 'vertical_video': 1, 'deleted': 1},
        )
        data = [{
            'id': str(d.get('_id')),
            'name': str(d.get('name') or ''),
            'account': str(d.get('account') or ''),
            'agency': str(d.get('agency') or ''),
            'vertical_video': d.get('vertical_video') is True,
            'deleted': d.get('deleted') is True,
        } for d in docs]
        _cache['at'] = time.time()
        _cache['data'] = data
        return data


def list_accounts():
    """不重複的影音帳戶字串，遞增排序。**從含已刪的全集推導**——只有已刪 campaign 的帳戶
    （如 arch）仍然是合法帳戶，它過去花的錢要算進 BH 的預算。"""
    return sorted({c['account'] for c in list_video_campaigns() if c['account']})


def campaigns_for_account(account, include_deleted=False):
    """某帳戶的影音 campaign 清單。帳戶字串大小寫必須完全一致。

    ⚠️ BH 取數一律傳 include_deleted=True（見 v_client）。預設值 False 是給
       「只想看目前在跑什麼」的情境用的，不要拿它去算歷史花費。
    """
    return [c for c in list_video_campaigns()
            if c['account'] == account and (include_deleted or not c['deleted'])]


def validate_account(account):
    """帳戶字串存在性檢查，回 (是否存在, 最多 3 個模糊建議)。

    為什麼要做：Excel 打錯一個字，Action4 只會查無資料回空，BH 會靜默記成花費 0
    —— 比報錯還危險。所以在上傳當下就擋下來並給建議。

    判準與 list_accounts 一致（含已刪），確保「驗證通過的帳戶，同步時一定拿得到 campaign」。
    """
    accounts = list_accounts()
    if account in accounts:
        return True, []

    # ⚠️ 大小寫不同是最常見的打錯法，但 difflib 是**大小寫敏感**的
    #    （'evox_cpm' 對 'EVOX_CPM' 相似度接近 0，會一個建議都給不出來）。
    #    所以一律降冪比對，再映射回目錄裡的正確大小寫。
    #    注意仍然回 False——Action4 與目錄查詢都用精確字串，大小寫錯就是查不到。
    lower_map = {}
    for a in accounts:
        lower_map.setdefault(a.lower(), a)

    key = str(account).strip().lower()
    if key in lower_map:
        return False, [lower_map[key]]
    hits = difflib.get_close_matches(key, list(lower_map.keys()), n=3, cutoff=0.6)
    return False, [lower_map[h] for h in hits]
