# Budget-Hunter MGID 平台整合 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 budget-hunter（BH，本 Flask/Python repo）現有的 R/D 兩平台之外，新增第三個廣告平台 **MGID（M）**，讓帳戶清單、每日同步、全區間同步、補洞檢查、Excel 上傳與前端 UI 都支援 M，統一輸出到 `bh_daily_stats` 的 `{spend, impressions, clicks, conversions}`。

**Architecture:** 完全對稱現有 R/D 的 pattern：新增 `services/bh_clients/m_client.py`（`MgidClient`）；`database.py` 新增 `MgidToken` model（對 `nexus.mgid_tokens`，唯一鍵 `api_client_id`）＋ `get_mgid_token` / `get_mgid_token_map` helper；`bh_sync.py` 三個進入點各加 `elif platform == 'M'` 分支；`bh_service.py` 上傳與 `generate_bh_template.py` 模板放行 M；`bh.html` 前端加 M 呈現。轉換數比照 R「可設定」（沿用 `bh_accounts.cv_definition` 欄位存 MGID 事件名）；token 比照 D「可上傳」（Excel 帶 token 就 upsert 進 `nexus.mgid_tokens`）。

**Tech Stack:** Python 3 / Flask 2.3 / Flask-SQLAlchemy 3.1 / PyMySQL / requests / pandas / openpyxl。前端 `bh.html`（Vue 3 CDN，`[[ ]]` 分隔符）。DB＝Cloud SQL `internal-tool`，共用庫 schema `nexus`。

## Global Constraints

- **MGID 白牌呼叫三要素（缺一即 401/403）**：Host＝`https://api.native.broadciel.com/v1`（**不可**用 `api.mgid.com`）；認證＝`Authorization: Bearer {token}`（32 字元，**不可**用 `?token=`）；URL 路徑用 **`Client API ID`（86xxxx）**，不是 advertiser `Client ID`（98xxxx，用它會 403）。
- **MGID 併發 6 以上會 429** → 抓報表必須節流（並發 ≤5）＋退避重試。
- **MGID 金額欄位是物件** `{"amount":"20","currency":"TWD"}`（`spent`/`cpc`/`cpm`），寫 DB 前必須攤平取 `amount`。`ctr` 是小數（0.01＝1%）。
- **MGID 日期**：ISO8601（`2026-07-01T00:00:00.000Z`），單次區間上限 **90 天**；`dimensions[]` 最多 3 個；`limit` ≤ 1000。
- **Token 查詢鍵一律 `api_client_id`**（對應 `nexus.d_tokens` 的 `account_id`）；`bh_accounts.account_id` 對 M 帳戶存的就是 `api_client_id`（86xxxx）。
- **命名慣例**：DB 欄位 snake_case；前端 API 變數 camelCase。**溝通與程式碼註解用繁體中文。**
- **本 repo 無 pytest 套件**；驗證慣例＝`poc/*.py` 探測腳本（打真實唯讀 API、從 DB 取 token、不上 git）＋手動跑同步路由對照 `bh_daily_stats`。POC token 一律走環境變數，不落 git。
- **surgical changes**：只動 M 需要的地方，不重構 R/D 既有邏輯（R 優化見附錄，獨立、可不做）。

---

## File Structure

| 檔案 | 責任 | 動作 |
|---|---|---|
| `services/bh_clients/m_client.py` | MGID Report API client：認證、限流退避、金額攤平、`fetch_daily_stats` 回統一 map | **Create** |
| `database.py` | 新增 `MgidToken` model ＋ `get_mgid_token` / `get_mgid_token_map`；`BHAccount.platform` Enum 加 `'M'` | Modify |
| `services/bh_sync.py` | 三個進入點各加 `elif platform == 'M'` 分支 | Modify |
| `services/bh_service.py` | 上傳解析放行 `'M'`；M token upsert 進 `nexus.mgid_tokens` | Modify |
| `generate_bh_template.py` | 平台下拉 `"R,D,M"`；cv 下拉加 MGID 事件 | Modify |
| `templates/bh.html` | M badge、平台顯示、M 的轉換設定 UI | Modify |
| `poc/probe_mgid_bh.py` | 探測腳本：真 token 打 statistics-reports、印攤平後統一 map（驗證用） | **Create** |
| `database/migrations/` 或手動 SQL | `ALTER TABLE bh_accounts` enum 加 'M'（`nexus.mgid_tokens` 已存在，不建） | **Create**（SQL 檔） |

---

## Task 1: DB migration — platform enum 加 'M'

> **⚠ `nexus.mgid_tokens` 表已存在（TS 工具已建並灌帳號），本 task 不建表、不碰它。** 只擴充 `bh_accounts.platform` enum。

**Files:**
- Create: `database/migrations/2026-07-11-add-mgid.sql`

**Interfaces:**
- Produces: `bh_accounts.platform` 可存 `'M'`。

- [ ] **Step 1: 寫 migration SQL**

```sql
-- 2026-07-11-add-mgid.sql
-- BH 新增 MGID(M) 平台：只擴充 platform enum。
-- 註：nexus.mgid_tokens 已由 TS 工具建立並灌帳號，此處不建表。

-- bh_accounts.platform 加入 'M'（db.create_all() 不會 ALTER 既有 enum，必須手動）
ALTER TABLE bh_accounts
  MODIFY COLUMN platform ENUM('R','D','M') NOT NULL COMMENT '廣告平台: R/D/M';
```

- [ ] **Step 2: 對測試/staging DB 執行並確認**

Run（用 repo 現有連線方式，例如 Cloud SQL proxy 或既有 PyMySQL 連線）：
```bash
# 範例：以 repo 既有連線參數執行 SQL 檔（實際連線指令依 .env / server-ca.pem 設定）
venv/bin/python - <<'PY'
import os, pymysql
# 依 .env 讀 DB 連線；此處僅示意，實作時沿用 app.py 的連線設定
print("執行 database/migrations/2026-07-11-add-mgid.sql，確認 enum 與表存在")
PY
```
Expected: `bh_accounts` `SHOW COLUMNS LIKE 'platform'` 顯示 `enum('R','D','M')`；`nexus.mgid_tokens` 存在。

- [ ] **Step 3: 確認既有 12 帳號 token 已在表內（TS 工具已灌）**

Run:
```sql
SELECT api_client_id, client_name FROM nexus.mgid_tokens ORDER BY id;
```
Expected: 若 TS 工具已灌則回 12 列（Serene House/東吳/貸霸/...）；若為空表示本環境需另行灌 token（記錄下來，Task 6 上傳可補）。

- [ ] **Step 4: Commit**

```bash
git add database/migrations/2026-07-11-add-mgid.sql
git commit -m "feat(bh): DB migration 新增 MGID(M) 平台 enum 與 nexus.mgid_tokens 表"
```

---

## Task 2: `MgidToken` model ＋ token helper

**Files:**
- Modify: `database.py`（在 `RAccountToken` 之後、`User` 之前新增；helper 放對應位置）

**Interfaces:**
- Produces:
  - `class MgidToken` — 對 `nexus.mgid_tokens`，欄位 `api_client_id, client_id, client_name, token, source, created_time, updated_time`。
  - `get_mgid_token(api_client_id) -> MgidToken | None`（回整列，含 `.token`）。
  - `get_mgid_token_map(api_client_ids) -> dict[str, str]`（`{api_client_id: token}`）。

- [ ] **Step 1: 新增 model 與 helper**

在 `database.py` 加入（比照 `BHDAccountToken` / `get_d_token`）：
```python
class MgidToken(db.Model):
    # 共用庫 nexus.mgid_tokens（跨工具單一真相，MGID 一帳一 token，唯一鍵 api_client_id）。
    # 與 D 不同：無 dctool 鏡像、無 30s 同步，全 source='adtools'。查詢鍵一律 api_client_id。
    __tablename__ = 'mgid_tokens'
    __table_args__ = {'schema': 'nexus'}

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    api_client_id = db.Column(db.String(64), nullable=False)   # URL 路徑用 id（86xxxx）
    client_id = db.Column(db.String(64), nullable=True)        # advertiser 顯示 id（98xxxx）
    client_name = db.Column(db.String(255), nullable=False)
    token = db.Column(db.Text, nullable=False)
    source = db.Column(db.Enum('adtools'), nullable=False, default='adtools')
    created_time = db.Column(db.DateTime, default=datetime.utcnow)
    updated_time = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def get_mgid_token(api_client_id):
    """取單一 MGID 帳號 token row（唯一鍵 api_client_id）；無則回 None。"""
    return MgidToken.query.filter_by(api_client_id=str(api_client_id)).first()


def get_mgid_token_map(api_client_ids):
    """批次取 {api_client_id: token}。"""
    ids = [str(a) for a in api_client_ids]
    if not ids:
        return {}
    rows = MgidToken.query.filter(MgidToken.api_client_id.in_(ids)).all()
    return {r.api_client_id: r.token for r in rows}
```

- [ ] **Step 2: `BHAccount.platform` enum 加 'M'**

`database.py` line 11：
```python
    platform = db.Column(db.Enum('R', 'D', 'M'), nullable=False, comment='廣告平台: R/D/M')
```

- [ ] **Step 3: 驗證 import 與查詢可用**

Run:
```bash
venv/bin/python - <<'PY'
from app import app  # 觸發 db 初始化
from database import get_mgid_token, get_mgid_token_map, MgidToken
with app.app_context():
    rows = MgidToken.query.limit(3).all()
    print("mgid_tokens 前 3 列:", [(r.api_client_id, r.client_name) for r in rows])
    print("map:", get_mgid_token_map([r.api_client_id for r in rows]).keys())
PY
```
Expected: 印出真實帳號（若表已灌）；無 Traceback。

- [ ] **Step 4: Commit**

```bash
git add database.py
git commit -m "feat(bh): 新增 MgidToken model 與 get_mgid_token(_map) helper；platform enum 加 M"
```

---

## Task 3: `MgidClient`（`services/bh_clients/m_client.py`）

**Files:**
- Create: `services/bh_clients/m_client.py`
- Create: `poc/probe_mgid_bh.py`

**Interfaces:**
- Consumes: 無（自帶 token 由呼叫端傳入）。
- Produces:
  - `class MgidClient` — `__init__(self, token: str)`。
  - `fetch_daily_stats(self, api_client_id: str, start_date: str, end_date: str, cv_definition: str = None) -> dict`
    回 `{ (api_client_id, 'YYYY-MM-DD'): {'spend': float, 'impressions': int, 'clicks': int, 'conversions': int} }`（與 `DiscoveryClient.fetch_daily_stats` 輸出形狀一致，key 的 account_id＝`api_client_id`）。
  - `MGID_CV_METRICS` — MGID 可選轉換 metric 常數：`['conversionsInterest', 'conversionsDecision', 'conversionsBuy']`。

- [ ] **Step 1: 寫探測腳本（先驗證 API 契約，等同 failing test）**

Create `poc/probe_mgid_bh.py`：
```python
"""
PoC：驗證 BH MGID(M) client 契約——用真 token 打 statistics-reports，
攤平金額欄，印出統一 map { (api_client_id, date): {spend,imp,click,cv} }。
token 走環境變數 MGID_TEST_TOKEN、帳號走 MGID_TEST_API_CLIENT_ID，不落 git。
執行：MGID_TEST_TOKEN=xxx MGID_TEST_API_CLIENT_ID=860502 venv/bin/python poc/probe_mgid_bh.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.bh_clients.m_client import MgidClient

token = os.environ['MGID_TEST_TOKEN']
acc = os.environ['MGID_TEST_API_CLIENT_ID']
c = MgidClient(token)
m = c.fetch_daily_stats(acc, '2026-07-01', '2026-07-07', cv_definition='conversionsBuy')
for k, v in sorted(m.items()):
    print(k, v)
print("rows:", len(m))
```

- [ ] **Step 2: 跑腳本確認會失敗（client 尚未實作）**

Run:
```bash
MGID_TEST_TOKEN=dummy MGID_TEST_API_CLIENT_ID=860502 venv/bin/python poc/probe_mgid_bh.py
```
Expected: FAIL — `ModuleNotFoundError: No module named 'services.bh_clients.m_client'`。

- [ ] **Step 3: 實作 `MgidClient`**

Create `services/bh_clients/m_client.py`：
```python
import os
import time
import requests
import logging

logger = logging.getLogger(__name__)

# MGID 可選轉換 metric（比照 R 的 cv_definition，帳戶層可設定要加總哪幾個）
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
            # day 維度回傳鍵名以實測為準（probe 先 dump）；先容忍 'day' 或 'date'
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
```

> **⚠ 未查證項（實作時必先 probe 確認）**：`statistics-reports` 在 `dimensions[]=day` 下回傳的日期鍵名（`day` vs `date`）、以及轉換 metric 的實際欄位名（`conversionsBuy` 等來源 advertiser.md 標示「未逐一實測」）。Step 4 的 probe 就是要 dump 真實回應鍵名，若與上面假設不同，在此 client 修正對應鍵。

- [ ] **Step 4: 用真 token 跑 probe，確認回真資料且鍵名正確**

Run（token 從 `nexus.mgid_tokens` 撈一個真帳號，設進環境變數）：
```bash
MGID_TEST_TOKEN='<真 token>' MGID_TEST_API_CLIENT_ID='<真 api_client_id>' \
  venv/bin/python poc/probe_mgid_bh.py
```
Expected: 印出數列 `(api_client_id, 'YYYY-MM-DD') {'spend':..,'impressions':..,'clicks':..,'conversions':..}`，`spend` 為數字（非 dict）。若印不出或 KeyError → 依實際回應鍵名修 client（見上 ⚠）。

- [ ] **Step 5: Commit**

```bash
git add services/bh_clients/m_client.py poc/probe_mgid_bh.py
git commit -m "feat(bh): 新增 MgidClient（statistics-reports/金額攤平/429 退避/可設定轉換）"
```

---

## Task 4: `bh_sync.py` — 每日同步 M 分支（`sync_daily_stats`）

**Files:**
- Modify: `services/bh_sync.py`（`sync_daily_stats`：帳戶分組處理處，D 分支之後）

**Interfaces:**
- Consumes: `MgidClient.fetch_daily_stats`、`get_mgid_token_map`、`self._upsert_stats`。
- Produces: M 帳戶每日 stats 寫入 `bh_daily_stats`。

- [ ] **Step 1: import 與帳戶分組**

`bh_sync.py` 頂部 import 加入：
```python
from database import db, BHAccount, BHDailyStats, get_d_token, get_d_token_map, get_mgid_token_map
from services.bh_clients.m_client import MgidClient
```
`sync_daily_stats` 內，`d_accounts` 定義處旁新增：
```python
            m_accounts = [a for a in accounts if a.platform == 'M']
```

- [ ] **Step 2: 在 D 分支處理完之後，新增 M 分支**

在 `sync_daily_stats` 的 `if d_accounts:` 區塊結束後插入（MGID 一帳一 token，不需 token 分組；並發 ≤5）：
```python
            # --- Process M Platform (MGID, 併發 <=5 + 429 退避) ---
            if m_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(m_accounts)} M-Platform accounts (MGID)...'})}\n\n"
                m_ids = [a.account_id for a in m_accounts]
                m_token_map = get_mgid_token_map(m_ids)  # api_client_id -> token

                m_executor = ThreadPoolExecutor(max_workers=5)  # MGID 併發 6+ 會 429
                try:
                    def _fetch_m(acc):
                        token = m_token_map.get(acc.account_id)
                        if not token:
                            return (acc, None, 'No MGID token')
                        try:
                            client = MgidClient(token)
                            smap = client.fetch_daily_stats(
                                acc.account_id, target_date, target_date, acc.cv_definition)
                            return (acc, smap, None)
                        except Exception as e:
                            return (acc, None, str(e))

                    futures = {m_executor.submit(_fetch_m, acc): acc for acc in m_accounts}
                    for future in as_completed(futures):
                        acc, smap, err = future.result()
                        if err:
                            yield f"data: {json.dumps({'msg': f'  [M] {acc.account_id} 略過: {err}'})}\n\n"
                            continue
                        key = (acc.account_id, target_date)
                        stats = smap.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                        self._upsert_stats(acc.account_id, target_date, stats)
                        log_msg = f"    [M] {acc.account_id}: Spend={int(stats.get('spend',0))}, Clicks={stats.get('clicks',0)}"
                        yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                finally:
                    m_executor.shutdown(wait=False)
                yield f"data: {json.dumps({'msg': f'  M Platform processed ({len(m_accounts)} accounts).'})}\n\n"
```

- [ ] **Step 3: 手動驗證（單一 M 帳戶每日同步）**

先在 `bh_accounts` 建一筆 M 測試帳戶（`platform='M'`、`account_id=<真 api_client_id>`、走期含昨日、`cv_definition='conversionsBuy'`）。透過 app 觸發每日同步（route `/api/bh/sync`，帶 `account_id` 過濾），或直接呼叫 generator：
```bash
venv/bin/python - <<'PY'
from app import app
from services.bh_sync import BHSyncService
with app.app_context():
    for line in BHSyncService().sync_daily_stats(target_date='2026-07-10', account_id='<真 api_client_id>'):
        print(line, end='')
PY
```
Expected: 日誌出現 `[M] <id>: Spend=..., Clicks=...`；查 `bh_daily_stats` 該帳號昨日有列。

- [ ] **Step 4: Commit**

```bash
git add services/bh_sync.py
git commit -m "feat(bh): sync_daily_stats 新增 MGID(M) 每日同步分支（並發<=5+退避）"
```

---

## Task 5: `bh_sync.py` — 全區間同步與補洞檢查 M 分支

**Files:**
- Modify: `services/bh_sync.py`（`sync_account_full_range_by_pk` 的平台分支；`sync_consistency_check` 的 `_process_account` 平台分支）

**Interfaces:**
- Consumes: `MgidClient.fetch_daily_stats`、`get_mgid_token`、`self._upsert_stats`。
- Produces: M 帳戶全區間 / 缺漏日 stats 寫入。

- [ ] **Step 1: `sync_account_full_range_by_pk` 加 M 分支**

在該方法 `elif account.platform == 'D':` 區塊之後新增（MGID 單次上限 90 天，用 90 天批次；比照 D 用 `fetch_daily_stats` 逐日填）：
```python
                elif account.platform == 'M':
                    from database import get_mgid_token
                    token_row = get_mgid_token(account_id)
                    if not token_row:
                        yield f"data: {json.dumps({'msg': f'No MGID Token found for this account.', 'type': 'error'})}\n\n"
                        return
                    m_client = MgidClient(token_row.token)

                    batch_size = 90  # MGID 單次區間上限 90 天
                    for i in range(0, total_days, batch_size):
                        batch = dates_to_sync[i:i+batch_size]
                        s_str = batch[0].strftime('%Y-%m-%d')
                        e_str = batch[-1].strftime('%Y-%m-%d')
                        yield f"data: {json.dumps({'msg': f'Fetching {s_str} ~ {e_str}...'})}\n\n"
                        try:
                            m_map = m_client.fetch_daily_stats(str(account_id), s_str, e_str, account.cv_definition)
                            for target_date in batch:
                                target_str = target_date.strftime('%Y-%m-%d')
                                key = (str(account_id), target_str)
                                stats = m_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                self._upsert_stats(account_id, target_str, stats, app=app)
                                log_msg = f"  [{target_str}] Spend: {int(stats.get('spend', 0))} | Imp: {stats.get('impressions', 0)} | Click: {stats.get('clicks', 0)} | Conv: {stats.get('conversions', 0)}"
                                print(f"[BH-FullSync-M] ID:{account_id} {log_msg}", flush=True)
                                yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                            yield f"data: {json.dumps({'msg': f'  -> Saved.'})}\n\n"
                        except Exception as e:
                            yield f"data: {json.dumps({'msg': f'  Error: {e}', 'type': 'error'})}\n\n"
```

- [ ] **Step 2: `sync_consistency_check._process_account` 加 M 分支**

該函式 `if platform == 'R':` / `elif platform == 'D':` 之後新增（缺漏日以 90 天為上限切段補抓）：
```python
                        elif platform == 'M':
                            from database import get_mgid_token
                            token_row = get_mgid_token(acc_id)
                            if not token_row:
                                logs.append(f"[M] {acc_id} 無 MGID token，略過")
                                return logs
                            m_client = MgidClient(token_row.token)
                            # 缺漏日切連續段（每段 <=90 天）
                            seg = [missing_dates[0]]
                            segments = []
                            for d in missing_dates[1:]:
                                if (d - seg[-1]).days == 1 and len(seg) < 90:
                                    seg.append(d)
                                else:
                                    segments.append(seg); seg = [d]
                            segments.append(seg)
                            for segment in segments:
                                s_str = segment[0].strftime('%Y-%m-%d')
                                e_str = segment[-1].strftime('%Y-%m-%d')
                                try:
                                    m_map = m_client.fetch_daily_stats(str(acc_id), s_str, e_str, cv_def)
                                    for td in segment:
                                        tstr = td.strftime('%Y-%m-%d')
                                        stats = m_map.get((str(acc_id), tstr), {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                        self._upsert_stats(acc_id, tstr, stats)
                                except Exception as e:
                                    logs.append(f"[M] {acc_id} {s_str}~{e_str} error: {e}")
```

> 註：`_process_account` 的簽名已含 `cv_def` 參數（R 用），M 直接沿用同一參數傳 `cv_definition`，呼叫端不需改。

- [ ] **Step 3: 手動驗證全區間同步**

用 Task 4 建的 M 測試帳戶，透過 route `/api/bh/account_pk/<pk_id>/sync_full` 或直接呼叫 generator 跑一段歷史區間，確認 `bh_daily_stats` 逐日填入且金額為攤平後數字。
Expected: 日誌 `[BH-FullSync-M]` 各日有值；DB 對照無缺日。

- [ ] **Step 4: Commit**

```bash
git add services/bh_sync.py
git commit -m "feat(bh): 全區間同步與補洞檢查新增 MGID(M) 分支（90 天切段）"
```

---

## Task 6: `bh_service.py` 上傳 ＋ 模板放行 M

**Files:**
- Modify: `services/bh_service.py`（platform 驗證、M token upsert）
- Modify: `generate_bh_template.py`（平台下拉、cv 下拉）

**Interfaces:**
- Consumes: `MgidToken`。
- Produces: Excel 可上傳 M 帳戶；帶 token 時 upsert 進 `nexus.mgid_tokens`。

- [ ] **Step 1: platform 驗證放行 M**

`bh_service.py` line 55：
```python
                if platform not in ['R', 'D', 'M']:
```

- [ ] **Step 2: M token upsert（比照 D token 區塊，緊接其後）**

`bh_service.py` 的 `# --- D Platform Token Logic ---` 區塊之後新增：
```python
                # --- M Platform Token Logic（MGID，一帳一 token，鍵 api_client_id＝AccID）---
                if platform == 'M':
                    from database import MgidToken
                    m_token_val = None
                    for col in ('M Token', 'Token', 'token'):
                        if col in row and pd.notna(row[col]):
                            m_token_val = str(row[col]).strip()
                            break
                    if m_token_val:
                        existing = MgidToken.query.filter_by(api_client_id=acc_id).first()
                        if existing:
                            if existing.token != m_token_val:
                                existing.token = m_token_val
                                existing.updated_time = datetime.utcnow()
                        else:
                            db.session.add(MgidToken(
                                api_client_id=acc_id,
                                client_name=(account.account_name or acc_id),
                                token=m_token_val,
                                source='adtools',
                            ))
```

- [ ] **Step 3: 模板放行 M ＋ cv 下拉加 MGID 事件**

`generate_bh_template.py`：
```python
# 平台下拉加 M
dv_platform = DataValidation(type="list", formula1='"R,D,M"', allow_blank=False)
dv_platform.error = '必須填寫 R、D 或 M'
```
cv_options 併入 MGID 事件（R 與 M 共用同一 cv 欄，union 清單；上傳時各平台只解析自己認得的值）：
```python
cv_options = [
    'CompleteCheckout', 'AddToCart', 'ViewContent', 'Checkout',
    'Bookmark', 'Search', 'CompleteRegistration',
    'conversionsInterest', 'conversionsDecision', 'conversionsBuy',  # MGID(M) 專用
]
```

- [ ] **Step 4: 重新產生模板並驗證**

Run:
```bash
venv/bin/python generate_bh_template.py
```
Expected: 印出 `Generated static/bh_import_template.xlsx ...`；開啟確認平台下拉含 M、cv 下拉含 conversionsBuy 等。

- [ ] **Step 5: 手動驗證上傳一筆 M 帳戶（帶 token）**

準備一列 `平台=M, AccID=<api_client_id>, ..., Token=<真 token>`，走上傳 route。
Expected: `bh_accounts` 新增該 M 帳戶；`nexus.mgid_tokens` 出現/更新該 `api_client_id`。

- [ ] **Step 6: Commit**

```bash
git add services/bh_service.py generate_bh_template.py static/bh_import_template.xlsx
git commit -m "feat(bh): 上傳與 Excel 模板放行 MGID(M)，token upsert 進 nexus.mgid_tokens"
```

---

## Task 7: 前端 `bh.html` M 呈現（badge only）

> **範圍收斂（已查證）**：既有前端**沒有任何 cv_definition 編輯器**（R 也沒有），且更新路由 `bh_update_account` 不處理 `cv_definition`——`cv_definition` 是「Excel 上傳專屬」欄位。M 比照 R：cv 走 Excel 設定（Task 6 已完成），詳情面板**不加** cv 編輯器（加了也存不進去、且 R 沒有、不一致）。本 task 只加 **M badge 顏色**，不動更新路由。

**Files:**
- Modify: `templates/bh.html`（platform badge 顏色加 M）

**Interfaces:**
- Consumes: 後端回傳的 `acc.platform === 'M'`。
- Produces: M 帳戶在清單以可辨識 badge 顯示（平台文字 `[[ acc.platform ]]` 既有邏輯已自動顯示 M）。

- [ ] **Step 1: badge 顏色加 M**

`bh.html` line 167 附近 badge `:style` 三元運算擴充為含 M（給 M 一個可辨識色，如綠色 `#198754`）：
```html
:style="acc.platform === 'R' ? 'background:#0d6efd;border:1px solid #0d6efd;' : (acc.platform === 'M' ? 'background:#198754;border:1px solid #198754;' : 'background:transparent;border:1px solid white;')"
```
（其餘平台文字顯示、詳情面板共同欄位皆走既有邏輯，不需改動。R 的 agent select、D 的 token 欄位維持原樣。）

- [ ] **Step 2: 驗證**

- 靜態確認：`grep -n "198754" templates/bh.html` 有該色；badge `:style` 三元式語法正確（含 R / M / 其他三分支）。
- 若能啟動 app：開 `/bh`，M 帳戶顯示綠色 M badge、平台欄顯示 `M`。（無法起 app 時，以靜態檢查為準並說明。）
- 確認未新增任何非功能性 UI（不加存不進去的 cv 編輯器）。
Expected: M badge 綠色正確顯示；R/D 呈現不變。

- [ ] **Step 3: Commit**

```bash
git add templates/bh.html
git commit -m "feat(bh): 前端 bh.html 新增 MGID(M) badge 顏色"
```

---

## Self-Review 結果（計畫對照）

- **DB／enum**：Task 1（SQL migration）＋Task 2（model/helper/enum）覆蓋。✔
- **Client（host/認證/金額攤平/限流/可設定轉換）**：Task 3 覆蓋，未查證的日期鍵名/metric 名以 probe 收斂（已標 ⚠）。✔
- **三個同步進入點**：Task 4（每日）＋Task 5（全區間＋補洞）覆蓋。✔
- **上傳／模板（token 比照 D 可上傳）**：Task 6 覆蓋。✔
- **前端（轉換比照 R 可設定）**：Task 7 覆蓋。✔
- **待實作時確認的兩個未查證點**（皆已在 Task 3 標記）：① `statistics-reports` 日期維度回傳鍵名；② 轉換 metric 實際欄位名。這兩點只能靠真 token probe 收斂，計畫已把 probe 放在實作最前面。

---

## 附錄 A：R client 優化（獨立、可不做，與 MGID 整合解耦）

對照 rixbee-api skill 檢視 `services/bh_clients/r_client.py`，發現以下可優化點。**皆非 MGID 整合的必要條件**，列此供決定是否另開工單；風險/效益已標註，未查證處已標明。

1. **認證在 query string＋用 GET**（L122-134）。skill 建議 header＋POST JSON body。**最大實質風險＝token 出現在 URL 會被存取日誌記錄**（安全性）；次要是多帳號 `user_id[]` 撐爆 URL（BH 每日一次一帳、URL 短，此點風險低）。改動屬行為變更，需對真帳號回歸數字一致才可上。
2. **未帶分頁上界 `start`/`end`**。skill 指預設 `end=500` 會靜默截斷。BH 只要 `day`+`user_id` 兩維、列數≈天數，實務不會破 500，**風險低**；建議補 `end=10000` 保險（低成本）。
3. **R client 零重試**（D 有 `_request_with_retry`）。碰 `1003 每日上限`/暫時性錯誤直接丟例外。可加與 D 對稱的退避。
4. **`dimensions[]=day` 於 L93、L114 重複 append**（無害，順手清）。

## 附錄 B：D client 觀察（健康，建議不動）

`d_client.py` 已具 §3.6 bulk＋逐 ad 兜底＋429 退避，狀態良好。僅兩個低優先觀察：`_fetch_via_bulk` 未在內部切 7 天（>7 天會 `80008`，目前靠呼叫端都傳 ≤7 才安全）；bulk 路徑未套用逐 ad 那三條 campaign 日期剪枝（單帳號 token 無影響）。**建議維持現狀，不在本次改動。**
