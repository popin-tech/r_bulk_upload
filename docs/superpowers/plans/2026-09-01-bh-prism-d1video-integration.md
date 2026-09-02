# Budget-Hunter 新增 P（Prism）與 V（D1 影音）平台 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 budget-hunter（BH，本 Flask/Python repo）現有的 R/D/M 三平台之外，新增 **P（Prism / PAC platform）** 與 **V（D1 影音，Action4）** 兩個平台，讓帳戶清單、每日同步、全區間同步、補洞檢查、Excel 上傳與前端 UI 都支援，統一輸出到 `bh_daily_stats` 的 `{spend, impressions, clicks, conversions}`。

**Architecture:** 完全對稱現有 R/D/M 的 pattern。P 走單一 POST 端點、一把全域 token、**一天一發就拿到所有廣告主**（不需要 token 表、不需要切段）。V 走兩個資料源：Firestore `article-action.campaign`（帳戶／campaign 清單，唯一有影音旗標的地方）＋ Action4（唯一有影音成效的地方），口徑照抄 ad_tools 已上線的 TS 實作。兩平台皆**無轉換數**，UI 要顯示 `—` 而非 0。

**Tech Stack:** Python 3.12 / Flask 2.3 / Flask-SQLAlchemy 3.1 / PyMySQL / requests / **pymongo（新增）** / pandas / openpyxl。測試用 stdlib `unittest`（**本 repo 沒有 pytest**）。前端 `templates/bh.html` ＋ `static/bh.js`（Vue 3 CDN，`[[ ]]` 分隔符）。DB＝Cloud SQL `internal-tool`（`budget_hunter` ＋共用庫 `nexus`）。

**Spec:** 本檔的 Global Constraints 段即為 spec（所有數值皆為 2026-09-01 實打驗證）。上游權威來源：
- P：`/Users/benson/Documents/project/internal/ad_tools/.claude/skills/prism-api/SKILL.md`
- V 參考實作（TS，照抄口徑）：`/Users/benson/Documents/project/internal/ad_tools/src/core/action4.ts`、`src/core/firestore_d1.ts`、`src/tools/d1videoad/metrics.ts`、`src/tools/d1videoad/report.ts`

**修訂紀錄：** 2026-09-02 依 code review 修訂 —— 失敗語意改 fail-closed（原設計會靜默低估）、V 改含已刪除 campaign、里程碑重新切分（原 Task 4 會放行不會同步的 V 帳戶）、Action4 併發改全域管制、無轉換 UI 補上每日明細、補齊 V 層單元測試、命名規範改為對齊現況的 snake_case。

---

## 執行狀態（2026-09-02）

| Task | 狀態 | 備註 |
|---|---|---|
| 1 DB migration | ✅ | 正式 DB enum 已是 `('R','D','M','P','V')`，既有 217 筆帳戶完好 |
| 2 PrismClient | ✅ | 9 測試 ＋ 真 API 對上回歸基準 |
| 3 bh_sync P 分支 | ✅ | 真同步驗過，fail-closed 用錯 token 實測成立 |
| 4 P 上傳/範本/badge | ✅ | 執行中踩到範本回歸，見下方修訂紀錄 |
| 5 CPA/CV 顯示 — | ✅ | 執行中踩到 v-if/v-else 斷鏈，已修 |
| **6 P 上線** | ⏸ | secret 與 cloudbuild 已就緒，**等 Task 12 一起 push** |
| 7 action4_client | ✅ | 11 測試（含全域併發上限實測） |
| 8 d1_video_metrics | ✅ | 7 測試 |
| 9 d1_video_catalog | ✅ | 9 測試；真 Firestore：395 支 / 未刪 308 / 170 帳戶 |
| 10 v_client | ✅ | 7 測試；真 API：18 支加總 CPM 72.00 整 |
| 11 V 同步/上傳/範本 | ✅ | 端到端實測通過 |
| **12 V 上線** | ⏸ | **卡在 Step 3 的 D1 後台對帳（需人工登入後台）** |

測試數：1 → **55**，全綠。

---

## Global Constraints

### 通用

- **命名慣例**：DB 欄位與 **API 回應欄位一律 snake_case**。
  > ⚠️ 前一版本檔沿用了 MGID plan 的「前端 API 變數 camelCase」，**與本 repo 現況不符**。實查 `templates/bh.html` 消費的欄位全是 snake_case——`acc.account_id`、`acc.current_cpa`、`acc.cpa_goal`、`acc.yesterday_spend`、`acc.remaining_days`、`acc.daily_budget`、`selectedAccount.d_token`、`stat.conversions`……**20 個欄位、camelCase 零個**。新增欄位一律跟著 snake_case，不要在同一個物件上製造單一例外。
- **程式碼註解與溝通用繁體中文。**
- **測試**：本 repo **沒有 pytest**，一律用 stdlib `unittest`，放 `tests/`，跑法 `venv/bin/python -m unittest tests.<module> -v`。既有範例見 `tests/test_bh_token_import.py`。**現有基線：`unittest discover` 共 1 個測試，通過。**
- **不打真 API 的單元測試**：所有 client 測試一律 `unittest.mock.patch` 掉 `requests.*` 與 pymongo；要打真 API 的驗證放 `poc/*.py`（token 走環境變數，不落 git）。
- **surgical changes**：只動 P/V 需要的地方，不重構 R/D/M 既有邏輯。
- **兩平台都沒有轉換數**，`conversions` 一律填 0，但 UI 必須把 CPA **與 CV** 顯示成 `—`（帳戶清單與每日明細兩個地方都要）。
- **Secret 一律走環境變數，不可寫死預設值。** （反例：`services/bh_clients/r_client.py:17-26` 把 token 寫死當 `os.getenv` 預設值，本次不沿用。）

### ⚠️ 失敗語意：一律 fail-closed，寧可整批失敗也不要寫入低估的數字

這是本 plan 最重要的一條，**兩個平台都適用**。

原因在 `services/bh_sync.py:494` 的補洞判準：

```python
missing_dates = sorted([d for d in needed_dates if d not in existing_dates_set])
```

它**只看「那天在 `bh_daily_stats` 有沒有列」，不看列的值是不是 0**。所以：

1. 抓取時部分失敗 → 缺的日期被當成「沒投放」寫入 0
2. 那天從此「有列了」
3. **補洞檢查永遠不會再處理它** → 永久低估，而且 BH 沒有任何地方會顯示這個 warning

⇒ **任何抓取只要有一部分失敗，就必須整批 raise、一列都不寫。** 上游 TS 實作有 `warnings[]` 回傳到畫面橫幅（`ad_tools/src/tools/d1videoad/report.ts:92`），BH 沒有對應 UI，所以不能沿用它的「部分成功」語意。

### P（Prism）

- 端點：`POST https://ads.pacplatform.net/api/external/reports/generate`，`Content-Type: application/json`。
- 認證＝**把靜態 token 放在 body 的 `token` 欄位**（不是 header、不是 Bearer、不會過期）。token 走 `PRISM_API_TOKEN` 環境變數，**沒有預設值**。
- **必填**：`token` / `start_date` / `end_date` / `dimensions` / `metrics`，任一缺少或給空陣列 → 400 `{"error":"Missing required fields"}`。
- 日期格式 **`YYYY-MM-DD`**（D 平台是 `YYYYMMDD`，別搞混）。時區 Asia/Taipei，`date` 就是台北本地日，`end_date` 含當日。
- **`format` 預設是 `csv`，必須明寫 `"json"`**。
- **無區間上限**（實測 963 天一發 2.6 秒）、**無分頁**、未觀察到限流 ⇒ **不要寫切段或退避邏輯**。
- **一把 token 看得到所有廣告主** ⇒ `dimensions:["date","advertiser"]` 一發拿回所有帳戶，每日同步只要 **1 個 request**。
- ⚠️ **`advertiser_ids: []` 會 500**（組出空的 `IN ()`）。空就**整個欄位不要帶**。
- ⚠️ **`advertiser` 可能是 `null`**（實測 2026-08-28 有一列 `advertiser=null, advertiser_name="Unknown"`，129,857 曝光）。**必須丟棄**，不可變成 `account_id=None` 的鬼帳戶。
- ⚠️ **不合法的 metric 永遠靜默吞掉（HTTP 200）**；不合法的 dimension 若排在合法的前面會 **500** `Column N contains an aggregation function`。⇒ 送出前用白名單自檢，收到後再用**實際 row 的 key** 反查（`headers` 不可信，它照列你請求的欄位）。
- 合法 dimensions（13）：`date` `campaign_id` `adgroup_id` `creative_id` `advertiser` `domain` `slot` `device` `country` `city` `title` `ad_description` `cta_label`。
- 合法 metrics（11）：`impressions` `clicks` `ctr` `spend` `viewable_impressions` `viewability` `view_25` `view_50` `view_75` `view_100` `vtr`。
- **不存在 `conversions`**（平台根本沒實作轉換追蹤，`prism_events` 全表零筆 conversion 事件）。
- ⚠️ JSON 的 `date` 長相是 `"Fri, 28 Aug 2026 00:00:00 GMT"` —— **字尾寫 GMT 但值是台北日，不要做時區換算**。解析：`datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %Z").date()`（已實測可行）。
- ⚠️ **`impressions=0` 卻有 `clicks>0` 是真實存在的**（各 metric 是獨立 `COUNTIF`，無參照完整性）⇒ 算 CPC/CTR 時分母必須 guard。**現況已安全，不需改動**：`bh_service.py` 的 `current_cpc` 有 `if s['clicks'] > 0`、`current_ctr` 有 `if s['impressions'] > 0`。本 plan 不動這段，執行者不要以為漏做了。
- `ctr` / `viewability` / `vtr` 在分母 0 時是 **`null` 不是 0**（本 plan 不請求這些 metric，僅備註）。
- **實測基準（2026-08-28，可當回歸對照）**：`292-462-3142 Coupang_Ads` imp 1,596,066 / clk 162 / spend 162.0；`464-144-2909 安達人壽` imp 42,118 / clk 2,281 / spend 6,317.70。

### V（D1 影音）

- **影音完全不在 D 平台報表 API 裡**：實測 242 帳號、8302 個 campaign 的 `campaign/lists`，`type` 100% native，一支 video 都沒有；D1 原始碼 `Api/Campaign.php:607` `is_video → 80000`。⇒ 不可沿用 `d_client.py`。
- **成效來源＝Action4**：`GET https://action4.popin.cc/popin-action/?op=article&nid={campaign_id}&country=&start={YYYYMMDD}&stop={YYYYMMDD}&categories=ca_all`
  - **公網可直接打、無認證**（Cloud Run 直連即可，不需跳板／VPC）。
  - ⚠️ **查詢區間上限 12 個月**（D1 `apiUtils.php:115` 寫死）。超過會**靜默回 `{"result":1}` 不帶任何日期**、不報錯 ⇒ 送出前必須自己擋。
  - ⚠️ **一定要帶 `categories=ca_all`**，否則回應塞滿 `ca_`/`cc_`/`ab_` 分類子表（實測 11,379 → 1,425 bytes，省 87%）。
  - ⚠️ 回應**只含「有量的日子」**，沒投放的日期整個不出現（不是回 0）。
  - `result` 非 `1` 代表 Action4 那端有問題，**不可當「查無資料」吞掉**，要 raise。
- **⚠️ Action4 全域併發上限 6**：只有併發 6 經過實測（ad_tools 一輪 18 支約 1.5 秒）。**管制必須放在模組層的 semaphore**，不能只靠單一 executor 的 `max_workers` —— V 的每帳戶 executor（6）× 每日同步的帳戶併發（3）＝實際 18，是沒驗證過的量。
- **口徑照抄 D1 後台 `apiUtils.php arrangeStats()`（不是自己推的）**：
  - **只算 mobile，PC 一律不計。** Action4 有回 `pc_video_*`（約佔 1.7% 曝光），但加進來就跟 AM 在 D1 後台看到的數字對不起來。
  - `impressions` = `mobile_video_imp` + `mobile_video_vertical_imp`
  - `clicks` = `mobile_video_link`
  - `spend` = (`charge.mobile_video_imp` + `charge.mobile_video_vertical_imp`) **÷ 1000**（charge 存的是「CPM × 曝光」）
  - **`_over`（超投）欄位不計入。**
  - `conversions` = 0
  - 驗證：campaign `6a8dbcf60b53275b884686e5` 2026-08-28，charge 407,664 ÷ 1000 = 407.664 元 ÷ 5,662 曝光 × 1000 = **CPM 72.00 元整**；campaign `6a913d5f8dbcc677dd696cb6` 同日 311,112 ÷ 4,321 = **72.00** —— 兩支整數 CPM 反推證明除數正確。
- **⚠️ BH 一律納入已刪除（`deleted:true`）的 campaign。**
  - 理由：BH 做的是**預算追蹤**——campaign 走期中被刪掉，它花掉的錢仍然計入這個帳戶的預算。ad_tools 的「預設不含已刪除」是**報表瀏覽的 UX 選擇**，不是預算會計的口徑，不可照抄。
  - 一致性要求：`list_accounts()` 是從**全部** campaign（含已刪）推導出來的，所以取數也**必須**含已刪，否則會出現「帳戶通過驗證 → 同步拿到空清單 → 寫入全 0」（例：帳戶 `arch` 只有一支已刪 campaign）。
- **帳戶／campaign 清單來源＝Firestore `article-action`**（MongoDB 相容協定）：
  - 連線字串 `D1_FIRESTORE_URI`（`<uuid>.asia-northeast2.firestore.goog:443`，公網 IP、Google 憑證，認證走 URI 內嵌 SCRAM-SHA-256，**不是 GCP IAM**）⇒ Cloud Run 一般對外網路即可，免 VPC connector。
  - 查詢：`db.campaign.find({country_id: 'tw', video: true}, {projection: {name:1, account:1, agency:1, vertical_video:1, deleted:1}})`
  - ⚠️ **`video` / `vertical_video` / `deleted` 是真 boolean，不是字串 `"True"`**。用字串查會靜默回 0 筆。
  - **實測（2026-09-01）：台灣影音 campaign 395 支、未刪 308 支、170 個不重複 `account` 值。**
  - ⚠️ **campaign doc 全部 33 個欄位裡沒有任何 numeric account id**，唯一的帳戶識別是 `account` 字串（如 `4A_CPM_chubblife`、`EVOX_CPM`、`LANCOME`）。它與 D 平台的數字 `account_id` 是**兩套命名空間，沒有對照表**。
  - 這張表**沒有走期／預算／開關**（在內網 Redis 與百度內網 MySQL）。BH 的走期本來就是 AE 自己在 Excel 填的，不受影響。

### 已拍板的設計決策（2026-09-01 與使用者確認）

1. **平台代碼**：P 與 V 各自獨立。**V 不可塞進 `platform='D'`** —— `bh_service.py:129/318/510` 與 `bh_sync.py:127` 只要看到 `'D'` 就會去 `nexus.d_tokens` 找 token、前端也會秀 D Token 欄位；影音免認證，塞進 D 會卡在 `No Token found` 然後 return。
2. **V 的帳戶識別**：Excel 照填 `account` 字串（自由打字），**BH 在上傳當下連 Firestore 比對**，對不上就擋下該列並回「你是不是要填 XXX」（`difflib.get_close_matches`）。範本不做動態產生。**理由**：打錯一個字 Action4 只會查無資料回空，BH 會靜默記成花費 0，比報錯還危險。
3. **歸戶**：影音與廣編**分開兩列**，不做「客戶」聚合層。零 schema 變更（只 ALTER enum）。

### 里程碑與部署紀律

- **一個平台的「可上線」＝該平台的資料正確性與 UI 正確性都完成，且正式環境 secret 已就緒。** 不可只做完 client 就宣稱上線。
- **一個平台的 Excel 範本選項、上傳放行與 badge，必須與該平台的同步能力同一個里程碑發布。** 提前放行會讓 AE 建出「收得下、但永遠不會同步」的帳戶。

| 里程碑 | Tasks | 完成後可交付 |
|---|---|---|
| **M1：P 上線** | 1–6 | P 平台完整可用（含 CPA/CV 顯示與正式 secret） |
| **M2：V 上線** | 7–12 | V 平台完整可用 |

> **執行決策（2026-09-02，與使用者確認）**：Task 1–5 完成後**不單獨部署 P**，
> 改為先把 M2 的程式（Task 7–11）寫完，最後把 **Task 6 與 Task 12 合併成一次部署**。
> 理由：只擾動線上一次。在那之前正式環境完全不動（Task 1 的 enum migration 除外，
> 它是向後相容的，且已執行）。

---

## File Structure

| 檔案 | 責任 | 動作 | Task |
|---|---|---|---|
| `database/migrations/2026-09-01-add-prism-d1video.sql` | `bh_accounts.platform` enum 加 `'P'`、`'V'` | **Create** | 1 |
| `database.py` | `BHAccount.platform` Enum 加 `'P'`、`'V'` | Modify | 1 |
| `services/bh_clients/p_client.py` | Prism client：白名單自檢、單發報表、null advertiser 過濾、日期解析 | **Create** | 2 |
| `tests/test_p_client.py` | P client 單元測試（mock requests） | **Create** | 2 |
| `services/bh_sync.py` | 三個進入點各加 P 分支；後續加 V 分支與 `segment_dates` 純函式 | Modify | 3, 11 |
| `services/bh_service.py` | 上傳放行 P（後續 V）；V 列做 Firestore 驗證；無轉換平台旗標 | Modify | 4, 5, 11 |
| `generate_bh_template.py` | 平台下拉加 P（後續 V） | Modify | 4, 11 |
| `templates/bh.html` / `static/bh.js` | 平台 badge；無轉換平台的 CPA／CV 顯示 `—` | Modify | 4, 5, 11 |
| `cloudbuild.yaml` | 加 `PRISM_API_TOKEN`（後續 `D1_FIRESTORE_URI`）secret | Modify | 6, 12 |
| `poc/probe_prism_bh.py` | 打真 Prism API 印統一 map | **Create** | 2 |
| `services/bh_clients/action4_client.py` | Action4 低階 client：12 個月守衛、`categories=ca_all`、全域併發 semaphore | **Create** | 7 |
| `tests/test_action4_client.py` | 守衛、`result!=1`、日期鍵過濾、全域併發上限 | **Create** | 7 |
| `services/bh_clients/d1_video_metrics.py` | 影音口徑純函式（mobile-only、÷1000） | **Create** | 8 |
| `tests/test_d1_video_metrics.py` | 口徑測試（真實 Action4 樣本、CPM 反推） | **Create** | 8 |
| `services/bh_clients/d1_video_catalog.py` | Firestore 影音目錄：清單、快取、存在性檢查＋模糊建議 | **Create** | 9 |
| `tests/test_d1_video_catalog.py` | 目錄與模糊比對測試 | **Create** | 9 |
| `services/bh_clients/v_client.py` | 組裝層：目錄 → Action4 併發 → 統一 map（fail-closed、含已刪） | **Create** | 10 |
| `tests/test_v_client.py` | 失敗語意、含已刪、跨 campaign 加總、空清單 | **Create** | 10 |
| `tests/test_bh_sync_segments.py` | `segment_dates` 邊界測試 | **Create** | 11 |
| `tests/test_bh_service_v_upload.py` | V 上傳驗證失敗不 insert | **Create** | 11 |
| `requirements.txt` | 加 `pymongo` | Modify | 9 |
| `poc/probe_d1video_bh.py` | 打真 Firestore + Action4 印統一 map | **Create** | 10 |

---

# 里程碑 M1：P（Prism）上線

## Task 1: DB migration — platform enum 加 'P' 與 'V'

> 一次把兩個代碼都加進 DB enum，省第二次 ALTER。**這不會提前開放 V**：能不能建 V 帳戶由 `bh_service.py` 的上傳白名單決定（Task 4 只放行 P，Task 11 才放行 V）。
> **不需要建任何 `nexus` 新表**：P 是一把全域 token 走環境變數，V 完全免認證。

**Files:**
- Create: `database/migrations/2026-09-01-add-prism-d1video.sql`
- Modify: `database.py:11`

**Interfaces:**
- Produces: `bh_accounts.platform` 可存 `'P'` 與 `'V'`；`BHAccount.platform` 的 SQLAlchemy Enum 同步放行。

- [ ] **Step 1: 寫 migration SQL**

Create `database/migrations/2026-09-01-add-prism-d1video.sql`:

```sql
-- 2026-09-01-add-prism-d1video.sql
-- BH 新增 P（Prism / PAC platform）與 V（D1 影音 / Action4）兩個平台。
-- 只擴充 platform enum：
--   P 走一把全域 token（PRISM_API_TOKEN 環境變數），不需要 token 表。
--   V 走 Action4（公網免認證）＋ Firestore（D1_FIRESTORE_URI），也不需要 token 表。
-- 註：db.create_all() 不會 ALTER 既有 enum，必須手動執行本檔。
-- 註：DB 放行 != 功能開放。實際能不能建 V 帳戶由 bh_service 的上傳白名單控制。

ALTER TABLE bh_accounts
  MODIFY COLUMN platform ENUM('R','D','M','P','V') NOT NULL COMMENT '廣告平台: R/D/M/P/V';
```

- [ ] **Step 2: 同步 SQLAlchemy model**

Modify `database.py:11`，把：

```python
    platform = db.Column(db.Enum('R', 'D', 'M'), nullable=False, comment='廣告平台: R/D/M')
```

改成：

```python
    platform = db.Column(db.Enum('R', 'D', 'M', 'P', 'V'), nullable=False, comment='廣告平台: R/D/M/P/V')
```

- [ ] **Step 3: 對正式 DB 執行並確認**

從本機連正式 Cloud SQL（**必須帶 SSL＋`check_hostname: False`，否則會收到誤導性的 1045 Access denied**）：

```bash
venv/bin/python - <<'PY'
import pymysql, os
conn = pymysql.connect(
    host='35.234.61.181', port=3306,
    user=os.environ['BH_DB_USER'], password=os.environ['BH_DB_PASS'],
    database='budget_hunter', connect_timeout=15,
    ssl={'ca': 'server-ca.pem', 'check_hostname': False},
)
cur = conn.cursor()
cur.execute(
    "ALTER TABLE bh_accounts "
    "MODIFY COLUMN platform ENUM('R','D','M','P','V') NOT NULL COMMENT '廣告平台: R/D/M/P/V'"
)
conn.commit()
cur.execute("SHOW COLUMNS FROM bh_accounts LIKE 'platform'")
print(cur.fetchone())
conn.close()
PY
```

Expected: 印出的 Type 欄位為 `enum('R','D','M','P','V')`。

- [ ] **Step 4: Commit**

```bash
git add database/migrations/2026-09-01-add-prism-d1video.sql database.py
git commit -m "feat(bh): DB migration 新增 P(Prism) 與 V(D1影音) 平台 enum"
```

---

## Task 2: `PrismClient` — P 平台報表 client

**Files:**
- Create: `services/bh_clients/p_client.py`
- Create: `poc/probe_prism_bh.py`
- Test: `tests/test_p_client.py`

**Interfaces:**
- Produces:
  - `PRISM_DIMENSIONS: set[str]` — 13 個合法 dimension
  - `PRISM_METRICS: set[str]` — 11 個合法 metric
  - `parse_prism_date(value: str) -> str` — `"Fri, 28 Aug 2026 00:00:00 GMT"` → `"2026-08-28"`
  - `class PrismClient(token: str)`，方法 `fetch_daily_stats(start_date: str, end_date: str, advertiser_ids: list[str] | None = None, timeout: int = 120) -> dict[tuple[str, str], dict]`，回 `{(advertiser_id, 'YYYY-MM-DD'): {'spend': float, 'impressions': int, 'clicks': int, 'conversions': 0}}`

- [ ] **Step 1: 寫失敗的測試**

Create `tests/test_p_client.py`:

```python
import unittest
from unittest.mock import patch, MagicMock

from services.bh_clients.p_client import PrismClient, parse_prism_date


# 2026-08-28 真實回應（節錄自實打，含 advertiser=null 那一列）
REAL_RESPONSE = {
    "data": [
        {"advertiser": "292-462-3142", "advertiser_name": "Coupang_Ads",
         "clicks": 162, "date": "Fri, 28 Aug 2026 00:00:00 GMT",
         "impressions": 1596066, "spend": 162.0},
        {"advertiser": None, "advertiser_name": "Unknown",
         "clicks": 18, "date": "Fri, 28 Aug 2026 00:00:00 GMT",
         "impressions": 129857, "spend": 0.0},
        {"advertiser": "464-144-2909", "advertiser_name": "安達人壽",
         "clicks": 2281, "date": "Fri, 28 Aug 2026 00:00:00 GMT",
         "impressions": 42118, "spend": 6317.7000000000835},
    ],
    "headers": ["date", "advertiser", "advertiser_name", "impressions", "clicks", "spend"],
}


def _resp(payload, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = payload
    m.text = str(payload)
    return m


class TestParsePrismDate(unittest.TestCase):
    def test_rfc_style_with_gmt_suffix_is_taipei_day(self):
        # 字尾寫 GMT 但值其實是台北日，不可做時區換算
        self.assertEqual(parse_prism_date("Fri, 28 Aug 2026 00:00:00 GMT"), "2026-08-28")

    def test_iso_fallback(self):
        self.assertEqual(parse_prism_date("2026-08-28"), "2026-08-28")


class TestFetchDailyStats(unittest.TestCase):
    @patch("services.bh_clients.p_client.requests.post")
    def test_returns_unified_map_and_drops_null_advertiser(self, mock_post):
        mock_post.return_value = _resp(REAL_RESPONSE)
        out = PrismClient("dummy-token").fetch_daily_stats("2026-08-28", "2026-08-28")

        self.assertEqual(
            out[("292-462-3142", "2026-08-28")],
            {"spend": 162.0, "impressions": 1596066, "clicks": 162, "conversions": 0},
        )
        self.assertEqual(out[("464-144-2909", "2026-08-28")]["clicks"], 2281)
        # advertiser=null 的那列必須被丟掉，不可變成鬼帳戶
        self.assertEqual(len(out), 2)
        self.assertNotIn(None, [k[0] for k in out])

    @patch("services.bh_clients.p_client.requests.post")
    def test_empty_advertiser_ids_is_not_sent(self, mock_post):
        # advertiser_ids: [] 會讓後端組出空的 IN () 而 500，必須整個欄位不帶
        mock_post.return_value = _resp({"data": [], "headers": []})
        PrismClient("t").fetch_daily_stats("2026-08-28", "2026-08-28", advertiser_ids=[])
        self.assertNotIn("advertiser_ids", mock_post.call_args.kwargs["json"])

    @patch("services.bh_clients.p_client.requests.post")
    def test_advertiser_ids_is_sent_when_non_empty(self, mock_post):
        mock_post.return_value = _resp({"data": [], "headers": []})
        PrismClient("t").fetch_daily_stats("2026-08-28", "2026-08-28",
                                           advertiser_ids=["292-462-3142"])
        self.assertEqual(mock_post.call_args.kwargs["json"]["advertiser_ids"],
                         ["292-462-3142"])

    @patch("services.bh_clients.p_client.requests.post")
    def test_format_json_is_explicit(self, mock_post):
        # format 預設是 csv，沒明寫會拿到 CSV 附件
        mock_post.return_value = _resp({"data": [], "headers": []})
        PrismClient("t").fetch_daily_stats("2026-08-28", "2026-08-28")
        self.assertEqual(mock_post.call_args.kwargs["json"]["format"], "json")

    @patch("services.bh_clients.p_client.requests.post")
    def test_missing_metric_column_raises(self, mock_post):
        # 不合法 metric 會被靜默吞掉且 headers 照列 → 必須用實際 row 的 key 反查
        mock_post.return_value = _resp({
            "data": [{"date": "Fri, 28 Aug 2026 00:00:00 GMT",
                      "advertiser": "292-462-3142", "impressions": 10}],
            "headers": ["date", "advertiser", "impressions", "clicks", "spend"],
        })
        with self.assertRaises(Exception) as ctx:
            PrismClient("t").fetch_daily_stats("2026-08-28", "2026-08-28")
        self.assertIn("clicks", str(ctx.exception))

    @patch("services.bh_clients.p_client.requests.post")
    def test_non_200_raises(self, mock_post):
        # fail-closed：抓不到就拋，不可回空 map 讓上層寫 0
        mock_post.return_value = _resp({"error": "Unauthorized"}, status=401)
        with self.assertRaises(Exception):
            PrismClient("t").fetch_daily_stats("2026-08-28", "2026-08-28")

    def test_rejects_token_none(self):
        with self.assertRaises(ValueError):
            PrismClient(None)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_p_client -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'services.bh_clients.p_client'`

- [ ] **Step 3: 寫實作**

Create `services/bh_clients/p_client.py`:

```python
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
```

- [ ] **Step 4: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_p_client -v`
Expected: PASS（8 個測試）

- [ ] **Step 5: 寫 POC 打真 API 驗證**

Create `poc/probe_prism_bh.py`（**token 走環境變數，不落 git**）:

```python
"""打真 Prism API，印出 BH 統一 map。用法：
   PRISM_API_TOKEN=xxx venv/bin/python poc/probe_prism_bh.py 2026-08-28
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.bh_clients.p_client import PrismClient

day = sys.argv[1] if len(sys.argv) > 1 else '2026-08-28'
out = PrismClient(os.environ['PRISM_API_TOKEN']).fetch_daily_stats(day, day)
print(f'{day} 共 {len(out)} 個廣告主')
for (adv, d), s in sorted(out.items(), key=lambda kv: -kv[1]['spend']):
    print(f'  {adv}  imp={s["impressions"]:>10,}  clk={s["clicks"]:>6,}  spend={s["spend"]:>12,.2f}')
```

Run: `PRISM_API_TOKEN=<token> venv/bin/python poc/probe_prism_bh.py 2026-08-28`
Expected: 至少印出 `292-462-3142 imp=1,596,066 clk=162 spend=162.00` 與 `464-144-2909 imp=42,118 clk=2,281 spend=6,317.70`，且**沒有** `None` 這個廣告主。

- [ ] **Step 6: Commit**

```bash
git add services/bh_clients/p_client.py tests/test_p_client.py poc/probe_prism_bh.py
git commit -m "feat(bh): 新增 Prism(P) 平台 client，含 null advertiser 過濾與欄位白名單自檢"
```

---

## Task 3: `bh_sync.py` 三個進入點加 P 分支

**Files:**
- Modify: `services/bh_sync.py`（import 區、`sync_account_full_range_by_pk`、`sync_daily_stats`、`sync_consistency_check` 內的 `_process_account`）

**Interfaces:**
- Consumes: `PrismClient.fetch_daily_stats(start_date, end_date, advertiser_ids)`（Task 2）
- Produces: `platform='P'` 的帳戶在三條同步路徑都會寫進 `bh_daily_stats`

- [ ] **Step 1: 加 import 與 token helper**

Modify `services/bh_sync.py`，在 `from services.bh_clients.m_client import MgidClient` 之後加入：

```python
from services.bh_clients.p_client import PrismClient
import os
```

並在 `class BHSyncService:` 內、`__init__` 之前加入：

```python
    @staticmethod
    def _prism_client():
        """Prism 一把全域 token（環境變數注入，無預設值）。未設定就拋，讓呼叫端記 log 略過。"""
        return PrismClient(os.getenv('PRISM_API_TOKEN'))
```

- [ ] **Step 2: `sync_account_full_range_by_pk` 加 P 分支**

Modify `services/bh_sync.py`，在 `elif account.platform == 'M':` 那整段（約 167–189 行）的**後面**插入：

```python
                elif account.platform == 'P':
                    # Prism 無區間上限（實測 963 天一發 2.6 秒）⇒ 不切段，整個走期一發打完。
                    p_client = self._prism_client()
                    s_str = dates_to_sync[0].strftime('%Y-%m-%d')
                    e_str = dates_to_sync[-1].strftime('%Y-%m-%d')
                    yield f"data: {json.dumps({'msg': f'Fetching {s_str} ~ {e_str} (single request)...'})}\n\n"
                    try:
                        p_map = p_client.fetch_daily_stats(s_str, e_str, [str(account_id)])
                    except Exception as e:
                        # fail-closed：抓取失敗就整批不寫，避免 0 被寫進去後補洞檢查再也不碰
                        yield f"data: {json.dumps({'msg': f'  Error: {e}（本區間未寫入任何資料）', 'type': 'error'})}\n\n"
                        return
                    for target_date in dates_to_sync:
                        target_str = target_date.strftime('%Y-%m-%d')
                        stats = p_map.get((str(account_id), target_str),
                                          {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                        self._upsert_stats(account_id, target_str, stats, app=app)
                        log_msg = f"  [{target_str}] Spend: {int(stats.get('spend', 0))} | Imp: {stats.get('impressions', 0)} | Click: {stats.get('clicks', 0)}"
                        print(f"[BH-FullSync-P] ID:{account_id} {log_msg}", flush=True)
                        yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                    yield f"data: {json.dumps({'msg': f'  -> Saved.'})}\n\n"
```

- [ ] **Step 3: `sync_daily_stats` 加 P 分支（一發拿全部）**

Modify `services/bh_sync.py`：

(a) 在 `m_accounts = [a for a in accounts if a.platform == 'M']`（約 237 行）後面加：

```python
            p_accounts = [a for a in accounts if a.platform == 'P']
```

(b) 在 M 平台那整段結束（`yield ... 'M Platform processed ...'`，約 379 行）**後面**插入：

```python
            # --- Process P Platform (Prism：一把 token 看全部廣告主 → 整批 1 個 request) ---
            if p_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(p_accounts)} P-Platform accounts (Prism, single request)...'})}\n\n"
                try:
                    p_ids = list({str(a.account_id) for a in p_accounts})
                    p_map = self._prism_client().fetch_daily_stats(target_date, target_date, p_ids)
                    for acc in p_accounts:
                        stats = p_map.get((str(acc.account_id), target_date),
                                          {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                        self._upsert_stats(acc.account_id, target_date, stats)
                        log_msg = f"    [P] {acc.account_id}: Spend={int(stats.get('spend',0))}, Clicks={stats.get('clicks',0)}"
                        yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                    yield f"data: {json.dumps({'msg': f'  P Platform processed ({len(p_accounts)} accounts, 1 request).'})}\n\n"
                except Exception as e:
                    # fail-closed：整批不寫（P 是一發全拿，本來就沒有部分成功的中間狀態）
                    yield f"data: {json.dumps({'msg': f'  Error in P Platform: {str(e)}（{len(p_accounts)} 個帳戶皆未寫入）', 'type': 'error'})}\n\n"
```

- [ ] **Step 4: `sync_consistency_check` 加 P 分支**

Modify `services/bh_sync.py`，在 `_process_account` 內 `elif platform == 'M':` 那整段（約 607–635 行）的**後面**插入：

```python
                        elif platform == 'P':
                            # 無區間上限 ⇒ 直接用缺漏日的頭尾一發打完，多回來的日子用不到就丟。
                            s_str = missing_dates[0].strftime('%Y-%m-%d')
                            e_str = missing_dates[-1].strftime('%Y-%m-%d')
                            try:
                                p_map = self._prism_client().fetch_daily_stats(
                                    s_str, e_str, [str(acc_id)])
                            except Exception as e:
                                # fail-closed：不寫任何一天，留給下次補洞檢查重試
                                logs.append(f"[P] {acc_id} {s_str}~{e_str} error: {e}（未寫入）")
                                return logs
                            for td in missing_dates:
                                tstr = td.strftime('%Y-%m-%d')
                                stats = p_map.get((str(acc_id), tstr),
                                                  {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                self._upsert_stats(acc_id, tstr, stats)
                            logs.append(f"     P-Platform: Processed {len(missing_dates)} days.")
```

- [ ] **Step 5: 冒煙驗證（本機起服務，對單一 P 帳戶跑全區間同步）**

先手動塞一筆測試帳戶（用 Task 1 的連線方式），`platform='P'`、`account_id='292-462-3142'`、走期 `2026-08-27`~`2026-08-28`，然後：

```bash
PRISM_API_TOKEN=<token> venv/bin/python app.py
# 另一個終端：
curl -N 'http://localhost:8080/api/bh/account_pk/<PK>/sync_full'
```

Expected: SSE 串流出現 `[BH-FullSync-P] ID:292-462-3142 [2026-08-28] Spend: 162 | Imp: 1596066 | Click: 162`，最後 `Full Sync Completed!`。

再驗一次 fail-closed：把 `PRISM_API_TOKEN` 改成錯的重跑，Expected: 出現 `Error: Prism API 401 ...（本區間未寫入任何資料）`，且 `bh_daily_stats` **沒有**新增任何列。

- [ ] **Step 6: Commit**

```bash
git add services/bh_sync.py
git commit -m "feat(bh): bh_sync 三個進入點支援 P(Prism)，每日同步整批一發、失敗不寫入"
```

---

## Task 4: P 的上傳、範本與 badge

> **只開放 P。** V 的上傳放行、範本選項與 badge 在 Task 11 —— 提前開放會讓 AE 建出「收得下、但永遠不會同步」的 V 帳戶。

**Files:**
- Modify: `services/bh_service.py:56`
- Modify: `generate_bh_template.py`
- Modify: `templates/bh.html:166-169`
- Modify: `static/bh.js`

**Interfaces:**
- Consumes: Task 1 的 enum
- Produces: AE 可以用 Excel 上傳 `平台=P` 的帳戶並在清單看到紫色 P badge

- [ ] **Step 1: 上傳放行 'P'（不放行 'V'）**

Modify `services/bh_service.py:56`，把：

```python
                if platform not in ['R', 'D', 'M']:
```

改成：

```python
                # 註：'V'（D1 影音）刻意還沒放行——它的驗證與同步在 Task 11 才完成，
                #     提前收下會建出永遠不會同步的帳戶。
                if platform not in ['R', 'D', 'M', 'P']:
```

- [ ] **Step 2: 範本下拉加 P**

Modify `generate_bh_template.py:59-61`，把：

```python
dv_platform = DataValidation(type="list", formula1='"R,D,M"', allow_blank=False)
dv_platform.error = '必須填寫 R、D 或 M'
```

改成：

```python
dv_platform = DataValidation(type="list", formula1='"R,D,M,P"', allow_blank=False)
dv_platform.error = '必須填寫 R、D、M 或 P（Prism）'
```

並在 `data` 的範例列後面加一列 P 範例（**這只是給 generator 自己用的樣本，不會進線上範本**，理由見下方警告）：

```python
    # P Platform（Prism）example：AccID＝廣告主 id，格式 233-688-3595。
    # 平台無轉換追蹤，CPAGoal 留空。
    ['P', '292-462-3142', 'Prism 範例帳戶', 80000, '2026-08-01', '2026-08-31', 12, None, '', ''],
```

同時在 `generate_bh_template.py` 檔頭加一段警告 docstring，說明不可拿它覆蓋線上範本（內容見下方警告框）。

> ⚠️⚠️ **執行時發現（2026-09-02，已踩過一次）：`static/bh_import_template.xlsx` 不是
> `generate_bh_template.py` 產生的。** committed 的那份是**手工維護**的：
> ① 內含真實客戶範例列（juliart_覺亞髮品）帶真的 D / MGID token，AE 照著那三列填；
> ② `tests/test_bh_token_import.py` 把它當成「恰好 3 列 R/D/M」的夾具，斷言
> `result == {"total": 3, "inserted": 3, "errors": []}`。
>
> ⇒ **不可跑 `generate_bh_template.py` 覆蓋它**（會同時毀掉這兩者，且測試會紅）。
> **也不可加範例列**（會讓那支測試的 total/inserted 變 4）。只能就地改 data validation。

- [ ] **Step 3: 就地改線上範本的下拉（不重產）**

Run：

```bash
venv/bin/python - <<'PY'
from openpyxl import load_workbook
PATH = 'static/bh_import_template.xlsx'
wb = load_workbook(PATH); ws = wb.active
dv = next(d for d in ws.data_validations.dataValidation if str(d.sqref).startswith('A2'))
print('before:', dv.formula1, '| sqref:', dv.sqref)
dv.formula1 = '"R,D,M,P"'
dv.error = '必須填寫 R、D、M 或 P（Prism）'
wb.save(PATH)
ws2 = load_workbook(PATH).active
print('after :', [d.formula1 for d in ws2.data_validations.dataValidation])
for r in ws2.iter_rows(min_row=1, max_row=5, values_only=True):
    if any(c is not None for c in r): print('  ', r)
PY
```

Expected: formula1 之一為 `"R,D,M,P"`（**不含 V**），且 **R/D/M 三列真實範例資料原封不動還在**。

- [ ] **Step 4: 前端 badge 改成用共用函式**

Modify `templates/bh.html:166-169`，把：

```html
                                    <span class="badge mb-1"
                                        :style="acc.platform === 'R' ? 'background: #0d6efd; border: 1px solid #0d6efd;' : (acc.platform === 'M' ? 'background: #198754; border: 1px solid #198754;' : 'background: transparent; border: 1px solid white;')">
                                        [[ acc.platform ]]
                                    </span>
```

改成：

```html
                                    <span class="badge mb-1" :style="platformBadgeStyle(acc.platform)">
                                        [[ acc.platform ]]
                                    </span>
```

- [ ] **Step 5: 在 bh.js 加 `platformBadgeStyle`**

Modify `static/bh.js`，在 `setup()` 內既有的 `getAgentName` 函式旁邊加入，並在 `setup()` 的 `return { ... }` 裡加上 `platformBadgeStyle`：

```javascript
        // 平台 badge 配色：R 藍 / M 綠 / P 紫 / D 透明底白框（維持原樣）。V 橘在 Task 11 加。
        const PLATFORM_COLORS = {
            R: '#0d6efd',
            M: '#198754',
            P: '#6f42c1',
        };
        const platformBadgeStyle = (platform) => {
            const c = PLATFORM_COLORS[platform];
            return c
                ? `background: ${c}; border: 1px solid ${c};`
                : 'background: transparent; border: 1px solid white;';
        };
```

- [ ] **Step 6: 手動驗證上傳流程**

Run: 起服務，用新範本上傳兩列：`P / 292-462-3142` 與 `V / EVOX_CPM`。

Expected:
1. P 那列 `inserted`，清單出現紫色 `P` badge
2. **V 那列被擋下**，errors 含 `Invalid Platform 'V'`
3. R/D/M 的 badge 顏色與改動前一致

- [ ] **Step 7: Commit**

```bash
git add services/bh_service.py generate_bh_template.py static/bh_import_template.xlsx templates/bh.html static/bh.js
git commit -m "feat(bh): Excel 範本、上傳驗證與前端 badge 支援 P 平台（V 尚未放行）"
```

---

## Task 5: 無轉換平台的 CPA／CV 呈現

> P（與稍後的 V）都沒有轉換數。現在 UI 會把 `conversions=0` / `current_cpa=0` 顯示成 `0`，AE 會誤讀成「CPA 超好」。**帳戶清單與每日明細兩個地方都要改。**

**Files:**
- Modify: `services/bh_service.py`（模組常數、`get_accounts`、`export_accounts_excel`）
- Modify: `templates/bh.html`（帳戶清單 CPA 欄、抽屜 CPA Goal 欄、每日明細 Conv./CPA 欄）

**Interfaces:**
- Consumes: `BHAccount.platform`
- Produces: `get_accounts()` 回傳的每筆多一個 **snake_case** 欄位 `supports_conversions: bool`；`current_cpa` 對無轉換平台為 `None`

- [ ] **Step 1: 後端標記平台是否支援轉換**

Modify `services/bh_service.py`，在 import 區之後加入模組級常數：

```python
# 沒有轉換數據的平台：
#   P（Prism）——平台根本沒實作轉換追蹤（prism_events 全表零筆 conversion 事件）
#   V（D1 影音）——Action4 沒有轉換維度
# 這兩個平台的 CPA/CV 不可顯示 0（會被誤讀成「CPA 超好」），一律顯示 —。
PLATFORMS_WITHOUT_CONVERSIONS = {'P', 'V'}
```

在 `get_accounts` 的 `for acc in accounts:` 迴圈內、`data = acc.to_dict()` 之後加入：

```python
            # 欄位名跟著本 repo 現況用 snake_case（bh.html 消費的欄位全是 snake_case）
            data['supports_conversions'] = acc.platform not in PLATFORMS_WITHOUT_CONVERSIONS
```

並把同一迴圈內原本的：

```python
            if s['cv'] > 0:
                data['current_cpa'] = s['spend'] / s['cv']
            else:
                data['current_cpa'] = 0
```

改成：

```python
            if not data['supports_conversions']:
                data['current_cpa'] = None   # 平台沒有轉換數，不是「CPA 為 0」
            elif s['cv'] > 0:
                data['current_cpa'] = s['spend'] / s['cv']
            else:
                data['current_cpa'] = 0
```

- [ ] **Step 2: Excel 匯出同步處理**

Modify `services/bh_service.py` 的 `export_accounts_excel`，把：

```python
                'CPA目標': d.get('cpa_goal'),
                '目前CPA': d.get('current_cpa'),
```

改成：

```python
                'CPA目標': d.get('cpa_goal') if d.get('supports_conversions') else '不適用',
                '目前CPA': d.get('current_cpa') if d.get('supports_conversions') else '不適用',
```

- [ ] **Step 3: 帳戶清單的 CPA 欄顯示 `—`**

Modify `templates/bh.html:222-225`，把：

```html
                                        :class="{'text-danger': acc.cpa_goal && acc.current_cpa > acc.cpa_goal}">[[
                                        formatNumber(acc.current_cpa) ]]</span>
                                    <span class="text-white-50" v-if="acc.cpa_goal" style="font-size: 13px;">/ [[
                                        formatNumber(acc.cpa_goal) ]]</span>
```

改成：

```html
                                        :class="{'text-danger': acc.supports_conversions && acc.cpa_goal && acc.current_cpa > acc.cpa_goal}">[[
                                        acc.supports_conversions ? formatNumber(acc.current_cpa) : '—' ]]</span>
                                    <template v-if="acc.supports_conversions">
                                        <span class="text-white-50" v-if="acc.cpa_goal" style="font-size: 13px;">/ [[
                                            formatNumber(acc.cpa_goal) ]]</span>
                                        <span class="text-white-50" v-else style="font-size: 13px;">/ -</span>
                                    </template>
                                    <span class="text-white-50" v-else style="font-size: 11px;"
                                        title="此平台沒有轉換追蹤">平台無轉換</span>
```

> ⚠️ **執行時修正（2026-09-02）**：原本這段的下一行還有一個 `<span ... v-else>/ -</span>`
> 兄弟節點，plan 第一版沒把它算進去。`v-else` 必須**緊接**在 `v-if` 後面，中間插入其他元素
> 會斷鏈；所以改成用 `<template v-if>` 把原本那組 `cpa_goal` 的 v-if/v-else 包起來，
> 外層再做一組 v-if/v-else。上面已是修正後的正確版本，**連同原本的 `/ -` 那行一起取代**。

- [ ] **Step 4: 抽屜的 CPA Goal 欄位對無轉換平台隱藏**

Modify `templates/bh.html:327`（`CPC Goal / CPA Goal / CTR Goal` 那個 row 裡的第二個 `col-4`，整段在 `321-336` 行），把：

```html
                                    <div class="col-4">
                                        <label class="form-label text-white small">CPA Goal</label>
```

改成：

```html
                                    <div class="col-4" v-if="selectedAccount.supports_conversions">
                                        <label class="form-label text-white small">CPA Goal</label>
```

> 隱藏後該列剩兩欄、靠左排，這是可接受的呈現，不需要改 grid。

- [ ] **Step 5: 每日明細的 Conv. 與 CPA 欄顯示 `—`**

Modify `templates/bh.html:422-425`（抽屜下方 `dailyStats` 表格的 `<tr v-for>`，該 `<tr>` 起於 417 行），把：

```html
                                            <td class="text-end">[[ stat.conversions.toLocaleString() ]]</td>
                                            <td class="text-end">[[ calcCtr(stat) ]]</td>
                                            <td class="text-end">[[ calcCpc(stat) ]]</td>
                                            <td class="text-end">[[ calcCpa(stat) ]]</td>
```

改成：

```html
                                            <td class="text-end">[[ selectedAccount.supports_conversions ? stat.conversions.toLocaleString() : '—' ]]</td>
                                            <td class="text-end">[[ calcCtr(stat) ]]</td>
                                            <td class="text-end">[[ calcCpc(stat) ]]</td>
                                            <td class="text-end">[[ selectedAccount.supports_conversions ? calcCpa(stat) : '—' ]]</td>
```

> 這張表在抽屜內，`selectedAccount` 一定存在（外層 `v-if="selectedAccount"`，`templates/bh.html:250`），不需要額外的 null guard。

- [ ] **Step 6: 驗證**

Run: 起服務看 `/bh`，確認：
1. P 帳戶的清單 CPA 欄顯示 `—` ＋ 小字「平台無轉換」
2. 點開 P 帳戶抽屜：**沒有** CPA Goal 輸入框；每日明細的 **Conv. 與 CPA 都是 `—`**，CTR/CPC 正常
3. R/D/M 帳戶的清單、抽屜、每日明細全部顯示不變
4. 匯出 Excel，P 列的 CPA 兩欄是「不適用」

- [ ] **Step 7: 跑全部測試**

Run: `venv/bin/python -m unittest discover -s tests -v`
Expected: 全部 PASS

- [ ] **Step 8: Commit**

```bash
git add services/bh_service.py templates/bh.html
git commit -m "feat(bh): 無轉換平台的 CPA/CV 顯示 — 而非 0（帳戶清單＋每日明細）"
```

---

## Task 6: P 平台上線（secret ＋ 正式驗收）

> **這是 M1 的收尾。** 前面五個 task 都跑完才做這一個。

**Files:**
- Modify: `cloudbuild.yaml`

**Interfaces:**
- Consumes: Task 1–5 的全部成果
- Produces: 正式環境的 P 平台可用

- [ ] **Step 1: 建立 Secret Manager secret**

Run:

```bash
printf '%s' '<PRISM_API_TOKEN 值>' | gcloud secrets create PRISM_API_TOKEN \
  --project=popinpoc1 --data-file=- --replication-policy=automatic
gcloud secrets add-iam-policy-binding PRISM_API_TOKEN --project=popinpoc1 \
  --member='serviceAccount:439393162392-compute@developer.gserviceaccount.com' \
  --role='roles/secretmanager.secretAccessor'
```

Expected: `Created secret [PRISM_API_TOKEN].` 與 IAM 綁定成功。

- [ ] **Step 2: cloudbuild.yaml 掛上 secret**

Modify `cloudbuild.yaml` 的 Cloud Run deploy step，把：

```yaml
        '--set-secrets=GOOGLE_CREDENTIALS_JSON=SERVICE_ACCOUNT_JSON:latest',
```

改成：

```yaml
        '--set-secrets=GOOGLE_CREDENTIALS_JSON=SERVICE_ACCOUNT_JSON:latest,PRISM_API_TOKEN=PRISM_API_TOKEN:latest',
```

> ⚠️ **必須用 `--set-secrets`，不可放進 `--set-env-vars`**：那把 token 是全域的、不綁廣告主，等於全體廣告主的報表讀取權，明文寫進 build 設定會進 git 與 build log。

- [ ] **Step 3: 部署並驗收**

Run: push 到 main 觸發 cloudbuild（或 `gcloud builds submit`）。部署完成後在正式站：

1. 下載範本，確認平台下拉是 `R,D,M,P`
2. 上傳一個真實 P 帳戶（`292-462-3142`，走期含 2026-08-28）
3. 跑該帳戶的全區間同步
4. 確認 8/28 那天 Spend=162、Imp=1,596,066、Click=162
5. 確認清單 CPA 欄是 `—`

Expected: 五項全數符合。

- [ ] **Step 4: Commit**

```bash
git add cloudbuild.yaml
git commit -m "chore(bh): Cloud Run 掛上 PRISM_API_TOKEN secret，P 平台上線"
```

> **✅ M1 完成：P 平台已完整上線。** 以下是 M2（V）。

---

# 里程碑 M2：V（D1 影音）上線

## Task 7: `action4_client.py` — Action4 低階 client

**Files:**
- Create: `services/bh_clients/action4_client.py`
- Test: `tests/test_action4_client.py`

**Interfaces:**
- Produces:
  - `ACTION4_MAX_MONTHS = 12`、`ACTION4_MAX_CONCURRENCY = 6`
  - `exceeds_action4_window(start: str, stop: str) -> bool`（純函式，`YYYYMMDD`）
  - `is_date_key(k: str) -> bool`（純函式）
  - `fetch_campaign_stats(campaign_id: str, start: str, stop: str, timeout: int = 60, retries: int = 2) -> dict[str, dict]`，回 `{'YYYYMMDD': {...原始欄位...}}`

- [ ] **Step 1: 寫失敗的測試**

Create `tests/test_action4_client.py`:

```python
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock

from services.bh_clients.action4_client import (
    exceeds_action4_window, is_date_key, fetch_campaign_stats,
)


def _resp(text, status=200):
    m = MagicMock()
    m.status_code = status
    m.text = text
    return m


class TestWindowGuard(unittest.TestCase):
    def test_exactly_12_months_is_allowed(self):
        self.assertFalse(exceeds_action4_window('20250901', '20260901'))

    def test_one_day_over_12_months_is_rejected(self):
        # 超過會靜默回 {"result":1} 不帶任何日期，會被誤讀成「這段沒投放」
        self.assertTrue(exceeds_action4_window('20250831', '20260901'))

    def test_short_range_is_allowed(self):
        self.assertFalse(exceeds_action4_window('20260828', '20260829'))

    def test_leap_day_stop_does_not_crash(self):
        # 2/29 往前推一年不存在，不可讓 replace() 拋 ValueError
        self.assertFalse(exceeds_action4_window('20240301', '20240229'))


class TestIsDateKey(unittest.TestCase):
    def test_accepts_ymd(self):
        self.assertTrue(is_date_key('20260828'))

    def test_rejects_result_and_others(self):
        self.assertFalse(is_date_key('result'))
        self.assertFalse(is_date_key('2026082'))


class TestFetchCampaignStats(unittest.TestCase):
    @patch('services.bh_clients.action4_client.requests.get')
    def test_returns_only_date_keys(self, mock_get):
        mock_get.return_value = _resp(
            '{"result":1,"20260828":{"mobile_video_imp":5662},"20260829":{"mobile_video_imp":3}}'
        )
        out = fetch_campaign_stats('6a8dbcf60b53275b884686e5', '20260828', '20260829')
        self.assertEqual(sorted(out.keys()), ['20260828', '20260829'])
        self.assertNotIn('result', out)

    @patch('services.bh_clients.action4_client.requests.get')
    def test_sends_categories_ca_all(self, mock_get):
        # 不帶 categories=ca_all 回應會塞滿 ca_/cc_/ab_ 子表（實測大 8 倍）
        mock_get.return_value = _resp('{"result":1}')
        fetch_campaign_stats('abc', '20260828', '20260828')
        self.assertIn('categories=ca_all', mock_get.call_args.args[0])

    @patch('services.bh_clients.action4_client.requests.get')
    def test_result_not_1_raises(self, mock_get):
        # result 非 1 是 Action4 那端有問題，不可當「查無資料」吞掉
        mock_get.return_value = _resp('{"result":0}')
        with self.assertRaises(Exception):
            fetch_campaign_stats('abc', '20260828', '20260828', retries=0)

    def test_over_window_raises_before_request(self):
        with self.assertRaises(Exception) as ctx:
            fetch_campaign_stats('abc', '20250101', '20260901')
        self.assertIn('12', str(ctx.exception))


class TestGlobalConcurrencyCap(unittest.TestCase):
    @patch('services.bh_clients.action4_client.requests.get')
    def test_never_more_than_six_requests_in_flight(self, mock_get):
        # 併發管制必須在模組層：v_client 每帳戶開 6 條、bh_sync 又同時跑多個帳戶，
        # 只靠 executor 的 max_workers 實際會變成 6 × 帳戶數（沒驗證過的量）。
        state = {'now': 0, 'max': 0}
        lock = threading.Lock()

        def fake_get(url, timeout=None):
            with lock:
                state['now'] += 1
                state['max'] = max(state['max'], state['now'])
            time.sleep(0.02)
            with lock:
                state['now'] -= 1
            return _resp('{"result":1}')

        mock_get.side_effect = fake_get
        with ThreadPoolExecutor(max_workers=20) as ex:
            list(ex.map(
                lambda i: fetch_campaign_stats(f'c{i}', '20260828', '20260828'),
                range(20)))
        self.assertLessEqual(state['max'], 6)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_action4_client -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'services.bh_clients.action4_client'`

- [ ] **Step 3: 寫實作**

Create `services/bh_clients/action4_client.py`:

```python
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
```

- [ ] **Step 4: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_action4_client -v`
Expected: PASS（10 個測試）

- [ ] **Step 5: Commit**

```bash
git add services/bh_clients/action4_client.py tests/test_action4_client.py
git commit -m "feat(bh): 新增 Action4 client，含 12 個月守衛與全域併發上限 6"
```

---

## Task 8: `d1_video_metrics.py` — 影音口徑純函式

**Files:**
- Create: `services/bh_clients/d1_video_metrics.py`
- Test: `tests/test_d1_video_metrics.py`

**Interfaces:**
- Consumes: Action4 單日原始 dict（Task 7 回傳值之一）
- Produces: `day_metrics(stats: dict | None) -> dict`，回 `{'spend': float, 'impressions': int, 'clicks': int, 'conversions': 0}`

- [ ] **Step 1: 寫失敗的測試**

Create `tests/test_d1_video_metrics.py`:

```python
import unittest

from services.bh_clients.d1_video_metrics import day_metrics


# 2026-08-28 真實 Action4 回應（campaign 6a8dbcf60b53275b884686e5，橫式影音）
REAL_HORIZONTAL = {
    "mobile_video_imp": 5662, "mobile_video_imp_over": 166,
    "mobile_video_link": 9, "mobile_video_100": 1963,
    "pc_video_imp": 128, "pc_video_imp_over": 2, "pc_video_100": 59,
    "charge": {
        "M_mobile_video_imp": 407664000000, "M_mobile_video_imp_over": 11952000000,
        "M_pc_video_imp": 9216000000, "M_pc_video_imp_over": 144000000,
        "mobile_video_imp": 407664, "mobile_video_imp_over": 11952,
        "pc_video_imp": 9216, "pc_video_imp_over": 144,
    },
}

# 2026-08-28 真實回應（campaign 6a913d5f8dbcc677dd696cb6，直式影音）
REAL_VERTICAL = {
    "mobile_video_vertical_imp": 4321, "mobile_video_link": 14,
    "pc_video_vertical_imp": 101,
    "charge": {
        "M_mobile_video_vertical_imp": 311112000000,
        "M_pc_video_vertical_imp": 7272000000,
        "mobile_video_vertical_imp": 311112, "pc_video_vertical_imp": 7272,
    },
}


class TestDayMetrics(unittest.TestCase):
    def test_horizontal_video_mobile_only(self):
        m = day_metrics(REAL_HORIZONTAL)
        # 只算 mobile，pc_video_imp(128) 與 _over(166) 都不計
        self.assertEqual(m['impressions'], 5662)
        self.assertEqual(m['clicks'], 9)
        self.assertAlmostEqual(m['spend'], 407.664, places=3)  # 407664 ÷ 1000
        self.assertEqual(m['conversions'], 0)

    def test_vertical_video_counted(self):
        m = day_metrics(REAL_VERTICAL)
        self.assertEqual(m['impressions'], 4321)
        self.assertAlmostEqual(m['spend'], 311.112, places=3)

    def test_implied_cpm_is_exactly_72(self):
        # 整數 CPM 反推證明除數 1000 正確（兩支不同 campaign 都是 72.00）
        for sample in (REAL_HORIZONTAL, REAL_VERTICAL):
            m = day_metrics(sample)
            self.assertAlmostEqual(m['spend'] / m['impressions'] * 1000, 72.00, places=2)

    def test_missing_fields_are_zero(self):
        self.assertEqual(day_metrics({}),
                         {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0})

    def test_none_is_zero(self):
        self.assertEqual(day_metrics(None)['impressions'], 0)

    def test_over_only_residual_row_is_all_zero(self):
        # Action4 偶爾回只有 _over 的殘列（實測 2026-08-29 就是這種）
        m = day_metrics({"mobile_video_imp_over": 4,
                         "charge": {"mobile_video_imp_over": 288}})
        self.assertEqual(m['impressions'], 0)
        self.assertEqual(m['spend'], 0.0)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_d1_video_metrics -v`
Expected: FAIL，`ModuleNotFoundError`

- [ ] **Step 3: 寫實作**

Create `services/bh_clients/d1_video_metrics.py`:

```python
# D1 影音報表的口徑（純函式，可離線驗證）。
#
# 公式全部照抄 D1 後台原始碼，不是自己推的：
#   popin-discovery-v2/app/Library/apiUtils.php 的 arrangeStats()
#
# ⚠️⚠️ 只算 mobile，PC 一律不計。arrangeStats 是寫死的
#      （$stats['video_imp'] = $dailyStats['mobile_video_imp']，完全沒碰 pc_video_*），
#      前端 lib6 的 video 也確實只出 mobile。Action4 有回 pc_video_*（實測約佔 1.7% 曝光），
#      但加進來就跟 AM 在 D1 後台看到的數字對不起來。
#
# ⚠️ _over（超投）欄位不計入，同樣是為了對齊後台。
#
# ⚠️ 金額的除數是 1000：Action4 的 charge.mobile_video_imp 存的是「CPM × 曝光」。
#    實測 campaign 6a8dbcf60b53275b884686e5（2026-08-28）：charge 407,664 ÷ 1000
#    = 407.664 元 ÷ 5,662 曝光 × 1000 = CPM 72.00 元整；直式那支 311,112 ÷ 4,321
#    也是 72.00 —— 兩支整數 CPM 反推證明除數正確。
#
# ⚠️ D1 影音沒有轉換追蹤，conversions 恆為 0（UI 顯示 — 的處理見 bh_service/bh.html）。

# 收費曝光欄位（橫式＋直式）
IMP_FIELDS = ('mobile_video_imp', 'mobile_video_vertical_imp')
# 點擊欄位
CLICK_FIELD = 'mobile_video_link'
# 金額欄位（charge 子表，值為 CPM × 曝光，取用要 ÷ 1000）
CHARGE_FIELDS = ('mobile_video_imp', 'mobile_video_vertical_imp')
CHARGE_DIVISOR = 1000


def _num(stats, field):
    """頂層數值欄位取值；缺欄／型別不對一律當 0（Action4 沒量的欄位就是整個不出現）。"""
    v = (stats or {}).get(field)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def _charge(stats, field):
    """charge 子表取值。"""
    v = ((stats or {}).get('charge') or {}).get(field)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def day_metrics(stats):
    """Action4 單日原始統計 → BH 統一格式 {spend, impressions, clicks, conversions}。"""
    return {
        'spend': sum(_charge(stats, f) for f in CHARGE_FIELDS) / CHARGE_DIVISOR,
        'impressions': int(sum(_num(stats, f) for f in IMP_FIELDS)),
        'clicks': int(_num(stats, CLICK_FIELD)),
        'conversions': 0,
    }
```

- [ ] **Step 4: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_d1_video_metrics -v`
Expected: PASS（6 個測試）

- [ ] **Step 5: Commit**

```bash
git add services/bh_clients/d1_video_metrics.py tests/test_d1_video_metrics.py
git commit -m "feat(bh): 新增 D1 影音口徑純函式，mobile-only、charge 除 1000"
```

---

## Task 9: `d1_video_catalog.py` — Firestore 影音帳戶目錄

**Files:**
- Create: `services/bh_clients/d1_video_catalog.py`
- Modify: `requirements.txt`
- Test: `tests/test_d1_video_catalog.py`

**Interfaces:**
- Produces:
  - `d1_firestore_available() -> bool`
  - `list_video_campaigns(force_refresh: bool = False) -> list[dict]`，每筆 `{'id', 'name', 'account', 'agency', 'vertical_video', 'deleted'}`（**含已刪除**）
  - `list_accounts() -> list[str]`（不重複 `account`，遞增排序，**從含已刪的全集推導**）
  - `campaigns_for_account(account: str, include_deleted: bool = False) -> list[dict]`
  - `validate_account(account: str) -> tuple[bool, list[str]]`，回 `(是否存在, 最多 3 個模糊建議)`

- [ ] **Step 1: 加 pymongo 依賴**

Modify `requirements.txt`，在 `PyMySQL==1.1.0` 後面加一行：

```
pymongo==4.10.1
```

Run: `venv/bin/pip install -r requirements.txt`
Expected: 安裝成功，`venv/bin/python -c "import pymongo; print(pymongo.version)"` 印出版本。

- [ ] **Step 2: 寫失敗的測試**

Create `tests/test_d1_video_catalog.py`:

```python
import unittest
from unittest.mock import patch

from services.bh_clients import d1_video_catalog as cat


# 取自 2026-09-01 真實 Firestore 查詢的樣本
FAKE_CAMPAIGNS = [
    {'id': '6a1d75328ac0fc73cf275385', 'name': '4A_CPM_chubblife_202606_heho',
     'account': '4A_CPM_chubblife', 'agency': 'popintw', 'vertical_video': False, 'deleted': False},
    {'id': '6a8dbcf60b53275b884686e5', 'name': '國泰影音',
     'account': 'EVOX_CPM', 'agency': 'popintw', 'vertical_video': False, 'deleted': False},
    {'id': '6a913d5f8dbcc677dd696cb6', 'name': '國泰直式影音',
     'account': 'EVOX_CPM', 'agency': 'popintw', 'vertical_video': True, 'deleted': True},
    {'id': '5e82b7a80fc10c15f35e8b94', 'name': '【Video】亞克娛樂',
     'account': 'arch', 'agency': 'haja', 'vertical_video': False, 'deleted': True},
]


class CatalogTestCase(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(cat, 'list_video_campaigns', return_value=FAKE_CAMPAIGNS)
        self.addCleanup(patcher.stop)
        patcher.start()


class TestListAccounts(CatalogTestCase):
    def test_includes_accounts_that_only_have_deleted_campaigns(self):
        # arch 只有一支已刪 campaign，但它花過的錢仍計入預算 → 必須在清單裡
        self.assertEqual(cat.list_accounts(), ['4A_CPM_chubblife', 'EVOX_CPM', 'arch'])


class TestCampaignsForAccount(CatalogTestCase):
    def test_excludes_deleted_by_default(self):
        self.assertEqual(cat.campaigns_for_account('arch'), [])

    def test_include_deleted_returns_them(self):
        self.assertEqual(len(cat.campaigns_for_account('arch', include_deleted=True)), 1)

    def test_include_deleted_returns_both_campaigns_of_account(self):
        ids = {c['id'] for c in cat.campaigns_for_account('EVOX_CPM', include_deleted=True)}
        self.assertEqual(ids, {'6a8dbcf60b53275b884686e5', '6a913d5f8dbcc677dd696cb6'})


class TestValidateAccount(CatalogTestCase):
    def test_exact_match_is_valid(self):
        self.assertEqual(cat.validate_account('EVOX_CPM'), (True, []))

    def test_account_with_only_deleted_campaigns_is_valid(self):
        # 與 list_accounts 一致：驗證通過的帳戶，同步時一定拿得到 campaign
        ok, _ = cat.validate_account('arch')
        self.assertTrue(ok)

    def test_typo_returns_suggestion(self):
        # 少一個 b —— Action4 查不到只會回空，BH 會靜默記 0，所以必須在上傳當下擋
        ok, suggestions = cat.validate_account('4A_CPM_chublife')
        self.assertFalse(ok)
        self.assertIn('4A_CPM_chubblife', suggestions)

    def test_case_mismatch_is_invalid_but_suggested(self):
        ok, suggestions = cat.validate_account('evox_cpm')
        self.assertFalse(ok)
        self.assertIn('EVOX_CPM', suggestions)

    def test_garbage_returns_no_suggestion(self):
        self.assertEqual(cat.validate_account('zzzzzzzzzzzz'), (False, []))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 3: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_d1_video_catalog -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'services.bh_clients.d1_video_catalog'`

- [ ] **Step 4: 寫實作**

Create `services/bh_clients/d1_video_catalog.py`:

```python
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
        from pymongo import MongoClient
        _client = MongoClient(uri, serverSelectionTimeoutMS=20000)
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
```

> ⚠️ **執行時追加兩項（2026-09-02，皆實測後才發現）**：
> 1. **模糊比對必須大小寫不敏感**。第一版直接把 `account` 丟給 `difflib`，`evox_cpm` →
>    `EVOX_CPM` 完全給不出建議，而那正是最常見的打錯法。已改成上面的降冪比對版本。
> 2. **`MongoClient` 必須帶 `tlsCAFile=certifi.where()`**。macOS 的 Python.framework
>    沒有 CA bundle，連 `*.firestore.goog:443` 會噴
>    `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate`。
>    `requirements.txt` 一併明列 `certifi`（原本只是 requests 的傳遞依賴）。

- [ ] **Step 5: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_d1_video_catalog -v`
Expected: PASS（9 個測試）

- [ ] **Step 6: 打真 Firestore 驗證數量**

Run:

```bash
D1_FIRESTORE_URI='<uri>' venv/bin/python -c "
from services.bh_clients import d1_video_catalog as c
all_ = c.list_video_campaigns()
print('影音 campaign 總數:', len(all_), '/ 未刪:', len([x for x in all_ if not x['deleted']]))
print('不重複 account:', len(c.list_accounts()))
print(c.validate_account('4A_CPM_chublife'))
"
```

Expected: 總數約 395、未刪約 308、不重複 account 約 170（數字會隨時間增長，量級對即可）；最後一行印出 `(False, ['4A_CPM_chubblife'])`。

- [ ] **Step 7: Commit**

```bash
git add services/bh_clients/d1_video_catalog.py tests/test_d1_video_catalog.py requirements.txt
git commit -m "feat(bh): 新增 D1 影音帳戶目錄（Firestore），含已刪 campaign 與模糊比對建議"
```

---

## Task 10: `v_client.py` — V 平台組裝層（fail-closed）

**Files:**
- Create: `services/bh_clients/v_client.py`
- Create: `poc/probe_d1video_bh.py`
- Test: `tests/test_v_client.py`

**Interfaces:**
- Consumes: `d1_video_catalog.campaigns_for_account`（Task 9）、`action4_client.fetch_campaign_stats`（Task 7）、`d1_video_metrics.day_metrics`（Task 8）
- Produces: `class D1VideoClient()`，方法 `fetch_daily_stats(account: str, start_date: str, end_date: str) -> dict[tuple[str, str], dict]`，key 的日期是 `YYYY-MM-DD`（與 R/D/M/P 一致）

- [ ] **Step 1: 寫失敗的測試**

Create `tests/test_v_client.py`:

```python
import unittest
from unittest.mock import patch

from services.bh_clients.v_client import D1VideoClient


C_H = {'id': 'cam_h', 'name': '橫式', 'account': 'EVOX_CPM',
       'agency': 'popintw', 'vertical_video': False, 'deleted': False}
C_V = {'id': 'cam_v', 'name': '直式', 'account': 'EVOX_CPM',
       'agency': 'popintw', 'vertical_video': True, 'deleted': True}

# 兩支 campaign 在同一天都有量（真實數字：橫式 407.664 元 / 5662 imp / 9 clk，
#                              直式 311.112 元 / 4321 imp / 14 clk）
REPORT_H = {'20260828': {'mobile_video_imp': 5662, 'mobile_video_link': 9,
                         'charge': {'mobile_video_imp': 407664}}}
REPORT_V = {'20260828': {'mobile_video_vertical_imp': 4321, 'mobile_video_link': 14,
                         'charge': {'mobile_video_vertical_imp': 311112}}}


@patch('services.bh_clients.v_client.fetch_campaign_stats')
@patch('services.bh_clients.d1_video_catalog.campaigns_for_account')
class TestFetchDailyStats(unittest.TestCase):

    def test_aggregates_multiple_campaigns_into_one_account_day(self, mock_cps, mock_fetch):
        mock_cps.return_value = [C_H, C_V]
        mock_fetch.side_effect = lambda cid, s, e: REPORT_H if cid == 'cam_h' else REPORT_V

        out = D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-28', '2026-08-28')

        stats = out[('EVOX_CPM', '2026-08-28')]
        self.assertAlmostEqual(stats['spend'], 718.776, places=3)   # 407.664 + 311.112
        self.assertEqual(stats['impressions'], 9983)                # 5662 + 4321
        self.assertEqual(stats['clicks'], 23)                       # 9 + 14
        self.assertEqual(stats['conversions'], 0)

    def test_requests_deleted_campaigns_too(self, mock_cps, mock_fetch):
        # BH 是預算追蹤：campaign 被刪不代表它花過的錢要消失
        mock_cps.return_value = [C_H]
        mock_fetch.return_value = REPORT_H
        D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-28', '2026-08-28')
        self.assertIs(mock_cps.call_args.kwargs.get('include_deleted'), True)

    def test_any_campaign_failure_raises_and_returns_nothing(self, mock_cps, mock_fetch):
        # fail-closed：只要一支失敗就整批拋。回半套會讓上層把缺的日期寫 0，
        # 而補洞檢查只看「那天有沒有列」，寫下去就永遠不會被修正。
        mock_cps.return_value = [C_H, C_V]

        def side_effect(cid, s, e):
            if cid == 'cam_v':
                raise Exception('Action4 HTTP 500')
            return REPORT_H

        mock_fetch.side_effect = side_effect
        with self.assertRaises(Exception) as ctx:
            D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-28', '2026-08-28')
        self.assertIn('cam_v', str(ctx.exception))

    def test_account_without_campaigns_raises_not_silent_zero(self, mock_cps, mock_fetch):
        # 目錄查不到 campaign 是資料異常（validate_account 應該早就擋掉），
        # 不可回空 map 讓上層寫成全 0
        mock_cps.return_value = []
        with self.assertRaises(Exception):
            D1VideoClient().fetch_daily_stats('ghost_account', '2026-08-28', '2026-08-28')

    def test_days_without_volume_are_absent_not_zero_rows(self, mock_cps, mock_fetch):
        # Action4 只回有量的日子；沒量的日子不該出現在 map 裡（由上層決定填 0）
        mock_cps.return_value = [C_H]
        mock_fetch.return_value = REPORT_H
        out = D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-27', '2026-08-28')
        self.assertEqual(list(out.keys()), [('EVOX_CPM', '2026-08-28')])

    def test_over_only_residual_days_are_dropped(self, mock_cps, mock_fetch):
        mock_cps.return_value = [C_H]
        mock_fetch.return_value = {
            '20260829': {'mobile_video_imp_over': 4, 'charge': {'mobile_video_imp_over': 288}}
        }
        out = D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-29', '2026-08-29')
        self.assertEqual(out, {})

    def test_passes_ymd_format_to_action4(self, mock_cps, mock_fetch):
        # BH 用 YYYY-MM-DD，Action4 要 YYYYMMDD
        mock_cps.return_value = [C_H]
        mock_fetch.return_value = REPORT_H
        D1VideoClient().fetch_daily_stats('EVOX_CPM', '2026-08-01', '2026-08-28')
        self.assertEqual(mock_fetch.call_args.args[1:], ('20260801', '20260828'))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_v_client -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'services.bh_clients.v_client'`

- [ ] **Step 3: 寫實作**

Create `services/bh_clients/v_client.py`:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

from services.bh_clients.action4_client import fetch_campaign_stats
from services.bh_clients.d1_video_metrics import day_metrics
from services.bh_clients import d1_video_catalog

# V（D1 影音）組裝層：Firestore 拿 campaign 清單 → Action4 併發抓成效 → 疊成 BH 統一 map。
#
# 兩個資料源的分工（原因見各自檔頭）：
#   d1_video_catalog（Firestore）：帳戶名、活動名、影音旗標、是否已刪
#   action4_client：所有成效數字（D 平台報表 API 一格都沒有）
#
# ⚠️ 這裡的 max_workers 只是「這個帳戶最多同時排幾支」；對 Action4 的**真正併發上限**是
#    action4_client 模組層的 semaphore（6）。bh_sync 同時跑多個帳戶時，多出來的請求會在
#    semaphore 上排隊，不會突破 6。

PER_ACCOUNT_WORKERS = 6


def _to_ymd(dash):
    return dash.replace('-', '')


def _to_dash(ymd):
    return f'{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}'


class D1VideoClient:
    def fetch_daily_stats(self, account, start_date, end_date):
        """抓某影音帳戶的日報表，回 {(account, 'YYYY-MM-DD'): stats}。

        帳戶底下的所有 campaign（**含已刪除**）會加總到同一個帳戶 key——BH 是以帳戶為
        單位管預算，campaign 被刪不代表它花過的錢要從預算裡消失。

        ⚠️ fail-closed：只要有任何一支 campaign 抓取失敗就整批 raise，不回半套結果。
           BH 沒有地方能呈現 campaign 層級的 warning，而上層拿到不完整的 map 會把缺的
           日期寫成 0；補洞檢查（bh_sync.py:494）只看「那天有沒有列」，0 一旦寫進去就
           永遠不會被修正 → 永久低估。
        """
        campaigns = d1_video_catalog.campaigns_for_account(account, include_deleted=True)
        if not campaigns:
            raise Exception(
                f'D1 影音帳戶「{account}」查無任何 campaign（目錄可能已變動，請重新確認帳戶名）')

        s, e = _to_ymd(start_date), _to_ymd(end_date)
        stats = {}
        errors = []

        with ThreadPoolExecutor(max_workers=PER_ACCOUNT_WORKERS) as ex:
            futures = {ex.submit(fetch_campaign_stats, c['id'], s, e): c for c in campaigns}
            for fut in as_completed(futures):
                c = futures[fut]
                try:
                    report = fut.result()
                except Exception as exc:
                    errors.append(f'{c["name"]}（{c["id"]}）：{exc}')
                    continue
                for ymd, raw in report.items():
                    m = day_metrics(raw)
                    # Action4 偶爾回只有 _over 的殘列，全 0 的日子不要放進 map
                    if m['impressions'] == 0 and m['clicks'] == 0 and m['spend'] == 0:
                        continue
                    key = (str(account), _to_dash(ymd))
                    a = stats.setdefault(
                        key, {'spend': 0.0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                    a['spend'] += m['spend']
                    a['impressions'] += m['impressions']
                    a['clicks'] += m['clicks']

        if errors:
            raise Exception(
                f'D1 影音帳戶「{account}」有 {len(errors)}/{len(campaigns)} 支 campaign 抓取失敗，'
                f'整批不寫入：' + '；'.join(errors[:3]))
        return stats
```

- [ ] **Step 4: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_v_client -v`
Expected: PASS（7 個測試）

- [ ] **Step 5: 寫 POC 打真 API 驗證**

Create `poc/probe_d1video_bh.py`:

```python
"""打真 Firestore + Action4，印出 BH 統一 map。用法：
   D1_FIRESTORE_URI=xxx venv/bin/python poc/probe_d1video_bh.py EVOX_CPM 2026-08-28 2026-08-28
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.bh_clients import d1_video_catalog
from services.bh_clients.v_client import D1VideoClient

account = sys.argv[1] if len(sys.argv) > 1 else 'EVOX_CPM'
sd = sys.argv[2] if len(sys.argv) > 2 else '2026-08-28'
ed = sys.argv[3] if len(sys.argv) > 3 else '2026-08-28'

print('帳戶存在性:', d1_video_catalog.validate_account(account))
cps = d1_video_catalog.campaigns_for_account(account, include_deleted=True)
print(f'{account} 影音 campaign {len(cps)} 支（含已刪）')
for c in cps:
    flag = '直式' if c['vertical_video'] else '橫式'
    print(f"  {c['id']} {flag} {'[已刪]' if c['deleted'] else ''} {c['name']}")

out = D1VideoClient().fetch_daily_stats(account, sd, ed)
print(f'\n{sd}~{ed} 共 {len(out)} 天有量')
for (acc, d), s in sorted(out.items()):
    cpm = s['spend'] / s['impressions'] * 1000 if s['impressions'] else 0
    print(f"  {d} imp={s['impressions']:>8,} clk={s['clicks']:>5,} "
          f"spend={s['spend']:>10,.2f} 反推CPM={cpm:.2f}")
```

Run: `D1_FIRESTORE_URI='<uri>' venv/bin/python poc/probe_d1video_bh.py CPM_MundoPixarExperience 2026-08-28 2026-08-28`

> ⚠️ **執行時修正（2026-09-02）**：plan 原本寫 `EVOX_CPM`，但實查 Firestore 後發現
> 那兩支拿來驗證口徑的 campaign（`6a8dbcf60b53275b884686e5` 橫式、
> `6a913d5f8dbcc677dd696cb6` 直式）其實屬於 **`CPM_MundoPixarExperience`**，
> 不是 EVOX_CPM。驗證帳戶一律改用前者。

Expected（2026-09-02 實跑結果，可當回歸基準）：18 支 campaign（含已刪）、
`imp=38,919 clk=77 spend=2,802.17`、**反推 CPM = 72.00 整**。
`spend` 量級是幾百到幾千元（**不是幾十萬** —— 若看到幾十萬代表除數 1000 掉了）。
整個帳戶 18 支加總後 CPM 仍是整數，是除數正確的強證據（不只是單支巧合）。

- [ ] **Step 6: 跑全部測試**

Run: `venv/bin/python -m unittest discover -s tests -v`
Expected: 全部 PASS

- [ ] **Step 7: Commit**

```bash
git add services/bh_clients/v_client.py tests/test_v_client.py poc/probe_d1video_bh.py
git commit -m "feat(bh): 新增 D1 影音(V) 組裝層，含已刪 campaign、任一支失敗即整批不寫"
```

---

## Task 11: V 的同步、上傳驗證、範本與 badge

> V 的所有對外開放（範本選項、上傳放行、badge）與同步能力在**同一個 task** 發布，避免出現「收得下但不會同步」的帳戶。

**Files:**
- Modify: `services/bh_sync.py`（import、`segment_dates` 純函式、三個進入點）
- Modify: `services/bh_service.py`（上傳放行 V ＋ Firestore 驗證）
- Modify: `generate_bh_template.py`
- Modify: `static/bh.js`
- Test: `tests/test_bh_sync_segments.py`、`tests/test_bh_service_v_upload.py`

**Interfaces:**
- Consumes: `D1VideoClient.fetch_daily_stats(account, start_date, end_date)`（Task 10）、`d1_video_catalog.validate_account` / `d1_firestore_available`（Task 9）
- Produces: `segment_dates(dates: list[date], max_len: int) -> list[list[date]]`（模組級純函式）；`platform='V'` 三條同步路徑可用；上傳打錯帳戶名會被擋下並給建議

- [ ] **Step 1: 寫 `segment_dates` 的失敗測試**

Create `tests/test_bh_sync_segments.py`:

```python
import unittest
from datetime import date, timedelta

from services.bh_sync import segment_dates


class TestSegmentDates(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(segment_dates([], 360), [])

    def test_single_continuous_run_under_limit(self):
        days = [date(2026, 8, 1) + timedelta(days=i) for i in range(5)]
        self.assertEqual(segment_dates(days, 360), [days])

    def test_splits_on_gap(self):
        days = [date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 10)]
        self.assertEqual(segment_dates(days, 360),
                         [[date(2026, 8, 1), date(2026, 8, 2)], [date(2026, 8, 10)]])

    def test_splits_at_max_len_boundary(self):
        days = [date(2025, 1, 1) + timedelta(days=i) for i in range(361)]
        segs = segment_dates(days, 360)
        self.assertEqual([len(s) for s in segs], [360, 1])

    def test_exactly_max_len_is_one_segment(self):
        days = [date(2025, 1, 1) + timedelta(days=i) for i in range(360)]
        self.assertEqual(len(segment_dates(days, 360)), 1)

    def test_every_segment_is_within_action4_window(self):
        # 360 天上限是為了不踩 Action4 的 12 個月硬限制
        days = [date(2025, 1, 1) + timedelta(days=i) for i in range(800)]
        for seg in segment_dates(days, 360):
            self.assertLessEqual((seg[-1] - seg[0]).days, 359)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_bh_sync_segments -v`
Expected: FAIL，`ImportError: cannot import name 'segment_dates'`

- [ ] **Step 3: 加 import 與 `segment_dates` 純函式**

Modify `services/bh_sync.py`，在 `from services.bh_clients.p_client import PrismClient` 後面加：

```python
from services.bh_clients.v_client import D1VideoClient
```

並在 `class BHSyncService:` **之前**（模組層）加入：

```python
def segment_dates(dates, max_len):
    """把排序過的日期清單切成「連續且長度 <= max_len」的段。純函式，供 V 分支與測試使用。

    註：R（7 天）與 M（90 天）有各自的既有 inline 實作，本次不動它們（surgical changes）。
    """
    if not dates:
        return []
    segments = []
    seg = [dates[0]]
    for d in dates[1:]:
        if (d - seg[-1]).days == 1 and len(seg) < max_len:
            seg.append(d)
        else:
            segments.append(seg)
            seg = [d]
    segments.append(seg)
    return segments
```

- [ ] **Step 4: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_bh_sync_segments -v`
Expected: PASS（6 個測試）

- [ ] **Step 5: `sync_account_full_range_by_pk` 加 V 分支**

Modify `services/bh_sync.py`，在 Task 3 Step 2 加的 `elif account.platform == 'P':` 那整段後面插入：

```python
                elif account.platform == 'V':
                    # Action4 區間上限 12 個月 ⇒ 依 360 天切段（留邊際，避免踩線後靜默回空）。
                    v_client = D1VideoClient()
                    for segment in segment_dates(dates_to_sync, 360):
                        s_str = segment[0].strftime('%Y-%m-%d')
                        e_str = segment[-1].strftime('%Y-%m-%d')
                        yield f"data: {json.dumps({'msg': f'Fetching {s_str} ~ {e_str}...'})}\n\n"
                        try:
                            v_map = v_client.fetch_daily_stats(str(account_id), s_str, e_str)
                        except Exception as e:
                            # fail-closed：本段不寫任何一天（v_client 已保證是「全有或全無」）
                            yield f"data: {json.dumps({'msg': f'  Error: {e}（{s_str}~{e_str} 未寫入）', 'type': 'error'})}\n\n"
                            return
                        for target_date in segment:
                            target_str = target_date.strftime('%Y-%m-%d')
                            stats = v_map.get((str(account_id), target_str),
                                              {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                            self._upsert_stats(account_id, target_str, stats, app=app)
                            log_msg = f"  [{target_str}] Spend: {int(stats.get('spend', 0))} | Imp: {stats.get('impressions', 0)} | Click: {stats.get('clicks', 0)}"
                            print(f"[BH-FullSync-V] ID:{account_id} {log_msg}", flush=True)
                            yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                        yield f"data: {json.dumps({'msg': f'  -> Saved.'})}\n\n"
```

- [ ] **Step 6: `sync_daily_stats` 加 V 分支**

Modify `services/bh_sync.py`：

(a) 在 `p_accounts = [a for a in accounts if a.platform == 'P']` 後面加：

```python
            v_accounts = [a for a in accounts if a.platform == 'V']
```

(b) 在 Task 3 Step 3 加的 P 平台整段後面插入：

```python
            # --- Process V Platform (D1 影音：Firestore 清單 + Action4) ---
            # 註：這裡的 max_workers 只是帳戶層的排程；對 Action4 的真正併發上限是
            #     action4_client 模組層的 semaphore(6)，多的請求會排隊不會突破。
            if v_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(v_accounts)} V-Platform accounts (D1 Video)...'})}\n\n"
                v_client = D1VideoClient()
                v_executor = ThreadPoolExecutor(max_workers=3)
                try:
                    def _fetch_v(acc):
                        try:
                            return acc, v_client.fetch_daily_stats(
                                str(acc.account_id), target_date, target_date), None
                        except Exception as e:
                            return acc, None, str(e)

                    futures = {v_executor.submit(_fetch_v, acc): acc for acc in v_accounts}
                    for future in as_completed(futures):
                        acc, vmap, err = future.result()
                        if err:
                            # fail-closed：這個帳戶今天完全不寫，留給補洞檢查重試
                            yield f"data: {json.dumps({'msg': f'  [V] {acc.account_id} 未寫入: {err}', 'type': 'error'})}\n\n"
                            continue
                        stats = vmap.get((str(acc.account_id), target_date),
                                         {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                        self._upsert_stats(acc.account_id, target_date, stats)
                        log_msg = f"    [V] {acc.account_id}: Spend={int(stats.get('spend',0))}, Clicks={stats.get('clicks',0)}"
                        yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                finally:
                    v_executor.shutdown(wait=False)
                yield f"data: {json.dumps({'msg': f'  V Platform processed ({len(v_accounts)} accounts).'})}\n\n"
```

- [ ] **Step 7: `sync_consistency_check` 加 V 分支**

Modify `services/bh_sync.py`，在 Task 3 Step 4 加的 `elif platform == 'P':` 那整段後面插入：

```python
                        elif platform == 'V':
                            v_client = D1VideoClient()
                            for segment in segment_dates(missing_dates, 360):
                                s_str = segment[0].strftime('%Y-%m-%d')
                                e_str = segment[-1].strftime('%Y-%m-%d')
                                try:
                                    v_map = v_client.fetch_daily_stats(str(acc_id), s_str, e_str)
                                except Exception as e:
                                    # fail-closed：本段不寫，下次補洞檢查會再抓一次
                                    logs.append(f"[V] {acc_id} {s_str}~{e_str} error: {e}（未寫入）")
                                    continue
                                for td in segment:
                                    tstr = td.strftime('%Y-%m-%d')
                                    stats = v_map.get((str(acc_id), tstr),
                                                      {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                    self._upsert_stats(acc_id, tstr, stats)
                            logs.append(f"     V-Platform: Processed {len(missing_dates)} days.")
```

- [ ] **Step 8: 寫上傳驗證的失敗測試**

Create `tests/test_bh_service_v_upload.py`:

```python
import unittest
from io import BytesIO
from unittest.mock import patch

import pandas as pd

from services.bh_service import BHService


def _excel(rows):
    df = pd.DataFrame(rows, columns=[
        '平台', 'AccID', '名稱', 'Budget', 'StartDate', 'EndDate',
        'CPCGoal', 'CPAGoal', 'R的cv定義', 'D&MGID的Token'])
    buf = BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return buf


ROW_OK = ['V', 'EVOX_CPM', '國泰影音', 150000, '2026-08-01', '2026-08-31', 20, None, '', '']
ROW_TYPO = ['V', 'EVOX_CM', '打錯的', 150000, '2026-08-01', '2026-08-31', 20, None, '', '']


class TestVUploadValidation(unittest.TestCase):
    @patch('services.bh_service.db')
    @patch('services.bh_clients.d1_video_catalog.validate_account')
    @patch('services.bh_clients.d1_video_catalog.list_video_campaigns')
    @patch('services.bh_clients.d1_video_catalog.d1_firestore_available', return_value=True)
    def test_typo_row_is_rejected_with_suggestion(
            self, _avail, _list, mock_validate, mock_db):
        mock_validate.side_effect = lambda a: (True, []) if a == 'EVOX_CPM' else (False, ['EVOX_CPM'])
        result = BHService().process_excel_upload(_excel([ROW_OK, ROW_TYPO]), 'me@popin.cc')

        self.assertEqual(result['inserted'], 1)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('EVOX_CM', result['errors'][0])
        self.assertIn('EVOX_CPM', result['errors'][0])   # 建議

    @patch('services.bh_service.db')
    @patch('services.bh_clients.d1_video_catalog.d1_firestore_available', return_value=False)
    def test_all_v_rows_rejected_when_catalog_unavailable(self, _avail, mock_db):
        # 沒有目錄就無法驗證 → 一律不收，不可放進來變成永遠同步不到的帳戶
        result = BHService().process_excel_upload(_excel([ROW_OK]), 'me@popin.cc')
        self.assertEqual(result['inserted'], 0)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('D1_FIRESTORE_URI', result['errors'][0])


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 9: 跑測試確認失敗**

Run: `venv/bin/python -m unittest tests.test_bh_service_v_upload -v`
Expected: FAIL —— V 目前還沒放行，`inserted` 會是 0 且錯誤訊息是 `Invalid Platform 'V'`

- [ ] **Step 10: 上傳放行 V ＋ 加驗證**

Modify `services/bh_service.py:56`，把 Task 4 Step 1 的：

```python
                # 註：'V'（D1 影音）刻意還沒放行——它的驗證與同步在 Task 11 才完成，
                #     提前收下會建出永遠不會同步的帳戶。
                if platform not in ['R', 'D', 'M', 'P']:
```

改成：

```python
                if platform not in ['R', 'D', 'M', 'P', 'V']:
```

在 `process_excel_upload` 內、`for index, row in df.iterrows():` 迴圈**之前**（`results = {...}` 之後）插入：

```python
        # V（D1 影音）帳戶字串驗證：Excel 打錯一個字，Action4 只會查無資料回空，
        # BH 會靜默記成花費 0——比報錯還危險。所以在上傳當下就比對 Firestore 目錄並給建議。
        # 目錄有 5 分鐘快取，整批上傳只會連一次 Firestore。
        has_v_rows = any(str(r).strip().upper() == 'V' for r in df.get('平台', []))
        v_catalog_error = None
        if has_v_rows:
            from services.bh_clients import d1_video_catalog
            if not d1_video_catalog.d1_firestore_available():
                v_catalog_error = '未設定 D1_FIRESTORE_URI，無法驗證 D1 影音帳戶，V 平台的列全部略過'
            else:
                try:
                    d1_video_catalog.list_video_campaigns()
                except Exception as e:
                    v_catalog_error = f'連線 D1 影音目錄失敗（{e}），V 平台的列全部略過'
```

接著在同一個迴圈內、`if not acc_id:` 檢查**之後**插入：

```python
                if platform == 'V':
                    if v_catalog_error:
                        results['errors'].append(f"Row {index+2}: {v_catalog_error}")
                        continue
                    from services.bh_clients import d1_video_catalog
                    ok, suggestions = d1_video_catalog.validate_account(acc_id)
                    if not ok:
                        hint = f"，你是不是要填「{suggestions[0]}」？" if suggestions else \
                               "（大小寫必須完全一致；可用的帳戶清單見 D1 後台）"
                        results['errors'].append(
                            f"Row {index+2}: 查無 D1 影音帳戶「{acc_id}」{hint}")
                        continue
```

- [ ] **Step 11: 跑測試確認通過**

Run: `venv/bin/python -m unittest tests.test_bh_service_v_upload -v`
Expected: PASS（2 個測試）

- [ ] **Step 12: 範本與 badge 加 V**

Modify `generate_bh_template.py`，把 Task 4 Step 2 的：

```python
dv_platform = DataValidation(type="list", formula1='"R,D,M,P"', allow_blank=False)
dv_platform.error = '必須填寫 R、D、M 或 P（Prism）'
```

改成：

```python
dv_platform = DataValidation(type="list", formula1='"R,D,M,P,V"', allow_blank=False)
dv_platform.error = '必須填寫 R、D、M、P（Prism）或 V（D1影音）'
```

並在 P 的範例列後面加一列：

```python
    # V Platform（D1 影音）example：AccID＝D1 影音帳戶字串，大小寫必須完全一致。
    # 平台無轉換追蹤，CPAGoal 留空。
    ['V', '4A_CPM_chubblife', 'D1影音 範例帳戶', 150000, '2026-08-01', '2026-08-31', 20, None, '', ''],
```

Modify `static/bh.js`，把 Task 4 Step 5 的 `PLATFORM_COLORS` 加上 V：

```javascript
        // 平台 badge 配色：R 藍 / M 綠 / P 紫 / V 橘 / D 透明底白框（維持原樣）
        const PLATFORM_COLORS = {
            R: '#0d6efd',
            M: '#198754',
            P: '#6f42c1',
            V: '#fd7e14',
        };
```

同樣**不可重產**線上範本（理由見 Task 4 Step 3 的警告），就地改下拉：

```bash
venv/bin/python - <<'PY'
from openpyxl import load_workbook
PATH = 'static/bh_import_template.xlsx'
wb = load_workbook(PATH); ws = wb.active
dv = next(d for d in ws.data_validations.dataValidation if str(d.sqref).startswith('A2'))
dv.formula1 = '"R,D,M,P,V"'
dv.error = '必須填寫 R、D、M、P（Prism）或 V（D1影音）'
wb.save(PATH)
print([d.formula1 for d in load_workbook(PATH).active.data_validations.dataValidation])
PY
```

Expected: 下拉為 `"R,D,M,P,V"`，R/D/M 三列真實範例資料仍在，`unittest discover` 仍全綠。

- [ ] **Step 13: 端到端驗證**

Run:

```bash
D1_FIRESTORE_URI='<uri>' PRISM_API_TOKEN='<token>' venv/bin/python app.py
```

上傳三列：`V / CPM_MundoPixarExperience`（正確）、`V / CPM_MundoPixarExperienc`（打錯，少一個 e）、`P / 292-462-3142`。

Expected:
1. `inserted: 2`，errors 含 `Row 3: 查無 D1 影音帳戶「CPM_MundoPixarExperienc」，你是不是要填「CPM_MundoPixarExperience」？`
2. 清單出現橘色 `V` badge
3. 對 `CPM_MundoPixarExperience`（走期 2026-08-27~2026-08-28）跑全區間同步，SSE 出現
   `[BH-FullSync-V] ID:CPM_MundoPixarExperience [2026-08-28] Spend: 2802 | Imp: 38919 | Click: 77`
4. 抽屜的每日明細，Conv. 與 CPA 都是 `—`

- [ ] **Step 14: 跑全部測試**

Run: `venv/bin/python -m unittest discover -s tests -v`
Expected: 全部 PASS

- [ ] **Step 15: Commit**

```bash
git add services/bh_sync.py services/bh_service.py generate_bh_template.py static/bh.js static/bh_import_template.xlsx tests/test_bh_sync_segments.py tests/test_bh_service_v_upload.py
git commit -m "feat(bh): V(D1影音) 三條同步路徑、上傳驗證、範本與 badge 一併開放"
```

---

## Task 12: V 平台上線（secret ＋ 正式驗收）

> **這是 M2 的收尾。**

**Files:**
- Modify: `cloudbuild.yaml`

**Interfaces:**
- Consumes: Task 7–11 的全部成果
- Produces: 正式環境的 V 平台可用

- [ ] **Step 1: 重用既有的 Firestore secret（不要新建）**

> ✅ **執行時查證（2026-09-02）**：`popinpoc1` 的 Secret Manager **已經有
> `ad-tools-d1videoad-firestore-uri`**（ad_tools tool#8 建的，就是同一組 D1 Firestore
> 連線字串）。**不要再建一份 `D1_FIRESTORE_URI`** —— 同一組憑證存兩份會漂移，換密碼時
> 一定有一邊忘了改。直接把既有 secret 掛成 BH 的 `D1_FIRESTORE_URI` 環境變數即可。

Run（只需補 BH 服務帳號的讀取權限）：

```bash
gcloud secrets describe ad-tools-d1videoad-firestore-uri --project=popinpoc1
gcloud secrets add-iam-policy-binding ad-tools-d1videoad-firestore-uri --project=popinpoc1 \
  --member='serviceAccount:439393162392-compute@developer.gserviceaccount.com' \
  --role='roles/secretmanager.secretAccessor'
```

Expected: secret 存在，IAM 綁定成功。

> ⚠️ 這串 URI **內嵌 SCRAM 帳密**，等同 D1 campaign 設定庫的讀取權，必須走 Secret Manager。

- [ ] **Step 2: cloudbuild.yaml 掛上 secret**

Modify `cloudbuild.yaml`，把 Task 6 Step 2 的：

```yaml
        '--set-secrets=GOOGLE_CREDENTIALS_JSON=SERVICE_ACCOUNT_JSON:latest,PRISM_API_TOKEN=PRISM_API_TOKEN:latest',
```

改成：

```yaml
        '--set-secrets=GOOGLE_CREDENTIALS_JSON=SERVICE_ACCOUNT_JSON:latest,PRISM_API_TOKEN=PRISM_API_TOKEN:latest,D1_FIRESTORE_URI=ad-tools-d1videoad-firestore-uri:latest',
```

> 左邊是 BH 讀的環境變數名 `D1_FIRESTORE_URI`，右邊是既有的 secret 名
> `ad-tools-d1videoad-firestore-uri`。兩者刻意不同名，就是為了重用而不是複製。

- [ ] **Step 3: 🔴 對帳：拿實際數字去對 D1 後台 UI**

> **這是整份 plan 唯一還沒對過外部真值的地方。** 除數 1000 與 mobile-only 口徑目前只有「兩支 campaign 反推 CPM 都是 72.00」的自我一致性佐證，**沒有跟後台對過**。上線前必須做。

Run: 在正式站對 `EVOX_CPM` 跑完同步後，打開 D1 後台該帳戶的影音報表，逐日比對「收費曝光」與「金額」。

Expected: BH 的 `impressions` 與 `spend` 與 D1 後台**逐日相等**。
- 若 BH 偏高約 1.7% → PC 被算進去了，檢查 `d1_video_metrics.IMP_FIELDS`
- 若 BH 是後台的 1000 倍 → `CHARGE_DIVISOR` 掉了
- 若 BH 偏低 → 檢查是不是漏了已刪除的 campaign（`include_deleted=True`）

**對不上就不要上線**，先回頭修 `d1_video_metrics.py` 並補一個對應的測試。

- [ ] **Step 4: 部署並驗收**

Run: push 觸發 cloudbuild。部署完成後在正式站：

1. 下載範本，確認下拉是 `R,D,M,P,V`
2. 上傳一個打錯的 V 帳戶名，確認被擋下且有建議
3. 上傳正確的 V 帳戶並跑全區間同步，數字與 Step 3 對帳結果一致
4. 確認清單與每日明細的 CPA／Conv. 都是 `—`
5. 跑一次補洞檢查，確認不會對已有資料的日期重抓

Expected: 五項全數符合。

- [ ] **Step 5: Commit**

```bash
git add cloudbuild.yaml
git commit -m "chore(bh): Cloud Run 掛上 D1_FIRESTORE_URI secret，V 平台上線"
```

> **✅ M2 完成：V 平台已完整上線。**

---

## 附錄：本 plan 刻意不做的事

- **不做「客戶」聚合層**：影音與廣編分開兩列（已拍板決策 3）。AE 要看客戶總預算得自己加總。若之後要做，需要新增 client 表並改 API 與前端，是獨立的 plan。
- **不重構 R/D/M 的既有切段邏輯**：`segment_dates` 只給 V 用；R 的 7 天與 M 的 90 天保留各自的 inline 實作。
- **不處理 Prism 的 `domain` / `slot` 媒體維度**：那是 D/R/M 都沒有的能力，但 BH 是預算追蹤工具，用不到。
- **不改 `r_client.py` 寫死的 token 預設值**：那是既有問題，不在本次範圍；但新平台一律不沿用該做法。
- **不做 `.gitignore` 調整**：`poc/` 已在既有規則涵蓋範圍內，執行 Task 2/10 時確認一次即可。
