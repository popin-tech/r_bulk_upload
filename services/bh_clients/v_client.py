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
           日期寫成 0；補洞檢查（bh_sync.py 的 missing_dates）只看「那天有沒有列」，
           0 一旦寫進去就永遠不會被修正 → 永久低估。
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
