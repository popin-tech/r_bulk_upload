"""回歸測試：背景執行緒不得持有 SQLAlchemy 實例。

2026-09-02 正式環境事故：M 區塊的 worker 第一行是 `m_token_map.get(acc.account_id)`，
而主執行緒每次 _upsert_stats 都會 commit；SQLAlchemy 預設 expire_on_commit=True，
commit 後所有實例失效，worker 再讀屬性會觸發 refresh → 需要 app context → 背景執行緒
沒有 → RuntimeError「Working outside of application context」。該行又在 try 之外，
例外穿過 future.result() 冒到最外層，**整個 sync_daily_stats 中止**。

舊版 M 是最後一個平台，所以只是靜默少同步幾個 M 帳戶（正式環境 18 個 active 只寫到
14~16 個，長期沒人發現）；加了 P/V 之後，同一個中止會連 P、V 一起殺掉。

測試手法：_upsert_stats 換成「只記錄呼叫、但照樣 db.session.commit()」的 stub。
commit 正是讓實例過期的機制，所以這樣就能忠實重現，又不必讓 sqlite 去吃 MySQL 的型別。
"""
import unittest
from datetime import date
from unittest.mock import patch

from flask import Flask

from database import db, BHAccount, BHDailyStats
from services.bh_sync import BHSyncService

TARGET = '2026-09-01'
# 帳戶數必須 > executor 的 max_workers(5)，否則所有 worker 都在第一次 commit 之前跑完，
# 過期問題不會浮現。
M_ACCOUNT_COUNT = 12


class TestSyncDailyStatsThreadSafety(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        db.init_app(self.app)
        self.ctx = self.app.app_context()
        self.ctx.push()
        # 只建需要的表：db.create_all() 會連 nexus.* 共用庫表一起建，sqlite 不支援 schema
        BHAccount.__table__.create(db.engine)
        for i in range(M_ACCOUNT_COUNT):
            db.session.add(BHAccount(
                platform='M', account_id=f'86{i:04d}', account_name=f'M-{i}',
                budget=1000, start_date=date(2026, 8, 1), end_date=date(2026, 9, 30),
                owner_email='t@example.com', status='active'))
        db.session.commit()
        self.addCleanup(self._teardown)

    def _teardown(self):
        db.session.remove()
        BHAccount.__table__.drop(db.engine)
        self.ctx.pop()

    def _run(self):
        upserted = []

        def fake_upsert(_self, account_id, d, stats, app=None):
            upserted.append(str(account_id))
            db.session.commit()      # ← 真正讓 ORM 實例過期的那一步

        msgs = []
        with patch.object(BHSyncService, '_upsert_stats', fake_upsert), \
             patch('services.bh_sync.get_mgid_token_map',
                   side_effect=lambda ids: {str(i): 'tok' for i in ids}), \
             patch('services.bh_sync.MgidClient') as MockClient:
            MockClient.return_value.fetch_daily_stats.side_effect = (
                lambda acc_id, s, e: {(str(acc_id), s): {
                    'spend': 1.0, 'impressions': 2, 'clicks': 3, 'conversions': 0}})
            for chunk in BHSyncService().sync_daily_stats(target_date=TARGET):
                msgs.append(chunk)
        return ''.join(msgs), upserted

    def test_sync_does_not_abort_with_app_context_error(self):
        blob, _ = self._run()
        self.assertNotIn('Working outside of application context', blob)
        self.assertNotIn('Critical Error', blob)

    def test_every_m_account_is_synced(self):
        # 中止的症狀就是「只寫了前幾個帳戶」——正式環境 18 個 active 只寫到 14~16 個
        _, upserted = self._run()
        self.assertEqual(len(upserted), M_ACCOUNT_COUNT)
        self.assertEqual(sorted(upserted),
                         sorted(f'86{i:04d}' for i in range(M_ACCOUNT_COUNT)))


if __name__ == '__main__':
    unittest.main()
