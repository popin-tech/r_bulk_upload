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
