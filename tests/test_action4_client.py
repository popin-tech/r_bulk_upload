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
        self.assertGreater(state['max'], 1, '應該真的有併發，否則這個測試沒意義')


if __name__ == '__main__':
    unittest.main()
