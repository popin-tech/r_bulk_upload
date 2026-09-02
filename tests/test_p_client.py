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
