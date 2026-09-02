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

    def test_booleans_are_not_counted_as_numbers(self):
        # isinstance(True, int) 是 True，防禦 Action4 回布林時被當成 1
        m = day_metrics({"mobile_video_imp": True, "charge": {"mobile_video_imp": True}})
        self.assertEqual(m['impressions'], 0)
        self.assertEqual(m['spend'], 0.0)


if __name__ == '__main__':
    unittest.main()
