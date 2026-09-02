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

    def test_no_date_is_lost(self):
        days = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 3, 1),
                date(2026, 3, 2), date(2026, 3, 3)]
        flat = [d for seg in segment_dates(days, 360) for d in seg]
        self.assertEqual(flat, days)


if __name__ == '__main__':
    unittest.main()
