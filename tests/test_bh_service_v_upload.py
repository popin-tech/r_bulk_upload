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

    @patch('services.bh_service.db')
    @patch('services.bh_clients.d1_video_catalog.validate_account', return_value=(True, []))
    @patch('services.bh_clients.d1_video_catalog.list_video_campaigns')
    @patch('services.bh_clients.d1_video_catalog.d1_firestore_available', return_value=True)
    def test_catalog_is_only_touched_once_per_upload(
            self, _avail, mock_list, _validate, mock_db):
        # 目錄有 5 分鐘快取，但整批上傳也只該主動預熱一次
        BHService().process_excel_upload(_excel([ROW_OK, ROW_OK, ROW_OK]), 'me@popin.cc')
        self.assertEqual(mock_list.call_count, 1)

    @patch('services.bh_service.db')
    @patch('services.bh_clients.d1_video_catalog.d1_firestore_available', return_value=True)
    def test_non_v_upload_does_not_touch_firestore(self, mock_avail, mock_db):
        # 純 R/D/M/P 的上傳不該去連 Firestore
        row_p = ['P', '292-462-3142', 'Prism', 80000, '2026-08-01', '2026-08-31', 12, None, '', '']
        result = BHService().process_excel_upload(_excel([row_p]), 'me@popin.cc')
        self.assertEqual(result['inserted'], 1)
        mock_avail.assert_not_called()


if __name__ == '__main__':
    unittest.main()
