"""欄位標題帶括號註解時仍要能上傳。

範本自 2026-09-03 起把 AccID 的標題改成「AccID（MGID 填 API ID）」，但：
  1. 程式是用標題文字當鍵取值（row.get('AccID')）；
  2. AE 手上還留著舊範本，欄名是純 'AccID'。
⇒ 讀進來先把括號註解去掉，新舊範本都要能收。
"""
import unittest
from io import BytesIO
from unittest.mock import patch

import pandas as pd

import database
from services import bh_service


class _Q:
    def filter_by(self, **kwargs): return self
    def first(self): return None


class _Rec:
    query = _Q()
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.id = None


class _Session:
    def __init__(self): self.added = []
    def add(self, r): self.added.append(r)
    def commit(self): pass
    def rollback(self): pass


class _DB:
    def __init__(self): self.session = _Session()


def _excel(accid_header):
    cols = ['平台', accid_header, '名稱', 'Budget', 'StartDate', 'EndDate',
            'CPCGoal', 'CPAGoal', 'R的cv定義', 'D&MGID的Token']
    rows = [['R', 9269, 'juliart', 35000, '2026-07-01', '2026-07-31', 5, None, 'CompleteCheckout', None]]
    buf = BytesIO()
    pd.DataFrame(rows, columns=cols).to_excel(buf, index=False)
    buf.seek(0)
    return buf


def _upload(accid_header):
    fake = _DB()
    with patch.object(bh_service, 'db', fake), \
         patch.object(bh_service, 'BHAccount', _Rec), \
         patch.object(bh_service, 'BHDAccountToken', _Rec), \
         patch.object(database, 'MgidToken', _Rec):
        result = bh_service.BHService().process_excel_upload(_excel(accid_header), 'me@popin.cc')
    return result, fake.session.added


class TestHeaderNotes(unittest.TestCase):
    def test_plain_header_still_works(self):
        # AE 手上的舊範本
        result, added = _upload('AccID')
        self.assertEqual(result['inserted'], 1, result)
        self.assertEqual(added[0].account_id, '9269')

    def test_fullwidth_parenthesised_note(self):
        # 新範本
        result, added = _upload('AccID（MGID 填 API ID）')
        self.assertEqual(result['inserted'], 1, result)
        self.assertEqual(added[0].account_id, '9269')

    def test_halfwidth_parenthesised_note(self):
        result, added = _upload('AccID (MGID 填 API ID)')
        self.assertEqual(result['inserted'], 1, result)
        self.assertEqual(added[0].account_id, '9269')

    def test_strip_helper_is_a_noop_for_plain_names(self):
        for name in ('平台', 'Budget', 'StartDate', 'R的cv定義', 'D&MGID的Token'):
            self.assertEqual(bh_service._strip_header_note(name), name)


if __name__ == '__main__':
    unittest.main()
