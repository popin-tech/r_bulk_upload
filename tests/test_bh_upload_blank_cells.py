"""BH 帳戶上傳：空白／N/A 儲存格不能變成 'nan' 寫進資料庫。

2026-10-14 事件：AE 上傳的 D 列 AccID 空白，pandas 讀成 NaN，str() 後變 'nan'，
`if not acc_id` 擋不住 ⇒ bh_accounts 與共用 token 表 nexus.d_tokens 都多一列 account_id='nan'，
nexus 資料倉庫把它當新帳戶又抓一份 37975 的數字（10/4、10/5 D 花費多算 6,560）。
同一個成因還有：
  - 整欄 AccID 純數字、其中一格空白 ⇒ pandas 把整欄讀成小數，其他正常列變成 '37975.0'
  - 名稱空白 ⇒ account_name 'nan'
  - Budget／日期空白 ⇒ NaN／NaT 送進 MySQL，整批 commit 失敗、看不出是哪一列
"""
import unittest
from io import BytesIO
from unittest.mock import patch

from openpyxl import Workbook

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


class _Account(_Rec): pass
class _DToken(_Rec): pass
class _MgidToken(_Rec): pass


class _Session:
    def __init__(self): self.added = []
    def add(self, r): self.added.append(r)
    def commit(self): pass
    def rollback(self): pass


class _DB:
    def __init__(self): self.session = _Session()


HEADERS = ['平台', 'AccID（MGID 填 API ID）', '名稱', 'Budget', 'StartDate', 'EndDate',
           'CPCGoal', 'CPAGoal', 'R的cv定義', 'D&MGID的Token']


def row(platform='D', acc=37975, name='直客_仙桃牌', budget=116000,
        start='2026-09-08', end='2026-10-14', token='tok_abc'):
    return [platform, acc, name, budget, start, end, None, None, None, token]


def upload(rows):
    """用 openpyxl 產生真的 xlsx（空白就是真的空儲存格），跑真正的 process_excel_upload。"""
    wb = Workbook()
    ws = wb.active
    ws.append(HEADERS)
    for r in rows:
        ws.append(r)
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    db = _DB()
    with (
        patch.object(bh_service, 'db', db),
        patch.object(bh_service, 'BHAccount', _Account),
        patch.object(bh_service, 'BHDAccountToken', _DToken),
        patch.object(database, 'MgidToken', _MgidToken),
    ):
        result = bh_service.BHService().process_excel_upload(buf, owner_email='t@example.com')
    return result, db.session.added


def of(added, cls):
    return [r for r in added if isinstance(r, cls)]


class BlankAccIdTest(unittest.TestCase):
    def test_blank_accid_with_token_writes_nothing(self):
        # 本次事件：D、AccID 空白、有填 token
        result, added = upload([row(acc=None)])
        self.assertEqual(result['inserted'], 0)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('Row 2', result['errors'][0])
        self.assertIn('AccID', result['errors'][0])
        self.assertEqual(added, [])

    def test_blank_accid_for_every_platform(self):
        result, added = upload([row(platform='M', acc=None), row(platform='R', acc=None, token=None)])
        self.assertEqual(result['inserted'], 0)
        self.assertEqual(len(result['errors']), 2)
        self.assertEqual(added, [])

    def test_na_like_text_is_blank(self):
        # pandas 預設把這些字當缺值；手打也不該變成帳戶 ID
        result, added = upload([row(acc=v) for v in ('N/A', 'NA', 'null', 'None', 'nan', '#N/A', '   ')])
        self.assertEqual(result['inserted'], 0)
        self.assertEqual(len(result['errors']), 7)
        self.assertEqual(added, [])

    def test_one_blank_does_not_turn_other_ids_into_floats(self):
        result, added = upload([row(acc=37975), row(acc=None, token=None), row(platform='M', acc=866244)])
        self.assertEqual(result['inserted'], 2)
        self.assertEqual(len(result['errors']), 1)
        self.assertEqual(sorted(a.account_id for a in of(added, _Account)), ['37975', '866244'])
        self.assertEqual([t.account_id for t in of(added, _DToken)], ['37975'])
        self.assertEqual([t.api_client_id for t in of(added, _MgidToken)], ['866244'])

    def test_text_formatted_id_still_works(self):
        result, added = upload([row(acc='37975'), row(platform='P', acc='233-688-3595', token=None)])
        self.assertEqual(result, {'total': 2, 'inserted': 2, 'errors': []})
        self.assertEqual([a.account_id for a in of(added, _Account)], ['37975', '233-688-3595'])

    def test_wrong_id_format_is_rejected(self):
        # D／R／M 要純數字、P 要 xxx-xxx-xxxx（nexus 資料倉庫與走速用同一套規則）
        result, added = upload([
            row(acc='37975.5'), row(platform='R', acc='abc', token=None),
            row(platform='M', acc='client:1'), row(platform='P', acc='2336883595', token=None),
        ])
        self.assertEqual(result['inserted'], 0)
        self.assertEqual(len(result['errors']), 4)
        self.assertEqual(added, [])


class OtherBlankCellsTest(unittest.TestCase):
    def test_blank_name_falls_back_instead_of_nan(self):
        result, added = upload([row(name=None), row(acc=37976, name='N/A')])
        self.assertEqual(result['inserted'], 2)
        names = [a.account_name for a in of(added, _Account)] + [t.account_name for t in of(added, _DToken)]
        self.assertNotIn('nan', names)
        self.assertEqual([a.account_name for a in of(added, _Account)], ['D_37975', 'D_37976'])
        self.assertEqual([t.account_name for t in of(added, _DToken)], ['D_37975', 'D_37976'])

    def test_blank_budget_is_a_row_error_not_a_batch_failure(self):
        result, added = upload([row(budget=None), row(acc=37976)])
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('Row 2', result['errors'][0])
        self.assertIn('Budget', result['errors'][0])
        self.assertEqual([a.account_id for a in of(added, _Account)], ['37976'])

    def test_blank_dates_are_row_errors(self):
        result, added = upload([row(start=None), row(acc=37976, end=None), row(acc=37977)])
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(len(result['errors']), 2)
        self.assertEqual([a.account_id for a in of(added, _Account)], ['37977'])

    def test_blank_platform_message(self):
        result, added = upload([row(platform=None)])
        self.assertEqual(result['inserted'], 0)
        self.assertNotIn("'NAN'", result['errors'][0])
        self.assertEqual(added, [])


if __name__ == '__main__':
    unittest.main()
