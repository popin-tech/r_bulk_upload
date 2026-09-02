import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import database
from services import bh_service


TEMPLATE_PATH = Path(__file__).parents[1] / "static" / "bh_import_template.xlsx"
TOKEN_COLUMN = "D&MGID的Token"


class _EmptyQuery:
    def filter_by(self, **kwargs):
        return self

    def first(self):
        return None


class _Record:
    query = _EmptyQuery()

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.id = None


class _Account(_Record):
    pass


class _DToken(_Record):
    pass


class _MgidToken(_Record):
    pass


class _Session:
    def __init__(self):
        self.added = []
        self.committed = False

    def add(self, record):
        self.added.append(record)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


class _DB:
    def __init__(self):
        self.session = _Session()


class BHSharedTokenColumnTest(unittest.TestCase):
    def test_template_imports_shared_token_for_d_and_mgid(self):
        template = pd.read_excel(TEMPLATE_PATH)
        self.assertIn(TOKEN_COLUMN, template.columns)

        expected_d_token = str(template.loc[template["平台"] == "D", TOKEN_COLUMN].iloc[0]).strip()
        expected_m_token = str(template.loc[template["平台"] == "M", TOKEN_COLUMN].iloc[0]).strip()
        fake_db = _DB()

        with (
            patch.object(bh_service, "db", fake_db),
            patch.object(bh_service, "BHAccount", _Account),
            patch.object(bh_service, "BHDAccountToken", _DToken),
            patch.object(database, "MgidToken", _MgidToken),
            # 範本自 2026-09-02 起多了 P 與 V 兩列範例。V 的列在上傳時會去比對
            # Firestore 影音目錄，測試環境沒有 D1_FIRESTORE_URI，故在此 mock 掉；
            # 本測試要驗的是 D/MGID 共用 token 欄位，不是影音目錄。
            patch("services.bh_clients.d1_video_catalog.d1_firestore_available",
                  return_value=True),
            patch("services.bh_clients.d1_video_catalog.list_video_campaigns",
                  return_value=[]),
            patch("services.bh_clients.d1_video_catalog.validate_account",
                  return_value=(True, [])),
        ):
            result = bh_service.BHService().process_excel_upload(
                TEMPLATE_PATH,
                owner_email="test@example.com",
            )

        d_token = next(record for record in fake_db.session.added if isinstance(record, _DToken))
        m_token = next(record for record in fake_db.session.added if isinstance(record, _MgidToken))

        # 範本五列：R / D / M / P / V
        self.assertEqual(result, {"total": 5, "inserted": 5, "errors": []})
        self.assertEqual(d_token.token, expected_d_token)
        self.assertEqual(m_token.token, expected_m_token)
        self.assertTrue(fake_db.session.committed)


if __name__ == "__main__":
    unittest.main()
