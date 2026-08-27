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
        ):
            result = bh_service.BHService().process_excel_upload(
                TEMPLATE_PATH,
                owner_email="test@example.com",
            )

        d_token = next(record for record in fake_db.session.added if isinstance(record, _DToken))
        m_token = next(record for record in fake_db.session.added if isinstance(record, _MgidToken))

        self.assertEqual(result, {"total": 3, "inserted": 3, "errors": []})
        self.assertEqual(d_token.token, expected_d_token)
        self.assertEqual(m_token.token, expected_m_token)
        self.assertTrue(fake_db.session.committed)


if __name__ == "__main__":
    unittest.main()
