import unittest
from unittest.mock import patch

from services.bh_clients import d1_video_catalog as cat


# 取自 2026-09-01 真實 Firestore 查詢的樣本
FAKE_CAMPAIGNS = [
    {'id': '6a1d75328ac0fc73cf275385', 'name': '4A_CPM_chubblife_202606_heho',
     'account': '4A_CPM_chubblife', 'agency': 'popintw', 'vertical_video': False, 'deleted': False},
    {'id': '6a8dbcf60b53275b884686e5', 'name': '國泰影音',
     'account': 'EVOX_CPM', 'agency': 'popintw', 'vertical_video': False, 'deleted': False},
    {'id': '6a913d5f8dbcc677dd696cb6', 'name': '國泰直式影音',
     'account': 'EVOX_CPM', 'agency': 'popintw', 'vertical_video': True, 'deleted': True},
    {'id': '5e82b7a80fc10c15f35e8b94', 'name': '【Video】亞克娛樂',
     'account': 'arch', 'agency': 'haja', 'vertical_video': False, 'deleted': True},
]


class CatalogTestCase(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(cat, 'list_video_campaigns', return_value=FAKE_CAMPAIGNS)
        self.addCleanup(patcher.stop)
        patcher.start()


class TestListAccounts(CatalogTestCase):
    def test_includes_accounts_that_only_have_deleted_campaigns(self):
        # arch 只有一支已刪 campaign，但它花過的錢仍計入預算 → 必須在清單裡
        self.assertEqual(cat.list_accounts(), ['4A_CPM_chubblife', 'EVOX_CPM', 'arch'])


class TestCampaignsForAccount(CatalogTestCase):
    def test_excludes_deleted_by_default(self):
        self.assertEqual(cat.campaigns_for_account('arch'), [])

    def test_include_deleted_returns_them(self):
        self.assertEqual(len(cat.campaigns_for_account('arch', include_deleted=True)), 1)

    def test_include_deleted_returns_both_campaigns_of_account(self):
        ids = {c['id'] for c in cat.campaigns_for_account('EVOX_CPM', include_deleted=True)}
        self.assertEqual(ids, {'6a8dbcf60b53275b884686e5', '6a913d5f8dbcc677dd696cb6'})


class TestValidateAccount(CatalogTestCase):
    def test_exact_match_is_valid(self):
        self.assertEqual(cat.validate_account('EVOX_CPM'), (True, []))

    def test_account_with_only_deleted_campaigns_is_valid(self):
        # 與 list_accounts 一致：驗證通過的帳戶，同步時一定拿得到 campaign
        ok, _ = cat.validate_account('arch')
        self.assertTrue(ok)

    def test_typo_returns_suggestion(self):
        # 少一個 b —— Action4 查不到只會回空，BH 會靜默記 0，所以必須在上傳當下擋
        ok, suggestions = cat.validate_account('4A_CPM_chublife')
        self.assertFalse(ok)
        self.assertIn('4A_CPM_chubblife', suggestions)

    def test_case_mismatch_is_invalid_but_suggested(self):
        ok, suggestions = cat.validate_account('evox_cpm')
        self.assertFalse(ok)
        self.assertIn('EVOX_CPM', suggestions)

    def test_garbage_returns_no_suggestion(self):
        self.assertEqual(cat.validate_account('zzzzzzzzzzzz'), (False, []))


if __name__ == '__main__':
    unittest.main()
